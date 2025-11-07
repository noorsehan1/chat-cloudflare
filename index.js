import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

function createEmptySeat() {
  return {
    noimageUrl: "",
    namauser: "",
    color: "",
    itembawah: 0,
    itematas: 0,
    vip: 0,
    viptanda: 0,
    points: [],
    lockTime: undefined
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.clients = new Set();
    this.userToSeat = new Map();

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;
    
    this._timers = [];
    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), 100);
    this._timers.push(this._tickTimer, this._flushTimer);

    this.lowcard = new LowCardGameManager(this);

    this.offlineUsers = new Map();
    this.offlineTimers = new Map();
    this.lastActivity = new Map();
    this.pingTimeouts = new Map();
    
    this.OFFLINE_TIMEOUT_MS = 30 * 1000;
    this.PING_TIMEOUT_MS = 30 * 1000;
    this.HEARTBEAT_INTERVAL = 15 * 1000;
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        if (ws.idtarget) {
          this.lastActivity.set(ws.idtarget, Date.now());
        }
        return true;
      }
    } catch (e) {
      this.cleanupClient(ws);
    }
    return false;
  }

  safeBroadcastToRoom(room, msg) {
    try {
      this.broadcastToRoom(room, msg);
    } catch (error) {}
  }

  sendPingToClient(ws) {
    if (ws.idtarget && ws.readyState === 1) {
      const pingSent = this.safeSend(ws, ["ping", ws.idtarget]);
      if (pingSent) {
        this.setPingTimeout(ws.idtarget);
      }
    }
  }

  setPingTimeout(userId) {
    if (this.pingTimeouts.has(userId)) {
      clearTimeout(this.pingTimeouts.get(userId));
    }

    const timeoutId = setTimeout(() => {
      const userWs = Array.from(this.clients).find(c => c.idtarget === userId);
      if (userWs) {
        this.handlePingTimeout(userWs);
      }
      this.pingTimeouts.delete(userId);
    }, this.PING_TIMEOUT_MS);

    this.pingTimeouts.set(userId, timeoutId);
  }

  handlePingTimeout(ws) {
    const userId = ws.idtarget;
    if (!userId) return;

    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(["needReconnect", "Ping timeout - please reconnect"]));
      }
    } catch (e) {}

    this.cleanupClient(ws);
  }

  heartbeat() {
    const now = Date.now();
    for (const ws of Array.from(this.clients)) {
      if (ws.readyState === 1 && ws.idtarget) {
        const lastActive = this.lastActivity.get(ws.idtarget) || 0;
        
        if (now - lastActive >= this.HEARTBEAT_INTERVAL) {
          this.sendPingToClient(ws);
        }
      }
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of Array.from(this.clients)) {
      if (c.roomname === room) this.safeSend(c, msg);
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const info of seatMap.values()) {
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.safeBroadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 100) {
        messages.splice(0, messages.length - 100);
      }
      for (const msg of messages) this.safeBroadcastToRoom(room, msg);
      messages.length = 0;
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      const updates = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatMapUpdates.has(seat)) continue;
        const info = seatMapUpdates.get(seat);
        const { points, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0)
        this.safeBroadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      seatMapUpdates.clear();
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of Array.from(this.clients)) this.safeSend(c, ["currentNumber", this.currentNumber]);
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          this.safeBroadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  periodicFlush() {
    try { this.flushKursiUpdates(); } catch (e) {}
    try { this.flushChatBuffer(); } catch (e) {}
    try { this.cleanExpiredLocks(); } catch (e) {}
    
    try { this.heartbeat(); } catch (e) {}
    try { this.checkInactiveUsers(); } catch (e) {}
    try { this.checkOfflineUsers(); } catch (e) {}

    for (const [id, msgs] of Array.from(this.privateMessageBuffer)) {
      try {
        for (const c of this.clients) {
          if (c.idtarget === id) {
            for (const m of msgs) this.safeSend(c, m);
            this.privateMessageBuffer.delete(id);
            if (c.roomname) this.broadcastRoomUserCount(c.roomname);
            break;
          }
        }
      } catch (e) {}
    }
  }

  checkInactiveUsers() {
    const now = Date.now();
    const toRemove = [];

    for (const [id, lastActive] of this.lastActivity.entries()) {
      if (now - lastActive >= this.PING_TIMEOUT_MS + 5000) {
        toRemove.push(id);
      }
    }

    for (const id of toRemove) {
      this.forceUserReconnect(id);
    }
  }

  forceUserReconnect(userId) {
    const userWs = Array.from(this.clients).find(c => c.idtarget === userId);
    if (userWs) {
      try {
        if (userWs.readyState === 1) {
          userWs.send(JSON.stringify(["needReconnect", "Inactive - please reconnect"]));
        }
      } catch (e) {}

      this.cleanupClient(userWs);
    }

    const hadSeats = this.userToSeat.has(userId);
    if (hadSeats) {
      this.offlineUsers.set(userId, {
        roomname: "",
        seats: [],
        timestamp: Date.now(),
        needsReconnect: true
      });
      this.scheduleOfflineRemoval(userId);
    }

    this.offlineTimers.delete(userId);
    this.lastActivity.delete(userId);
    this.pingTimeouts.delete(userId);
    this.removeAllSeatsById(userId);
  }

  checkOfflineUsers() {
    const now = Date.now();
    const toRemove = [];
    
    for (const [id, saved] of this.offlineUsers.entries()) {
      if (now - saved.timestamp >= this.OFFLINE_TIMEOUT_MS) {
        toRemove.push(id);
      }
    }

    for (const id of toRemove) {
      this.offlineUsers.delete(id);
      this.offlineTimers.delete(id);
      this.lastActivity.delete(id);
      this.pingTimeouts.delete(id);
      this.removeAllSeatsById(id);
    }
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;
    const now = Date.now();

    for (const [seat, info] of seatMap) {
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000)
        Object.assign(info, createEmptySeat());
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (!k) continue;
      if (k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = now;
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const meta = {};
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      for (const p of info.points) allPoints.push({ seat, ...p });
      if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
        meta[seat] = {
          noimageUrl: info.noimageUrl,
          namauser: info.namauser,
          color: info.color,
          itembawah: info.itembawah,
          itematas: info.itematas,
          vip: info.vip,
          viptanda: info.viptanda
        };
      }
    }
    this.safeSend(ws, ["allPointsList", room, allPoints]);
    this.safeSend(ws, ["allUpdateKursiList", room, meta]);
  }

  cleanupClientById(idtarget) {
    for (const c of Array.from(this.clients)) {
      if (c.idtarget === idtarget) {
        try { c.close(4000, "Duplicate connection cleanup"); } catch {}
        this.cleanupondestroy(c);
      }
    }
  }

  removeAllSeatsById(idtarget) {
    let removedAny = false;
    for (const [room, seatMap] of this.roomSeats) {
      let removedInRoom = false;
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.safeBroadcastToRoom(room, ["removeKursi", room, seat]);
          removedInRoom = true;
          removedAny = true;
        }
      }
      if (removedInRoom) this.broadcastRoomUserCount(room);
    }
    this.userToSeat.delete(idtarget);
  }

  getAllOnlineUsers() {
    const users = [];
    for (const ws of this.clients) if (ws.idtarget) users.push(ws.idtarget);
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    for (const ws of this.clients) if (ws.roomname === roomName && ws.idtarget) users.push(ws.idtarget);
    return users;
  }

  scheduleOfflineRemoval(idtarget) {
    if (this.offlineTimers.has(idtarget)) {
      clearTimeout(this.offlineTimers.get(idtarget));
    }
    
    const timeoutId = setTimeout(() => {
      if (this.offlineUsers.has(idtarget)) {
        this.offlineUsers.delete(idtarget);
        this.removeAllSeatsById(idtarget);
        this.lastActivity.delete(idtarget);
        this.pingTimeouts.delete(idtarget);
      }
      this.offlineTimers.delete(idtarget);
    }, this.OFFLINE_TIMEOUT_MS);
    
    this.offlineTimers.set(idtarget, timeoutId);
  }

  cancelOfflineRemoval(idtarget) {
    if (this.offlineTimers.has(idtarget)) {
      clearTimeout(this.offlineTimers.get(idtarget));
      this.offlineTimers.delete(idtarget);
    }
    if (this.offlineUsers.has(idtarget)) {
      this.offlineUsers.delete(idtarget);
    }
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
  }

  cleanupClient(ws) {
    if (ws._cleanupCalled) return;
    ws._cleanupCalled = true;

    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    if (ws.roomname && !this.offlineUsers.has(id)) {
      this.offlineUsers.set(id, {
        roomname: ws.roomname,
        seats: ws.numkursi ? Array.from(ws.numkursi) : [],
        timestamp: Date.now(),
        needsReconnect: false
      });
      this.scheduleOfflineRemoval(id);
    }

    this.clients.delete(ws);
    const previousRoom = ws.roomname;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    
    this.pingTimeouts.delete(id);
    
    if (previousRoom && roomList.includes(previousRoom)) {
      setTimeout(() => this.broadcastRoomUserCount(previousRoom), 50);
    }
  }

  cleanupondestroy(ws) {
    if (!ws) return;
    
    const id = ws.idtarget;
    
    if (id) {
      this.removeAllSeatsById(id);
      this.cancelOfflineRemoval(id);
      this.lastActivity.delete(id);
      
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }
      
      this.userToSeat.delete(id);
      this.offlineUsers.delete(id);
      
      if (this.offlineTimers.has(id)) {
        clearTimeout(this.offlineTimers.get(id));
        this.offlineTimers.delete(id);
      }
    }
    
    if (ws.numkursi) {
      ws.numkursi.clear();
    }
    
    this.clients.delete(ws);
    const previousRoom = ws.roomname;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    
    try {
      if (ws.readyState === 1) {
        ws.close(1000, "Cleanup on destroy");
      }
    } catch (e) {}
    
    if (previousRoom) {
      this.broadcastRoomUserCount(previousRoom);
    }
  }

  handleMessage(ws, raw) {
    if (ws.idtarget) {
      this.lastActivity.set(ws.idtarget, Date.now());
    }

    let data;
    try { data = JSON.parse(raw); } catch { return this.safeSend(ws, ["error", "Invalid JSON"]); }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    switch (evt) {
      case "onDestroy": {
        this.cleanupondestroy(ws);
        break;
      }
      case "setIdTarget": {
        const newId = data[1];
        this.cleanupClientById(newId);
        ws.idtarget = newId;

        this.lastActivity.set(newId, Date.now());

        if (this.privateMessageBuffer.has(ws.idtarget)) {
          for (const msg of this.privateMessageBuffer.get(ws.idtarget)) this.safeSend(ws, msg);
          this.privateMessageBuffer.delete(ws.idtarget);
        }

        const offline = this.offlineUsers.get(newId);
        if (offline) {
          if (offline.needsReconnect) {
            this.safeSend(ws, ["needJoinRoom", "Session expired - please join room again"]);
            this.offlineUsers.delete(newId);
            this.cancelOfflineRemoval(newId);
          } else {
            const { roomname, seats } = offline;
            ws.roomname = roomname;
            ws.numkursi = new Set(seats);

            const seatMap = this.roomSeats.get(roomname);
            for (const s of seats) {
              const info = seatMap.get(s);
              if (info.namauser === "" || info.namauser.startsWith("__LOCK__")) {
                info.namauser = newId;
              }
            }

            this.sendAllStateTo(ws, roomname);
            this.broadcastRoomUserCount(roomname);

            this.offlineUsers.delete(newId);
            this.cancelOfflineRemoval(newId);
            
            this.safeSend(ws, ["reconnectSuccess", roomname]);
          }
        }
        break;
      }

      case "pong": {
        if (ws.idtarget) {
          this.lastActivity.set(ws.idtarget, Date.now());
          if (this.pingTimeouts.has(ws.idtarget)) {
            clearTimeout(this.pingTimeouts.get(ws.idtarget));
            this.pingTimeouts.delete(ws.idtarget);
          }
        }
        break;
      }

      case "ping": {
        const idtarget = data[1];
        if (idtarget) {
          this.lastActivity.set(idtarget, Date.now());
        }
        this.safeSend(ws, ["pong"]);
        break;
      }

      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        let delivered = false;
        for (const c of this.clients) if (c.idtarget === idtarget) { this.safeSend(c, notif); delivered = true; }
        if (!delivered) {
          if (!this.privateMessageBuffer.has(idtarget)) this.privateMessageBuffer.set(idtarget, []);
          this.privateMessageBuffer.get(idtarget).push(notif);
        }
        break;
      }

      case "private": {
        const [, idt, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", idt, url, msg, ts, sender];
        this.safeSend(ws, out);
        let delivered = false;
        for (const c of this.clients) if (c.idtarget === idt) { this.safeSend(c, out); delivered = true; }
        if (!delivered) {
          if (!this.privateMessageBuffer.has(idt)) this.privateMessageBuffer.set(idt, []);
          this.privateMessageBuffer.get(idt).push(out);
          this.safeSend(ws, ["privateFailed", idt, "User offline"]);
        }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";

        const activeSockets = Array.from(this.clients).filter(c => c.idtarget === username);
        const online = activeSockets.length > 0;
        this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);

        if (activeSockets.length > 1) {
          const sortedSockets = activeSockets.sort((a, b) => (a._connectTime || 0) - (b._connectTime || 0));
          const newestSocket = sortedSockets[sortedSockets.length - 1];
          const oldSockets = sortedSockets.slice(0, -1);

          for (const oldWs of oldSockets) {
            if (oldWs.roomname && oldWs.numkursi) {
              for (const seat of oldWs.numkursi) {
                const seatMap = this.roomSeats.get(oldWs.roomname);
                if (seatMap && seatMap.has(seat)) {
                  Object.assign(seatMap.get(seat), createEmptySeat());
                  this.safeBroadcastToRoom(oldWs.roomname, ["removeKursi", oldWs.roomname, seat]);
                }
              }
              this.broadcastRoomUserCount(oldWs.roomname);
            }

            this.userToSeat.delete(username);

            try {
              if (oldWs.readyState === 1) {
                oldWs.close(4000, "Duplicate login — old session closed");
              }
              this.cleanupClient(oldWs);
            } catch (e) {
              this.cleanupClient(oldWs);
            }
          }
        }
        break;
      }

      case "getAllRoomsUserCount":
        this.handleGetAllRoomsUserCount(ws);
        break;

      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;

      case "getAllOnlineUsers":
        this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
        break;

      case "getRoomOnlineUsers": {
        const roomName = data[1];
        if (!roomList.includes(roomName)) return this.safeSend(ws, ["error", "Unknown room"]);
        this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
        break;
      }

      case "joinRoom": {
        const newRoom = data[1];
        if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
        if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);
        ws.roomname = newRoom;
        const seatMap = this.roomSeats.get(newRoom);
        const foundSeat = this.lockSeat(newRoom, ws);
        if (foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);
        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
        this.sendAllStateTo(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);
        
        if (ws.idtarget) {
          this.lastActivity.set(ws.idtarget, Date.now());
        }
        break;
      }

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname).push([
          "chat", roomname, noImageURL, username, message, usernameColor, chatTextColor
        ]);
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        si.points.push({ x, y, fast });
        if (si.points.length > 200) si.points.shift();
        this.safeBroadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        Object.assign(seatMap.get(seat), createEmptySeat());
        for (const c of this.clients) c.numkursi?.delete(seat);
        this.safeBroadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        Object.assign(currentInfo, { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda });
        seatMap.set(seat, currentInfo);
        if (!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room, new Map());
        this.updateKursiBuffer.get(room).set(seat, { ...currentInfo, points: [] });
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for gift"]);
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname).push([
          "gift", roomname, sender, receiver, giftName, Date.now()
        ]);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
        this.lowcard.handleEvent(ws, data);
        break;

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
    }
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") return new Response("Expected WebSocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const ws = server;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    ws._connectTime = Date.now();
    ws._cleanupCalled = false;

    this.clients.add(ws);

    const safeCleanup = () => {
      if (!ws._cleanupCalled) {
        ws._cleanupCalled = true;
        this.cleanupClient(ws);
      }
    };

    ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));
    ws.addEventListener("close", safeCleanup);
    ws.addEventListener("error", safeCleanup);

    return new Response(null, { status: 101, webSocket: client });
  }

  cleanupAllTimers() {
    for (const timer of this._timers) {
      clearInterval(timer);
    }
    this._timers = [];
  }
}

export default {
  async fetch(req, env) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if (new URL(req.url).pathname === "/health")
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
