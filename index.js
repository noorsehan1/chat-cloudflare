import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
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
    lastPoint: null,
    lockTime: undefined
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // Data structures
    this.clients = new Set();
    this.userToSeat = new Map();
    this.roomSeats = new Map();
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.roomChatHistory = new Map();
    this.seatLocks = new Map();
    this.userJoinLocks = new Map();
    this.messageCounts = new Map();
    this.pendingCleanups = new Map();
    
    // Game & VIP
    this.lowcard = new LowCardGameManager(this);
    this.vipManager = new VipBadgeManager(this);

    // Constants
    this.MAX_SEATS = 35;
    this.MAX_MESSAGES_PER_SECOND = 20;
    this.RECONNECT_GRACE_PERIOD = 20000;
    this.currentNumber = 1;
    this.maxNumber = 6;
    this.hasEverSetId = false;
    this._lastCleanupTime = 0;
    this._cleanupInterval = 30000;

    // Initialize rooms
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }

    // Number rotation global untuk semua user setiap 15 menit
    this._numberTimer = setInterval(() => {
      this.rotateNumber();
    }, 15 * 60 * 1000);

    this._nextConnId = 1;
  }

  rotateNumber() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;

    for (const c of this.clients) {
      if (c.readyState === 1 && c.roomname) {
        this.safeSend(c, ["currentNumber", this.currentNumber]);
      }
    }
  }

  processPendingCleanupsIfNeeded() {
    const now = Date.now();
    if (now - this._lastCleanupTime < this._cleanupInterval) {
      return;
    }
    
    this._lastCleanupTime = now;
    this.processPendingCleanups();
    this.cleanExpiredLocks();
  }

  processPendingCleanups() {
    const now = Date.now();
    const toRemove = [];
    
    for (const [idtarget, disconnectTime] of this.pendingCleanups) {
      if (now - disconnectTime < this.RECONNECT_GRACE_PERIOD) continue;
      
      const isStillConnected = Array.from(this.clients).some(
        client => client.idtarget === idtarget && client.readyState === 1
      );
      
      if (!isStillConnected) {
        console.log(`[CLEANUP] Removing user after grace period: ${idtarget}`);
        toRemove.push(idtarget);
      } else {
        toRemove.push(idtarget);
      }
    }
    
    for (const idtarget of toRemove) {
      if (!this.isUserConnected(idtarget)) {
        this.removeUserData(idtarget);
      }
      this.pendingCleanups.delete(idtarget);
    }
  }

  flushBuffersIfNeeded() {
    let hasData = false;
    
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        hasData = true;
        break;
      }
    }
    
    for (const [room, updates] of this.updateKursiBuffer) {
      if (updates.size > 0) {
        hasData = true;
        break;
      }
    }
    
    if (hasData) {
      this.flushBuffers();
    }
  }

  flushBuffers() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        for (const msg of messages) {
          this.broadcastToRoom(room, msg);
        }
        this.chatMessageBuffer.set(room, []);
      }
    }
    
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      const updates = [];
      for (const [seat, info] of seatMapUpdates) {
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  removeUserData(idtarget) {
    if (!idtarget) return;

    this.vipManager.cleanupUserVipBadges(idtarget);

    for (const [room, seatMap] of this.roomSeats) {
      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget || seatInfo.namauser === `__LOCK__${idtarget}`) {
          Object.assign(seatInfo, createEmptySeat());
          this.seatLocks.delete(`${room}-${seatNumber}`);
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.userJoinLocks.delete(idtarget);
  }

  isUserConnected(idtarget) {
    return Array.from(this.clients).some(
      client => client.idtarget === idtarget && client.readyState === 1
    );
  }

  cleanExpiredLocks() {
    const now = Date.now();
    
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          this.seatLocks.delete(`${room}-${seat}`);
          this.clearSeatBuffer(room, seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  async acquireUserJoinLock(idtarget) {
    if (!idtarget) return true;
    
    if (this.userJoinLocks.has(idtarget)) {
      return false;
    }
    
    this.userJoinLocks.set(idtarget, Date.now());
    return true;
  }

  releaseUserJoinLock(idtarget) {
    if (!idtarget) return;
    this.userJoinLocks.delete(idtarget);
  }

  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const key = ws.idtarget || ws._connId || 'anonymous';
    const windowStart = Math.floor(now / 1000);

    if (!this.messageCounts.has(key)) {
      this.messageCounts.set(key, { count: 0, window: windowStart });
    }

    const stats = this.messageCounts.get(key);
    if (stats.window !== windowStart) {
      stats.count = 0;
      stats.window = windowStart;
    }

    let limit = this.MAX_MESSAGES_PER_SECOND;
    if (messageType === "chat") limit = 50;
    else if (messageType === "updatePoint") limit = 100;

    stats.count += 1;
    if (stats.count > limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }

    return true;
  }

  safeSend(ws, arr) {
    try {
      if (ws && ws.readyState === 1) {
        if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 1_000_000) {
          return false;
        }
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {
      // silent
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    let sentCount = 0;
    for (const client of this.clients) {
      if (client.roomname === room && client.readyState === 1) {
        if (this.safeSend(client, msg)) {
          sentCount++;
        }
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) {
      counts[room] = 0;
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const info of seatMap.values()) {
          if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
            counts[room]++;
          }
        }
      }
    }
    return counts;
  }

  broadcastRoomUserCount(room) {
    try {
      const count = this.getJumlahRoom()[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {
      // silent
    }
  }

  clearSeatBuffer(room, seatNumber) {
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) {
      roomMap.delete(seatNumber);
    }
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    const userId = ws.idtarget;

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (seatMap.get(i)?.namauser === userId) {
        return null;
      }
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seat = seatMap.get(i);
      if (seat && seat.namauser === "") {
        seat.namauser = "__LOCK__" + userId;
        seat.lockTime = Date.now();
        this.seatLocks.set(`${room}-${i}`, { owner: userId, ts: Date.now() });
        return i;
      }
    }
    
    return null;
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    const count = this.getJumlahRoom()[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);

    const kursiUpdates = [];
    const lastPointsData = [];
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;

      if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
        kursiUpdates.push([
          seat,
          {
            noimageUrl: info.noimageUrl,
            namauser: info.namauser,
            color: info.color,
            itembawah: info.itembawah,
            itematas: info.itematas,
            vip: info.vip,
            viptanda: info.viptanda
          }
        ]);

        if (info.lastPoint) {
          lastPointsData.push({
            seat: seat,
            x: info.lastPoint.x,
            y: info.lastPoint.y,
            fast: info.lastPoint.fast
          });
        }
      }
    }

    if (kursiUpdates.length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, kursiUpdates]);
    }

    if (lastPointsData.length > 0) {
      this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    }

    if (this.roomChatHistory.has(room)) {
      const history = this.roomChatHistory.get(room);
      const recentChats = history.slice(-10);
      for (const chat of recentChats) {
        this.safeSend(ws, [
          "chat",
          room,
          chat.noImageURL,
          chat.username,
          chat.message,
          chat.usernameColor,
          chat.chatTextColor
        ]);
      }
    }
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id) return;

    const lockAcquired = await this.acquireUserJoinLock(id);
    if (!lockAcquired) {
      this.safeSend(ws, ["error", "Another operation in progress"]);
      return;
    }

    try {
      this.pendingCleanups.delete(id);
      ws.idtarget = id;

      if (baru === true) {
        ws.roomname = undefined;
        ws.numkursi = new Set();
        this.safeSend(ws, ["needJoinRoom"]);
      } else if (baru === false) {
        const seatInfo = this.userToSeat.get(id);

        if (seatInfo) {
          const { room, seat } = seatInfo;
          const seatMap = this.roomSeats.get(room);

          if (seatMap?.has(seat)) {
            const seatData = seatMap.get(seat);

            if (seatData.namauser === id) {
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              this.sendAllStateTo(ws, room);
              this.broadcastRoomUserCount(room);
            } else {
              this.userToSeat.delete(id);
              this.safeSend(ws, ["needJoinRoom"]);
            }
          } else {
            this.userToSeat.delete(id);
            this.safeSend(ws, ["needJoinRoom"]);
          }
        } else {
          this.safeSend(ws, ["needJoinRoom"]);
        }
      }
    } finally {
      this.releaseUserJoinLock(id);
    }
  }

  async handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) return false;
    if (!ws.idtarget) {
      this.safeSend(ws, ["error", "No user ID set"]);
      return false;
    }

    const lockAcquired = await this.acquireUserJoinLock(ws.idtarget);
    if (!lockAcquired) {
      this.safeSend(ws, ["error", "Another join operation in progress"]);
      return false;
    }

    try {
      if (ws.roomname && ws.roomname !== newRoom) {
        this.removeUserData(ws.idtarget);
      }

      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      if (existingSeatInfo && existingSeatInfo.room === newRoom) {
        this.safeSend(ws, ["error", "Already in this room"]);
        return false;
      }

      ws.roomname = newRoom;
      const foundSeat = this.lockSeat(newRoom, ws);

      if (foundSeat === null) {
        this.safeSend(ws, ["roomFull", newRoom]);
        return false;
      }

      ws.numkursi = new Set([foundSeat]);
      this.safeSend(ws, ["numberKursiSaya", foundSeat]);
      this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);

      this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
      this.sendAllStateTo(ws, newRoom);
      this.vipManager.getAllVipBadges(ws, newRoom);
      this.broadcastRoomUserCount(newRoom);

      return true;
    } finally {
      this.releaseUserJoinLock(ws.idtarget);
    }
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;

    let data;
    try {
      data = JSON.parse(raw);
    } catch (e) {
      return;
    }

    if (!Array.isArray(data) || data.length === 0) return;

    const evt = data[0];
    if (!this.checkRateLimit(ws, evt)) return;

    try {
      switch (evt) {
        case "onDestroy": {
          const idtarget = ws.idtarget;
          ws._manualDisconnect = true;
          this.pendingCleanups.delete(idtarget);
          this.removeUserData(idtarget);
          this.clients.delete(ws);
          break;
        }

        case "setIdTarget2": {
          const id = data[1];
          const baru = data[2];
          this.handleSetIdTarget2(ws, id, baru);
          break;
        }

        case "setIdTarget": {
          const newId = data[1];
          if (ws.idtarget && ws.idtarget !== newId) {
            this.removeUserData(ws.idtarget);
          }

          ws.idtarget = newId;
          this.pendingCleanups.delete(newId);

          const prevSeat = this.userToSeat.get(newId);
          if (prevSeat) {
            ws.roomname = prevSeat.room;
            ws.numkursi = new Set([prevSeat.seat]);
            this.sendAllStateTo(ws, prevSeat.room);
          } else if (this.hasEverSetId) {
            this.safeSend(ws, ["needJoinRoom"]);
          }

          this.hasEverSetId = true;
          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
          break;
        }

        // ❌ DIHAPUS: case "isInRoom" - tidak ada di kode awal
        // ❌ DIHAPUS: case "leaveRoom" - tidak ada di kode awal

        case "joinRoom": {
          const newRoom = data[1];
          this.handleJoinRoom(ws, newRoom);
          this.processPendingCleanupsIfNeeded();
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return;

          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);

          this.flushBuffers();
          
          if (!this.roomChatHistory.has(roomname)) {
            this.roomChatHistory.set(roomname, []);
          }
          const history = this.roomChatHistory.get(roomname);
          history.push({
            timestamp: Date.now(),
            noImageURL,
            username,
            message,
            usernameColor,
            chatTextColor
          });
          if (history.length > 10) {
            this.roomChatHistory.set(roomname, history.slice(-10));
          }
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;

          si.lastPoint = { x, y, fast, timestamp: Date.now() };
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) return;

          const seatMap = this.roomSeats.get(room);
          const currentInfo = seatMap.get(seat) || createEmptySeat();

          Object.assign(currentInfo, {
            noimageUrl, namauser, color, itembawah, itematas,
            vip: vip || 0,
            viptanda: viptanda || 0
          });

          seatMap.set(seat, currentInfo);
          this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
          this.broadcastRoomUserCount(room);
          this.flushBuffers();
          break;
        }

        case "vipbadge":
        case "removeVipBadge":
        case "getAllVipBadges":
          this.vipManager.handleEvent(ws, data);
          break;

        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          const room = ws.roomname;
          if (room !== "LowCard") return;
          this.lowcard.handleEvent(ws, data);
          break;
        }

        // ❌ DIHAPUS: case tambahan LowCard game events

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          for (const c of this.clients) {
            if (c.idtarget === idtarget && c.readyState === 1) {
              this.safeSend(c, notif);
              break;
            }
          }
          break;
        }

        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const out = ["private", idt, url, msg, ts, sender];
          this.safeSend(ws, out);
          for (const c of this.clients) {
            if (c.idtarget === idt && c.readyState === 1) {
              this.safeSend(c, out);
              break;
            }
          }
          break;
        }

        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";

          let online = false;
          for (const c of this.clients) {
            if (c.idtarget === username && c.readyState === 1) {
              online = true;
              break;
            }
          }

          this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);
          break;
        }

        // ❌ DIHAPUS: case "userTyping" - tidak ada di kode awal

        case "getAllRoomsUserCount":
          const allCounts = this.getJumlahRoom();
          const result = roomList.map(room => [room, allCounts[room]]);
          this.safeSend(ws, ["allRoomsUserCount", result]);
          this.processPendingCleanupsIfNeeded();
          break;

        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          this.processPendingCleanupsIfNeeded();
          break;

        case "getOnlineUsers":
          const users = [];
          for (const client of this.clients) {
            if (client.idtarget && client.readyState === 1) {
              users.push(client.idtarget);
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;

        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) return;
          const roomUsers = [];
          for (const client of this.clients) {
            if (client.roomname === roomName && client.idtarget && client.readyState === 1) {
              roomUsers.push(client.idtarget);
            }
          }
          this.safeSend(ws, ["roomOnlineUsers", roomName, roomUsers]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);

          Object.assign(seatMap.get(seat), createEmptySeat());
          this.seatLocks.delete(`${room}-${seat}`);

          this.clearSeatBuffer(room, seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          if (!this.chatMessageBuffer.has(roomname))
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
          this.flushBuffers();
          break;
        }
      }
    } catch (error) {
      console.error(`[HANDLE MESSAGE] Error in ${evt}:`, error);
      this.safeSend(ws, ["error", `Failed to process ${evt}`]);
    }
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      await server.accept();

      const ws = server;
      ws._connId = `conn#${this._nextConnId++}`;
      ws._manualDisconnect = false;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          console.error('[WS MESSAGE] Error:', error);
        }
      });

      ws.addEventListener("error", (event) => {
        const id = ws.idtarget;
        if (id && !ws._manualDisconnect) {
          this.pendingCleanups.set(id, Date.now());
        }
      });

      ws.addEventListener("close", (event) => {
        const id = ws.idtarget;
        if (ws._manualDisconnect) {
          this.removeUserData(id);
          this.clients.delete(ws);
        } else if (id) {
          this.pendingCleanups.set(id, Date.now());
        } else {
          this.clients.delete(ws);
        }
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      if (new URL(req.url).pathname === "/health") {
        return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
      }
      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
