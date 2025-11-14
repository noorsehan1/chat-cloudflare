// ChatServer Durable Object (Bahasa Indonesia)
// Versi lengkap semua case

import { LowCardGameManager } from "./lowcard.js";

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
    this.pendingRemove = new Map();
    this.clientLastPong = new Map();
    this.roomSeats = new Map();

    this.MAX_SEATS = 35;
    this.initializeRoomSeats();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;
    
    this._tickTimer = null;
    this._pingTimer = null;
    this.startTickTimer();

    this.lowcard = new LowCardGameManager(this);

    this.gracePeriod = 3000;
    
    this.pingInterval = 10000;
    this.pingTimeout = 20000;
    this.startPingTimer();
  }

  initializeRoomSeats() {
    for (const room of roomList) {
      const seats = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seats.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seats);
    }
  }

  startPingTimer() {
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
    }
    this._pingTimer = setInterval(() => this.pingAllClients(), this.pingInterval);
  }

  startTickTimer() {
    if (this._tickTimer) {
      clearInterval(this._tickTimer);
    }
    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
  }

  pingAllClients() {
    const now = Date.now();
    
    for (const client of this.clients) {
      if (client.readyState === 1) {
        try {
          const pingTimestamp = Date.now();
          client.send(JSON.stringify(["ping", pingTimestamp]));
          
          const lastPong = this.clientLastPong.get(client);
          if (lastPong && (now - lastPong > this.pingTimeout)) {
            this.cleanupClientDestroy(client);
            if (client.readyState === 1) {
              client.close(4001, "Ping timeout");
            }
          }
        } catch (e) {
          this.cleanupClientDestroy(client);
        }
      }
    }
  }

  handlePong(ws, timestamp) {
    if (!ws || !ws.idtarget) return;
    this.clientLastPong.set(ws, Date.now());
  }

  sendCurrentNumber(ws) {
    if (!ws || ws.readyState !== 1) return false;
    try {
      ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
      return true;
    } catch (e) {
      this.cleanupClient(ws);
      return false;
    }
  }

  safeSend(ws, arr) {
    if (!ws || ws.readyState !== 1) return false;
    try {
      ws.send(JSON.stringify(arr));
      return true;
    } catch (e) {
      this.cleanupClient(ws);
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    let sentCount = 0;
    for (const client of this.clients) {
      if (client.roomname === room && client.readyState === 1) {
        if (this.safeSend(client, msg)) sentCount++;
      }
    }
    return sentCount;
  }

  broadcastToAll(msg) {
    let sentCount = 0;
    for (const client of this.clients) {
      if (client.readyState === 1) {
        if (this.safeSend(client, msg)) sentCount++;
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const info of seatMap.values()) {
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
          cnt[room]++;
        }
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      const readyClients = Array.from(this.clients).filter(c => c.readyState === 1);
      for (const c of readyClients) {
        this.sendCurrentNumber(c);
      }
    } catch (e) {}
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  cleanupClosedWebSockets() {
    const closedClients = Array.from(this.clients).filter(client => 
      client.readyState === 2 || client.readyState === 3
    );
    for (const client of closedClients) {
      this.cleanupClientDestroy(client);
    }
  }

  manualPeriodicFlush() {
    this.cleanExpiredLocks();
    this.cleanupClosedWebSockets();
  }

  handleGetAllRoomsUserCount(ws) {
    console.log('🟢 handleGetAllRoomsUserCount DIPANGGIL'); // ✅ DEBUG
    
    const allCounts = this.getJumlahRoom();
    const result = Object.entries(allCounts).map(([room, count]) => ({ 
        roomName: room, 
        userCount: count 
    }));
    
    console.log('📊 Data yang dikirim:', result); // ✅ DEBUG
    this.safeSend(ws, ["allRoomsUserCount", result]);
}

  handleGetAllOnlineUsers(ws) {
    const users = [];
    for (const client of this.clients) {
      if (client.idtarget && client.readyState === 1) {
        users.push(client.idtarget);
      }
    }
    this.safeSend(ws, ["allOnlineUsers", [...new Set(users)]]);
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    console.log('All counts:', allCounts); // ✅ DEBUG LOG
    
    const result = Object.entries(allCounts).map(([room, count]) => ({
        roomName: room, 
        userCount: count 
    }));
    
    console.log('Sending to client:', result); // ✅ DEBUG LOG
    this.safeSend(ws, ["allRoomsUserCount", result]);
}

  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;
    const now = Date.now();

    for (const [seat, info] of seatMap) {
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000) {
        Object.assign(info, createEmptySeat());
      }
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k && k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = now;
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    if (!this.isWebSocketReady(ws)) return;

    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const meta = {};
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      
      for (const p of info.points) {
        allPoints.push({ seat, ...p });
      }
      
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

  isWebSocketReady(ws) {
    return ws && ws.readyState === 1;
  }

  sendPointKursi(ws, room) {
    if (!this.isWebSocketReady(ws)) return;
    this.sendCurrentNumber(ws);
    this.sendAllStateTo(ws, room);
  }

  removeAllSeatsById(idtarget) {
    if (!idtarget) return 0;
    
    let removedCount = 0;
    
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      
      for (const [seat, info] of seatMap) {
        if (info.namauser === idtarget || String(info.namauser).startsWith("__LOCK__" + idtarget)) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          removedCount++;
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);
    return removedCount;
  }

  cleanupClientDestroy(ws) {
    if (!ws) return;
    
    const id = ws.idtarget;
    
    this.clients.delete(ws);
    this.clientLastPong.delete(ws);
    
    if (id) {
      this.batalkanPendingRemoval(id);
      this.removeAllSeatsById(id);
    }

    if (ws.numkursi) ws.numkursi.clear();
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  batalkanPendingRemoval(userId) {
    if (userId && this.pendingRemove.has(userId)) {
      clearTimeout(this.pendingRemove.get(userId));
      this.pendingRemove.delete(userId);
    }
  }

  cleanupClient(ws) {
    if (!ws) return;
    
    const id = ws.idtarget;
    
    this.clients.delete(ws);
    this.clientLastPong.delete(ws);
    
    if (id) {
      const hasActiveConnection = Array.from(this.clients).some(
        client => client.idtarget === id && this.isWebSocketReady(client)
      );
      
      if (!hasActiveConnection) {
        this.batalkanPendingRemoval(id);
        
        const timeout = setTimeout(() => {
          this.removeAllSeatsById(id);
          this.pendingRemove.delete(id);
        }, this.gracePeriod);

        this.pendingRemove.set(id, timeout);
      } else {
        this.batalkanPendingRemoval(id);
      }
    }

    if (ws.numkursi) ws.numkursi.clear();
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  isInLowcardRoom(ws) {
    return ws.roomname === "LowCard";
  }

  handleDuplicateConnections(userId, currentWs) {
    const duplicates = Array.from(this.clients).filter(
      client => client.idtarget === userId && client !== currentWs && this.isWebSocketReady(client)
    );

    for (const dup of duplicates) {
      this.cleanupClientDestroy(dup);
      if (dup.readyState === 1) {
        dup.close(4000, "Duplicate connection closed");
      }
    }
  }

  handleMessage(ws, raw) {
    if (!this.isWebSocketReady(ws)) return;
    
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      return;
    }
    
    if (!Array.isArray(data) || data.length === 0) return;

    const evt = data[0];
    this.manualPeriodicFlush();

    switch (evt) {
      case "setIdTarget": {
        const newId = data[1];
        if (!newId) return;

        this.batalkanPendingRemoval(newId);
        ws.idtarget = newId;
        this.clientLastPong.set(ws, Date.now());
        this.handleDuplicateConnections(newId, ws);

        const seatInfo = this.userToSeat.get(newId);
        if (seatInfo) {
          ws.roomname = seatInfo.room;
          this.sendPointKursi(ws, seatInfo.room);
          this.broadcastRoomUserCount(seatInfo.room);
        }
        break;
      }

      case "pong": {
        const timestamp = data[1];
        this.handlePong(ws, timestamp);
        break;
      }

      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        
        for (const c of this.clients) {
          if (c.idtarget === idtarget && c.readyState === 1) { 
            this.safeSend(c, notif);
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
          }
        }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";

        const activeSockets = Array.from(this.clients)
          .filter(c => c.idtarget === username && c.readyState === 1);
        const online = activeSockets.length > 0;

        this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);

        if (activeSockets.length > 1) {
          this.handleDuplicateConnections(username, ws);
        }
        break;
      }

      case "getAllRoomsUserCount": 
        this.handleGetAllRoomsUserCount(ws); 
        break;

      case "getAllOnlineUsers": 
      case "getOnlineUsers": 
        this.handleGetAllOnlineUsers(ws);
        break;
        
      case "getRoomOnlineUsers": {
        const roomName = data[1];
        this.handleGetRoomOnlineUsers(ws, roomName);
        break;
      }
        
      case "getCurrentNumber": 
        this.sendCurrentNumber(ws);
        break;

      case "joinRoom": {
        const newRoom = data[1];
        if (!roomList.includes(newRoom)) {
          this.safeSend(ws, ["roomFull", newRoom]);
          return;
        }
        
        if (ws.idtarget) {
          this.batalkanPendingRemoval(ws.idtarget);
          this.removeAllSeatsById(ws.idtarget);
        }
        
        ws.roomname = newRoom;
        const foundSeat = this.lockSeat(newRoom, ws);
        
        if (foundSeat === null) {
          this.safeSend(ws, ["roomFull", newRoom]);
          return;
        }
        
        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        
        if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
        
        this.sendAllStateTo(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);
        this.sendCurrentNumber(ws);
        break;
      }

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (!roomList.includes(roomname)) return;
        this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (!roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        
        si.points = [{ x, y, fast }];
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (!roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        Object.assign(seatMap.get(seat), createEmptySeat());
        for (const c of this.clients) c.numkursi?.delete(seat);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (!roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        
        Object.assign(currentInfo, { 
          noimageUrl, namauser, color, itembawah, itematas, vip, viptanda,
          points: currentInfo.points
        });
        
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
          noimageUrl, namauser, color, itembawah, itematas, vip, viptanda
        }]]]);
        
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (!roomList.includes(roomname)) return;
        this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, giftName, Date.now()]);
        break;
      }

      case "onDestroy": {
        if (ws.idtarget) {
          this.batalkanPendingRemoval(ws.idtarget);
          this.cleanupClientDestroy(ws);
        }
        break;
      }

      case "resetRoom": {
        for (const room of roomList) {
          const seatMap = this.roomSeats.get(room);
          for (let i = 1; i <= this.MAX_SEATS; i++) {
            seatMap.set(i, createEmptySeat());
          }
          this.broadcastToRoom(room, ["resetRoom", room]);
        }
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (!this.isInLowcardRoom(ws)) return;
        this.lowcard.handleEvent(ws, data);
        break;

      default:
        break;
    }
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const ws = server;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    this.clients.add(ws);
    this.clientLastPong.set(ws, Date.now());

    ws.addEventListener("message", (ev) => {
      this.handleMessage(ws, ev.data);
    });

    ws.addEventListener("close", (event) => {
      this.cleanupClient(ws);
    });

    ws.addEventListener("error", (error) => {
      this.cleanupClient(ws);
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(req, env) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    }
    return new Response("WebSocket endpoint", { status: 200 });
  }
};


