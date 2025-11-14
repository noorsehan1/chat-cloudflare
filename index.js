// ChatServer Durable Object (Bahasa Indonesia)
// Versi dengan fix reconnection issue

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

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;
    
    // ✅ PERBAIKAN: Timer dengan protection
    this._tickTimer = null;
    this.startTickTimer();

    this.lowcard = new LowCardGameManager(this);

    this.gracePeriod = 3000; // ✅ DIPERCEPAT: 5 detik -> 3 detik
    this.pendingRemove = new Map();
    
    // ✅ BARU: Tracking connection state
    this.connectionStats = {
      totalConnections: 0,
      activeConnections: 0,
      failedReconnections: 0
    };
  }

  // ✅ PERBAIKAN: Timer management yang lebih aman
  startTickTimer() {
    if (this._tickTimer) {
      clearInterval(this._tickTimer);
    }
    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
  }

  // ✅ PERBAIKAN: Function khusus kirim currentNumber
  sendCurrentNumber(ws) {
    if (!ws || ws.readyState !== 1) {
      return false;
    }
    
    try {
      ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
      return true;
    } catch (e) {
      console.log('Send currentNumber error:', e);
      return false;
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {
      console.log('SafeSend error:', e);
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    let sentCount = 0;
    for (const c of Array.from(this.clients)) {
      if (c.roomname === room && c.readyState === 1) {
        if (this.safeSend(c, msg)) sentCount++;
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

  // ✅ PERBAIKAN: Tick dengan error handling
  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const readyClients = Array.from(this.clients).filter(c => c.readyState === 1);
      
      for (const c of readyClients) {
        this.sendCurrentNumber(c);
      }
    } catch (e) {
      console.log('Tick error:', e);
    }
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
      this.cleanupClient(client);
    }
  }

  manualPeriodicFlush() {
    this.cleanExpiredLocks();
    this.cleanupClosedWebSockets();
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  // ✅ CASE TERTINGGAL: Handler untuk getAllOnlineUsers
  handleGetAllOnlineUsers(ws) {
    const users = [];
    for (const client of this.clients) {
      if (client.idtarget && client.readyState === 1) {
        users.push(client.idtarget);
      }
    }
    this.safeSend(ws, ["allOnlineUsers", [...new Set(users)]]); // Remove duplicates
  }

  // ✅ CASE TERTINGGAL: Handler untuk getRoomOnlineUsers
  handleGetRoomOnlineUsers(ws, roomName) {
    if (!roomList.includes(roomName)) {
      return this.safeSend(ws, ["error", "Unknown room"]);
    }
    
    const users = [];
    for (const client of this.clients) {
      if (client.roomname === roomName && client.idtarget && client.readyState === 1) {
        users.push(client.idtarget);
      }
    }
    this.safeSend(ws, ["roomOnlineUsers", roomName, [...new Set(users)]]);
  }

  // ✅ PERBAIKAN: Lock seat dengan timeout yang lebih pendek
  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;
    const now = Date.now();

    // Cleanup expired locks lebih agresif
    for (const [seat, info] of seatMap) {
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 3000) {
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

  // ✅ PERBAIKAN: Optimized room state sending
  sendPointKursi(ws, room) {
    if (!ws || ws.readyState !== 1) return;
    
    const seatMap = this.roomSeats.get(room);
    
    // Kirim currentNumber pertama
    this.sendCurrentNumber(ws);

    // Kirim data kursi dalam batch
    const batchUpdates = [];
    const pointUpdates = [];
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info || !info.namauser || String(info.namauser).startsWith("__LOCK__")) continue;

      batchUpdates.push([seat, {
        noimageUrl: info.noimageUrl,
        namauser: info.namauser,
        color: info.color,
        itembawah: info.itembawah,
        itematas: info.itematas,
        vip: info.vip,
        viptanda: info.viptanda
      }]);

      if (info.points.length > 0) {
        const point = info.points[0];
        pointUpdates.push({ seat, point });
      }
    }

    // Kirim batch updates
    if (batchUpdates.length > 0) {
      this.safeSend(ws, ["kursiBatchUpdate", room, batchUpdates]);
    }

    // Kirim point updates
    pointUpdates.forEach(({ seat, point }) => {
      this.safeSend(ws, ["pointUpdated", room, seat, point.x, point.y, point.fast]);
    });
  }

  // ✅ PERBAIKAN: Cleanup yang lebih aggressive
  removeAllSeatsById(idtarget) {
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

  // ✅ PERBAIKAN: Cleanup client yang lebih thorough
  cleanupClientDestroy(ws) {
    const id = ws.idtarget;
    
    // Hapus dari clients set pertama
    this.clients.delete(ws);
    
    if (id) {
      // Cancel pending removal
      if (this.pendingRemove.has(id)) {
        clearTimeout(this.pendingRemove.get(id));
        this.pendingRemove.delete(id);
      }

      // Langsung hapus seats tanpa grace period untuk destroy
      this.removeAllSeatsById(id);
    }

    // Bersihkan properties
    if (ws.numkursi) ws.numkursi.clear();
    ws.roomname = undefined;
    ws.idtarget = undefined;
    
    // Update stats
    this.connectionStats.activeConnections = Array.from(this.clients).filter(c => c.readyState === 1).length;
  }

  batalkanPendingRemoval(userId) {
    if (userId && this.pendingRemove.has(userId)) {
      clearTimeout(this.pendingRemove.get(userId));
      this.pendingRemove.delete(userId);
    }
  }

  // ✅ PERBAIKAN: Cleanup client dengan logic yang lebih baik
  cleanupClient(ws) {
    const id = ws.idtarget;
    
    this.clients.delete(ws);
    
    if (id) {
      // Cek apakah user ini masih punya koneksi aktif lain
      const hasActiveConnection = Array.from(this.clients).some(
        client => client.idtarget === id && client.readyState === 1
      );
      
      if (!hasActiveConnection) {
        // Cancel existing timeout
        this.batalkanPendingRemoval(id);
        
        // Set timeout yang lebih pendek
        const timeout = setTimeout(() => {
          console.log(`Removing seats for user: ${id}`);
          this.removeAllSeatsById(id);
          this.pendingRemove.delete(id);
        }, this.gracePeriod);

        this.pendingRemove.set(id, timeout);
      } else {
        // Masih ada koneksi aktif, cancel removal
        this.batalkanPendingRemoval(id);
      }
    }

    // Cleanup properties
    if (ws.numkursi) ws.numkursi.clear();
    ws.roomname = undefined;
    ws.idtarget = undefined;
    
    // Update stats
    this.connectionStats.activeConnections = Array.from(this.clients).filter(c => c.readyState === 1).length;
  }

  isInLowcardRoom(ws) {
    return ws.roomname === "LowCard";
  }

  // ✅ PERBAIKAN: Handle duplicate connections lebih aggressive
  handleDuplicateConnections(userId, currentWs) {
    const duplicates = Array.from(this.clients).filter(
      client => client.idtarget === userId && client !== currentWs && client.readyState === 1
    );

    for (const dup of duplicates) {
      try {
        dup.close(4000, "Duplicate connection closed");
        this.clients.delete(dup);
        console.log(`Closed duplicate connection for user: ${userId}`);
      } catch (e) {
        this.clients.delete(dup);
      }
    }
  }

  // ✅ PERBAIKAN: Main message handler dengan connection management yang better
  handleMessage(ws, raw) {
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      return this.safeSend(ws, ["error", "Invalid JSON"]); 
    }
    
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    // Periodic cleanup
    this.manualPeriodicFlush();

    switch (evt) {
      case "setIdTarget": {
        const newId = data[1];
        
        if (!newId) {
          return this.safeSend(ws, ["error", "User ID required"]);
        }

        this.batalkanPendingRemoval(newId);
        ws.idtarget = newId;

        // ✅ PERBAIKAN: Handle duplicate connections lebih aggressive
        this.handleDuplicateConnections(newId, ws);

        const seatInfo = this.userToSeat.get(newId);
        if (seatInfo) {
          ws.roomname = seatInfo.room;
          this.sendPointKursi(ws, seatInfo.room);
          this.broadcastRoomUserCount(seatInfo.room);
        }

        // Update connection stats
        this.connectionStats.totalConnections++;
        this.connectionStats.activeConnections = Array.from(this.clients).filter(c => c.readyState === 1).length;
        
        break;
      }

      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        
        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === idtarget && c.readyState === 1) { 
            this.safeSend(c, notif); 
            delivered = true; 
          }
        }
        
        if (!delivered) {
          this.safeSend(ws, ["notifFailed", idtarget, "User offline"]);
        }
        break;
      }

      case "private": {
        const [, idt, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", idt, url, msg, ts, sender];
        this.safeSend(ws, out);
        
        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === idt && c.readyState === 1) { 
            this.safeSend(c, out); 
            delivered = true; 
          }
        }
        
        if (!delivered) {
          this.safeSend(ws, ["privateFailed", idt, "User offline"]);
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

        // Handle multiple connections untuk user yang sama
        if (activeSockets.length > 1) {
          this.handleDuplicateConnections(username, ws);
        }
        break;
      }

      case "getAllRoomsUserCount": 
        this.handleGetAllRoomsUserCount(ws); 
        break;

      case "getAllOnlineUsers": 
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
        if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
        
        if (ws.idtarget) {
          this.batalkanPendingRemoval(ws.idtarget);
          this.removeAllSeatsById(ws.idtarget);
        }
        
        ws.roomname = newRoom;
        const foundSeat = this.lockSeat(newRoom, ws);
        
        if (foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);
        
        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        
        if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
        
        if (ws.readyState === 1) {
          this.sendAllStateTo(ws, newRoom);
          this.broadcastRoomUserCount(newRoom);
          this.sendCurrentNumber(ws);
        }

        break;
      }

      // ... (other cases remain the same as your original code)
      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
        
        this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        
        si.points = [{ x, y, fast }];
        
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        Object.assign(seatMap.get(seat), createEmptySeat());
        for (const c of this.clients) c.numkursi?.delete(seat);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        
        Object.assign(currentInfo, { 
          noimageUrl, 
          namauser, 
          color, 
          itembawah, 
          itematas, 
          vip, 
          viptanda,
          points: currentInfo.points
        });
        
        seatMap.set(seat, currentInfo);
        
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
          noimageUrl, 
          namauser, 
          color, 
          itembawah, 
          itematas, 
          vip, 
          viptanda
        }]]]);
        
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for gift"]);
        
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

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (!this.isInLowcardRoom(ws)) {
          return this.safeSend(ws, ["error", "Game Lowcard hanya tersedia di room LowCard"]);
        }
        this.lowcard.handleEvent(ws, data);
        break;

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
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

    // ✅ PERBAIKAN: Event handlers dengan better error handling
    ws.addEventListener("message", (ev) => {
      try {
        this.handleMessage(ws, ev.data);
      } catch (error) {
        console.log('Message handler error:', error);
        this.safeSend(ws, ["error", "Internal server error"]);
      }
    });

    ws.addEventListener("close", (event) => {
      console.log(`WebSocket closed: ${event.code} ${event.reason}`);
      this.cleanupClient(ws);
    });

    ws.addEventListener("error", (error) => {
      console.log('WebSocket error:', error);
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
