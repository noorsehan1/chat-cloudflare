// ChatServer Durable Object (Bahasa Indonesia)
// Versi tanpa buffer sama sekali

import { LowCardGameManager } from "./lowcard.js";

const ROOM_LIST = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

const MAX_SEATS = 35;
const NUMBER_INTERVAL = 15 * 60 * 1000; // 15 menit
const GRACE_PERIOD  =3000; // 5 detik
const LOCK_TIMEOUT = 3000; // 10 detik

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

    // Inisialisasi kursi untuk semua room
    this.roomSeats = new Map();
    for (const room of ROOM_LIST) {
      const seats = new Map();
      for (let i = 1; i <= MAX_SEATS; i++) seats.set(i, createEmptySeat());
      this.roomSeats.set(room, seats);
    }

    // Sistem angka berganti
    this.currentNumber = 1;
    this.maxNumber = 6;
    
    // Timer untuk ganti angka
    try {
      this._tickTimer = setInterval(() => this.tick(), NUMBER_INTERVAL);
    } catch (e) {
      console.error("Timer error:", e);
    }

    this.lowcard = new LowCardGameManager(this);
  }

  // ✅ PERBAIKAN: Function khusus kirim currentNumber
  sendCurrentNumber(ws) {
    if (!this.isWebSocketReady(ws)) return false;
    
    try {
      ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
      return true;
    } catch (e) {
      return false;
    }
  }

  isWebSocketReady(ws) {
    return ws && ws.readyState === 1;
  }

  safeSend(ws, data) {
    if (!this.isWebSocketReady(ws)) return false;
    
    try {
      ws.send(JSON.stringify(data));
      return true;
    } catch (e) {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    let sentCount = 0;
    for (const client of this.clients) {
      if (client.roomname === room && this.isWebSocketReady(client)) {
        if (this.safeSend(client, msg)) sentCount++;
      }
    }
    return sentCount;
  }

  broadcastToAll(msg) {
    let sentCount = 0;
    for (const client of this.clients) {
      if (this.isWebSocketReady(client)) {
        if (this.safeSend(client, msg)) sentCount++;
      }
    }
    return sentCount;
  }

  getRoomUserCounts() {
    const counts = {};
    for (const room of ROOM_LIST) {
      const seatMap = this.roomSeats.get(room);
      let count = 0;
      
      for (const info of seatMap.values()) {
        if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
          count++;
        }
      }
      
      counts[room] = count;
    }
    return counts;
  }

  broadcastRoomUserCount(room) {
    const counts = this.getRoomUserCounts();
    this.broadcastToRoom(room, ["roomUserCount", room, counts[room]]);
  }

  // ✅ PERBAIKAN: Tick dengan error handling
  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      // Kirim ke semua client yang ready
      for (const client of this.clients) {
        if (this.isWebSocketReady(client)) {
          this.sendCurrentNumber(client);
        }
      }
    } catch (e) {
      console.error("Tick error:", e);
    }
  }

  cleanExpiredLocks() {
    const now = Date.now();
    
    for (const room of ROOM_LIST) {
      const seatMap = this.roomSeats.get(room);
      
      for (const [seat, info] of seatMap) {
        const isExpiredLock = info.namauser.startsWith("__LOCK__") && 
                             info.lockTime && 
                             (now - info.lockTime) > LOCK_TIMEOUT;
        
        if (isExpiredLock) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  cleanupClosedWebSockets() {
    for (const client of this.clients) {
      if (client.readyState === 2 || client.readyState === 3) {
        this.cleanupClient(client);
      }
    }
  }

  periodicMaintenance() {
    this.cleanExpiredLocks();
    this.cleanupClosedWebSockets();
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;
    
    const seatMap = this.roomSeats.get(room);
    const now = Date.now();

    // Bersihkan lock yang expired
    for (const [seat, info] of seatMap) {
      const isExpiredLock = info.namauser.startsWith("__LOCK__") && 
                           info.lockTime && 
                           (now - info.lockTime) > 5000;
      
      if (isExpiredLock) {
        Object.assign(info, createEmptySeat());
      }
    }

    // Cari kursi kosong
    for (let seat = 1; seat <= MAX_SEATS; seat++) {
      const seatInfo = seatMap.get(seat);
      
      if (seatInfo && seatInfo.namauser === "") {
        seatInfo.namauser = `__LOCK__${ws.idtarget}`;
        seatInfo.lockTime = now;
        this.userToSeat.set(ws.idtarget, { room, seat });
        return seat;
      }
    }
    
    return null;
  }

  sendRoomState(ws, room) {
    if (!this.isWebSocketReady(ws)) return;
    
    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const seatMetadata = {};

    // Kumpulkan data points dan metadata
    for (let seat = 1; seat <= MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;

      // Points
      for (const point of info.points) {
        allPoints.push({ seat, ...point });
      }

      // Metadata kursi
      if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
        seatMetadata[seat] = {
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

    // Kirim data
    this.safeSend(ws, ["allPointsList", room, allPoints]);
    this.safeSend(ws, ["allUpdateKursiList", room, seatMetadata]);
    this.sendCurrentNumber(ws);
  }

  removeUserSeats(userId) {
    let removedCount = 0;
    
    for (const room of ROOM_LIST) {
      const seatMap = this.roomSeats.get(room);
      
      for (const [seat, info] of seatMap) {
        const isUserSeat = info.namauser === userId || 
                          info.namauser.startsWith(`__LOCK__${userId}`);
        
        if (isUserSeat) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          removedCount++;
        }
      }
    }

    this.userToSeat.delete(userId);
    return removedCount;
  }

  batalkanPendingRemoval(userId) {
    if (userId && this.pendingRemove.has(userId)) {
      clearTimeout(this.pendingRemove.get(userId));
      this.pendingRemove.delete(userId);
    }
  }

  getAllOnlineUsers() {
    const users = [];
    for (const client of this.clients) {
      if (client.idtarget && this.isWebSocketReady(client)) {
        users.push(client.idtarget);
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    for (const client of this.clients) {
      if (client.roomname === roomName && client.idtarget && this.isWebSocketReady(client)) {
        users.push(client.idtarget);
      }
    }
    return users;
  }

  cleanupClient(ws) {
    const userId = ws.idtarget;
    
    this.clients.delete(ws);
    
    if (userId) {
      // Cek apakah ada koneksi aktif lain untuk user ini
      const hasActiveConnection = Array.from(this.clients).some(
        client => client.idtarget === userId && this.isWebSocketReady(client)
      );
      
      if (!hasActiveConnection) {
        // Hapus timeout yang ada
        if (this.pendingRemove.has(userId)) {
          clearTimeout(this.pendingRemove.get(userId));
        }

        // Jadwalkan penghapusan kursi
        const timeout = setTimeout(() => {
          this.removeUserSeats(userId);
          this.pendingRemove.delete(userId);
        }, GRACE_PERIOD);

        this.pendingRemove.set(userId, timeout);
      } else {
        // Batalkan penghapusan jika ada koneksi aktif
        this.batalkanPendingRemoval(userId);
      }
    }

    // Reset WebSocket properties
    ws.numkursi?.clear();
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  isInLowcardRoom(ws) {
    return ws.roomname === "LowCard";
  }

  handleMessage(ws, rawMessage) {
    let data;
    try { 
      data = JSON.parse(rawMessage); 
    } catch (e) { 
      return this.safeSend(ws, ["error", "Invalid JSON"]); 
    }
    
    if (!Array.isArray(data) || data.length === 0) {
      return this.safeSend(ws, ["error", "Invalid message format"]);
    }
    
    const [eventType, ...args] = data;
    this.periodicMaintenance();

    switch (eventType) {
      case "setIdTarget":
        this.handleSetIdTarget(ws, args[0]);
        break;

      case "sendnotif":
        this.handleSendNotification(ws, args);
        break;

      case "private":
        this.handlePrivateMessage(ws, args);
        break;

      case "isUserOnline":
        this.handleUserOnlineCheck(ws, args[0], args[1]);
        break;

      case "getAllRoomsUserCount":
        this.handleGetAllRoomsUserCount(ws);
        break;
        
      case "getCurrentNumber":
        this.sendCurrentNumber(ws);
        break;
        
      case "getAllOnlineUsers":
        this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
        break;
        
      case "getRoomOnlineUsers":
        this.handleGetRoomOnlineUsers(ws, args[0]);
        break;

      case "joinRoom":
        this.handleJoinRoom(ws, args[0]);
        break;

      case "chat":
        this.handleChatMessage(ws, args);
        break;

      case "updatePoint":
        this.handleUpdatePoint(ws, args);
        break;

      case "removeKursiAndPoint":
        this.handleRemoveSeat(ws, args[0], args[1]);
        break;

      case "updateKursi":
        this.handleUpdateSeat(ws, args);
        break;

      case "gift":
        this.handleGift(ws, args);
        break;

      case "onDestroy":
        this.handleDestroy(ws);
        break;

      // Game LowCard events
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

  // Handler methods untuk memecah logika
  handleSetIdTarget(ws, newId) {
    this.batalkanPendingRemoval(newId);
    ws.idtarget = newId;

    // Tutup koneksi duplikat
    for (const client of this.clients) {
      if (client.idtarget === newId && client !== ws && this.isWebSocketReady(client)) {
        client.close(4000, "Duplicate connection");
        this.clients.delete(client);
      }
    }

    // Pulihkan state sebelumnya jika ada
    const seatInfo = this.userToSeat.get(newId);
    if (seatInfo) {
      ws.roomname = seatInfo.room;
      this.sendRoomState(ws, seatInfo.room);
      this.broadcastRoomUserCount(seatInfo.room);
    }
  }

  handleSendNotification(ws, [targetId, imageUrl, username, description]) {
    const notification = ["notif", imageUrl, username, description, Date.now()];
    let delivered = false;
    
    for (const client of this.clients) {
      if (client.idtarget === targetId && this.isWebSocketReady(client)) {
        this.safeSend(client, notification);
        delivered = true;
      }
    }
    
    if (!delivered) {
      this.safeSend(ws, ["notifFailed", targetId, "User offline"]);
    }
  }

  handlePrivateMessage(ws, [targetId, imageUrl, message, sender]) {
    const privateMsg = ["private", targetId, imageUrl, message, Date.now(), sender];
    this.safeSend(ws, privateMsg);
    
    let delivered = false;
    for (const client of this.clients) {
      if (client.idtarget === targetId && this.isWebSocketReady(client)) {
        this.safeSend(client, privateMsg);
        delivered = true;
      }
    }
    
    if (!delivered) {
      this.safeSend(ws, ["privateFailed", targetId, "User offline"]);
    }
  }

  handleUserOnlineCheck(ws, username, marker = "") {
    const activeConnections = Array.from(this.clients)
      .filter(client => client.idtarget === username && this.isWebSocketReady(client));
    
    const isOnline = activeConnections.length > 0;
    this.safeSend(ws, ["userOnlineStatus", username, isOnline, marker]);

    // Handle duplicate connections
    if (activeConnections.length > 1) {
      this.handleDuplicateConnections(username, activeConnections);
    }
  }

  handleDuplicateConnections(username, connections) {
    const newest = connections[connections.length - 1];
    const oldConnections = connections.slice(0, -1);

    // Hapus state kursi user
    const seatInfo = this.userToSeat.get(username);
    if (seatInfo) {
      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);
      
      if (seatMap && seatMap.has(seat)) {
        Object.assign(seatMap.get(seat), createEmptySeat());
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }
      
      this.userToSeat.delete(username);
    }

    // Tutup koneksi lama
    for (const oldConnection of oldConnections) {
      if (this.isWebSocketReady(oldConnection)) {
        oldConnection.close(4000, "Duplicate login");
      }
      this.clients.delete(oldConnection);
    }
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getRoomUserCounts();
    const result = ROOM_LIST.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  handleGetRoomOnlineUsers(ws, roomName) {
    if (!ROOM_LIST.includes(roomName)) {
      return this.safeSend(ws, ["error", "Unknown room"]);
    }
    
    this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
  }

  handleJoinRoom(ws, newRoom) {
    if (!ROOM_LIST.includes(newRoom)) {
      return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
    }
    
    // Bersihkan state sebelumnya
    if (ws.idtarget) {
      this.batalkanPendingRemoval(ws.idtarget);
      this.removeUserSeats(ws.idtarget);
    }
    
    ws.roomname = newRoom;
    const assignedSeat = this.lockSeat(newRoom, ws);
    
    if (assignedSeat === null) {
      return this.safeSend(ws, ["roomFull", newRoom]);
    }
    
    ws.numkursi = new Set([assignedSeat]);
    this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
    
    if (ws.idtarget) {
      this.userToSeat.set(ws.idtarget, { room: newRoom, seat: assignedSeat });
    }
    
    // Kirim state room
    if (this.isWebSocketReady(ws)) {
      this.sendRoomState(ws, newRoom);
      this.broadcastRoomUserCount(newRoom);
      this.sendCurrentNumber(ws);
    }
  }

  handleChatMessage(ws, [room, imageUrl, username, message, usernameColor, textColor]) {
    if (!ROOM_LIST.includes(room)) {
      return this.safeSend(ws, ["error", "Invalid room for chat"]);
    }
    
    this.broadcastToRoom(room, ["chat", room, imageUrl, username, message, usernameColor, textColor]);
  }

  handleUpdatePoint(ws, [room, seat, x, y, fast]) {
    if (!ROOM_LIST.includes(room)) return;
    
    const seatMap = this.roomSeats.get(room);
    const seatInfo = seatMap.get(seat);
    
    if (seatInfo) {
      seatInfo.points = [{ x, y, fast }];
      this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
    }
  }

  handleRemoveSeat(ws, room, seat) {
    if (!ROOM_LIST.includes(room)) return;
    
    const seatMap = this.roomSeats.get(room);
    Object.assign(seatMap.get(seat), createEmptySeat());
    
    // Hapus dari semua client
    for (const client of this.clients) {
      client.numkursi?.delete(seat);
    }
    
    this.broadcastToRoom(room, ["removeKursi", room, seat]);
    this.broadcastRoomUserCount(room);
  }

  handleUpdateSeat(ws, [room, seat, imageUrl, username, color, bottomItem, topItem, vip, vipMark]) {
    if (!ROOM_LIST.includes(room)) return;
    
    const seatMap = this.roomSeats.get(room);
    const currentInfo = seatMap.get(seat) || createEmptySeat();
    
    Object.assign(currentInfo, { 
      noimageUrl: imageUrl, 
      namauser: username, 
      color, 
      itembawah: bottomItem, 
      itematas: topItem, 
      vip, 
      viptanda: vipMark,
      points: currentInfo.points
    });
    
    this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
      noimageUrl: imageUrl, 
      namauser: username, 
      color, 
      itembawah: bottomItem, 
      itematas: topItem, 
      vip, 
      viptanda: vipMark
    }]]]);
    
    this.broadcastRoomUserCount(room);
  }

  handleGift(ws, [room, sender, receiver, giftName]) {
    if (!ROOM_LIST.includes(room)) return;
    
    this.broadcastToRoom(room, ["gift", room, sender, receiver, giftName, Date.now()]);
  }

  handleDestroy(ws) {
    if (ws.idtarget) {
      this.batalkanPendingRemoval(ws.idtarget);
      this.cleanupClient(ws);
    }
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade") || "";
    
    if (upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    // Setup WebSocket
    const ws = server;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    
    this.clients.add(ws);

    // Event listeners
    ws.addEventListener("message", (event) => {
      this.handleMessage(ws, event.data);
    });

    ws.addEventListener("close", () => {
      this.cleanupClient(ws);
    });

    ws.addEventListener("error", () => {
      this.cleanupClient(ws);
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(req, env) {
    const upgradeHeader = req.headers.get("Upgrade") || "";
    
    if (upgradeHeader.toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { 
        status: 200, 
        headers: { "content-type": "text/plain" } 
      });
    }
    
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
