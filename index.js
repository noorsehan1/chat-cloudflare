// ChatServer Durable Object - Fully Optimized Version
import { LowCardGameManager } from "./lowcard.js";

const ROOM_LIST = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

const MAX_SEATS = 35;
const NUMBER_INTERVAL = 15 * 60 * 1000;
const GRACE_PERIOD = 3000;
const LOCK_TIMEOUT = 3000;
const MAX_ROOM_CACHE_AGE = 10 * 60 * 1000; // 10 menit
const BROADCAST_CHUNK_SIZE = 25;

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
    this.storage = state.storage;
    
    // ✅ OPTIMISASI MAXIMAL: Memory-efficient structures
    this.clients = new Map(); // userId -> { ws, room, lastActive }
    this.roomSubscriptions = new Map(); // room -> Set(userId)
    this.userToSeat = new Map();
    this.pendingRemove = new Map();
    this.rateLimits = new Map();

    // ✅ OPTIMISASI: Memory-managed room seats dengan cache expiry
    this.roomSeats = new Map();
    this.roomSeatsLastUsed = new Map();
    
    // Sistem angka berganti dengan persistence
    this.currentNumber = 1;
    this.maxNumber = 6;
    
    // ✅ OPTIMISASI: Robust timer management
    this._setupTimers();

    this.lowcard = new LowCardGameManager(this);
  }

  async _setupTimers() {
    // Load persistent state
    this.currentNumber = await this.storage.get("currentNumber") || 1;
    
    // Clear existing timers
    if (this._tickTimer) clearInterval(this._tickTimer);
    if (this._cleanupTimer) clearInterval(this._cleanupTimer);
    
    this._tickTimer = setInterval(() => this.tick(), NUMBER_INTERVAL);
    this._cleanupTimer = setInterval(() => this.periodicMaintenance(), 30000);
  }

  // ✅ OPTIMISASI: Ultra-fast WebSocket check
  isWebSocketReady(ws) {
    return ws?.readyState === 1;
  }

  safeSend(ws, data) {
    if (!this.isWebSocketReady(ws)) return false;
    
    try {
      // ✅ OPTIMISASI: Micro-batching untuk multiple rapid sends
      if (ws._pendingSend) {
        ws._pendingSend.push(data);
        return true;
      }
      
      ws._pendingSend = [data];
      setTimeout(() => {
        if (ws._pendingSend) {
          try {
            const toSend = ws._pendingSend.length === 1 ? 
              JSON.stringify(ws._pendingSend[0]) : 
              JSON.stringify(ws._pendingSend);
            ws.send(toSend);
          } catch (e) {
            // Will be cleaned up in next maintenance
          }
          ws._pendingSend = null;
        }
      }, 5); // 5ms batching window
      
      return true;
    } catch (e) {
      return false;
    }
  }

  // ✅ OPTIMISASI MAXIMAL: Chunked broadcast dengan async processing
  async broadcastToRoom(room, msg, excludeUserId = null) {
    if (!this.roomSubscriptions.has(room)) return 0;
    
    const subscribersSet = this.roomSubscriptions.get(room);
    if (subscribersSet.size === 0) return 0;
    
    const subscribers = Array.from(subscribersSet);
    const msgString = JSON.stringify(msg);
    let totalSent = 0;

    // ✅ OPTIMISASI: Filter exclude user sekali saja
    let targetSubscribers = subscribers;
    if (excludeUserId) {
      const excludeIndex = subscribers.indexOf(excludeUserId);
      if (excludeIndex > -1) {
        targetSubscribers = subscribers.filter((_, idx) => idx !== excludeIndex);
      }
    }

    // ✅ OPTIMISASI: Process dalam chunks untuk menghindari blocking
    for (let i = 0; i < targetSubscribers.length; i += BROADCAST_CHUNK_SIZE) {
      const chunk = targetSubscribers.slice(i, i + BROADCAST_CHUNK_SIZE);
      const chunkPromises = [];
      
      for (const userId of chunk) {
        const clientInfo = this.clients.get(userId);
        if (clientInfo && this.isWebSocketReady(clientInfo.ws)) {
          chunkPromises.push(
            new Promise(resolve => {
              try {
                clientInfo.ws.send(msgString);
                clientInfo.lastActive = Date.now();
                resolve(true);
              } catch (e) {
                // Auto-cleanup failed connections
                this.clients.delete(userId);
                subscribersSet.delete(userId);
                resolve(false);
              }
            })
          );
        }
      }
      
      const results = await Promise.allSettled(chunkPromises);
      totalSent += results.filter(result => result.status === 'fulfilled' && result.value).length;
      
      // ✅ OPTIMISASI: Yield event loop antara chunks untuk room besar
      if (chunkPromises.length > 0) {
        await new Promise(resolve => setTimeout(resolve, 0));
      }
    }
    
    return totalSent;
  }

  // ✅ OPTIMISASI: Memory-managed room seats dengan automatic cleanup
  getRoomSeats(room) {
    const now = Date.now();
    
    // Cleanup expired room cache
    for (const [cachedRoom, lastUsed] of this.roomSeatsLastUsed) {
      if (now - lastUsed > MAX_ROOM_CACHE_AGE && cachedRoom !== room) {
        this.roomSeats.delete(cachedRoom);
        this.roomSeatsLastUsed.delete(cachedRoom);
      }
    }
    
    if (!this.roomSeats.has(room)) {
      const seats = new Map();
      for (let i = 1; i <= MAX_SEATS; i++) {
        seats.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seats);
    }
    
    this.roomSeatsLastUsed.set(room, now);
    return this.roomSeats.get(room);
  }

  getRoomUserCounts() {
    const counts = {};
    const now = Date.now();
    
    for (const room of ROOM_LIST) {
      const seatMap = this.getRoomSeats(room);
      let count = 0;
      
      // ✅ OPTIMISASI: Fast iteration
      for (const info of seatMap.values()) {
        if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
          count++;
        }
      }
      
      counts[room] = count;
    }
    return counts;
  }

  async broadcastRoomUserCount(room) {
    const counts = this.getRoomUserCounts();
    await this.broadcastToRoom(room, ["roomUserCount", room, counts[room]]);
  }

  // ✅ OPTIMISASI: Persistent tick dengan batch processing
  async tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      // ✅ OPTIMISASI: Persist ke storage secara async
      this.storage.put("currentNumber", this.currentNumber).catch(console.error);
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const promises = [];
      
      // ✅ OPTIMISASI: Batch WebSocket sends
      for (const [userId, clientInfo] of this.clients) {
        if (this.isWebSocketReady(clientInfo.ws)) {
          promises.push(
            new Promise(resolve => {
              try {
                clientInfo.ws.send(message);
                clientInfo.lastActive = Date.now();
                resolve(true);
              } catch (e) {
                this.clients.delete(userId);
                resolve(false);
              }
            })
          );
        }
      }
      
      await Promise.allSettled(promises);
    } catch (e) {
      console.error("Tick error:", e);
    }
  }

  // ✅ OPTIMISASI: Efficient lock cleanup
  cleanExpiredLocks() {
    const now = Date.now();
    const cleanupPromises = [];
    
    for (const room of ROOM_LIST) {
      const seatMap = this.getRoomSeats(room);
      
      for (const [seat, info] of seatMap) {
        const isExpiredLock = info.namauser.startsWith("__LOCK__") && 
                             info.lockTime && 
                             (now - info.lockTime) > LOCK_TIMEOUT;
        
        if (isExpiredLock) {
          Object.assign(info, createEmptySeat());
          cleanupPromises.push(
            this.broadcastToRoom(room, ["removeKursi", room, seat])
          );
        }
      }
    }
    
    // Process cleanups concurrently
    return Promise.allSettled(cleanupPromises);
  }

  cleanupClosedWebSockets() {
    const now = Date.now();
    const inactiveThreshold = 60000; // 1 menit
    
    for (const [userId, clientInfo] of this.clients) {
      const isInactive = now - clientInfo.lastActive > inactiveThreshold;
      
      if (!this.isWebSocketReady(clientInfo.ws) || isInactive) {
        this.clients.delete(userId);
        if (clientInfo.room && this.roomSubscriptions.has(clientInfo.room)) {
          this.roomSubscriptions.get(clientInfo.room).delete(userId);
        }
      }
    }
  }

  async periodicMaintenance() {
    try {
      await this.cleanExpiredLocks();
      this.cleanupClosedWebSockets();
      
      // Cleanup expired rate limits
      const now = Date.now();
      for (const [userId, limit] of this.rateLimits) {
        if (now - limit.startTime > 60000) {
          this.rateLimits.delete(userId);
        }
      }
    } catch (e) {
      console.error("Maintenance error:", e);
    }
  }

  // ✅ OPTIMISASI: Efficient rate limiting
  checkRateLimit(userId) {
    const now = Date.now();
    const WINDOW_MS = 60000;
    const MAX_REQUESTS = 100;
    
    let userLimit = this.rateLimits.get(userId);
    if (!userLimit) {
      userLimit = { count: 1, startTime: now };
      this.rateLimits.set(userId, userLimit);
      return true;
    }
    
    if (now - userLimit.startTime > WINDOW_MS) {
      userLimit.count = 1;
      userLimit.startTime = now;
      return true;
    }
    
    userLimit.count++;
    return userLimit.count <= MAX_REQUESTS;
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;
    
    const seatMap = this.getRoomSeats(room);
    const now = Date.now();
    let availableSeat = null;

    // ✅ OPTIMISASI: Single pass untuk cleanup dan pencarian
    for (let seat = 1; seat <= MAX_SEATS; seat++) {
      const seatInfo = seatMap.get(seat);
      
      // Cleanup expired lock
      if (seatInfo.namauser.startsWith("__LOCK__") && 
          seatInfo.lockTime && 
          (now - seatInfo.lockTime) > LOCK_TIMEOUT) {
        Object.assign(seatInfo, createEmptySeat());
      }
      
      // Temukan kursi kosong pertama
      if (!availableSeat && (!seatInfo.namauser || seatInfo.namauser === "")) {
        availableSeat = seat;
      }
    }
    
    if (availableSeat) {
      const seatInfo = seatMap.get(availableSeat);
      seatInfo.namauser = `__LOCK__${ws.idtarget}`;
      seatInfo.lockTime = now;
      this.userToSeat.set(ws.idtarget, { room, seat: availableSeat });
      return availableSeat;
    }
    
    return null;
  }

  sendRoomState(ws, room) {
    if (!this.isWebSocketReady(ws)) return;
    
    const seatMap = this.getRoomSeats(room);
    const allPoints = [];
    const seatMetadata = {};

    // ✅ OPTIMISASI: Pre-allocate arrays untuk performance
    for (let seat = 1; seat <= MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;

      // Points
      if (info.points.length > 0) {
        allPoints.push({ seat, ...info.points[0] });
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

    // ✅ OPTIMISASI: Batch sends dengan micro-batching
    this.safeSend(ws, ["allPointsList", room, allPoints]);
    this.safeSend(ws, ["allUpdateKursiList", room, seatMetadata]);
    this.sendCurrentNumber(ws);
  }

  sendCurrentNumber(ws) {
    if (!this.isWebSocketReady(ws)) return false;
    return this.safeSend(ws, ["currentNumber", this.currentNumber]);
  }

  removeUserSeats(userId) {
    let removedCount = 0;
    
    for (const room of ROOM_LIST) {
      const seatMap = this.getRoomSeats(room);
      
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
    return Array.from(this.clients.keys());
  }

  getOnlineUsersByRoom(roomName) {
    return this.roomSubscriptions.has(roomName) ? 
           Array.from(this.roomSubscriptions.get(roomName)) : [];
  }

  // ✅ OPTIMISASI: Efficient client cleanup
  cleanupClient(ws) {
    const userId = ws.idtarget;
    
    if (userId) {
      this.clients.delete(userId);
      
      if (ws.roomname && this.roomSubscriptions.has(ws.roomname)) {
        this.roomSubscriptions.get(ws.roomname).delete(userId);
      }
      
      this.batalkanPendingRemoval(userId);
      
      const hasActiveConnection = Array.from(this.clients.values())
        .some(client => client.idtarget === userId && this.isWebSocketReady(client.ws));
      
      if (!hasActiveConnection) {
        const timeout = setTimeout(() => {
          this.removeUserSeats(userId);
          this.pendingRemove.delete(userId);
        }, GRACE_PERIOD);
        
        this.pendingRemove.set(userId, timeout);
      }
    }
    
    // Cleanup WebSocket properties
    if (ws.numkursi) ws.numkursi.clear();
    ws.roomname = undefined;
    ws.idtarget = undefined;
    if (ws._pendingSend) ws._pendingSend = null;
  }

  isInLowcardRoom(ws) {
    return ws.roomname === "LowCard";
  }

  async handleMessage(ws, rawMessage) {
    // Rate limiting
    if (ws.idtarget && !this.checkRateLimit(ws.idtarget)) {
      return this.safeSend(ws, ["error", "Too many messages, please wait"]);
    }
    
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
    
    // ✅ OPTIMISASI: Maintenance hanya setiap 10 pesan untuk reduce overhead
    if (Math.random() < 0.1) { // 10% chance
      this.periodicMaintenance();
    }

    try {
      switch (eventType) {
        case "setIdTarget":
          await this.handleSetIdTarget(ws, args[0]);
          break;
        case "sendnotif":
          await this.handleSendNotification(ws, args);
          break;
        case "private":
          await this.handlePrivateMessage(ws, args);
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
          await this.handleJoinRoom(ws, args[0]);
          break;
        case "chat":
          await this.handleChatMessage(ws, args);
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
          await this.handleGift(ws, args);
          break;
        case "onDestroy":
          this.handleDestroy(ws);
          break;
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (!this.isInLowcardRoom(ws)) {
            this.safeSend(ws, ["error", "Game Lowcard hanya tersedia di room LowCard"]);
          } else {
            this.lowcard.handleEvent(ws, data);
          }
          break;
        default:
          this.safeSend(ws, ["error", "Unknown event"]);
      }
    } catch (error) {
      console.error(`Error handling ${eventType}:`, error);
      this.safeSend(ws, ["error", "Internal server error"]);
    }
  }

  async handleSetIdTarget(ws, newId) {
    this.batalkanPendingRemoval(newId);
    ws.idtarget = newId;

    // Update client management
    this.clients.set(newId, {
      ws,
      room: ws.roomname,
      lastActive: Date.now(),
      idtarget: newId
    });

    // Update room subscriptions
    if (ws.roomname) {
      if (!this.roomSubscriptions.has(ws.roomname)) {
        this.roomSubscriptions.set(ws.roomname, new Set());
      }
      this.roomSubscriptions.get(ws.roomname).add(newId);
    }

    // Handle duplicate connections
    for (const [userId, clientInfo] of this.clients) {
      if (userId === newId && clientInfo.ws !== ws && this.isWebSocketReady(clientInfo.ws)) {
        clientInfo.ws.close(4000, "Duplicate connection");
        this.clients.delete(userId);
        break; // ✅ OPTIMISASI: Hanya perlu handle satu duplicate
      }
    }

    // Restore previous state
    const seatInfo = this.userToSeat.get(newId);
    if (seatInfo) {
      ws.roomname = seatInfo.room;
      this.sendRoomState(ws, seatInfo.room);
      this.broadcastRoomUserCount(seatInfo.room);
    }
  }

  async handleSendNotification(ws, [targetId, imageUrl, username, description]) {
    const notification = ["notif", imageUrl, username, description, Date.now()];
    let delivered = false;
    
    const targetClient = this.clients.get(targetId);
    if (targetClient && this.isWebSocketReady(targetClient.ws)) {
      this.safeSend(targetClient.ws, notification);
      delivered = true;
    }
    
    if (!delivered) {
      this.safeSend(ws, ["notifFailed", targetId, "User offline"]);
    }
  }

  async handlePrivateMessage(ws, [targetId, imageUrl, message, sender]) {
    const privateMsg = ["private", targetId, imageUrl, message, Date.now(), sender];
    this.safeSend(ws, privateMsg);
    
    let delivered = false;
    const targetClient = this.clients.get(targetId);
    if (targetClient && this.isWebSocketReady(targetClient.ws)) {
      this.safeSend(targetClient.ws, privateMsg);
      delivered = true;
    }
    
    if (!delivered) {
      this.safeSend(ws, ["privateFailed", targetId, "User offline"]);
    }
  }

  handleUserOnlineCheck(ws, username, marker = "") {
    const isOnline = this.clients.has(username);
    this.safeSend(ws, ["userOnlineStatus", username, isOnline, marker]);

    if (isOnline) {
      const clientInfo = this.clients.get(username);
      if (clientInfo.ws !== ws && this.isWebSocketReady(clientInfo.ws)) {
        this.removeUserSeats(username);
        clientInfo.ws.close(4000, "Duplicate login");
        this.clients.delete(username);
      }
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

  async handleJoinRoom(ws, newRoom) {
    if (!ROOM_LIST.includes(newRoom)) {
      return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
    }
    
    // Cleanup previous state
    if (ws.idtarget) {
      this.batalkanPendingRemoval(ws.idtarget);
      this.removeUserSeats(ws.idtarget);
      
      if (ws.roomname && this.roomSubscriptions.has(ws.roomname)) {
        this.roomSubscriptions.get(ws.roomname).delete(ws.idtarget);
      }
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
      
      this.clients.set(ws.idtarget, {
        ws,
        room: newRoom,
        lastActive: Date.now(),
        idtarget: ws.idtarget
      });
      
      if (!this.roomSubscriptions.has(newRoom)) {
        this.roomSubscriptions.set(newRoom, new Set());
      }
      this.roomSubscriptions.get(newRoom).add(ws.idtarget);
    }
    
    // Send room state
    if (this.isWebSocketReady(ws)) {
      this.sendRoomState(ws, newRoom);
      await this.broadcastRoomUserCount(newRoom);
      this.sendCurrentNumber(ws);
    }
  }

  async handleChatMessage(ws, [room, imageUrl, username, message, usernameColor, textColor]) {
    if (!ROOM_LIST.includes(room)) {
      return this.safeSend(ws, ["error", "Invalid room for chat"]);
    }
    
    await this.broadcastToRoom(room, ["chat", room, imageUrl, username, message, usernameColor, textColor], ws.idtarget);
  }

  handleUpdatePoint(ws, [room, seat, x, y, fast]) {
    if (!ROOM_LIST.includes(room)) return;
    
    const seatMap = this.getRoomSeats(room);
    const seatInfo = seatMap.get(seat);
    
    if (seatInfo) {
      seatInfo.points = [{ x, y, fast }];
      this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
    }
  }

  handleRemoveSeat(ws, room, seat) {
    if (!ROOM_LIST.includes(room)) return;
    
    const seatMap = this.getRoomSeats(room);
    Object.assign(seatMap.get(seat), createEmptySeat());
    
    this.broadcastToRoom(room, ["removeKursi", room, seat]);
    this.broadcastRoomUserCount(room);
  }

  handleUpdateSeat(ws, [room, seat, imageUrl, username, color, bottomItem, topItem, vip, vipMark]) {
    if (!ROOM_LIST.includes(room)) return;
    
    const seatMap = this.getRoomSeats(room);
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

  async handleGift(ws, [room, sender, receiver, giftName]) {
    if (!ROOM_LIST.includes(room)) return;
    
    await this.broadcastToRoom(room, ["gift", room, sender, receiver, giftName, Date.now()]);
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

    // Minimal WebSocket setup
    server.roomname = undefined;
    server.idtarget = undefined;
    server.numkursi = new Set();
    
    // ✅ OPTIMISASI: Efficient event listeners dengan proper cleanup
    const messageHandler = (event) => {
      this.handleMessage(server, event.data).catch(() => {
        // Silent fail - will be cleaned up
      });
    };
    
    const closeHandler = () => {
      server.removeEventListener("message", messageHandler);
      server.removeEventListener("close", closeHandler);
      server.removeEventListener("error", errorHandler);
      this.cleanupClient(server);
    };
    
    const errorHandler = () => {
      this.cleanupClient(server);
    };

    server.addEventListener("message", messageHandler);
    server.addEventListener("close", closeHandler);
    server.addEventListener("error", errorHandler);

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
        headers: { 
          "content-type": "text/plain",
          "cache-control": "no-cache"
        } 
      });
    }
    
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
