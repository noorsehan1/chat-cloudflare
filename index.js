// index.js - ChatServer2 untuk Durable Object (MEMORY ONLY - DIRECT REPLACE)
import { LowCardGameManager } from "./lowcard.js";

// Constants
const CONSTANTS = Object.freeze({
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 1,
  GRACE_PERIOD: 5000,
  MAX_MESSAGE_SIZE: 10000,
  MAX_GLOBAL_CONNECTIONS: 500,
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  MAX_NUMBER: 6,
  HEARTBEAT_INTERVAL: 30000,
  CLEANUP_INTERVAL: 30000,
  MAX_RATE_LIMIT: 100,
  RATE_WINDOW: 60000,
  MAX_USER_IDLE: 24 * 60 * 60 * 1000,
  MAX_ARRAY_SIZE: 500,
  MAX_TIMEOUT_MS: 10000,
  MAX_JSON_DEPTH: 100,
  MAX_GIFT_NAME: 100,
  MAX_USERNAME_LENGTH: 50,
  MAX_MESSAGE_LENGTH: 1000,
  MAX_ACTIVE_CLIENTS_HISTORY: 2000,
  LOCK_TIMEOUT_MS: 5000,
  PROMISE_TIMEOUT_MS: 30000,
});

// Room list
const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", 
  "India", "Indonesia", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love", 
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa", 
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"
]);

// ==================== MEMORY MONITOR ====================
class MemoryMonitor {
  constructor() {
    this.lastCheck = Date.now();
    this.consecutiveHighMemory = 0;
    this.gcInterval = null;
    this.isRunning = false;
  }
  
  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.gcInterval = setInterval(() => this.check(), 60000);
  }
  
  stop() {
    this.isRunning = false;
    if (this.gcInterval) {
      clearInterval(this.gcInterval);
      this.gcInterval = null;
    }
  }
  
  check() {
    if (!this.isRunning) return false;
    
    try {
      const memoryUsage = process.memoryUsage?.() || { heapUsed: 0, heapTotal: 0 };
      const heapUsedMB = memoryUsage.heapUsed / 1024 / 1024;
      
      if (heapUsedMB > 400) {
        this.consecutiveHighMemory++;
        console.log(`[MEMORY] High memory usage: ${heapUsedMB.toFixed(2)} MB (${this.consecutiveHighMemory})`);
        
        if (this.consecutiveHighMemory >= 5) {
          console.log("[MEMORY] Persistent high memory detected");
          this.consecutiveHighMemory = 0;
        }
      } else {
        this.consecutiveHighMemory = 0;
      }
    } catch(e) {
      // Ignore errors in memory monitoring
    }
    return false;
  }
}

// ==================== RATE LIMITER ====================
class RateLimiter {
  constructor(windowMs = CONSTANTS.RATE_WINDOW, maxRequests = CONSTANTS.MAX_RATE_LIMIT) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this._cleanupInterval = null;
    this._isDestroyed = false;
    // FIX: Jangan auto-start, biarkan ChatServer2 yang mengontrol
  }
  
  _startCleanup() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this._cleanupInterval = setInterval(() => this.cleanup(), 60000);
  }
  
  start() {
    if (this._isDestroyed) return;
    this._startCleanup();
  }
  
  stop() {
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
  }
  
  check(userId) {
    if (!userId || this._isDestroyed) return true;
    
    const now = Date.now();
    let data = this.requests.get(userId);
    
    if (!data) {
      this.requests.set(userId, { count: 1, windowStart: now });
      return true;
    }
    
    if (now - data.windowStart >= this.windowMs) {
      data.count = 1;
      data.windowStart = now;
      return true;
    }
    
    if (data.count >= this.maxRequests) return false;
    
    data.count++;
    return true;
  }

  cleanup() {
    if (this._isDestroyed) return;
    
    const now = Date.now();
    for (const [userId, data] of this.requests) {
      if (now - data.windowStart >= this.windowMs) {
        this.requests.delete(userId);
      }
    }
    
    if (this.requests.size > 10000) {
      const entries = Array.from(this.requests.entries());
      entries.sort((a, b) => a[1].windowStart - b[1].windowStart);
      const toDelete = entries.slice(0, Math.floor(entries.length * 0.2));
      for (const [userId] of toDelete) {
        this.requests.delete(userId);
      }
    }
  }
  
  destroy() {
    this._isDestroyed = true;
    this.stop();
    this.requests.clear();
  }
}

// ==================== SEAT DATA CLASS ====================
class SeatData {
  constructor() {
    this.noimageUrl = "";
    this.namauser = "";
    this.color = "";
    this.itembawah = 0;
    this.itematas = 0;
    this.vip = 0;
    this.viptanda = 0;
    this.lastPoint = null;
    this.lastUpdated = Date.now();
  }
  
  isEmpty() {
    return !this.namauser || this.namauser === "";
  }
  
  clear() {
    this.noimageUrl = "";
    this.namauser = "";
    this.color = "";
    this.itembawah = 0;
    this.itematas = 0;
    this.vip = 0;
    this.viptanda = 0;
    this.lastPoint = null;
    this.lastUpdated = Date.now();
  }
  
  copyFrom(other) {
    if (other) {
      this.noimageUrl = other.noimageUrl || "";
      this.namauser = other.namauser || "";
      this.color = other.color || "";
      this.itembawah = other.itembawah || 0;
      this.itematas = other.itematas || 0;
      this.vip = other.vip || 0;
      this.viptanda = other.viptanda || 0;
      this.lastPoint = other.lastPoint ? { ...other.lastPoint } : null;
      this.lastUpdated = Date.now();
    }
    return this;
  }
  
  toJSON() {
    return {
      noimageUrl: this.noimageUrl,
      namauser: this.namauser,
      color: this.color,
      itembawah: this.itembawah,
      itematas: this.itematas,
      vip: this.vip,
      viptanda: this.viptanda,
      lastPoint: this.lastPoint ? { ...this.lastPoint } : null,
      lastUpdated: this.lastUpdated
    };
  }
}

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
    
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
      this.seats.set(i, new SeatData());
    }
  }
  
  replaceSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const seat = this.seats.get(seatNumber);
    if (seat) {
      seat.copyFrom(seatData);
      return true;
    }
    return false;
  }
  
  replacePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const seat = this.seats.get(seatNumber);
    if (seat) {
      seat.lastPoint = {
        x: point.x,
        y: point.y,
        fast: point.fast || false,
        timestamp: Date.now()
      };
      seat.lastUpdated = Date.now();
      return true;
    }
    return false;
  }
  
  removeSeat(seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const seat = this.seats.get(seatNumber);
    if (seat) {
      seat.clear();
      return true;
    }
    return false;
  }
  
  getSeat(seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return null;
    
    const seat = this.seats.get(seatNumber);
    return seat ? seat.toJSON() : null;
  }
  
  getOccupiedSeats() {
    const occupied = {};
    for (const [seatNum, seat] of this.seats) {
      if (!seat.isEmpty()) {
        occupied[seatNum] = seat.namauser;
      }
    }
    return occupied;
  }
  
  getOccupiedCount() {
    let count = 0;
    for (const seat of this.seats.values()) {
      if (!seat.isEmpty()) {
        count++;
      }
    }
    return count;
  }
  
  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
      if (!seat.isEmpty()) {
        meta[seatNum] = {
          noimageUrl: seat.noimageUrl || "",
          namauser: seat.namauser,
          color: seat.color || "",
          itembawah: seat.itembawah || 0,
          itematas: seat.itematas || 0,
          vip: seat.vip || 0,
          viptanda: seat.viptanda || 0
        };
      }
    }
    return meta;
  }
  
  getAllPoints() {
    const points = [];
    for (const [seatNum, seat] of this.seats) {
      if (seat.lastPoint && seat.lastPoint.x !== undefined && !seat.isEmpty()) {
        points.push({
          seat: seatNum,
          x: seat.lastPoint.x,
          y: seat.lastPoint.y,
          fast: seat.lastPoint.fast ? 1 : 0
        });
      }
    }
    return points;
  }
  
  setMute(isMuted) {
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    return this.muteStatus;
  }
  
  getMute() {
    return this.muteStatus;
  }
  
  setCurrentNumber(number) {
    this.currentNumber = number;
  }
  
  getCurrentNumber() {
    return this.currentNumber;
  }
  
  destroy() {
    this.seats.clear();
  }
}

// ==================== MAIN CHATSERVER2 CLASS ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._pendingPromises = new Map();
    this._lastCleanupLog = null;
    this._lastCountUpdate = Date.now();
    
    this.roomManagers = new Map();
    this._roomCountsCache = new Map();
    
    this.clients = new Set();
    this._activeClients = [];
    
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.disconnectedTimers = new Map();
    this._pendingReconnections = new Map();
    this.userLastSeen = new Map();
    this.userIPs = new Map();
    this._ipConnectionCount = new Map();
    
    this.rateLimiter = new RateLimiter();
    this._wsEventListeners = new Map();
    this._cleanupInterval = null;
    this._promiseCleanupInterval = null;
    
    try { 
      this.lowcard = new LowCardGameManager(this); 
    } catch (error) { 
      console.error("Failed to initialize game manager:", error);
      this.lowcard = null; 
    }
    
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
    this.numberTickTimer = null;
    this._tickRunning = false;
    
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
      this._roomCountsCache.set(room, 0);
    }
    
    this.memoryMonitor = new MemoryMonitor();
    
    this.startNumberTickTimer();
    this._startPeriodicCleanup();
    this._startPromiseCleanup();
    this.rateLimiter.start();  // FIX: Start rate limiter
    this.memoryMonitor.start();
  }
  
  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) {
      counts[room] = this._roomCountsCache.get(room) || 0;
    }
    return counts;
  }
  
  getAllRoomCountsArray() {
    return roomList.map(room => [room, this._roomCountsCache.get(room) || 0]);
  }
  
  updateRoomCount(room, delta = null) {
    if (!room || !roomList.includes(room)) return 0;
    
    let count;
    if (delta !== null) {
      const currentCount = this._roomCountsCache.get(room) || 0;
      count = Math.max(0, currentCount + delta);
      this._roomCountsCache.set(room, count);
    } else {
      const roomManager = this.roomManagers.get(room);
      count = roomManager.getOccupiedCount();
      this._roomCountsCache.set(room, count);
    }
    
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  
  getRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    
    const cached = this._roomCountsCache.get(room);
    if (cached !== undefined) {
      return cached;
    }
    
    const roomManager = this.roomManagers.get(room);
    const count = roomManager.getOccupiedCount();
    this._roomCountsCache.set(room, count);
    return count;
  }
  
  refreshRoomCounts() {
    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      const count = roomManager.getOccupiedCount();
      this._roomCountsCache.set(room, count);
    }
  }
  
  updateSeatDirect(room, seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    
    const oldSeat = roomManager.getSeat(seatNumber);
    const wasOccupied = oldSeat && oldSeat.namauser && oldSeat.namauser !== "";
    const isOccupied = seatData.namauser && seatData.namauser !== "";
    
    const success = roomManager.replaceSeat(seatNumber, seatData);
    
    if (success) {
      if (wasOccupied && !isOccupied) {
        const currentCount = this._roomCountsCache.get(room) || 0;
        this._roomCountsCache.set(room, Math.max(0, currentCount - 1));
      } else if (!wasOccupied && isOccupied) {
        const currentCount = this._roomCountsCache.get(room) || 0;
        this._roomCountsCache.set(room, currentCount + 1);
      }
    }
    
    return success;
  }
  
  updatePointDirect(room, seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    
    return roomManager.replacePoint(seatNumber, point);
  }
  
  removeSeatDirect(room, seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    
    const seatData = roomManager.getSeat(seatNumber);
    const wasOccupied = seatData && seatData.namauser && seatData.namauser !== "";
    
    const success = roomManager.removeSeat(seatNumber);
    
    if (success && wasOccupied) {
      const currentCount = this._roomCountsCache.get(room) || 0;
      this._roomCountsCache.set(room, Math.max(0, currentCount - 1));
    }
    
    return success;
  }
  
  assignNewSeat(room, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return null;
    
    const currentCount = roomManager.getOccupiedCount();
    if (currentCount >= CONSTANTS.MAX_SEATS) return null;
    
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      const seatData = roomManager.getSeat(seat);
      if (!seatData || !seatData.namauser || seatData.namauser === "") {
        const newSeat = new SeatData();
        newSeat.namauser = userId;
        newSeat.lastUpdated = Date.now();
        
        if (roomManager.replaceSeat(seat, newSeat.toJSON())) {
          const currentCount = this._roomCountsCache.get(room) || 0;
          this._roomCountsCache.set(room, currentCount + 1);
          this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, userId]);
          return seat;
        }
      }
    }
    return null;
  }
  
  _addUserConnection(userId, ws, ip = null) {
    if (!userId || !ws) return;
    
    let userConnections = this.userConnections.get(userId);
    if (!userConnections) {
      userConnections = new Set();
      this.userConnections.set(userId, userConnections);
    }
    
    for (const conn of userConnections) {
      if (conn === ws) return;
    }
    
    if (userConnections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
      const oldest = Array.from(userConnections)[0];
      if (oldest && oldest.readyState === 1) {
        try { oldest.close(1000, "Too many connections"); } catch {}
        userConnections.delete(oldest);
        this._removeFromActiveClients(oldest);
      }
    }
    
    userConnections.add(ws);
    this.userLastSeen.set(userId, Date.now());
    
    if (ip) {
      let count = this._ipConnectionCount.get(ip) || 0;
      count++;
      this._ipConnectionCount.set(ip, count);
      this.userIPs.set(ip, (this.userIPs.get(ip) || 0) + 1);
    }
  }
  
  _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    
    const userConnections = this.userConnections.get(userId);
    if (userConnections) {
      userConnections.delete(ws);
      if (userConnections.size === 0) {
        this.userConnections.delete(userId);
      }
    }
    
    if (ws._ip) {
      const count = this._ipConnectionCount.get(ws._ip) || 0;
      if (count <= 1) {
        this._ipConnectionCount.delete(ws._ip);
      } else {
        this._ipConnectionCount.set(ws._ip, count - 1);
      }
      
      const userIpCount = this.userIPs.get(ws._ip) || 0;
      if (userIpCount <= 1) {
        this.userIPs.delete(ws._ip);
      } else {
        this.userIPs.set(ws._ip, userIpCount - 1);
      }
    }
  }
  
  _removeFromActiveClients(ws) {
    const index = this._activeClients.indexOf(ws);
    if (index > -1) {
      this._activeClients.splice(index, 1);
    }
  }
  
  _compactActiveClients() {
    const before = this._activeClients.length;
    this._activeClients = this._activeClients.filter(ws => 
      ws !== null && ws.readyState === 1 && !ws._isClosing
    );
    const after = this._activeClients.length;
    if (before !== after && before > 100) {
      console.log(`[CLEANUP] Compacted _activeClients: ${before} -> ${after}`);
    }
  }
  
  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    
    const clientArray = this.roomClients.get(room);
    if (clientArray) {
      const index = clientArray.indexOf(ws);
      if (index > -1) {
        clientArray.splice(index, 1);
      }
    }
  }
  
  async safeSend(ws, msg) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
      
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      if (message.length > CONSTANTS.MAX_MESSAGE_SIZE) return false;
      
      ws.send(message);
      if (ws.idtarget) {
        this.userLastSeen.set(ws.idtarget, Date.now());
      }
      return true;
    } catch {
      return false;
    }
  }
  
  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    const clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    
    const message = JSON.stringify(msg);
    let sentCount = 0;
    
    for (let i = 0; i < clientArray.length; i++) {
      const client = clientArray[i];
      if (client && client.readyState === 1 && client.roomname === room && !client._isClosing) {
        try { 
          client.send(message); 
          sentCount++; 
        } catch (e) {}
      }
    }
    return sentCount;
  }
  
  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
      
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return;
      
      const allKursiMeta = roomManager.getAllSeatsMeta();
      const lastPointsData = roomManager.getAllPoints();
      
      const seatInfo = this.userToSeat.get(ws.idtarget);
      const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
      
      let filteredMeta = allKursiMeta;
      let filteredPoints = lastPointsData;
      
      if (excludeSelfSeat && selfSeat) {
        filteredMeta = { ...allKursiMeta };
        delete filteredMeta[selfSeat];
        filteredPoints = lastPointsData.filter(p => p.seat !== selfSeat);
      }
      
      if (Object.keys(filteredMeta).length > 0) {
        await this.safeSend(ws, ["allUpdateKursiList", room, filteredMeta]);
      }
      
      if (filteredPoints.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, filteredPoints]);
      }
      
      await this.safeSend(ws, ["roomUserCount", room, this.getRoomCount(room)]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      
      if (selfSeat) {
        await this.safeSend(ws, ["numberKursiSaya", selfSeat]);
      }
      
    } catch (error) {
      console.error("Error sending state:", error);
    }
  }
  
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) { 
      await this.safeSend(ws, ["error", "User ID not set"]); 
      return false; 
    }
    if (!roomList.includes(room)) { 
      await this.safeSend(ws, ["error", "Invalid room"]); 
      return false; 
    }
    if (!this.rateLimiter.check(ws.idtarget)) { 
      await this.safeSend(ws, ["error", "Too many requests"]); 
      return false; 
    }
    
    try {
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      const currentRoomBeforeJoin = this.userCurrentRoom.get(ws.idtarget);
      
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const roomManager = this.roomManagers.get(room);
        const seatData = roomManager.getSeat(seatNum);
        
        if (seatData && seatData.namauser === ws.idtarget) {
          this.cancelCleanup(ws.idtarget);
          ws.roomname = room;
          
          let clientArray = this.roomClients.get(room);
          if (!clientArray) {
            clientArray = [];
            this.roomClients.set(room, clientArray);
          }
          
          if (!clientArray.includes(ws)) {
            clientArray.push(ws);
          }
          
          this._addUserConnection(ws.idtarget, ws);
          this.userCurrentRoom.set(ws.idtarget, room);
          
          await this.sendAllStateTo(ws, room);
          
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          
          return true;
        } else {
          this.userToSeat.delete(ws.idtarget);
        }
      }
      
      if (currentRoomBeforeJoin && currentRoomBeforeJoin !== room) {
        const oldSeatInfo = this.userToSeat.get(ws.idtarget);
        if (oldSeatInfo && oldSeatInfo.room === currentRoomBeforeJoin) {
          this.removeSeatDirect(currentRoomBeforeJoin, oldSeatInfo.seat);
          this.broadcastToRoom(currentRoomBeforeJoin, ["removeKursi", currentRoomBeforeJoin, oldSeatInfo.seat]);
        }
        this._removeFromRoomClients(ws, currentRoomBeforeJoin);
        this.userToSeat.delete(ws.idtarget);
        this.userCurrentRoom.delete(ws.idtarget);
      }
      
      this.cancelCleanup(ws.idtarget);
      
      const currentCount = this.getRoomCount(room);
      if (currentCount >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      const assignedSeat = this.assignNewSeat(room, ws.idtarget);
      if (!assignedSeat) { 
        await this.safeSend(ws, ["roomFull", room]); 
        return false; 
      }
      
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      
      let clientArray = this.roomClients.get(room);
      if (!clientArray) {
        clientArray = [];
        this.roomClients.set(room, clientArray);
      }
      
      if (!clientArray.includes(ws)) {
        clientArray.push(ws);
      }
      
      this._addUserConnection(ws.idtarget, ws);
      
      await this.sendAllStateTo(ws, room);
      
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      
      const roomManager = this.roomManagers.get(room);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      
      return true;
      
    } catch (error) {
      console.error("Error joining room:", error);
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }
  
  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    
    try {
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo && seatInfo.room === room) {
        this.removeSeatDirect(room, seatInfo.seat);
        this.broadcastToRoom(room, ["removeKursi", room, seatInfo.seat]);
        this.userToSeat.delete(ws.idtarget);
        this.userCurrentRoom.delete(ws.idtarget);
      }
      
      this._removeFromRoomClients(ws, room);
      this._removeUserConnection(ws.idtarget, ws);
      ws.roomname = undefined;
      
    } catch (error) {
      console.error("Error in cleanupFromRoom:", error);
    }
  }
  
  _startPeriodicCleanup() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this._cleanupInterval = setInterval(() => {
      this._periodicCleanup().catch(err => {
        console.error("Periodic cleanup error:", err);
      });
    }, CONSTANTS.CLEANUP_INTERVAL);
  }
  
  _startPromiseCleanup() {
    if (this._promiseCleanupInterval) clearInterval(this._promiseCleanupInterval);
    this._promiseCleanupInterval = setInterval(() => {
      if (this._pendingPromises.size > 100) {
        const now = Date.now();
        for (const [key, data] of this._pendingPromises) {
          if (now - data.timestamp > CONSTANTS.PROMISE_TIMEOUT_MS) {
            this._pendingPromises.delete(key);
          }
        }
      }
    }, 10000);
  }
  
  scheduleCleanup(userId) {
    if (!userId) return;
    this.cancelCleanup(userId);
    
    const timerId = setTimeout(async () => {
      try {
        this.disconnectedTimers.delete(userId);
        this._pendingReconnections.delete(userId);
        const isStillConnected = await this.isUserStillConnected(userId);
        if (!isStillConnected) {
          await this.forceUserCleanup(userId);
        }
      } catch (error) {
        console.error("Error in scheduleCleanup timer:", error);
      }
    }, CONSTANTS.GRACE_PERIOD);
    
    timerId._scheduledTime = Date.now();
    this.disconnectedTimers.set(userId, timerId);
  }
  
  cancelCleanup(userId) {
    if (!userId) return;
    const timer = this.disconnectedTimers.get(userId);
    if (timer) { 
      clearTimeout(timer); 
      this.disconnectedTimers.delete(userId);
    }
    this._pendingReconnections.delete(userId);
  }
  
  async isUserStillConnected(userId) {
    if (!userId) return false;
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return false;
    for (const conn of connections) {
      if (conn && conn.readyState === 1 && !conn._isClosing) return true;
    }
    return false;
  }
  
  async forceUserCleanup(userId) {
    if (!userId) return;
    
    try {
      this.cancelCleanup(userId);
      const currentRoom = this.userCurrentRoom.get(userId);
      
      if (currentRoom) {
        const seatInfo = this.userToSeat.get(userId);
        
        if (seatInfo) {
          this.removeSeatDirect(currentRoom, seatInfo.seat);
          this.broadcastToRoom(currentRoom, ["removeKursi", currentRoom, seatInfo.seat]);
        }
      }
      
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      this.userLastSeen.delete(userId);
      
      for (const [room, clientArray] of this.roomClients) {
        if (clientArray?.length > 0) {
          const index = clientArray.findIndex(c => c?.idtarget === userId);
          if (index > -1) {
            clientArray.splice(index, 1);
          }
        }
      }
      
      for (let i = 0; i < this._activeClients.length; i++) {
        if (this._activeClients[i]?.idtarget === userId) {
          this._activeClients.splice(i, 1);
          i--;
        }
      }
      
      this.userConnections.delete(userId);
      
    } catch (error) {
      console.error("Error in forceUserCleanup:", error);
    }
  }
  
  // FIX: Perbaikan total untuk safeWebSocketCleanup - hapus semua event listeners dan timers
  async safeWebSocketCleanup(ws) {
    if (!ws) return;
    
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      ws._isClosing = true;
      this.clients.delete(ws);
      this._removeFromActiveClients(ws);
      
      // FIX: Hapus semua event listeners dari Map
      const handlers = this._wsEventListeners.get(ws);
      if (handlers) {
        if (handlers.message) ws.removeEventListener("message", handlers.message);
        if (handlers.error) ws.removeEventListener("error", handlers.error);
        if (handlers.close) ws.removeEventListener("close", handlers.close);
        this._wsEventListeners.delete(ws);
      }
      
      // FIX: Hapus promise yang terkait dengan ws ini
      for (const [key, data] of this._pendingPromises) {
        if (data.userId === userId) {
          this._pendingPromises.delete(key);
        }
      }
      
      if (userId) {
        this._removeUserConnection(userId, ws);
        
        const hasOtherConnections = await this.isUserStillConnected(userId);
        
        if (!hasOtherConnections) {
          this.cancelCleanup(userId);
          
          const seatInfo = this.userToSeat.get(userId);
          if (seatInfo && seatInfo.room === room) {
            this._pendingReconnections.set(userId, {
              seatInfo: seatInfo,
              currentRoom: room,
              timestamp: Date.now()
            });
          }
          
          // FIX: Timer akan dihapus di _periodicCleanup() setelah timeout
          // Tidak perlu clear manual di sini
        } else {
          // FIX: Jika masih ada koneksi lain, hapus data reconnection
          this._pendingReconnections.delete(userId);
        }
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
      }
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Normal closure"); } catch (e) {}
      }
      
      // Cleanup properties
      ws._chatServer = null;
      ws._ip = null;
      ws._connectionTime = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      
    } catch (error) {
      console.error("Error in safeWebSocketCleanup:", error);
    }
  }
  
  async _periodicCleanup() {
    if (this._isClosing) return;
    
    const now = Date.now();
    
    try {
      if (this._activeClients.length > 500) {
        this._compactActiveClients();
      }
      
      if (this._activeClients.length > CONSTANTS.MAX_ACTIVE_CLIENTS_HISTORY) {
        this._activeClients = this._activeClients.slice(-CONSTANTS.MAX_ACTIVE_CLIENTS_HISTORY);
      }
      
      for (const [room, clients] of this.roomClients) {
        const before = clients.length;
        const filtered = clients.filter(c => 
          c !== null && c.readyState === 1 && !c._isClosing
        );
        
        if (filtered.length !== before) {
          this.roomClients.set(room, filtered);
          if (before > 100) {
            console.log(`[CLEANUP] Room ${room}: ${before} -> ${filtered.length}`);
          }
        }
      }
      
      const beforeClients = this.clients.size;
      for (const ws of this.clients) {
        if (!ws || ws.readyState !== 1 || ws._isClosing) {
          this.clients.delete(ws);
        }
      }
      if (beforeClients !== this.clients.size) {
        console.log(`[CLEANUP] Clients Set: ${beforeClients} -> ${this.clients.size}`);
      }
      
      for (const [userId, connections] of this.userConnections) {
        const alive = new Set();
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            alive.add(conn);
          }
        }
        
        if (alive.size === 0) {
          this.userConnections.delete(userId);
        } else if (alive.size !== connections.size) {
          this.userConnections.set(userId, alive);
        }
      }
      
      // FIX: Bersihkan semua timer yang sudah timeout
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer._scheduledTime && (now - timer._scheduledTime) > CONSTANTS.GRACE_PERIOD + 5000) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
          this._pendingReconnections.delete(userId);
          await this.forceUserCleanup(userId);
        }
      }
      
      for (const [userId, data] of this._pendingReconnections) {
        if (data.timestamp && (now - data.timestamp) > CONSTANTS.GRACE_PERIOD * 2) {
          this._pendingReconnections.delete(userId);
        }
      }
      
      // HANYA UNTUK USER OFFLINE (sudah tidak punya koneksi)
      for (const [userId, lastSeen] of this.userLastSeen) {
        if (now - lastSeen > CONSTANTS.MAX_USER_IDLE) {
          await this.forceUserCleanup(userId);
        }
      }
      
      if (this.userIPs.size > 10000) {
        const entries = Array.from(this.userIPs.entries());
        entries.sort((a, b) => a[1] - b[1]);
        const toDelete = entries.slice(0, Math.floor(entries.length * 0.3));
        for (const [ip] of toDelete) {
          this.userIPs.delete(ip);
        }
      }
      
      if (this._ipConnectionCount.size > 5000) {
        const entries = Array.from(this._ipConnectionCount.entries());
        entries.sort((a, b) => a[1] - b[1]);
        const toDelete = entries.slice(0, Math.floor(entries.length * 0.3));
        for (const [ip] of toDelete) {
          this._ipConnectionCount.delete(ip);
        }
      }
      
      if (now - this._lastCountUpdate > 30000) {
        this.refreshRoomCounts();
        this._lastCountUpdate = now;
      }
      
      this.rateLimiter.cleanup();
      
      this.memoryMonitor.check();
      
      if (!this._lastCleanupLog || now - this._lastCleanupLog > 3600000) {
        const activeReal = this._activeClients.filter(c => c?.readyState === 1).length;
        console.log(`[MEMORY] Active: ${activeReal}/${this._activeClients.length}, Rooms: ${this.roomClients.size}, Users: ${this.userConnections.size}, Timers: ${this.disconnectedTimers.size}, Pending: ${this._pendingPromises.size}`);
        this._lastCleanupLog = now;
      }
    } catch (error) {
      console.error("Periodic cleanup error:", error);
    }
  }
  
  async forceGlobalCleanup() {
    console.log("[CLEANUP] Forcing global cleanup due to high memory");
    
    // FIX: Bersihkan semua timer di disconnectedTimers
    for (const [userId, timer] of this.disconnectedTimers) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
    this._pendingReconnections.clear();
    
    for (const ws of this._activeClients) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          await this.safeSend(ws, ["serverBusy", "Server is busy, please reconnect"]);
        } catch(e) {}
      }
    }
    
    this.refreshRoomCounts();
  }
  
  setRoomMute(roomName, isMuted) {
    if (!roomName || !roomList.includes(roomName)) return false;
    
    const roomManager = this.roomManagers.get(roomName);
    const muteValue = roomManager.setMute(isMuted);
    
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }
  
  getRoomMute(roomName) {
    if (!roomName || !roomList.includes(roomName)) return false;
    const roomManager = this.roomManagers.get(roomName);
    return roomManager.getMute();
  }
  
  startNumberTickTimer() {
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    
    const scheduleNext = () => {
      if (this._isClosing) return;
      
      this.numberTickTimer = setTimeout(async () => {
        try {
          await this._safeTick();
        } catch (error) {
          console.error("Error in tick:", error);
        }
        scheduleNext();
      }, this.intervalMillis);
    };
    
    scheduleNext();
  }
  
  async _safeTick() {
    if (this._tickRunning || this._isClosing) return;
    this._tickRunning = true;
    
    try {
      const newNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      this.currentNumber = newNumber;
      
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const notifiedUsers = new Set();
      
      for (let i = 0; i < this._activeClients.length; i++) {
        const client = this._activeClients[i];
        if (client?.readyState === 1 && client.roomname && !client._isClosing) {
          if (!notifiedUsers.has(client.idtarget)) {
            try {
              client.send(message);
              notifiedUsers.add(client.idtarget);
            } catch (e) {}
          }
        }
      }
      
    } finally {
      this._tickRunning = false;
    }
  }
  
  async handleSetIdTarget2(ws, id, baru, ip = null) {
    if (!id || !ws) return;
    
    try {
      if (ip) {
        const ipCount = this.userIPs.get(ip) || 0;
        if (ipCount > 20) {
          ws.close(1000, "Too many connections from this IP");
          return;
        }
        this.userIPs.set(ip, ipCount + 1);
        
        const connCount = this._ipConnectionCount.get(ip) || 0;
        this._ipConnectionCount.set(ip, connCount + 1);
      }
      
      const existingConnections = this.userConnections.get(id);
      if (existingConnections && existingConnections.size > 0) {
        const oldWs = Array.from(existingConnections)[0];
        if (oldWs && oldWs !== ws && oldWs.readyState === 1) {
          oldWs._isClosing = true;
          try {
            await this.safeSend(oldWs, ["connectionReplaced", "New connection detected"]);
            oldWs.close(1000, "Replaced by new connection");
          } catch(e) {}
          this.clients.delete(oldWs);
          this._removeFromActiveClients(oldWs);
          if (oldWs.roomname) {
            this._removeFromRoomClients(oldWs, oldWs.roomname);
          }
          this._removeUserConnection(id, oldWs);
        }
      }
      
      this.cancelCleanup(id);
      
      if (baru === true) {
        ws.idtarget = id;
        ws.roomname = undefined;
        ws._isClosing = false;
        ws._connectionTime = Date.now();
        this._addUserConnection(id, ws, ip);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      
      const pendingData = this._pendingReconnections.get(id);
      if (pendingData && pendingData.seatInfo) {
        const { room, seat } = pendingData.seatInfo;
        const roomManager = this.roomManagers.get(room);
        
        if (roomManager && seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const seatData = roomManager.getSeat(seat);
          if (seatData && seatData.namauser === id) {
            ws.roomname = room;
            const clientArray = this.roomClients.get(room);
            if (clientArray && !clientArray.includes(ws)) {
              clientArray.push(ws);
            }
            this._addUserConnection(id, ws, ip);
            this.userToSeat.set(id, { room, seat });
            this.userCurrentRoom.set(id, room);
            
            await this.sendAllStateTo(ws, room);
            if (seatData.lastPoint) {
              await this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast ? 1 : 0]);
            }
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            
            this._pendingReconnections.delete(id);
            return;
          }
        }
      }
      
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const roomManager = this.roomManagers.get(room);
          if (roomManager) {
            const seatData = roomManager.getSeat(seat);
            if (seatData && seatData.namauser === id) {
              ws.roomname = room;
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) {
                clientArray.push(ws);
              }
              this._addUserConnection(id, ws, ip);
              await this.sendAllStateTo(ws, room);
              if (seatData.lastPoint) {
                await this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast ? 1 : 0]);
              }
              await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
              await this.safeSend(ws, ["numberKursiSaya", seat]);
              return;
            }
          }
        }
        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
        if (seatInfo.room) {
          await this.forceUserCleanup(id);
        }
      }
      
      this._addUserConnection(id, ws, ip);
      await this.safeSend(ws, ["needJoinRoom"]);
      
    } catch (error) {
      console.error("Error in handleSetIdTarget2:", error);
      await this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }
  
  // FIX: Perbaikan handleMessage dengan validasi input yang lebih kuat dan promise tracking yang benar
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    if (ws.idtarget && !this.rateLimiter.check(ws.idtarget)) {
      await this.safeSend(ws, ["error", "Too many requests"]);
      return;
    }
    
    let messageStr = raw;
    if (raw instanceof ArrayBuffer) {
      try {
        messageStr = new TextDecoder().decode(raw);
      } catch (e) {
        return;
      }
    }
    
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) {
      try { ws.close(1009, "Message too large"); } catch {}
      return;
    }
    
    // FIX: Validasi input yang lebih kuat
    let data;
    try {
      const parsed = JSON.parse(messageStr);
      
      if (!Array.isArray(parsed) || parsed.length === 0) {
        return;
      }
      
      for (let i = 0; i < parsed.length; i++) {
        const item = parsed[i];
        const type = typeof item;
        
        if (type !== 'string' && type !== 'number' && type !== 'boolean' && item !== null) {
          console.warn(`[SECURITY] Invalid message type at index ${i}: ${type}`);
          return;
        }
        
        if (type === 'string') {
          const suspicious = ['__proto__', 'constructor', 'prototype', '__defineGetter__', '__defineSetter__'];
          for (const pattern of suspicious) {
            if (item.includes(pattern)) {
              console.warn(`[SECURITY] Suspicious string detected: ${pattern}`);
              return;
            }
          }
        }
      }
      
      data = parsed;
    } catch (e) {
      try { ws.close(1008, "Protocol error"); } catch {}
      return;
    }
    
    const evt = data[0];
    
    // FIX: Promise tracking dengan timeout yang benar
    const promiseKey = Symbol('promise');
    const timeoutId = setTimeout(() => {
      if (this._pendingPromises.has(promiseKey)) {
        console.error(`[PROMISE] Timeout for user ${ws.idtarget || 'unknown'}, event: ${evt}`);
        this._pendingPromises.delete(promiseKey);
      }
    }, CONSTANTS.PROMISE_TIMEOUT_MS);
    
    this._pendingPromises.set(promiseKey, {
      timestamp: Date.now(),
      userId: ws.idtarget || 'unknown',
      event: evt
    });
    
    try {
      await this._processMessage(ws, data, evt);
    } catch (error) {
      console.error(`[ERROR] Processing event ${evt}:`, error);
      try {
        await this.safeSend(ws, ["error", "Failed to process message"]);
      } catch (e) {}
    } finally {
      clearTimeout(timeoutId);
      this._pendingPromises.delete(promiseKey);
    }
  }
  
  async _processMessage(ws, data, evt) {
    try {
      switch (evt) {
        case "isInRoom": {
          const idtarget = ws.idtarget;
          if (!idtarget) { 
            await this.safeSend(ws, ["inRoomStatus", false]); 
            return; 
          }
          const currentRoom = this.userCurrentRoom.get(idtarget);
          await this.safeSend(ws, ["inRoomStatus", currentRoom !== undefined]);
          break;
        }
        
        case "rollangak": {
          const roomName = data[1], username = data[2], angka = data[3];
          if (!roomName || !roomList.includes(roomName)) break;
          this.broadcastToRoom(roomName, ["rollangakBroadcast", roomName, username, angka]);
          break;
        }
        
        case "modwarning": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName) && ws.idtarget) {
            this.broadcastToRoom(roomName, ["modwarning", roomName]);
          }
          break;
        }
        
        case "setMuteType": {
          const isMuted = data[1], roomName = data[2];
          if (!roomName || !roomList.includes(roomName)) break;
          const success = this.setRoomMute(roomName, isMuted);
          const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
          await this.safeSend(ws, ["muteTypeSet", muteValue, success, roomName]);
          break;
        }
        
        case "getMuteType": {
          const roomName = data[1];
          if (!roomName || !roomList.includes(roomName)) break;
          const roomManager = this.roomManagers.get(roomName);
          await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), roomName]);
          break;
        }
        
        case "onDestroy": { 
          await this.safeWebSocketCleanup(ws); 
          break; 
        }
        
        case "setIdTarget2": {
          const ip = ws._ip || null;
          await this.handleSetIdTarget2(ws, data[1], data[2], ip); 
          break; 
        }
        
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          const targetConnections = this.userConnections.get(idtarget);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                await this.safeSend(client, notif);
                break;
              }
            }
          }
          break;
        }
        
        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const sanitizedMsg = msg?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          const sanitizedSender = sender?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "";
          const out = ["private", idt, url, sanitizedMsg, ts, sanitizedSender];
          
          await this.safeSend(ws, out);
          
          const targetConnections = this.userConnections.get(idt);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                await this.safeSend(client, out);
                break;
              }
            }
          }
          break;
        }
        
        case "isUserOnline": {
          const username = data[1], tanda = data[2] ?? "";
          const isOnline = await this.isUserStillConnected(username);
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, tanda]);
          break;
        }
        
        case "getAllRoomsUserCount": {
          const result = this.getAllRoomCountsArray();
          await this.safeSend(ws, ["allRoomsUserCount", result]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) break;
          const count = this.getRoomCount(roomName);
          await this.safeSend(ws, ["roomUserCount", roomName, count]);
          break;
        }
        
        case "getCurrentNumber": { 
          await this.safeSend(ws, ["currentNumber", this.currentNumber]); 
          break; 
        }
        
        case "getOnlineUsers": {
          const users = [];
          const seenUsers = new Set();
          for (let i = 0; i < this._activeClients.length; i++) {
            const client = this._activeClients[i];
            if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
              if (!seenUsers.has(client.idtarget)) {
                users.push(client.idtarget);
                seenUsers.add(client.idtarget);
              }
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) return;
          const users = [];
          const seenUsers = new Set();
          const clientArray = this.roomClients.get(roomName);
          if (clientArray) {
            for (let i = 0; i < clientArray.length; i++) {
              const client = clientArray[i];
              if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
                if (!seenUsers.has(client.idtarget)) {
                  users.push(client.idtarget);
                  seenUsers.add(client.idtarget);
                }
              }
            }
          }
          await this.safeSend(ws, ["roomOnlineUsers", roomName, users]);
          break;
        }
        
        case "joinRoom": {
          const success = await this.handleJoinRoom(ws, data[1]);
          if (success && ws.roomname) this.updateRoomCount(ws.roomname);
          break;
        }
        
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          
          if (ws.roomname !== roomname || ws.idtarget !== username) return;
          if (!roomList.includes(roomname)) return;
          
          const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          const sanitizedUsername = username?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "";
          
          if (sanitizedMessage.includes('\0') || sanitizedUsername.includes('\0')) return;
          
          let isPrimary = true;
          const userConnections = this.userConnections.get(username);
          if (userConnections?.size > 0) {
            let earliest = null;
            for (const conn of userConnections) {
              if (conn?.readyState === 1 && !conn._isClosing) {
                if (!earliest || (conn._connectionTime || 0) < (earliest._connectionTime || 0)) {
                  earliest = conn;
                }
              }
            }
            if (earliest && earliest !== ws) isPrimary = false;
          }
          if (!isPrimary) return;
          
          const chatMsg = ["chat", roomname, noImageURL, sanitizedUsername, sanitizedMessage, usernameColor, chatTextColor];
          this.broadcastToRoom(roomname, chatMsg);
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat) return;
          
          const success = this.updatePointDirect(room, seat, { 
            x: parseFloat(x), 
            y: parseFloat(y), 
            fast: fast === 1 || fast === true 
          });
          
          if (success) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat) return;
          
          const success = this.removeSeatDirect(room, seat);
          
          if (success) {
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
          }
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
          
          const roomManager = this.roomManagers.get(room);
          const existingSeat = roomManager.getSeat(seat);
          
          const updatedSeat = {
            noimageUrl: noimageUrl?.slice(0, 255) || "", 
            namauser: namauser || "", 
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0,
            lastPoint: existingSeat?.lastPoint || null,
            lastUpdated: Date.now()
          };
          
          const success = this.updateSeatDirect(room, seat, updatedSeat);
          
          if (!success) {
            await this.safeSend(ws, ["error", "Failed to update seat"]);
            return;
          }
          
          if (namauser === ws.idtarget) {
            this.userToSeat.set(namauser, { room, seat });
            this.userCurrentRoom.set(namauser, room);
          }
          
          const kursiBatchData = [];
          kursiBatchData.push([seat, {
            noimageUrl: noimageUrl || "",
            namauser: namauser || "",
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0
          }]);
          
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, kursiBatchData]);
          this.updateRoomCount(room);
          
          break;
        }
        
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (ws.roomname !== roomname || ws.idtarget !== sender) return;
          if (!roomList.includes(roomname)) return;
          
          const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          const giftData = ["gift", roomname, sender, receiver, safeGiftName, Date.now()];
          this.broadcastToRoom(roomname, giftData);
          break;
        }
        
        case "leaveRoom": {
          const room = ws.roomname;
          if (!room || !roomList.includes(room)) return;
          await this.cleanupFromRoom(ws, room);
          await this.safeSend(ws, ["roomLeft", room]);
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            try {
              await this.lowcard.handleEvent(ws, data);
            } catch (error) {
              console.error("Game error:", error);
            }
          }
          break;
        }
        
        case "ping": {
          await this.safeSend(ws, ["pong", Date.now()]);
          break;
        }
        
        default: break;
      }
    } catch (error) {
      console.error("Error processing message:", error);
      throw error;
    }
  }
  
  async getMemoryStats() {
    const activeReal = this._activeClients.filter(c => c?.readyState === 1).length;
    const activeNull = this._activeClients.filter(c => c === null).length;
    
    let totalRoomClients = 0;
    let nullRoomClients = 0;
    for (const clients of this.roomClients.values()) {
      totalRoomClients += clients.length;
      nullRoomClients += clients.filter(c => c === null).length;
    }
    
    const roomStats = [];
    for (const [room, mgr] of this.roomManagers) {
      roomStats.push({
        room,
        occupiedSeats: mgr.getOccupiedCount(),
        totalSeats: CONSTANTS.MAX_SEATS,
        muteStatus: mgr.getMute()
      });
    }
    
    const memoryUsage = process.memoryUsage?.() || { heapUsed: 0, heapTotal: 0 };
    
    return {
      timestamp: Date.now(),
      uptime: Date.now() - this._startTime,
      memoryUsage: {
        heapUsedMB: (memoryUsage.heapUsed / 1024 / 1024).toFixed(2),
        heapTotalMB: (memoryUsage.heapTotal / 1024 / 1024).toFixed(2)
      },
      activeClients: {
        total: this._activeClients.length,
        real: activeReal,
        null: activeNull,
        wastePercent: activeNull ? ((activeNull / this._activeClients.length) * 100).toFixed(2) : 0
      },
      roomClients: {
        total: totalRoomClients,
        null: nullRoomClients,
        rooms: this.roomClients.size
      },
      clientsSet: this.clients.size,
      pendingPromises: this._pendingPromises.size,
      userConnections: this.userConnections.size,
      disconnectedTimers: this.disconnectedTimers.size,
      pendingReconnections: this._pendingReconnections.size,
      rateLimiterSize: this.rateLimiter.requests.size,
      userIPsSize: this.userIPs.size,
      ipConnectionCountSize: this._ipConnectionCount.size,
      roomManagersSize: this.roomManagers.size,
      userToSeatSize: this.userToSeat.size,
      userCurrentRoomSize: this.userCurrentRoom.size,
      userLastSeenSize: this.userLastSeen.size,
      roomsDetail: roomStats
    };
  }
  
  // FIX: Perbaikan shutdown - clear semua timer dan cleanup menyeluruh
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    console.log("[SHUTDOWN] Starting graceful shutdown...");
    
    // Clear NUMBER tick timer
    if (this.numberTickTimer) {
      clearTimeout(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    
    // Clear CLEANUP interval
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    // Clear PROMISE cleanup interval
    if (this._promiseCleanupInterval) {
      clearInterval(this._promiseCleanupInterval);
      this._promiseCleanupInterval = null;
    }
    
    // Clear MEMORY MONITOR interval
    if (this.memoryMonitor) {
      this.memoryMonitor.stop();
    }
    
    // Stop RATE LIMITER
    if (this.rateLimiter) {
      this.rateLimiter.stop();
    }
    
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try {
        await this.lowcard.destroy();
        console.log("[SHUTDOWN] Game manager destroyed");
      } catch(e) {
        console.error("[SHUTDOWN] Error destroying game manager:", e);
      }
    }
    this.lowcard = null;
    
    // Tutup semua koneksi WebSocket yang masih aktif
    for (const ws of this._activeClients) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          ws.close(1000, "Server shutdown");
        } catch(e) {}
      }
    }
    
    // FIX: Bersihkan semua timer di disconnectedTimers
    for (const [userId, timer] of this.disconnectedTimers) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
    this._pendingReconnections.clear();
    
    // Bersihkan semua event listeners
    for (const [ws, handlers] of this._wsEventListeners) {
      if (handlers) {
        if (handlers.message) ws.removeEventListener("message", handlers.message);
        if (handlers.error) ws.removeEventListener("error", handlers.error);
        if (handlers.close) ws.removeEventListener("close", handlers.close);
      }
    }
    
    // Destroy all room managers
    for (const roomManager of this.roomManagers.values()) {
      roomManager.destroy();
    }
    
    // Bersihkan semua Map dan Set
    this.clients.clear();
    this._activeClients = [];
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this.roomClients.clear();
    this.userLastSeen.clear();
    this.userIPs.clear();
    this._ipConnectionCount.clear();
    this._roomCountsCache.clear();
    this.roomManagers.clear();
    this._pendingPromises.clear();
    this._wsEventListeners.clear();
    
    console.log("[SHUTDOWN] Shutdown complete");
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          const activeCount = this._activeClients.filter(c => c && c.readyState === 1).length;
          const memoryUsage = process.memoryUsage?.() || { heapUsed: 0 };
          return new Response(JSON.stringify({ 
            status: "healthy", 
            connections: activeCount,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            memory: {
              heapUsedMB: (memoryUsage.heapUsed / 1024 / 1024).toFixed(2),
              activeClientsLength: this._activeClients.length,
              pendingPromises: this._pendingPromises.size,
              userConnections: this.userConnections.size
            }
          }), { 
            status: 200, 
            headers: { "content-type": "application/json" } 
          });
        }
        
        if (url.pathname === "/debug/memory") {
          const stats = await this.getMemoryStats();
          return new Response(JSON.stringify(stats, null, 2), {
            status: 200,
            headers: { "content-type": "application/json" }
          });
        }
        
        if (url.pathname === "/debug/roomcounts") {
          const counts = {};
          for (const room of roomList) {
            counts[room] = this.getRoomCount(room);
          }
          return new Response(JSON.stringify({
            counts: counts,
            total: Object.values(counts).reduce((a,b) => a + b, 0),
            activeClientsReal: this._activeClients.filter(c => c !== null && c.readyState === 1).length,
            activeClientsArrayLength: this._activeClients.length
          }), {
            headers: { "content-type": "application/json" }
          });
        }
        
        if (url.pathname === "/shutdown") {
          await this.shutdown();
          return new Response("Shutting down...", { status: 200 });
        }
        
        return new Response("ChatServer2 Running - Memory Only Direct Replace", { status: 200 });
      }
      
      const activeConnections = this._activeClients.filter(c => c && c.readyState === 1 && !c._isClosing).length;
      if (activeConnections > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      try {
        await server.accept();
      } catch (acceptError) {
        return new Response("WebSocket accept failed", { status: 500 });
      }
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      
      const cf = request.cf;
      if (cf?.colo) {
        ws._ip = request.headers.get("CF-Connecting-IP") || 
                 request.headers.get("X-Forwarded-For")?.split(",")[0] || 
                 "unknown";
      }
      
      this.clients.add(ws);
      this._activeClients.push(ws);
      
      // Buat event handlers dengan referensi yang bisa di-cleanup nanti
      const messageHandler = (ev) => {
        this.handleMessage(ws, ev.data).catch(() => {});
      };
      
      const errorHandler = () => {
        this.safeWebSocketCleanup(ws).catch(() => {});
      };
      
      const closeHandler = () => {
        this.safeWebSocketCleanup(ws).catch(() => {});
      };
      
      ws.addEventListener("message", messageHandler);
      ws.addEventListener("error", errorHandler);
      ws.addEventListener("close", closeHandler);
      
      // Simpan semua handler di Map untuk cleanup nanti
      this._wsEventListeners.set(ws, {
        message: messageHandler,
        error: errorHandler,
        close: closeHandler
      });
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// ==================== WORKER EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      const chatId = env.CHAT_SERVER.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER.get(chatId);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      
      if (url.pathname === "/health" || url.pathname === "/debug/memory" || url.pathname === "/debug/roomcounts" || url.pathname === "/shutdown") {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running - Memory Only Direct Replace", {
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      console.error("Worker error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
