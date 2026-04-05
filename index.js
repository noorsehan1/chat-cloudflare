// index.js - ChatServer2 untuk Durable Object (FIXED MEMORY LEAKS - BEST VERSION)
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
  MAX_HEAP_SIZE_MB: 512,
  GC_INTERVAL_MS: 5 * 60 * 1000
});

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
    this._isDestroyed = false;
  }
  
  start() {
    if (this.gcInterval || this._isDestroyed) return;
    this.gcInterval = setInterval(() => this.check(), CONSTANTS.GC_INTERVAL_MS);
  }
  
  stop() {
    if (this.gcInterval) {
      clearInterval(this.gcInterval);
      this.gcInterval = null;
    }
    this._isDestroyed = true;
  }
  
  check() {
    if (this._isDestroyed) return false;
    try {
      if (global.gc) global.gc();
      const memoryUsage = process.memoryUsage?.() || { heapUsed: 0, heapTotal: 0 };
      const heapUsedMB = memoryUsage.heapUsed / 1024 / 1024;
      
      if (heapUsedMB > CONSTANTS.MAX_HEAP_SIZE_MB) {
        this.consecutiveHighMemory++;
        if (this.consecutiveHighMemory >= 3) {
          this.consecutiveHighMemory = 0;
          return true;
        }
      } else {
        this.consecutiveHighMemory = 0;
      }
    } catch(e) {}
    return false;
  }
}

// ==================== RATE LIMITER ====================
class RateLimiter {
  constructor(windowMs = CONSTANTS.RATE_WINDOW, maxRequests = CONSTANTS.MAX_RATE_LIMIT) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this._cleanupInterval = setInterval(() => this.cleanup(), 60000);
    this._isDestroyed = false;
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
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
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
      if (!seat.isEmpty()) occupied[seatNum] = seat.namauser;
    }
    return occupied;
  }
  
  getOccupiedCount() {
    let count = 0;
    for (const seat of this.seats.values()) {
      if (!seat.isEmpty()) count++;
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
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._lastCleanupLog = null;
    this._lastCountUpdate = Date.now();
    
    // Data structures
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
    this._wsEventListeners = new WeakMap();
    this._cleanupInterval = null;
    
    // Game manager
    try { 
      this.lowcard = new LowCardGameManager(this); 
    } catch (error) { 
      console.error("Failed to initialize game manager:", error);
      this.lowcard = null; 
    }
    
    // Number tick
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
    this.numberTickTimer = null;
    this._tickRunning = false;
    
    // Initialize rooms
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
      this._roomCountsCache.set(room, 0);
    }
    
    this.memoryMonitor = new MemoryMonitor();
    
    this.startNumberTickTimer();
    this._startPeriodicCleanup();
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
    if (cached !== undefined) return cached;
    const roomManager = this.roomManagers.get(room);
    const count = roomManager.getOccupiedCount();
    this._roomCountsCache.set(room, count);
    return count;
  }
  
  refreshRoomCounts() {
    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      this._roomCountsCache.set(room, roomManager.getOccupiedCount());
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
    if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;
    
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
      this._ipConnectionCount.set(ip, (this._ipConnectionCount.get(ip) || 0) + 1);
      this.userIPs.set(ip, (this.userIPs.get(ip) || 0) + 1);
    }
  }
  
  _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    
    const userConnections = this.userConnections.get(userId);
    if (userConnections) {
      userConnections.delete(ws);
      if (userConnections.size === 0) this.userConnections.delete(userId);
    }
    
    if (ws._ip) {
      const ipCount = this._ipConnectionCount.get(ws._ip) || 0;
      if (ipCount <= 1) this._ipConnectionCount.delete(ws._ip);
      else this._ipConnectionCount.set(ws._ip, ipCount - 1);
      
      const userIpCount = this.userIPs.get(ws._ip) || 0;
      if (userIpCount <= 1) this.userIPs.delete(ws._ip);
      else this.userIPs.set(ws._ip, userIpCount - 1);
    }
  }
  
  _removeFromActiveClients(ws) {
    const index = this._activeClients.indexOf(ws);
    if (index > -1) this._activeClients.splice(index, 1);
  }
  
  _compactActiveClients() {
    this._activeClients = this._activeClients.filter(ws => 
      ws !== null && ws.readyState === 1 && !ws._isClosing
    );
  }
  
  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    const clientArray = this.roomClients.get(room);
    if (clientArray) {
      const index = clientArray.indexOf(ws);
      if (index > -1) clientArray.splice(index, 1);
    }
  }
  
  async safeSend(ws, msg) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      if (message.length > CONSTANTS.MAX_MESSAGE_SIZE) return false;
      ws.send(message);
      if (ws.idtarget) this.userLastSeen.set(ws.idtarget, Date.now());
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
    
    for (let i = clientArray.length - 1; i >= 0; i--) {
      const client = clientArray[i];
      if (client && client.readyState === 1 && client.roomname === room && !client._isClosing) {
        try { 
          client.send(message); 
          sentCount++; 
        } catch (e) {
          clientArray.splice(i, 1);
        }
      } else if (client) {
        clientArray.splice(i, 1);
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
        filteredMeta = {};
        for (const [seat, data] of Object.entries(allKursiMeta)) {
          if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
        }
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
      if (selfSeat) await this.safeSend(ws, ["numberKursiSaya", selfSeat]);
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
          if (!clientArray.includes(ws)) clientArray.push(ws);
          
          this._addUserConnection(ws.idtarget, ws);
          this.userCurrentRoom.set(ws.idtarget, room);
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
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
      
      if (this.getRoomCount(room) >= CONSTANTS.MAX_SEATS) {
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
      if (!clientArray.includes(ws)) clientArray.push(ws);
      
      this._addUserConnection(ws.idtarget, ws);
      await this.sendAllStateTo(ws, room);
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
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
      this._periodicCleanup().catch(err => console.error("Periodic cleanup error:", err));
    }, CONSTANTS.CLEANUP_INTERVAL);
  }
  
  scheduleCleanup(userId) {
    if (!userId) return;
    this.cancelCleanup(userId);
    
    const timerId = setTimeout(async () => {
      try {
        this.disconnectedTimers.delete(userId);
        this._pendingReconnections.delete(userId);
        if (!(await this.isUserStillConnected(userId))) {
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
      
      // Cleanup from roomClients
      for (const [room, clientArray] of this.roomClients) {
        if (clientArray?.length > 0) {
          for (let i = clientArray.length - 1; i >= 0; i--) {
            if (clientArray[i]?.idtarget === userId) clientArray.splice(i, 1);
          }
        }
      }
      
      // Cleanup from activeClients
      for (let i = this._activeClients.length - 1; i >= 0; i--) {
        if (this._activeClients[i]?.idtarget === userId) this._activeClients.splice(i, 1);
      }
      
      this.userConnections.delete(userId);
    } catch (error) {
      console.error("Error in forceUserCleanup:", error);
    }
  }
  
  async safeWebSocketCleanup(ws) {
  if (!ws) return;
  
  const userId = ws.idtarget;
  const room = ws.roomname;
  
  try {
    ws._isClosing = true;
    this.clients.delete(ws);
    this._removeFromActiveClients(ws);
    
    if (userId) {
      this._removeUserConnection(userId, ws);
      
      if (!(await this.isUserStillConnected(userId))) {
        this.cancelCleanup(userId);
        
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo && seatInfo.room === room) {
          // ✅ TAMBAHKAN INI - Update cache saat disconnect
          const currentCount = this._roomCountsCache.get(room) || 0;
          const newCount = Math.max(0, currentCount - 1);
          this._roomCountsCache.set(room, newCount);
          this.broadcastToRoom(room, ["roomUserCount", room, newCount]);
          
          this._pendingReconnections.set(userId, {
            seatInfo: seatInfo,
            currentRoom: room,
            timestamp: Date.now()
          });
        }
        
        const timerId = setTimeout(async () => {
          try {
            this.disconnectedTimers.delete(userId);
            this._pendingReconnections.delete(userId);
            if (!(await this.isUserStillConnected(userId))) {
              await this.forceUserCleanup(userId);
            }
          } catch (error) {
            console.error("Error in cleanup timer:", error);
          }
        }, CONSTANTS.GRACE_PERIOD);
        
        timerId._scheduledTime = Date.now();
        this.disconnectedTimers.set(userId, timerId);
      }
    }
    
    if (room) this._removeFromRoomClients(ws, room);
    if (ws.readyState === 1) {
      try { ws.close(1000, "Normal closure"); } catch (e) {}
    }
    
    const listeners = this._wsEventListeners.get(ws);
    if (listeners) {
      listeners.forEach(({ event, handler }) => {
        try { ws.removeEventListener(event, handler); } catch(e) {}
      });
      this._wsEventListeners.delete(ws);
    }
    
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
      // Compact active clients if needed
      if (this._activeClients.length > 500) this._compactActiveClients();
      
      // Limit array size if still too large
      if (this._activeClients.length > CONSTANTS.MAX_ACTIVE_CLIENTS_HISTORY) {
        this._activeClients = this._activeClients.slice(-CONSTANTS.MAX_ACTIVE_CLIENTS_HISTORY);
      }
      
      // Cleanup room clients
      for (const [room, clients] of this.roomClients) {
        const filtered = [];
        for (let i = 0; i < clients.length; i++) {
          const c = clients[i];
          if (c !== null && c.readyState === 1 && !c._isClosing) {
            filtered.push(c);
          }
        }
        if (filtered.length !== clients.length) {
          this.roomClients.set(room, filtered);
        }
      }
      
      // Cleanup clients set
      for (const ws of this.clients) {
        if (!ws || ws.readyState !== 1 || ws._isClosing) {
          this.clients.delete(ws);
        }
      }
      
      // Cleanup user connections
      for (const [userId, connections] of this.userConnections) {
        const alive = new Set();
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            alive.add(conn);
          }
        }
        if (alive.size === 0) this.userConnections.delete(userId);
        else if (alive.size !== connections.size) this.userConnections.set(userId, alive);
      }
      
      // Cleanup disconnected timers
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer._scheduledTime && (now - timer._scheduledTime) > CONSTANTS.GRACE_PERIOD + 5000) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
          this._pendingReconnections.delete(userId);
          await this.forceUserCleanup(userId);
        }
      }
      
      // Cleanup pending reconnections
      for (const [userId, data] of this._pendingReconnections) {
        if (data.timestamp && (now - data.timestamp) > CONSTANTS.GRACE_PERIOD * 2) {
          this._pendingReconnections.delete(userId);
        }
      }
      
      // Cleanup idle users
      for (const [userId, lastSeen] of this.userLastSeen) {
        if (now - lastSeen > CONSTANTS.MAX_USER_IDLE) {
          await this.forceUserCleanup(userId);
        }
      }
      
      // Limit userIPs size
      if (this.userIPs.size > 10000) {
        const entries = Array.from(this.userIPs.entries());
        entries.sort((a, b) => a[1] - b[1]);
        const toDelete = entries.slice(0, Math.floor(entries.length * 0.3));
        for (const [ip] of toDelete) this.userIPs.delete(ip);
      }
      
      // Limit ipConnectionCount size
      if (this._ipConnectionCount.size > 5000) {
        const entries = Array.from(this._ipConnectionCount.entries());
        entries.sort((a, b) => a[1] - b[1]);
        const toDelete = entries.slice(0, Math.floor(entries.length * 0.3));
        for (const [ip] of toDelete) this._ipConnectionCount.delete(ip);
      }
      
      // Refresh counts periodically
      if (now - this._lastCountUpdate > 30000) {
        this.refreshRoomCounts();
        this._lastCountUpdate = now;
      }
      
      this.rateLimiter.cleanup();
      
      // Check memory pressure
      if (this.memoryMonitor.check()) {
        await this.forceGlobalCleanup();
      }
      
      // Log stats periodically
      if (!this._lastCleanupLog || now - this._lastCleanupLog > 3600000) {
        const activeReal = this._activeClients.filter(c => c?.readyState === 1).length;
        console.log(`[MEMORY] Active: ${activeReal}/${this._activeClients.length}, Users: ${this.userConnections.size}, Timers: ${this.disconnectedTimers.size}`);
        this._lastCleanupLog = now;
      }
    } catch (error) {
      console.error("Periodic cleanup error:", error);
    }
  }
  
  async forceGlobalCleanup() {
    console.log("[CLEANUP] Forcing global cleanup due to high memory");
    
    for (const [userId, timer] of this.disconnectedTimers) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
    this._pendingReconnections.clear();
    
    const clientsToNotify = [...this._activeClients];
    for (const ws of clientsToNotify) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          await this.safeSend(ws, ["serverBusy", "Server is busy, please reconnect"]);
        } catch(e) {}
      }
    }
    
    this.refreshRoomCounts();
    if (global.gc) global.gc();
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
    return this.roomManagers.get(roomName).getMute();
  }
  
  startNumberTickTimer() {
    if (this.numberTickTimer) {
      clearInterval(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    
    this.numberTickTimer = setInterval(async () => {
      if (this._isClosing) return;
      try {
        await this._safeTick();
      } catch (error) {
        console.error("Error in tick:", error);
      }
    }, this.intervalMillis);
    
    console.log(`[TICK] Timer started with interval: ${this.intervalMillis}ms`);
  }
  
  async _safeTick() {
    if (this._tickRunning || this._isClosing) return;
    this._tickRunning = true;
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      let sentCount = 0;
      
      for (const client of this.clients) {
        if (client?.readyState === 1 && !client._isClosing && client.roomname) {
          try {
            client.send(message);
            sentCount++;
          } catch (e) {}
        }
      }
      
      if (sentCount > 0) {
        console.log(`[TICK] Number ${this.currentNumber} sent to ${sentCount} clients`);
      }
    } finally {
      this._tickRunning = false;
    }
  }
  
 async handleSetIdTarget2(ws, id, baru, ip = null) {
    if (!id || !ws) return;
    try {
      // ✅ IP BLOCK DIHAPUS - hanya mencatat untuk statistik
      if (ip) {
        // Catat IP tanpa batasan (hanya untuk monitoring)
        this.userIPs.set(ip, (this.userIPs.get(ip) || 0) + 1);
        this._ipConnectionCount.set(ip, (this._ipConnectionCount.get(ip) || 0) + 1);
      }
      
      // ✅ USER BLOCK TETAP AKTIF (1 koneksi per user)
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
          if (oldWs.roomname) this._removeFromRoomClients(oldWs, oldWs.roomname);
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
      
      // Check pending reconnection
      const pendingData = this._pendingReconnections.get(id);
      if (pendingData && pendingData.seatInfo) {
        const { room, seat } = pendingData.seatInfo;
        const roomManager = this.roomManagers.get(room);
        if (roomManager && seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const seatData = roomManager.getSeat(seat);
          if (seatData && seatData.namauser === id) {
            ws.roomname = room;
            const clientArray = this.roomClients.get(room);
            if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
            this._addUserConnection(id, ws, ip);
            this.userToSeat.set(id, { room, seat });
            this.userCurrentRoom.set(id, room);
            await this.sendAllStateTo(ws, room);
            if (seatData.lastPoint) {
              await this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast ? 1 : 0]);
            }
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            await this.safeSend(ws, ["currentNumber", this.currentNumber]);
            this._pendingReconnections.delete(id);
            return;
          }
        }
      }
      
      // Check existing seat
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
              if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
              this._addUserConnection(id, ws, ip);
              await this.sendAllStateTo(ws, room);
              if (seatData.lastPoint) {
                await this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast ? 1 : 0]);
              }
              await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
              await this.safeSend(ws, ["numberKursiSaya", seat]);
              await this.safeSend(ws, ["currentNumber", this.currentNumber]);
              return;
            }
          }
        }
        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
        if (seatInfo.room) await this.forceUserCleanup(id);
      }
      
      this._addUserConnection(id, ws, ip);
      await this.safeSend(ws, ["needJoinRoom"]);
    } catch (error) {
      console.error("Error in handleSetIdTarget2:", error);
      await this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }
  
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
    
    if (messageStr.includes('__proto__') || messageStr.includes('constructor')) return;
    
    let data;
    try {
      data = JSON.parse(messageStr);
    } catch (e) {
      try { ws.close(1008, "Protocol error"); } catch {}
      return;
    }
    
    if (!Array.isArray(data) || data.length === 0) return;
    this._processMessage(ws, data, data[0]).catch(error => {
      console.error("Message processing error:", error);
    });
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
          await this.safeSend(ws, ["inRoomStatus", this.userCurrentRoom.get(idtarget) !== undefined]);
          break;
        }
        
        case "rollangak": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName)) {
            this.broadcastToRoom(roomName, ["rollangakBroadcast", roomName, data[2], data[3]]);
          }
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
          if (roomName && roomList.includes(roomName)) {
            const success = this.setRoomMute(roomName, isMuted);
            await this.safeSend(ws, ["muteTypeSet", !!isMuted, success, roomName]);
          }
          break;
        }
        
        case "getMuteType": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName)) {
            const roomManager = this.roomManagers.get(roomName);
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), roomName]);
          }
          break;
        }
        
        case "onDestroy": { 
          await this.safeWebSocketCleanup(ws); 
          break; 
        }
        
        case "setIdTarget2": {
          await this.handleSetIdTarget2(ws, data[1], data[2], ws._ip || null); 
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
          const out = ["private", idt, url, msg?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "", Date.now(), sender?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || ""];
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
          const username = data[1];
          const isOnline = await this.isUserStillConnected(username);
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] ?? ""]);
          break;
        }
        
        case "getAllRoomsUserCount": {
          await this.safeSend(ws, ["allRoomsUserCount", this.getAllRoomCountsArray()]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = data[1];
          if (roomList.includes(roomName)) {
            await this.safeSend(ws, ["roomUserCount", roomName, this.getRoomCount(roomName)]);
          }
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
          
          // Check primary connection
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
          
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, sanitizedUsername, sanitizedMessage, usernameColor, chatTextColor]);
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat) return;
          
          if (this.updatePointDirect(room, seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 || fast === true })) {
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
          
          if (this.removeSeatDirect(room, seat)) {
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
          
          if (!this.updateSeatDirect(room, seat, updatedSeat)) {
            await this.safeSend(ws, ["error", "Failed to update seat"]);
            return;
          }
          
          if (namauser === ws.idtarget) {
            this.userToSeat.set(namauser, { room, seat });
            this.userCurrentRoom.set(namauser, room);
          }
          
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
            noimageUrl: noimageUrl || "",
            namauser: namauser || "",
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0
          }]]]);
          this.updateRoomCount(room);
          break;
        }
        
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (ws.roomname !== roomname || ws.idtarget !== sender) return;
          if (!roomList.includes(roomname)) return;
          
          const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, safeGiftName, Date.now()]);
          break;
        }
        
        case "leaveRoom": {
          const room = ws.roomname;
          if (room && roomList.includes(room)) {
            await this.cleanupFromRoom(ws, room);
            await this.safeSend(ws, ["roomLeft", room]);
          }
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
      }
    } catch (error) {
      console.error("Error processing message:", error);
    }
  }
  
  async getMemoryStats() {
    const activeReal = this._activeClients.filter(c => c?.readyState === 1).length;
    let totalRoomClients = 0;
    for (const clients of this.roomClients.values()) {
      totalRoomClients += clients.length;
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
        waste: this._activeClients.length - activeReal
      },
      roomClients: { total: totalRoomClients },
      clientsSet: this.clients.size,
      userConnections: this.userConnections.size,
      disconnectedTimers: this.disconnectedTimers.size,
      pendingReconnections: this._pendingReconnections.size,
      rateLimiterSize: this.rateLimiter.requests?.size || 0,
      userIPsSize: this.userIPs.size,
      ipConnectionCountSize: this._ipConnectionCount.size,
      userToSeatSize: this.userToSeat.size,
      userCurrentRoomSize: this.userCurrentRoom.size,
      userLastSeenSize: this.userLastSeen.size
    };
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    console.log("[SHUTDOWN] Starting graceful shutdown...");
    
    if (this.numberTickTimer) {
      clearInterval(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    this.memoryMonitor.stop();
    
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try {
        await this.lowcard.destroy();
      } catch(e) {}
    }
    this.lowcard = null;
    
    const clientsToClose = [...this._activeClients];
    for (const ws of clientsToClose) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { ws.close(1000, "Server shutdown"); } catch(e) {}
      }
    }
    
    for (const timer of this.disconnectedTimers.values()) {
      clearTimeout(timer);
    }
    
    if (this.rateLimiter) this.rateLimiter.destroy();
    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    
    // Clear all collections
    this.clients.clear();
    this._activeClients = [];
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this.roomClients.clear();
    this.disconnectedTimers.clear();
    this._pendingReconnections.clear();
    this.userLastSeen.clear();
    this.userIPs.clear();
    this._ipConnectionCount.clear();
    this._roomCountsCache.clear();
    this.roomManagers.clear();
    this._wsEventListeners = null;
    
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
            memory: { heapUsedMB: (memoryUsage.heapUsed / 1024 / 1024).toFixed(2) }
          }), { status: 200, headers: { "content-type": "application/json" } });
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
          for (const room of roomList) counts[room] = this.getRoomCount(room);
          return new Response(JSON.stringify({
            counts: counts,
            total: Object.values(counts).reduce((a,b) => a + b, 0)
          }), { headers: { "content-type": "application/json" } });
        }
        
        if (url.pathname === "/shutdown") {
          await this.shutdown();
          return new Response("Shutting down...", { status: 200 });
        }
        
        return new Response("ChatServer2 Running - Memory Leak Fixed", { status: 200 });
      }
      
      const activeConnections = this._activeClients.filter(c => c && c.readyState === 1 && !c._isClosing).length;
      if (activeConnections > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      try { await server.accept(); } catch (acceptError) {
        return new Response("WebSocket accept failed", { status: 500 });
      }
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      
      ws._ip = request.headers.get("CF-Connecting-IP") || 
               request.headers.get("X-Forwarded-For")?.split(",")[0] || 
               "unknown";
      
      this.clients.add(ws);
      this._activeClients.push(ws);
      
      const messageHandler = (ev) => { this.handleMessage(ws, ev.data).catch(() => {}); };
      const errorHandler = () => { this.safeWebSocketCleanup(ws).catch(() => {}); };
      const closeHandler = () => { this.safeWebSocketCleanup(ws).catch(() => {}); };
      
      ws.addEventListener("message", messageHandler);
      ws.addEventListener("error", errorHandler);
      ws.addEventListener("close", closeHandler);
      
      this._wsEventListeners.set(ws, [
        { event: "message", handler: messageHandler },
        { event: "error", handler: errorHandler },
        { event: "close", handler: closeHandler }
      ]);
      
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
      const chatId = env.CHAT_SERVER.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER.get(chatId);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/shutdown"].includes(url.pathname)) {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running - Memory Leak Fixed", {
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      console.error("Worker error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
