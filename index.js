// index.js - ChatServer2 with LowCardGameManager - CLOUDFLARE WORKERS READY
// FIX: User online tidak akan dihapus meskipun diam
import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS UNTUK CLOUDFLARE WORKERS ====================
const CONSTANTS = Object.freeze({
  // MEMORY CONFIGURATION
  MAX_HEAP_SIZE_MB: 128,
  GC_INTERVAL_MS: 3 * 60 * 1000,
  
  // BUFFER CONFIGURATION
  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MAX_CHAT_BUFFER_SIZE: 20,
  MESSAGE_TTL_MS: 8000,
  BUFFER_FLUSH_INTERVAL_MS: 50,
  MAX_BUFFER_AGE_MS: 5000,
  BUFFER_ROOM_TTL_MS: 60000,
  
  // CONNECTION LIMITS - UNTUK 128 MB (AMAN)
  MAX_GLOBAL_CONNECTIONS: 150,        // Turun dari 250
  MAX_ACTIVE_CLIENTS_LIMIT: 120,      // Turun dari 200
  MAX_ROOM_CLIENTS_LIMIT: 80,         // Tetap
  MAX_USER_CONNECTIONS_SIZE: 180,     // Turun dari 250
  MAX_RATE_LIMITER_SIZE: 400,         // Turun dari 500
  MAX_CONNECTIONS_PER_USER: 1
  
 // CONNECTION LIMITS
MAX_GLOBAL_CONNECTIONS: 250,        // Naikkan dari 200 (buffer 50)
MAX_ACTIVE_CLIENTS_LIMIT: 200,      // Naikkan dari 100 → 200
MAX_ROOM_CLIENTS_LIMIT: 200,        // Naikkan dari 80 → 200
MAX_USER_CONNECTIONS_SIZE: 250,     // Naikkan dari 150 → 250
MAX_RATE_LIMITER_SIZE: 500,         // Naikkan dari 300 → 500
MAX_CONNECTIONS_PER_USER: 1,        // Tetap 1
  
  // SEATS
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  // RATE LIMIT
  MAX_RATE_LIMIT: 40,
  RATE_WINDOW: 60000,
  
  // MESSAGE LIMITS
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 200,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 40,
  
  // CLEANUP
  CLEANUP_INTERVAL: 10000,
  MAX_USER_IDLE: 2 * 60 * 1000, // Masih ada tapi tidak digunakan untuk hapus user online
  ROOM_MANAGER_IDLE_TIMEOUT: 3 * 60 * 1000,
  CLEANUP_BATCH_SIZE: 10,
  CLEANUP_DELAY_MS: 5,
  MAX_CLEANUP_DURATION_MS: 30,
  
  // TIMER
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  LOCK_TIMEOUT_MS: 2000,
  PROMISE_TIMEOUT_MS: 10000,
  MAX_TIMEOUT_MS: 5000,
  MAX_TIMER_MS: 2147483647,
  
  // LAINNYA
  MAX_JSON_DEPTH: 30,
  MAX_ARRAY_SIZE: 80,
  POINTS_CACHE_MS: 30,
  ROOM_IDLE_BEFORE_CLEANUP: 15 * 60 * 1000,
  
  // MEMORY LIMITS FOR CLEANUP
  MAX_USERS_BEFORE_CLEANUP: 100,
  MAX_SEATS_BEFORE_CLEANUP: 500,
  EMERGENCY_CLEANUP_INTERVAL_MS: 30000,
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

// ==================== UTILITY FUNCTIONS ====================
async function withTimeout(promise, timeoutMs, fallbackValue = null) {
  let timeoutId;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error('Operation timeout')), timeoutMs);
  });
  
  try {
    return await Promise.race([promise, timeoutPromise]);
  } catch (error) {
    return fallbackValue;
  } finally {
    clearTimeout(timeoutId);
  }
}

function safeStringify(obj, maxSize = CONSTANTS.MAX_MESSAGE_SIZE) {
  try {
    const seen = new WeakSet();
    const result = JSON.stringify(obj, (key, value) => {
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return '[Circular]';
        seen.add(value);
      }
      if (typeof value === 'string' && value.length > 1000) {
        return value.substring(0, 1000);
      }
      return value;
    });
    
    if (result && result.length > maxSize) {
      return result.substring(0, maxSize);
    }
    return result;
  } catch (e) {
    return JSON.stringify({ error: "Stringify failed" });
  }
}

function safeParseJSON(str, maxDepth = CONSTANTS.MAX_JSON_DEPTH) {
  if (!str || str.length > CONSTANTS.MAX_MESSAGE_SIZE) return null;
  
  try {
    const obj = JSON.parse(str);
    
    function checkDepth(o, depth = 0) {
      if (depth > maxDepth) return false;
      if (o && typeof o === 'object') {
        const values = Object.values(o);
        for (let i = 0; i < Math.min(values.length, 50); i++) {
          if (!checkDepth(values[i], depth + 1)) return false;
        }
      }
      return true;
    }
    
    return checkDepth(obj) ? obj : null;
  } catch {
    return null;
  }
}

// ==================== MEMORY MONITOR (CLOUDFLARE WORKERS VERSION) ====================
class MemoryMonitor {
  constructor() {
    this.lastCheck = Date.now();
    this.gcInterval = null;
    this._isDestroyed = false;
    this.lastGC = Date.now();
    this.GC_COOLDOWN = 20000;
    this.lastMemoryLog = Date.now();
  }
  
  start() {
    if (this.gcInterval || this._isDestroyed) return;
    this.gcInterval = setInterval(() => {
      try {
        this.check();
      } catch (e) {}
    }, CONSTANTS.GC_INTERVAL_MS);
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
      const now = Date.now();
      if (now - this.lastMemoryLog > 60000) {
        console.log(`[MEMORY] Cloudflare Workers - memory monitoring disabled`);
        this.lastMemoryLog = now;
      }
      return false;
    } catch(e) {
      return false;
    }
  }
}

// ==================== GLOBAL CHAT BUFFER (OPTIMIZED) ====================
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._flushTimer = null;
    this._isDestroyed = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.flushInterval = CONSTANTS.BUFFER_FLUSH_INTERVAL_MS;
    this.maxAge = CONSTANTS.MAX_BUFFER_AGE_MS;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._totalQueued = 0;
    this.lastWarningTime = 0;
    this.WARNING_THRESHOLD = 80;
    this.lastAgeWarningTime = 0;
  }
  
  setFlushCallback(callback) {
    this._flushCallback = callback;
  }
  
  _startTimer() {
    if (this._flushTimer) clearTimeout(this._flushTimer);
    if (this._isDestroyed) return;
    
    this._flushTimer = setTimeout(() => {
      this._flush();
    }, this.flushInterval);
  }
  
  add(room, message) {
    if (this._isDestroyed) {
      this._sendImmediate(room, message);
      return;
    }
    
    if (this._messageQueue.length >= this.maxQueueSize) {
      this._sendImmediate(room, message);
      return;
    }
    
    if (this._messageQueue.length > this.WARNING_THRESHOLD) {
      const now = Date.now();
      if (now - this.lastWarningTime > 60000) {
        console.warn(`[GlobalChatBuffer] Queue at ${this._messageQueue.length}/${this.maxQueueSize}`);
        this.lastWarningTime = now;
      }
    }
    
    const now = Date.now();
    
    const oldestMessage = this._messageQueue[0];
    if (oldestMessage && now - oldestMessage.timestamp > this.maxAge) {
      const nowTime = Date.now();
      if (nowTime - this.lastAgeWarningTime > 60000) {
        console.warn(`[GlobalChatBuffer] Old message detected: ${now - oldestMessage.timestamp}ms`);
        this.lastAgeWarningTime = nowTime;
      }
    }
    
    this._messageQueue.push({
      room,
      message,
      timestamp: now
    });
    this._totalQueued++;
    
    if (!this._flushTimer) {
      this._startTimer();
    }
  }
  
  _cleanupExpiredMessages() {
    if (this._isDestroyed) return;
    
    const now = Date.now();
    const expiredIndices = [];
    
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      if (now - this._messageQueue[i].timestamp > this.messageTTL) {
        expiredIndices.push(i);
      }
    }
    
    for (const idx of expiredIndices.reverse()) {
      this._messageQueue.splice(idx, 1);
      this._totalQueued--;
    }
    
    if (expiredIndices.length > 0) {
      console.log(`[GlobalChatBuffer] Cleaned ${expiredIndices.length} expired messages`);
    }
  }
  
  _flush() {
    if (this._flushTimer) {
      clearTimeout(this._flushTimer);
      this._flushTimer = null;
    }
    
    if (this._messageQueue.length === 0 || !this._flushCallback) return;
    
    this._cleanupExpiredMessages();
    
    if (this._messageQueue.length === 0) return;
    
    const roomGroups = {};
    const batch = [...this._messageQueue];
    this._messageQueue = [];
    this._totalQueued = 0;
    
    for (const item of batch) {
      if (!roomGroups[item.room]) {
        roomGroups[item.room] = [];
      }
      roomGroups[item.room].push(item.message);
    }
    
    let errorCount = 0;
    for (const room in roomGroups) {
      const msgs = roomGroups[room];
      for (const msg of msgs) {
        try {
          this._flushCallback(room, msg);
        } catch (e) {
          errorCount++;
          console.error(`[GlobalChatBuffer] Flush error for room ${room}:`, e);
        }
      }
    }
    
    if (errorCount > 0) {
      console.warn(`[GlobalChatBuffer] ${errorCount} messages failed to flush`);
    }
    
    if (this._messageQueue.length > 0 && !this._isDestroyed) {
      this._startTimer();
    }
  }
  
  _sendImmediate(room, message) {
    if (this._flushCallback) {
      try {
        this._flushCallback(room, message);
      } catch (e) {
        console.error(`[GlobalChatBuffer] Immediate send error:`, e);
      }
    }
  }
  
  async flushAll() {
    while (this._messageQueue.length > 0) {
      this._flush();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
    
    if (this._flushTimer) {
      clearTimeout(this._flushTimer);
      this._flushTimer = null;
    }
  }
  
  getStats() {
    const estimatedMemoryKB = Math.round(
      this._messageQueue.length * CONSTANTS.MAX_MESSAGE_SIZE / 1024
    );
    
    return {
      queuedMessages: this._messageQueue.length,
      totalQueued: this._totalQueued,
      estimatedMemoryKB: Math.min(estimatedMemoryKB, 800),
      maxQueueSize: this.maxQueueSize
    };
  }
  
  async destroy() {
    this._isDestroyed = true;
    
    const messages = [...this._messageQueue];
    this._messageQueue = [];
    this._totalQueued = 0;
    
    const roomGroups = {};
    for (const item of messages) {
      if (!roomGroups[item.room]) {
        roomGroups[item.room] = [];
      }
      roomGroups[item.room].push(item.message);
    }
    
    for (const room in roomGroups) {
      const msgs = roomGroups[room];
      for (const msg of msgs) {
        try {
          await this._flushCallback(room, msg);
        } catch (e) {
          console.error(`[GlobalChatBuffer] Destroy error:`, e);
        }
      }
    }
    
    if (this._flushTimer) {
      clearTimeout(this._flushTimer);
      this._flushTimer = null;
    }
    
    this._flushCallback = null;
  }
}

// ==================== RATE LIMITER ====================
class RateLimiter {
  constructor(windowMs = CONSTANTS.RATE_WINDOW, maxRequests = CONSTANTS.MAX_RATE_LIMIT) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this._isDestroyed = false;
    this._cleanupInterval = setInterval(() => {
      this.cleanup();
    }, 30000);
  }

  check(userId) {
    if (!userId || this._isDestroyed) return true;
    const now = Date.now();
    let data = this.requests.get(userId);
    
    if (!data) {
      if (this.requests.size >= CONSTANTS.MAX_RATE_LIMITER_SIZE) {
        this._forceCleanup();
      }
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
    const staleTimeout = this.windowMs;
    
    for (const [userId, data] of this.requests) {
      if (now - data.windowStart >= staleTimeout) {
        this.requests.delete(userId);
      }
    }
    
    if (this.requests.size > CONSTANTS.MAX_RATE_LIMITER_SIZE) {
      this._forceCleanup();
    }
  }
  
  _forceCleanup() {
    if (this.requests.size <= CONSTANTS.MAX_RATE_LIMITER_SIZE) return;
    
    const entries = [];
    for (const [userId, data] of this.requests) {
      entries.push({ userId, timestamp: data.windowStart });
    }
    
    entries.sort((a, b) => a.timestamp - b.timestamp);
    const keepCount = Math.floor(CONSTANTS.MAX_RATE_LIMITER_SIZE * 0.6);
    
    const toDelete = entries.slice(keepCount);
    for (const item of toDelete) {
      this.requests.delete(item.userId);
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
    if (other && typeof other === 'object') {
      this.noimageUrl = other.noimageUrl?.slice(0, 255);
      this.namauser = other.namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH);
      this.color = other.color?.slice(0, 50);
      this.itembawah = parseInt(other.itembawah);
      this.itematas = parseInt(other.itematas);
      this.vip = parseInt(other.vip);
      this.viptanda = parseInt(other.viptanda);
      if (other.lastPoint) {
        this.lastPoint = {
          x: other.lastPoint.x,
          y: other.lastPoint.y,
          fast: other.lastPoint.fast || false,
          timestamp: Date.now()
        };
      } else {
        this.lastPoint = null;
      }
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
    this.lastActivity = Date.now();
    this._cachedPoints = null;
    this._pointsCacheTime = 0;
    
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
      this.seats.set(i, new SeatData());
    }
  }
  
  updateActivity() {
    this.lastActivity = Date.now();
  }
  
  isIdle() {
    return Date.now() - this.lastActivity > CONSTANTS.ROOM_MANAGER_IDLE_TIMEOUT && this.getOccupiedCount() === 0;
  }
  
  replaceSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const seat = this.seats.get(seatNumber);
    if (seat) {
      seat.copyFrom(seatData);
      this.updateActivity();
      this._pointsCacheTime = 0;
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
      this.updateActivity();
      this._pointsCacheTime = 0;
      return true;
    }
    return false;
  }
  
  removeSeat(seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const seat = this.seats.get(seatNumber);
    if (seat) {
      seat.clear();
      this.updateActivity();
      this._pointsCacheTime = 0;
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
          noimageUrl: seat.noimageUrl,
          namauser: seat.namauser,
          color: seat.color,
          itembawah: seat.itembawah,
          itematas: seat.itematas,
          vip: seat.vip,
          viptanda: seat.viptanda
        };
      }
    }
    return meta;
  }
  
  getAllPoints() {
    if (this._cachedPoints && Date.now() - this._pointsCacheTime < CONSTANTS.POINTS_CACHE_MS) {
      return this._cachedPoints;
    }
    
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
    
    this._cachedPoints = points;
    this._pointsCacheTime = Date.now();
    return points;
  }
  
  setMute(isMuted) {
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    this.updateActivity();
    return this.muteStatus;
  }
  
  getMute() {
    return this.muteStatus;
  }
  
  setCurrentNumber(number) {
    this.currentNumber = number;
    this.updateActivity();
  }
  
  getCurrentNumber() {
    return this.currentNumber;
  }
  
  destroy() {
    for (const seat of this.seats.values()) {
      seat.clear();
    }
    this.seats.clear();
    this._cachedPoints = null;
  }
}

// ==================== MAIN CHATSERVER CLASS ====================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._isCleaningUp = false;
    this._cleaningUp = new Set();
    this._lastCleanupLog = null;
    this._lastValidation = Date.now();
    this._connectionLocks = new Set();
    
    this._activeClients = new Set();
    
    this.roomManagers = new Map();
    this.clients = new Set();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.userLastSeen = new Map();
    this.roomClients = new Map();
    this._activeListeners = new Map();
    this._clientWebSockets = new Set();
    
    this.rateLimiter = new RateLimiter();
    this._cleanupInterval = null;
    
    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg) => {
      this._sendDirectToRoom(room, msg);
    });
    
    try {
      this.lowcard = new LowCardGameManager(this);
      console.log('[GAME] LowCardGameManager initialized');
    } catch (error) {
      console.error('[GAME] Failed to initialize:', error);
      this.lowcard = null;
    }
    
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    this.intervalMillis = Math.min(CONSTANTS.NUMBER_TICK_INTERVAL, CONSTANTS.MAX_TIMER_MS);
    this.numberTickTimer = null;
    this._tickRunning = false;
    
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
    }
    
    this.memoryMonitor = new MemoryMonitor();
    
    this.startNumberTickTimer();
    this._startPeriodicCleanup();
    this.memoryMonitor.start();
    
    this._emergencyCleanupInterval = setInterval(() => {
      this._emergencyCleanup();
    }, CONSTANTS.EMERGENCY_CLEANUP_INTERVAL_MS);
  }
  
  _hasLiveConnection(userId) {
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return false;
    for (const conn of connections) {
      if (conn && conn.readyState === 1 && !conn._isClosing) {
        return true;
      }
    }
    return false;
  }
  
  _emergencyCleanup() {
    console.log(`[EMERGENCY] Running cleanup`);
    
    this.chatBuffer.flushAll().catch(() => {});
    
    const toCleanup = [];
    for (const [userId, connections] of this.userConnections) {
      let hasLiveConnection = false;
      for (const conn of connections) {
        if (conn && conn.readyState === 1 && !conn._isClosing) {
          hasLiveConnection = true;
          break;
        }
      }
      if (!hasLiveConnection) {
        toCleanup.push(userId);
      }
    }
    
    for (let i = 0; i < Math.min(toCleanup.length, 20); i++) {
      this.forceUserCleanup(toCleanup[i]).catch(() => {});
    }
    
    this._compressRoomClients();
    this._checkMemoryAndCleanup();
  }
  
  _checkMemoryAndCleanup() {
    const totalUsers = this.userConnections.size;
    const totalSeats = this._getTotalOccupiedSeats();
    
    if (totalUsers > CONSTANTS.MAX_USERS_BEFORE_CLEANUP || totalSeats > CONSTANTS.MAX_SEATS_BEFORE_CLEANUP) {
      console.log(`[MEMORY] High load: ${totalUsers} users, ${totalSeats} seats. Cleaning up...`);
      
      const usersWithNoConnection = [];
      for (const [userId, connections] of this.userConnections) {
        let hasLiveConnection = false;
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            hasLiveConnection = true;
            break;
          }
        }
        if (!hasLiveConnection) {
          usersWithNoConnection.push(userId);
        }
      }
      
      usersWithNoConnection.sort((a, b) => {
        return (this.userLastSeen.get(a) || 0) - (this.userLastSeen.get(b) || 0);
      });
      
      const toRemove = Math.floor(usersWithNoConnection.length * 0.5);
      for (let i = 0; i < toRemove; i++) {
        this.forceUserCleanup(usersWithNoConnection[i]).catch(() => {});
      }
    }
  }
  
  _getTotalOccupiedSeats() {
    let total = 0;
    for (const roomManager of this.roomManagers.values()) {
      total += roomManager.getOccupiedCount();
    }
    return total;
  }
  
  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      counts[room] = roomManager ? roomManager.getOccupiedCount() : 0;
    }
    return counts;
  }
  
  getAllRoomCountsArray() {
    return roomList.map(room => {
      const roomManager = this.roomManagers.get(room);
      return [room, roomManager ? roomManager.getOccupiedCount() : 0];
    });
  }
  
  updateRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    const roomManager = this.roomManagers.get(room);
    const count = roomManager.getOccupiedCount();
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  
  getRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    const roomManager = this.roomManagers.get(room);
    return roomManager.getOccupiedCount();
  }
  
  updateSeatDirect(room, seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    
    const oldSeat = roomManager.getSeat(seatNumber);
    const wasOccupied = oldSeat && oldSeat.namauser && oldSeat.namauser !== "";
    const isOccupied = seatData.namauser && seatData.namauser !== "";
    
    const success = roomManager.replaceSeat(seatNumber, seatData);
    
    if (success && wasOccupied !== isOccupied) {
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
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
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
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
        const emptySeat = new SeatData();
        emptySeat.namauser = "";
        emptySeat.vip = 0;
        emptySeat.noimageUrl = "";
        emptySeat.color = "";
        emptySeat.itembawah = 0;
        emptySeat.itematas = 0;
        emptySeat.viptanda = 0;
        emptySeat.lastUpdated = Date.now();
        
        if (roomManager.replaceSeat(seat, emptySeat.toJSON())) {
          this.userToSeat.set(userId, { room, seat });
          this.userCurrentRoom.set(userId, room);
          
          this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, userId]);
          this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
          return seat;
        }
      }
    }
    return null;
  }
  
  _addUserConnection(userId, ws) {
    if (!userId || !ws) return;
    
    const lockKey = `conn_${userId}`;
    if (this._connectionLocks?.has(lockKey)) return;
    this._connectionLocks.add(lockKey);
    
    try {
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
      
      if (this.userConnections.size > CONSTANTS.MAX_USER_CONNECTIONS_SIZE) {
        this._trimUserConnections();
      }
    } finally {
      this._connectionLocks.delete(lockKey);
    }
  }
  
  _trimUserConnections() {
    const entries = Array.from(this.userConnections.entries());
    entries.sort((a, b) => (a[1].size || 0) - (b[1].size || 0));
    const toDelete = entries.slice(0, Math.floor(entries.length * 0.15));
    for (const [userId] of toDelete) {
      this.forceUserCleanup(userId).catch(() => {});
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
  }
  
  _removeFromActiveClients(ws) {
    this._activeClients.delete(ws);
  }
  
  _addToActiveClients(ws) {
    this._activeClients.add(ws);
  }
  
  _addToRoomClients(ws, room) {
    if (!ws || !room) return;
    let clientArray = this.roomClients.get(room);
    if (!clientArray) {
      clientArray = [];
      this.roomClients.set(room, clientArray);
    }
    if (!clientArray.includes(ws)) {
      clientArray.push(ws);
    }
  }
  
  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    const clientArray = this.roomClients.get(room);
    if (clientArray) {
      const index = clientArray.indexOf(ws);
      if (index > -1) clientArray.splice(index, 1);
    }
  }
  
  _cleanupWebSocketListeners(ws) {
    const listeners = this._activeListeners.get(ws);
    if (listeners) {
      for (const { event, handler } of listeners) {
        try { ws.removeEventListener(event, handler); } catch(e) {}
      }
      this._activeListeners.delete(ws);
    }
    
    if (ws._abortController) {
      try { ws._abortController.abort(); } catch(e) {}
      ws._abortController = null;
    }
    
    const propsToDelete = [
      'roomname', 'idtarget', '_isClosing', '_connectionTime', 
      '_isCleaningUp', '_lastPing', '_pingTimeout', 'username', 
      'sessionId', '_reconnectAttempts', '_messageQueue',
      '_lastMessageTime', '_bytesReceived', '_bytesSent', '_abortController'
    ];
    
    for (const prop of propsToDelete) {
      try { delete ws[prop]; } catch(e) {
        try { ws[prop] = null; } catch(e2) {}
      }
    }
    
    if (typeof ws.readyState !== 'number') {
      try { ws.readyState = 3; } catch(e) {}
    }
  }
  
  _safeCloseWebSocket(ws, code = 1000, reason = "Normal closure") {
    if (!ws || ws.readyState !== 1) return;
    try {
      ws.close(code, reason);
    } catch(e) {}
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws._isClosing) return false;
    
    const state = ws.readyState;
    if (state !== 1) return false;
    
    try {
      const message = typeof msg === "string" ? msg : safeStringify(msg);
      if (message.length > CONSTANTS.MAX_MESSAGE_SIZE) return false;
      
      if (ws.readyState === 1 && !ws._isClosing) {
        ws.send(message);
        return true;
      }
      return false;
    } catch (error) {
      if (error.code === 'ERR_INVALID_STATE' || error.message?.includes('CLOSED')) {
        this.safeWebSocketCleanup(ws).catch(() => {});
      }
      return false;
    }
  }
  
  _sendDirectToRoom(room, msg) {
    let clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    
    const snapshot = clientArray.slice();
    const messageStr = safeStringify(msg);
    let sentCount = 0;
    
    for (let i = 0; i < snapshot.length; i++) {
      const client = snapshot[i];
      if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
        try {
          client.send(messageStr);
          sentCount++;
        } catch (e) {
          this.safeWebSocketCleanup(client).catch(() => {});
        }
      }
    }
    return sentCount;
  }
  
  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    if (msg[0] === "chat") {
      this.chatBuffer.add(room, msg);
      return this.getRoomCount(room);
    }
    
    return this._sendDirectToRoom(room, msg);
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
    } catch (error) {}
  }
  
  _validateUserId(userId) {
    if (!userId || typeof userId !== 'string') return false;
    if (userId.length > CONSTANTS.MAX_USERNAME_LENGTH) return false;
    if (/[^\w\s\-_.@]/i.test(userId)) return false;
    return true;
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
    
    return withTimeout(this._handleJoinRoomInternal(ws, room), CONSTANTS.PROMISE_TIMEOUT_MS, false);
  }
  
  async _handleJoinRoomInternal(ws, room) {
    try {
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      const currentRoomBeforeJoin = this.userCurrentRoom.get(ws.idtarget);
      
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const roomManager = this.roomManagers.get(room);
        const seatData = roomManager.getSeat(seatNum);
        
        if (seatData && seatData.namauser === ws.idtarget) {
          ws.roomname = room;
          this._addToRoomClients(ws, room);
          this._addUserConnection(ws.idtarget, ws);
          this.userCurrentRoom.set(ws.idtarget, room);
          
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
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
      this._addToRoomClients(ws, room);
      this._addUserConnection(ws.idtarget, ws);
      
      const roomManager = this.roomManagers.get(room);
      
      await this.sendAllStateTo(ws, room);
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      await this.safeSend(ws, ["roomUserCount", room, this.getRoomCount(room)]);
      
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
    } catch (error) {}
  }
  
  async safeWebSocketCleanup(ws) {
    if (!ws || this._cleaningUp.has(ws)) return;
    this._cleaningUp.add(ws);
    
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      ws._isClosing = true;
      this.clients.delete(ws);
      this._removeFromActiveClients(ws);
      this._clientWebSockets.delete(ws);
      
      if (userId) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo && seatInfo.room) {
          this.removeSeatDirect(seatInfo.room, seatInfo.seat);
          this.broadcastToRoom(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
          this.updateRoomCount(seatInfo.room);
        }
        
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this.userLastSeen.delete(userId);
        this._removeUserConnection(userId, ws);
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
      }
      
      this._cleanupWebSocketListeners(ws);
      this._safeCloseWebSocket(ws, 1000, "Normal closure");
      
    } catch (error) {} finally {
      this._cleaningUp.delete(ws);
    }
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
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    
    const scheduleNext = () => {
      if (this._isClosing) return;
      
      let interval = this.intervalMillis;
      if (interval > CONSTANTS.MAX_TIMER_MS) {
        interval = CONSTANTS.MAX_TIMER_MS;
      }
      
      this.numberTickTimer = setTimeout(async () => {
        if (this._isClosing) return;
        
        try {
          await this._safeTick();
        } catch (error) {} finally {
          scheduleNext();
        }
      }, interval);
    };
    
    scheduleNext();
  }
  
  async _safeTick() {
    if (this._tickRunning || this._isClosing) return;
    this._tickRunning = true;
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }
      
      const message = safeStringify(["currentNumber", this.currentNumber]);
      const notifiedUsers = new Set();
      const clients = Array.from(this._activeClients);
      
      for (let i = 0; i < clients.length; i++) {
        const client = clients[i];
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
  
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    if (!this._validateUserId(id)) {
      await this.safeSend(ws, ["error", "Invalid user ID"]);
      return;
    }
    
    try {
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
          this._cleanupWebSocketListeners(oldWs);
        }
      }
      
      if (baru === true) {
        ws.idtarget = id;
        ws.roomname = undefined;
        ws._isClosing = false;
        ws._connectionTime = Date.now();
        this._addUserConnection(id, ws);
        this._addToActiveClients(ws);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      this._addToActiveClients(ws);
      
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const roomManager = this.roomManagers.get(room);
          if (roomManager) {
            const seatData = roomManager.getSeat(seat);
            if (seatData && seatData.namauser === id) {
              ws.roomname = room;
              this._addToRoomClients(ws, room);
              this._addUserConnection(id, ws);
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
      
      this._addUserConnection(id, ws);
      await this.safeSend(ws, ["needJoinRoom"]);
    } catch (error) {
      console.error("Error in handleSetIdTarget2:", error);
      await this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }
  
  async _checkIsPrimary(username, ws) {
    const userConnections = this.userConnections.get(username);
    if (!userConnections || userConnections.size === 0) return true;
    
    let earliest = null;
    for (const conn of userConnections) {
      if (conn?.readyState === 1 && !conn._isClosing) {
        if (!earliest || (conn._connectionTime || 0) < (earliest._connectionTime || 0)) {
          earliest = conn;
        }
      }
    }
    return earliest === ws;
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
      this._safeCloseWebSocket(ws, 1009, "Message too large");
      return;
    }
    
    const data = safeParseJSON(messageStr);
    if (!data) {
      this._safeCloseWebSocket(ws, 1008, "Protocol error");
      return;
    }
    
    if (!Array.isArray(data) || data.length === 0) return;
    this._processMessage(ws, data, data[0]).catch(error => {});
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
          await this.handleSetIdTarget2(ws, data[1], data[2]); 
          break; 
        }
        
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          if (!this._validateUserId(idtarget)) break;
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
          if (!this._validateUserId(idt)) break;
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
          const clients = Array.from(this._activeClients);
          for (let i = 0; i < clients.length; i++) {
            const client = clients[i];
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
          
          const isPrimary = await this._checkIsPrimary(username, ws);
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
            noimageUrl: noimageUrl?.slice(0, 255),
            namauser: namauser,
            color: color,
            itembawah: itembawah,
            itematas: itematas,
            vip: vip,
            viptanda: viptanda,
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
            noimageUrl: noimageUrl,
            namauser: namauser,
            color: color,
            itembawah: itembawah,
            itematas: itematas,
            vip: vip,
            viptanda: viptanda
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
            } catch (error) {}
          }
          break;
        }
      }
    } catch (error) {}
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
      this.userConnections.delete(userId);
      
    } catch (error) {}
  }
  
  _compressRoomClients() {
    for (const [room, clients] of this.roomClients) {
      const filtered = [];
      for (let i = 0; i < clients.length; i++) {
        const ws = clients[i];
        if (ws !== null && ws.readyState === 1 && ws.roomname === room) {
          filtered.push(ws);
        }
      }
      
      if (filtered.length !== clients.length) {
        this.roomClients.set(room, filtered);
      }
    }
  }
  
  _cleanupUserLastSeen() {
    const now = Date.now();
    const expired = [];
    
    for (const [userId, lastSeen] of this.userLastSeen) {
      const hasLiveConnection = this._hasLiveConnection(userId);
      if (!hasLiveConnection) {
        expired.push(userId);
      }
    }
    
    for (let i = 0; i < expired.length; i++) {
      this.userLastSeen.delete(expired[i]);
    }
  }
  
  _cleanupEmptyRooms() {
    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      if (roomManager && roomManager.getOccupiedCount() === 0) {
        const idleTime = Date.now() - roomManager.lastActivity;
        if (idleTime > CONSTANTS.ROOM_IDLE_BEFORE_CLEANUP) {
          roomManager.destroy();
          this.roomManagers.delete(room);
          console.log(`[CLEANUP] Removed empty room: ${room}`);
          this.roomManagers.set(room, new RoomManager(room));
        }
      }
    }
  }
  
  _startPeriodicCleanup() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this._cleanupInterval = setInterval(() => {
      this._safePeriodicCleanup().catch(err => {});
    }, CONSTANTS.CLEANUP_INTERVAL);
  }
  
  async _safePeriodicCleanup() {
    if (this._isCleaningUp || this._isClosing) return;
    this._isCleaningUp = true;
    
    const startTime = Date.now();
    
    try {
      await this.chatBuffer.flushAll();
      
      if (Date.now() - startTime < CONSTANTS.MAX_CLEANUP_DURATION_MS) {
        this._mediumCleanup();
      }
      
      if (Date.now() - startTime < CONSTANTS.MAX_CLEANUP_DURATION_MS && 
          this._needsHeavyCleanup()) {
        await this._heavyCleanup();
      }
      
      const now = Date.now();
      if (!this._lastCleanupLog || now - this._lastCleanupLog > 3600000) {
        this._logCleanupStats();
        this._lastCleanupLog = now;
      }
      
    } catch (error) {} finally {
      this._isCleaningUp = false;
    }
  }
  
  _mediumCleanup() {
    for (const [userId, connections] of this.userConnections) {
      const alive = new Set();
      for (const conn of connections) {
        if (conn && conn.readyState === 1 && !conn._isClosing) {
          alive.add(conn);
        }
      }
      if (alive.size === 0) {
        this.userConnections.delete(userId);
        this.forceUserCleanup(userId).catch(() => {});
      } else if (alive.size !== connections.size) {
        this.userConnections.set(userId, alive);
      }
    }
    
    this._compressRoomClients();
    this._cleanupUserLastSeen();
    this._cleanupEmptyRooms();
    this.rateLimiter.cleanup();
    
    this.chatBuffer._cleanupExpiredMessages();
    
    const now = Date.now();
    const zombies = [];
    for (const ws of this._activeClients) {
      if (ws && ws.readyState !== 1) {
        zombies.push(ws);
      }
    }
    for (const ws of zombies) {
      this._removeFromActiveClients(ws);
    }
    
    if (zombies.length > 0) {
      console.log(`[CLEANUP] Removed ${zombies.length} zombie connections`);
    }
  }
  
  async _heavyCleanup() {
    const now = Date.now();
    const expiredUsers = [];
    
    for (const [userId, connections] of this.userConnections) {
      let hasLiveConnection = false;
      for (const conn of connections) {
        if (conn && conn.readyState === 1 && !conn._isClosing) {
          hasLiveConnection = true;
          break;
        }
      }
      if (!hasLiveConnection) {
        expiredUsers.push(userId);
      }
    }
    
    for (let i = 0; i < Math.min(expiredUsers.length, CONSTANTS.CLEANUP_BATCH_SIZE * 2); i++) {
      await this.forceUserCleanup(expiredUsers[i]);
    }
  }
  
  _needsHeavyCleanup() {
    const totalClients = this._activeClients.size;
    const totalUsers = this.userLastSeen.size;
    return totalClients > 100 || totalUsers > 200;
  }
  
  _logCleanupStats() {
    let activeReal = 0;
    for (const c of this._activeClients) {
      if (c?.readyState === 1) activeReal++;
    }
    const bufferStats = this.chatBuffer.getStats();
    console.log(`[STATS] Active: ${activeReal}/${this._activeClients.size}, ` +
                `Users: ${this.userConnections.size}, ` +
                `Buffer: ${bufferStats.queuedMessages}/${bufferStats.maxQueueSize} msgs, ` +
                `BufferMem: ${bufferStats.estimatedMemoryKB}KB`);
  }
  
  async getMemoryStats() {
    let activeReal = 0;
    for (const c of this._activeClients) {
      if (c?.readyState === 1) activeReal++;
    }
    let totalRoomClients = 0;
    for (const clients of this.roomClients.values()) {
      totalRoomClients += clients.length;
    }
    
    const bufferStats = this.chatBuffer.getStats();
    
    return {
      timestamp: Date.now(),
      uptime: Date.now() - this._startTime,
      memoryUsage: {
        heapUsedMB: "N/A (Cloudflare Workers)",
        heapTotalMB: "N/A (Cloudflare Workers)"
      },
      activeClients: {
        total: this._activeClients.size,
        real: activeReal,
        waste: this._activeClients.size - activeReal
      },
      roomClients: { total: totalRoomClients },
      clientsSet: this.clients.size,
      userConnections: this.userConnections.size,
      userToSeatSize: this.userToSeat.size,
      userCurrentRoomSize: this.userCurrentRoom.size,
      userLastSeenSize: this.userLastSeen.size,
      chatBuffer: bufferStats
    };
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    console.log("[SHUTDOWN] Starting graceful shutdown...");
    
    await this.chatBuffer.flushAll();
    
    if (this.numberTickTimer) {
      clearTimeout(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    if (this._emergencyCleanupInterval) {
      clearInterval(this._emergencyCleanupInterval);
      this._emergencyCleanupInterval = null;
    }
    
    this.memoryMonitor.stop();
    await this.chatBuffer.destroy();
    
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try {
        await this.lowcard.destroy();
      } catch(e) {}
    }
    this.lowcard = null;
    
    const clientsToClose = Array.from(this._activeClients);
    for (let i = 0; i < clientsToClose.length; i++) {
      const ws = clientsToClose[i];
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { 
          this._cleanupWebSocketListeners(ws);
          ws.close(1000, "Server shutdown"); 
        } catch(e) {}
      }
    }
    
    if (this.rateLimiter) {
      this.rateLimiter.destroy();
      this.rateLimiter = null;
    }
    
    for (const roomManager of this.roomManagers.values()) {
      roomManager.destroy();
    }
    
    this.roomManagers.clear();
    this.roomClients.clear();
    this.clients.clear();
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this.userLastSeen.clear();
    this._activeListeners.clear();
    this._clientWebSockets.clear();
    this._cleaningUp.clear();
    this._connectionLocks.clear();
    
    console.log("[SHUTDOWN] Shutdown complete");
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          for (const c of this._activeClients) {
            if (c && c.readyState === 1) activeCount++;
          }
          const bufferStats = this.chatBuffer.getStats();
          return new Response(JSON.stringify({ 
            status: "healthy", 
            connections: activeCount,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            memory: { heapUsedMB: "N/A (Cloudflare Workers)" },
            buffer: bufferStats
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
        
        if (url.pathname === "/debug/leak") {
          const stats = await this.getMemoryStats();
          const warnings = [];
          if (stats.activeClients.waste > 30) warnings.push(`High zombie connections: ${stats.activeClients.waste}`);
          if (stats.chatBuffer.queuedMessages > 80) warnings.push(`High buffered messages: ${stats.chatBuffer.queuedMessages}`);
          return new Response(JSON.stringify({ ...stats, warnings }, null, 2), {
            status: 200,
            headers: { "content-type": "application/json" }
          });
        }
        
        if (url.pathname === "/debug/gc") {
          return new Response("GC not available in Cloudflare Workers", { status: 200 });
        }
        
        if (url.pathname === "/shutdown") {
          await this.shutdown();
          return new Response("Shutting down...", { status: 200 });
        }
        
        return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200 });
      }
      
      const activeConnections = this._activeClients.size;
      if (activeConnections > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      const abortController = new AbortController();
      
      try { 
        await server.accept(); 
      } catch (acceptError) {
        if (abortController) abortController.abort();
        if (server) {
          this._cleanupWebSocketListeners(server);
          this.clients.delete(server);
          this._removeFromActiveClients(server);
        }
        return new Response("WebSocket accept failed", { status: 500 });
      }
      
      const ws = server;
      
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      ws._abortController = abortController;
      
      this.clients.add(ws);
      this._addToActiveClients(ws);
      this._clientWebSockets.add(client);
      
      const messageHandler = (ev) => { 
        this.handleMessage(ws, ev.data).catch(() => {});
      };
      const errorHandler = () => { 
        this.safeWebSocketCleanup(ws).catch(() => {}); 
      };
      const closeHandler = () => { 
        this.safeWebSocketCleanup(ws).catch(() => {}); 
      };
      
      ws.addEventListener("message", messageHandler, { signal: abortController.signal });
      ws.addEventListener("error", errorHandler, { signal: abortController.signal });
      ws.addEventListener("close", closeHandler, { signal: abortController.signal });
      
      this._activeListeners.set(ws, [
        { event: "message", handler: messageHandler },
        { event: "error", handler: errorHandler },
        { event: "close", handler: closeHandler }
      ]);
      
      client.addEventListener("close", () => {
        this._clientWebSockets.delete(client);
      });
      
      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER.get(chatId);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/debug/leak", "/debug/gc", "/shutdown"].includes(url.pathname)) {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running - Cloudflare Workers", {
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      console.error("Worker error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
