// index.js - ChatServer2 with LowCardGameManager - CLOUDFLARE WORKERS READY
// OPTIMIZED MEMORY - Target di bawah 128 MB
// NO RATE LIMIT - No connection limits
// FIXED: Pemisahan Map seats dan points (tidak bercampur)
// FIXED: User keluar → hapus tag kursi dan point
// FIXED: User masuk → buat tag kursi baru
// FIXED: Room count = jumlah kursi di Map seats

import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = Object.freeze({
  // MEMORY CONFIGURATION - OPTIMIZED
  MAX_HEAP_SIZE_MB: 128,
  GC_INTERVAL_MS: 3 * 60 * 1000,
  
  // BUFFER CONFIGURATION - FLUSH 1 DETIK
  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MAX_CHAT_BUFFER_SIZE: 10,
  MESSAGE_TTL_MS: 5000,
  MAX_BUFFER_AGE_MS: 5000,
  BUFFER_CLEANUP_INTERVAL_MS: 1000,
  
  // CONNECTION LIMITS - DIKURANGI
  MAX_GLOBAL_CONNECTIONS: 500,
  MAX_ACTIVE_CLIENTS_LIMIT: 500,
  MAX_ROOM_CLIENTS_LIMIT: 300,
  MAX_USER_CONNECTIONS_SIZE: 500,
  MAX_CONNECTIONS_PER_USER: 1,
  
  // MEMORY LIMITS
  MAX_ACTIVE_CLIENTS_ABS: 500,
  MAX_USER_MAPS_SIZE: 1000,
  MAX_CLEANUP_BATCH: 50,
  FORCE_CLEANUP_INTERVAL: 3 * 60 * 1000,
  
  // SEATS
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  // MESSAGE LIMITS
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 200,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 40,
  
  // CLEANUP
  CLEANUP_INTERVAL: 5000,
  MAX_USER_IDLE: 2 * 60 * 1000,
  ROOM_MANAGER_IDLE_TIMEOUT: 3 * 60 * 1000,
  CLEANUP_BATCH_SIZE: 10,
  CLEANUP_DELAY_MS: 5,
  MAX_CLEANUP_DURATION_MS: 30,
  
  // WEBSOCKET CLEANUP
  WS_CLEANUP_INTERVAL_MS: 30000,
  
  // TIMER
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  MASTER_TICK_INTERVAL_MS: 1000,
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
  MAX_SEATS_BEFORE_CLEANUP: 300,
  EMERGENCY_CLEANUP_INTERVAL_MS: 15000,
  
  // TIMER SAFETY
  MASTER_TICK_TIMEOUT_MS: 3000,
  NUMBER_TICK_TIMEOUT_MS: 5000,
  MAX_CONSECUTIVE_ERRORS: 5,
  HEALTH_CHECK_INTERVAL: 60000,
  NUMBER_TICK_DEAD_THRESHOLD_MS: 180000,
  MASTER_TICK_STUCK_THRESHOLD_MS: 10000,
  
  // FORCE CLEANUP THRESHOLDS
  FORCE_CLEANUP_CONNECTIONS: 400,
  FORCE_CLEANUP_GAMES: 8,
  FORCE_CLEANUP_BUFFER: 80,
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

function isValidUsername(username) {
  if (!username || typeof username !== 'string') return false;
  if (username.trim().length === 0) return false;
  if (username.length > CONSTANTS.MAX_USERNAME_LENGTH) return false;
  if (/[^\w\s\-_.@]/i.test(username)) return false;
  return true;
}

// ==================== MEMORY MONITOR ====================
class MemoryMonitor {
  constructor(chatServer) {
    this.chatServer = chatServer;
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
        if (this.chatServer && this.chatServer._activeClients) {
          const activeCount = this.chatServer._activeClients.size;
          if (activeCount > CONSTANTS.FORCE_CLEANUP_CONNECTIONS) {
            this.chatServer._emergencyCleanup();
          }
        }
        this.lastMemoryLog = now;
      }
      return false;
    } catch(e) {
      return false;
    }
  }
}

// ==================== GLOBAL CHAT BUFFER ====================
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._retryQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._totalQueued = 0;
    this.lastWarningTime = 0;
    this.WARNING_THRESHOLD = 40;
    
    this._lastCleanupTime = Date.now();
    this._cleanupIntervalMs = CONSTANTS.BUFFER_CLEANUP_INTERVAL_MS;
    
    this._pendingMessages = new Map();
    this._nextMsgId = 0;
    
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = 20;
  }
  
  setFlushCallback(callback) {
    this._flushCallback = callback;
  }
  
  _generateMsgId() {
    return `${Date.now()}_${++this._nextMsgId}_${Math.random().toString(36).substr(2, 6)}`;
  }
  
  add(room, message) {
    if (this._isDestroyed) {
      this._sendImmediate(room, message);
      return;
    }
    
    let roomSize = this._roomQueueSizes.get(room) || 0;
    if (roomSize >= this.MAX_PER_ROOM) {
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
        this.lastWarningTime = now;
      }
    }
    
    const msgId = this._generateMsgId();
    this._messageQueue.push({
      room,
      message,
      msgId,
      timestamp: Date.now()
    });
    this._totalQueued++;
    this._roomQueueSizes.set(room, roomSize + 1);
    
    return msgId;
  }
  
  tick(now) {
    if (this._isDestroyed) return;
    
    if (now - this._lastCleanupTime >= this._cleanupIntervalMs) {
      this._cleanupExpiredMessages(now);
      this._processRetryQueue(now);
      this._cleanupPendingAcks(now);
      this._lastCleanupTime = now;
    }
    
    this._flush();
  }
  
  _cleanupExpiredMessages(now) {
    if (this._isDestroyed) return;
    
    const expiredIndices = [];
    
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      const msgAge = now - this._messageQueue[i].timestamp;
      if (msgAge > this.messageTTL + 1000) {
        expiredIndices.push(i);
      }
    }
    
    for (const idx of expiredIndices.reverse()) {
      const item = this._messageQueue[idx];
      if (item) {
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
      }
      this._messageQueue.splice(idx, 1);
      this._totalQueued--;
    }
    
    if (this._messageQueue.length > this.maxQueueSize * 0.8) {
      const toRemove = Math.floor(this._messageQueue.length * 0.3);
      for (let i = 0; i < toRemove; i++) {
        const item = this._messageQueue[i];
        if (item) {
          const roomSize = this._roomQueueSizes.get(item.room) || 0;
          this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
        }
      }
      this._messageQueue.splice(0, toRemove);
    }
  }
  
  _processRetryQueue(now) {
    const toRetry = this._retryQueue.filter(item => now >= item.nextRetry);
    for (const item of toRetry) {
      if (item.retries >= 3) {
        continue;
      }
      
      const sent = this._sendWithCallback(item.room, item.message, item.msgId);
      if (!sent) {
        item.retries++;
        item.nextRetry = now + (1000 * Math.pow(2, item.retries));
        this._retryQueue.push(item);
      }
    }
    
    this._retryQueue = this._retryQueue.filter(item => now < item.nextRetry);
  }
  
  _cleanupPendingAcks(now) {
    for (const [msgId, pending] of this._pendingMessages) {
      if (now - pending.timestamp > 6000) {
        this._pendingMessages.delete(msgId);
      }
    }
  }
  
  _sendWithCallback(room, message, msgId) {
    if (!this._flushCallback) return false;
    try {
      this._flushCallback(room, message, msgId);
      return true;
    } catch (e) {
      return false;
    }
  }
  
  _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback) return;
    if (this._isFlushing) return;
    
    this._isFlushing = true;
    
    try {
      const roomGroups = {};
      const batch = [...this._messageQueue];
      this._messageQueue = [];
      this._totalQueued = 0;
      
      for (const item of batch) {
        if (!roomGroups[item.room]) {
          roomGroups[item.room] = [];
        }
        roomGroups[item.room].push({ message: item.message, msgId: item.msgId });
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
      }
      
      let errorCount = 0;
      for (const room in roomGroups) {
        const items = roomGroups[room];
        for (const item of items) {
          try {
            this._flushCallback(room, item.message, item.msgId);
          } catch (e) {
            errorCount++;
            this._retryQueue.push({
              room,
              message: item.message,
              msgId: item.msgId,
              retries: 0,
              nextRetry: Date.now() + 1000
            });
          }
        }
      }
    } finally {
      this._isFlushing = false;
    }
  }
  
  _sendImmediate(room, message) {
    if (this._flushCallback) {
      try {
        this._flushCallback(room, message, this._generateMsgId());
      } catch (e) {}
    }
  }
  
  async flushAll() {
    while (this._messageQueue.length > 0 || this._retryQueue.length > 0) {
      this._flush();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }
  
  getStats() {
    return {
      queuedMessages: this._messageQueue.length,
      retryQueue: this._retryQueue.length,
      pendingAcks: this._pendingMessages.size,
      totalQueued: this._totalQueued,
      maxQueueSize: this.maxQueueSize,
    };
  }
  
  async destroy() {
    this._isDestroyed = true;
    
    const messages = [...this._messageQueue, ...this._retryQueue];
    this._messageQueue = [];
    this._retryQueue = [];
    this._totalQueued = 0;
    this._roomQueueSizes.clear();
    
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
        } catch (e) {}
      }
    }
    
    this._flushCallback = null;
    this._pendingMessages.clear();
  }
}

// ==================== ROOM MANAGER ====================
// PISAHAN Map seats dan points
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();        // ONLY data kursi
    this.points = new Map();       // ONLY data point (posisi cursor)
    this.muteStatus = false;
    this.currentNumber = 1;
    this.lastActivity = Date.now();
  }
  
  updateActivity() {
    this.lastActivity = Date.now();
  }
  
  isIdle() {
    return Date.now() - this.lastActivity > CONSTANTS.ROOM_MANAGER_IDLE_TIMEOUT && this.getOccupiedCount() === 0;
  }
  
  // ============ SEAT METHODS (KURSI) ============
  
  getAvailableSeat() {
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) {
        return seat;
      }
    }
    return null;
  }
  
  addNewSeat(userId) {
    const newSeatNumber = this.getAvailableSeat();
    if (!newSeatNumber) return null;
    
    this.seats.set(newSeatNumber, {
      noimageUrl: "",
      namauser: userId,
      color: "",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0,
      lastUpdated: Date.now()
    });
    
    this.updateActivity();
    return newSeatNumber;
  }
  
  getSeat(seatNumber) {
    return this.seats.get(seatNumber) || null;
  }
  
  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const existingSeat = this.seats.get(seatNumber);
    if (existingSeat) {
      existingSeat.noimageUrl = seatData.noimageUrl || "";
      existingSeat.namauser = seatData.namauser || "";
      existingSeat.color = seatData.color || "";
      existingSeat.itembawah = seatData.itembawah || 0;
      existingSeat.itematas = seatData.itematas || 0;
      existingSeat.vip = seatData.vip || 0;
      existingSeat.viptanda = seatData.viptanda || 0;
      existingSeat.lastUpdated = Date.now();
    } else {
      this.seats.set(seatNumber, {
        noimageUrl: seatData.noimageUrl || "",
        namauser: seatData.namauser || "",
        color: seatData.color || "",
        itembawah: seatData.itembawah || 0,
        itematas: seatData.itematas || 0,
        vip: seatData.vip || 0,
        viptanda: seatData.viptanda || 0,
        lastUpdated: Date.now()
      });
    }
    
    this.updateActivity();
    return true;
  }
  
  removeSeat(seatNumber) {
    const deleted = this.seats.delete(seatNumber);
    // Point juga ikut dihapus
    this.points.delete(seatNumber);
    if (deleted) this.updateActivity();
    return deleted;
  }
  
  isSeatOccupied(seatNumber) {
    return this.seats.has(seatNumber);
  }
  
  getSeatOwner(seatNumber) {
    const seat = this.seats.get(seatNumber);
    return seat ? seat.namauser : null;
  }
  
  getOccupiedCount() {
    return this.seats.size;
  }
  
  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
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
    return meta;
  }
  
  getOccupiedSeats() {
    const occupied = {};
    for (const [seatNum, seat] of this.seats) {
      occupied[seatNum] = seat.namauser;
    }
    return occupied;
  }
  
  // ============ POINT METHODS ============
  
  updatePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    this.points.set(seatNumber, {
      x: point.x,
      y: point.y,
      fast: point.fast || false,
      timestamp: Date.now()
    });
    
    this.updateActivity();
    return true;
  }
  
  getPoint(seatNumber) {
    return this.points.get(seatNumber) || null;
  }
  
  getAllPoints() {
    const points = [];
    for (const [seatNum, point] of this.points) {
      points.push({
        seat: seatNum,
        x: point.x,
        y: point.y,
        fast: point.fast ? 1 : 0
      });
    }
    return points;
  }
  
  removePoint(seatNumber) {
    return this.points.delete(seatNumber);
  }
  
  clearAllPoints() {
    this.points.clear();
  }
  
  // ============ MUTE METHODS ============
  
  setMute(isMuted) {
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    this.updateActivity();
    return this.muteStatus;
  }
  
  getMute() {
    return this.muteStatus;
  }
  
  // ============ NUMBER METHODS ============
  
  setCurrentNumber(number) {
    this.currentNumber = number;
    this.updateActivity();
  }
  
  getCurrentNumber() {
    return this.currentNumber;
  }
  
  // ============ CLEANUP ============
  
  compressOldPoints() {
    const now = Date.now();
    for (const [seatNum, point] of this.points) {
      if (point.timestamp && (now - point.timestamp) > 300000) {
        this.points.delete(seatNum);
      }
    }
  }
  
  destroy() {
    this.seats.clear();
    this.points.clear();
  }
}

// ==================== MAIN CHATSERVER CLASS ====================
export class ChatServer2 {
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
    this._seatLocks = new Set();
    
    this._activeClients = new Set();
    
    this.roomManagers = new Map();
    this.clients = new Set();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this._activeListeners = new Map();
    this._clientWebSockets = new Set();
    
    this._cleanupInterval = null;
    this._memoryCheckInterval = null;
    this._webSocketCleanupInterval = null;
    
    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => {
      this._sendDirectToRoom(room, msg, msgId);
    });
    
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      this.lowcard = null;
    }
    
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    this.intervalMillis = Math.min(CONSTANTS.NUMBER_TICK_INTERVAL, CONSTANTS.MAX_TIMER_MS);
    this.numberTickTimer = null;
    this._tickRunning = false;
    this._tickErrorCount = 0;
    this._lastTickSuccess = Date.now();
    
    this._masterTimerInterval = null;
    this._lastMasterTick = Date.now();
    this._tickCounter = 0;
    this._masterTickRunning = false;
    
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
    }
    
    this.memoryMonitor = new MemoryMonitor(this);
    
    this.startNumberTickTimer();
    this.startMasterTimer();
    this._startPeriodicCleanup();
    this.memoryMonitor.start();
    
    this._emergencyCleanupInterval = setInterval(() => {
      this._emergencyCleanup();
    }, CONSTANTS.EMERGENCY_CLEANUP_INTERVAL_MS);
    
    this._forceCleanupInterval = setInterval(() => {
      this._forceMemoryCleanup();
    }, CONSTANTS.FORCE_CLEANUP_INTERVAL);
    
    this._memoryCheckInterval = setInterval(() => {
      this._checkMemoryAndForceCleanup();
    }, 60000);
    
    this._lastHealthCheck = Date.now();
    this._consecutiveErrors = 0;
  }
  
  _checkMemoryAndForceCleanup() {
    const activeCount = this._activeClients.size;
    const gameCount = this.lowcard?.activeGames?.size || 0;
    const bufferCount = this.chatBuffer?._messageQueue?.length || 0;
    
    if (activeCount > CONSTANTS.FORCE_CLEANUP_CONNECTIONS || 
        gameCount > CONSTANTS.FORCE_CLEANUP_GAMES || 
        bufferCount > CONSTANTS.FORCE_CLEANUP_BUFFER) {
      this._emergencyCleanup();
    }
    
    for (const roomManager of this.roomManagers.values()) {
      roomManager.compressOldPoints();
    }
  }
  
  _cleanupClosedWebSockets() {
    const zombies = [];
    
    for (const ws of this._activeClients) {
      if (!ws) {
        zombies.push(ws);
        continue;
      }
      
      if (ws.readyState !== 1) {
        zombies.push(ws);
        continue;
      }
    }
    
    for (const ws of zombies) {
      this.safeWebSocketCleanup(ws).catch(() => {});
    }
    
    return zombies.length;
  }
  
  _startWebSocketCleanup() {
    if (this._webSocketCleanupInterval) {
      clearInterval(this._webSocketCleanupInterval);
    }
    
    this._webSocketCleanupInterval = setInterval(() => {
      try {
        const cleanedWS = this._cleanupClosedWebSockets();
        
        if (cleanedWS > 0) {
          for (const room of roomList) {
            this.updateRoomCount(room);
          }
        }
      } catch (error) {}
    }, CONSTANTS.WS_CLEANUP_INTERVAL_MS);
  }
  
  assignNewSeat(room, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return null;
    if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;
    
    const existingSeatInfo = this.userToSeat.get(userId);
    if (existingSeatInfo && existingSeatInfo.room === room) {
      const seatNum = existingSeatInfo.seat;
      const seatOwner = roomManager.getSeatOwner(seatNum);
      if (seatOwner === userId) {
        return seatNum;
      } else {
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
    }
    
    const lockKey = `seat_${room}`;
    if (this._seatLocks.has(lockKey)) return null;
    this._seatLocks.add(lockKey);
    
    try {
      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;
      
      const newSeatNumber = roomManager.addNewSeat(userId);
      if (!newSeatNumber) return null;
      
      this.userToSeat.set(userId, { room, seat: newSeatNumber });
      this.userCurrentRoom.set(userId, room);
      
      this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      
      return newSeatNumber;
    } finally {
      this._seatLocks.delete(lockKey);
    }
  }
  
  startMasterTimer() {
    if (this._masterTimerInterval) {
      clearInterval(this._masterTimerInterval);
    }
    
    this._masterTimerInterval = setInterval(() => {
      this._safeMasterTick();
    }, CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }
  
  async _safeMasterTick() {
    if (this._isClosing) return;
    if (this._masterTickRunning) return;
    
    this._masterTickRunning = true;
    
    try {
      await Promise.race([
        this._masterTickInternal(),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Master tick timeout')), CONSTANTS.MASTER_TICK_TIMEOUT_MS)
        )
      ]);
    } catch (error) {
      this._consecutiveErrors++;
    } finally {
      this._masterTickRunning = false;
    }
  }
  
  async _masterTickInternal() {
    const now = Date.now();
    this._lastMasterTick = now;
    this._tickCounter++;
    
    if (this.chatBuffer && typeof this.chatBuffer.tick === 'function') {
      try {
        this.chatBuffer.tick(now);
      } catch (error) {}
    }
    
    if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
      try {
        this.lowcard.masterTick();
      } catch (error) {}
    }
    
    if (this._tickCounter % 60 === 0) {
      await this._healthCheck();
    }
  }
  
  async _healthCheck() {
    const now = Date.now();
    const timeSinceLastNumberTick = now - (this._lastTickSuccess || now);
    
    if (timeSinceLastNumberTick > CONSTANTS.NUMBER_TICK_DEAD_THRESHOLD_MS) {
      this._emergencyNumberRecovery();
    }
    
    const timeSinceLastMasterTick = now - this._lastMasterTick;
    if (timeSinceLastMasterTick > CONSTANTS.MASTER_TICK_STUCK_THRESHOLD_MS) {
      if (this._masterTimerInterval) {
        clearInterval(this._masterTimerInterval);
        this._masterTimerInterval = null;
      }
      this.startMasterTimer();
    }
  }
  
  startNumberTickTimer() {
    if (this.numberTickTimer) {
      clearTimeout(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    
    this._tickRunning = false;
    this._tickErrorCount = 0;
    this._lastTickSuccess = Date.now();
    
    const scheduleNext = () => {
      if (this._isClosing) return;
      
      const timeSinceLastSuccess = Date.now() - this._lastTickSuccess;
      if (timeSinceLastSuccess > 120000) {
        this._forceRecovery();
      }
      
      let interval = this.intervalMillis;
      if (interval > CONSTANTS.MAX_TIMER_MS) {
        interval = CONSTANTS.MAX_TIMER_MS;
      }
      
      this.numberTickTimer = setTimeout(async () => {
        await this._safeTickWithRetry();
        scheduleNext();
      }, interval);
    };
    
    scheduleNext();
  }
  
  async _safeTickWithRetry() {
    const MAX_RETRIES = 3;
    
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        await Promise.race([
          this._safeTick(),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Number tick timeout')), CONSTANTS.NUMBER_TICK_TIMEOUT_MS)
          )
        ]);
        this._tickErrorCount = 0;
        this._lastTickSuccess = Date.now();
        return;
      } catch (error) {
        this._tickErrorCount++;
        
        if (attempt < MAX_RETRIES) {
          await new Promise(resolve => setTimeout(resolve, 100 * attempt));
        }
      }
    }
    
    this._emergencyNumberRecovery();
  }
  
  async _safeTick() {
    if (this._tickRunning || this._isClosing) return;
    this._tickRunning = true;
    
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      for (const roomManager of this.roomManagers.values()) {
        try {
          roomManager.setCurrentNumber(this.currentNumber);
        } catch (e) {}
      }
      
      const message = safeStringify(["currentNumber", this.currentNumber]);
      const clientsToNotify = [];
      
      for (const client of this._activeClients) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
          clientsToNotify.push(client);
        }
      }
      
      const batchSize = 50;
      for (let i = 0; i < clientsToNotify.length; i += batchSize) {
        const batch = clientsToNotify.slice(i, i + batchSize);
        for (const client of batch) {
          try {
            client.send(message);
          } catch (e) {}
        }
        if (i + batchSize < clientsToNotify.length) {
          await new Promise(resolve => setTimeout(resolve, 1));
        }
      }
      
    } catch (error) {
      throw error;
    } finally {
      this._tickRunning = false;
    }
  }
  
  _emergencyNumberRecovery() {
    this._tickRunning = false;
    this._tickErrorCount = 0;
    
    this.currentNumber = 1;
    for (const roomManager of this.roomManagers.values()) {
      try {
        roomManager.setCurrentNumber(1);
      } catch (e) {}
    }
    
    if (this.numberTickTimer) {
      clearTimeout(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    this.startNumberTickTimer();
    
    const recoveryMsg = safeStringify(["currentNumber", 1]);
    for (const client of this._activeClients) {
      if (client && client.readyState === 1 && !client._isClosing) {
        try {
          client.send(recoveryMsg);
        } catch (e) {}
      }
    }
  }
  
  _forceRecovery() {
    if (this.numberTickTimer) {
      clearTimeout(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    this._tickRunning = false;
    this.startNumberTickTimer();
  }
  
  _forceMemoryCleanup() {
    if (this._isClosing) return;
    
    this._compressRoomClients();
    
    const zombies = [];
    for (const ws of this._activeClients) {
      if (!ws || ws.readyState !== 1) {
        zombies.push(ws);
      }
    }
    for (const ws of zombies) {
      this._activeClients.delete(ws);
    }
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
    
    for (let i = 0; i < Math.min(toCleanup.length, CONSTANTS.CLEANUP_BATCH_SIZE * 2); i++) {
      this.forceUserCleanup(toCleanup[i]).catch(() => {});
    }
    
    this._compressRoomClients();
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
    
    const wasOccupied = roomManager.isSeatOccupied(seatNumber);
    const isOccupied = seatData.namauser && seatData.namauser !== "";
    
    const success = roomManager.updateSeat(seatNumber, seatData);
    
    if (success && wasOccupied !== isOccupied) {
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    }
    return success;
  }
  
  updatePointDirect(room, seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    return roomManager.updatePoint(seatNumber, point);
  }
  
  removeSeatDirect(room, seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    
    const wasOccupied = roomManager.isSeatOccupied(seatNumber);
    const success = roomManager.removeSeat(seatNumber);
    
    if (success && wasOccupied) {
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    }
    return success;
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
    if (this._activeClients.size > CONSTANTS.MAX_ACTIVE_CLIENTS_ABS) {
      this._emergencyCleanup();
    }
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
      if (index > -1) {
        clientArray.splice(index, 1);
      }
    }
  }
  
  _compressRoomClients() {
    for (const [room, clients] of this.roomClients) {
      const filtered = clients.filter(ws => ws && ws.readyState === 1 && ws.roomname === room);
      if (filtered.length !== clients.length) {
        this.roomClients.set(room, filtered);
      }
    }
  }
  
  _cleanupWebSocketListeners(ws) {
    if (ws._abortController) {
      try { ws._abortController.abort(); } catch(e) {}
      ws._abortController = null;
    }
    
    const listeners = this._activeListeners.get(ws);
    if (listeners) {
      for (const { event, handler } of listeners) {
        try { ws.removeEventListener(event, handler); } catch(e) {}
      }
      this._activeListeners.delete(ws);
    }
    
    const events = ['message', 'error', 'close', 'open'];
    for (const event of events) {
      try {
        if (typeof ws.removeAllListeners === 'function') {
          ws.removeAllListeners(event);
        }
      } catch(e) {}
    }
    
    const propsToDelete = [
      'roomname', 'idtarget', '_isClosing', '_connectionTime',
      '_isCleaningUp', 'username', 'sessionId', '_reconnectAttempts',
      '_messageQueue', '_lastMessageTime', '_bytesReceived', '_bytesSent', '_abortController'
    ];
    
    for (const prop of propsToDelete) {
      try { delete ws[prop]; } catch(e) {
        try { ws[prop] = null; } catch(e2) {}
      }
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
      
      try {
        ws.send(message);
        return true;
      } catch (sendError) {
        if (sendError.code === 'ECONNRESET' || 
            sendError.message?.includes('ECONNRESET') ||
            sendError.message?.includes('CLOSED')) {
          await this.safeWebSocketCleanup(ws);
          return false;
        }
        throw sendError;
      }
    } catch (error) {
      if (error.code === 'ERR_INVALID_STATE' || 
          error.message?.includes('CLOSED') ||
          error.message?.includes('ECONNRESET')) {
        this.safeWebSocketCleanup(ws).catch(() => {});
      }
      return false;
    }
  }
  
  _sendDirectToRoom(room, msg, msgId = null) {
    let clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    
    const liveClients = clientArray.filter(ws => 
      ws && ws.readyState === 1 && !ws._isClosing && ws.roomname === room
    );
    
    if (liveClients.length === 0) return 0;
    
    const messageStr = safeStringify(msg);
    let sentCount = 0;
    
    for (let i = 0; i < liveClients.length; i++) {
      const client = liveClients[i];
      try {
        client.send(messageStr);
        sentCount++;
      } catch (e) {
        this.safeWebSocketCleanup(client).catch(() => {});
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
      
      this.updateRoomCount(room);
    } catch (error) {}
  }
  
  async safeWebSocketCleanup(ws) {
    if (!ws || this._cleaningUp.has(ws)) return;
    this._cleaningUp.add(ws);
    
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      ws._isClosing = true;
      
      if (ws.readyState === 1) {
        try {
          ws.send(JSON.stringify(["connectionClosed", "Server cleanup"]));
        } catch (e) {}
      }
      
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
        this._removeUserConnection(userId, ws);
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
      }
      
      this._cleanupWebSocketListeners(ws);
      
      if (ws.readyState === 1) {
        try {
          ws.close(1000, "Normal closure");
        } catch(e) {
          try { ws.close(); } catch(e2) {}
        }
      }
      
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
              
              const point = roomManager.getPoint(seat);
              if (point) {
                await this.safeSend(ws, ["pointUpdated", room, seat, point.x, point.y, point.fast ? 1 : 0]);
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
    
    let data;
    try {
      data = safeParseJSON(messageStr);
    } catch (e) {
      return;
    }
    
    if (!data || !Array.isArray(data) || data.length === 0) return;
    
    try {
      await Promise.race([
        this._processMessage(ws, data, data[0]),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Message processing timeout')), 5000)
        )
      ]);
    } catch (error) {
      await this.safeSend(ws, ["error", "Processing failed"]);
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
          
          if (!isValidUsername(sender)) {
            await this.safeSend(ws, ["error", "Invalid sender username"]);
            break;
          }
          
          if (!isValidUsername(idt)) {
            await this.safeSend(ws, ["error", "Invalid target username"]);
            break;
          }
          
          const sanitizedMsg = msg?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          if (sanitizedMsg.trim().length === 0) {
            await this.safeSend(ws, ["error", "Message cannot be empty"]);
            break;
          }
          
          const out = ["private", idt, url, sanitizedMsg, Date.now(), sender];
          
          await this.safeSend(ws, out);
          
          const targetConnections = this.userConnections.get(idt);
          if (targetConnections) {
            let sentToTarget = false;
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                await this.safeSend(client, out);
                sentToTarget = true;
                break;
              }
            }
            if (!sentToTarget) {
              await this.safeSend(ws, ["error", `User ${idt} is offline`]);
            }
          } else {
            await this.safeSend(ws, ["error", `User ${idt} is offline`]);
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
            noimageUrl: noimageUrl?.slice(0, 255) || "",
            namauser: namauser,
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0,
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
      this.userConnections.delete(userId);
      
    } catch (error) {}
  }
  
  _cleanupEmptyRooms() {
    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      if (roomManager && roomManager.getOccupiedCount() === 0) {
        const idleTime = Date.now() - roomManager.lastActivity;
        if (idleTime > CONSTANTS.ROOM_IDLE_BEFORE_CLEANUP) {
          roomManager.destroy();
          this.roomManagers.delete(room);
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
    
    this._startWebSocketCleanup();
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
    this._cleanupEmptyRooms();
    
    const zombies = [];
    for (const ws of this._activeClients) {
      if (ws && ws.readyState !== 1) {
        zombies.push(ws);
      }
    }
    for (const ws of zombies) {
      this._removeFromActiveClients(ws);
    }
  }
  
  _logCleanupStats() {
    let activeReal = 0;
    for (const c of this._activeClients) {
      if (c?.readyState === 1) activeReal++;
    }
    const bufferStats = this.chatBuffer.getStats();
  }
  
  async getMemoryStats() {
    let activeReal = 0;
    for (const c of this._activeClients) {
      if (c?.readyState === 1) activeReal++;
    }
    let totalRoomClients = 0;
    for (const clients of this.roomClients.values()) {
      totalRoomClients += clients.filter(ws => ws !== null).length;
    }
    
    const bufferStats = this.chatBuffer.getStats();
    
    let totalSeats = 0;
    let totalPoints = 0;
    for (const rm of this.roomManagers.values()) {
      totalSeats += rm.seats.size;
      totalPoints += rm.points.size;
    }
    
    return {
      timestamp: Date.now(),
      uptime: Date.now() - this._startTime,
      activeClients: {
        total: this._activeClients.size,
        real: activeReal,
        waste: this._activeClients.size - activeReal
      },
      roomClients: { total: totalRoomClients },
      userConnections: this.userConnections.size,
      userToSeatSize: this.userToSeat.size,
      userCurrentRoomSize: this.userCurrentRoom.size,
      chatBuffer: bufferStats,
      seats: totalSeats,
      points: totalPoints
    };
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    if (this._masterTimerInterval) {
      clearInterval(this._masterTimerInterval);
      this._masterTimerInterval = null;
    }
    if (this._forceCleanupInterval) {
      clearInterval(this._forceCleanupInterval);
      this._forceCleanupInterval = null;
    }
    if (this._memoryCheckInterval) {
      clearInterval(this._memoryCheckInterval);
      this._memoryCheckInterval = null;
    }
    if (this._webSocketCleanupInterval) {
      clearInterval(this._webSocketCleanupInterval);
      this._webSocketCleanupInterval = null;
    }
    
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
    this._activeListeners.clear();
    this._clientWebSockets.clear();
    this._cleaningUp.clear();
    this._connectionLocks.clear();
    this._seatLocks.clear();
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
            buffer: bufferStats,
            masterTimer: this._masterTimerInterval ? "running" : "stopped",
            numberTimer: this.numberTickTimer ? "running" : "stopped",
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
        
        if (url.pathname === "/debug/gc") {
          this._forceMemoryCleanup();
          this._cleanupClosedWebSockets();
          return new Response("Force cleanup executed", { status: 200 });
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
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER_2.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER_2.get(chatId);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/debug/gc", "/shutdown"].includes(url.pathname)) {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running - Cloudflare Workers", {
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
};
