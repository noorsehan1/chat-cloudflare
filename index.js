// ==================== CHAT SERVER 2 - ZERO CRASH ZERO MEMORY LEAK (FINAL) ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-03"
// optimized_for = "128MB Memory"

let LowCardGameManager;
try {
  LowCardGameManager = (await import("./lowcard.js")).LowCardGameManager;
} catch (e) {
  LowCardGameManager = class StubLowCardGameManager {
    constructor() {}
    masterTick() {}
    async handleEvent() {}
    async destroy() {}
  };
}

const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  MAX_GLOBAL_CONNECTIONS: 200,
  MAX_CONNECTIONS_PER_IP: 5,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 4000,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 25,
  MAX_TOTAL_BUFFER_MESSAGES: 30,
  MESSAGE_TTL_MS: 8000,
  MAX_CONNECTIONS_PER_USER: 1,
  PM_BATCH_SIZE: 3,
  PM_BATCH_DELAY_MS: 30,
  CONNECTION_CRITICAL_THRESHOLD_RATIO: 0.90,
  CONNECTION_WARNING_THRESHOLD_RATIO: 0.80,
  FORCE_CLEANUP_MEMORY_TICKS: 45,
  MAX_RETRY_QUEUE_SIZE: 50,
  MAX_FLUSH_ITERATIONS: 500,
  MAX_MESSAGES_PER_MINUTE: 45,
  MESSAGE_RATE_WINDOW_MS: 60000,
  MAX_RETRY_ATTEMPTS: 3,
  FLUSH_BATCH_SIZE: 50,
  MAX_ROOM_QUEUE_SIZE: 15,
  MAX_CLEANUP_BATCH_SIZE: 10,
  MAX_USERNAME_CACHE_SIZE: 500,
  SEAT_CLEANUP_BATCH_MS: 5,
  LOCK_TIMEOUT_MS: 3000,
  SEND_QUEUE_MAX_SIZE: 100,
  SEND_RETRY_DELAY_MS: 50,
  SEND_MAX_RETRIES: 3,
  BROADCAST_BATCH_SIZE: 10,
  BROADCAST_BATCH_DELAY_MS: 1,
  MEMORY_CHECK_INTERVAL_MS: 30000,
  MEMORY_PRESSURE_THRESHOLD: 0.85,
  EMERGENCY_CLEANUP_THRESHOLD: 0.95,
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

// ─────────────────────────────────────────────
// SimpleLock dengan Timeout dan Anti-Starvation
// ─────────────────────────────────────────────
class SimpleLock {
  constructor(timeoutMs = CONSTANTS.LOCK_TIMEOUT_MS) {
    this.locked = false;
    this.queue = [];
    this.timeoutMs = timeoutMs;
  }

  async acquire() {
    if (!this.locked) {
      this.locked = true;
      return () => { this.locked = false; this._next(); };
    }
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this.queue.findIndex(item => item.resolve === resolve);
        if (index > -1) {
          this.queue.splice(index, 1);
          reject(new Error("Lock timeout"));
        }
      }, this.timeoutMs);
      
      this.queue.push({ resolve, reject, timeout });
    });
  }

  _next() {
    if (this.queue.length === 0) {
      this.locked = false;
      return;
    }
    const next = this.queue.shift();
    if (next) {
      clearTimeout(next.timeout);
      this.locked = true;
      next.resolve(() => { this.locked = false; this._next(); });
    }
  }
}

// ─────────────────────────────────────────────
// SafeWebSocket - Queue, Retry, Backpressure
// ─────────────────────────────────────────────
class SafeWebSocket {
  constructor(ws, onCleanup) {
    this.ws = ws;
    this.sendQueue = [];
    this.isProcessing = false;
    this.isDestroyed = false;
    this.onCleanup = onCleanup;
    this.messageId = 0;
  }

  async send(data) {
    if (this.isDestroyed || this.ws.readyState !== 1) return false;
    
    if (this.sendQueue.length > CONSTANTS.SEND_QUEUE_MAX_SIZE) {
      return false;
    }
    
    this.sendQueue.push(data);
    if (!this.isProcessing) {
      this._processQueue().catch(() => {});
    }
    return true;
  }

  async _processQueue() {
    if (this.isProcessing || this.isDestroyed) return;
    this.isProcessing = true;
    
    while (this.sendQueue.length > 0 && !this.isDestroyed && this.ws.readyState === 1) {
      const data = this.sendQueue.shift();
      let retries = 0;
      
      while (retries < CONSTANTS.SEND_MAX_RETRIES && !this.isDestroyed && this.ws.readyState === 1) {
        try {
          this.ws.send(data);
          break;
        } catch (e) {
          retries++;
          if (retries < CONSTANTS.SEND_MAX_RETRIES) {
            await new Promise(r => setTimeout(r, CONSTANTS.SEND_RETRY_DELAY_MS));
          }
        }
      }
      
      if (this.sendQueue.length > 0 && !this.isDestroyed) {
        await new Promise(r => setTimeout(r, 1));
      }
    }
    
    this.isProcessing = false;
  }

  destroy() {
    this.isDestroyed = true;
    this.sendQueue = [];
    if (this.onCleanup) this.onCleanup(this.ws);
  }
}

// ─────────────────────────────────────────────
// IP Rate Limiter (Anti-DoS dengan Auto Cleanup)
// ─────────────────────────────────────────────
class IPRateLimiter {
  constructor() {
    this.ipConnections = new Map();
    this.ipMessageCount = new Map();
    this.cleanupInterval = setInterval(() => this._cleanup(), 60000);
  }

  _cleanup() {
    const now = Date.now();
    for (const [ip, data] of this.ipMessageCount) {
      if (now - data.windowStart > CONSTANTS.MESSAGE_RATE_WINDOW_MS * 2) {
        this.ipMessageCount.delete(ip);
      }
    }
    if (this.ipConnections.size > 1000) {
      const entries = Array.from(this.ipConnections.entries());
      const toDelete = entries.slice(800);
      for (const [ip] of toDelete) {
        this.ipConnections.delete(ip);
      }
    }
  }

  canConnect(ip) {
    const count = this.ipConnections.get(ip) || 0;
    return count < CONSTANTS.MAX_CONNECTIONS_PER_IP;
  }

  addConnection(ip) {
    const count = this.ipConnections.get(ip) || 0;
    this.ipConnections.set(ip, count + 1);
  }

  removeConnection(ip) {
    const count = this.ipConnections.get(ip) || 0;
    if (count <= 1) {
      this.ipConnections.delete(ip);
    } else {
      this.ipConnections.set(ip, count - 1);
    }
  }

  checkRateLimit(ip) {
    if (!ip) return true;
    
    const now = Date.now();
    let data = this.ipMessageCount.get(ip);
    
    if (!data) {
      this.ipMessageCount.set(ip, { count: 1, windowStart: now });
      return true;
    }
    
    if (now - data.windowStart > CONSTANTS.MESSAGE_RATE_WINDOW_MS) {
      data.count = 1;
      data.windowStart = now;
      return true;
    }
    
    if (data.count >= CONSTANTS.MAX_MESSAGES_PER_MINUTE * 2) {
      return false;
    }
    
    data.count++;
    return true;
  }

  destroy() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    this.ipConnections.clear();
    this.ipMessageCount.clear();
  }
}

// ─────────────────────────────────────────────
// Memory Monitor (Deteksi dan Emergency Cleanup)
// ─────────────────────────────────────────────
class MemoryMonitor {
  constructor(onPressure, onEmergency) {
    this.onPressure = onPressure;
    this.onEmergency = onEmergency;
    this.interval = setInterval(() => this._check(), CONSTANTS.MEMORY_CHECK_INTERVAL_MS);
    this.isDestroyed = false;
  }

  _check() {
    if (this.isDestroyed) return;
    try {
      const memUsage = process.memoryUsage();
      const heapUsed = memUsage.heapUsed;
      const heapTotal = memUsage.heapTotal;
      const ratio = heapUsed / heapTotal;
      
      if (ratio > CONSTANTS.MEMORY_PRESSURE_THRESHOLD && this.onPressure) {
        this.onPressure(ratio);
      }
      
      if (ratio > CONSTANTS.EMERGENCY_CLEANUP_THRESHOLD && this.onEmergency) {
        this.onEmergency(ratio);
      }
    } catch (e) {}
  }

  destroy() {
    this.isDestroyed = true;
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
  }
}

// ─────────────────────────────────────────────
// PMBuffer (Zero Leak)
// ─────────────────────────────────────────────
class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
    this._isDestroyed = false;
    this.MAX_QUEUE_SIZE = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES * 2;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    if (this._isDestroyed) return;
    if (this._queue.length > this.MAX_QUEUE_SIZE) {
      this._queue.shift();
    }
    this._queue.push({ targetId, message });
    if (!this._isProcessing) {
      this._process().catch(() => {});
    }
  }

  async _process() {
    if (this._isProcessing || this._isDestroyed) return;
    this._isProcessing = true;

    try {
      while (this._queue.length > 0 && !this._isDestroyed) {
        const batch = this._queue.splice(0, this.BATCH_SIZE);
        for (const item of batch) {
          if (this._flushCallback) {
            await this._flushCallback(item.targetId, item.message).catch(() => {});
          }
        }
        if (this._queue.length > 0 && !this._isDestroyed) {
          await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
        }
      }
    } finally {
      this._isProcessing = false;
    }
  }

  async destroy() {
    this._isDestroyed = true;
    this._queue = [];
    this._flushCallback = null;
  }
}

// ─────────────────────────────────────────────
// GlobalChatBuffer (Zero Leak)
// ─────────────────────────────────────────────
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this._flushCallback = null;
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = CONSTANTS.MAX_ROOM_QUEUE_SIZE;
    this._flushScheduled = false;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(room, message) {
    if (this._isDestroyed) return null;

    const roomSize = this._roomQueueSizes.get(room) || 0;
    if (roomSize >= this.MAX_PER_ROOM || this._messageQueue.length >= this.maxQueueSize) {
      this._sendImmediate(room, message);
      return null;
    }

    this._messageQueue.push({ room, message });
    this._roomQueueSizes.set(room, roomSize + 1);
    this._scheduleFlush();
    return null;
  }

  _scheduleFlush() {
    if (this._flushScheduled || this._isDestroyed) return;
    this._flushScheduled = true;
    queueMicrotask(() => {
      this._flushScheduled = false;
      this._flush().catch(() => {});
    });
  }

  async _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing || this._isDestroyed) return;
    this._isFlushing = true;

    try {
      const batchSize = Math.min(this._messageQueue.length, CONSTANTS.FLUSH_BATCH_SIZE);
      const batch = this._messageQueue.splice(0, batchSize);

      for (const item of batch) {
        const current = this._roomQueueSizes.get(item.room) || 0;
        if (current <= 1) {
          this._roomQueueSizes.delete(item.room);
        } else {
          this._roomQueueSizes.set(item.room, current - 1);
        }
      }

      for (const item of batch) {
        try {
          this._flushCallback(item.room, item.message);
        } catch (e) {}
      }
      
      if (this._messageQueue.length > 0 && !this._isDestroyed) {
        this._scheduleFlush();
      }
    } finally {
      this._isFlushing = false;
    }
  }

  _sendImmediate(room, message) {
    if (this._flushCallback) {
      try { this._flushCallback(room, message); } catch (e) {}
    }
  }

  async destroy() {
    this._isDestroyed = true;
    this._messageQueue = [];
    this._roomQueueSizes.clear();
    this._flushCallback = null;
  }
}

// ─────────────────────────────────────────────
// RoomManager (Zero Leak)
// ─────────────────────────────────────────────
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
  }

  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }

  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const entry = {
      noimageUrl: seatData.noimageUrl?.slice(0, 255) || "",
      namauser: seatData.namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
      color: seatData.color || "",
      itembawah: seatData.itembawah || 0,
      itematas: seatData.itematas || 0,
      vip: seatData.vip || 0,
      viptanda: seatData.viptanda || 0,
    };
    this.seats.set(seatNumber, entry);
    return true;
  }

  removeSeat(seatNumber) {
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
    return deleted;
  }

  isSeatOccupied(seatNumber) { 
    const seat = this.seats.get(seatNumber);
    return seat && seat.namauser && seat.namauser !== "";
  }
  
  getOccupiedCount() { 
    let count = 0;
    for (const seat of this.seats.values()) {
      if (seat.namauser && seat.namauser !== "") count++;
    }
    return count;
  }

  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
      if (seat.namauser && seat.namauser !== "") {
        meta[seatNum] = {
          noimageUrl: seat.noimageUrl, namauser: seat.namauser, color: seat.color,
          itembawah: seat.itembawah, itematas: seat.itematas, vip: seat.vip, viptanda: seat.viptanda
        };
      }
    }
    return meta;
  }

  updatePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    this.points.set(seatNumber, { x: point.x, y: point.y, fast: point.fast || false });
    return true;
  }

  getAllPoints() {
    const points = [];
    for (const [seatNum, point] of this.points) {
      points.push({ seat: seatNum, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return points;
  }

  setMute(isMuted) {
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    return this.muteStatus;
  }
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; }
  getCurrentNumber() { return this.currentNumber; }
  removePoint(seatNumber) { return this.points.delete(seatNumber); }

  destroy() {
    this.seats.clear();
    this.points.clear();
  }
}

// ─────────────────────────────────────────────
// ChatServer2 - ZERO CRASH ZERO MEMORY LEAK PRODUCTION
// ─────────────────────────────────────────────
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._isEmergencyMode = false;

    // Locks dengan timeout
    this.seatLock = new SimpleLock();
    this.connectionLock = new SimpleLock();
    this.roomLock = new SimpleLock();
    this.cleanupLock = new SimpleLock();

    // IP Rate Limiter
    this.ipRateLimiter = new IPRateLimiter();

    // Memory Monitor
    this.memoryMonitor = new MemoryMonitor(
      (ratio) => this._onMemoryPressure(ratio),
      (ratio) => this._onEmergencyCleanup(ratio)
    );

    // Data structures dengan batas maksimal
    this._activeClients = new Map(); // ws -> SafeWebSocket
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map(); // userId -> Set<SafeWebSocket>
    this.roomClients = new Map(); // room -> Set<SafeWebSocket>
    this._userMessageCount = new Map();

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg) => this._sendDirectToRoom(room, msg));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const targetConns = this.userConnections.get(targetId);
      if (targetConns && targetConns.size > 0) {
        for (const client of targetConns) {
          if (client && !client.isDestroyed) {
            await client.send(message);
            break;
          }
        }
      }
    });

    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      this.lowcard = null;
    }

    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    this._masterTickCounter = 0;
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }

  _onMemoryPressure(ratio) {
    console.warn(`[MEMORY] Pressure detected: ${Math.round(ratio * 100)}%`);
    this._cleanupStaleData().catch(() => {});
  }

  async _onEmergencyCleanup(ratio) {
    if (this._isEmergencyMode) return;
    this._isEmergencyMode = true;
    
    console.error(`[EMERGENCY] Memory critical: ${Math.round(ratio * 100)}% - Forcing cleanup`);
    
    await this._emergencyFullCleanup();
    
    setTimeout(() => {
      this._isEmergencyMode = false;
    }, 10000);
  }

  async _emergencyFullCleanup() {
    const release = await this.cleanupLock.acquire();
    try {
      // Hapus semua user tanpa koneksi aktif
      for (const [userId, connections] of this.userConnections) {
        let hasActive = false;
        for (const conn of connections) {
          if (conn && !conn.isDestroyed && conn.ws.readyState === 1) {
            hasActive = true;
            break;
          }
        }
        if (!hasActive) {
          await this._cleanupUserCompletely(userId);
        }
      }

      // Bersihkan seat tanpa user
      for (const [roomName, roomManager] of this.roomManagers) {
        let changed = false;
        for (const [seatNum, seatData] of roomManager.seats) {
          if (seatData && seatData.namauser) {
            if (!this.userConnections.has(seatData.namauser)) {
              roomManager.removeSeat(seatNum);
              roomManager.removePoint(seatNum);
              this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
              changed = true;
            }
          }
        }
        if (changed) this.updateRoomCount(roomName);
      }

      // Force GC hint (hanya untuk V8)
      if (global.gc) {
        try { global.gc(); } catch (e) {}
      }
    } finally {
      release();
    }
  }

  _masterTick() {
    if (this._isClosing) return;
    this._masterTickCounter++;
    
    try {
      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        this._handleNumberTick();
      }

      if (this._masterTickCounter % CONSTANTS.FORCE_CLEANUP_MEMORY_TICKS === 0) {
        this._cleanupStaleData().catch(() => {});
        this._cleanupRateLimits();
      }
    } catch (e) {}
  }

  _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }

      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const snapshot = Array.from(this._activeClients.values());
      
      for (const client of snapshot) {
        if (client && !client.isDestroyed) {
          client.send(message).catch(() => {});
        }
      }
    } catch (e) {}
  }

  async _cleanupStaleData() {
    const release = await this.cleanupLock.acquire();
    try {
      let processed = 0;
      const BATCH_SIZE = CONSTANTS.MAX_CLEANUP_BATCH_SIZE;
      
      // Bersihkan mapping userToSeat yang tidak valid
      for (const [userId, seatInfo] of this.userToSeat) {
        if (processed++ > BATCH_SIZE) {
          await new Promise(r => setTimeout(r, CONSTANTS.SEAT_CLEANUP_BATCH_MS));
          processed = 0;
        }
        const roomManager = this.roomManagers.get(seatInfo.room);
        if (!roomManager) {
          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
          continue;
        }
        const seatData = roomManager.getSeat(seatInfo.seat);
        if (!seatData || seatData.namauser !== userId) {
          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
        }
      }

      // Bersihkan seat yang tidak memiliki koneksi aktif
      processed = 0;
      for (const [roomName, roomManager] of this.roomManagers) {
        let changed = false;
        for (const [seatNum, seatData] of roomManager.seats) {
          if (processed++ > BATCH_SIZE) {
            await new Promise(r => setTimeout(r, CONSTANTS.SEAT_CLEANUP_BATCH_MS));
            processed = 0;
          }
          if (seatData && seatData.namauser) {
            if (!this.userConnections.has(seatData.namauser)) {
              roomManager.removeSeat(seatNum);
              roomManager.removePoint(seatNum);
              this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
              changed = true;
            }
          }
        }
        if (changed) {
          this.updateRoomCount(roomName);
        }
      }
    } finally {
      release();
    }
  }

  _cleanupRateLimits() {
    const now = Date.now();
    for (const [userId, userData] of this._userMessageCount) {
      if (!this.userConnections.has(userId)) {
        this._userMessageCount.delete(userId);
      } else if (now - userData.windowStart > CONSTANTS.MESSAGE_RATE_WINDOW_MS * 2) {
        this._userMessageCount.delete(userId);
      }
    }
    
    if (this._userMessageCount.size > CONSTANTS.MAX_USERNAME_CACHE_SIZE) {
      const entries = Array.from(this._userMessageCount.entries());
      const toDelete = entries.slice(CONSTANTS.MAX_USERNAME_CACHE_SIZE);
      for (const [userId] of toDelete) {
        this._userMessageCount.delete(userId);
      }
    }
  }

  async _cleanupUserCompletely(userId) {
    if (!userId) return;

    // Hapus dari semua room
    for (const [roomName, roomManager] of this.roomManagers) {
      let removed = false;
      for (let seatNum = 1; seatNum <= CONSTANTS.MAX_SEATS; seatNum++) {
        const seatData = roomManager.getSeat(seatNum);
        if (seatData && seatData.namauser === userId) {
          roomManager.removeSeat(seatNum);
          roomManager.removePoint(seatNum);
          this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
          removed = true;
        }
      }
      if (removed) {
        this.updateRoomCount(roomName);
      }
    }

    // Hapus semua mapping
    this.userToSeat.delete(userId);
    this.userCurrentRoom.delete(userId);
    this.userConnections.delete(userId);
    this._userMessageCount.delete(userId);
  }

  async _cleanupWebSocket(ws) {
    if (!ws) return;
    
    let safeWs = this._activeClients.get(ws);
    if (!safeWs) {
      for (const [key, val] of this._activeClients) {
        if (val.ws === ws) {
          safeWs = val;
          break;
        }
      }
    }
    
    if (safeWs) {
      if (safeWs.isDestroyed) return;
      safeWs.isDestroyed = true;
    }
    
    if (ws._isCleaningUp) return;
    ws._isCleaningUp = true;

    try {
      const userId = ws.idtarget;
      const room = ws.roomname;
      const ip = ws._ip;

      if (userId) {
        await this._cleanupUserCompletely(userId);
      }

      if (room) {
        const clientSet = this.roomClients.get(room);
        if (clientSet && safeWs) clientSet.delete(safeWs);
        this.updateRoomCount(room);
      }

      if (safeWs) this._activeClients.delete(safeWs.ws);
      this._activeClients.delete(ws);

      if (ip) {
        this.ipRateLimiter.removeConnection(ip);
      }

      if (ws.readyState === 1) {
        try { ws.close(1000, "Closed"); } catch (e) {}
      }
    } catch (e) {
      // Ignore cleanup errors
    } finally {
      ws._isCleaningUp = false;
    }
  }

  async assignNewSeat(room, userId) {
    const release = await this.seatLock.acquire();
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return null;

      // Hapus user dari room lain
      for (const [otherRoom, otherManager] of this.roomManagers) {
        if (otherRoom !== room) {
          for (let seatNum = 1; seatNum <= CONSTANTS.MAX_SEATS; seatNum++) {
            const seatData = otherManager.getSeat(seatNum);
            if (seatData && seatData.namauser === userId) {
              otherManager.removeSeat(seatNum);
              otherManager.removePoint(seatNum);
              this.broadcastToRoom(otherRoom, ["removeKursi", otherRoom, seatNum]);
              this.updateRoomCount(otherRoom);
            }
          }
        }
      }
      
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);

      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        return null;
      }

      // Cari kursi kosong dengan double-check untuk race condition
      let newSeatNumber = null;
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        const seatData = roomManager.getSeat(seat);
        if (!seatData || !seatData.namauser || seatData.namauser === "") {
          // Double check sebelum assign
          const recheck = roomManager.getSeat(seat);
          if (!recheck || !recheck.namauser || recheck.namauser === "") {
            newSeatNumber = seat;
            break;
          }
        }
      }
      
      if (!newSeatNumber) return null;

      const success = roomManager.updateSeat(newSeatNumber, {
        noimageUrl: "", namauser: userId, color: "", 
        itembawah: 0, itematas: 0, vip: 0, viptanda: 0
      });
      
      if (!success) return null;

      this.userToSeat.set(userId, { room, seat: newSeatNumber });
      this.userCurrentRoom.set(userId, room);

      this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);

      return newSeatNumber;
    } finally {
      release();
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

    const release = await this.roomLock.acquire();
    try {
      const oldRoom = ws.roomname;
      if (oldRoom) {
        const oldClientSet = this.roomClients.get(oldRoom);
        if (oldClientSet) {
          const safeWs = this._activeClients.get(ws);
          if (safeWs) oldClientSet.delete(safeWs);
        }
      }
      
      await this._cleanupUserCompletely(ws.idtarget);

      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }

      const assignedSeat = await this.assignNewSeat(room, ws.idtarget);
      if (!assignedSeat) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }

      ws.roomname = room;
      
      let clientSet = this.roomClients.get(room);
      if (!clientSet) {
        clientSet = new Set();
        this.roomClients.set(room, clientSet);
      }
      const safeWs = this._activeClients.get(ws);
      if (safeWs) clientSet.add(safeWs);

      let userConns = this.userConnections.get(ws.idtarget);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(ws.idtarget, userConns);
      }
      if (safeWs) userConns.add(safeWs);

      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await new Promise(resolve => setTimeout(resolve, 100));
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      await this.sendAllStateTo(ws, room);

      return true;
    } catch (error) {
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;

    const release = await this.connectionLock.acquire();
    try {
      if (baru === true) {
        await this._cleanupUserCompletely(id);
      }

      const existingConns = this.userConnections.get(id);
      if (existingConns && existingConns.size > 0) {
        for (const oldSafeWs of existingConns) {
          if (oldSafeWs.ws !== ws && oldSafeWs.ws.readyState === 1) {
            try { oldSafeWs.ws.close(1000, "Replaced"); } catch (e) {}
          }
          oldSafeWs.destroy();
        }
        existingConns.clear();
      }

      ws.idtarget = id;
      ws._isClosing = false;

      let userConns = this.userConnections.get(id);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(id, userConns);
      }
      const safeWs = this._activeClients.get(ws);
      if (safeWs) userConns.add(safeWs);

      const seatInfo = this.userToSeat.get(id);

      if (seatInfo && baru === false) {
        const { room, seat } = seatInfo;
        const roomManager = this.roomManagers.get(room);
        if (roomManager) {
          const seatData = roomManager.getSeat(seat);
          if (seatData && seatData.namauser === id) {
            ws.roomname = room;
            const clientSet = this.roomClients.get(room);
            if (clientSet && safeWs) clientSet.add(safeWs);
            await this.sendAllStateTo(ws, room, true);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await this.safeSend(ws, ["currentNumber", this.currentNumber]);
            await this.safeSend(ws, ["reconnectSuccess", room, seat]);
            return;
          }
        }
      }

      if (baru === false) {
        await this.safeSend(ws, ["needJoinRoom"]);
      } else {
        await this.safeSend(ws, ["joinroomawal"]);
      }
    } catch (error) {
      await this.safeSend(ws, ["error", "Connection failed"]);
    } finally {
      release();
    }
  }

  _checkRateLimit(userId, ip) {
    if (ip && !this.ipRateLimiter.checkRateLimit(ip)) {
      return false;
    }
    
    if (!userId) return true;

    const now = Date.now();
    let userData = this._userMessageCount.get(userId);

    if (!userData) {
      this._userMessageCount.set(userId, { count: 1, windowStart: now });
      return true;
    }

    if (now - userData.windowStart > CONSTANTS.MESSAGE_RATE_WINDOW_MS) {
      userData.count = 1;
      userData.windowStart = now;
      return true;
    }

    if (userData.count >= CONSTANTS.MAX_MESSAGES_PER_MINUTE) {
      return false;
    }

    userData.count++;
    return true;
  }

  async _sendDirectToRoom(room, msg) {
    const clientSet = this.roomClients.get(room);
    if (!clientSet?.size) return 0;

    let messageStr;
    try {
      messageStr = typeof msg === "string" ? msg : JSON.stringify(msg);
    } catch (e) {
      return 0;
    }
    
    const clients = Array.from(clientSet);
    let sentCount = 0;
    
    // Batch send untuk menghindari blocking
    for (let i = 0; i < clients.length; i += CONSTANTS.BROADCAST_BATCH_SIZE) {
      const batch = clients.slice(i, i + CONSTANTS.BROADCAST_BATCH_SIZE);
      await Promise.allSettled(
        batch.map(async (client) => {
          if (client && !client.isDestroyed) {
            const success = await client.send(messageStr);
            if (success) sentCount++;
          }
        })
      );
      
      if (i + CONSTANTS.BROADCAST_BATCH_SIZE < clients.length) {
        await new Promise(r => setTimeout(r, CONSTANTS.BROADCAST_BATCH_DELAY_MS));
      }
    }
    
    return sentCount;
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    try {
      if (msg[0] === "chat" && this.chatBuffer) {
        this.chatBuffer.add(room, msg);
        return this.roomClients.get(room)?.size || 0;
      }
      this._sendDirectToRoom(room, msg).catch(() => {});
      return this.roomClients.get(room)?.size || 0;
    } catch (e) {
      return 0;
    }
  }

  async safeSend(ws, msg) {
    if (!ws) return false;
    if (ws._isClosing || ws.readyState !== 1) return false;
    try {
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      ws.send(message);
      return true;
    } catch (e) {
      if (!ws._pendingCleanup) {
        ws._pendingCleanup = true;
        queueMicrotask(() => this._cleanupWebSocket(ws).catch(() => {}));
      }
      return false;
    }
  }

  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    if (!ws || !ws.idtarget) return;
    if (ws.readyState !== 1 || !room || ws.roomname !== room) return;

    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return;

      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (!seatInfo || seatInfo.room !== room) return;

      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);

      const allKursiMeta = roomManager.getAllSeatsMeta();
      const lastPointsData = roomManager.getAllPoints();
      const selfSeat = seatInfo.seat;

      if (excludeSelfSeat && selfSeat) {
        const filteredMeta = {};
        for (const [seat, data] of Object.entries(allKursiMeta)) {
          if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
        }
        if (Object.keys(filteredMeta).length > 0) {
          await this.safeSend(ws, ["allUpdateKursiList", room, filteredMeta]);
        }
      } else if (Object.keys(allKursiMeta).length > 0) {
        await this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
      }

      if (lastPointsData.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }
    } catch (e) {}
  }

  updatePointDirect(room, seatNumber, point, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    return roomManager.updatePoint(seatNumber, point);
  }

  updateRoomCount(room) {
    const count = this.roomManagers.get(room)?.getOccupiedCount() || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }

  getRoomCount(room) { return this.roomManagers.get(room)?.getOccupiedCount() || 0; }
  getAllRoomCountsArray() { return roomList.map(room => [room, this.getRoomCount(room)]); }
  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) counts[room] = this.getRoomCount(room);
    return counts;
  }

  setRoomMute(roomName, isMuted) {
    const roomManager = this.roomManagers.get(roomName);
    if (!roomManager) return false;
    const muteValue = roomManager.setMute(isMuted);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    if (this._isClosing) return;

    let messageStr = raw;
    if (raw instanceof ArrayBuffer) {
      try {
        messageStr = new TextDecoder().decode(raw.slice(0, CONSTANTS.MAX_MESSAGE_SIZE));
      } catch (e) { return; }
    }
    
    if (typeof messageStr !== 'string') return;
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;

    let data;
    try { data = JSON.parse(messageStr); } catch (e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;

    if (data[0] === "chat" && ws.idtarget) {
      if (!this._checkRateLimit(ws.idtarget, ws._ip)) {
        await this.safeSend(ws, ["error", "Rate limit exceeded"]);
        return;
      }
    }

    try { await this._processMessage(ws, data, data[0]); } catch (e) {}
  }

  async _processMessage(ws, data, evt) {
    switch (evt) {
      case "isInRoom":
        await this.safeSend(ws, ["inRoomStatus", this.userCurrentRoom.get(ws.idtarget) !== undefined]);
        break;
      case "setIdTarget2":
        await this.handleSetIdTarget2(ws, data[1], data[2]);
        break;
      case "joinRoom":
        await this.handleJoinRoom(ws, data[1]);
        break;
      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (ws.roomname !== roomname || ws.idtarget !== username) return;
        if (!roomList.includes(roomname)) return;
        const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
        if (sanitizedMessage.includes('\0')) return;
        this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
        break;
      }
      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (ws.roomname !== room || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
        const safeX = Math.min(Math.max(parseFloat(x) || 0, 0), 1000);
        const safeY = Math.min(Math.max(parseFloat(y) || 0, 0), 1000);
        if (this.updatePointDirect(room, seat, { x: safeX, y: safeY, fast: fast === 1 }, ws.idtarget)) {
          this.broadcastToRoom(room, ["pointUpdated", room, seat, safeX, safeY, fast]);
        }
        break;
      }
      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room) return;
        if (namauser !== ws.idtarget) return;
        const success = this.roomManagers.get(room)?.updateSeat(seat, {
          noimageUrl: noimageUrl?.slice(0, 255) || "",
          namauser: namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
          color: color || "",
          itembawah: itembawah || 0,
          itematas: itematas || 0,
          vip: vip || 0,
          viptanda: viptanda || 0,
        });
        if (success) {
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
            noimageUrl, namauser, color, itembawah, itematas, vip, viptanda
          }]]]);
        }
        break;
      }
      case "setMuteType": {
        const isMuted = data[1], roomName = data[2];
        if (roomName && roomList.includes(roomName)) {
          this.setRoomMute(roomName, isMuted);
          await this.safeSend(ws, ["muteTypeSet", !!isMuted, true, roomName]);
        }
        break;
      }
      case "getMuteType": {
        const roomName = data[1];
        if (roomName && roomList.includes(roomName)) {
          await this.safeSend(ws, ["muteTypeResponse", this.roomManagers.get(roomName).getMute(), roomName]);
        }
        break;
      }
      case "getAllRoomsUserCount":
        await this.safeSend(ws, ["allRoomsUserCount", this.getAllRoomCountsArray()]);
        break;
      case "getRoomUserCount": {
        const roomName = data[1];
        if (roomList.includes(roomName)) {
          await this.safeSend(ws, ["roomUserCount", roomName, this.getRoomCount(roomName)]);
        }
        break;
      }
      case "getCurrentNumber":
        await this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;
      case "isUserOnline": {
        const username = data[1];
        const isOnline = this.userConnections.has(username) && this.userConnections.get(username)?.size > 0;
        await this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] ?? ""]);
        break;
      }
      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (!roomList.includes(roomname)) return;
        const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
        this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, safeGiftName, Date.now()]);
        break;
      }
      case "rollangak": {
        const [, roomname, username, angka] = data;
        if (!roomList.includes(roomname)) return;
        this.broadcastToRoom(roomname, ["rollangakBroadcast", roomname, username, angka]);
        break;
      }
      case "modwarning": {
        const [, roomname] = data;
        if (!roomList.includes(roomname)) return;
        this.broadcastToRoom(roomname, ["modwarning", roomname]);
        break;
      }
      case "getOnlineUsers": {
        const users = Array.from(this.userConnections.keys());
        await this.safeSend(ws, ["allOnlineUsers", users]);
        break;
      }
      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        if (!idtarget || !username) return;
        const targetConns = this.userConnections.get(idtarget);
        if (!targetConns || targetConns.size === 0) {
          await this.safeSend(ws, ["notifError", idtarget, "User offline"]);
          return;
        }
        let sent = false;
        for (const client of targetConns) {
          if (client && !client.isDestroyed) {
            if (await client.send(JSON.stringify(["notif", noimageUrl || "", username || "", deskripsi || "", Date.now()]))) {
              sent = true;
              break;
            }
          }
        }
        await this.safeSend(ws, sent ? ["notifSent", idtarget, username, deskripsi] : ["notifError", idtarget, "No active connection"]);
        break;
      }
      case "checkUserConnection": {
        const [, targetUserId] = data;
        const isConnected = targetUserId && this.userConnections.has(targetUserId) && 
          Array.from(this.userConnections.get(targetUserId) || []).some(c => c && !c.isDestroyed);
        await this.safeSend(ws, ["userConnectionStatus", targetUserId || "", isConnected]);
        break;
      }
      case "private": {
        const [, idtarget, noimageUrl, message, sender] = data;
        if (!idtarget || !sender) return;
        await this.safeSend(ws, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
        this.pmBuffer.add(idtarget, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
        break;
      }
      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (GAME_ROOMS.includes(ws.roomname) && this.lowcard && !this._isClosing) {
          try { await this.lowcard.handleEvent(ws, data); } catch (e) {}
        }
        break;
      case "onDestroy":
        await this._cleanupWebSocket(ws);
        break;
      default:
        break;
    }
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      const clientIp = request.headers.get("CF-Connecting-IP") || 
                       request.headers.get("X-Forwarded-For")?.split(",")[0] || 
                       "unknown";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          return new Response(JSON.stringify({
            status: "healthy",
            connections: this._activeClients.size,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            memory: "128MB optimized - Zero Crash Zero Leak",
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/reset") {
          await this._forceResetAllData();
          return new Response("Reset complete", { status: 200 });
        }
        return new Response("ChatServer2 - Zero Crash Production", { status: 200 });
      }

      if (!this.ipRateLimiter.canConnect(clientIp)) {
        return new Response("Too many connections from this IP", { status: 429 });
      }

      if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }

      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];

      server.accept();

      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._isCleaningUp = false;
      ws._pendingCleanup = false;
      ws._ip = clientIp;

      this.ipRateLimiter.addConnection(clientIp);

      const safeWs = new SafeWebSocket(ws, (w) => this._cleanupWebSocket(w).catch(() => {}));
      this._activeClients.set(ws, safeWs);

      const messageHandler = (ev) => this.handleMessage(ws, ev.data);
      const closeHandler = () => {
        this.ipRateLimiter.removeConnection(clientIp);
        this._cleanupWebSocket(ws).catch(() => {});
      };
      const errorHandler = () => {
        this.ipRateLimiter.removeConnection(clientIp);
        this._cleanupWebSocket(ws).catch(() => {});
      };

      ws.addEventListener("message", messageHandler);
      ws.addEventListener("close", closeHandler);
      ws.addEventListener("error", errorHandler);

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }

  async _forceResetAllData() {
    for (const [ws, safeWs] of this._activeClients) {
      if (ws && ws.readyState === 1) {
        try { ws.close(1000, "Server reset"); } catch (e) {}
      }
      if (safeWs) safeWs.destroy();
    }

    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._userMessageCount.clear();
    this.ipRateLimiter.ipConnections.clear();
    this.ipRateLimiter.ipMessageCount.clear();

    for (const room of roomList) {
      if (this.roomManagers.has(room)) {
        this.roomManagers.get(room).destroy();
      }
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    this.currentNumber = 1;
    this._startTime = Date.now();
  }

  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;

    if (this._masterTimer) {
      clearInterval(this._masterTimer);
      this._masterTimer = null;
    }

    this.memoryMonitor.destroy();
    this.ipRateLimiter.destroy();

    for (const [ws, safeWs] of this._activeClients) {
      if (ws && ws.readyState === 1) {
        try { ws.close(1000, "Shutdown"); } catch (e) {}
      }
      if (safeWs) safeWs.destroy();
    }

    for (const roomManager of this.roomManagers.values()) {
      roomManager.destroy();
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      const bindingName = "CHAT_SERVER";
      const chatId = env[bindingName].idFromName("chat-room");
      const chatObj = env[bindingName].get(chatId);
      return chatObj.fetch(req);
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
};
