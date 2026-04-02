import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambayan", "Lounge Talk",
  "Noxxeliverothcifsa", "One Side Love", "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// ========== CONSTANTS ==========
const CONSTANTS = {
  MAX_QUEUE_SIZE: 200,
  MAX_LOCK_QUEUE_SIZE: 100,
  LOCK_ACQUIRE_TIMEOUT: 3000,
  LOCK_TIMEOUT: 8000,
  BUFFER_SIZE_LIMIT: 20,
  BROADCAST_BATCH_SIZE: 20,
  CACHE_VALID_DURATION: 5000,
  LOAD_THRESHOLD: 0.85,
  LOAD_RECOVERY_THRESHOLD: 0.65,
  KURSI_UPDATE_DEBOUNCE: 150,
  DEBOUNCE_CLEANUP_INTERVAL: 30000,
  MAX_DEBOUNCE_AGE: 20000,
  GRACE_PERIOD: 3000,
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 2,
  SAFE_SEND_RETRY: 2,
  SAFE_SEND_RETRY_DELAY: 100,
  MAX_MESSAGE_SIZE: 50000,
  MAX_ERROR_COUNT: 3,
  MAX_BUFFERED_AMOUNT: 300000,
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  MAX_NUMBER: 6,
  MAX_RETRIES: 3,
  MAX_GLOBAL_CONNECTIONS: 500,
  FORCED_CLEANUP_INTERVAL: 60000,
  MAX_TIMER_AGE: 30000,
  MAX_MESSAGE_QUEUE_SIZE: 100,
  MESSAGES_PER_SECOND_LIMIT: 5,
  MAX_BUFFER_PER_ROOM: 100,
  EMERGENCY_MEMORY_THRESHOLD: 0.85,
  EMERGENCY_CLEANUP_INTERVAL: 5000,
  STORAGE_TIMEOUT: 3000,
  MAX_FAILED_BATCHES: 100,
  MAX_QUEUE_HARD_LIMIT: 500,
  POINT_FLUSH_INTERVAL: 2000,
  MAX_ERROR_LOGS: 100,
  MEMORY_CHECK_INTERVAL: 3000,
  MEMORY_CRITICAL_THRESHOLD: 80,
  MEMORY_PANIC_THRESHOLD: 100,
  SAVE_STATE_INTERVAL: 60000,
  SEAT_UPDATE_RETRIES: 3,
  SEAT_UPDATE_RETRY_DELAY: 10,
  MAX_POINTS_PER_SEAT_PER_SECOND: 3
};

// ========== GLOBAL ERROR HANDLER ==========
const originalConsoleError = console.error;
let errorLogs = [];
console.error = (...args) => {
  const msg = args.join(' ');
  
  const ignorePatterns = [
    'WebSocket closed',
    'Connection closed',
    'ERR_STREAM_WRITE_AFTER_END',
    'ECONNRESET',
    'ERR_INVALID_STATE',
    'Socket is not writable',
    'Premature close'
  ];
  
  if (ignorePatterns.some(pattern => msg.includes(pattern))) {
    return;
  }
  
  errorLogs.push({ time: Date.now(), msg });
  if (errorLogs.length > CONSTANTS.MAX_ERROR_LOGS) errorLogs.shift();
  originalConsoleError.apply(console, args);
};

// ========== SEAT RATE LIMITER ==========
class SeatRateLimiter {
  constructor() {
    this.seatUpdates = new Map();
    this.MAX_ENTRIES_PER_SEAT = 100;
    this._cleanupInterval = setInterval(() => this.cleanup(), 60000);
  }
  
  canUpdate(room, seat) {
    const key = `${room}-${seat}`;
    const now = Date.now();
    let updates = this.seatUpdates.get(key) || [];
    
    if (updates.length > this.MAX_ENTRIES_PER_SEAT) {
      updates = updates.slice(-this.MAX_ENTRIES_PER_SEAT);
    }
    
    const recent = updates.filter(t => now - t < 100);
    
    if (recent.length >= CONSTANTS.MAX_POINTS_PER_SEAT_PER_SECOND) {
      return false;
    }
    
    recent.push(now);
    this.seatUpdates.set(key, recent);
    return true;
  }
  
  cleanup() {
    const now = Date.now();
    for (const [key, timestamps] of this.seatUpdates) {
      const filtered = timestamps.filter(t => now - t < 1000);
      if (filtered.length === 0) {
        this.seatUpdates.delete(key);
      } else {
        this.seatUpdates.set(key, filtered);
      }
    }
  }
  
  destroy() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this.seatUpdates.clear();
  }
}

// ========== PROMISE LOCK MANAGER ==========
class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
    this.lockTimestamps = new Map();
    this._destroyed = false;
    this._lockId = 0;
  }

  async acquire(resourceId) {
    if (this._destroyed) throw new Error("LockManager destroyed");
    
    const lockId = ++this._lockId;
    const startTime = Date.now();
    
    if (this.locks.has(resourceId)) {
      const lockTime = this.lockTimestamps.get(resourceId) || 0;
      if (Date.now() - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
      }
    }

    const currentQueue = this.queue.get(resourceId) || [];
    if (currentQueue.length > CONSTANTS.MAX_LOCK_QUEUE_SIZE) {
      throw new Error(`Too many waiting for lock: ${resourceId}`);
    }

    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, true);
      this.lockTimestamps.set(resourceId, Date.now());
      return () => this.release(resourceId, lockId);
    }

    if (!this.queue.has(resourceId)) this.queue.set(resourceId, []);

    return new Promise((resolve, reject) => {
      let resolved = false;
      
      const timeoutId = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          const queue = this.queue.get(resourceId);
          if (queue) {
            const idx = queue.findIndex(item => item.lockId === lockId);
            if (idx !== -1) {
              queue.splice(idx, 1);
              reject(new Error(`Lock queue timeout for ${resourceId} after ${Date.now() - startTime}ms`));
            }
          }
        }
      }, CONSTANTS.LOCK_ACQUIRE_TIMEOUT);
      
      this.queue.get(resourceId).push({ 
        lockId,
        resolve: () => {
          if (!resolved) {
            resolved = true;
            clearTimeout(timeoutId);
            resolve(() => this.release(resourceId, lockId));
          }
        }, 
        reject: (err) => {
          if (!resolved) {
            resolved = true;
            clearTimeout(timeoutId);
            reject(err);
          }
        },
        timeoutId,
        timestamp: Date.now()
      });
    });
  }

  release(resourceId, lockId) {
    if (this._destroyed) return;
    
    const queue = this.queue.get(resourceId);
    if (queue && queue.length > 0) {
      const next = queue.shift();
      if (next.timeoutId) clearTimeout(next.timeoutId);
      this.lockTimestamps.set(resourceId, Date.now());
      
      setTimeout(() => {
        try {
          next.resolve();
        } catch (error) {
          this.forceRelease(resourceId);
        }
      }, 0);
      
      if (queue.length === 0) {
        this.queue.delete(resourceId);
      }
    } else {
      this.forceRelease(resourceId);
    }
  }

  forceRelease(resourceId) {
    this.locks.delete(resourceId);
    this.lockTimestamps.delete(resourceId);
    const queue = this.queue.get(resourceId);
    if (queue) {
      const pending = [...queue];
      this.queue.delete(resourceId);
      for (const item of pending) {
        if (item.timeoutId) clearTimeout(item.timeoutId);
        if (item.reject) {
          try {
            item.reject(new Error(`Lock force released: ${resourceId}`));
          } catch (e) {}
        }
      }
    }
  }

  cleanupStuckLocks() {
    const now = Date.now();
    for (const [resourceId, lockTime] of this.lockTimestamps) {
      if (now - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
      }
    }
  }
  
  destroy() {
    this._destroyed = true;
    for (const [resourceId, queue] of this.queue) {
      if (queue) {
        for (const item of queue) {
          if (item.timeoutId) clearTimeout(item.timeoutId);
          if (item.reject) item.reject(new Error("Lock manager destroyed"));
        }
      }
    }
    this.locks.clear();
    this.queue.clear();
    this.lockTimestamps.clear();
  }
}

// ========== QUEUE MANAGER ==========
class QueueManager {
  constructor(concurrency = 3) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = CONSTANTS.MAX_QUEUE_SIZE;
    this.processing = false;
    this.destroyed = false;
  }

  async add(job, retryCount = 0) {
    if (this.destroyed) throw new Error("Queue manager destroyed");
    
    const currentLoad = this.active / this.concurrency;
    let maxAllowed = this.maxQueueSize;
    if (currentLoad > 0.8) {
      maxAllowed = Math.floor(this.maxQueueSize / 2);
    }
    
    if (this.queue.length >= maxAllowed) {
      if (retryCount < 2) {
        const delay = 50 * Math.pow(2, retryCount);
        await new Promise(r => setTimeout(r, delay));
        return this.add(job, retryCount + 1);
      }
      console.warn(`Queue full (${this.queue.length}), dropping job`);
      return null;
    }
    
    if (this.queue.length > maxAllowed) {
      const expiredCount = Math.floor(this.queue.length * 0.3);
      for (let i = 0; i < expiredCount; i++) {
        const expired = this.queue.shift();
        if (expired.timeoutId) clearTimeout(expired.timeoutId);
        if (expired.reject) {
          try { expired.reject(new Error("Queue cleared - overload")); } catch (e) {}
        }
      }
    }
    
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const index = this.queue.findIndex(item => item.reject === reject);
        if (index !== -1) {
          this.queue.splice(index, 1);
          reject(new Error("Queue timeout"));
        }
      }, 10000);
      
      this.queue.push({ job, resolve, reject, timeoutId, timestamp: Date.now() });
      if (!this.processing) this.process();
    });
  }

  async process() {
    if (this.processing || this.destroyed) return;
    this.processing = true;
    
    try {
      const processNext = async () => {
        while (this.queue.length > 0 && this.active < this.concurrency && !this.destroyed) {
          try {
            while (this.queue.length > 0 && Date.now() - this.queue[0].timestamp > 30000) {
              const expired = this.queue.shift();
              if (expired.timeoutId) clearTimeout(expired.timeoutId);
              if (expired.reject) {
                try { expired.reject(new Error("Request timeout")); } catch (e) {}
              }
            }
            if (this.queue.length === 0) break;
            
            this.active++;
            const { job, resolve, reject, timeoutId } = this.queue.shift();
            if (timeoutId) clearTimeout(timeoutId);
            
            try {
              const result = await Promise.race([
                job(),
                new Promise((_, reject) => setTimeout(() => reject(new Error("Job timeout")), 10000))
              ]);
              if (resolve && !this.destroyed) {
                try { resolve(result); } catch (e) {}
              }
            } catch (error) {
              if (reject && !this.destroyed) {
                try { reject(error); } catch (e) {}
              }
            } finally {
              this.active--;
            }
          } catch (error) {
            console.error("Queue processing error:", error);
            this.active--;
          }
        }
      };
      
      await processNext();
    } catch (error) {
      console.error("Queue processor error:", error);
    } finally {
      this.processing = false;
      
      if (this.queue.length > 0 && !this.destroyed) {
        setTimeout(() => this.process(), 100);
      }
    }
  }

  clear() {
    const oldQueue = this.queue;
    this.queue = [];
    for (const item of oldQueue) {
      if (item.timeoutId) clearTimeout(item.timeoutId);
      if (item.reject) {
        try { item.reject(new Error("Queue cleared")); } catch (e) {}
      }
    }
  }

  size() { return this.queue.length; }
  
  destroy() {
    this.destroyed = true;
    this.clear();
    this.active = 0;
    this.processing = false;
  }
}

// ========== RATE LIMITER ==========
class RateLimiter {
  constructor(windowMs = 60000, maxRequests = 100) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
  }

  check(userId) {
    if (!userId) return true;
    const now = Date.now();
    const userRequests = this.requests.get(userId) || [];
    const recentRequests = userRequests.filter(time => now - time < this.windowMs);
    if (recentRequests.length >= this.maxRequests) return false;
    recentRequests.push(now);
    this.requests.set(userId, recentRequests);
    return true;
  }

  cleanup() {
    const now = Date.now();
    for (const [userId, requests] of this.requests) {
      const recentRequests = requests.filter(time => now - time < this.windowMs);
      if (recentRequests.length === 0) this.requests.delete(userId);
      else this.requests.set(userId, recentRequests);
    }
  }
  
  destroy() {
    this.requests.clear();
  }
}

// ========== ROOM RATE LIMITER ==========
class RoomRateLimiter {
  constructor() {
    this.roomLimits = new Map();
    this.MAX_USERS_PER_ROOM = 35;
    this._cleanupInterval = setInterval(() => this.cleanup(), 60000);
  }
  
  check(room, userId) {
    if (!room || !userId) return true;
    
    if (!this.roomLimits.has(room)) {
      this.roomLimits.set(room, new Map());
    }
    
    const userLimits = this.roomLimits.get(room);
    
    if (userLimits.size > this.MAX_USERS_PER_ROOM) {
      const entries = Array.from(userLimits.entries());
      const toRemove = entries.slice(0, Math.floor(entries.length * 0.2));
      for (const [oldUserId] of toRemove) {
        userLimits.delete(oldUserId);
      }
    }
    
    const now = Date.now();
    const userTimestamps = userLimits.get(userId) || [];
    const recent = userTimestamps.filter(t => now - t < 1000);
    
    if (recent.length >= CONSTANTS.MESSAGES_PER_SECOND_LIMIT) {
      return false;
    }
    
    recent.push(now);
    userLimits.set(userId, recent);
    return true;
  }
  
  cleanup() {
    const now = Date.now();
    for (const [room, userMap] of this.roomLimits) {
      for (const [userId, timestamps] of userMap) {
        const recent = timestamps.filter(t => now - t < 1000);
        if (recent.length === 0) {
          userMap.delete(userId);
        } else {
          userMap.set(userId, recent);
        }
      }
      if (userMap.size === 0) {
        this.roomLimits.delete(room);
      }
    }
  }
  
  destroy() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this.roomLimits.clear();
  }
}

// ========== MESSAGE QUEUE ==========
class MessageQueue {
  constructor(ws, chatServer) {
    this.ws = ws;
    this.chatServer = chatServer;
    this.queue = [];
    this.processing = false;
    this.maxSize = CONSTANTS.MAX_MESSAGE_QUEUE_SIZE;
  }
  
  async add(rawMessage) {
    if (this.queue.length >= this.maxSize) {
      return false;
    }
    
    this.queue.push(rawMessage);
    
    if (!this.processing) {
      this.process();
    }
    
    return true;
  }
  
  async process() {
    if (this.processing) return;
    this.processing = true;
    
    while (this.queue.length > 0 && this.ws.readyState === 1) {
      const raw = this.queue.shift();
      try {
        await this.chatServer.handleMessage(this.ws, raw);
      } catch (error) {
        // Error sudah ditangani di handleMessage
      }
    }
    
    this.processing = false;
    
    if (this.queue.length > 0 && this.ws.readyState === 1) {
      setImmediate(() => this.process());
    }
  }
  
  clear() {
    this.queue = [];
    this.processing = false;
  }
}

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
    lastUpdated: Date.now(),
    _version: 0
  };
}

function getCompactSeatData(seat) {
  if (!seat || !seat.namauser) return null;
  return {
    noimageUrl: seat.noimageUrl || "",
    namauser: seat.namauser,
    color: seat.color || "",
    itembawah: seat.itembawah || 0,
    itematas: seat.itematas || 0,
    vip: seat.vip || 0,
    viptanda: seat.viptanda || 0
  };
}

function safeGC() {
  try {
    if (typeof global !== 'undefined' && global.gc && typeof global.gc === 'function') {
      global.gc();
      return true;
    }
    
    if (typeof performance !== 'undefined' && performance.memory) {
      const memUsage = performance.memory.usedJSHeapSize / 1024 / 1024;
      if (memUsage > 100) {
        let temp = new Array(10000);
        temp = null;
      }
    }
  } catch(e) {}
  return false;
}

export class ChatServer {
  static _instance = null;
  
  constructor(state, env) {
    if (ChatServer._instance) {
      return ChatServer._instance;
    }
    ChatServer._instance = this;
    
    this._initialized = false;
    this._emergencyMode = false;
    
    try {
      this.state = state;
      this.env = env;
      this._startTime = Date.now();
      this._lastActivityTime = Date.now();
      this._failedBatches = [];
      this._isShuttingDown = false;
      this._messageQueues = new Map();
      this._pendingReconnections = new Map();
      
      this.muteStatus = new Map();
      for (const room of roomList) this.muteStatus.set(room, false);
      
      this.storage = state?.storage;
      
      this.lockManager = new PromiseLockManager();
      this.cleanupInProgress = new Set();
      this.clients = new Set();
      this.userToSeat = new Map();
      this.roomClients = new Map();
      this.userCurrentRoom = new Map();
      this.MAX_SEATS = CONSTANTS.MAX_SEATS;
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.updateKursiBuffer = new Map();
      this.bufferSizeLimit = CONSTANTS.BUFFER_SIZE_LIMIT;
      this.userConnections = new Map();

      this._pointBuffer = new Map();
      this._pointFlushDelay = 100;
      this._hasBufferedUpdates = false;

      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      this.roomRateLimiter = new RoomRateLimiter();
      this.seatRateLimiter = new SeatRateLimiter();
      
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;

      try { this.lowcard = new LowCardGameManager(this); } catch { this.lowcard = null; }

      this.gracePeriod = CONSTANTS.GRACE_PERIOD;
      this.disconnectedTimers = new Map();
      this.cleanupQueue = new QueueManager(3);
      
      this.currentNumber = 1;
      this.maxNumber = CONSTANTS.MAX_NUMBER;
      this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
      this.lastNumberTick = Date.now();
      this._nextConnId = 1;
      
      this.numberTickTimer = null;
      this._intervals = [];

      this.initializeRooms();
      this.startNumberTickTimer();
      
      this.roomCountsCache = null;
      this._countsCacheTime = 0;
      this.cacheValidDuration = CONSTANTS.CACHE_VALID_DURATION;

      for (const room of roomList) {
        this._pointBuffer.set(room, []);
      }

      if (typeof navigator !== 'undefined' && navigator.userAgent?.includes('Cloudflare-Workers')) {
        CONSTANTS.MEMORY_CRITICAL_THRESHOLD = 80;
        CONSTANTS.MEMORY_PANIC_THRESHOLD = 100;
        CONSTANTS.MEMORY_CHECK_INTERVAL = 3000;
      }

      const safeStart = (fn, name) => {
        try {
          fn.call(this);
        } catch (e) {
          console.error(`Failed to start ${name}:`, e);
          this._emergencyMode = true;
        }
      };
      
      safeStart(this.startAutoCleanup, 'startAutoCleanup');
      safeStart(this.startIdleCleanup, 'startIdleCleanup');
      safeStart(this.startMemoryMonitor, 'startMemoryMonitor');
      safeStart(this.startForcedCleanup, 'startForcedCleanup');
      safeStart(this.startEmergencyCleanup, 'startEmergencyCleanup');
      safeStart(this.startPeriodicFlush, 'startPeriodicFlush');
      safeStart(this.startPeriodicPointFlush, 'startPeriodicPointFlush');
      safeStart(this.startPeriodicSave, 'startPeriodicSave');
      
      this.loadState().catch(() => {});
      
      this._initialized = true;

      if (typeof process !== 'undefined') {
        process.on('uncaughtException', (error) => {
          console.error('Uncaught exception:', error);
          if (this._emergencyMemoryCleanup) this._emergencyMemoryCleanup();
        });
        
        process.on('unhandledRejection', (error) => {
          console.error('Unhandled rejection:', error);
        });
      }

    } catch (error) {
      console.error("ChatServer constructor error:", error);
      this.initializeFallback();
    }
  }

  // ========== STATE MANAGEMENT ==========
  async loadState() {
    try {
      const saved = await this.storage?.get("chatState");
      if (saved) {
        this.currentNumber = saved.currentNumber || 1;
        this.lastNumberTick = saved.lastNumberTick || Date.now();
        
        if (saved.roomSeats) {
          for (const [room, seats] of Object.entries(saved.roomSeats)) {
            const seatMap = this.roomSeats.get(room);
            const occupancyMap = this.seatOccupancy.get(room);
            if (seatMap && occupancyMap) {
              for (const [seat, data] of Object.entries(seats)) {
                const seatNum = parseInt(seat);
                const seatData = {
                  ...data,
                  lastUpdated: Date.now(),
                  _version: 0
                };
                seatMap.set(seatNum, seatData);
                if (data.namauser) {
                  occupancyMap.set(seatNum, data.namauser);
                }
              }
            }
          }
        }
      }
    } catch (error) {
      console.error("Load state error:", error);
    }
  }

  async saveState() {
    try {
      const roomSeatsObj = {};
      for (const [room, seatMap] of this.roomSeats) {
        const seats = {};
        for (const [seat, data] of seatMap) {
          if (data?.namauser) {
            seats[seat] = {
              namauser: data.namauser,
              noimageUrl: data.noimageUrl || "",
              color: data.color || "",
              lastPoint: data.lastPoint
            };
          }
        }
        if (Object.keys(seats).length > 0) {
          roomSeatsObj[room] = seats;
        }
      }
      
      await this.storage?.put("chatState", {
        currentNumber: this.currentNumber,
        lastNumberTick: this.lastNumberTick,
        roomSeats: roomSeatsObj,
        timestamp: Date.now()
      });
    } catch (error) {
      console.error("Save state error:", error);
    }
  }

  startPeriodicSave() {
    this._saveInterval = setInterval(() => {
      this.saveState().catch(() => {});
    }, CONSTANTS.SAVE_STATE_INTERVAL);
    this._intervals.push(this._saveInterval);
  }

  // ========== MEMORY MANAGEMENT ==========
  _emergencyMemoryCleanup() {
    for (const [room, points] of this._pointBuffer) {
      if (points.length > 100) {
        this.broadcastToRoom(room, ["batchPoints", room, points]);
        points.length = 0;
      }
    }
    this.flushKursiUpdates();
  }

  _forceMemoryCleanup() {
    this._pointBuffer.clear();
    for (const room of roomList) {
      this._pointBuffer.set(room, []);
    }
    
    const memUsage = process.memoryUsage();
    const heapUsedMB = memUsage.heapUsed / 1024 / 1024;
    
    if (heapUsedMB > CONSTANTS.MEMORY_PANIC_THRESHOLD + 20) {
      for (const [room, buffer] of this.updateKursiBuffer) {
        if (buffer.size > 20) {
          const entries = Array.from(buffer.entries());
          const keep = entries.slice(-15);
          this.updateKursiBuffer.set(room, new Map(keep));
        }
      }
    }
    
    if (this.lowcard && this.lowcard.activeGames) {
      this.lowcard.activeGames.clear();
    }
    
    safeGC();
  }

  cleanupMessageQueues() {
    const toDelete = [];
    for (const [ws, queue] of this._messageQueues) {
      if (!ws || ws.readyState !== 1 || ws._isClosing) {
        queue.clear();
        toDelete.push(ws);
      }
    }
    for (const ws of toDelete) {
      this._messageQueues.delete(ws);
    }
  }

  cleanupAllTimers() {
    if (this.numberTickTimer) {
      clearTimeout(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    
    for (const [userId, timer] of this.disconnectedTimers) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
    
    if (this._intervals) {
      for (const interval of this._intervals) {
        clearInterval(interval);
      }
      this._intervals = [];
    }
    
    if (this._periodicFlush) clearInterval(this._periodicFlush);
    if (this._pointFlushInterval) clearInterval(this._pointFlushInterval);
    if (this._emergencyCleanup) clearInterval(this._emergencyCleanup);
    if (this._forcedCleanupInterval) clearInterval(this._forcedCleanupInterval);
    if (this._idleCheckInterval) clearInterval(this._idleCheckInterval);
    if (this._memoryMonitor) clearInterval(this._memoryMonitor);
    if (this._saveInterval) clearInterval(this._saveInterval);
  }

  startPeriodicPointFlush() {
    if (this._pointFlushInterval) clearInterval(this._pointFlushInterval);
    
    this._pointFlushInterval = setInterval(() => {
      try {
        if (this._destroyed || this._isShuttingDown) {
          if (this._pointFlushInterval) clearInterval(this._pointFlushInterval);
          return;
        }
        
        const rooms = Array.from(this._pointBuffer.keys());
        for (const room of rooms) {
          try {
            const points = this._pointBuffer.get(room);
            if (points && points.length > 0) {
              const pointsToSend = [...points];
              points.length = 0;
              if (pointsToSend.length > 0) {
                this.broadcastToRoom(room, ["batchPoints", room, pointsToSend]);
              }
            }
          } catch (roomError) {
            console.error(`Flush error for room ${room}:`, roomError);
            this._pointBuffer.delete(room);
          }
        }
      } catch (error) {
        console.error("Periodic point flush error:", error);
      }
    }, CONSTANTS.POINT_FLUSH_INTERVAL);
    
    this._intervals.push(this._pointFlushInterval);
  }

  startPeriodicFlush() {
    this._periodicFlush = setInterval(() => {
      try {
        for (const room of roomList) {
          this.flushRoomKursiUpdates(room);
        }
      } catch (error) {}
    }, 1000);
    this._intervals.push(this._periodicFlush);
  }

  startEmergencyCleanup() {
    this._emergencyCleanup = setInterval(() => {
      try {
        const memUsage = process.memoryUsage();
        const heapPercent = memUsage.heapUsed / memUsage.heapTotal;
        const heapUsedMB = memUsage.heapUsed / 1024 / 1024;
        
        if (heapUsedMB > CONSTANTS.MEMORY_PANIC_THRESHOLD) {
          this._forceMemoryCleanup();
        } else if (heapUsedMB > CONSTANTS.MEMORY_CRITICAL_THRESHOLD) {
          this._emergencyMemoryCleanup();
        } else if (heapPercent > 0.9) {
          this._forceMemoryCleanup();
        } else if (heapPercent > 0.8) {
          for (const [room, buffer] of this.updateKursiBuffer) {
            if (buffer && buffer.size > 50) {
              const entries = Array.from(buffer.entries());
              const limited = new Map(entries.slice(-50));
              this.updateKursiBuffer.set(room, limited);
            }
          }
        }
        
        if (this._failedBatches && this._failedBatches.length > CONSTANTS.MAX_FAILED_BATCHES) {
          this._failedBatches = this._failedBatches.slice(-Math.floor(CONSTANTS.MAX_FAILED_BATCHES / 2));
        }
        
      } catch(e) {}
    }, CONSTANTS.EMERGENCY_CLEANUP_INTERVAL);
    this._intervals.push(this._emergencyCleanup);
  }

  startForcedCleanup() {
    this._forcedCleanupInterval = setInterval(() => {
      try {
        const now = Date.now();
        
        for (const [userId, timer] of this.disconnectedTimers) {
          if (timer && (now - (timer._scheduledTime || 0)) > CONSTANTS.MAX_TIMER_AGE) {
            clearTimeout(timer);
            this.disconnectedTimers.delete(userId);
            this._pendingReconnections.delete(userId);
          }
        }
        
        this.roomRateLimiter.cleanup();
        this.seatRateLimiter.cleanup();
        
        if (Math.random() < 0.1) {
          safeGC();
        }
      } catch (e) {}
    }, CONSTANTS.FORCED_CLEANUP_INTERVAL);
    this._intervals.push(this._forcedCleanupInterval);
  }

  startIdleCleanup() {
    this._idleCheckInterval = setInterval(() => {
      try {
        const now = Date.now();
        const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
        
        if (activeConnections === 0 && (now - this._lastActivityTime) > 300000) {
          this.flushKursiUpdates();
        } else if (activeConnections > 0) {
          this._lastActivityTime = now;
        }
      } catch (error) {}
    }, 60000);
    this._intervals.push(this._idleCheckInterval);
  }

  startMemoryMonitor() {
    this._memoryMonitor = setInterval(() => {
      try {
        const memUsage = process.memoryUsage();
        const heapUsedMB = memUsage.heapUsed / 1024 / 1024;
        
        if (heapUsedMB > 80 && heapUsedMB < 100) {
          this.cleanupMessageQueues();
          this.validateGracePeriodTimers();
        }
        
        if (heapUsedMB > CONSTANTS.MEMORY_CRITICAL_THRESHOLD) {
          this._emergencyMemoryCleanup();
          
          for (const room of roomList) {
            this.flushRoomKursiUpdates(room);
            const points = this._pointBuffer.get(room);
            if (points && points.length > 0) {
              this.broadcastToRoom(room, ["batchPoints", room, points]);
              points.length = 0;
            }
          }
        }
        
        if (heapUsedMB > CONSTANTS.MEMORY_PANIC_THRESHOLD) {
          this._forceMemoryCleanup();
          
          if (this.lowcard && this.lowcard.activeGames) {
            this.lowcard.activeGames.clear();
          }
          
          safeGC();
        }
        
        const queueSize = this.cleanupQueue?.size() || 0;
        if (queueSize > 100) {
          safeGC();
        }
        
        this.cleanupMessageQueues();
        
      } catch (e) {}
    }, CONSTANTS.MEMORY_CHECK_INTERVAL);
    this._intervals.push(this._memoryMonitor);
  }

  startAutoCleanup() {
    const autoCleanupInterval = setInterval(async () => {
      try {
        await this.cleanup();
      } catch (error) {}
    }, 300000);
    this._intervals.push(autoCleanupInterval);
  }

  async gracefulShutdown() {
    if (this._isShuttingDown) return;
    this._isShuttingDown = true;
    
    const shutdownMsg = JSON.stringify(["serverShutdown", "Server is restarting"]);
    for (const client of this.clients) {
      if (client.readyState === 1) {
        try {
          client.send(shutdownMsg);
        } catch(e) {}
      }
    }
    
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    this.flushKursiUpdates();
    
    for (const [room, points] of this._pointBuffer.entries()) {
      if (points && points.length > 0) {
        const pointsToSend = [...points];
        points.length = 0;
        this.broadcastToRoom(room, ["batchPoints", room, pointsToSend]);
      }
    }
    
    await this.saveState();
    
    for (const client of this.clients) {
      try {
        client.close(1000, "Server shutdown");
      } catch(e) {}
    }
    
    await this.destroy();
  }

  async destroy() {
    this.cleanupAllTimers();
    
    for (const [userId, timer] of this.disconnectedTimers) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
    this._pendingReconnections.clear();
    
    for (const [ws, queue] of this._messageQueues) {
      queue.clear();
    }
    this._messageQueues.clear();
    
    if (this.lockManager) this.lockManager.destroy();
    if (this.cleanupQueue) this.cleanupQueue.destroy();
    if (this.rateLimiter) this.rateLimiter.destroy();
    if (this.connectionRateLimiter) this.connectionRateLimiter.destroy();
    if (this.roomRateLimiter) this.roomRateLimiter.destroy();
    if (this.seatRateLimiter) this.seatRateLimiter.destroy();
    
    if (this.lowcard) {
      try { await this.lowcard.destroy(); } catch(e) {}
      this.lowcard = null;
    }
    
    this.clients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.roomSeats.clear();
    this.seatOccupancy.clear();
    this.updateKursiBuffer.clear();
    this.userConnections.clear();
    this._pointBuffer.clear();
    this.roomClients.clear();
    this.muteStatus.clear();
    this.roomCountsCache = null;
    this.cleanupInProgress.clear();
    this._failedBatches = [];
  }

  startNumberTickTimer() {
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    const scheduleNext = () => {
      const now = Date.now();
      const nextTickTime = this.lastNumberTick + this.intervalMillis;
      const delay = Math.max(0, nextTickTime - now);
      this.numberTickTimer = setTimeout(() => {
        this.tick();
        this.lastNumberTick = Date.now();
        scheduleNext();
      }, delay);
    };
    scheduleNext();
  }

  initializeFallback() {
    try {
      this.clients = new Set();
      this.userToSeat = new Map();
      this.userCurrentRoom = new Map();
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.roomClients = new Map();
      this.updateKursiBuffer = new Map();
      this.userConnections = new Map();
      this.disconnectedTimers = new Map();
      this._pendingReconnections = new Map();
      this.lockManager = new PromiseLockManager();
      this.cleanupInProgress = new Set();
      this.MAX_SEATS = CONSTANTS.MAX_SEATS;
      this.currentNumber = 1;
      this._nextConnId = 1;
      this.lowcard = null;
      this.gracePeriod = CONSTANTS.GRACE_PERIOD;
      this.cleanupQueue = new QueueManager(3);
      this.muteStatus = new Map();
      for (const room of roomList) this.muteStatus.set(room, false);
      this.storage = this.state?.storage;
      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      this.roomRateLimiter = new RoomRateLimiter();
      this.seatRateLimiter = new SeatRateLimiter();
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
      this._pointBuffer = new Map();
      this._pointFlushDelay = 100;
      this._hasBufferedUpdates = false;
      this._intervals = [];
      this.roomCountsCache = null;
      this._countsCacheTime = 0;
      this._startTime = Date.now();
      this._lastActivityTime = Date.now();
      this._failedBatches = [];
      this._isShuttingDown = false;
      this._messageQueues = new Map();
      
      this.createDefaultRoom();
      this.lastNumberTick = Date.now();
      this.numberTickTimer = null;
      this.startNumberTickTimer();
      
      const safeStart = (fn, name) => {
        try { fn.call(this); } catch(e) { console.error(`Fallback ${name} failed:`, e); }
      };
      safeStart(this.startAutoCleanup, 'startAutoCleanup');
      safeStart(this.startIdleCleanup, 'startIdleCleanup');
      safeStart(this.startMemoryMonitor, 'startMemoryMonitor');
      safeStart(this.startForcedCleanup, 'startForcedCleanup');
      safeStart(this.startEmergencyCleanup, 'startEmergencyCleanup');
      safeStart(this.startPeriodicFlush, 'startPeriodicFlush');
      safeStart(this.startPeriodicPointFlush, 'startPeriodicPointFlush');
    } catch (error) {}
  }

  async performMemoryCleanup() {
    try {
      const deadClients = [];
      for (const client of this.clients) {
        if (!client || client.readyState === 3) deadClients.push(client);
      }
      for (const client of deadClients) this.clients.delete(client);

      const roomClientsSnapshot = Array.from(this.roomClients.entries());
      for (const [room, clientArray] of roomClientsSnapshot) {
        if (clientArray) {
          const filtered = clientArray.filter(c => c && c.readyState === 1);
          if (filtered.length !== clientArray.length) {
            this.roomClients.set(room, filtered);
          }
        }
      }

      const now = Date.now();
      const disconnectedSnapshot = Array.from(this.disconnectedTimers.entries());
      for (const [userId, timer] of disconnectedSnapshot) {
        if (timer._scheduledTime && (now - timer._scheduledTime) > this.gracePeriod + 30000) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
          this._pendingReconnections.delete(userId);
        }
      }

      this.lockManager?.cleanupStuckLocks();
      this.rateLimiter.cleanup();
      this.connectionRateLimiter.cleanup();
      this.roomRateLimiter.cleanup();
      this.seatRateLimiter.cleanup();
      this.cleanupMessageQueues();

    } catch (error) {}
  }

  setRoomMute(roomName, isMuted) {
    try {
      if (!roomName || !roomList.includes(roomName)) return false;
      const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
      this.muteStatus.set(roomName, muteValue);
      this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
      return true;
    } catch { return false; }
  }

  getRoomMute(roomName) {
    try {
      if (!roomName || !roomList.includes(roomName)) return false;
      return this.muteStatus.get(roomName) === true;
    } catch { return false; }
  }

  _addUserConnection(userId, ws) {
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
        oldest._isDuplicate = true;
        try { oldest.close(1000, "Too many connections"); } catch {}
        userConnections.delete(oldest);
      }
    }
    userConnections.add(ws);
  }

  _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    const userConnections = this.userConnections.get(userId);
    if (userConnections) {
      userConnections.delete(ws);
      if (userConnections.size === 0 && !this._pendingReconnections.has(userId)) {
        this.userConnections.delete(userId);
      }
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

  async withLock(resourceId, operation, timeout = CONSTANTS.LOCK_ACQUIRE_TIMEOUT) {
    let release;
    let timeoutId;
    try {
      release = await this.lockManager.acquire(resourceId);
      const timeoutPromise = new Promise((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error(`Lock timeout after ${timeout}ms`)), timeout);
      });
      const result = await Promise.race([operation(), timeoutPromise]);
      clearTimeout(timeoutId);
      return result;
    } catch (error) {
      console.error(`Lock operation failed for ${resourceId}:`, error.message);
      return null;
    } finally {
      if (timeoutId) clearTimeout(timeoutId);
      if (release) {
        try { release(); } catch (e) {}
      }
    }
  }

  async withLockRetry(resourceId, operation, maxRetries = 3) {
    let lastError;
    
    for (let i = 0; i < maxRetries; i++) {
      try {
        return await this.withLock(resourceId, operation);
      } catch (error) {
        lastError = error;
        if (error.message?.includes('timeout') && i < maxRetries - 1) {
          await new Promise(r => setTimeout(r, 100 * (i + 1)));
          continue;
        }
        break;
      }
    }
    
    console.error(`Lock operation failed after ${maxRetries} retries:`, lastError);
    return null;
  }

  checkAndEnableSafeMode() {
    const load = this.getServerLoad();
    if (load > this.loadThreshold && !this.safeMode) this.enableSafeMode();
    else if (load < CONSTANTS.LOAD_RECOVERY_THRESHOLD && this.safeMode) this.disableSafeMode();
  }

  enableSafeMode() {
    if (this.safeMode) return;
    this.safeMode = true;
    this.cleanupQueue.concurrency = 1;
    this._pointFlushDelay = 200;
    setTimeout(() => {
      if (this.getServerLoad() < CONSTANTS.LOAD_RECOVERY_THRESHOLD) this.disableSafeMode();
    }, 60000);
  }

  disableSafeMode() {
    this.safeMode = false;
    this.cleanupQueue.concurrency = 3;
    this._pointFlushDelay = 100;
  }

  getServerLoad() {
    const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
    const queueSize = this.cleanupQueue?.size() || 0;
    const queueLoad = Math.min(queueSize / 50, 0.3);
    return Math.min(activeConnections / 100 + queueLoad, 0.95);
  }

  async savePointWithRetry(room, seat, x, y, fast) {
    try {
      if (seat < 1 || seat > this.MAX_SEATS) return false;
      const xNum = typeof x === 'number' ? x : parseFloat(x);
      const yNum = typeof y === 'number' ? y : parseFloat(y);
      if (isNaN(xNum) || isNaN(yNum)) return false;
      
      await this.updateSeatAtomic(room, seat, (currentSeat) => {
        currentSeat.lastPoint = { 
          x: xNum, 
          y: yNum, 
          fast: fast || false, 
          timestamp: Date.now() 
        };
        return currentSeat;
      });
      
      this.broadcastPointDirect(room, seat, xNum, yNum, fast);
      return true;
      
    } catch (error) {
      this.broadcastPointDirect(room, seat, x, y, fast);
      return false;
    }
  }

  broadcastPointDirect(room, seat, x, y, fast) {
    try {
      if (!room || !roomList.includes(room)) return;
      if (seat < 1 || seat > this.MAX_SEATS) return;
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return;
      const message = JSON.stringify(["pointUpdated", room, seat, x, y, fast]);
      for (const client of clientArray) {
        if (client?.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
          try { client.send(message); } catch {}
        }
      }
    } catch {}
  }

  createDefaultRoom() {
    try {
      const room = "General";
      const seatMap = new Map();
      const occupancyMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
        occupancyMap.set(i, null);
      }
      this.roomSeats.set(room, seatMap);
      this.seatOccupancy.set(room, occupancyMap);
      this.roomClients.set(room, []);
      this.updateKursiBuffer.set(room, new Map());
      this._pointBuffer.set(room, []);
    } catch {}
  }

  initializeRooms() {
    for (const room of roomList) {
      try {
        const seatMap = new Map();
        const occupancyMap = new Map();
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          seatMap.set(i, createEmptySeat());
          occupancyMap.set(i, null);
        }
        this.roomSeats.set(room, seatMap);
        this.seatOccupancy.set(room, occupancyMap);
        this.roomClients.set(room, []);
        this.updateKursiBuffer.set(room, new Map());
        this._pointBuffer.set(room, []);
      } catch {}
    }
  }

  async ensureSeatsData(room) {
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      if (!seatMap || !occupancyMap) return;
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatMap.has(seat)) seatMap.set(seat, createEmptySeat());
        if (!occupancyMap.has(seat)) occupancyMap.set(seat, null);
      }
    } catch {}
  }

  flushRoomKursiUpdates(room) {
    const buffer = this.updateKursiBuffer.get(room);
    if (buffer && buffer.size > 0) {
      const updates = [];
      for (const [seat, info] of buffer.entries()) {
        if (seat >= 1 && seat <= this.MAX_SEATS && info?.namauser) {
          updates.push([seat, info]);
          if (updates.length >= CONSTANTS.BROADCAST_BATCH_SIZE) {
            this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
            updates.length = 0;
          }
        }
      }
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  async updateSeatAtomic(room, seatNumber, updateFn) {
    if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return null;
    
    let retries = 0;
    
    while (retries < CONSTANTS.SEAT_UPDATE_RETRIES) {
      try {
        return await this.withLockRetry(`seat-update-${room}-${seatNumber}`, async () => {
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          if (!seatMap || !occupancyMap) return null;
          
          let currentSeat = seatMap.get(seatNumber);
          if (!currentSeat) {
            currentSeat = createEmptySeat();
            seatMap.set(seatNumber, currentSeat);
          }
          
          const currentVersion = currentSeat._version || 0;
          const updatedSeat = updateFn(currentSeat);
          
          if (updatedSeat._version && updatedSeat._version <= currentVersion) {
            return currentSeat;
          }
          
          updatedSeat._version = currentVersion + 1;
          updatedSeat.lastUpdated = Date.now();
          
          if (updatedSeat.namauser && updatedSeat.namauser !== "") {
            occupancyMap.set(seatNumber, updatedSeat.namauser);
          } else {
            occupancyMap.set(seatNumber, null);
          }
          
          seatMap.set(seatNumber, updatedSeat);
          
          const buffer = this.updateKursiBuffer.get(room);
          if (buffer) {
            if (updatedSeat.namauser) {
              buffer.set(seatNumber, getCompactSeatData(updatedSeat));
              if (buffer.size >= this.bufferSizeLimit) {
                this.flushRoomKursiUpdates(room);
              }
            } else {
              buffer.delete(seatNumber);
            }
          }
          
          return updatedSeat;
        });
      } catch (error) {
        if (error.message?.includes('timeout') && retries < CONSTANTS.SEAT_UPDATE_RETRIES - 1) {
          retries++;
          await new Promise(r => setTimeout(r, CONSTANTS.SEAT_UPDATE_RETRY_DELAY * retries));
          continue;
        }
        throw error;
      }
    }
    return null;
  }

  clearSeatBuffer(room, seatNumber) {
    try {
      if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return;
      const roomMap = this.updateKursiBuffer.get(room);
      if (roomMap) roomMap.delete(seatNumber);
    } catch {}
  }

  findUserSeatInRoom(userId, room) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const seatData = seatMap.get(seat);
      if (seatData?.namauser === userId) {
        return seat;
      }
    }
    return null;
  }

  async reconnectToSeat(ws, room, seatNumber) {
    const seatMap = this.roomSeats.get(room);
    const occupancyMap = this.seatOccupancy.get(room);
    
    if (!seatMap || !occupancyMap) return false;
    
    const seatData = seatMap.get(seatNumber);
    
    if (occupancyMap.get(seatNumber) !== ws.idtarget) return false;
    
    occupancyMap.set(seatNumber, ws.idtarget);
    ws.roomname = room;
    ws.numkursi = new Set([seatNumber]);
    
    const clientArray = this.roomClients.get(room);
    if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
    
    this._addUserConnection(ws.idtarget, ws);
    this.userToSeat.set(ws.idtarget, { room, seat: seatNumber });
    this.userCurrentRoom.set(ws.idtarget, room);
    
    this.sendAllStateTo(ws, room);
    
    if (seatData && seatData.lastPoint) {
      this.safeSend(ws, ["pointUpdated", room, seatNumber, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast]);
    }
    
    this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
    this.updateRoomCount(room);
    
    return true;
  }

  async findEmptySeat(room, ws) {
    if (!room || !ws || !ws.idtarget) return null;
    try {
      const release = await this.lockManager.acquire(`room-find-seat-${room}`);
      try {
        const occupancyMap = this.seatOccupancy.get(room);
        if (!occupancyMap) return null;
        
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          if (occupancyMap.get(i) === ws.idtarget) return i;
        }
        
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          if (occupancyMap.get(i) === null) return i;
        }
        
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const occupantId = occupancyMap.get(i);
          if (occupantId) {
            const isOnline = await this.isUserStillConnected(occupantId);
            if (!isOnline && !this._pendingReconnections.has(occupantId)) {
              await this.cleanupUserFromSeat(room, i, occupantId, true);
              return i;
            }
          }
        }
        return null;
      } finally {
        release();
      }
    } catch { return null; }
  }

  async assignSeatAtomic(room, seat, userId) {
    const release = await this.lockManager.acquire(`atomic-assign-${room}-${seat}`);
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      const seatMap = this.roomSeats.get(room);
      if (!occupancyMap || !seatMap) return false;
      
      const occupantId = occupancyMap.get(seat);
      const seatData = seatMap.get(seat);
      const isStillEmpty = occupantId === null && (!seatData || !seatData.namauser);
      if (!isStillEmpty) return false;
      
      occupancyMap.set(seat, userId);
      
      return true;
    } finally { release(); }
  }

  async cleanupUserFromSeat(room, seatNumber, userId, immediate = true) {
    try {
      if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return;
      await this.withLock(`seat-${room}-${seatNumber}`, async () => {
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        if (!seatMap || !occupancyMap) return;
        
        if (occupancyMap.get(seatNumber) !== userId) return;
        
        const isOnline = await this.isUserStillConnected(userId);
        if (isOnline) {
          return;
        }
        
        if (this._pendingReconnections.has(userId)) {
          return;
        }
        
        if (immediate) {
          occupancyMap.set(seatNumber, null);
          seatMap.set(seatNumber, createEmptySeat());
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.updateRoomCount(room);
          this.userToSeat.delete(userId);
        }
      });
    } catch {}
  }

  scheduleCleanup(userId) {
    if (!userId) return;
    try {
      this.cancelCleanup(userId);
      
      const userData = {
        seatInfo: this.userToSeat.get(userId),
        currentRoom: this.userCurrentRoom.get(userId),
        connections: this.userConnections.get(userId)
      };
      this._pendingReconnections.set(userId, userData);
      
      const LONGER_GRACE_PERIOD = 5000;
      
      const timerId = setTimeout(async () => {
        try {
          this.disconnectedTimers.delete(userId);
          this._pendingReconnections.delete(userId);
          
          const isStillConnected = await this.isUserStillConnected(userId);
          if (!isStillConnected) {
            await this.withLock(`grace-cleanup-${userId}`, async () => {
              const doubleCheck = await this.isUserStillConnected(userId);
              if (!doubleCheck) {
                const finalCheck = this.userConnections.get(userId);
                if (!finalCheck || finalCheck.size === 0) {
                  await this.forceUserCleanup(userId);
                }
              }
            });
          }
        } catch {}
      }, LONGER_GRACE_PERIOD);
      
      timerId._scheduledTime = Date.now();
      timerId._userId = userId;
      this.disconnectedTimers.set(userId, timerId);
    } catch {}
  }

  cancelCleanup(userId) {
    if (!userId) return;
    try {
      const timer = this.disconnectedTimers.get(userId);
      if (timer) { 
        clearTimeout(timer); 
        this.disconnectedTimers.delete(userId);
      }
      this._pendingReconnections.delete(userId);
      this.cleanupInProgress?.delete(userId);
    } catch {}
  }

  async isUserStillConnected(userId) {
    if (!userId) return false;
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return false;
    for (const conn of connections) {
      if (!conn) continue;
      if (conn.readyState !== 1) continue;
      if (conn._isDuplicate || conn._isClosing) continue;
      return true;
    }
    return false;
  }

  async safeQueueAdd(job, fallback) {
    try {
      return await this.cleanupQueue.add(job);
    } catch (error) {
      console.error('Queue failed, using fallback:', error);
      if (fallback) return await fallback();
      return null;
    }
  }

  async forceUserCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    this.cleanupInProgress.add(userId);
    try {
      await this.withLock(`force-cleanup-${userId}`, async () => {
        this.cancelCleanup(userId);
        const currentRoom = this.userCurrentRoom.get(userId);
        const roomsToCheck = currentRoom ? [currentRoom] : roomList;
        const seatsToCleanup = [];
        
        for (const room of roomsToCheck) {
          const seatMap = this.roomSeats.get(room);
          if (!seatMap) continue;
          for (let i = 1; i <= this.MAX_SEATS; i++) {
            const seatInfo = seatMap.get(i);
            if (seatInfo?.namauser === userId) seatsToCleanup.push({ room, seatNumber: i });
          }
        }
        
        const BATCH_SIZE = 5;
        for (let i = 0; i < seatsToCleanup.length; i += BATCH_SIZE) {
          const batch = seatsToCleanup.slice(i, i + BATCH_SIZE);
          await Promise.allSettled(batch.map(({ room, seatNumber }) => 
            this.cleanupUserFromSeat(room, seatNumber, userId, true)
          ));
          if (i + BATCH_SIZE < seatsToCleanup.length) {
            await new Promise(r => setTimeout(r, 10));
          }
        }
        
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        
        const remainingConnections = this.userConnections.get(userId);
        if (remainingConnections) {
          let hasValid = false;
          for (const conn of remainingConnections) {
            if (conn?.readyState === 1 && !conn._isDuplicate && !conn._isClosing) {
              hasValid = true;
              break;
            }
          }
          if (!hasValid) this.userConnections.delete(userId);
        }
        
        const roomClientsSnapshot = Array.from(this.roomClients.entries());
        for (const [room, clientArray] of roomClientsSnapshot) {
          if (clientArray?.length > 0) {
            const filtered = clientArray.filter(c => c?.idtarget !== userId);
            if (filtered.length !== clientArray.length) this.roomClients.set(room, filtered);
          }
        }
      });
    } finally { this.cleanupInProgress.delete(userId); }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    try {
      await this.withLock(`room-cleanup-${room}-${ws.idtarget}`, async () => {
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (seatInfo?.room === room) await this.cleanupUserFromSeat(room, seatInfo.seat, ws.idtarget, true);
        this._removeFromRoomClients(ws, room);
        this._removeUserConnection(ws.idtarget, ws);
        this.userCurrentRoom.delete(ws.idtarget);
        ws.roomname = undefined;
        ws.numkursi = new Set();
        this.userToSeat.delete(ws.idtarget);
        this.updateRoomCount(room);
      });
    } catch {}
  }

  async fullRemoveById(idtarget) {
    if (!idtarget) return;
    try {
      await this.withLock(`full-remove-${idtarget}`, async () => {
        this.cancelCleanup(idtarget);
        const currentRoom = this.userCurrentRoom.get(idtarget);
        const roomsToClean = currentRoom ? [currentRoom] : roomList;
        
        for (const room of roomsToClean) {
          const seatMap = this.roomSeats.get(room);
          if (!seatMap) continue;
          for (let seatNumber = 1; seatNumber <= this.MAX_SEATS; seatNumber++) {
            const info = seatMap.get(seatNumber);
            if (info?.namauser === idtarget) {
              Object.assign(info, createEmptySeat());
              this.clearSeatBuffer(room, seatNumber);
              this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
            }
          }
          this.updateRoomCount(room);
        }
        
        this.userToSeat.delete(idtarget);
        this.userCurrentRoom.delete(idtarget);
        this.userConnections.delete(idtarget);
        this._pendingReconnections.delete(idtarget);
        
        const clientsToRemove = [];
        for (const client of this.clients) {
          if (client?.idtarget === idtarget) clientsToRemove.push(client);
        }
        for (const client of clientsToRemove) {
          if (client.readyState === 1) try { client.close(1000, "Session removed"); } catch {}
          this.clients.delete(client);
          for (const [room, clientArray] of this.roomClients) {
            if (clientArray) {
              const index = clientArray.indexOf(client);
              if (index > -1) clientArray.splice(index, 1);
            }
          }
        }
      });
    } catch {}
  }

  validateGracePeriodTimers() {
    try {
      const now = Date.now();
      const maxGracePeriod = this.gracePeriod + 5000;
      const snapshot = Array.from(this.disconnectedTimers.entries());
      for (const [userId, timer] of snapshot) {
        if (timer?._scheduledTime) {
          if (now - timer._scheduledTime > maxGracePeriod) {
            clearTimeout(timer);
            this.disconnectedTimers.delete(userId);
            this._pendingReconnections.delete(userId);
            this.executeGracePeriodCleanup(userId);
          }
        }
      }
    } catch {}
  }

  async executeGracePeriodCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    this.checkAndEnableSafeMode();
    if (this.safeMode) { setTimeout(() => this.executeGracePeriodCleanup(userId), 5000); return; }
    this.cleanupInProgress.add(userId);
    try {
      await this.withLock(`user-cleanup-${userId}`, async () => {
        const isConnected = await this.isUserStillConnected(userId);
        if (!isConnected) await this.forceUserCleanup(userId);
      });
    } finally { this.cleanupInProgress.delete(userId); }
  }

  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) { this.safeSend(ws, ["error", "User ID not set"]); return false; }
    if (!roomList.includes(room)) { this.safeSend(ws, ["error", "Invalid room"]); return false; }
    if (!this.rateLimiter.check(ws.idtarget)) { this.safeSend(ws, ["error", "Too many requests"]); return false; }
    
    const lockKey = `join-room-${room}`;
    const release = await this.lockManager.acquire(lockKey);
    
    try {
      if (ws.roomname === room) {
        this.sendAllStateTo(ws, room);
        return true;
      }
      
      this.cancelCleanup(ws.idtarget);
      await this.ensureSeatsData(room);
      
      const previousRoom = this.userCurrentRoom.get(ws.idtarget);
      
      const existingSeat = this.findUserSeatInRoom(ws.idtarget, room);
      if (existingSeat) {
        const success = await this.reconnectToSeat(ws, room, existingSeat);
        if (success) return true;
      }
      
      const pendingData = this._pendingReconnections.get(ws.idtarget);
      if (pendingData && pendingData.seatInfo && pendingData.seatInfo.room === room) {
        const { seat } = pendingData.seatInfo;
        const success = await this.reconnectToSeat(ws, room, seat);
        if (success) {
          this._pendingReconnections.delete(ws.idtarget);
          return true;
        }
      }
      
      if (previousRoom === room) {
        this.sendAllStateTo(ws, room);
        this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
        return true;
      } else if (previousRoom) {
        await this.cleanupFromRoom(ws, previousRoom);
      }
      
      let assignedSeat = null;
      let retries = 3;
      
      while (retries > 0 && !assignedSeat) {
        assignedSeat = await this.findEmptySeat(room, ws);
        if (!assignedSeat) {
          await new Promise(r => setTimeout(r, 100));
          retries--;
        }
      }
      
      if (!assignedSeat) { 
        this.safeSend(ws, ["roomFull", room]); 
        return false; 
      }
      
      const success = await this.assignSeatAtomic(room, assignedSeat, ws.idtarget);
      if (!success) {
        this.safeSend(ws, ["error", "Failed to assign seat"]);
        return false;
      }
      
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      ws.numkursi = new Set([assignedSeat]);
      
      const clientArray = this.roomClients.get(room);
      if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
      this._addUserConnection(ws.idtarget, ws);
      
      this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
      
      setTimeout(() => this.sendAllStateTo(ws, room), 100);
      
      this.updateRoomCount(room);
      return true;
      
    } catch (error) {
      console.error(`Join room error for ${ws.idtarget}:`, error);
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally { 
      release(); 
    }
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    
    try {
      let cleanupInfo = null;
      
      await this.withLock(`reconnect-${id}`, async () => {
        const existingConnections = this.userConnections.get(id);
        if (existingConnections && existingConnections.size > 0) {
          const oldWs = Array.from(existingConnections)[0];
          if (oldWs && oldWs !== ws && oldWs.readyState === 1) {
            oldWs._isDuplicate = true;
            oldWs._isClosing = true;
            try {
              this.safeSend(oldWs, ["connectionReplaced", "New connection detected"]);
              oldWs.close(1000, "Replaced by new connection");
            } catch(e) {}
            this.clients.delete(oldWs);
            if (oldWs.roomname) this._removeFromRoomClients(oldWs, oldWs.roomname);
          }
        }
        
        this.cancelCleanup(id);
        
        if (baru === true) {
          ws.idtarget = id;
          ws.roomname = undefined;
          ws.numkursi = new Set();
          ws._connectionTime = Date.now();
          ws._isDuplicate = false;
          ws._isClosing = false;
          this._addUserConnection(id, ws);
          this.safeSend(ws, ["joinroomawal"]);
          cleanupInfo = { needCleanup: true, id };
          return;
        }
        
        ws.idtarget = id;
        ws._connectionTime = Date.now();
        ws._isDuplicate = false;
        ws._isClosing = false;
        
        const existingSeat = this.findUserSeatInRoom(id, this.userCurrentRoom.get(id) || "");
        if (existingSeat && this.userCurrentRoom.get(id)) {
          const room = this.userCurrentRoom.get(id);
          const success = await this.reconnectToSeat(ws, room, existingSeat);
          if (success) return;
        }
        
        const seatInfo = this.userToSeat.get(id);
        if (seatInfo) {
          const { room, seat } = seatInfo;
          if (seat >= 1 && seat <= this.MAX_SEATS) {
            const success = await this.reconnectToSeat(ws, room, seat);
            if (success) return;
          }
          this.userToSeat.delete(id);
          this.userCurrentRoom.delete(id);
          if (seatInfo.room) {
            cleanupInfo = { needCleanup: true, room: seatInfo.room, seat: seatInfo.seat, id };
          }
        }
        
        this._addUserConnection(id, ws);
        this.safeSend(ws, ["needJoinRoom"]);
      });
      
      if (cleanupInfo && cleanupInfo.needCleanup) {
        if (cleanupInfo.room) {
          await this.safeQueueAdd(async () => { 
            await this.cleanupUserFromSeat(cleanupInfo.room, cleanupInfo.seat, cleanupInfo.id, true); 
          }, async () => {
            await this.cleanupUserFromSeat(cleanupInfo.room, cleanupInfo.seat, cleanupInfo.id, true);
          });
        } else if (cleanupInfo.id) {
          await this.safeQueueAdd(async () => { 
            await this.forceUserCleanup(cleanupInfo.id); 
          }, async () => {
            await this.forceUserCleanup(cleanupInfo.id);
          });
        }
      }
      
    } catch (error) {
      console.error("SetIdTarget2 error:", error);
      this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }

  async safeSend(ws, arr, retry = CONSTANTS.SAFE_SEND_RETRY) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing) return false;
      if (ws.bufferedAmount > CONSTANTS.MAX_BUFFERED_AMOUNT) {
        if (retry > 0) {
          await new Promise(r => setTimeout(r, CONSTANTS.SAFE_SEND_RETRY_DELAY));
          return this.safeSend(ws, arr, retry - 1);
        }
        return false;
      }
      ws.send(JSON.stringify(arr));
      return true;
    } catch { return false; }
  }

  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      const clientArray = this.roomClients.get(room);
      if (!clientArray?.length) return 0;
      let sentCount = 0;
      const message = JSON.stringify(msg);
      for (const client of clientArray) {
        if (client?.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
          try { 
            client.send(message); 
            sentCount++; 
          } catch (e) {
            if (e.code === 1001 || e.code === 1006 || e.message?.includes('closed') || e.message?.includes('CLOSED')) {
              setTimeout(() => this._removeFromRoomClients(client, room), 0);
            }
          }
        }
      }
      return sentCount;
    } catch { return 0; }
  }

  broadcastRoomUserCount(room) {
    try {
      if (!room || !roomList.includes(room)) return;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) if (seatMap.get(i)?.namauser) count++;
      if (this.roomCountsCache) this.roomCountsCache[room] = count;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch {}
  }

  sendAllStateTo(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      
      const allKursiMeta = {};
      const lastPointsData = [];
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (info?.namauser) {
          allKursiMeta[seat] = {
            noimageUrl: info.noimageUrl || "",
            namauser: info.namauser,
            color: info.color || "",
            itembawah: info.itembawah || 0,
            itematas: info.itematas || 0,
            vip: info.vip || 0,
            viptanda: info.viptanda || 0
          };
        }
        
        if (info?.lastPoint && info.lastPoint.x !== undefined && info.lastPoint.y !== undefined) {
          lastPointsData.push({ 
            seat: seat, 
            x: info.lastPoint.x, 
            y: info.lastPoint.y, 
            fast: info.lastPoint.fast || false 
          });
        }
      }
      
      if (Object.keys(allKursiMeta).length > 0) {
        this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
      }
      
      if (lastPointsData.length > 0) {
        this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }
      
      const counts = this.getJumlahRoom();
      this.safeSend(ws, ["roomUserCount", room, counts[room] || 0]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
    } catch (error) {
      // Silent fail
    }
  }

  getJumlahRoom() {
    try {
      const now = Date.now();
      if (this.roomCountsCache && this._countsCacheTime && 
          (now - this._countsCacheTime) < this.cacheValidDuration) {
        return this.roomCountsCache;
      }
      const counts = {};
      for (const room of roomList) counts[room] = 0;
      for (const room of roomList) {
        const occupancyMap = this.seatOccupancy.get(room);
        if (!occupancyMap) continue;
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          if (occupancyMap.get(i)) counts[room]++;
        }
      }
      this.roomCountsCache = counts;
      this._countsCacheTime = now;
      return counts;
    } catch {
      const fallback = {};
      for (const room of roomList) fallback[room] = 0;
      return fallback;
    }
  }

  invalidateRoomCache(room) { this.roomCountsCache = null; }

  updateRoomCount(room) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return 0;
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) if (seatMap.get(i)?.namauser) count++;
      this.invalidateRoomCache(room);
      this.broadcastRoomUserCount(room);
      return count;
    } catch { return 0; }
  }

  async fullSeatConsistencyCheck() {
    return;
  }

  sampledSeatConsistencyCheck() {
    return;
  }

  async validateSeatConsistency(room) {
    return;
  }

  async cleanupDuplicateConnections() {
    try {
      const userConnectionCount = new Map();
      for (const client of this.clients) {
        if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
          userConnectionCount.set(client.idtarget, (userConnectionCount.get(client.idtarget) || 0) + 1);
        }
      }
      const duplicateUsers = [];
      for (const [userId, count] of userConnectionCount) if (count > 1) duplicateUsers.push(userId);
      for (const userId of duplicateUsers) {
        await this.withLock(`duplicate-connections-${userId}`, async () => {
          const allConnections = [];
          for (const client of this.clients) {
            if (client?.idtarget === userId && client.readyState === 1 && !client._isClosing) {
              allConnections.push({ client, connectionTime: client._connectionTime || 0 });
            }
          }
          if (allConnections.length <= 1) return;
          allConnections.sort((a, b) => b.connectionTime - a.connectionTime);
          const connectionsToClose = allConnections.slice(1);
          for (const { client } of connectionsToClose) {
            client._isDuplicate = true;
            client._isClosing = true;
            try {
              if (client.readyState === 1) {
                this.safeSend(client, ["duplicateConnection", "Another connection was opened"]);
                client.close(1000, "Duplicate connection");
              }
            } catch(e) {}
            this.clients.delete(client);
            if (client.roomname) this._removeFromRoomClients(client, client.roomname);
            this._removeUserConnection(userId, client);
          }
          const remainingConnections = new Set();
          for (const client of this.clients) {
            if (client?.idtarget === userId && client.readyState === 1) remainingConnections.add(client);
          }
          this.userConnections.set(userId, remainingConnections);
        });
      }
    } catch {}
  }

  getAllOnlineUsers() {
    try {
      const users = [];
      const seenUsers = new Set();
      for (const client of this.clients) {
        if (client?.idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
          if (!seenUsers.has(client.idtarget)) { users.push(client.idtarget); seenUsers.add(client.idtarget); }
        }
      }
      return users;
    } catch { return []; }
  }

  getOnlineUsersByRoom(roomName) {
    try {
      const users = [];
      const seenUsers = new Set();
      const clientArray = this.roomClients.get(roomName);
      if (clientArray) {
        for (const client of clientArray) {
          if (client?.idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
            if (!seenUsers.has(client.idtarget)) { users.push(client.idtarget); seenUsers.add(client.idtarget); }
          }
        }
      }
      return users;
    } catch { return []; }
  }

  flushKursiUpdates() {
    try {
      if (!this.updateKursiBuffer) return;
      for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
        if (!room || !roomList.includes(room)) continue;
        if (seatMapUpdates.size === 0) continue;
        const updates = [];
        for (const [seat, info] of seatMapUpdates.entries()) {
          if (seat < 1 || seat > this.MAX_SEATS) continue;
          if (info?.namauser) {
            updates.push([seat, info]);
            if (updates.length >= CONSTANTS.BROADCAST_BATCH_SIZE) {
              this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
              updates.length = 0;
            }
          }
        }
        if (updates.length > 0) this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
        this.updateKursiBuffer.set(room, new Map());
      }
    } catch {}
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      const clientsToNotify = [];
      const notifiedUsers = new Set();
      for (const client of this.clients) {
        if (client?.readyState === 1 && client.roomname && !client._isDuplicate && !client._isClosing) {
          if (!notifiedUsers.has(client.idtarget)) { clientsToNotify.push(client); notifiedUsers.add(client.idtarget); }
        }
      }
      for (const client of clientsToNotify) this.safeSend(client, ["currentNumber", this.currentNumber]);
    } catch {}
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget || !ws) return;
    try {
      this.withLock(`destroy-${idtarget}`, async () => {
        if (ws.isManualDestroy) {
          await this.safeQueueAdd(async () => { await this.fullRemoveById(idtarget); }, async () => {
            await this.fullRemoveById(idtarget);
          });
        } else {
          const seatInfo = this.userToSeat.get(idtarget);
          if (seatInfo) {
            await this.safeQueueAdd(async () => { await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, idtarget, true); }, async () => {
              await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, idtarget, true);
            });
          }
          this.userToSeat.delete(idtarget);
          this.userCurrentRoom.delete(idtarget);
        }
        this.cancelCleanup(idtarget);
        this._removeUserConnection(idtarget, ws);
        for (const [room, clientArray] of this.roomClients) {
          if (clientArray) {
            const index = clientArray.indexOf(ws);
            if (index > -1) clientArray.splice(index, 1);
          }
        }
        this.clients.delete(ws);
        if (ws.readyState === 1) try { ws.close(1000, "Manual destroy"); } catch {}
      }).catch(() => {});
    } catch {
      try { this.clients.delete(ws); this.cancelCleanup(idtarget); this._removeUserConnection(idtarget, ws); } catch {}
    }
  }

  async safeWebSocketCleanup(ws) {
    if (!ws) return;
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    if (ws._cleanupStarted) return;
    ws._cleanupStarted = true;
    
    try { ws._isClosing = true; } catch(e) {}
    
    setImmediate(async () => {
      try {
        const queue = this._messageQueues.get(ws);
        if (queue) {
          queue.clear();
          this._messageQueues.delete(ws);
        }
        
        this.clients.delete(ws);
        
        if (userId) {
          this._removeUserConnection(userId, ws);
          this.cancelCleanup(userId);
          
          const hasActive = await this.isUserStillConnected(userId);
          if (!hasActive && !this._pendingReconnections.has(userId)) {
            this.scheduleCleanup(userId);
          }
        }
        
        if (room) {
          this._removeFromRoomClients(ws, room);
        }
        
        if (ws.readyState === 1) {
          try {
            ws.close(1000, "Normal closure");
          } catch(e) {}
        }
        
      } catch (error) {
        try { this.clients.delete(ws); } catch(e) {}
        try { if (userId) this.cancelCleanup(userId); } catch(e) {}
      }
    });
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing) return;
    if (ws.idtarget && !this.rateLimiter.check(ws.idtarget)) {
      this.safeSend(ws, ["error", "Too many requests"]);
      return;
    }
    
    try {
      if (raw.length > CONSTANTS.MAX_MESSAGE_SIZE) {
        try { ws.close(1009, "Message too large"); } catch {}
        return;
      }
      
      let data;
      try {
        data = JSON.parse(raw);
        ws.errorCount = 0;
      } catch {
        ws.errorCount = (ws.errorCount || 0) + 1;
        if (ws.errorCount > CONSTANTS.MAX_ERROR_COUNT) try { ws.close(1008, "Protocol error"); } catch {}
        return;
      }
      
      if (!Array.isArray(data) || data.length === 0) return;
      const evt = data[0];
      
      try {
        switch (evt) {
          case "isInRoom": {
            const idtarget = ws.idtarget;
            if (!idtarget) { this.safeSend(ws, ["inRoomStatus", false]); break; }
            const currentRoom = this.userCurrentRoom.get(idtarget);
            this.safeSend(ws, ["inRoomStatus", currentRoom !== undefined]);
            break;
          }
          case "rollangak": {
            const roomName = data[1], username = data[2], angka = data[3];
            if (!roomName || !roomList.includes(roomName)) { this.safeSend(ws, ["error", "Invalid room"]); break; }
            this.broadcastToRoom(roomName, ["rollangakBroadcast", roomName, username, angka]);
            break;
          }
          case "modwarning": {
            const roomName = data[1];
            if (roomName && roomList.includes(roomName)) this.broadcastToRoom(roomName, ["modwarning", roomName]);
            break;
          }
          case "setMuteType": {
            const isMuted = data[1], roomName = data[2];
            if (!roomName || !roomList.includes(roomName)) { this.safeSend(ws, ["error", "Room tidak valid"]); break; }
            const success = this.setRoomMute(roomName, isMuted);
            const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
            this.safeSend(ws, ["muteTypeSet", muteValue, success, roomName]);
            break;
          }
          case "getMuteType": {
            const roomName = data[1];
            if (!roomName || !roomList.includes(roomName)) { this.safeSend(ws, ["error", "Room tidak valid"]); break; }
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(roomName), roomName]);
            break;
          }
          case "onDestroy": { this.handleOnDestroy(ws, ws.idtarget); break; }
          case "setIdTarget2": { await this.handleSetIdTarget2(ws, data[1], data[2]); break; }
          case "sendnotif": {
            const [, idtarget, noimageUrl, username, deskripsi] = data;
            const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
            for (const client of this.clients) {
              if (client?.idtarget === idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
                this.safeSend(client, notif); break;
              }
            }
            break;
          }
          case "private": {
            const [, idt, url, msg, sender] = data;
            const ts = Date.now(), out = ["private", idt, url, msg, ts, sender];
            this.safeSend(ws, out);
            for (const client of this.clients) {
              if (client?.idtarget === idt && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
                this.safeSend(client, out); break;
              }
            }
            break;
          }
          case "isUserOnline": {
            const username = data[1], tanda = data[2] ?? "";
            const isOnline = await this.isUserStillConnected(username);
            this.safeSend(ws, ["userOnlineStatus", username, isOnline, tanda]);
            break;
          }
          case "getAllRoomsUserCount": {
            const allCounts = this.getJumlahRoom();
            const result = roomList.map(room => [room, allCounts[room]]);
            this.safeSend(ws, ["allRoomsUserCount", result]);
            break;
          }
          case "getCurrentNumber": { this.safeSend(ws, ["currentNumber", this.currentNumber]); break; }
          case "getOnlineUsers": { this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]); break; }
          case "getRoomOnlineUsers": {
            const roomName = data[1];
            if (!roomList.includes(roomName)) break;
            this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
            break;
          }
          case "joinRoom": {
            const success = await this.handleJoinRoom(ws, data[1]);
            if (success && ws.roomname) this.updateRoomCount(ws.roomname);
            break;
          }
          case "chat": {
            const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
            if (ws.roomname !== roomname || ws.idtarget !== username) break;
            if (!roomList.includes(roomname)) break;
            
            if (!this.roomRateLimiter.check(roomname, username)) {
              this.safeSend(ws, ["error", "Too many messages in this room"]);
              break;
            }
            
            let isPrimary = true;
            const userConnections = this.userConnections.get(username);
            if (userConnections?.size > 0) {
              let earliest = null;
              for (const conn of userConnections) {
                if (conn?.readyState === 1 && !conn._isClosing) {
                  if (!earliest || (conn._connectionTime || 0) < (earliest._connectionTime || 0)) earliest = conn;
                }
              }
              if (earliest && earliest !== ws) isPrimary = false;
            }
            if (!isPrimary) break;
            const chatMsg = ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor];
            this.broadcastToRoom(roomname, chatMsg);
            break;
          }
          case "updatePoint": {
            const [, room, seat, x, y, fast] = data;
            if (ws.roomname !== room || !roomList.includes(room)) break;
            if (seat < 1 || seat > this.MAX_SEATS) break;
            
            if (!this.seatRateLimiter.canUpdate(room, seat)) {
              break;
            }
            
            const points = this._pointBuffer.get(room);
            if (points) {
              points.push({ seat, x, y, fast, timestamp: Date.now() });
              if (points.length >= CONSTANTS.BUFFER_SIZE_LIMIT) {
                const pointsToSend = [...points];
                points.length = 0;
                this.broadcastToRoom(room, ["batchPoints", room, pointsToSend]);
              }
            }
            
            this.savePointWithRetry(room, seat, x, y, fast).catch(() => {});
            this.broadcastPointDirect(room, seat, x, y, fast);
            break;
          }
          case "removeKursiAndPoint": {
            const [, room, seat] = data;
            if (seat < 1 || seat > this.MAX_SEATS) break;
            if (ws.roomname !== room || !roomList.includes(room)) break;
            await this.updateSeatAtomic(room, seat, () => createEmptySeat());
            this.clearSeatBuffer(room, seat);
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
            break;
          }
          case "updateKursi": {
            const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
            
            if (seat < 1 || seat > this.MAX_SEATS) {
              this.safeSend(ws, ["error", "Invalid seat"]);
              break;
            }
            if (ws.roomname !== room || !roomList.includes(room)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }
            
            // ✅ LOCK khusus untuk update kursi
            const release = await this.lockManager.acquire(`kursi-update-${room}-${seat}`);
            
            try {
              const seatMap = this.roomSeats.get(room);
              const occupancyMap = this.seatOccupancy.get(room);
              
              if (!seatMap || !occupancyMap) {
                this.safeSend(ws, ["error", "Room not found"]);
                break;
              }
              
              // ✅ CEK: Apakah seat ini milik user yang mengupdate?
              if (occupancyMap.get(seat) !== ws.idtarget) {
                this.safeSend(ws, ["error", "Not your seat"]);
                break;
              }
              
              // ✅ AMBIL data seat saat ini atau buat baru
              let seatData = seatMap.get(seat);
              if (!seatData || !seatData.namauser) {
                seatData = createEmptySeat();
                seatData.namauser = ws.idtarget;
                seatMap.set(seat, seatData);
              }
              
              // ✅ UPDATE data dari client
              seatData.noimageUrl = noimageUrl || "";
              seatData.namauser = namauser || ws.idtarget;
              seatData.color = color || "";
              seatData.itembawah = parseInt(itembawah) || 0;
              seatData.itematas = parseInt(itematas) || 0;
              seatData.vip = parseInt(vip) || 0;
              seatData.viptanda = parseInt(viptanda) || 0;
              seatData.lastUpdated = Date.now();
              seatData._version = (seatData._version || 0) + 1;
              
              // ✅ PASTIKAN occupancy sesuai dengan namauser terbaru
              occupancyMap.set(seat, seatData.namauser);
              
              // ✅ SIMPAN ke buffer untuk batch update
              const buffer = this.updateKursiBuffer.get(room);
              if (buffer) {
                buffer.set(seat, getCompactSeatData(seatData));
                if (buffer.size >= this.bufferSizeLimit) {
                  this.flushRoomKursiUpdates(room);
                }
              }
              
              // ✅ UPDATE relasi user ke seat
              if (namauser === ws.idtarget || seatData.namauser === ws.idtarget) {
                this.userToSeat.set(ws.idtarget, { room, seat });
                this.userCurrentRoom.set(ws.idtarget, room);
              }
              
              // ✅ UPDATE room count
              this.updateRoomCount(room);
              
              // ✅ BROADCAST ke semua user di room
              const response = ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda];
              const clientArray = this.roomClients.get(room);
              
              if (clientArray && clientArray.length > 0) {
                const message = JSON.stringify(response);
                for (const client of clientArray) {
                  if (client?.readyState === 1 && client.roomname === room && 
                      !client._isDuplicate && !client._isClosing) {
                    try { 
                      client.send(message); 
                    } catch(e) {}
                  }
                }
              }
              
              // ✅ KONFIRMASI ke pengirim
              this.safeSend(ws, response);
              
            } catch (error) {
              console.error("Update kursi error:", error);
              this.safeSend(ws, ["error", "Failed to update seat"]);
            } finally {
              release();
            }
            break;
          }
          case "gift": {
            const [, roomname, sender, receiver, giftName] = data;
            if (ws.roomname !== roomname || ws.idtarget !== sender) break;
            if (!roomList.includes(roomname)) break;
            const giftData = ["gift", roomname, sender, receiver, giftName, Date.now()];
            this.broadcastToRoom(roomname, giftData);
            break;
          }
          case "leaveRoom": {
            const room = ws.roomname;
            if (!room || !roomList.includes(room)) break;
            await this.cleanupFromRoom(ws, room);
            this.updateRoomCount(room);
            this.safeSend(ws, ["roomLeft", room]);
            break;
          }
          case "gameLowCardStart":
          case "gameLowCardJoin":
          case "gameLowCardNumber":
          case "gameLowCardEnd":
            if (["LowCard 1", "LowCard 2", "Noxxeliverothcifsa", "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love","Heart Lovers"].includes(ws.roomname)) {
              if (this.lowcard) {
                try {
                  await this.lowcard.handleEvent(ws, data);
                } catch (error) {
                  console.error("Game handler error:", error);
                  this.safeSend(ws, ["error", "Game error"]);
                }
              } else {
                this.safeSend(ws, ["error", "Game system not available"]);
              }
            }
            break;
          default: break;
        }
        
        this.flushKursiUpdates();
        
      } catch (error) {
        console.error("Message handling error:", error);
        if (ws.readyState === 1) this.safeSend(ws, ["error", "Server error"]);
      }
    } catch {}
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      
      if (this._isShuttingDown) {
        return new Response("Server is shutting down", { status: 503 });
      }
      
      const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
      if (activeConnections > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      await server.accept();
      
      const ws = server;
      ws._connId = `conn#${this._nextConnId++}`;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();
      ws.isManualDestroy = false;
      ws.errorCount = 0;
      ws._isDuplicate = false;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      ws._cleanupStarted = false;
      
      this.clients.add(ws);
      this._lastActivityTime = Date.now();
      
      const messageQueue = new MessageQueue(ws, this);
      this._messageQueues.set(ws, messageQueue);
      
      ws.addEventListener("message", (ev) => {
        messageQueue.add(ev.data).catch(() => {});
      });
      
      ws.addEventListener("error", () => {});
      
      ws.addEventListener("close", async (event) => {
        try {
          if (event.code !== 1000 || event.reason !== "Replaced by new connection") {
            await this.safeWebSocketCleanup(ws);
          } else {
            this.clients.delete(ws);
            if (ws.idtarget) this._removeUserConnection(ws.idtarget, ws);
            if (ws.roomname) this._removeFromRoomClients(ws, ws.roomname);
          }
        } catch (error) {
          this.clients.delete(ws);
          if (ws.idtarget) {
            this._removeUserConnection(ws.idtarget, ws);
            this.cancelCleanup(ws.idtarget);
          }
        }
      });
      
      return new Response(null, { status: 101, webSocket: client });
    } catch {
      return new Response("Internal server error", { status: 500 });
    }
  }
  
  async cleanup() {
    try {
      await this.performMemoryCleanup();
    } catch (error) {}
    try {
      await this.cleanupDuplicateConnections();
    } catch (error) {}
    try {
      this.validateGracePeriodTimers();
    } catch (error) {}
    try {
      this.sampledSeatConsistencyCheck();
    } catch (error) {}
    try {
      await this.fullSeatConsistencyCheck();
    } catch (error) {}
  }
  
  async getHealthStatus() {
    const now = Date.now();
    const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
    
    let totalPoints = 0;
    for (const points of this._pointBuffer.values()) {
      totalPoints += points.length;
    }
    
    const memUsage = process.memoryUsage();
    const heapPercent = memUsage.heapUsed / memUsage.heapTotal;
    const heapUsedMB = memUsage.heapUsed / 1024 / 1024;
    
    const criticalIssues = [];
    if (heapUsedMB > CONSTANTS.MEMORY_CRITICAL_THRESHOLD) criticalIssues.push("high_memory");
    if (totalPoints > 1000) criticalIssues.push("point_buffer_backlog");
    if (this.cleanupQueue?.size() > 200) criticalIssues.push("queue_backlog");
    
    return {
      status: criticalIssues.length > 0 ? "degraded" : "healthy",
      uptime: now - this._startTime,
      activeConnections,
      memoryPressure: heapPercent,
      memoryUsedMB: heapUsedMB,
      totalPointsBuffered: totalPoints,
      queueHealth: {
        size: this.cleanupQueue?.size() || 0,
        active: this.cleanupQueue?.active || 0,
        healthy: (this.cleanupQueue?.size() || 0) < 100
      },
      bufferHealth: {
        totalBuffers: this.updateKursiBuffer.size,
        healthy: true
      },
      criticalIssues,
      failedBatches: this._failedBatches?.length || 0,
      messageQueues: this._messageQueues.size,
      pendingReconnections: this._pendingReconnections.size
    };
  }
}

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      
      if (url.pathname === "/cleanup") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        await obj.cleanup();
        return new Response("Cleanup completed", { status: 200 });
      }
      
      if (url.pathname === "/destroy") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        await obj.destroy();
        return new Response("Destroy completed", { status: 200 });
      }
      
      if (url.pathname === "/shutdown") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        await obj.gracefulShutdown();
        return new Response("Shutdown initiated", { status: 200 });
      }
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      
      if (url.pathname === "/health") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        const health = await obj.getHealthStatus();
        const statusCode = health.status === "healthy" ? 200 : 503;
        return new Response(JSON.stringify(health), { 
          status: statusCode, 
          headers: { "content-type": "application/json", "cache-control": "no-cache" } 
        });
      }
      
      if (url.pathname === "/metrics") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        const activeConnections = Array.from(obj.clients || []).filter(c => c?.readyState === 1).length;
        let totalPoints = 0;
        if (obj._pointBuffer) {
          for (const points of obj._pointBuffer.values()) {
            totalPoints += points.length;
          }
        }
        const memUsage = process.memoryUsage();
        const metrics = {
          status: "healthy",
          activeConnections,
          totalClients: obj.clients?.size || 0,
          activeGames: obj.lowcard?.activeGames?.size || 0,
          queueSize: obj.cleanupQueue?.size() || 0,
          bufferSize: obj.updateKursiBuffer?.size || 0,
          pointsBuffered: totalPoints,
          failedBatches: obj._failedBatches?.length || 0,
          messageQueues: obj._messageQueues?.size || 0,
          pendingReconnections: obj._pendingReconnections?.size || 0,
          memoryHeapUsedMB: memUsage.heapUsed / 1024 / 1024,
          timestamp: Date.now()
        };
        return new Response(JSON.stringify(metrics), {
          headers: { "content-type": "application/json" }
        });
      }
      
      return new Response("WebSocket endpoint", { status: 200, headers: { "content-type": "text/plain" } });
      
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
