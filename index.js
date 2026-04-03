import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", 
  "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love", 
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const CONSTANTS = {
  MAX_QUEUE_SIZE: 200,
  MAX_LOCK_QUEUE_SIZE: 100,
  LOCK_ACQUIRE_TIMEOUT: 3000,        // FIX: 5000 → 3000
  BUFFER_SIZE_LIMIT: 20,
  BROADCAST_BATCH_SIZE: 20,
  CACHE_VALID_DURATION: 5000,
  LOAD_THRESHOLD: 0.85,
  LOAD_RECOVERY_THRESHOLD: 0.65,
  KURSI_UPDATE_DEBOUNCE: 150,
  DEBOUNCE_CLEANUP_INTERVAL: 30000,
  MAX_DEBOUNCE_AGE: 20000,
  LOCK_TIMEOUT: 5000,                // FIX: 10000 → 5000
  GRACE_PERIOD: 5000,
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 3,
  SAFE_SEND_RETRY: 2,
  SAFE_SEND_RETRY_DELAY: 100,
  MAX_MESSAGE_SIZE: 50000,
  MAX_ERROR_COUNT: 3,
  MAX_BUFFERED_AMOUNT: 300000,
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  MAX_NUMBER: 6,
  MAX_RETRIES: 5,                    // FIX: 3 → 5
  MAX_GLOBAL_CONNECTIONS: 500,
  FORCED_CLEANUP_INTERVAL: 60000,
  MAX_TIMER_AGE: 30000,
  MAX_MESSAGE_QUEUE_SIZE: 100,
  MESSAGES_PER_SECOND_LIMIT: 5,
  MAX_BUFFER_PER_ROOM: 100,
  EMERGENCY_MEMORY_THRESHOLD: 0.80,  // FIX: 0.85 → 0.80
  EMERGENCY_CLEANUP_INTERVAL: 5000,
  STORAGE_TIMEOUT: 5000,             // FIX: 3000 → 5000
  MAX_FAILED_BATCHES: 100,
  MAX_QUEUE_HARD_LIMIT: 500,
  MAX_USER_DATA_SIZE: 50000,
  MAX_USER_TO_SEAT_SIZE: 500,
  MAX_DISCONNECTED_TIMERS: 200,
  MAX_RECURSIVE_BATCH: 100,
  MAX_CONCURRENT_ROOM_OPS: 5,
  STORAGE_RETRY_DELAY_BASE: 100,
  STORAGE_RETRY_MAX_DELAY: 1000,
  HEARTBEAT_INTERVAL: 30000,
  HEARTBEAT_TIMEOUT: 60000,
  MAX_RESTART_ATTEMPTS: 3,
  RESTART_COOLDOWN: 60000,
  STORAGE_FAILURE_THRESHOLD: 10      // NEW: untuk circuit breaker
};

// Enhanced error logging (tidak menyembunyikan error kritis)
const originalConsoleError = console.error;
console.error = (...args) => {
  const msg = args.join(' ');
  const nonCriticalErrors = [
    'WebSocket closed',
    'Connection closed',
    'ERR_STREAM_WRITE_AFTER_END',
    'ECONNRESET',
    'bufferedAmount'
  ];
  
  const isNonCritical = nonCriticalErrors.some(err => msg.includes(err));
  
  if (!isNonCritical) {
    originalConsoleError.apply(console, [
      `[${new Date().toISOString()}] ERROR:`,
      ...args
    ]);
  }
};

class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
    this.lockTimestamps = new Map();
    this._destroyed = false;
    this._lockId = 0;
    this._metrics = { acquired: 0, released: 0, timeouts: 0 };
  }

  async acquire(resourceId) {
    if (this._destroyed) throw new Error("LockManager destroyed");
    
    const lockId = ++this._lockId;
    const startTime = Date.now();
    
    if (this.locks.has(resourceId)) {
      const lockTime = this.lockTimestamps.get(resourceId) || 0;
      if (Date.now() - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        console.warn(`Force releasing stuck lock: ${resourceId}, age: ${Date.now() - lockTime}ms`);
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
      this._metrics.acquired++;
      return () => this.release(resourceId, lockId);
    }

    if (!this.queue.has(resourceId)) this.queue.set(resourceId, []);

    return new Promise((resolve, reject) => {
      let resolved = false;
      
      // FIX: Gunakan CONSTANTS.LOCK_ACQUIRE_TIMEOUT
      const timeoutId = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          this._metrics.timeouts++;
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
            this._metrics.acquired++;
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
    
    this._metrics.released++;
    const queue = this.queue.get(resourceId);
    
    if (queue && queue.length > 0) {
      const next = queue.shift();
      if (next.timeoutId) clearTimeout(next.timeoutId);
      this.lockTimestamps.set(resourceId, Date.now());
      
      setTimeout(() => {
        try {
          next.resolve();
        } catch (error) {
          console.error(`Error in lock release callback for ${resourceId}:`, error);
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
        try {
          if (item.reject) item.reject(new Error(`Lock force released: ${resourceId}`));
        } catch (e) {}
      }
    }
  }

  cleanupStuckLocks() {
    const now = Date.now();
    let cleaned = 0;
    for (const [resourceId, lockTime] of this.lockTimestamps) {
      if (now - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
        cleaned++;
      }
    }
    if (cleaned > 0) {
      console.warn(`Cleaned up ${cleaned} stuck locks`);
    }
  }
  
  getMetrics() {
    return { ...this._metrics, pending: this.queue.size, active: this.locks.size };
  }
  
  destroy() {
    this._destroyed = true;
    for (const [resourceId, queue] of this.queue) {
      if (queue) {
        for (const item of queue) {
          if (item.timeoutId) clearTimeout(item.timeoutId);
          if (item.reject) {
            try { item.reject(new Error("Lock manager destroyed")); } catch {}
          }
        }
      }
    }
    this.locks.clear();
    this.queue.clear();
    this.lockTimestamps.clear();
  }
}

class QueueManager {
  constructor(concurrency = 3) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = CONSTANTS.MAX_QUEUE_SIZE;
    this.processing = false;
    this.destroyed = false;
    this._processedCount = 0;
    this._metrics = { processed: 0, failed: 0, timedout: 0 };
  }

  async add(job, retryCount = 0) {
    if (this.destroyed) throw new Error("Queue manager destroyed");
    
    if (this.queue.length >= CONSTANTS.MAX_QUEUE_HARD_LIMIT) {
      if (retryCount < 3) {
        const delay = 100 * Math.pow(2, retryCount);
        await new Promise(r => setTimeout(r, delay));
        return this.add(job, retryCount + 1);
      }
      this._metrics.failed++;
      throw new Error("Server busy - queue full after retries");
    }
    
    if (this.queue.length > this.maxQueueSize) {
      const expiredCount = Math.floor(this.queue.length * 0.3);
      for (let i = 0; i < expiredCount; i++) {
        const expired = this.queue.shift();
        if (expired.timeoutId) clearTimeout(expired.timeoutId);
        if (expired.reject) {
          try { expired.reject(new Error("Queue cleared - overload")); } catch {}
        }
      }
      this._metrics.timedout += expiredCount;
    }
    
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const index = this.queue.findIndex(item => item.reject === reject);
        if (index !== -1) {
          const [item] = this.queue.splice(index, 1);
          if (item.timeoutId) clearTimeout(item.timeoutId);
          this._metrics.timedout++;
          reject(new Error("Queue timeout"));
        }
      }, 5000);  // FIX: 10000 → 5000
      
      this.queue.push({ job, resolve, reject, timeoutId, timestamp: Date.now() });
      if (!this.processing) this.process();
    });
  }

  async process() {
    if (this.processing || this.destroyed) return;
    this.processing = true;
    this._processedCount = 0;
    
    try {
      const processNext = async () => {
        while (this.queue.length > 0 && this.active < this.concurrency && 
               !this.destroyed && this._processedCount < CONSTANTS.MAX_RECURSIVE_BATCH) {
          try {
            while (this.queue.length > 0 && Date.now() - this.queue[0].timestamp > 30000) {
              const expired = this.queue.shift();
              if (expired.timeoutId) clearTimeout(expired.timeoutId);
              if (expired.reject) {
                try { expired.reject(new Error("Request timeout")); } catch {}
              }
              this._metrics.timedout++;
            }
            if (this.queue.length === 0) break;
            
            this.active++;
            this._processedCount++;
            const { job, resolve, reject, timeoutId } = this.queue.shift();
            if (timeoutId) clearTimeout(timeoutId);
            
            try {
              // FIX: Queue timeout 5000ms
              const result = await Promise.race([
                job(),
                new Promise((_, reject) => 
                  setTimeout(() => reject(new Error("Job timeout after 5s")), 5000)
                )
              ]);
              if (resolve && !this.destroyed) {
                try { resolve(result); this._metrics.processed++; } catch (e) {}
              }
            } catch (error) {
              this._metrics.failed++;
              if (reject && !this.destroyed) {
                try { reject(error); } catch (e) {}
              }
            } finally {
              this.active--;
            }
          } catch (error) {
            this.active--;
            this._metrics.failed++;
          }
        }
      };
      
      await processNext();
    } catch (error) {
      console.error("Queue processing error:", error);
    } finally {
      this.processing = false;
      this._processedCount = 0;
      
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
        try { item.reject(new Error("Queue cleared")); } catch {}
      }
    }
  }

  size() { return this.queue.length; }
  getMetrics() { return { ...this._metrics, queueSize: this.queue.length, active: this.active }; }
  
  destroy() {
    this.destroyed = true;
    this.clear();
    this.active = 0;
    this.processing = false;
  }
}

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
      if (recentRequests.length === 0) {
        this.requests.delete(userId);
      } else {
        this.requests.set(userId, recentRequests);
      }
    }
  }
  
  destroy() {
    this.requests.clear();
  }
}

class RoomRateLimiter {
  constructor() {
    this.roomLimits = new Map();
  }
  
  check(room, userId) {
    if (!room || !userId) return true;
    
    if (!this.roomLimits.has(room)) {
      this.roomLimits.set(room, new Map());
    }
    
    const userLimits = this.roomLimits.get(room);
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
}

class RoomOperationLimiter {
  constructor() {
    this.roomOps = new Map();
  }
  
  check(room, operationType) {
    if (!room) return true;
    const now = Date.now();
    const ops = this.roomOps.get(room) || [];
    const recentOps = ops.filter(t => now - t < 100);
    
    if (recentOps.length >= 10) {
      return false;
    }
    
    recentOps.push(now);
    this.roomOps.set(room, recentOps);
    return true;
  }
  
  cleanup() {
    const now = Date.now();
    for (const [room, timestamps] of this.roomOps) {
      const recent = timestamps.filter(t => now - t < 1000);
      if (recent.length === 0) {
        this.roomOps.delete(room);
      } else {
        this.roomOps.set(room, recent);
      }
    }
  }
}

class MessageQueue {
  constructor(ws, chatServer) {
    this.ws = ws;
    this.chatServer = chatServer;
    this.queue = [];
    this.processing = false;
    this.maxSize = CONSTANTS.MAX_MESSAGE_QUEUE_SIZE;
    this._metrics = { processed: 0, dropped: 0 };
  }
  
  async add(rawMessage) {
    // FIX: Better overflow protection
    if (this.queue.length >= this.maxSize) {
      this._metrics.dropped++;
      // Clean up old messages if queue is too large
      if (this.queue.length > this.maxSize * 1.5) {
        this.queue = this.queue.slice(-Math.floor(this.maxSize / 2));
      }
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
        this._metrics.processed++;
      } catch (error) {
        // Error already handled in handleMessage
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
  
  getMetrics() {
    return { ...this._metrics, queueLength: this.queue.length };
  }
}

class CircuitBreaker {
  // FIX: Ubah default failureThreshold dari 3 ke 10
  constructor(failureThreshold = CONSTANTS.STORAGE_FAILURE_THRESHOLD, timeout = 60000) {
    this.failures = 0;
    this.failureThreshold = failureThreshold;
    this.timeout = timeout;
    this.lastFailureTime = 0;
    this.state = 'CLOSED';
    this._metrics = { totalCalls: 0, failedCalls: 0, rejectedCalls: 0 };
  }
  
  async call(fn) {
    this._metrics.totalCalls++;
    
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailureTime > this.timeout) {
        console.log('Circuit breaker transitioning from OPEN to HALF_OPEN');
        this.state = 'HALF_OPEN';
      } else {
        this._metrics.rejectedCalls++;
        throw new Error('Circuit breaker is OPEN - service unavailable');
      }
    }
    
    try {
      const result = await fn();
      if (this.state === 'HALF_OPEN') {
        console.log('Circuit breaker transitioning from HALF_OPEN to CLOSED');
        this.state = 'CLOSED';
        this.failures = 0;
      }
      return result;
    } catch (error) {
      this._metrics.failedCalls++;
      this.failures++;
      this.lastFailureTime = Date.now();
      
      if (this.failures >= this.failureThreshold) {
        console.error(`Circuit breaker opening after ${this.failures} failures`);
        this.state = 'OPEN';
      }
      throw error;
    }
  }
  
  reset() {
    this.failures = 0;
    this.state = 'CLOSED';
    this._metrics = { totalCalls: 0, failedCalls: 0, rejectedCalls: 0 };
  }
  
  getMetrics() {
    return { state: this.state, ...this._metrics };
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

export class ChatServer {
  constructor(state, env) {
    console.log(`[${new Date().toISOString()}] ChatServer initializing...`);
    
    try {
      this.state = state;
      this.env = env;
      this._startTime = Date.now();
      this._lastActivityTime = Date.now();
      this._failedBatches = [];
      this._isShuttingDown = false;
      this._messageQueues = new Map();
      this._pendingReconnections = new Map();
      this.userDataSize = new Map();
      this._restartAttempts = 0;
      this._lastRestartTime = 0;
      
      this.muteStatus = new Map();
      for (const room of roomList) this.muteStatus.set(room, false);
      
      this.storage = state?.storage;
      // FIX: Circuit breaker dengan threshold 10 (bukan 3)
      this.storageCircuitBreaker = new CircuitBreaker(CONSTANTS.STORAGE_FAILURE_THRESHOLD, 60000);
      
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
      this.roomSemaphore = new Map();

      this._pointBuffer = new Map();
      this._pointFlushDelay = 100;
      this._hasBufferedUpdates = false;

      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      this.roomRateLimiter = new RoomRateLimiter();
      this.roomOperationLimiter = new RoomOperationLimiter();
      
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;

      try { 
        this.lowcard = new LowCardGameManager(this); 
        console.log("LowCardGameManager initialized successfully");
      } catch (error) { 
        console.error("Failed to initialize LowCardGameManager:", error);
        this.lowcard = null; 
      }

      this.gracePeriod = CONSTANTS.GRACE_PERIOD;
      this.disconnectedTimers = new Map();
      this.cleanupQueue = new QueueManager(3);
      
      this.currentNumber = 1;
      this.maxNumber = CONSTANTS.MAX_NUMBER;
      this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
      this._nextConnId = 1;
      this.lastNumberTick = Date.now();
      
      this.numberTickTimer = null;
      this._heartbeatInterval = null;
      this._intervals = [];

      try { 
        this.initializeRooms(); 
      } catch (error) { 
        console.error("Failed to initialize rooms:", error);
        this.createDefaultRoom(); 
      }

      this.startNumberTickTimer();
      this.startHeartbeat();
      
      this.roomCountsCache = null;
      this._countsCacheTime = 0;
      this.cacheValidDuration = CONSTANTS.CACHE_VALID_DURATION;

      for (const room of roomList) {
        this._pointBuffer.set(room, []);
      }

      this.loadState().catch(error => {
        console.error("Failed to load state:", error);
      });
      
      this.startAutoCleanup();
      this.startIdleCleanup();
      this.startMemoryMonitor();
      this.startForcedCleanup();
      this.startEmergencyCleanup();
      this.startPeriodicFlush();
      
      console.log(`[${new Date().toISOString()}] ChatServer initialized successfully`);
      
    } catch (error) {
      console.error("ChatServer constructor error:", error);
      this.initializeFallback();
    }
  }

  startHeartbeat() {
    this._heartbeatInterval = setInterval(() => {
      try {
        const now = Date.now();
        const heartbeatMsg = JSON.stringify(["heartbeat", now]);
        
        for (const client of this.clients) {
          if (client.readyState === 1 && !client._isClosing) {
            try {
              client.send(heartbeatMsg);
              client._lastHeartbeat = now;
            } catch (error) {
              // Silent fail
            }
          }
        }
        
        // FIX: Check for stale connections and close them
        for (const client of this.clients) {
          if (client.readyState === 1 && client._lastHeartbeat && 
              (now - client._lastHeartbeat) > CONSTANTS.HEARTBEAT_TIMEOUT) {
            console.warn(`Closing stale connection: ${client._connId}`);
            try { client.close(1000, "Heartbeat timeout"); } catch {}
          }
        }
      } catch (error) {
        // Silent fail
      }
    }, CONSTANTS.HEARTBEAT_INTERVAL);
    this._intervals.push(this._heartbeatInterval);
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
        
        // FIX: Ubah threshold dari 0.95 ke 0.80 (Cloudflare Worker limit)
        if (heapPercent > 0.80) {
          console.error(`CRITICAL: Memory pressure at ${(heapPercent * 100).toFixed(1)}%`);
          
          // Emergency cleanup - clear all buffers (lebih agresif)
          for (const [room, buffer] of this.updateKursiBuffer) {
            if (buffer) {
              buffer.clear();
            }
          }
          this.userDataSize.clear();
          this._messageQueues.clear();
          
          // FIX: Hapus global.gc() - tidak tersedia di Cloudflare Workers
          // if (global.gc) {
          //   try { global.gc(); } catch {}
          // }
        }
        else if (heapPercent > 0.75) {
          console.warn(`High memory pressure: ${(heapPercent * 100).toFixed(1)}%`);
          
          for (const [room, buffer] of this.updateKursiBuffer) {
            if (buffer && buffer.size > 30) {  // Kurangi threshold
              const entries = Array.from(buffer.entries());
              const limited = new Map(entries.slice(-30));
              this.updateKursiBuffer.set(room, limited);
            }
          }
        }
        
        // Limit map sizes
        if (this.userToSeat.size > CONSTANTS.MAX_USER_TO_SEAT_SIZE) {
          const entries = Array.from(this.userToSeat.entries());
          const toDelete = entries.slice(0, this.userToSeat.size - CONSTANTS.MAX_USER_TO_SEAT_SIZE);
          for (const [userId] of toDelete) {
            if (!this.isUserStillConnected(userId)) {
              this.userToSeat.delete(userId);
            }
          }
        }
        
        if (this.disconnectedTimers.size > CONSTANTS.MAX_DISCONNECTED_TIMERS) {
          const oldest = Array.from(this.disconnectedTimers.keys())
            .slice(0, this.disconnectedTimers.size - CONSTANTS.MAX_DISCONNECTED_TIMERS);
          for (const userId of oldest) {
            this.cancelCleanup(userId);
          }
        }
        
        if (this._failedBatches && this._failedBatches.length > CONSTANTS.MAX_FAILED_BATCHES) {
          this._failedBatches = this._failedBatches.slice(-Math.floor(CONSTANTS.MAX_FAILED_BATCHES / 2));
        }
        
      } catch(e) {
        console.error("Emergency cleanup error:", e);
      }
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
        this.roomOperationLimiter.cleanup();
        this.rateLimiter.cleanup();
        this.connectionRateLimiter.cleanup();
        
      } catch (e) {
        console.error("Forced cleanup error:", e);
      }
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
        const activeConns = Array.from(this.clients).filter(c => c?.readyState === 1).length;
        const queueSize = this.cleanupQueue?.size() || 0;
        
        const memUsage = process.memoryUsage();
        const heapPercent = memUsage.heapUsed / memUsage.heapTotal;
        
        if (Math.random() < 0.1) {
          console.log(`Metrics - Connections: ${activeConns}, Queue: ${queueSize}, Memory: ${(heapPercent * 100).toFixed(1)}%`);
        }
        
        if (heapPercent > CONSTANTS.EMERGENCY_MEMORY_THRESHOLD) {
          this.flushKursiUpdates();
        }
      } catch (e) {}
    }, 30000);
    this._intervals.push(this._memoryMonitor);
  }

  startAutoCleanup() {
    const autoCleanupInterval = setInterval(async () => {
      try {
        await this.cleanup();
      } catch (error) {
        console.error("Auto cleanup error:", error);
      }
    }, 300000);
    this._intervals.push(autoCleanupInterval);
  }

  async gracefulShutdown() {
    if (this._isShuttingDown) return;
    this._isShuttingDown = true;
    
    console.log(`[${new Date().toISOString()}] Graceful shutdown initiated...`);
    
    const shutdownMsg = JSON.stringify(["serverShutdown", "Server is restarting"]);
    for (const client of this.clients) {
      if (client.readyState === 1) {
        try {
          client.send(shutdownMsg);
        } catch(e) {}
      }
    }
    
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    this.flushKursiUpdates();
    
    await this.saveState();
    
    for (const client of this.clients) {
      try {
        client.close(1000, "Server shutdown");
      } catch(e) {}
    }
    
    await this.destroy();
    
    console.log(`[${new Date().toISOString()}] Graceful shutdown completed`);
  }

  async destroy() {
    console.log(`[${new Date().toISOString()}] Destroying ChatServer...`);
    
    if (this._intervals) {
      for (const interval of this._intervals) {
        clearInterval(interval);
      }
      this._intervals = [];
    }
    if (this.numberTickTimer) {
      clearTimeout(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
      this._heartbeatInterval = null;
    }
    if (this._idleCheckInterval) {
      clearInterval(this._idleCheckInterval);
      this._idleCheckInterval = null;
    }
    if (this._memoryMonitor) {
      clearInterval(this._memoryMonitor);
      this._memoryMonitor = null;
    }
    if (this._forcedCleanupInterval) {
      clearInterval(this._forcedCleanupInterval);
      this._forcedCleanupInterval = null;
    }
    if (this._emergencyCleanup) {
      clearInterval(this._emergencyCleanup);
      this._emergencyCleanup = null;
    }
    if (this._periodicFlush) {
      clearInterval(this._periodicFlush);
      this._periodicFlush = null;
    }
    
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
    this.userDataSize.clear();
    this.roomSemaphore.clear();
    
    console.log(`[${new Date().toISOString()}] ChatServer destroyed`);
  }

  async loadState() {
    let lastError = null;
    
    try {
      if (this.storage) {
        // FIX: Tambah retry menjadi 5 kali
        for (let retry = 0; retry < 5; retry++) {
          try {
            await this.storageCircuitBreaker.call(async () => {
              const results = await Promise.race([
                Promise.all([
                  this.storage.get("currentNumber"),
                  this.storage.get("lastNumberTick")
                ]),
                new Promise((_, reject) => 
                  setTimeout(() => reject(new Error("Storage timeout")), CONSTANTS.STORAGE_TIMEOUT)
                )
              ]);
              
              if (results && results[0] !== undefined && results[0] !== null) {
                this.currentNumber = results[0];
              }
              if (results && results[1] !== undefined && results[1] !== null) {
                this.lastNumberTick = results[1];
              }
            });
            
            console.log(`State loaded: currentNumber=${this.currentNumber}`);
            return;
          } catch (error) {
            lastError = error;
            
            if (retry < 4) {
              const delay = Math.min(
                CONSTANTS.STORAGE_RETRY_DELAY_BASE * Math.pow(2, retry),
                CONSTANTS.STORAGE_RETRY_MAX_DELAY
              );
              console.warn(`Storage loadState failed (attempt ${retry + 1}/5), retrying in ${delay}ms:`, error.message);
              await new Promise(r => setTimeout(r, delay));
              continue;
            }
            
            throw error;
          }
        }
      }
    } catch (error) {
      console.error(`Storage loadState failed after 5 attempts:`, error.message);
      // FIX: Fallback ke default values
      console.warn("Using default values for state");
      this.currentNumber = 1;
      this.lastNumberTick = Date.now();
    }
  }

  async saveState() {
    let lastError = null;
    
    try {
      if (this.storage) {
        for (let retry = 0; retry < 5; retry++) {
          try {
            await this.storageCircuitBreaker.call(async () => {
              await Promise.race([
                Promise.all([
                  this.storage.put("currentNumber", this.currentNumber),
                  this.storage.put("lastNumberTick", this.lastNumberTick)
                ]),
                new Promise((_, reject) => 
                  setTimeout(() => reject(new Error("Storage timeout")), CONSTANTS.STORAGE_TIMEOUT)
                )
              ]);
            });
            
            return;
          } catch (error) {
            lastError = error;
            
            if (retry < 4) {
              const delay = Math.min(
                CONSTANTS.STORAGE_RETRY_DELAY_BASE * Math.pow(2, retry),
                CONSTANTS.STORAGE_RETRY_MAX_DELAY
              );
              console.warn(`Storage saveState failed (attempt ${retry + 1}/5), retrying in ${delay}ms:`, error.message);
              await new Promise(r => setTimeout(r, delay));
              continue;
            }
            
            throw error;
          }
        }
      }
    } catch (error) {
      console.error(`Storage saveState failed after 5 attempts:`, error.message);
    }
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
        this.saveState();
        scheduleNext();
      }, delay);
    };
    scheduleNext();
  }

  initializeFallback() {
    console.warn("Initializing fallback mode...");
    
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
      this.storageCircuitBreaker = new CircuitBreaker(CONSTANTS.STORAGE_FAILURE_THRESHOLD, 60000);
      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      this.roomRateLimiter = new RoomRateLimiter();
      this.roomOperationLimiter = new RoomOperationLimiter();
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
      this.userDataSize = new Map();
      this.roomSemaphore = new Map();
      
      this.createDefaultRoom();
      this.lastNumberTick = Date.now();
      this.numberTickTimer = null;
      this.startNumberTickTimer();
      this.startHeartbeat();
      this.startAutoCleanup();
      this.startIdleCleanup();
      this.startMemoryMonitor();
      this.startForcedCleanup();
      this.startEmergencyCleanup();
      this.startPeriodicFlush();
      
      console.log("Fallback mode initialized");
    } catch (error) {
      console.error("Fallback initialization failed:", error);
    }
  }

  async withRoomSemaphore(room, operation) {
    const current = this.roomSemaphore.get(room) || 0;
    if (current >= CONSTANTS.MAX_CONCURRENT_ROOM_OPS) {
      throw new Error('Too many operations for room');
    }
    
    const newCount = current + 1;
    this.roomSemaphore.set(room, newCount);
    try {
      return await operation();
    } finally {
      this.roomSemaphore.set(room, newCount - 1);
      if (this.roomSemaphore.get(room) === 0) {
        this.roomSemaphore.delete(room);
      }
    }
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
      const userConnectionsSnapshot = Array.from(this.userConnections.entries());
      
      for (const [userId, connections] of userConnectionsSnapshot) {
        if (!connections) continue;
        
        const activeConnections = new Set();
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            activeConnections.add(conn);
          }
        }
        
        if (activeConnections.size === 0) {
          if (!this._pendingReconnections.has(userId)) {
            setTimeout(() => this.forceUserCleanup(userId), 0);
          }
          this.userConnections.delete(userId);
        } else if (activeConnections.size !== connections.size) {
          this.userConnections.set(userId, activeConnections);
        }
      }

      if (this.userToSeat.size > CONSTANTS.MAX_USER_TO_SEAT_SIZE) {
        const entries = Array.from(this.userToSeat.entries());
        const toDelete = entries.slice(0, this.userToSeat.size - CONSTANTS.MAX_USER_TO_SEAT_SIZE);
        for (const [userId] of toDelete) {
          if (!this.isUserStillConnected(userId)) {
            this.userToSeat.delete(userId);
          }
        }
      }

      for (const [room, buffer] of this.updateKursiBuffer) {
        if (buffer && buffer.size > CONSTANTS.MAX_BUFFER_PER_ROOM) {
          const entries = Array.from(buffer.entries());
          const newBuffer = new Map(entries.slice(-CONSTANTS.MAX_BUFFER_PER_ROOM));
          this.updateKursiBuffer.set(room, newBuffer);
        }
      }

      const disconnectedSnapshot = Array.from(this.disconnectedTimers.entries());
      for (const [userId, timer] of disconnectedSnapshot) {
        if (timer._scheduledTime && (now - timer._scheduledTime) > this.gracePeriod + 5000) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
          this._pendingReconnections.delete(userId);
          this.executeGracePeriodCleanup(userId);
        }
      }

      this.lockManager?.cleanupStuckLocks();
      this.rateLimiter.cleanup();
      this.connectionRateLimiter.cleanup();
      this.roomRateLimiter.cleanup();
      this.roomOperationLimiter.cleanup();

    } catch (error) {
      console.error("Memory cleanup error:", error);
    }
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
        timeoutId = setTimeout(() => {
          reject(new Error(`Lock timeout after ${timeout}ms`));
        }, timeout);
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

  checkAndEnableSafeMode() {
    const load = this.getServerLoad();
    if (load > this.loadThreshold && !this.safeMode) {
      console.warn(`Enabling safe mode - load: ${load}`);
      this.enableSafeMode();
    } else if (load < CONSTANTS.LOAD_RECOVERY_THRESHOLD && this.safeMode) {
      console.log(`Disabling safe mode - load: ${load}`);
      this.disableSafeMode();
    }
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

  flushBufferedPoints() {
    return;
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
          seatMap.set(i, null);
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
        if (!seatMap.has(seat)) seatMap.set(seat, null);
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
    const MAX_RETRIES = 3;
    
    while (retries < MAX_RETRIES) {
      try {
        return await this.withLock(`seat-update-${room}-${seatNumber}`, () => {
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
          
          const userId = updatedSeat.namauser;
          if (userId) {
            let totalSize = this.userDataSize.get(userId) || 0;
            const oldSize = currentSeat.namauser ? JSON.stringify(currentSeat).length : 0;
            const newSize = JSON.stringify(updatedSeat).length;
            const newTotalSize = totalSize - oldSize + newSize;
            
            if (newTotalSize > CONSTANTS.MAX_USER_DATA_SIZE) {
              console.warn(`User ${userId} data size exceeds limit: ${newTotalSize}`);
            }
            
            this.userDataSize.set(userId, Math.max(0, newTotalSize));
          }
          
          if (updatedSeat.namauser && updatedSeat.namauser !== "") {
            occupancyMap.set(seatNumber, updatedSeat.namauser);
          } else {
            occupancyMap.set(seatNumber, null);
            if (userId) this.userDataSize.delete(userId);
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
        if (error.message?.includes('timeout') && retries < MAX_RETRIES - 1) {
          retries++;
          await new Promise(r => setTimeout(r, 10 * retries));
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

  async findEmptySeat(room, ws) {
    if (!room || !ws || !ws.idtarget) return null;
    try {
      const roomLock = await this.lockManager.acquire(`room-find-seat-${room}`);
      try {
        const occupancyMap = this.seatOccupancy.get(room);
        const seatMap = this.roomSeats.get(room);
        if (!occupancyMap || !seatMap) return null;
        
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const occupantId = occupancyMap.get(i);
          const seatData = seatMap.get(i);
          if (occupantId === ws.idtarget && seatData?.namauser === ws.idtarget) return i;
        }
        
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const occupantId = occupancyMap.get(i);
          const seatData = seatMap.get(i);
          if (occupantId === null && (!seatData || !seatData.namauser)) return i;
        }
        
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const occupantId = occupancyMap.get(i);
          const seatData = seatMap.get(i);
          if (occupantId && seatData?.namauser === occupantId) {
            const isOnline = await this.isUserStillConnected(occupantId);
            if (!isOnline && !this._pendingReconnections.has(occupantId)) {
              await this.cleanupUserFromSeat(room, i, occupantId, true);
              return i;
            }
          }
        }
        return null;
      } finally {
        roomLock();
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
      if (!seatData) {
        seatMap.set(seat, { noimageUrl: "", namauser: userId, color: "", itembawah: 0, itematas: 0, vip: 0, viptanda: 0, lastPoint: null, lastUpdated: Date.now(), _version: 0 });
      } else {
        seatData.namauser = userId;
        seatData.lastUpdated = Date.now();
        seatData._version = (seatData._version || 0) + 1;
      }
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
        
        const seatInfo = seatMap.get(seatNumber);
        if (!seatInfo || seatInfo.namauser !== userId) return;
        
        if (immediate) {
          if (this._pendingReconnections.has(userId)) {
            occupancyMap.set(seatNumber, null);
            return;
          }
          
          Object.assign(seatInfo, createEmptySeat());
          occupancyMap.set(seatNumber, null);
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.updateRoomCount(room);
          this.userToSeat.delete(userId);
          this.userDataSize.delete(userId);
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
      
      const timerId = setTimeout(async () => {
        try {
          this.disconnectedTimers.delete(userId);
          this._pendingReconnections.delete(userId);
          const isStillConnected = await this.isUserStillConnected(userId);
          if (!isStillConnected) {
            await this.withLock(`grace-cleanup-${userId}`, async () => {
              const doubleCheck = await this.isUserStillConnected(userId);
              if (!doubleCheck) await this.forceUserCleanup(userId);
            });
          }
        } catch {}
      }, this.gracePeriod);
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
        this.userDataSize.delete(userId);
        
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
        this.userDataSize.delete(ws.idtarget);
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
        this.userDataSize.delete(idtarget);
        
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
    if (!this.roomOperationLimiter.check(room, "join")) { this.safeSend(ws, ["error", "Room busy"]); return false; }
    
    try {
      return await this.withRoomSemaphore(room, async () => {
        const roomRelease = await this.lockManager.acquire(`room-join-assign-${room}`);
        try {
          this.cancelCleanup(ws.idtarget);
          await this.ensureSeatsData(room);
          
          const previousRoom = this.userCurrentRoom.get(ws.idtarget);
          
          const pendingData = this._pendingReconnections.get(ws.idtarget);
          if (pendingData && pendingData.seatInfo && pendingData.seatInfo.room === room) {
            const { seat } = pendingData.seatInfo;
            const occupancyMap = this.seatOccupancy.get(room);
            const seatMap = this.roomSeats.get(room);
            
            if (occupancyMap && seatMap) {
              if (occupancyMap.get(seat) === null) {
                occupancyMap.set(seat, ws.idtarget);
                const seatData = seatMap.get(seat);
                if (seatData && seatData.namauser === ws.idtarget) {
                  ws.roomname = room;
                  ws.numkursi = new Set([seat]);
                  const clientArray = this.roomClients.get(room);
                  if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
                  this._addUserConnection(ws.idtarget, ws);
                  this.userToSeat.set(ws.idtarget, { room, seat });
                  this.userCurrentRoom.set(ws.idtarget, room);
                  
                  this.sendAllStateTo(ws, room);
                  if (seatData.lastPoint) {
                    this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast]);
                  }
                  this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
                  this.updateRoomCount(room);
                  
                  this._pendingReconnections.delete(ws.idtarget);
                  return true;
                }
              }
            }
          }
          
          if (previousRoom === room) {
            this.sendAllStateTo(ws, room);
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
            return true;
          } else if (previousRoom) {
            await this.cleanupFromRoom(ws, previousRoom);
          }
          
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (seatInfo?.room === room) {
            const occupancyMap = this.seatOccupancy.get(room);
            if (occupancyMap?.get(seatInfo.seat) === ws.idtarget) {
              ws.roomname = room;
              ws.numkursi = new Set([seatInfo.seat]);
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
              this._addUserConnection(ws.idtarget, ws);
              this.sendAllStateTo(ws, room);
              this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
              return true;
            }
          }
          
          let assignedSeat = null;
          const occupancyMap = this.seatOccupancy.get(room);
          if (occupancyMap) {
            for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
              if (occupancyMap.get(seat) === null) {
                occupancyMap.set(seat, ws.idtarget);
                assignedSeat = seat;
                break;
              }
            }
          }
          
          if (!assignedSeat) { 
            this.safeSend(ws, ["roomFull", room]); 
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
        } finally { 
          roomRelease(); 
        }
      });
    } catch (error) {
      console.error("Join room error:", error);
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
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
        
        const pendingData = this._pendingReconnections.get(id);
        if (pendingData && pendingData.seatInfo) {
          const { room, seat } = pendingData.seatInfo;
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          
          if (seatMap && occupancyMap && seat >= 1 && seat <= this.MAX_SEATS) {
            const seatData = seatMap.get(seat);
            if (seatData && seatData.namauser === id) {
              occupancyMap.set(seat, id);
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
              this._addUserConnection(id, ws);
              this.userToSeat.set(id, { room, seat });
              this.userCurrentRoom.set(id, room);
              
              this.sendAllStateTo(ws, room);
              if (seatData.lastPoint) {
                this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast]);
              }
              this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
              this.updateRoomCount(room);
              
              this._pendingReconnections.delete(id);
              return;
            }
          }
        }
        
        const seatInfo = this.userToSeat.get(id);
        if (seatInfo) {
          const { room, seat } = seatInfo;
          if (seat < 1 || seat > this.MAX_SEATS) {
            this.userToSeat.delete(id);
            this.userCurrentRoom.delete(id);
            this._addUserConnection(id, ws);
            this.safeSend(ws, ["needJoinRoom"]);
            return;
          }
          
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          if (seatMap && occupancyMap) {
            const seatData = seatMap.get(seat);
            const occupantId = occupancyMap.get(seat);
            if (seatData?.namauser === id && occupantId === id) {
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
              this._addUserConnection(id, ws);
              this.sendAllStateTo(ws, room);
              if (seatData.lastPoint) {
                this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast]);
              }
              this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
              this.updateRoomCount(room);
              return;
            }
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
          await this.cleanupQueue.add(async () => { 
            await this.cleanupUserFromSeat(cleanupInfo.room, cleanupInfo.seat, cleanupInfo.id, true); 
          });
        } else if (cleanupInfo.id) {
          await this.cleanupQueue.add(async () => { 
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
        console.warn(`Connection ${ws._connId} buffer full, closing`);
        try { ws.close(1000, "Connection too slow"); } catch {}
        return false;
      }
      
      if (ws.bufferedAmount > CONSTANTS.MAX_BUFFERED_AMOUNT * 0.8) {
        return false;
      }
      
      ws.send(JSON.stringify(arr));
      return true;
    } catch (error) {
      if (retry > 0 && !error.message?.includes('CLOSED')) {
        await new Promise(r => setTimeout(r, CONSTANTS.SAFE_SEND_RETRY_DELAY));
        return this.safeSend(ws, arr, retry - 1);
      }
      return false;
    }
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
    const room = roomList[0];
    const seatMap = this.roomSeats.get(room);
    const occupancyMap = this.seatOccupancy.get(room);
    
    if (!seatMap || !occupancyMap) return;
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const seatData = seatMap.get(seat);
      const occupant = occupancyMap.get(seat);
      
      if (seatData?.namauser && !occupant && !this._pendingReconnections.has(seatData.namauser)) {
        occupancyMap.set(seat, seatData.namauser);
      } else if (!seatData?.namauser && occupant) {
        occupancyMap.set(seat, null);
      } else if (seatData?.namauser && occupant && seatData.namauser !== occupant) {
        const isOccupantOnline = await this.isUserStillConnected(occupant);
        if (isOccupantOnline) {
          seatData.namauser = occupant;
          seatData.lastUpdated = Date.now();
        } else if (!this._pendingReconnections.has(occupant)) {
          occupancyMap.set(seat, null);
        }
      }
    }
  }

  sampledSeatConsistencyCheck() {
    try {
      const room = roomList[0]; 
      if (this.getServerLoad() >= 0.8) return;
      this.validateSeatConsistency(room);
    } catch {}
  }

  async validateSeatConsistency(room) {
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      if (!seatMap || !occupancyMap) return;
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const occupantId = occupancyMap.get(seat);
        const seatData = seatMap.get(seat);
        if (occupantId && (!seatData || !seatData.namauser)) {
          if (!seatData) seatMap.set(seat, createEmptySeat());
          else Object.assign(seatData, createEmptySeat());
          occupancyMap.set(seat, null);
        } else if (!occupantId && seatData?.namauser) {
          const isOnline = await this.isUserStillConnected(seatData.namauser);
          if (isOnline) occupancyMap.set(seat, seatData.namauser);
          else if (!this._pendingReconnections.has(seatData.namauser)) {
            Object.assign(seatData, createEmptySeat());
          }
        } else if (occupantId && seatData?.namauser && seatData.namauser !== occupantId) {
          const isOccupantOnline = await this.isUserStillConnected(occupantId);
          if (isOccupantOnline) {
            seatData.namauser = occupantId;
          } else if (!this._pendingReconnections.has(occupantId)) {
            occupancyMap.set(seat, null);
            Object.assign(seatData, createEmptySeat());
          }
        }
      }
    } catch {}
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
          await this.cleanupQueue.add(async () => { await this.fullRemoveById(idtarget); });
        } else {
          const seatInfo = this.userToSeat.get(idtarget);
          if (seatInfo) {
            await this.cleanupQueue.add(async () => { await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, idtarget, true); });
          }
          this.userToSeat.delete(idtarget);
          this.userCurrentRoom.delete(idtarget);
          this.userDataSize.delete(idtarget);
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
    try {
      ws._isClosing = true;
      
      const queue = this._messageQueues.get(ws);
      if (queue) {
        queue.clear();
        this._messageQueues.delete(ws);
      }
      
      if (ws.removeAllListeners) {
        ws.removeAllListeners();
      }
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      
      this.clients.delete(ws);
      if (userId) {
        this._removeUserConnection(userId, ws);
        this.cancelCleanup(userId);
        if (!ws.isManualDestroy && !ws._isDuplicate) this.scheduleCleanup(userId);
      }
      if (room) this._removeFromRoomClients(ws, room);
      if (ws.readyState === 1) try { ws.close(1000, "Normal closure"); } catch {}
      setTimeout(() => { 
        ws.roomname = null; 
        ws.idtarget = null; 
        ws.numkursi = null;
        ws._isDuplicate = null;
        ws._isClosing = null;
        ws._connectionTime = null;
      }, 1000);
    } catch { 
      this.clients.delete(ws); 
      if (userId) this.cancelCleanup(userId); 
    }
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
            if (!idtarget) { this.safeSend(ws, ["inRoomStatus", false]); return; }
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
            if (!roomList.includes(roomName)) return;
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
            if (ws.roomname !== roomname || ws.idtarget !== username) return;
            if (!roomList.includes(roomname)) return;
            
            if (!this.roomRateLimiter.check(roomname, username)) {
              this.safeSend(ws, ["error", "Too many messages in this room"]);
              return;
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
            if (!isPrimary) return;
            const chatMsg = ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor];
            this.broadcastToRoom(roomname, chatMsg);
            break;
          }
          case "updatePoint": {
            const [, room, seat, x, y, fast] = data;
            if (ws.roomname !== room || !roomList.includes(room)) return;
            if (seat < 1 || seat > this.MAX_SEATS) return;
            this.savePointWithRetry(room, seat, x, y, fast).catch(() => {});
            this.broadcastPointDirect(room, seat, x, y, fast);
            break;
          }
          case "removeKursiAndPoint": {
            const [, room, seat] = data;
            if (seat < 1 || seat > this.MAX_SEATS) return;
            if (ws.roomname !== room || !roomList.includes(room)) return;
            await this.updateSeatAtomic(room, seat, () => createEmptySeat());
            this.clearSeatBuffer(room, seat);
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
            break;
          }
          case "updateKursi": {
            const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
            
            if (seat < 1 || seat > this.MAX_SEATS) return;
            if (ws.roomname !== room || !roomList.includes(room)) return;
            
            await this.updateSeatAtomic(room, seat, () => ({
              noimageUrl: noimageUrl || "", 
              namauser: namauser || "", 
              color: color || "",
              itembawah: itembawah || 0,
              itematas: itematas || 0,
              vip: vip || 0,
              viptanda: viptanda || 0,
              lastPoint: null,
              lastUpdated: Date.now()
            }));
            
            if (namauser === ws.idtarget) {
              this.userToSeat.set(namauser, { room, seat });
              this.userCurrentRoom.set(namauser, room);
            }
            
            this.updateRoomCount(room);
            
            const response = ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda];
            
            const clientArray = this.roomClients.get(room);
            if (clientArray && clientArray.length > 0) {
              const message = JSON.stringify(response);
              for (const client of clientArray) {
                if (client?.readyState === 1 && client.roomname === room && 
                    !client._isDuplicate && !client._isClosing) {
                  try { client.send(message); } catch {}
                }
              }
            }
            
            this.safeSend(ws, response);
            
            break;
          }
          case "gift": {
            const [, roomname, sender, receiver, giftName] = data;
            if (ws.roomname !== roomname || ws.idtarget !== sender) return;
            if (!roomList.includes(roomname)) return;
            const giftData = ["gift", roomname, sender, receiver, giftName, Date.now()];
            this.broadcastToRoom(roomname, giftData);
            break;
          }
          case "leaveRoom": {
            const room = ws.roomname;
            if (!room || !roomList.includes(room)) return;
            await this.cleanupFromRoom(ws, room);
            this.updateRoomCount(room);
            this.safeSend(ws, ["roomLeft", room]);
            break;
          }
          case "gameLowCardStart":
          case "gameLowCardJoin":
          case "gameLowCardNumber":
          case "gameLowCardEnd":
            if (["LowCard 1", "LowCard 2", "Noxxeliverothcifsa", "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"].includes(ws.roomname)) {
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
        console.error("Message handler error:", error);
        if (ws.readyState === 1) this.safeSend(ws, ["error", "Server error"]);
      }
    } catch (error) {
      console.error("Message processing error:", error);
    }
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
        console.warn(`Connection rejected: max connections reached (${activeConnections}/${CONSTANTS.MAX_GLOBAL_CONNECTIONS})`);
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
      ws._lastHeartbeat = Date.now();
      
      this.clients.add(ws);
      this._lastActivityTime = Date.now();
      
      const messageQueue = new MessageQueue(ws, this);
      this._messageQueues.set(ws, messageQueue);
      
      ws.addEventListener("message", (ev) => {
        messageQueue.add(ev.data).catch(() => {});
      });
      
      ws.addEventListener("error", (error) => {
        console.error(`WebSocket error for ${ws._connId}:`, error);
      });
      
      ws.addEventListener("close", (event) => {
        Promise.resolve().then(() => {
          try {
            if (event.code !== 1000 || event.reason !== "Replaced by new connection") {
              this.safeWebSocketCleanup(ws);
            } else {
              this.clients.delete(ws);
              if (ws.idtarget) this._removeUserConnection(ws.idtarget, ws);
              if (ws.roomname) this._removeFromRoomClients(ws, ws.roomname);
            }
          } catch (error) {
            // Silent fail
          }
        }).catch(() => {});
      });
      
      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }
  
  async cleanup() {
    try {
      await this.performMemoryCleanup();
    } catch (error) {
      console.error("Memory cleanup error:", error);
    }
    try {
      await this.cleanupDuplicateConnections();
    } catch (error) {
      console.error("Duplicate connections cleanup error:", error);
    }
    try {
      this.validateGracePeriodTimers();
    } catch (error) {
      console.error("Grace period validation error:", error);
    }
    try {
      this.sampledSeatConsistencyCheck();
    } catch (error) {
      console.error("Seat consistency check error:", error);
    }
    try {
      await this.fullSeatConsistencyCheck();
    } catch (error) {
      console.error("Full seat consistency check error:", error);
    }
  }
  
  async getHealthStatus() {
    const now = Date.now();
    const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
    const memUsage = process.memoryUsage();
    const heapPercent = memUsage.heapUsed / memUsage.heapTotal;
    
    let totalBufferSize = 0;
    for (const buffer of this.updateKursiBuffer.values()) {
      totalBufferSize += buffer?.size || 0;
    }
    
    return {
      status: activeConnections > 450 ? "degraded" : "healthy",
      uptime: now - this._startTime,
      activeConnections,
      memoryPressure: heapPercent,
      safeMode: this.safeMode,
      lockMetrics: this.lockManager?.getMetrics(),
      queueMetrics: this.cleanupQueue?.getMetrics(),
      circuitBreaker: this.storageCircuitBreaker?.getMetrics(),
      queueHealth: {
        size: this.cleanupQueue?.size() || 0,
        active: this.cleanupQueue?.active || 0,
        healthy: (this.cleanupQueue?.size() || 0) < 100
      },
      bufferHealth: {
        totalBuffers: this.updateKursiBuffer.size,
        totalEntries: totalBufferSize,
        healthy: totalBufferSize < 500
      },
      mapSizes: {
        userToSeat: this.userToSeat.size,
        userConnections: this.userConnections.size,
        disconnectedTimers: this.disconnectedTimers.size,
        pendingReconnections: this._pendingReconnections.size,
        messageQueues: this._messageQueues.size
      },
      failedBatches: this._failedBatches?.length || 0
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
        const metrics = {
          status: "healthy",
          activeConnections,
          totalClients: obj.clients?.size || 0,
          activeGames: obj.lowcard?.activeGames?.size || 0,
          queueSize: obj.cleanupQueue?.size() || 0,
          bufferSize: obj.updateKursiBuffer?.size || 0,
          failedBatches: obj._failedBatches?.length || 0,
          messageQueues: obj._messageQueues?.size || 0,
          pendingReconnections: obj._pendingReconnections?.size || 0,
          safeMode: obj.safeMode || false,
          timestamp: Date.now()
        };
        return new Response(JSON.stringify(metrics), {
          headers: { "content-type": "application/json" }
        });
      }
      
      return new Response("WebSocket endpoint", { status: 200, headers: { "content-type": "text/plain" } });
      
    } catch (error) {
      console.error("Worker fetch error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
}
