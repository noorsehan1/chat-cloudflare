import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambayan", "Lounge Talk",
  "Noxxeliverothcifsa", "One Side Love", "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// Game rooms constant
const GAME_ROOMS = ["LowCard 1", "LowCard 2", "Noxxeliverothcifsa", "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love"];

// ========== CONSTANTS ==========
const CONSTANTS = {
  MAX_QUEUE_SIZE: 200,
  MAX_LOCK_QUEUE_SIZE: 100,
  MAX_POINTS_PER_ROOM: 100,
  MAX_POINTS_TOTAL: 2000,
  MAX_POINTS_BEFORE_FLUSH: 200,
  LOCK_ACQUIRE_TIMEOUT: 5000,
  BUFFER_SIZE_LIMIT: 20,
  BROADCAST_BATCH_SIZE: 20,
  CACHE_VALID_DURATION: 5000,
  LOAD_THRESHOLD: 0.85,
  LOAD_RECOVERY_THRESHOLD: 0.65,
  KURSI_UPDATE_DEBOUNCE: 150,
  DEBOUNCE_CLEANUP_INTERVAL: 30000,
  MAX_DEBOUNCE_AGE: 20000,
  LOCK_TIMEOUT: 10000,
  GRACE_PERIOD: 5000,
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
  MAX_MESSAGE_QUEUE_SIZE: 30,
  MESSAGES_PER_SECOND_LIMIT: 5,
  QUEUE_PROCESS_DELAY_MS: 50,
  MAX_GAMES_CONCURRENT: 50,
  MAX_DEBOUNCE_SIZE: 300,
  MAX_QUEUE_SIZE_WARNING: 150,
  MAX_SEAT_CONSISTENCY_CHECK_PER_CYCLE: 50
};

// ========== GLOBAL ERROR HANDLER ==========
const originalConsoleError = console.error;
console.error = (...args) => {
  const msg = args.join(' ');
  if (msg.includes('WebSocket closed') || 
      msg.includes('Connection closed') ||
      msg.includes('ERR_STREAM_WRITE_AFTER_END') ||
      msg.includes('ECONNRESET')) {
    return;
  }
  originalConsoleError.apply(console, args);
};

// Unhandled rejection handler
if (typeof process !== 'undefined' && process.on) {
  process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  });
}

class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
    this.lockTimestamps = new Map();
    this._destroyed = false;
    this._lockId = 0;
    this._cleanupInterval = null;
    this._startCleanupInterval();
  }

  _startCleanupInterval() {
    this._cleanupInterval = setInterval(() => {
      this.cleanupStuckLocks();
    }, 30000);
  }

  async acquire(resourceId) {
    if (this._destroyed) throw new Error("LockManager destroyed");
    
    const lockId = ++this._lockId;
    
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
              reject(new Error(`Lock queue timeout for ${resourceId}`));
            }
          }
        }
      }, CONSTANTS.LOCK_TIMEOUT);
      
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
    if (this._destroyed) return;
    const now = Date.now();
    for (const [resourceId, lockTime] of this.lockTimestamps) {
      if (now - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
      }
    }
  }
  
  destroy() {
    this._destroyed = true;
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
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

class QueueManager {
  constructor(concurrency = 3) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = CONSTANTS.MAX_QUEUE_SIZE;
    this.processing = false;
    this.destroyed = false;
    this._processTimeout = null;
  }

  async add(job) {
    if (this.destroyed) throw new Error("Queue manager destroyed");
    if (this.queue.length >= this.maxQueueSize) {
      console.warn(`Queue full: ${this.queue.length}/${this.maxQueueSize}`);
      throw new Error("Server busy");
    }
    
    if (this.queue.length > CONSTANTS.MAX_QUEUE_SIZE_WARNING) {
      console.warn(`QueueManager warning: size=${this.queue.length}, active=${this.active}`);
    }
    
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const index = this.queue.findIndex(item => item.reject === reject);
        if (index !== -1) {
          this.queue.splice(index, 1);
          reject(new Error("Queue timeout"));
        }
      }, 30000);
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
              new Promise((_, reject) => setTimeout(() => reject(new Error("Job timeout")), 15000))
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
        }
      };
      
      await processNext();
      
      if (this.queue.length > 0 && !this.destroyed) {
        if (this._processTimeout) clearTimeout(this._processTimeout);
        this._processTimeout = setTimeout(() => this.process(), CONSTANTS.QUEUE_PROCESS_DELAY_MS);
      }
    } finally {
      this.processing = false;
    }
  }

  clear() {
    if (this._processTimeout) {
      clearTimeout(this._processTimeout);
      this._processTimeout = null;
    }
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

class RateLimiter {
  constructor(windowMs = 60000, maxRequests = 100) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this._cleanupInterval = setInterval(() => this.cleanup(), 60000);
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
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    this.requests.clear();
  }
}

class RoomRateLimiter {
  constructor() {
    this.roomLimits = new Map();
    this._cleanupInterval = setInterval(() => this.cleanup(), 30000);
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
  
  destroy() {
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    this.roomLimits.clear();
  }
}

class MessageQueue {
  constructor(ws, chatServer) {
    this.ws = ws;
    this.chatServer = chatServer;
    this.queue = [];
    this.processing = false;
    this.maxSize = CONSTANTS.MAX_MESSAGE_QUEUE_SIZE;
    this.lastProcessTime = 0;
    this.minProcessInterval = 100;
    this._processTimeout = null;
  }
  
  async add(rawMessage) {
    if (this.queue.length >= this.maxSize) {
      this.chatServer.safeSend(this.ws, ["error", "Message queue full, slow down"]);
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
    
    try {
      const now = Date.now();
      const timeSinceLastProcess = now - this.lastProcessTime;
      if (timeSinceLastProcess < this.minProcessInterval && this.lastProcessTime > 0) {
        await new Promise(r => setTimeout(r, this.minProcessInterval - timeSinceLastProcess));
      }
      
      while (this.queue.length > 0 && this.ws.readyState === 1 && !this.ws._isClosing) {
        const raw = this.queue.shift();
        try {
          this.lastProcessTime = Date.now();
          await this.chatServer.handleMessage(this.ws, raw);
        } catch (error) {
          // Error already logged in handleMessage
        }
      }
    } finally {
      this.processing = false;
      
      if (this.queue.length > 0 && this.ws.readyState === 1 && !this.ws._isClosing) {
        if (this._processTimeout) clearTimeout(this._processTimeout);
        this._processTimeout = setTimeout(() => this.process(), 10);
        if (this.chatServer) this.chatServer._addTimer(this._processTimeout);
      }
    }
  }
  
  clear() {
    if (this._processTimeout) {
      clearTimeout(this._processTimeout);
      this._processTimeout = null;
    }
    this.queue = [];
    this.processing = false;
  }
}

function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "", itembawah: 0, itematas: 0,
    vip: 0, viptanda: 0, lastPoint: null, lastUpdated: Date.now(),
    _version: 0
  };
}

export class ChatServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      this._startTime = Date.now();
      this._lastActivityTime = Date.now();
      this._failedBatches = [];
      this._isShuttingDown = false;
      this._messageQueues = new Map();
      this._activeWebSocketPairs = new Set();
      this._allTimers = new Set();
      
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
      this._kursiUpdateDebounce = new Map();

      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      this.roomRateLimiter = new RoomRateLimiter();
      
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;

      try { 
        this.lowcard = new LowCardGameManager(this); 
      } catch (error) { 
        console.error("LowCardGameManager init error:", error);
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
      this._debounceCleanupTimer = null;

      this._intervals = [];

      try { 
        this.initializeRooms(); 
      } catch (error) { 
        console.error("initializeRooms error:", error);
        this.createDefaultRoom(); 
      }

      this.startNumberTickTimer();
      this.startDebounceCleanupTimer();
      
      this.roomCountsCache = null;
      this._countsCacheTime = 0;
      this.cacheValidDuration = CONSTANTS.CACHE_VALID_DURATION;

      for (const room of roomList) {
        this._pointBuffer.set(room, []);
      }

      this.loadState().catch(error => console.error("loadState error:", error));
      this.startAutoCleanup();
      this.startIdleCleanup();
      this.startMemoryMonitor();
      this.startForcedCleanup();

    } catch (error) {
      console.error("ChatServer constructor error:", error);
      this.initializeFallback();
    }
  }

  _addTimer(timer) {
    this._allTimers.add(timer);
    timer._cleanup = () => this._allTimers.delete(timer);
  }

  _clearAllTimers() {
    for (const timer of this._allTimers) {
      clearTimeout(timer);
      clearInterval(timer);
    }
    this._allTimers.clear();
  }

  startForcedCleanup() {
    const forcedCleanupInterval = setInterval(() => {
      try {
        const now = Date.now();
        
        if (this._kursiUpdateDebounce.size > CONSTANTS.MAX_DEBOUNCE_SIZE) {
          const entries = Array.from(this._kursiUpdateDebounce.entries());
          const toDelete = entries.slice(0, this._kursiUpdateDebounce.size - CONSTANTS.MAX_DEBOUNCE_SIZE);
          for (const [key, timer] of toDelete) {
            clearTimeout(timer);
            this._kursiUpdateDebounce.delete(key);
          }
        }
        
        for (const [key, timer] of this._kursiUpdateDebounce) {
          if (timer && (now - (timer._createdAt || 0)) > CONSTANTS.MAX_TIMER_AGE) {
            clearTimeout(timer);
            this._kursiUpdateDebounce.delete(key);
          }
        }
        
        for (const [userId, timer] of this.disconnectedTimers) {
          if (timer && (now - (timer._scheduledTime || 0)) > CONSTANTS.MAX_TIMER_AGE) {
            clearTimeout(timer);
            this.disconnectedTimers.delete(userId);
          }
        }
        
        for (const [room, points] of this._pointBuffer) {
          if (points.length > CONSTANTS.MAX_POINTS_PER_ROOM) {
            this.flushBufferedPoints();
            break;
          }
        }
        
        if (this._failedBatches && this._failedBatches.length > 0) {
          this._failedBatches = this._failedBatches.filter(failed => {
            return (now - failed.timestamp) < 5000;
          });
        }
        
        this.roomRateLimiter.cleanup();
        
        if (this._kursiUpdateDebounce.size > 500 || (this.cleanupQueue?.size() || 0) > 100) {
          console.warn(`High load: debounce=${this._kursiUpdateDebounce.size}, queue=${this.cleanupQueue?.size() || 0}`);
        }
        
      } catch (e) {
        console.error("forcedCleanup error:", e);
      }
    }, CONSTANTS.FORCED_CLEANUP_INTERVAL);
    this._intervals.push(forcedCleanupInterval);
    this._addTimer(forcedCleanupInterval);
  }

  startIdleCleanup() {
    const idleCheckInterval = setInterval(() => {
      try {
        const now = Date.now();
        const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
        
        if (activeConnections === 0 && (now - this._lastActivityTime) > 300000) {
          this.flushKursiUpdates();
          this.flushBufferedPoints();
          
          const nowTime = Date.now();
          const snapshot = Array.from(this._kursiUpdateDebounce.entries());
          for (const [key, timer] of snapshot) {
            if (timer && (nowTime - (timer._createdAt || 0)) > 60000) {
              clearTimeout(timer);
              this._kursiUpdateDebounce.delete(key);
            }
          }
        } else if (activeConnections > 0) {
          this._lastActivityTime = now;
        }
      } catch (error) {
        console.error("idleCleanup error:", error);
      }
    }, 60000);
    this._intervals.push(idleCheckInterval);
    this._addTimer(idleCheckInterval);
  }

  startMemoryMonitor() {
    const memoryMonitor = setInterval(() => {
      try {
        const activeConns = Array.from(this.clients).filter(c => c?.readyState === 1).length;
        const debounceSize = this._kursiUpdateDebounce.size;
        const queueSize = this.cleanupQueue?.size() || 0;
        const messageQueueSize = this._messageQueues.size;
        
        if (debounceSize > 800 || queueSize > 80 || messageQueueSize > 400) {
          console.warn(`Memory pressure warning: debounce=${debounceSize}, queue=${queueSize}, msgQueues=${messageQueueSize}, conns=${activeConns}`);
        }
        
      } catch (e) {
        console.error("memoryMonitor error:", e);
      }
    }, 30000);
    this._intervals.push(memoryMonitor);
    this._addTimer(memoryMonitor);
  }

  startAutoCleanup() {
    const autoCleanupInterval = setInterval(async () => {
      try {
        await this.cleanup();
      } catch (error) {
        console.error("autoCleanup error:", error);
      }
    }, 300000);
    this._intervals.push(autoCleanupInterval);
    this._addTimer(autoCleanupInterval);
  }

  startDebounceCleanupTimer() {
    if (this._debounceCleanupTimer) clearInterval(this._debounceCleanupTimer);
    this._debounceCleanupTimer = setInterval(() => {
      try {
        const now = Date.now();
        const snapshot = Array.from(this._kursiUpdateDebounce.entries());
        for (const [key, timer] of snapshot) {
          if (timer && timer._createdAt && (now - timer._createdAt) > CONSTANTS.MAX_DEBOUNCE_AGE) {
            clearTimeout(timer);
            this._kursiUpdateDebounce.delete(key);
          } else if (timer && !timer._createdAt) {
            clearTimeout(timer);
            this._kursiUpdateDebounce.delete(key);
          }
        }
      } catch (error) {
        console.error("debounceCleanup error:", error);
      }
    }, CONSTANTS.DEBOUNCE_CLEANUP_INTERVAL);
    
    this._intervals.push(this._debounceCleanupTimer);
    this._addTimer(this._debounceCleanupTimer);
  }

  async gracefulShutdown() {
    if (this._isShuttingDown) return;
    this._isShuttingDown = true;
    
    console.log("Starting graceful shutdown...");
    
    const shutdownTimeout = setTimeout(() => {
      console.error("Shutdown timeout, forcing exit");
      this.forceShutdown();
    }, 15000);
    this._addTimer(shutdownTimeout);
    
    try {
      const notifyPromises = [];
      for (const client of this.clients) {
        if (client.readyState === 1 && !client._isClosing) {
          notifyPromises.push(this.safeSend(client, ["serverShutdown", "Server is restarting"]));
        }
      }
      await Promise.allSettled(notifyPromises);
      
      await this.waitForQuiescence(5000);
      
      await Promise.allSettled([
        this.flushKursiUpdates(),
        this.flushBufferedPoints(),
        this.saveState()
      ]);
      
      await this.closeAllConnections(3000);
      
      await this.destroy();
      clearTimeout(shutdownTimeout);
      console.log("Graceful shutdown completed");
      
    } catch (error) {
      console.error("Shutdown error:", error);
      this.forceShutdown();
    }
  }

  async waitForQuiescence(maxWaitMs) {
    const start = Date.now();
    while (Date.now() - start < maxWaitMs) {
      const activeOps = this.cleanupQueue?.active || 0;
      const pendingOps = this.cleanupQueue?.size() || 0;
      if (activeOps === 0 && pendingOps === 0) break;
      await new Promise(r => setTimeout(r, 100));
    }
  }

  async closeAllConnections(timeoutMs) {
    const promises = Array.from(this.clients).map(client => {
      return new Promise(resolve => {
        const timeout = setTimeout(resolve, timeoutMs);
        try {
          if (client.readyState === 1 && !client._isClosing) {
            client.close(1000, "Server shutdown");
          }
        } catch(e) {}
        setTimeout(resolve, 1000);
      });
    });
    await Promise.allSettled(promises);
  }

  forceShutdown() {
    for (const client of this.clients) {
      try {
        if (client.readyState === 1) client.close(1000, "Force shutdown");
      } catch(e) {}
    }
    this.destroy();
  }

  async destroy() {
    console.log("Destroying ChatServer...");
    
    this._clearAllTimers();
    
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
    if (this._debounceCleanupTimer) {
      clearInterval(this._debounceCleanupTimer);
      this._debounceCleanupTimer = null;
    }
    
    for (const [key, timer] of this._kursiUpdateDebounce) {
      clearTimeout(timer);
    }
    this._kursiUpdateDebounce.clear();
    
    for (const [userId, timer] of this.disconnectedTimers) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
    
    for (const [ws, queue] of this._messageQueues) {
      queue.clear();
    }
    this._messageQueues.clear();
    
    for (const pair of this._activeWebSocketPairs) {
      try {
        const [client, server] = Object.values(pair);
        if (client && client.readyState === 1) client.close();
        if (server && server.readyState === 1) server.close();
      } catch(e) {}
    }
    this._activeWebSocketPairs.clear();
    
    if (this.lockManager) this.lockManager.destroy();
    if (this.cleanupQueue) this.cleanupQueue.destroy();
    if (this.rateLimiter) this.rateLimiter.destroy();
    if (this.connectionRateLimiter) this.connectionRateLimiter.destroy();
    if (this.roomRateLimiter) this.roomRateLimiter.destroy();
    
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
    
    console.log("ChatServer destroyed");
  }

  async loadState() {
    try {
      if (this.storage) {
        const savedNumber = await this.storage.get("currentNumber");
        if (savedNumber && typeof savedNumber === 'number' && savedNumber >= 1 && savedNumber <= this.maxNumber) {
          this.currentNumber = savedNumber;
        }
        const savedLastTick = await this.storage.get("lastNumberTick");
        if (savedLastTick && typeof savedLastTick === 'number') {
          this.lastNumberTick = savedLastTick;
        }
      }
    } catch (error) {
      console.error("loadState error:", error);
    }
  }

  async saveState() {
    try {
      if (this.storage && !this._isShuttingDown) {
        await this.storage.put("currentNumber", this.currentNumber);
        await this.storage.put("lastNumberTick", this.lastNumberTick);
      }
    } catch (error) {
      console.error("saveState error:", error);
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
        this.saveState().catch(e => console.error("saveState tick error:", e));
        scheduleNext();
      }, delay);
      this._addTimer(this.numberTickTimer);
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
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
      this._pointBuffer = new Map();
      this._pointFlushDelay = 100;
      this._hasBufferedUpdates = false;
      this._kursiUpdateDebounce = new Map();
      this._intervals = [];
      this._allTimers = new Set();
      this._activeWebSocketPairs = new Set();
      this._messageQueues = new Map();
      this.roomCountsCache = null;
      this._countsCacheTime = 0;
      this._startTime = Date.now();
      this._lastActivityTime = Date.now();
      this._failedBatches = [];
      this._isShuttingDown = false;
      
      this.createDefaultRoom();
      this.lastNumberTick = Date.now();
      this.numberTickTimer = null;
      this.startNumberTickTimer();
      this.startAutoCleanup();
      this.startDebounceCleanupTimer();
      this.startIdleCleanup();
      this.startMemoryMonitor();
      this.startForcedCleanup();
    } catch (error) {
      console.error("initializeFallback error:", error);
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
          setTimeout(() => this.forceUserCleanup(userId), 0);
          this.userConnections.delete(userId);
        } else if (activeConnections.size !== connections.size) {
          this.userConnections.set(userId, activeConnections);
        }
      }

      for (const [room, points] of this._pointBuffer) {
        if (points && points.length > CONSTANTS.MAX_POINTS_PER_ROOM) {
          this._pointBuffer.set(room, points.slice(-CONSTANTS.MAX_POINTS_PER_ROOM));
        }
      }
      
      let totalPoints = 0;
      for (const points of this._pointBuffer.values()) totalPoints += points.length;
      if (totalPoints > CONSTANTS.MAX_POINTS_TOTAL) {
        const reduceBy = Math.ceil((totalPoints - CONSTANTS.MAX_POINTS_TOTAL) / this._pointBuffer.size);
        for (const [room, points] of this._pointBuffer) {
          if (points.length > reduceBy) this._pointBuffer.set(room, points.slice(reduceBy));
        }
      }

      for (const [room, buffer] of this.updateKursiBuffer) {
        if (buffer && buffer.size > CONSTANTS.BUFFER_SIZE_LIMIT * 5) {
          const entries = Array.from(buffer.entries());
          const newBuffer = new Map(entries.slice(-CONSTANTS.BUFFER_SIZE_LIMIT * 2));
          this.updateKursiBuffer.set(room, newBuffer);
        }
      }

      const disconnectedSnapshot = Array.from(this.disconnectedTimers.entries());
      for (const [userId, timer] of disconnectedSnapshot) {
        if (timer._scheduledTime && (now - timer._scheduledTime) > this.gracePeriod + 5000) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
        }
      }

      this.lockManager?.cleanupStuckLocks();
      this.rateLimiter.cleanup();
      this.connectionRateLimiter.cleanup();
      this.roomRateLimiter.cleanup();

    } catch (error) {
      console.error("performMemoryCleanup error:", error);
    }
  }

  setRoomMute(roomName, isMuted) {
    try {
      if (!roomName || !roomList.includes(roomName)) return false;
      const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
      this.muteStatus.set(roomName, muteValue);
      this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
      return true;
    } catch (error) {
      console.error("setRoomMute error:", error);
      return false;
    }
  }

  getRoomMute(roomName) {
    try {
      if (!roomName || !roomList.includes(roomName)) return false;
      return this.muteStatus.get(roomName) === true;
    } catch (error) {
      console.error("getRoomMute error:", error);
      return false;
    }
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
      if (oldest && oldest.readyState === 1 && !oldest._isClosing) {
        oldest._isDuplicate = true;
        oldest._isClosing = true;
        try { 
          this.safeSend(oldest, ["duplicateConnection", "Too many connections"]);
          oldest.close(1000, "Too many connections"); 
        } catch {}
        userConnections.delete(oldest);
        this.clients.delete(oldest);
        if (oldest.roomname) this._removeFromRoomClients(oldest, oldest.roomname);
      }
    }
    userConnections.add(ws);
  }

  _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    const userConnections = this.userConnections.get(userId);
    if (userConnections) {
      userConnections.delete(ws);
      if (userConnections.size === 0) this.userConnections.delete(userId);
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
      throw error;
    } finally {
      if (timeoutId) clearTimeout(timeoutId);
      if (release) {
        try { release(); } catch (e) {}
      }
    }
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
    const disableTimer = setTimeout(() => {
      if (this.getServerLoad() < CONSTANTS.LOAD_RECOVERY_THRESHOLD) this.disableSafeMode();
    }, 60000);
    this._addTimer(disableTimer);
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
    return Math.min(activeConnections / CONSTANTS.MAX_GLOBAL_CONNECTIONS + queueLoad, 0.95);
  }

  flushBufferedPoints() {
    for (const [room, points] of this._pointBuffer) {
      if (points.length > 0) {
        const batch = [...points];
        const success = this.broadcastPointsBatch(room, batch);
        if (success) {
          points.splice(0, batch.length);
        } else {
          if (!this._failedBatches) this._failedBatches = [];
          this._failedBatches.push({ room, batch, timestamp: Date.now() });
        }
      }
    }
    
    if (this._failedBatches && this._failedBatches.length > 0) {
      const now = Date.now();
      this._failedBatches = this._failedBatches.filter(failed => {
        if (now - failed.timestamp < 5000) {
          const success = this.broadcastPointsBatch(failed.room, failed.batch);
          return !success;
        }
        return false;
      });
    }
  }

  broadcastPointsBatch(room, batch) {
    try {
      if (!room || !roomList.includes(room)) return false;
      const validBatch = batch.filter(point => point && point.seat >= 1 && point.seat <= this.MAX_SEATS);
      if (validBatch.length === 0) return true;
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return false;
      const message = JSON.stringify(["pointsBatch", room, validBatch]);
      let successCount = 0;
      for (const client of clientArray) {
        if (client?.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
          try { 
            client.send(message); 
            successCount++;
          } catch (error) {}
        }
      }
      return successCount > 0;
    } catch (error) {
      console.error("broadcastPointsBatch error:", error);
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
          try { client.send(message); } catch (error) {}
        }
      }
    } catch (error) {
      console.error("broadcastPointDirect error:", error);
    }
  }

  async savePointWithRetry(room, seat, x, y, fast) {
    try {
      if (seat < 1 || seat > this.MAX_SEATS) return false;
      const xNum = typeof x === 'number' ? x : parseFloat(x);
      const yNum = typeof y === 'number' ? y : parseFloat(y);
      if (isNaN(xNum) || isNaN(yNum)) return false;
      
      const points = this._pointBuffer.get(room) || [];
      if (points.length > CONSTANTS.MAX_POINTS_BEFORE_FLUSH) this.flushBufferedPoints();
      
      await this.updateSeatAtomic(room, seat, (currentSeat) => {
        if (!currentSeat) currentSeat = createEmptySeat();
        currentSeat.lastPoint = { x: xNum, y: yNum, fast: fast || false, timestamp: Date.now() };
        return currentSeat;
      });
      
      this.broadcastPointDirect(room, seat, xNum, yNum, fast);
      return true;
    } catch (error) {
      console.error("savePointWithRetry error:", error);
      this.broadcastPointDirect(room, seat, x, y, fast);
      return false;
    }
  }

  createDefaultRoom() {
    try {
      const room = "General";
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
    } catch (error) {
      console.error("createDefaultRoom error:", error);
    }
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
      } catch (error) {
        console.error(`initializeRooms error for ${room}:`, error);
      }
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
    } catch (error) {
      console.error("ensureSeatsData error:", error);
    }
  }

  flushRoomKursiUpdates(room) {
    try {
      const buffer = this.updateKursiBuffer.get(room);
      if (buffer && buffer.size > 0) {
        const updates = [];
        for (const [seat, info] of buffer.entries()) {
          if (seat >= 1 && seat <= this.MAX_SEATS && info?.namauser) {
            const { lastPoint, lastUpdated, _version, ...rest } = info;
            updates.push([seat, rest]);
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
    } catch (error) {
      console.error("flushRoomKursiUpdates error:", error);
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
          
          const updatedSeat = updateFn(currentSeat);
          
          if (!updatedSeat || !updatedSeat.namauser) {
            seatMap.set(seatNumber, null);
            occupancyMap.set(seatNumber, null);
            const buffer = this.updateKursiBuffer.get(room);
            if (buffer) buffer.delete(seatNumber);
            return null;
          }
          
          updatedSeat._version = (updatedSeat._version || 0) + 1;
          updatedSeat.lastUpdated = Date.now();
          
          occupancyMap.set(seatNumber, updatedSeat.namauser);
          seatMap.set(seatNumber, updatedSeat);
          
          const buffer = this.updateKursiBuffer.get(room);
          if (buffer) {
            if (buffer.size >= this.bufferSizeLimit * 2) {
              this.flushRoomKursiUpdates(room);
            }
            if (buffer.size >= this.bufferSizeLimit) {
              const entries = Array.from(buffer.entries());
              const firstKeys = entries.slice(0, Math.floor(this.bufferSizeLimit / 2));
              for (const [key] of firstKeys) buffer.delete(key);
            }
            const { lastPoint, lastUpdated, _version, ...bufferInfo } = updatedSeat;
            buffer.set(seatNumber, bufferInfo);
          }
          
          return updatedSeat;
        });
      } catch (error) {
        if (error.message?.includes('timeout') && retries < MAX_RETRIES - 1) {
          retries++;
          await new Promise(r => setTimeout(r, 10 * retries));
          continue;
        }
        console.error(`updateSeatAtomic error for ${room}-${seatNumber}:`, error);
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
    } catch (error) {
      console.error("clearSeatBuffer error:", error);
    }
  }

  async findEmptySeat(room, ws) {
    if (!room || !ws || !ws.idtarget) return null;
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
        const release = await this.lockManager.acquire(`seat-check-${room}-${i}`);
        try {
          const occupantId = occupancyMap.get(i);
          const seatData = seatMap.get(i);
          if (occupantId === null && (!seatData || !seatData?.namauser)) return i;
        } finally { release(); }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        if (occupantId === null && seatData?.namauser) {
          seatMap.set(i, null);
          return i;
        }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        if (occupantId && seatData?.namauser === occupantId) {
          const isOnline = await this.isUserStillConnected(occupantId);
          if (!isOnline) {
            await this.cleanupUserFromSeat(room, i, occupantId, true);
            return i;
          }
        }
      }
      return null;
    } catch (error) {
      console.error("findEmptySeat error:", error);
      return null;
    }
  }

  async assignSeatAtomic(room, seat, userId) {
    const release = await this.lockManager.acquire(`atomic-assign-${room}-${seat}`);
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      const seatMap = this.roomSeats.get(room);
      if (!occupancyMap || !seatMap) return false;
      
      const occupantId = occupancyMap.get(seat);
      const seatData = seatMap.get(seat);
      const isStillEmpty = occupantId === null && (!seatData || !seatData?.namauser);
      if (!isStillEmpty) return false;
      
      occupancyMap.set(seat, userId);
      if (!seatData || !seatData.namauser) {
        seatMap.set(seat, null);
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
          seatMap.set(seatNumber, null);
          occupancyMap.set(seatNumber, null);
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.updateRoomCount(room);
          this.userToSeat.delete(userId);
        }
      });
    } catch (error) {
      console.error(`cleanupUserFromSeat error for ${room}-${seatNumber}:`, error);
    }
  }

  scheduleCleanup(userId) {
    if (!userId) return;
    try {
      this.cancelCleanup(userId);
      const timerId = setTimeout(async () => {
        try {
          this.disconnectedTimers.delete(userId);
          const isStillConnected = await this.isUserStillConnected(userId);
          if (!isStillConnected) {
            await this.withLock(`grace-cleanup-${userId}`, async () => {
              const doubleCheck = await this.isUserStillConnected(userId);
              if (!doubleCheck) await this.forceUserCleanup(userId);
            });
          }
        } catch (error) {
          console.error("scheduleCleanup timeout error:", error);
        }
      }, this.gracePeriod);
      timerId._scheduledTime = Date.now();
      timerId._userId = userId;
      this.disconnectedTimers.set(userId, timerId);
      this._addTimer(timerId);
    } catch (error) {
      console.error("scheduleCleanup error:", error);
    }
  }

  cancelCleanup(userId) {
    if (!userId) return;
    try {
      const timer = this.disconnectedTimers.get(userId);
      if (timer) { 
        clearTimeout(timer); 
        this._allTimers.delete(timer);
        this.disconnectedTimers.delete(userId); 
      }
      this.cleanupInProgress?.delete(userId);
    } catch (error) {
      console.error("cancelCleanup error:", error);
    }
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
    } catch (error) {
      console.error(`forceUserCleanup error for ${userId}:`, error);
    } finally { 
      this.cleanupInProgress.delete(userId); 
    }
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
    } catch (error) {
      console.error("cleanupFromRoom error:", error);
    }
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
              seatMap.set(seatNumber, null);
              this.clearSeatBuffer(room, seatNumber);
              this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
            }
          }
          this.updateRoomCount(room);
        }
        
        this.userToSeat.delete(idtarget);
        this.userCurrentRoom.delete(idtarget);
        this.userConnections.delete(idtarget);
        
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
    } catch (error) {
      console.error(`fullRemoveById error for ${idtarget}:`, error);
    }
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
            this._allTimers.delete(timer);
            this.disconnectedTimers.delete(userId);
            this.executeGracePeriodCleanup(userId);
          }
        }
      }
    } catch (error) {
      console.error("validateGracePeriodTimers error:", error);
    }
  }

  async executeGracePeriodCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    this.checkAndEnableSafeMode();
    if (this.safeMode) { 
      const retryTimer = setTimeout(() => this.executeGracePeriodCleanup(userId), 5000);
      this._addTimer(retryTimer);
      return; 
    }
    this.cleanupInProgress.add(userId);
    try {
      await this.withLock(`user-cleanup-${userId}`, async () => {
        const isConnected = await this.isUserStillConnected(userId);
        if (!isConnected) await this.forceUserCleanup(userId);
      });
    } catch (error) {
      console.error("executeGracePeriodCleanup error:", error);
    } finally { 
      this.cleanupInProgress.delete(userId); 
    }
  }

  // ========== ROOM MANAGEMENT ==========
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) { 
      this.safeSend(ws, ["error", "User ID not set"]); 
      return false; 
    }
    if (!roomList.includes(room)) { 
      this.safeSend(ws, ["error", "Invalid room"]); 
      return false; 
    }
    if (!this.rateLimiter.check(ws.idtarget)) { 
      this.safeSend(ws, ["error", "Too many requests"]); 
      return false; 
    }
    
    try {
      const roomRelease = await this.lockManager.acquire(`room-join-assign-${room}`);
      try {
        this.cancelCleanup(ws.idtarget);
        await this.ensureSeatsData(room);
        
        const previousRoom = this.userCurrentRoom.get(ws.idtarget);
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
        
        const seatMap = this.roomSeats.get(room);
        if (!seatMap.has(assignedSeat) || seatMap.get(assignedSeat) === null) {
          seatMap.set(assignedSeat, null);
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
        
        const stateTimer = setTimeout(() => this.sendAllStateTo(ws, room), 100);
        this._addTimer(stateTimer);
        
        this.updateRoomCount(room);
        return true;
      } finally { 
        roomRelease(); 
      }
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
          if (oldWs && oldWs !== ws && oldWs.readyState === 1 && !oldWs._isClosing) {
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
          
          if (ws.userData) {
            ws.noimageUrl = ws.userData.noimageUrl || "";
            ws.color = ws.userData.color || "";
            ws.itembawah = ws.userData.itembawah || 0;
            ws.itematas = ws.userData.itematas || 0;
            ws.vip = ws.userData.vip || 0;
            ws.viptanda = ws.userData.viptanda || 0;
            ws.username = ws.userData.username || id;
          }
          
          this._addUserConnection(id, ws);
          this.safeSend(ws, ["joinroomawal"]);
          cleanupInfo = { needCleanup: true, id };
          return;
        }
        
        ws.idtarget = id;
        ws._connectionTime = Date.now();
        ws._isDuplicate = false;
        ws._isClosing = false;
        
        if (ws.userData) {
          ws.noimageUrl = ws.userData.noimageUrl || "";
          ws.color = ws.userData.color || "";
          ws.itembawah = ws.userData.itembawah || 0;
          ws.itematas = ws.userData.itematas || 0;
          ws.vip = ws.userData.vip || 0;
          ws.viptanda = ws.userData.viptanda || 0;
          ws.username = ws.userData.username || id;
        }
        
        const seatInfo = this.userToSeat.get(id);
        if (seatInfo) {
          const { room, seat } = seatInfo;
          
          const currentSeatInfo = this.userToSeat.get(id);
          if (!currentSeatInfo || currentSeatInfo.room !== room || currentSeatInfo.seat !== seat) {
            this._addUserConnection(id, ws);
            this.safeSend(ws, ["needJoinRoom"]);
            return;
          }
          
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
              
              if (seatData) {
                seatData.noimageUrl = ws.noimageUrl || seatData.noimageUrl;
                seatData.color = ws.color || seatData.color;
                seatData.itembawah = ws.itembawah !== undefined ? ws.itembawah : seatData.itembawah;
                seatData.itematas = ws.itematas !== undefined ? ws.itematas : seatData.itematas;
                seatData.vip = ws.vip !== undefined ? ws.vip : seatData.vip;
                seatData.viptanda = ws.viptanda !== undefined ? ws.viptanda : seatData.viptanda;
                seatData.lastUpdated = Date.now();
                seatData._version = (seatData._version || 0) + 1;
              }
              
              this.sendAllStateTo(ws, room);
              if (seatData?.lastPoint) {
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
        if (retry > 0) {
          await new Promise(r => setTimeout(r, CONSTANTS.SAFE_SEND_RETRY_DELAY));
          return this.safeSend(ws, arr, retry - 1);
        }
        return false;
      }
      ws.send(JSON.stringify(arr));
      return true;
    } catch (error) {
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
            if (e.code === 1001 || e.code === 1006) {
              const cleanupTimer = setTimeout(() => this._removeFromRoomClients(client, room), 0);
              this._addTimer(cleanupTimer);
            }
          }
        }
      }
      return sentCount;
    } catch (error) {
      console.error("broadcastToRoom error:", error);
      return 0;
    }
  }

  broadcastRoomUserCount(room) {
    try {
      if (!room || !roomList.includes(room)) return;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const seatData = seatMap.get(i);
        if (seatData && seatData.namauser) count++;
      }
      if (this.roomCountsCache) this.roomCountsCache[room] = count;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {
      console.error("broadcastRoomUserCount error:", error);
    }
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
        if (info && info.namauser) {
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
        if (info?.lastPoint) {
          lastPointsData.push({ 
            seat: seat, 
            x: info.lastPoint.x || 0, 
            y: info.lastPoint.y || 0, 
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
      console.error("sendAllStateTo error:", error);
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
    } catch (error) {
      console.error("getJumlahRoom error:", error);
      const fallback = {};
      for (const room of roomList) fallback[room] = 0;
      return fallback;
    }
  }

  invalidateRoomCache(room) { 
    this.roomCountsCache = null; 
  }

  updateRoomCount(room) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return 0;
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const seatData = seatMap.get(i);
        if (seatData && seatData.namauser) count++;
      }
      this.invalidateRoomCache(room);
      this.broadcastRoomUserCount(room);
      return count;
    } catch (error) {
      console.error("updateRoomCount error:", error);
      return 0;
    }
  }

  async fullSeatConsistencyCheck() {
    let checksPerformed = 0;
    for (const room of roomList) {
      if (checksPerformed >= CONSTANTS.MAX_SEAT_CONSISTENCY_CHECK_PER_CYCLE) break;
      try {
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        if (!seatMap || !occupancyMap) continue;
        
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
          if (checksPerformed >= CONSTANTS.MAX_SEAT_CONSISTENCY_CHECK_PER_CYCLE) break;
          checksPerformed++;
          
          const seatData = seatMap.get(seat);
          const occupant = occupancyMap.get(seat);
          
          if (seatData?.namauser && !occupant) {
            occupancyMap.set(seat, seatData.namauser);
          } else if (!seatData?.namauser && occupant) {
            occupancyMap.set(seat, null);
            seatMap.set(seat, null);
          } else if (seatData?.namauser && occupant && seatData.namauser !== occupant) {
            const isOccupantOnline = await this.isUserStillConnected(occupant);
            if (isOccupantOnline) {
              seatData.namauser = occupant;
              seatData.lastUpdated = Date.now();
            } else {
              occupancyMap.set(seat, null);
              seatMap.set(seat, null);
            }
          }
        }
      } catch (error) {
        console.error(`fullSeatConsistencyCheck error for ${room}:`, error);
      }
    }
  }

  sampledSeatConsistencyCheck() {
    try {
      const roomsToCheck = [];
      const roomCount = roomList.length;
      const sampleSize = Math.min(3, roomCount);
      for (let i = 0; i < sampleSize; i++) {
        roomsToCheck.push(roomList[Math.floor(Math.random() * roomCount)]);
      }
      for (const room of roomsToCheck) {
        if (this.getServerLoad() >= 0.8) break;
        this.validateSeatConsistency(room);
      }
    } catch (error) {
      console.error("sampledSeatConsistencyCheck error:", error);
    }
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
          seatMap.set(seat, null);
          occupancyMap.set(seat, null);
        } else if (!occupantId && seatData?.namauser) {
          const isOnline = await this.isUserStillConnected(seatData.namauser);
          if (isOnline) occupancyMap.set(seat, seatData.namauser);
          else seatMap.set(seat, null);
        } else if (occupantId && seatData?.namauser && seatData.namauser !== occupantId) {
          const isOccupantOnline = await this.isUserStillConnected(occupantId);
          if (isOccupantOnline) {
            seatData.namauser = occupantId;
          } else {
            occupancyMap.set(seat, null);
            seatMap.set(seat, null);
          }
        }
      }
    } catch (error) {
      console.error(`validateSeatConsistency error for ${room}:`, error);
    }
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
    } catch (error) {
      console.error("cleanupDuplicateConnections error:", error);
    }
  }

  getAllOnlineUsers() {
    try {
      const users = [];
      const seenUsers = new Set();
      for (const client of this.clients) {
        if (client?.idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
          if (!seenUsers.has(client.idtarget)) { 
            users.push(client.idtarget); 
            seenUsers.add(client.idtarget); 
          }
        }
      }
      return users;
    } catch (error) {
      console.error("getAllOnlineUsers error:", error);
      return [];
    }
  }

  getOnlineUsersByRoom(roomName) {
    try {
      const users = [];
      const seenUsers = new Set();
      const clientArray = this.roomClients.get(roomName);
      if (clientArray) {
        for (const client of clientArray) {
          if (client?.idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
            if (!seenUsers.has(client.idtarget)) { 
              users.push(client.idtarget); 
              seenUsers.add(client.idtarget); 
            }
          }
        }
      }
      return users;
    } catch (error) {
      console.error("getOnlineUsersByRoom error:", error);
      return [];
    }
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
            const { lastPoint, lastUpdated, _version, ...rest } = info;
            updates.push([seat, rest]);
            if (updates.length >= CONSTANTS.BROADCAST_BATCH_SIZE) {
              this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
              updates.length = 0;
            }
          }
        }
        if (updates.length > 0) this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
        this.updateKursiBuffer.set(room, new Map());
      }
    } catch (error) {
      console.error("flushKursiUpdates error:", error);
    }
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      const clientsToNotify = [];
      const notifiedUsers = new Set();
      for (const client of this.clients) {
        if (client?.readyState === 1 && client.roomname && !client._isDuplicate && !client._isClosing) {
          if (!notifiedUsers.has(client.idtarget)) { 
            clientsToNotify.push(client); 
            notifiedUsers.add(client.idtarget); 
          }
        }
      }
      for (const client of clientsToNotify) {
        this.safeSend(client, ["currentNumber", this.currentNumber]);
      }
    } catch (error) {
      console.error("tick error:", error);
    }
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
      }).catch(error => console.error("handleOnDestroy lock error:", error));
    } catch (error) {
      console.error("handleOnDestroy error:", error);
      try { 
        this.clients.delete(ws); 
        this.cancelCleanup(idtarget); 
        this._removeUserConnection(idtarget, ws); 
      } catch {}
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
      
      if (ws._pair) {
        this._activeWebSocketPairs.delete(ws._pair);
      }
      
      const cleanupTimer = setTimeout(() => { 
        ws.roomname = null; 
        ws.idtarget = null; 
        ws.numkursi = null;
        ws._kursiUpdateDebounce = null;
        ws._isDuplicate = null;
        ws._isClosing = null;
        ws._connectionTime = null;
        ws._pair = null;
      }, 1000);
      this._addTimer(cleanupTimer);
      
    } catch (error) {
      console.error("safeWebSocketCleanup error:", error);
      this.clients.delete(ws); 
      if (userId) this.cancelCleanup(userId); 
    }
  }

  // ========== MESSAGE HANDLING ==========
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
      } catch (error) {
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
            this.savePointWithRetry(room, seat, x, y, fast).catch(error => console.error("updatePoint error:", error));
            this.broadcastPointDirect(room, seat, x, y, fast);
            break;
          }
          case "removeKursiAndPoint": {
            const [, room, seat] = data;
            if (seat < 1 || seat > this.MAX_SEATS) return;
            if (ws.roomname !== room || !roomList.includes(room)) return;
            await this.updateSeatAtomic(room, seat, () => null);
            this.clearSeatBuffer(room, seat);
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
            break;
          }
          case "updateKursi": {
            const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
            
            if (seat < 1 || seat > this.MAX_SEATS) return;
            if (ws.roomname !== room || !roomList.includes(room)) return;
            
            const debounceKey = `${room}-${seat}`;
            
            const existingTimer = this._kursiUpdateDebounce.get(debounceKey);
            if (existingTimer) {
              clearTimeout(existingTimer);
              this._kursiUpdateDebounce.delete(debounceKey);
            }
            
            let isExecuted = false;
            
            const cleanupTimer = () => {
              if (this._kursiUpdateDebounce.get(debounceKey) === timerId) {
                this._kursiUpdateDebounce.delete(debounceKey);
              }
            };
            
            const executeUpdate = async () => {
              if (isExecuted) return;
              isExecuted = true;
              
              try {
                const result = await this.updateSeatAtomic(room, seat, (currentSeat) => {
                  const updatedSeat = currentSeat ? { ...currentSeat } : {
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
                  
                  updatedSeat.noimageUrl = noimageUrl || updatedSeat.noimageUrl;
                  updatedSeat.namauser = namauser || updatedSeat.namauser;
                  updatedSeat.color = color || updatedSeat.color;
                  updatedSeat.itembawah = itembawah !== undefined ? itembawah : updatedSeat.itembawah;
                  updatedSeat.itematas = itematas !== undefined ? itematas : updatedSeat.itematas;
                  updatedSeat.vip = vip !== undefined ? vip : updatedSeat.vip;
                  updatedSeat.viptanda = viptanda !== undefined ? viptanda : updatedSeat.viptanda;
                  
                  return updatedSeat;
                });
                
                if (namauser === ws.idtarget) {
                  ws.userData = {
                    noimageUrl: noimageUrl || "",
                    color: color || "",
                    itembawah: itembawah || 0,
                    itematas: itematas || 0,
                    vip: vip || 0,
                    viptanda: viptanda || 0,
                    username: namauser
                  };
                  ws.noimageUrl = noimageUrl;
                  ws.color = color;
                  ws.itembawah = itembawah;
                  ws.itematas = itematas;
                  ws.vip = vip;
                  ws.viptanda = viptanda;
                  ws.username = namauser;
                  
                  if (namauser) {
                    this.userToSeat.set(namauser, { room, seat });
                    this.userCurrentRoom.set(namauser, room);
                  }
                }
                
                this.updateRoomCount(room);
                
                const response = ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda];
                
                const clientArray = this.roomClients.get(room);
                if (clientArray && clientArray.length > 0) {
                  const message = JSON.stringify(response);
                  for (const client of clientArray) {
                    if (client !== ws && client?.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
                      try { client.send(message); } catch {}
                    }
                  }
                }
                
                this.safeSend(ws, response);
                
              } catch (error) {
                console.error("Update kursi error:", error);
                this.safeSend(ws, ["error", "Failed to update seat"]);
              } finally {
                cleanupTimer();
              }
            };
            
            const timerId = setTimeout(async () => {
              try {
                await executeUpdate();
              } catch (error) {
                console.error("Debounce execution failed:", error);
              } finally {
                if (this._kursiUpdateDebounce.get(debounceKey) === timerId) {
                  this._kursiUpdateDebounce.delete(debounceKey);
                }
              }
            }, CONSTANTS.KURSI_UPDATE_DEBOUNCE);
            
            timerId._createdAt = Date.now();
            timerId._cleanup = cleanupTimer;
            this._kursiUpdateDebounce.set(debounceKey, timerId);
            this._addTimer(timerId);
            
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
            if (GAME_ROOMS.includes(ws.roomname)) {
              if (this.lowcard) {
                if (this.lowcard.activeGames?.size >= CONSTANTS.MAX_GAMES_CONCURRENT) {
                  this.safeSend(ws, ["error", "Game system busy, please wait"]);
                  break;
                }
                await this.lowcard.handleEvent(ws, data);
              } else {
                this.safeSend(ws, ["error", "Game system not available"]);
              }
            }
            break;
          default: break;
        }
        
        this.flushKursiUpdates();
        this.flushBufferedPoints();
        
      } catch (error) {
        console.error("Message handling error:", error);
        if (ws.readyState === 1) this.safeSend(ws, ["error", "Server error"]);
      }
    } catch (error) {
      console.error("handleMessage outer error:", error);
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
        return new Response("Server overloaded", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      this._activeWebSocketPairs.add(pair);
      
      await server.accept();
      
      const ws = server;
      ws._pair = pair;
      ws._connId = `conn#${this._nextConnId++}`;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();
      ws.isManualDestroy = false;
      ws.errorCount = 0;
      ws._isDuplicate = false;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      ws.userData = null;
      ws.noimageUrl = "";
      ws.color = "";
      ws.itembawah = 0;
      ws.itematas = 0;
      ws.vip = 0;
      ws.viptanda = 0;
      ws.username = "";
      
      this.clients.add(ws);
      this._lastActivityTime = Date.now();
      
      const messageQueue = new MessageQueue(ws, this);
      this._messageQueues.set(ws, messageQueue);
      
      ws.addEventListener("message", (ev) => {
        messageQueue.add(ev.data).catch(error => console.error("Message queue add error:", error));
      });
      
      ws.addEventListener("error", (error) => {
        console.error("WebSocket error:", error);
      });
      
      ws.addEventListener("close", (event) => {
        Promise.resolve().then(() => {
          if (event.code !== 1000 || event.reason !== "Replaced by new connection") {
            this.safeWebSocketCleanup(ws);
          } else {
            this.clients.delete(ws);
            if (ws.idtarget) this._removeUserConnection(ws.idtarget, ws);
            if (ws.roomname) this._removeFromRoomClients(ws, ws.roomname);
          }
          this._messageQueues.delete(ws);
          if (ws._pair) {
            this._activeWebSocketPairs.delete(ws._pair);
          }
        });
      });
      
      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error("fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }
  
  async cleanup() {
    try {
      await this.performMemoryCleanup();
    } catch (error) {
      console.error("cleanup performMemoryCleanup error:", error);
    }
    try {
      await this.cleanupDuplicateConnections();
    } catch (error) {
      console.error("cleanup cleanupDuplicateConnections error:", error);
    }
    try {
      this.validateGracePeriodTimers();
    } catch (error) {
      console.error("cleanup validateGracePeriodTimers error:", error);
    }
    try {
      this.sampledSeatConsistencyCheck();
    } catch (error) {
      console.error("cleanup sampledSeatConsistencyCheck error:", error);
    }
    try {
      await this.fullSeatConsistencyCheck();
    } catch (error) {
      console.error("cleanup fullSeatConsistencyCheck error:", error);
    }
  }
  
  async getHealthStatus() {
    const now = Date.now();
    const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
    
    return {
      status: activeConnections > 450 ? "degraded" : "healthy",
      uptime: now - this._startTime,
      activeConnections,
      memoryPressure: this.getServerLoad(),
      queueHealth: {
        size: this.cleanupQueue?.size() || 0,
        active: this.cleanupQueue?.active || 0,
        healthy: (this.cleanupQueue?.size() || 0) < 100
      },
      debounceHealth: {
        size: this._kursiUpdateDebounce.size,
        healthy: this._kursiUpdateDebounce.size < 500
      },
      gameHealth: {
        active: this.lowcard?.activeGames?.size || 0,
        max: CONSTANTS.MAX_GAMES_CONCURRENT,
        healthy: (this.lowcard?.activeGames?.size || 0) < CONSTANTS.MAX_GAMES_CONCURRENT
      },
      failedBatches: this._failedBatches?.length || 0,
      messageQueues: this._messageQueues.size
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
          debounceSize: obj._kursiUpdateDebounce?.size || 0,
          failedBatches: obj._failedBatches?.length || 0,
          messageQueues: obj._messageQueues?.size || 0,
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
};
