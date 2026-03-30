import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambayan", "Lounge Talk",
  "Noxxeliverothcifsa", "One Side Love", "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// ========== CONSTANTS ==========
const CONSTANTS = {
  MAX_QUEUE_SIZE: 200,
  MAX_LOCK_QUEUE_SIZE: 50, // Dikurangi dari 100
  LOCK_ACQUIRE_TIMEOUT: 2000, // Dikurangi dari 5000
  BUFFER_SIZE_LIMIT: 20,
  BROADCAST_BATCH_SIZE: 20,
  CACHE_VALID_DURATION: 5000,
  LOAD_THRESHOLD: 0.85,
  LOAD_RECOVERY_THRESHOLD: 0.65,
  KURSI_UPDATE_DEBOUNCE: 150,
  DEBOUNCE_CLEANUP_INTERVAL: 30000,
  MAX_DEBOUNCE_AGE: 20000,
  LOCK_TIMEOUT: 3000, // Dikurangi dari 10000
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
  MAX_MAP_SIZE: 1000, // Baru: batas ukuran Map
  MAX_PENDING_RECONNECTIONS: 200, // Baru: batas pending reconnections
  MAX_DISCONNECTED_TIMERS: 200, // Baru: batas timer grace period
  MAX_LOCK_QUEUE_HARD_LIMIT: 50, // Baru: batas keras lock queue
  BROADCAST_BACKPRESSURE_LIMIT: 512 * 1024, // Baru: 512KB limit per client
  CLEANUP_BATCH_SIZE: 10, // Baru: batch size untuk cleanup
  CLEANUP_BATCH_DELAY: 100, // Baru: delay antar batch
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

// ========== FIXED: PromiseLockManager dengan Batas Queue ==========
class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
    this.lockTimestamps = new Map();
    this._destroyed = false;
    this._lockId = 0;
  }

  async acquire(resourceId, priority = false) {
    if (this._destroyed) throw new Error("LockManager destroyed");
    
    const lockId = ++this._lockId;
    
    // Cleanup stuck locks
    if (this.locks.has(resourceId)) {
      const lockTime = this.lockTimestamps.get(resourceId) || 0;
      if (Date.now() - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
      }
    }

    const currentQueue = this.queue.get(resourceId) || [];
    
    // FIX: Batas keras untuk lock queue
    if (currentQueue.length >= CONSTANTS.MAX_LOCK_QUEUE_HARD_LIMIT) {
      if (priority) {
        // Priority operation: hapus queue tertua
        const oldest = currentQueue.shift();
        if (oldest && oldest.reject) {
          oldest.reject(new Error("Lock queue full - priority override"));
        }
      } else {
        throw new Error(`Lock queue full for ${resourceId}`);
      }
    }

    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, true);
      this.lockTimestamps.set(resourceId, Date.now());
      return () => this.release(resourceId, lockId);
    }

    if (!this.queue.has(resourceId)) this.queue.set(resourceId, []);

    // FIX: Timeout adaptif berdasarkan panjang queue
    const timeoutDuration = currentQueue.length > 20 ? 3000 : CONSTANTS.LOCK_ACQUIRE_TIMEOUT;

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
      }, timeoutDuration);
      
      this.queue.get(resourceId).push({ 
        lockId,
        priority,
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
      
      // Sort by priority
      this.queue.get(resourceId).sort((a, b) => {
        if (a.priority === b.priority) return 0;
        return a.priority ? -1 : 1;
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

// ========== FIXED: QueueManager dengan Prioritas dan Batas ==========
class QueueManager {
  constructor(concurrency = 3) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = CONSTANTS.MAX_QUEUE_SIZE;
    this.processing = false;
    this.destroyed = false;
  }

  async add(job, options = {}) {
    if (this.destroyed) throw new Error("Queue manager destroyed");
    
    const { priority = 'normal', timeout = 10000 } = options;
    const priorityValue = { high: 0, normal: 1, low: 2 }[priority];
    
    // FIX: Batas keras dengan prioritas
    if (this.queue.length >= CONSTANTS.MAX_QUEUE_HARD_LIMIT) {
      if (priority === 'high') {
        // High priority: hapus low priority tertua
        const lowPriorityIndex = this.queue.findIndex(item => item.priority === 2);
        if (lowPriorityIndex !== -1) {
          const removed = this.queue.splice(lowPriorityIndex, 1)[0];
          if (removed.reject) {
            removed.reject(new Error("Queue cleared for high priority"));
          }
        } else {
          throw new Error("Queue full - no low priority jobs to remove");
        }
      } else {
        throw new Error("Server busy - queue full");
      }
    }
    
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const index = this.queue.findIndex(item => item.reject === reject);
        if (index !== -1) {
          this.queue.splice(index, 1);
          reject(new Error("Queue timeout"));
        }
      }, timeout);
      
      this.queue.push({ 
        job, 
        resolve, 
        reject, 
        timeoutId, 
        timestamp: Date.now(),
        priority: priorityValue
      });
      
      // Sort by priority
      this.queue.sort((a, b) => a.priority - b.priority);
      
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
            // Cleanup expired jobs
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

// ========== FIXED: RateLimiter dengan Cleanup ==========
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
    
    // FIX: Batasi ukuran Map
    if (this.requests.size > CONSTANTS.MAX_MAP_SIZE) {
      const oldest = Array.from(this.requests.keys())[0];
      this.requests.delete(oldest);
    }
    
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

// ========== FIXED: MessageQueue dengan Batas Keras ==========
class MessageQueue {
  constructor(ws, chatServer) {
    this.ws = ws;
    this.chatServer = chatServer;
    this.queue = [];
    this.processing = false;
    this.maxSize = CONSTANTS.MAX_MESSAGE_QUEUE_SIZE;
    this.droppedCount = 0;
  }
  
  async add(rawMessage) {
    // FIX: Batas keras dengan dropping old messages
    if (this.queue.length >= this.maxSize) {
      this.droppedCount++;
      
      // Hapus 30% pesan tertua
      const toRemove = Math.floor(this.queue.length * 0.3);
      this.queue.splice(0, toRemove);
      
      // Jika masih penuh, tolak
      if (this.queue.length >= this.maxSize) {
        return false;
      }
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
    
    // FIX: Batch processing untuk efisiensi
    const BATCH_SIZE = 5;
    
    while (this.queue.length > 0 && this.ws.readyState === 1) {
      const batch = [];
      for (let i = 0; i < BATCH_SIZE && this.queue.length > 0; i++) {
        batch.push(this.queue.shift());
      }
      
      for (const raw of batch) {
        try {
          await this.chatServer.handleMessage(this.ws, raw);
        } catch (error) {
          // Error already handled
        }
      }
      
      // Small delay to prevent event loop blocking
      if (this.queue.length > 0) {
        await new Promise(resolve => setImmediate(resolve));
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
    this.droppedCount = 0;
  }
  
  size() {
    return this.queue.length;
  }
}

// ========== FIXED: SafeGracePeriodManager ==========
class SafeGracePeriodManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.cleanupTasks = new Map();
    this.sessionVersions = new Map();
    this.pendingCleanups = new Map();
    this.batchInterval = null;
    this.gracePeriod = CONSTANTS.GRACE_PERIOD;
  }
  
  start() {
    if (this.batchInterval) return;
    this.batchInterval = setInterval(() => this.processBatch(), 1000);
  }
  
  stop() {
    if (this.batchInterval) {
      clearInterval(this.batchInterval);
      this.batchInterval = null;
    }
  }
  
  scheduleCleanup(userId) {
    if (!userId) return;
    
    // FIX: Batas jumlah timer aktif
    if (this.cleanupTasks.size > CONSTANTS.MAX_DISCONNECTED_TIMERS) {
      // Force cleanup tertua
      const oldest = Array.from(this.cleanupTasks.keys())[0];
      if (oldest) {
        this.forceCleanupNow(oldest);
      }
    }
    
    // Increment version untuk invalidate cleanup sebelumnya
    const version = (this.sessionVersions.get(userId) || 0) + 1;
    this.sessionVersions.set(userId, version);
    
    // Simpan ke pending batch
    this.pendingCleanups.set(userId, {
      userId,
      version,
      scheduledAt: Date.now()
    });
  }
  
  async processBatch() {
    const now = Date.now();
    const toCleanup = [];
    
    // Ambil yang sudah melewati grace period
    for (const [userId, data] of this.pendingCleanups) {
      if (now - data.scheduledAt >= this.gracePeriod) {
        toCleanup.push(data);
      }
    }
    
    if (toCleanup.length === 0) return;
    
    // Proses dalam batch kecil
    for (let i = 0; i < toCleanup.length; i += CONSTANTS.CLEANUP_BATCH_SIZE) {
      const batch = toCleanup.slice(i, i + CONSTANTS.CLEANUP_BATCH_SIZE);
      
      await Promise.allSettled(
        batch.map(async (data) => {
          // Cek versi terbaru
          const currentVersion = this.sessionVersions.get(data.userId);
          if (currentVersion === data.version) {
            // Tidak ada reconnect, aman untuk cleanup
            await this.chatServer.forceUserCleanup(data.userId);
          }
          this.pendingCleanups.delete(data.userId);
          this.cleanupTasks.delete(data.userId);
        })
      );
      
      // Delay antar batch
      if (i + CONSTANTS.CLEANUP_BATCH_SIZE < toCleanup.length) {
        await new Promise(r => setTimeout(r, CONSTANTS.CLEANUP_BATCH_DELAY));
      }
    }
    
    // FIX: Batasi ukuran Map
    if (this.sessionVersions.size > CONSTANTS.MAX_MAP_SIZE) {
      const oldest = Array.from(this.sessionVersions.keys())[0];
      this.sessionVersions.delete(oldest);
    }
  }
  
  cancelCleanup(userId) {
    if (!userId) return;
    
    // Hapus dari pending
    this.pendingCleanups.delete(userId);
    this.cleanupTasks.delete(userId);
    
    // Increment version untuk invalidate
    this.sessionVersions.set(userId, (this.sessionVersions.get(userId) || 0) + 1);
  }
  
  async forceCleanupNow(userId) {
    this.pendingCleanups.delete(userId);
    this.cleanupTasks.delete(userId);
    await this.chatServer.forceUserCleanup(userId);
  }
  
  cleanup() {
    const now = Date.now();
    
    // Hapus pending cleanup yang terlalu tua
    for (const [userId, data] of this.pendingCleanups) {
      if (now - data.scheduledAt > this.gracePeriod + 10000) {
        this.pendingCleanups.delete(userId);
        this.cleanupTasks.delete(userId);
      }
    }
    
    // Batasi ukuran
    if (this.sessionVersions.size > CONSTANTS.MAX_MAP_SIZE) {
      const entries = Array.from(this.sessionVersions.entries());
      const toKeep = entries.slice(-CONSTANTS.MAX_MAP_SIZE);
      this.sessionVersions.clear();
      for (const [k, v] of toKeep) {
        this.sessionVersions.set(k, v);
      }
    }
  }
  
  destroy() {
    this.stop();
    this.pendingCleanups.clear();
    this.cleanupTasks.clear();
    this.sessionVersions.clear();
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

// ========== FIXED: ChatServer dengan Semua Perbaikan ==========
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
      
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;

      // FIX: Grace period manager
      this.graceManager = new SafeGracePeriodManager(this);
      this.graceManager.start();

      try { this.lowcard = new LowCardGameManager(this); } catch { this.lowcard = null; }

      this.cleanupQueue = new QueueManager(3);
      
      this.currentNumber = 1;
      this.maxNumber = CONSTANTS.MAX_NUMBER;
      this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
      this._nextConnId = 1;
      this.lastNumberTick = Date.now();
      
      this.numberTickTimer = null;

      this._intervals = [];
      this._broadcastBuffer = new Map();
      this._broadcastTimers = new Map();
      this._pendingUpdates = new Map();
      this._roomsWithChanges = new Set();

      try { this.initializeRooms(); } catch { this.createDefaultRoom(); }

      this.startNumberTickTimer();
      
      this.roomCountsCache = null;
      this._countsCacheTime = 0;
      this.cacheValidDuration = CONSTANTS.CACHE_VALID_DURATION;

      for (const room of roomList) {
        this._pointBuffer.set(room, []);
      }

      this.loadState();
      this.startAutoCleanup();
      this.startIdleCleanup();
      this.startMemoryMonitor();
      this.startForcedCleanup();
      this.startEmergencyCleanup();
      this.startPeriodicFlush();
      this.startMapCleanup(); // FIX: Periodic map cleanup

    } catch (error) {
      console.error("ChatServer constructor error:", error);
      this.initializeFallback();
    }
  }

  // FIX: Periodic map cleanup
  startMapCleanup() {
    this._mapCleanupInterval = setInterval(() => {
      try {
        // Bersihkan message queues yang sudah mati
        for (const [ws, queue] of this._messageQueues) {
          if (!ws || ws.readyState !== 1) {
            if (queue) queue.clear();
            this._messageQueues.delete(ws);
          }
        }
        
        // Batasi _failedBatches
        if (this._failedBatches && this._failedBatches.length > CONSTANTS.MAX_FAILED_BATCHES) {
          this._failedBatches = this._failedBatches.slice(-Math.floor(CONSTANTS.MAX_FAILED_BATCHES / 2));
        }
        
        // Batasi _pendingReconnections (fallback)
        if (this._pendingReconnections && this._pendingReconnections.size > CONSTANTS.MAX_PENDING_RECONNECTIONS) {
          const entries = Array.from(this._pendingReconnections.entries());
          const toKeep = entries.slice(-CONSTANTS.MAX_PENDING_RECONNECTIONS);
          this._pendingReconnections.clear();
          for (const [k, v] of toKeep) {
            this._pendingReconnections.set(k, v);
          }
        }
        
        // Batasi userConnections
        if (this.userConnections && this.userConnections.size > CONSTANTS.MAX_MAP_SIZE) {
          const entries = Array.from(this.userConnections.entries());
          const toKeep = entries.slice(-CONSTANTS.MAX_MAP_SIZE);
          this.userConnections.clear();
          for (const [k, v] of toKeep) {
            this.userConnections.set(k, v);
          }
        }
        
        // Cleanup grace manager
        if (this.graceManager) {
          this.graceManager.cleanup();
        }
        
      } catch(e) {}
    }, 30000);
    this._intervals.push(this._mapCleanupInterval);
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
        
        if (heapPercent > 0.9) {
          for (const [room, buffer] of this.updateKursiBuffer) {
            if (buffer) buffer.clear();
          }
          if (global.gc) global.gc();
        }
        else if (heapPercent > 0.8) {
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
        
        // FIX: Emergency cleanup untuk message queues
        if (this._messageQueues && this._messageQueues.size > 500) {
          let toRemove = [];
          for (const [ws, queue] of this._messageQueues) {
            if (ws.readyState !== 1 || queue.size() > 200) {
              toRemove.push(ws);
            }
          }
          for (const ws of toRemove) {
            const queue = this._messageQueues.get(ws);
            if (queue) queue.clear();
            this._messageQueues.delete(ws);
          }
        }
        
      } catch(e) {}
    }, CONSTANTS.EMERGENCY_CLEANUP_INTERVAL);
    this._intervals.push(this._emergencyCleanup);
  }

  startForcedCleanup() {
    this._forcedCleanupInterval = setInterval(() => {
      try {
        const now = Date.now();
        
        // Cleanup grace manager
        if (this.graceManager) {
          this.graceManager.cleanup();
        }
        
        this.roomRateLimiter.cleanup();
        
        if (global.gc && Math.random() < 0.1) {
          global.gc();
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
    if (typeof gc !== 'undefined') {
      this._memoryMonitor = setInterval(() => {
        try {
          const activeConns = Array.from(this.clients).filter(c => c?.readyState === 1).length;
          const queueSize = this.cleanupQueue?.size() || 0;
          
          if (queueSize > 100) {
            if (global.gc) global.gc();
          }
          
          const memUsage = process.memoryUsage();
          if (memUsage.heapUsed / memUsage.heapTotal > CONSTANTS.EMERGENCY_MEMORY_THRESHOLD) {
            this.flushKursiUpdates();
            if (global.gc) global.gc();
          }
        } catch (e) {}
      }, 30000);
      this._intervals.push(this._memoryMonitor);
    }
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
    
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    this.flushKursiUpdates();
    
    await this.saveState();
    
    for (const client of this.clients) {
      try {
        client.close(1000, "Server shutdown");
      } catch(e) {}
    }
    
    await this.destroy();
  }

  async destroy() {
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
    if (this._mapCleanupInterval) {
      clearInterval(this._mapCleanupInterval);
      this._mapCleanupInterval = null;
    }
    
    if (this.graceManager) {
      this.graceManager.destroy();
    }
    
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
    this._broadcastBuffer.clear();
    this._broadcastTimers.clear();
    this._pendingUpdates.clear();
    this._roomsWithChanges.clear();
  }

  // FIX: Broadcast dengan backpressure
  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      const clientArray = this.roomClients.get(room);
      if (!clientArray?.length) return 0;
      
      let sentCount = 0;
      const message = JSON.stringify(msg);
      
      for (const client of clientArray) {
        if (client?.readyState !== 1) continue;
        if (client.roomname !== room) continue;
        if (client._isDuplicate || client._isClosing) continue;
        
        // FIX: Backpressure - skip if client buffer is full
        if (client.bufferedAmount > CONSTANTS.BROADCAST_BACKPRESSURE_LIMIT) {
          continue;
        }
        
        try { 
          client.send(message); 
          sentCount++; 
        } catch (e) {
          if (e.code === 1001 || e.code === 1006 || e.message?.includes('closed') || e.message?.includes('CLOSED')) {
            setTimeout(() => this._removeFromRoomClients(client, room), 0);
          }
        }
      }
      return sentCount;
    } catch { return 0; }
  }

  // FIX: Schedule broadcast dengan debounce
  scheduleBroadcast(room, msg) {
    if (!this._broadcastBuffer.has(room)) {
      this._broadcastBuffer.set(room, []);
    }
    
    const buffer = this._broadcastBuffer.get(room);
    buffer.push(msg);
    
    if (buffer.length >= CONSTANTS.BROADCAST_BATCH_SIZE) {
      this.flushBroadcastBuffer(room);
    } else if (!this._broadcastTimers.has(room)) {
      const timer = setTimeout(() => this.flushBroadcastBuffer(room), 50);
      this._broadcastTimers.set(room, timer);
    }
  }
  
  flushBroadcastBuffer(room) {
    const timer = this._broadcastTimers.get(room);
    if (timer) {
      clearTimeout(timer);
      this._broadcastTimers.delete(room);
    }
    
    const buffer = this._broadcastBuffer.get(room);
    if (!buffer || buffer.length === 0) return;
    
    this._broadcastBuffer.set(room, []);
    
    // Combine updates jika memungkinkan
    if (buffer.length > 1 && buffer[0][0] === 'kursiBatchUpdate') {
      const allUpdates = [];
      for (const msg of buffer) {
        if (msg[0] === 'kursiBatchUpdate' && Array.isArray(msg[3])) {
          allUpdates.push(...msg[3]);
        }
      }
      if (allUpdates.length > 0) {
        this.broadcastToRoom(room, ['kursiBatchUpdate', room, allUpdates]);
        return;
      }
    }
    
    for (const msg of buffer) {
      this.broadcastToRoom(room, msg);
    }
  }

  async loadState() {
    try {
      if (this.storage) {
        const results = await Promise.race([
          Promise.all([
            this.storage.get("currentNumber"),
            this.storage.get("lastNumberTick")
          ]),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error("Storage timeout")), CONSTANTS.STORAGE_TIMEOUT)
          )
        ]);
        
        if (results && results[0]) this.currentNumber = results[0];
        if (results && results[1]) this.lastNumberTick = results[1];
      }
    } catch (error) {
      this.currentNumber = 1;
      this.lastNumberTick = Date.now();
    }
  }

  async saveState() {
    try {
      if (this.storage) {
        await Promise.race([
          Promise.all([
            this.storage.put("currentNumber", this.currentNumber),
            this.storage.put("lastNumberTick", this.lastNumberTick)
          ]),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error("Storage timeout")), CONSTANTS.STORAGE_TIMEOUT)
          )
        ]);
      }
    } catch (error) {
      // Silent fail
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
    try {
      this.clients = new Set();
      this.userToSeat = new Map();
      this.userCurrentRoom = new Map();
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.roomClients = new Map();
      this.updateKursiBuffer = new Map();
      this.userConnections = new Map();
      this._pendingReconnections = new Map();
      this.lockManager = new PromiseLockManager();
      this.cleanupInProgress = new Set();
      this.MAX_SEATS = CONSTANTS.MAX_SEATS;
      this.currentNumber = 1;
      this._nextConnId = 1;
      this.lowcard = null;
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
      this._intervals = [];
      this.roomCountsCache = null;
      this._countsCacheTime = 0;
      this._startTime = Date.now();
      this._lastActivityTime = Date.now();
      this._failedBatches = [];
      this._isShuttingDown = false;
      this._messageQueues = new Map();
      this.graceManager = new SafeGracePeriodManager(this);
      this.graceManager.start();
      this._broadcastBuffer = new Map();
      this._broadcastTimers = new Map();
      this._pendingUpdates = new Map();
      this._roomsWithChanges = new Set();
      
      this.createDefaultRoom();
      this.lastNumberTick = Date.now();
      this.numberTickTimer = null;
      this.startNumberTickTimer();
      this.startAutoCleanup();
      this.startIdleCleanup();
      this.startMemoryMonitor();
      this.startForcedCleanup();
      this.startEmergencyCleanup();
      this.startPeriodicFlush();
      this.startMapCleanup();
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
          if (!this._pendingReconnections?.has(userId)) {
            setTimeout(() => this.forceUserCleanup(userId), 0);
          }
          this.userConnections.delete(userId);
        } else if (activeConnections.size !== connections.size) {
          this.userConnections.set(userId, activeConnections);
        }
      }

      for (const [room, buffer] of this.updateKursiBuffer) {
        if (buffer && buffer.size > CONSTANTS.MAX_BUFFER_PER_ROOM) {
          const entries = Array.from(buffer.entries());
          const newBuffer = new Map(entries.slice(-CONSTANTS.MAX_BUFFER_PER_ROOM));
          this.updateKursiBuffer.set(room, newBuffer);
        }
      }

      this.lockManager?.cleanupStuckLocks();
      this.rateLimiter.cleanup();
      this.connectionRateLimiter.cleanup();
      this.roomRateLimiter.cleanup();
      this.graceManager?.cleanup();

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
    
    // FIX: Batasi ukuran Set per user
    if (userConnections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
      const oldest = Array.from(userConnections)[0];
      if (oldest && oldest.readyState === 1) {
        oldest._isDuplicate = true;
        try { oldest.close(1000, "Too many connections"); } catch {}
        userConnections.delete(oldest);
      }
    }
    
    // FIX: Batasi total ukuran Map
    if (this.userConnections.size > CONSTANTS.MAX_MAP_SIZE) {
      const oldest = Array.from(this.userConnections.keys())[0];
      this.userConnections.delete(oldest);
    }
    
    userConnections.add(ws);
  }

  _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    const userConnections = this.userConnections.get(userId);
    if (userConnections) {
      userConnections.delete(ws);
      if (userConnections.size === 0 && !this._pendingReconnections?.has(userId)) {
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
          // FIX: Backpressure check
          if (client.bufferedAmount > CONSTANTS.BROADCAST_BACKPRESSURE_LIMIT) continue;
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
            if (!isOnline && !this._pendingReconnections?.has(occupantId)) {
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
          if (this._pendingReconnections?.has(userId)) {
            occupancyMap.set(seatNumber, null);
            return;
          }
          
          Object.assign(seatInfo, createEmptySeat());
          occupancyMap.set(seatNumber, null);
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.updateRoomCount(room);
          this.userToSeat.delete(userId);
        }
      });
    } catch {}
  }

  // FIX: Menggunakan SafeGracePeriodManager
  scheduleCleanup(userId) {
    if (!userId) return;
    this.graceManager.scheduleCleanup(userId);
  }

  cancelCleanup(userId) {
    if (!userId) return;
    this.graceManager.cancelCleanup(userId);
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
        
        // FIX: Batch cleanup
        for (let i = 0; i < seatsToCleanup.length; i += CONSTANTS.CLEANUP_BATCH_SIZE) {
          const batch = seatsToCleanup.slice(i, i + CONSTANTS.CLEANUP_BATCH_SIZE);
          await Promise.allSettled(batch.map(({ room, seatNumber }) => 
            this.cleanupUserFromSeat(room, seatNumber, userId, true)
          ));
          if (i + CONSTANTS.CLEANUP_BATCH_SIZE < seatsToCleanup.length) {
            await new Promise(r => setTimeout(r, CONSTANTS.CLEANUP_BATCH_DELAY));
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
        if (this._pendingReconnections) this._pendingReconnections.delete(idtarget);
        
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
    // Handled by SafeGracePeriodManager
    if (this.graceManager) {
      this.graceManager.cleanup();
    }
  }

  async executeGracePeriodCleanup(userId) {
    // Handled by SafeGracePeriodManager
    if (this.graceManager) {
      await this.graceManager.forceCleanupNow(userId);
    }
  }

  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) { this.safeSend(ws, ["error", "User ID not set"]); return false; }
    if (!roomList.includes(room)) { this.safeSend(ws, ["error", "Invalid room"]); return false; }
    if (!this.rateLimiter.check(ws.idtarget)) { this.safeSend(ws, ["error", "Too many requests"]); return false; }
    
    try {
      const roomRelease = await this.lockManager.acquire(`room-join-assign-${room}`);
      try {
        this.cancelCleanup(ws.idtarget);
        await this.ensureSeatsData(room);
        
        const previousRoom = this.userCurrentRoom.get(ws.idtarget);
        
        const pendingData = this._pendingReconnections?.get(ws.idtarget);
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
                
                if (this._pendingReconnections) this._pendingReconnections.delete(ws.idtarget);
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
    } catch (error) {
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
        
        const pendingData = this._pendingReconnections?.get(id);
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
              
              if (this._pendingReconnections) this._pendingReconnections.delete(id);
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
      this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }

  async safeSend(ws, arr, retry = CONSTANTS.SAFE_SEND_RETRY) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing) return false;
      // FIX: Backpressure check
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

  // FIX: Optimized getJumlahRoom dengan incremental update
  getJumlahRoom() {
    try {
      const now = Date.now();
      const cacheDuration = this.getServerLoad() > 0.7 ? 10000 : 5000;
      
      if (this.roomCountsCache && this._countsCacheTime && 
          (now - this._countsCacheTime) < cacheDuration) {
        return this.roomCountsCache;
      }
      
      // Incremental update jika memungkinkan
      const counts = this.roomCountsCache ? { ...this.roomCountsCache } : {};
      let changed = false;
      
      if (this._roomsWithChanges && this._roomsWithChanges.size > 0) {
        for (const room of this._roomsWithChanges) {
          const newCount = this._calculateRoomCount(room);
          if (counts[room] !== newCount) {
            counts[room] = newCount;
            changed = true;
          }
        }
        this._roomsWithChanges.clear();
      }
      
      if (changed || !this.roomCountsCache) {
        // Full calculation if needed
        if (!changed) {
          for (const room of roomList) {
            const occupancyMap = this.seatOccupancy.get(room);
            if (!occupancyMap) continue;
            let count = 0;
            for (let i = 1; i <= this.MAX_SEATS; i++) {
              if (occupancyMap.get(i)) count++;
            }
            counts[room] = count;
          }
        }
        this.roomCountsCache = counts;
        this._countsCacheTime = now;
      }
      
      return counts;
    } catch {
      const fallback = {};
      for (const room of roomList) fallback[room] = 0;
      return fallback;
    }
  }
  
  _calculateRoomCount(room) {
    const occupancyMap = this.seatOccupancy.get(room);
    if (!occupancyMap) return 0;
    let count = 0;
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancyMap.get(i)) count++;
    }
    return count;
  }

  invalidateRoomCache(room) { 
    this.roomCountsCache = null;
    if (this._roomsWithChanges) {
      this._roomsWithChanges.add(room);
    }
  }

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
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      if (!seatMap || !occupancyMap) continue;
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const seatData = seatMap.get(seat);
        const occupant = occupancyMap.get(seat);
        
        if (seatData?.namauser && !occupant && !this._pendingReconnections?.has(seatData.namauser)) {
          occupancyMap.set(seat, seatData.namauser);
        } else if (!seatData?.namauser && occupant) {
          occupancyMap.set(seat, null);
        } else if (seatData?.namauser && occupant && seatData.namauser !== occupant) {
          const isOccupantOnline = await this.isUserStillConnected(occupant);
          if (isOccupantOnline) {
            seatData.namauser = occupant;
            seatData.lastUpdated = Date.now();
          } else if (!this._pendingReconnections?.has(occupant)) {
            occupancyMap.set(seat, null);
          }
        }
      }
    }
  }

  sampledSeatConsistencyCheck() {
    try {
      const roomsToCheck = [];
      const roomCount = roomList.length;
      for (let i = 0; i < 3; i++) roomsToCheck.push(roomList[Math.floor(Math.random() * roomCount)]);
      for (const room of roomsToCheck) {
        if (this.getServerLoad() >= 0.8) break;
        this.validateSeatConsistency(room);
      }
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
          else if (!this._pendingReconnections?.has(seatData.namauser)) {
            Object.assign(seatData, createEmptySeat());
          }
        } else if (occupantId && seatData?.namauser && seatData.namauser !== occupantId) {
          const isOccupantOnline = await this.isUserStillConnected(occupantId);
          if (isOccupantOnline) {
            seatData.namauser = occupantId;
          } else if (!this._pendingReconnections?.has(occupantId)) {
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
                  // FIX: Backpressure check
                  if (client.bufferedAmount > CONSTANTS.BROADCAST_BACKPRESSURE_LIMIT) continue;
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
            if (["LowCard 1", "LowCard 2", "Noxxeliverothcifsa", "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love"].includes(ws.roomname)) {
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
      
      this.clients.add(ws);
      this._lastActivityTime = Date.now();
      
      const messageQueue = new MessageQueue(ws, this);
      this._messageQueues.set(ws, messageQueue);
      
      ws.addEventListener("message", (ev) => {
        messageQueue.add(ev.data).catch(() => {});
      });
      
      ws.addEventListener("error", () => {});
      
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
    
    const warnings = [];
    if (activeConnections > 400) warnings.push("HIGH_CONNECTIONS");
    if (this.cleanupQueue?.size() > 50) warnings.push("QUEUE_BACKPRESSURE");
    if (this._messageQueues?.size > 100) warnings.push("MESSAGE_QUEUE_BACKLOG");
    if (this._pendingReconnections?.size > 150) warnings.push("PENDING_RECONNECTIONS");
    
    return {
      status: warnings.length > 2 ? "critical" : 
              warnings.length > 0 ? "degraded" : "healthy",
      warnings,
      uptime: now - this._startTime,
      activeConnections,
      memoryPressure: this.getServerLoad(),
      queueHealth: {
        size: this.cleanupQueue?.size() || 0,
        active: this.cleanupQueue?.active || 0,
        healthy: (this.cleanupQueue?.size() || 0) < 100
      },
      bufferHealth: {
        totalBuffers: this.updateKursiBuffer.size,
        healthy: true
      },
      failedBatches: this._failedBatches?.length || 0,
      messageQueues: this._messageQueues.size,
      pendingReconnections: this._pendingReconnections?.size || 0,
      graceManager: {
        pending: this.graceManager?.pendingCleanups?.size || 0,
        versions: this.graceManager?.sessionVersions?.size || 0
      }
    };
  }
}

// ========== FIXED: RoomRateLimiter ==========
class RoomRateLimiter {
  constructor() {
    this.roomLimits = new Map();
    this.maxSize = 500;
  }
  
  check(room, userId) {
    if (!room || !userId) return true;
    
    // FIX: Batasi ukuran Map
    if (this.roomLimits.size > this.maxSize) {
      const oldest = Array.from(this.roomLimits.keys())[0];
      this.roomLimits.delete(oldest);
    }
    
    if (!this.roomLimits.has(room)) {
      this.roomLimits.set(room, new Map());
    }
    
    const userLimits = this.roomLimits.get(room);
    
    // FIX: Batasi ukuran per room
    if (userLimits.size > 200) {
      const oldest = Array.from(userLimits.keys())[0];
      userLimits.delete(oldest);
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
        const statusCode = health.status === "healthy" ? 200 : 
                           health.status === "degraded" ? 200 : 503;
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
          gracePeriodPending: obj.graceManager?.pendingCleanups?.size || 0,
          timestamp: Date.now()
        };
        return new Response(JSON.stringify(metrics), {
          headers: { "content-type": "application/json" }
        });
      }
      
      return new Response("WebSocket endpoint", { status: 200, headers: { "content-type": "text/plain" } });
      
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
}
