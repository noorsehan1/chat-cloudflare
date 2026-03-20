import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambayan", "Lounge Talk",
  "Noxxeliverothcifsa", "One Side Love", "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// Constants
const CONSTANTS = {
  LOCK_TIMEOUT: 10000,
  LOCK_ACQUIRE_TIMEOUT: 2000,
  GRACE_PERIOD: 5000,
  MAX_QUEUE_SIZE: 200,
  MAX_LOCK_QUEUE_SIZE: 100,
  BUFFER_SIZE_LIMIT: 50,
  CACHE_VALID_DURATION: 2000,
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 5,
  SAFE_SEND_RETRY: 2,
  SAFE_SEND_RETRY_DELAY: 100,
  BROADCAST_BATCH_SIZE: 20,
  MEMORY_CLEANUP_INTERVAL: 30000,
  CLIENT_CLEANUP_INTERVAL: 15000,
  LOCK_CLEANUP_INTERVAL: 10000,
  RATE_LIMIT_CLEANUP_INTERVAL: 30000,
  SEAT_CHECK_INTERVAL: 30000,
  LOAD_CHECK_INTERVAL: 5000,
  MAIN_TIMER_INTERVAL: 50,
  MAX_MESSAGE_SIZE: 50000,
  MAX_ERROR_COUNT: 3,
  MAX_BUFFERED_AMOUNT: 500000,
  LOAD_THRESHOLD: 0.9,
  LOAD_RECOVERY_THRESHOLD: 0.7,
  MAX_USER_TO_SEAT_SIZE: 10000,
  MAX_ROOM_COUNT_CACHE_SIZE: 100,
  MAX_LOCK_QUEUE_TOTAL: 1000,
  MEMORY_WARNING_LIMITS: {
    clients: 10000,
    userConnections: 5000,
    userToSeat: 5000,
    roomClients: 1000,
    disconnectedTimers: 1000,
    locks: 500
  },
  MAX_ITERATIONS_PER_TICK: 1000,
  BACKPRESSURE_THRESHOLD: 150,
  HEALTH_CHECK_INTERVAL: 60000,
  MAX_EVENT_LISTENERS: 100,
  DEADLOCK_DETECTION_INTERVAL: 5000,
  MEMORY_HARD_LIMIT: 100 * 1024 * 1024,
  CIRCUIT_BREAKER_THRESHOLD: 10,
  CIRCUIT_BREAKER_TIMEOUT: 30000
};

class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
    this.lockTimestamps = new Map();
    this.waitingForLocks = new Map();
    this.circuitBreakers = new Map();
  }

  async acquire(resourceId) {
    if (this.locks.has(resourceId)) {
      const lockTime = this.lockTimestamps.get(resourceId) || 0;
      if (Date.now() - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
      }
    }

    if (this.isCircuitOpen(resourceId)) {
      throw new Error(`Circuit breaker open for ${resourceId}`);
    }

    const stack = new Error().stack;
    this.waitingForLocks.set(resourceId, {
      timestamp: Date.now(),
      stack: stack
    });

    const currentQueue = this.queue.get(resourceId) || [];
    if (currentQueue.length > CONSTANTS.MAX_LOCK_QUEUE_SIZE) {
      this.recordFailure(resourceId);
      throw new Error(`Too many waiting for lock: ${resourceId}`);
    }

    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, true);
      this.lockTimestamps.set(resourceId, Date.now());
      this.waitingForLocks.delete(resourceId);
      return () => this.release(resourceId);
    }

    if (!this.queue.has(resourceId)) {
      this.queue.set(resourceId, []);
    }

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const queue = this.queue.get(resourceId);
        if (queue) {
          const index = queue.findIndex(item => item.resolve === resolve);
          if (index !== -1) {
            queue.splice(index, 1);
            this.recordFailure(resourceId);
            reject(new Error(`Lock queue timeout for ${resourceId}`));
          }
        }
      }, CONSTANTS.LOCK_TIMEOUT);

      this.queue.get(resourceId).push({ 
        resolve: () => {
          clearTimeout(timeoutId);
          this.waitingForLocks.delete(resourceId);
          resolve();
        }, 
        reject,
        timeoutId,
        resourceId,
        timestamp: Date.now()
      });
    }).then(() => () => this.release(resourceId));
  }

  isCircuitOpen(resourceId) {
    const breaker = this.circuitBreakers.get(resourceId);
    if (!breaker) return false;
    
    if (breaker.failures >= CONSTANTS.CIRCUIT_BREAKER_THRESHOLD) {
      if (Date.now() - breaker.lastFailure > CONSTANTS.CIRCUIT_BREAKER_TIMEOUT) {
        this.circuitBreakers.delete(resourceId);
        return false;
      }
      return true;
    }
    return false;
  }

  recordFailure(resourceId) {
    const breaker = this.circuitBreakers.get(resourceId) || {
      failures: 0,
      lastFailure: Date.now()
    };
    breaker.failures++;
    breaker.lastFailure = Date.now();
    this.circuitBreakers.set(resourceId, breaker);
  }

  recordSuccess(resourceId) {
    this.circuitBreakers.delete(resourceId);
  }

  release(resourceId) {
    const queue = this.queue.get(resourceId);
    if (queue && queue.length > 0) {
      const next = queue.shift();
      if (next.timeoutId) clearTimeout(next.timeoutId);
      this.lockTimestamps.set(resourceId, Date.now());
      this.recordSuccess(resourceId);
      next.resolve();
      if (queue.length === 0) {
        this.queue.delete(resourceId);
      }
    } else {
      this.forceRelease(resourceId);
    }
    this.waitingForLocks.delete(resourceId);
  }

  forceRelease(resourceId) {
    this.locks.delete(resourceId);
    this.lockTimestamps.delete(resourceId);
    
    const queue = this.queue.get(resourceId);
    if (queue) {
      for (const item of queue) {
        if (item.timeoutId) clearTimeout(item.timeoutId);
        if (item.reject) {
          item.reject(new Error(`Lock force released: ${resourceId}`));
        }
      }
      this.queue.delete(resourceId);
    }
    this.waitingForLocks.delete(resourceId);
  }

  cleanupStuckLocks() {
    const now = Date.now();
    for (const [resourceId, lockTime] of this.lockTimestamps) {
      if (now - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
      }
    }

    for (const [resourceId, info] of this.waitingForLocks) {
      if (now - info.timestamp > CONSTANTS.LOCK_TIMEOUT) {
        console.warn(`Potential deadlock detected: ${resourceId}`);
        this.forceRelease(resourceId);
      }
    }
  }

  getDeadlockInfo() {
    const deadlocks = [];
    for (const [resourceId, info] of this.waitingForLocks) {
      deadlocks.push({
        resourceId,
        waitingSince: info.timestamp,
        stack: info.stack
      });
    }
    return deadlocks;
  }

  cleanupAll() {
    const now = Date.now();
    const stuckResources = [];
    
    for (const [resourceId, lockTime] of this.lockTimestamps) {
      if (now - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        stuckResources.push(resourceId);
      }
    }
    
    for (const resourceId of stuckResources) {
      this.forceRelease(resourceId);
    }
    
    if (this.queue.size > CONSTANTS.MAX_LOCK_QUEUE_TOTAL) {
      const resources = Array.from(this.queue.keys());
      for (let i = 0; i < resources.length - 500; i++) {
        this.forceRelease(resources[i]);
      }
    }

    for (const [resourceId, info] of this.waitingForLocks) {
      if (now - info.timestamp > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
      }
    }
  }
}

class QueueManager {
  constructor(concurrency = 5) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = CONSTANTS.MAX_QUEUE_SIZE;
    this.processing = false;
    this.totalProcessed = 0;
    this.totalRejected = 0;
    this.avgWaitTime = 0;
    this.lastBackpressureTime = 0;
  }

  async add(job) {
    if (this.queue.length > CONSTANTS.BACKPRESSURE_THRESHOLD) {
      const now = Date.now();
      if (now - this.lastBackpressureTime > 5000) {
        console.warn(`Backpressure: queue size ${this.queue.length}`);
        this.lastBackpressureTime = now;
      }
    }

    if (this.queue.length > this.maxQueueSize) {
      this.totalRejected++;
      throw new Error("Server busy, try again later");
    }

    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject, timestamp: Date.now() });
      if (!this.processing) {
        this.process();
      }
    });
  }

  async process() {
    if (this.processing) return;
    this.processing = true;

    let iterations = 0;

    while (this.queue.length > 0 && this.active < this.concurrency && iterations < CONSTANTS.MAX_ITERATIONS_PER_TICK) {
      iterations++;
      
      while (this.queue.length > 0 && Date.now() - this.queue[0].timestamp > 30000) {
        const expired = this.queue.shift();
        expired.reject(new Error("Request timeout"));
        this.totalRejected++;
      }
      
      if (this.queue.length === 0) break;
      
      this.active++;
      const { job, resolve, reject, timestamp } = this.queue.shift();
      
      const waitTime = Date.now() - timestamp;
      this.avgWaitTime = (this.avgWaitTime * 0.9) + (waitTime * 0.1);
      
      try {
        const result = await Promise.race([
          job(),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error("Job execution timeout")), 10000)
          )
        ]);
        resolve(result);
        this.totalProcessed++;
      } catch (error) {
        reject(error);
        this.totalRejected++;
      } finally {
        this.active--;
      }
    }

    this.processing = false;
    if (this.queue.length > 0) {
      const delay = this.queue.length > 100 ? 5 : 10;
      setTimeout(() => this.process(), delay);
    }
  }

  clear() {
    const oldQueue = this.queue;
    this.queue = [];
    for (const item of oldQueue) {
      item.reject(new Error("Queue cleared"));
    }
    this.totalRejected += oldQueue.length;
  }

  size() {
    return this.queue.length;
  }

  getMetrics() {
    return {
      size: this.queue.length,
      active: this.active,
      totalProcessed: this.totalProcessed,
      totalRejected: this.totalRejected,
      avgWaitTime: this.avgWaitTime,
      concurrency: this.concurrency
    };
  }
}

class RateLimiter {
  constructor(windowMs = 60000, maxRequests = 100) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this.buckets = new Map();
    this.lastCleanup = Date.now();
  }

  check(userId) {
    if (!userId) return true;
    
    const now = Date.now();
    
    if (now - this.lastCleanup > 60000) {
      this.cleanup();
      this.lastCleanup = now;
    }
    
    let userBuckets = this.buckets.get(userId);
    if (!userBuckets) {
      userBuckets = [];
      this.buckets.set(userId, userBuckets);
    }
    
    const cutoff = now - this.windowMs;
    while (userBuckets.length > 0 && userBuckets[0] < cutoff) {
      userBuckets.shift();
    }
    
    if (userBuckets.length >= this.maxRequests) {
      return false;
    }
    
    userBuckets.push(now);
    return true;
  }

  cleanup() {
    const now = Date.now();
    const cutoff = now - this.windowMs;
    
    for (const [userId, buckets] of this.buckets) {
      const valid = buckets.filter(time => time > cutoff);
      if (valid.length === 0) {
        this.buckets.delete(userId);
      } else if (valid.length !== buckets.length) {
        this.buckets.set(userId, valid);
      }
    }
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
    lastUpdated: Date.now()
  };
}

export class ChatServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      
      // Mute status
      this.muteStatus = new Map();
      for (const room of roomList) {
        this.muteStatus.set(room, false);
      }
      
      this.storage = state?.storage;
      
      // Core data structures
      this.lockManager = new PromiseLockManager();
      this.cleanupInProgress = new Set();
      this.clients = new Set();
      this.userToSeat = new Map();
      this.roomClients = new Map();
      this.userCurrentRoom = new Map();
      this.MAX_SEATS = CONSTANTS.MAX_SEATS;
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.userConnections = new Map();
      this.wsEventListeners = new Map();

      // Rate limiters
      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      
      // Safe mode
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
      this.lastLoadCheck = 0;
      this.loadCheckInterval = CONSTANTS.LOAD_CHECK_INTERVAL;

      this.roomCircuitBreakers = new Map();

      // Lowcard game
      try {
        this.lowcard = new LowCardGameManager(this);
      } catch {
        this.lowcard = null;
      }

      // Disconnect handling
      this.gracePeriod = CONSTANTS.GRACE_PERIOD;
      this.disconnectedTimers = new Map();
      this.cleanupQueue = new QueueManager(5);
      
      // Number ticker
      this.currentNumber = 1;
      this.maxNumber = 6;
      this.intervalMillis = 15 * 60 * 1000;
      this._nextConnId = 1;

      // Main timer
      this.mainTimer = null;
      this.tickCounter = 0;
      this.lastNumberTick = Date.now();
      this.numberTickInterval = this.intervalMillis;

      // INITIALIZE ROOMS
      try {
        this.initializeRooms();
        console.log(`✅ All ${roomList.length} rooms initialized successfully`);
      } catch (error) {
        console.error("Failed to initialize rooms, using default room only:", error);
        this.createDefaultRoom();
      }

      this.startMainTimer();
      
      // Cache
      this.roomCountsCache = new Map();
      this.cacheValidDuration = CONSTANTS.CACHE_VALID_DURATION;
      this.lastCacheUpdate = 0;

      // Cleanup counters
      this.lastMemoryCleanup = Date.now();
      this.memoryCleanupInterval = CONSTANTS.MEMORY_CLEANUP_INTERVAL;
      this.lastClientCleanup = Date.now();
      this.clientCleanupInterval = CONSTANTS.CLIENT_CLEANUP_INTERVAL;
      this.lastLockCleanup = Date.now();
      this.lockCleanupInterval = CONSTANTS.LOCK_CLEANUP_INTERVAL;
      this.lastRateLimitCleanup = Date.now();
      this.rateLimitCleanupInterval = CONSTANTS.RATE_LIMIT_CLEANUP_INTERVAL;
      this.lastSeatCheck = Date.now();
      this.seatCheckInterval = CONSTANTS.SEAT_CHECK_INTERVAL;
      this.lastMemoryHealthCheck = Date.now();
      this.memoryHealthCheckInterval = 300000;

      this.metrics = {
        messagesProcessed: 0,
        errors: 0,
        restarts: 0,
        deadlocksDetected: 0,
        lastRestart: null,
        startTime: Date.now()
      };

      this.healthCheckInterval = setInterval(() => {
        this.performHealthCheck();
      }, CONSTANTS.HEALTH_CHECK_INTERVAL);

    } catch (error) {
      console.error("ChatServer constructor error:", error);
      this.metrics.errors++;
      this.initializeFallback();
    }
  }

  initializeFallback() {
    this.clients = new Set();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.roomSeats = new Map();
    this.seatOccupancy = new Map();
    this.roomClients = new Map();
    this.userConnections = new Map();
    this.disconnectedTimers = new Map();
    this.lockManager = new PromiseLockManager();
    this.cleanupInProgress = new Set();
    this.MAX_SEATS = CONSTANTS.MAX_SEATS;
    this.currentNumber = 1;
    this._nextConnId = 1;
    this.mainTimer = null;
    this.lowcard = null;
    this.gracePeriod = CONSTANTS.GRACE_PERIOD;
    this.cleanupQueue = new QueueManager(5);
    this.wsEventListeners = new Map();
    
    this.muteStatus = new Map();
    for (const room of roomList) {
      this.muteStatus.set(room, false);
    }
    
    this.storage = this.state?.storage;
    
    this.rateLimiter = new RateLimiter(60000, 100);
    this.connectionRateLimiter = new RateLimiter(10000, 5);
    this.safeMode = false;
    this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
    
    this.createDefaultRoom();

    this.lastMemoryCleanup = Date.now();
    this.memoryCleanupInterval = CONSTANTS.MEMORY_CLEANUP_INTERVAL;
    this.lastClientCleanup = Date.now();
    this.clientCleanupInterval = CONSTANTS.CLIENT_CLEANUP_INTERVAL;
    this.lastLockCleanup = Date.now();
    this.lockCleanupInterval = CONSTANTS.LOCK_CLEANUP_INTERVAL;
    this.lastRateLimitCleanup = Date.now();
    this.rateLimitCleanupInterval = CONSTANTS.RATE_LIMIT_CLEANUP_INTERVAL;
    this.lastSeatCheck = Date.now();
    this.seatCheckInterval = CONSTANTS.SEAT_CHECK_INTERVAL;
    this.tickCounter = 0;
    this.lastNumberTick = Date.now();
    this.numberTickInterval = 15 * 60 * 1000;
    
    this.metrics = {
      messagesProcessed: 0,
      errors: 1,
      restarts: 0,
      deadlocksDetected: 0,
      lastRestart: null,
      startTime: Date.now()
    };
    
    this.healthCheckInterval = setInterval(() => {
      this.performHealthCheck();
    }, CONSTANTS.HEALTH_CHECK_INTERVAL);
    
    this.startMainTimer();
  }

  performHealthCheck() {
    try {
      const memory = process.memoryUsage?.().heapUsed || 0;
      
      if (memory > CONSTANTS.MEMORY_HARD_LIMIT) {
        console.error(`Critical: Memory ${memory} > limit, forcing cleanup`);
        this.aggressiveCleanup();
      }
      
      const deadlocks = this.lockManager?.getDeadlockInfo() || [];
      if (deadlocks.length > 0) {
        this.metrics.deadlocksDetected += deadlocks.length;
        console.warn(`Deadlocks detected:`, deadlocks);
      }
      
      const queueSize = this.cleanupQueue?.size() || 0;
      if (queueSize > CONSTANTS.BACKPRESSURE_THRESHOLD * 2) {
        console.warn(`Queue size critical: ${queueSize}`);
      }
      
    } catch (error) {
      console.error('Health check error:', error);
    }
  }

  startMainTimer() {
    if (this.mainTimer) {
      clearInterval(this.mainTimer);
    }
    this.mainTimer = setInterval(() => {
      this.runMainTasks().catch(() => {});
    }, CONSTANTS.MAIN_TIMER_INTERVAL);
  }

  async runMainTasks() {
    const now = Date.now();
    
    if (this.tickCounter % 10 === 0) {
      if (now - this.lastLoadCheck >= this.loadCheckInterval) {
        this.checkAndEnableSafeMode();
        this.lastLoadCheck = now;
      }
      this.validateGracePeriodTimers();
    }
    
    if (now - this.lastLockCleanup >= this.lockCleanupInterval) {
      this.lockManager?.cleanupStuckLocks();
      this.lastLockCleanup = now;
    }
    
    if (now - this.lastClientCleanup >= this.clientCleanupInterval) {
      await this.cleanupDuplicateConnections();
      this.lastClientCleanup = now;
    }
    
    if (now - this.lastMemoryCleanup >= this.memoryCleanupInterval) {
      await this.performMemoryCleanup();
      this.lastMemoryCleanup = now;
    }
    
    if (now - this.lastRateLimitCleanup >= this.rateLimitCleanupInterval) {
      this.rateLimiter.cleanup();
      this.connectionRateLimiter.cleanup();
      this.lastRateLimitCleanup = now;
    }
    
    if (now - this.lastSeatCheck >= this.seatCheckInterval && this.getServerLoad() < 0.8) {
      this.sampledSeatConsistencyCheck();
      this.lastSeatCheck = now;
    }
    
    if (now - this.lastMemoryHealthCheck >= this.memoryHealthCheckInterval) {
      this.checkMemoryHealth();
      this.lastMemoryHealthCheck = now;
    }
    
    if (now - this.lastNumberTick >= this.numberTickInterval) {
      this.tick();
      this.lastNumberTick = now;
    }
    
    this.tickCounter = (this.tickCounter + 1) % 1000;
  }

  async performMemoryCleanup() {
    try {
      const deadClients = [];
      for (const client of this.clients) {
        if (!client || client.readyState === 3) deadClients.push(client);
      }
      for (const client of deadClients) {
        this.cleanupWebSocketListeners(client);
        this.clients.delete(client);
      }

      for (const [room, clientArray] of this.roomClients) {
        if (clientArray) {
          const filtered = clientArray.filter(c => c && c.readyState === 1);
          if (filtered.length !== clientArray.length) {
            this.roomClients.set(room, filtered);
          }
        }
      }

      for (const [userId, connections] of this.userConnections) {
        const activeConnections = new Set();
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            if (conn._connectionTime && (Date.now() - conn._connectionTime) < 86400000) {
              activeConnections.add(conn);
            }
          }
        }
        if (activeConnections.size === 0) {
          this.userConnections.delete(userId);
        } else if (activeConnections.size !== connections.size) {
          this.userConnections.set(userId, activeConnections);
        }
      }

      if (this.userToSeat.size > CONSTANTS.MAX_USER_TO_SEAT_SIZE) {
        const entries = Array.from(this.userToSeat.entries());
        const toDelete = Math.floor(entries.length * 0.2);
        let deleted = 0;
        
        for (let i = 0; i < entries.length && deleted < toDelete; i++) {
          const [userId, seatInfo] = entries[i];
          const isOnline = await this.isUserStillConnected(userId);
          
          if (!isOnline) {
            this.userToSeat.delete(userId);
            deleted++;
          }
        }
      }

      if (this.roomCountsCache && this.roomCountsCache.size > CONSTANTS.MAX_ROOM_COUNT_CACHE_SIZE) {
        this.roomCountsCache = null;
      }

      if (this.lockManager) {
        this.lockManager.cleanupAll();
      }

      const now = Date.now();
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer._scheduledTime && (now - timer._scheduledTime) > this.gracePeriod + 5000) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
        }
      }

      for (const [room, breaker] of this.roomCircuitBreakers) {
        if (now - breaker.lastFailure > CONSTANTS.CIRCUIT_BREAKER_TIMEOUT) {
          this.roomCircuitBreakers.delete(room);
        }
      }

    } catch (error) {
      console.error("Memory cleanup error:", error);
      this.metrics.errors++;
    }
  }

  cleanupWebSocketListeners(ws) {
    if (!ws) return;
    
    const listeners = this.wsEventListeners.get(ws);
    if (listeners) {
      if (listeners.message) ws.removeEventListener('message', listeners.message);
      if (listeners.close) ws.removeEventListener('close', listeners.close);
      if (listeners.error) ws.removeEventListener('error', listeners.error);
      this.wsEventListeners.delete(ws);
    }
  }

  checkMemoryHealth() {
    if (typeof process !== 'undefined' && process.memoryUsage) {
      const used = process.memoryUsage().heapUsed / 1024 / 1024;
      const mapSizes = {
        clients: this.clients.size,
        userConnections: this.userConnections.size,
        roomClients: this.roomClients.size,
        userToSeat: this.userToSeat.size,
        disconnectedTimers: this.disconnectedTimers.size,
        locks: this.lockManager?.locks.size || 0
      };
      
      console.log(`Memory: ${used.toFixed(2)} MB`, mapSizes);
      
      if (used > 500) {
        console.error('High memory usage, forcing aggressive cleanup');
        this.aggressiveCleanup();
      }
    }
  }

  aggressiveCleanup() {
    this.performMemoryCleanup();
    
    for (const [ws, listeners] of this.wsEventListeners) {
      this.cleanupWebSocketListeners(ws);
    }
    
    if (global.gc) {
      try {
        global.gc();
      } catch {}
    }
  }

  setRoomMute(roomName, isMuted) {
    try {
      if (!roomName || !roomList.includes(roomName)) {
        return false;
      }
      
      const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
      this.muteStatus.set(roomName, muteValue);
      
      this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
      
      return true;
    } catch (error) {
      this.metrics.errors++;
      return false;
    }
  }

  getRoomMute(roomName) {
    try {
      if (!roomName || !roomList.includes(roomName)) {
        return false;
      }
      return this.muteStatus.get(roomName) === true;
    } catch {
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
      if (oldest && oldest.readyState === 1) {
        oldest._isDuplicate = true;
        try { 
          this.safeSend(oldest, ["duplicateConnection", "Too many connections"]);
          oldest.close(1000, "Too many connections"); 
        } catch {}
        userConnections.delete(oldest);
        this.cleanupWebSocketListeners(oldest);
      }
    }
    
    userConnections.add(ws);
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

  async withLock(resourceId, operation, timeout = CONSTANTS.LOCK_ACQUIRE_TIMEOUT) {
    let release;
    try {
      release = await this.lockManager.acquire(resourceId);
      
      const result = await Promise.race([
        operation(),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error(`Lock operation timeout: ${resourceId}`)), timeout)
        )
      ]);
      
      return result;
    } catch (error) {
      this.metrics.errors++;
      throw error;
    } finally {
      if (release) {
        try { 
          release(); 
        } catch {}
      }
    }
  }

  checkAndEnableSafeMode() {
    const load = this.getServerLoad();
    
    if (load > this.loadThreshold && !this.safeMode) {
      this.enableSafeMode();
    } else if (load < CONSTANTS.LOAD_RECOVERY_THRESHOLD && this.safeMode) {
      this.disableSafeMode();
    }
  }

  enableSafeMode() {
    if (this.safeMode) return;
    
    this.safeMode = true;
    this.cleanupQueue.concurrency = 2;
    
    console.warn('Safe mode enabled');
    
    setTimeout(() => {
      if (this.getServerLoad() < CONSTANTS.LOAD_RECOVERY_THRESHOLD) {
        this.disableSafeMode();
      }
    }, 60000);
  }

  disableSafeMode() {
    this.safeMode = false;
    this.cleanupQueue.concurrency = 5;
  }

  getServerLoad() {
    const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
    const queueSize = this.cleanupQueue?.size() || 0;
    const queueLoad = Math.min(queueSize / 50, 0.3);
    const circuitBreakerLoad = this.roomCircuitBreakers.size / 10;
    return Math.min(activeConnections / 100 + queueLoad + circuitBreakerLoad, 0.95);
  }

  broadcastPointDirect(room, seat, x, y, fast) {
    try {
      if (!room || !roomList.includes(room)) return;
      if (seat < 1 || seat > this.MAX_SEATS) return;
      
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return;
      
      const message = JSON.stringify(["pointUpdated", room, seat, x, y, fast]);
      const sentUsers = new Set();
      
      for (const client of clientArray) {
        if (client?.readyState === 1 && 
            client.roomname === room && 
            client.idtarget &&
            !sentUsers.has(client.idtarget)) {
          try { 
            client.send(message); 
            sentUsers.add(client.idtarget);
          } catch {}
        }
      }
    } catch {}
  }

  async savePointWithRetry(room, seat, x, y, fast) {
    try {
      if (seat < 1 || seat > this.MAX_SEATS) return false;
      
      const xNum = typeof x === 'number' ? x : parseFloat(x);
      const yNum = typeof y === 'number' ? y : parseFloat(y);
      
      if (isNaN(xNum) || isNaN(yNum)) return false;
      
      const updatedSeat = await this.updateSeatAtomic(room, seat, (currentSeat) => {
        currentSeat.lastPoint = { 
          x: xNum, 
          y: yNum, 
          fast: fast || false, 
          timestamp: Date.now() 
        };
        return currentSeat;
      });
      
      this.broadcastPointDirect(room, seat, xNum, yNum, fast);
      return !!updatedSeat;
      
    } catch {
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
        seatMap.set(i, createEmptySeat());
        occupancyMap.set(i, null);
      }
      
      this.roomSeats.set(room, seatMap);
      this.seatOccupancy.set(room, occupancyMap);
      this.roomClients.set(room, []);
      
      console.log(`✅ Default room ${room} initialized with ${this.MAX_SEATS} seats`);
      
    } catch (error) {
      console.error("Failed to create default room:", error);
    }
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
        
        console.log(`✅ Room ${room} initialized with ${this.MAX_SEATS} seats`);
        
      } catch (error) {
        console.error(`Failed to initialize room ${room}:`, error);
      }
    }
  }

  async ensureSeatsData(room) {
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      
      if (!seatMap || !occupancyMap) {
        console.warn(`Room ${room} has no seat data, creating...`);
        this.roomSeats.set(room, new Map());
        this.seatOccupancy.set(room, new Map());
        return;
      }
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatMap.has(seat)) {
          seatMap.set(seat, createEmptySeat());
        }
        if (!occupancyMap.has(seat)) {
          occupancyMap.set(seat, null);
        }
      }
      
    } catch (error) {
      console.error(`Error ensuring seats data for room ${room}:`, error);
    }
  }

  async updateSeatAtomic(room, seatNumber, updateFn) {
    try {
      if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return null;
      
      return await this.withLock(`seat-update-${room}-${seatNumber}`, () => {
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        
        if (!seatMap || !occupancyMap) {
          console.error(`Room ${room} not found for seat update`);
          return null;
        }
        
        let currentSeat = seatMap.get(seatNumber);
        if (!currentSeat) {
          currentSeat = createEmptySeat();
          seatMap.set(seatNumber, currentSeat);
        }
        
        const updatedSeat = updateFn(currentSeat);
        updatedSeat.lastUpdated = Date.now();
        
        if (updatedSeat.namauser && updatedSeat.namauser !== "") {
          occupancyMap.set(seatNumber, updatedSeat.namauser);
        } else {
          occupancyMap.set(seatNumber, null);
        }
        
        seatMap.set(seatNumber, updatedSeat);
        
        return updatedSeat;
      });
      
    } catch (error) {
      console.error(`Error updating seat ${room}-${seatNumber}:`, error);
      return null;
    }
  }

  async findEmptySeat(room, ws) {
    if (!room || !ws || !ws.idtarget) return null;
    
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      const seatMap = this.roomSeats.get(room);
      
      if (!occupancyMap || !seatMap) {
        console.error(`Room ${room} not found for findEmptySeat`);
        return null;
      }
      
      let occupiedCount = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        if (occupancyMap.get(i)) occupiedCount++;
      }
      console.log(`🔍 Room ${room}: ${occupiedCount}/${this.MAX_SEATS} seats occupied`);
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        
        if (occupantId === ws.idtarget && seatData?.namauser === ws.idtarget) {
          console.log(`✅ User ${ws.idtarget} already in seat ${i}`);
          return i;
        }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const release = await this.lockManager.acquire(`seat-check-${room}-${i}`);
        try {
          const occupantId = occupancyMap.get(i);
          const seatData = seatMap.get(i);
          
          const isOccupancyEmpty = occupantId === null;
          const isSeatDataEmpty = !seatData || !seatData.namauser;
          
          if (isOccupancyEmpty && isSeatDataEmpty) {
            console.log(`✅ Found empty seat ${i} for user ${ws.idtarget}`);
            return i;
          }
        } finally {
          release();
        }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        
        if (occupantId === null && seatData?.namauser) {
          console.log(`🔄 Cleaning inconsistent seat ${i}`);
          Object.assign(seatData, createEmptySeat());
          return i;
        }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        
        if (occupantId && seatData?.namauser === occupantId) {
          const isOnline = await this.isUserStillConnected(occupantId);
          
          if (!isOnline) {
            console.log(`🔄 Cleaning offline user from seat ${i}`);
            await this.cleanupUserFromSeat(room, i, occupantId, true);
            return i;
          }
        }
      }
      
      console.log(`❌ No empty seat found in room ${room}`);
      return null;
      
    } catch (error) {
      console.error(`Error finding empty seat in ${room}:`, error);
      return null;
    }
  }

  async assignSeatAtomic(room, seat, userId) {
    const release = await this.lockManager.acquire(`atomic-assign-${room}-${seat}`);
    
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      const seatMap = this.roomSeats.get(room);
      
      if (!occupancyMap || !seatMap) {
        console.error(`Room ${room} not found for assignSeatAtomic`);
        return false;
      }
      
      const occupantId = occupancyMap.get(seat);
      const seatData = seatMap.get(seat);
      
      const isStillEmpty = occupantId === null && (!seatData || !seatData.namauser);
      
      if (!isStillEmpty) {
        console.warn(`Seat ${room}-${seat} is not empty (occupant: ${occupantId}, seatUser: ${seatData?.namauser})`);
        return false;
      }
      
      occupancyMap.set(seat, userId);
      
      if (!seatData) {
        seatMap.set(seat, { 
          noimageUrl: "", 
          namauser: userId, 
          color: "", 
          itembawah: 0, 
          itematas: 0, 
          vip: 0, 
          viptanda: 0, 
          lastPoint: null, 
          lastUpdated: Date.now() 
        });
      } else {
        seatData.namauser = userId;
        seatData.lastUpdated = Date.now();
      }
      
      console.log(`✅ User ${userId} assigned to room ${room} seat ${seat}`);
      return true;
      
    } catch (error) {
      console.error(`Error assigning seat ${room}-${seat}:`, error);
      return false;
    } finally {
      release();
    }
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
        
        const isOnline = await this.isUserStillConnected(userId);
        if (isOnline) {
          console.log(`User ${userId} still online, skipping cleanup`);
          return;
        }
        
        if (immediate) {
          console.log(`Cleaning up user ${userId} from room ${room} seat ${seatNumber}`);
          Object.assign(seatInfo, createEmptySeat());
          occupancyMap.set(seatNumber, null);
          
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.updateRoomCount(room);
          this.userToSeat.delete(userId);
        }
      });
      
    } catch (error) {
      console.error(`Error cleaning up user from seat:`, error);
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
              if (!doubleCheck) {
                await this.forceUserCleanup(userId);
              }
            });
          }
        } catch (error) {
          console.error(`Error in cleanup timer for ${userId}:`, error);
        }
      }, this.gracePeriod);
      
      timerId._scheduledTime = Date.now();
      timerId._userId = userId;
      this.disconnectedTimers.set(userId, timerId);
      
    } catch (error) {
      console.error(`Error scheduling cleanup for ${userId}:`, error);
    }
  }

  cancelCleanup(userId) {
    if (!userId) return;
    
    try {
      const timer = this.disconnectedTimers.get(userId);
      if (timer) {
        clearTimeout(timer);
        this.disconnectedTimers.delete(userId);
        console.log(`Cleanup cancelled for user ${userId}`);
      }
      
      this.cleanupInProgress?.delete(userId);
      
    } catch (error) {
      console.error(`Error cancelling cleanup for ${userId}:`, error);
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
      
      const connectionAge = Date.now() - (conn._connectionTime || 0);
      if (connectionAge > 86400000) continue;
      
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
            if (seatInfo?.namauser === userId) {
              seatsToCleanup.push({ room, seatNumber: i });
            }
          }
        }
        
        const cleanupPromises = seatsToCleanup.map(({ room, seatNumber }) =>
          this.cleanupUserFromSeat(room, seatNumber, userId, true)
        );
        
        await Promise.allSettled(cleanupPromises);
        
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
          
          if (!hasValid) {
            this.userConnections.delete(userId);
          }
        }
        
        for (const [room, clientArray] of this.roomClients) {
          if (clientArray?.length > 0) {
            const filtered = clientArray.filter(c => c?.idtarget !== userId);
            if (filtered.length !== clientArray.length) {
              this.roomClients.set(room, filtered);
            }
          }
        }
        
        console.log(`✅ Force cleanup completed for user ${userId}`);
      });
      
    } catch (error) {
      console.error(`Error force cleaning user ${userId}:`, error);
    } finally {
      this.cleanupInProgress.delete(userId);
    }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    
    try {
      await this.withLock(`room-cleanup-${room}-${ws.idtarget}`, async () => {
        const seatInfo = this.userToSeat.get(ws.idtarget);
        
        if (seatInfo?.room === room) {
          await this.cleanupUserFromSeat(room, seatInfo.seat, ws.idtarget, true);
        }
        
        this._removeFromRoomClients(ws, room);
        this._removeUserConnection(ws.idtarget, ws);
        this.userCurrentRoom.delete(ws.idtarget);
        
        ws.roomname = undefined;
        ws.numkursi = new Set();
        
        this.userToSeat.delete(ws.idtarget);
        this.updateRoomCount(room);
        
        console.log(`User ${ws.idtarget} cleaned up from room ${room}`);
      });
      
    } catch (error) {
      console.error(`Error cleaning up from room:`, error);
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
              Object.assign(info, createEmptySeat());
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
          if (client?.idtarget === idtarget) {
            clientsToRemove.push(client);
          }
        }
        
        for (const client of clientsToRemove) {
          if (client.readyState === 1) {
            try { client.close(1000, "Session removed"); } catch {}
          }
          this.cleanupWebSocketListeners(client);
          this.clients.delete(client);
          
          for (const [room, clientArray] of this.roomClients) {
            if (clientArray) {
              const index = clientArray.indexOf(client);
              if (index > -1) clientArray.splice(index, 1);
            }
          }
        }
        
        console.log(`✅ User ${idtarget} completely removed`);
      });
      
    } catch (error) {
      console.error(`Error removing user ${idtarget}:`, error);
    }
  }

  validateGracePeriodTimers() {
    try {
      const now = Date.now();
      const maxGracePeriod = this.gracePeriod + 1000;
      
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer?._scheduledTime) {
          const elapsed = now - timer._scheduledTime;
          
          if (elapsed > maxGracePeriod) {
            clearTimeout(timer);
            this.disconnectedTimers.delete(userId);
            this.executeGracePeriodCleanup(userId);
          }
        }
      }
      
    } catch (error) {
      console.error("Error validating grace period timers:", error);
    }
  }

  async executeGracePeriodCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    
    this.checkAndEnableSafeMode();
    
    if (this.safeMode) {
      setTimeout(() => this.executeGracePeriodCleanup(userId), 2000);
      return;
    }
    
    this.cleanupInProgress.add(userId);
    
    try {
      await this.withLock(`user-cleanup-${userId}`, async () => {
        const isConnected = await this.isUserStillConnected(userId);
        if (!isConnected) {
          await this.forceUserCleanup(userId);
        }
      });
      
    } catch (error) {
      console.error(`Error in grace period cleanup for ${userId}:`, error);
    } finally {
      this.cleanupInProgress.delete(userId);
    }
  }

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
    
    console.log(`🔍 Join attempt: User ${ws.idtarget} to room ${room}`);
    console.log(`   Room exists in roomSeats: ${this.roomSeats.has(room)}`);
    console.log(`   Room exists in seatOccupancy: ${this.seatOccupancy.has(room)}`);
    
    const breaker = this.roomCircuitBreakers.get(room);
    if (breaker && breaker.failures > CONSTANTS.CIRCUIT_BREAKER_THRESHOLD) {
      if (Date.now() - breaker.lastFailure < CONSTANTS.CIRCUIT_BREAKER_TIMEOUT) {
        this.safeSend(ws, ["error", "Room is temporarily unavailable"]);
        return false;
      } else {
        this.roomCircuitBreakers.delete(room);
      }
    }
    
    try {
      const roomRelease = await this.lockManager.acquire(`room-join-${room}`);
      
      try {
        this.cancelCleanup(ws.idtarget);
        await this.ensureSeatsData(room);
        
        const seatMap = this.roomSeats.get(room);
        if (seatMap) {
          let occupied = 0;
          for (let i = 1; i <= this.MAX_SEATS; i++) {
            const seat = seatMap.get(i);
            if (seat?.namauser) occupied++;
          }
          console.log(`   Room ${room} current occupancy: ${occupied}/${this.MAX_SEATS}`);
        }
        
        const previousRoom = this.userCurrentRoom.get(ws.idtarget);
        
        if (previousRoom) {
          if (previousRoom === room) {
            console.log(`User ${ws.idtarget} already in room ${room}, sending state`);
            this.sendAllStateTo(ws, room);
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
            return true;
          } else {
            await this.cleanupFromRoom(ws, previousRoom);
          }
        }
        
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (seatInfo?.room === room) {
          const occupancyMap = this.seatOccupancy.get(room);
          if (occupancyMap?.get(seatInfo.seat) === ws.idtarget) {
            console.log(`User ${ws.idtarget} reconnecting to seat ${seatInfo.seat}`);
            ws.roomname = room;
            ws.numkursi = new Set([seatInfo.seat]);
            
            const clientArray = this.roomClients.get(room);
            if (clientArray && !clientArray.includes(ws)) {
              clientArray.push(ws);
            }
            
            this._addUserConnection(ws.idtarget, ws);
            this.sendAllStateTo(ws, room);
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
            
            return true;
          }
        }
        
        let assignedSeat = null;
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
          const seatRelease = await this.lockManager.acquire(`seat-assign-${room}-${seat}`);
          
          try {
            const occupancyMap = this.seatOccupancy.get(room);
            if (!occupancyMap) continue;
            
            if (occupancyMap.get(seat) === null) {
              occupancyMap.set(seat, ws.idtarget);
              assignedSeat = seat;
              console.log(`Temporarily assigned seat ${seat} to user ${ws.idtarget}`);
              break;
            }
          } finally {
            seatRelease();
          }
        }
        
        if (!assignedSeat) {
          console.log(`Room ${room} is full`);
          const breaker = this.roomCircuitBreakers.get(room) || { failures: 0, lastFailure: Date.now() };
          breaker.failures++;
          breaker.lastFailure = Date.now();
          this.roomCircuitBreakers.set(room, breaker);
          this.safeSend(ws, ["roomFull", room]);
          return false;
        }
        
        const success = await this.assignSeatAtomic(room, assignedSeat, ws.idtarget);
        if (!success) {
          console.error(`Failed to assign seat ${assignedSeat} to user ${ws.idtarget}`);
          this.safeSend(ws, ["error", "Failed to assign seat"]);
          return false;
        }
        
        this.roomCircuitBreakers.delete(room);
        
        this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
        this.userCurrentRoom.set(ws.idtarget, room);
        ws.roomname = room;
        ws.numkursi = new Set([assignedSeat]);
        
        const clientArray = this.roomClients.get(room);
        if (clientArray && !clientArray.includes(ws)) {
          clientArray.push(ws);
        }
        
        this._addUserConnection(ws.idtarget, ws);
        
        this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
        this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
        
        setTimeout(() => this.sendAllStateTo(ws, room), 100);
        this.updateRoomCount(room);
        this.metrics.messagesProcessed++;
        
        console.log(`✅ User ${ws.idtarget} successfully joined room ${room} seat ${assignedSeat}`);
        return true;
        
      } finally {
        roomRelease();
      }
      
    } catch (error) {
      console.error(`Error in handleJoinRoom for ${ws.idtarget}:`, error);
      this.metrics.errors++;
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    
    try {
      await this.withLock(`reconnect-${id}`, async () => {
        this.cancelCleanup(id);
        
        if (baru === true) {
          await this.cleanupQueue.add(async () => {
            await this.forceUserCleanup(id);
          });
          
          ws.idtarget = id;
          ws.roomname = undefined;
          ws.numkursi = new Set();
          ws._connectionTime = Date.now();
          
          console.log(`New user ${id} connected`);
          this.safeSend(ws, ["joinroomawal"]);
          return;
        }
        
        ws.idtarget = id;
        ws._connectionTime = Date.now();
        
        const seatInfo = this.userToSeat.get(id);
        
        if (seatInfo) {
          const { room, seat } = seatInfo;
          
          if (seat < 1 || seat > this.MAX_SEATS) {
            this.userToSeat.delete(id);
            this.userCurrentRoom.delete(id);
            this.safeSend(ws, ["needJoinRoom"]);
            return;
          }
          
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          
          if (seatMap && occupancyMap) {
            const seatData = seatMap.get(seat);
            const occupantId = occupancyMap.get(seat);
            
            if (seatData?.namauser === id && occupantId === id) {
              console.log(`User ${id} reconnecting to room ${room} seat ${seat}`);
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) {
                clientArray.push(ws);
              }
              
              this._addUserConnection(id, ws);
              this.sendAllStateTo(ws, room);
              
              if (seatData.lastPoint) {
                this.safeSend(ws, [
                  "pointUpdated", 
                  room, 
                  seat, 
                  seatData.lastPoint.x, 
                  seatData.lastPoint.y, 
                  seatData.lastPoint.fast
                ]);
              }
              
              this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
              this.updateRoomCount(room);
              return;
            }
          }
          
          this.userToSeat.delete(id);
          this.userCurrentRoom.delete(id);
          
          if (seatInfo.room) {
            await this.cleanupQueue.add(async () => {
              await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, id, true);
            });
          }
        }
        
        console.log(`User ${id} needs to join a room`);
        this.safeSend(ws, ["needJoinRoom"]);
      });
      
    } catch (error) {
      console.error(`Error in handleSetIdTarget2 for ${id}:`, error);
      this.metrics.errors++;
      this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }

  async safeSend(ws, arr, retry = CONSTANTS.SAFE_SEND_RETRY) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing) {
        return false;
      }
      
      if (ws.bufferedAmount > CONSTANTS.MAX_BUFFERED_AMOUNT) {
        if (retry > 0) {
          await new Promise(r => setTimeout(r, CONSTANTS.SAFE_SEND_RETRY_DELAY));
          return this.safeSend(ws, arr, retry - 1);
        }
        return false;
      }
      
      ws.send(JSON.stringify(arr));
      return true;
      
    } catch {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      
      const clientArray = this.roomClients.get(room);
      if (!clientArray?.length) return 0;
      
      let sentCount = 0;
      const sentToUsers = new Set();
      const message = JSON.stringify(msg);
      
      for (const client of clientArray) {
        if (client?.readyState === 1 && 
            client.roomname === room && 
            !client._isDuplicate && 
            !client._isClosing) {
          
          if (client.idtarget && sentToUsers.has(client.idtarget)) continue;
          
          try {
            client.send(message);
            sentCount++;
            if (client.idtarget) sentToUsers.add(client.idtarget);
          } catch {}
        }
      }
      
      return sentCount;
      
    } catch {
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
        const info = seatMap.get(i);
        if (info?.namauser) count++;
      }
      
      if (this.roomCountsCache) {
        this.roomCountsCache[room] = count;
      }
      
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
      
    } catch {}
  }

  sendAllStateTo(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) {
        console.error(`Room ${room} not found in sendAllStateTo`);
        return;
      }
      
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
      console.error(`Error sending state to user:`, error);
    }
  }

  getJumlahRoom() {
    try {
      const now = Date.now();
      
      if (this.roomCountsCache && (now - this.lastCacheUpdate) < this.cacheValidDuration) {
        return this.roomCountsCache;
      }
      
      const counts = {};
      for (const room of roomList) {
        counts[room] = 0;
      }
      
      for (const room of roomList) {
        const occupancyMap = this.seatOccupancy.get(room);
        if (!occupancyMap) continue;
        
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          if (occupancyMap.get(i)) {
            counts[room]++;
          }
        }
      }
      
      this.roomCountsCache = counts;
      this.lastCacheUpdate = now;
      
      return counts;
      
    } catch {
      const fallback = {};
      for (const room of roomList) {
        fallback[room] = 0;
      }
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
        const info = seatMap.get(i);
        if (info?.namauser) count++;
      }
      
      this.invalidateRoomCache(room);
      this.broadcastRoomUserCount(room);
      
      return count;
      
    } catch {
      return 0;
    }
  }

  sampledSeatConsistencyCheck() {
    try {
      const roomsToCheck = [];
      const roomCount = roomList.length;
      
      for (let i = 0; i < 3; i++) {
        const randomIndex = Math.floor(Math.random() * roomCount);
        roomsToCheck.push(roomList[randomIndex]);
      }
      
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
          
          if (isOnline) {
            occupancyMap.set(seat, seatData.namauser);
          } else {
            Object.assign(seatData, createEmptySeat());
          }
          
        } else if (occupantId && seatData?.namauser && seatData.namauser !== occupantId) {
          const isOccupantOnline = await this.isUserStillConnected(occupantId);
          
          if (isOccupantOnline) {
            seatData.namauser = occupantId;
          } else {
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
          const count = userConnectionCount.get(client.idtarget) || 0;
          userConnectionCount.set(client.idtarget, count + 1);
        }
      }
      
      const duplicateUsers = [];
      for (const [userId, count] of userConnectionCount) {
        if (count > 1) duplicateUsers.push(userId);
      }
      
      const batchSize = 10;
      for (let i = 0; i < duplicateUsers.length; i += batchSize) {
        const batch = duplicateUsers.slice(i, i + batchSize);
        await Promise.allSettled(
          batch.map(userId => this.handleDuplicateConnections(userId))
        );
      }
      
    } catch {}
  }

  async handleDuplicateConnections(userId) {
    if (!userId) return;
    
    try {
      await this.withLock(`duplicate-connections-${userId}`, async () => {
        const allConnections = [];
        
        for (const client of this.clients) {
          if (client?.idtarget === userId && 
              client.readyState === 1 && 
              !client._isClosing) {
            allConnections.push({
              client,
              connectionTime: client._connectionTime || 0
            });
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
          } catch {}
          
          this.cleanupWebSocketListeners(client);
          this.clients.delete(client);
          
          if (client.roomname) {
            this._removeFromRoomClients(client, client.roomname);
          }
          
          this._removeUserConnection(userId, client);
        }
        
        const remainingConnections = new Set();
        for (const client of this.clients) {
          if (client?.idtarget === userId && client.readyState === 1) {
            remainingConnections.add(client);
          }
        }
        
        this.userConnections.set(userId, remainingConnections);
      });
      
    } catch {}
  }

  getAllOnlineUsers() {
    try {
      const users = [];
      const seenUsers = new Set();
      
      for (const client of this.clients) {
        if (client?.idtarget && 
            client.readyState === 1 && 
            !client._isDuplicate && 
            !client._isClosing) {
          
          if (!seenUsers.has(client.idtarget)) {
            users.push(client.idtarget);
            seenUsers.add(client.idtarget);
          }
        }
      }
      
      return users;
      
    } catch {
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
          if (client?.idtarget && 
              client.readyState === 1 && 
              !client._isDuplicate && 
              !client._isClosing) {
            
            if (!seenUsers.has(client.idtarget)) {
              users.push(client.idtarget);
              seenUsers.add(client.idtarget);
            }
          }
        }
      }
      
      return users;
      
    } catch {
      return [];
    }
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? 
                          this.currentNumber + 1 : 1;
      
      const clientsToNotify = [];
      const notifiedUsers = new Set();
      
      for (const client of this.clients) {
        if (client?.readyState === 1 && 
            client.roomname && 
            !client._isDuplicate && 
            !client._isClosing) {
          
          if (!notifiedUsers.has(client.idtarget)) {
            clientsToNotify.push(client);
            notifiedUsers.add(client.idtarget);
          }
        }
      }
      
      for (const client of clientsToNotify) {
        this.safeSend(client, ["currentNumber", this.currentNumber]);
      }
      
    } catch {}
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget || !ws) return;
    
    try {
      this.withLock(`destroy-${idtarget}`, async () => {
        if (ws.isManualDestroy) {
          await this.cleanupQueue.add(async () => {
            await this.fullRemoveById(idtarget);
          });
        } else {
          const seatInfo = this.userToSeat.get(idtarget);
          if (seatInfo) {
            await this.cleanupQueue.add(async () => {
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
        this.cleanupWebSocketListeners(ws);
        
        if (ws.readyState === 1) {
          try { ws.close(1000, "Manual destroy"); } catch {}
        }
      });
      
    } catch {
      try {
        this.clients.delete(ws);
        this.cleanupWebSocketListeners(ws);
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
      this.clients.delete(ws);
      this.cleanupWebSocketListeners(ws);
      
      if (userId) {
        this._removeUserConnection(userId, ws);
        this.cancelCleanup(userId);
        
        if (!ws.isManualDestroy && !ws._isDuplicate) {
          this.scheduleCleanup(userId);
        }
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
      }
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Normal closure"); } catch {}
      }
      
      setTimeout(() => {
        ws.roomname = null;
        ws.idtarget = null;
        ws.numkursi = null;
      }, 1000);
      
    } catch {
      this.clients.delete(ws);
      this.cleanupWebSocketListeners(ws);
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
        if (ws.errorCount > CONSTANTS.MAX_ERROR_COUNT) {
          try { ws.close(1008, "Protocol error"); } catch {}
        }
        return;
      }
      
      if (!Array.isArray(data) || data.length === 0) return;
      
      const evt = data[0];
      
      try {
        switch (evt) {
          case "isInRoom": {
            const idtarget = ws.idtarget;
            if (!idtarget) {
              this.safeSend(ws, ["inRoomStatus", false]);
              return;
            }
            const currentRoom = this.userCurrentRoom.get(idtarget);
            this.safeSend(ws, ["inRoomStatus", currentRoom !== undefined]);
            break;
          }
          
          case "rollangak": {
            const roomName = data[1];
            const username = data[2];
            const angka = data[3];
            
            if (!roomName || !roomList.includes(roomName)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }
            
            this.broadcastToRoom(roomName, ["rollangakBroadcast", roomName, username, angka]);
            break;
          }

          case "modwarning": {
            const roomName = data[1];
            if (roomName && roomList.includes(roomName)) {
              this.broadcastToRoom(roomName, ["modwarning", roomName]);
            }
            break;
          }

          case "setMuteType": {
            const isMuted = data[1];
            const roomName = data[2];
            
            if (!roomName || !roomList.includes(roomName)) {
              this.safeSend(ws, ["error", "Room tidak valid"]);
              break;
            }
            
            const success = this.setRoomMute(roomName, isMuted);
            const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
            
            this.safeSend(ws, ["muteTypeSet", muteValue, success, roomName]);
            break;
          }
          
          case "getMuteType": {
            const roomName = data[1];
            
            if (!roomName || !roomList.includes(roomName)) {
              this.safeSend(ws, ["error", "Room tidak valid"]);
              break;
            }
            
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(roomName), roomName]);
            break;
          }
          
          case "onDestroy": {
            this.handleOnDestroy(ws, ws.idtarget);
            break;
          }
          
          case "setIdTarget2": {
            await this.handleSetIdTarget2(ws, data[1], data[2]);
            break;
          }
          
          case "sendnotif": {
            const [, idtarget, noimageUrl, username, deskripsi] = data;
            const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
            
            for (const client of this.clients) {
              if (client?.idtarget === idtarget && 
                  client.readyState === 1 && 
                  !client._isDuplicate && 
                  !client._isClosing) {
                this.safeSend(client, notif);
                break;
              }
            }
            break;
          }
          
          case "private": {
            const [, idt, url, msg, sender] = data;
            const ts = Date.now();
            const out = ["private", idt, url, msg, ts, sender];
            
            this.safeSend(ws, out);
            
            for (const client of this.clients) {
              if (client?.idtarget === idt && 
                  client.readyState === 1 && 
                  !client._isDuplicate && 
                  !client._isClosing) {
                this.safeSend(client, out);
                break;
              }
            }
            break;
          }
          
          case "isUserOnline": {
            const username = data[1];
            const tanda = data[2] ?? "";
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
          
          case "getCurrentNumber": {
            this.safeSend(ws, ["currentNumber", this.currentNumber]);
            break;
          }
          
          case "getOnlineUsers": {
            this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
            break;
          }
          
          case "getRoomOnlineUsers": {
            const roomName = data[1];
            if (!roomList.includes(roomName)) return;
            this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
            break;
          }
          
          case "joinRoom": {
            const success = await this.handleJoinRoom(ws, data[1]);
            if (success && ws.roomname) {
              this.updateRoomCount(ws.roomname);
            }
            break;
          }
          
          case "chat": {
            const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
            
            if (ws.roomname !== roomname || ws.idtarget !== username) return;
            if (!roomList.includes(roomname)) return;
            
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
            
            const chatMsg = ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor];
            this.broadcastToRoom(roomname, chatMsg);
            break;
          }
          
          case "updatePoint": {
            const [, room, seat, x, y, fast] = data;
            
            if (ws.roomname !== room || !roomList.includes(room)) return;
            if (seat < 1 || seat > this.MAX_SEATS) return;
            
            this.savePointWithRetry(room, seat, x, y, fast).catch(() => {});
            break;
          }
          
          case "removeKursiAndPoint": {
            const [, room, seat] = data;
            
            if (seat < 1 || seat > this.MAX_SEATS) return;
            if (ws.roomname !== room || !roomList.includes(room)) return;
            
            await this.updateSeatAtomic(room, seat, () => createEmptySeat());
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
            break;
          }
          
          case "updateKursi": {
            const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
            
            if (seat < 1 || seat > this.MAX_SEATS) return;
            if (ws.roomname !== room || !roomList.includes(room)) return;
            
            await this.updateSeatAtomic(room, seat, () => ({
              noimageUrl, 
              namauser, 
              color,
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
            this.broadcastToRoom(room, ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda]);
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
                await this.lowcard.handleEvent(ws, data);
              } else {
                this.safeSend(ws, ["error", "Game system not available"]);
              }
            }
            break;
            
          default:
            break;
        }
        
        this.metrics.messagesProcessed++;
      } catch (error) {
        this.metrics.errors++;
        if (ws.readyState === 1) {
          this.safeSend(ws, ["error", "Server error"]);
        }
      }
      
    } catch {}
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      
      // Health check endpoint
      if (url.pathname === "/health") {
        return new Response("OK", { 
          status: 200,
          headers: { "Content-Type": "text/plain" }
        });
      }
      
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
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
      
      const messageHandler = (ev) => {
        Promise.resolve().then(() => {
          this.handleMessage(ws, ev.data).catch(() => {});
        });
      };
      
      const errorHandler = () => {};
      
      const closeHandler = () => {
        Promise.resolve().then(() => {
          this.safeWebSocketCleanup(ws);
        });
      };
      
      ws.addEventListener("message", messageHandler);
      ws.addEventListener("error", errorHandler);
      ws.addEventListener("close", closeHandler);
      
      this.wsEventListeners.set(ws, {
        message: messageHandler,
        error: errorHandler,
        close: closeHandler
      });
      
      return new Response(null, { 
        status: 101, 
        webSocket: client 
      });
      
    } catch (error) {
      console.error("Fetch error:", error);
      this.metrics.errors++;
      return new Response("Internal server error", { status: 500 });
    }
  }
}



// ✅ EKSPOR YANG BENAR - HANYA SEKALI

// Ekspor class ChatServer (sudah diekspor di deklarasi class)
// Hapus baris: export { ChatServer };

// Ekspor default untuk Worker
export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      
      // Health check endpoint
      if (url.pathname === "/health") {
        return new Response("OK", { 
          status: 200,
          headers: { "Content-Type": "text/plain" }
        });
      }
      
      // WebSocket upgrade
      if (req.headers.get("Upgrade") === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      
      // Default response
      return new Response("WebSocket server ready", { 
        status: 200,
        headers: { "Content-Type": "text/plain" }
      });
      
    } catch (error) {
      console.error("Worker fetch error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
