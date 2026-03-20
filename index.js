import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambayan", "Lounge Talk",
  "Noxxeliverothcifsa", "One Side Love", "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// Constants - Optimized values
const CONSTANTS = {
  LOCK_TIMEOUT: 8000, // Reduced from 10000
  LOCK_ACQUIRE_TIMEOUT: 1500, // Reduced from 2000
  GRACE_PERIOD: 4000, // Reduced from 5000
  MAX_QUEUE_SIZE: 100, // Reduced from 200
  MAX_LOCK_QUEUE_SIZE: 50, // Reduced from 100
  MAX_POINTS_PER_ROOM: 100, // Reduced from 200
  MAX_POINTS_TOTAL: 2000, // Reduced from 5000
  MAX_POINTS_BEFORE_FLUSH: 500, // Reduced from 1000
  BUFFER_SIZE_LIMIT: 25, // Reduced from 50
  CACHE_VALID_DURATION: 3000, // Increased from 2000 (reduce updates)
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 3, // Reduced from 5
  SAFE_SEND_RETRY: 1, // Reduced from 2
  SAFE_SEND_RETRY_DELAY: 50, // Reduced from 100
  BROADCAST_BATCH_SIZE: 15, // Reduced from 20
  MEMORY_CLEANUP_INTERVAL: 15000, // Reduced from 30000
  CLIENT_CLEANUP_INTERVAL: 8000, // Reduced from 15000
  LOCK_CLEANUP_INTERVAL: 5000, // Reduced from 10000
  RATE_LIMIT_CLEANUP_INTERVAL: 15000, // Reduced from 30000
  SEAT_CHECK_INTERVAL: 15000, // Reduced from 30000
  LOAD_CHECK_INTERVAL: 3000, // Reduced from 5000
  MAIN_TIMER_INTERVAL: 100, // Increased from 50 (reduce CPU)
  MAX_MESSAGE_SIZE: 50000,
  MAX_ERROR_COUNT: 3,
  MAX_BUFFERED_AMOUNT: 500000,
  LOAD_THRESHOLD: 0.9,
  LOAD_RECOVERY_THRESHOLD: 0.7
};

class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
    this.lockTimestamps = new Map();
    this.cleanupTimer = null;
  }

  async acquire(resourceId) {
    // Quick check dan cleanup lock yang stuck
    const lockTime = this.lockTimestamps.get(resourceId);
    if (lockTime && Date.now() - lockTime > CONSTANTS.LOCK_TIMEOUT) {
      this.forceRelease(resourceId);
    }

    // Cek queue size dengan early rejection
    const currentQueue = this.queue.get(resourceId);
    if (currentQueue && currentQueue.length > CONSTANTS.MAX_LOCK_QUEUE_SIZE) {
      throw new Error(`Lock queue full: ${resourceId}`);
    }

    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, true);
      this.lockTimestamps.set(resourceId, Date.now());
      return () => this.release(resourceId);
    }

    // Optimasi queue dengan array pooling
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
            reject(new Error(`Lock timeout for ${resourceId}`));
          }
        }
      }, CONSTANTS.LOCK_TIMEOUT);

      this.queue.get(resourceId).push({ resolve, reject, timeoutId });
    }).then(() => () => this.release(resourceId));
  }

  release(resourceId) {
    const queue = this.queue.get(resourceId);
    if (queue && queue.length > 0) {
      const next = queue.shift();
      if (next.timeoutId) clearTimeout(next.timeoutId);
      this.lockTimestamps.set(resourceId, Date.now());
      next.resolve();
      
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
      for (const item of queue) {
        if (item.timeoutId) clearTimeout(item.timeoutId);
        if (item.reject) item.reject(new Error(`Lock released: ${resourceId}`));
      }
      this.queue.delete(resourceId);
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
}

class QueueManager {
  constructor(concurrency = 3) { // Reduced from 5
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = CONSTANTS.MAX_QUEUE_SIZE;
    this.processing = false;
    this.jobTimeouts = new WeakMap(); // Track job timeouts
  }

  async add(job) {
    if (this.queue.length >= this.maxQueueSize) {
      throw new Error("Server busy");
    }

    return new Promise((resolve, reject) => {
      const jobWrapper = { 
        job, 
        resolve, 
        reject, 
        timestamp: Date.now(),
        timeout: setTimeout(() => {
          const index = this.queue.indexOf(jobWrapper);
          if (index !== -1) {
            this.queue.splice(index, 1);
            reject(new Error("Job timeout"));
          }
        }, 10000)
      };
      
      this.queue.push(jobWrapper);
      
      if (!this.processing) {
        this.process();
      }
    });
  }

  async process() {
    if (this.processing || this.active >= this.concurrency) return;
    this.processing = true;

    while (this.queue.length > 0 && this.active < this.concurrency) {
      // Hapus expired jobs
      const now = Date.now();
      while (this.queue.length > 0 && now - this.queue[0].timestamp > 30000) {
        const expired = this.queue.shift();
        clearTimeout(expired.timeout);
        expired.reject(new Error("Request timeout"));
      }
      
      if (this.queue.length === 0) break;
      
      const jobData = this.queue.shift();
      this.active++;
      
      // Process tanpa Promise.race untuk mengurangi overhead
      try {
        const result = await jobData.job();
        clearTimeout(jobData.timeout);
        jobData.resolve(result);
      } catch (error) {
        clearTimeout(jobData.timeout);
        jobData.reject(error);
      } finally {
        this.active--;
      }
    }

    this.processing = false;
    if (this.queue.length > 0) {
      setImmediate?.(() => this.process()) || setTimeout(() => this.process(), 5);
    }
  }

  clear() {
    for (const item of this.queue) {
      clearTimeout(item.timeout);
      item.reject(new Error("Queue cleared"));
    }
    this.queue = [];
    this.active = 0;
  }

  size() {
    return this.queue.length;
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
    let userRequests = this.requests.get(userId);
    
    if (!userRequests) {
      userRequests = [];
      this.requests.set(userId, userRequests);
    }
    
    // Filter sambil cek
    let count = 0;
    for (let i = userRequests.length - 1; i >= 0; i--) {
      if (now - userRequests[i] < this.windowMs) {
        count++;
      } else {
        userRequests.splice(i, 1);
      }
    }
    
    if (count >= this.maxRequests) return false;
    
    userRequests.push(now);
    return true;
  }

  cleanup() {
    const now = Date.now();
    for (const [userId, requests] of this.requests) {
      const valid = [];
      for (const time of requests) {
        if (now - time < this.windowMs) {
          valid.push(time);
        }
      }
      if (valid.length === 0) {
        this.requests.delete(userId);
      } else if (valid.length !== requests.length) {
        this.requests.set(userId, valid);
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
      
      // Mute status - gunakan array untuk reduce overhead
      this.muteStatus = new Array(roomList.length).fill(false);
      
      this.storage = state?.storage;
      
      // Core data structures - optimized initialization
      this.lockManager = new PromiseLockManager();
      this.cleanupInProgress = new Set();
      this.clients = new Set();
      this.userToSeat = new Map();
      this.roomClients = new Array(roomList.length).fill().map(() => []);
      this.userCurrentRoom = new Map();
      this.MAX_SEATS = CONSTANTS.MAX_SEATS;
      
      // Pre-allocate room structures
      this.roomSeats = new Array(roomList.length);
      this.seatOccupancy = new Array(roomList.length);
      this.updateKursiBuffer = new Array(roomList.length);
      this._pointBuffer = new Array(roomList.length);
      
      for (let i = 0; i < roomList.length; i++) {
        const seatMap = new Map();
        const occupancyMap = new Map();
        
        for (let j = 1; j <= this.MAX_SEATS; j++) {
          seatMap.set(j, null);
          occupancyMap.set(j, null);
        }
        
        this.roomSeats[i] = seatMap;
        this.seatOccupancy[i] = occupancyMap;
        this.updateKursiBuffer[i] = new Map();
        this._pointBuffer[i] = [];
      }
      
      this.userConnections = new Map();
      this.bufferSizeLimit = CONSTANTS.BUFFER_SIZE_LIMIT;
      
      // Point buffer management
      this._pointFlushTimer = null;
      this._pointFlushDelay = 32; // Increased from 16
      this._hasBufferedUpdates = false;

      // Rate limiters
      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      
      // Safe mode
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
      this.lastLoadCheck = 0;

      // Lowcard game
      try {
        this.lowcard = new LowCardGameManager(this);
      } catch {
        this.lowcard = null;
      }

      // Disconnect handling
      this.gracePeriod = CONSTANTS.GRACE_PERIOD;
      this.disconnectedTimers = new Map();
      this.cleanupQueue = new QueueManager(3); // Reduced concurrency
      
      // Number ticker
      this.currentNumber = 1;
      this.maxNumber = 6;
      this.intervalMillis = 15 * 60 * 1000;
      this._nextConnId = 1;

      // Main timer - akan dimulai nanti
      this.mainTimer = null;
      this.tickCounter = 0;
      this.lastNumberTick = Date.now();
      this.numberTickInterval = this.intervalMillis;

      // Cache
      this.roomCountsCache = new Array(roomList.length).fill(0);
      this.cacheValidDuration = CONSTANTS.CACHE_VALID_DURATION;
      this.lastCacheUpdate = 0;

      // Cleanup counters
      this.lastMemoryCleanup = Date.now();
      this.lastClientCleanup = Date.now();
      this.lastLockCleanup = Date.now();
      this.lastRateLimitCleanup = Date.now();
      this.lastSeatCheck = Date.now();

      // Start main timer
      this.startMainTimer();

    } catch (error) {
      console.error("ChatServer constructor error:", error);
      this.initializeFallback();
    }
  }

  initializeFallback() {
    this.clients = new Set();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
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
    this.cleanupQueue = new QueueManager(3);
    
    // Initialize arrays
    this.muteStatus = new Array(roomList.length).fill(false);
    this.roomClients = new Array(roomList.length).fill().map(() => []);
    this.roomSeats = new Array(roomList.length);
    this.seatOccupancy = new Array(roomList.length);
    this.updateKursiBuffer = new Array(roomList.length);
    this._pointBuffer = new Array(roomList.length);
    this.roomCountsCache = new Array(roomList.length).fill(0);
    
    for (let i = 0; i < roomList.length; i++) {
      const seatMap = new Map();
      const occupancyMap = new Map();
      
      for (let j = 1; j <= this.MAX_SEATS; j++) {
        seatMap.set(j, null);
        occupancyMap.set(j, null);
      }
      
      this.roomSeats[i] = seatMap;
      this.seatOccupancy[i] = occupancyMap;
      this.updateKursiBuffer[i] = new Map();
      this._pointBuffer[i] = [];
    }
    
    this.storage = this.state?.storage;
    this.rateLimiter = new RateLimiter(60000, 100);
    this.connectionRateLimiter = new RateLimiter(10000, 5);
    this.safeMode = false;
    this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
    
    this._pointFlushTimer = null;
    this._pointFlushDelay = 32;
    this._hasBufferedUpdates = false;

    this.lastMemoryCleanup = Date.now();
    this.lastClientCleanup = Date.now();
    this.lastLockCleanup = Date.now();
    this.lastRateLimitCleanup = Date.now();
    this.lastSeatCheck = Date.now();
    this.tickCounter = 0;
    this.lastNumberTick = Date.now();
    this.numberTickInterval = 15 * 60 * 1000;
    this.startMainTimer();
  }

  // Helper untuk convert room name ke index
  _roomToIndex(roomName) {
    const index = roomList.indexOf(roomName);
    return index === -1 ? null : index;
  }

  // ========== TIMER MANAGEMENT ==========

  startMainTimer() {
    if (this.mainTimer) {
      clearInterval(this.mainTimer);
    }
    this.mainTimer = setInterval(() => {
      this.runMainTasks().catch(() => {});
    }, CONSTANTS.MAIN_TIMER_INTERVAL);
    
    // Prevent timer from keeping process alive
    if (this.mainTimer.unref) this.mainTimer.unref();
  }

  async runMainTasks() {
    const now = Date.now();
    
    // Fast tasks - gunakan sampling untuk mengurangi load
    if (this._hasBufferedUpdates) {
      this.flushKursiUpdates();
      this.flushBufferedPoints();
    }
    
    // Medium tasks dengan interval yang lebih efisien
    if (this.tickCounter % 5 === 0) { // Setiap 500ms dengan interval 100ms
      if (now - this.lastLoadCheck >= this.loadCheckInterval) {
        this.checkAndEnableSafeMode();
        this.lastLoadCheck = now;
      }
    }
    
    if (this.tickCounter % 10 === 0) { // Setiap 1000ms
      this.validateGracePeriodTimers();
    }
    
    // Lock cleanup - lebih sering tapi lightweight
    if (now - this.lastLockCleanup >= this.lockCleanupInterval) {
      if (this.lockManager?.locks.size > 0) {
        this.lockManager.cleanupStuckLocks();
      }
      this.lastLockCleanup = now;
    }
    
    // Client cleanup - lebih agresif
    if (now - this.lastClientCleanup >= this.clientCleanupInterval) {
      await this.cleanupDuplicateConnections();
      this.lastClientCleanup = now;
    }
    
    // Memory cleanup - lebih sering
    if (now - this.lastMemoryCleanup >= this.memoryCleanupInterval) {
      await this.performMemoryCleanup();
      this.lastMemoryCleanup = now;
    }
    
    // Rate limiter cleanup
    if (now - this.lastRateLimitCleanup >= this.rateLimitCleanupInterval) {
      this.rateLimiter.cleanup();
      this.connectionRateLimiter.cleanup();
      this.lastRateLimitCleanup = now;
    }
    
    // Seat check - hanya jika load rendah
    if (now - this.lastSeatCheck >= this.seatCheckInterval && this.getServerLoad() < 0.7) {
      this.sampledSeatConsistencyCheck();
      this.lastSeatCheck = now;
    }
    
    // Number tick
    if (now - this.lastNumberTick >= this.numberTickInterval) {
      this.tick();
      this.lastNumberTick = now;
    }
    
    this.tickCounter = (this.tickCounter + 1) % 50; // Reset lebih cepat
  }

  // ========== MEMORY MANAGEMENT ==========

  async performMemoryCleanup() {
    try {
      // Cleanup dead clients - optimasi dengan Set
      const deadClients = [];
      for (const client of this.clients) {
        if (!client || client.readyState === 3) {
          deadClients.push(client);
        }
      }
      for (const client of deadClients) {
        this.clients.delete(client);
      }

      // Cleanup room clients dengan array index
      for (let i = 0; i < this.roomClients.length; i++) {
        const clientArray = this.roomClients[i];
        if (clientArray && clientArray.length > 0) {
          const filtered = clientArray.filter(c => c && c.readyState === 1);
          if (filtered.length !== clientArray.length) {
            this.roomClients[i] = filtered;
          }
        }
      }

      // Cleanup user connections
      let totalPoints = 0;
      for (const [userId, connections] of this.userConnections) {
        const activeConnections = new Set();
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            if (conn._connectionTime && (Date.now() - conn._connectionTime) < 3600000) { // 1 jam
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

      // Cleanup point buffers - lebih agresif
      for (let i = 0; i < this._pointBuffer.length; i++) {
        const points = this._pointBuffer[i];
        if (points && points.length > CONSTANTS.MAX_POINTS_PER_ROOM) {
          this._pointBuffer[i] = points.slice(-CONSTANTS.MAX_POINTS_PER_ROOM);
        }
        totalPoints += this._pointBuffer[i]?.length || 0;
      }

      // Aggressive cleanup if total points too high
      if (totalPoints > CONSTANTS.MAX_POINTS_TOTAL) {
        const reduceBy = Math.ceil((totalPoints - CONSTANTS.MAX_POINTS_TOTAL) / this._pointBuffer.length);
        for (let i = 0; i < this._pointBuffer.length; i++) {
          const points = this._pointBuffer[i];
          if (points.length > reduceBy) {
            this._pointBuffer[i] = points.slice(reduceBy);
          }
        }
      }

      // Cleanup disconnected timers
      const now = Date.now();
      const maxGracePeriod = this.gracePeriod + 2000;
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer._scheduledTime && (now - timer._scheduledTime) > maxGracePeriod) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
        }
      }

    } catch (error) {
      // Silent fail
    }
  }

  // ========== MUTE STATUS ==========

  setRoomMute(roomName, isMuted) {
    try {
      const index = this._roomToIndex(roomName);
      if (index === null) return false;
      
      const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
      this.muteStatus[index] = muteValue;
      
      this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
      return true;
    } catch {
      return false;
    }
  }

  getRoomMute(roomName) {
    try {
      const index = this._roomToIndex(roomName);
      return index !== null ? this.muteStatus[index] : false;
    } catch {
      return false;
    }
  }

  // ========== CONNECTION MANAGEMENT ==========

  _addUserConnection(userId, ws) {
    if (!userId || !ws) return;
    
    let userConnections = this.userConnections.get(userId);
    if (!userConnections) {
      userConnections = new Set();
      this.userConnections.set(userId, userConnections);
    }
    
    // Batasi jumlah koneksi per user
    if (userConnections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
      const connections = Array.from(userConnections);
      // Close oldest connection
      const oldest = connections.reduce((oldest, conn) => {
        return (!oldest || (conn._connectionTime || 0) < (oldest._connectionTime || 0)) ? conn : oldest;
      }, null);
      
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
      if (userConnections.size === 0) {
        this.userConnections.delete(userId);
      }
    }
  }

  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    
    const index = this._roomToIndex(room);
    if (index === null) return;
    
    const clientArray = this.roomClients[index];
    if (clientArray) {
      const pos = clientArray.indexOf(ws);
      if (pos > -1) {
        clientArray.splice(pos, 1);
      }
    }
  }

  // ========== LOCK MANAGEMENT ==========

  async withLock(resourceId, operation, timeout = CONSTANTS.LOCK_ACQUIRE_TIMEOUT) {
    let release;
    try {
      release = await this.lockManager.acquire(resourceId);
      return await operation();
    } finally {
      if (release) {
        try { release(); } catch {}
      }
    }
  }

  // ========== SAFE MODE ==========

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
    this._pointFlushDelay = 100;
  }

  disableSafeMode() {
    this.safeMode = false;
    this.cleanupQueue.concurrency = 3;
    this._pointFlushDelay = 32;
  }

  getServerLoad() {
    const activeConnections = this.clients.size;
    const queueSize = this.cleanupQueue?.size() || 0;
    return Math.min(activeConnections / 150 + queueSize / 100, 0.95);
  }

  // ========== POINT MANAGEMENT ==========

  schedulePointFlush(room) {
    if (this._pointFlushTimer) return;
    
    this._pointFlushTimer = setTimeout(() => {
      this._pointFlushTimer = null;
      if (this._hasBufferedUpdates) {
        this.flushBufferedPoints();
      }
    }, this._pointFlushDelay);
  }

  flushBufferedPoints() {
    this._hasBufferedUpdates = false;
    
    for (let i = 0; i < this._pointBuffer.length; i++) {
      const points = this._pointBuffer[i];
      if (points && points.length > 0) {
        const batch = points.splice(0, points.length);
        if (batch.length > 0) {
          this.broadcastPointsBatch(roomList[i], batch);
        }
      }
    }
  }

  broadcastPointsBatch(room, batch) {
    try {
      const index = this._roomToIndex(room);
      if (index === null) return;
      
      const validBatch = batch.filter(point => 
        point && point.seat >= 1 && point.seat <= this.MAX_SEATS
      );
      
      if (validBatch.length === 0) return;
      
      const clientArray = this.roomClients[index];
      if (!clientArray || clientArray.length === 0) return;
      
      const message = JSON.stringify(["pointsBatch", room, validBatch]);
      
      // Broadcast dengan batas
      let sent = 0;
      for (const client of clientArray) {
        if (client?.readyState === 1 && client.roomname === room) {
          try { 
            client.send(message); 
            sent++;
            if (sent >= 50) break; // Batasi broadcast
          } catch {}
        }
      }
    } catch {}
  }

  broadcastPointDirect(room, seat, x, y, fast) {
    try {
      const index = this._roomToIndex(room);
      if (index === null) return;
      
      const clientArray = this.roomClients[index];
      if (!clientArray || clientArray.length === 0) return;
      
      const message = JSON.stringify(["pointUpdated", room, seat, x, y, fast]);
      
      let sent = 0;
      for (const client of clientArray) {
        if (client?.readyState === 1 && client.roomname === room) {
          try { 
            client.send(message); 
            sent++;
            if (sent >= 50) break;
          } catch {}
        }
      }
    } catch {}
  }

  async savePointWithRetry(room, seat, x, y, fast) {
    try {
      const index = this._roomToIndex(room);
      if (index === null || seat < 1 || seat > this.MAX_SEATS) return false;
      
      const xNum = typeof x === 'number' ? x : parseFloat(x);
      const yNum = typeof y === 'number' ? y : parseFloat(y);
      
      if (isNaN(xNum) || isNaN(yNum)) return false;
      
      // Buffer point
      const points = this._pointBuffer[index];
      points.push({ seat, x: xNum, y: yNum, fast: !!fast });
      this._hasBufferedUpdates = true;
      
      if (points.length > CONSTANTS.MAX_POINTS_BEFORE_FLUSH) {
        this.flushBufferedPoints();
      } else {
        this.schedulePointFlush(room);
      }
      
      // Update seat data async
      this.updateSeatAtomic(room, seat, (currentSeat) => {
        currentSeat.lastPoint = { 
          x: xNum, 
          y: yNum, 
          fast: fast || false, 
          timestamp: Date.now() 
        };
        return currentSeat;
      }).catch(() => {});
      
      // Broadcast langsung untuk real-time
      this.broadcastPointDirect(room, seat, xNum, yNum, fast);
      return true;
      
    } catch {
      return false;
    }
  }

  // ========== SEAT MANAGEMENT ==========

  async ensureSeatsData(room) {
    const index = this._roomToIndex(room);
    if (index === null) return;
    
    const seatMap = this.roomSeats[index];
    const occupancyMap = this.seatOccupancy[index];
    
    if (!seatMap || !occupancyMap) return;
    
    // Only check if needed
    if (seatMap.size < this.MAX_SEATS) {
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatMap.has(seat)) seatMap.set(seat, null);
        if (!occupancyMap.has(seat)) occupancyMap.set(seat, null);
      }
    }
  }

  async updateSeatAtomic(room, seatNumber, updateFn) {
    try {
      const index = this._roomToIndex(room);
      if (index === null || seatNumber < 1 || seatNumber > this.MAX_SEATS) return null;
      
      return await this.withLock(`seat-${index}-${seatNumber}`, () => {
        const seatMap = this.roomSeats[index];
        const occupancyMap = this.seatOccupancy[index];
        
        if (!seatMap || !occupancyMap) return null;
        
        let currentSeat = seatMap.get(seatNumber);
        if (!currentSeat) {
          currentSeat = createEmptySeat();
          seatMap.set(seatNumber, currentSeat);
        }
        
        const updatedSeat = updateFn(currentSeat);
        updatedSeat.lastUpdated = Date.now();
        
        if (updatedSeat.namauser) {
          occupancyMap.set(seatNumber, updatedSeat.namauser);
        } else {
          occupancyMap.set(seatNumber, null);
        }
        
        seatMap.set(seatNumber, updatedSeat);
        
        // Update buffer
        const buffer = this.updateKursiBuffer[index];
        if (buffer && updatedSeat.namauser) {
          if (buffer.size >= this.bufferSizeLimit) {
            const firstKey = buffer.keys().next().value;
            if (firstKey) buffer.delete(firstKey);
          }
          
          const { lastPoint, lastUpdated, ...bufferInfo } = updatedSeat;
          buffer.set(seatNumber, bufferInfo);
        }
        
        return updatedSeat;
      });
    } catch {
      return null;
    }
  }

  clearSeatBuffer(room, seatNumber) {
    const index = this._roomToIndex(room);
    if (index === null || seatNumber < 1 || seatNumber > this.MAX_SEATS) return;
    
    const roomMap = this.updateKursiBuffer[index];
    if (roomMap) roomMap.delete(seatNumber);
  }

  async findEmptySeat(room, ws) {
    if (!room || !ws || !ws.idtarget) return null;
    
    try {
      const index = this._roomToIndex(room);
      if (index === null) return null;
      
      const occupancyMap = this.seatOccupancy[index];
      const seatMap = this.roomSeats[index];
      
      if (!occupancyMap || !seatMap) return null;
      
      // Cek kursi user sendiri
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        
        if (occupantId === ws.idtarget && seatData?.namauser === ws.idtarget) {
          return i;
        }
      }
      
      // Cari kursi kosong
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        
        if (occupantId === null && (!seatData || !seatData.namauser)) {
          return i;
        }
      }
      
      return null;
    } catch {
      return null;
    }
  }

  async assignSeatAtomic(room, seat, userId) {
    const index = this._roomToIndex(room);
    if (index === null) return false;
    
    const release = await this.lockManager.acquire(`assign-${index}-${seat}`);
    
    try {
      const occupancyMap = this.seatOccupancy[index];
      const seatMap = this.roomSeats[index];
      
      if (!occupancyMap || !seatMap) return false;
      
      const occupantId = occupancyMap.get(seat);
      const seatData = seatMap.get(seat);
      
      if (occupantId !== null || (seatData && seatData.namauser)) return false;
      
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
      
      return true;
    } finally {
      release();
    }
  }

  async cleanupUserFromSeat(room, seatNumber, userId, immediate = true) {
    try {
      const index = this._roomToIndex(room);
      if (index === null || seatNumber < 1 || seatNumber > this.MAX_SEATS) return;
      
      await this.withLock(`seat-clean-${index}-${seatNumber}`, async () => {
        const seatMap = this.roomSeats[index];
        const occupancyMap = this.seatOccupancy[index];
        
        if (!seatMap || !occupancyMap) return;
        
        const seatInfo = seatMap.get(seatNumber);
        if (!seatInfo || seatInfo.namauser !== userId) return;
        
        if (immediate) {
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

  // ========== CLEANUP MANAGEMENT ==========

  scheduleCleanup(userId) {
    if (!userId) return;
    
    try {
      this.cancelCleanup(userId);
      
      const timerId = setTimeout(async () => {
        try {
          this.disconnectedTimers.delete(userId);
          
          const isStillConnected = await this.isUserStillConnected(userId);
          
          if (!isStillConnected) {
            await this.withLock(`grace-${userId}`, async () => {
              const doubleCheck = await this.isUserStillConnected(userId);
              if (!doubleCheck) {
                await this.forceUserCleanup(userId);
              }
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
    
    const timer = this.disconnectedTimers.get(userId);
    if (timer) {
      clearTimeout(timer);
      this.disconnectedTimers.delete(userId);
    }
    this.cleanupInProgress?.delete(userId);
  }

  async isUserStillConnected(userId) {
    if (!userId) return false;
    
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return false;
    
    for (const conn of connections) {
      if (conn?.readyState === 1 && !conn._isDuplicate && !conn._isClosing) {
        return true;
      }
    }
    return false;
  }

  async forceUserCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    
    this.cleanupInProgress.add(userId);
    
    try {
      await this.withLock(`force-clean-${userId}`, async () => {
        this.cancelCleanup(userId);
        
        // Cari semua kursi user
        const currentRoom = this.userCurrentRoom.get(userId);
        const roomsToCheck = currentRoom ? [currentRoom] : roomList;
        const seatsToCleanup = [];
        
        for (const room of roomsToCheck) {
          const index = this._roomToIndex(room);
          if (index === null) continue;
          
          const seatMap = this.roomSeats[index];
          if (!seatMap) continue;
          
          for (let i = 1; i <= this.MAX_SEATS; i++) {
            const seatInfo = seatMap.get(i);
            if (seatInfo?.namauser === userId) {
              seatsToCleanup.push({ room, seatNumber: i });
            }
          }
        }
        
        // Cleanup semua kursi
        for (const { room, seatNumber } of seatsToCleanup) {
          await this.cleanupUserFromSeat(room, seatNumber, userId, true);
        }
        
        // Cleanup data user
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        
        // Cleanup connections
        this.userConnections.delete(userId);
        
        // Cleanup room clients
        for (let i = 0; i < this.roomClients.length; i++) {
          const clientArray = this.roomClients[i];
          if (clientArray?.length > 0) {
            const filtered = clientArray.filter(c => c?.idtarget !== userId);
            if (filtered.length !== clientArray.length) {
              this.roomClients[i] = filtered;
            }
          }
        }
      });
    } finally {
      this.cleanupInProgress.delete(userId);
    }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    
    try {
      await this.withLock(`room-clean-${room}-${ws.idtarget}`, async () => {
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
      });
    } catch {}
  }

  async fullRemoveById(idtarget) {
    if (!idtarget) return;
    
    try {
      await this.withLock(`full-remove-${idtarget}`, async () => {
        this.cancelCleanup(idtarget);
        
        // Cleanup semua kursi
        for (let i = 0; i < roomList.length; i++) {
          const room = roomList[i];
          const seatMap = this.roomSeats[i];
          if (!seatMap) continue;
          
          for (let seatNumber = 1; seatNumber <= this.MAX_SEATS; seatNumber++) {
            const info = seatMap.get(seatNumber);
            if (info?.namauser === idtarget) {
              Object.assign(info, createEmptySeat());
              this.seatOccupancy[i].set(seatNumber, null);
              this.clearSeatBuffer(room, seatNumber);
              this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
            }
          }
          
          this.updateRoomCount(room);
        }
        
        // Cleanup data user
        this.userToSeat.delete(idtarget);
        this.userCurrentRoom.delete(idtarget);
        this.userConnections.delete(idtarget);
        
        // Cleanup clients
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
          this.clients.delete(client);
          
          for (let i = 0; i < this.roomClients.length; i++) {
            const clientArray = this.roomClients[i];
            if (clientArray) {
              const pos = clientArray.indexOf(client);
              if (pos > -1) clientArray.splice(pos, 1);
            }
          }
        }
      });
    } catch {}
  }

  validateGracePeriodTimers() {
    try {
      const now = Date.now();
      const maxGracePeriod = this.gracePeriod + 2000;
      
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer?._scheduledTime && now - timer._scheduledTime > maxGracePeriod) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
          this.executeGracePeriodCleanup(userId).catch(() => {});
        }
      }
    } catch {}
  }

  async executeGracePeriodCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    
    if (this.safeMode) {
      setTimeout(() => this.executeGracePeriodCleanup(userId), 2000);
      return;
    }
    
    this.cleanupInProgress.add(userId);
    
    try {
      await this.withLock(`grace-clean-${userId}`, async () => {
        const isConnected = await this.isUserStillConnected(userId);
        if (!isConnected) {
          await this.forceUserCleanup(userId);
        }
      });
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
    
    const index = this._roomToIndex(room);
    if (index === null) {
      this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    if (!this.rateLimiter.check(ws.idtarget)) {
      this.safeSend(ws, ["error", "Too many requests"]);
      return false;
    }
    
    try {
      const roomRelease = await this.lockManager.acquire(`join-${index}`);
      
      try {
        this.cancelCleanup(ws.idtarget);
        await this.ensureSeatsData(room);
        
        // Cek room sebelumnya
        const previousRoom = this.userCurrentRoom.get(ws.idtarget);
        
        if (previousRoom) {
          if (previousRoom === room) {
            this.sendAllStateTo(ws, room);
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
            roomRelease();
            return true;
          } else {
            await this.cleanupFromRoom(ws, previousRoom);
          }
        }
        
        // Cek seat info existing
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (seatInfo?.room === room) {
          const occupancyMap = this.seatOccupancy[index];
          if (occupancyMap?.get(seatInfo.seat) === ws.idtarget) {
            ws.roomname = room;
            ws.numkursi = new Set([seatInfo.seat]);
            
            const clientArray = this.roomClients[index];
            if (!clientArray.includes(ws)) {
              clientArray.push(ws);
            }
            
            this._addUserConnection(ws.idtarget, ws);
            this.sendAllStateTo(ws, room);
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
            roomRelease();
            return true;
          }
        }
        
        // Cari kursi kosong
        let assignedSeat = null;
        const occupancyMap = this.seatOccupancy[index];
        
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
          if (occupancyMap.get(seat) === null) {
            assignedSeat = seat;
            break;
          }
        }
        
        if (!assignedSeat) {
          this.safeSend(ws, ["roomFull", room]);
          roomRelease();
          return false;
        }
        
        // Assign seat
        const success = await this.assignSeatAtomic(room, assignedSeat, ws.idtarget);
        
        if (!success) {
          this.safeSend(ws, ["roomFull", room]);
          roomRelease();
          return false;
        }
        
        this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
        this.userCurrentRoom.set(ws.idtarget, room);
        ws.roomname = room;
        ws.numkursi = new Set([assignedSeat]);
        
        const clientArray = this.roomClients[index];
        if (!clientArray.includes(ws)) {
          clientArray.push(ws);
        }
        
        this._addUserConnection(ws.idtarget, ws);
        this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
        this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
        
        setTimeout(() => this.sendAllStateTo(ws, room), 50);
        this.updateRoomCount(room);
        
        roomRelease();
        return true;
      } catch (error) {
        roomRelease();
        throw error;
      }
    } catch {
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
          
          const index = this._roomToIndex(room);
          if (index === null) {
            this.userToSeat.delete(id);
            this.userCurrentRoom.delete(id);
            this.safeSend(ws, ["needJoinRoom"]);
            return;
          }
          
          const seatMap = this.roomSeats[index];
          const occupancyMap = this.seatOccupancy[index];
          
          if (seatMap && occupancyMap) {
            const seatData = seatMap.get(seat);
            const occupantId = occupancyMap.get(seat);
            
            if (seatData?.namauser === id && occupantId === id) {
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              
              const clientArray = this.roomClients[index];
              if (!clientArray.includes(ws)) {
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
        
        this.safeSend(ws, ["needJoinRoom"]);
      });
    } catch {
      this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }

  // ========== BROADCAST & SEND ==========

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
      const index = this._roomToIndex(room);
      if (index === null) return 0;
      
      const clientArray = this.roomClients[index];
      if (!clientArray?.length) return 0;
      
      let sentCount = 0;
      const message = JSON.stringify(msg);
      
      for (const client of clientArray) {
        if (client?.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
          try {
            client.send(message);
            sentCount++;
            if (sentCount >= 100) break; // Limit broadcast
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
      const index = this._roomToIndex(room);
      if (index === null) return;
      
      const seatMap = this.roomSeats[index];
      if (!seatMap) return;
      
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const info = seatMap.get(i);
        if (info?.namauser) count++;
      }
      
      this.roomCountsCache[index] = count;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch {}
  }

  // ========== STATE MANAGEMENT ==========

  sendAllStateTo(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
      
      const index = this._roomToIndex(room);
      if (index === null) return;
      
      const seatMap = this.roomSeats[index];
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
      
      const count = this.roomCountsCache[index] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
    } catch {}
  }

  // ========== USER COUNT ==========

  getJumlahRoom() {
    try {
      const now = Date.now();
      
      if (now - this.lastCacheUpdate < this.cacheValidDuration) {
        const result = {};
        for (let i = 0; i < roomList.length; i++) {
          result[roomList[i]] = this.roomCountsCache[i] || 0;
        }
        return result;
      }
      
      const counts = {};
      for (let i = 0; i < roomList.length; i++) {
        let count = 0;
        const occupancyMap = this.seatOccupancy[i];
        if (occupancyMap) {
          for (let j = 1; j <= this.MAX_SEATS; j++) {
            if (occupancyMap.get(j)) count++;
          }
        }
        counts[roomList[i]] = count;
        this.roomCountsCache[i] = count;
      }
      
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

  updateRoomCount(room) {
    try {
      const index = this._roomToIndex(room);
      if (index === null) return 0;
      
      const seatMap = this.roomSeats[index];
      if (!seatMap) return 0;
      
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const info = seatMap.get(i);
        if (info?.namauser) count++;
      }
      
      this.roomCountsCache[index] = count;
      this.broadcastRoomUserCount(room);
      return count;
    } catch {
      return 0;
    }
  }

  // ========== CONSISTENCY CHECKS ==========

  sampledSeatConsistencyCheck() {
    try {
      // Check only 3 random rooms
      for (let i = 0; i < 3; i++) {
        const randomIndex = Math.floor(Math.random() * roomList.length);
        if (this.getServerLoad() >= 0.8) break;
        this.validateSeatConsistency(roomList[randomIndex]);
      }
    } catch {}
  }

  async validateSeatConsistency(room) {
    try {
      const index = this._roomToIndex(room);
      if (index === null) return;
      
      const seatMap = this.roomSeats[index];
      const occupancyMap = this.seatOccupancy[index];
      
      if (!seatMap || !occupancyMap) return;
      
      // Sample check - only check 10 random seats
      const seatsToCheck = [];
      for (let i = 0; i < 10; i++) {
        seatsToCheck.push(Math.floor(Math.random() * this.MAX_SEATS) + 1);
      }
      
      for (const seat of seatsToCheck) {
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
        }
      }
    } catch {}
  }

  // ========== DUPLICATE CONNECTION HANDLING ==========

  async cleanupDuplicateConnections() {
    try {
      const userConnectionCount = new Map();
      
      for (const client of this.clients) {
        if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
          const count = userConnectionCount.get(client.idtarget) || 0;
          userConnectionCount.set(client.idtarget, count + 1);
        }
      }
      
      for (const [userId, count] of userConnectionCount) {
        if (count > 1) {
          await this.handleDuplicateConnections(userId);
        }
      }
    } catch {}
  }

  async handleDuplicateConnections(userId) {
    if (!userId) return;
    
    try {
      await this.withLock(`dup-${userId}`, async () => {
        const allConnections = [];
        
        for (const client of this.clients) {
          if (client?.idtarget === userId && client.readyState === 1 && !client._isClosing) {
            allConnections.push({
              client,
              connectionTime: client._connectionTime || 0
            });
          }
        }
        
        if (allConnections.length <= 1) return;
        
        allConnections.sort((a, b) => b.connectionTime - a.connectionTime);
        
        // Keep newest, close others
        for (let i = 1; i < allConnections.length; i++) {
          const { client } = allConnections[i];
          client._isDuplicate = true;
          client._isClosing = true;
          
          try {
            if (client.readyState === 1) {
              this.safeSend(client, ["duplicateConnection", "Another connection was opened"]);
              client.close(1000, "Duplicate connection");
            }
          } catch {}
          
          this.clients.delete(client);
          
          if (client.roomname) {
            this._removeFromRoomClients(client, client.roomname);
          }
          
          this._removeUserConnection(userId, client);
        }
      });
    } catch {}
  }

  // ========== UTILITY ==========

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
    } catch {
      return [];
    }
  }

  getOnlineUsersByRoom(roomName) {
    try {
      const index = this._roomToIndex(roomName);
      if (index === null) return [];
      
      const users = [];
      const seenUsers = new Set();
      const clientArray = this.roomClients[index];
      
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
    } catch {
      return [];
    }
  }

  flushKursiUpdates() {
    try {
      if (!this.updateKursiBuffer) return;
      
      for (let i = 0; i < this.updateKursiBuffer.length; i++) {
        const seatMapUpdates = this.updateKursiBuffer[i];
        if (!seatMapUpdates || seatMapUpdates.size === 0) continue;
        
        const room = roomList[i];
        const updates = [];
        
        for (const [seat, info] of seatMapUpdates.entries()) {
          if (seat < 1 || seat > this.MAX_SEATS) continue;
          
          if (info?.namauser) {
            const { lastPoint, ...rest } = info;
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
        
        this.updateKursiBuffer[i] = new Map();
      }
      
      this._hasBufferedUpdates = false;
    } catch {}
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      let sent = 0;
      
      for (const client of this.clients) {
        if (client?.readyState === 1 && client.roomname && !client._isDuplicate && !client._isClosing) {
          try {
            client.send(message);
            sent++;
            if (sent >= 200) break; // Limit broadcast
          } catch {}
        }
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
        
        for (let i = 0; i < this.roomClients.length; i++) {
          const clientArray = this.roomClients[i];
          if (clientArray) {
            const pos = clientArray.indexOf(ws);
            if (pos > -1) clientArray.splice(pos, 1);
          }
        }
        
        this.clients.delete(ws);
        
        if (ws.readyState === 1) {
          try { ws.close(1000, "Manual destroy"); } catch {}
        }
      });
    } catch {
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
      this.clients.delete(ws);
      
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
    } catch {
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
      } catch {
        ws.errorCount = (ws.errorCount || 0) + 1;
        if (ws.errorCount > CONSTANTS.MAX_ERROR_COUNT) {
          try { ws.close(1008, "Protocol error"); } catch {}
        }
        return;
      }
      
      if (!Array.isArray(data) || data.length === 0) return;
      
      const evt = data[0];
      
      // Fast path untuk event umum
      switch (evt) {
        case "isInRoom": {
          const idtarget = ws.idtarget;
          this.safeSend(ws, ["inRoomStatus", !!idtarget && this.userCurrentRoom.has(idtarget)]);
          break;
        }
        
        case "getCurrentNumber": {
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        }
        
        case "getAllRoomsUserCount": {
          const allCounts = this.getJumlahRoom();
          const result = roomList.map(room => [room, allCounts[room]]);
          this.safeSend(ws, ["allRoomsUserCount", result]);
          break;
        }
        
        case "getMuteType": {
          const roomName = data[1];
          this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(roomName), roomName]);
          break;
        }
        
        default: {
          // Handle other events in background
          setTimeout(() => this.handleMessageAsync(ws, data, evt), 0);
          break;
        }
      }
    } catch {}
  }

  async handleMessageAsync(ws, data, evt) {
    try {
      switch (evt) {
        case "rollangak": {
          const roomName = data[1];
          const username = data[2];
          const angka = data[3];
          if (roomName && roomList.includes(roomName)) {
            this.broadcastToRoom(roomName, ["rollangakBroadcast", roomName, username, angka]);
          }
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
          if (roomName && roomList.includes(roomName)) {
            const success = this.setRoomMute(roomName, isMuted);
            const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
            this.safeSend(ws, ["muteTypeSet", muteValue, success, roomName]);
          } else {
            this.safeSend(ws, ["error", "Room tidak valid"]);
          }
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
            if (client?.idtarget === idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
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
            if (client?.idtarget === idt && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
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
        
        case "getOnlineUsers": {
          this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
          break;
        }
        
        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (roomList.includes(roomName)) {
            this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
          }
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
          
          if (ws.roomname !== roomname || ws.idtarget !== username || !roomList.includes(roomname)) return;
          
          // Quick primary check
          const userConnections = this.userConnections.get(username);
          if (userConnections?.size > 0) {
            let isPrimary = true;
            for (const conn of userConnections) {
              if (conn?.readyState === 1 && !conn._isClosing && conn !== ws) {
                isPrimary = false;
                break;
              }
            }
            if (!isPrimary) return;
          }
          
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          
          if (ws.roomname === room && roomList.includes(room) && seat >= 1 && seat <= this.MAX_SEATS) {
            this.savePointWithRetry(room, seat, x, y, fast).catch(() => {});
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          
          if (seat >= 1 && seat <= this.MAX_SEATS && ws.roomname === room && roomList.includes(room)) {
            await this.updateSeatAtomic(room, seat, () => createEmptySeat());
            this.clearSeatBuffer(room, seat);
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
          }
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          
          if (seat >= 1 && seat <= this.MAX_SEATS && ws.roomname === room && roomList.includes(room)) {
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
          }
          break;
        }
        
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          
          if (ws.roomname === roomname && ws.idtarget === sender && roomList.includes(roomname)) {
            this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, giftName, Date.now()]);
          }
          break;
        }
        
        case "leaveRoom": {
          const room = ws.roomname;
          if (room && roomList.includes(room)) {
            await this.cleanupFromRoom(ws, room);
            this.updateRoomCount(room);
            this.safeSend(ws, ["roomLeft", room]);
          }
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (this.lowcard && ["LowCard 1", "LowCard 2", "Noxxeliverothcifsa", "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love"].includes(ws.roomname)) {
            await this.lowcard.handleEvent(ws, data);
          }
          break;
      }
    } catch {
      if (ws.readyState === 1) {
        this.safeSend(ws, ["error", "Server error"]);
      }
    }
  }

  // ========== FETCH HANDLER ==========

  async fetch(request) {
    try {
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
      
      ws.addEventListener("message", (ev) => {
        this.handleMessage(ws, ev.data).catch(() => {});
      });
      
      ws.addEventListener("error", () => {});
      
      ws.addEventListener("close", () => {
        this.safeWebSocketCleanup(ws);
      });
      
      return new Response(null, { 
        status: 101, 
        webSocket: client 
      });
    } catch {
      return new Response("Internal server error", { status: 500 });
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      
      if (new URL(req.url).pathname === "/health") {
        return new Response("ok", { 
          status: 200, 
          headers: { 
            "content-type": "text/plain", 
            "cache-control": "no-cache" 
          } 
        });
      }
      
      return new Response("WebSocket endpoint", { 
        status: 200, 
        headers: { "content-type": "text/plain" } 
      });
    } catch {
      return new Response("Server error", { status: 500 });
    }
  }
};
