import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambayan", "Lounge Talk",
  "Noxxeliverothcifsa", "One Side Love", "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// OPTIMIZED CONSTANTS untuk Cloudflare Free Plan
const CONSTANTS = {
  // Memory optimization - dikurangi drastis
  MAX_SEATS: 35, // Dikurangi dari 35 untuk hemat memory
  MAX_CONNECTIONS_PER_USER: 2, // Dikurangi dari 5
  MAX_QUEUE_SIZE: 50, // Dikurangi dari 200
  MAX_LOCK_QUEUE_SIZE: 30, // Dikurangi dari 100
  
  // Point optimization
  MAX_POINTS_PER_ROOM: 50, // Dikurangi dari 200
  MAX_POINTS_TOTAL: 1000, // Dikurangi dari 5000
  MAX_POINTS_BEFORE_FLUSH: 200, // Dikurangi dari 1000
  POINT_BATCH_SIZE: 5, // Batch kecil untuk free plan
  POINT_FLUSH_DELAY: 50, // Delay lebih lama untuk mengurangi load
  
  // Buffer optimization
  BUFFER_SIZE_LIMIT: 20, // Dikurangi dari 50
  BROADCAST_BATCH_SIZE: 10, // Dikurangi dari 20
  
  // Timer intervals - ditingkatkan untuk mengurangi CPU
  MAIN_TIMER_INTERVAL: 100, // Dari 50 -> 100 (kurangi frekuensi)
  MEMORY_CLEANUP_INTERVAL: 60000, // 1 menit
  CLIENT_CLEANUP_INTERVAL: 60000, // 1 menit
  LOCK_CLEANUP_INTERVAL: 30000, // 30 detik
  RATE_LIMIT_CLEANUP_INTERVAL: 120000, // 2 menit
  SEAT_CHECK_INTERVAL: 120000, // 2 menit
  LOAD_CHECK_INTERVAL: 30000, // 30 detik
  
  // Timeouts
  LOCK_TIMEOUT: 8000, // Dikurangi dari 10000
  LOCK_ACQUIRE_TIMEOUT: 1500, // Dikurangi dari 2000
  GRACE_PERIOD: 3000, // Dikurangi dari 5000
  
  // Cache
  CACHE_VALID_DURATION: 5000, // Ditingkatkan dari 2000
  
  // Message limits
  MAX_MESSAGE_SIZE: 10000, // Dikurangi dari 50000
  MAX_BUFFERED_AMOUNT: 100000, // Dikurangi dari 500000
  MAX_ERROR_COUNT: 3,
  SAFE_SEND_RETRY: 1, // Dikurangi dari 2
  SAFE_SEND_RETRY_DELAY: 50, // Dikurangi dari 100
  
  // Load thresholds - lebih sensitif
  LOAD_THRESHOLD: 0.75, // Dikurangi dari 0.9
  LOAD_RECOVERY_THRESHOLD: 0.5, // Dikurangi dari 0.7
  
  // Memory limits untuk Cloudflare
  MAX_HEAP_MB: 128, // Limit heap memory
  MAX_CONCURRENT_JOBS: 2 // Dikurangi dari 5
};

// Optimized PromiseLockManager dengan cleanup otomatis
class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
    this.lockTimestamps = new Map();
    this._cleanupInterval = setInterval(() => this.cleanupStuckLocks(), CONSTANTS.LOCK_CLEANUP_INTERVAL);
  }

  async acquire(resourceId) {
    if (this.locks.has(resourceId)) {
      const lockTime = this.lockTimestamps.get(resourceId) || 0;
      if (Date.now() - lockTime > CONSTANTS.LOCK_TIMEOUT) {
        this.forceRelease(resourceId);
      }
    }

    const currentQueue = this.queue.get(resourceId) || [];
    if (currentQueue.length > CONSTANTS.MAX_LOCK_QUEUE_SIZE) {
      throw new Error(`Lock queue full: ${resourceId}`);
    }

    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, true);
      this.lockTimestamps.set(resourceId, Date.now());
      return () => this.release(resourceId);
    }

    if (!this.queue.has(resourceId)) {
      this.queue.set(resourceId, []);
    }

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const idx = this.queue.get(resourceId)?.indexOf(resolve);
        if (idx !== -1 && idx !== undefined) {
          this.queue.get(resourceId).splice(idx, 1);
          reject(new Error(`Lock timeout: ${resourceId}`));
        }
      }, CONSTANTS.LOCK_TIMEOUT);

      this.queue.get(resourceId).push({ resolve, timeoutId });
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
        item.reject?.(new Error(`Lock released: ${resourceId}`));
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

  destroy() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    for (const resourceId of this.queue.keys()) {
      this.forceRelease(resourceId);
    }
  }
}

// Optimized QueueManager dengan limit ketat
class QueueManager {
  constructor(concurrency = CONSTANTS.MAX_CONCURRENT_JOBS) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = CONSTANTS.MAX_QUEUE_SIZE;
    this.processing = false;
  }

  async add(job) {
    if (this.queue.length >= this.maxQueueSize) {
      throw new Error("Server busy");
    }

    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject, timestamp: Date.now() });
      if (!this.processing) this.process();
    });
  }

  async process() {
    if (this.processing) return;
    this.processing = true;

    while (this.queue.length > 0 && this.active < this.concurrency) {
      while (this.queue.length > 0 && Date.now() - this.queue[0].timestamp > 20000) {
        this.queue.shift().reject(new Error("Timeout"));
      }
      
      if (this.queue.length === 0) break;
      
      this.active++;
      const { job, resolve, reject } = this.queue.shift();
      
      try {
        const result = await Promise.race([
          job(),
          new Promise((_, reject) => setTimeout(() => reject(new Error("Job timeout")), 8000))
        ]);
        resolve(result);
      } catch (error) {
        reject(error);
      } finally {
        this.active--;
      }
    }

    this.processing = false;
    if (this.queue.length > 0) setTimeout(() => this.process(), 100);
  }

  size() { return this.queue.length; }
}

// Optimized RateLimiter
class RateLimiter {
  constructor(windowMs = 60000, maxRequests = 50) { // Dikurangi dari 100
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this._cleanupInterval = setInterval(() => this.cleanup(), CONSTANTS.RATE_LIMIT_CLEANUP_INTERVAL);
  }

  check(userId) {
    if (!userId) return true;
    
    const now = Date.now();
    let requests = this.requests.get(userId);
    
    if (!requests) {
      this.requests.set(userId, [now]);
      return true;
    }
    
    while (requests.length > 0 && now - requests[0] >= this.windowMs) {
      requests.shift();
    }
    
    if (requests.length >= this.maxRequests) return false;
    
    requests.push(now);
    return true;
  }

  cleanup() {
    const now = Date.now();
    for (const [userId, requests] of this.requests) {
      while (requests.length > 0 && now - requests[0] >= this.windowMs) {
        requests.shift();
      }
      if (requests.length === 0) this.requests.delete(userId);
    }
  }

  destroy() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this.requests.clear();
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
      
      // Initialize maps dengan size yang lebih kecil
      this.muteStatus = new Map();
      for (const room of roomList) this.muteStatus.set(room, false);
      
      this.storage = state?.storage;
      
      // Core structures
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

      // Point buffer
      this._pointBuffer = new Map();
      this._pointFlushTimer = null;
      this._pointFlushDelay = CONSTANTS.POINT_FLUSH_DELAY;

      // Rate limiters
      this.rateLimiter = new RateLimiter(60000, 50);
      this.connectionRateLimiter = new RateLimiter(10000, 2);
      
      // Safe mode
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
      this.lastLoadCheck = 0;
      
      // Game
      try {
        this.lowcard = new LowCardGameManager(this);
      } catch {
        this.lowcard = null;
      }

      // Disconnect handling
      this.gracePeriod = CONSTANTS.GRACE_PERIOD;
      this.disconnectedTimers = new Map();
      this.cleanupQueue = new QueueManager(CONSTANTS.MAX_CONCURRENT_JOBS);
      
      // Number ticker
      this.currentNumber = 1;
      this.maxNumber = 6;
      this._nextConnId = 1;

      // Main timer
      this.mainTimer = null;
      this.tickCounter = 0;
      this.lastNumberTick = Date.now();

      // Initialize rooms
      this.initializeRooms();

      // Start timers
      this.startMainTimer();
      
      // Cache
      this.roomCountsCache = null;
      this.cacheValidDuration = CONSTANTS.CACHE_VALID_DURATION;
      this.lastCacheUpdate = 0;

      // Initialize buffers
      for (const room of roomList) {
        this._pointBuffer.set(room, []);
        this.updateKursiBuffer.set(room, new Map());
      }

      // Cleanup timestamps
      this.lastMemoryCleanup = Date.now();
      this.lastClientCleanup = Date.now();
      this.lastLockCleanup = Date.now();
      this.lastRateLimitCleanup = Date.now();
      this.lastSeatCheck = Date.now();

      // Memory monitor
      this._memoryCheckInterval = setInterval(() => this.checkMemory(), 300000); // 5 menit

    } catch (error) {
      console.error("Constructor error:", error);
      this.initializeFallback();
    }
  }

  checkMemory() {
    if (global.gc) {
      try {
        global.gc();
      } catch {}
    }
    
    // Clear cache jika terlalu besar
    if (this.roomCountsCache && Date.now() - this.lastCacheUpdate > 60000) {
      this.roomCountsCache = null;
    }
    
    // Limit buffer size
    for (const [room, points] of this._pointBuffer) {
      if (points.length > CONSTANTS.MAX_POINTS_PER_ROOM) {
        this._pointBuffer.set(room, points.slice(-CONSTANTS.MAX_POINTS_PER_ROOM));
      }
    }
    
    for (const [room, buffer] of this.updateKursiBuffer) {
      if (buffer.size > CONSTANTS.BUFFER_SIZE_LIMIT) {
        const newBuffer = new Map();
        let count = 0;
        for (const [key, value] of buffer) {
          if (count < CONSTANTS.BUFFER_SIZE_LIMIT) {
            newBuffer.set(key, value);
            count++;
          }
        }
        this.updateKursiBuffer.set(room, newBuffer);
      }
    }
  }

  initializeFallback() {
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
    this.cleanupQueue = new QueueManager(CONSTANTS.MAX_CONCURRENT_JOBS);
    
    this.muteStatus = new Map();
    for (const room of roomList) this.muteStatus.set(room, false);
    
    this.rateLimiter = new RateLimiter(60000, 50);
    this.connectionRateLimiter = new RateLimiter(10000, 2);
    this.safeMode = false;
    
    this._pointBuffer = new Map();
    this._pointFlushDelay = CONSTANTS.POINT_FLUSH_DELAY;
    
    this.createDefaultRoom();
    this.startMainTimer();
    
    this._memoryCheckInterval = setInterval(() => this.checkMemory(), 300000);
  }

  startMainTimer() {
    if (this.mainTimer) clearInterval(this.mainTimer);
    this.mainTimer = setInterval(() => {
      this.runMainTasks().catch(() => {});
    }, CONSTANTS.MAIN_TIMER_INTERVAL);
  }

  async runMainTasks() {
    const now = Date.now();
    
    // Flush setiap tick
    this.flushKursiUpdates();
    this.flushBufferedPoints();
    
    // Periodic tasks dengan interval lebih jarang
    if (this.tickCounter % 5 === 0) {
      if (now - this.lastLoadCheck >= CONSTANTS.LOAD_CHECK_INTERVAL) {
        this.checkAndEnableSafeMode();
        this.lastLoadCheck = now;
      }
    }
    
    if (now - this.lastLockCleanup >= CONSTANTS.LOCK_CLEANUP_INTERVAL) {
      this.lockManager?.cleanupStuckLocks();
      this.lastLockCleanup = now;
    }
    
    if (now - this.lastClientCleanup >= CONSTANTS.CLIENT_CLEANUP_INTERVAL) {
      await this.cleanupDuplicateConnections();
      this.lastClientCleanup = now;
    }
    
    if (now - this.lastMemoryCleanup >= CONSTANTS.MEMORY_CLEANUP_INTERVAL) {
      await this.performMemoryCleanup();
      this.lastMemoryCleanup = now;
    }
    
    if (now - this.lastRateLimitCleanup >= CONSTANTS.RATE_LIMIT_CLEANUP_INTERVAL) {
      this.rateLimiter.cleanup();
      this.connectionRateLimiter.cleanup();
      this.lastRateLimitCleanup = now;
    }
    
    if (now - this.lastSeatCheck >= CONSTANTS.SEAT_CHECK_INTERVAL && this.getServerLoad() < 0.6) {
      this.sampledSeatConsistencyCheck();
      this.lastSeatCheck = now;
    }
    
    if (now - this.lastNumberTick >= 900000) { // 15 menit
      this.tick();
      this.lastNumberTick = now;
    }
    
    this.tickCounter = (this.tickCounter + 1) % 200;
  }

  async performMemoryCleanup() {
    try {
      // Cleanup dead clients
      let changed = false;
      for (const client of this.clients) {
        if (!client || client.readyState === 3) {
          this.clients.delete(client);
          changed = true;
        }
      }

      // Cleanup room clients
      for (const [room, clients] of this.roomClients) {
        const filtered = clients.filter(c => c && c.readyState === 1);
        if (filtered.length !== clients.length) {
          this.roomClients.set(room, filtered);
          changed = true;
        }
      }

      // Cleanup user connections
      for (const [userId, connections] of this.userConnections) {
        const active = new Set();
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            active.add(conn);
          }
        }
        if (active.size === 0) {
          this.userConnections.delete(userId);
          changed = true;
        } else if (active.size !== connections.size) {
          this.userConnections.set(userId, active);
          changed = true;
        }
      }

      // Cleanup timers
      const now = Date.now();
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer._scheduledTime && now - timer._scheduledTime > this.gracePeriod + 5000) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
          changed = true;
        }
      }

      if (changed && global.gc) {
        try { global.gc(); } catch {}
      }

    } catch (error) {
      // Silent error
    }
  }

  getServerLoad() {
    const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
    const queueSize = this.cleanupQueue?.size() || 0;
    return Math.min(activeConnections / 150 + queueSize / 100, 0.9);
  }

  checkAndEnableSafeMode() {
    const load = this.getServerLoad();
    if (load > this.loadThreshold && !this.safeMode) {
      this.safeMode = true;
      this._pointFlushDelay = 100;
      this.cleanupQueue.concurrency = 1;
    } else if (load < CONSTANTS.LOAD_RECOVERY_THRESHOLD && this.safeMode) {
      this.safeMode = false;
      this._pointFlushDelay = CONSTANTS.POINT_FLUSH_DELAY;
      this.cleanupQueue.concurrency = CONSTANTS.MAX_CONCURRENT_JOBS;
    }
  }

  // ========== POINT MANAGEMENT ==========
  
  flushBufferedPoints() {
    for (const [room, points] of this._pointBuffer) {
      if (points.length > 0) {
        const batch = points.splice(0, CONSTANTS.POINT_BATCH_SIZE);
        if (batch.length > 0) {
          this.broadcastPointsBatch(room, batch);
        }
      }
    }
  }

  broadcastPointsBatch(room, batch) {
    if (!room || !roomList.includes(room)) return;
    
    const clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return;
    
    const message = JSON.stringify(["pointsBatch", room, batch]);
    
    for (const client of clientArray) {
      if (client?.readyState === 1 && client.roomname === room) {
        try { client.send(message); } catch {}
      }
    }
  }

  broadcastPointDirect(room, seat, x, y, fast) {
    if (!room || !roomList.includes(room)) return;
    if (seat < 1 || seat > this.MAX_SEATS) return;
    
    const clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return;
    
    const message = JSON.stringify(["pointUpdated", room, seat, x, y, fast]);
    
    for (const client of clientArray) {
      if (client?.readyState === 1 && client.roomname === room) {
        try { client.send(message); } catch {}
      }
    }
  }

  async savePointWithRetry(room, seat, x, y, fast) {
    try {
      if (seat < 1 || seat > this.MAX_SEATS) return false;
      
      const xNum = typeof x === 'number' ? x : parseFloat(x);
      const yNum = typeof y === 'number' ? y : parseFloat(y);
      if (isNaN(xNum) || isNaN(yNum)) return false;
      
      await this.updateSeatAtomic(room, seat, (currentSeat) => {
        currentSeat.lastPoint = { x: xNum, y: yNum, fast: fast || false, timestamp: Date.now() };
        return currentSeat;
      });
      
      const points = this._pointBuffer.get(room);
      if (points) {
        points.push({ seat, x: xNum, y: yNum, fast: fast || false });
        if (points.length > CONSTANTS.MAX_POINTS_PER_ROOM) points.shift();
      }
      
      this.broadcastPointDirect(room, seat, xNum, yNum, fast);
      return true;
      
    } catch {
      this.broadcastPointDirect(room, seat, x, y, fast);
      return false;
    }
  }

  // ========== SEAT MANAGEMENT ==========
  
  initializeRooms() {
    for (const room of roomList) {
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
    }
  }

  createDefaultRoom() {
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
  }

  async updateSeatAtomic(room, seatNumber, updateFn) {
    if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return null;
    
    return await this.withLock(`seat-${room}-${seatNumber}`, () => {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
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
      const buffer = this.updateKursiBuffer.get(room);
      if (buffer) {
        if (updatedSeat.namauser) {
          if (buffer.size >= this.bufferSizeLimit) {
            const firstKey = buffer.keys().next().value;
            if (firstKey) buffer.delete(firstKey);
          }
          buffer.set(seatNumber, {
            noimageUrl: updatedSeat.noimageUrl || "",
            namauser: updatedSeat.namauser,
            color: updatedSeat.color || "",
            itembawah: updatedSeat.itembawah || 0,
            itematas: updatedSeat.itematas || 0,
            vip: updatedSeat.vip || 0,
            viptanda: updatedSeat.viptanda || 0
          });
        } else {
          buffer.delete(seatNumber);
        }
      }
      
      return updatedSeat;
    });
  }

  flushKursiUpdates() {
    for (const [room, buffer] of this.updateKursiBuffer) {
      if (!room || !roomList.includes(room)) continue;
      if (!buffer || buffer.size === 0) continue;
      
      const updates = [];
      for (const [seat, info] of buffer) {
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
      
      buffer.clear();
    }
  }

  // ========== CONNECTION & CLEANUP ==========
  
  async withLock(resourceId, operation, timeout = CONSTANTS.LOCK_ACQUIRE_TIMEOUT) {
    let release;
    try {
      release = await this.lockManager.acquire(resourceId);
      return await Promise.race([
        operation(),
        new Promise((_, reject) => setTimeout(() => reject(new Error(`Timeout: ${resourceId}`)), timeout))
      ]);
    } finally {
      if (release) try { release(); } catch {}
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
    } catch {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    const clients = this.roomClients.get(room);
    if (!clients?.length) return 0;
    
    const message = JSON.stringify(msg);
    let count = 0;
    const seen = new Set();
    
    for (const client of clients) {
      if (client?.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
        if (client.idtarget && seen.has(client.idtarget)) continue;
        try {
          client.send(message);
          count++;
          if (client.idtarget) seen.add(client.idtarget);
        } catch {}
      }
    }
    return count;
  }

  updateRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return 0;
    
    let count = 0;
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (seatMap.get(i)?.namauser) count++;
    }
    
    this.roomCountsCache = null;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }

  getJumlahRoom() {
    const now = Date.now();
    if (this.roomCountsCache && now - this.lastCacheUpdate < this.cacheValidDuration) {
      return this.roomCountsCache;
    }
    
    const counts = {};
    for (const room of roomList) counts[room] = 0;
    
    for (const room of roomList) {
      const occupancy = this.seatOccupancy.get(room);
      if (occupancy) {
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          if (occupancy.get(i)) counts[room]++;
        }
      }
    }
    
    this.roomCountsCache = counts;
    this.lastCacheUpdate = now;
    return counts;
  }

  async isUserStillConnected(userId) {
    if (!userId) return false;
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return false;
    
    for (const conn of connections) {
      if (conn && conn.readyState === 1 && !conn._isDuplicate && !conn._isClosing) {
        return true;
      }
    }
    return false;
  }

  scheduleCleanup(userId) {
    if (!userId) return;
    this.cancelCleanup(userId);
    
    const timerId = setTimeout(async () => {
      this.disconnectedTimers.delete(userId);
      if (!await this.isUserStillConnected(userId)) {
        await this.forceUserCleanup(userId);
      }
    }, this.gracePeriod);
    
    timerId._scheduledTime = Date.now();
    this.disconnectedTimers.set(userId, timerId);
  }

  cancelCleanup(userId) {
    const timer = this.disconnectedTimers.get(userId);
    if (timer) {
      clearTimeout(timer);
      this.disconnectedTimers.delete(userId);
    }
    this.cleanupInProgress?.delete(userId);
  }

  async forceUserCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    this.cleanupInProgress.add(userId);
    
    try {
      await this.withLock(`cleanup-${userId}`, async () => {
        this.cancelCleanup(userId);
        
        // Find and cleanup all seats
        for (const room of roomList) {
          const seatMap = this.roomSeats.get(room);
          if (!seatMap) continue;
          
          for (let i = 1; i <= this.MAX_SEATS; i++) {
            const seat = seatMap.get(i);
            if (seat?.namauser === userId) {
              Object.assign(seat, createEmptySeat());
              const occupancy = this.seatOccupancy.get(room);
              if (occupancy) occupancy.set(i, null);
              this.updateKursiBuffer.get(room)?.delete(i);
              this.broadcastToRoom(room, ["removeKursi", room, i]);
            }
          }
          this.updateRoomCount(room);
        }
        
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this.userConnections.delete(userId);
        
        // Remove from room clients
        for (const [room, clients] of this.roomClients) {
          const filtered = clients.filter(c => c?.idtarget !== userId);
          if (filtered.length !== clients.length) {
            this.roomClients.set(room, filtered);
          }
        }
      });
    } finally {
      this.cleanupInProgress.delete(userId);
    }
  }

  async cleanupDuplicateConnections() {
    const userCount = new Map();
    for (const client of this.clients) {
      if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
        userCount.set(client.idtarget, (userCount.get(client.idtarget) || 0) + 1);
      }
    }
    
    for (const [userId, count] of userCount) {
      if (count > 1) await this.handleDuplicateConnections(userId);
    }
  }

  async handleDuplicateConnections(userId) {
    await this.withLock(`dup-${userId}`, async () => {
      const connections = [];
      for (const client of this.clients) {
        if (client?.idtarget === userId && client.readyState === 1 && !client._isClosing) {
          connections.push({ client, time: client._connectionTime || 0 });
        }
      }
      
      if (connections.length <= 1) return;
      
      connections.sort((a, b) => b.time - a.time);
      const toClose = connections.slice(1);
      
      for (const { client } of toClose) {
        client._isDuplicate = true;
        client._isClosing = true;
        try {
          if (client.readyState === 1) {
            this.safeSend(client, ["duplicateConnection", "Another connection opened"]);
            client.close(1000, "Duplicate");
          }
        } catch {}
        this.clients.delete(client);
        if (client.roomname) this._removeFromRoomClients(client, client.roomname);
        this._removeUserConnection(userId, client);
      }
      
      const remaining = new Set();
      for (const client of this.clients) {
        if (client?.idtarget === userId && client.readyState === 1) {
          remaining.add(client);
        }
      }
      this.userConnections.set(userId, remaining);
    });
  }

  _addUserConnection(userId, ws) {
    if (!userId || !ws) return;
    let connections = this.userConnections.get(userId);
    if (!connections) {
      connections = new Set();
      this.userConnections.set(userId, connections);
    }
    connections.add(ws);
  }

  _removeUserConnection(userId, ws) {
    const connections = this.userConnections.get(userId);
    if (connections) {
      connections.delete(ws);
      if (connections.size === 0) this.userConnections.delete(userId);
    }
  }

  _removeFromRoomClients(ws, room) {
    const clients = this.roomClients.get(room);
    if (clients) {
      const idx = clients.indexOf(ws);
      if (idx > -1) clients.splice(idx, 1);
    }
  }

  sampledSeatConsistencyCheck() {
    const samples = [];
    for (let i = 0; i < 2; i++) {
      samples.push(roomList[Math.floor(Math.random() * roomList.length)]);
    }
    for (const room of samples) {
      if (this.getServerLoad() < 0.6) this.validateSeatConsistency(room);
    }
  }

  async validateSeatConsistency(room) {
    const seatMap = this.roomSeats.get(room);
    const occupancyMap = this.seatOccupancy.get(room);
    if (!seatMap || !occupancyMap) return;
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const occupant = occupancyMap.get(seat);
      const seatData = seatMap.get(seat);
      
      if (occupant && (!seatData || !seatData.namauser)) {
        if (!seatData) seatMap.set(seat, createEmptySeat());
        else Object.assign(seatData, createEmptySeat());
        occupancyMap.set(seat, null);
      } else if (!occupant && seatData?.namauser) {
        if (await this.isUserStillConnected(seatData.namauser)) {
          occupancyMap.set(seat, seatData.namauser);
        } else {
          Object.assign(seatData, createEmptySeat());
        }
      }
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
      
      if (room) this._removeFromRoomClients(ws, room);
      if (ws.readyState === 1) try { ws.close(1000, "Normal"); } catch {}
    } catch {
      this.clients.delete(ws);
      if (userId) this.cancelCleanup(userId);
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    const message = JSON.stringify(["currentNumber", this.currentNumber]);
    const notified = new Set();
    
    for (const client of this.clients) {
      if (client?.readyState === 1 && client.roomname && !client._isDuplicate && !client._isClosing) {
        if (client.idtarget && !notified.has(client.idtarget)) {
          try { client.send(message); } catch {}
          if (client.idtarget) notified.add(client.idtarget);
        }
      }
    }
  }

  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) {
      this.safeSend(ws, ["error", "User ID required"]);
      return false;
    }
    if (!roomList.includes(room)) {
      this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    const release = await this.lockManager.acquire(`join-${room}`);
    try {
      this.cancelCleanup(ws.idtarget);
      
      const prevRoom = this.userCurrentRoom.get(ws.idtarget);
      if (prevRoom === room) {
        this.sendAllStateTo(ws, room);
        return true;
      }
      if (prevRoom) await this.cleanupFromRoom(ws, prevRoom);
      
      // Find empty seat
      let assignedSeat = null;
      const occupancy = this.seatOccupancy.get(room);
      if (!occupancy) return false;
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (occupancy.get(seat) === null) {
          assignedSeat = seat;
          break;
        }
      }
      
      if (!assignedSeat) {
        this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      occupancy.set(assignedSeat, ws.idtarget);
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      ws.numkursi = new Set([assignedSeat]);
      
      let clients = this.roomClients.get(room);
      if (!clients) {
        clients = [];
        this.roomClients.set(room, clients);
      }
      if (!clients.includes(ws)) clients.push(ws);
      
      this._addUserConnection(ws.idtarget, ws);
      this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
      
      setTimeout(() => this.sendAllStateTo(ws, room), 100);
      this.updateRoomCount(room);
      return true;
    } finally {
      release();
    }
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    
    const release = await this.lockManager.acquire(`reconnect-${id}`);
    try {
      this.cancelCleanup(id);
      
      if (baru === true) {
        await this.cleanupQueue.add(async () => {
          await this.forceUserCleanup(id);
        });
        ws.idtarget = id;
        ws._connectionTime = Date.now();
        this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._connectionTime = Date.now();
      
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        if (seat >= 1 && seat <= this.MAX_SEATS) {
          const seatMap = this.roomSeats.get(room);
          const occupancy = this.seatOccupancy.get(room);
          if (seatMap && occupancy && seatMap.get(seat)?.namauser === id && occupancy.get(seat) === id) {
            ws.roomname = room;
            ws.numkursi = new Set([seat]);
            
            let clients = this.roomClients.get(room);
            if (!clients) {
              clients = [];
              this.roomClients.set(room, clients);
            }
            if (!clients.includes(ws)) clients.push(ws);
            
            this._addUserConnection(id, ws);
            this.sendAllStateTo(ws, room);
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
    } finally {
      release();
    }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget) return;
    
    const release = await this.lockManager.acquire(`cleanup-${room}-${ws.idtarget}`);
    try {
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
    } finally {
      release();
    }
  }

  async cleanupUserFromSeat(room, seat, userId, immediate = true) {
    if (seat < 1 || seat > this.MAX_SEATS) return;
    
    const release = await this.lockManager.acquire(`seat-${room}-${seat}`);
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancy = this.seatOccupancy.get(room);
      if (!seatMap || !occupancy) return;
      
      const seatInfo = seatMap.get(seat);
      if (!seatInfo || seatInfo.namauser !== userId) return;
      
      if (immediate) {
        Object.assign(seatInfo, createEmptySeat());
        occupancy.set(seat, null);
        this.updateKursiBuffer.get(room)?.delete(seat);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.updateRoomCount(room);
        this.userToSeat.delete(userId);
      }
    } finally {
      release();
    }
  }

  sendAllStateTo(ws, room) {
    if (!ws || ws.readyState !== 1 || ws.roomname !== room) return;
    
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;
    
    const kursi = {};
    const points = [];
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info?.namauser) {
        kursi[seat] = {
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
        points.push({ seat, x: info.lastPoint.x, y: info.lastPoint.y, fast: info.lastPoint.fast });
      }
    }
    
    if (Object.keys(kursi).length) this.safeSend(ws, ["allUpdateKursiList", room, kursi]);
    if (points.length) this.safeSend(ws, ["allPointsList", room, points]);
    
    const counts = this.getJumlahRoom();
    this.safeSend(ws, ["roomUserCount", room, counts[room] || 0]);
    this.safeSend(ws, ["currentNumber", this.currentNumber]);
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing) return;
    if (ws.idtarget && !this.rateLimiter.check(ws.idtarget)) {
      this.safeSend(ws, ["error", "Rate limit"]);
      return;
    }
    
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
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
          
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
          
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname === room && roomList.includes(room) && seat >= 1 && seat <= this.MAX_SEATS) {
            await this.savePointWithRetry(room, seat, x, y, fast);
          }
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (ws.roomname === room && roomList.includes(room) && seat >= 1 && seat <= this.MAX_SEATS) {
            await this.updateSeatAtomic(room, seat, () => ({
              noimageUrl, namauser, color,
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
        
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (ws.roomname === room && roomList.includes(room) && seat >= 1 && seat <= this.MAX_SEATS) {
            await this.updateSeatAtomic(room, seat, () => createEmptySeat());
            this.updateKursiBuffer.get(room)?.delete(seat);
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
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
        
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (ws.roomname === roomname && ws.idtarget === username && roomList.includes(roomname)) {
            // Check primary connection
            let isPrimary = true;
            const connections = this.userConnections.get(username);
            if (connections?.size > 0) {
              let earliest = null;
              for (const conn of connections) {
                if (conn?.readyState === 1 && !conn._isClosing) {
                  if (!earliest || (conn._connectionTime || 0) < (earliest._connectionTime || 0)) {
                    earliest = conn;
                  }
                }
              }
              if (earliest && earliest !== ws) isPrimary = false;
            }
            if (isPrimary) {
              this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
            }
          }
          break;
        }
        
        case "setMuteType": {
          const isMuted = data[1];
          const roomName = data[2];
          if (roomName && roomList.includes(roomName)) {
            this.setRoomMute(roomName, isMuted);
            this.safeSend(ws, ["muteTypeSet", isMuted, true, roomName]);
          }
          break;
        }
        
        case "getMuteType": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName)) {
            this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(roomName), roomName]);
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = this.getJumlahRoom();
          const result = roomList.map(room => [room, counts[room]]);
          this.safeSend(ws, ["allRoomsUserCount", result]);
          break;
        }
        
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "onDestroy":
          this.handleOnDestroy(ws, ws.idtarget);
          break;
          
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
          const out = ["private", idt, url, msg, Date.now(), sender];
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
          const isOnline = await this.isUserStillConnected(username);
          this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] || ""]);
          break;
        }
        
        case "rollangak": {
          const roomName = data[1];
          if (roomList.includes(roomName)) {
            this.broadcastToRoom(roomName, ["rollangakBroadcast", roomName, data[2], data[3]]);
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
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (this.lowcard && ["LowCard 1", "LowCard 2", "Noxxeliverothcifsa", "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love"].includes(ws.roomname)) {
            await this.lowcard.handleEvent(ws, data);
          }
          break;
      }
    } catch (error) {
      if (ws.readyState === 1) {
        this.safeSend(ws, ["error", "Server error"]);
      }
    }
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget || !ws) return;
    
    if (ws.isManualDestroy) {
      this.cleanupQueue.add(async () => {
        await this.fullRemoveById(idtarget);
      });
    } else {
      const seatInfo = this.userToSeat.get(idtarget);
      if (seatInfo) {
        this.cleanupQueue.add(async () => {
          await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, idtarget, true);
        });
      }
      this.userToSeat.delete(idtarget);
      this.userCurrentRoom.delete(idtarget);
    }
    
    this.cancelCleanup(idtarget);
    this._removeUserConnection(idtarget, ws);
    this._removeFromRoomClients(ws, ws.roomname);
    this.clients.delete(ws);
    if (ws.readyState === 1) try { ws.close(1000, "Destroy"); } catch {}
  }

  async fullRemoveById(idtarget) {
    if (!idtarget) return;
    
    const release = await this.lockManager.acquire(`full-${idtarget}`);
    try {
      this.cancelCleanup(idtarget);
      
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (seatMap) {
          for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
            const info = seatMap.get(seat);
            if (info?.namauser === idtarget) {
              Object.assign(info, createEmptySeat());
              this.updateKursiBuffer.get(room)?.delete(seat);
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
            }
          }
          this.updateRoomCount(room);
        }
      }
      
      this.userToSeat.delete(idtarget);
      this.userCurrentRoom.delete(idtarget);
      this.userConnections.delete(idtarget);
      
      const toRemove = [];
      for (const client of this.clients) {
        if (client?.idtarget === idtarget) toRemove.push(client);
      }
      for (const client of toRemove) {
        this.clients.delete(client);
        if (client.roomname) this._removeFromRoomClients(client, client.roomname);
        if (client.readyState === 1) try { client.close(1000, "Removed"); } catch {}
      }
    } finally {
      release();
    }
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket") {
        return new Response("WebSocket only", { status: 426 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      await server.accept();
      
      const ws = server;
      ws._connId = `c${this._nextConnId++}`;
      ws.roomname = null;
      ws.idtarget = null;
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
      
      ws.addEventListener("close", () => {
        this.safeWebSocketCleanup(ws).catch(() => {});
      });
      
      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }

  destroy() {
    if (this.mainTimer) clearInterval(this.mainTimer);
    if (this._memoryCheckInterval) clearInterval(this._memoryCheckInterval);
    if (this._pointFlushTimer) clearTimeout(this._pointFlushTimer);
    this.lockManager?.destroy();
    this.rateLimiter?.destroy();
    this.connectionRateLimiter?.destroy();
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
        return new Response("ok", { headers: { "content-type": "text/plain", "cache-control": "no-cache" } });
      }
      
      return new Response("WebSocket endpoint");
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
};
