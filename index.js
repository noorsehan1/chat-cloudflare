import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambayan", "Lounge Talk",
  "Noxxeliverothcifsa", "One Side Love", "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// ========== CONSTANTS ==========
const CONSTANTS = {
  LOCK_TIMEOUT: 15000,
  LOCK_ACQUIRE_TIMEOUT: 3000,
  MAX_QUEUE_SIZE: 100,
  MAX_LOCK_QUEUE_SIZE: 50,
  GRACE_PERIOD: 5000,
  CACHE_VALID_DURATION: 5000,
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 1,
  SAFE_SEND_RETRY: 3,
  SAFE_SEND_RETRY_DELAY: 100,
  BROADCAST_BATCH_SIZE: 20,
  MAX_MESSAGE_SIZE: 100000,
  MAX_ERROR_COUNT: 5,
  MAX_BUFFERED_AMOUNT: 300000,
  LOAD_THRESHOLD: 0.95,
  LOAD_RECOVERY_THRESHOLD: 0.8,
  MEMORY_WARNING_THRESHOLD: 300,
  MEMORY_CRITICAL_THRESHOLD: 400,
  MAX_TOTAL_USERS: 300,
  MAX_TOTAL_CLIENTS: 500,
  MAX_DISCONNECTED_TIMERS: 300,
  MAX_QUEUE_SIZE_EMERGENCY: 150,
  EMERGENCY_CLEANUP_BATCH: 20,
  AUTO_CLEANUP_INTERVAL: 300000,
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  MAX_NUMBER: 6
};

class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
    this.lockTimestamps = new Map();
  }

  async acquire(resourceId) {
    if (this.locks.has(resourceId)) {
      const lockTime = this.lockTimestamps.get(resourceId) || 0;
      if (Date.now() - lockTime > CONSTANTS.LOCK_TIMEOUT) this.forceRelease(resourceId);
    }

    const currentQueue = this.queue.get(resourceId) || [];
    if (currentQueue.length > CONSTANTS.MAX_LOCK_QUEUE_SIZE) {
      throw new Error(`Too many waiting for lock: ${resourceId}`);
    }

    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, true);
      this.lockTimestamps.set(resourceId, Date.now());
      return () => this.release(resourceId);
    }

    if (!this.queue.has(resourceId)) this.queue.set(resourceId, []);

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const idx = this.queue.get(resourceId)?.indexOf(resolve);
        if (idx !== -1 && idx !== undefined) {
          this.queue.get(resourceId).splice(idx, 1);
          reject(new Error(`Lock queue timeout for ${resourceId}`));
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
      if (queue.length === 0) this.queue.delete(resourceId);
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
        item.reject?.(new Error(`Lock force released: ${resourceId}`));
      }
      this.queue.delete(resourceId);
    }
  }

  cleanupStuckLocks() {
    const now = Date.now();
    for (const [resourceId, lockTime] of this.lockTimestamps) {
      if (now - lockTime > CONSTANTS.LOCK_TIMEOUT) this.forceRelease(resourceId);
    }
  }
}

class QueueManager {
  constructor(concurrency = 3) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = CONSTANTS.MAX_QUEUE_SIZE;
    this.processing = false;
    this.maxJobTime = 30000;
  }

  async add(job) {
    if (this.queue.length > this.maxQueueSize) throw new Error("Server busy");
    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject, timestamp: Date.now() });
      if (!this.processing) this.process();
    });
  }

  async process() {
    if (this.processing) return;
    this.processing = true;
    while (this.queue.length > 0 && this.active < this.concurrency) {
      while (this.queue.length > 0 && Date.now() - this.queue[0].timestamp > 30000) {
        const expired = this.queue.shift();
        expired.reject(new Error("Request timeout"));
      }
      if (this.queue.length === 0) break;
      this.active++;
      const { job, resolve, reject } = this.queue.shift();
      try {
        const result = await Promise.race([
          job(),
          new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 10000))
        ]);
        resolve(result);
      } catch (error) {
        reject(error);
      } finally {
        this.active--;
      }
    }
    this.processing = false;
    if (this.queue.length > 0) setTimeout(() => this.process(), 10);
  }

  clear() {
    const oldQueue = this.queue;
    this.queue = [];
    for (const item of oldQueue) item.reject(new Error("Queue cleared"));
  }

  cleanupStuckJobs() {
    const now = Date.now();
    const toRemove = [];
    for (let i = 0; i < this.queue.length; i++) {
      if (now - this.queue[i].timestamp > this.maxJobTime) {
        toRemove.push(i);
      }
    }
    for (let i = toRemove.length - 1; i >= 0; i--) {
      const job = this.queue[toRemove[i]];
      job.reject(new Error("Job timeout"));
      this.queue.splice(toRemove[i], 1);
    }
  }

  size() { return this.queue.length; }
}

class RateLimiter {
  constructor(windowMs = 60000, maxRequests = 100) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this.maxSize = 1000;
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
    
    if (this.requests.size > this.maxSize) {
      const toDelete = Array.from(this.requests.keys()).slice(0, this.requests.size - this.maxSize);
      for (const userId of toDelete) {
        this.requests.delete(userId);
      }
    }
  }
}

function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "", itembawah: 0, itematas: 0,
    vip: 0, viptanda: 0, lastPoint: null, lastUpdated: Date.now()
  };
}

export class ChatServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      
      this.muteStatus = new Map();
      for (const room of roomList) this.muteStatus.set(room, false);
      
      this.storage = state?.storage;
      
      // Core data
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
      
      // Rate limiters
      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      
      this.safeMode = false;
      this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
      
      try { this.lowcard = new LowCardGameManager(this); } catch { this.lowcard = null; }
      
      this.gracePeriod = CONSTANTS.GRACE_PERIOD;
      this.disconnectedTimers = new Map();
      this.cleanupQueue = new QueueManager(3);
      
      this.currentNumber = 1;
      this.maxNumber = CONSTANTS.MAX_NUMBER;
      this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
      this._nextConnId = 1;
      this.lastNumberTick = Date.now();
      
      this.numberTickTimer = null;
      this._intervals = [];
      this._lastStatsLog = Date.now();
      this._errorCount = 0;
      this._lastErrorTime = Date.now();
      this._lastLightCleanup = Date.now();
      
      try { this.initializeRooms(); } catch { this.createDefaultRoom(); }
      
      this.startNumberTickTimer();
      
      this.roomCountsCache = new Map();
      this.cacheValidDuration = CONSTANTS.CACHE_VALID_DURATION;
      this.lastCacheUpdate = 0;
      
      this.loadState();
      this.startCleanupInterval();
      this.startMemoryMonitor();
      this.startAutoCleanup();
      
      console.log("✅ ChatServer started");
    } catch (error) {
      console.error("Constructor error:", error);
      this.initializeFallback();
    }
  }
  
  // ========== AUTO CLEANUP ==========
  startAutoCleanup() {
    const autoCleanupInterval = setInterval(async () => {
      try {
        await this.cleanup();
      } catch (error) {
        console.error("Auto cleanup error:", error.message);
      }
    }, CONSTANTS.AUTO_CLEANUP_INTERVAL);
    this._intervals.push(autoCleanupInterval);
  }
  
  // ========== DESTROY CLEANUP ==========
  async destroy() {
    console.log("🧹 Cleaning up ChatServer...");
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
    for (const [userId, timer] of this.disconnectedTimers) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
    console.log("✅ ChatServer cleanup complete");
  }
  
  // ========== CLEANUP INTERVAL ==========
  startCleanupInterval() {
    const cleanupInterval = setInterval(() => {
      this.performMemoryCleanup();
    }, 60000);
    this._intervals.push(cleanupInterval);
  }
  
  // ========== MEMORY MONITORING ==========
  startMemoryMonitor() {
    const interval = setInterval(() => {
      this.checkMemoryHealth();
    }, 60000);
    this._intervals.push(interval);
  }
  
  checkMemoryHealth() {
    try {
      const stats = this.getMemoryStats();
      const now = Date.now();
      if (now - this._lastStatsLog > 300000) {
        console.log(`📊 STATS: Users:${stats.totalUsers} Conn:${stats.totalConnections} Seats:${stats.occupiedSeats}`);
        this._lastStatsLog = now;
      }
      
      if (stats.totalTimers > CONSTANTS.MAX_DISCONNECTED_TIMERS) {
        for (const [userId, timer] of this.disconnectedTimers) {
          clearTimeout(timer);
        }
        this.disconnectedTimers.clear();
      }
      
      if (stats.queueSize > CONSTANTS.MAX_QUEUE_SIZE_EMERGENCY) {
        this.cleanupQueue?.clear();
      }
      
      if (stats.totalUsers > CONSTANTS.MEMORY_WARNING_THRESHOLD) {
        if (stats.totalUsers > CONSTANTS.MEMORY_CRITICAL_THRESHOLD) {
          this.emergencyMemoryCleanup();
        }
      }
      
      const load = this.getServerLoad();
      if (load > 0.95 && !this.safeMode) this.enableSafeMode();
      else if (load < 0.7 && this.safeMode) this.disableSafeMode();
      
    } catch (error) {}
  }
  
  getMemoryStats() {
    let totalConnections = 0;
    for (const conns of this.userConnections.values()) totalConnections += conns.size;
    let occupiedSeats = 0;
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (let i = 1; i <= this.MAX_SEATS; i++) if (seatMap.get(i)?.namauser) occupiedSeats++;
      }
    }
    return {
      totalUsers: this.userConnections.size,
      totalConnections: totalConnections,
      totalClients: this.clients.size,
      totalTimers: this.disconnectedTimers.size,
      occupiedSeats: occupiedSeats,
      queueSize: this.cleanupQueue?.size() || 0,
      safeMode: this.safeMode,
      load: this.getServerLoad()
    };
  }
  
  async emergencyMemoryCleanup() {
    try {
      const usersToClean = [];
      for (const [userId, connections] of this.userConnections) {
        let hasActiveConnection = false;
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            hasActiveConnection = true;
            break;
          }
        }
        if (!hasActiveConnection && connections.size > 0) {
          usersToClean.push(userId);
        }
      }
      
      const toClean = usersToClean.slice(0, CONSTANTS.EMERGENCY_CLEANUP_BATCH);
      for (const userId of toClean) {
        try { 
          await this.forceUserCleanup(userId);
          await new Promise(r => setTimeout(r, 10));
        } catch(e) {}
      }
      
      if (this.cleanupQueue) this.cleanupQueue.cleanupStuckJobs();
    } catch (error) {}
  }
  
  async loadState() {
    try {
      if (this.storage) {
        const savedNumber = await this.storage.get("currentNumber");
        if (savedNumber) this.currentNumber = savedNumber;
        const savedLastTick = await this.storage.get("lastNumberTick");
        if (savedLastTick) this.lastNumberTick = savedLastTick;
      }
    } catch {}
  }
  
  async saveState() {
    try {
      if (this.storage) {
        await this.storage.put("currentNumber", this.currentNumber);
        await this.storage.put("lastNumberTick", this.lastNumberTick);
      }
    } catch {}
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
    this.lowcard = null;
    this.gracePeriod = CONSTANTS.GRACE_PERIOD;
    this.cleanupQueue = new QueueManager(3);
    this.muteStatus = new Map();
    for (const room of roomList) this.muteStatus.set(room, false);
    this.storage = this.state?.storage;
    this.rateLimiter = new RateLimiter(60000, 100);
    this.connectionRateLimiter = new RateLimiter(10000, 5);
    this.safeMode = false;
    this.loadThreshold = CONSTANTS.LOAD_THRESHOLD;
    this._intervals = [];
    this._lastStatsLog = Date.now();
    this._errorCount = 0;
    this._lastErrorTime = Date.now();
    this._lastLightCleanup = Date.now();
    this.createDefaultRoom();
    this.lastNumberTick = Date.now();
    this.numberTickTimer = null;
    this.startNumberTickTimer();
    this.startCleanupInterval();
    this.startMemoryMonitor();
    this.startAutoCleanup();
  }
  
  // ========== MEMORY MANAGEMENT ==========
  async performMemoryCleanup() {
    try {
      const now = Date.now();
      const stats = this.getMemoryStats();
      
      const isMemorySafe = stats.totalUsers < 100 && stats.queueSize < 50;
      if (isMemorySafe && (now - this._lastLightCleanup) < 300000) {
        this.rateLimiter.cleanup();
        this.connectionRateLimiter.cleanup();
        this.lockManager?.cleanupStuckLocks();
        this.cleanupQueue?.cleanupStuckJobs();
        return;
      }
      this._lastLightCleanup = now;
      
      const deadClients = [];
      for (const client of this.clients) {
        if (!client || client.readyState === 3) deadClients.push(client);
      }
      for (const client of deadClients) this.clients.delete(client);
      
      if (this.clients.size > CONSTANTS.MAX_TOTAL_CLIENTS) {
        const clients = Array.from(this.clients);
        clients.sort((a, b) => (a._connectionTime || 0) - (b._connectionTime || 0));
        const toClose = clients.slice(0, this.clients.size - CONSTANTS.MAX_TOTAL_CLIENTS);
        for (const client of toClose) {
          try {
            if (client.readyState === 1) client.close(1000, "Server overload");
          } catch(e) {}
          this.clients.delete(client);
        }
      }
      
      for (const [room, clientArray] of this.roomClients) {
        if (clientArray) {
          const filtered = clientArray.filter(c => c && c.readyState === 1 && !c._isClosing);
          if (filtered.length !== clientArray.length) this.roomClients.set(room, filtered);
        }
      }
      
      const usersToCheck = Array.from(this.userConnections.entries()).slice(0, 500);
      for (const [userId, connections] of usersToCheck) {
        const activeConnections = new Set();
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing && !conn._isDuplicate) {
            activeConnections.add(conn);
          } else {
            try { conn.close(1000, "Connection dead"); } catch(e) {}
            this.clients.delete(conn);
          }
        }
        
        if (activeConnections.size === 0) {
          await this.forceUserCleanup(userId);
          this.userConnections.delete(userId);
        } else if (activeConnections.size !== connections.size) {
          this.userConnections.set(userId, activeConnections);
        }
      }
      
      if (this.userConnections.size > CONSTANTS.MAX_TOTAL_USERS) {
        const toRemove = [];
        for (const [userId, connections] of this.userConnections) {
          let hasActive = false;
          for (const conn of connections) {
            if (conn && conn.readyState === 1 && !conn._isClosing) {
              hasActive = true;
              break;
            }
          }
          if (!hasActive) toRemove.push(userId);
        }
        const removeCount = Math.min(toRemove.length, 100);
        for (let i = 0; i < removeCount; i++) {
          await this.forceUserCleanup(toRemove[i]);
        }
      }
      
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer._scheduledTime && (now - timer._scheduledTime) > this.gracePeriod + 5000) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
        }
      }
      
      if (this.disconnectedTimers.size > CONSTANTS.MAX_DISCONNECTED_TIMERS) {
        const oldest = Array.from(this.disconnectedTimers.entries()).slice(0, 100);
        for (const [userId, timer] of oldest) {
          clearTimeout(timer);
          this.disconnectedTimers.delete(userId);
        }
      }
      
      if (this.roomCountsCache && (now - this.lastCacheUpdate) > this.cacheValidDuration + 10000) {
        this.roomCountsCache = new Map();
        this.lastCacheUpdate = now;
      }
      
      if (this.storage && Math.random() < 0.01) {
        try {
          const keys = await this.storage.list({ limit: 20 });
          for (const [key, value] of keys) {
            if (key.startsWith("point-") && value?.timestamp && Date.now() - value.timestamp > 86400000) {
              await this.storage.delete(key);
            }
          }
        } catch(e) {}
      }
      
      this.lockManager?.cleanupStuckLocks();
      this.rateLimiter.cleanup();
      this.connectionRateLimiter.cleanup();
      this.cleanupQueue?.cleanupStuckJobs();
      
    } catch (error) {}
  }
  
  // ========== MUTE STATUS ==========
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
  
  // ========== CONNECTION MANAGEMENT ==========
  _addUserConnection(userId, ws) {
    if (!userId || !ws) return;
    const existingConnections = this.userConnections.get(userId);
    if (existingConnections && existingConnections.size > 0) {
      const oldConnection = Array.from(existingConnections)[0];
      if (oldConnection && oldConnection !== ws && oldConnection.readyState === 1) {
        oldConnection._isDuplicate = true;
        oldConnection._isClosing = true;
        try {
          this.safeSend(oldConnection, ["connectionReplaced", "New connection detected"]);
          oldConnection.close(1000, "Replaced by new connection");
        } catch(e) {}
        if (oldConnection.roomname) this._removeFromRoomClients(oldConnection, oldConnection.roomname);
        this.clients.delete(oldConnection);
      }
      this.userConnections.delete(userId);
    }
    const newConnections = new Set();
    newConnections.add(ws);
    this.userConnections.set(userId, newConnections);
    this.cancelCleanup(userId);
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
  
  // ========== LOCK MANAGEMENT ==========
  async withLock(resourceId, operation, timeout = CONSTANTS.LOCK_ACQUIRE_TIMEOUT) {
    let release;
    try {
      release = await this.lockManager.acquire(resourceId);
      const result = await Promise.race([
        operation(),
        new Promise((_, reject) => setTimeout(() => reject(new Error(`Lock timeout`)), timeout))
      ]);
      return result;
    } finally {
      if (release) try { release(); } catch {}
    }
  }
  
  // ========== SAFE MODE ==========
  checkAndEnableSafeMode() {
    const load = this.getServerLoad();
    if (load > this.loadThreshold && !this.safeMode) this.enableSafeMode();
    else if (load < CONSTANTS.LOAD_RECOVERY_THRESHOLD && this.safeMode) this.disableSafeMode();
  }
  
  enableSafeMode() {
    if (this.safeMode) return;
    this.safeMode = true;
    this.cleanupQueue.concurrency = 2;
    console.log("⚠️ SAFE MODE ENABLED");
    setTimeout(() => {
      if (this.getServerLoad() < CONSTANTS.LOAD_RECOVERY_THRESHOLD) this.disableSafeMode();
    }, 120000);
  }
  
  disableSafeMode() {
    this.safeMode = false;
    this.cleanupQueue.concurrency = 3;
    console.log("✅ SAFE MODE DISABLED");
  }
  
  getServerLoad() {
    const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1).length;
    const queueSize = this.cleanupQueue?.size() || 0;
    const queueLoad = Math.min(queueSize / 100, 0.3);
    return Math.min(activeConnections / 200 + queueLoad, 0.95);
  }
  
  // ========== POINT MANAGEMENT ==========
  broadcastPointDirect(room, seat, x, y, fast) {
    try {
      if (!room || !roomList.includes(room)) return;
      if (seat < 1 || seat > this.MAX_SEATS) return;
      
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return;
      
      const message = JSON.stringify(["pointUpdated", room, seat, x, y, fast]);
      for (const client of clientArray) {
        if (client?.readyState === 1 && client.roomname === room && !client._isClosing) {
          try { client.send(message); } catch {}
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
    } catch {
      this.broadcastPointDirect(room, seat, x, y, fast);
      return false;
    }
  }
  
  // ========== SEAT MANAGEMENT ==========
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
  
  async updateSeatAtomic(room, seatNumber, updateFn) {
    try {
      if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return null;
      return await this.withLock(`seat-update-${room}-${seatNumber}`, () => {
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
        
        if (updatedSeat.namauser && updatedSeat.namauser !== "") {
          occupancyMap.set(seatNumber, updatedSeat.namauser);
        } else {
          occupancyMap.set(seatNumber, null);
        }
        
        seatMap.set(seatNumber, updatedSeat);
        return updatedSeat;
      });
    } catch { return null; }
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
          if (occupantId === null && (!seatData || !seatData.namauser)) return i;
        } finally { release(); }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        if (occupantId === null && seatData?.namauser) {
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
            await this.cleanupUserFromSeat(room, i, occupantId, true);
            return i;
          }
        }
      }
      return null;
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
        seatMap.set(seat, { noimageUrl: "", namauser: userId, color: "", itembawah: 0, itematas: 0, vip: 0, viptanda: 0, lastPoint: null, lastUpdated: Date.now() });
      } else {
        seatData.namauser = userId;
        seatData.lastUpdated = Date.now();
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
          Object.assign(seatInfo, createEmptySeat());
          occupancyMap.set(seatNumber, null);
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
    
    if (this.disconnectedTimers.size > CONSTANTS.MAX_DISCONNECTED_TIMERS) {
      const oldest = Array.from(this.disconnectedTimers.entries())[0];
      if (oldest) {
        clearTimeout(oldest[1]);
        this.disconnectedTimers.delete(oldest[0]);
      }
    }
    
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
      if (timer) { clearTimeout(timer); this.disconnectedTimers.delete(userId); }
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
        
        await Promise.allSettled(seatsToCleanup.map(({ room, seatNumber }) => this.cleanupUserFromSeat(room, seatNumber, userId, true)));
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
        
        for (const [room, clientArray] of this.roomClients) {
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
    } catch {}
  }
  
  validateGracePeriodTimers() {
    try {
      const now = Date.now();
      const maxGracePeriod = this.gracePeriod + 5000;
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer?._scheduledTime) {
          if (now - timer._scheduledTime > maxGracePeriod) {
            clearTimeout(timer);
            this.disconnectedTimers.delete(userId);
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
  
  // ========== ROOM MANAGEMENT ==========
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) { this.safeSend(ws, ["error", "User ID not set"]); return false; }
    if (!roomList.includes(room)) { this.safeSend(ws, ["error", "Invalid room"]); return false; }
    if (!this.rateLimiter.check(ws.idtarget)) { this.safeSend(ws, ["error", "Too many requests"]); return false; }
    
    try {
      const roomRelease = await this.lockManager.acquire(`room-join-${room}`);
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
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
          const seatRelease = await this.lockManager.acquire(`seat-assign-${room}-${seat}`);
          try {
            const occupancyMap = this.seatOccupancy.get(room);
            if (!occupancyMap) continue;
            if (occupancyMap.get(seat) === null) {
              occupancyMap.set(seat, ws.idtarget);
              assignedSeat = seat;
              break;
            }
          } finally { seatRelease(); }
        }
        
        if (!assignedSeat) { this.safeSend(ws, ["roomFull", room]); return false; }
        
        // UPDATE SEAT ATOMIC dengan data user
        await this.updateSeatAtomic(room, assignedSeat, (currentSeat) => {
          return {
            noimageUrl: ws.noimageUrl || "",
            namauser: ws.idtarget,
            color: ws.color || "",
            itembawah: ws.itembawah || 0,
            itematas: ws.itematas || 0,
            vip: ws.vip || 0,
            viptanda: ws.viptanda || 0,
            lastPoint: null,
            lastUpdated: Date.now()
          };
        });
        
        this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
        this.userCurrentRoom.set(ws.idtarget, room);
        ws.roomname = room;
        ws.numkursi = new Set([assignedSeat]);
        
        const clientArray = this.roomClients.get(room);
        if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
        this._addUserConnection(ws.idtarget, ws);
        
        this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
        this.safeSend(ws, ["muteTypeResponse", this.getRoomMute(room), room]);
        
        // Kirim semua state ke user (termasuk kursi sendiri)
        this.sendAllStateTo(ws, room);
        
        // Broadcast ke room bahwa ada user baru
        this.broadcastToRoom(room, ["userJoined", room, assignedSeat, ws.idtarget]);
        
        this.updateRoomCount(room);
        return true;
      } finally { roomRelease(); }
    } catch (error) {
      console.error(`Join room error: ${error.message}`);
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }
  
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    try {
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
          await this.cleanupQueue.add(async () => { await this.forceUserCleanup(id); });
          ws.idtarget = id;
          ws.roomname = undefined;
          ws.numkursi = new Set();
          ws._connectionTime = Date.now();
          ws._isDuplicate = false;
          ws._isClosing = false;
          this._addUserConnection(id, ws);
          this.safeSend(ws, ["joinroomawal"]);
          return;
        }
        
        ws.idtarget = id;
        ws._connectionTime = Date.now();
        ws._isDuplicate = false;
        ws._isClosing = false;
        
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
            await this.cleanupQueue.add(async () => { await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, id, true); });
          }
        }
        this._addUserConnection(id, ws);
        this.safeSend(ws, ["needJoinRoom"]);
      });
    } catch { this.safeSend(ws, ["error", "Reconnection failed"]); }
  }
  
  // ========== BROADCAST & SEND ==========
  async safeSend(ws, arr, retry = CONSTANTS.SAFE_SEND_RETRY) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing) return false;
      
      if (ws.bufferedAmount > CONSTANTS.MAX_BUFFERED_AMOUNT) {
        if (retry > 0) {
          const delay = CONSTANTS.SAFE_SEND_RETRY_DELAY * (CONSTANTS.SAFE_SEND_RETRY - retry + 2);
          await new Promise(r => setTimeout(r, delay));
          return this.safeSend(ws, arr, retry - 1);
        }
        try {
          ws.send(JSON.stringify(arr));
          return true;
        } catch(e) {
          return false;
        }
      }
      
      ws.send(JSON.stringify(arr));
      return true;
    } catch (error) {
      if (ws?.idtarget) this._errorCount++;
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
        if (client?.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
          if (client.idtarget && sentToUsers.has(client.idtarget)) continue;
          try { client.send(message); sentCount++; if (client.idtarget) sentToUsers.add(client.idtarget); } catch {}
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
  
  // ========== STATE MANAGEMENT ==========
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
      console.error("sendAllStateTo error:", error.message);
    }
  }
  
  // ========== USER COUNT ==========
  getJumlahRoom() {
    try {
      const now = Date.now();
      if (this.roomCountsCache && (now - this.lastCacheUpdate) < this.cacheValidDuration) return this.roomCountsCache;
      const counts = {};
      for (const room of roomList) counts[room] = 0;
      for (const room of roomList) {
        const occupancyMap = this.seatOccupancy.get(room);
        if (!occupancyMap) continue;
        for (let i = 1; i <= this.MAX_SEATS; i++) if (occupancyMap.get(i)) counts[room]++;
      }
      this.roomCountsCache = counts;
      this.lastCacheUpdate = now;
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
  
  // ========== CONSISTENCY CHECKS ==========
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
          else Object.assign(seatData, createEmptySeat());
        } else if (occupantId && seatData?.namauser && seatData.namauser !== occupantId) {
          const isOccupantOnline = await this.isUserStillConnected(occupantId);
          if (isOccupantOnline) seatData.namauser = occupantId;
          else { occupancyMap.set(seat, null); Object.assign(seatData, createEmptySeat()); }
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
          allConnections.sort((a, b) => a.connectionTime - b.connectionTime);
          const connectionsToClose = allConnections.slice(1);
          for (const { client } of connectionsToClose) {
            client._isDuplicate = true;
            client._isClosing = true;
            try {
              if (client.readyState === 1) {
                this.safeSend(client, ["duplicateConnection", "Only one connection allowed"]);
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
  
  // ========== UTILITY ==========
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
      });
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
      this.clients.delete(ws);
      if (userId) {
        this._removeUserConnection(userId, ws);
        this.cancelCleanup(userId);
        if (!ws.isManualDestroy && !ws._isDuplicate) this.scheduleCleanup(userId);
      }
      if (room) this._removeFromRoomClients(ws, room);
      if (ws.readyState === 1) try { ws.close(1000, "Normal closure"); } catch {}
      setTimeout(() => { ws.roomname = null; ws.idtarget = null; ws.numkursi = null; }, 1000);
    } catch { this.clients.delete(ws); if (userId) this.cancelCleanup(userId); }
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
            
            await this.updateSeatAtomic(room, seat, (currentSeat) => {
              currentSeat.lastPoint = { 
                x: typeof x === 'number' ? x : parseFloat(x), 
                y: typeof y === 'number' ? y : parseFloat(y), 
                fast: fast || false, 
                timestamp: Date.now() 
              };
              return currentSeat;
            });
            
            this.broadcastPointDirect(room, seat, x, y, fast);
            // Kirim juga ke pengirim
            this.safeSend(ws, ["pointUpdated", room, seat, x, y, fast]);
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
              noimageUrl: noimageUrl || "",
              namauser: namauser,
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
              ws.numkursi = new Set([seat]);
            }
            
            this.updateRoomCount(room);
            this.broadcastToRoom(room, ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda]);
            // Kirim juga ke pengirim
            this.safeSend(ws, ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda]);
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
              if (this.lowcard) await this.lowcard.handleEvent(ws, data);
              else this.safeSend(ws, ["error", "Game system not available"]);
            }
            break;
          default: break;
        }
      } catch (error) {
        this._errorCount++;
        this._lastErrorTime = Date.now();
        if (ws.readyState === 1) this.safeSend(ws, ["error", "Server error"]);
      }
    } catch {}
  }
  
  // ========== FETCH HANDLER ==========
  async fetch(request) {
    try {
      const url = new URL(request.url);
      
      if (url.pathname === "/wakeup") {
        return new Response("OK", { status: 200 });
      }
      
      if (url.pathname === "/stats") {
        const stats = this.getMemoryStats();
        return new Response(JSON.stringify(stats, null, 2), {
          headers: { "Content-Type": "application/json" }
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
      
      ws.addEventListener("message", (ev) => {
        Promise.resolve().then(() => this.handleMessage(ws, ev.data).catch(() => {}));
      });
      
      ws.addEventListener("error", () => {});
      
      ws.addEventListener("close", (event) => {
        Promise.resolve().then(() => {
          if (event.code !== 1000 || event.reason !== "Replaced by new connection") {
            this.safeWebSocketCleanup(ws);
          } else {
            this.clients.delete(ws);
            if (ws.idtarget) this._removeUserConnection(ws.idtarget, ws);
            if (ws.roomname) this._removeFromRoomClients(ws, ws.roomname);
          }
        });
      });
      
      return new Response(null, { status: 101, webSocket: client });
    } catch {
      return new Response("Internal server error", { status: 500 });
    }
  }
  
  async cleanup() {
    try {
      await this.performMemoryCleanup();
    } catch (error) {
      console.error("Cleanup error (performMemoryCleanup):", error.message);
    }
    
    try {
      await this.cleanupDuplicateConnections();
    } catch (error) {
      console.error("Cleanup error (cleanupDuplicateConnections):", error.message);
    }
    
    try {
      this.validateGracePeriodTimers();
    } catch (error) {
      console.error("Cleanup error (validateGracePeriodTimers):", error.message);
    }
    
    try {
      this.sampledSeatConsistencyCheck();
    } catch (error) {
      console.error("Cleanup error (sampledSeatConsistencyCheck):", error.message);
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      
      if (url.pathname === "/wakeup") {
        return new Response("OK", { status: 200 });
      }
      
      if (url.pathname === "/cleanup") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        await obj.cleanup();
        return new Response("Cleanup completed", { status: 200 });
      }
      
      if (url.pathname === "/stats") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      
      if (url.pathname === "/destroy") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        await obj.destroy();
        return new Response("Destroy completed", { status: 200 });
      }
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      
      if (url.pathname === "/health") {
        return new Response("ok", { status: 200, headers: { "content-type": "text/plain", "cache-control": "no-cache" } });
      }
      
      return new Response("WebSocket endpoint", { status: 200, headers: { "content-type": "text/plain" } });
      
    } catch (error) {
      console.error("Fetch error:", error.message);
      return new Response("Server error", { status: 500 });
    }
  }
};
