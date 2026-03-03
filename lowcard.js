import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambahan", "Lounge Talk",
  "Noxxeliverothcifsa", "Friendly Corner", "Easy Talk", "Relax & Chat", "The Chatter Room"
];

class PromiseLockManager {
  constructor(timeout = 3000) {
    this.locks = new Map();
    this.queue = new Map();
    this.timeout = timeout;
    this.acquireCount = 0;
    this.releaseCount = 0;
  }

  async acquire(resourceId) {
    this.acquireCount++;
    
    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, {
        locked: true,
        timestamp: Date.now()
      });
      this.releaseCount++;
      return () => this.release(resourceId);
    }

    const lock = this.locks.get(resourceId);
    if (lock && Date.now() - lock.timestamp > 5000) {
      this.locks.delete(resourceId);
      this.locks.set(resourceId, {
        locked: true,
        timestamp: Date.now()
      });
      this.releaseCount++;
      return () => this.release(resourceId);
    }

    return new Promise((resolve, reject) => {
      if (!this.queue.has(resourceId)) {
        this.queue.set(resourceId, []);
      }
      
      const timeoutId = setTimeout(() => {
        const queue = this.queue.get(resourceId);
        if (queue) {
          const index = queue.indexOf(resolveWrapper);
          if (index > -1) queue.splice(index, 1);
        }
        reject(new Error(`Lock timeout for ${resourceId}`));
      }, this.timeout);

      const resolveWrapper = () => {
        clearTimeout(timeoutId);
        resolve(() => this.release(resourceId));
      };
      
      this.queue.get(resourceId).push(resolveWrapper);
    });
  }

  release(resourceId) {
    this.releaseCount++;
    
    const queue = this.queue.get(resourceId);
    if (queue && queue.length > 0) {
      const nextResolve = queue.shift();
      if (nextResolve) {
        this.locks.set(resourceId, {
          locked: true,
          timestamp: Date.now()
        });
        nextResolve();
      }
      if (queue.length === 0) {
        this.queue.delete(resourceId);
      }
    } else {
      this.locks.delete(resourceId);
    }
  }

  hasLock(resourceId) {
    return this.locks.has(resourceId);
  }

  cleanup() {
    const now = Date.now();
    const expiredLocks = [];
    
    for (const [resourceId, lock] of this.locks) {
      if (now - lock.timestamp > 10000) {
        expiredLocks.push(resourceId);
      }
    }
    
    for (const resourceId of expiredLocks) {
      this.locks.delete(resourceId);
      this.queue.delete(resourceId);
    }
  }
}

class QueueManager {
  constructor(concurrency = 3) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
    this.maxQueueSize = 200;
    this.processing = false;
  }

  async add(job) {
    if (this.queue.length > this.maxQueueSize) {
      return Promise.reject(new Error("Queue full"));
    }

    return new Promise((resolve, reject) => {
      this.queue.push({ 
        job, 
        resolve, 
        reject,
        added: Date.now()
      });
      
      if (!this.processing) {
        this.process();
      }
    });
  }

  async process() {
    if (this.processing) return;
    this.processing = true;
    
    while (this.queue.length > 0 && this.active < this.concurrency) {
      const now = Date.now();
      while (this.queue.length > 0 && now - this.queue[0].added > 10000) {
        const expired = this.queue.shift();
        expired.reject(new Error("Job timeout"));
      }
      
      if (this.queue.length === 0) break;
      
      const { job, resolve, reject } = this.queue.shift();
      this.active++;
      
      Promise.race([
        job(),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Job execution timeout")), 5000))
      ])
        .then(resolve)
        .catch(reject)
        .finally(() => {
          this.active--;
          setTimeout(() => this.process(), 0);
        });
    }
    
    this.processing = false;
  }

  clear() {
    const oldQueue = this.queue;
    this.queue = [];
    for (const item of oldQueue) {
      item.reject(new Error("Queue cleared"));
    }
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
    this.cleanupTimer = setInterval(() => this.cleanup(), windowMs);
  }

  check(userId) {
    if (!userId) return true;
    
    const now = Date.now();
    const userRequests = this.requests.get(userId) || [];
    const recentRequests = userRequests.filter(time => now - time < this.windowMs);
    
    if (recentRequests.length >= this.maxRequests) {
      return false;
    }
    
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
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
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
      this.isShuttingDown = false;
      this.startTime = Date.now();
      
      // MUTE STATUS
      this.muteStatus = new Map();
      for (const room of roomList) {
        this.muteStatus.set(room, false);
      }
      
      this.storage = state?.storage;
      
      // Lock managers
      this.lockManager = new PromiseLockManager(3000);
      this.seatLockManager = new PromiseLockManager(2000);
      
      // Cleanup tracking
      this.cleanupInProgress = new Set();
      this.cleanupQueue = new QueueManager(2);
      
      // Data structures
      this.clients = new Set();
      this.userToSeat = new Map();
      this.roomClients = new Map();
      this.userCurrentRoom = new Map();
      this.userConnections = new Map();
      this.disconnectedTimers = new Map();
      
      // Room data
      this.MAX_SEATS = 35;
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      
      // Buffers
      this.updateKursiBuffer = new Map();
      this.bufferSizeLimit = 30;
      this._pointBuffer = new Map();
      this._pointFlushTimer = null;
      
      // Rate limiters
      this.rateLimiter = new RateLimiter(60000, 50);
      this.connectionRateLimiter = new RateLimiter(10000, 3);
      
      // Connection attempts
      this.connectionAttempts = new Map();
      this.maxConnectionAttempts = 5;
      this.connectionBanTime = 30000;
      
      // Safe mode
      this.safeMode = false;
      this.loadThreshold = 0.8;
      this.lastLoadCheck = 0;
      this.loadCheckInterval = 10000;
      
      // Lowcard game
      try {
        this.lowcard = new LowCardGameManager(this);
      } catch {
        this.lowcard = null;
      }

      // Grace period
      this.gracePeriod = 3000;
      
      // Number ticker - 15 MENIT (900000 ms)
      this.currentNumber = 1;
      this.maxNumber = 6;
      this.intervalMillis = 15 * 60 * 1000; // 15 MENIT - TIDAK BERUBAH!
      this._nextConnId = 1;
      this._timers = [];
      
      // Initialize rooms
      this.initializeRooms();
      
      // Cache
      this.roomCountsCache = new Map();
      this.cacheValidDuration = 2000;
      this.lastCacheUpdate = 0;
      
      // Initialize point buffer
      for (const room of roomList) {
        this._pointBuffer.set(room, []);
      }
      
      // Start timers
      this.startOptimizedTimers();
      this.startPeriodicCleanup();
      
      console.log(`ChatServer started at ${new Date().toISOString()} - Tick interval: ${this.intervalMillis/60000} menit`);
      
    } catch (error) {
      console.error("ChatServer constructor error:", error);
      this.createEmergencyRoom();
    }
  }

  // ========== INITIALIZATION METHODS ==========

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
      } catch (err) {
        console.error(`Error initializing room ${room}:`, err);
      }
    }
  }

  createEmergencyRoom() {
    const room = "General";
    const seatMap = new Map();
    const occupancyMap = new Map();
    
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      seatMap.set(i, createEmptySeat());
      occupancyMap.set(i, null);
    }
    
    this.roomSeats = new Map([[room, seatMap]]);
    this.seatOccupancy = new Map([[room, occupancyMap]]);
    this.roomClients = new Map([[room, []]]);
    this.updateKursiBuffer = new Map([[room, new Map()]]);
    this._pointBuffer = new Map([[room, []]]);
    this.clients = new Set();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.disconnectedTimers = new Map();
    this.muteStatus = new Map([[room, false]]);
  }

  // ========== TIMER MANAGEMENT ==========

  startOptimizedTimers() {
    this._timers = [];
    
    // TIMER 1: Tick number - 15 MENIT (sesuai keinginan)
    this._tickTimer = setInterval(() => {
      if (this.isShuttingDown) return;
      try {
        this.tick(); // Update nomor dari 1-6
        console.log(`Tick executed - currentNumber: ${this.currentNumber}`);
      } catch (err) {
        console.error("Tick timer error:", err);
      }
    }, this.intervalMillis); // 15 MENIT - TIDAK BERUBAH!
    
    // TIMER 2: Flush buffer - 50ms untuk real-time updates
    this._flushTimer = setInterval(() => {
      if (this.isShuttingDown) return;
      try {
        this.flushBufferedPoints();
        this.flushKursiUpdates();
      } catch (err) {
        console.error("Flush timer error:", err);
      }
    }, 50); // 50ms untuk real-time
    
    // TIMER 3: Maintenance ringan - 30 detik
    this._maintenanceTimer = setInterval(() => {
      if (this.isShuttingDown) return;
      try {
        this.checkAndEnableSafeMode();
        if (this.getServerLoad() < 0.7) {
          this.lightCleanup();
        }
      } catch (err) {
        console.error("Maintenance timer error:", err);
      }
    }, 30000); // 30 detik
    
    // TIMER 4: Deep cleanup - 5 menit
    this._deepCleanupTimer = setInterval(() => {
      if (this.isShuttingDown) return;
      try {
        if (this.clients.size < 50) {
          this.deepCleanup();
        }
      } catch (err) {
        console.error("Deep cleanup error:", err);
      }
    }, 300000); // 5 menit
    
    this._timers = [
      this._tickTimer,        // 15 MENIT
      this._flushTimer,       // 50ms
      this._maintenanceTimer, // 30 detik
      this._deepCleanupTimer  // 5 menit
    ];
    
    console.log(`Timers started: tick=${this.intervalMillis/60000}menit, flush=50ms, maintenance=30detik, deep=5menit`);
  }

  startPeriodicCleanup() {
    this._lockCleanupTimer = setInterval(() => {
      if (this.lockManager) {
        this.lockManager.cleanup();
      }
      if (this.seatLockManager) {
        this.seatLockManager.cleanup();
      }
    }, 60000); // 1 menit
    
    this._timers.push(this._lockCleanupTimer);
  }

  // ========== CLEANUP METHODS ==========

  lightCleanup() {
    const deadClients = [];
    for (const client of this.clients) {
      if (!client || client.readyState === 3) {
        deadClients.push(client);
      }
    }
    
    for (const client of deadClients) {
      this.clients.delete(client);
    }
    
    const now = Date.now();
    for (const [userId, timer] of this.disconnectedTimers) {
      if (timer && timer._scheduledTime && now - timer._scheduledTime > this.gracePeriod + 2000) {
        clearTimeout(timer);
        this.disconnectedTimers.delete(userId);
      }
    }
    
    this.cleanupEmptySeatBuffers();
  }

  deepCleanup() {
    const usersWithConnections = new Set();
    for (const client of this.clients) {
      if (client && client.idtarget && client.readyState === 1) {
        usersWithConnections.add(client.idtarget);
      }
    }
    
    for (const [userId] of this.userToSeat) {
      if (!usersWithConnections.has(userId)) {
        this.userToSeat.delete(userId);
      }
    }
    
    for (const [userId] of this.userCurrentRoom) {
      if (!usersWithConnections.has(userId)) {
        this.userCurrentRoom.delete(userId);
      }
    }
    
    for (const [room, clients] of this.roomClients) {
      if (clients) {
        const validClients = clients.filter(c => c && c.readyState === 1);
        if (validClients.length !== clients.length) {
          this.roomClients.set(room, validClients);
        }
      }
    }
    
    for (const [userId, connections] of this.userConnections) {
      if (!connections || connections.size === 0) {
        this.userConnections.delete(userId);
      } else {
        const validConnections = new Set();
        for (const conn of connections) {
          if (conn && conn.readyState === 1) {
            validConnections.add(conn);
          }
        }
        if (validConnections.size === 0) {
          this.userConnections.delete(userId);
        } else if (validConnections.size !== connections.size) {
          this.userConnections.set(userId, validConnections);
        }
      }
    }
    
    for (const room of roomList) {
      this.validateSeatConsistency(room);
    }
  }

  cleanupEmptySeatBuffers() {
    try {
      for (const [room, buffer] of this.updateKursiBuffer) {
        if (buffer && buffer.size > 0) {
          const seatMap = this.roomSeats.get(room);
          if (seatMap) {
            const seatsToDelete = [];
            for (const [seat] of buffer) {
              const seatData = seatMap.get(seat);
              if (!seatData || !seatData.namauser) {
                seatsToDelete.push(seat);
              }
            }
            
            for (const seat of seatsToDelete) {
              buffer.delete(seat);
            }
          }
        }
      }
    } catch (error) {}
  }

  async cleanupDuplicateConnections() {
    try {
      if (this.clients.size < 10) return;
      
      const userConnectionsMap = new Map();
      
      for (const client of this.clients) {
        if (client && client.idtarget && client.readyState === 1 && !client._isClosing) {
          if (!userConnectionsMap.has(client.idtarget)) {
            userConnectionsMap.set(client.idtarget, []);
          }
          userConnectionsMap.get(client.idtarget).push({
            client,
            time: client._connectionTime || 0,
            room: client.roomname
          });
        }
      }
      
      for (const [userId, connections] of userConnectionsMap) {
        if (connections.length > 1) {
          connections.sort((a, b) => b.time - a.time);
          
          for (let i = 1; i < connections.length; i++) {
            const { client } = connections[i];
            client._isDuplicate = true;
            
            try {
              if (client.readyState === 1) {
                this.safeSend(client, ["duplicateConnection", "Koneksi duplikat terdeteksi"]);
                client.close(1000, "Duplicate connection");
              }
            } catch {}
            
            this.clients.delete(client);
            if (client.roomname) {
              this._removeFromRoomClients(client, client.roomname);
            }
            this._removeUserConnection(userId, client);
          }
          
          const remainingConnections = new Set();
          remainingConnections.add(connections[0].client);
          this.userConnections.set(userId, remainingConnections);
        }
      }
    } catch (error) {
      console.error("Error in cleanupDuplicateConnections:", error);
    }
  }

  // ========== MUTE STATUS METHODS ==========

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

  // ========== LOCK MANAGEMENT ==========

  async withLock(resourceId, operation, timeout = 2000) {
    if (this.isShuttingDown) {
      throw new Error("Server shutting down");
    }
    
    let release;
    try {
      release = await this.lockManager.acquire(resourceId);
      return await operation();
    } catch (error) {
      if (error.message.includes('Lock timeout')) {
        throw error;
      }
      throw error;
    } finally {
      if (release) {
        try {
          release();
        } catch {}
      }
    }
  }

  async withSeatLock(room, seat, operation) {
    const lockId = `seat-${room}-${seat}`;
    let release;
    try {
      release = await this.seatLockManager.acquire(lockId);
      return await operation();
    } finally {
      if (release) {
        try {
          release();
        } catch {}
      }
    }
  }

  // ========== SERVER LOAD MANAGEMENT ==========

  checkAndEnableSafeMode() {
    const now = Date.now();
    if (now - this.lastLoadCheck < this.loadCheckInterval) return;
    
    this.lastLoadCheck = now;
    const load = this.getServerLoad();
    
    if (load > this.loadThreshold && !this.safeMode) {
      this.enableSafeMode();
    } else if (load < 0.6 && this.safeMode) {
      this.disableSafeMode();
    }
  }

  getServerLoad() {
    const activeConnections = Array.from(this.clients).filter(c => c && c.readyState === 1).length;
    const queueSize = this.cleanupQueue ? this.cleanupQueue.size() : 0;
    const lockQueueSize = this.lockManager ? this.lockManager.queue.size : 0;
    
    const connectionLoad = Math.min(activeConnections / 150, 0.8);
    const queueLoad = Math.min(queueSize / 100, 0.5);
    const lockLoad = Math.min(lockQueueSize / 50, 0.3);
    
    return Math.min(connectionLoad + queueLoad + lockLoad, 1.0);
  }

  enableSafeMode() {
    if (this.safeMode) return;
    this.safeMode = true;
    this.cleanupQueue.concurrency = 1;
    
    setTimeout(() => {
      if (this.getServerLoad() < 0.6) {
        this.disableSafeMode();
      }
    }, 30000);
  }

  disableSafeMode() {
    this.safeMode = false;
    this.cleanupQueue.concurrency = 2;
  }

  // ========== POINT MANAGEMENT ==========

  flushBufferedPoints() {
    for (const [room, points] of this._pointBuffer) {
      if (points && points.length > 0) {
        const batch = points.splice(0, Math.min(points.length, 20));
        if (batch.length > 0) {
          this.broadcastPointsBatch(room, batch);
        }
      }
    }
  }

  broadcastPointsBatch(room, batch) {
    try {
      if (!room || !roomList.includes(room)) return;
      
      const validBatch = batch.filter(point => point && point.seat >= 1 && point.seat <= this.MAX_SEATS);
      if (validBatch.length === 0) return;
      
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return;
      
      const message = JSON.stringify(["pointsBatch", room, validBatch]);
      
      for (let i = 0; i < clientArray.length; i++) {
        const client = clientArray[i];
        if (client && client.readyState === 1 && client.roomname === room) {
          try { 
            client.send(message); 
          } catch {}
        }
      }
    } catch {}
  }

  broadcastPointDirect(room, seat, x, y, fast) {
    try {
      if (!room || !roomList.includes(room)) return;
      if (seat < 1 || seat > this.MAX_SEATS) return;
      
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return;
      
      const message = JSON.stringify(["pointUpdated", room, seat, x, y, fast]);
      
      for (let i = 0; i < clientArray.length; i++) {
        const client = clientArray[i];
        if (client && client.readyState === 1 && client.roomname === room) {
          try { 
            client.send(message); 
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
      
      await this.withSeatLock(room, seat, async () => {
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) return;
        
        const seatData = seatMap.get(seat);
        if (seatData) {
          seatData.lastPoint = { 
            x: xNum, 
            y: yNum, 
            fast: fast || false, 
            timestamp: Date.now() 
          };
        }
      });
      
      this.broadcastPointDirect(room, seat, xNum, yNum, fast);
      
      return true;
    } catch {
      this.broadcastPointDirect(room, seat, x, y, fast);
      return false;
    }
  }

  // ========== KURSI MANAGEMENT ==========

  clearSeatBuffer(room, seatNumber) {
    try {
      if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return;
      
      const roomMap = this.updateKursiBuffer.get(room);
      if (roomMap) {
        roomMap.delete(seatNumber);
      }
    } catch {}
  }

  async updateSeatAtomic(room, seatNumber, updateFn) {
    try {
      if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return null;
      
      return await this.withSeatLock(room, seatNumber, () => {
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        
        if (!seatMap || !occupancyMap) return null;
        
        let currentSeat = seatMap.get(seatNumber);
        if (!currentSeat) {
          currentSeat = createEmptySeat();
          seatMap.set(seatNumber, currentSeat);
        }
        
        const updatedSeat = updateFn({ ...currentSeat });
        updatedSeat.lastUpdated = Date.now();
        
        if (updatedSeat.namauser && updatedSeat.namauser !== "") {
          occupancyMap.set(seatNumber, updatedSeat.namauser);
        } else {
          occupancyMap.set(seatNumber, null);
        }
        
        seatMap.set(seatNumber, updatedSeat);
        
        const buffer = this.updateKursiBuffer.get(room);
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

  // ========== SEAT VALIDATION ==========

  async ensureSeatsData(room) {
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      
      if (!seatMap || !occupancyMap) return;
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatMap.has(seat)) {
          seatMap.set(seat, createEmptySeat());
        }
        if (!occupancyMap.has(seat)) {
          occupancyMap.set(seat, null);
        }
      }
    } catch (error) {}
  }

  async validateSeatConsistency(room) {
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      
      if (!seatMap || !occupancyMap) return;
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const occupantId = occupancyMap.get(seat);
        const seatData = seatMap.get(seat) || createEmptySeat();
        
        if (occupantId && (!seatData.namauser || seatData.namauser === "")) {
          seatData.namauser = occupantId;
          seatMap.set(seat, seatData);
        } else if (!occupantId && seatData.namauser && seatData.namauser !== "") {
          const isUserOnline = await this.isUserStillConnected(seatData.namauser);
          if (isUserOnline) {
            occupancyMap.set(seat, seatData.namauser);
          } else {
            Object.assign(seatData, createEmptySeat());
            seatMap.set(seat, seatData);
          }
        } else if (occupantId && seatData.namauser && seatData.namauser !== occupantId) {
          const isOccupantOnline = await this.isUserStillConnected(occupantId);
          if (isOccupantOnline) {
            seatData.namauser = occupantId;
            seatMap.set(seat, seatData);
          } else {
            occupancyMap.set(seat, null);
            Object.assign(seatData, createEmptySeat());
            seatMap.set(seat, seatData);
          }
        }
      }
    } catch (error) {}
  }

  async checkSeatConsistency() {
    try {
      if (this.clients.size > 100) return 0;
      
      const randomRoom = roomList[Math.floor(Math.random() * roomList.length)];
      await this.validateSeatConsistency(randomRoom);
      return 0;
    } catch (error) {
      return 0;
    }
  }

  // ========== FIND EMPTY SEAT ==========

  async findEmptySeat(room, ws) {
    if (!room || !ws || !ws.idtarget) return null;
    
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      const seatMap = this.roomSeats.get(room);
      
      if (!occupancyMap || !seatMap) return null;
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        if (occupancyMap.get(i) === ws.idtarget) {
          return i;
        }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        
        if (occupantId === null && (!seatData || !seatData.namauser || seatData.namauser === "")) {
          return i;
        }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        
        if (occupantId === null && seatData && seatData.namauser && seatData.namauser !== "") {
          Object.assign(seatData, createEmptySeat());
          seatMap.set(i, seatData);
          return i;
        }
      }
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        
        if (occupantId) {
          const isOnline = await this.isUserStillConnected(occupantId);
          if (!isOnline) {
            await this.cleanupUserFromSeat(room, i, occupantId, true);
            return i;
          }
        }
      }
      
      return null;
    } catch {
      return null;
    }
  }

  // ========== USER CONNECTION STATUS ==========

  async isUserStillConnected(userId) {
    if (!userId || this.isShuttingDown) return false;
    
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return false;
    
    for (const conn of connections) {
      if (!conn) continue;
      if (conn.readyState !== 1) continue;
      if (conn._isDuplicate) continue;
      if (conn._isClosing) continue;
      
      return true;
    }
    
    return false;
  }

  _addUserConnection(userId, ws) {
    if (!userId || !ws || this.isShuttingDown) return;
    
    let userConnections = this.userConnections.get(userId);
    if (!userConnections) {
      userConnections = new Set();
      this.userConnections.set(userId, userConnections);
    }
    
    if (userConnections.size >= 2) {
      let oldestConn = null;
      let oldestTime = Infinity;
      for (const conn of userConnections) {
        if (conn._connectionTime && conn._connectionTime < oldestTime) {
          oldestTime = conn._connectionTime;
          oldestConn = conn;
        }
      }
      if (oldestConn) {
        userConnections.delete(oldestConn);
        try {
          if (oldestConn.readyState === 1) {
            oldestConn.close(1000, "Too many connections");
          }
        } catch {}
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

  // ========== GRACE PERIOD & CLEANUP ==========

  scheduleCleanup(userId) {
    try {
      if (!userId || this.isShuttingDown) return;
      
      this.cancelCleanup(userId);
      
      const timerId = setTimeout(async () => {
        try {
          this.disconnectedTimers.delete(userId);
          
          const isStillConnected = await this.isUserStillConnected(userId);
          if (!isStillConnected) {
            await this.forceUserCleanup(userId);
          }
        } catch (error) {}
      }, this.gracePeriod);
      
      timerId._scheduledTime = Date.now();
      timerId._userId = userId;
      this.disconnectedTimers.set(userId, timerId);
    } catch (error) {}
  }

  cancelCleanup(userId) {
    try {
      if (!userId) return;
      
      const timer = this.disconnectedTimers.get(userId);
      if (timer) {
        clearTimeout(timer);
        this.disconnectedTimers.delete(userId);
      }
      
      if (this.cleanupInProgress && this.cleanupInProgress.has(userId)) {
        this.cleanupInProgress.delete(userId);
      }
    } catch (error) {}
  }

  async forceUserCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId) || this.isShuttingDown) return;
    
    this.cleanupInProgress.add(userId);
    
    try {
      await this.withLock(`force-cleanup-${userId}`, async () => {
        this.cancelCleanup(userId);
        
        for (const room of roomList) {
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          
          if (!seatMap || !occupancyMap) continue;
          
          for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
            const seatData = seatMap.get(seat);
            const occupantId = occupancyMap.get(seat);
            
            if (seatData && seatData.namauser === userId) {
              Object.assign(seatData, createEmptySeat());
              seatMap.set(seat, seatData);
              occupancyMap.set(seat, null);
              
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.updateRoomCount(room);
            } else if (occupantId === userId) {
              occupancyMap.set(seat, null);
            }
          }
        }
        
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        
        for (const [room, clients] of this.roomClients) {
          if (clients) {
            const filtered = clients.filter(c => !c || c.idtarget !== userId);
            if (filtered.length !== clients.length) {
              this.roomClients.set(room, filtered);
            }
          }
        }
        
        this.userConnections.delete(userId);
      });
    } catch (error) {
    } finally {
      this.cleanupInProgress.delete(userId);
    }
  }

  async fullRemoveById(userId) {
    if (!userId) return;
    
    try {
      await this.withLock(`full-remove-${userId}`, async () => {
        this.cancelCleanup(userId);
        
        for (const room of roomList) {
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          
          if (seatMap) {
            for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
              const seatData = seatMap.get(seat);
              if (seatData && seatData.namauser === userId) {
                Object.assign(seatData, createEmptySeat());
                seatMap.set(seat, seatData);
                
                if (occupancyMap) {
                  occupancyMap.set(seat, null);
                }
                
                this.clearSeatBuffer(room, seat);
                this.broadcastToRoom(room, ["removeKursi", room, seat]);
                this.updateRoomCount(room);
              }
            }
          }
        }
        
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this.userConnections.delete(userId);
        
        const clientsToRemove = [];
        for (const client of this.clients) {
          if (client && client.idtarget === userId) {
            clientsToRemove.push(client);
          }
        }
        
        for (const client of clientsToRemove) {
          try {
            if (client.readyState === 1) {
              client.close(1000, "Session removed");
            }
          } catch {}
          this.clients.delete(client);
          
          if (client.roomname) {
            this._removeFromRoomClients(client, client.roomname);
          }
        }
      });
    } catch (error) {
      console.error("Error in fullRemoveById:", error);
    }
  }

  async cleanupUserFromSeat(room, seatNumber, userId, immediate = true) {
    try {
      if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return;
      
      await this.withSeatLock(room, seatNumber, () => {
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        
        if (!seatMap || !occupancyMap) return;
        
        const seatInfo = seatMap.get(seatNumber);
        if (!seatInfo || seatInfo.namauser !== userId) return;
        
        Object.assign(seatInfo, createEmptySeat());
        occupancyMap.set(seatNumber, null);
        
        if (immediate) {
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.updateRoomCount(room);
        }
      });
    } catch (error) {}
  }

  async cleanupFromRoom(ws, room) {
    if (!ws || !ws.idtarget || !ws.roomname || ws.roomname !== room) return;
    
    try {
      await this.withLock(`room-cleanup-${room}-${ws.idtarget}`, async () => {
        const seatInfo = this.userToSeat.get(ws.idtarget);
        
        if (seatInfo && seatInfo.room === room) {
          await this.cleanupUserFromSeat(room, seatInfo.seat, ws.idtarget, true);
        }
        
        this._removeFromRoomClients(ws, room);
        this._removeUserConnection(ws.idtarget, ws);
        this.userCurrentRoom.delete(ws.idtarget);
        
        ws.roomname = undefined;
        
        this.userToSeat.delete(ws.idtarget);
        this.updateRoomCount(room);
      });
    } catch (error) {}
  }

  // ========== JOIN ROOM ==========

  async handleJoinRoom(ws, room) {
    if (!ws || !ws.idtarget || this.isShuttingDown) {
      this.safeSend(ws, ["error", "Server unavailable"]);
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
      if (ws.roomname === room) {
        this.sendAllStateTo(ws, room);
        const isMuted = this.getRoomMute(room);
        this.safeSend(ws, ["muteTypeResponse", isMuted, room]);
        return true;
      }
      
      if (ws.roomname) {
        await this.cleanupFromRoom(ws, ws.roomname);
      }
      
      await this.ensureSeatsData(room);
      
      const assignedSeat = await this.findEmptySeat(room, ws);
      
      if (!assignedSeat) {
        this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      await this.withSeatLock(room, assignedSeat, () => {
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        
        if (!seatMap || !occupancyMap) return;
        
        occupancyMap.set(assignedSeat, ws.idtarget);
        
        let seatData = seatMap.get(assignedSeat);
        if (!seatData) {
          seatData = createEmptySeat();
        }
        seatData.namauser = ws.idtarget;
        seatData.lastUpdated = Date.now();
        seatMap.set(assignedSeat, seatData);
      });
      
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      
      const clientArray = this.roomClients.get(room);
      if (clientArray && !clientArray.includes(ws)) {
        clientArray.push(ws);
      }
      
      this._addUserConnection(ws.idtarget, ws);
      
      this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      
      const isMuted = this.getRoomMute(room);
      this.safeSend(ws, ["muteTypeResponse", isMuted, room]);
      
      setTimeout(() => {
        this.sendAllStateTo(ws, room);
      }, 100);
      
      this.updateRoomCount(room);
      
      return true;
      
    } catch (error) {
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }

  // ========== ROOM COUNTS ==========

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
        
        let roomCount = 0;
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          if (occupancyMap.get(i)) {
            roomCount++;
          }
        }
        counts[room] = roomCount;
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

  updateRoomCount(room) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return 0;
      
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const info = seatMap.get(i);
        if (info && info.namauser && info.namauser !== "") {
          count++;
        }
      }
      
      this.roomCountsCache = null;
      this.broadcastRoomUserCount(room);
      
      return count;
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
        if (info && info.namauser && info.namauser !== "") {
          count++;
        }
      }
      
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch {}
  }

  invalidateRoomCache(room) {
    this.roomCountsCache = null;
  }

  // ========== BROADCAST ==========

  safeSend(ws, arr) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing || this.isShuttingDown) {
        return false;
      }
      
      if (ws.bufferedAmount > 500000) return false;
      
      ws.send(JSON.stringify(arr));
      return true;
    } catch {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room) || this.isShuttingDown) return 0;
      
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return 0;
      
      let sentCount = 0;
      const message = JSON.stringify(msg);
      
      for (let i = 0; i < clientArray.length; i++) {
        const client = clientArray[i];
        if (client && client.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
          try {
            client.send(message);
            sentCount++;
          } catch {}
        }
      }
      
      return sentCount;
    } catch {
      return 0;
    }
  }

  sendAllStateTo(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || ws._isDuplicate || ws._isClosing) {
        return;
      }
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      
      const allKursiMeta = {};
      const lastPointsData = [];
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        
        if (info && info.namauser && info.namauser !== "") {
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
        
        if (info && info.lastPoint) {
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
      const count = counts[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
    } catch (error) {}
  }

  // ========== FLUSH ==========

  flushKursiUpdates() {
    try {
      if (!this.updateKursiBuffer) return;
      
      for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
        if (!room || !roomList.includes(room)) continue;
        if (!seatMapUpdates || seatMapUpdates.size === 0) continue;
        
        const updates = [];
        for (const [seat, info] of seatMapUpdates.entries()) {
          if (seat < 1 || seat > this.MAX_SEATS) continue;
          
          if (info && info.namauser && info.namauser !== "") {
            updates.push([seat, info]);
            
            if (updates.length >= 20) {
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
    } catch {}
  }

  // ========== TICK ==========

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      for (const room of roomList) {
        this.broadcastToRoom(room, ["currentNumber", this.currentNumber]);
      }
      
      console.log(`Tick: number changed to ${this.currentNumber}`);
    } catch {}
  }

  // ========== SET ID TARGET ==========

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws || this.isShuttingDown) {
      this.safeSend(ws, ["error", "Server unavailable"]);
      return;
    }
    
    try {
      await this.withLock(`reconnect-${id}`, async () => {
        this.cancelCleanup(id);
        
        if (baru === true) {
          await this.cleanupQueue.add(async () => {
            await this.forceUserCleanup(id);
          });
          
          ws.idtarget = id;
          ws.roomname = undefined;
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
          
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          
          if (seatMap && occupancyMap) {
            const seatData = seatMap.get(seat);
            const occupantId = occupancyMap.get(seat);
            
            if (seatData && seatData.namauser === id && occupantId === id) {
              ws.roomname = room;
              
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) {
                clientArray.push(ws);
              }
              
              this._addUserConnection(id, ws);
              this.sendAllStateTo(ws, room);
              
              if (seatData.lastPoint) {
                this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast]);
              }
              
              const isMuted = this.getRoomMute(room);
              this.safeSend(ws, ["muteTypeResponse", isMuted, room]);
              
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

  // ========== WEBSOCKET CLEANUP ==========

  async safeWebSocketCleanup(ws) {
    if (!ws || this.isShuttingDown) return;
    
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      ws._isClosing = true;
      this.clients.delete(ws);
      
      if (userId) {
        this._removeUserConnection(userId, ws);
        
        if (!ws.isManualDestroy && !ws._isDuplicate) {
          this.scheduleCleanup(userId);
        }
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
      }
      
      try {
        if (ws.readyState === 1) {
          ws.close(1000, "Normal closure");
        }
      } catch {}
      
    } catch (error) {
      this.clients.delete(ws);
      if (userId) {
        this.cancelCleanup(userId);
      }
    }
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
            const { room, seat } = seatInfo;
            await this.cleanupQueue.add(async () => {
              await this.cleanupUserFromSeat(room, seat, idtarget, true);
            });
          }
          
          this.userToSeat.delete(idtarget);
          this.userCurrentRoom.delete(idtarget);
        }
        
        this.cancelCleanup(idtarget);
        this._removeUserConnection(idtarget, ws);
        
        if (this.roomClients) {
          for (const [room, clientArray] of this.roomClients) {
            if (clientArray) {
              const index = clientArray.indexOf(ws);
              if (index > -1) {
                clientArray.splice(index, 1);
              }
            }
          }
        }
        
        this.clients.delete(ws);
        
        if (ws.readyState === 1) {
          try { ws.close(1000, "Manual destroy"); } catch {}
        }
      });
    } catch {
      this.clients.delete(ws);
      this.cancelCleanup(idtarget);
      this._removeUserConnection(idtarget, ws);
    }
  }

  // ========== GET ONLINE USERS ==========

  getAllOnlineUsers() {
    try {
      const users = [];
      const seenUsers = new Set();
      
      for (const client of this.clients) {
        if (client && client.idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
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
      if (!roomName || !roomList.includes(roomName)) return [];
      
      const users = [];
      const seenUsers = new Set();
      const clientArray = this.roomClients.get(roomName);
      
      if (clientArray) {
        for (const client of clientArray) {
          if (client && client.idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
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

  // ========== HANDLE MESSAGE ==========

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing || this.isShuttingDown) {
      return;
    }
    
    if (ws.idtarget && !this.rateLimiter.check(ws.idtarget)) {
      this.safeSend(ws, ["error", "Too many requests"]);
      return;
    }
    
    try {
      if (raw.length > 20000) {
        try { ws.close(1009, "Message too large"); } catch {}
        return;
      }
      
      let data;
      try {
        data = JSON.parse(raw);
      } catch {
        return;
      }
      
      if (!Array.isArray(data) || data.length === 0) return;
      
      const evt = data[0];
      
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
          
          const isMuted = this.getRoomMute(roomName);
          this.safeSend(ws, ["muteTypeResponse", isMuted, roomName]);
          break;
        }
        
        case "onDestroy": {
          const idtarget = ws.idtarget;
          if (idtarget) {
            ws.isManualDestroy = true;
            this.handleOnDestroy(ws, idtarget);
          }
          break;
        }
        
        case "setIdTarget2": 
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
        
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          
          for (const client of this.clients) {
            if (client && client.idtarget === idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
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
            if (client && client.idtarget === idt && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
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
          const result = roomList.map(room => [room, allCounts[room] || 0]);
          this.safeSend(ws, ["allRoomsUserCount", result]);
          break;
        }
        
        case "getCurrentNumber": 
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        
        case "getOnlineUsers": 
          this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
          break;
        
        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) break;
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
          const connections = this.userConnections.get(username);
          if (connections && connections.size > 0) {
            let earliest = null;
            for (const conn of connections) {
              if (conn.readyState === 1 && !conn._isClosing) {
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
          
          this.savePointWithRetry(room, seat, x, y, fast);
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
          break;
        }
        
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          
          if (ws.roomname !== roomname || ws.idtarget !== sender) return;
          if (!roomList.includes(roomname)) return;
          
          const timestamp = Date.now();
          this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, giftName, timestamp]);
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
          if (ws.roomname === "LowCard 1" || ws.roomname === "LowCard 2" || ws.roomname === "Noxxeliverothcifsa"  || ws.roomname === "Chikahan Tambahan") {
            if (this.lowcard) {
              await this.lowcard.handleEvent(ws, data);
            }
          }
          break;
          
        default:
          break;
      }
    } catch (error) {
      if (ws.readyState === 1) {
        this.safeSend(ws, ["error", "Server error"]);
      }
    }
  }

  // ========== SHUTDOWN ==========

  async shutdown() {
    this.isShuttingDown = true;
    
    for (const timer of this._timers) {
      if (timer) {
        try { clearInterval(timer); } catch {}
      }
    }
    this._timers = [];
    
    if (this.rateLimiter) {
      this.rateLimiter.destroy();
    }
    if (this.connectionRateLimiter) {
      this.connectionRateLimiter.destroy();
    }
    
    for (const client of this.clients) {
      try {
        if (client.readyState === 1) {
          client.close(1000, "Server shutdown");
        }
      } catch {}
    }
    
    this.clients.clear();
  }

  // ========== FETCH ==========

  async fetch(request) {
    const url = new URL(request.url);
    
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        uptime: Date.now() - this.startTime,
        clients: this.clients.size,
        rooms: this.roomSeats.size,
        safeMode: this.safeMode,
        currentNumber: this.currentNumber,
        tickInterval: this.intervalMillis / 60000 + " menit"
      }), { 
        status: 200,
        headers: { 
          "content-type": "application/json",
          "cache-control": "no-cache"
        }
      });
    }
    
    if (url.pathname === "/stats") {
      const stats = {
        clients: this.clients.size,
        userConnections: this.userConnections.size,
        roomSeats: this.roomSeats.size,
        activeTimers: this._timers.length,
        safeMode: this.safeMode,
        uptime: Date.now() - this.startTime,
        currentNumber: this.currentNumber,
        tickInterval: this.intervalMillis / 60000 + " menit",
        lockManager: {
          locks: this.lockManager.locks.size,
          queue: this.lockManager.queue.size,
          acquireCount: this.lockManager.acquireCount,
          releaseCount: this.lockManager.releaseCount
        },
        cleanupQueue: this.cleanupQueue ? this.cleanupQueue.size() : 0
      };
      
      return new Response(JSON.stringify(stats), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }
    
    if (url.pathname === "/shutdown" && request.method === "POST") {
      const auth = request.headers.get("Authorization");
      if (auth === "Bearer your-secret-token") {
        this.shutdown();
        return new Response("Shutting down", { status: 200 });
      }
      return new Response("Unauthorized", { status: 401 });
    }
    
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("WebSocket endpoint. Use /health for status.", { 
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    }
    
    try {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      await server.accept();
      
      const ws = server;
      ws._connId = `conn#${this._nextConnId++}`;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.isManualDestroy = false;
      ws.errorCount = 0;
      ws._isDuplicate = false;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      
      this.clients.add(ws);
      
      ws.addEventListener("message", (ev) => {
        Promise.resolve().then(() => {
          this.handleMessage(ws, ev.data).catch(() => {});
        });
      });
      
      ws.addEventListener("close", () => {
        Promise.resolve().then(() => {
          this.safeWebSocketCleanup(ws);
        });
      });
      
      ws.addEventListener("error", () => {});
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      return new Response("Internal server error", { status: 500 });
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      
      if (url.pathname === "/health") {
        return new Response("ok", { 
          status: 200,
          headers: { "content-type": "text/plain" }
        });
      }
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      
      return new Response("Chat Server - Use WebSocket connection", { 
        status: 200,
        headers: { "content-type": "text/plain" }
      });
      
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
};
