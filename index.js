import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", "India", "Indonesia", "Birthday Party", "Heart Lovers","Cat lovers","Chikahan Tambahan", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "The Chatter Room"
];

class PromiseLockManager {
  constructor() {
    this.locks = new Map();
    this.queue = new Map();
  }

  async acquire(resourceId) {
    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, true);
      return () => this.release(resourceId);
    }

    if (!this.queue.has(resourceId)) {
      this.queue.set(resourceId, []);
    }

    return new Promise((resolve) => {
      this.queue.get(resourceId).push(resolve);
    }).then(() => () => this.release(resourceId));
  }

  release(resourceId) {
    const queue = this.queue.get(resourceId);
    if (queue && queue.length > 0) {
      const nextResolve = queue.shift();
      nextResolve();
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
}

class QueueManager {
  constructor(concurrency = 5) {
    this.queue = [];
    this.active = 0;
    this.concurrency = concurrency;
  }

  async add(job) {
    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject });
      this.run();
    });
  }

  async run() {
    if (this.active >= this.concurrency || this.queue.length === 0) return;
    this.active++;
    const { job, resolve, reject } = this.queue.shift();
    try {
      const result = await job();
      resolve(result);
    } catch (error) {
      reject(error);
    } finally {
      this.active--;
      this.run();
    }
  }

  clear() {
    this.queue = [];
  }
}

class RateLimiter {
  constructor(windowMs = 60000, maxRequests = 100) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    setInterval(() => this.cleanup(), windowMs);
  }

  check(userId) {
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
      
      // MUTE STATUS - Map untuk menyimpan status per room
      this.muteStatus = new Map();
      // Set default false untuk semua room
      for (const room of roomList) {
        this.muteStatus.set(room, false);
      }
      
      this.storage = state.storage;
      
      this.lockManager = new PromiseLockManager();
      this.cleanupInProgress = new Set();
      this.clients = new Set();
      this.userToSeat = new Map();
      this.roomClients = new Map();
      this.userCurrentRoom = new Map();
      this.MAX_SEATS = 35;
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.updateKursiBuffer = new Map();
      this.bufferSizeLimit = 50;
      this.userConnections = new Map();

      this._pointBuffer = new Map();
      this._pointFlushTimer = null;
      this._pointFlushDelay = 16;
      this._hasBufferedUpdates = false;

      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      
      this.connectionAttempts = new Map();
      this.maxConnectionAttempts = 10;
      this.connectionBanTime = 30000;
      
      this.safeMode = false;
      this.loadThreshold = 0.9;
      this.lastLoadCheck = 0;
      this.loadCheckInterval = 5000;

      try {
        this.lowcard = new LowCardGameManager(this);
      } catch {
        this.lowcard = null;
      }

      this.gracePeriod = 5000;
      this.disconnectedTimers = new Map();
      this.cleanupQueue = new QueueManager(5);
      this.currentNumber = 1;
      this.maxNumber = 6;
      this.intervalMillis = 15 * 60 * 1000;
      this._nextConnId = 1;
      this._timers = [];

      this._cleanupAllTimers();

      try {
        this.initializeRooms();
      } catch {
        this.createDefaultRoom();
      }

      this.startTimers();
      this.roomCountsCache = new Map();
      this.cacheValidDuration = 2000;
      this.lastCacheUpdate = 0;

      for (const room of roomList) {
        this._pointBuffer.set(room, []);
      }

      this._gracePeriodValidationTimer = null;

    } catch (error) {
      console.error("ChatServer constructor error:", error);
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
      this.MAX_SEATS = 35;
      this.currentNumber = 1;
      this._nextConnId = 1;
      this._timers = [];
      this.lowcard = null;
      this.gracePeriod = 5000;
      this.cleanupQueue = new QueueManager(5);
      
      // MUTE STATUS - tetap inisialisasi
      this.muteStatus = new Map();
      for (const room of roomList) {
        this.muteStatus.set(room, false);
      }
      
      this.storage = state?.storage;
      
      this.rateLimiter = new RateLimiter(60000, 100);
      this.connectionRateLimiter = new RateLimiter(10000, 5);
      this.connectionAttempts = new Map();
      this.safeMode = false;
      this.loadThreshold = 0.9;
      
      this._pointBuffer = new Map();
      this._pointFlushTimer = null;
      this._pointFlushDelay = 16;
      this._hasBufferedUpdates = false;
      
      this._cleanupAllTimers();
      
      this.createDefaultRoom();
    }
  }

  // ========== MUTE STATUS METHODS ==========

  setRoomMute(roomName, isMuted) {
    try {
      if (!roomName || !roomList.includes(roomName)) {
        console.log("[Mute] Room tidak valid:", roomName);
        return false;
      }
      
      // Konversi ke boolean strict
      const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
      
      console.log(`[Mute] SET room ${roomName} = ${muteValue}`);
      
      // Simpan ke MEMORY untuk room ini
      this.muteStatus.set(roomName, muteValue);
      
      // BROADCAST KE ROOM YANG DI-SET SAJA - hanya untuk notifikasi, tidak mempengaruhi fungsi lain
      this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
      
      return true;
    } catch (error) {
      console.error("[Mute] Error set:", error);
      return false;
    }
  }

  getRoomMute(roomName) {
    try {
      if (!roomName || !roomList.includes(roomName)) {
        console.log(`[Mute] Room ${roomName} tidak valid`);
        return false;
      }
      
      // Ambil dari MEMORY, default false jika tidak ada
      const value = this.muteStatus.get(roomName);
      console.log(`[Mute] GET room ${roomName} = ${value}`);
      return value === true;
    } catch (error) {
      console.error("[Mute] Error get:", error);
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

  async withLock(resourceId, operation, timeout = 2000) {
    const release = await this.lockManager.acquire(resourceId);
    let timeoutId;
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new Error(`Lock timeout for ${resourceId}`));
      }, timeout);
    });
    
    try {
      const result = await Promise.race([operation(), timeoutPromise]);
      clearTimeout(timeoutId);
      return result;
    } catch (error) {
      throw error;
    } finally {
      try {
        clearTimeout(timeoutId);
        release();
      } catch {}
    }
  }

  checkAndEnableSafeMode() {
    const now = Date.now();
    if (now - this.lastLoadCheck < this.loadCheckInterval) return;
    this.lastLoadCheck = now;
    const load = this.getServerLoad();
    if (load > this.loadThreshold && !this.safeMode) {
      this.enableSafeMode();
    } else if (load < 0.7 && this.safeMode) {
      this.disableSafeMode();
    }
  }

  enableSafeMode() {
    if (this.safeMode) return;
    this.safeMode = true;
    this.cleanupQueue.concurrency = 2;
    this._pointFlushDelay = 100;
    setTimeout(() => {
      if (this.getServerLoad() < 0.7) {
        this.disableSafeMode();
      }
    }, 60000);
  }

  disableSafeMode() {
    this.safeMode = false;
    this.cleanupQueue.concurrency = 5;
    this._pointFlushDelay = 16;
  }

  schedulePointFlush(room) {
    if (this._pointFlushTimer) return;
    this._pointFlushTimer = setTimeout(() => {
      this._pointFlushTimer = null;
      this.flushBufferedPoints();
    }, this._pointFlushDelay);
  }

  flushBufferedPoints() {
    for (const [room, points] of this._pointBuffer) {
      if (points.length > 0) {
        const batch = points.splice(0, points.length);
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
          try { client.send(message); } catch {}
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
          try { client.send(message); } catch {}
        }
      }
    } catch {}
  }

  scheduleCleanup(userId) {
    try {
      if (!userId) return;
      this._cleanupExpiredTimers();
      this.cancelCleanup(userId);
      const timerId = setTimeout(async () => {
        try {
          this.disconnectedTimers.delete(userId);
          const isStillConnected = await this.isUserStillConnected(userId);
          if (!isStillConnected) {
            await this.withLock(`grace-cleanup-${userId}`, async () => {
              const doubleCheckConnected = await this.isUserStillConnected(userId);
              if (!doubleCheckConnected) {
                await this.forceUserCleanup(userId);
              }
            });
          }
        } catch (error) {}
      }, this.gracePeriod);
      timerId._scheduledTime = Date.now();
      timerId._userId = userId;
      this.disconnectedTimers.set(userId, timerId);
    } catch (error) {}
  }

  _cleanupExpiredTimers() {
    try {
      const now = Date.now();
      const expiredUsers = [];
      for (const [userId, timer] of this.disconnectedTimers) {
        if (!timer) expiredUsers.push(userId);
      }
      for (const userId of expiredUsers) {
        this.disconnectedTimers.delete(userId);
      }
    } catch {}
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

  async isUserStillConnected(userId) {
    if (!userId) return false;
    const userConnections = this.userConnections.get(userId);
    if (!userConnections || userConnections.size === 0) return false;
    for (const conn of userConnections) {
      if (!conn) continue;
      if (conn.readyState !== 1) continue;
      if (conn._isDuplicate) continue;
      if (conn._isClosing) continue;
      if (conn._connectionTime) {
        const connectionAge = Date.now() - conn._connectionTime;
        if (connectionAge > 24 * 60 * 60 * 1000) continue;
      }
      return true;
    }
    return false;
  }

  async executeGracePeriodCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    this.checkAndEnableSafeMode();
    if (this.safeMode) {
      setTimeout(() => { this.executeGracePeriodCleanup(userId); }, 2000);
      return;
    }
    this.cleanupInProgress.add(userId);
    try {
      await this.withLock(`user-cleanup-${userId}`, async () => {
        const isStillConnected = await this.isUserStillConnected(userId);
        if (!isStillConnected) {
          await this.forceUserCleanup(userId);
        }
      });
    } catch (error) {
    } finally {
      this.cleanupInProgress.delete(userId);
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
      this.updateKursiBuffer.set(room, new Map());
      this._pointBuffer.set(room, []);
    } catch {}
  }

  initializeRooms() {
    for (const room of roomList) {
      try {
        const seatMap = new Map();
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          seatMap.set(i, null);
        }
        this.roomSeats.set(room, seatMap);
        const occupancyMap = new Map();
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          occupancyMap.set(i, null);
        }
        this.seatOccupancy.set(room, occupancyMap);
        this.roomClients.set(room, []);
        this.updateKursiBuffer.set(room, new Map());
        this._pointBuffer.set(room, []);
      } catch {}
    }
  }

  startTimers() {
    try {
      this._cleanupTimers();
      this._tickTimer = setInterval(() => { try { this.tick(); } catch {} }, this.intervalMillis);
      this._flushTimer = setInterval(() => { try { if (this.clients.size > 0) this.periodicFlush(); } catch {} }, 50);
      this._consistencyTimer = setInterval(() => { try { if (this.clients.size > 0 && this.getServerLoad() < 0.7) this.checkSeatConsistency(); } catch {} }, 120000);
      this._connectionCleanupTimer = setInterval(() => { try { if (this.getServerLoad() < 0.8) this.cleanupDuplicateConnections(); } catch {} }, 30000);
      this._stuckCleanupTimer = setInterval(() => { try { if (this.getServerLoad() < 0.7) this.cleanupStuckUsers(); } catch {} }, 45000);
      this._memoryCleanupTimer = setInterval(() => { try { this._performMemoryCleanup(); } catch {} }, 60000);
      this._safeModeTimer = setInterval(() => { try { this.checkAndEnableSafeMode(); } catch {} }, 10000);
      this._gracePeriodValidationTimer = setInterval(() => { try { this._validateGracePeriodTimers(); } catch {} }, 1000);
      this._seatValidationTimer = setInterval(() => { try { if (this.getServerLoad() < 0.8) { for (const room of roomList) this.validateSeatConsistency(room); } } catch {} }, 30000);
      this._timers = [this._tickTimer, this._flushTimer, this._consistencyTimer, this._connectionCleanupTimer, this._stuckCleanupTimer, this._memoryCleanupTimer, this._safeModeTimer, this._gracePeriodValidationTimer, this._seatValidationTimer];
    } catch {
      this._timers = [];
    }
  }

  _validateGracePeriodTimers() {
    try {
      const now = Date.now();
      const maxGracePeriod = this.gracePeriod + 1000;
      for (const [userId, timer] of this.disconnectedTimers) {
        if (timer && timer._scheduledTime) {
          const elapsed = now - timer._scheduledTime;
          if (elapsed > maxGracePeriod) {
            clearTimeout(timer);
            this.disconnectedTimers.delete(userId);
            this.executeGracePeriodCleanup(userId);
          }
        }
      }
    } catch (error) {}
  }

  _performMemoryCleanup() {
    try {
      this._cleanupExpiredTimers();
      const deadClients = [];
      for (const client of this.clients) {
        if (!client || client.readyState === 3) deadClients.push(client);
      }
      for (const client of deadClients) {
        this.clients.delete(client);
      }
    } catch {}
  }

  async cleanupStuckUsers() {
    try {
      const allTrackedUsers = new Set();
      for (const [userId] of this.userCurrentRoom) allTrackedUsers.add(userId);
      for (const [userId] of this.userToSeat) allTrackedUsers.add(userId);
      for (const [room, occupancyMap] of this.seatOccupancy) {
        for (const [seat, userId] of occupancyMap) {
          if (userId) allTrackedUsers.add(userId);
        }
      }
      const batchSize = 10;
      const userArray = Array.from(allTrackedUsers);
      for (let i = 0; i < userArray.length; i += batchSize) {
        const batch = userArray.slice(i, i + batchSize);
        await Promise.allSettled(batch.map(async (userId) => {
          try {
            if (this.cleanupInProgress.has(userId)) return;
            if (this.disconnectedTimers.has(userId)) return;
            const isConnected = await this.isUserStillConnected(userId);
            if (!isConnected) {
              await this.withLock(`stuck-cleanup-${userId}`, async () => {
                const doubleCheck = await this.isUserStillConnected(userId);
                if (!doubleCheck) {
                  await this.forceUserCleanup(userId);
                }
              });
            }
          } catch (error) {}
        }));
      }
    } catch (error) {}
  }

  getServerLoad() {
    const activeConnections = Array.from(this.clients).filter(c => c.readyState === 1).length;
    return Math.min(activeConnections / 100, 0.95);
  }

  _cleanupAllTimers() {
    try {
      if (this._timers) {
        for (const timer of this._timers) {
          if (timer) {
            try { clearInterval(timer); clearTimeout(timer); } catch {}
          }
        }
        this._timers = [];
      }
      if (this.disconnectedTimers) {
        for (const timer of this.disconnectedTimers.values()) {
          if (timer) { try { clearTimeout(timer); } catch {} }
        }
        this.disconnectedTimers.clear();
      }
      if (this.cleanupQueue) this.cleanupQueue.clear();
      if (this._pointFlushTimer) {
        clearTimeout(this._pointFlushTimer);
        this._pointFlushTimer = null;
      }
      if (this._gracePeriodValidationTimer) {
        clearInterval(this._gracePeriodValidationTimer);
        this._gracePeriodValidationTimer = null;
      }
    } catch {}
  }

  _cleanupTimers() {
    this._cleanupAllTimers();
  }

  updateRoomCount(room) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return 0;
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const info = seatMap.get(i);
        if (info && info.namauser && info.namauser !== "") count++;
      }
      this.invalidateRoomCache(room);
      this.broadcastRoomUserCount(room);
      return count;
    } catch { return 0; }
  }

  async checkSeatConsistency() {
    try {
      const roomIndex = Math.floor(Math.random() * roomList.length);
      const room = roomList[roomIndex];
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      if (!seatMap || !occupancyMap) return 0;
      let inconsistencies = 0;
      for (let j = 0; j < Math.min(10, this.MAX_SEATS); j++) {
        const seat = Math.floor(Math.random() * this.MAX_SEATS) + 1;
        const seatData = seatMap.get(seat);
        const occupancyUser = occupancyMap.get(seat);
        if (seatData && seatData.namauser && seatData.namauser !== "") {
          if (occupancyUser !== seatData.namauser) {
            inconsistencies++;
            occupancyMap.set(seat, seatData.namauser);
          }
        } else {
          if (occupancyUser) {
            inconsistencies++;
            occupancyMap.set(seat, null);
          }
        }
      }
      return inconsistencies;
    } catch { return 0; }
  }

  async validateSeatConsistency(room) {
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      if (!seatMap || !occupancyMap) return;
      let fixed = 0;
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const occupantId = occupancyMap.get(seat);
        const seatData = seatMap.get(seat);
        if (occupantId && (!seatData || !seatData.namauser || seatData.namauser === "")) {
          if (!seatData) seatMap.set(seat, createEmptySeat());
          else Object.assign(seatData, createEmptySeat());
          occupancyMap.set(seat, null);
          fixed++;
        } else if (!occupantId && seatData && seatData.namauser && seatData.namauser !== "") {
          let isUserOnline = false;
          const connections = this.userConnections.get(seatData.namauser);
          if (connections && connections.size > 0) {
            for (const conn of connections) {
              if (conn && conn.readyState === 1) { isUserOnline = true; break; }
            }
          }
          if (isUserOnline) occupancyMap.set(seat, seatData.namauser);
          else Object.assign(seatData, createEmptySeat());
          fixed++;
        } else if (occupantId && seatData && seatData.namauser && seatData.namauser !== occupantId) {
          if (occupantId) {
            let isOccupantOnline = false;
            const connections = this.userConnections.get(occupantId);
            if (connections && connections.size > 0) {
              for (const conn of connections) {
                if (conn && conn.readyState === 1) { isOccupantOnline = true; break; }
              }
            }
            if (isOccupantOnline) seatData.namauser = occupantId;
            else {
              occupancyMap.set(seat, null);
              Object.assign(seatData, createEmptySeat());
            }
          }
          fixed++;
        }
      }
    } catch (error) {}
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
            if (seatInfo && seatInfo.namauser === userId) {
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
        if (remainingConnections && remainingConnections.size > 0) {
          let hasValidConnection = false;
          for (const conn of remainingConnections) {
            if (conn && conn.readyState === 1 && !conn._isDuplicate && !conn._isClosing) {
              hasValidConnection = true;
              break;
            }
          }
          if (!hasValidConnection) this.userConnections.delete(userId);
        } else {
          this.userConnections.delete(userId);
        }
        if (this.roomClients) {
          for (const [room, clientArray] of this.roomClients) {
            if (clientArray && clientArray.length > 0) {
              let newIndex = 0;
              for (let i = 0; i < clientArray.length; i++) {
                const client = clientArray[i];
                if (!client || client.idtarget !== userId) {
                  clientArray[newIndex++] = client;
                }
              }
              clientArray.length = newIndex;
            }
          }
        }
      });
    } catch (error) {
    } finally {
      this.cleanupInProgress.delete(userId);
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
        if (immediate) {
          Object.assign(seatInfo, createEmptySeat());
          occupancyMap.set(seatNumber, null);
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.updateRoomCount(room);
        }
        if (immediate) {
          this.userToSeat.delete(userId);
        }
      });
    } catch (error) {}
  }

  async cleanupFromRoom(ws, room) {
    if (!ws || !ws.idtarget || !ws.roomname) return;
    try {
      await this.withLock(`room-cleanup-${room}-${ws.idtarget}`, async () => {
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (!seatInfo || seatInfo.room !== room) return;
        
        const { seat } = seatInfo;
        await this.cleanupUserFromSeat(room, seat, ws.idtarget, true);
        
        this._removeFromRoomClients(ws, room);
        
        this._removeUserConnection(ws.idtarget, ws);
        
        this.userCurrentRoom.delete(ws.idtarget);
        
        ws.roomname = undefined;
        ws.numkursi = new Set();
        
        this.userToSeat.delete(ws.idtarget);
        
        this.updateRoomCount(room);
        
        console.log(`[Cleanup] User ${ws.idtarget} dibersihkan dari room ${room}`);
      });
    } catch (error) {
      console.error("[Cleanup] Error:", error);
    }
  }

  clearSeatBuffer(room, seatNumber) {
    try {
      if (seatNumber < 1 || seatNumber > this.MAX_SEATS) return;
      const roomMap = this.updateKursiBuffer.get(room);
      if (roomMap) roomMap.delete(seatNumber);
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
    } catch { return null; }
  }

  async savePointWithRetry(room, seat, x, y, fast) {
    try {
      if (seat < 1 || seat > this.MAX_SEATS) return false;
      const xNum = typeof x === 'number' ? x : parseFloat(x);
      const yNum = typeof y === 'number' ? y : parseFloat(y);
      if (isNaN(xNum) || isNaN(yNum)) return false;
      const updatedSeat = await this.updateSeatAtomic(room, seat, (currentSeat) => {
        currentSeat.lastPoint = { x: xNum, y: yNum, fast: fast || false, timestamp: Date.now() };
        return currentSeat;
      });
      this.broadcastPointDirect(room, seat, xNum, yNum, fast);
      return !!updatedSeat;
    } catch {
      this.broadcastPointDirect(room, seat, x, y, fast);
      return false;
    }
  }
  
  async ensureSeatsData(room) {
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      if (!seatMap || !occupancyMap) return;
      let needsFix = false;
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatMap.has(seat)) { seatMap.set(seat, null); needsFix = true; }
        if (!occupancyMap.has(seat)) { occupancyMap.set(seat, null); needsFix = true; }
      }
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const seatData = seatMap.get(seat);
        const occupancyData = occupancyMap.get(seat);
        if (occupancyData) {
          if (seatData && seatData.namauser && seatData.namauser !== occupancyData) {
            seatMap.set(seat, null);
            needsFix = true;
          }
        }
      }
    } catch (error) {}
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
        if (occupantId === ws.idtarget && seatData && seatData.namauser === ws.idtarget) return i;
      }
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const release = await this.lockManager.acquire(`seat-check-${room}-${i}`);
        try {
          const occupantId = occupancyMap.get(i);
          const seatData = seatMap.get(i);
          const isOccupancyEmpty = occupantId === null;
          const isSeatDataEmpty = !seatData || !seatData.namauser || seatData.namauser === "";
          if (isOccupancyEmpty && isSeatDataEmpty) return i;
        } finally { release(); }
      }
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        if (occupantId === null && seatData && seatData.namauser && seatData.namauser !== "") {
          Object.assign(seatData, createEmptySeat());
          return i;
        }
      }
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupantId = occupancyMap.get(i);
        const seatData = seatMap.get(i);
        if (occupantId && seatData && seatData.namauser === occupantId) {
          let isOccupantOnline = false;
          const connections = this.userConnections.get(occupantId);
          if (connections && connections.size > 0) {
            for (const conn of connections) {
              if (conn && conn.readyState === 1 && !conn._isDuplicate && !conn._isClosing) {
                isOccupantOnline = true;
                break;
              }
            }
          }
          if (!isOccupantOnline) {
            await this.cleanupUserFromSeat(room, i, occupantId, true);
            return i;
          }
        }
      }
      return null;
    } catch (error) { return null; }
  }

  async handleJoinRoom(ws, room) {
    if (!ws || !ws.idtarget) {
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
    
    console.log(`[Join] User ${ws.idtarget} mencoba join room ${room}`);
    console.log(`[Join] Current room sebelumnya: ${ws.roomname}`);
    
    try {
      const roomRelease = await this.lockManager.acquire(`room-join-${room}`);
      try {
        this.cancelCleanup(ws.idtarget);
        await this.ensureSeatsData(room);
        
        const previousRoom = this.userCurrentRoom.get(ws.idtarget);
        
        if (previousRoom) {
          if (previousRoom === room) {
            console.log(`[Join] User ${ws.idtarget} sudah di room ${room}`);
            this.sendAllStateTo(ws, room);
            
            // KIRIM STATUS MUTE SAAT JOIN - agar client tahu status terbaru
            const isMuted = this.getRoomMute(room);
            this.safeSend(ws, ["muteTypeResponse", isMuted, room]);
            
            return true;
          } else {
            console.log(`[Join] User ${ws.idtarget} pindah dari ${previousRoom} ke ${room}`);
            await this.cleanupFromRoom(ws, previousRoom);
            
            console.log(`[Join] Setelah cleanup - roomname: ${ws.roomname}, currentRoom: ${this.userCurrentRoom.get(ws.idtarget)}`);
          }
        }
        
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (seatInfo && seatInfo.room === room) {
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          if (seatMap && occupancyMap) {
            const occupantId = occupancyMap.get(seatInfo.seat);
            if (occupantId === ws.idtarget) {
              ws.roomname = room;
              ws.numkursi = new Set([seatInfo.seat]);
              
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
              
              this._addUserConnection(ws.idtarget, ws);
              this.sendAllStateTo(ws, room);
              
              // KIRIM STATUS MUTE SAAT JOIN
              const isMuted = this.getRoomMute(room);
              this.safeSend(ws, ["muteTypeResponse", isMuted, room]);
              
              return true;
            }
          }
        }
        
        let assignedSeat = null;
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
          const seatRelease = await this.lockManager.acquire(`seat-assign-${room}-${seat}`);
          try {
            const occupancyMap = this.seatOccupancy.get(room);
            if (!occupancyMap) continue;
            
            const occupantId = occupancyMap.get(seat);
            if (occupantId === null) {
              occupancyMap.set(seat, ws.idtarget);
              assignedSeat = seat;
              break;
            }
          } finally { seatRelease(); }
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
        
        // KIRIM STATUS MUTE SAAT JOIN ROOM BARU
        const isMuted = this.getRoomMute(room);
        this.safeSend(ws, ["muteTypeResponse", isMuted, room]);
        
        setTimeout(() => { this.sendAllStateTo(ws, room); }, 100);
        this.updateRoomCount(room);
        
        return true;
        
      } finally { roomRelease(); }
    } catch (error) {
      console.error("[Join] Error:", error);
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
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
      const isStillEmpty = occupantId === null && (!seatData || !seatData.namauser || seatData.namauser === "");
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

  getJumlahRoom() {
    try {
      const now = Date.now();
      if (this.roomCountsCache && (now - this.lastCacheUpdate) < this.cacheValidDuration) return this.roomCountsCache;
      const counts = Object.fromEntries(roomList.map(r => [r, 0]));
      for (const room of roomList) {
        const occupancyMap = this.seatOccupancy.get(room);
        if (!occupancyMap) continue;
        let roomCount = 0;
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const occupantId = occupancyMap.get(i);
          if (occupantId) roomCount++;
        }
        counts[room] = roomCount;
      }
      this.roomCountsCache = counts;
      this.lastCacheUpdate = now;
      return counts;
    } catch { return Object.fromEntries(roomList.map(r => [r, 0])); }
  }

  invalidateRoomCache(room) {
    this.roomCountsCache = null;
  }

  safeSend(ws, arr) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isDuplicate || ws._isClosing) return false;
      if (ws.bufferedAmount > 500000) return false;
      try { ws.send(JSON.stringify(arr)); return true; } catch { return false; }
    } catch { return false; }
  }

  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return 0;
      let sentCount = 0;
      const sentToUsers = new Set();
      for (let i = 0; i < clientArray.length; i++) {
        const client = clientArray[i];
        if (client && client.readyState === 1 && client.roomname === room && !client._isDuplicate && !client._isClosing) {
          if (client.idtarget && sentToUsers.has(client.idtarget)) continue;
          if (this.safeSend(client, msg)) {
            sentCount++;
            if (client.idtarget) sentToUsers.add(client.idtarget);
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
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const info = seatMap.get(i);
        if (info && info.namauser && info.namauser !== "") count++;
      }
      if (this.roomCountsCache) this.roomCountsCache[room] = count;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch {}
  }

  sendAllStateTo(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || ws._isDuplicate || ws._isClosing) return;
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
          lastPointsData.push({ seat: seat, x: info.lastPoint.x || 0, y: info.lastPoint.y || 0, fast: info.lastPoint.fast || false });
        }
      }
      if (Object.keys(allKursiMeta).length > 0) this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
      if (lastPointsData.length > 0) this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      const counts = this.getJumlahRoom();
      const count = counts[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
    } catch (error) {}
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    try {
      await this.withLock(`reconnect-${id}`, async () => {
        this.cancelCleanup(id);
        if (baru === true) {
          await this.cleanupQueue.add(async () => { await this.forceUserCleanup(id); });
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
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          if (seatMap && occupancyMap) {
            const seatData = seatMap.get(seat);
            const occupancyUser = occupancyMap.get(seat);
            if (seatData && seatData.namauser === id && occupancyUser === id) {
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
              this._addUserConnection(id, ws);
              this.sendAllStateTo(ws, room);
              if (seatData.lastPoint) {
                this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast]);
              }
              // KIRIM STATUS MUTE SAAT RECONNECT
              const isMuted = this.getRoomMute(room);
              this.safeSend(ws, ["muteTypeResponse", isMuted, room]);
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
        this.safeSend(ws, ["needJoinRoom"]);
      });
    } catch {
      this.safeSend(ws, ["error", "Reconnection failed, please try joining a room manually"]);
    }
  }

  async cleanupDuplicateConnections() {
    try {
      const userConnectionCount = new Map();
      for (const client of this.clients) {
        if (client && client.idtarget && client.readyState === 1 && !client._isClosing) {
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
        await Promise.allSettled(batch.map(userId => this.handleDuplicateConnections(userId)));
      }
    } catch {}
  }

  async handleDuplicateConnections(userId) {
    if (!userId) return;
    try {
      await this.withLock(`duplicate-connections-${userId}`, async () => {
        const allConnections = [];
        for (const client of this.clients) {
          if (client && client.idtarget === userId && client.readyState === 1 && !client._isClosing) {
            allConnections.push({ client, connectionTime: client._connectionTime || 0, room: client.roomname });
          }
        }
        if (allConnections.length <= 1) return;
        allConnections.sort((a, b) => b.connectionTime - a.connectionTime);
        const connectionsToClose = allConnections.slice(1);
        for (const { client } of connectionsToClose) {
          client._isDuplicate = true;
          client._isClosing = true;
          try { if (client.readyState === 1) this.safeSend(client, ["duplicateConnection", "Another connection was opened with your account"]); } catch {}
          try { if (client.readyState === 1) client.close(1000, "Duplicate connection"); } catch {}
          this.clients.delete(client);
          if (client.roomname) this._removeFromRoomClients(client, client.roomname);
          this._removeUserConnection(userId, client);
        }
        const remainingConnections = new Set();
        for (const client of this.clients) {
          if (client && client.idtarget === userId && client.readyState === 1) remainingConnections.add(client);
        }
        this.userConnections.set(userId, remainingConnections);
      });
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
            const { room, seat } = seatInfo;
            await this.cleanupQueue.add(async () => { await this.cleanupUserFromSeat(room, seat, idtarget, true); });
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
              if (index > -1) clientArray.splice(index, 1);
            }
          }
        }
        this.clients.delete(ws);
        if (ws.readyState === 1) { try { ws.close(1000, "Manual destroy"); } catch {} }
      });
    } catch {
      try { this.clients.delete(ws); this.cancelCleanup(idtarget); this._removeUserConnection(idtarget, ws); } catch {}
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
            if (info && info.namauser === idtarget) {
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
        const clientsToRemove = [];
        for (const client of this.clients) {
          if (client && client.idtarget === idtarget) clientsToRemove.push(client);
        }
        for (const client of clientsToRemove) {
          if (client.readyState === 1) { try { client.close(1000, "Session removed"); } catch {} }
          this.clients.delete(client);
          if (this.roomClients) {
            for (const [room, clientArray] of this.roomClients) {
              if (clientArray) {
                const index = clientArray.indexOf(client);
                if (index > -1) clientArray.splice(index, 1);
              }
            }
          }
        }
      });
    } catch {}
  }

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
    } catch { return []; }
  }

  getOnlineUsersByRoom(roomName) {
    try {
      const users = [];
      const seenUsers = new Set();
      const clientArray = this.roomClients.get(roomName);
      if (clientArray) {
        for (let i = 0; i < clientArray.length; i++) {
          const client = clientArray[i];
          if (client && client.idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
            if (!seenUsers.has(client.idtarget)) {
              users.push(client.idtarget);
              seenUsers.add(client.idtarget);
            }
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
          if (info && info.namauser && info.namauser !== "") {
            const { lastPoint, ...rest } = info;
            updates.push([seat, rest]);
            if (updates.length >= 20) {
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

  periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushBufferedPoints();
    } catch {}
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      const clientsToNotify = [];
      const notifiedUsers = new Set();
      for (const client of this.clients) {
        if (client && client.readyState === 1 && client.roomname && !client._isDuplicate && !client._isClosing) {
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
      try { if (ws.readyState === 1) ws.close(1000, "Normal closure"); } catch {}
      setTimeout(() => { ws.roomname = null; ws.idtarget = null; ws.numkursi = null; }, 1000);
    } catch (error) {
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
      if (raw.length > 50000) { try { ws.close(1009, "Message too large"); } catch {} return; }
      let data;
      try { data = JSON.parse(raw); if (ws.errorCount) ws.errorCount = 0; } catch { 
        ws.errorCount = (ws.errorCount || 0) + 1;
        if (ws.errorCount > 3) { try { ws.close(1008, "Protocol error"); } catch {} }
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
            const isInRoom = currentRoom !== undefined;
            this.safeSend(ws, ["inRoomStatus", isInRoom]);
            break;
          }
          case "rollangak": {
            const roomName = data[1];
            const username = data[2];
            const angka = data[3];
            if (!roomName || !roomList.includes(roomName)) { this.safeSend(ws, ["error", "Invalid room"]); break; }
            this.broadcastToRoom(roomName, ["rollangakBroadcast", roomName, username, angka]);
            break;
          }
          // ===== MUTE METHODS =====
          case "setMuteType": {
            const isMuted = data[1];
            const roomName = data[2];
            
            if (!roomName || !roomList.includes(roomName)) {
              this.safeSend(ws, ["error", "Room tidak valid"]);
              break;
            }
            
            const success = this.setRoomMute(roomName, isMuted);
            const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
            
            // Kirim response ke pengirim
            this.safeSend(ws, ["muteTypeSet", muteValue, success, roomName]);
            
            // BROADCAST KE ROOM YANG DI-SET - semua client di room tahu status berubah
            // Tapi ini HANYA NOTIFIKASI, tidak mempengaruhi fungsi lain
            this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
            
            break;
          }
          
          case "getMuteType": {
            const roomName = data[1];
            
            if (!roomName || !roomList.includes(roomName)) {
              this.safeSend(ws, ["error", "Room tidak valid"]);
              break;
            }
            
            const isMuted = this.getRoomMute(roomName);
            
            // Kirim status ke pengirim saja
            this.safeSend(ws, ["muteTypeResponse", isMuted, roomName]);
            break;
          }
          // ===== END OF MUTE METHODS =====
          
          case "onDestroy": {
            const idtarget = ws.idtarget;
            this.handleOnDestroy(ws, idtarget);
            break;
          }
          case "setIdTarget2": 
            await this.handleSetIdTarget2(ws, data[1], data[2]);
            break;
          case "sendnotif": {
            const [, idtarget, noimageUrl, username, deskripsi] = data;
            const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
            let sent = false;
            for (const client of this.clients) {
              if (client && client.idtarget === idtarget && client.readyState === 1 && !client._isDuplicate && !client._isClosing) {
                if (this.safeSend(client, notif)) { sent = true; break; }
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
                if (this.safeSend(client, out)) break;
              }
            }
            break;
          }
          case "isUserOnline": {
            const username = data[1];
            const tanda = data[2] ?? "";
            let isOnline = false;
            const connections = this.userConnections.get(username);
            if (connections && connections.size > 0) {
              for (const conn of connections) {
                if (conn.readyState === 1 && !conn._isDuplicate && !conn._isClosing) { isOnline = true; break; }
              }
            }
            this.safeSend(ws, ["userOnlineStatus", username, isOnline, tanda]);
            break;
          }
          case "getAllRoomsUserCount": {
            const allCounts = this.getJumlahRoom();
            const result = roomList.map(room => [room, allCounts[room]]);
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
            
            // TIDAK ADA PENGECEKAN MUTE - Biarkan chat tetap berjalan normal
            // Mute hanya untuk notifikasi, tidak mempengaruhi fungsi chat
            
            let isPrimaryConnection = true;
            const userConnections = this.userConnections.get(username);
            if (userConnections && userConnections.size > 0) {
              let earliestConnection = null;
              for (const conn of userConnections) {
                if (conn.readyState === 1 && !conn._isClosing) {
                  if (!earliestConnection || (conn._connectionTime || 0) < (earliestConnection._connectionTime || 0)) {
                    earliestConnection = conn;
                  }
                }
              }
              if (earliestConnection && earliestConnection !== ws) isPrimaryConnection = false;
            }
            if (!isPrimaryConnection) return;
            
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
            const giftData = ["gift", roomname, sender, receiver, giftName, timestamp];
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
            if (ws.roomname === "LowCard 1" || ws.roomname === "LowCard 2") {
              if (this.lowcard) await this.lowcard.handleEvent(ws, data);
              else this.safeSend(ws, ["error", "Game system not available"]);
            }
            break;
          default: break;
        }
      } catch (error) {
        if (ws.readyState === 1) this.safeSend(ws, ["error", "Server error"]);
      }
    } catch (error) {}
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket") return new Response("Expected WebSocket", { status: 426 });
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
        try { Promise.resolve().then(() => { this.handleMessage(ws, ev.data).catch(() => {}); }); } catch (error) {}
      });
      ws.addEventListener("error", (error) => {});
      ws.addEventListener("close", (event) => { Promise.resolve().then(() => { this.safeWebSocketCleanup(ws); }); });
      return new Response(null, { status: 101, webSocket: client });
    } catch (error) { return new Response("Internal server error", { status: 500 }); }
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
        return new Response("ok", { status: 200, headers: { "content-type": "text/plain", "cache-control": "no-cache" } });
      }
      return new Response("WebSocket endpoint", { status: 200, headers: { "content-type": "text/plain" } });
    } catch (error) { return new Response("Server error", { status: 500 }); }
  }
};
