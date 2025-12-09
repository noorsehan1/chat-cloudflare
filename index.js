import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "HINDI", "Indonesia", "MEPHISTOPHELES", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// ============ QUEUE MANAGER UNTUK CONTROLLED CONCURRENCY ============
class QueueManager {
  constructor(concurrency = 3) {
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

// ============ DEBOUNCED CLEANUP MANAGER ============
class DebouncedCleanupManager {
  constructor(server, interval = 2000) {
    this.server = server;
    this.interval = interval;
    this.pendingCleanups = new Set();
    this.timer = null;
    this.start();
  }

  start() {
    if (this.timer) return;
    this.timer = setInterval(() => this.processCleanups(), this.interval);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  schedule(userId) {
    if (!userId) return;
    this.pendingCleanups.add(userId);
  }

  async processCleanups() {
    if (this.pendingCleanups.size === 0) return;

    const usersToCleanup = Array.from(this.pendingCleanups);
    this.pendingCleanups.clear();

    // Process in batches to avoid overwhelming the server
    const batchSize = 5;
    for (let i = 0; i < usersToCleanup.length; i += batchSize) {
      const batch = usersToCleanup.slice(i, i + batchSize);
      await Promise.allSettled(
        batch.map(userId => this.server.executeGracePeriodCleanup(userId))
      );
    }
  }
}

class LockManager {
  constructor() {
    this.locks = new Map();
    this.lockTimeout = 3000; // Reduced from 5000ms
  }

  async acquire(resourceId) {
    const startTime = Date.now();
    
    // Fast path: check without waiting first
    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, true);
      return () => this.release(resourceId);
    }
    
    // Wait with incremental backoff
    let waitTime = 10;
    while (this.locks.has(resourceId)) {
      if (Date.now() - startTime > this.lockTimeout) {
        console.warn(`[LockManager] Timeout on ${resourceId}, forcing release`);
        this.locks.delete(resourceId);
        throw new Error(`Timeout waiting for lock on ${resourceId}`);
      }
      await new Promise(resolve => setTimeout(resolve, waitTime));
      waitTime = Math.min(waitTime * 1.5, 50); // Cap at 50ms
    }
    
    this.locks.set(resourceId, true);
    return () => this.release(resourceId);
  }

  release(resourceId) {
    this.locks.delete(resourceId);
  }

  hasLock(resourceId) {
    return this.locks.has(resourceId);
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

      // Initialize dengan optimasi
      this.lockManager = new LockManager();
      this.cleanupInProgress = new Set();
      this.clients = new Set();
      this.userToSeat = new Map();
      
      // OPTIMIZED: Gunakan array untuk roomClients (lebih cepat untuk iterasi)
      this.roomClients = new Map();
      
      // NEW: User current room tracking untuk O(1) cleanup
      this.userCurrentRoom = new Map(); // userId -> currentRoom
      
      this.MAX_SEATS = 35;
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      
      // OPTIMIZED: Buffer dengan size limit
      this.updateKursiBuffer = new Map();
      this.bufferSizeLimit = 100;
      
      // Tracking untuk multiple connections
      this.userConnections = new Map();
      
      // Game managers
      try {
        this.lowcard = new LowCardGameManager(this);
      } catch (lowcardError) {
        console.error("[ChatServer] Error creating LowCardGameManager:", lowcardError);
        this.lowcard = null;
      }
      
      try {
        this.vipManager = new VipBadgeManager(this);
      } catch (vipError) {
        console.error("[ChatServer] Error creating VipBadgeManager:", vipError);
        this.vipManager = null;
      }

      this.gracePeriod = 5000;
      this.disconnectedTimers = new Map();
      
      // NEW: Debounced cleanup manager
      this.debouncedCleanup = new DebouncedCleanupManager(this, 2000);
      
      // NEW: Queue untuk controlled concurrency
      this.cleanupQueue = new QueueManager(3); // Max 3 concurrent cleanups
      
      this.currentNumber = 1;
      this.maxNumber = 6;
      this.intervalMillis = 15 * 60 * 1000;
      this._nextConnId = 1;
      
      // Initialize timers array
      this._timers = [];

      // Initialize rooms
      try {
        this.initializeRooms();
      } catch (roomError) {
        console.error("[ChatServer] Error initializing rooms:", roomError);
        this.createDefaultRoom();
      }

      // Start timers
      this.startTimers();
      
      // Cache untuk room user counts
      this.roomCountsCache = new Map();
      this.cacheValidDuration = 5000; // 5 seconds
      this.lastCacheUpdate = 0;
      
      console.log("[ChatServer] Initialized with optimizations");

    } catch (error) {
      console.error("[ChatServer] CRITICAL ERROR in constructor:", error);
      // Emergency minimal setup
      this.clients = new Set();
      this.userToSeat = new Map();
      this.userCurrentRoom = new Map();
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.roomClients = new Map();
      this.updateKursiBuffer = new Map();
      this.userConnections = new Map();
      this.disconnectedTimers = new Map();
      this.lockManager = new LockManager();
      this.cleanupInProgress = new Set();
      this.MAX_SEATS = 35;
      this.currentNumber = 1;
      this._nextConnId = 1;
      this._timers = [];
      this.lowcard = null;
      this.vipManager = null;
      
      this.debouncedCleanup = new DebouncedCleanupManager(this);
      this.cleanupQueue = new QueueManager(3);
      
      this.createDefaultRoom();
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
      this.roomClients.set(room, []); // Array, bukan Set
      this.updateKursiBuffer.set(room, new Map());
    } catch (error) {
      console.error("[ChatServer] Error creating default room:", error);
    }
  }

  initializeRooms() {
    for (const room of roomList) {
      try {
        const seatMap = new Map();
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          seatMap.set(i, createEmptySeat());
        }
        this.roomSeats.set(room, seatMap);

        const occupancyMap = new Map();
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          occupancyMap.set(i, null);
        }
        this.seatOccupancy.set(room, occupancyMap);

        this.roomClients.set(room, []); // OPTIMIZED: Array, bukan Set
        this.updateKursiBuffer.set(room, new Map());
      } catch (roomError) {
        console.error(`[ChatServer] Error initializing room ${room}:`, roomError);
      }
    }
  }

  startTimers() {
    try {
      // Clear existing timers first
      this._cleanupTimers();
      
      // Number rotation timer
      this._tickTimer = setInterval(() => {
        try {
          this.tick();
        } catch (tickError) {
          console.error("[ChatServer] Error in tick timer:", tickError);
        }
      }, this.intervalMillis);

      // Buffer flush timer dengan interval lebih panjang
      this._flushTimer = setInterval(() => {
        try {
          if (this.clients.size > 0) this.periodicFlush();
        } catch (flushError) {
          console.error("[ChatServer] Error in flush timer:", flushError);
        }
      }, 100); // Increased from 50ms

      // Consistency check timer dengan interval lebih panjang
      this._consistencyTimer = setInterval(() => {
        try {
          if (this.clients.size > 0 && this.getServerLoad() < 0.7) {
            this.checkSeatConsistency();
          }
        } catch (consistencyError) {
          console.error("[ChatServer] Error in consistency check:", consistencyError);
        }
      }, 60000); // Increased from 30000ms

      // Connection cleanup timer
      this._connectionCleanupTimer = setInterval(() => {
        try {
          if (this.getServerLoad() < 0.8) {
            this.cleanupDuplicateConnections();
          }
        } catch (cleanupError) {
          console.error("[ChatServer] Error in connection cleanup:", cleanupError);
        }
      }, 15000); // Increased from 10000ms

      this._timers = [this._tickTimer, this._flushTimer, this._consistencyTimer, this._connectionCleanupTimer];
      
    } catch (error) {
      console.error("[ChatServer] Error starting timers:", error);
      this._timers = [];
    }
  }

  // NEW: Helper untuk mendapatkan server load
  getServerLoad() {
    // Simple heuristic based on active connections
    const activeConnections = Array.from(this.clients).filter(c => c.readyState === 1).length;
    return Math.min(activeConnections / 100, 0.95); // Assume 100 connections = 95% load
  }

  _cleanupTimers() {
    try {
      if (!this._timers) {
        this._timers = [];
        return;
      }
      
      for (const timer of this._timers) {
        if (timer) {
          try {
            clearInterval(timer);
            clearTimeout(timer);
          } catch (timerError) {
            // Ignore
          }
        }
      }
      this._timers = [];

      // Clear disconnected timers
      if (this.disconnectedTimers) {
        for (const timer of this.disconnectedTimers.values()) {
          if (timer) {
            try {
              clearTimeout(timer);
            } catch (timerError) {
              // Ignore
            }
          }
        }
        this.disconnectedTimers.clear();
      }

      // Stop debounced cleanup
      if (this.debouncedCleanup) {
        this.debouncedCleanup.stop();
      }

      // Clear cleanup queue
      if (this.cleanupQueue) {
        this.cleanupQueue.clear();
      }
    } catch (error) {
      console.error("[ChatServer] Error cleaning up timers:", error);
    }
  }

  // ============ NEW: REAL-TIME ROOM COUNT UPDATER ============
  updateRoomCount(room) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return 0;
      
      // Hitung user yang aktif di room
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const info = seatMap.get(i);
        if (info && info.namauser && info.namauser !== "") {
          count++;
        }
      }
      
      // Update cache dan broadcast
      this.invalidateRoomCache(room);
      this.broadcastRoomUserCount(room);
      
      return count;
    } catch (error) {
      console.error(`[ChatServer] Error in updateRoomCount:`, error);
      return 0;
    }
  }

  // ============ OPTIMIZED CONSISTENCY CHECK ============
  async checkSeatConsistency() {
    try {
      // Check only one room per iteration to spread load
      const roomIndex = Math.floor(Math.random() * roomList.length);
      const room = roomList[roomIndex];
      
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      
      if (!seatMap || !occupancyMap) return [];

      let inconsistencies = 0;
      
      // Check only 10 random seats per room
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
      
      if (inconsistencies > 0) {
        console.warn(`[ChatServer] Found ${inconsistencies} inconsistencies in room ${room}`);
      }
      
      return inconsistencies;
    } catch (error) {
      console.error(`[ChatServer] Error in checkSeatConsistency:`, error);
      return 0;
    }
  }

  // ============ OPTIMIZED LOCK MANAGEMENT ============
  async withLock(resourceId, operation, timeout = 3000) {
    const release = await this.lockManager.acquire(resourceId);
    try {
      const startTime = Date.now();
      const result = await operation();
      const duration = Date.now() - startTime;
      
      if (duration > 100) {
        console.warn(`[ChatServer] Lock ${resourceId} held for ${duration}ms`);
      }
      
      return result;
    } finally {
      try {
        release();
      } catch (releaseError) {
        console.error(`[ChatServer] Error releasing lock ${resourceId}:`, releaseError);
      }
    }
  }

  // ============ OPTIMIZED USER MANAGEMENT ============
  scheduleCleanup(userId) {
    try {
      if (!userId) return;
      
      // Gunakan debounced cleanup, bukan timer per user
      this.debouncedCleanup.schedule(userId);
      
      // Cancel individual timer jika ada
      const oldTimer = this.disconnectedTimers.get(userId);
      if (oldTimer) {
        try {
          clearTimeout(oldTimer);
        } catch (clearError) {
          // Ignore
        }
        this.disconnectedTimers.delete(userId);
      }
    } catch (error) {
      console.error(`[ChatServer] Error scheduling cleanup for ${userId}:`, error);
    }
  }

  cancelCleanup(userId) {
    try {
      if (!userId) return;
      
      const timer = this.disconnectedTimers.get(userId);
      if (timer) {
        try {
          clearTimeout(timer);
        } catch (clearError) {
          // Ignore
        }
        this.disconnectedTimers.delete(userId);
      }
    } catch (error) {
      console.error(`[ChatServer] Error canceling cleanup for ${userId}:`, error);
    }
  }

  async executeGracePeriodCleanup(userId) {
    if (!userId) return;
    
    try {
      // Gunakan queue untuk controlled concurrency
      await this.cleanupQueue.add(async () => {
        // Quick check: is user still connected?
        let isStillConnected = false;
        for (const client of this.clients) {
          if (client && client.idtarget === userId && client.readyState === 1 && !client._isDuplicate) {
            isStillConnected = true;
            break;
          }
        }
        
        if (isStillConnected) {
          return;
        }
        
        // User offline, proceed with cleanup
        await this.withLock(`user-cleanup-${userId}`, async () => {
          // Double check in lock
          let finalCheck = false;
          for (const client of this.clients) {
            if (client && client.idtarget === userId && client.readyState === 1 && !client._isDuplicate) {
              finalCheck = true;
              break;
            }
          }
          
          if (!finalCheck) {
            await this.forceUserCleanup(userId);
          }
        });
      });
    } catch (error) {
      console.error(`[ChatServer] Error in grace period cleanup for ${userId}:`, error);
    }
  }

  async forceUserCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    
    this.cleanupInProgress.add(userId);
    try {
      this.cancelCleanup(userId);

      // OPTIMIZED: Get current room first (O(1) operation)
      const currentRoom = this.userCurrentRoom.get(userId);
      const roomsToCheck = currentRoom ? [currentRoom] : roomList;

      // Collect seats to cleanup
      const seatsToCleanup = [];
      for (const room of roomsToCheck) {
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;
        
        // OPTIMIZED: Direct iteration without Map.entries() overhead
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const seatInfo = seatMap.get(i);
          if (seatInfo && seatInfo.namauser === userId) {
            seatsToCleanup.push({ room, seatNumber: i });
          }
        }
      }

      // Cleanup seats with controlled parallelism
      const cleanupPromises = seatsToCleanup.map(({ room, seatNumber }) =>
        this.cleanupUserFromSeat(room, seatNumber, userId, true)
      );
      
      // Use Promise.allSettled instead of Promise.all
      await Promise.allSettled(cleanupPromises);

      // Remove from tracking
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      this.userConnections.delete(userId);

      // Remove from room clients
      if (this.roomClients) {
        for (const [room, clientArray] of this.roomClients) {
          if (clientArray && clientArray.length > 0) {
            // OPTIMIZED: Filter array in place
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

    } catch (error) {
      console.error(`[ChatServer] Error in forceUserCleanup for ${userId}:`, error);
    } finally {
      this.cleanupInProgress.delete(userId);
    }
  }

  async cleanupUserFromSeat(room, seatNumber, userId, immediate = true) {
    try {
      await this.withLock(`seat-${room}-${seatNumber}`, async () => {
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        if (!seatMap || !occupancyMap) return;

        const seatInfo = seatMap.get(seatNumber);
        if (!seatInfo || seatInfo.namauser !== userId) return;

        // Remove VIP badge if exists
        if (seatInfo.viptanda > 0 && this.vipManager) {
          try {
            await this.vipManager.removeVipBadge(room, seatNumber);
          } catch (vipError) {
            console.error(`[ChatServer] Error removing VIP badge:`, vipError);
          }
        }

        if (immediate) {
          // Reset seat
          Object.assign(seatInfo, createEmptySeat());
          
          // Clear occupancy
          occupancyMap.set(seatNumber, null);
          
          // Clear buffer
          this.clearSeatBuffer(room, seatNumber);
          
          // Broadcast removal
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          
          // Update user count with cache invalidation
          this.updateRoomCount(room);
        }

        if (immediate) {
          this.userToSeat.delete(userId);
        }
      });
    } catch (error) {
      console.error(`[ChatServer] Error in cleanupUserFromSeat:`, error);
    }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws || !ws.idtarget || !ws.roomname) return;
    
    try {
      await this.withLock(`room-cleanup-${room}-${ws.idtarget}`, async () => {
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (!seatInfo || seatInfo.room !== room) return;
        
        const { seat } = seatInfo;
        
        // Cleanup from seat
        await this.cleanupUserFromSeat(room, seat, ws.idtarget, true);
        
        // Remove from room clients (OPTIMIZED array removal)
        const clientArray = this.roomClients.get(room);
        if (clientArray) {
          const index = clientArray.indexOf(ws);
          if (index > -1) {
            clientArray.splice(index, 1);
          }
        }
        
        // Update user connections
        const userConnections = this.userConnections.get(ws.idtarget);
        if (userConnections) {
          userConnections.delete(ws);
          if (userConnections.size === 0) {
            this.userConnections.delete(ws.idtarget);
          }
        }
        
        // Update user current room tracking
        this.userCurrentRoom.delete(ws.idtarget);
        
        // Cleanup VIP badges
        if (this.vipManager) {
          try {
            await this.vipManager.cleanupUserVipBadges(ws.idtarget);
          } catch (vipError) {
            console.error(`[ChatServer] Error cleaning VIP badges:`, vipError);
          }
        }
        
        // Reset client state
        ws.roomname = undefined;
        ws.numkursi = new Set();
        this.userToSeat.delete(ws.idtarget);
        
        // Broadcast updated user count
        this.updateRoomCount(room);
      });
    } catch (error) {
      console.error(`[ChatServer] Error in cleanupFromRoom:`, error);
    }
  }

  // ============ OPTIMIZED SEAT MANAGEMENT ============
  clearSeatBuffer(room, seatNumber) {
    try {
      const roomMap = this.updateKursiBuffer.get(room);
      if (roomMap) roomMap.delete(seatNumber);
    } catch (error) {
      console.error(`[ChatServer] Error clearing seat buffer:`, error);
    }
  }

  async updateSeatAtomic(room, seatNumber, updateFn) {
    try {
      return await this.withLock(`seat-update-${room}-${seatNumber}`, () => {
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        
        if (!seatMap || !occupancyMap) return null;
        
        // Get current seat
        let currentSeat = seatMap.get(seatNumber);
        if (!currentSeat) {
          currentSeat = createEmptySeat();
          seatMap.set(seatNumber, currentSeat);
        }
        
        // Apply update
        const updatedSeat = updateFn(currentSeat);
        updatedSeat.lastUpdated = Date.now();
        
        // Update occupancy
        if (updatedSeat.namauser && updatedSeat.namauser !== "") {
          occupancyMap.set(seatNumber, updatedSeat.namauser);
        } else {
          occupancyMap.set(seatNumber, null);
        }
        
        seatMap.set(seatNumber, updatedSeat);
        
        // Update buffer with size limit
        const buffer = this.updateKursiBuffer.get(room);
        if (buffer && updatedSeat.namauser) {
          if (buffer.size >= this.bufferSizeLimit) {
            // Remove oldest entry
            const firstKey = buffer.keys().next().value;
            if (firstKey) buffer.delete(firstKey);
          }
          
          const { lastPoint, lastUpdated, ...bufferInfo } = updatedSeat;
          buffer.set(seatNumber, bufferInfo);
        }
        
        return updatedSeat;
      });
    } catch (error) {
      console.error(`[ChatServer] Error in updateSeatAtomic:`, error);
      return null;
    }
  }

  // ============ OPTIMIZED FIND EMPTY SEAT (NO NESTED LOCKS) ============
  async findEmptySeat(room, ws) {
    if (!room || !ws || !ws.idtarget) return null;
    
    try {
      // STEP 1: Get all necessary data WITHOUT holding locks during computation
      const occupancyMap = this.seatOccupancy.get(room);
      const seatMap = this.roomSeats.get(room);
      
      if (!occupancyMap || !seatMap) return null;

      // STEP 2: Find candidate seat WITHOUT nested locks
      let candidateSeat = null;
      
      // Check if user already has a seat
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        if (occupancyMap.get(i) === ws.idtarget) {
          const seatData = seatMap.get(i);
          if (seatData && seatData.namauser === ws.idtarget) {
            // Quick consistency check
            await this.withLock(`seat-verify-quick-${room}-${i}`, async () => {
              const verifySeatData = seatMap.get(i);
              const verifyOccupancy = occupancyMap.get(i);
              if (verifySeatData && verifySeatData.namauser === ws.idtarget && 
                  verifyOccupancy === ws.idtarget) {
                candidateSeat = i;
              }
            });
            if (candidateSeat) return candidateSeat;
          }
        }
      }
      
      // Find empty seat
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        if (occupancyMap.get(i) === null) {
          const seatData = seatMap.get(i);
          if (!seatData || seatData.namauser === "") {
            candidateSeat = i;
            break;
          }
        }
      }
      
      if (candidateSeat) return candidateSeat;
      
      // Check disconnected users
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupiedBy = occupancyMap.get(i);
        if (occupiedBy && occupiedBy !== ws.idtarget) {
          // Quick online check WITHOUT lock first
          let isOccupantOnline = false;
          for (const client of this.clients) {
            if (client && client.idtarget === occupiedBy && 
                client.readyState === 1 && !client._isDuplicate) {
              isOccupantOnline = true;
              break;
            }
          }
          
          if (!isOccupantOnline) {
            const seatData = seatMap.get(i);
            if (seatData && seatData.namauser === occupiedBy) {
              // Acquire lock ONLY for cleanup
              await this.cleanupUserFromSeat(room, i, occupiedBy, true);
              return i;
            }
          }
        }
      }
      
      return null;
    } catch (error) {
      console.error(`[ChatServer] Error in findEmptySeat for user ${ws.idtarget}:`, error);
      return null;
    }
  }

  // ============ OPTIMIZED ROOM MANAGEMENT ============
  async handleJoinRoom(ws, room) {
    if (!ws || !ws.idtarget) {
      this.safeSend(ws, ["error", "User ID not set"]);
      return false;
    }
    
    if (!roomList.includes(room)) {
      this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    try {
      return await this.withLock(`join-room-${room}-${ws.idtarget}`, async () => {
        // Cancel cleanup
        this.cancelCleanup(ws.idtarget);
        
        // OPTIMIZED: Cleanup only from previous room (O(1) operation)
        const previousRoom = this.userCurrentRoom.get(ws.idtarget);
        if (previousRoom && previousRoom !== room) {
          await this.cleanupFromRoom(ws, previousRoom);
        }
        
        // Find empty seat
        const seat = await this.findEmptySeat(room, ws);
        if (!seat) {
          this.safeSend(ws, ["roomFull", room]);
          return false;
        }
        
        // Update all state
        await this.withLock(`seat-assign-${room}-${seat}`, async () => {
          // Update occupancy
          const occupancyMap = this.seatOccupancy.get(room);
          if (occupancyMap) {
            occupancyMap.set(seat, ws.idtarget);
          }
          
          // Update user tracking
          this.userToSeat.set(ws.idtarget, { room, seat });
          this.userCurrentRoom.set(ws.idtarget, room); // O(1) tracking
          ws.roomname = room;
          ws.numkursi = new Set([seat]);
          
          // Add to room clients (OPTIMIZED array push)
          const clientArray = this.roomClients.get(room);
          if (clientArray && !clientArray.includes(ws)) {
            clientArray.push(ws);
          }
          
          // Update user connections
          let userConnections = this.userConnections.get(ws.idtarget);
          if (!userConnections) {
            userConnections = new Set();
            this.userConnections.set(ws.idtarget, userConnections);
          }
          userConnections.add(ws);
          
          console.log(`[ChatServer] User ${ws.idtarget} assigned to seat ${seat} in room ${room}`);
        });
        
        // Send state updates
        this.sendAllStateTo(ws, room);
        
        // PERBAIKAN: Update jumlah room SEBELUM broadcast
        this.updateRoomCount(room);
        
        // Send join confirmation
        this.safeSend(ws, ["rooMasuk", seat, room]);
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        
        // Send VIP badges
        if (this.vipManager) {
          try {
            await this.vipManager.getAllVipBadges(ws, room);
          } catch (vipError) {
            console.error(`[ChatServer] Error getting VIP badges:`, vipError);
          }
        }
        
        return true;
      });
    } catch (error) {
      console.error(`[ChatServer] Error in handleJoinRoom for ${ws.idtarget}:`, error);
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }

  // ============ OPTIMIZED CACHE FOR ROOM COUNTS ============
  getJumlahRoom() {
    try {
      // Check cache validity
      const now = Date.now();
      if (this.roomCountsCache && 
          (now - this.lastCacheUpdate) < this.cacheValidDuration) {
        return this.roomCountsCache;
      }
      
      // Recalculate
      const counts = Object.fromEntries(roomList.map(r => [r, 0]));
      
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;
        
        // Count users in room
        let roomCount = 0;
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const info = seatMap.get(i);
          if (info && info.namauser && info.namauser !== "") {
            roomCount++;
          }
        }
        counts[room] = roomCount;
      }
      
      // Update cache
      this.roomCountsCache = counts;
      this.lastCacheUpdate = now;
      
      return counts;
    } catch (error) {
      console.error(`[ChatServer] Error in getJumlahRoom:`, error);
      return Object.fromEntries(roomList.map(r => [r, 0]));
    }
  }

  invalidateRoomCache(room) {
    this.roomCountsCache = null;
  }

  // ============ OPTIMIZED MESSAGE HANDLING ============
  safeSend(ws, arr) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isDuplicate) return false;
      
      // Check buffer more efficiently
      if (ws.bufferedAmount > 1000000) { // Reduced from 5000000
        return false;
      }
      
      try {
        ws.send(JSON.stringify(arr));
        return true;
      } catch (error) {
        return false;
      }
    } catch (error) {
      return false;
    }
  }

  // ============ OPTIMIZED BROADCAST (ARRAY ITERATION) ============
  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return 0;
      
      let sentCount = 0;
      const sentToUsers = new Set();
      
      // OPTIMIZED: Direct array iteration (faster than Set iteration)
      for (let i = 0; i < clientArray.length; i++) {
        const client = clientArray[i];
        if (client && client.readyState === 1 && client.roomname === room && !client._isDuplicate) {
          // Prevent duplicate sends to same user
          if (client.idtarget && sentToUsers.has(client.idtarget)) {
            continue;
          }
          
          if (this.safeSend(client, msg)) {
            sentCount++;
            if (client.idtarget) {
              sentToUsers.add(client.idtarget);
            }
          }
        }
      }
      
      return sentCount;
    } catch (error) {
      console.error(`[ChatServer] Error in broadcastToRoom:`, error);
      return 0;
    }
  }

  broadcastRoomUserCount(room) {
    try {
      if (!room || !roomList.includes(room)) return;
      
      // Hitung jumlah user yang aktif di room
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      
      let count = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const info = seatMap.get(i);
        if (info && info.namauser && info.namauser !== "") {
          count++;
        }
      }
      
      // PERBAIKAN: Simpan ke cache
      if (this.roomCountsCache) {
        this.roomCountsCache[room] = count;
      }
      
      // Broadcast ke semua user di room
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
      
    } catch (error) {
      console.error(`[ChatServer] Error in broadcastRoomUserCount:`, error);
    }
  }

  sendAllStateTo(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || ws._isDuplicate) return;
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      const allKursiMeta = {};
      const lastPointsData = [];

      // OPTIMIZED: Pre-allocate arrays for batch sending
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info || !info.namauser || info.namauser === "") continue;
        
        allKursiMeta[seat] = {
          noimageUrl: info.noimageUrl,
          namauser: info.namauser,
          color: info.color,
          itembawah: info.itembawah,
          itematas: info.itematas,
          vip: info.vip,
          viptanda: info.viptanda
        };

        if (info.lastPoint) {
          lastPointsData.push({
            seat: seat,
            x: info.lastPoint.x,
            y: info.lastPoint.y,
            fast: info.lastPoint.fast
          });
        }
      }

      // Send data in larger batches
      if (Object.keys(allKursiMeta).length > 0) {
        this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
      }

      if (lastPointsData.length > 0) {
        // Send points in batches of 10
        for (let i = 0; i < lastPointsData.length; i += 10) {
          const batch = lastPointsData.slice(i, i + 10);
          this.safeSend(ws, ["allPointsList", room, batch]);
        }
      }

      const counts = this.getJumlahRoom();
      const count = counts[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);
    } catch (error) {
      console.error(`[ChatServer] Error in sendAllStateTo:`, error);
    }
  }

  // ============ OPTIMIZED EVENT HANDLERS ============
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    
    try {
      await this.withLock(`reconnect-${id}`, async () => {
        if (baru === true) {
          // Use queue for cleanup
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
        
        // Check for valid seat
        const seatInfo = this.userToSeat.get(id);
        
        if (seatInfo) {
          const { room, seat } = seatInfo;
          
          // Quick verification without heavy locking
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          
          if (seatMap && occupancyMap) {
            const seatData = seatMap.get(seat);
            const occupancyUser = occupancyMap.get(seat);
            
            if (seatData && seatData.namauser === id && occupancyUser === id) {
              // Valid seat - reconnect
              this.cancelCleanup(id);
              
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) {
                clientArray.push(ws);
              }
              
              let userConnections = this.userConnections.get(id);
              if (!userConnections) {
                userConnections = new Set();
                this.userConnections.set(id, userConnections);
              }
              userConnections.add(ws);
              
              this.sendAllStateTo(ws, room);
              this.updateRoomCount(room);
              
              //this.safeSend(ws, ["rooMasuk", seat, room]);
              this.safeSend(ws, ["currentNumber", this.currentNumber]);
              
              if (this.vipManager) {
                try {
                  await this.vipManager.getAllVipBadges(ws, room);
                } catch (vipError) {
                  console.error(`[ChatServer] Error getting VIP badges:`, vipError);
                }
              }
              
              console.log(`[ChatServer] User ${id} reconnected to seat ${seat} in room ${room}`);
              return;
            }
          }
          
          // Invalid data - cleanup
          console.warn(`[ChatServer] Inconsistent seat data for ${id}, cleaning up`);
          this.userToSeat.delete(id);
          this.userCurrentRoom.delete(id);
          if (seatInfo.room) {
            await this.cleanupQueue.add(async () => {
              await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, id, true);
            });
          }
        }
        
        // No valid seat - start fresh
        this.safeSend(ws, ["needJoinRoom"]);
      });
    } catch (error) {
      console.error(`[ChatServer] Error in handleSetIdTarget2 for ${id}:`, error);
      this.safeSend(ws, ["error", "Reconnection failed, please try joining a room manually"]);
    }
  }

  // ============ MULTIPLE CONNECTION MANAGEMENT ============
  async cleanupDuplicateConnections() {
    try {
      const now = Date.now();
      const userConnectionCount = new Map();
      
      // Count active connections per user
      for (const client of this.clients) {
        if (client && client.idtarget && client.readyState === 1) {
          const count = userConnectionCount.get(client.idtarget) || 0;
          userConnectionCount.set(client.idtarget, count + 1);
        }
      }
      
      // Find users with multiple connections
      const duplicateUsers = [];
      for (const [userId, count] of userConnectionCount) {
        if (count > 1) {
          duplicateUsers.push(userId);
        }
      }
      
      // Process in batches
      const batchSize = 5;
      for (let i = 0; i < duplicateUsers.length; i += batchSize) {
        const batch = duplicateUsers.slice(i, i + batchSize);
        await Promise.allSettled(
          batch.map(userId => this.handleDuplicateConnections(userId))
        );
      }
    } catch (error) {
      console.error(`[ChatServer] Error in cleanupDuplicateConnections:`, error);
    }
  }

  async handleDuplicateConnections(userId) {
    if (!userId) return;
    
    try {
      await this.withLock(`duplicate-connections-${userId}`, async () => {
        const allConnections = [];
        
        // Collect all connections for this user
        for (const client of this.clients) {
          if (client && client.idtarget === userId && client.readyState === 1) {
            allConnections.push({
              client,
              connectionTime: client._connectionTime || 0,
              room: client.roomname
            });
          }
        }
        
        if (allConnections.length <= 1) return;
        
        console.warn(`[ChatServer] User ${userId} has ${allConnections.length} connections`);
        
        // Sort by connection time (newest first)
        allConnections.sort((a, b) => b.connectionTime - a.connectionTime);
        
        // Keep only the newest connection, close others
        const connectionsToClose = allConnections.slice(1);
        
        for (const { client } of connectionsToClose) {
          console.log(`[ChatServer] Closing duplicate connection for ${userId}`);
          
          client._isDuplicate = true;
          
          try {
            if (client.readyState === 1) {
              this.safeSend(client, ["duplicateConnection", "Another connection was opened with your account"]);
            }
          } catch (sendError) {
            // Ignore
          }
          
          try {
            if (client.readyState === 1) {
              client.close(1000, "Duplicate connection");
            }
          } catch (closeError) {
            // Ignore
          }
          
          // Cleanup from collections
          this.clients.delete(client);
          if (client.roomname) {
            const clientArray = this.roomClients.get(client.roomname);
            if (clientArray) {
              const index = clientArray.indexOf(client);
              if (index > -1) {
                clientArray.splice(index, 1);
              }
            }
          }
        }
        
        // Update userConnections tracking
        const remainingConnections = new Set();
        for (const client of this.clients) {
          if (client && client.idtarget === userId && client.readyState === 1) {
            remainingConnections.add(client);
          }
        }
        this.userConnections.set(userId, remainingConnections);
      });
    } catch (error) {
      console.error(`[ChatServer] Error handling duplicate connections for ${userId}:`, error);
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
        
        // Update user connections
        const userConnections = this.userConnections.get(idtarget);
        if (userConnections) {
          userConnections.delete(ws);
          if (userConnections.size === 0) {
            this.userConnections.delete(idtarget);
          }
        }
        
        // Remove from room clients
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
          try {
            ws.close(1000, "Manual destroy");
          } catch (error) {
            // Ignore
          }
        }
      });
    } catch (error) {
      console.error(`[ChatServer] Error in handleOnDestroy:`, error);
      try {
        this.clients.delete(ws);
        this.cancelCleanup(idtarget);
      } catch (fallbackError) {
        // Ignore
      }
    }
  }

  async fullRemoveById(idtarget) {
    if (!idtarget) return;
    
    try {
      await this.withLock(`full-remove-${idtarget}`, async () => {
        this.cancelCleanup(idtarget);
        
        if (this.vipManager) {
          try {
            await this.vipManager.cleanupUserVipBadges(idtarget);
          } catch (vipError) {
            console.error(`[ChatServer] Error cleaning VIP badges:`, vipError);
          }
        }
        
        // Only cleanup from current room (O(1) operation)
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

        // Close all connections for this user
        const clientsToRemove = [];
        for (const client of this.clients) {
          if (client && client.idtarget === idtarget) {
            clientsToRemove.push(client);
          }
        }
        
        for (const client of clientsToRemove) {
          if (client.readyState === 1) {
            try {
              client.close(1000, "Session removed");
            } catch (closeError) {
              // Ignore
            }
          }
          this.clients.delete(client);
          
          if (this.roomClients) {
            for (const [room, clientArray] of this.roomClients) {
              if (clientArray) {
                const index = clientArray.indexOf(client);
                if (index > -1) {
                  clientArray.splice(index, 1);
                }
              }
            }
          }
        }
      });
    } catch (error) {
      console.error(`[ChatServer] Error in fullRemoveById:`, error);
    }
  }

  // ============ OPTIMIZED UTILITY METHODS ============
  getAllOnlineUsers() {
    try {
      const users = [];
      const seenUsers = new Set();
      
      for (const client of this.clients) {
        if (client && client.idtarget && client.readyState === 1 && !client._isDuplicate) {
          if (!seenUsers.has(client.idtarget)) {
            users.push(client.idtarget);
            seenUsers.add(client.idtarget);
          }
        }
      }
      return users;
    } catch (error) {
      console.error(`[ChatServer] Error in getAllOnlineUsers:`, error);
      return [];
    }
  }

  getOnlineUsersByRoom(roomName) {
    try {
      const users = [];
      const seenUsers = new Set();
      const clientArray = this.roomClients.get(roomName);
      if (clientArray) {
        for (let i = 0; i < clientArray.length; i++) {
          const client = clientArray[i];
          if (client && client.idtarget && client.readyState === 1 && !client._isDuplicate) {
            if (!seenUsers.has(client.idtarget)) {
              users.push(client.idtarget);
              seenUsers.add(client.idtarget);
            }
          }
        }
      }
      return users;
    } catch (error) {
      console.error(`[ChatServer] Error in getOnlineUsersByRoom:`, error);
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
          if (info && info.namauser && info.namauser !== "") {
            const { lastPoint, ...rest } = info;
            updates.push([seat, rest]);
            
            // Limit batch size
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
    } catch (error) {
      console.error(`[ChatServer] Error in flushKursiUpdates:`, error);
    }
  }

  periodicFlush() {
    try {
      this.flushKursiUpdates();
    } catch (error) {
      console.error(`[ChatServer] Error in periodicFlush:`, error);
    }
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const clientsToNotify = [];
      const notifiedUsers = new Set();
      
      // Collect unique users to notify
      for (const client of this.clients) {
        if (client && client.readyState === 1 && client.roomname && !client._isDuplicate) {
          if (!notifiedUsers.has(client.idtarget)) {
            clientsToNotify.push(client);
            notifiedUsers.add(client.idtarget);
          }
        }
      }
      
      // Send notifications
      for (const client of clientsToNotify) {
        this.safeSend(client, ["currentNumber", this.currentNumber]);
      }
    } catch (error) {
      console.error(`[ChatServer] Error in tick:`, error);
      throw error;
    }
  }

  // ============ OPTIMIZED MAIN MESSAGE HANDLER ============
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isDuplicate) return;

    try {
      // Quick size check first
      if (raw.length > 50000) { // Reduced from 100000
        try {
          ws.close(1009, "Message too large");
        } catch (closeError) {
          // Ignore
        }
        return;
      }

      let data;
      try { 
        data = JSON.parse(raw); 
        if (ws.errorCount) ws.errorCount = 0;
      } catch (e) { 
        ws.errorCount = (ws.errorCount || 0) + 1;
        if (ws.errorCount > 3) { // Reduced from 5
          try {
            ws.close(1008, "Protocol error");
          } catch (e2) {}
        }
        return; 
      }
      
      if (!Array.isArray(data) || data.length === 0) return;

      const evt = data[0];

      try {
        switch (evt) {
          case "vipbadge":
          case "removeVipBadge":
          case "getAllVipBadges":
            if (this.vipManager) {
              await this.vipManager.handleEvent(ws, data);
            } else {
              this.safeSend(ws, ["error", "VIP system not available"]);
            }
            break;

          case "isInRoom": {
            const idtarget = ws.idtarget;
            if (!idtarget) {
              this.safeSend(ws, ["inRoomStatus", false]);
              return;
            }
            // Quick check using userCurrentRoom (O(1))
            const currentRoom = this.userCurrentRoom.get(idtarget);
            const isInRoom = currentRoom !== undefined;
            this.safeSend(ws, ["inRoomStatus", isInRoom]);
            break;
          }

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
            
            // Send to only one connection
            let sent = false;
            for (const client of this.clients) {
              if (client && client.idtarget === idtarget && client.readyState === 1 && !client._isDuplicate) {
                if (this.safeSend(client, notif)) {
                  sent = true;
                  break;
                }
              }
            }
            break;
          }

          case "private": {
            const [, idt, url, msg, sender] = data;
            const ts = Date.now();
            const out = ["private", idt, url, msg, ts, sender];
            
            this.safeSend(ws, out);
            
            // Send to receiver (one connection only)
            for (const client of this.clients) {
              if (client && client.idtarget === idt && client.readyState === 1 && !client._isDuplicate) {
                if (this.safeSend(client, out)) {
                  break;
                }
              }
            }
            break;
          }

          case "isUserOnline": {
            const username = data[1];
            const tanda = data[2] ?? "";
            let isOnline = false;
            
            // Quick check with userConnections map
            const connections = this.userConnections.get(username);
            if (connections && connections.size > 0) {
              for (const conn of connections) {
                if (conn.readyState === 1 && !conn._isDuplicate) {
                  isOnline = true;
                  break;
                }
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

          case "joinRoom": 
            const success = await this.handleJoinRoom(ws, data[1]);
            
            // PERBAIKAN: Jika join berhasil, update jumlah room
            if (success && ws.roomname) {
              this.updateRoomCount(ws.roomname);
            }
            break;

          case "chat": {
            const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
            
            if (ws.roomname !== roomname || ws.idtarget !== username) return;
            if (!roomList.includes(roomname)) return;

            // Check if this is the primary connection
            let isPrimaryConnection = true;
            const userConnections = this.userConnections.get(username);
            if (userConnections && userConnections.size > 0) {
              let earliestConnection = null;
              for (const conn of userConnections) {
                if (conn.readyState === 1) {
                  if (!earliestConnection || 
                      (conn._connectionTime || 0) < (earliestConnection._connectionTime || 0)) {
                    earliestConnection = conn;
                  }
                }
              }
              if (earliestConnection && earliestConnection !== ws) {
                isPrimaryConnection = false;
              }
            }
            
            if (!isPrimaryConnection) {
              console.log(`[ChatServer] Skipping chat from duplicate connection for user ${username}`);
              return;
            }

            const chatMsg = ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor];
            this.broadcastToRoom(roomname, chatMsg);
            break;
          }

          case "updatePoint": {
            const [, room, seat, x, y, fast] = data;
            
            if (ws.roomname !== room || !roomList.includes(room)) return;
            
            await this.updateSeatAtomic(room, seat, (currentSeat) => {
              currentSeat.lastPoint = { x, y, fast, timestamp: Date.now() };
              return currentSeat;
            });
            
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
            break;
          }

          case "removeKursiAndPoint": {
            const [, room, seat] = data;
            
            if (ws.roomname !== room || !roomList.includes(room)) return;
            
            await this.updateSeatAtomic(room, seat, () => createEmptySeat());
            this.clearSeatBuffer(room, seat);
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            
            // PERBAIKAN: Update jumlah room setelah remove
            this.updateRoomCount(room);
            
            break;
          }

          case "updateKursi": {
            const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
            
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
            
            // PERBAIKAN: Update user tracking DAN jumlah room
            if (namauser === ws.idtarget) {
              this.userToSeat.set(namauser, { room, seat });
              this.userCurrentRoom.set(namauser, room);
            }
            
            // PERBAIKAN PENTING: Update jumlah room REAL-TIME
            this.updateRoomCount(room);
            
            // Juga broadcast kursi update ke room
            this.broadcastToRoom(room, [
              "updateKursiResponse", 
              room, 
              seat, 
              noimageUrl, 
              namauser, 
              color, 
              itembawah, 
              itematas, 
              vip, 
              viptanda
            ]);
            
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
            
            // Cleanup dari room
            await this.cleanupFromRoom(ws, room);
            
            // Update jumlah room
            this.updateRoomCount(room);
            
            // Kirim konfirmasi ke user
            this.safeSend(ws, ["roomLeft", room]);
            break;
          }

          case "gameLowCardStart":
          case "gameLowCardJoin":
          case "gameLowCardNumber":
          case "gameLowCardEnd":
            if (ws.roomname === "LowCard" || ws.roomname === "HINDI") {
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
      } catch (error) {
        console.error(`[ChatServer] Error handling event ${evt}:`, error);
        
        if (ws.readyState === 1) {
          this.safeSend(ws, ["error", `Server error: ${error.message}`]);
        }
      }
    } catch (error) {
      console.error(`[ChatServer] Unhandled error in handleMessage:`, error);
    }
  }

  // ============ OPTIMIZED WEBSOCKET SERVER ============
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
      ws._connectionTime = Date.now();

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        try {
          // Use microtask for better responsiveness
          Promise.resolve().then(() => {
            this.handleMessage(ws, ev.data).catch(error => {
              console.error("[ChatServer] Unhandled error in message handler:", error);
            });
          });
        } catch (syncError) {
          console.error("[ChatServer] Sync error in message handler:", syncError);
        }
      });

      ws.addEventListener("error", (event) => {
        console.error("[ChatServer] WebSocket error:", event.error);
      });

      ws.addEventListener("close", (event) => {
        if (ws.idtarget && !ws.isManualDestroy && !ws._isDuplicate) {
          this.scheduleCleanup(ws.idtarget);
        }
        
        // Cleanup in background
        setTimeout(() => {
          try {
            if (ws.idtarget) {
              const userConnections = this.userConnections.get(ws.idtarget);
              if (userConnections) {
                userConnections.delete(ws);
                if (userConnections.size === 0) {
                  this.userConnections.delete(ws.idtarget);
                }
              }
            }
            
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
          } catch (cleanupError) {
            console.error("[ChatServer] Error in close handler cleanup:", cleanupError);
          }
        }, 0); // Schedule for next tick
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error("[ChatServer] Error in fetch:", error);
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
    } catch (error) {
      console.error("[ChatServer] Error in default fetch:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};

