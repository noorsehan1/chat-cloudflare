import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard 1", "LowCard 2", "General", "Pakistan", "Philippines", "HINDI", "Indonesia", 
  "Birthday Party", "Heart Lovers", "MEPHISTOPHELES", "Chhichhore", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

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

class DebouncedCleanupManager {
  constructor(server, interval = 1500) {
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
    const batchSize = 10;
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
    this.lockTimeout = 500;
  }

  async acquire(resourceId) {
    if (!this.locks.has(resourceId)) {
      this.locks.set(resourceId, Promise.resolve());
      return () => this.release(resourceId);
    }

    let resolveLock;
    const lockPromise = new Promise(resolve => {
      resolveLock = resolve;
    });

    const currentLock = this.locks.get(resourceId);
    const newLock = currentLock.then(() => lockPromise);
    this.locks.set(resourceId, newLock);

    const release = () => {
      resolveLock();
      if (this.locks.get(resourceId) === newLock) {
        this.locks.delete(resourceId);
      }
    };

    return release;
  }

  release(resourceId) {
    if (this.locks.has(resourceId)) {
      this.locks.delete(resourceId);
    }
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
    lastPoint: null,
    lastUpdated: Date.now()
  };
}

export class ChatServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;

      this.lockManager = new LockManager();
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
      this.pointCache = new Map();

      try {
        this.lowcard = new LowCardGameManager(this);
      } catch {
        this.lowcard = null;
      }

      this.gracePeriod = 3000;
      this.disconnectedTimers = new Map();
      this.debouncedCleanup = new DebouncedCleanupManager(this, 1500);
      this.cleanupQueue = new QueueManager(5);
      this.currentNumber = 1;
      this.maxNumber = 6;
      this.intervalMillis = 15 * 60 * 1000;
      this._nextConnId = 1;
      this._timers = [];

      try {
        this.initializeRooms();
      } catch {
        this.createDefaultRoom();
      }

      this.startTimers();
      this.roomCountsCache = new Map();
      this.cacheValidDuration = 2000;
      this.lastCacheUpdate = 0;

    } catch (error) {
      this.clients = new Set();
      this.userToSeat = new Map();
      this.userCurrentRoom = new Map();
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.roomClients = new Map();
      this.updateKursiBuffer = new Map();
      this.pointCache = new Map();
      this.userConnections = new Map();
      this.disconnectedTimers = new Map();
      this.lockManager = new LockManager();
      this.cleanupInProgress = new Set();
      this.MAX_SEATS = 35;
      this.currentNumber = 1;
      this._nextConnId = 1;
      this._timers = [];
      this.lowcard = null;
      this.gracePeriod = 3000;
      this.debouncedCleanup = new DebouncedCleanupManager(this, 1500);
      this.cleanupQueue = new QueueManager(5);
      this.createDefaultRoom();
    }
  }

  scheduleCleanup(userId) {
    try {
      if (!userId) return;
      
      this.cancelCleanup(userId);
      
      if (this.debouncedCleanup && this.debouncedCleanup.pendingCleanups) {
        this.debouncedCleanup.pendingCleanups.delete(userId);
      }
      
      const timerId = setTimeout(async () => {
        try {
          const isStillConnected = await this.isUserStillConnected(userId);
          if (!isStillConnected) {
            await this.executeGracePeriodCleanup(userId);
          }
        } catch {
          // Ignore cleanup errors
        } finally {
          this.disconnectedTimers.delete(userId);
        }
      }, this.gracePeriod);
      
      this.disconnectedTimers.set(userId, timerId);
      
      if (this.debouncedCleanup) {
        this.debouncedCleanup.schedule(userId);
      }
      
    } catch {
      // Ignore scheduling errors
    }
  }

  cancelCleanup(userId) {
    try {
      if (!userId) return;
      
      const timer = this.disconnectedTimers.get(userId);
      if (timer) {
        clearTimeout(timer);
        this.disconnectedTimers.delete(userId);
      }
      
      if (this.debouncedCleanup && this.debouncedCleanup.pendingCleanups) {
        this.debouncedCleanup.pendingCleanups.delete(userId);
      }
      
      if (this.cleanupInProgress && this.cleanupInProgress.has(userId)) {
        this.cleanupInProgress.delete(userId);
      }
      
    } catch {
      // Ignore cancellation errors
    }
  }

  async isUserStillConnected(userId) {
    if (!userId) return false;
    
    const userConnections = this.userConnections.get(userId);
    if (userConnections && userConnections.size > 0) {
      for (const conn of userConnections) {
        if (conn && conn.readyState === 1 && !conn._isDuplicate) {
          return true;
        }
      }
    }
    
    return false;
  }

  async executeGracePeriodCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    
    try {
      return await this.cleanupQueue.add(async () => {
        await this.withLock(`user-cleanup-${userId}`, async () => {
          const isStillConnected = await this.isUserStillConnected(userId);
          
          if (isStillConnected) {
            this.cancelCleanup(userId);
            return;
          }
          
          await this.forceUserCleanup(userId);
        });
      });
    } catch {
      // Ignore cleanup errors
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
    } catch {
      // Ignore room creation errors
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
        this.roomClients.set(room, []);
        this.updateKursiBuffer.set(room, new Map());
      } catch {
        // Ignore room initialization errors
      }
    }
  }

  startTimers() {
    try {
      this._cleanupTimers();
      
      this._tickTimer = setInterval(() => {
        try {
          this.tick();
        } catch {
          // Ignore tick errors
        }
      }, this.intervalMillis);

      this._flushTimer = setInterval(() => {
        try {
          if (this.clients.size > 0) this.periodicFlush();
        } catch {
          // Ignore flush errors
        }
      }, 100);

      this._consistencyTimer = setInterval(() => {
        try {
          if (this.clients.size > 0 && this.getServerLoad() < 0.7) {
            this.checkSeatConsistency();
          }
        } catch {
          // Ignore consistency errors
        }
      }, 120000);

      this._connectionCleanupTimer = setInterval(() => {
        try {
          if (this.getServerLoad() < 0.8) {
            this.cleanupDuplicateConnections();
          }
        } catch {
          // Ignore cleanup errors
        }
      }, 30000);

      this._cacheCleanupTimer = setInterval(() => {
        try {
          this.cleanupPointCache();
        } catch {
          // Ignore cache cleanup errors
        }
      }, 300000);

      this._timers = [
        this._tickTimer, 
        this._flushTimer, 
        this._consistencyTimer, 
        this._connectionCleanupTimer,
        this._cacheCleanupTimer
      ];
      
    } catch {
      this._timers = [];
    }
  }

  getServerLoad() {
    const activeConnections = Array.from(this.clients).filter(c => c.readyState === 1).length;
    return Math.min(activeConnections / 100, 0.95);
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
          } catch {
            // Ignore timer errors
          }
        }
      }
      this._timers = [];

      if (this.disconnectedTimers) {
        for (const timer of this.disconnectedTimers.values()) {
          if (timer) {
            try {
              clearTimeout(timer);
            } catch {
              // Ignore timer errors
            }
          }
        }
        this.disconnectedTimers.clear();
      }

      if (this.debouncedCleanup) {
        this.debouncedCleanup.stop();
      }

      if (this.cleanupQueue) {
        this.cleanupQueue.clear();
      }
    } catch {
      // Ignore cleanup errors
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
      
      this.invalidateRoomCache(room);
      this.broadcastRoomUserCount(room);
      
      return count;
    } catch {
      return 0;
    }
  }

  invalidateRoomCache(room) {
    this.roomCountsCache = null;
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
    } catch {
      return 0;
    }
  }

  async withLock(resourceId, operation) {
    const release = await this.lockManager.acquire(resourceId);
    try {
      const result = await operation();
      return result;
    } finally {
      try {
        release();
      } catch {
        // Ignore release errors
      }
    }
  }

  async forceUserCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    
    this.cleanupInProgress.add(userId);
    try {
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
      this.userConnections.delete(userId);

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

    } catch {
      // Ignore cleanup errors
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
    } catch {
      // Ignore cleanup errors
    }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws || !ws.idtarget || !ws.roomname) return;
    
    try {
      await this.withLock(`room-cleanup-${room}-${ws.idtarget}`, async () => {
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (!seatInfo || seatInfo.room !== room) return;
        
        const { seat } = seatInfo;
        
        await this.cleanupUserFromSeat(room, seat, ws.idtarget, true);
        
        const clientArray = this.roomClients.get(room);
        if (clientArray) {
          const index = clientArray.indexOf(ws);
          if (index > -1) {
            clientArray.splice(index, 1);
          }
        }
        
        const userConnections = this.userConnections.get(ws.idtarget);
        if (userConnections) {
          userConnections.delete(ws);
          if (userConnections.size === 0) {
            this.userConnections.delete(ws.idtarget);
          }
        }
        
        this.userCurrentRoom.delete(ws.idtarget);
        
        ws.roomname = undefined;
        ws.numkursi = new Set();
        this.userToSeat.delete(ws.idtarget);
        
        this.updateRoomCount(room);
      });
    } catch {
      // Ignore cleanup errors
    }
  }

  clearSeatBuffer(room, seatNumber) {
    try {
      const roomMap = this.updateKursiBuffer.get(room);
      if (roomMap) roomMap.delete(seatNumber);
    } catch {
      // Ignore buffer errors
    }
  }

  async updateSeatAtomic(room, seatNumber, updateFn) {
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
          bufferInfo.vip = 0;
          bufferInfo.viptanda = 0;
          buffer.set(seatNumber, bufferInfo);
        }
        
        return updatedSeat;
      });
    } catch {
      return null;
    }
  }

  async savePointWithRetry(room, seat, x, y, fast) {
    try {
      const pointKey = `${room}:${seat}`;
      const pointData = { 
        x: x || 0, 
        y: y || 0, 
        fast: fast || false,
        timestamp: Date.now(),
        seat,
        room
      };
      
      this.pointCache.set(pointKey, pointData);
      
      setTimeout(async () => {
        try {
          const seatMap = this.roomSeats.get(room);
          if (seatMap) {
            const seatInfo = seatMap.get(seat);
            if (seatInfo) {
              seatInfo.lastPoint = pointData;
              seatInfo.lastUpdated = Date.now();
            }
          }
        } catch {
          // Ignore async update errors
        }
      }, 0);
      
      return true;
    } catch (error) {
      try {
        setTimeout(async () => {
          try {
            const seatMap = this.roomSeats.get(room);
            if (seatMap) {
              const seatInfo = seatMap.get(seat);
              if (seatInfo) {
                seatInfo.lastPoint = { x, y, fast, timestamp: Date.now() };
              }
            }
          } catch {
            // Ignore retry errors
          }
        }, 10);
        return true;
      } catch {
        return false;
      }
    }
  }

  cleanupPointCache() {
    try {
      const now = Date.now();
      const keysToDelete = [];
      
      for (const [key, pointData] of this.pointCache.entries()) {
        if (now - (pointData.timestamp || 0) > 300000) {
          keysToDelete.push(key);
        }
      }
      
      for (const key of keysToDelete) {
        this.pointCache.delete(key);
      }
    } catch {
      // Ignore cache cleanup errors
    }
  }

  async findEmptySeat(room, ws) {
    if (!room || !ws || !ws.idtarget) return null;
    
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      const seatMap = this.roomSeats.get(room);
      
      if (!occupancyMap || !seatMap) return null;

      let candidateSeat = null;
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        if (occupancyMap.get(i) === ws.idtarget) {
          const seatData = seatMap.get(i);
          if (seatData && seatData.namauser === ws.idtarget) {
            candidateSeat = i;
            break;
          }
        }
      }
      
      if (candidateSeat) return candidateSeat;
      
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
      
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupiedBy = occupancyMap.get(i);
        if (occupiedBy && occupiedBy !== ws.idtarget) {
          let isOccupantOnline = false;
          
          const userConnections = this.userConnections.get(occupiedBy);
          if (userConnections && userConnections.size > 0) {
            for (const conn of userConnections) {
              if (conn && conn.readyState === 1 && !conn._isDuplicate) {
                isOccupantOnline = true;
                break;
              }
            }
          }
          
          if (!isOccupantOnline) {
            const seatData = seatMap.get(i);
            if (seatData && seatData.namauser === occupiedBy) {
              await this.cleanupUserFromSeat(room, i, occupiedBy, true);
              return i;
            }
          }
        }
      }
      
      return null;
    } catch {
      return null;
    }
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
    
    try {
      return await this.withLock(`join-room-${room}-${ws.idtarget}`, async () => {
        this.cancelCleanup(ws.idtarget);
        
        const previousRoom = this.userCurrentRoom.get(ws.idtarget);
        if (previousRoom && previousRoom !== room) {
          await this.cleanupFromRoom(ws, previousRoom);
        }
        
        const seat = await this.findEmptySeat(room, ws);
        if (!seat) {
          this.safeSend(ws, ["roomFull", room]);
          return false;
        }
        
        const occupancyMap = this.seatOccupancy.get(room);
        if (occupancyMap) {
          occupancyMap.set(seat, ws.idtarget);
        }
        
        this.userToSeat.set(ws.idtarget, { room, seat });
        this.userCurrentRoom.set(ws.idtarget, room);
        ws.roomname = room;
        ws.numkursi = new Set([seat]);
        
        const clientArray = this.roomClients.get(room);
        if (clientArray && !clientArray.includes(ws)) {
          clientArray.push(ws);
        }
        
        let userConnections = this.userConnections.get(ws.idtarget);
        if (!userConnections) {
          userConnections = new Set();
          this.userConnections.set(ws.idtarget, userConnections);
        }
        userConnections.add(ws);
        
        this.safeSend(ws, ["rooMasuk", seat, room]);
        
        await new Promise(resolve => setTimeout(resolve, 10));
        
        this.sendAllStateTo(ws, room);
        this.updateRoomCount(room);
        
        return true;
      });
    } catch (error) {
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }

  getJumlahRoom() {
    try {
      const now = Date.now();
      if (this.roomCountsCache && 
          (now - this.lastCacheUpdate) < this.cacheValidDuration) {
        return this.roomCountsCache;
      }
      
      const counts = Object.fromEntries(roomList.map(r => [r, 0]));
      
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;
        
        let roomCount = 0;
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const info = seatMap.get(i);
          if (info && info.namauser && info.namauser !== "") {
            roomCount++;
          }
        }
        counts[room] = roomCount;
      }
      
      this.roomCountsCache = counts;
      this.lastCacheUpdate = now;
      
      return counts;
    } catch {
      return Object.fromEntries(roomList.map(r => [r, 0]));
    }
  }

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
    } catch {
      return [];
    }
  }

  safeSend(ws, arr) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isDuplicate) return false;
      
      if (ws.bufferedAmount > 500000) {
        return false;
      }
      
      try {
        ws.send(JSON.stringify(arr));
        return true;
      } catch {
        return false;
      }
    } catch {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      
      const clientArray = this.roomClients.get(room);
      if (!clientArray || clientArray.length === 0) return 0;
      
      let messageString;
      try {
        messageString = JSON.stringify(msg);
      } catch {
        return 0;
      }
      
      let sentCount = 0;
      const sentToUsers = new Set();
      
      for (let i = 0; i < clientArray.length; i++) {
        const client = clientArray[i];
        if (client && client.readyState === 1 && client.roomname === room && !client._isDuplicate) {
          if (client.idtarget && sentToUsers.has(client.idtarget)) {
            continue;
          }
          
          try {
            if (client.bufferedAmount < 500000) {
              client.send(messageString);
              sentCount++;
              if (client.idtarget) {
                sentToUsers.add(client.idtarget);
              }
            }
          } catch {
            // Ignore send errors
          }
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
        if (info && info.namauser && info.namauser !== "") {
          count++;
        }
      }
      
      if (this.roomCountsCache) {
        this.roomCountsCache[room] = count;
      }
      
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
      
    } catch {
      // Ignore broadcast errors
    }
  }

  sendAllStateTo(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || ws._isDuplicate) return;
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      const allKursiMeta = {};
      const lastPointsData = [];

      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info || !info.namauser || info.namauser === "") continue;
        
        allKursiMeta[seat] = {
          noimageUrl: info.noimageUrl,
          namauser: info.namauser,
          color: info.color,
          itembawah: info.itembawah,
          itematas: info.itematas,
          vip: 0,
          viptanda: 0
        };

        const pointKey = `${room}:${seat}`;
        const cachedPoint = this.pointCache.get(pointKey);
        
        if (cachedPoint) {
          lastPointsData.push({
            seat: seat,
            x: cachedPoint.x || 0,
            y: cachedPoint.y || 0,
            fast: cachedPoint.fast || false
          });
        } else if (info.lastPoint) {
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
        for (let i = 0; i < lastPointsData.length; i += 15) {
          const batch = lastPointsData.slice(i, i + 15);
          this.safeSend(ws, ["allPointsList", room, batch]);
        }
      }

      const counts = this.getJumlahRoom();
      const count = counts[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);
      
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
    } catch (error) {
      // Ignore state sending errors
    }
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    
    try {
      await this.withLock(`reconnect-${id}`, async () => {
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
          
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          
          if (seatMap && occupancyMap) {
            const seatData = seatMap.get(seat);
            const occupancyUser = occupancyMap.get(seat);
            
            if (seatData && seatData.namauser === id && occupancyUser === id) {
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
              
              const pointKey = `${room}:${seat}`;
              const cachedPoint = this.pointCache.get(pointKey);
              if (cachedPoint) {
                this.safeSend(ws, ["pointUpdated", room, seat, 
                  cachedPoint.x, 
                  cachedPoint.y, 
                  cachedPoint.fast
                ]);
              } else if (seatData.lastPoint) {
                this.safeSend(ws, ["pointUpdated", room, seat, 
                  seatData.lastPoint.x, 
                  seatData.lastPoint.y, 
                  seatData.lastPoint.fast
                ]);
              }
              
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
      this.safeSend(ws, ["error", "Reconnection failed, please try joining a room manually"]);
    }
  }

  async cleanupDuplicateConnections() {
    try {
      const userConnectionCount = new Map();
      
      for (const client of this.clients) {
        if (client && client.idtarget && client.readyState === 1) {
          const count = userConnectionCount.get(client.idtarget) || 0;
          userConnectionCount.set(client.idtarget, count + 1);
        }
      }
      
      const duplicateUsers = [];
      for (const [userId, count] of userConnectionCount) {
        if (count > 1) {
          duplicateUsers.push(userId);
        }
      }
      
      const batchSize = 10;
      for (let i = 0; i < duplicateUsers.length; i += batchSize) {
        const batch = duplicateUsers.slice(i, i + batchSize);
        await Promise.allSettled(
          batch.map(userId => this.handleDuplicateConnections(userId))
        );
      }
    } catch {
      // Ignore duplicate cleanup errors
    }
  }

  async handleDuplicateConnections(userId) {
    if (!userId) return;
    
    try {
      await this.withLock(`duplicate-connections-${userId}`, async () => {
        const allConnections = [];
        
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
        
        allConnections.sort((a, b) => b.connectionTime - a.connectionTime);
        
        const connectionsToClose = allConnections.slice(1);
        
        for (const { client } of connectionsToClose) {
          client._isDuplicate = true;
          
          try {
            if (client.readyState === 1) {
              this.safeSend(client, ["duplicateConnection", "Another connection was opened with your account"]);
            }
          } catch {
            // Ignore send errors
          }
          
          try {
            if (client.readyState === 1) {
              client.close(1000, "Duplicate connection");
            }
          } catch {
            // Ignore close errors
          }
          
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
        
        const remainingConnections = new Set();
        for (const client of this.clients) {
          if (client && client.idtarget === userId && client.readyState === 1) {
            remainingConnections.add(client);
          }
        }
        this.userConnections.set(userId, remainingConnections);
      });
    } catch {
      // Ignore duplicate handling errors
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
        
        const userConnections = this.userConnections.get(idtarget);
        if (userConnections) {
          userConnections.delete(ws);
          if (userConnections.size === 0) {
            this.userConnections.delete(idtarget);
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
        
        if (ws.readyState === 1) {
          try {
            ws.close(1000, "Manual destroy");
          } catch {
            // Ignore close errors
          }
        }
      });
    } catch {
      try {
        this.clients.delete(ws);
        this.cancelCleanup(idtarget);
      } catch {
        // Ignore fallback errors
      }
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
          if (client && client.idtarget === idtarget) {
            clientsToRemove.push(client);
          }
        }
        
        for (const client of clientsToRemove) {
          if (client.readyState === 1) {
            try {
              client.close(1000, "Session removed");
            } catch {
              // Ignore close errors
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
    } catch {
      // Ignore full remove errors
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
            const { lastPoint, lastUpdated, ...rest } = info;
            updates.push([seat, rest]);
            
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
    } catch {
      // Ignore flush errors
    }
  }

  periodicFlush() {
    try {
      this.flushKursiUpdates();
    } catch {
      // Ignore periodic flush errors
    }
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const clientsToNotify = [];
      const notifiedUsers = new Set();
      
      for (const client of this.clients) {
        if (client && client.readyState === 1 && client.roomname && !client._isDuplicate) {
          if (!notifiedUsers.has(client.idtarget)) {
            clientsToNotify.push(client);
            notifiedUsers.add(client.idtarget);
          }
        }
      }
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      
      for (const client of clientsToNotify) {
        try {
          if (client.bufferedAmount < 500000) {
            client.send(message);
          }
        } catch {
          // Ignore send errors
        }
      }
    } catch {
      // Ignore tick errors
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isDuplicate) return;

    try {
      if (raw.length > 50000) {
        try {
          ws.close(1009, "Message too large");
        } catch {
          // Ignore close errors
        }
        return;
      }

      let data;
      try { 
        data = JSON.parse(raw); 
        if (ws.errorCount) ws.errorCount = 0;
      } catch { 
        ws.errorCount = (ws.errorCount || 0) + 1;
        if (ws.errorCount > 3) {
          try {
            ws.close(1008, "Protocol error");
          } catch {}
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
            
            if (success && ws.roomname) {
              this.updateRoomCount(ws.roomname);
            }
            break;

          case "chat": {
            const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
            
            if (ws.roomname !== roomname || ws.idtarget !== username) return;
            if (!roomList.includes(roomname)) return;

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
              return;
            }

            const chatMsg = JSON.stringify(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
            
            const clientArray = this.roomClients.get(roomname);
            if (clientArray) {
              const sentToUsers = new Set();
              for (let i = 0; i < clientArray.length; i++) {
                const client = clientArray[i];
                if (client && client.readyState === 1 && client.roomname === roomname && !client._isDuplicate) {
                  if (client.idtarget && sentToUsers.has(client.idtarget)) continue;
                  try {
                    if (client.bufferedAmount < 500000) {
                      client.send(chatMsg);
                      if (client.idtarget) sentToUsers.add(client.idtarget);
                    }
                  } catch {
                    // Ignore send errors
                  }
                }
              }
            }
            break;
          }

          case "updatePoint": {
            const [, room, seat, x, y, fast] = data;
            
            if (ws.roomname !== room || !roomList.includes(room)) return;
            
            await this.savePointWithRetry(room, seat, x, y, fast);
            
            const pointMsg = JSON.stringify(["pointUpdated", room, seat, x, y, fast]);
            
            const clientArray = this.roomClients.get(room);
            if (clientArray) {
              const sentToUsers = new Set();
              
              if (ws.idtarget && ws.readyState === 1) {
                try {
                  if (ws.bufferedAmount < 500000) {
                    ws.send(pointMsg);
                    sentToUsers.add(ws.idtarget);
                  }
                } catch {
                  // Ignore send error
                }
              }
              
              for (let i = 0; i < clientArray.length; i++) {
                const client = clientArray[i];
                if (client && client.readyState === 1 && client.roomname === room && 
                    !client._isDuplicate && client !== ws) {
                  
                  if (client.idtarget && sentToUsers.has(client.idtarget)) {
                    continue;
                  }
                  
                  try {
                    if (client.bufferedAmount < 500000) {
                      client.send(pointMsg);
                      if (client.idtarget) {
                        sentToUsers.add(client.idtarget);
                      }
                    }
                  } catch {
                    // Ignore send errors
                  }
                }
              }
            }
            break;
          }

          case "removeKursiAndPoint": {
            const [, room, seat] = data;
            
            if (ws.roomname !== room || !roomList.includes(room)) return;
            
            await this.updateSeatAtomic(room, seat, () => createEmptySeat());
            this.clearSeatBuffer(room, seat);
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
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
              lastPoint: null,
              lastUpdated: Date.now()
            }));
            
            if (namauser === ws.idtarget) {
              this.userToSeat.set(namauser, { room, seat });
              this.userCurrentRoom.set(namauser, room);
            }
            
            this.updateRoomCount(room);
            
            this.broadcastToRoom(room, [
              "updateKursiResponse", 
              room, 
              seat, 
              noimageUrl, 
              namauser, 
              color, 
              itembawah, 
              itematas, 
              0, // vip
              0  // viptanda
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
      } catch {
        if (ws.readyState === 1) {
          this.safeSend(ws, ["error", "Server error"]);
        }
      }
    } catch {
      // Ignore message handling errors
    }
  }

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
          this.handleMessage(ws, ev.data).catch(() => {
            // Ignore async errors
          });
        } catch {
          // Ignore sync errors
        }
      });

      ws.addEventListener("error", () => {
        // Ignore WebSocket errors
      });

      ws.addEventListener("close", (event) => {
        if (ws.idtarget && !ws.isManualDestroy && !ws._isDuplicate) {
          this.scheduleCleanup(ws.idtarget);
        }
        
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
        } catch {
          // Fallback cleanup
          this.clients.delete(ws);
        }
      });

      return new Response(null, { status: 101, webSocket: client });
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
