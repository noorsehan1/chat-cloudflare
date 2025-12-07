import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "HINDI", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

class LockManager {
  constructor() {
    this.locks = new Map();
    this.lockTimeout = 5000;
  }

  async acquire(resourceId) {
    const startTime = Date.now();
    
    while (this.locks.has(resourceId)) {
      if (Date.now() - startTime > this.lockTimeout) {
        console.warn(`[LockManager] Timeout on ${resourceId}, forcing release`);
        this.locks.delete(resourceId);
        throw new Error(`Timeout waiting for lock on ${resourceId}`);
      }
      await new Promise(resolve => setTimeout(resolve, 10));
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

      // Initialize HARUS di constructor, jangan di setTimeout
      this.lockManager = new LockManager();
      this.cleanupInProgress = new Set();
      this.clients = new Set();
      this.userToSeat = new Map();
      this.roomClients = new Map();
      this.MAX_SEATS = 35;
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.updateKursiBuffer = new Map();
      
      // Game managers - INIT SEKARANG, jangan lazy
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
      
      console.log("[ChatServer] Initialized successfully");

    } catch (error) {
      console.error("[ChatServer] CRITICAL ERROR in constructor:", error);
      // Emergency minimal setup
      this.clients = new Set();
      this.userToSeat = new Map();
      this.roomSeats = new Map();
      this.seatOccupancy = new Map();
      this.roomClients = new Map();
      this.updateKursiBuffer = new Map();
      this.disconnectedTimers = new Map();
      this.lockManager = new LockManager();
      this.cleanupInProgress = new Set();
      this.MAX_SEATS = 35;
      this.currentNumber = 1;
      this._nextConnId = 1;
      this._timers = [];
      this.lowcard = null;
      this.vipManager = null;
      
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
      this.roomClients.set(room, new Set());
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

        this.roomClients.set(room, new Set());
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

      // Buffer flush timer
      this._flushTimer = setInterval(() => {
        try {
          if (this.clients.size > 0) this.periodicFlush();
        } catch (flushError) {
          console.error("[ChatServer] Error in flush timer:", flushError);
        }
      }, 50);

      this._timers = [this._tickTimer, this._flushTimer];
      
    } catch (error) {
      console.error("[ChatServer] Error starting timers:", error);
      this._timers = []; // Pastikan array tetap ada
    }
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
    } catch (error) {
      console.error("[ChatServer] Error cleaning up timers:", error);
    }
  }

  // ============ LOCK MANAGEMENT ============
  async withLock(resourceId, operation) {
    const release = await this.lockManager.acquire(resourceId);
    try {
      return await operation();
    } finally {
      try {
        release();
      } catch (releaseError) {
        console.error(`[ChatServer] Error releasing lock ${resourceId}:`, releaseError);
      }
    }
  }

  // ============ USER MANAGEMENT ============
  scheduleCleanup(userId) {
    try {
      if (!userId) return;
      
      const oldTimer = this.disconnectedTimers.get(userId);
      if (oldTimer) {
        try {
          clearTimeout(oldTimer);
        } catch (clearError) {
          // Ignore
        }
      }

      const timer = setTimeout(() => {
        this.executeGracePeriodCleanup(userId);
      }, this.gracePeriod);

      this.disconnectedTimers.set(userId, timer);
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
      // OPTIMISASI: Check connection status SEBELUM ambil lock
      let isStillConnected = false;
      try {
        // Iterasi clients di luar lock untuk cepat
        for (const client of this.clients) {
          if (client && client.idtarget === userId && client.readyState === 1) {
            isStillConnected = true;
            break;
          }
        }
      } catch (checkError) {
        console.error(`[ChatServer] Error checking connection for ${userId}:`, checkError);
      }
      
      if (isStillConnected) {
        // User masih online, cancel cleanup
        this.cancelCleanup(userId);
        return;
      }
      
      // User offline, proceed dengan lock
      await this.withLock(`user-cleanup-${userId}`, async () => {
        // Hapus timer
        this.disconnectedTimers.delete(userId);
        
        // Double check dalam lock (tapi cepat)
        let finalCheck = false;
        for (const client of this.clients) {
          if (client && client.idtarget === userId && client.readyState === 1) {
            finalCheck = true;
            break;
          }
        }
        
        if (!finalCheck) {
          await this.forceUserCleanup(userId);
        }
      });
    } catch (error) {
      console.error(`[ChatServer] Error in grace period cleanup for ${userId}:`, error);
      if (!error.message.includes("Timeout")) {
        this.scheduleCleanup(userId); // Coba lagi
      }
    }
  }

  async forceUserCleanup(userId) {
    if (!userId || this.cleanupInProgress.has(userId)) return;
    
    this.cleanupInProgress.add(userId);
    try {
      this.cancelCleanup(userId);

      // Collect all seats occupied by user
      const seatsToCleanup = [];
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;
        
        for (const [seatNumber, seatInfo] of seatMap) {
          if (seatInfo && seatInfo.namauser === userId) {
            seatsToCleanup.push({ room, seatNumber });
          }
        }
      }

      // Cleanup semua seat PARALEL
      const cleanupPromises = seatsToCleanup.map(({ room, seatNumber }) =>
        this.cleanupUserFromSeat(room, seatNumber, userId, true)
      );
      
      await Promise.all(cleanupPromises);

      // Remove from tracking
      this.userToSeat.delete(userId);

      // Remove from room clients
      if (this.roomClients) {
        for (const [room, clientSet] of this.roomClients) {
          if (clientSet) {
            for (const client of Array.from(clientSet)) {
              if (client && client.idtarget === userId) {
                clientSet.delete(client);
              }
            }
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
        if (!seatMap) return;

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
          
          // Clear buffer
          this.clearSeatBuffer(room, seatNumber);
          
          // Broadcast removal
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          
          // Update user count
          this.broadcastRoomUserCount(room);
        }

        // Update occupancy
        const occupancyMap = this.seatOccupancy.get(room);
        if (occupancyMap) {
          occupancyMap.set(seatNumber, immediate ? null : userId);
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
        
        // Remove from room clients
        const clientSet = this.roomClients.get(room);
        if (clientSet) clientSet.delete(ws);
        
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
        this.broadcastRoomUserCount(room);
      });
    } catch (error) {
      console.error(`[ChatServer] Error in cleanupFromRoom:`, error);
    }
  }

  // ============ SEAT MANAGEMENT ============
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
        if (!seatMap) return null;
        
        const currentSeat = seatMap.get(seatNumber) || createEmptySeat();
        const updatedSeat = updateFn(currentSeat);
        updatedSeat.lastUpdated = Date.now();
        
        seatMap.set(seatNumber, updatedSeat);
        
        // Update buffer
        const buffer = this.updateKursiBuffer.get(room);
        if (buffer) {
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

  async findEmptySeat(room, ws) {
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      const seatMap = this.roomSeats.get(room);
      
      if (!occupancyMap || !seatMap) return null;

      // First, check if user already has a seat
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        if (occupancyMap.get(i) === ws.idtarget) {
          return i;
        }
      }

      // Then, find truly empty seat
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        if (occupancyMap.get(i) === null) {
          const seatData = seatMap.get(i);
          if (!seatData || seatData.namauser === "") {
            return i;
          } else {
            // Sync occupancy with actual data
            occupancyMap.set(i, seatData.namauser);
          }
        }
      }

      // Finally, check for disconnected users
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const occupiedBy = occupancyMap.get(i);
        if (occupiedBy && occupiedBy !== ws.idtarget) {
          let isOccupantOnline = false;
          for (const client of this.clients) {
            if (client && client.idtarget === occupiedBy && client.readyState === 1) {
              isOccupantOnline = true;
              break;
            }
          }
          
          if (!isOccupantOnline) {
            const seatData = seatMap.get(i);
            if (seatData && seatData.namauser === occupiedBy) {
              // Cleanup disconnected user
              await this.cleanupUserFromSeat(room, i, occupiedBy, true);
              return i;
            }
          }
        }
      }
    } catch (error) {
      console.error(`[ChatServer] Error in findEmptySeat:`, error);
    }
    
    return null;
  }

  // ============ ROOM MANAGEMENT ============
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
    return await this.withLock(`join-${ws.idtarget}`, async () => {
      this.cancelCleanup(ws.idtarget);
      
      // Cleanup from previous room
      if (ws.roomname && ws.roomname !== room) {
        await this.cleanupFromRoom(ws, ws.roomname);
      }
      
      // Find empty seat
      const seat = await this.findEmptySeat(room, ws);
      if (!seat) {
        this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      // **PERUBAHAN: Hanya update occupancy, TIDAK update seat data**
      const occupancyMap = this.seatOccupancy.get(room);
      if (occupancyMap) {
        occupancyMap.set(seat, ws.idtarget);
      }
      
      // **PERUBAHAN: Jangan langsung mengisi seat dengan data user**
      // Seat tetap dalam keadaan kosong (createEmptySeat)
      
      // Update user tracking
      this.userToSeat.set(ws.idtarget, { room, seat });
      ws.roomname = room;
      ws.numkursi = new Set([seat]);
      
      // Add to room clients
      const clientSet = this.roomClients.get(room);
      if (clientSet) clientSet.add(ws);
      
      // **PERUBAHAN: Tidak perlu sendAllStateTo karena seat kosong**
       this.sendAllStateTo(ws, room);
      
      // Kirim state awal ruangan (kursi-kursi lain yang sudah terisi)
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        const allKursiMeta = {};
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const info = seatMap.get(i);
          if (info && info.namauser) {
            allKursiMeta[i] = {
              noimageUrl: info.noimageUrl,
              namauser: info.namauser,
              color: info.color,
              itembawah: info.itembawah,
              itematas: info.itematas,
              vip: info.vip,
              viptanda: info.viptanda
            };
          }
        }
        
        if (Object.keys(allKursiMeta).length > 0) {
          this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
        }
      }
      
      // Broadcast updated user count
      this.broadcastRoomUserCount(room);
      
      // **PERUBAHAN PENTING: Kirim info bahwa seat KOSONG**
      // Kirim empty seat state ke client
      this.safeSend(ws, ["emptySeatAssigned", seat, room]);
      
      // Kirim konfirmasi join
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
    console.error(`[ChatServer] Error in handleJoinRoom:`, error);
    this.safeSend(ws, ["error", "Failed to join room"]);
    return false;
  }
}

  getJumlahRoom() {
    try {
      const counts = Object.fromEntries(roomList.map(r => [r, 0]));
      
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;
        
        for (const info of seatMap.values()) {
          if (info && info.namauser) {
            counts[room]++;
          }
        }
      }
      
      return counts;
    } catch (error) {
      console.error(`[ChatServer] Error in getJumlahRoom:`, error);
      return Object.fromEntries(roomList.map(r => [r, 0]));
    }
  }

  // ============ MESSAGE HANDLING ============
  safeSend(ws, arr) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      
      if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 5000000) {
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

  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      
      const clientSet = this.roomClients.get(room);
      if (!clientSet) return 0;
      
      let sentCount = 0;
      const clientsArray = Array.from(clientSet);
      
      for (const client of clientsArray) {
        if (client && client.readyState === 1 && client.roomname === room) {
          if (this.safeSend(client, msg)) {
            sentCount++;
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
      
      const counts = this.getJumlahRoom();
      const count = counts[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {
      console.error(`[ChatServer] Error in broadcastRoomUserCount:`, error);
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
        if (!info || !info.namauser) continue;
        
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

      if (Object.keys(allKursiMeta).length > 0) {
        this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
      }

      if (lastPointsData.length > 0) {
        this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }

      const counts = this.getJumlahRoom();
      const count = counts[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);
    } catch (error) {
      console.error(`[ChatServer] Error in sendAllStateTo:`, error);
    }
  }

  // ============ EVENT HANDLERS ============
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    
    try {
      // FIX RACE CONDITION: Pakai lock juga untuk cleanup user baru
      if (baru === true) {
        await this.withLock(`user-cleanup-${id}`, async () => {
          await this.forceUserCleanup(id);
        });
      }
      
      await this.withLock(`user-setid-${id}`, async () => {
        if (baru === true) {
          ws.idtarget = id;
          ws.roomname = undefined;
          ws.numkursi = new Set();
          this.safeSend(ws, ["joinroomawal"]);
          return;
        }
        
        ws.idtarget = id;
        
        const seatInfo = this.userToSeat.get(id);
        const seatMap = seatInfo ? this.roomSeats.get(seatInfo.room) : null;
        const currentSeatInfo = seatMap?.get(seatInfo?.seat);
        
        const canReconnect = seatInfo && 
                           currentSeatInfo?.namauser === id &&
                           !this.disconnectedTimers.has(id);
        
        if (canReconnect) {
          const { room, seat } = seatInfo;
          ws.roomname = room;
          ws.numkursi = new Set([seat]);
          
          const clientSet = this.roomClients.get(room);
          if (clientSet) clientSet.add(ws);
          
          this.sendAllStateTo(ws, room);
          this.broadcastRoomUserCount(room);
          
          if (this.vipManager) {
            try {
              await this.vipManager.getAllVipBadges(ws, room);
            } catch (vipError) {
              console.error(`[ChatServer] Error getting VIP badges:`, vipError);
            }
          }
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          
        } else {
          this.safeSend(ws, ["needJoinRoom"]);
        }
      });
    } catch (error) {
      console.error(`[ChatServer] Error in handleSetIdTarget2 for ${id}:`, error);
      if (error.message.includes("Timeout")) {
        this.safeSend(ws, ["error", "Server busy, please try again"]);
      }
    }
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget || !ws) return;
    
    try {
      this.withLock(`destroy-${idtarget}`, async () => {
        if (ws.isManualDestroy) {
          await this.fullRemoveById(idtarget);
        } else {
          const seatInfo = this.userToSeat.get(idtarget);
          if (seatInfo) {
            const { room, seat } = seatInfo;
            await this.cleanupUserFromSeat(room, seat, idtarget, true);
          }
          this.userToSeat.delete(idtarget);
        }
        
        this.cancelCleanup(idtarget);
        
        if (this.roomClients) {
          for (const clientSet of this.roomClients.values()) {
            if (clientSet) clientSet.delete(ws);
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
        
        for (const room of roomList) {
          const seatMap = this.roomSeats.get(room);
          if (!seatMap) continue;

          for (const [seatNumber, info] of seatMap) {
            if (info && info.namauser === idtarget) {
              Object.assign(info, createEmptySeat());
              this.clearSeatBuffer(room, seatNumber);
              this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
            }
          }
          
          this.broadcastRoomUserCount(room);
        }

        this.userToSeat.delete(idtarget);

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
            for (const clientSet of this.roomClients.values()) {
              if (clientSet) clientSet.delete(client);
            }
          }
        }
      });
    } catch (error) {
      console.error(`[ChatServer] Error in fullRemoveById:`, error);
    }
  }

  // ============ UTILITY METHODS ============
  getAllOnlineUsers() {
    try {
      const users = [];
      for (const client of this.clients) {
        if (client && client.idtarget && client.readyState === 1) {
          users.push(client.idtarget);
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
      const clientSet = this.roomClients.get(roomName);
      if (clientSet) {
        for (const client of clientSet) {
          if (client && client.idtarget && client.readyState === 1) {
            users.push(client.idtarget);
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
        
        const updates = [];
        for (const [seat, info] of seatMapUpdates.entries()) {
          if (info) {
            const { lastPoint, ...rest } = info;
            updates.push([seat, rest]);
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
      for (const client of this.clients) {
        if (client && client.readyState === 1 && client.roomname) {
          clientsToNotify.push(client);
        }
      }
      
      for (const client of clientsToNotify) {
        this.safeSend(client, ["currentNumber", this.currentNumber]);
      }
    } catch (error) {
      console.error(`[ChatServer] Error in tick:`, error);
      throw error;
    }
  }

  // ============ MAIN MESSAGE HANDLER ============
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1) return;

    try {
      if (raw.length > 100000) {
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
        if (ws.errorCount > 5) {
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
            const seatInfo = this.userToSeat.get(idtarget);
            if (!seatInfo) {
              this.safeSend(ws, ["inRoomStatus", false]);
              return;
            }
            const { room, seat } = seatInfo;
            const seatMap = this.roomSeats.get(room);
            const seatData = seatMap?.get(seat);
            const isInRoom = seatData?.namauser === idtarget;
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
            for (const client of this.clients) {
              if (client && client.idtarget === idtarget && client.readyState === 1) {
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
              if (client && client.idtarget === idt && client.readyState === 1) {
                this.safeSend(client, out);
                break;
              }
            }
            break;
          }

          case "isUserOnline": {
            const username = data[1];
            const tanda = data[2] ?? "";
            let isOnline = false;
            for (const client of this.clients) {
              if (client && client.idtarget === username && client.readyState === 1) {
                isOnline = true;
                break;
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
            await this.handleJoinRoom(ws, data[1]);
            break;

          case "chat": {
            const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
            
            if (ws.roomname !== roomname || ws.idtarget !== username) return;
            if (!roomList.includes(roomname)) return;

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
            this.broadcastRoomUserCount(room);
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
            
            this.broadcastRoomUserCount(room);
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

  // ============ WEBSOCKET SERVER ============
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

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data).catch(error => {
            console.error("[ChatServer] Unhandled error in message handler:", error);
          });
        } catch (syncError) {
          console.error("[ChatServer] Sync error in message handler:", syncError);
        }
      });

      ws.addEventListener("error", (event) => {
        console.error("[ChatServer] WebSocket error:", event.error);
      });

      ws.addEventListener("close", (event) => {
        if (ws.idtarget && !ws.isManualDestroy) {
          try {
            this.scheduleCleanup(ws.idtarget);
          } catch (scheduleError) {
            console.error("[ChatServer] Error scheduling cleanup:", scheduleError);
          }
        }
        
        try {
          if (this.roomClients) {
            for (const clientSet of this.roomClients.values()) {
              if (clientSet) clientSet.delete(ws);
            }
          }
          
          this.clients.delete(ws);
        } catch (cleanupError) {
          console.error("[ChatServer] Error in close handler:", cleanupError);
        }
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


