import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

class LockManager {
  constructor() {
    this.locks = new Map();
    this.lockTimeout = 5000; // 5 detik timeout
  }

  async acquire(resourceId) {
    const startTime = Date.now();
    
    while (this.locks.has(resourceId)) {
      if (Date.now() - startTime > this.lockTimeout) {
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
    this.state = state;
    this.env = env;

    // Lock management
    this.lockManager = new LockManager();
    this.cleanupInProgress = new Set();

    // Client management
    this.clients = new Set();
    this.userToSeat = new Map();
    this.roomClients = new Map();

    // Room management
    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    this.seatOccupancy = new Map();

    // Buffers
    this.updateKursiBuffer = new Map();

    // Game managers
    this.lowcard = new LowCardGameManager(this);
    this.vipManager = new VipBadgeManager(this);

    // Disconnection management
    this.gracePeriod = 5000;
    this.disconnectedTimers = new Map();

    // Number rotation
    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000; // 15 menit

    // Connection ID counter
    this._nextConnId = 1;

    // Initialize rooms
    this.initializeRooms();

    // Start timers
    this.startTimers();
  }

  initializeRooms() {
    for (const room of roomList) {
      // Initialize seats
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);

      // Initialize occupancy
      const occupancyMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        occupancyMap.set(i, null);
      }
      this.seatOccupancy.set(room, occupancyMap);

      // Initialize client sets
      this.roomClients.set(room, new Set());

      // Initialize buffers
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  startTimers() {
    // Number rotation timer
    this._tickTimer = setInterval(() => {
      this.tick();
    }, this.intervalMillis);

    // Buffer flush timer
    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) this.periodicFlush();
    }, 50);

    // Store for cleanup
    this._timers = [this._tickTimer, this._flushTimer];
  }

  _cleanupTimers() {
    for (const timer of this._timers) {
      clearInterval(timer);
      clearTimeout(timer);
    }
    this._timers = [];

    for (const timer of this.disconnectedTimers.values()) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
  }

  // ============ LOCK MANAGEMENT ============
  async withLock(resourceId, operation) {
    const release = await this.lockManager.acquire(resourceId);
    try {
      return await operation();
    } finally {
      release();
    }
  }

  // ============ USER MANAGEMENT ============
  scheduleCleanup(userId) {
    const oldTimer = this.disconnectedTimers.get(userId);
    if (oldTimer) clearTimeout(oldTimer);

    const timer = setTimeout(() => {
      this.executeGracePeriodCleanup(userId);
    }, this.gracePeriod);

    this.disconnectedTimers.set(userId, timer);
  }

  cancelCleanup(userId) {
    const timer = this.disconnectedTimers.get(userId);
    if (timer) {
      clearTimeout(timer);
      this.disconnectedTimers.delete(userId);
    }
  }

  async executeGracePeriodCleanup(userId) {
    await this.withLock(`user-cleanup-${userId}`, async () => {
      this.disconnectedTimers.delete(userId);

      // Check if user is still connected
      const isStillConnected = Array.from(this.clients).some(c => 
        c.idtarget === userId && c.readyState === 1
      );

      if (!isStillConnected) {
        await this.forceUserCleanup(userId);
      }
    });
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
          if (seatInfo.namauser === userId) {
            seatsToCleanup.push({ room, seatNumber });
          }
        }
      }

      // Cleanup all seats
      for (const { room, seatNumber } of seatsToCleanup) {
        await this.cleanupUserFromSeat(room, seatNumber, userId, true);
      }

      // Remove from tracking
      this.userToSeat.delete(userId);

      // Remove from room clients
      for (const clientSet of this.roomClients.values()) {
        for (const client of Array.from(clientSet)) {
          if (client.idtarget === userId) {
            clientSet.delete(client);
          }
        }
      }

    } finally {
      this.cleanupInProgress.delete(userId);
    }
  }

  async cleanupUserFromSeat(room, seatNumber, userId, immediate = true) {
    await this.withLock(`seat-${room}-${seatNumber}`, async () => {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      const seatInfo = seatMap.get(seatNumber);
      if (!seatInfo || seatInfo.namauser !== userId) return;

      // Remove VIP badge if exists
      if (seatInfo.viptanda > 0) {
        await this.vipManager.removeVipBadge(room, seatNumber);
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
  }

  async cleanupFromRoom(ws, room) {
    if (!ws.idtarget || !ws.roomname) return;
    
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
      await this.vipManager.cleanupUserVipBadges(ws.idtarget);
      
      // Reset client state
      ws.roomname = undefined;
      ws.numkursi = new Set();
      this.userToSeat.delete(ws.idtarget);
      
      // Broadcast updated user count
      this.broadcastRoomUserCount(room);
    });
  }

  // ============ SEAT MANAGEMENT ============
  clearSeatBuffer(room, seatNumber) {
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) roomMap.delete(seatNumber);
  }

  async updateSeatAtomic(room, seatNumber, updateFn) {
    return this.withLock(`seat-update-${room}-${seatNumber}`, () => {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return null;
      
      const currentSeat = seatMap.get(seatNumber) || createEmptySeat();
      const updatedSeat = updateFn(currentSeat);
      updatedSeat.lastUpdated = Date.now();
      
      seatMap.set(seatNumber, updatedSeat);
      
      // Update buffer
      const buffer = this.updateKursiBuffer.get(room);
      const { lastPoint, lastUpdated, ...bufferInfo } = updatedSeat;
      buffer.set(seatNumber, bufferInfo);
      
      return updatedSeat;
    });
  }

  findEmptySeat(room, ws) {
    const occupancyMap = this.seatOccupancy.get(room);
    if (!occupancyMap) return null;

    // First, check if user already has a seat
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancyMap.get(i) === ws.idtarget) {
        return i;
      }
    }

    // Then, find truly empty seat
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancyMap.get(i) === null) {
        const seatMap = this.roomSeats.get(room);
        const seatData = seatMap?.get(i);
        
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
        const isOccupantOnline = Array.from(this.clients).some(c =>
          c.idtarget === occupiedBy && c.readyState === 1
        );
        
        if (!isOccupantOnline) {
          const seatMap = this.roomSeats.get(room);
          const seatData = seatMap?.get(i);
          if (seatData?.namauser === occupiedBy) {
            // Cleanup disconnected user
            this.cleanupUserFromSeat(room, i, occupiedBy, true);
            return i;
          }
        }
      }
    }
    
    return null;
  }

  // ============ ROOM MANAGEMENT ============
  async handleJoinRoom(ws, room) {
    if (!ws.idtarget) {
      this.safeSend(ws, ["error", "User ID not set"]);
      return false;
    }
    
    if (!roomList.includes(room)) {
      this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    return this.withLock(`join-${ws.idtarget}`, async () => {
      this.cancelCleanup(ws.idtarget);
      
      // Cleanup from previous room
      if (ws.roomname && ws.roomname !== room) {
        await this.cleanupFromRoom(ws, ws.roomname);
      }
      
      // Find empty seat
      const seat = this.findEmptySeat(room, ws);
      if (!seat) {
        this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      // Update seat occupancy
      const occupancyMap = this.seatOccupancy.get(room);
      if (occupancyMap) {
        occupancyMap.set(seat, ws.idtarget);
      }
      
      // Update user tracking
      this.userToSeat.set(ws.idtarget, { room, seat });
      ws.roomname = room;
      ws.numkursi = new Set([seat]);
      
      // Add to room clients
      const clientSet = this.roomClients.get(room);
      if (clientSet) clientSet.add(ws);
      
      // Send room state to user
      this.sendAllStateTo(ws, room);
      this.broadcastRoomUserCount(room);
      
      // Send join confirmation
      this.safeSend(ws, ["rooMasuk", seat, room]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
      // Send VIP badges
      await this.vipManager.getAllVipBadges(ws, room);
      
      return true;
    });
  }

  getJumlahRoom() {
    const counts = Object.fromEntries(roomList.map(r => [r, 0]));
    
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      
      for (const info of seatMap.values()) {
        if (info.namauser) counts[room]++;
      }
    }
    
    return counts;
  }

  // ============ MESSAGE HANDLING ============
  safeSend(ws, arr) {
    if (!ws || ws.readyState !== 1) return false;
    
    // Check buffer limit
    if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 5000000) {
      return false;
    }
    
    try {
      ws.send(JSON.stringify(arr));
      return true;
    } catch (error) {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    const clientSet = this.roomClients.get(room);
    if (!clientSet) return 0;
    
    let sentCount = 0;
    for (const client of clientSet) {
      if (client.readyState === 1 && client.roomname === room) {
        if (this.safeSend(client, msg)) {
          sentCount++;
        }
      }
    }
    
    return sentCount;
  }

  broadcastRoomUserCount(room) {
    if (!room || !roomList.includes(room)) return;
    
    const counts = this.getJumlahRoom();
    const count = counts[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1 || !room || ws.roomname !== room) return;
    
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

    // Send batch updates
    if (Object.keys(allKursiMeta).length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    }

    if (lastPointsData.length > 0) {
      this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    }

    // Send room user count
    const counts = this.getJumlahRoom();
    const count = counts[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
  }

  // ============ EVENT HANDLERS ============
  async handleSetIdTarget2(ws, id, baru) {
    if (!id) return;
    
    await this.withLock(`user-setid-${id}`, async () => {
      if (baru === true) {
        // New user - force cleanup and reset
        await this.forceUserCleanup(id);
        ws.idtarget = id;
        ws.roomname = undefined;
        ws.numkursi = new Set();
        this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      // Set user ID
      ws.idtarget = id;
      
      // Check if user can reconnect to previous seat
      const seatInfo = this.userToSeat.get(id);
      const seatMap = seatInfo ? this.roomSeats.get(seatInfo.room) : null;
      const currentSeatInfo = seatMap?.get(seatInfo?.seat);
      
      const canReconnect = seatInfo && 
                         currentSeatInfo?.namauser === id &&
                         !this.disconnectedTimers.has(id);
      
      if (canReconnect) {
        // Reconnect to previous seat
        const { room, seat } = seatInfo;
        ws.roomname = room;
        ws.numkursi = new Set([seat]);
        
        // Add to room clients
        const clientSet = this.roomClients.get(room);
        if (clientSet) clientSet.add(ws);
        
        // Send room state
        this.sendAllStateTo(ws, room);
        this.broadcastRoomUserCount(room);
        
        // Send VIP badges and current number
        await this.vipManager.getAllVipBadges(ws, room);
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        
      } else {
        // Cannot reconnect - need to join room
        await this.forceUserCleanup(id);
        
        if (ws.readyState === 1) {
          setTimeout(() => {
            if (ws.readyState === 1) {
              this.safeSend(ws, ["needJoinRoom"]);
            }
          }, 100);
        }
      }
    });
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;
    
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
      
      // Remove from all client sets
      for (const clientSet of this.roomClients.values()) {
        clientSet.delete(ws);
      }
      
      this.clients.delete(ws);
      
      // Close connection
      if (ws.readyState === 1) {
        try {
          ws.close(1000, "Manual destroy");
        } catch (error) {
          // Ignore close errors
        }
      }
    });
  }

  async fullRemoveById(idtarget) {
    if (!idtarget) return;
    
    await this.withLock(`full-remove-${idtarget}`, async () => {
      this.cancelCleanup(idtarget);
      
      // Cleanup VIP badges
      await this.vipManager.cleanupUserVipBadges(idtarget);
      
      // Remove from all seats
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;

        for (const [seatNumber, info] of seatMap) {
          if (info.namauser === idtarget) {
            Object.assign(info, createEmptySeat());
            this.clearSeatBuffer(room, seatNumber);
            this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          }
        }
        
        this.broadcastRoomUserCount(room);
      }

      // Remove from tracking
      this.userToSeat.delete(idtarget);

      // Close and remove all connections for this user
      for (const client of Array.from(this.clients)) {
        if (client && client.idtarget === idtarget) {
          if (client.readyState === 1) {
            client.close(1000, "Session removed");
          }
          this.clients.delete(client);
          
          for (const clientSet of this.roomClients.values()) {
            clientSet.delete(client);
          }
        }
      }
    });
  }

  // ============ UTILITY METHODS ============
  getAllOnlineUsers() {
    const users = [];
    for (const client of this.clients) {
      if (client.idtarget && client.readyState === 1) {
        users.push(client.idtarget);
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    const clientSet = this.roomClients.get(roomName);
    if (clientSet) {
      for (const client of clientSet) {
        if (client.idtarget && client.readyState === 1) {
          users.push(client.idtarget);
        }
      }
    }
    return users;
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      if (!roomList.includes(room)) continue;
      
      const updates = [];
      for (const [seat, info] of seatMapUpdates.entries()) {
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
      }
      
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      
      // Clear buffer
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    
    for (const client of this.clients) {
      if (client.readyState === 1 && client.roomname) {
        this.safeSend(client, ["currentNumber", this.currentNumber]);
      }
    }
  }

  // ============ MAIN MESSAGE HANDLER ============
  async handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;

    // Check message size
    if (raw.length > 100000) {
      ws.close(1009, "Message too large");
      return;
    }

    // Parse message
    let data;
    try { 
      data = JSON.parse(raw); 
      ws.errorCount = 0;
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
          await this.vipManager.handleEvent(ws, data);
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
            if (client.idtarget === idtarget && client.readyState === 1) {
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
            if (client.idtarget === idt && client.readyState === 1) {
              this.safeSend(client, out);
              break;
            }
          }
          break;
        }

        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";
          const isOnline = Array.from(this.clients).some(
            c => c.idtarget === username && c.readyState === 1
          );
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

          // Format: ["gift", roomname, sender, receiver, giftName, timestamp]
          // Sesuai dengan client Java yang mengharapkan 5 parameter + timestamp
          const timestamp = Date.now();
          const giftData = ["gift", roomname, sender, receiver, giftName, timestamp];
          
          this.broadcastToRoom(roomname, giftData);
          break;
        }

        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (ws.roomname === "LowCard") {
            await this.lowcard.handleEvent(ws, data);
          }
          break;
          
        default: 
          // Ignore unknown events
          break;
      }
    } catch (error) {
      console.error(`Error handling event ${evt}:`, error);
      
      // Send error to client if possible
      if (ws.readyState === 1) {
        this.safeSend(ws, ["error", `Server error: ${error.message}`]);
      }
    }
  }

  // ============ WEBSOCKET SERVER ============
  async fetch(request) {
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

    // Add to clients
    this.clients.add(ws);

    // Setup event listeners
    ws.addEventListener("message", (ev) => {
      this.handleMessage(ws, ev.data).catch(error => {
        console.error("Unhandled error in message handler:", error);
      });
    });

    ws.addEventListener("error", (event) => {
      // Log error but don't crash
      console.error("WebSocket error:", event.error);
    });

    ws.addEventListener("close", (event) => {
      // Schedule cleanup if not manual destroy
      if (ws.idtarget && !ws.isManualDestroy) {
        this.scheduleCleanup(ws.idtarget);
      }
      
      // Remove from all client sets
      for (const clientSet of this.roomClients.values()) {
        clientSet.delete(ws);
      }
      
      // Remove from main clients set
      this.clients.delete(ws);
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(req, env) {
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
  }
};
