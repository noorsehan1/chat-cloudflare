// ==================== CHAT SERVER - NO STORAGE VERSION ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-13"

let LowCardGameManager;
try {
  LowCardGameManager = (await import("./lowcard.js")).LowCardGameManager;
} catch (e) {
  LowCardGameManager = class StubLowCardGameManager {
    constructor() {}
    masterTick() {}
    async handleEvent() {}
    async destroy() {}
  };
}

const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  MAX_GLOBAL_CONNECTIONS: 250,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 5000,
  MAX_MESSAGE_LENGTH: 5000,
  MAX_USERNAME_LENGTH: 30,
  MAX_GIFT_NAME: 30,
  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MESSAGE_TTL_MS: 8000,
  PM_BATCH_SIZE: 5,
  PM_BATCH_DELAY_MS: 30,
  LOCK_TIMEOUT_MS: 3000,
  PM_BUFFER_MAX_SIZE: 1000,
  ORPHAN_CLEANUP_INTERVAL_TICKS: 30,
  FORCE_CLEANUP_INTERVAL_TICKS: 60, // Tambahan cleanup paksa
});

const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "Indonesia", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"
]);

// ─────────────────────────────────────────────
// SimpleLock
// ─────────────────────────────────────────────
class SimpleLock {
  constructor(timeoutMs = CONSTANTS.LOCK_TIMEOUT_MS) {
    this._locked = false;
    this._waitQueue = [];
    this._timeoutMs = timeoutMs;
  }

  async acquire() {
    if (!this._locked) {
      this._locked = true;
      return () => { this._release(); };
    }
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this._waitQueue.findIndex(item => item.resolve === resolve);
        if (index !== -1) this._waitQueue.splice(index, 1);
        reject(new Error('Lock timeout'));
      }, this._timeoutMs);
      
      this._waitQueue.push({
        resolve: () => {
          clearTimeout(timeout);
          this._locked = true;
          resolve(() => { this._release(); });
        },
        reject
      });
    });
  }

  _release() {
    this._locked = false;
    if (this._waitQueue.length > 0) {
      const next = this._waitQueue.shift();
      if (next) next.resolve();
    }
  }
}

// ─────────────────────────────────────────────
// PMBuffer
// ─────────────────────────────────────────────
class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
    this._isDestroyed = false;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    if (this._isDestroyed || this._queue.length > CONSTANTS.PM_BUFFER_MAX_SIZE) return;
    this._queue.push({ targetId, message });
    if (!this._isProcessing) this._process();
  }

  async _process() {
    if (this._isProcessing || this._isDestroyed) return;
    this._isProcessing = true;

    while (this._queue.length > 0 && !this._isDestroyed) {
      const batch = this._queue.splice(0, this.BATCH_SIZE);
      for (const item of batch) {
        try {
          if (this._flushCallback) await this._flushCallback(item.targetId, item.message);
        } catch (e) {}
      }
      if (this._queue.length > 0 && !this._isDestroyed) {
        await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
      }
    }
    this._isProcessing = false;
  }

  async flushAll() {
    while (this._queue.length > 0 && !this._isDestroyed) {
      await this._process();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  async destroy() {
    this._isDestroyed = true;
    await this.flushAll();
    this._queue = [];
    this._flushCallback = null;
  }
}

// ─────────────────────────────────────────────
// GlobalChatBuffer
// ─────────────────────────────────────────────
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this._flushCallback = null;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(room, message) {
    if (this._isDestroyed) {
      this._sendImmediate(room, message);
      return;
    }

    if (this._messageQueue.length >= CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES) {
      this._sendImmediate(room, message);
      return;
    }

    this._messageQueue.push({ room, message, timestamp: Date.now() });
  }

  tick(now) {
    if (this._isDestroyed) return;
    this._cleanupExpiredMessages(now);
    this._flush();
  }

  _cleanupExpiredMessages(now) {
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      const item = this._messageQueue[i];
      if (item && now - item.timestamp > this.messageTTL) {
        this._messageQueue.splice(i, 1);
      }
    }
  }

  _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing || this._isDestroyed) return;
    this._isFlushing = true;

    try {
      const batch = this._messageQueue.splice(0);
      for (const item of batch) {
        try {
          if (item && this._flushCallback) this._flushCallback(item.room, item.message);
        } catch (e) {}
      }
    } finally {
      this._isFlushing = false;
    }
  }

  _sendImmediate(room, message) {
    if (this._flushCallback && !this._isDestroyed) {
      try {
        this._flushCallback(room, message);
      } catch (e) {}
    }
  }

  async destroy() {
    this._isDestroyed = true;
    this._messageQueue = [];
    this._flushCallback = null;
  }
}

// ─────────────────────────────────────────────
// RoomManager
// ─────────────────────────────────────────────
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addNewSeat(userId) {
    const newSeatNumber = this.getAvailableSeat();
    if (!newSeatNumber) return null;
    this.seats.set(newSeatNumber, {
      noimageUrl: "",
      namauser: userId,
      color: "",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0,
      lastUpdated: Date.now()
    });
    return newSeatNumber;
  }

  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }

  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const existingSeat = this.seats.get(seatNumber);
    const entry = {
      noimageUrl: seatData.noimageUrl?.slice(0, 255) || "",
      namauser: seatData.namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
      color: seatData.color || "",
      itembawah: seatData.itembawah || 0,
      itematas: seatData.itematas || 0,
      vip: seatData.vip || 0,
      viptanda: seatData.viptanda || 0,
      lastUpdated: Date.now()
    };
    if (existingSeat) {
      Object.assign(existingSeat, entry);
    } else {
      this.seats.set(seatNumber, entry);
    }
    return true;
  }

  removeSeat(seatNumber) {
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
    return deleted;
  }

  getOccupiedCount() { return this.seats.size; }

  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
      if (seat) {
        meta[seatNum] = {
          noimageUrl: seat.noimageUrl,
          namauser: seat.namauser,
          color: seat.color,
          itembawah: seat.itembawah,
          itematas: seat.itematas,
          vip: seat.vip,
          viptanda: seat.viptanda
        };
      }
    }
    return meta;
  }

  updatePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    this.points.set(seatNumber, {
      x: point.x,
      y: point.y,
      fast: point.fast || false,
      timestamp: Date.now()
    });
    return true;
  }

  getPoint(seatNumber) { return this.points.get(seatNumber) || null; }

  getAllPoints() {
    const points = [];
    for (const [seatNum, point] of this.points) {
      if (point) {
        points.push({ seat: seatNum, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
      }
    }
    return points;
  }

  setMute(isMuted) {
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    return this.muteStatus;
  }
  
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; }
  getCurrentNumber() { return this.currentNumber; }
  removePoint(seatNumber) { return this.points.delete(seatNumber); }
  destroy() { this.seats.clear(); this.points.clear(); }
}

// ─────────────────────────────────────────────
// ChatServer - NO STORAGE, AUTO RESET ON DEPLOY
// ─────────────────────────────────────────────
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._deployId = this._generateDeployId();

    // Locks
    this.roomLock = new SimpleLock();
    this.userLock = new SimpleLock();
    
    // WebSocket storage
    this._wsSet = new Set();
    
    // Data storage (semua di memory, tidak pakai state.storage)
    this.roomManagers = new Map();
    this.userToSeat = new Map();      // userId -> { room, seat }
    this.userConnections = new Map();  // userId -> Set of WebSocket
    this.roomClients = new Map();      // room -> Set of WebSocket

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;

    // Buffers
    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg) => this._sendToRoom(room, msg));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const connections = this.userConnections.get(targetId);
      if (connections) {
        for (const client of connections) {
          if (client && client.readyState === 1 && !client._isClosing) {
            await this.safeSend(client, message);
            break;
          }
        }
      }
    });

    // Game manager
    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      this.lowcard = null;
    }

    // Initialize rooms
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    // Start master timer
    this._masterTickCounter = 0;
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
    
    // Cek deploy setiap 5 detik (tidak pakai storage)
    this._deployCheckInterval = setInterval(() => this._checkDeploy(), 5000);
  }

  _generateDeployId() {
    const str = `${CONSTANTS.MAX_SEATS}|${CONSTANTS.MAX_NUMBER}|${roomList.length}|${Date.now()}`;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) - hash) + str.charCodeAt(i);
      hash = hash & hash;
    }
    return Math.abs(hash).toString(36);
  }

  _checkDeploy() {
    const newDeployId = this._generateDeployId();
    if (newDeployId !== this._deployId) {
      console.log(`[DEPLOY] New deployment detected! Resetting all data...`);
      this._deployId = newDeployId;
      this._resetAllData();
    }
  }

  _resetAllData() {
    console.log(`[RESET] Force resetting all data...`);
    
    // Close all connections with restart message
    for (const ws of this._wsSet) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          ws.send(JSON.stringify(["serverRestart", "Server restarting, please reconnect..."]));
          ws.close(1000, "Server restart");
        } catch (e) {}
      }
    }
    
    // Clear all data structures
    this._wsSet.clear();
    this.userToSeat.clear();
    this.userConnections.clear();
    
    // Reset all rooms
    for (const room of roomList) {
      if (this.roomManagers.has(room)) {
        this.roomManagers.get(room).destroy();
      }
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // Reset game
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try { this.lowcard.destroy(); } catch(e) {}
    }
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      this.lowcard = null;
    }
    
    this.currentNumber = 1;
    this._masterTickCounter = 0;
    this._startTime = Date.now();
    
    console.log(`[RESET] All data has been reset!`);
  }

  // ==================== CLEANUP SEDERHANA ====================
  async _cleanupUser(userId) {
    if (!userId) return;
    
    let release;
    try {
      release = await this.userLock.acquire();
    } catch(e) {
      return;
    }
    
    try {
      // Hapus dari semua room
      for (const [room, roomManager] of this.roomManagers) {
        for (const [seat, seatData] of roomManager.seats) {
          if (seatData && seatData.namauser === userId) {
            roomManager.removeSeat(seat);
            this._sendToRoom(room, ["removeKursi", room, seat]);
            this._updateRoomCount(room);
            break;
          }
        }
      }
      
      // Hapus dari semua Map
      this.userToSeat.delete(userId);
      this.userConnections.delete(userId);
      
    } finally {
      if (release) release();
    }
  }

  async _cleanupWebSocket(ws) {
    if (!ws || ws._isClosing) return;
    ws._isClosing = true;
    
    try {
      // Hapus dari room
      if (ws.roomname) {
        const clients = this.roomClients.get(ws.roomname);
        if (clients) clients.delete(ws);
      }
      
      // Hapus dari user connections
      if (ws.idtarget) {
        const connections = this.userConnections.get(ws.idtarget);
        if (connections) {
          connections.delete(ws);
          // Jika tidak ada koneksi lain, hapus semua data user
          if (connections.size === 0) {
            await this._cleanupUser(ws.idtarget);
          }
        }
      }
      
      // Hapus dari global set
      this._wsSet.delete(ws);
      
      // Tutup koneksi
      if (ws.readyState === 1) {
        try { ws.close(1000, "Cleanup"); } catch(e) {}
      }
      
      ws.roomname = undefined;
      ws.idtarget = undefined;
      
    } catch (e) {
      console.error(`[WS CLEANUP] Error:`, e);
    }
  }

  // Force cleanup untuk user yang tidak punya koneksi aktif
  async _forceCleanup() {
    const toCleanup = [];
    
    for (const [userId, seatInfo] of this.userToSeat) {
      const connections = this.userConnections.get(userId);
      let hasActive = false;
      if (connections) {
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            hasActive = true;
            break;
          }
        }
      }
      if (!hasActive) {
        toCleanup.push(userId);
      }
    }
    
    for (const userId of toCleanup) {
      await this._cleanupUser(userId);
    }
    
    if (toCleanup.length > 0) {
      console.log(`[FORCE CLEANUP] Removed ${toCleanup.length} orphaned users`);
    }
  }

  // ==================== MASTER TICK ====================
  async _masterTick() {
    if (this._isClosing) return;
    
    this._masterTickCounter++;
    const now = Date.now();
    
    // Number rotation
    if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const rm of this.roomManagers.values()) {
        rm.setCurrentNumber(this.currentNumber);
      }
      
      const msg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const client of this._wsSet) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
          try { client.send(msg); } catch(e) {}
        }
      }
    }
    
    // Orphan cleanup tiap 30 tick
    if (this._masterTickCounter % CONSTANTS.ORPHAN_CLEANUP_INTERVAL_TICKS === 0) {
      await this._forceCleanup();
    }
    
    // Chat buffer tick
    if (this.chatBuffer) this.chatBuffer.tick(now);
    
    // Game tick
    if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
      try { this.lowcard.masterTick(); } catch(e) {}
    }
  }

  // ==================== HELPER METHODS ====================
  _sendToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return 0;
    
    const msgStr = JSON.stringify(msg);
    let sent = 0;
    
    for (const client of clients) {
      if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
        try {
          client.send(msgStr);
          sent++;
        } catch(e) {
          this._cleanupWebSocket(client).catch(()=>{});
        }
      }
    }
    return sent;
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    if (msg[0] === "chat") {
      if (this.chatBuffer) this.chatBuffer.add(room, msg);
      return this.roomClients.get(room)?.size || 0;
    }
    
    return this._sendToRoom(room, msg);
  }

  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try {
      ws.send(typeof msg === "string" ? msg : JSON.stringify(msg));
      return true;
    } catch(e) {
      await this._cleanupWebSocket(ws);
      return false;
    }
  }

  _updateRoomCount(room) {
    const count = this.roomManagers.get(room)?.getOccupiedCount() || 0;
    this._sendToRoom(room, ["roomUserCount", room, count]);
    return count;
  }

  async assignSeat(room, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager || roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;
    
    // Cek apakah user sudah punya seat
    for (const [seat, seatData] of roomManager.seats) {
      if (seatData && seatData.namauser === userId) {
        return seat;
      }
    }
    
    const newSeat = roomManager.addNewSeat(userId);
    if (!newSeat) return null;
    
    this.userToSeat.set(userId, { room, seat: newSeat });
    this._sendToRoom(room, ["userOccupiedSeat", room, newSeat, userId]);
    this._updateRoomCount(room);
    
    return newSeat;
  }

  async sendAllState(ws, room, excludeSelf = true) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager || !ws || ws.readyState !== 1) return;
    
    await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    
    const seatInfo = this.userToSeat.get(ws.idtarget);
    const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
    
    // Kirim semua kursi (kecuali self)
    const allSeats = roomManager.getAllSeatsMeta();
    if (excludeSelf && selfSeat) delete allSeats[selfSeat];
    if (Object.keys(allSeats).length > 0) {
      await this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
    }
    
    // Kirim semua points (kecuali self)
    const allPoints = roomManager.getAllPoints();
    const otherPoints = excludeSelf && selfSeat ? allPoints.filter(p => p.seat !== selfSeat) : allPoints;
    if (otherPoints.length > 0) {
      await this.safeSend(ws, ["allPointsList", room, otherPoints]);
    }
  }

  // ==================== HANDLE JOIN ROOM ====================
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) {
      await this.safeSend(ws, ["error", "User ID not set"]);
      return false;
    }
    if (!roomList.includes(room)) {
      await this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    // FIRST: Cek apakah user sudah punya seat di room ini
    const existing = this.userToSeat.get(ws.idtarget);
    if (existing && existing.room === room) {
      const rm = this.roomManagers.get(room);
      const seatData = rm?.getSeat(existing.seat);
      if (seatData && seatData.namauser === ws.idtarget) {
        // User sudah punya seat, langsung reconnect
        ws.roomname = room;
        
        let clients = this.roomClients.get(room);
        if (!clients) {
          clients = new Set();
          this.roomClients.set(room, clients);
        }
        clients.add(ws);
        
        let conns = this.userConnections.get(ws.idtarget);
        if (!conns) {
          conns = new Set();
          this.userConnections.set(ws.idtarget, conns);
        }
        conns.add(ws);
        
        await this.safeSend(ws, ["rooMasuk", existing.seat, room]);
        await this.safeSend(ws, ["numberKursiSaya", existing.seat]);
        await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), room]);
        await this.sendAllState(ws, room, true);
        
        return true;
      } else {
        // Data tidak valid, hapus
        this.userToSeat.delete(ws.idtarget);
      }
    }
    
    // SECOND: Cek apakah user terblokir di room ini (data stale di seat)
    // Ini yang bikin user tidak bisa masuk - cek dan cleanup manual
    const roomManager = this.roomManagers.get(room);
    if (roomManager) {
      let staleSeat = null;
      for (const [seat, seatData] of roomManager.seats) {
        if (seatData && seatData.namauser === ws.idtarget) {
          staleSeat = seat;
          break;
        }
      }
      
      if (staleSeat) {
        // Ada data stale, hapus dulu
        console.log(`[JOIN] Found stale seat ${staleSeat} for user ${ws.idtarget} in room ${room}, cleaning...`);
        roomManager.removeSeat(staleSeat);
        this._sendToRoom(room, ["removeKursi", room, staleSeat]);
        this._updateRoomCount(room);
      }
    }
    
    let release;
    try {
      release = await this.roomLock.acquire();
    } catch(e) {
      await this.safeSend(ws, ["error", "Server busy"]);
      return false;
    }
    
    try {
      // Keluar dari room lama jika ada
      if (ws.roomname && ws.roomname !== room) {
        const oldRm = this.roomManagers.get(ws.roomname);
        if (oldRm) {
          for (const [seat, seatData] of oldRm.seats) {
            if (seatData && seatData.namauser === ws.idtarget) {
              oldRm.removeSeat(seat);
              this._sendToRoom(ws.roomname, ["removeKursi", ws.roomname, seat]);
              this._updateRoomCount(ws.roomname);
              break;
            }
          }
        }
        this.userToSeat.delete(ws.idtarget);
      }
      
      if (!roomManager) {
        if (release) release();
        return false;
      }
      
      // Cek kapasitas
      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        if (release) release();
        return false;
      }
      
      // Assign seat baru
      const seat = await this.assignSeat(room, ws.idtarget);
      if (!seat) {
        await this.safeSend(ws, ["roomFull", room]);
        if (release) release();
        return false;
      }
      
      ws.roomname = room;
      
      let clients = this.roomClients.get(room);
      if (!clients) {
        clients = new Set();
        this.roomClients.set(room, clients);
      }
      clients.add(ws);
      
      let conns = this.userConnections.get(ws.idtarget);
      if (!conns) {
        conns = new Set();
        this.userConnections.set(ws.idtarget, conns);
      }
      conns.add(ws);
      
      await this.safeSend(ws, ["rooMasuk", seat, room]);
      await this.safeSend(ws, ["numberKursiSaya", seat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      await this.sendAllState(ws, room, true);
      
      if (release) release();
      return true;
      
    } catch (error) {
      console.error(`[JOIN] Error:`, error);
      await this.safeSend(ws, ["error", "Failed to join"]);
      if (release) release();
      return false;
    }
  }

  // ==================== HANDLE SET ID ====================
  async handleSetIdTarget2(ws, id, isNew) {
    if (!id || !ws) return;
    
    let release;
    try {
      release = await this.userLock.acquire();
    } catch(e) {
      await this.safeSend(ws, ["error", "Server busy"]);
      return;
    }
    
    try {
      if (ws.readyState !== 1) {
        if (release) release();
        return;
      }
      
      if (isNew === true) {
        // User baru, bersihkan semua data lama
        await this._cleanupUser(id);
        
        const existingConns = this.userConnections.get(id);
        if (existingConns) {
          for (const oldWs of existingConns) {
            if (oldWs !== ws) {
              try { oldWs.close(1000, "New connection"); } catch(e) {}
            }
          }
          this.userConnections.delete(id);
        }
      } else {
        // User reconnect
        const existingConns = this.userConnections.get(id);
        if (existingConns) {
          for (const oldWs of existingConns) {
            if (oldWs !== ws && oldWs.readyState === 1) {
              try { oldWs.close(1000, "Replaced"); } catch(e) {}
            }
          }
          existingConns.clear();
        }
      }
      
      ws.idtarget = id;
      ws._isClosing = false;
      
      let conns = this.userConnections.get(id);
      if (!conns) {
        conns = new Set();
        this.userConnections.set(id, conns);
      }
      conns.add(ws);
      this._wsSet.add(ws);
      
      const seatInfo = this.userToSeat.get(id);
      
      if (seatInfo && isNew === false) {
        const { room, seat } = seatInfo;
        const roomManager = this.roomManagers.get(room);
        const seatData = roomManager?.getSeat(seat);
        
        if (seatData && seatData.namauser === id) {
          ws.roomname = room;
          
          const clients = this.roomClients.get(room);
          if (clients) clients.add(ws);
          
          // Kirim data sendiri
          const point = roomManager.getPoint(seat);
          if (point) {
            await this.safeSend(ws, ["pointUpdated", room, seat, point.x, point.y, point.fast ? 1 : 0]);
          }
          
          await this.safeSend(ws, ["numberKursiSaya", seat]);
          await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          
          // Kirim data user lain
          const allSeats = roomManager.getAllSeatsMeta();
          delete allSeats[seat];
          if (Object.keys(allSeats).length > 0) {
            await this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
          }
          
          const allPoints = roomManager.getAllPoints();
          const otherPoints = allPoints.filter(p => p.seat !== seat);
          if (otherPoints.length > 0) {
            await this.safeSend(ws, ["allPointsList", room, otherPoints]);
          }
          
          await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
          await this.safeSend(ws, ["reconnectSuccess", room, seat]);
          
          this._sendToRoom(room, ["userOccupiedSeat", room, seat, id]);
          
          if (release) release();
          return;
        }
        
        // Data stale
        this.userToSeat.delete(id);
      }
      
      // User baru atau data stale
      if (isNew === false) {
        await this.safeSend(ws, ["needJoinRoom"]);
      } else {
        await this.safeSend(ws, ["joinroomawal"]);
      }
      
    } catch (error) {
      console.error(`[SET ID] Error:`, error);
      if (ws && ws.readyState === 1) {
        await this.safeSend(ws, ["error", "Connection failed"]);
      }
    } finally {
      if (release) release();
    }
  }

  // ==================== HANDLE MESSAGE ====================
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    let msgStr = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
    if (msgStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    
    let data;
    try { data = JSON.parse(msgStr); } catch(e) { return; }
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
          
        case "chat": {
          const [, room, noimg, user, msg, userColor, textColor] = data;
          if (!ws.roomname || ws.roomname !== room || ws.idtarget !== user) return;
          if (!roomList.includes(room)) return;
          const cleanMsg = msg?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          if (cleanMsg.includes('\0')) return;
          this.broadcastToRoom(room, ["chat", room, noimg, user, cleanMsg, userColor, textColor]);
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          const rm = this.roomManagers.get(room);
          if (!rm) return;
          const seatData = rm.getSeat(seat);
          if (!seatData || seatData.namauser !== ws.idtarget) return;
          if (rm.updatePoint(seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 })) {
            this._sendToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (ws.roomname !== room || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          const rm = this.roomManagers.get(room);
          if (!rm) return;
          const seatData = rm.getSeat(seat);
          if (!seatData || seatData.namauser !== ws.idtarget) return;
          
          rm.removeSeat(seat);
          this._sendToRoom(room, ["removeKursi", room, seat]);
          this._updateRoomCount(room);
          this.userToSeat.delete(ws.idtarget);
          
          const clients = this.roomClients.get(room);
          if (clients) clients.delete(ws);
          ws.roomname = undefined;
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimg, user, color, bawah, atas, vip, tanda] = data;
          if (ws.roomname !== room || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (user !== ws.idtarget) return;
          const rm = this.roomManagers.get(room);
          if (!rm) return;
          
          rm.updateSeat(seat, {
            noimageUrl: noimg?.slice(0, 255) || "",
            namauser: user?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
            color: color || "",
            itembawah: bawah || 0,
            itematas: atas || 0,
            vip: vip || 0,
            viptanda: tanda || 0
          });
          this._sendToRoom(room, ["kursiBatchUpdate", room, [[seat, rm.getSeat(seat)]]]);
          break;
        }
        
        case "setMuteType": {
          const isMuted = data[1], roomName = data[2];
          if (roomName && roomList.includes(roomName)) {
            const rm = this.roomManagers.get(roomName);
            if (rm) {
              const muted = rm.setMute(isMuted);
              this._sendToRoom(roomName, ["muteStatusChanged", muted, roomName]);
              await this.safeSend(ws, ["muteTypeSet", !!isMuted, true, roomName]);
            }
          }
          break;
        }
        
        case "getMuteType": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName)) {
            const rm = this.roomManagers.get(roomName);
            await this.safeSend(ws, ["muteTypeResponse", rm?.getMute() || false, roomName]);
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of roomList) counts[room] = this.roomManagers.get(room)?.getOccupiedCount() || 0;
          await this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = data[1];
          if (roomList.includes(roomName)) {
            await this.safeSend(ws, ["roomUserCount", roomName, this.roomManagers.get(roomName)?.getOccupiedCount() || 0]);
          }
          break;
        }
        
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const username = data[1];
          let online = false;
          const conns = this.userConnections.get(username);
          if (conns) {
            for (const c of conns) {
              if (c && c.readyState === 1 && !c._isClosing) {
                online = true;
                break;
              }
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", username, online, data[2] ?? ""]);
          break;
        }
        
        case "gift": {
          const [, room, sender, receiver, gift] = data;
          if (!roomList.includes(room)) return;
          const safeGift = (gift || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          this._sendToRoom(room, ["gift", room, sender, receiver, safeGift, Date.now()]);
          break;
        }
        
        case "rollangak": {
          const [, room, user, angka] = data;
          if (!roomList.includes(room)) return;
          this._sendToRoom(room, ["rollangakBroadcast", room, user, angka]);
          break;
        }
        
        case "modwarning": {
          const [, room] = data;
          if (!roomList.includes(room)) return;
          this._sendToRoom(room, ["modwarning", room]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const [userId, conns] of this.userConnections) {
            for (const c of conns) {
              if (c && c.readyState === 1 && !c._isClosing) {
                users.push(userId);
                break;
              }
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "sendnotif": {
          const [, target, noimg, user, desc] = data;
          const conns = this.userConnections.get(target);
          if (conns) {
            for (const c of conns) {
              if (c && c.readyState === 1 && !c._isClosing) {
                await this.safeSend(c, ["notif", noimg, user, desc, Date.now()]);
                break;
              }
            }
          }
          break;
        }
        
        case "private": {
          const [, target, noimg, msg, sender] = data;
          if (!target || !sender) return;
          await this.safeSend(ws, ["private", target, noimg, msg, Date.now(), sender]);
          this.pmBuffer.add(target, ["private", target, noimg, msg, Date.now(), sender]);
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            try {
              await this.lowcard.handleEvent(ws, data);
            } catch (error) {
              await this.safeSend(ws, ["gameLowCardError", "Game error"]);
            }
          }
          break;
          
        case "onDestroy":
          await this._cleanupWebSocket(ws);
          break;
      }
    } catch (error) {
      console.error(`[MSG] Error:`, error);
    }
  }

  // ==================== FETCH / WEBSOCKET ====================
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      // HTTP endpoints
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health" || url.pathname === "/") {
          const active = Array.from(this._wsSet).filter(ws => ws && ws.readyState === 1 && !ws._isClosing).length;
          const roomCounts = {};
          for (const [room, rm] of this.roomManagers) {
            roomCounts[room] = rm.getOccupiedCount();
          }
          return new Response(JSON.stringify({
            status: "healthy",
            connections: active,
            rooms: roomCounts,
            totalSeats: active,
            uptime: Date.now() - this._startTime,
            deployId: this._deployId
          }), { headers: { "content-type": "application/json" } });
        }
        
        if (url.pathname === "/reset") {
          this._resetAllData();
          return new Response("All data has been reset!", { status: 200 });
        }
        
        if (url.pathname === "/cleanup") {
          await this._forceCleanup();
          return new Response("Cleanup completed", { status: 200 });
        }
        
        if (url.pathname === "/debug") {
          const userCount = this.userToSeat.size;
          const wsCount = this._wsSet.size;
          const activeWs = Array.from(this._wsSet).filter(ws => ws && ws.readyState === 1).length;
          const roomCounts = {};
          for (const [room, rm] of this.roomManagers) {
            roomCounts[room] = rm.getOccupiedCount();
          }
          return new Response(JSON.stringify({
            userToSeatSize: userCount,
            wsSetSize: wsCount,
            activeConnections: activeWs,
            rooms: roomCounts,
            deployId: this._deployId
          }, null, 2), { headers: { "content-type": "application/json" } });
        }
        
        return new Response("Chat Server Running", { status: 200 });
      }
      
      // WebSocket
      if (this._wsSet.size >= CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server full", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      server.accept();
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      
      this._wsSet.add(ws);
      
      const abortController = new AbortController();
      
      ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data), { signal: abortController.signal });
      ws.addEventListener("error", () => this._cleanupWebSocket(ws), { signal: abortController.signal });
      ws.addEventListener("close", () => this._cleanupWebSocket(ws), { signal: abortController.signal });
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      console.error(`[FETCH] Error:`, error);
      return new Response("Server error", { status: 500 });
    }
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    if (this._masterTimer) clearInterval(this._masterTimer);
    if (this._deployCheckInterval) clearInterval(this._deployCheckInterval);
    
    for (const ws of this._wsSet) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          ws.close(1000, "Server shutdown");
        } catch(e) {}
      }
    }
    
    if (this.chatBuffer) await this.chatBuffer.destroy();
    if (this.pmBuffer) await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      await this.lowcard.destroy();
    }
    
    this._wsSet.clear();
    this.userToSeat.clear();
    this.userConnections.clear();
    this.roomClients.clear();
    for (const rm of this.roomManagers.values()) rm.destroy();
    this.roomManagers.clear();
  }
}

// ─────────────────────────────────────────────
// Worker Export
// ─────────────────────────────────────────────
export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER.idFromName("main");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    } catch (error) {
      console.error(`[WORKER] Error:`, error);
      return new Response("Server error", { status: 500 });
    }
  }
}
