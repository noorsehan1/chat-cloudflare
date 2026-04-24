// ==================== CHAT SERVER - SIMPLE CLEAN ====================
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
  LOCK_TIMEOUT_MS: 5000,
  PM_BUFFER_MAX_SIZE: 1000,
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

class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
    this._isDestroyed = false;
    this._maxQueueSize = CONSTANTS.PM_BUFFER_MAX_SIZE;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    if (this._isDestroyed) return;
    try {
      if (this._queue.length >= this._maxQueueSize) this._queue.shift();
      this._queue.push({ targetId, message });
      if (!this._isProcessing) this._process();
    } catch (e) {}
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
    this._isProcessing = false;
    this._flushCallback = null;
  }
}

class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = 25;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(room, message) {
    if (this._isDestroyed) {
      this._sendImmediate(room, message);
      return;
    }
    try {
      const roomSize = this._roomQueueSizes.get(room) || 0;
      if (roomSize >= this.MAX_PER_ROOM || this._messageQueue.length >= this.maxQueueSize) {
        this._sendImmediate(room, message);
        return;
      }
      this._messageQueue.push({ room, message, timestamp: Date.now() });
      this._roomQueueSizes.set(room, roomSize + 1);
    } catch (e) {
      this._sendImmediate(room, message);
    }
  }

  tick(now) {
    if (this._isDestroyed) return;
    try {
      this._cleanupExpiredMessages(now);
      this._flush();
    } catch (e) {}
  }

  _cleanupExpiredMessages(now) {
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      const item = this._messageQueue[i];
      if (item && now - item.timestamp > this.messageTTL) {
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
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
        if (item) {
          const roomSize = this._roomQueueSizes.get(item.room) || 0;
          this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
        }
      }
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
      try { this._flushCallback(room, message); } catch (e) {}
    }
  }

  async flushAll() {
    while (this._messageQueue.length > 0 && !this._isDestroyed) {
      this._flush();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  async destroy() {
    this._isDestroyed = true;
    await this.flushAll();
    this._messageQueue = [];
    this._roomQueueSizes.clear();
    this._flushCallback = null;
  }
}

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

  findSeatByUserId(userId) {
    for (const [seat, seatData] of this.seats) {
      if (seatData && seatData.namauser === userId) {
        return seat;
      }
    }
    return null;
  }

  removeUserCompletely(userId) {
    const seatNumber = this.findSeatByUserId(userId);
    if (seatNumber) {
      this.seats.delete(seatNumber);
      this.points.delete(seatNumber);
      return seatNumber;
    }
    return null;
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

  destroy() {
    this.seats.clear();
    this.points.clear();
  }
}

// ─────────────────────────────────────────────
// ChatServer - SIMPLE CLEAN VERSION (FULL FIXED)
// ─────────────────────────────────────────────
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._masterTickCounter = 0;
    this._masterTimer = null;

    this._wsRawSet = new Set();
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg) => this._sendDirectToRoom(room, msg));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      try {
        const targetConnections = this.userConnections.get(targetId);
        if (targetConnections) {
          for (const client of targetConnections) {
            if (client && client.readyState === 1 && !client._isClosing) {
              await this.safeSend(client, message);
              break;
            }
          }
        }
      } catch(e) {}
    });

    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      this.lowcard = null;
    }

    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    this._startMasterTimer();
  }

  async _cleanupWebSocket(ws) {
    if (!ws || ws._isClosing) return;
    
    // Jika koneksi ini digantikan oleh koneksi baru, jangan cleanup data user
    if (ws._isReplaced) {
      console.log(`[CLEANUP] Skipping cleanup for replaced connection: ${ws.idtarget}`);
      ws._isClosing = true;
      this._wsRawSet.delete(ws);
      return;
    }
    
    ws._isClosing = true;
    
    try {
      const userId = ws.idtarget;
      const roomName = ws.roomname;
      
      // Cek apakah masih ada koneksi aktif lain untuk user ini
      let hasOtherConnection = false;
      if (userId) {
        const otherConns = this.userConnections.get(userId);
        if (otherConns) {
          for (const conn of otherConns) {
            if (conn !== ws && conn.readyState === 1 && !conn._isClosing) {
              hasOtherConnection = true;
              break;
            }
          }
        }
      }
      
      // Hanya hapus seat jika tidak ada koneksi lain
      if (!hasOtherConnection && userId && roomName) {
        const roomManager = this.roomManagers.get(roomName);
        if (roomManager) {
          const seatRemoved = roomManager.removeUserCompletely(userId);
          if (seatRemoved) {
            console.log(`[CLEANUP] Removed user ${userId} from seat ${seatRemoved} in ${roomName}`);
            this.broadcastToRoom(roomName, ["removeKursi", roomName, seatRemoved]);
            this.updateRoomCount(roomName);
          }
        }
      }
      
      // Hapus tracking user jika tidak ada koneksi lain
      if (!hasOtherConnection && userId) {
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      
      // Hapus WebSocket dari semua tracking
      if (userId) {
        const userConns = this.userConnections.get(userId);
        if (userConns) {
          userConns.delete(ws);
          if (userConns.size === 0) {
            this.userConnections.delete(userId);
          }
        }
      }
      
      if (roomName) {
        const clientSet = this.roomClients.get(roomName);
        if (clientSet) clientSet.delete(ws);
      }
      
      this._wsRawSet.delete(ws);
      
      // Tutup koneksi jika masih terbuka
      if (ws.readyState === 1) {
        try { ws.close(1000, "Cleanup done"); } catch(e) {}
      }
      
      ws.roomname = undefined;
      ws.idtarget = undefined;
      
    } catch (e) {
      console.error(`[CLEANUP] Error:`, e);
    }
  }

  async _cleanupUserFromAllRooms(userId) {
    if (!userId) return;
    
    console.log(`[CLEANUP_USER] Removing ${userId} from all rooms`);
    
    for (const [room, roomManager] of this.roomManagers) {
      const seatRemoved = roomManager.removeUserCompletely(userId);
      if (seatRemoved) {
        this.broadcastToRoom(room, ["removeKursi", room, seatRemoved]);
        this.updateRoomCount(room);
      }
    }
    
    this.userToSeat.delete(userId);
    this.userCurrentRoom.delete(userId);
    
    const conns = this.userConnections.get(userId);
    if (conns) {
      for (const ws of conns) {
        if (ws && ws.readyState === 1) {
          try { ws.close(1000, "Force cleanup"); } catch(e) {}
        }
      }
      this.userConnections.delete(userId);
    }
  }

  _startMasterTimer() {
    if (this._masterTimer) clearInterval(this._masterTimer);
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }

  async _masterTick() {
    if (this._isClosing) return;
    
    try {
      this._masterTickCounter++;
      
      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        await this._handleNumberTick();
      }
      
      if (this.chatBuffer) this.chatBuffer.tick(Date.now());
      
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try { this.lowcard.masterTick(); } catch(e) {}
      }
    } catch (error) {
      console.error(`[MASTER TICK] ${error?.message}`);
    }
  }

  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        if (roomManager) roomManager.setCurrentNumber(this.currentNumber);
      }
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const client of this._wsRawSet) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
          try { client.send(message); } catch (e) {}
        }
      }
    } catch (error) {}
  }

  async assignNewSeat(room, userId) {
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager || roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;

      const existingSeat = roomManager.findSeatByUserId(userId);
      if (existingSeat) return existingSeat;

      const newSeatNumber = roomManager.addNewSeat(userId);
      if (!newSeatNumber) return null;

      this.userToSeat.set(userId, { room, seat: newSeatNumber });
      this.userCurrentRoom.set(userId, room);
      this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      return newSeatNumber;
    } catch(e) {
      return null;
    }
  }

  getRoomCount(room) { 
    try {
      const rm = this.roomManagers.get(room);
      return rm ? rm.getOccupiedCount() : 0; 
    } catch(e) { return 0; }
  }

  updateRoomCount(room) {
    try {
      const count = this.getRoomCount(room);
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { return 0; }
  }

  _sendDirectToRoom(room, msg) {
    try {
      const clientSet = this.roomClients.get(room);
      if (!clientSet?.size) return 0;
      const messageStr = JSON.stringify(msg);
      let sentCount = 0;
      
      for (const client of clientSet) {
        if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
          try {
            client.send(messageStr);
            sentCount++;
          } catch (e) {
            this._cleanupWebSocket(client).catch(()=>{});
          }
        }
      }
      return sentCount;
    } catch(e) { return 0; }
  }

  broadcastToRoom(room, msg) {
    try {
      if (!room || !roomList.includes(room)) return 0;
      
      if (msg[0] === "gift") {
        return this._sendDirectToRoom(room, msg);
      }
      
      if (msg[0] === "chat") {
        if (this.chatBuffer) this.chatBuffer.add(room, msg);
        return this.roomClients.get(room)?.size || 0;
      }
      
      return this._sendDirectToRoom(room, msg);
    } catch (error) { return 0; }
  }

  async safeSend(ws, msg) {
    if (!ws) return false;
    if (ws._isClosing || ws.readyState !== 1) return false;
    try {
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      ws.send(message);
      return true;
    } catch (error) {
      await this._cleanupWebSocket(ws);
      return false;
    }
  }

  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || ws._isClosing) return;
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return;
      
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      
      const allKursiMeta = roomManager.getAllSeatsMeta();
      const lastPointsData = roomManager.getAllPoints();
      const seatInfo = this.userToSeat.get(ws.idtarget);
      const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
      
      let filteredMeta = allKursiMeta;
      if (excludeSelfSeat && selfSeat) {
        filteredMeta = {};
        for (const [seat, data] of Object.entries(allKursiMeta)) {
          if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
        }
      }
      
      if (Object.keys(filteredMeta).length > 0) {
        await this.safeSend(ws, ["allUpdateKursiList", room, filteredMeta]);
      }
      if (lastPointsData.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }
    } catch (error) {}
  }

  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) {
      await this.safeSend(ws, ["error", "User ID not set"]);
      return false;
    }
    if (!roomList.includes(room)) {
      await this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }

    try {
      const oldRoom = ws.roomname;
      
      if (oldRoom && oldRoom !== room) {
        const oldRoomManager = this.roomManagers.get(oldRoom);
        if (oldRoomManager) {
          const oldSeat = oldRoomManager.removeUserCompletely(ws.idtarget);
          if (oldSeat) {
            this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
            this.updateRoomCount(oldRoom);
          }
        }
        this.userToSeat.delete(ws.idtarget);
        this.userCurrentRoom.delete(ws.idtarget);
      }
      
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      let assignedSeat = null;
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const seatData = roomManager.getSeat(seatNum);
        if (seatData && seatData.namauser === ws.idtarget) {
          assignedSeat = seatNum;
        }
      }
      
      if (!assignedSeat) {
        if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
          await this.safeSend(ws, ["roomFull", room]);
          return false;
        }
        assignedSeat = await this.assignNewSeat(room, ws.idtarget);
        if (!assignedSeat) {
          await this.safeSend(ws, ["roomFull", room]);
          return false;
        }
      }

      ws.roomname = room;
      
      let clientSet = this.roomClients.get(room);
      if (!clientSet) {
        clientSet = new Set();
        this.roomClients.set(room, clientSet);
      }
      clientSet.add(ws);

      let userConns = this.userConnections.get(ws.idtarget);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(ws.idtarget, userConns);
      }
      userConns.add(ws);

      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      await new Promise(resolve => setTimeout(resolve, 1000));
      await this.sendAllStateTo(ws, room, true);

      return true;
    } catch (error) {
      console.error(`[JOIN_ROOM] Error:`, error);
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;

    try {
      if (ws.readyState !== 1) {
        console.log(`[SET_ID] WebSocket closed for ${id}`);
        return;
      }
      
      console.log(`[SET_ID] Setting up for ${id}, baru=${baru}`);
      
      // Ambil koneksi lama SEBELUM menambah yang baru
      const existingConns = this.userConnections.get(id);
      
      ws.idtarget = id;
      ws._isClosing = false;
      ws._isReplaced = false;
      
      // Tambahkan koneksi baru ke tracking
      let userConns = this.userConnections.get(id);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(id, userConns);
      }
      userConns.add(ws);
      this._wsRawSet.add(ws);
      
      // Tutup semua koneksi LAMA (kecuali yang sekarang)
      if (existingConns) {
        for (const oldWs of existingConns) {
          if (oldWs !== ws && oldWs.readyState === 1) {
            console.log(`[SET_ID] Closing old connection for ${id}`);
            oldWs._isReplaced = true;
            oldWs._isClosing = true;
            try { 
              oldWs.close(1000, "Replaced by new connection"); 
            } catch (e) {}
          }
        }
      }
      
      // Kirim response ke client
      if (ws.readyState === 1) {
        await this.safeSend(ws, ["joinroomawal"]);
      }
      
    } catch (error) {
      console.error(`[SET_ID_TARGET] Error:`, error);
      if (ws && ws.readyState === 1) {
        await this.safeSend(ws, ["error", "Connection failed"]);
      }
    }
  }

  async handleForceResetUser(ws, userId) {
    if (!userId || userId !== ws.idtarget) {
      await this.safeSend(ws, ["error", "Cannot reset another user"]);
      return;
    }
    
    console.log(`[FORCE_RESET] Resetting user ${userId}`);
    await this._cleanupUserFromAllRooms(userId);
    await this.safeSend(ws, ["resetComplete", userId]);
    await this.safeSend(ws, ["joinroomawal"]);
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    if (raw instanceof ArrayBuffer) return;
    
    let messageStr = raw;
    if (typeof raw !== 'string') {
      try { messageStr = new TextDecoder().decode(raw); } catch (e) { return; }
    }
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    
    let data;
    try { data = JSON.parse(messageStr); } catch (e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;
    try { await this._processMessage(ws, data, data[0]); } catch (error) {}
  }

  async _processMessage(ws, data, evt) {
    try {
      switch (evt) {
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
        case "forceResetUser":
          await this.handleForceResetUser(ws, data[1]);
          break;
        case "joinRoom": {
          const success = await this.handleJoinRoom(ws, data[1]);
          if (success && ws.roomname) this.updateRoomCount(ws.roomname);
          break;
        }
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          
          if (!ws.roomname) return;
          if (ws.roomname !== roomname) return;
          if (ws.idtarget !== username) return;
          if (!roomList.includes(roomname)) return;
          
          const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          if (sanitizedMessage.includes('\0')) return;
          
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
          break;
        }
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || !roomList.includes(room) || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          const roomManager = this.roomManagers.get(room);
          if (!roomManager) return;
          const seatData = roomManager.getSeat(seat);
          if (!seatData || seatData.namauser !== ws.idtarget) return;
          if (roomManager.updatePoint(seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 || fast === true })) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          const roomManager = this.roomManagers.get(room);
          if (!roomManager) return;
          const seatData = roomManager.getSeat(seat);
          if (!seatData || seatData.namauser !== ws.idtarget) return;
          
          roomManager.removeSeat(seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.updateRoomCount(room);
          this.userToSeat.delete(ws.idtarget);
          this.userCurrentRoom.delete(ws.idtarget);
          
          const clientSet = this.roomClients.get(room);
          if (clientSet) clientSet.delete(ws);
          ws.roomname = undefined;
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
          const roomManager = this.roomManagers.get(room);
          if (!roomManager) return;
          const updatedSeat = {
            noimageUrl: noimageUrl?.slice(0, 255) || "",
            namauser: namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0,
          };
          roomManager.updateSeat(seat, updatedSeat);
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, updatedSeat]]]);
          break;
        }
        case "setMuteType": {
          const isMuted = data[1], roomName = data[2];
          if (roomName && roomList.includes(roomName)) {
            const success = this.setRoomMute(roomName, isMuted);
            await this.safeSend(ws, ["muteTypeSet", !!isMuted, success, roomName]);
          }
          break;
        }
        case "getMuteType": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName)) {
            await this.safeSend(ws, ["muteTypeResponse", this.roomManagers.get(roomName).getMute(), roomName]);
          }
          break;
        }
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of roomList) counts[room] = this.getRoomCount(room);
          await this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        case "getRoomUserCount": {
          const roomName = data[1];
          if (roomList.includes(roomName)) await this.safeSend(ws, ["roomUserCount", roomName, this.getRoomCount(roomName)]);
          break;
        }
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        case "isUserOnline": {
          const username = data[1];
          let isOnline = false;
          const connections = this.userConnections.get(username);
          if (connections && connections.size > 0) {
            for (const conn of connections) {
              if (conn && conn.readyState === 1 && !conn._isClosing) { isOnline = true; break; }
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] ?? ""]);
          break;
        }
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, safeGiftName, Date.now()]);
          break;
        }
        case "rollangak": {
          const [, roomname, username, angka] = data;
          if (!roomList.includes(roomname)) return;
          this.broadcastToRoom(roomname, ["rollangakBroadcast", roomname, username, angka]);
          break;
        }
        case "modwarning": {
          const [, roomname] = data;
          if (!roomList.includes(roomname)) return;
          this.broadcastToRoom(roomname, ["modwarning", roomname]);
          break;
        }
        case "getOnlineUsers": {
          const users = [];
          for (const [userId, connections] of this.userConnections) {
            for (const conn of connections) {
              if (conn && conn.readyState === 1 && !conn._isClosing) {
                users.push(userId);
                break;
              }
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const targetConnections = this.userConnections.get(idtarget);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                await this.safeSend(client, ["notif", noimageUrl, username, deskripsi, Date.now()]);
                break;
              }
            }
          }
          break;
        }
        case "private": {
          const [, idtarget, noimageUrl, message, sender] = data;
          if (!idtarget || !sender) return;
          await this.safeSend(ws, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
          this.pmBuffer.add(idtarget, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
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
              await this.safeSend(ws, ["gameLowCardError", "Game error, please try again"]);
            }
          } else if (!GAME_ROOMS.includes(ws.roomname)) {
            await this.safeSend(ws, ["gameLowCardError", "Game not available in this room"]);
          }
          break;
        case "onDestroy":
          await this._cleanupWebSocket(ws);
          break;
        default:
          break;
      }
    } catch (error) {}
  }

  setRoomMute(roomName, isMuted) {
    try {
      const roomManager = this.roomManagers.get(roomName);
      if (!roomManager) return false;
      const muteValue = roomManager.setMute(isMuted);
      this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
      return true;
    } catch(e) { return false; }
  }

  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    if (this._masterTimer) { clearInterval(this._masterTimer); this._masterTimer = null; }
    if (this.chatBuffer) await this.chatBuffer.destroy();
    if (this.pmBuffer) await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') try { await this.lowcard.destroy(); } catch (e) {}

    for (const ws of this._wsRawSet) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { await this._cleanupWebSocket(ws); } catch (e) {}
      }
    }

    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this._wsRawSet.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          for (const ws of this._wsRawSet) {
            if (ws && ws.readyState === 1 && !ws._isClosing) activeCount++;
          }
          return new Response(JSON.stringify({
            status: "healthy",
            connections: activeCount,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        return new Response("ChatServer Running", { status: 200 });
      }

      if (this._wsRawSet.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }

      let pair;
      let client;
      let server;
      
      try {
        pair = new WebSocketPair();
        client = pair[0];
        server = pair[1];
      } catch (e) {
        return new Response("WebSocket creation failed", { status: 500 });
      }

      try {
        server.accept();
      } catch (acceptError) {
        try { if (server) server.close(); } catch(e) {}
        try { if (client) client.close(); } catch(e) {}
        return new Response("WebSocket accept failed", { status: 500 });
      }

      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._isReplaced = false;

      this._wsRawSet.add(ws);

      const messageHandler = async (ev) => {
        await this.handleMessage(ws, ev.data);
      };
      
      const errorHandler = async () => { 
        await this._cleanupWebSocket(ws);
      };
      
      const closeHandler = async () => { 
        await this._cleanupWebSocket(ws);
      };

      ws.addEventListener("message", messageHandler);
      ws.addEventListener("error", errorHandler);
      ws.addEventListener("close", closeHandler);

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error(`[FETCH ERROR] ${error?.message || 'Unknown'}`);
      return new Response("Internal server error", { status: 500 });
    }
  }

  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) counts[room] = this.getRoomCount(room);
    return counts;
  }
}

// ─────────────────────────────────────────────
// Worker Export
// ─────────────────────────────────────────────
export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER.get(chatId);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") return chatObj.fetch(req);
      return new Response("ChatServer Running", { status: 200 });
    } catch (error) {
      console.error(`[WORKER ERROR] ${error?.message || 'Unknown'}`);
      return new Response("Server error", { status: 500 });
    }
  }
}
