// ==================== CHAT SERVER 2 - FINAL STABLE VERSION ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-03"
//
// Durable Object Binding: CHAT_SERVER_2
// Class Name: ChatServer2

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
  ZOMBIE_CLEANUP_TICKS: 600,

  MAX_GLOBAL_CONNECTIONS: 250,
  MAX_ACTIVE_CLIENTS_LIMIT: 250,
  MAX_SEATS: 25,
  MAX_NUMBER: 6,

  MAX_MESSAGE_SIZE: 5000,
  MAX_MESSAGE_LENGTH: 5000,
  MAX_USERNAME_LENGTH: 30,
  MAX_GIFT_NAME: 30,

  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MESSAGE_TTL_MS: 8000,

  MAX_CONNECTIONS_PER_USER: 2,

  ROOM_IDLE_BEFORE_CLEANUP: 15 * 60 * 1000,

  PM_BATCH_SIZE: 5,
  PM_BATCH_DELAY_MS: 30,

  WS_ACCEPT_TIMEOUT_MS: 5000,
  FORCE_CLEANUP_TIMEOUT_MS: 2000,

  CONNECTION_CRITICAL_THRESHOLD_RATIO: 0.9,
  CONNECTION_WARNING_THRESHOLD_RATIO: 0.75,
  FORCE_CLEANUP_MEMORY_TICKS: 60,
  
  RECONNECT_GRACE_PERIOD_MS: 3000,
  SEAT_RELEASE_DELAY_MS: 500,
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
// AsyncLock
// ─────────────────────────────────────────────
class AsyncLock {
  constructor(timeoutMs = 2000) {
    this.locks = new Map();
    this.waitingQueues = new Map();
    this.timeoutMs = timeoutMs;
  }

  async acquire(key) {
    if (!this.locks.has(key)) {
      this.locks.set(key, true);
      return () => this._release(key);
    }

    if (!this.waitingQueues.has(key)) {
      this.waitingQueues.set(key, []);
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const queue = this.waitingQueues.get(key);
        const index = queue?.findIndex(item => item.resolve === resolve);
        if (index !== undefined && index > -1) {
          queue.splice(index, 1);
          reject(new Error(`Lock timeout: ${key}`));
        }
      }, this.timeoutMs);

      this.waitingQueues.get(key).push({
        resolve: () => {
          clearTimeout(timeout);
          this.locks.set(key, true);
          resolve(() => this._release(key));
        },
        reject
      });
    });
  }

  _release(key) {
    this.locks.delete(key);
    const queue = this.waitingQueues.get(key);
    if (queue && queue.length > 0) {
      const next = queue.shift();
      if (next) next.resolve();
    }
    if (queue && queue.length === 0) this.waitingQueues.delete(key);
  }

  getStats() {
    let totalWaiting = 0;
    for (const queue of this.waitingQueues.values()) totalWaiting += queue.length;
    return { lockedKeys: this.locks.size, waitingCount: totalWaiting };
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
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    this._queue.push({ targetId, message, timestamp: Date.now() });
    if (!this._isProcessing) this._process();
  }

  async _process() {
    if (this._isProcessing) return;
    this._isProcessing = true;

    while (this._queue.length > 0) {
      const batch = this._queue.splice(0, this.BATCH_SIZE);
      for (const item of batch) {
        try {
          if (this._flushCallback) await this._flushCallback(item.targetId, item.message);
        } catch (e) {}
      }
      if (this._queue.length > 0) await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
    }
    this._isProcessing = false;
  }

  async flushAll() {
    while (this._queue.length > 0) {
      await this._process();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return { queuedPM: this._queue.length, isProcessing: this._isProcessing };
  }

  async destroy() {
    await this.flushAll();
    this._queue = [];
    this._isProcessing = false;
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
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._totalQueued = 0;
    this._nextMsgId = 0;
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = 25;
    this._retryQueue = [];
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  _generateMsgId() { return `${Date.now()}_${++this._nextMsgId}_${Math.random().toString(36).substr(2, 4)}`; }

  add(room, message) {
    if (this._isDestroyed) { this._sendImmediate(room, message); return null; }

    const roomSize = this._roomQueueSizes.get(room) || 0;
    if (roomSize >= this.MAX_PER_ROOM || this._messageQueue.length >= this.maxQueueSize) {
      this._sendImmediate(room, message);
      return null;
    }

    const msgId = this._generateMsgId();
    this._messageQueue.push({ room, message, msgId, timestamp: Date.now() });
    this._totalQueued++;
    this._roomQueueSizes.set(room, roomSize + 1);
    return msgId;
  }

  tick(now) {
    if (this._isDestroyed) return;
    this._cleanupExpiredMessages(now);
    this._processRetryQueue(now);
    this._flush();
  }

  _cleanupExpiredMessages(now) {
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      if (now - this._messageQueue[i].timestamp > this.messageTTL + 1000) {
        const item = this._messageQueue[i];
        if (item) {
          const roomSize = this._roomQueueSizes.get(item.room) || 0;
          this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
        }
        this._messageQueue.splice(i, 1);
        this._totalQueued--;
      }
    }

    if (this._messageQueue.length > this.maxQueueSize * 0.8) {
      const toRemove = Math.floor(this._messageQueue.length * 0.3);
      for (let i = 0; i < toRemove; i++) {
        const item = this._messageQueue[i];
        if (item) {
          const roomSize = this._roomQueueSizes.get(item.room) || 0;
          this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
        }
      }
      this._messageQueue.splice(0, toRemove);
      this._totalQueued = this._messageQueue.length;
    }
  }

  _processRetryQueue(now) {
    const remaining = [];

    for (const item of this._retryQueue) {
      if (now < item.nextRetry) {
        remaining.push(item);
        continue;
      }
      if (item.retries >= 2) {
        continue;
      }
      const sent = this._sendWithCallback(item.room, item.message, item.msgId);
      if (!sent) {
        item.retries++;
        item.nextRetry = now + (1000 * Math.pow(2, item.retries));
        remaining.push(item);
      }
    }

    this._retryQueue = remaining;
  }

  _sendWithCallback(room, message, msgId) {
    if (!this._flushCallback) return false;
    try { this._flushCallback(room, message, msgId); return true; } catch (e) { return false; }
  }

  _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing) return;
    this._isFlushing = true;

    try {
      const batch = this._messageQueue.splice(0);
      this._totalQueued = 0;

      for (const item of batch) {
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
      }

      const roomGroups = new Map();
      for (const item of batch) {
        if (!roomGroups.has(item.room)) roomGroups.set(item.room, []);
        roomGroups.get(item.room).push(item);
      }

      for (const [room, items] of roomGroups) {
        for (const item of items) {
          try {
            this._flushCallback(room, item.message, item.msgId);
          } catch (e) {
            this._retryQueue.push({ room, message: item.message, msgId: item.msgId, retries: 0, nextRetry: Date.now() + 1000 });
          }
        }
      }
    } finally {
      this._isFlushing = false;
    }
  }

  _sendImmediate(room, message) {
    if (this._flushCallback) try { this._flushCallback(room, message, this._generateMsgId()); } catch (e) {}
  }

  async flushAll() {
    while (this._messageQueue.length > 0 || this._retryQueue.length > 0) {
      this._flush();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return {
      queuedMessages: this._messageQueue.length,
      retryQueue: this._retryQueue.length,
      totalQueued: this._totalQueued,
      maxQueueSize: this.maxQueueSize,
      roomQueues: Object.fromEntries(this._roomQueueSizes)
    };
  }

  async destroy() {
    this._isDestroyed = true;
    this._messageQueue = [];
    this._retryQueue = [];
    this._totalQueued = 0;
    this._roomQueueSizes.clear();
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
    this.lastActivity = Date.now();
  }

  updateActivity() { this.lastActivity = Date.now(); }

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
      noimageUrl: "", namauser: userId, color: "", itembawah: 0,
      itematas: 0, vip: 0, viptanda: 0, lastUpdated: Date.now()
    });
    this.updateActivity();
    return newSeatNumber;
  }

  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }

  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const existingSeat = this.seats.get(seatNumber);
    const entry = {
      noimageUrl: seatData.noimageUrl || "",
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
    this.updateActivity();
    return true;
  }

  removeSeat(seatNumber) {
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
    if (deleted) this.updateActivity();
    return deleted;
  }

  isSeatOccupied(seatNumber) { return this.seats.has(seatNumber); }
  getSeatOwner(seatNumber) { const seat = this.seats.get(seatNumber); return seat ? seat.namauser : null; }
  getOccupiedCount() { return this.seats.size; }

  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
      meta[seatNum] = {
        noimageUrl: seat.noimageUrl, namauser: seat.namauser, color: seat.color,
        itembawah: seat.itembawah, itematas: seat.itematas, vip: seat.vip, viptanda: seat.viptanda
      };
    }
    return meta;
  }

  updatePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    this.points.set(seatNumber, { x: point.x, y: point.y, fast: point.fast || false, timestamp: Date.now() });
    this.updateActivity();
    return true;
  }

  getPoint(seatNumber) { return this.points.get(seatNumber) || null; }

  getAllPoints() {
    const points = [];
    for (const [seatNum, point] of this.points) {
      points.push({ seat: seatNum, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return points;
  }

  setMute(isMuted) {
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    this.updateActivity();
    return this.muteStatus;
  }
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; this.updateActivity(); }
  getCurrentNumber() { return this.currentNumber; }

  removePoint(seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    return this.points.delete(seatNumber);
  }

  destroy() {
    this.seats.clear();
    this.points.clear();
  }
}

// ─────────────────────────────────────────────
// ChatServer2 (Durable Object) - FINAL STABLE
// ─────────────────────────────────────────────
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._isCleaningUp = false;

    this.seatLocker = new AsyncLock(2000);
    this.connectionLocker = new AsyncLock(1500);
    this.roomLocker = new AsyncLock(1500);

    this._activeClients = new Set();
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();

    this._wsCleaningUp = new Map();
    this.roomClients = new Map();
    this._activeListeners = new Map();

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const targetConnections = this.userConnections.get(targetId);
      if (targetConnections) {
        for (const client of targetConnections) {
          if (client && client.readyState === 1 && !client._isClosing && !this._wsCleaningUp.get(client)) {
            await this.safeSend(client, message);
            break;
          }
        }
      }
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

    this._masterTickCounter = 0;
    this._masterTimer = null;
    this._startMasterTimer();
  }

  _startMasterTimer() {
    if (this._masterTimer) clearInterval(this._masterTimer);
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }

  _masterTick() {
    if (this._isClosing) return;
    this._masterTickCounter++;
    const now = Date.now();

    try {
      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        this._handleNumberTick();
      }

      if (this.chatBuffer) this.chatBuffer.tick(now);

      if (this._masterTickCounter % CONSTANTS.ZOMBIE_CLEANUP_TICKS === 0) {
        this._cleanupZombieWebSocketsAndData();
      }

      if (this._masterTickCounter % CONSTANTS.FORCE_CLEANUP_MEMORY_TICKS === 0) {
        this._checkConnectionPressure();
      }

      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        this.lowcard.masterTick();
      }
    } catch (error) {}
  }

  async _checkConnectionPressure() {
    const total = this._activeClients.size;
    const max = CONSTANTS.MAX_GLOBAL_CONNECTIONS;

    if (total > max * CONSTANTS.CONNECTION_CRITICAL_THRESHOLD_RATIO) {
      await this._emergencyFullCleanup();
    } else if (total > max * CONSTANTS.CONNECTION_WARNING_THRESHOLD_RATIO) {
      if (this.chatBuffer) this.chatBuffer._flush();
    }
  }

  async _emergencyFullCleanup() {
    if (this.chatBuffer) await this.chatBuffer.flushAll();
    if (this.pmBuffer) await this.pmBuffer.flushAll();

    const snapshot = Array.from(this._activeClients);
    for (const ws of snapshot) {
      if (ws && ws.readyState !== 1 && !this._wsCleaningUp.get(ws)) {
        await this._forceFullCleanupWebSocket(ws);
      }
    }

    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      if (roomManager && roomManager.getOccupiedCount() === 0) {
        roomManager.destroy();
        this.roomManagers.set(room, new RoomManager(room));
      }
    }
  }

  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }

      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const clientsToNotify = [];
      const snapshot = Array.from(this._activeClients);
      
      for (const client of snapshot) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing && !this._wsCleaningUp.get(client)) {
          clientsToNotify.push(client);
        }
      }

      const batchSize = 30;
      for (let i = 0; i < clientsToNotify.length; i += batchSize) {
        const batch = clientsToNotify.slice(i, i + batchSize);
        for (const client of batch) {
          try {
            if (client.readyState === 1) client.send(message);
          } catch (e) {}
        }
        if (i + batchSize < clientsToNotify.length) {
          await new Promise(resolve => setTimeout(resolve, 1));
        }
      }
    } catch (error) {}
  }

  async _forceFullCleanupWebSocket(ws) {
    if (!ws || this._wsCleaningUp.get(ws)) return;
    
    this._wsCleaningUp.set(ws, true);

    const userId = ws.idtarget;
    const room = ws.roomname;

    try {
      ws._isClosing = true;

      if (room) {
        const clientSet = this.roomClients.get(room);
        if (clientSet) {
          clientSet.delete(ws);
        }
      }

      if (userId) {
        const userConnSet = this.userConnections.get(userId);
        if (userConnSet) {
          userConnSet.delete(ws);
          
          let hasOtherValidConnection = false;
          const snapshotConns = Array.from(userConnSet);
          for (const otherWs of snapshotConns) {
            if (otherWs && otherWs !== ws && otherWs.readyState === 1 && !otherWs._isClosing && !this._wsCleaningUp.get(otherWs)) {
              hasOtherValidConnection = true;
              break;
            }
          }
          
          if (userConnSet.size === 0 || !hasOtherValidConnection) {
            this.userConnections.delete(userId);
            
            if (userId && room) {
              const seatInfo = this.userToSeat.get(userId);
              if (seatInfo && seatInfo.room === room) {
                await this._removeUserSeatAndPointFromRoom(userId, room);
              }
              this.userToSeat.delete(userId);
              this.userCurrentRoom.delete(userId);
            }
          }
        }
      }

      this._cleanupWebSocketListeners(ws);
      this._activeClients.delete(ws);

      if (ws.readyState === 1) {
        try { 
          ws.close(1000, "Cleanup completed"); 
        } catch (e) {}
      }

    } catch (error) {}
    finally {
      this._wsCleaningUp.delete(ws);
    }
  }

  async _cleanupZombieWebSocketsAndData() {
    if (this._isCleaningUp) return;
    this._isCleaningUp = true;

    try {
      const zombies = [];
      const snapshot = Array.from(this._activeClients);
      
      for (const ws of snapshot) {
        const isDead = !ws || ws.readyState !== 1 || ws._isClosing === true;
        if (isDead && !this._wsCleaningUp.get(ws)) {
          zombies.push(ws);
        }
      }

      for (const ws of zombies) {
        await this._forceFullCleanupWebSocket(ws);
      }

      // Cleanup orphaned users
      const orphanedUsers = [];
      const snapshotConnections = Array.from(this.userConnections.entries());
      for (const [userId, connections] of snapshotConnections) {
        let hasLiveConnection = false;
        const snapshotConns = Array.from(connections);
        for (const conn of snapshotConns) {
          if (conn && conn.readyState === 1 && !conn._isClosing && !this._wsCleaningUp.get(conn)) {
            hasLiveConnection = true;
            break;
          }
        }
        if (!hasLiveConnection) orphanedUsers.push(userId);
      }

      for (const userId of orphanedUsers) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo) {
          const roomManager = this.roomManagers.get(seatInfo.room);
          if (roomManager) {
            const seatData = roomManager.getSeat(seatInfo.seat);
            if (seatData && seatData.namauser === userId) {
              roomManager.removeSeat(seatInfo.seat);
              roomManager.removePoint(seatInfo.seat);
              this.broadcastToRoom(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
              this.broadcastToRoom(seatInfo.room, ["pointRemoved", seatInfo.room, seatInfo.seat]);
              this.updateRoomCount(seatInfo.room);
            }
          }
        }
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this.userConnections.delete(userId);
      }

      // Cleanup room clients
      const snapshotRooms = Array.from(this.roomClients.entries());
      for (const [room, clientSet] of snapshotRooms) {
        const toDelete = [];
        const snapshotSet = Array.from(clientSet);
        for (const ws of snapshotSet) {
          if (!ws || ws.readyState !== 1 || ws.roomname !== room || this._wsCleaningUp.get(ws)) {
            toDelete.push(ws);
          }
        }
        for (const ws of toDelete) {
          clientSet.delete(ws);
        }
      }

    } catch (error) {}
    finally {
      this._isCleaningUp = false;
    }
  }

  async _removeUserSeatAndPointFromRoom(userId, room) {
    const seatInfo = this.userToSeat.get(userId);
    if (!seatInfo || seatInfo.room !== room) return false;

    const seatNumber = seatInfo.seat;
    const roomManager = this.roomManagers.get(room);

    if (roomManager) {
      const seatData = roomManager.getSeat(seatNumber);
      if (seatData && seatData.namauser === userId) {
        roomManager.removeSeat(seatNumber);
        roomManager.removePoint(seatNumber);
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.broadcastToRoom(room, ["pointRemoved", room, seatNumber]);
        this.updateRoomCount(room);
        return true;
      }
    }
    return false;
  }

  async _removeUserSeatAndPoint(userId) {
    const seatInfo = this.userToSeat.get(userId);
    if (!seatInfo) return false;

    const { room, seat: seatNumber } = seatInfo;
    const roomManager = this.roomManagers.get(room);

    if (roomManager) {
      const seatData = roomManager.getSeat(seatNumber);
      if (seatData && seatData.namauser === userId) {
        roomManager.removeSeat(seatNumber);
        roomManager.removePoint(seatNumber);
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.broadcastToRoom(room, ["pointRemoved", room, seatNumber]);
        this.updateRoomCount(room);
      }
    }

    this.userToSeat.delete(userId);
    this.userCurrentRoom.delete(userId);
    return true;
  }

  async _withSeatLock(room, seatNumber, operation) {
    let release = null;
    try {
      release = await this.seatLocker.acquire(`seat_${room}_${seatNumber}`);
      return await operation();
    } finally {
      if (release) {
        try { release(); } catch (e) {}
      }
    }
  }

  async assignNewSeat(room, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager || roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;

    const existingSeatInfo = this.userToSeat.get(userId);
    if (existingSeatInfo && existingSeatInfo.room === room) {
      const seatNum = existingSeatInfo.seat;
      if (roomManager.getSeatOwner(seatNum) === userId) return seatNum;
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
    }

    const newSeatNumber = roomManager.addNewSeat(userId);
    if (!newSeatNumber) return null;

    this.userToSeat.set(userId, { room, seat: newSeatNumber });
    this.userCurrentRoom.set(userId, room);
    this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
    this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    return newSeatNumber;
  }

  async updateSeatWithLock(room, seatNumber, seatData, userId) {
    return this._withSeatLock(room, seatNumber, async () => {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      const existingSeat = roomManager.getSeat(seatNumber);
      if (existingSeat && existingSeat.namauser !== userId) return false;

      const wasOccupied = roomManager.isSeatOccupied(seatNumber);
      const isOccupied = seatData.namauser && seatData.namauser !== "";
      const isNewSeat = !existingSeat;

      const success = roomManager.updateSeat(seatNumber, seatData);
      if (!success) return false;

      if (isNewSeat && isOccupied) {
        this.userToSeat.set(userId, { room, seat: seatNumber });
        this.userCurrentRoom.set(userId, room);
        this.broadcastToRoom(room, ["userOccupiedSeat", room, seatNumber, userId]);
      }

      if (wasOccupied !== isOccupied) {
        this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      }

      this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seatNumber, {
        noimageUrl: seatData.noimageUrl, namauser: seatData.namauser, color: seatData.color,
        itembawah: seatData.itembawah, itematas: seatData.itematas, vip: seatData.vip, viptanda: seatData.viptanda
      }]]]);
      return true;
    });
  }

  async safeRemoveSeat(room, seatNumber, userId) {
    return this._withSeatLock(room, seatNumber, async () => {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;
      const seatData = roomManager.getSeat(seatNumber);
      if (!seatData || seatData.namauser !== userId) return false;
      const success = roomManager.removeSeat(seatNumber);
      if (success) {
        roomManager.removePoint(seatNumber);
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.broadcastToRoom(room, ["pointRemoved", room, seatNumber]);
        this.updateRoomCount(room);
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      return success;
    });
  }

  async _addUserConnection(userId, ws) {
    let release = null;
    try {
      release = await this.connectionLocker.acquire(`conn_${userId}`);
      let userConnections = this.userConnections.get(userId);
      if (!userConnections) {
        userConnections = new Set();
        this.userConnections.set(userId, userConnections);
      }

      if (userConnections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
        const oldConnections = Array.from(userConnections);
        for (const oldWs of oldConnections) {
          if (oldWs !== ws) {
            if (oldWs.readyState === 1 && !oldWs._isClosing && !this._wsCleaningUp.get(oldWs)) {
              try {
                await this.safeSend(oldWs, ["connectionReplaced", "Reconnecting..."]);
                oldWs._toBeReplaced = true;
                setTimeout(() => {
                  if (oldWs && !this._wsCleaningUp.get(oldWs)) {
                    this._forceFullCleanupWebSocket(oldWs).catch(() => {});
                  }
                }, CONSTANTS.RECONNECT_GRACE_PERIOD_MS);
              } catch (e) {}
            }
            userConnections.delete(oldWs);
            this._activeClients.delete(oldWs);
          }
        }
      }

      userConnections.add(ws);
    } finally { 
      if (release) try { release(); } catch (e) {}
    }
  }

  async _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    let release = null;
    try {
      release = await this.connectionLocker.acquire(`conn_${userId}`);
      const userConnections = this.userConnections.get(userId);
      if (userConnections) {
        userConnections.delete(ws);
        if (userConnections.size === 0) this.userConnections.delete(userId);
      }
    } finally { 
      if (release) try { release(); } catch (e) {}
    }
  }

  _addToRoomClients(ws, room) {
    if (!ws || !room) return;
    let clientSet = this.roomClients.get(room);
    if (!clientSet) {
      clientSet = new Set();
      this.roomClients.set(room, clientSet);
    }
    clientSet.add(ws);
  }

  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    const clientSet = this.roomClients.get(room);
    if (clientSet) clientSet.delete(ws);
  }

  _cleanupWebSocketListeners(ws) {
    if (ws._abortController) {
      try { ws._abortController.abort(); } catch (e) {}
      ws._abortController = null;
    }
    const listeners = this._activeListeners.get(ws);
    if (listeners) {
      for (const { event, handler } of listeners) {
        try { ws.removeEventListener(event, handler); } catch (e) {}
      }
      this._activeListeners.delete(ws);
    }
  }

  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) counts[room] = this.roomManagers.get(room)?.getOccupiedCount() || 0;
    return counts;
  }

  getAllRoomCountsArray() { return roomList.map(room => [room, this.roomManagers.get(room)?.getOccupiedCount() || 0]); }
  getRoomCount(room) { return this.roomManagers.get(room)?.getOccupiedCount() || 0; }

  updateRoomCount(room) {
    const count = this.getRoomCount(room);
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }

  updatePointDirect(room, seatNumber, point, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    return roomManager.updatePoint(seatNumber, point);
  }

  _sendDirectToRoom(room, msg, msgId = null) {
    const clientSet = this.roomClients.get(room);
    if (!clientSet?.size) return 0;
    const messageStr = JSON.stringify(msg);
    let sentCount = 0;
    const snapshot = Array.from(clientSet);
    for (const client of snapshot) {
      if (!client || client.readyState !== 1 || client._isClosing || client.roomname !== room || this._wsCleaningUp.get(client)) {
        continue;
      }
      try {
        client.send(messageStr);
        sentCount++;
      } catch (e) {
        this._forceFullCleanupWebSocket(client).catch(() => {});
      }
    }
    return sentCount;
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    try {
      if (msg[0] === "gift") {
        return this._sendDirectToRoom(room, msg);
      }
      
      if (msg[0] === "chat") {
        if (this.chatBuffer) this.chatBuffer.add(room, msg);
        return this.roomClients.get(room)?.size || 0;
      }
      
      return this._sendDirectToRoom(room, msg);
    } catch (error) {
      return 0;
    }
  }

  async safeSend(ws, msg) {
    if (!ws) return false;
    if (ws._isClosing || ws.readyState !== 1 || this._wsCleaningUp.get(ws)) return false;
    try {
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      ws.send(message);
      return true;
    } catch (error) {
      if (!ws._isCleaningUp && (error.code === 'ECONNRESET' || error.message?.includes('ECONNRESET') || error.message?.includes('CLOSED'))) {
        ws._isCleaningUp = true;
        await this._forceFullCleanupWebSocket(ws);
      }
      return false;
    }
  }

  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || this._wsCleaningUp.get(ws)) return;
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
      if (Object.keys(filteredMeta).length > 0) await this.safeSend(ws, ["allUpdateKursiList", room, filteredMeta]);
      if (lastPointsData.length > 0) await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    } catch (error) {}
  }

  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) { await this.safeSend(ws, ["error", "User ID not set"]); return false; }
    if (!roomList.includes(room)) { await this.safeSend(ws, ["error", "Invalid room"]); return false; }
    return this._handleJoinRoomInternal(ws, room);
  }

  async _handleJoinRoomInternal(ws, room) {
    const release = await this.roomLocker.acquire(`joinroom_${room}_${ws.idtarget}`);
    try {
      return await this._doJoinRoom(ws, room);
    } finally {
      try { release(); } catch (e) {}
    }
  }

  async _doJoinRoom(ws, room) {
    try {
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      const currentRoomBeforeJoin = this.userCurrentRoom.get(ws.idtarget);

      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const roomManager = this.roomManagers.get(room);
        const seatData = roomManager.getSeat(seatNum);
        
        if (seatData && seatData.namauser === ws.idtarget) {
          ws.roomname = room;
          this._addToRoomClients(ws, room);
          await this._addUserConnection(ws.idtarget, ws);
          this.userCurrentRoom.set(ws.idtarget, room);
          
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          
          await new Promise(resolve => setTimeout(resolve, 50));
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          
          await new Promise(resolve => setTimeout(resolve, 100));
          
          if (!ws || ws.readyState !== 1 || ws._isClosing || this._wsCleaningUp.get(ws)) return true;
          
          await this.sendAllStateTo(ws, room);
          return true;
        } else {
          this.userToSeat.delete(ws.idtarget);
        }
      }

      if (currentRoomBeforeJoin && currentRoomBeforeJoin !== room) {
        const oldSeatInfo = this.userToSeat.get(ws.idtarget);
        if (oldSeatInfo && oldSeatInfo.room === currentRoomBeforeJoin) {
          let hasOtherConnection = false;
          const otherConnections = this.userConnections.get(ws.idtarget);
          if (otherConnections) {
            const snapshotConns = Array.from(otherConnections);
            for (const otherWs of snapshotConns) {
              if (otherWs !== ws && otherWs.roomname === currentRoomBeforeJoin && 
                  otherWs.readyState === 1 && !otherWs._isClosing) {
                hasOtherConnection = true;
                break;
              }
            }
          }
          
          if (!hasOtherConnection) {
            await this.safeRemoveSeat(currentRoomBeforeJoin, oldSeatInfo.seat, ws.idtarget);
            this.broadcastToRoom(currentRoomBeforeJoin, ["removeKursi", currentRoomBeforeJoin, oldSeatInfo.seat]);
            this.broadcastToRoom(currentRoomBeforeJoin, ["pointRemoved", currentRoomBeforeJoin, oldSeatInfo.seat]);
          }
        }
        this._removeFromRoomClients(ws, currentRoomBeforeJoin);
      }

      if (this.getRoomCount(room) >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }

      let assignedSeat = null;
      const existingSeat = this.userToSeat.get(ws.idtarget);
      if (existingSeat && existingSeat.room === room) {
        assignedSeat = existingSeat.seat;
        const roomManager = this.roomManagers.get(room);
        const seatData = roomManager.getSeat(assignedSeat);
        if (!seatData || seatData.namauser !== ws.idtarget) {
          assignedSeat = null;
        }
      }
      
      if (!assignedSeat) {
        assignedSeat = await this.assignNewSeat(room, ws.idtarget);
      }
      
      if (!assignedSeat) { 
        await this.safeSend(ws, ["roomFull", room]); 
        return false; 
      }

      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      this._addToRoomClients(ws, room);
      await this._addUserConnection(ws.idtarget, ws);

      const roomManager = this.roomManagers.get(room);
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);

      await new Promise(resolve => setTimeout(resolve, 100));
      
      if (!ws || ws.readyState !== 1 || ws._isClosing || this._wsCleaningUp.get(ws)) return true;

      await this.sendAllStateTo(ws, room);
      return true;
    } catch (error) {
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    try {
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo && seatInfo.room === room) {
        await this.safeRemoveSeat(room, seatInfo.seat, ws.idtarget);
        this.broadcastToRoom(room, ["removeKursi", room, seatInfo.seat]);
        this.broadcastToRoom(room, ["pointRemoved", room, seatInfo.seat]);
      }
      this._removeFromRoomClients(ws, room);
      await this._removeUserConnection(ws.idtarget, ws);
      ws.roomname = undefined;
      this.updateRoomCount(room);
    } catch (error) {}
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    try {
      const existingConnections = this.userConnections.get(id);
      if (existingConnections && existingConnections.size > 0) {
        const oldConnections = Array.from(existingConnections);
        for (const oldWs of oldConnections) {
          if (oldWs !== ws) {
            const isDead = oldWs.readyState !== 1 || oldWs._isClosing === true;
            if (isDead) {
              await this._forceFullCleanupWebSocket(oldWs);
            } else if (!baru) {
              try { 
                await this.safeSend(oldWs, ["connectionReplaced", "New connection detected"]);
                oldWs._toBeReplaced = true;
                setTimeout(() => {
                  if (oldWs && !this._wsCleaningUp.get(oldWs)) {
                    this._forceFullCleanupWebSocket(oldWs).catch(() => {});
                  }
                }, CONSTANTS.RECONNECT_GRACE_PERIOD_MS);
              } catch (e) {}
            }
          }
        }
      }

      if (baru === true) {
        ws.idtarget = id;
        ws.roomname = undefined;
        ws._isClosing = false;
        ws._connectionTime = Date.now();
        await this._addUserConnection(id, ws);
        this._activeClients.add(ws);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }

      ws.idtarget = id;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      this._activeClients.add(ws);

      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const roomManager = this.roomManagers.get(room);
          if (roomManager) {
            const seatData = roomManager.getSeat(seat);
            if (seatData && seatData.namauser === id) {
              ws.roomname = room;
              this._addToRoomClients(ws, room);
              await this._addUserConnection(id, ws);
              await this.sendAllStateTo(ws, room);
              const point = roomManager.getPoint(seat);
              if (point) await this.safeSend(ws, ["pointUpdated", room, seat, point.x, point.y, point.fast ? 1 : 0]);
              await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
              await this.safeSend(ws, ["numberKursiSaya", seat]);
              await this.safeSend(ws, ["currentNumber", this.currentNumber]);
              return;
            }
          }
        }
        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
      }
      await this._addUserConnection(id, ws);
      await this.safeSend(ws, ["needJoinRoom"]);
    } catch (error) {
      await this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing || this._wsCleaningUp.get(ws)) return;
    
    // Handle binary messages (jaga-jaga)
    if (raw instanceof ArrayBuffer) {
      // Cloudflare Workers otomatis handle PING/PONG, ini hanya untuk jaga-jaga
      return;
    }
    
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
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userCurrentRoom.get(ws.idtarget) !== undefined]);
          break;
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
        case "joinRoom": {
          const success = await this.handleJoinRoom(ws, data[1]);
          if (success && ws.roomname) this.updateRoomCount(ws.roomname);
          break;
        }
        case "leaveRoom": {
          const room = ws.roomname;
          if (room && roomList.includes(room)) {
            await this.cleanupFromRoom(ws, room);
            await this.safeSend(ws, ["roomLeft", room]);
          }
          break;
        }
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (ws.roomname !== roomname || ws.idtarget !== username) return;
          if (!roomList.includes(roomname)) return;
          const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          if (sanitizedMessage.includes('\0')) return;
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
          break;
        }
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || !roomList.includes(room) || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (this.updatePointDirect(room, seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 || fast === true }, ws.idtarget)) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (await this.safeRemoveSeat(room, seat, ws.idtarget)) {
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.broadcastToRoom(room, ["pointRemoved", room, seat]);
            this.updateRoomCount(room);
          }
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
          const updatedSeat = {
            noimageUrl: noimageUrl?.slice(0, 255) || "",
            namauser: namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0,
            lastUpdated: Date.now()
          };
          const success = await this.updateSeatWithLock(room, seat, updatedSeat, ws.idtarget);
          if (!success) await this.safeSend(ws, ["error", "Failed to update seat"]);
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
        case "getAllRoomsUserCount":
          await this.safeSend(ws, ["allRoomsUserCount", this.getAllRoomCountsArray()]);
          break;
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
            const snapshotConns = Array.from(connections);
            for (const conn of snapshotConns) {
              if (conn && conn.readyState === 1 && !conn._isClosing && !this._wsCleaningUp.get(conn)) { isOnline = true; break; }
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
          const snapshotUsers = Array.from(this.userConnections.entries());
          for (const [userId, connections] of snapshotUsers) {
            const snapshotConns = Array.from(connections);
            for (const conn of snapshotConns) {
              if (conn && conn.readyState === 1 && !conn._isClosing && !this._wsCleaningUp.get(conn)) {
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
            const snapshotConns = Array.from(targetConnections);
            for (const client of snapshotConns) {
              if (client && client.readyState === 1 && !client._isClosing && !this._wsCleaningUp.get(client)) {
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
          }
          break;
        case "onDestroy":
          await this._forceFullCleanupWebSocket(ws);
          break;
        default:
          break;
      }
    } catch (error) {}
  }

  setRoomMute(roomName, isMuted) {
    const roomManager = this.roomManagers.get(roomName);
    if (!roomManager) return false;
    const muteValue = roomManager.setMute(isMuted);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }

  async getMemoryStats() {
    try {
      const snapshot = Array.from(this._activeClients);
      let activeReal = 0;
      for (const c of snapshot) {
        if (c?.readyState === 1 && !this._wsCleaningUp.get(c)) activeReal++;
      }

      let totalRoomClients = 0;
      for (const clientSet of this.roomClients.values()) totalRoomClients += clientSet.size;

      let totalSeats = 0, totalPoints = 0;
      for (const rm of this.roomManagers.values()) {
        totalSeats += rm.seats.size;
        totalPoints += rm.points.size;
      }

      return {
        timestamp: Date.now(),
        uptime: Date.now() - this._startTime,
        memory: {
          note: "process.memoryUsage() not available in Cloudflare Workers",
          activeConnections: activeReal,
          connectionPressure: `${Math.round((activeReal / CONSTANTS.MAX_GLOBAL_CONNECTIONS) * 100)}%`
        },
        activeClients: { total: this._activeClients.size, real: activeReal },
        roomClients: { total: totalRoomClients },
        userConnections: this.userConnections.size,
        userToSeatSize: this.userToSeat.size,
        chatBuffer: this.chatBuffer ? this.chatBuffer.getStats() : {},
        pmBuffer: this.pmBuffer ? this.pmBuffer.getStats() : {},
        seats: totalSeats,
        points: totalPoints
      };
    } catch (error) {
      return { error: "Failed to get stats" };
    }
  }

  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    if (this._masterTimer) { clearInterval(this._masterTimer); this._masterTimer = null; }
    if (this.chatBuffer) {
      await this.chatBuffer.flushAll();
      await this.chatBuffer.destroy();
    }
    if (this.pmBuffer) await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') try { await this.lowcard.destroy(); } catch (e) {}
    this.lowcard = null;

    const snapshot = Array.from(this._activeClients);
    for (const ws of snapshot) {
      if (ws && ws.readyState === 1 && !ws._isClosing && !this._wsCleaningUp.get(ws)) {
        try { this._cleanupWebSocketListeners(ws); ws.close(1000, "Server shutdown"); } catch (e) {}
      }
    }

    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._activeListeners.clear();
    this._wsCleaningUp.clear();
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          const snapshot = Array.from(this._activeClients);
          for (const c of snapshot) if (c && c.readyState === 1 && !this._wsCleaningUp.get(c)) activeCount++;
          return new Response(JSON.stringify({
            status: "healthy",
            connections: activeCount,
            connectionPressure: `${Math.round((activeCount / CONSTANTS.MAX_GLOBAL_CONNECTIONS) * 100)}%`,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            chatBuffer: this.chatBuffer ? this.chatBuffer.getStats() : {},
            pmBuffer: this.pmBuffer ? this.pmBuffer.getStats() : {},
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/debug/memory") {
          return new Response(JSON.stringify(await this.getMemoryStats(), null, 2), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/debug/roomcounts") {
          const counts = {};
          for (const room of roomList) counts[room] = this.getRoomCount(room);
          return new Response(JSON.stringify({ counts, total: Object.values(counts).reduce((a, b) => a + b, 0) }), { headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/shutdown") { await this.shutdown(); return new Response("Shutting down...", { status: 200 }); }
        return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200 });
      }

      if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }

      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      const abortController = new AbortController();

      const acceptPromise = server.accept();
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error("WebSocket accept timeout")), CONSTANTS.WS_ACCEPT_TIMEOUT_MS);
      });

      try {
        await Promise.race([acceptPromise, timeoutPromise]);
      } catch (acceptError) {
        abortController.abort();
        try { server.close(); } catch (e) {}
        return new Response("WebSocket accept timeout", { status: 500 });
      }

      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._isCleaningUp = false;
      ws._connectionTime = Date.now();
      ws._abortController = abortController;

      this._activeClients.add(ws);

      const messageHandler = async (ev) => {
        await this.handleMessage(ws, ev.data);
      };
      
      const errorHandler = () => { 
        this._forceFullCleanupWebSocket(ws).catch(() => {}); 
      };
      const closeHandler = () => { 
        this._forceFullCleanupWebSocket(ws).catch(() => {}); 
      };

      ws.addEventListener("message", messageHandler, { signal: abortController.signal });
      ws.addEventListener("error", errorHandler, { signal: abortController.signal });
      ws.addEventListener("close", closeHandler, { signal: abortController.signal });

      this._activeListeners.set(ws, [
        { event: "message", handler: messageHandler },
        { event: "error", handler: errorHandler },
        { event: "close", handler: closeHandler }
      ]);

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// ─────────────────────────────────────────────
// Worker Export
// ─────────────────────────────────────────────
export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER_2.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER_2.get(chatId);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") return chatObj.fetch(req);
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/shutdown"].includes(url.pathname)) return chatObj.fetch(req);
      return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200, headers: { "content-type": "text/plain" } });
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
};
