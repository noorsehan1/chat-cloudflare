// ==================== CHAT SERVER 2 - PRODUCTION READY (128MB OPTIMIZED) ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-03"
// optimized_for = "128MB Memory"

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
  MAX_GLOBAL_CONNECTIONS: 200,
  MAX_ACTIVE_CLIENTS_LIMIT: 200,
  MAX_SEATS: 25,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 4000,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 25,
  MAX_TOTAL_BUFFER_MESSAGES: 30,
  MESSAGE_TTL_MS: 8000,
  MAX_CONNECTIONS_PER_USER: 1,
  ROOM_IDLE_BEFORE_CLEANUP: 15 * 60 * 1000,
  PM_BATCH_SIZE: 3,
  PM_BATCH_DELAY_MS: 30,
  WS_ACCEPT_TIMEOUT_MS: 10000,
  FORCE_CLEANUP_TIMEOUT_MS: 2000,
  CONNECTION_CRITICAL_THRESHOLD_RATIO: 0.90,
  CONNECTION_WARNING_THRESHOLD_RATIO: 0.80,
  FORCE_CLEANUP_MEMORY_TICKS: 45,
  RECONNECT_GRACE_PERIOD_MS: 10000,
  SEAT_RELEASE_DELAY_MS: 1000,
  MAX_RETRY_QUEUE_SIZE: 50,
  MAX_RECONNECT_STALE_MS: 60000,
  MAX_FLUSH_ITERATIONS: 500,
  MAX_MESSAGES_PER_MINUTE: 45,
  MESSAGE_RATE_WINDOW_MS: 60000,
  CLEANUP_LOCK_TIMEOUT_MS: 500,
  WS_CLEANING_UP_MAX_AGE_MS: 5000,
  MAX_RETRY_ATTEMPTS: 3,
  STALE_MESSAGE_CLEANUP_MS: 120000,
  FLUSH_BATCH_SIZE: 50,
  CLEANUP_SWEEP_INTERVAL_MS: 30000,
  EMERGENCY_SWEEP_INTERVAL_TICKS: 10,
  MAX_ROOM_QUEUE_SIZE: 15,
  MEMORY_PRESSURE_THRESHOLD: 0.85,
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
// AsyncLock (Memory Optimized)
// ─────────────────────────────────────────────
class AsyncLock {
  constructor(timeoutMs = 2000) {
    this.locks = new Map();
    this.waitingQueues = new Map();
    this.timeoutMs = timeoutMs;
    this._stats = { totalWaits: 0, timeouts: 0 };
    this._cleanupInterval = setInterval(() => this._cleanupStaleQueues(), 60000);
    this._isDestroyed = false;
  }

  _cleanupStaleQueues() {
    if (this._isDestroyed) return;
    for (const [key, queue] of this.waitingQueues.entries()) {
      if (queue.length === 0) {
        this.waitingQueues.delete(key);
      }
    }
  }

  async acquire(key) {
    if (this._isDestroyed) throw new Error("Lock is destroyed");
    
    if (!this.locks.has(key)) {
      this.locks.set(key, true);
      return () => this._release(key);
    }

    if (!this.waitingQueues.has(key)) {
      this.waitingQueues.set(key, []);
    }

    this._stats.totalWaits++;
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const queue = this.waitingQueues.get(key);
        if (queue) {
          const index = queue.findIndex(item => item.resolve === resolve);
          if (index > -1) {
            queue.splice(index, 1);
            if (queue.length === 0) this.waitingQueues.delete(key);
          }
        }
        this._stats.timeouts++;
        reject(new Error(`Lock timeout: ${key}`));
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
    if (this._isDestroyed) return;
    this.locks.delete(key);
    const queue = this.waitingQueues.get(key);
    if (queue && queue.length > 0) {
      const next = queue.shift();
      if (next) next.resolve();
    }
    if (!queue || queue.length === 0) this.waitingQueues.delete(key);
  }

  getStats() {
    let totalWaiting = 0;
    for (const queue of this.waitingQueues.values()) totalWaiting += queue.length;
    return { 
      lockedKeys: this.locks.size, 
      waitingCount: totalWaiting,
      totalWaits: this._stats.totalWaits,
      timeouts: this._stats.timeouts
    };
  }

  destroy() {
    this._isDestroyed = true;
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    this.locks.clear();
    this.waitingQueues.clear();
  }
}

// ─────────────────────────────────────────────
// PMBuffer (Memory Optimized)
// ─────────────────────────────────────────────
class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
    this._isDestroyed = false;
    this._stats = { totalProcessed: 0, totalDropped: 0 };
    this.MAX_QUEUE_SIZE = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES * 2;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    if (this._isDestroyed) return;
    if (this._queue.length > this.MAX_QUEUE_SIZE) {
      const dropped = this._queue.shift();
      this._stats.totalDropped++;
    }
    this._queue.push({ targetId, message, timestamp: Date.now() });
    if (!this._isProcessing) {
      this._process().catch(() => {});
    }
  }

  async _process() {
    if (this._isProcessing || this._isDestroyed) return;
    this._isProcessing = true;

    try {
      while (this._queue.length > 0 && !this._isDestroyed) {
        const batch = this._queue.splice(0, this.BATCH_SIZE);
        const promises = [];
        
        for (const item of batch) {
          if (this._flushCallback) {
            promises.push(
              this._flushCallback(item.targetId, item.message).catch(() => {})
            );
          }
        }
        
        if (promises.length > 0) {
          await Promise.allSettled(promises);
          this._stats.totalProcessed += promises.length;
        }
        
        if (this._queue.length > 0 && !this._isDestroyed) {
          await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
        }
      }
    } finally {
      this._isProcessing = false;
    }
  }

  async flushAll() {
    let maxIterations = CONSTANTS.MAX_FLUSH_ITERATIONS;
    while (this._queue.length > 0 && maxIterations-- > 0 && !this._isDestroyed) {
      await this._process();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return { 
      queuedPM: this._queue.length, 
      isProcessing: this._isProcessing,
      totalProcessed: this._stats.totalProcessed,
      totalDropped: this._stats.totalDropped
    };
  }

  async destroy() {
    await this.flushAll();
    this._isDestroyed = true;
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
  }
}

// ─────────────────────────────────────────────
// GlobalChatBuffer (Memory Optimized)
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
    this.MAX_PER_ROOM = CONSTANTS.MAX_ROOM_QUEUE_SIZE;
    this._retryQueue = [];
    this._stats = { 
      totalMessages: 0, 
      totalDropped: 0, 
      totalRetried: 0,
      droppedRetries: 0 
    };
    this._flushScheduled = false;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  _generateMsgId() { 
    const random = Math.random().toString(36).slice(2, 10).padEnd(8, '0');
    return `${Date.now()}_${++this._nextMsgId}_${random}`; 
  }

  _decrementRoomSize(room) {
    const current = this._roomQueueSizes.get(room) || 0;
    if (current <= 1) {
      this._roomQueueSizes.delete(room);
    } else {
      this._roomQueueSizes.set(room, current - 1);
    }
  }

  add(room, message) {
    if (this._isDestroyed) { 
      this._sendImmediate(room, message); 
      return null; 
    }

    const roomSize = this._roomQueueSizes.get(room) || 0;
    if (roomSize >= this.MAX_PER_ROOM || this._messageQueue.length >= this.maxQueueSize) {
      this._sendImmediate(room, message);
      this._stats.totalDropped++;
      return null;
    }

    const msgId = this._generateMsgId();
    this._messageQueue.push({ room, message, msgId, timestamp: Date.now() });
    this._totalQueued++;
    this._roomQueueSizes.set(room, roomSize + 1);
    this._stats.totalMessages++;
    
    this._scheduleFlush();
    return msgId;
  }

  _scheduleFlush() {
    if (this._flushScheduled || this._isDestroyed) return;
    this._flushScheduled = true;
    queueMicrotask(() => {
      this._flushScheduled = false;
      this._flush().catch(() => {});
    });
  }

  tick(now) {
    if (this._isDestroyed) return;
    this._cleanupExpiredMessages(now);
    this._processRetryQueue(now);
  }

  _cleanupExpiredMessages(now) {
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      if (now - this._messageQueue[i].timestamp > this.messageTTL + 1000) {
        const item = this._messageQueue[i];
        if (item) this._decrementRoomSize(item.room);
        this._messageQueue.splice(i, 1);
        this._totalQueued--;
        this._stats.totalDropped++;
      }
    }

    if (this._messageQueue.length > this.maxQueueSize * 0.8) {
      const toRemove = Math.floor(this._messageQueue.length * 0.3);
      for (let i = 0; i < toRemove; i++) {
        const item = this._messageQueue[i];
        if (item) this._decrementRoomSize(item.room);
      }
      this._messageQueue.splice(0, toRemove);
      this._totalQueued = this._messageQueue.length;
    }
  }

  _processRetryQueue(now) {
    if (this._retryQueue.length > CONSTANTS.MAX_RETRY_QUEUE_SIZE * 2) {
      const dropped = this._retryQueue.length - CONSTANTS.MAX_RETRY_QUEUE_SIZE;
      this._retryQueue = this._retryQueue.slice(0, CONSTANTS.MAX_RETRY_QUEUE_SIZE);
      this._stats.droppedRetries += dropped;
    }

    const remaining = [];
    let droppedCount = 0;

    for (const item of this._retryQueue) {
      if (now < item.nextRetry) {
        remaining.push(item);
        continue;
      }
      if (item.retries >= CONSTANTS.MAX_RETRY_ATTEMPTS) {
        droppedCount++;
        this._stats.totalDropped++;
        continue;
      }
      const sent = this._sendWithCallback(item.room, item.message, item.msgId);
      if (!sent) {
        item.retries++;
        item.nextRetry = now + (1000 * Math.pow(2, item.retries));
        remaining.push(item);
        this._stats.totalRetried++;
      }
    }

    this._retryQueue = remaining;
    if (droppedCount > 0) {
      this._stats.droppedRetries += droppedCount;
    }
  }

  _sendWithCallback(room, message, msgId) {
    if (!this._flushCallback) return false;
    try { 
      this._flushCallback(room, message, msgId); 
      return true; 
    } catch (e) { 
      return false; 
    }
  }

  async _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing || this._isDestroyed) return;
    this._isFlushing = true;

    try {
      const batchSize = Math.min(this._messageQueue.length, CONSTANTS.FLUSH_BATCH_SIZE);
      const batch = this._messageQueue.splice(0, batchSize);
      this._totalQueued = this._messageQueue.length;

      for (const item of batch) {
        this._decrementRoomSize(item.room);
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
            this._retryQueue.push({ 
              room, 
              message: item.message, 
              msgId: item.msgId, 
              retries: 0, 
              nextRetry: Date.now() + 1000 
            });
          }
        }
      }
      
      if (this._messageQueue.length > 0 && !this._isDestroyed) {
        this._scheduleFlush();
      }
    } finally {
      this._isFlushing = false;
    }
  }

  _sendImmediate(room, message) {
    if (this._flushCallback) {
      try { 
        const safeMessage = this._safeStringify(message);
        if (safeMessage) {
          this._flushCallback(room, safeMessage, this._generateMsgId()); 
        }
      } catch (e) {}
    }
  }
  
  _safeStringify(msg) {
    try {
      return JSON.stringify(msg);
    } catch (e) {
      return null;
    }
  }

  async flushAll() {
    let maxIterations = CONSTANTS.MAX_FLUSH_ITERATIONS;
    while ((this._messageQueue.length > 0 || this._retryQueue.length > 0) && maxIterations-- > 0 && !this._isDestroyed) {
      await this._flush();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return {
      queuedMessages: this._messageQueue.length,
      retryQueue: this._retryQueue.length,
      totalQueued: this._totalQueued,
      maxQueueSize: this.maxQueueSize,
      roomQueues: Object.fromEntries(this._roomQueueSizes),
      stats: { ...this._stats }
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
// RoomManager (Memory Optimized)
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
      const seatData = this.seats.get(seat);
      if (!seatData || !seatData.namauser || seatData.namauser === "") {
        return seat;
      }
    }
    return null;
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
    this.updateActivity();
    return true;
  }

  removeSeat(seatNumber) {
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
    if (deleted) this.updateActivity();
    return deleted;
  }

  isSeatOccupied(seatNumber) { 
    const seat = this.seats.get(seatNumber);
    return seat && seat.namauser && seat.namauser !== "";
  }
  
  getSeatOwner(seatNumber) { const seat = this.seats.get(seatNumber); return seat ? seat.namauser : null; }
  
  getOccupiedCount() { 
    let count = 0;
    for (const seat of this.seats.values()) {
      if (seat.namauser && seat.namauser !== "") count++;
    }
    return count;
  }

  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
      if (seat.namauser && seat.namauser !== "") {
        meta[seatNum] = {
          noimageUrl: seat.noimageUrl, namauser: seat.namauser, color: seat.color,
          itembawah: seat.itembawah, itematas: seat.itematas, vip: seat.vip, viptanda: seat.viptanda
        };
      }
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
// ChatServer2 - 128MB OPTIMIZED
// ─────────────────────────────────────────────
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._isCleaningUp = false;

    this.seatLocker = new AsyncLock(2000);
    this.connectionLocker = new AsyncLock(1500);
    this.roomLocker = new AsyncLock(1500);
    this.cleanupLocker = new AsyncLock(CONSTANTS.CLEANUP_LOCK_TIMEOUT_MS);

    this._activeClients = new Set();
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();

    this._wsCleaningUp = new Map();
    this.roomClients = new Map();
    this._activeListeners = new Map();

    this._reconnectingUsers = new Map();
    this._reconnectTimers = new Map();

    this._userMessageCount = new Map();

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const targetConnections = this.userConnections.get(targetId);
      if (targetConnections) {
        const snapshotConns = Array.from(targetConnections);
        for (const client of snapshotConns) {
          if (client && client.readyState === 1 && !client._isClosing && !this._wsCleaningUp.has(client._cleanupId)) {
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
    this._cleanupSweepTimer = null;
    this._startMasterTimer();
    this._startCleanupSweeper();
    
    // Memory pressure monitoring
    this._lastMemoryCheck = Date.now();
  }

  _startCleanupSweeper() {
    if (this._cleanupSweepTimer) clearInterval(this._cleanupSweepTimer);
    this._cleanupSweepTimer = setInterval(() => {
      if (this._isClosing) return;
      const now = Date.now();
      this._sweepStaleCleanupEntries(now);
      this._emergencySweep();
    }, CONSTANTS.CLEANUP_SWEEP_INTERVAL_MS);
  }

  _startMasterTimer() {
    if (this._masterTimer) clearInterval(this._masterTimer);
    this._masterTimer = setInterval(() => {
      if (this._isClosing) return;
      try {
        this._masterTick();
      } catch (error) {
        console.error("[CRITICAL] MasterTick error:", error);
      }
    }, CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }

  _masterTick() {
    if (this._isClosing) return;
    this._masterTickCounter++;
    const now = Date.now();

    try {
      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        this._handleNumberTick().catch(() => {});
      }

      if (this.chatBuffer) this.chatBuffer.tick(now);

      if (this._masterTickCounter % CONSTANTS.FORCE_CLEANUP_MEMORY_TICKS === 0) {
        this._checkConnectionPressure().catch(() => {});
        this._sweepMessageCounts();
        this._checkMemoryPressure();
      }
      
      if (this._masterTickCounter % CONSTANTS.EMERGENCY_SWEEP_INTERVAL_TICKS === 0) {
        this._emergencySweep();
      }

      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try {
          const result = this.lowcard.masterTick();
          if (result && typeof result.catch === 'function') {
            result.catch((err) => {
              console.error("[LowCard] masterTick promise error:", err);
            });
          }
        } catch (syncError) {
          console.error("[LowCard] masterTick sync error:", syncError);
        }
      }
    } catch (error) {
      console.error("[MasterTick] Unhandled error:", error);
    }
  }

  _checkMemoryPressure() {
    // Memory pressure detection for 128MB limit
    if (global.gc) {
      try { global.gc(); } catch (e) {}
    }
    
    const stats = {
      activeClients: this._activeClients.size,
      wsCleaningUp: this._wsCleaningUp.size,
      userConnections: this.userConnections.size,
      roomClientsTotal: Array.from(this.roomClients.values()).reduce((a, b) => a + b.size, 0)
    };
    
    // Force cleanup if too many objects
    if (stats.activeClients > 180 || stats.wsCleaningUp > 500 || stats.userConnections > 300) {
      this._emergencyFullCleanup().catch(() => {});
    }
  }

  _emergencySweep() {
    // Clean up stale wsCleaningUp entries
    if (this._wsCleaningUp.size > 500) {
      this._wsCleaningUp.clear();
    }
    
    // Clean zombie connections
    const toDeleteUser = [];
    for (const [userId, connections] of this.userConnections.entries()) {
      const toDeleteConns = [];
      for (const ws of connections) {
        if (!ws || ws.readyState !== 1) {
          toDeleteConns.push(ws);
        }
      }
      for (const ws of toDeleteConns) {
        connections.delete(ws);
      }
      if (connections.size === 0) {
        toDeleteUser.push(userId);
      }
    }
    for (const userId of toDeleteUser) {
      this.userConnections.delete(userId);
    }
    
    // Clean roomClients zombies
    const toDeleteRoom = [];
    for (const [room, clients] of this.roomClients.entries()) {
      const toDeleteClients = [];
      for (const client of clients) {
        if (!client || client.readyState !== 1) {
          toDeleteClients.push(client);
        }
      }
      for (const client of toDeleteClients) {
        clients.delete(client);
      }
      if (clients.size === 0) {
        toDeleteRoom.push(room);
      }
    }
    for (const room of toDeleteRoom) {
      this.roomClients.delete(room);
    }
    
    // Clean activeClients zombies
    const toDeleteActive = [];
    for (const client of this._activeClients) {
      if (!client || client.readyState !== 1) {
        toDeleteActive.push(client);
      }
    }
    for (const client of toDeleteActive) {
      this._activeClients.delete(client);
    }
    
    // Limit reconnect timers
    if (this._reconnectTimers.size > 100) {
      const entries = Array.from(this._reconnectTimers.entries());
      const toDelete = entries.slice(100);
      for (const [id, timer] of toDelete) {
        clearTimeout(timer);
        this._reconnectTimers.delete(id);
        this._reconnectingUsers.delete(id);
      }
    }
  }

  _sweepStaleCleanupEntries(now) {
    for (const [key, timestamp] of this._wsCleaningUp.entries()) {
      if (now - timestamp > CONSTANTS.WS_CLEANING_UP_MAX_AGE_MS) {
        this._wsCleaningUp.delete(key);
      }
    }
  }

  _sweepMessageCounts() {
    const now = Date.now();
    for (const [userId, userData] of this._userMessageCount.entries()) {
      if (!this.userConnections.has(userId)) {
        this._userMessageCount.delete(userId);
      } else if (now - userData.windowStart > CONSTANTS.MESSAGE_RATE_WINDOW_MS * 2) {
        this._userMessageCount.delete(userId);
      }
    }
    
    // Limit size
    if (this._userMessageCount.size > 500) {
      const entries = Array.from(this._userMessageCount.entries());
      const toDelete = entries.slice(400);
      for (const [userId] of toDelete) {
        this._userMessageCount.delete(userId);
      }
    }
  }

  async _checkConnectionPressure() {
    const total = this._activeClients.size;
    const max = CONSTANTS.MAX_GLOBAL_CONNECTIONS;

    if (total > max * CONSTANTS.CONNECTION_CRITICAL_THRESHOLD_RATIO) {
      await this._emergencyFullCleanup();
    } else if (total > max * CONSTANTS.CONNECTION_WARNING_THRESHOLD_RATIO) {
      if (this.chatBuffer) await this.chatBuffer._flush();
    }
  }

  async _emergencyFullCleanup() {
    if (this.chatBuffer) await this.chatBuffer.flushAll();
    if (this.pmBuffer) await this.pmBuffer.flushAll();

    const snapshot = Array.from(this._activeClients);
    const BATCH_SIZE = 30;
    
    for (let i = 0; i < snapshot.length; i += BATCH_SIZE) {
      const batch = snapshot.slice(i, i + BATCH_SIZE);
      const cleanupPromises = [];
      
      for (const ws of batch) {
        if (ws && ws.readyState !== 1 && !this._wsCleaningUp.has(ws._cleanupId)) {
          cleanupPromises.push(this._forceFullCleanupWebSocket(ws).catch(() => {}));
        }
      }
      
      if (cleanupPromises.length > 0) {
        await Promise.allSettled(cleanupPromises);
      }
      
      if (i + BATCH_SIZE < snapshot.length) {
        await new Promise(resolve => setTimeout(resolve, 5));
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

      let messageStr;
      try {
        messageStr = JSON.stringify(["currentNumber", this.currentNumber]);
      } catch (e) {
        return;
      }
      
      const clientsToNotify = [];
      const snapshot = Array.from(this._activeClients);

      for (const client of snapshot) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing && !this._wsCleaningUp.has(client._cleanupId)) {
          clientsToNotify.push(client);
        }
      }

      let iterCount = 0;
      for (const client of clientsToNotify) {
        await this.safeSend(client, messageStr).catch(() => {});
        iterCount++;
        if (iterCount % 30 === 0) {
          await new Promise(resolve => setTimeout(resolve, 0));
        }
      }

    } catch (error) {
      console.error("[NumberTick] Error:", error);
    }
  }

  async _forceFullCleanupWebSocket(ws) {
    if (!ws) return;

    if (!ws._cleanupId) {
      ws._cleanupId = `ws_${Date.now()}_${Math.random().toString(36).substr(2, 8)}`;
    }

    if (this._wsCleaningUp.has(ws._cleanupId)) return;

    let release = null;
    let didSetCleaningFlag = false;

    try {
      release = await this.cleanupLocker.acquire(`cleanup_${ws._cleanupId}`);
      
      if (this._wsCleaningUp.has(ws._cleanupId)) return;

      this._wsCleaningUp.set(ws._cleanupId, Date.now());
      didSetCleaningFlag = true;

      const userId = ws.idtarget;
      const room = ws.roomname;

      const isInReconnectGrace = userId && this._reconnectingUsers.has(userId);

      ws._isClosing = true;

      if (!isInReconnectGrace && userId && room) {
        const seatInfo = this.userToSeat.get(userId);

        if (seatInfo && seatInfo.room === room) {
          const seatNumber = seatInfo.seat;
          const roomManager = this.roomManagers.get(room);

          if (roomManager) {
            const seatData = roomManager.getSeat(seatNumber);

            if (seatData && seatData.namauser === userId) {
              roomManager.removeSeat(seatNumber);
              roomManager.removePoint(seatNumber);

              this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
              this.updateRoomCount(room);
            }
          }

          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
        }
      }

      if (room) {
        const clientSet = this.roomClients.get(room);
        if (clientSet) clientSet.delete(ws);
      }

      if (userId) {
        const userConnSet = this.userConnections.get(userId);
        if (userConnSet) {
          userConnSet.delete(ws);
          if (userConnSet.size === 0) {
            this.userConnections.delete(userId);
          }
        }

        if (!isInReconnectGrace) {
          this._userMessageCount.delete(userId);
        }

        if (!isInReconnectGrace) {
          const timer = this._reconnectTimers.get(userId);
          if (timer !== undefined) {
            clearTimeout(timer);
            this._reconnectTimers.delete(userId);
          }
          this._reconnectingUsers.delete(userId);
        }
      }

      this._cleanupWebSocketListeners(ws);
      this._activeClients.delete(ws);

      if (ws.readyState === 1) {
        try {
          ws.close(1000, isInReconnectGrace ? "Reconnecting..." : "Cleanup completed");
        } catch (e) {}
      }

    } catch (error) {
      console.error(`Cleanup error for ${ws._cleanupId}:`, error);
    } finally {
      if (release) {
        try { release(); } catch (e) {}
      }
      if (didSetCleaningFlag) {
        setTimeout(() => {
          this._wsCleaningUp.delete(ws._cleanupId);
        }, 2000);
      }
    }
  }

  async assignNewSeat(room, userId) {
    const release = await this.seatLocker.acquire(`room_seat_assign_${room}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return null;

      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;

      const newSeatNumber = roomManager.getAvailableSeat();
      if (!newSeatNumber) return null;

      const success = roomManager.updateSeat(newSeatNumber, {
        noimageUrl: "", 
        namauser: userId, 
        color: "", 
        itembawah: 0,
        itematas: 0, 
        vip: 0, 
        viptanda: 0
      });
      
      if (!success) return null;

      this.userToSeat.set(userId, { room, seat: newSeatNumber });
      this.userCurrentRoom.set(userId, room);

      this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);

      return newSeatNumber;
    } finally {
      release();
    }
  }

  async _doJoinRoom(ws, room) {
    const release = await this.roomLocker.acquire(`room_join_full_${room}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }

      const assignedSeat = await this.assignNewSeat(room, ws.idtarget);
      if (!assignedSeat) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }

      ws.roomname = room;
      
      let clientSet = this.roomClients.get(room);
      if (!clientSet) {
        clientSet = new Set();
        this.roomClients.set(room, clientSet);
      }
      clientSet.add(ws);

      let userConnections = this.userConnections.get(ws.idtarget);
      if (!userConnections) {
        userConnections = new Set();
        this.userConnections.set(ws.idtarget, userConnections);
      }
      userConnections.add(ws);
      this._activeClients.add(ws);

      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await new Promise(resolve => setTimeout(resolve, 500));
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      await this.sendAllStateTo(ws, room);

      return true;
    } catch (error) {
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
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

    const roomManager = this.roomManagers.get(room);
    const isRoomFull = roomManager && roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS;

    if (!isRoomFull) {
      const oldRoom = ws.roomname;
      if (oldRoom && oldRoom !== room) {
        const oldClientSet = this.roomClients.get(oldRoom);
        if (oldClientSet) {
          oldClientSet.delete(ws);
        }
      }

      for (const [otherRoom, otherManager] of this.roomManagers) {
        for (const [seatNum, seatData] of otherManager.seats) {
          if (seatData && seatData.namauser === ws.idtarget) {
            otherManager.removeSeat(seatNum);
            otherManager.removePoint(seatNum);
            this.broadcastToRoom(otherRoom, ["removeKursi", otherRoom, seatNum]);
            this.updateRoomCount(otherRoom);
          }
        }
      }
      
      this.userToSeat.delete(ws.idtarget);
      this.userCurrentRoom.delete(ws.idtarget);
    }

    return await this._doJoinRoom(ws, room);
  }

  async safeRemoveSeat(room, seatNumber, userId) {
    const release = await this.seatLocker.acquire(`room_seat_remove_${room}_${seatNumber}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      const seatData = roomManager.getSeat(seatNumber);
      if (!seatData || seatData.namauser !== userId) return false;

      const success = roomManager.removeSeat(seatNumber);
      if (success) {
        roomManager.removePoint(seatNumber);

        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.updateRoomCount(room);

        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      return success;
    } finally {
      release();
    }
  }

  async updateSeatWithLock(room, seatNumber, seatData, userId) {
    const release = await this.seatLocker.acquire(`seat_${room}_${seatNumber}`);
    try {
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

      let batchMessage;
      try {
        batchMessage = ["kursiBatchUpdate", room, [[seatNumber, {
          noimageUrl: seatData.noimageUrl, namauser: seatData.namauser, color: seatData.color,
          itembawah: seatData.itembawah, itematas: seatData.itematas, vip: seatData.vip, viptanda: seatData.viptanda
        }]]];
        this.broadcastToRoom(room, batchMessage);
      } catch (e) {}
      
      return true;
    } finally {
      release();
    }
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
        const toRemove = [];
        for (const existing of userConnections) {
          if (existing !== ws) {
            toRemove.push(existing);
          }
        }
        
        for (const oldWs of toRemove) {
          userConnections.delete(oldWs);
          this._activeClients.delete(oldWs);
          this._cleanupWebSocketListeners(oldWs);
          
          setTimeout(() => {
            if (oldWs && oldWs.readyState !== 1 && !this._wsCleaningUp.has(oldWs._cleanupId)) {
              this._forceFullCleanupWebSocket(oldWs).catch(() => {});
            }
          }, 100);
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
    const safePoint = {
      x: Math.min(Math.max(parseFloat(point.x) || 0, 0), 1000),
      y: Math.min(Math.max(parseFloat(point.y) || 0, 0), 1000),
      fast: point.fast || false
    };
    return roomManager.updatePoint(seatNumber, safePoint);
  }

  _sendDirectToRoom(room, msg, msgId = null) {
    const clientSet = this.roomClients.get(room);
    if (!clientSet?.size) return 0;

    let messageStr;
    try {
      messageStr = typeof msg === "string" ? msg : JSON.stringify(msg);
    } catch (e) {
      return 0;
    }
    
    let sentCount = 0;
    const failedClients = [];

    const snapshot = Array.from(clientSet);
    for (const client of snapshot) {
      if (!client || client.readyState !== 1 || client._isClosing || this._wsCleaningUp.has(client._cleanupId)) {
        continue;
      }
      try {
        client.send(messageStr);
        sentCount++;
      } catch (e) {
        if (!client._pendingCleanup) {
          client._pendingCleanup = true;
          failedClients.push(client);
        }
      }
    }

    if (failedClients.length > 0) {
      queueMicrotask(() => {
        for (const client of failedClients) {
          this._forceFullCleanupWebSocket(client).catch(() => {});
        }
      });
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
    if (ws._isClosing || ws.readyState !== 1 || this._wsCleaningUp.has(ws._cleanupId)) return false;
    try {
      let message;
      if (typeof msg === "string") {
        message = msg;
      } else {
        try {
          message = JSON.stringify(msg);
        } catch (jsonError) {
          return false;
        }
      }
      ws.send(message);
      return true;
    } catch (error) {
      if (!ws._pendingCleanup && (error.code === 'ECONNRESET' || error.message?.includes('ECONNRESET') || error.message?.includes('CLOSED'))) {
        ws._pendingCleanup = true;
        queueMicrotask(() => {
          this._forceFullCleanupWebSocket(ws).catch(() => {});
        });
      }
      return false;
    }
  }

  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || this._wsCleaningUp.has(ws._cleanupId)) return;

      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return;

      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (!seatInfo || seatInfo.room !== room) return;

      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);

      const allKursiMeta = roomManager.getAllSeatsMeta();
      const lastPointsData = roomManager.getAllPoints();
      const selfSeat = seatInfo.seat;

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

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    try {
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo && seatInfo.room === room) {
        await this.safeRemoveSeat(room, seatInfo.seat, ws.idtarget);
      }
      this._removeFromRoomClients(ws, room);
      await this._removeUserConnection(ws.idtarget, ws);
      this._userMessageCount.delete(ws.idtarget);
      ws.roomname = undefined;
      this.updateRoomCount(room);
    } catch (error) {}
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;

    const release = await this.connectionLocker.acquire(`reconnect_${id}`);
    try {
      const isReconnect = (baru !== true);

      if (isReconnect) {
        this._reconnectingUsers.set(id, Date.now());

        const existingTimer = this._reconnectTimers.get(id);
        if (existingTimer !== undefined) clearTimeout(existingTimer);

        const newTimer = setTimeout(() => {
          if (this._reconnectingUsers.has(id)) {
            this._reconnectingUsers.delete(id);
          }
          this._reconnectTimers.delete(id);
        }, CONSTANTS.RECONNECT_GRACE_PERIOD_MS);

        this._reconnectTimers.set(id, newTimer);
      }

      if (!isReconnect) {
        const snapshotRooms = Array.from(this.roomManagers.entries());
        for (const [roomName, roomManager] of snapshotRooms) {
          const snapshotSeats = Array.from(roomManager.seats.entries());
          for (const [seatNum, seatData] of snapshotSeats) {
            if (seatData.namauser === id) {
              roomManager.removeSeat(seatNum);
              roomManager.removePoint(seatNum);
              this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
              this.updateRoomCount(roomName);
            }
          }
        }

        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
      }

      const existingConnections = this.userConnections.get(id);
      if (existingConnections && existingConnections.size > 0) {
        const oldConnections = Array.from(existingConnections);
        for (const oldWs of oldConnections) {
          if (oldWs !== ws) {
            try {
              await this.safeSend(oldWs, ["connectionReplaced", "Reconnecting..."]);
              if (oldWs.readyState === 1) {
                oldWs.close(1000, "Reconnecting...");
              }
            } catch (e) {}

            if (oldWs.roomname) {
              const clientSet = this.roomClients.get(oldWs.roomname);
              if (clientSet) clientSet.delete(oldWs);
            }

            existingConnections.delete(oldWs);
            this._activeClients.delete(oldWs);
            this._cleanupWebSocketListeners(oldWs);
          }
        }
      }

      ws.idtarget = id;
      ws._isClosing = false;
      ws._pendingCleanup = false;
      ws._connectionTime = Date.now();
      this._activeClients.add(ws);
      await this._addUserConnection(id, ws);

      const seatInfo = this.userToSeat.get(id);

      if (seatInfo && isReconnect) {
        const { room, seat } = seatInfo;
        const roomManager = this.roomManagers.get(room);

        if (roomManager) {
          const seatData = roomManager.getSeat(seat);

          if (seatData && seatData.namauser === id) {
            ws.roomname = room;
            this._addToRoomClients(ws, room);
            await this.sendAllStateTo(ws, room, true);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await this.safeSend(ws, ["currentNumber", this.currentNumber]);
            await this.safeSend(ws, ["reconnectSuccess", room, seat]);

            const timer = this._reconnectTimers.get(id);
            if (timer !== undefined) {
              clearTimeout(timer);
              this._reconnectTimers.delete(id);
            }
            this._reconnectingUsers.delete(id);
            return;
          }
        }
      }

      if (baru === false) {
        await this.safeSend(ws, ["needJoinRoom"]);
      }
      
      if (baru === true) {
        await this.safeSend(ws, ["joinroomawal"]);
      }

    } catch (error) {
      await this.safeSend(ws, ["error", "Connection failed"]);
    } finally {
      release();
    }
  }

  _checkRateLimit(userId) {
    if (!userId) return true;

    const now = Date.now();
    let userData = this._userMessageCount.get(userId);

    if (!userData) {
      userData = { count: 1, windowStart: now };
      this._userMessageCount.set(userId, userData);
      return true;
    }

    if (now - userData.windowStart > CONSTANTS.MESSAGE_RATE_WINDOW_MS) {
      userData.count = 1;
      userData.windowStart = now;
      return true;
    }

    if (userData.count >= CONSTANTS.MAX_MESSAGES_PER_MINUTE) {
      return false;
    }

    userData.count++;
    return true;
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing || this._wsCleaningUp.has(ws._cleanupId)) return;
    if (this._isClosing || this._isCleaningUp) return;

    if (raw instanceof ArrayBuffer) {
      if (raw.byteLength === 0) return;
      try {
        const decoder = new TextDecoder();
        const sliced = raw.slice(0, CONSTANTS.MAX_MESSAGE_SIZE);
        raw = decoder.decode(sliced);
      } catch (e) {
        return;
      }
    }

    let messageStr = raw;
    if (typeof raw !== 'string') {
      return;
    }
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;

    let data;
    try { data = JSON.parse(messageStr); } catch (e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;

    if (data[0] === "chat" && ws.idtarget) {
      if (!this._checkRateLimit(ws.idtarget)) {
        await this.safeSend(ws, ["error", "Rate limit exceeded. Please slow down."]);
        return;
      }
    }

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
          const safeX = Math.min(Math.max(parseFloat(x) || 0, 0), 1000);
          const safeY = Math.min(Math.max(parseFloat(y) || 0, 0), 1000);
          if (this.updatePointDirect(room, seat, { x: safeX, y: safeY, fast: fast === 1 || fast === true }, ws.idtarget)) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, safeX, safeY, fast]);
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          await this.safeRemoveSeat(room, seat, ws.idtarget);
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
              if (conn && conn.readyState === 1 && !conn._isClosing && !this._wsCleaningUp.has(conn._cleanupId)) {
                isOnline = true;
                break;
              }
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
              if (conn && conn.readyState === 1 && !conn._isClosing && !this._wsCleaningUp.has(conn._cleanupId)) {
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
          
          if (!idtarget || !username) {
            await this.safeSend(ws, ["notifError", "Missing target or sender"]);
            return;
          }
          
          const targetConnections = this.userConnections.get(idtarget);
          
          if (!targetConnections || targetConnections.size === 0) {
            await this.safeSend(ws, ["notifError", idtarget, "User is offline"]);
            return;
          }
          
          let sent = false;
          const snapshotConns = Array.from(targetConnections);
          
          for (const client of snapshotConns) {
            if (client && client.readyState === 1 && !client._isClosing && !this._wsCleaningUp.has(client._cleanupId)) {
              const notifMessage = ["notif", noimageUrl || "", username || "", deskripsi || "", Date.now()];
              const success = await this.safeSend(client, notifMessage);
              if (success) {
                sent = true;
              }
            }
          }
          
          if (sent) {
            await this.safeSend(ws, ["notifSent", idtarget, username, deskripsi]);
          } else {
            await this.safeSend(ws, ["notifError", idtarget, "User has no active connections"]);
          }
          break;
        }
        case "checkUserConnection": {
          const [, targetUserId] = data;
          if (!targetUserId) {
            await this.safeSend(ws, ["userConnectionStatus", "", false]);
            return;
          }
          
          const connections = this.userConnections.get(targetUserId);
          let isConnected = false;
          
          if (connections && connections.size > 0) {
            const snapshotConns = Array.from(connections);
            for (const conn of snapshotConns) {
              if (conn && conn.readyState === 1 && !conn._isClosing && !this._wsCleaningUp.has(conn._cleanupId)) {
                isConnected = true;
                break;
              }
            }
          }
          
          await this.safeSend(ws, ["userConnectionStatus", targetUserId, isConnected]);
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
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard && !this._isClosing) {
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
        if (c?.readyState === 1 && !this._wsCleaningUp.has(c._cleanupId)) activeReal++;
      }

      let totalRoomClients = 0;
      for (const clientSet of this.roomClients.values()) totalRoomClients += clientSet.size;

      let totalSeats = 0, totalPoints = 0;
      for (const rm of this.roomManagers.values()) {
        totalSeats += rm.getOccupiedCount();
        totalPoints += rm.points.size;
      }

      return {
        timestamp: Date.now(),
        uptime: Date.now() - this._startTime,
        activeClients: { total: this._activeClients.size, real: activeReal },
        roomClients: { total: totalRoomClients },
        userConnections: this.userConnections.size,
        userToSeatSize: this.userToSeat.size,
        chatBuffer: this.chatBuffer ? this.chatBuffer.getStats() : {},
        pmBuffer: this.pmBuffer ? this.pmBuffer.getStats() : {},
        seats: totalSeats,
        points: totalPoints,
        reconnectingUsers: this._reconnectingUsers.size,
        reconnectTimers: this._reconnectTimers.size,
        rateLimitMapSize: this._userMessageCount.size,
        wsCleaningUpSize: this._wsCleaningUp.size,
        activeListenersSize: this._activeListeners.size,
        asyncLockStats: {
          seat: this.seatLocker.getStats(),
          connection: this.connectionLocker.getStats(),
          room: this.roomLocker.getStats(),
          cleanup: this.cleanupLocker.getStats(),
        }
      };
    } catch (error) {
      return { error: "Failed to get stats" };
    }
  }

  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;

    if (this._masterTimer) { clearInterval(this._masterTimer); this._masterTimer = null; }
    if (this._cleanupSweepTimer) { clearInterval(this._cleanupSweepTimer); this._cleanupSweepTimer = null; }

    for (const timer of this._reconnectTimers.values()) {
      clearTimeout(timer);
    }
    this._reconnectTimers.clear();

    if (this.chatBuffer) {
      await this.chatBuffer.flushAll();
      await this.chatBuffer.destroy();
    }
    if (this.pmBuffer) await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') try { await this.lowcard.destroy(); } catch (e) {}
    this.lowcard = null;

    const snapshot = Array.from(this._activeClients);
    for (const ws of snapshot) {
      if (ws && ws.readyState === 1 && !ws._isClosing && !this._wsCleaningUp.has(ws._cleanupId)) {
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
    this._reconnectingUsers.clear();
    this._reconnectTimers.clear();
    this._userMessageCount.clear();
    
    this.seatLocker.destroy();
    this.connectionLocker.destroy();
    this.roomLocker.destroy();
    this.cleanupLocker.destroy();
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          const snapshot = Array.from(this._activeClients);
          for (const c of snapshot) if (c && c.readyState === 1 && !this._wsCleaningUp.has(c._cleanupId)) activeCount++;
          return new Response(JSON.stringify({
            status: "healthy",
            connections: activeCount,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            memory: "128MB optimized",
            chatBuffer: this.chatBuffer ? this.chatBuffer.getStats() : {},
            pmBuffer: this.pmBuffer ? this.pmBuffer.getStats() : {}
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
        if (url.pathname === "/reset") {
          await this._forceResetAllData();
          return new Response("All data has been reset successfully!", { status: 200 });
        }
        return new Response("ChatServer2 Running - Cloudflare Workers (128MB Optimized)", { status: 200 });
      }

      if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }

      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      const abortController = new AbortController();

      // FIX: accept() is sync, not a Promise
      try {
        server.accept();
      } catch (acceptError) {
        abortController.abort();
        try { server.close(); } catch (e) {}
        return new Response("WebSocket accept failed", { status: 500 });
      }

      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._isCleaningUp = false;
      ws._pendingCleanup = false;
      ws._connectionTime = Date.now();
      ws._abortController = abortController;
      ws._cleanupId = `ws_${Date.now()}_${Math.random().toString(36).substr(2, 8)}`;

      this._activeClients.add(ws);

      const messageHandler = async (ev) => {
        await this.handleMessage(ws, ev.data);
      };

      const errorHandler = () => {
        this._cleanupWebSocketListeners(ws);
        this._forceFullCleanupWebSocket(ws).catch(() => {});
      };

      const closeHandler = () => {
        this._cleanupWebSocketListeners(ws);
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
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }

  async _forceResetAllData() {
    for (const timer of this._reconnectTimers.values()) {
      clearTimeout(timer);
    }
    this._reconnectTimers.clear();

    const snapshot = Array.from(this._activeClients);
    for (const ws of snapshot) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          await this.safeSend(ws, ["serverRestart", "Server is restarting, please reconnect..."]);
          ws.close(1000, "Server restart");
        } catch (e) {}
      }
      if (ws._cleanupId) this._wsCleaningUp.set(ws._cleanupId, Date.now());
    }

    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._wsCleaningUp.clear();
    this._activeListeners.clear();
    this._reconnectingUsers.clear();
    this._reconnectTimers.clear();
    this._userMessageCount.clear();

    for (const room of roomList) {
      if (this.roomManagers.has(room)) {
        this.roomManagers.get(room).destroy();
      }
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    if (this.chatBuffer) {
      await this.chatBuffer.destroy();
      this.chatBuffer = new GlobalChatBuffer();
      this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));
    }

    if (this.pmBuffer) {
      await this.pmBuffer.destroy();
      this.pmBuffer = new PMBuffer();
      this.pmBuffer.setFlushCallback(async (targetId, message) => {
        const targetConnections = this.userConnections.get(targetId);
        if (targetConnections) {
          const snapshotConns = Array.from(targetConnections);
          for (const client of snapshotConns) {
            if (client && client.readyState === 1 && !client._isClosing && !this._wsCleaningUp.has(client._cleanupId)) {
              await this.safeSend(client, message);
              break;
            }
          }
        }
      });
    }

    try {
      if (this.lowcard && typeof this.lowcard.destroy === 'function') {
        await this.lowcard.destroy();
      }
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      this.lowcard = null;
    }

    this.currentNumber = 1;
    this._masterTickCounter = 0;
    this._startTime = Date.now();
  }
}

// ─────────────────────────────────────────────
// Worker Export
// ─────────────────────────────────────────────
export default {
  async fetch(req, env) {
    try {
      const bindingName = "CHAT_SERVER";
      const chatId = env[bindingName].idFromName("chat-room");
      const chatObj = env[bindingName].get(chatId);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/shutdown", "/reset"].includes(url.pathname)) {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running - Cloudflare Workers (128MB Optimized)", { 
        status: 200, 
        headers: { "content-type": "text/plain" } 
      });
    } catch (error) {
      console.error("Worker fetch error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
