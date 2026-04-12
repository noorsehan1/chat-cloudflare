// ==================== CHAT SERVER WITH ASYNC LOCK - FULL COMPLETE ====================
// index.js

import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  CLEANUP_TICKS: 5,
  FORCE_CLEANUP_TICKS: 60,
  MEMORY_CHECK_TICKS: 60,
  HEALTH_CHECK_TICKS: 60,
  WS_CLEANUP_TICKS: 10,
  ROOM_CLEANUP_TICKS: 300,
  
  MAX_GLOBAL_CONNECTIONS: 500,
  MAX_ACTIVE_CLIENTS_LIMIT: 500,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 200,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 40,
  
  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MESSAGE_TTL_MS: 5000,
  
  MAX_CONNECTIONS_PER_USER: 3,
  
  FORCE_CLEANUP_CONNECTIONS: 400,
  FORCE_CLEANUP_GAMES: 8,
  FORCE_CLEANUP_BUFFER: 80,
  
  CLEANUP_BATCH_SIZE: 10,
  ROOM_IDLE_BEFORE_CLEANUP: 15 * 60 * 1000,
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

// ==================== ASYNC LOCK ====================
class AsyncLock {
  constructor() {
    this.locks = new Map();
    this.waitingQueues = new Map();
  }
  
  async acquire(key) {
    if (!this.locks.has(key)) {
      this.locks.set(key, true);
      return () => this._release(key);
    }
    
    if (!this.waitingQueues.has(key)) {
      this.waitingQueues.set(key, []);
    }
    
    return new Promise((resolve) => {
      this.waitingQueues.get(key).push(() => {
        this.locks.set(key, true);
        resolve(() => this._release(key));
      });
    });
  }
  
  _release(key) {
    this.locks.delete(key);
    const queue = this.waitingQueues.get(key);
    if (queue && queue.length > 0) {
      const nextResolve = queue.shift();
      if (nextResolve) nextResolve();
    }
    if (queue && queue.length === 0) this.waitingQueues.delete(key);
  }
}

// ==================== UTILITY FUNCTIONS ====================
function safeStringify(obj, maxSize = CONSTANTS.MAX_MESSAGE_SIZE) {
  try {
    const seen = new WeakSet();
    const result = JSON.stringify(obj, (key, value) => {
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return '[Circular]';
        seen.add(value);
      }
      if (typeof value === 'string' && value.length > 1000) return value.substring(0, 1000);
      return value;
    });
    return result && result.length > maxSize ? result.substring(0, maxSize) : result;
  } catch (e) {
    return JSON.stringify({ error: "Stringify failed" });
  }
}

function safeParseJSON(str) {
  if (!str || str.length > CONSTANTS.MAX_MESSAGE_SIZE) return null;
  try { return JSON.parse(str); } catch { return null; }
}

// ==================== GLOBAL CHAT BUFFER ====================
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._retryQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._totalQueued = 0;
    this._pendingMessages = new Map();
    this._nextMsgId = 0;
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = 20;
  }
  
  setFlushCallback(callback) { this._flushCallback = callback; }
  
  _generateMsgId() { return `${Date.now()}_${++this._nextMsgId}_${Math.random().toString(36).substr(2, 6)}`; }
  
  add(room, message) {
    if (this._isDestroyed) { this._sendImmediate(room, message); return; }
    
    let roomSize = this._roomQueueSizes.get(room) || 0;
    if (roomSize >= this.MAX_PER_ROOM || this._messageQueue.length >= this.maxQueueSize) {
      this._sendImmediate(room, message);
      return;
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
    this._cleanupPendingAcks(now);
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
    }
  }
  
  _processRetryQueue(now) {
    const toRetry = this._retryQueue.filter(item => now >= item.nextRetry);
    for (const item of toRetry) {
      if (item.retries >= 3) continue;
      const sent = this._sendWithCallback(item.room, item.message, item.msgId);
      if (!sent) {
        item.retries++;
        item.nextRetry = now + (1000 * Math.pow(2, item.retries));
        this._retryQueue.push(item);
      }
    }
    this._retryQueue = this._retryQueue.filter(item => now < item.nextRetry);
  }
  
  _cleanupPendingAcks(now) {
    for (const [msgId, pending] of this._pendingMessages) {
      if (now - pending.timestamp > 6000) this._pendingMessages.delete(msgId);
    }
  }
  
  _sendWithCallback(room, message, msgId) {
    if (!this._flushCallback) return false;
    try { this._flushCallback(room, message, msgId); return true; } catch (e) { return false; }
  }
  
  _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing) return;
    this._isFlushing = true;
    
    try {
      const roomGroups = {};
      const batch = [...this._messageQueue];
      this._messageQueue = [];
      this._totalQueued = 0;
      
      for (const item of batch) {
        if (!roomGroups[item.room]) roomGroups[item.room] = [];
        roomGroups[item.room].push({ message: item.message, msgId: item.msgId });
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
      }
      
      for (const room in roomGroups) {
        for (const item of roomGroups[room]) {
          try { this._flushCallback(room, item.message, item.msgId); } catch (e) {
            this._retryQueue.push({ room, message: item.message, msgId: item.msgId, retries: 0, nextRetry: Date.now() + 1000 });
          }
        }
      }
    } finally { this._isFlushing = false; }
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
      pendingAcks: this._pendingMessages.size,
      totalQueued: this._totalQueued,
      maxQueueSize: this.maxQueueSize,
    };
  }
  
  async destroy() {
    this._isDestroyed = true;
    this._messageQueue = [];
    this._retryQueue = [];
    this._totalQueued = 0;
    this._roomQueueSizes.clear();
    this._flushCallback = null;
    this._pendingMessages.clear();
  }
}

// ==================== ROOM MANAGER ====================
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
      noimageUrl: "", namauser: userId, color: "", itembawah: 0, itematas: 0, vip: 0, viptanda: 0, lastUpdated: Date.now()
    });
    this.updateActivity();
    return newSeatNumber;
  }
  
  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }
  
  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const existingSeat = this.seats.get(seatNumber);
    if (existingSeat) {
      existingSeat.noimageUrl = seatData.noimageUrl || "";
      existingSeat.namauser = seatData.namauser || "";
      existingSeat.color = seatData.color || "";
      existingSeat.itembawah = seatData.itembawah || 0;
      existingSeat.itematas = seatData.itematas || 0;
      existingSeat.vip = seatData.vip || 0;
      existingSeat.viptanda = seatData.viptanda || 0;
      existingSeat.lastUpdated = Date.now();
    } else {
      this.seats.set(seatNumber, {
        noimageUrl: seatData.noimageUrl || "", namauser: seatData.namauser || "", color: seatData.color || "",
        itembawah: seatData.itembawah || 0, itematas: seatData.itematas || 0, vip: seatData.vip || 0,
        viptanda: seatData.viptanda || 0, lastUpdated: Date.now()
      });
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
  
  setMute(isMuted) { this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1; this.updateActivity(); return this.muteStatus; }
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; this.updateActivity(); }
  getCurrentNumber() { return this.currentNumber; }
  destroy() { this.seats.clear(); this.points.clear(); }
}

// ==================== MAIN CHATSERVER CLASS ====================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._isCleaningUp = false;
    this._cleaningUp = new Set();
    
    this.seatLocker = new AsyncLock();
    this.connectionLocker = new AsyncLock();
    
    this._activeClients = new Set();
    this.roomManagers = new Map();
    this.clients = new Set();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this._activeListeners = new Map();
    this._clientWebSockets = new Set();
    
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    
    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));
    
    try {
      this.lowcard = new LowCardGameManager(this);
      if (this.lowcard && this.lowcard._masterTickInterval) {
        clearInterval(this.lowcard._masterTickInterval);
        this.lowcard._masterTickInterval = null;
      }
    } catch (error) { this.lowcard = null; }
    
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
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
      if (this._masterTickCounter % CONSTANTS.WS_CLEANUP_TICKS === 0) this._cleanupDeadWebSockets();
      if (this._masterTickCounter % CONSTANTS.CLEANUP_TICKS === 0) this._mediumCleanup();
      if (this._masterTickCounter % CONSTANTS.MEMORY_CHECK_TICKS === 0) this._checkMemoryAndForceCleanup();
      if (this._masterTickCounter % CONSTANTS.ROOM_CLEANUP_TICKS === 0) this._cleanupEmptyRooms();
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') this.lowcard.masterTick();
    } catch (error) {}
  }
  
  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }
      
      const message = safeStringify(["currentNumber", this.currentNumber]);
      const clientsToNotify = [];
      for (const client of this._activeClients) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
          clientsToNotify.push(client);
        }
      }
      
      const batchSize = 50;
      for (let i = 0; i < clientsToNotify.length; i += batchSize) {
        const batch = clientsToNotify.slice(i, i + batchSize);
        for (const client of batch) {
          try { client.send(message); } catch (e) {}
        }
        if (i + batchSize < clientsToNotify.length) {
          await new Promise(resolve => setTimeout(resolve, 1));
        }
      }
    } catch (error) {}
  }
  
  async assignNewSeat(room, userId) {
    const release = await this.seatLocker.acquire(`seat_${room}`);
    try {
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
    } finally { release(); }
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
      
      this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seatNumber, {
        noimageUrl: seatData.noimageUrl, namauser: seatData.namauser, color: seatData.color,
        itembawah: seatData.itembawah, itematas: seatData.itematas, vip: seatData.vip, viptanda: seatData.viptanda
      }]]]);
      return true;
    } finally { release(); }
  }
  
  async _addUserConnection(userId, ws) {
    const release = await this.connectionLocker.acquire(`conn_${userId}`);
    try {
      let userConnections = this.userConnections.get(userId);
      if (!userConnections) {
        userConnections = new Set();
        this.userConnections.set(userId, userConnections);
      }
      for (const conn of userConnections) if (conn === ws) return;
      
      if (userConnections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
        const oldest = Array.from(userConnections)[0];
        if (oldest && oldest.readyState === 1) {
          try { oldest.close(1000, "Too many connections"); } catch {}
          userConnections.delete(oldest);
          this._removeFromActiveClients(oldest);
        }
      }
      userConnections.add(ws);
    } finally { release(); }
  }
  
  async _forceCleanupWebSocket(ws) {
    if (!ws || this._cleaningUp.has(ws)) return;
    this._cleaningUp.add(ws);
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      ws._isClosing = true;
      if (userId && room) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo && seatInfo.room === room) {
          const roomManager = this.roomManagers.get(room);
          const seatData = roomManager?.getSeat(seatInfo.seat);
          if (seatData && seatData.namauser === userId) {
            roomManager.removeSeat(seatInfo.seat);
            this.broadcastToRoom(room, ["removeKursi", room, seatInfo.seat]);
            this.updateRoomCount(room);
          }
        }
      }
      if (userId) {
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this._removeUserConnection(userId, ws);
      }
      if (room) this._removeFromRoomClients(ws, room);
      this._cleanupWebSocketListeners(ws);
      this.clients.delete(ws);
      this._removeFromActiveClients(ws);
      this._clientWebSockets.delete(ws);
      if (ws.readyState === 1) try { ws.close(1000, "User disconnected"); } catch(e) {}
    } catch (error) {} finally { this._cleaningUp.delete(ws); }
  }
  
  async _cleanupWebSocketOnly(ws) {
    if (!ws || this._cleaningUp.has(ws)) return;
    this._cleaningUp.add(ws);
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      ws._isClosing = true;
      if (room) this._removeFromRoomClients(ws, room);
      if (userId) this._removeUserConnection(userId, ws);
      this._cleanupWebSocketListeners(ws);
      this.clients.delete(ws);
      this._removeFromActiveClients(ws);
      this._clientWebSockets.delete(ws);
      if (ws.readyState === 1) try { ws.close(1000, "Cleanup"); } catch(e) {}
    } catch (error) {} finally { this._cleaningUp.delete(ws); }
  }
  
  _cleanupDeadWebSockets() {
    const zombies = [];
    for (const ws of this._activeClients) if (!ws || ws.readyState !== 1) zombies.push(ws);
    for (const ws of zombies) this._cleanupWebSocketOnly(ws).catch(() => {});
  }
  
  _mediumCleanup() {
    for (const [userId, connections] of this.userConnections) {
      const alive = new Set();
      for (const conn of connections) if (conn && conn.readyState === 1 && !conn._isClosing) alive.add(conn);
      if (alive.size !== connections.size) this.userConnections.set(userId, alive);
    }
    this._compressRoomClients();
  }
  
  _checkMemoryAndForceCleanup() {
    if (this._activeClients.size > CONSTANTS.FORCE_CLEANUP_CONNECTIONS) this._emergencyCleanup();
  }
  
  _emergencyCleanup() {
    this.chatBuffer.flushAll().catch(() => {});
    const toCleanup = [];
    for (const [userId, connections] of this.userConnections) {
      let hasLive = false;
      for (const conn of connections) if (conn && conn.readyState === 1 && !conn._isClosing) { hasLive = true; break; }
      if (!hasLive) toCleanup.push(userId);
    }
    for (let i = 0; i < Math.min(toCleanup.length, CONSTANTS.CLEANUP_BATCH_SIZE * 2); i++) {
      this._forceRemoveUserSeat(toCleanup[i]).catch(() => {});
    }
    this._compressRoomClients();
  }
  
  async _forceRemoveUserSeat(userId) {
    if (!userId) return;
    try {
      const seatInfo = this.userToSeat.get(userId);
      if (seatInfo) {
        const roomManager = this.roomManagers.get(seatInfo.room);
        if (roomManager) {
          const seatData = roomManager.getSeat(seatInfo.seat);
          if (seatData && seatData.namauser === userId) {
            roomManager.removeSeat(seatInfo.seat);
            this.broadcastToRoom(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
            this.updateRoomCount(seatInfo.room);
          }
        }
      }
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      this.userConnections.delete(userId);
    } catch (error) {}
  }
  
  _cleanupEmptyRooms() {
    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      if (roomManager && roomManager.getOccupiedCount() === 0 && Date.now() - roomManager.lastActivity > CONSTANTS.ROOM_IDLE_BEFORE_CLEANUP) {
        roomManager.destroy();
        this.roomManagers.delete(room);
        this.roomManagers.set(room, new RoomManager(room));
      }
    }
  }
  
  _compressRoomClients() {
    for (const [room, clients] of this.roomClients) {
      const filtered = clients.filter(ws => ws && ws.readyState === 1 && ws.roomname === room);
      if (filtered.length !== clients.length) this.roomClients.set(room, filtered);
    }
  }
  
  _removeFromActiveClients(ws) { this._activeClients.delete(ws); }
  _addToActiveClients(ws) {
    if (this._activeClients.size > CONSTANTS.MAX_ACTIVE_CLIENTS_LIMIT) this._emergencyCleanup();
    this._activeClients.add(ws);
  }
  
  _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    const userConnections = this.userConnections.get(userId);
    if (userConnections) {
      userConnections.delete(ws);
      if (userConnections.size === 0) this.userConnections.delete(userId);
    }
  }
  
  _addToRoomClients(ws, room) {
    if (!ws || !room) return;
    let clientArray = this.roomClients.get(room);
    if (!clientArray) { clientArray = []; this.roomClients.set(room, clientArray); }
    if (!clientArray.includes(ws)) clientArray.push(ws);
  }
  
  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    const clientArray = this.roomClients.get(room);
    if (clientArray) {
      const index = clientArray.indexOf(ws);
      if (index > -1) clientArray.splice(index, 1);
    }
  }
  
  _cleanupWebSocketListeners(ws) {
    if (ws._abortController) { try { ws._abortController.abort(); } catch(e) {} ws._abortController = null; }
    const listeners = this._activeListeners.get(ws);
    if (listeners) {
      for (const { event, handler } of listeners) try { ws.removeEventListener(event, handler); } catch(e) {}
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
  
  safeRemoveSeat(room, seatNumber, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    const success = roomManager.removeSeat(seatNumber);
    if (success) {
      this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      this.updateRoomCount(room);
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
    }
    return success;
  }
  
  updatePointDirect(room, seatNumber, point, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    return roomManager.updatePoint(seatNumber, point);
  }
  
  _sendDirectToRoom(room, msg, msgId = null) {
    let clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    const liveClients = clientArray.filter(ws => ws && ws.readyState === 1 && !ws._isClosing && ws.roomname === room);
    if (liveClients.length === 0) return 0;
    const messageStr = safeStringify(msg);
    let sentCount = 0;
    for (const client of liveClients) {
      try { client.send(messageStr); sentCount++; } catch (e) { this._cleanupWebSocketOnly(client).catch(() => {}); }
    }
    return sentCount;
  }
  
  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    if (msg[0] === "chat") { this.chatBuffer.add(room, msg); return this.getRoomCount(room); }
    return this._sendDirectToRoom(room, msg);
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try {
      const message = typeof msg === "string" ? msg : safeStringify(msg);
      if (message.length > CONSTANTS.MAX_MESSAGE_SIZE) return false;
      ws.send(message);
      return true;
    } catch (error) {
      if (error.code === 'ECONNRESET' || error.message?.includes('ECONNRESET') || error.message?.includes('CLOSED')) {
        await this._cleanupWebSocketOnly(ws);
      }
      return false;
    }
  }
  
  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
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
        for (const [seat, data] of Object.entries(allKursiMeta)) if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
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
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          await new Promise(resolve => setTimeout(resolve, 1000));
          await this.sendAllStateTo(ws, room);
          return true;
        } else { this.userToSeat.delete(ws.idtarget); }
      }
      
      if (currentRoomBeforeJoin && currentRoomBeforeJoin !== room) {
        const oldSeatInfo = this.userToSeat.get(ws.idtarget);
        if (oldSeatInfo && oldSeatInfo.room === currentRoomBeforeJoin) {
          this.safeRemoveSeat(currentRoomBeforeJoin, oldSeatInfo.seat, ws.idtarget);
          this.broadcastToRoom(currentRoomBeforeJoin, ["removeKursi", currentRoomBeforeJoin, oldSeatInfo.seat]);
        }
        this._removeFromRoomClients(ws, currentRoomBeforeJoin);
        this.userToSeat.delete(ws.idtarget);
        this.userCurrentRoom.delete(ws.idtarget);
      }
      
      if (this.getRoomCount(room) >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      const assignedSeat = await this.assignNewSeat(room, ws.idtarget);
      if (!assignedSeat) { await this.safeSend(ws, ["roomFull", room]); return false; }
      
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
      await new Promise(resolve => setTimeout(resolve, 1000));
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
        this.safeRemoveSeat(room, seatInfo.seat, ws.idtarget);
        this.broadcastToRoom(room, ["removeKursi", room, seatInfo.seat]);
      }
      this._removeFromRoomClients(ws, room);
      this._removeUserConnection(ws.idtarget, ws);
      ws.roomname = undefined;
      this.updateRoomCount(room);
    } catch (error) {}
  }
  
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    try {
      const existingConnections = this.userConnections.get(id);
      if (existingConnections && existingConnections.size > 0) {
        for (const oldWs of existingConnections) {
          if (oldWs !== ws && oldWs.readyState === 1 && !oldWs._isClosing) {
            try { await this.safeSend(oldWs, ["connectionReplaced", "New connection detected"]); } catch(e) {}
            await this._forceCleanupWebSocket(oldWs);
          }
        }
      }
      
      if (baru === true) {
        ws.idtarget = id;
        ws.roomname = undefined;
        ws._isClosing = false;
        ws._connectionTime = Date.now();
        await this._addUserConnection(id, ws);
        this._addToActiveClients(ws);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      this._addToActiveClients(ws);
      
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
    } catch (error) { await this.safeSend(ws, ["error", "Reconnection failed"]); }
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    let messageStr = raw;
    if (raw instanceof ArrayBuffer) try { messageStr = new TextDecoder().decode(raw); } catch (e) { return; }
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    let data;
    try { data = safeParseJSON(messageStr); } catch (e) { return; }
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
          if (this.safeRemoveSeat(room, seat, ws.idtarget)) {
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
          }
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
          const updatedSeat = {
            noimageUrl: noimageUrl?.slice(0, 255) || "", namauser, color: color || "",
            itembawah: itembawah || 0, itematas: itematas || 0, vip: vip || 0, viptanda: viptanda || 0, lastUpdated: Date.now()
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
            for (const conn of connections) {
              if (conn && conn.readyState === 1 && !conn._isClosing) { isOnline = true; break; }
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] ?? ""]);
          break;
        }
        
        // ==================== GIFT ====================
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, safeGiftName, Date.now()]);
          break;
        }
        
        // ==================== ROLL ANGKA ====================
        case "rollangak": {
          const [, roomname, username, angka] = data;
          if (!roomList.includes(roomname)) return;
          this.broadcastToRoom(roomname, ["rollangakBroadcast", roomname, username, angka]);
          break;
        }
        
        // ==================== MOD WARNING ====================
        case "modwarning": {
          const [, roomname] = data;
          if (!roomList.includes(roomname)) return;
          this.broadcastToRoom(roomname, ["modwarning", roomname]);
          break;
        }
        
        // ==================== GET ONLINE USERS ====================
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
        
        // ==================== SEND NOTIFICATION ====================
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
        
        // ==================== PRIVATE MESSAGE ====================
        case "private": {
          const [, idtarget, noimageUrl, message, sender] = data;
          const targetConnections = this.userConnections.get(idtarget);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                await this.safeSend(client, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
                break;
              }
            }
          }
          break;
        }
        
        // ==================== GAME LOWCARD ====================
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            try { await this.lowcard.handleEvent(ws, data); } catch (error) {}
          }
          break;
          
        // ==================== ON DESTROY ====================
        case "onDestroy":
          await this._forceCleanupWebSocket(ws);
          break;
          
        default: break;
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
    let activeReal = 0;
    for (const c of this._activeClients) if (c?.readyState === 1) activeReal++;
    let totalRoomClients = 0;
    for (const clients of this.roomClients.values()) totalRoomClients += clients.filter(ws => ws !== null).length;
    let totalSeats = 0, totalPoints = 0;
    for (const rm of this.roomManagers.values()) { totalSeats += rm.seats.size; totalPoints += rm.points.size; }
    return {
      timestamp: Date.now(),
      uptime: Date.now() - this._startTime,
      activeClients: { total: this._activeClients.size, real: activeReal, waste: this._activeClients.size - activeReal },
      roomClients: { total: totalRoomClients },
      userConnections: this.userConnections.size,
      userToSeatSize: this.userToSeat.size,
      userCurrentRoomSize: this.userCurrentRoom.size,
      chatBuffer: this.chatBuffer.getStats(),
      seats: totalSeats,
      points: totalPoints
    };
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    if (this._masterTimer) { clearInterval(this._masterTimer); this._masterTimer = null; }
    await this.chatBuffer.flushAll();
    await this.chatBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') try { await this.lowcard.destroy(); } catch(e) {}
    this.lowcard = null;
    const clientsToClose = Array.from(this._activeClients);
    for (const ws of clientsToClose) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { this._cleanupWebSocketListeners(ws); ws.close(1000, "Server shutdown"); } catch(e) {}
      }
    }
    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this.clients.clear();
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._activeListeners.clear();
    this._clientWebSockets.clear();
    this._cleaningUp.clear();
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          for (const c of this._activeClients) if (c && c.readyState === 1) activeCount++;
          return new Response(JSON.stringify({ 
            status: "healthy", connections: activeCount, rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime, buffer: this.chatBuffer.getStats(),
            masterTimer: this._masterTimer ? "running" : "stopped",
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/debug/memory") return new Response(JSON.stringify(await this.getMemoryStats(), null, 2), { status: 200, headers: { "content-type": "application/json" } });
        if (url.pathname === "/debug/roomcounts") {
          const counts = {}; for (const room of roomList) counts[room] = this.getRoomCount(room);
          return new Response(JSON.stringify({ counts, total: Object.values(counts).reduce((a,b) => a + b, 0) }), { headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/shutdown") { await this.shutdown(); return new Response("Shutting down...", { status: 200 }); }
        return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200 });
      }
      
      if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) return new Response("Server overloaded", { status: 503 });
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      const abortController = new AbortController();
      
      try { await server.accept(); } catch { abortController.abort(); return new Response("WebSocket accept failed", { status: 500 }); }
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      ws._abortController = abortController;
      
      this.clients.add(ws);
      this._addToActiveClients(ws);
      this._clientWebSockets.add(client);
      
      const messageHandler = (ev) => { this.handleMessage(ws, ev.data).catch(() => {}); };
      const errorHandler = () => { this._forceCleanupWebSocket(ws).catch(() => {}); };
      const closeHandler = () => { this._forceCleanupWebSocket(ws).catch(() => {}); };
      
      ws.addEventListener("message", messageHandler, { signal: abortController.signal });
      ws.addEventListener("error", errorHandler, { signal: abortController.signal });
      ws.addEventListener("close", closeHandler, { signal: abortController.signal });
      
      this._activeListeners.set(ws, [
        { event: "message", handler: messageHandler },
        { event: "error", handler: errorHandler },
        { event: "close", handler: closeHandler }
      ]);
      
      client.addEventListener("close", () => { this._clientWebSockets.delete(client); });
      
      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER.get(chatId);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") return chatObj.fetch(req);
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/shutdown"].includes(url.pathname)) return chatObj.fetch(req);
      return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200, headers: { "content-type": "text/plain" } });
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
};
