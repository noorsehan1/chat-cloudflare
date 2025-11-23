import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

const CONSTANTS = {
  MAX_SEATS: 35,
  RECONNECT_TIMEOUT: 3000,
  FLUSH_INTERVAL: 50,
  AUTO_REMOVE_INTERVAL: 2000,
  MESSAGE_RATE_LIMIT: 20,
  CHAT_RATE_LIMIT: 30,
  POINT_RATE_LIMIT: 50,
  SESSION_TIMEOUT: 5000
};

function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "", itembawah: 0, itematas: 0, 
    vip: 0, viptanda: 0, lastPoint: null, lockTime: undefined
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();
    this.userToSeat = new Map();
    this.userToConnection = new Map();
    this.userSessions = new Map();
    
    this.roomSeats = new Map(roomList.map(room => [
      room, new Map(Array.from({length: CONSTANTS.MAX_SEATS}, (_, i) => 
        [i + 1, createEmptySeat()]
      ))
    ]));

    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);

    this.updateKursiBuffer = new Map(roomList.map(room => [room, new Map()]));
    this.chatMessageBuffer = new Map(roomList.map(room => [room, []]));
    this.seatLocks = new Map();
    this.roomChatHistory = new Map(roomList.map(room => [room, []]));
    this.userDisconnectTime = new Map();

    this._nextConnId = 1;
    this.currentNumber = 1;
    this.maxNumber = 6;

    this.pingTimeouts = new Map();
    this.cleanupInProgress = new Set();
    this.usersToRemove = new Map();
    this.messageCounts = new Map();
    this.userOperations = new Map();

    this._startTimers();
  }

  _startTimers() {
    this._tickTimer = setInterval(() => this._safeTick(), 15 * 60 * 1000);
    this._flushTimer = setInterval(() => this._safeFlush(), CONSTANTS.FLUSH_INTERVAL);
    this._autoRemoveTimer = setInterval(() => this._safeAutoRemove(), CONSTANTS.AUTO_REMOVE_INTERVAL);
    this._sessionCleanupTimer = setInterval(() => this._cleanupExpiredSessions(), 10000);
  }

  _safeTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const c of this.clients) {
        if (c.readyState === 1 && c.roomname) {
          this.safeSend(c, ["currentNumber", this.currentNumber]);
        }
      }
    } catch (error) {}
  }

  _safeFlush() {
    try {
      if (this.clients.size > 0) this.periodicFlush();
    } catch (error) {
      this._resetBuffers();
    }
  }

  _safeAutoRemove() {
    try {
      this.batchAutoRemove();
    } catch (error) {
      this.cleanupInProgress.clear();
    }
  }

  _resetBuffers() {
    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }
  }

  _createUserSession(idtarget) {
    if (!idtarget) return;
    const sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    this.userSessions.set(idtarget, {
      id: sessionId,
      createdAt: Date.now(),
      lastActivity: Date.now()
    });
    return sessionId;
  }

  _validateUserSession(idtarget, currentSessionId = null) {
    if (!idtarget) return false;
    const session = this.userSessions.get(idtarget);
    if (!session) return false;
    session.lastActivity = Date.now();
    if (currentSessionId && session.id !== currentSessionId) return false;
    return true;
  }

  _cleanupExpiredSessions() {
    const now = Date.now();
    for (const [idtarget, session] of this.userSessions) {
      if (now - session.lastActivity > CONSTANTS.SESSION_TIMEOUT) {
        this.userSessions.delete(idtarget);
        this.fullRemoveById(idtarget);
      }
    }
  }

  _closeExistingConnection(idtarget, newSessionId = null) {
    if (!idtarget) return;
    const existingConnection = this.userToConnection.get(idtarget);
    if (existingConnection && existingConnection.readyState === 1) {
      this.safeSend(existingConnection, ["sessionExpired", newSessionId]);
      existingConnection.close(1008, "New session started");
      this.clients.delete(existingConnection);
    }
    this.userToConnection.set(idtarget, null);
  }

  _sendRecentChatHistory(ws, room) {
    if (ws.readyState !== 1) return;
    if (this.roomChatHistory.has(room)) {
      const history = this.roomChatHistory.get(room);
      const recentChats = history.slice(-10);
      for (let i = 0; i < recentChats.length; i++) {
        const chat = recentChats[i];
        this.safeSend(ws, [
          "chat",
          room,
          chat.noImageURL,
          chat.username,
          chat.message,
          chat.usernameColor,
          chat.chatTextColor
        ]);
      }
      this.safeSend(ws, ["chatHistoryComplete", room, recentChats.length]);
    }
  }

  async removeUserFromPreviousRoom(idtarget, previousRoom) {
    if (!idtarget || !previousRoom) return;

    try {
      const seatMap = this.roomSeats.get(previousRoom);
      if (!seatMap) return;

      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget || seatInfo.namauser === `__LOCK__${idtarget}`) {
          if (seatInfo.viptanda > 0) {
            this.vipManager.removeVipBadge(previousRoom, seatNumber);
          }
          Object.assign(seatInfo, createEmptySeat());
          this.clearSeatBuffer(previousRoom, seatNumber);
          this.seatLocks.delete(`${previousRoom}-${seatNumber}`);
          this.broadcastToRoom(previousRoom, ["removeKursi", previousRoom, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(previousRoom);
    } catch (error) {}
  }

  async acquireUserLock(idtarget, operation = 'general') {
    if (!idtarget) return true;
    const lockKey = `${idtarget}-${operation}`;
    const now = Date.now();
    if (this.userOperations.has(lockKey)) {
      const lockTime = this.userOperations.get(lockKey);
      if (now - lockTime < 3000) return false;
      this.userOperations.delete(lockKey);
    }
    this.userOperations.set(lockKey, now);
    return true;
  }

  releaseUserLock(idtarget, operation = 'general') {
    if (!idtarget) return;
    const lockKey = `${idtarget}-${operation}`;
    this.userOperations.delete(lockKey);
  }

  clearSeatBuffer(room, seatNumber) {
    if (!room || typeof seatNumber !== "number") return;
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) roomMap.delete(seatNumber);
  }

  scheduleCleanupTimeout(idtarget) {
    if (!idtarget) return;
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
    }
    const timeout = setTimeout(() => {
      this.pingTimeouts.delete(idtarget);
      this.usersToRemove.set(idtarget, Date.now());
    }, CONSTANTS.RECONNECT_TIMEOUT);
    this.pingTimeouts.set(idtarget, timeout);
  }

  forceUserCleanup(idtarget) {
    if (!idtarget) return;
    this.releaseUserLock(idtarget, 'join');
    this.releaseUserLock(idtarget, 'setId');
    this.usersToRemove.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.cleanupInProgress.delete(idtarget);
    this.userDisconnectTime.delete(idtarget);
    this.userToConnection.delete(idtarget);
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget || seatInfo.namauser === `__LOCK__${idtarget}`) {
          if (seatInfo.viptanda > 0) this.vipManager.removeVipBadge(room, seatNumber);
          Object.assign(seatInfo, createEmptySeat());
          this.clearSeatBuffer(room, seatNumber);
          this.seatLocks.delete(`${room}-${seatNumber}`);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }
    this.userToSeat.delete(idtarget);
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;
    this.releaseUserLock(idtarget, 'join');
    this.releaseUserLock(idtarget, 'setId');
    this.vipManager.cleanupUserVipBadges(idtarget);
    this.usersToRemove.delete(idtarget);
    this.userDisconnectTime.delete(idtarget);
    this.userToConnection.delete(idtarget);
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const [seatNumber, info] of seatMap) {
        const userName = info.namauser;
        if (!userName) continue;
        if (userName === idtarget || userName === `__LOCK__${idtarget}`) {
          Object.assign(info, createEmptySeat());
          this.seatLocks.delete(`${room}-${seatNumber}`);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.clearSeatBuffer(room, seatNumber);
        }
      }
      this.broadcastRoomUserCount(room);
    }
    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.cleanupInProgress.delete(idtarget);
    const clientsToRemove = [];
    for (const c of this.clients) {
      if (c && c.idtarget === idtarget) {
        clientsToRemove.push(c);
        if (c.readyState === 1) c.close(1000, "Session removed");
      }
    }
    for (const c of clientsToRemove) {
      this.clients.delete(c);
    }
  }

  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const key = ws.idtarget || ws._connId || 'anonymous';
    const windowStart = Math.floor(now / 1000);
    if (!this.messageCounts.has(key)) {
      this.messageCounts.set(key, { count: 1, window: windowStart });
      return true;
    }
    const stats = this.messageCounts.get(key);
    if (stats.window !== windowStart) {
      stats.count = 1;
      stats.window = windowStart;
      return true;
    }
    let limit = CONSTANTS.MESSAGE_RATE_LIMIT;
    if (messageType === "chat") limit = CONSTANTS.CHAT_RATE_LIMIT;
    if (messageType === "updatePoint") limit = CONSTANTS.POINT_RATE_LIMIT;
    stats.count++;
    if (stats.count > limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }
    return true;
  }

  async batchAutoRemove() {
    try {
      const now = Date.now();
      const usersToRemoveNow = [];
      for (const [idtarget, removalTime] of this.usersToRemove) {
        if (usersToRemoveNow.length >= 10) break;
        if (now - removalTime >= 1000) {
          usersToRemoveNow.push(idtarget);
        }
      }
      for (const idtarget of usersToRemoveNow) {
        if (this.cleanupInProgress.has(idtarget)) continue;
        this.cleanupInProgress.add(idtarget);
        try {
          const stillActive = Array.from(this.clients).some(
            c => c.idtarget === idtarget && c.readyState === 1
          );
          if (!stillActive) this.fullRemoveById(idtarget);
          this.usersToRemove.delete(idtarget);
        } finally {
          this.cleanupInProgress.delete(idtarget);
        }
      }
    } catch (error) {}
  }

  safeSend(ws, arr) {
    try {
      if (ws && ws.readyState === 1) {
        if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 1_000_000) return false;
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {}
    return false;
  }

  broadcastToRoom(room, msg) {
    let sentCount = 0;
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        if (this.safeSend(c, msg)) sentCount++;
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const info of seatMap.values()) {
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    try {
      const count = this.getJumlahRoom()[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {}
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        for (let i = 0; i < messages.length; i++) {
          this.broadcastToRoom(room, messages[i]);
        }
        this.chatMessageBuffer.set(room, []);
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      const updates = [];
      for (const [seat, info] of seatMapUpdates.entries()) {
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  async periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
    } catch (error) {}
  }

  async handleSetIdTarget2(ws, id, baru, sessionId = null) {
    if (!id) {
      this.safeSend(ws, ["error", "Invalid user ID"]);
      return;
    }
    const lockAcquired = await this.acquireUserLock(id, 'setId');
    if (!lockAcquired) {
      this.safeSend(ws, ["error", "Another operation in progress"]);
      return;
    }
    try {
      if (!baru && sessionId) {
        const isValidSession = this._validateUserSession(id, sessionId);
        if (!isValidSession) {
          this.safeSend(ws, ["sessionExpired", null]);
          return;
        }
      }
      const newSessionId = this._createUserSession(id);
      this._closeExistingConnection(id, newSessionId);
      ws.idtarget = id;
      ws.sessionId = newSessionId;
      this.userToConnection.set(id, ws);
      if (baru === true) {
        this.userDisconnectTime.delete(id);
        ws.roomname = undefined;
        ws.numkursi = new Set();
        this.forceUserCleanup(id);
        this.safeSend(ws, ["sessionCreated", newSessionId]);
      } else if (baru === false) {
        const seatInfo = this.userToSeat.get(id);
        if (seatInfo && this._validateUserSession(id, sessionId)) {
          const { room, seat } = seatInfo;
          const seatMap = this.roomSeats.get(room);
          if (seatMap?.has(seat)) {
            const seatData = seatMap.get(seat);
            if (seatData.namauser === id) {
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              this.sendAllStateTo(ws, room);
              this._sendRecentChatHistory(ws, room);
              this.broadcastRoomUserCount(room);
              this.safeSend(ws, ["sessionRestored", newSessionId]);
            } else {
              this.userToSeat.delete(id);
              this.forceUserCleanup(id);
              this.safeSend(ws, ["needJoinRoom"]);
            }
          } else {
            this.userToSeat.delete(id);
            this.forceUserCleanup(id);
            this.safeSend(ws, ["needJoinRoom"]);
          }
        } else {
          this.safeSend(ws, ["needJoinRoom"]);
        }
      }
    } catch (error) {
      this.safeSend(ws, ["error", "Internal server error"]);
    } finally {
      this.releaseUserLock(id, 'setId');
    }
  }

  async handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) {
      this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    if (!ws.idtarget) {
      this.safeSend(ws, ["error", "No user ID set"]);
      return false;
    }
    const idtarget = ws.idtarget;
    const lockAcquired = await this.acquireUserLock(idtarget, 'join');
    if (!lockAcquired) {
      this.safeSend(ws, ["error", "Another join operation in progress"]);
      return false;
    }
    try {
      const existingSeatInfo = this.userToSeat.get(idtarget);
      if (existingSeatInfo && existingSeatInfo.room === newRoom) {
        this.safeSend(ws, ["error", "Already in this room"]);
        return false;
      }
      if (ws.roomname && ws.roomname !== newRoom) {
        await this.removeUserFromPreviousRoom(idtarget, ws.roomname);
      }
      ws.roomname = newRoom;
      const foundSeat = this.lockSeat(newRoom, ws);
      if (foundSeat === null) {
        this.safeSend(ws, ["roomFull", newRoom]);
        ws.roomname = undefined;
        return false;
      }
      ws.numkursi = new Set([foundSeat]);
      this.userToSeat.set(idtarget, { room: newRoom, seat: foundSeat });
      this.safeSend(ws, ["numberKursiSaya", foundSeat]);
      this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
      this.sendAllStateTo(ws, newRoom);
      this.vipManager.getAllVipBadges(ws, newRoom);
      this.broadcastRoomUserCount(newRoom);
      return true;
    } catch (error) {
      this.safeSend(ws, ["error", "Join room failed"]);
      return false;
    } finally {
      this.releaseUserLock(idtarget, 'join');
    }
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;
    const now = Date.now();
    const userId = ws.idtarget;
    for (const [seatNumber, seatInfo] of seatMap) {
      if (String(seatInfo.namauser).startsWith("__LOCK__") && 
          seatInfo.lockTime && 
          (now - seatInfo.lockTime > 10000)) {
        Object.assign(seatInfo, createEmptySeat());
        this.seatLocks.delete(`${room}-${seatNumber}`);
        this.clearSeatBuffer(room, seatNumber);
      }
    }
    for (let seatNumber = 1; seatNumber <= CONSTANTS.MAX_SEATS; seatNumber++) {
      const seatInfo = seatMap.get(seatNumber);
      if (seatInfo && seatInfo.namauser === "") {
        Object.assign(seatInfo, {
          namauser: "__LOCK__" + userId,
          lockTime: now
        });
        this.seatLocks.set(`${room}-${seatNumber}`, { 
          owner: userId, 
          ts: now 
        });
        return seatNumber;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;
    const allKursiMeta = {};
    const lastPointsData = [];
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
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
    }
    if (Object.keys(allKursiMeta).length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    }
    if (lastPointsData.length > 0) {
      this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    }
    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    const count = this.getJumlahRoom()[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
    if (this.roomChatHistory.has(room)) {
      const history = this.roomChatHistory.get(room);
      const recentChats = history.slice(-5);
      for (let i = 0; i < recentChats.length; i++) {
        const chat = recentChats[i];
        this.safeSend(ws, [
          "chat",
          room,
          chat.noImageURL,
          chat.username,
          chat.message,
          chat.usernameColor,
          chat.chatTextColor
        ]);
      }
    }
  }

  async removeAllSeatsById(idtarget) {
    if (!idtarget) return;
    try {
      const seatInfo = this.userToSeat.get(idtarget);
      if (!seatInfo) return;
      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap || !seatMap.has(seat)) {
        this.userToSeat.delete(idtarget);
        return;
      }
      const currentSeat = seatMap.get(seat);
      if (currentSeat.namauser === idtarget || currentSeat.namauser === `__LOCK__${idtarget}`) {
        if (currentSeat.viptanda > 0) {
          this.vipManager.removeVipBadge(room, seat);
        }
        Object.assign(currentSeat, createEmptySeat());
        this.clearSeatBuffer(room, seat);
        this.seatLocks.delete(`${room}-${seat}`);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }
      this.userToSeat.delete(idtarget);
      this.usersToRemove.delete(idtarget);
    } catch (error) {}
  }

  cleanupClientSafely(ws) {
    const id = ws.idtarget;
    this.clients.delete(ws);
    if (id && this.userToConnection.get(id) === ws) {
      this.userToConnection.delete(id);
    }
    if (!id) return;
    if (this.cleanupInProgress.has(id)) return;
    this.cleanupInProgress.add(id);
    try {
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }
      const hasActiveConnection = this.userToConnection.has(id) && 
                                 this.userToConnection.get(id)?.readyState === 1;
      if (!hasActiveConnection) {
        this.fullRemoveById(id);
      } else {
        this.messageCounts.delete(id);
      }
    } catch (error) {
    } finally {
      this.cleanupInProgress.delete(id);
    }
  }

  handleOnDestroy(ws, idtarget) {
    const targetId = idtarget || ws.idtarget;
    if (!targetId) {
      this.clients.delete(ws);
      return;
    }
    if (this.userToConnection.get(targetId) === ws) {
      this.userToConnection.delete(targetId);
    }
    this.fullRemoveById(targetId);
    this.clients.delete(ws);
  }

  getAllOnlineUsers() {
    const users = [];
    for (const c of this.clients) {
      if (c.idtarget && c.readyState === 1) {
        users.push(c.idtarget);
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    for (const c of this.clients) {
      if (c.roomname === roomName && c.idtarget && c.readyState === 1) {
        users.push(c.idtarget);
      }
    }
    return users;
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    try {
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {}
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      return; 
    }
    if (!Array.isArray(data) || data.length === 0) return;
    const evt = data[0];
    if (!this.checkRateLimit(ws, evt)) return;
    try {
      switch (evt) {
        case "vipbadge":
        case "removeVipBadge":
        case "getAllVipBadges":
          this.vipManager.handleEvent(ws, data);
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
        case "onDestroy": 
          this.handleOnDestroy(ws, data[1] || ws.idtarget);
          break;
        case "setIdTarget2": 
          this.handleSetIdTarget2(ws, data[1], data[2], data[3]);
          break;
        case "setIdTarget": {
          const newId = data[1];
          if (ws.idtarget && ws.idtarget !== newId) {
            this.forceUserCleanup(ws.idtarget);
          }
          ws.idtarget = newId;
          this.userToConnection.set(newId, ws);
          if (this.pingTimeouts.has(newId)) {
            clearTimeout(this.pingTimeouts.get(newId));
            this.pingTimeouts.delete(newId);
          }
          this.usersToRemove.delete(newId);
          const prevSeat = this.userToSeat.get(newId);
          if (prevSeat) {
            ws.roomname = prevSeat.room;
            ws.numkursi = new Set([prevSeat.seat]);
            this.sendAllStateTo(ws, prevSeat.room);
          } else {
            this.safeSend(ws, ["needJoinRoom"]);
          }
          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
          break;
        }
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          for (const c of this.clients) {
            if (c.idtarget === idtarget && c.readyState === 1) {
              this.safeSend(c, notif);
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
          for (const c of this.clients) {
            if (c.idtarget === idt && c.readyState === 1) {
              this.safeSend(c, out);
              break;
            }
          }
          break;
        }
        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";
          let online = false;
          for (const c of this.clients) {
            if (c.idtarget === username && c.readyState === 1) {
              online = true;
              break;
            }
          }
          this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);
          break;
        }
        case "getAllRoomsUserCount": 
          this.handleGetAllRoomsUserCount(ws);
          break;
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
          this.handleJoinRoom(ws, data[1]);
          break;
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return;
          if (!this.chatMessageBuffer.has(roomname))
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          if (!this.roomChatHistory.has(roomname)) {
            this.roomChatHistory.set(roomname, []);
          }
          const history = this.roomChatHistory.get(roomname);
          history.push({
            timestamp: Date.now(),
            noImageURL,
            username,
            message,
            usernameColor,
            chatTextColor
          });
          if (history.length > 5) {
            this.roomChatHistory.set(roomname, history.slice(-5));
          }
          break;
        }
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;
          si.lastPoint = { x, y, fast, timestamp: Date.now() };
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          if (!seatMap || !seatMap.has(seat)) return;
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.seatLocks.delete(`${room}-${seat}`);
          this.clearSeatBuffer(room, seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), {
            noimageUrl, namauser, color, itembawah, itematas,
            vip: vip || 0,
            viptanda: viptanda || 0
          });
          if (!this.updateKursiBuffer.has(room))
            this.updateKursiBuffer.set(room, new Map());
          this.updateKursiBuffer.get(room).set(seat, { ...seatMap.get(seat) });
          this.broadcastRoomUserCount(room);
          break;
        }
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          if (!this.chatMessageBuffer.has(roomname))
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
          break;
        }
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardLeave":
        case "gameLowCardSubmit":
          if (ws.roomname === "LowCard") {
            this.lowcard.handleEvent(ws, data);
          }
          break;
        default: 
          break;
      }
    } catch (e) {}
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
      ws.sessionId = null;
      this.clients.add(ws);
      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          this.cleanupClientSafely(ws);
        }
      });
      ws.addEventListener("error", (event) => {
        this.cleanupClientSafely(ws);
      });
      ws.addEventListener("close", (event) => {
        const id = ws.idtarget;
        if (id) {
          this.userDisconnectTime.set(id, Date.now());
          this.scheduleCleanupTimeout(id);
        }
        this.cleanupClientSafely(ws);
      });
      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
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
        return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
      }
      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
