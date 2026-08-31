// ==================== CHAT-SERVER-HIBERNATION-NO-PING.JS ====================
// VERSION: 10.1.0 - FORCE CLEANUP ON DISCONNECT

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_INTERVAL_MS: 900000,
  MAX_NUMBER: 6,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);

class DataManager {
  constructor(ctx) {
    this.ctx = ctx;
    this.cache = {
      roomsData: {},
      userSeatData: {},
      userCounts: {},
      onlineUsers: new Set(),
      currentNumber: 1
    };
    this._isSaving = false;
    this._saveQueue = [];
    this._isRestoring = false;
  }

  async _atomicSave(updates) {
    return new Promise((resolve, reject) => {
      this._saveQueue.push({ updates, resolve, reject });
      if (!this._isSaving) {
        this._processQueue();
      }
    });
  }

  async _processQueue() {
    if (this._isSaving || this._saveQueue.length === 0) return;
    this._isSaving = true;
    try {
      while (this._saveQueue.length > 0) {
        const item = this._saveQueue.shift();
        try {
          if (item.updates.roomsData !== undefined) {
            this.cache.roomsData = JSON.parse(JSON.stringify(item.updates.roomsData));
          }
          if (item.updates.userSeatData !== undefined) {
            this.cache.userSeatData = JSON.parse(JSON.stringify(item.updates.userSeatData));
          }
          if (item.updates.currentNumber !== undefined) {
            this.cache.currentNumber = item.updates.currentNumber;
          }
          if (item.updates.userCounts !== undefined) {
            this.cache.userCounts = JSON.parse(JSON.stringify(item.updates.userCounts));
          }
          if (item.updates.onlineUsers !== undefined) {
            this.cache.onlineUsers = new Set(item.updates.onlineUsers);
          }
          const storageUpdates = {};
          if (item.updates.roomsData !== undefined) storageUpdates.roomsData = item.updates.roomsData;
          if (item.updates.userSeatData !== undefined) storageUpdates.userSeatData = item.updates.userSeatData;
          if (item.updates.currentNumber !== undefined) storageUpdates.currentNumber = item.updates.currentNumber;
          if (item.updates.userCounts !== undefined) storageUpdates.userCounts = item.updates.userCounts;
          if (item.updates.onlineUsers !== undefined) storageUpdates.onlineUsers = item.updates.onlineUsers;
          await this.ctx.storage.put(storageUpdates);
          item.resolve();
        } catch(e) {
          await this._rollbackCache();
          item.reject(e);
        }
      }
    } finally {
      this._isSaving = false;
    }
  }

  async _rollbackCache() {
    try {
      const storage = await this.ctx.storage.get(["roomsData", "userSeatData", "currentNumber", "userCounts", "onlineUsers"]);
      this.cache.roomsData = storage.roomsData || {};
      this.cache.userSeatData = storage.userSeatData || {};
      this.cache.currentNumber = storage.currentNumber || 1;
      this.cache.userCounts = storage.userCounts || {};
      this.cache.onlineUsers = new Set(storage.onlineUsers || []);
    } catch(e) {}
  }

  getRoomsData() { return this.cache.roomsData; }
  getUserSeatData() { return this.cache.userSeatData; }
  getOnlineUsers() { return this.cache.onlineUsers; }
  getCurrentNumber() { return this.cache.currentNumber; }
  getUserCounts() { return this.cache.userCounts; }

  async restoreFromStorage() {
    if (this._isRestoring) return this.cache;
    this._isRestoring = true;
    try {
      const storage = await this.ctx.storage.get(["roomsData", "userSeatData", "currentNumber", "userCounts", "onlineUsers"]);
      this.cache.roomsData = storage.roomsData || {};
      this.cache.userSeatData = storage.userSeatData || {};
      this.cache.currentNumber = storage.currentNumber || 1;
      this.cache.userCounts = storage.userCounts || {};
      this.cache.onlineUsers = new Set(storage.onlineUsers || []);
      await this._validateConsistency();
      return this.cache;
    } catch(e) { return this.cache; } finally { this._isRestoring = false; }
  }

  async _validateConsistency() {
    let fixed = false;
    const roomsData = this.cache.roomsData;
    const userSeatData = this.cache.userSeatData;
    const onlineUsers = this.cache.onlineUsers;
    for (const [username, seatInfo] of Object.entries(userSeatData)) {
      if (!seatInfo || !seatInfo.room) {
        delete userSeatData[username];
        onlineUsers.delete(username);
        fixed = true;
        continue;
      }
      const roomData = roomsData[seatInfo.room];
      if (!roomData || !roomData.seats || !roomData.seats[seatInfo.seat]) {
        delete userSeatData[username];
        onlineUsers.delete(username);
        fixed = true;
        continue;
      }
      if (roomData.seats[seatInfo.seat].namauser !== username) {
        delete userSeatData[username];
        onlineUsers.delete(username);
        fixed = true;
        continue;
      }
    }
    for (const username of onlineUsers) {
      if (!userSeatData[username]) {
        onlineUsers.delete(username);
        fixed = true;
      }
    }
    for (const room of ROOMS) {
      const roomData = roomsData[room];
      if (roomData && roomData.seats) {
        this.cache.userCounts[room] = Object.keys(roomData.seats).length;
      } else {
        this.cache.userCounts[room] = 0;
      }
    }
    if (fixed) {
      await this._atomicSave({
        roomsData: roomsData,
        userSeatData: userSeatData,
        onlineUsers: Array.from(onlineUsers),
        userCounts: this.cache.userCounts
      });
    }
  }

  async addUserToRoom(username, roomName, seat, isMulti = false, seatData = {}) {
    if (!username || !roomName || !seat) throw new Error("Invalid parameters");
    const roomsData = JSON.parse(JSON.stringify(this.cache.roomsData));
    const userSeatData = JSON.parse(JSON.stringify(this.cache.userSeatData));
    const onlineUsers = new Set(this.cache.onlineUsers);
    const userCounts = JSON.parse(JSON.stringify(this.cache.userCounts));
    if (!roomsData[roomName]) roomsData[roomName] = { seats: {}, points: {}, muted: false, number: 1 };
    roomsData[roomName].seats[seat] = {
      noimageUrl: seatData.noimageUrl || "",
      namauser: username,
      color: seatData.color || "",
      itembawah: seatData.itembawah || 0,
      itematas: seatData.itematas || 0,
      vip: seatData.vip || 0,
      viptanda: seatData.viptanda || 0
    };
    userSeatData[username] = {
      room: roomName, seat: seat, isMulti: isMulti,
      multiRoom: isMulti ? roomName : null, multiSeat: isMulti ? seat : null
    };
    onlineUsers.add(username);
    userCounts[roomName] = Object.keys(roomsData[roomName].seats).length;
    await this._atomicSave({ roomsData, userSeatData, onlineUsers: Array.from(onlineUsers), userCounts });
    return { room: roomName, seat: seat };
  }

  async removeUserFromAllRooms(username) {
    if (!username) throw new Error("Username required");
    const roomsData = JSON.parse(JSON.stringify(this.cache.roomsData));
    const userSeatData = JSON.parse(JSON.stringify(this.cache.userSeatData));
    const onlineUsers = new Set(this.cache.onlineUsers);
    const userCounts = JSON.parse(JSON.stringify(this.cache.userCounts));
    let removed = false;
    let roomsAffected = [];
    for (const [roomName, roomData] of Object.entries(roomsData)) {
      if (!roomData || !roomData.seats) continue;
      let seatToRemove = null;
      for (const [seat, data] of Object.entries(roomData.seats)) {
        if (data && data.namauser === username) { seatToRemove = parseInt(seat); break; }
      }
      if (seatToRemove !== null) {
        delete roomData.seats[seatToRemove];
        if (roomData.points) delete roomData.points[seatToRemove];
        userCounts[roomName] = Object.keys(roomData.seats).length;
        removed = true;
        roomsAffected.push({ room: roomName, seat: seatToRemove });
        if (Object.keys(roomData.seats).length === 0 && Object.keys(roomData.points || {}).length === 0) {
          delete roomsData[roomName];
          userCounts[roomName] = 0;
        }
      }
    }
    if (userSeatData[username]) { delete userSeatData[username]; removed = true; }
    if (onlineUsers.has(username)) { onlineUsers.delete(username); removed = true; }
    if (removed) {
      await this._atomicSave({ roomsData, userSeatData, onlineUsers: Array.from(onlineUsers), userCounts });
      await this.ctx.storage.delete(`user_${username}_data`);
      await this.ctx.storage.delete(`user_${username}_room`);
      await this.ctx.storage.delete(`user_${username}_seat`);
    }
    return { removed, roomsAffected };
  }

  async updateSeatData(roomName, seat, data) {
    if (!roomName || !seat || !data) throw new Error("Invalid parameters");
    const roomsData = JSON.parse(JSON.stringify(this.cache.roomsData));
    if (!roomsData[roomName] || !roomsData[roomName].seats[seat]) throw new Error("Seat not found");
    roomsData[roomName].seats[seat] = {
      noimageUrl: data.noimageUrl || "",
      namauser: data.namauser || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0
    };
    await this._atomicSave({ roomsData });
    return true;
  }

  async updatePoint(roomName, seat, x, y, fast) {
    if (!roomName || !seat) throw new Error("Invalid parameters");
    const roomsData = JSON.parse(JSON.stringify(this.cache.roomsData));
    if (!roomsData[roomName] || !roomsData[roomName].seats[seat]) throw new Error("Seat not found");
    if (!roomsData[roomName].points) roomsData[roomName].points = {};
    roomsData[roomName].points[seat] = { x: x || 0, y: y || 0, fast: !!fast };
    await this._atomicSave({ roomsData });
    return true;
  }

  async updateNumber(number) {
    const roomsData = JSON.parse(JSON.stringify(this.cache.roomsData));
    for (const [roomName, roomData] of Object.entries(roomsData)) {
      if (roomData) roomData.number = number;
    }
    await this._atomicSave({ roomsData, currentNumber: number });
    return true;
  }

  async updateMuteStatus(roomName, muted) {
    if (!roomName) throw new Error("Room name required");
    const roomsData = JSON.parse(JSON.stringify(this.cache.roomsData));
    if (!roomsData[roomName]) roomsData[roomName] = { seats: {}, points: {}, muted: false, number: 1 };
    roomsData[roomName].muted = !!muted;
    await this._atomicSave({ roomsData });
    return true;
  }

  async forceCleanupUser(username) {
    if (!username) return false;
    const roomsData = JSON.parse(JSON.stringify(this.cache.roomsData));
    const userSeatData = JSON.parse(JSON.stringify(this.cache.userSeatData));
    const onlineUsers = new Set(this.cache.onlineUsers);
    const userCounts = JSON.parse(JSON.stringify(this.cache.userCounts));
    let removed = false;
    let roomsAffected = [];
    for (const [roomName, roomData] of Object.entries(roomsData)) {
      if (!roomData || !roomData.seats) continue;
      let seatToRemove = null;
      for (const [seat, data] of Object.entries(roomData.seats)) {
        if (data && data.namauser === username) { seatToRemove = parseInt(seat); break; }
      }
      if (seatToRemove !== null) {
        delete roomData.seats[seatToRemove];
        if (roomData.points) delete roomData.points[seatToRemove];
        userCounts[roomName] = Object.keys(roomData.seats).length;
        removed = true;
        roomsAffected.push({ room: roomName, seat: seatToRemove });
        if (Object.keys(roomData.seats).length === 0 && Object.keys(roomData.points || {}).length === 0) {
          delete roomsData[roomName];
          userCounts[roomName] = 0;
        }
      }
    }
    if (userSeatData[username]) { delete userSeatData[username]; removed = true; }
    if (onlineUsers.has(username)) { onlineUsers.delete(username); removed = true; }
    if (removed) {
      await this._atomicSave({ roomsData, userSeatData, onlineUsers: Array.from(onlineUsers), userCounts });
      await this.ctx.storage.delete(`user_${username}_data`);
      await this.ctx.storage.delete(`user_${username}_room`);
      await this.ctx.storage.delete(`user_${username}_seat`);
    }
    return { removed, roomsAffected };
  }

  async resetAllData() {
    this.cache.roomsData = {};
    this.cache.userSeatData = {};
    this.cache.userCounts = {};
    this.cache.onlineUsers = new Set();
    this.cache.currentNumber = 1;
    await this.ctx.storage.delete("roomsData");
    await this.ctx.storage.delete("userSeatData");
    await this.ctx.storage.delete("currentNumber");
    await this.ctx.storage.delete("userCounts");
    await this.ctx.storage.delete("onlineUsers");
    return true;
  }
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.ctx = state;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    this._isNumberUpdating = false;
    this._isRestoring = false;
    this.dataManager = new DataManager(this.ctx);
    this.roomClients = new Map();
    for (const room of ROOMS) this.roomClients.set(room, new Set());
    this._restoreAllState().then(() => {});
  }

  get _roomsDataCache() { return this.dataManager.getRoomsData(); }
  get _userSeatDataCache() { return this.dataManager.getUserSeatData(); }
  get _onlineUsers() { return this.dataManager.getOnlineUsers(); }
  get currentNumber() { return this.dataManager.getCurrentNumber(); }
  get _userCounts() { return this.dataManager.getUserCounts(); }

  async _restoreAllState() {
    if (this._isRestoring) return;
    this._isRestoring = true;
    try {
      await this.dataManager.restoreFromStorage();
      const webSockets = this.ctx.getWebSockets();
      for (const ws of webSockets) {
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username) {
            const userSeat = this._userSeatDataCache[attachment.username];
            if (userSeat) {
              ws.username = attachment.username;
              ws.room = userSeat.room;
              ws.roomname = userSeat.room;
              ws._cachedUsername = attachment.username;
              ws._cachedRoom = userSeat.room;
              ws._isMulti = attachment.isMulti || userSeat.isMulti || false;
              ws._closing = false;
              ws.serializeAttachment({
                username: attachment.username, room: userSeat.room, seat: userSeat.seat,
                isMulti: ws._isMulti, multiRoom: userSeat.multiRoom || userSeat.room,
                multiSeat: userSeat.multiSeat || userSeat.seat, seatInfo: userSeat
              });
            }
          }
        } catch(e) {}
      }
      this._refreshRoomClients(true);
      if (!this.closing && !this.isDestroyed) {
        const existingAlarm = await this.ctx.storage.getAlarm();
        if (!existingAlarm) this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      }
    } catch(e) {} finally { this._isRestoring = false; }
  }

  _getActiveWebSockets() {
    try { return this.ctx.getWebSockets(); } catch(e) { return []; }
  }

  _refreshRoomClients(force = false) {
    for (const room of ROOMS) this.roomClients.set(room, new Set());
    const webSockets = this._getActiveWebSockets();
    const roomsData = this._roomsDataCache;
    for (const ws of webSockets) {
      try {
        if (ws._closing || ws.readyState !== 1) continue;
        let room = ws._cachedRoom, username = ws._cachedUsername;
        if (!room || !username) {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username && attachment.room) {
            room = attachment.room; username = attachment.username;
            ws._cachedRoom = room; ws._cachedUsername = username;
          }
        }
        if (!room) room = ws.room || ws.roomname;
        if (!username) username = ws.username || ws.idtarget;
        if (room && username && ROOMS_SET.has(room)) {
          const roomData = roomsData[room];
          if (roomData && roomData.seats) {
            let found = false;
            for (const [seat, data] of Object.entries(roomData.seats)) {
              if (data && data.namauser === username) { found = true; break; }
            }
            if (found) {
              const roomClients = this.roomClients.get(room);
              if (roomClients) roomClients.add(ws);
            } else {
              ws._cachedRoom = null; ws.room = null; ws.roomname = null;
            }
          }
        }
      } catch(e) {}
    }
  }

  async _forceCleanupUser(ws) {
    if (!ws) return false;
    const username = ws.username || ws._cachedUsername;
    if (!username) {
      ws._closing = true;
      try { ws.serializeAttachment({}); } catch(e) {}
      return false;
    }
    const result = await this.dataManager.forceCleanupUser(username);
    if (result.removed) {
      for (const { room, seat } of result.roomsAffected) {
        this.broadcast(room, ["removeKursi", room, seat]);
        await this.updateRoomCount(room);
      }
      ws._isMulti = false; ws._multiRoom = null; ws._multiSeat = null;
      ws._cachedRoom = null; ws._cachedUsername = null;
      ws.room = null; ws.roomname = null; ws.idtarget = null; ws.username = null;
      ws._closing = true;
      try { ws.serializeAttachment({}); } catch(e) {}
      this._refreshRoomClients(true);
      await this._updateUserCounts();
    }
    return result.removed;
  }

  async webSocketClose(ws, code, reason, wasClean) {
    if (!ws) return;
    ws._closing = true;
    await this._forceCleanupUser(ws);
  }

  async webSocketError(ws, error) {
    if (!ws) return;
    ws._closing = true;
    await this._forceCleanupUser(ws);
  }

  async webSocketMessage(ws, message) {
    if (!ws || ws._closing || this.closing || this.isDestroyed) return;
    try {
      let attachment = null;
      try { attachment = ws.deserializeAttachment(); } catch(e) {}
      if (attachment && attachment.username) {
        ws.username = attachment.username;
        ws.room = attachment.room;
        ws.roomname = attachment.room;
        ws.idtarget = attachment.username;
        ws._isMulti = attachment.isMulti || false;
        ws._multiRoom = attachment.multiRoom || null;
        ws._multiSeat = attachment.multiSeat || null;
        ws._cachedUsername = attachment.username;
        ws._cachedRoom = attachment.room;
        if (attachment.seatInfo) {
          this.dataManager.cache.userSeatData[attachment.username] = attachment.seatInfo;
        }
      }
      let str = typeof message === 'string' ? message : new TextDecoder().decode(message);
      if (str.length > C.MAX_MESSAGE_SIZE) return;
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      const [evt, ...args] = data;
      if (evt === "chat" || evt === "updatePoint" || evt === "gift" || evt === "rollangak") {
        const room = args[0];
        if (room && !ROOMS_SET.has(room)) return;
      }
      await this._handleEventInternal(ws, [evt, ...args]);
    } catch(e) { await this._forceCleanupUser(ws); }
  }

  async _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    if (ws.readyState !== 1) return;
    await this._forceCleanupUser(ws);
    ws.username = username; ws.idtarget = username; ws.room = null; ws.roomname = null;
    ws._closing = false; ws._isMulti = false; ws._multiRoom = null; ws._multiSeat = null;
    ws._cachedUsername = username; ws._cachedRoom = null;
    ws.serializeAttachment({ username: username });
    if (isNewUser) { this.safeSend(ws, ["joinroomawal"]); } else { this.safeSend(ws, ["needJoinRoom"]); }
  }

  async _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) return false;
    const username = ws.username;
    await this._forceCleanupUser(ws);
    let roomsData = this._roomsDataCache;
    if (!roomsData[roomName]) {
      roomsData[roomName] = { seats: {}, points: {}, muted: false, number: 1 };
      await this.dataManager._atomicSave({ roomsData });
    }
    let seat = null;
    const roomData = roomsData[roomName];
    if (Object.keys(roomData.seats).length >= C.MAX_SEATS) {
      this.safeSend(ws, ["roomFull", roomName]);
      return false;
    }
    for (let s = 1; s <= C.MAX_SEATS; s++) {
      if (!roomData.seats[s]) { seat = s; break; }
    }
    if (!seat) { this.safeSend(ws, ["roomFull", roomName]); return false; }
    await this.dataManager.addUserToRoom(username, roomName, seat, false);
    ws._cachedUsername = username; ws._cachedRoom = roomName;
    ws.room = roomName; ws.roomname = roomName; ws._isMulti = false; ws._closing = false;
    ws.serializeAttachment({
      username: username, room: roomName, seat: seat,
      isMulti: false, seatInfo: { room: roomName, seat: seat, isMulti: false }
    });
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomData.muted || false, roomName]);
    const count = Object.keys(roomData.seats).length;
    this.safeSend(ws, ["roomUserCount", roomName, count]);
    this.broadcast(roomName, ["roomUserCount", roomName, count]);
    this._refreshRoomClients(true);
    setTimeout(() => {
      try { if (ws && ws.readyState === 1) this.sendAllStateTo(ws, roomName, true); } catch(e) {}
    }, 1000);
    return true;
  }

  async _updateUserCounts() {
    try {
      const roomsData = this._roomsDataCache;
      const newCounts = {};
      let totalUsers = 0;
      for (const room of ROOMS) {
        const roomData = roomsData[room];
        const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
        newCounts[room] = count;
        totalUsers += count;
      }
      this.dataManager.cache.userCounts = newCounts;
      await this.dataManager._atomicSave({ userCounts: newCounts });
      return { counts: newCounts, total: totalUsers };
    } catch(e) { return { counts: this._userCounts, total: this._onlineUsers.size }; }
  }

  async updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      const roomsData = this._roomsDataCache;
      const roomData = roomsData[room];
      const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
      this.dataManager.cache.userCounts[room] = count;
      await this.dataManager._atomicSave({ userCounts: this.dataManager.cache.userCounts });
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { return 0; }
  }

  _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed || !room) return;
    this._refreshRoomClients(false);
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    const clientArray = Array.from(clients);
    const toRemove = new Set();
    const roomsData = this._roomsDataCache;
    for (const ws of clientArray) {
      if (!ws) { toRemove.add(ws); continue; }
      try {
        let wsRoom = ws._cachedRoom;
        if (!wsRoom) {
          try { const attachment = ws.deserializeAttachment(); wsRoom = attachment?.room; } catch(e) {}
        }
        if (!wsRoom) wsRoom = ws.room || ws.roomname;
        if (wsRoom !== room) { toRemove.add(ws); continue; }
        const roomData = roomsData[room];
        let isValid = false;
        if (roomData && roomData.seats) {
          const username = ws._cachedUsername || ws.username;
          for (const [seat, data] of Object.entries(roomData.seats)) {
            if (data && data.namauser === username) { isValid = true; break; }
          }
        }
        if (!isValid) { toRemove.add(ws); continue; }
        if (ws.readyState === 1 && !ws._closing) ws.send(msgStr);
        else toRemove.add(ws);
      } catch(e) { toRemove.add(ws); }
    }
    if (toRemove.size > 0) {
      for (const ws of toRemove) { try { clients.delete(ws); } catch(e) {} }
    }
  }

  broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try { this._broadcastToRoom(room, JSON.stringify(msg)); } catch(e) {}
  }

  safeSend(ws, msg) {
    if (!ws) return false;
    try {
      if (ws.readyState !== 1 || ws._closing || this.closing || this.isDestroyed) return false;
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) { return false; }
  }

  async sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    try { if (ws.readyState !== 1 || ws._closing) return; } catch(e) { return; }
    const roomsData = this._roomsDataCache;
    const roomData = roomsData[room];
    if (!roomData) return;
    try {
      const allSeats = roomData.seats || {};
      const allPoints = roomData.points || {};
      const userSeat = this._userSeatDataCache[ws.username];
      const selfSeat = userSeat?.seat;
      const count = Object.keys(allSeats).length;
      this.safeSend(ws, ["roomUserCount", room, count]);
      if (allSeats && Object.keys(allSeats).length > 0) {
        if (excludeSelf && selfSeat && allSeats[selfSeat]) {
          const filtered = { ...allSeats };
          delete filtered[selfSeat];
          if (Object.keys(filtered).length > 0) this.safeSend(ws, ["allUpdateKursiList", room, filtered]);
        } else {
          this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
        }
      }
      if (allPoints && Object.keys(allPoints).length > 0) {
        let filteredPoints = Object.entries(allPoints).map(([seat, point]) => ({
          seat: parseInt(seat), x: point.x, y: point.y, fast: point.fast ? 1 : 0
        }));
        if (excludeSelf && selfSeat) filteredPoints = filteredPoints.filter(p => p.seat !== selfSeat);
        if (filteredPoints.length > 0) this.safeSend(ws, ["allPointsList", room, filteredPoints]);
      }
    } catch(e) {}
  }

  async alarm() {
    if (this.closing || this.isDestroyed) return;
    await this._updateNumber();
    await this._cleanupOrphanUsers();
    if (!this.closing && !this.isDestroyed) this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
  }

  async _updateNumber() {
    if (this._isNumberUpdating || this.closing || this.isDestroyed) return;
    this._isNumberUpdating = true;
    try {
      const newNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      await this.dataManager.updateNumber(newNumber);
      const numberMsg = JSON.stringify(["currentNumber", newNumber]);
      this._refreshRoomClients(true);
      for (const [room, clients] of this.roomClients) {
        if (clients && clients.size > 0) this._broadcastToRoom(room, numberMsg);
      }
    } catch(e) { await this.dataManager.restoreFromStorage(); } finally { this._isNumberUpdating = false; }
  }

  async _cleanupOrphanUsers() {
    try {
      const webSockets = this._getActiveWebSockets();
      const connectedUsers = new Set();
      for (const ws of webSockets) {
        try {
          const uname = ws._cachedUsername || ws.username || ws.deserializeAttachment()?.username;
          if (uname && ws.readyState === 1 && !ws._closing) connectedUsers.add(uname);
        } catch(e) {}
      }
      const userSeatData = this._userSeatDataCache;
      let changed = false;
      for (const [username, seatInfo] of Object.entries(userSeatData)) {
        if (!connectedUsers.has(username)) {
          if (!seatInfo.isMulti) {
            await this.dataManager.forceCleanupUser(username);
            changed = true;
          } else {
            const roomsData = this._roomsDataCache;
            let hasSeat = false;
            for (const [roomName, roomData] of Object.entries(roomsData)) {
              if (!roomData || !roomData.seats) continue;
              for (const [seat, data] of Object.entries(roomData.seats)) {
                if (data && data.namauser === username) { hasSeat = true; break; }
              }
              if (hasSeat) break;
            }
            if (!hasSeat) { await this.dataManager.forceCleanupUser(username); changed = true; }
          }
        }
      }
      if (changed) { this._refreshRoomClients(true); await this._updateUserCounts(); }
    } catch(e) {}
  }

  async _updateWebSocketRoom(ws, roomName, username, seat, isMulti = false) {
    if (!ws || !roomName || !username) return false;
    try {
      const seatInfo = {
        room: roomName, seat: seat, isMulti: isMulti,
        multiRoom: isMulti ? roomName : null, multiSeat: isMulti ? seat : null
      };
      ws.serializeAttachment({
        username: username, room: roomName, seat: seat,
        isMulti: isMulti, multiRoom: isMulti ? roomName : null,
        multiSeat: isMulti ? seat : null, seatInfo: seatInfo
      });
      ws._cachedUsername = username; ws._cachedRoom = roomName;
      ws.username = username; ws.idtarget = username;
      ws.room = roomName; ws.roomname = roomName;
      ws._isMulti = isMulti; ws._multiRoom = isMulti ? roomName : null;
      ws._multiSeat = isMulti ? seat : null; ws._closing = false;
      this.dataManager.cache.userSeatData[username] = seatInfo;
      this.dataManager.cache.onlineUsers.add(username);
      await this.dataManager._atomicSave({
        userSeatData: this.dataManager.cache.userSeatData,
        onlineUsers: Array.from(this.dataManager.cache.onlineUsers)
      });
      this._refreshRoomClients(true);
      return true;
    } catch(e) { return false; }
  }

  async _handleEventInternal(ws, data) {
    try {
      if (!ws || !data || !data[0]) return;
      const [evt, ...args] = data;
      switch(evt) {
        case "resetServer": {
          await this.dataManager.resetAllData();
          this._refreshRoomClients(true);
          this.safeSend(ws, ["resetResult", { success: true }]);
          break;
        }
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        case "setIdTarget2": {
          const username = args[0]; const isNewUser = args[1];
          await this._handleSetId(ws, username, isNewUser);
          break;
        }
        case "joinRoom":
          await this._handleJoin(ws, args[0]);
          break;
        case "multiJoin": {
          const multiUsername = args[0]; const multiRoomname = args[1];
          if (!multiUsername || !multiRoomname) {
            this.safeSend(ws, ["multiJoinError", "Username dan room harus diisi"]);
            break;
          }
          if (!ROOMS_SET.has(multiRoomname)) {
            this.safeSend(ws, ["multiJoinError", "Room tidak valid"]);
            break;
          }
          await this.dataManager.forceCleanupUser(multiUsername);
          let roomsData = this._roomsDataCache;
          if (!roomsData[multiRoomname]) {
            roomsData[multiRoomname] = { seats: {}, points: {}, muted: false, number: 1 };
            await this.dataManager._atomicSave({ roomsData });
          }
          let seat = null;
          const roomData = roomsData[multiRoomname];
          if (Object.keys(roomData.seats).length >= C.MAX_SEATS) {
            this.safeSend(ws, ["multiJoinError", "Room penuh"]);
            break;
          }
          for (let s = 1; s <= C.MAX_SEATS; s++) {
            if (!roomData.seats[s]) { seat = s; break; }
          }
          if (!seat) {
            this.safeSend(ws, ["multiJoinError", "Tidak ada kursi tersedia"]);
            break;
          }
          await this.dataManager.addUserToRoom(multiUsername, multiRoomname, seat, true);
          await this._updateWebSocketRoom(ws, multiRoomname, multiUsername, seat, true);
          const webSockets = this._getActiveWebSockets();
          for (const wsKey of webSockets) {
            if (wsKey === ws) continue;
            try {
              const uname = wsKey._cachedUsername || wsKey.username || wsKey.deserializeAttachment()?.username;
              if (uname === multiUsername && wsKey.readyState === 1) {
                await this._updateWebSocketRoom(wsKey, multiRoomname, multiUsername, seat, true);
              }
            } catch(e) {}
          }
          this._refreshRoomClients(true);
          this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
          this.broadcast(multiRoomname, ["roomUserCount", multiRoomname, Object.keys(roomData.seats).length]);
          break;
        }
        case "setActiveMulti": {
          const targetUsername = args[0];
          if (!targetUsername) {
            this.safeSend(ws, ["activeChangedMultiError", "Username tidak boleh kosong"]);
            break;
          }
          let userSeat = this._userSeatDataCache[targetUsername];
          if (!userSeat) {
            const roomsData = this._roomsDataCache;
            for (const [roomName, roomData] of Object.entries(roomsData)) {
              if (!roomData || !roomData.seats) continue;
              for (const [seat, data] of Object.entries(roomData.seats)) {
                if (data && data.namauser === targetUsername) {
                  userSeat = { room: roomName, seat: parseInt(seat), isMulti: true };
                  break;
                }
              }
              if (userSeat) break;
            }
          }
          if (!userSeat) {
            this.safeSend(ws, ["activeChangedMultiError", `User ${targetUsername} tidak ditemukan`]);
            break;
          }
          const roomName = userSeat.room;
          const seatNumber = userSeat.seat;
          const seatInfo = {
            room: roomName, seat: seatNumber, isMulti: true,
            multiRoom: roomName, multiSeat: seatNumber
          };
          this.dataManager.cache.userSeatData[targetUsername] = seatInfo;
          await this.dataManager._atomicSave({ userSeatData: this.dataManager.cache.userSeatData });
          const webSockets = this._getActiveWebSockets();
          for (const wsKey of webSockets) {
            try {
              const uname = wsKey._cachedUsername || wsKey.username || wsKey.deserializeAttachment()?.username;
              if (uname === targetUsername && wsKey.readyState === 1) {
                await this._updateWebSocketRoom(wsKey, roomName, targetUsername, seatNumber, true);
                this.safeSend(wsKey, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
              }
            } catch(e) {}
          }
          this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
          this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
          this._refreshRoomClients(true);
          break;
        }
        case "exitMulti": {
          const targetUsername = args[0];
          if (!targetUsername) {
            this.safeSend(ws, ["exitMultiError", "Username tidak boleh kosong"]);
            break;
          }
          await this.dataManager.forceCleanupUser(targetUsername);
          const webSockets = this._getActiveWebSockets();
          for (const wsKey of webSockets) {
            try {
              const uname = wsKey._cachedUsername || wsKey.username || wsKey.deserializeAttachment()?.username;
              if (uname === targetUsername) {
                wsKey._isMulti = false; wsKey._multiRoom = null; wsKey._multiSeat = null;
                wsKey._cachedRoom = null; wsKey.room = null; wsKey.roomname = null; wsKey.idtarget = null;
                wsKey.serializeAttachment({ username: targetUsername, isMulti: false });
                this.safeSend(wsKey, ["exitMultiSuccess", targetUsername, null, null]);
              }
            } catch(e) {}
          }
          this._refreshRoomClients(true);
          this.safeSend(ws, ["exitMultiSuccess", targetUsername, null, null]);
          break;
        }
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          try {
            const roomsData = JSON.parse(JSON.stringify(this._roomsDataCache));
            if (roomsData[kursiRoom] && roomsData[kursiRoom].seats[kursiSeat]) {
              roomsData[kursiRoom].seats[kursiSeat] = {
                noimageUrl: kursiNoimg || "", namauser: kursiName || "",
                color: kursiColor || "", itembawah: kursiBawah || 0,
                itematas: kursiAtas || 0, vip: kursiVip || 0, viptanda: kursiVt || 0
              };
              await this.dataManager._atomicSave({ roomsData });
              const updatedSeat = roomsData[kursiRoom].seats[kursiSeat];
              if (updatedSeat) this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
            }
          } catch(e) {}
          break;
        }
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;
          let isValid = false;
          const roomData = this._roomsDataCache[chatRoom];
          if (roomData && roomData.seats) {
            for (const [seat, data] of Object.entries(roomData.seats)) {
              if (data && data.namauser === chatUser) { isValid = true; break; }
            }
          }
          if (!isValid) { this.safeSend(ws, ["chatError", "Anda tidak berada di room ini"]); break; }
          this._broadcastToRoom(chatRoom, JSON.stringify(["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]));
          break;
        }
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (!pointRoom || typeof pointSeat !== 'number') break;
          try {
            const roomsData = JSON.parse(JSON.stringify(this._roomsDataCache));
            if (roomsData[pointRoom] && roomsData[pointRoom].seats[pointSeat]) {
              if (!roomsData[pointRoom].points) roomsData[pointRoom].points = {};
              roomsData[pointRoom].points[pointSeat] = { x: pointX || 0, y: pointY || 0, fast: !!pointFast };
              await this.dataManager._atomicSave({ roomsData });
              this._broadcastToRoom(pointRoom, JSON.stringify(["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]));
            }
          } catch(e) {}
          break;
        }
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          const roomData = this._roomsDataCache[removeRoom];
          let username = null;
          if (roomData && roomData.seats && roomData.seats[removeSeat]) {
            username = roomData.seats[removeSeat].namauser;
          }
          if (username) {
            await this.dataManager.forceCleanupUser(username);
            const webSockets = this._getActiveWebSockets();
            for (const wsKey of webSockets) {
              try {
                const uname = wsKey._cachedUsername || wsKey.username;
                if (uname === username) {
                  wsKey._isMulti = false; wsKey._multiRoom = null; wsKey._multiSeat = null;
                  wsKey._cachedRoom = null; wsKey.room = null; wsKey.roomname = null; wsKey.idtarget = null;
                  wsKey.serializeAttachment({});
                }
              } catch(e) {}
            }
          }
          break;
        }
        case "private": {
          const [privTarget, privNoimg, privMsg, privSender] = args;
          if (privTarget && privMsg) {
            const userSeat = this._userSeatDataCache[privTarget];
            if (userSeat) {
              const webSockets = this._getActiveWebSockets();
              for (const wsKey of webSockets) {
                try {
                  const uname = wsKey._cachedUsername || wsKey.username || wsKey.deserializeAttachment()?.username;
                  if (uname === privTarget && wsKey.readyState === 1) {
                    this.safeSend(wsKey, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
                  }
                } catch(e) {}
              }
            }
            this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
          }
          break;
        }
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (giftRoom && ROOMS_SET.has(giftRoom)) {
            const receiverSeat = this._userSeatDataCache[giftReceiver];
            if (!receiverSeat || receiverSeat.room !== giftRoom) break;
            this._broadcastToRoom(giftRoom, JSON.stringify(["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]));
          }
          break;
        }
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (rollRoom && ROOMS_SET.has(rollRoom)) {
            const userSeat = this._userSeatDataCache[rollUser];
            if (!userSeat || userSeat.room !== rollRoom) break;
            this._broadcastToRoom(rollRoom, JSON.stringify(["rollangakBroadcast", rollRoom, rollUser, rollAngka]));
          }
          break;
        }
        case "sendnotif": {
          try {
            const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
            if (notifTarget && notifMsg) {
              const webSockets = this._getActiveWebSockets();
              for (const wsKey of webSockets) {
                try {
                  const uname = wsKey._cachedUsername || wsKey.username || wsKey.deserializeAttachment()?.username;
                  if (uname === notifTarget && wsKey.readyState === 1) {
                    this.safeSend(wsKey, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
                    break;
                  }
                } catch(e) {}
              }
            }
          } catch(e) {}
          break;
        }
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          const userSeat = this._userSeatDataCache[onlineTarget];
          if (userSeat) {
            const webSockets = this._getActiveWebSockets();
            for (const wsKey of webSockets) {
              try {
                const uname = wsKey._cachedUsername || wsKey.username || wsKey.deserializeAttachment()?.username;
                if (uname === onlineTarget && wsKey.readyState === 1) { isOnline = true; break; }
              } catch(e) {}
            }
          }
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
        case "getOnlineUsers": {
          const users = [];
          for (const [username, seatInfo] of Object.entries(this._userSeatDataCache)) {
            if (seatInfo) {
              const webSockets = this._getActiveWebSockets();
              for (const wsKey of webSockets) {
                try {
                  const uname = wsKey._cachedUsername || wsKey.username || wsKey.deserializeAttachment()?.username;
                  if (uname === username && wsKey.readyState === 1) { users.push(username); break; }
                } catch(e) {}
              }
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) {
            const roomData = this._roomsDataCache[room];
            counts[room] = roomData?.seats ? Object.keys(roomData.seats).length : 0;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        case "getRoomUserCount": {
          const roomName = args[0];
          if (roomName && ROOMS_SET.has(roomName)) {
            const roomData = this._roomsDataCache[roomName];
            const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
            this.safeSend(ws, ["roomUserCount", roomName, count]);
          }
          break;
        }
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (!muteRoom || !ROOMS_SET.has(muteRoom)) break;
          try {
            const roomsData = JSON.parse(JSON.stringify(this._roomsDataCache));
            if (!roomsData[muteRoom]) roomsData[muteRoom] = { seats: {}, points: {}, muted: false, number: 1 };
            roomsData[muteRoom].muted = !!muteVal;
            await this.dataManager._atomicSave({ roomsData });
            this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
            this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
          } catch(e) {}
          break;
        }
        case "modwarning": {
          const modRoom = args[0];
          if (modRoom && ROOMS_SET.has(modRoom)) this.broadcast(modRoom, ["modwarning", modRoom]);
          break;
        }
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (getMuteRoom && ROOMS_SET.has(getMuteRoom)) {
            const roomData = this._roomsDataCache[getMuteRoom];
            this.safeSend(ws, ["muteTypeResponse", roomData?.muted || false, getMuteRoom]);
          }
          break;
        }
        case "onDestroy": break;
        default: this.safeSend(ws, ["error", `Unknown event: ${evt}`]);
      }
    } catch(e) {}
  }

  async resetAllData() {
    const timestamp = Date.now();
    try {
      await this.dataManager.resetAllData();
      const resetMessage = JSON.stringify(["serverReset", "Server di-reset pada: " + new Date(timestamp).toLocaleString()]);
      const webSockets = this._getActiveWebSockets();
      for (const ws of webSockets) {
        try { if (ws.readyState === 1) ws.send(resetMessage); } catch(e) {}
      }
      for (const ws of webSockets) {
        try { if (ws.readyState === 1) ws.close(1000, "Server reset - " + timestamp); } catch(e) {}
      }
      this._refreshRoomClients(true);
      if (!this.closing && !this.isDestroyed) await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      await this.ctx.storage.put("lastReset", timestamp);
      return { success: true, message: "Semua data berhasil direset", timestamp: timestamp };
    } catch(e) {
      return { success: false, error: e.message, timestamp: timestamp };
    }
  }

  async fetch(req) {
    if (this.closing || this.isDestroyed) return new Response("Shutting down", { status: 503 });
    try {
      const url = new URL(req.url);
      if (url.pathname === "/reset" && req.method === "POST") {
        const result = await this.resetAllData();
        return new Response(JSON.stringify(result), { status: result.success ? 200 : 500, headers: { "Content-Type": "application/json" } });
      }
      if (url.pathname === "/status") {
        const webSockets = this._getActiveWebSockets();
        const status = {
          activeConnections: webSockets.length, rooms: this._userCounts,
          totalUsers: this._onlineUsers.size, currentNumber: this.currentNumber,
          isClosing: this.closing, isDestroyed: this.isDestroyed, uptime: Date.now() - this._startTime
        };
        return new Response(JSON.stringify(status), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server - No Ping/Pong", { status: 200, headers: { "Cache-Control": "no-cache" } });
      }
      const currentConnections = this._getActiveWebSockets().length;
      if (currentConnections >= C.MAX_GLOBAL_CONNECTIONS) return new Response("Server full", { status: 503 });
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      try { this.ctx.acceptWebSocket(server); } catch(e) {
        return new Response("WebSocket acceptance failed", { status: 500 });
      }
      server.username = null; server.room = null; server.roomname = null;
      server.idtarget = null; server._closing = false;
      server._wsId = Date.now() + Math.random();
      server._isMulti = false; server._multiRoom = null; server._multiSeat = null;
      server._cachedUsername = null; server._cachedRoom = null;
      server.serializeAttachment({});
      this._refreshRoomClients(true);
      return new Response(null, { status: 101, webSocket: client });
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true; this.isDestroyed = true;
    await this.dataManager._validateConsistency();
    const webSockets = this._getActiveWebSockets();
    for (const ws of webSockets) {
      if (ws?.readyState === 1) {
        try { ws.send(JSON.stringify(["serverShutdown", "Server shutting down"])); } catch(e) {}
        try { ws.close(1000, "Shutdown"); } catch(e) {}
      }
    }
    this.roomClients.clear();
    this.dataManager.cache.onlineUsers.clear();
    try { await this.ctx.storage.deleteAlarm(); } catch(e) {}
  }
}

export default ChatServer;