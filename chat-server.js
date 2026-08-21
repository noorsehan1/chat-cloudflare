// ==================== CHAT-SERVER.JS ====================
// VERSION: 10.0.0 - WITH ALLOWCONCURRENCY & NO HIBERNATION
// READY FOR DEPLOYMENT

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

// ==================== CHAT SERVER ====================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.ctx = state;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    
    this.wsSet = new Set();
    this.roomClients = new Map();
    
    for (const room of ROOMS) {
      this.roomClients.set(room, new Set());
    }
    
    this._roomsDataCache = {};
    this._userSeatDataCache = {};
    this.currentNumber = 1;
    
    this._onlineUsers = new Set();
    this._userCounts = {};
    for (const room of ROOMS) {
      this._userCounts[room] = 0;
    }
    
    this._isNumberUpdating = false;
    
    this._restoreAllState();
  }

  // ============ SERIALIZE / DESERIALIZE ============
  
  async serializeState() {
    return {
      roomsData: this._roomsDataCache,
      userSeatData: this._userSeatDataCache,
      currentNumber: this.currentNumber,
      userCounts: this._userCounts,
      onlineUsers: Array.from(this._onlineUsers),
    };
  }

  async deserializeState(data) {
    if (!data) return;
    this._roomsDataCache = data.roomsData || {};
    this._userSeatDataCache = data.userSeatData || {};
    this.currentNumber = data.currentNumber || 1;
    this._userCounts = data.userCounts || {};
    this._onlineUsers = new Set(data.onlineUsers || []);
  }

  // ============ CORE OPERATIONS ============

  async _updateCacheAndStorage(roomsData, userSeatData, currentNumber) {
    try {
      if (roomsData !== undefined) this._roomsDataCache = roomsData;
      if (userSeatData !== undefined) this._userSeatDataCache = userSeatData;
      if (currentNumber !== undefined) this.currentNumber = currentNumber;
      
      const updates = {};
      if (roomsData !== undefined) updates.roomsData = roomsData;
      if (userSeatData !== undefined) updates.userSeatData = userSeatData;
      if (currentNumber !== undefined) updates.currentNumber = currentNumber;
      
      await this.ctx.storage.put(updates);
    } catch(e) {
      await this._rollbackCache();
    }
  }

  async _rollbackCache() {
    try {
      const storage = await this.ctx.storage.get(["roomsData", "userSeatData", "currentNumber"]);
      if (storage.roomsData !== undefined) this._roomsDataCache = storage.roomsData;
      if (storage.userSeatData !== undefined) this._userSeatDataCache = storage.userSeatData;
      if (storage.currentNumber !== undefined) this.currentNumber = storage.currentNumber;
    } catch(e) {}
  }

  async _updateUserCounts() {
    try {
      const newCounts = {};
      let totalUsers = 0;
      
      for (const room of ROOMS) {
        const roomData = this._roomsDataCache[room];
        const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
        newCounts[room] = count;
        totalUsers += count;
      }
      
      this._userCounts = newCounts;
      this._onlineUsers.clear();
      for (const [username] of Object.entries(this._userSeatDataCache)) {
        this._onlineUsers.add(username);
      }
      
      await this.ctx.storage.put({
        userCounts: this._userCounts,
        onlineUsers: Array.from(this._onlineUsers)
      });
      
      return { counts: newCounts, total: totalUsers };
    } catch(e) {
      return { counts: this._userCounts, total: this._onlineUsers.size };
    }
  }

  async _loadFromStorage() {
    try {
      if (Object.keys(this._roomsDataCache).length === 0 && 
          Object.keys(this._userSeatDataCache).length === 0) {
        const roomsData = await this.ctx.storage.get("roomsData") || {};
        const userSeatData = await this.ctx.storage.get("userSeatData") || {};
        const currentNumber = await this.ctx.storage.get("currentNumber") || 1;
        const userCounts = await this.ctx.storage.get("userCounts") || {};
        const onlineUsers = await this.ctx.storage.get("onlineUsers") || [];
        
        this._roomsDataCache = roomsData;
        this._userSeatDataCache = userSeatData;
        this.currentNumber = currentNumber;
        this._userCounts = userCounts;
        this._onlineUsers = new Set(onlineUsers);
      }
      
      return {
        roomsData: this._roomsDataCache,
        userSeatData: this._userSeatDataCache,
        currentNumber: this.currentNumber,
        userCounts: this._userCounts,
        onlineUsers: Array.from(this._onlineUsers)
      };
    } catch(e) {
      return { roomsData: {}, userSeatData: {}, currentNumber: 1, userCounts: {}, onlineUsers: [] };
    }
  }

  async _getRoomData(roomName) {
    return this._roomsDataCache[roomName] || null;
  }

  async _getUserSeat(username) {
    return this._userSeatDataCache[username] || null;
  }

  async _updateRoomData(roomName, updater) {
    if (!this._roomsDataCache[roomName]) {
      this._roomsDataCache[roomName] = { seats: {}, points: {}, muted: false, number: 1 };
    }
    updater(this._roomsDataCache[roomName]);
    await this.ctx.storage.put("roomsData", this._roomsDataCache);
    await this._updateUserCounts();
    return this._roomsDataCache[roomName];
  }

  async _updateUserSeat(username, updater) {
    if (!this._userSeatDataCache[username]) {
      this._userSeatDataCache[username] = {};
    }
    updater(this._userSeatDataCache[username]);
    
    if (Object.keys(this._userSeatDataCache[username]).length === 0) {
      delete this._userSeatDataCache[username];
    }
    
    await this.ctx.storage.put("userSeatData", this._userSeatDataCache);
    
    if (this._userSeatDataCache[username]) {
      this._onlineUsers.add(username);
    } else {
      this._onlineUsers.delete(username);
    }
    await this.ctx.storage.put("onlineUsers", Array.from(this._onlineUsers));
    await this._updateUserCounts();
    
    return this._userSeatDataCache[username];
  }

  async _deleteUserSeat(username) {
    delete this._userSeatDataCache[username];
    await this.ctx.storage.put("userSeatData", this._userSeatDataCache);
    this._onlineUsers.delete(username);
    await this.ctx.storage.put("onlineUsers", Array.from(this._onlineUsers));
    await this._updateUserCounts();
  }

  async _deleteRoomIfEmpty(roomName) {
    const roomData = this._roomsDataCache[roomName];
    if (!roomData) return;
    
    const hasSeats = roomData.seats && Object.keys(roomData.seats).length > 0;
    const hasPoints = roomData.points && Object.keys(roomData.points).length > 0;
    
    if (!hasSeats && !hasPoints) {
      delete this._roomsDataCache[roomName];
      await this.ctx.storage.put("roomsData", this._roomsDataCache);
      await this._updateUserCounts();
    }
  }

  // ============ USER MANAGEMENT ============

  async _isUserInAnyRoom(username) {
    if (!username) return null;
    
    const seatInfo = this._userSeatDataCache[username];
    if (seatInfo && seatInfo.room) {
      const roomData = this._roomsDataCache[seatInfo.room];
      if (roomData && roomData.seats) {
        for (const [seat, data] of Object.entries(roomData.seats)) {
          if (data && data.namauser === username) {
            return { room: seatInfo.room, seat: parseInt(seat), isMulti: seatInfo.isMulti || false };
          }
        }
      }
      delete this._userSeatDataCache[username];
      await this.ctx.storage.put("userSeatData", this._userSeatDataCache);
      this._onlineUsers.delete(username);
      await this.ctx.storage.put("onlineUsers", Array.from(this._onlineUsers));
    }
    
    for (const [roomName, roomData] of Object.entries(this._roomsDataCache)) {
      if (!roomData || !roomData.seats) continue;
      for (const [seat, data] of Object.entries(roomData.seats)) {
        if (data && data.namauser === username) {
          this._userSeatDataCache[username] = { room: roomName, seat: parseInt(seat), isMulti: false };
          await this.ctx.storage.put("userSeatData", this._userSeatDataCache);
          this._onlineUsers.add(username);
          await this.ctx.storage.put("onlineUsers", Array.from(this._onlineUsers));
          return { room: roomName, seat: parseInt(seat), isMulti: false };
        }
      }
    }
    
    return null;
  }

  async _removeUserFromRoom(username, roomName) {
    if (!username || !roomName) return false;
    
    const roomData = this._roomsDataCache[roomName];
    if (!roomData || !roomData.seats) return false;
    
    let seat = null;
    for (const [s, data] of Object.entries(roomData.seats)) {
      if (data && data.namauser === username) {
        seat = parseInt(s);
        break;
      }
    }
    
    if (!seat) return false;
    
    delete roomData.seats[seat];
    if (roomData.points) {
      delete roomData.points[seat];
    }
    delete this._userSeatDataCache[username];
    this._onlineUsers.delete(username);
    
    await this.ctx.storage.put({
      roomsData: this._roomsDataCache,
      userSeatData: this._userSeatDataCache,
      onlineUsers: Array.from(this._onlineUsers)
    });
    
    await this._updateUserCounts();
    
    this.broadcast(roomName, ["removeKursi", roomName, seat]);
    await this.updateRoomCount(roomName);
    await this._deleteRoomIfEmpty(roomName);
    
    return true;
  }

  async _updateKursi(roomName, seat, data) {
    const roomData = this._roomsDataCache[roomName];
    if (!roomData || !roomData.seats || !roomData.seats[seat]) return false;
    
    roomData.seats[seat] = {
      noimageUrl: data.noimageUrl || "",
      namauser: data.namauser || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0
    };
    
    await this.ctx.storage.put("roomsData", this._roomsDataCache);
    return true;
  }

  async _updatePoint(roomName, seat, x, y, fast) {
    const roomData = this._roomsDataCache[roomName];
    if (!roomData || !roomData.seats || !roomData.seats[seat]) return false;
    
    if (!roomData.points) roomData.points = {};
    roomData.points[seat] = { x: x || 0, y: y || 0, fast: !!fast };
    
    await this.ctx.storage.put("roomsData", this._roomsDataCache);
    return true;
  }

  // ============ JOIN HANDLING ============

  async _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
      return false;
    }
    
    const username = ws.username;
    const existing = await this._isUserInAnyRoom(username);
    
    if (existing && existing.room !== roomName) {
      await this._removeUserFromRoom(username, existing.room);
    }
    
    let roomData = this._roomsDataCache[roomName];
    if (!roomData) {
      roomData = { seats: {}, points: {}, muted: false, number: 1 };
      this._roomsDataCache[roomName] = roomData;
    }
    
    let seat = null;
    for (const [s, data] of Object.entries(roomData.seats)) {
      if (data && data.namauser === username) {
        seat = parseInt(s);
        break;
      }
    }
    
    if (!seat) {
      const seatCount = Object.keys(roomData.seats).length;
      if (seatCount >= C.MAX_SEATS) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      
      for (let s = 1; s <= C.MAX_SEATS; s++) {
        if (!roomData.seats[s]) {
          seat = s;
          break;
        }
      }
      
      if (!seat) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      
      roomData.seats[seat] = {
        noimageUrl: "",
        namauser: username,
        color: "",
        itembawah: 0,
        itematas: 0,
        vip: 0,
        viptanda: 0
      };
      
      await this.ctx.storage.put("roomsData", this._roomsDataCache);
    }
    
    const seatInfo = { room: roomName, seat, isMulti: false };
    this._userSeatDataCache[username] = seatInfo;
    await this.ctx.storage.put("userSeatData", this._userSeatDataCache);
    
    this._onlineUsers.add(username);
    await this.ctx.storage.put("onlineUsers", Array.from(this._onlineUsers));
    await this._updateUserCounts();
    
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = username;
    ws._isMulti = false;
    ws._multiRoom = null;
    ws._multiSeat = null;
    
    ws.serializeAttachment({
      username: username,
      seatInfo: seatInfo,
      isMulti: false
    });
    
    for (const [otherRoom, clients] of this.roomClients) {
      if (otherRoom !== roomName && clients) {
        clients.delete(ws);
      }
    }
    const roomClients = this.roomClients.get(roomName);
    if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
    
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomData.muted || false, roomName]);
    
    const count = Object.keys(roomData.seats).length;
    this.safeSend(ws, ["roomUserCount", roomName, count]);
    this.broadcast(roomName, ["roomUserCount", roomName, count]);
    
    setTimeout(() => {
      try {
        if (ws && ws.readyState === 1) {
          this.sendAllStateTo(ws, roomName, true);
        }
      } catch(e) {}
    }, 1000);
    
    return true;
  }

  // ============ CLEANUP ============

  async _cleanupUserOnDisconnect(ws) {
    try {
      const username = ws.username;
      if (!username) return;
      
      const isMulti = ws._isMulti || false;
      
      if (isMulti) {
        const roomName = ws.room || ws.roomname;
        if (roomName) {
          const roomClients = this.roomClients.get(roomName);
          if (roomClients) roomClients.delete(ws);
        }
        this.wsSet.delete(ws);
        
        let hasOtherConnection = false;
        for (const wsKey of this.wsSet) {
          if (wsKey.username === username && wsKey.readyState === 1) {
            hasOtherConnection = true;
            break;
          }
        }
        if (!hasOtherConnection) {
          this._onlineUsers.delete(username);
          await this.ctx.storage.put("onlineUsers", Array.from(this._onlineUsers));
          await this._updateUserCounts();
        }
        return;
      }
      
      const roomName = ws.room || ws.roomname;
      
      if (roomName) {
        await this._removeUserFromRoom(username, roomName);
      } else {
        const userSeat = this._userSeatDataCache[username];
        if (userSeat && userSeat.room) {
          await this._removeUserFromRoom(username, userSeat.room);
        } else {
          await this._deleteUserSeat(username);
        }
      }
      
      const targetRoom = roomName || this._userSeatDataCache[username]?.room;
      if (targetRoom) {
        const roomClients = this.roomClients.get(targetRoom);
        if (roomClients) {
          roomClients.delete(ws);
        }
      }
      
      this.wsSet.delete(ws);
      
    } catch(e) {}
  }

  cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    try {
      const username = ws.username;
      const room = ws.room;
      
      if (room) {
        try { this.roomClients.get(room)?.delete(ws); } catch(e) {}
      }
      
      try { this.wsSet.delete(ws); } catch(e) {}
    } catch(e) {} finally {
      ws._cleaning = false;
      try { if (ws && ws.readyState === 1) ws.close(1000, "Cleanup"); } catch(e) {}
    }
  }

  // ============ BROADCAST ============

  _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed || !room) return;
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const clientArray = Array.from(clients);
    const toRemove = new Set();
    
    for (const ws of clientArray) {
      if (!ws) { toRemove.add(ws); continue; }
      
      const wsRoom = ws.room || ws.roomname;
      if (wsRoom !== room) {
        toRemove.add(ws);
        continue;
      }
      
      try {
        if (ws.readyState === 1 && !ws._closing) {
          ws.send(msgStr);
        } else {
          toRemove.add(ws);
        }
      } catch(e) { toRemove.add(ws); }
    }
    
    if (toRemove.size > 0) {
      for (const ws of toRemove) {
        try {
          clients.delete(ws);
          if (ws) this.cleanup(ws);
        } catch(e) {}
      }
    }
  }

  broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try { this._broadcastToRoom(room, JSON.stringify(msg)); } catch(e) {}
  }

  safeSend(ws, msg) {
    if (!ws) return false;
    try {
      if (ws.readyState !== 1 || ws._closing || this.closing || this.isDestroyed) {
        return false;
      }
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      this.cleanup(ws);
      return false;
    }
  }

  // ============ STATE MANAGEMENT ============

  async updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      const roomData = this._roomsDataCache[room];
      if (!roomData || !roomData.seats) return 0;
      const count = Object.keys(roomData.seats).length;
      
      this._userCounts[room] = count;
      await this.ctx.storage.put("userCounts", this._userCounts);
      
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { return 0; }
  }

  async sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    try {
      if (ws.readyState !== 1 || ws._closing) return;
    } catch(e) { return; }
    
    const roomData = this._roomsDataCache[room];
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
          if (Object.keys(filtered).length > 0) {
            this.safeSend(ws, ["allUpdateKursiList", room, filtered]);
          }
        } else {
          this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
        }
      }
      
      if (allPoints && Object.keys(allPoints).length > 0) {
        let filteredPoints = Object.entries(allPoints).map(([seat, point]) => ({
          seat: parseInt(seat),
          x: point.x,
          y: point.y,
          fast: point.fast ? 1 : 0
        }));
        
        if (excludeSelf && selfSeat) {
          filteredPoints = filteredPoints.filter(p => p.seat !== selfSeat);
        }
        
        if (filteredPoints.length > 0) {
          this.safeSend(ws, ["allPointsList", room, filteredPoints]);
        }
      }
      
    } catch(e) {}
  }

  // ============ ALARM / NUMBER UPDATER ============

  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    await this._updateNumber();
    this._cleanupDeadConnections();
    await this._cleanupStorage();
    
    this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
  }

  async _updateNumber() {
    if (this._isNumberUpdating || this.closing || this.isDestroyed) return;
    this._isNumberUpdating = true;
    try {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      await this.ctx.storage.put("currentNumber", this.currentNumber);
      
      const storage = await this._loadFromStorage();
      const roomsData = storage.roomsData || {};
      let changed = false;
      
      for (const [roomName, roomData] of Object.entries(roomsData)) {
        if (roomData) {
          roomData.number = this.currentNumber;
          changed = true;
        }
      }
      
      if (changed) {
        await this.ctx.storage.put("roomsData", roomsData);
      }
      
      const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const [room, clients] of this.roomClients) {
        if (clients && clients.size > 0) {
          this._broadcastToRoom(room, numberMsg);
        }
      }
      
    } catch(e) {
      const storage = await this.ctx.storage.get(["currentNumber", "roomsData"]);
      if (storage.currentNumber !== undefined) {
        this.currentNumber = storage.currentNumber;
      }
      if (storage.roomsData !== undefined) {
        this._roomsDataCache = storage.roomsData;
      }
    } finally {
      this._isNumberUpdating = false;
    }
  }

  async _cleanupStorage() {
    try {
      const storage = await this._loadFromStorage();
      const roomsData = storage.roomsData || {};
      const userSeatData = storage.userSeatData || {};
      
      let changed = false;
      
      for (const [username, seatInfo] of Object.entries(userSeatData)) {
        if (!seatInfo || !seatInfo.room) {
          delete userSeatData[username];
          changed = true;
          continue;
        }
        
        if (seatInfo.isMulti) {
          const roomData = roomsData[seatInfo.room];
          if (!roomData || !roomData.seats || !roomData.seats[seatInfo.seat]) {
            delete userSeatData[username];
            changed = true;
          }
          continue;
        }
        
        let isConnected = false;
        for (const ws of this.wsSet) {
          if (ws.username === username && ws.readyState === 1) {
            isConnected = true;
            break;
          }
        }
        
        if (!isConnected) {
          delete userSeatData[username];
          changed = true;
        }
      }
      
      for (const [roomName, roomData] of Object.entries(roomsData)) {
        const hasSeats = roomData.seats && Object.keys(roomData.seats).length > 0;
        const hasPoints = roomData.points && Object.keys(roomData.points).length > 0;
        
        if (!hasSeats && !hasPoints) {
          delete roomsData[roomName];
          changed = true;
        }
      }
      
      if (changed) {
        await this._saveToStorage(roomsData, userSeatData, storage.currentNumber);
      }
      
    } catch(e) {}
  }

  async _saveToStorage(roomsData, userSeatData, currentNumber) {
    try {
      if (roomsData !== undefined) {
        await this.ctx.storage.put("roomsData", roomsData);
      }
      if (userSeatData !== undefined) {
        await this.ctx.storage.put("userSeatData", userSeatData);
      }
      if (currentNumber !== undefined) {
        await this.ctx.storage.put("currentNumber", currentNumber);
      }
    } catch(e) {}
  }

  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      for (const ws of this.wsSet) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(ws);
        }
      }
      for (const ws of toRemove) {
        this.cleanup(ws);
      }
    } catch(e) {}
  }

  // ============ RESET ALL DATA ============

  async resetAllData() {
    const timestamp = Date.now();
    
    try {
      this._roomsDataCache = {};
      this._userSeatDataCache = {};
      this.currentNumber = 1;
      this._onlineUsers.clear();
      this._userCounts = {};
      for (const room of ROOMS) {
        this._userCounts[room] = 0;
      }
      
      await this.ctx.storage.delete("roomsData");
      await this.ctx.storage.delete("userSeatData");
      await this.ctx.storage.delete("currentNumber");
      await this.ctx.storage.delete("userCounts");
      await this.ctx.storage.delete("onlineUsers");
      
      const resetMessage = JSON.stringify(["serverReset", "Server di-reset pada: " + new Date(timestamp).toLocaleString()]);
      for (const ws of this.wsSet) {
        try {
          if (ws.readyState === 1) {
            ws.send(resetMessage);
          }
        } catch(e) {}
      }
      
      const wsCopy = Array.from(this.wsSet);
      for (const ws of wsCopy) {
        try {
          if (ws.readyState === 1) {
            ws.close(1000, "Server reset - " + timestamp);
          }
        } catch(e) {}
        try {
          this.cleanup(ws);
        } catch(e) {}
      }
      
      this.wsSet.clear();
      this.roomClients.clear();
      
      for (const room of ROOMS) {
        this.roomClients.set(room, new Set());
      }
      
      if (!this.closing && !this.isDestroyed) {
        await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      }
      
      await this.ctx.storage.put("lastReset", timestamp);
      
      return {
        success: true,
        message: "Semua data berhasil direset",
        timestamp: timestamp,
        resetTime: new Date(timestamp).toLocaleString()
      };
      
    } catch(e) {
      return {
        success: false,
        error: e.message,
        timestamp: timestamp
      };
    }
  }

  // ============ WEBSOCKET EVENT HANDLERS ============

  async webSocketMessage(ws, msg) {
    if (!ws || ws._closing || this.closing || this.isDestroyed) return;
    try { await this.handleMessage(ws, msg); } catch(e) {}
  }

  async webSocketClose(ws) { 
    if (!ws) return;
    try {
      await this._cleanupUserOnDisconnect(ws);
      this.cleanup(ws);
    } catch(e) {}
  }

  async webSocketError(ws) { 
    if (!ws) return;
    try {
      await this._cleanupUserOnDisconnect(ws);
      this.cleanup(ws);
    } catch(e) {}
  }

  // ============ HANDLE SET ID ============

  async _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    
    if (ws.readyState !== 1) {
      try { this.cleanup(ws); } catch(e) {}
      return;
    }
    
    if (!isNewUser) {
      const existing = await this._isUserInAnyRoom(username);
      if (existing && existing.isMulti) {
        return;
      }
    }
    
    const existing = await this._isUserInAnyRoom(username);
    if (existing) {
      await this._removeUserFromRoom(username, existing.room);
    }
    
    ws.username = username;
    ws.idtarget = username;
    ws.room = null;
    ws.roomname = null;
    ws._closing = false;
    ws._isMulti = false;
    ws._multiRoom = null;
    ws._multiSeat = null;
    
    ws.serializeAttachment({ username: username });
    
    if (!this.wsSet.has(ws)) this.wsSet.add(ws);
    
    if (isNewUser) { 
      this.safeSend(ws, ["joinroomawal"]); 
    } else { 
      this.safeSend(ws, ["needJoinRoom"]); 
    }
  }

  // ============ HANDLE MESSAGE ============

  async handleMessage(ws, raw) {
    if (!ws) return;
    try {
      if (ws.readyState !== 1 || ws._closing || this.closing || this.isDestroyed) {
        return;
      }
    } catch(e) { return; }
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
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
    } catch(e) {}
  }

  // ============ EVENT HANDLER INTERNAL ============

  async _handleEventInternal(ws, data) {
    try {
      if (!ws || !data || !data[0]) return;
      const [evt, ...args] = data;
      
      switch(evt) {
        case "resetServer": {
          const result = await this.resetAllData();
          this.safeSend(ws, ["resetResult", result]);
          break;
        }
        
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        
        case "setIdTarget2": {
          const username = args[0];
          const isNewUser = args[1];
          
          if (isNewUser === false) {
            const existing = await this._isUserInAnyRoom(username);
            if (existing && existing.isMulti) {
              break;
            }
          }
          
          await this._handleSetId(ws, username, isNewUser);
          break;
        }
        
        case "joinRoom":
          await this._handleJoin(ws, args[0]);
          break;
        
        case "multiJoin": {
          const multiUsername = args[0];
          const multiRoomname = args[1];
          if (!multiUsername || !multiRoomname) break;
          
          const existing = await this._isUserInAnyRoom(multiUsername);
          if (existing) {
            await this._removeUserFromRoom(multiUsername, existing.room);
          }
          
          let roomData = this._roomsDataCache[multiRoomname];
          if (!roomData) {
            roomData = { seats: {}, points: {}, muted: false, number: 1 };
            this._roomsDataCache[multiRoomname] = roomData;
          }
          
          let seat = null;
          const seatCount = Object.keys(roomData.seats).length;
          if (seatCount >= C.MAX_SEATS) break;
          
          for (let s = 1; s <= C.MAX_SEATS; s++) {
            if (!roomData.seats[s]) {
              seat = s;
              break;
            }
          }
          
          if (!seat) break;
          
          roomData.seats[seat] = {
            noimageUrl: "",
            namauser: multiUsername,
            color: "",
            itembawah: 0,
            itematas: 0,
            vip: 0,
            viptanda: 0
          };
          
          await this.ctx.storage.put("roomsData", this._roomsDataCache);
          
          const seatInfo = { room: multiRoomname, seat, isMulti: true, multiRoom: multiRoomname, multiSeat: seat };
          this._userSeatDataCache[multiUsername] = seatInfo;
          await this.ctx.storage.put("userSeatData", this._userSeatDataCache);
          
          this._onlineUsers.add(multiUsername);
          await this.ctx.storage.put("onlineUsers", Array.from(this._onlineUsers));
          await this._updateUserCounts();
          
          ws.serializeAttachment({
            username: multiUsername,
            seatInfo: seatInfo,
            isMulti: true,
            multiRoom: multiRoomname,
            multiSeat: seat
          });
          
          ws.username = multiUsername;
          ws.idtarget = multiUsername;
          ws.room = multiRoomname;
          ws.roomname = multiRoomname;
          ws._isMulti = true;
          ws._multiRoom = multiRoomname;
          ws._multiSeat = seat;
          
          for (const [otherRoom, clients] of this.roomClients) {
            if (otherRoom !== multiRoomname && clients) {
              clients.delete(ws);
            }
          }
          const roomClients = this.roomClients.get(multiRoomname);
          if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
          
          if (!this.wsSet.has(ws)) this.wsSet.add(ws);
          
          this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
          this.broadcast(multiRoomname, ["roomUserCount", multiRoomname, Object.keys(roomData.seats).length]);
          
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          
          let userSeat = this._userSeatDataCache[targetUsername];
          if (!userSeat) {
            this.safeSend(ws, ["activeChangedMultiError", "User not found"]);
            break;
          }
          
          const roomName = userSeat.room;
          const seatNumber = userSeat.seat;
          
          this._userSeatDataCache[targetUsername] = {
            room: roomName,
            seat: seatNumber,
            isMulti: true,
            multiRoom: roomName,
            multiSeat: seatNumber
          };
          await this.ctx.storage.put("userSeatData", this._userSeatDataCache);
          
          for (const [otherRoom, clients] of this.roomClients) {
            if (otherRoom !== roomName && clients) {
              clients.delete(ws);
            }
          }
          const roomClients = this.roomClients.get(roomName);
          if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
          
          ws.username = targetUsername;
          ws.idtarget = targetUsername;
          ws.room = roomName;
          ws.roomname = roomName;
          ws._isMulti = true;
          ws._multiRoom = roomName;
          ws._multiSeat = seatNumber;
          
          ws.serializeAttachment({
            username: targetUsername,
            seatInfo: { room: roomName, seat: seatNumber, isMulti: true },
            isMulti: true,
            multiRoom: roomName,
            multiSeat: seatNumber
          });
          
          if (!this.wsSet.has(ws)) this.wsSet.add(ws);
          
          this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
          this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
          
          break;
        }
        
        case "exitMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          
          try {
            let userSeat = this._userSeatDataCache[targetUsername];
            
            if (!userSeat) {
              for (const [roomName, roomData] of Object.entries(this._roomsDataCache)) {
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
              await this._deleteUserSeat(targetUsername);
              this.safeSend(ws, ["exitMultiSuccess", targetUsername, null, null]);
              break;
            }
            
            const roomName = userSeat.room;
            const seatNumber = userSeat.seat;
            
            await this._removeUserFromRoom(targetUsername, roomName);
            await this._deleteUserSeat(targetUsername);
            
            for (const wsKey of this.wsSet) {
              if (wsKey.username === targetUsername) {
                if (wsKey.room) {
                  const rc = this.roomClients.get(wsKey.room);
                  if (rc) rc.delete(wsKey);
                }
                wsKey._isMulti = false;
                wsKey._multiRoom = null;
                wsKey._multiSeat = null;
                try {
                  wsKey.serializeAttachment({});
                  wsKey.username = null;
                  wsKey.room = null;
                  wsKey.roomname = null;
                  wsKey.idtarget = null;
                } catch(e) {}
              }
            }
            
            this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
            await this.updateRoomCount(roomName);
            await this._deleteRoomIfEmpty(roomName);
            
            this.safeSend(ws, ["exitMultiSuccess", targetUsername, roomName, seatNumber]);
            
          } catch(e) {
            this.safeSend(ws, ["exitMultiError", e.message]);
          }
          break;
        }
        
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          
          const updated = await this._updateKursi(kursiRoom, kursiSeat, {
            noimageUrl: kursiNoimg || "",
            namauser: kursiName || "",
            color: kursiColor || "",
            itembawah: kursiBawah || 0,
            itematas: kursiAtas || 0,
            vip: kursiVip || 0,
            viptanda: kursiVt || 0
          });
          
          if (updated) {
            const roomData = this._roomsDataCache[kursiRoom];
            const updatedSeat = roomData?.seats?.[kursiSeat];
            if (updatedSeat) {
              this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
            }
          }
          break;
        }
        
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;
          
          const userSeat = this._userSeatDataCache[chatUser];
          if (!userSeat || userSeat.room !== chatRoom) {
            break;
          }
          
          this._broadcastToRoom(chatRoom, JSON.stringify(["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]));
          break;
        }
        
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (!pointRoom || typeof pointSeat !== 'number') break;
          
          const updated = await this._updatePoint(pointRoom, pointSeat, pointX, pointY, pointFast === 1);
          if (updated) {
            this._broadcastToRoom(pointRoom, JSON.stringify(["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]));
          }
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
            await this._removeUserFromRoom(username, removeRoom);
          }
          break;
        }
        
        case "private": {
          const [privTarget, privNoimg, privMsg, privSender] = args;
          if (privTarget && privMsg) {
            const userSeat = this._userSeatDataCache[privTarget];
            if (userSeat) {
              for (const wsKey of this.wsSet) {
                if (wsKey.username === privTarget && wsKey.readyState === 1) {
                  this.safeSend(wsKey, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
                }
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
              for (const wsKey of this.wsSet) {
                if (wsKey.username === notifTarget && wsKey.readyState === 1) {
                  this.safeSend(wsKey, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
                  break;
                }
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
            if (userSeat.isMulti) {
              isOnline = true;
            } else {
              for (const wsKey of this.wsSet) {
                if (wsKey.username === onlineTarget && wsKey.readyState === 1 && !wsKey._isMulti) {
                  isOnline = true;
                  break;
                }
              }
            }
          }
          
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const [username, seatInfo] of Object.entries(this._userSeatDataCache)) {
            if (seatInfo) {
              if (seatInfo.isMulti) {
                users.push(username);
              } else {
                for (const wsKey of this.wsSet) {
                  if (wsKey.username === username && wsKey.readyState === 1 && !wsKey._isMulti) {
                    users.push(username);
                    break;
                  }
                }
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
          
          const roomData = this._roomsDataCache[muteRoom];
          if (roomData) {
            roomData.muted = !!muteVal;
            await this.ctx.storage.put("roomsData", this._roomsDataCache);
          }
          
          this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
          break;
        }

        case "modwarning": {
          const modRoom = args[0];
          if (modRoom && ROOMS_SET.has(modRoom)) {
            this.broadcast(modRoom, ["modwarning", modRoom]);
          }
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
        
        case "onDestroy":
          this.cleanup(ws);
          break;
        
        default:
          this.safeSend(ws, ["error", `Unknown event: ${evt}`]);
          break;
      }
    } catch(e) {}
  }

  // ============ RESTORE STATE ============

  async _restoreAllState() {
    try {
      const roomsData = await this.ctx.storage.get("roomsData") || {};
      const userSeatData = await this.ctx.storage.get("userSeatData") || {};
      const currentNumber = await this.ctx.storage.get("currentNumber") || 1;
      const userCounts = await this.ctx.storage.get("userCounts") || {};
      const onlineUsers = await this.ctx.storage.get("onlineUsers") || [];
      
      this._roomsDataCache = roomsData;
      this._userSeatDataCache = userSeatData;
      this.currentNumber = currentNumber;
      this._userCounts = userCounts;
      this._onlineUsers = new Set(onlineUsers);
      
      for (const [username, seatInfo] of Object.entries(this._userSeatDataCache)) {
        if (!seatInfo || !seatInfo.room) {
          delete this._userSeatDataCache[username];
          this._onlineUsers.delete(username);
          continue;
        }
        const roomData = this._roomsDataCache[seatInfo.room];
        if (!roomData || !roomData.seats || !roomData.seats[seatInfo.seat]) {
          delete this._userSeatDataCache[username];
          this._onlineUsers.delete(username);
        }
      }
      
      // Restore WebSocket connections from Durable Object state
      const webSockets = this.ctx.getWebSockets();
      for (const ws of webSockets) {
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username) {
            const userSeat = this._userSeatDataCache[attachment.username];
            if (userSeat) {
              const isMulti = attachment.isMulti || userSeat.isMulti || false;
              const roomName = userSeat.room;
              const seatNumber = userSeat.seat;
              
              ws.username = attachment.username;
              ws.room = roomName;
              ws.roomname = roomName;
              ws.idtarget = attachment.username;
              ws._closing = false;
              ws._isMulti = isMulti;
              ws._multiRoom = roomName;
              ws._multiSeat = seatNumber;
              
              ws.serializeAttachment({
                username: attachment.username,
                seatInfo: userSeat,
                isMulti: isMulti,
                multiRoom: roomName,
                multiSeat: seatNumber
              });
              
              const roomClients = this.roomClients.get(roomName);
              if (roomClients) roomClients.add(ws);
              
              this.wsSet.add(ws);
              this._onlineUsers.add(attachment.username);
            }
          }
        } catch(e) {}
      }
      
      await this._updateUserCounts();
      
      await this.ctx.storage.put({
        roomsData: this._roomsDataCache,
        userSeatData: this._userSeatDataCache,
        userCounts: this._userCounts,
        onlineUsers: Array.from(this._onlineUsers)
      });
      
      if (!this.closing && !this.isDestroyed) {
        await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      }
      
    } catch(e) {}
  }

  // ============ FETCH ============

  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    try {
      const url = new URL(req.url);
      
      if (url.pathname === "/reset" && req.method === "POST") {
        const result = await this.resetAllData();
        return new Response(JSON.stringify(result), {
          status: result.success ? 200 : 500,
          headers: { "Content-Type": "application/json" }
        });
      }
      
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          uptime: Date.now() - this._startTime,
          connections: this.wsSet.size,
          rooms: Object.keys(this._roomsDataCache).length,
          onlineUsers: this._onlineUsers.size,
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server", { 
          status: 200,
          headers: { "Cache-Control": "no-cache" }
        });
      }
      
      if (this.wsSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server full", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      // 🔥 KUNCI UTAMA: accept dengan allowConcurrency true
      try { 
        this.ctx.acceptWebSocket(server, {
          allowConcurrency: true  // Biar bisa terima pesan sambil proses state
        });
      } 
      catch(e) { 
        return new Response("WebSocket acceptance failed", { status: 500 }); 
      }
      
      server.username = null;
      server.room = null;
      server.roomname = null;
      server.idtarget = null;
      server._closing = false;
      server._wsId = Date.now() + Math.random();
      server._isMulti = false;
      server._multiRoom = null;
      server._multiSeat = null;
      
      server.serializeAttachment({});
      
      if (!this.wsSet.has(server)) this.wsSet.add(server);
      
      return new Response(null, { 
        status: 101, 
        webSocket: client
      });
      
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ============ DESTROY ============

  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    await this._cleanupStorage();
    
    const wsCopy = Array.from(this.wsSet);
    for (const ws of wsCopy) {
      if (ws?.readyState === 1) {
        try { ws.send(JSON.stringify(["serverShutdown", "Server shutting down"])); } catch(e) {}
        try { ws.close(1000, "Shutdown"); } catch(e) {}
      }
      try { this.cleanup(ws); } catch(e) {}
    }
    
    this.wsSet.clear();
    this.roomClients.clear();
    this._onlineUsers.clear();
  }
}

export default ChatServer;
