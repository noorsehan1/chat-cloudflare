// ==================== CHAT-SERVER.JS ====================
// VERSION: 8.0.6 - MULTI PERSISTENT STATUS

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_INTERVAL_MS: 900000,
  MAX_NUMBER: 6,
  LOCK_TIMEOUT: 5000,
  USER_JOIN_LOCK_TIMEOUT: 10000,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.ctx = state;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    this._restored = false;
    
    this.wsSet = new Set();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.wsActiveMulti = new Map();
    
    this._joinLocks = new Map();
    this._kursiLocks = new Map();
    this._userJoinLock = new Map();
    
    this.currentNumber = 1;
    this._isNumberUpdating = false;
    
    this._storageCache = {
      roomsData: {},
      userSeatData: {},
      currentNumber: 1
    };
    this._cacheInitialized = false;
    this._cacheLoading = false;
    
    for (const room of ROOMS) {
      this.roomClients.set(room, new Set());
    }
    
    this._restoreAllState().then(() => {
      this._restored = true;
    }).catch(() => {
      this._restored = true;
    });
  }

  async _ensureCacheInitialized() {
    if (this._cacheInitialized && this._storageCache && 
        this._storageCache.roomsData && 
        Object.keys(this._storageCache.roomsData).length > 0) {
      return this._storageCache;
    }
    
    if (this._cacheLoading) {
      let waitCount = 0;
      while (this._cacheLoading && waitCount < 50) {
        await new Promise(resolve => setTimeout(resolve, 100));
        waitCount++;
      }
      return this._storageCache;
    }
    
    this._cacheLoading = true;
    try {
      await this._loadFromStorage();
    } catch(e) {
      // Silent
    } finally {
      this._cacheLoading = false;
    }
    
    return this._storageCache;
  }

  async _loadFromStorage() {
    try {
      const roomsData = await this.ctx.storage.get("roomsData") || {};
      const userSeatData = await this.ctx.storage.get("userSeatData") || {};
      const currentNumber = await this.ctx.storage.get("currentNumber") || 1;
      
      this._storageCache = { roomsData, userSeatData, currentNumber };
      this._cacheInitialized = true;
      this.currentNumber = currentNumber;
      
      return this._storageCache;
    } catch(e) {
      return { roomsData: {}, userSeatData: {}, currentNumber: 1 };
    }
  }

  _getCache() {
    return this._storageCache;
  }

  async _updateCacheAndStorage(updates) {
    try {
      if (updates.roomsData !== undefined) {
        this._storageCache.roomsData = updates.roomsData;
      }
      if (updates.userSeatData !== undefined) {
        this._storageCache.userSeatData = updates.userSeatData;
      }
      if (updates.currentNumber !== undefined) {
        this._storageCache.currentNumber = updates.currentNumber;
        this.currentNumber = updates.currentNumber;
      }
      
      if (updates.roomsData !== undefined) {
        await this.ctx.storage.put("roomsData", updates.roomsData);
      }
      if (updates.userSeatData !== undefined) {
        await this.ctx.storage.put("userSeatData", updates.userSeatData);
      }
      if (updates.currentNumber !== undefined) {
        await this.ctx.storage.put("currentNumber", updates.currentNumber);
      }
      
      this._cacheInitialized = true;
      
      return this._storageCache;
      
    } catch(e) {
      return this._storageCache;
    }
  }

  async _saveToStorage(roomsData, userSeatData, currentNumber) {
    const updates = {};
    if (roomsData !== undefined) updates.roomsData = roomsData;
    if (userSeatData !== undefined) updates.userSeatData = userSeatData;
    if (currentNumber !== undefined) updates.currentNumber = currentNumber;
    
    return await this._updateCacheAndStorage(updates);
  }

  async _getRoomData(roomName) {
    await this._ensureCacheInitialized();
    const cache = this._storageCache;
    if (cache && cache.roomsData) {
      return cache.roomsData[roomName] || null;
    }
    return null;
  }

  async _getUserSeat(username) {
    await this._ensureCacheInitialized();
    const cache = this._storageCache;
    if (cache && cache.userSeatData) {
      return cache.userSeatData[username] || null;
    }
    return null;
  }

  async _updateRoomData(roomName, updater) {
    await this._ensureCacheInitialized();
    
    const roomsData = this._storageCache.roomsData || {};
    
    if (!roomsData[roomName]) {
      roomsData[roomName] = { seats: {}, points: {}, muted: false, number: 1 };
    }
    
    updater(roomsData[roomName]);
    
    await this._updateCacheAndStorage({ roomsData: roomsData });
    
    return roomsData[roomName];
  }

  async _updateUserSeat(username, updater) {
    await this._ensureCacheInitialized();
    
    const userSeatData = this._storageCache.userSeatData || {};
    
    if (!userSeatData[username]) {
      userSeatData[username] = {};
    }
    
    updater(userSeatData[username]);
    
    if (Object.keys(userSeatData[username]).length === 0) {
      delete userSeatData[username];
    }
    
    await this._updateCacheAndStorage({ userSeatData: userSeatData });
    
    return userSeatData[username];
  }

  async _deleteUserSeat(username) {
    await this._ensureCacheInitialized();
    
    const userSeatData = this._storageCache.userSeatData || {};
    delete userSeatData[username];
    
    await this._updateCacheAndStorage({ userSeatData: userSeatData });
  }

  async _deleteRoomIfEmpty(roomName) {
    await this._ensureCacheInitialized();
    
    const roomData = this._storageCache.roomsData[roomName];
    if (!roomData) return;
    
    const hasSeats = roomData.seats && Object.keys(roomData.seats).length > 0;
    const hasPoints = roomData.points && Object.keys(roomData.points).length > 0;
    
    if (!hasSeats && !hasPoints) {
      const roomsData = this._storageCache.roomsData || {};
      delete roomsData[roomName];
      
      await this._updateCacheAndStorage({ roomsData: roomsData });
    }
  }

  async _isUserInAnyRoom(username) {
    if (!username) return null;
    
    await this._ensureCacheInitialized();
    
    const userSeatData = this._storageCache.userSeatData || {};
    const roomsData = this._storageCache.roomsData || {};
    
    const seatInfo = userSeatData[username];
    if (seatInfo && seatInfo.room) {
      const roomData = roomsData[seatInfo.room];
      if (roomData && roomData.seats) {
        for (const [seat, data] of Object.entries(roomData.seats)) {
          if (data && data.namauser === username) {
            return { room: seatInfo.room, seat: parseInt(seat) };
          }
        }
      }
      delete userSeatData[username];
      await this._updateCacheAndStorage({ userSeatData: userSeatData });
    }
    
    for (const [roomName, roomData] of Object.entries(roomsData)) {
      if (!roomData || !roomData.seats) continue;
      for (const [seat, data] of Object.entries(roomData.seats)) {
        if (data && data.namauser === username) {
          userSeatData[username] = { room: roomName, seat: parseInt(seat) };
          await this._updateCacheAndStorage({ userSeatData: userSeatData });
          return { room: roomName, seat: parseInt(seat) };
        }
      }
    }
    
    return null;
  }

  async _validateUserInRoom(username, roomName) {
    if (!username || !roomName) return false;
    
    const userSeat = await this._getUserSeat(username);
    if (!userSeat) return false;
    
    return userSeat.room === roomName;
  }

  async _validateWsInRoom(ws, roomName) {
    if (!ws || !roomName) return false;
    
    const wsRoom = ws.room || ws.roomname;
    return wsRoom === roomName;
  }

  async _validateUserAndWsInRoom(ws, username, roomName) {
    if (!ws || !username || !roomName) return false;
    
    const userSeat = await this._getUserSeat(username);
    if (!userSeat || userSeat.room !== roomName) return false;
    
    const wsRoom = ws.room || ws.roomname;
    if (wsRoom !== roomName) return false;
    
    return true;
  }

  async _removeUserFromRoom(username, roomName) {
    if (!username || !roomName) return false;
    
    await this._ensureCacheInitialized();
    
    const roomData = this._storageCache.roomsData[roomName];
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
    
    await this._updateRoomData(roomName, (data) => {
      data.seats = roomData.seats;
      data.points = roomData.points || {};
    });
    
    await this._deleteUserSeat(username);
    
    this.broadcast(roomName, ["removeKursi", roomName, seat]);
    await this.updateRoomCount(roomName);
    
    await this._deleteRoomIfEmpty(roomName);
    
    return true;
  }

  async _updateKursi(roomName, seat, data) {
    if (!roomName || !ROOMS_SET.has(roomName)) {
      return { success: false, error: 'Invalid room' };
    }
    
    if (typeof seat !== 'number' || seat < 1 || seat > C.MAX_SEATS) {
      return { success: false, error: 'Invalid seat number' };
    }
    
    if (!data || !data.namauser) {
      return { success: false, error: 'Username is required' };
    }
    
    await this._ensureCacheInitialized();
    
    const roomData = this._storageCache.roomsData[roomName];
    if (!roomData || !roomData.seats || !roomData.seats[seat]) {
      return { success: false, error: 'Seat not found' };
    }
    
    const currentSeatData = roomData.seats[seat];
    if (currentSeatData.namauser !== data.namauser) {
      return { success: false, error: 'You do not own this seat' };
    }
    
    roomData.seats[seat] = {
      noimageUrl: data.noimageUrl || currentSeatData.noimageUrl || "",
      namauser: data.namauser || currentSeatData.namauser || "",
      color: data.color || currentSeatData.color || "",
      itembawah: typeof data.itembawah === 'number' ? data.itembawah : (parseInt(data.itembawah) || 0),
      itematas: typeof data.itematas === 'number' ? data.itematas : (parseInt(data.itematas) || 0),
      vip: typeof data.vip === 'number' ? data.vip : (parseInt(data.vip) || 0),
      viptanda: typeof data.viptanda === 'number' ? data.viptanda : (parseInt(data.viptanda) || 0)
    };
    
    await this._updateRoomData(roomName, (d) => {
      d.seats = roomData.seats;
    });
    
    return { success: true, data: roomData.seats[seat] };
  }

  async _updatePoint(roomName, seat, x, y, fast) {
    await this._ensureCacheInitialized();
    
    const roomData = this._storageCache.roomsData[roomName];
    if (!roomData || !roomData.seats || !roomData.seats[seat]) return false;
    
    if (!roomData.points) roomData.points = {};
    
    roomData.points[seat] = { x: x || 0, y: y || 0, fast: !!fast };
    
    await this._updateRoomData(roomName, (d) => {
      d.points = roomData.points;
    });
    
    return true;
  }

  async _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
      return false;
    }
    
    const username = ws.username;
    const lockKey = `join_user_${username}`;
    
    if (this._userJoinLock.has(lockKey)) {
      const lockTime = this._userJoinLock.get(lockKey);
      if (Date.now() - lockTime < C.USER_JOIN_LOCK_TIMEOUT) {
        this.safeSend(ws, ["joinInProgress", "Please wait..."]);
        return false;
      } else {
        this._userJoinLock.delete(lockKey);
      }
    }
    
    this._userJoinLock.set(lockKey, Date.now());
    try { 
      return await this._joinInternal(ws, roomName, username); 
    } finally { 
      this._userJoinLock.delete(lockKey); 
    }
  }

  async _joinInternal(ws, roomName, username) {
    const existing = await this._isUserInAnyRoom(username);
    if (existing && existing.room !== roomName) {
      await this._removeUserFromRoom(username, existing.room);
    }
    
    await this._ensureCacheInitialized();
    
    let roomData = this._storageCache.roomsData[roomName];
    if (!roomData) {
      roomData = { seats: {}, points: {}, muted: false, number: 1 };
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
      
      await this._updateRoomData(roomName, (data) => {
        data.seats = roomData.seats;
        data.points = roomData.points || {};
        data.muted = roomData.muted || false;
        data.number = roomData.number || 1;
      });
    }
    
    const seatInfo = { room: roomName, seat, isMulti: false };
    await this._updateUserSeat(username, (data) => {
      Object.assign(data, seatInfo);
    });
    
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = username;
    
    ws.serializeAttachment({
      username: username,
      seatInfo: seatInfo
    });
    
    for (const [otherRoom, clients] of this.roomClients) {
      if (otherRoom !== roomName && clients) {
        clients.delete(ws);
      }
    }
    const roomClients = this.roomClients.get(roomName);
    if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
    
    this.wsActiveMulti.delete(ws);
    
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

  // ==================== MODIFIED: CLEANUP UNTUK MULTI ====================
  async _cleanupUserOnDisconnect(ws) {
    try {
      const username = ws.username;
      const roomName = ws.room || ws.roomname;
      
      if (!username) return;
      
      const isMulti = this.wsActiveMulti.has(ws);
      
      if (isMulti) {
        // ✅ MULTI: HANYA hapus koneksi, TIDAK hapus data kursi
        const connections = this.userConnections.get(username);
        if (connections) {
          connections.delete(ws);
          // JANGAN hapus userConnections meskipun size 0
          // Biarkan tetap ada untuk mempertahankan status multi
        }
        
        // Hapus dari roomClients (koneksi WebSocket)
        if (roomName) {
          const roomClients = this.roomClients.get(roomName);
          if (roomClients) roomClients.delete(ws);
        } else {
          try {
            const attachment = ws.deserializeAttachment();
            if (attachment && attachment.seatInfo && attachment.seatInfo.room) {
              const roomClients = this.roomClients.get(attachment.seatInfo.room);
              if (roomClients) roomClients.delete(ws);
            }
          } catch(e) {}
        }
        
        // HAPUS dari wsActiveMulti (koneksi WS sudah mati)
        this.wsActiveMulti.delete(ws);
        this.wsSet.delete(ws);
        
        // ✅ TETAP PERTAHANKAN data di storage (userSeatData)
        // JANGAN panggil _removeUserFromRoom atau _deleteUserSeat
        
        return;
      }
      
      // ===== USER BIASA (NON-MULTI) - TETAP SEPERTI KODE AWAL =====
      if (roomName) {
        await this._removeUserFromRoom(username, roomName);
      } else {
        const userSeat = await this._getUserSeat(username);
        if (userSeat && userSeat.room) {
          await this._removeUserFromRoom(username, userSeat.room);
        } else {
          await this._deleteUserSeat(username);
        }
      }
      
      const connections = this.userConnections.get(username);
      if (connections) {
        connections.delete(ws);
        if (connections.size === 0) {
          this.userConnections.delete(username);
        }
      }
      
      const targetRoom = roomName || (await this._getUserSeat(username))?.room;
      if (targetRoom) {
        const roomClients = this.roomClients.get(targetRoom);
        if (roomClients) {
          roomClients.delete(ws);
        }
      }
      
      this.wsActiveMulti.delete(ws);
      this.wsSet.delete(ws);
      
    } catch(e) {
      // Silent
    }
  }

  async webSocketMessage(ws, msg) {
    if (!ws || ws._closing || this.closing || this.isDestroyed) return;
    try { 
      await this.handleMessage(ws, msg); 
    } catch(e) {
      // Silent
    }
  }

  async webSocketClose(ws) { 
    if (!ws) return;
    try {
      await this._cleanupUserOnDisconnect(ws);
      this.cleanup(ws);
    } catch(e) {
      // Silent
    }
  }

  async webSocketError(ws) { 
    if (!ws) return;
    try {
      await this._cleanupUserOnDisconnect(ws);
      this.cleanup(ws);
    } catch(e) {
      // Silent
    }
  }

  broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    
    try {
      const msgStr = JSON.stringify(msg);
      const clients = this.roomClients.get(room);
      if (!clients || clients.size === 0) return;
      
      const toRemove = new Set();
      
      for (const ws of clients) {
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
    } catch(e) {
      // Silent
    }
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

  async updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      await this._ensureCacheInitialized();
      
      const roomData = this._storageCache.roomsData[room];
      if (!roomData || !roomData.seats) return 0;
      
      const count = Object.keys(roomData.seats).length;
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { return 0; }
  }

  async sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    try {
      if (ws.readyState !== 1 || ws._closing) return;
    } catch(e) { return; }
    
    await this._ensureCacheInitialized();
    
    const roomData = this._storageCache.roomsData[room];
    if (!roomData) return;
    
    try {
      const allSeats = roomData.seats || {};
      const allPoints = roomData.points || {};
      
      const userSeat = await this._getUserSeat(ws.username);
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

  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    await this._updateNumber();
    this._cleanupDeadConnections();
    this._cleanupStaleLocks();
    await this._cleanupStorage();
    await this._saveAllState();
    
    this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
  }

  async _updateNumber() {
    if (this._isNumberUpdating || this.closing || this.isDestroyed) return;
    this._isNumberUpdating = true;
    try {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      await this._updateCacheAndStorage({ currentNumber: this.currentNumber });
      
      const roomsData = this._storageCache.roomsData || {};
      let changed = false;
      
      for (const [roomName, roomData] of Object.entries(roomsData)) {
        if (roomData) {
          roomData.number = this.currentNumber;
          changed = true;
        }
      }
      
      if (changed) {
        await this._updateCacheAndStorage({ roomsData: roomsData });
      }
      
      for (const [room, clients] of this.roomClients) {
        if (clients && clients.size > 0) {
          this.broadcast(room, ["currentNumber", this.currentNumber]);
        }
      }
      
    } catch(e) {} finally {
      this._isNumberUpdating = false;
    }
  }

  async _cleanupStorage() {
    try {
      await this._ensureCacheInitialized();
      
      const roomsData = this._storageCache.roomsData || {};
      const userSeatData = this._storageCache.userSeatData || {};
      
      let changed = false;
      
      for (const [username, seatInfo] of Object.entries(userSeatData)) {
        // ✅ SKIP MULTI USER - JANGAN HAPUS
        if (seatInfo && seatInfo.isMulti === true) {
          continue;
        }
        
        if (!seatInfo || !seatInfo.room) {
          delete userSeatData[username];
          changed = true;
          continue;
        }
        
        const roomData = roomsData[seatInfo.room];
        if (!roomData || !roomData.seats || !roomData.seats[seatInfo.seat]) {
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
        await this._updateCacheAndStorage({ 
          roomsData: roomsData, 
          userSeatData: userSeatData 
        });
      }
      
    } catch(e) {
      // Silent
    }
  }

  async _saveAllState() {
    try {
      await this._ensureCacheInitialized();
      
      await this._updateCacheAndStorage({
        roomsData: this._storageCache.roomsData,
        userSeatData: this._storageCache.userSeatData,
        currentNumber: this.currentNumber
      });
    } catch(e) {
      // Silent
    }
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

  _cleanupStaleLocks() {
    try {
      const now = Date.now();
      for (const [key, time] of this._joinLocks) {
        if (now - time > C.LOCK_TIMEOUT) this._joinLocks.delete(key);
      }
      for (const [key, time] of this._kursiLocks) {
        if (now - time > C.LOCK_TIMEOUT) this._kursiLocks.delete(key);
      }
      for (const [key, time] of this._userJoinLock) {
        if (now - time > C.USER_JOIN_LOCK_TIMEOUT) this._userJoinLock.delete(key);
      }
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
      
      try {
        const activeData = this.wsActiveMulti.get(ws);
        if (activeData?.room) {
          this.roomClients.get(activeData.room)?.delete(ws);
        }
        this.wsActiveMulti.delete(ws);
      } catch(e) {}
      
      if (username) {
        try {
          const connections = this.userConnections.get(username);
          if (connections) {
            connections.delete(ws);
            if (connections.size === 0) {
              this.userConnections.delete(username);
            }
          }
        } catch(e) {}
      }
      
      try { this.wsSet.delete(ws); } catch(e) {}
    } catch(e) {} finally {
      ws._cleaning = false;
      try { if (ws && ws.readyState === 1) ws.close(1000, "Cleanup"); } catch(e) {}
    }
  }

  async _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    
    const userSeat = await this._getUserSeat(username);
    const isMultiUser = userSeat !== null && userSeat !== undefined && userSeat.isMulti === true;
    
    if (isMultiUser && isNewUser === false) {
      return;
    }
    
    if (isMultiUser && isNewUser === true) {
      await this._removeUserFromRoom(username, userSeat.room);
      await this._deleteUserSeat(username);
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
    
    ws.serializeAttachment({ username: username });
    
    let connections = this.userConnections.get(username);
    if (!connections) { 
      connections = new Set(); 
      this.userConnections.set(username, connections); 
    }
    if (!connections.has(ws)) connections.add(ws);
    if (!this.wsSet.has(ws)) this.wsSet.add(ws);
    
    this.wsActiveMulti.delete(ws);
    
    if (isNewUser) { 
      this.safeSend(ws, ["joinroomawal"]); 
    } else { 
      this.safeSend(ws, ["needJoinRoom"]); 
    }
  }

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
      
    } catch(e) {
      // Silent
    }
  }

  async _handleEventInternal(ws, data) {
    try {
      if (!ws || !data || !data[0]) return;
      const [evt, ...args] = data;
      
      await this._ensureCacheInitialized();
      
      switch(evt) {
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        
        case "setIdTarget2":
          await this._handleSetId(ws, args[0], args[1]);
          break;
        
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
          
          await this._ensureCacheInitialized();
          
          let roomData = this._storageCache.roomsData[multiRoomname];
          if (!roomData) {
            roomData = { seats: {}, points: {}, muted: false, number: 1 };
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
          
          await this._updateRoomData(multiRoomname, (data) => {
            data.seats = roomData.seats;
            data.points = roomData.points || {};
          });
          
          const seatInfo = { room: multiRoomname, seat, isMulti: true };
          await this._updateUserSeat(multiUsername, (data) => {
            Object.assign(data, seatInfo);
          });
          
          let connections = this.userConnections.get(multiUsername);
          if (!connections) connections = new Set();
          if (!connections.has(ws)) connections.add(ws);
          this.userConnections.set(multiUsername, connections);
          
          ws.serializeAttachment({
            username: multiUsername,
            seatInfo: seatInfo
          });
          
          this.wsActiveMulti.set(ws, { username: multiUsername, room: multiRoomname });
          
          for (const [otherRoom, clients] of this.roomClients) {
            if (otherRoom !== multiRoomname && clients) {
              clients.delete(ws);
            }
          }
          const roomClients = this.roomClients.get(multiRoomname);
          if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
          
          this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
          this.broadcast(multiRoomname, ["roomUserCount", multiRoomname, Object.keys(roomData.seats).length]);
          
          break;
        }
        
        // ==================== MODIFIED: EXIT MULTI ====================
        case "exitMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          
          try {
            let userSeat = await this._getUserSeat(targetUsername);
            
            if (!userSeat) {
              await this._ensureCacheInitialized();
              const roomsData = this._storageCache.roomsData;
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
            
            // ✅ PASTIKAN menghapus SEMUA data multi
            const roomName = userSeat?.room;
            const seatNumber = userSeat?.seat;
            
            // Hapus dari roomsData
            if (roomName && seatNumber) {
              await this._removeUserFromRoom(targetUsername, roomName);
            }
            
            // Hapus dari userSeatData
            await this._deleteUserSeat(targetUsername);
            
            // Hapus SEMUA koneksi WebSocket untuk user ini
            const connections = this.userConnections.get(targetUsername);
            if (connections) {
              const toRemove = Array.from(connections);
              for (const conn of toRemove) {
                // Hapus dari roomClients
                if (conn.room) {
                  const rc = this.roomClients.get(conn.room);
                  if (rc) rc.delete(conn);
                }
                if (roomName) {
                  const rc = this.roomClients.get(roomName);
                  if (rc) rc.delete(conn);
                }
                
                // Hapus dari wsActiveMulti
                this.wsActiveMulti.delete(conn);
                
                // Reset attachment
                try {
                  conn.serializeAttachment({});
                  conn.username = null;
                  conn.room = null;
                  conn.roomname = null;
                  conn.idtarget = null;
                } catch(e) {}
                
                // Kirim notifikasi
                try {
                  if (conn.readyState === 1) {
                    this.safeSend(conn, ["exitMultiForce", "You have been exited"]);
                  }
                } catch(e) {}
                
                this.wsSet.delete(conn);
              }
              
              // Hapus dari userConnections
              this.userConnections.delete(targetUsername);
            }
            
            // Hapus dari wsActiveMulti (semua entri)
            const toDelete = [];
            for (const [wsKey, data] of this.wsActiveMulti) {
              if (data && data.username === targetUsername) {
                toDelete.push(wsKey);
                if (data.room) {
                  const rc = this.roomClients.get(data.room);
                  if (rc) rc.delete(wsKey);
                }
                try {
                  wsKey.serializeAttachment({});
                  wsKey.username = null;
                  wsKey.room = null;
                  wsKey.roomname = null;
                  wsKey.idtarget = null;
                } catch(e) {}
                try {
                  if (wsKey.readyState === 1) {
                    this.safeSend(wsKey, ["exitMultiForce", "You have been exited"]);
                  }
                } catch(e) {}
                this.wsSet.delete(wsKey);
              }
            }
            for (const wsKey of toDelete) {
              this.wsActiveMulti.delete(wsKey);
            }
            
            // Broadcast perubahan
            if (roomName) {
              this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
              this.broadcast(roomName, ["userOffline", targetUsername, seatNumber]);
              await this.updateRoomCount(roomName);
              await this._deleteRoomIfEmpty(roomName);
            }
            
            this.safeSend(ws, ["exitMultiSuccess", targetUsername, roomName, seatNumber]);
            
          } catch(e) {
            // Silent
            this.safeSend(ws, ["exitMultiError", e.message]);
          }
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
          
          const userSeat = await this._getUserSeat(targetUsername);
          if (!userSeat) {
            this.safeSend(ws, ["setActiveMultiError", "User not found"]);
            break;
          }
          
          const roomName = userSeat.room;
          const seatNumber = userSeat.seat;
          
          let existingWs = null;
          for (const [wsKey, data] of this.wsActiveMulti) {
            if (data && data.username === targetUsername) {
              existingWs = wsKey;
              break;
            }
          }
          
          if (existingWs && existingWs !== ws) {
            const oldRoom = this.wsActiveMulti.get(existingWs)?.room;
            if (oldRoom) {
              const rc = this.roomClients.get(oldRoom);
              if (rc) rc.delete(existingWs);
            }
            
            const conns = this.userConnections.get(targetUsername);
            if (conns) {
              conns.delete(existingWs);
              if (conns.size === 0) {
                this.userConnections.delete(targetUsername);
              }
            }
            
            try {
              existingWs.serializeAttachment({});
              existingWs.username = null;
              existingWs.room = null;
              existingWs.roomname = null;
              existingWs.idtarget = null;
            } catch(e) {}
            
            this.wsSet.delete(existingWs);
            this.wsActiveMulti.delete(existingWs);
            
            try {
              if (existingWs.readyState === 1) {
                existingWs.send(JSON.stringify(["activeMultiReplaced", "New connection detected"]));
                existingWs.close(1000, "Replaced by new connection");
              }
            } catch(e) {}
          }
          
          this.wsActiveMulti.set(ws, { username: targetUsername, room: roomName });
          
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
          
          ws.serializeAttachment({
            username: targetUsername,
            seatInfo: userSeat
          });
          
          let connections = this.userConnections.get(targetUsername);
          if (!connections) {
            connections = new Set();
            this.userConnections.set(targetUsername, connections);
          }
          if (!connections.has(ws)) connections.add(ws);
          if (!this.wsSet.has(ws)) this.wsSet.add(ws);
          
          this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
          this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
          this.broadcast(roomName, ["userOnline", targetUsername, seatNumber]);
          
          break;
        }
        
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          
          if (!kursiRoom || typeof kursiSeat !== 'number' || kursiSeat < 1 || kursiSeat > C.MAX_SEATS) {
            this.safeSend(ws, ["updateKursiError", "Invalid room or seat number"]);
            break;
          }
          
          if (!ROOMS_SET.has(kursiRoom)) {
            this.safeSend(ws, ["updateKursiError", "Room not found"]);
            break;
          }
          
          if (!kursiName || typeof kursiName !== 'string' || kursiName.trim().length === 0) {
            this.safeSend(ws, ["updateKursiError", "Username is required"]);
            break;
          }
          
          const userSeat = await this._getUserSeat(kursiName);
          if (!userSeat || userSeat.seat !== kursiSeat || userSeat.room !== kursiRoom) {
            this.safeSend(ws, ["updateKursiError", "You do not own this seat"]);
            break;
          }
          
          const lockKey = `kursi_${kursiRoom}_${kursiSeat}`;
          if (this._kursiLocks.has(lockKey)) {
            this.safeSend(ws, ["updateKursiError", "Update in progress, please wait"]);
            break;
          }
          this._kursiLocks.set(lockKey, Date.now());
          
          try {
            const updateData = {
              noimageUrl: String(kursiNoimg || ""),
              namauser: String(kursiName || ""),
              color: String(kursiColor || ""),
              itembawah: typeof kursiBawah === 'number' ? kursiBawah : (parseInt(kursiBawah) || 0),
              itematas: typeof kursiAtas === 'number' ? kursiAtas : (parseInt(kursiAtas) || 0),
              vip: typeof kursiVip === 'number' ? kursiVip : (parseInt(kursiVip) || 0),
              viptanda: typeof kursiVt === 'number' ? kursiVt : (parseInt(kursiVt) || 0)
            };
            
            const result = await this._updateKursi(kursiRoom, kursiSeat, updateData);
            
            if (result.success) {
              this.safeSend(ws, ["updateKursiSuccess", kursiRoom, kursiSeat]);
              
              const roomData = await this._getRoomData(kursiRoom);
              const updatedSeat = roomData?.seats?.[kursiSeat];
              if (updatedSeat) {
                this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
              }
            } else {
              this.safeSend(ws, ["updateKursiError", result.error || "Update failed"]);
            }
            
          } catch(e) {
            // Silent
            this.safeSend(ws, ["updateKursiError", e.message || "Internal error"]);
          } finally {
            this._kursiLocks.delete(lockKey);
          }
          break;
        }
        
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;
          
          const userSeat = await this._getUserSeat(chatUser);
          if (!userSeat || userSeat.room !== chatRoom) {
            break;
          }
          
          const wsRoom = ws.room || ws.roomname;
          if (wsRoom !== chatRoom) {
            break;
          }
          
          this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]);
          break;
        }
        
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (!pointRoom || typeof pointSeat !== 'number') break;
          
          const userSeat = await this._getUserSeat(ws.username);
          if (!userSeat || userSeat.room !== pointRoom || userSeat.seat !== pointSeat) {
            break;
          }
          
          const updated = await this._updatePoint(pointRoom, pointSeat, pointX, pointY, pointFast === 1);
          if (updated) {
            this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]);
          }
          break;
        }
        
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (giftRoom && ROOMS_SET.has(giftRoom)) {
            const senderSeat = await this._getUserSeat(giftSender);
            const receiverSeat = await this._getUserSeat(giftReceiver);
            
            if (!senderSeat || senderSeat.room !== giftRoom) break;
            if (!receiverSeat || receiverSeat.room !== giftRoom) break;
            
            if ((ws.room || ws.roomname) !== giftRoom) break;
            
            this.broadcast(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]);
          }
          break;
        }
        
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (rollRoom && ROOMS_SET.has(rollRoom)) {
            const userSeat = await this._getUserSeat(rollUser);
            if (!userSeat || userSeat.room !== rollRoom) break;
            
            if ((ws.room || ws.roomname) !== rollRoom) break;
            
            this.broadcast(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, rollAngka]);
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          
          const userSeat = await this._getUserSeat(ws.username);
          if (!userSeat || userSeat.room !== removeRoom) break;
          
          const roomData = await this._getRoomData(removeRoom);
          let username = null;
          if (roomData && roomData.seats && roomData.seats[removeSeat]) {
            username = roomData.seats[removeSeat].namauser;
          }
          
          if (username) {
            await this._removeUserFromRoom(username, removeRoom);
          }
          break;
        }
        
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (!muteRoom || !ROOMS_SET.has(muteRoom)) break;
          
          const userSeat = await this._getUserSeat(ws.username);
          if (!userSeat || userSeat.room !== muteRoom) break;
          
          await this._updateRoomData(muteRoom, (data) => {
            data.muted = !!muteVal;
          });
          
          this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
          break;
        }
        
        case "modwarning": {
          const modRoom = args[0];
          if (modRoom && ROOMS_SET.has(modRoom)) {
            const userSeat = await this._getUserSeat(ws.username);
            if (!userSeat || userSeat.room !== modRoom) break;
            
            this.broadcast(modRoom, ["modwarning", modRoom]);
          }
          break;
        }
        
        case "private": {
          const [privTarget, privNoimg, privMsg, privSender] = args;
          if (privTarget && privMsg) {
            const targetConns = this.userConnections.get(privTarget);
            if (targetConns) {
              for (const targetWs of targetConns) {
                if (targetWs?.readyState === 1) {
                  this.safeSend(targetWs, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
                  break;
                }
              }
            }
            this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
          }
          break;
        }
        
        case "sendnotif": {
          try {
            const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
            if (notifTarget && notifMsg) {
              const targetConns = this.userConnections.get(notifTarget);
              if (targetConns) {
                for (const c of targetConns) {
                  if (c?.readyState === 1) {
                    this.safeSend(c, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
                    break;
                  }
                }
              }
            }
          } catch(e) {
            // Silent
          }
          break;
        }
        
        // ==================== MODIFIED: IS USER ONLINE ====================
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          
          const userSeat = await this._getUserSeat(onlineTarget);
          if (userSeat) {
            // ✅ MULTI ID → LANGSUNG ONLINE (tanpa cek apapun)
            if (userSeat.isMulti === true) {
              isOnline = true;
            } else {
              // User biasa → cek koneksi WebSocket
              const connections = this.userConnections.get(onlineTarget);
              if (connections) {
                for (const conn of connections) {
                  if (conn?.readyState === 1) {
                    isOnline = true;
                    break;
                  }
                }
              }
            }
          }
          
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
        
        // ==================== MODIFIED: GET ONLINE USERS ====================
        case "getOnlineUsers": {
          const users = [];
          await this._ensureCacheInitialized();
          const userSeatData = this._storageCache.userSeatData || {};
          
          for (const [username, seatInfo] of Object.entries(userSeatData)) {
            if (seatInfo) {
              let isOnline = false;
              
              // ✅ MULTI ID → LANGSUNG ONLINE (tanpa cek apapun)
              if (seatInfo.isMulti === true) {
                isOnline = true;
              } else {
                // User biasa → cek koneksi WebSocket
                const connections = this.userConnections.get(username);
                if (connections) {
                  for (const conn of connections) {
                    if (conn?.readyState === 1) {
                      isOnline = true;
                      break;
                    }
                  }
                }
              }
              
              if (isOnline) {
                users.push(username);
              }
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "getAllRoomsUserCount": {
          await this._ensureCacheInitialized();
          const counts = {};
          for (const room of ROOMS) {
            const roomData = this._storageCache.roomsData[room];
            counts[room] = roomData?.seats ? Object.keys(roomData.seats).length : 0;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = args[0];
          if (roomName && ROOMS_SET.has(roomName)) {
            await this._ensureCacheInitialized();
            const roomData = this._storageCache.roomsData[roomName];
            const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
            this.safeSend(ws, ["roomUserCount", roomName, count]);
          }
          break;
        }
        
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (getMuteRoom && ROOMS_SET.has(getMuteRoom)) {
            await this._ensureCacheInitialized();
            const roomData = this._storageCache.roomsData[getMuteRoom];
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
    } catch(e) {
      // Silent
    }
  }

  async _restoreAllState() {
    try {
      const roomsData = await this.ctx.storage.get("roomsData") || {};
      const userSeatData = await this.ctx.storage.get("userSeatData") || {};
      const currentNumber = await this.ctx.storage.get("currentNumber") || 1;
      
      this._storageCache = { roomsData, userSeatData, currentNumber };
      this._cacheInitialized = true;
      this.currentNumber = currentNumber;
      
      const webSockets = this.ctx.getWebSockets();
      for (const ws of webSockets) {
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username) {
            const userSeat = userSeatData[attachment.username];
            if (userSeat) {
              attachment.seatInfo = userSeat;
              ws.serializeAttachment(attachment);
              
              ws.username = attachment.username;
              ws.room = userSeat.room;
              ws.roomname = userSeat.room;
              ws.idtarget = attachment.username;
              ws._closing = false;
              
              const roomClients = this.roomClients.get(userSeat.room);
              if (roomClients) roomClients.add(ws);
              
              let conns = this.userConnections.get(attachment.username);
              if (!conns) conns = new Set();
              conns.add(ws);
              this.userConnections.set(attachment.username, conns);
              
              this.wsSet.add(ws);
            }
          }
        } catch(e) {}
      }
      
      if (!this.closing && !this.isDestroyed) {
        this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      }
      
    } catch(e) {
      // Silent
    }
  }

  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    await this._ensureCacheInitialized();
    
    try {
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
      const [client, server] = [pair[0], pair[1]];
      
      try { this.ctx.acceptWebSocket(server); } 
      catch(e) { return new Response("WebSocket acceptance failed", { status: 500 }); }
      
      server.username = null;
      server.room = null;
      server.roomname = null;
      server.idtarget = null;
      server._closing = false;
      server._wsId = Date.now() + Math.random();
      
      server.serializeAttachment({});
      
      if (!this.wsSet.has(server)) this.wsSet.add(server);
      
      return new Response(null, { status: 101, webSocket: client });
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ==================== MODIFIED: DESTROY ====================
  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    // ✅ JANGAN hapus storage untuk multi
    // await this._cleanupStorage(); // COMMENT
    // await this._saveAllState();   // COMMENT
    
    this._joinLocks.clear();
    this._kursiLocks.clear();
    this._userJoinLock.clear();
    
    const wsCopy = Array.from(this.wsSet);
    for (const ws of wsCopy) {
      // Cek apakah ini multi
      const isMulti = this.wsActiveMulti.has(ws);
      
      if (ws?.readyState === 1) {
        try { ws.send(JSON.stringify(["serverShutdown", "Server shutting down"])); } catch(e) {}
        try { ws.close(1000, "Shutdown"); } catch(e) {}
      }
      
      // ✅ UNTUK MULTI: JANGAN cleanup total, hanya tutup koneksi
      if (isMulti) {
        // Hapus dari set, tapi JANGAN hapus data storage
        this.wsSet.delete(ws);
        this.wsActiveMulti.delete(ws);
        // HAPUS dari roomClients
        const room = ws.room || ws.roomname;
        if (room) {
          const rc = this.roomClients.get(room);
          if (rc) rc.delete(ws);
        }
      } else {
        // User biasa: cleanup normal
        try { this.cleanup(ws); } catch(e) {}
      }
    }
    
    // ✅ JANGAN clear semuanya untuk multi
    // this.wsSet.clear();
    // this.userConnections.clear();
    // this.roomClients.clear();
    // this.wsActiveMulti.clear();
    
    // ✅ HANYA hapus yang benar-benar closed
    for (const [username, conns] of this.userConnections) {
      const toRemove = [];
      for (const conn of conns) {
        if (!conn || conn.readyState !== 1) {
          toRemove.push(conn);
        }
      }
      for (const conn of toRemove) {
        conns.delete(conn);
      }
      if (conns.size === 0) {
        // Cek apakah ini multi user
        let isMultiUser = false;
        for (const [wsKey, data] of this.wsActiveMulti) {
          if (data && data.username === username) {
            isMultiUser = true;
            break;
          }
        }
        // ✅ JANGAN hapus userConnections untuk multi
        if (!isMultiUser) {
          this.userConnections.delete(username);
        }
      }
    }
  }
}

export default ChatServer;
