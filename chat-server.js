// ==================== CHAT-SERVER.JS ====================
// VERSION: 7.0.0 - STORAGE-ONLY ARCHITECTURE
// SEMUA DATA LANGSUNG BACA/TULIS KE STORAGE
// TIDAK PAKAI MEMORY UNTUK DATA (userSeat, userRoom, rooms)

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_INTERVAL_MS: 900000, // 15 MENIT
  MAX_NUMBER: 6,
  BATCH_SIZE: 20,
  LOCK_TIMEOUT: 5000,
  MAX_EVENT_QUEUE: 100,
  MAX_PROCESS_TIME_MS: 500,
  USER_JOIN_LOCK_TIMEOUT: 10000,
  STORAGE_CACHE_TTL: 2000, // 2 detik
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
    this._restored = false;
    
    // ========== WEBSOCKET (HANYA INI DI MEMORY) ==========
    this.wsSet = new Set();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.wsActiveMulti = new Map();
    
    // ========== PROCESSING ==========
    this._processingMessages = new Set();
    this._cleaningUp = new Set();
    this._eventQueue = [];
    this._isProcessingQueue = false;
    this._pendingTimeouts = new Set();
    
    // ========== LOCKS ==========
    this._joinLocks = new Map();
    this._kursiLocks = new Map();
    this._userJoinLock = new Map();
    
    // ========== NUMBER ==========
    this.currentNumber = 1;
    this._isNumberUpdating = false;
    
    // ========== STORAGE CACHE (HANYA UNTUK PERFORMANCE) ==========
    this._storageCache = null;
    this._storageCacheTime = 0;
    this._storageCacheTTL = C.STORAGE_CACHE_TTL;
    
    // ========== INIT ROOMS ==========
    for (const room of ROOMS) {
      this.roomClients.set(room, new Set());
    }
    
    // ========== RESTORE STATE ==========
    this._restoreAllState().then(() => {
      this._restored = true;
    }).catch(() => {
      this._restored = true;
    });
  }

  // ============================================================
  // ✅ STORAGE OPERATIONS (SEMUA LANGSUNG KE STORAGE)
  // ============================================================

  // 1. LOAD DARI STORAGE (DENGAN CACHE)
  async _loadFromStorage() {
    const now = Date.now();
    if (this._storageCache && (now - this._storageCacheTime) < this._storageCacheTTL) {
      return this._storageCache;
    }
    
    try {
      const roomsData = await this.ctx.storage.get("roomsData") || {};
      const userSeatData = await this.ctx.storage.get("userSeatData") || {};
      const currentNumber = await this.ctx.storage.get("currentNumber") || 1;
      
      this._storageCache = { roomsData, userSeatData, currentNumber };
      this._storageCacheTime = now;
      
      return this._storageCache;
    } catch(e) {
      return { roomsData: {}, userSeatData: {}, currentNumber: 1 };
    }
  }

  // 2. SAVE KE STORAGE
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
      
      this._storageCache = { roomsData, userSeatData, currentNumber };
      this._storageCacheTime = Date.now();
      
    } catch(e) {
      console.error('[SAVE] Error:', e.message);
    }
  }

  // 3. GET ROOM DATA DARI STORAGE
  async _getRoomData(roomName) {
    const storage = await this._loadFromStorage();
    return storage.roomsData[roomName] || null;
  }

  // 4. GET USER SEAT DARI STORAGE
  async _getUserSeat(username) {
    const storage = await this._loadFromStorage();
    return storage.userSeatData[username] || null;
  }

  // 5. UPDATE ROOM DATA DI STORAGE
  async _updateRoomData(roomName, updater) {
    const storage = await this._loadFromStorage();
    const roomsData = storage.roomsData || {};
    
    if (!roomsData[roomName]) {
      roomsData[roomName] = { seats: {}, points: {}, muted: false, number: 1 };
    }
    
    updater(roomsData[roomName]);
    
    await this.ctx.storage.put("roomsData", roomsData);
    
    this._storageCache.roomsData = roomsData;
    this._storageCacheTime = Date.now();
    
    return roomsData[roomName];
  }

  // 6. UPDATE USER SEAT DI STORAGE
  async _updateUserSeat(username, updater) {
    const storage = await this._loadFromStorage();
    const userSeatData = storage.userSeatData || {};
    
    if (!userSeatData[username]) {
      userSeatData[username] = {};
    }
    
    updater(userSeatData[username]);
    
    if (Object.keys(userSeatData[username]).length === 0) {
      delete userSeatData[username];
    }
    
    await this.ctx.storage.put("userSeatData", userSeatData);
    
    this._storageCache.userSeatData = userSeatData;
    this._storageCacheTime = Date.now();
    
    return userSeatData[username];
  }

  // 7. DELETE USER SEAT DARI STORAGE
  async _deleteUserSeat(username) {
    const storage = await this._loadFromStorage();
    const userSeatData = storage.userSeatData || {};
    
    delete userSeatData[username];
    
    await this.ctx.storage.put("userSeatData", userSeatData);
    
    this._storageCache.userSeatData = userSeatData;
    this._storageCacheTime = Date.now();
  }

  // 8. DELETE ROOM JIKA KOSONG
  async _deleteRoomIfEmpty(roomName) {
    const roomData = await this._getRoomData(roomName);
    if (!roomData) return;
    
    const hasSeats = roomData.seats && Object.keys(roomData.seats).length > 0;
    const hasPoints = roomData.points && Object.keys(roomData.points).length > 0;
    
    if (!hasSeats && !hasPoints) {
      const storage = await this._loadFromStorage();
      const roomsData = storage.roomsData || {};
      delete roomsData[roomName];
      
      await this.ctx.storage.put("roomsData", roomsData);
      
      this._storageCache.roomsData = roomsData;
      this._storageCacheTime = Date.now();
      
      console.log(`[DELETE] Room "${roomName}" deleted from storage (empty)`);
    }
  }

  // 9. CEK USER DI ROOM LAIN (DARI STORAGE)
  async _isUserInAnyRoom(username) {
    if (!username) return null;
    
    const storage = await this._loadFromStorage();
    const userSeatData = storage.userSeatData || {};
    
    const seatInfo = userSeatData[username];
    if (seatInfo && seatInfo.room) {
      const roomData = storage.roomsData[seatInfo.room];
      if (roomData && roomData.seats) {
        for (const [seat, data] of Object.entries(roomData.seats)) {
          if (data && data.namauser === username) {
            return { room: seatInfo.room, seat: parseInt(seat) };
          }
        }
      }
      delete userSeatData[username];
      await this.ctx.storage.put("userSeatData", userSeatData);
    }
    
    for (const [roomName, roomData] of Object.entries(storage.roomsData)) {
      if (!roomData || !roomData.seats) continue;
      for (const [seat, data] of Object.entries(roomData.seats)) {
        if (data && data.namauser === username) {
          userSeatData[username] = { room: roomName, seat: parseInt(seat) };
          await this.ctx.storage.put("userSeatData", userSeatData);
          return { room: roomName, seat: parseInt(seat) };
        }
      }
    }
    
    return null;
  }

  // ============================================================
  // ✅ REMOVE USER (LANGSUNG DARI STORAGE)
  // ============================================================
  
  async _removeUserFromRoom(username, roomName) {
    if (!username || !roomName) return false;
    
    console.log(`[removeUserFromRoom] Removing "${username}" from "${roomName}"`);
    
    const roomData = await this._getRoomData(roomName);
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
    
    console.log(`[removeUserFromRoom] User "${username}" removed from "${roomName}" seat ${seat}`);
    return true;
  }

  // ============================================================
  // ✅ UPDATE KURSI (LANGSUNG KE STORAGE)
  // ============================================================
  
  async _updateKursi(roomName, seat, data) {
    const roomData = await this._getRoomData(roomName);
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
    
    await this._updateRoomData(roomName, (d) => {
      d.seats = roomData.seats;
    });
    
    return true;
  }

  // ============================================================
  // ✅ UPDATE POINT (LANGSUNG KE STORAGE)
  // ============================================================
  
  async _updatePoint(roomName, seat, x, y, fast) {
    const roomData = await this._getRoomData(roomName);
    if (!roomData || !roomData.seats || !roomData.seats[seat]) return false;
    
    if (!roomData.points) roomData.points = {};
    
    roomData.points[seat] = { x: x || 0, y: y || 0, fast: !!fast };
    
    await this._updateRoomData(roomName, (d) => {
      d.points = roomData.points;
    });
    
    return true;
  }

  // ============================================================
  // ✅ JOIN ROOM (LANGSUNG KE STORAGE)
  // ============================================================
  
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
    // 1. CEK USER DI ROOM LAIN
    const existing = await this._isUserInAnyRoom(username);
    if (existing && existing.room !== roomName) {
      console.log(`[join] User "${username}" in "${existing.room}", removing...`);
      await this._removeUserFromRoom(username, existing.room);
    }
    
    // 2. LOAD ROOM DATA
    let roomData = await this._getRoomData(roomName);
    if (!roomData) {
      roomData = { seats: {}, points: {}, muted: false, number: 1 };
    }
    
    // 3. CEK USER SUDAH DI ROOM INI
    let seat = null;
    for (const [s, data] of Object.entries(roomData.seats)) {
      if (data && data.namauser === username) {
        seat = parseInt(s);
        break;
      }
    }
    
    // 4. TAMBAHKAN JIKA BELUM
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
    
    // 5. SAVE USER SEAT
    const seatInfo = { room: roomName, seat, isMulti: false };
    await this._updateUserSeat(username, (data) => {
      Object.assign(data, seatInfo);
    });
    
    // 6. UPDATE WEBSOCKET
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = username;
    
    ws.serializeAttachment({
      username: username,
      seatInfo: seatInfo
    });
    
    // 7. UPDATE ROOM CLIENTS
    for (const [otherRoom, clients] of this.roomClients) {
      if (otherRoom !== roomName && clients) {
        clients.delete(ws);
      }
    }
    const roomClients = this.roomClients.get(roomName);
    if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
    
    this.wsActiveMulti.delete(ws);
    
    // 8. SEND RESPONSE
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomData.muted || false, roomName]);
    
    const count = Object.keys(roomData.seats).length;
    this.safeSend(ws, ["roomUserCount", roomName, count]);
    this.broadcast(roomName, ["roomUserCount", roomName, count]);
    
    // 9. SEND ALL STATE
    setTimeout(() => {
      try {
        if (ws && ws.readyState === 1) {
          this.sendAllStateTo(ws, roomName, true);
        }
      } catch(e) {}
    }, 1000);
    
    return true;
  }

  // ============================================================
  // ✅ CLEANUP USER ON DISCONNECT (WS CLOSE/ERROR)
  // ============================================================
  
  async _cleanupUserOnDisconnect(ws) {
    try {
      const username = ws.username;
      const roomName = ws.room || ws.roomname;
      
      if (!username) {
        console.log('[DISCONNECT] No username, skipping cleanup');
        return;
      }
      
      console.log(`[DISCONNECT] Cleaning up user "${username}" from room "${roomName}"`);
      
      // 1. Hapus dari storage
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
      
      // 2. Hapus dari userConnections
      const connections = this.userConnections.get(username);
      if (connections) {
        connections.delete(ws);
        if (connections.size === 0) {
          this.userConnections.delete(username);
        }
      }
      
      // 3. Hapus dari roomClients
      const targetRoom = roomName || (await this._getUserSeat(username))?.room;
      if (targetRoom) {
        const roomClients = this.roomClients.get(targetRoom);
        if (roomClients) {
          roomClients.delete(ws);
        }
      }
      
      // 4. Hapus dari wsActiveMulti
      this.wsActiveMulti.delete(ws);
      
      // 5. Hapus dari wsSet
      this.wsSet.delete(ws);
      
      console.log(`[DISCONNECT] User "${username}" fully cleaned`);
      
    } catch(e) {
      console.error('[DISCONNECT] Error:', e.message);
    }
  }

  // ============================================================
  // ✅ WEBSOCKET HANDLERS
  // ============================================================
  
  async webSocketMessage(ws, msg) {
    if (!ws || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) return;
    try { 
      await this.handleMessage(ws, msg); 
    } catch(e) {
      console.error('[WS MESSAGE] Error:', e.message);
    }
  }

  async webSocketClose(ws) { 
    if (!ws) return;
    try {
      console.log(`[WS CLOSE] WebSocket closed`);
      await this._cleanupUserOnDisconnect(ws);
      this.cleanup(ws);
    } catch(e) {
      console.error('[WS CLOSE] Error:', e.message);
    }
  }

  async webSocketError(ws) { 
    if (!ws) return;
    try {
      console.log(`[WS ERROR] WebSocket error`);
      await this._cleanupUserOnDisconnect(ws);
      this.cleanup(ws);
    } catch(e) {
      console.error('[WS ERROR] Error:', e.message);
    }
  }

  // ============================================================
  // ✅ BROADCAST
  // ============================================================
  
  _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed || !room) return;
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const clientArray = Array.from(clients);
    const toRemove = new Set();
    
    for (let i = 0; i < clientArray.length; i += C.BATCH_SIZE) {
      const batch = clientArray.slice(i, Math.min(i + C.BATCH_SIZE, clientArray.length));
      for (const ws of batch) {
        if (!ws) { toRemove.add(ws); continue; }
        
        const wsRoom = ws.room || ws.roomname;
        if (wsRoom !== room) {
          toRemove.add(ws);
          continue;
        }
        
        try {
          if (ws.readyState === 1 && !ws._closing && !this._cleaningUp.has(ws)) {
            ws.send(msgStr);
          } else {
            toRemove.add(ws);
          }
        } catch(e) { toRemove.add(ws); }
      }
    }
    
    if (toRemove.size > 0) {
      for (const ws of toRemove) {
        try {
          clients.delete(ws);
          if (ws && !this._cleaningUp.has(ws)) this.cleanup(ws);
        } catch(e) {}
      }
    }
  }

  broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try { this._broadcastToRoom(room, JSON.stringify(msg)); } catch(e) {}
  }

  // ============================================================
  // ✅ SAFE SEND
  // ============================================================
  
  safeSend(ws, msg) {
    if (!ws) return false;
    try {
      if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
        return false;
      }
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      this.cleanup(ws);
      return false;
    }
  }

  // ============================================================
  // ✅ UPDATE ROOM COUNT (DARI STORAGE)
  // ============================================================
  
  async updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      const roomData = await this._getRoomData(room);
      if (!roomData || !roomData.seats) return 0;
      const count = Object.keys(roomData.seats).length;
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { return 0; }
  }

  // ============================================================
  // ✅ SEND ALL STATE (DARI STORAGE)
  // ============================================================
  
  async sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    try {
      if (ws.readyState !== 1 || ws._closing) return;
    } catch(e) { return; }
    
    const roomData = await this._getRoomData(room);
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

  // ============================================================
  // ✅ ALARM (15 MENIT) - CLEANUP STORAGE
  // ============================================================
  
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
        this._storageCache.roomsData = roomsData;
        this._storageCacheTime = Date.now();
      }
      
      const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const [room, clients] of this.roomClients) {
        if (clients && clients.size > 0) {
          this._broadcastToRoom(room, numberMsg);
        }
      }
      
    } catch(e) {} finally {
      this._isNumberUpdating = false;
    }
  }

  // ============================================================
  // ✅ CLEANUP STORAGE
  // ============================================================
  
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
        
        const roomData = roomsData[seatInfo.room];
        if (!roomData || !roomData.seats || !roomData.seats[seatInfo.seat]) {
          delete userSeatData[username];
          changed = true;
          console.log(`[CLEANUP] Removed orphan user "${username}"`);
        }
      }
      
      for (const [roomName, roomData] of Object.entries(roomsData)) {
        const hasSeats = roomData.seats && Object.keys(roomData.seats).length > 0;
        const hasPoints = roomData.points && Object.keys(roomData.points).length > 0;
        
        if (!hasSeats && !hasPoints) {
          delete roomsData[roomName];
          changed = true;
          console.log(`[CLEANUP] Removed empty room "${roomName}"`);
        }
      }
      
      if (changed) {
        await this._saveToStorage(roomsData, userSeatData, storage.currentNumber);
      }
      
    } catch(e) {
      console.error('[CLEANUP STORAGE] Error:', e.message);
    }
  }

  // ============================================================
  // ✅ SAVE ALL STATE
  // ============================================================
  
  async _saveAllState() {
    try {
      const storage = await this._loadFromStorage();
      await this._saveToStorage(storage.roomsData, storage.userSeatData, this.currentNumber);
    } catch(e) {
      console.error('[SAVE ALL] Error:', e.message);
    }
  }

  // ============================================================
  // ✅ CLEANUP DEAD CONNECTIONS & STALE LOCKS
  // ============================================================
  
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

  // ============================================================
  // ✅ CLEANUP WEBSOCKET (MEMORY ONLY)
  // ============================================================
  
  cleanup(ws) {
    if (!ws || ws._cleaning || this._cleaningUp.has(ws)) return;
    ws._cleaning = true;
    this._cleaningUp.add(ws);
    
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
      this._cleaningUp.delete(ws);
      try { if (ws && ws.readyState === 1) ws.close(1000, "Cleanup"); } catch(e) {}
    }
  }

  // ============================================================
  // ✅ HANDLE SET ID
  // ============================================================
  
  async _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    
    if (ws.readyState !== 1) {
      try { this.cleanup(ws); } catch(e) {}
      return;
    }
    
    const existing = await this._isUserInAnyRoom(username);
    if (existing) {
      console.log(`[setId] User "${username}" in room "${existing.room}", removing...`);
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
    
    if (isNewUser) { 
      this.safeSend(ws, ["joinroomawal"]); 
    } else { 
      this.safeSend(ws, ["needJoinRoom"]); 
    }
  }

  // ============================================================
  // ✅ HANDLE MESSAGE
  // ============================================================
  
  async handleMessage(ws, raw) {
    if (!ws) return;
    try {
      if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
        return;
      }
    } catch(e) { return; }
    
    if (this._processingMessages.has(ws)) return;
    this._processingMessages.add(ws);
    
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
      
      if (this._eventQueue.length < C.MAX_EVENT_QUEUE) {
        this._eventQueue.push({ ws, data: [evt, ...args] });
        if (!this._isProcessingQueue) this._processEventQueue();
      }
    } catch(e) {} finally {
      try { this._processingMessages.delete(ws); } catch(e) {}
    }
  }

  _processEventQueue() {
    try {
      if (this._isProcessingQueue || this._eventQueue.length === 0) return;
      this._isProcessingQueue = true;
      const startTime = Date.now();
      let processed = 0;
      
      while (this._eventQueue.length > 0 && processed < 50) {
        if (Date.now() - startTime > C.MAX_PROCESS_TIME_MS) break;
        const item = this._eventQueue.shift();
        try { this._handleEventInternal(item.ws, item.data); } catch(e) {}
        processed++;
      }
      this._isProcessingQueue = false;
    } catch(e) {
      this._isProcessingQueue = false;
    }
  }

  // ============================================================
  // ✅ HANDLE EVENT INTERNAL (SEMUA PAKAI STORAGE)
  // ============================================================
  
  async _handleEventInternal(ws, data) {
    try {
      if (!ws || !data || !data[0]) return;
      const [evt, ...args] = data;
      
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
          
          let roomData = await this._getRoomData(multiRoomname);
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
        
        case "exitMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          
          try {
            console.log(`[exitMulti] Exiting user "${targetUsername}"`);
            
            // 1. CARI USER DI STORAGE
            let userSeat = await this._getUserSeat(targetUsername);
            
            // 2. FALLBACK: SCAN SEMUA ROOM
            if (!userSeat) {
              const storage = await this._loadFromStorage();
              const roomsData = storage.roomsData || {};
              for (const [roomName, roomData] of Object.entries(roomsData)) {
                if (!roomData || !roomData.seats) continue;
                for (const [seat, data] of Object.entries(roomData.seats)) {
                  if (data && data.namauser === targetUsername) {
                    userSeat = { room: roomName, seat: parseInt(seat) };
                    break;
                  }
                }
                if (userSeat) break;
              }
            }
            
            if (!userSeat) {
              console.log(`[exitMulti] User "${targetUsername}" not found anywhere`);
              await this._deleteUserSeat(targetUsername);
              this.safeSend(ws, ["exitMultiSuccess", targetUsername, null, null]);
              break;
            }
            
            const roomName = userSeat.room;
            const seatNumber = userSeat.seat;
            
            // 3. HAPUS DARI STORAGE
            await this._removeUserFromRoom(targetUsername, roomName);
            await this._deleteUserSeat(targetUsername);
            
            // 4. HAPUS DARI SEMUA WEBSOCKET
            const connections = this.userConnections.get(targetUsername);
            if (connections) {
              for (const conn of connections) {
                if (conn.room) {
                  const rc = this.roomClients.get(conn.room);
                  if (rc) rc.delete(conn);
                }
                this.wsActiveMulti.delete(conn);
                try {
                  conn.serializeAttachment({});
                  conn.username = null;
                  conn.room = null;
                  conn.roomname = null;
                  conn.idtarget = null;
                } catch(e) {}
              }
              this.userConnections.delete(targetUsername);
            }
            
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
              }
            }
            for (const wsKey of toDelete) {
              this.wsActiveMulti.delete(wsKey);
              this.wsSet.delete(wsKey);
            }
            
            // 5. BROADCAST
            this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
            await this.updateRoomCount(roomName);
            await this._deleteRoomIfEmpty(roomName);
            
            this.safeSend(ws, ["exitMultiSuccess", targetUsername, roomName, seatNumber]);
            console.log(`[exitMulti] User "${targetUsername}" successfully exited from "${roomName}" seat ${seatNumber}`);
            
          } catch(e) {
            console.error('[exitMulti] Error:', e.message);
            this.safeSend(ws, ["exitMultiError", e.message]);
          }
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
          const userSeat = await this._getUserSeat(targetUsername);
          if (!userSeat) break;
          
          const roomName = userSeat.room;
          const seatNumber = userSeat.seat;
          
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
          
          this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
          this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
          break;
        }
        
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          
          const lockKey = `kursi_${kursiRoom}_${kursiSeat}`;
          if (this._kursiLocks.has(lockKey)) break;
          this._kursiLocks.set(lockKey, Date.now());
          
          try {
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
              const roomData = await this._getRoomData(kursiRoom);
              const updatedSeat = roomData?.seats?.[kursiSeat];
              if (updatedSeat) {
                this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
              }
            }
          } finally {
            this._kursiLocks.delete(lockKey);
          }
          break;
        }
        
        // ✅ CHAT: LANGSUNG KIRIM (TANPA SAVE)
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;
          
          const userSeat = await this._getUserSeat(chatUser);
          if (!userSeat || userSeat.room !== chatRoom) {
            console.log(`[chat] User "${chatUser}" not in room "${chatRoom}"`);
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
        
        // ✅ PRIVATE: LANGSUNG KIRIM
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
        
        // ✅ GIFT: LANGSUNG KIRIM
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (giftRoom && ROOMS_SET.has(giftRoom)) {
            const receiverSeat = await this._getUserSeat(giftReceiver);
            if (!receiverSeat || receiverSeat.room !== giftRoom) break;
            this._broadcastToRoom(giftRoom, JSON.stringify(["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]));
          }
          break;
        }
        
        // ✅ ROLL: LANGSUNG KIRIM
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (rollRoom && ROOMS_SET.has(rollRoom)) {
            const userSeat = await this._getUserSeat(rollUser);
            if (!userSeat || userSeat.room !== rollRoom) break;
            this._broadcastToRoom(rollRoom, JSON.stringify(["rollangakBroadcast", rollRoom, rollUser, rollAngka]));
          }
          break;
        }
        
        // ✅ NOTIF: LANGSUNG KIRIM (SAMA SEPERTI KODE AWAL)
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
            console.error('[notif] Error:', e.message);
          }
          break;
        }
        
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          const userSeat = await this._getUserSeat(onlineTarget);
          if (userSeat) {
            const connections = this.userConnections.get(onlineTarget);
            if (connections) {
              for (const conn of connections) {
                if (conn?.readyState === 1) { isOnline = true; break; }
              }
            }
          }
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          const storage = await this._loadFromStorage();
          const userSeatData = storage.userSeatData || {};
          
          for (const [username, seatInfo] of Object.entries(userSeatData)) {
            if (seatInfo) {
              const connections = this.userConnections.get(username);
              if (connections) {
                for (const conn of connections) {
                  if (conn?.readyState === 1) {
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
          const storage = await this._loadFromStorage();
          const counts = {};
          for (const room of ROOMS) {
            const roomData = storage.roomsData[room];
            counts[room] = roomData?.seats ? Object.keys(roomData.seats).length : 0;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = args[0];
          if (roomName && ROOMS_SET.has(roomName)) {
            const roomData = await this._getRoomData(roomName);
            const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
            this.safeSend(ws, ["roomUserCount", roomName, count]);
          }
          break;
        }
        
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (!muteRoom || !ROOMS_SET.has(muteRoom)) break;
          
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
            this.broadcast(modRoom, ["modwarning", modRoom]);
          }
          break;
        }

        case "getMuteType": {
          const getMuteRoom = args[0];
          if (getMuteRoom && ROOMS_SET.has(getMuteRoom)) {
            const roomData = await this._getRoomData(getMuteRoom);
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
      console.error('[HANDLE EVENT] Error:', e.message);
    }
  }

  // ============================================================
  // ✅ RESTORE ALL STATE (LOAD DARI STORAGE)
  // ============================================================
  
  async _restoreAllState() {
    try {
      const storage = await this._loadFromStorage();
      const { roomsData, userSeatData, currentNumber } = storage;
      
      if (currentNumber !== undefined) {
        this.currentNumber = currentNumber;
      }
      
      // Validasi data
      for (const [username, seatInfo] of Object.entries(userSeatData)) {
        if (!seatInfo || !seatInfo.room) {
          delete userSeatData[username];
          continue;
        }
        const roomData = roomsData[seatInfo.room];
        if (!roomData || !roomData.seats || !roomData.seats[seatInfo.seat]) {
          delete userSeatData[username];
          console.log(`[RESTORE] Removed invalid user "${username}"`);
        }
      }
      
      // Restore websockets
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
      
      await this._saveToStorage(roomsData, userSeatData, currentNumber);
      
      if (!this.closing && !this.isDestroyed) {
        this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      }
      
    } catch(e) {
      console.error('[RESTORE] Error:', e.message);
    }
  }

  // ============================================================
  // ✅ FETCH
  // ============================================================
  
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
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

  // ============================================================
  // ✅ DESTROY
  // ============================================================
  
  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    await this._cleanupStorage();
    await this._saveAllState();
    
    this._joinLocks.clear();
    this._kursiLocks.clear();
    this._userJoinLock.clear();
    
    for (const timeout of this._pendingTimeouts) {
      clearTimeout(timeout);
    }
    this._pendingTimeouts.clear();
    
    const wsCopy = Array.from(this.wsSet);
    for (const ws of wsCopy) {
      if (ws?.readyState === 1) {
        try { ws.send(JSON.stringify(["serverShutdown", "Server shutting down"])); } catch(e) {}
        try { ws.close(1000, "Shutdown"); } catch(e) {}
      }
      try { this.cleanup(ws); } catch(e) {}
    }
    
    this.wsSet.clear();
    this.userConnections.clear();
    this.roomClients.clear();
    this.wsActiveMulti.clear();
    this._processingMessages.clear();
    this._cleaningUp.clear();
    this._eventQueue.clear();
  }
}

// ==================== EXPORT ====================
export default ChatServer;
