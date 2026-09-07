// ==================== CHAT-SERVER.JS ====================
// VERSION: 12.6.0 - COMPLETE FIXED ALL ISSUES

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_INTERVAL_MS: 900000,
  MAX_NUMBER: 6,
  LOCK_TIMEOUT: 5000,
  USER_JOIN_LOCK_TIMEOUT: 10000,
  CACHE_LOAD_TIMEOUT: 5000,
  CLEANUP_BATCH_SIZE: 50,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);
const TABLE_NAME = 'chat_data';

// WeakMap untuk tracking state WebSocket
const _wsCleanupState = new WeakMap();

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.ctx = state;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    this._restored = false;
    this._restoreDone = false;
    this._restoreFailed = false;
    this._restorePromise = null;
    this._restoreStartTime = Date.now();
    
    this.wsSet = new Set();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.wsActiveMulti = new Map();
    
    this._joinLocks = new Map();
    this._kursiLocks = new Map();
    this._userJoinLock = new Map();
    this._wsLock = null;
    
    this.currentNumber = 1;
    this._isNumberUpdating = false;
    
    this.db = env.DB;
    
    this._storageCache = {
      roomsData: {},
      currentNumber: 1
    };
    this._cacheInitialized = false;
    this._cacheLoading = false;
    this._cacheLoadAttempts = 0;
    
    this._cleaningSet = new Set();
    this._cleanupQueue = [];
    this._isProcessingCleanup = false;
    
    // Inisialisasi room clients
    for (const room of ROOMS) {
      this.roomClients.set(room, new Set());
    }
    
    // Mulai restore dengan timeout
    this._restorePromise = this._restoreAllState();
    
    // Set timeout untuk restore
    const restoreTimeout = setTimeout(() => {
      if (!this._restoreDone) {
        this._restored = true;
        this._restoreDone = true;
        this._restoreFailed = true;
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this.currentNumber = 1;
        
        if (!this.closing && !this.isDestroyed) {
          try {
            this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
          } catch(e) {}
        }
      }
    }, C.CACHE_LOAD_TIMEOUT || 5000);
    
    this._restorePromise
      .then(() => {
        clearTimeout(restoreTimeout);
        this._restored = true;
        this._restoreDone = true;
      })
      .catch(() => {
        clearTimeout(restoreTimeout);
        this._restored = true;
        this._restoreDone = true;
        this._restoreFailed = true;
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this.currentNumber = 1;
      });
  }

  // ============ LOAD FROM D1 - 1 QUERY SAJA ============
  async _loadFromStorage() {
    try {
      await this.db.prepare(`
        CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `).run();

      // 1 QUERY UNTUK SEMUA DATA
      const result = await this.db
        .prepare(`SELECT key, value FROM ${TABLE_NAME}`)
        .all();

      const roomsData = {};
      for (const room of ROOMS) {
        roomsData[room] = { seat: {}, point: {}, mute: false };
      }

      let currentNumber = 1;

      // LOOP DI MEMORY - BUKAN DATABASE
      for (const row of result.results) {
        try {
          const key = row.key;
          const value = JSON.parse(row.value);

          if (key === 'current_number') {
            currentNumber = parseInt(value) || 1;
            continue;
          }

          const parts = key.split('_');
          if (parts.length < 3) continue;
          
          const type = parts[0];
          const roomName = parts[1];
          
          if (type === 'mute') {
            if (roomsData[roomName]) {
              roomsData[roomName].mute = value;
            }
            continue;
          }

          const seatNumber = parseInt(parts[2]);
          if (isNaN(seatNumber)) continue;
          
          if (!roomsData[roomName]) {
            roomsData[roomName] = { seat: {}, point: {}, mute: false };
          }

          if (type === 'seat') {
            roomsData[roomName].seat[seatNumber] = value;
          } else if (type === 'point') {
            roomsData[roomName].point[seatNumber] = value;
          }
        } catch(e) {
          continue;
        }
      }

      this._storageCache = { roomsData, currentNumber };
      this._cacheInitialized = true;
      this.currentNumber = currentNumber;
      this._cacheLoadAttempts = 0;

      return this._storageCache;
      
    } catch(e) {
      this._cacheLoadAttempts = (this._cacheLoadAttempts || 0) + 1;
      if (this._cacheLoadAttempts > 3) {
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this.currentNumber = 1;
        this._restoreFailed = true;
      }
      throw e;
    }
  }

  // ============ SAVE KE D1 ============
  async _saveSeat(roomName, seatNumber, seatData) {
    const key = `seat_${roomName}_${seatNumber}`;
    if (!seatData || !seatData.namauser || seatData.namauser.trim() === '') {
      await this.db.prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`).bind(key).run();
      return;
    }
    await this.db
      .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
      .bind(key, JSON.stringify(seatData))
      .run();
  }

  async _savePoint(roomName, seatNumber, pointData) {
    const key = `point_${roomName}_${seatNumber}`;
    if (!pointData || (pointData.x === 0 && pointData.y === 0 && !pointData.fast)) {
      await this.db.prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`).bind(key).run();
      return;
    }
    await this.db
      .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
      .bind(key, JSON.stringify(pointData))
      .run();
  }

  async _saveMute(roomName, muted) {
    const key = `mute_${roomName}`;
    if (muted === false) {
      await this.db.prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`).bind(key).run();
      return;
    }
    await this.db
      .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
      .bind(key, JSON.stringify(muted))
      .run();
  }

  async _saveCurrentNumber() {
    await this.db
      .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
      .bind('current_number', String(this._storageCache.currentNumber))
      .run();
  }

  // ============ DELETE SEAT + POINT ============
  async _deleteSeatInRoom(roomName, seatNumber) {
    const roomBucket = await this._getRoomBucket(roomName);
    delete roomBucket.seat[seatNumber];
    delete roomBucket.point[seatNumber];
    
    await this.db
      .prepare(`DELETE FROM ${TABLE_NAME} WHERE key IN (?, ?)`)
      .bind(`seat_${roomName}_${seatNumber}`, `point_${roomName}_${seatNumber}`)
      .run();
    
    this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
    await this.updateRoomCount(roomName);
    return true;
  }

  // ============ BUCKET ROOM ============
  async _getRoomBucket(roomName) {
    await this._ensureCacheInitialized();
    if (!this._storageCache.roomsData[roomName]) {
      this._storageCache.roomsData[roomName] = { seat: {}, point: {}, mute: false };
    }
    return this._storageCache.roomsData[roomName];
  }

  // ============ UPDATE SEAT ============
  async _updateSeatInRoom(roomName, seatNumber, seatData) {
    const roomBucket = await this._getRoomBucket(roomName);
    if (!seatData || !seatData.namauser || seatData.namauser.trim() === '') {
      delete roomBucket.seat[seatNumber];
      await this._saveSeat(roomName, seatNumber, null);
      return true;
    }
    roomBucket.seat[seatNumber] = seatData;
    await this._saveSeat(roomName, seatNumber, seatData);
    return true;
  }

  // ============ UPDATE POINT ============
  async _updatePointInRoom(roomName, seatNumber, pointData) {
    const roomBucket = await this._getRoomBucket(roomName);
    if (!pointData || (pointData.x === 0 && pointData.y === 0 && !pointData.fast)) {
      delete roomBucket.point[seatNumber];
      await this._savePoint(roomName, seatNumber, null);
      return true;
    }
    roomBucket.point[seatNumber] = pointData;
    await this._savePoint(roomName, seatNumber, pointData);
    return true;
  }

  // ============ UPDATE MUTE ============
  async _updateMuteInRoom(roomName, muted) {
    const roomBucket = await this._getRoomBucket(roomName);
    roomBucket.mute = muted;
    await this._saveMute(roomName, muted);
    return true;
  }

  // ============ GET DATA ============
  async _getRoomData(roomName) {
    await this._ensureCacheInitialized();
    return this._storageCache.roomsData[roomName] || null;
  }

  async _getSeatData(roomName, seatNumber) {
    await this._ensureCacheInitialized();
    const roomBucket = this._storageCache.roomsData[roomName];
    if (!roomBucket || !roomBucket.seat) return null;
    return roomBucket.seat[seatNumber] || null;
  }

  async _getRoomCount(roomName) {
    await this._ensureCacheInitialized();
    const roomBucket = this._storageCache.roomsData[roomName];
    if (!roomBucket || !roomBucket.seat) return 0;
    let count = 0;
    for (const seat in roomBucket.seat) {
      if (roomBucket.seat[seat]?.namauser) count++;
    }
    return count;
  }

  // ============ FIND USER ============
  async _findUserInAnyRoom(username) {
    if (!username) return null;
    await this._ensureCacheInitialized();
    const roomsData = this._storageCache.roomsData || {};
    for (const [roomName, roomBucket] of Object.entries(roomsData)) {
      if (!roomBucket?.seat) continue;
      for (const [seat, data] of Object.entries(roomBucket.seat)) {
        if (data?.namauser === username) {
          return { room: roomName, seat: parseInt(seat), isMulti: data.isMulti || false };
        }
      }
    }
    return null;
  }

  // ============ CACHE ============
  async _ensureCacheInitialized() {
    if (this._cacheInitialized && this._storageCache) {
      return this._storageCache;
    }
    
    if (this._cacheLoading) {
      let waited = 0;
      while (this._cacheLoading && waited < C.CACHE_LOAD_TIMEOUT) {
        await new Promise(resolve => setTimeout(resolve, 100));
        waited += 100;
      }
      return this._storageCache || { roomsData: {}, currentNumber: 1 };
    }
    
    if (this._restorePromise && !this._restoreDone) {
      try {
        await Promise.race([
          this._restorePromise,
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Restore timeout')), C.CACHE_LOAD_TIMEOUT || 5000)
          )
        ]);
      } catch(e) {
        if (!this._cacheInitialized) {
          this._storageCache = { roomsData: {}, currentNumber: 1 };
          this._cacheInitialized = true;
          this._restoreFailed = true;
        }
      }
      return this._storageCache || { roomsData: {}, currentNumber: 1 };
    }
    
    this._cacheLoading = true;
    try {
      await this._loadFromStorage();
    } catch(e) {
      if (!this._cacheInitialized) {
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
      }
    } finally {
      this._cacheLoading = false;
    }
    return this._storageCache;
  }

  // ============ UPDATE KURSI ============
  async _updateKursi(roomName, seat, data) {
    if (!roomName || !ROOMS_SET.has(roomName)) {
      return { success: false, error: 'Invalid room' };
    }
    if (typeof seat !== 'number' || seat < 1 || seat > C.MAX_SEATS) {
      return { success: false, error: 'Invalid seat number' };
    }
    if (!data?.namauser) {
      return { success: false, error: 'Username is required' };
    }
    
    await this._ensureCacheInitialized();
    
    const roomBucket = this._storageCache.roomsData[roomName];
    if (!roomBucket?.seat?.[seat]) {
      return { success: false, error: 'Seat not found' };
    }
    
    const currentSeatData = roomBucket.seat[seat];
    if (currentSeatData.namauser !== data.namauser) {
      return { success: false, error: 'You do not own this seat' };
    }
    
    const updatedSeat = {
      noimageUrl: data.noimageUrl || currentSeatData.noimageUrl || "",
      namauser: data.namauser || currentSeatData.namauser || "",
      color: data.color || currentSeatData.color || "",
      itembawah: typeof data.itembawah === 'number' ? data.itembawah : (parseInt(data.itembawah) || 0),
      itematas: typeof data.itematas === 'number' ? data.itematas : (parseInt(data.itematas) || 0),
      vip: typeof data.vip === 'number' ? data.vip : (parseInt(data.vip) || 0),
      viptanda: typeof data.viptanda === 'number' ? data.viptanda : (parseInt(data.viptanda) || 0),
      isMulti: data.isMulti !== undefined ? data.isMulti : (currentSeatData.isMulti || false)
    };
    
    await this._updateSeatInRoom(roomName, seat, updatedSeat);
    return { success: true, data: updatedSeat };
  }

  // ============ UPDATE POINT DIRECT ============
  async _updatePointDirect(roomName, seat, x, y, fast) {
    await this._ensureCacheInitialized();
    if (!this._storageCache.roomsData[roomName]) {
      this._storageCache.roomsData[roomName] = { seat: {}, point: {}, mute: false };
    }
    const pointData = { x: x || 0, y: y || 0, fast: !!fast };
    this._storageCache.roomsData[roomName].point[seat] = pointData;
    await this._savePoint(roomName, seat, pointData);
    return true;
  }

  // ============ REMOVE USER ============
  async _removeUserFromRoom(username, roomName) {
    if (!username || !roomName) return false;
    await this._ensureCacheInitialized();
    const roomBucket = this._storageCache.roomsData[roomName];
    if (!roomBucket?.seat) return false;
    
    let seat = null;
    for (const [s, data] of Object.entries(roomBucket.seat)) {
      if (data?.namauser === username) {
        seat = parseInt(s);
        break;
      }
    }
    if (!seat) return false;
    await this._deleteSeatInRoom(roomName, seat);
    return true;
  }

  // ============ JOIN ROOM ============
  async _handleJoin(ws, roomName) {
    if (!ws?.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
      return false;
    }
    
    const username = ws.username;
    const lockKey = `join_user_${username}`;
    
    const existingLock = this._userJoinLock.get(lockKey);
    if (existingLock && Date.now() - existingLock < C.USER_JOIN_LOCK_TIMEOUT) {
      return false;
    }
    this._userJoinLock.set(lockKey, Date.now());
    
    try {
      return await this._joinInternal(ws, roomName, username);
    } finally {
      this._userJoinLock.delete(lockKey);
    }
  }

  async _joinInternal(ws, roomName, username) {
    const existing = await this._findUserInAnyRoom(username);
    if (existing && existing.room !== roomName) {
      await this._removeUserFromRoom(username, existing.room);
    }
    
    await this._ensureCacheInitialized();
    
    let roomBucket = this._storageCache.roomsData[roomName];
    if (!roomBucket) {
      roomBucket = { seat: {}, point: {}, mute: false };
      this._storageCache.roomsData[roomName] = roomBucket;
    }
    if (!roomBucket.seat) roomBucket.seat = {};
    if (!roomBucket.point) roomBucket.point = {};
    
    let seat = null;
    for (const [s, data] of Object.entries(roomBucket.seat)) {
      if (data?.namauser === username) {
        seat = parseInt(s);
        break;
      }
    }
    
    if (!seat) {
      const seatCount = Object.values(roomBucket.seat).filter(s => s?.namauser).length;
      if (seatCount >= C.MAX_SEATS) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      
      for (let s = 1; s <= C.MAX_SEATS; s++) {
        if (!roomBucket.seat[s]) {
          seat = s;
          break;
        }
      }
      
      if (!seat) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      
      const newSeat = {
        noimageUrl: "",
        namauser: username,
        color: "",
        itembawah: 0,
        itematas: 0,
        vip: 0,
        viptanda: 0,
        isMulti: false
      };
      
      await this._updateSeatInRoom(roomName, seat, newSeat);
    }
    
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = username;
    ws._room = roomName;
    ws._username = username;
    
    ws.serializeAttachment({
      username: username,
      seatInfo: { room: roomName, seat: seat }
    });
    
    for (const [otherRoom, clients] of this.roomClients) {
      if (otherRoom !== roomName && clients) {
        clients.delete(ws);
      }
    }
    const roomClients = this.roomClients.get(roomName);
    if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
    
    this.wsActiveMulti.delete(ws);
    
    const muteStatus = roomBucket.mute || false;
    
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", muteStatus, roomName]);
    
    await this.updateRoomCount(roomName);
    
    setTimeout(() => {
      try {
        if (ws?.readyState === 1) {
          this.sendAllStateTo(ws, roomName, true);
        }
      } catch(e) {}
    }, 1000);
    
    return true;
  }

  // ============ MULTI JOIN ============
  async _handleMultiJoin(ws, multiUsername, multiRoomname) {
    if (!multiUsername || !multiRoomname || !ROOMS_SET.has(multiRoomname)) return false;
    await this._ensureCacheInitialized();
    
    const existing = await this._findUserInAnyRoom(multiUsername);
    if (existing && !existing.isMulti) {
      await this._removeUserFromRoom(multiUsername, existing.room);
    }
    
    let roomBucket = this._storageCache.roomsData[multiRoomname];
    if (!roomBucket) {
      roomBucket = { seat: {}, point: {}, mute: false };
      this._storageCache.roomsData[multiRoomname] = roomBucket;
    }
    if (!roomBucket.seat) roomBucket.seat = {};
    
    let seat = null;
    for (const [s, data] of Object.entries(roomBucket.seat)) {
      if (data?.namauser === multiUsername && data.isMulti === true) {
        seat = parseInt(s);
        break;
      }
    }
    
    if (!seat) {
      const seatCount = Object.values(roomBucket.seat).filter(s => s?.namauser).length;
      if (seatCount >= C.MAX_SEATS) return false;
      
      for (let s = 1; s <= C.MAX_SEATS; s++) {
        if (!roomBucket.seat[s]) {
          seat = s;
          break;
        }
      }
      if (!seat) return false;
      
      const newSeat = {
        noimageUrl: "",
        namauser: multiUsername,
        color: "",
        itembawah: 0,
        itematas: 0,
        vip: 0,
        viptanda: 0,
        isMulti: true
      };
      await this._updateSeatInRoom(multiRoomname, seat, newSeat);
    }
    return { room: multiRoomname, seat: seat };
  }

  // ============ WEBSOCKET CLEANUP - PER USER ============
  async _cleanupUserCompletely(ws) {
    if (!ws) return;
    
    let state = _wsCleanupState.get(ws);
    if (!state) {
      state = {
        cleaning: false,
        cleanupPromise: null,
        cleanupDone: false
      };
      _wsCleanupState.set(ws, state);
    }
    
    if (state.cleanupDone) return;
    if (state.cleaning && state.cleanupPromise) {
      return state.cleanupPromise;
    }
    
    if (state.cleaning) {
      await new Promise(resolve => setTimeout(resolve, 100));
      if (state.cleanupDone) return;
      if (state.cleanupPromise) return state.cleanupPromise;
      state.cleaning = false;
    }
    
    state.cleaning = true;
    state.cleanupPromise = this._performCleanup(ws, state);
    
    try {
      await state.cleanupPromise;
    } catch(e) {
      // Silent
    } finally {
      state.cleaning = false;
      state.cleanupDone = true;
      state.cleanupPromise = null;
    }
  }

  async _performCleanup(ws, state) {
    try {
      const username = ws.username || ws._username;
      const roomName = ws.room || ws.roomname || ws._room;
      const isMulti = this.wsActiveMulti.has(ws);
      
      let actualRoom = roomName;
      let actualSeat = null;
      
      if (username && !actualRoom) {
        const found = await this._findUserInAnyRoom(username);
        if (found) {
          actualRoom = found.room;
          actualSeat = found.seat;
        }
      }
      
      if (username && actualRoom && !actualSeat) {
        const roomBucket = await this._getRoomBucket(actualRoom);
        if (roomBucket?.seat) {
          for (const [seat, data] of Object.entries(roomBucket.seat)) {
            if (data?.namauser === username) {
              actualSeat = parseInt(seat);
              break;
            }
          }
        }
      }
      
      if (ws && !ws._closing) {
        ws._closing = true;
      }
      
      try {
        if (ws && ws.readyState === 1) {
          ws.close(1000, "Cleanup");
        }
      } catch(e) {}
      
      if (isMulti) {
        if (username) {
          const connections = this.userConnections.get(username);
          if (connections) {
            connections.delete(ws);
            if (connections.size === 0) {
              this.userConnections.delete(username);
            }
          }
        }
        
        if (actualRoom) {
          const roomClients = this.roomClients.get(actualRoom);
          if (roomClients) roomClients.delete(ws);
        }
      } else {
        if (actualRoom && username) {
          try {
            const roomBucket = await this._getRoomBucket(actualRoom);
            if (roomBucket?.seat) {
              let seatToRemove = actualSeat;
              
              if (!seatToRemove) {
                for (const [seat, data] of Object.entries(roomBucket.seat)) {
                  if (data?.namauser === username) {
                    seatToRemove = parseInt(seat);
                    break;
                  }
                }
              }
              
              if (seatToRemove) {
                delete roomBucket.seat[seatToRemove];
                delete roomBucket.point[seatToRemove];
                
                await this.db
                  .prepare(`DELETE FROM ${TABLE_NAME} WHERE key IN (?, ?)`)
                  .bind(`seat_${actualRoom}_${seatToRemove}`, `point_${actualRoom}_${seatToRemove}`)
                  .run();
                
                this.broadcast(actualRoom, ["removeKursi", actualRoom, seatToRemove]);
                await this.updateRoomCount(actualRoom);
              }
            }
          } catch(e) {
            try {
              await this._removeUserFromRoom(username, actualRoom);
            } catch(e2) {}
          }
        } else if (username) {
          try {
            const found = await this._findUserInAnyRoom(username);
            if (found) {
              await this._removeUserFromRoom(username, found.room);
              await this.updateRoomCount(found.room);
            }
          } catch(e) {}
        }
        
        if (username) {
          const connections = this.userConnections.get(username);
          if (connections) {
            connections.delete(ws);
            if (connections.size === 0) {
              this.userConnections.delete(username);
            }
          }
        }
        
        if (actualRoom) {
          const roomClients = this.roomClients.get(actualRoom);
          if (roomClients) roomClients.delete(ws);
        } else if (username) {
          try {
            const found = await this._findUserInAnyRoom(username);
            if (found) {
              const roomClients = this.roomClients.get(found.room);
              if (roomClients) roomClients.delete(ws);
            }
          } catch(e) {}
        }
      }
      
      this.wsSet.delete(ws);
      this.wsActiveMulti.delete(ws);
      
      if (this._cleaningSet) {
        this._cleaningSet.delete(ws);
      }
      
      if (this._cleanupQueue) {
        const index = this._cleanupQueue.indexOf(ws);
        if (index !== -1) {
          this._cleanupQueue.splice(index, 1);
        }
      }
      
      try {
        ws.serializeAttachment({});
        ws.username = null;
        ws.room = null;
        ws.roomname = null;
        ws.idtarget = null;
        ws._closing = false;
        ws._cleaning = false;
        ws._username = null;
        ws._room = null;
      } catch(e) {}
      
    } catch(e) {
      // Silent
    }
  }

  // ============ WEB SOCKET EVENT HANDLERS ============
  async webSocketClose(ws) {
    if (!ws || this.isDestroyed) return;
    
    if (!ws._username && ws.username) {
      ws._username = ws.username;
    }
    if (!ws._room && (ws.room || ws.roomname)) {
      ws._room = ws.room || ws.roomname;
    }
    
    const state = _wsCleanupState.get(ws);
    if (state && (state.cleaning || state.cleanupDone)) return;
    
    await this._cleanupUserCompletely(ws);
  }

  async webSocketError(ws) {
    if (!ws || this.isDestroyed) return;
    
    if (!ws._username && ws.username) {
      ws._username = ws.username;
    }
    if (!ws._room && (ws.room || ws.roomname)) {
      ws._room = ws.room || ws.roomname;
    }
    
    const state = _wsCleanupState.get(ws);
    if (state && (state.cleaning || state.cleanupDone)) return;
    
    await this._cleanupUserCompletely(ws);
  }

  async webSocketMessage(ws, msg) {
    if (!ws || ws._closing || this.closing || this.isDestroyed || ws._cleaning) return;
    try { 
      await this.handleMessage(ws, msg); 
    } catch(e) {}
  }

  // ============ CLEANUP QUEUE ============
  _queueCleanup(ws) {
    if (!ws || this.isDestroyed) return;
    
    const state = _wsCleanupState.get(ws);
    if (state && (state.cleaning || state.cleanupDone)) return;
    
    if (this._cleaningSet && this._cleaningSet.has(ws)) return;
    
    if (!this._cleaningSet) {
      this._cleaningSet = new Set();
    }
    this._cleaningSet.add(ws);
    
    if (!this._cleanupQueue) {
      this._cleanupQueue = [];
    }
    this._cleanupQueue.push(ws);
    
    if (!this._isProcessingCleanup) {
      this._processCleanupQueue();
    }
  }

  async _processCleanupQueue() {
    if (this._isProcessingCleanup || this.isDestroyed) return;
    this._isProcessingCleanup = true;
    
    try {
      while (this._cleanupQueue && this._cleanupQueue.length > 0) {
        const batchSize = Math.min(C.CLEANUP_BATCH_SIZE || 50, this._cleanupQueue.length);
        const batch = this._cleanupQueue.splice(0, batchSize);
        
        for (const ws of batch) {
          if (this._cleaningSet) {
            this._cleaningSet.delete(ws);
          }
        }
        
        await Promise.allSettled(
          batch.map(ws => this._cleanupUserCompletely(ws))
        );
      }
    } catch(e) {
      // Silent
    } finally {
      this._isProcessingCleanup = false;
      if (this._cleanupQueue && this._cleanupQueue.length > 0 && !this.isDestroyed) {
        this._processCleanupQueue();
      }
    }
  }

  // ============ BROADCAST ============
  broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try {
      const msgStr = JSON.stringify(msg);
      const clients = this.roomClients.get(room);
      if (!clients || clients.size === 0) return;
      
      const toRemove = new Set();
      for (const ws of clients) {
        if (!ws) { 
          toRemove.add(ws); 
          continue; 
        }
        
        const state = _wsCleanupState.get(ws);
        if (state && (state.cleaning || state.cleanupDone)) {
          toRemove.add(ws);
          continue;
        }
        
        const wsRoom = ws.room || ws.roomname;
        if (wsRoom !== room) {
          toRemove.add(ws);
          continue;
        }
        
        try {
          if (ws.readyState === 1 && !ws._closing && !ws._cleaning) {
            ws.send(msgStr);
          } else {
            toRemove.add(ws);
          }
        } catch(e) { 
          toRemove.add(ws); 
        }
      }
      
      if (toRemove.size > 0) {
        for (const ws of toRemove) {
          try {
            clients.delete(ws);
            if (ws) {
              this._queueCleanup(ws);
            }
          } catch(e) {}
        }
      }
    } catch(e) {}
  }

  safeSend(ws, msg) {
    if (!ws) return false;
    try {
      if (ws.readyState !== 1 || ws._closing || ws._cleaning || this.closing || this.isDestroyed) {
        return false;
      }
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      this._queueCleanup(ws);
      return false;
    }
  }

  async updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      const count = await this._getRoomCount(room);
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { 
      return 0; 
    }
  }

  async sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws?.username) return;
    
    try {
      if (ws.readyState !== 1 || ws._closing || ws._cleaning) {
        return;
      }
    } catch(e) { 
      return; 
    }
    
    await this._ensureCacheInitialized();
    const roomBucket = this._storageCache.roomsData[room];
    if (!roomBucket) return;
    
    try {
      const allSeats = roomBucket.seat || {};
      const allPoints = roomBucket.point || {};
      
      let selfSeat = null;
      for (const [seat, data] of Object.entries(allSeats)) {
        if (data?.namauser === ws.username) {
          selfSeat = parseInt(seat);
          break;
        }
      }
      
      const count = Object.values(allSeats).filter(s => s?.namauser).length;
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
          x: point.x || 0,
          y: point.y || 0,
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

  // ============ ALARM ============
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    await this._updateNumber();
    this._cleanupStaleLocks();
    await this._cleanupStorage();
    this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
  }

  async _updateNumber() {
    if (this._isNumberUpdating || this.closing || this.isDestroyed) return;
    this._isNumberUpdating = true;
    try {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      this._storageCache.currentNumber = this.currentNumber;
      await this._saveCurrentNumber();
      for (const [room, clients] of this.roomClients) {
        if (clients?.size > 0) {
          this.broadcast(room, ["currentNumber", this.currentNumber]);
        }
      }
    } catch(e) {} finally {
      this._isNumberUpdating = false;
    }
  }

  // ============ CLEANUP STORAGE ============
  async _cleanupStorage() {
    try {
      await this._ensureCacheInitialized();
      const roomsData = this._storageCache.roomsData || {};
      
      for (const [roomName, roomBucket] of Object.entries(roomsData)) {
        const hasSeats = roomBucket.seat && Object.values(roomBucket.seat).some(s => s?.namauser);
        const hasPoints = roomBucket.point && Object.keys(roomBucket.point).length > 0;
        if (!hasSeats && !hasPoints) {
          delete roomsData[roomName];
          await this.db
            .prepare(`DELETE FROM ${TABLE_NAME} WHERE key LIKE ? OR key LIKE ? OR key = ?`)
            .bind(`seat_${roomName}_%`, `point_${roomName}_%`, `mute_${roomName}`)
            .run();
        }
      }
      
      this._storageCache.roomsData = roomsData;
    } catch(e) {}
  }

  // ============ CLEANUP STALE LOCKS ============
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

  // ============ RESTORE ============
  async _restoreAllState() {
    try {
      await this._loadFromStorage();
      await this._ensureCacheInitialized();
      
      const webSockets = this.ctx.getWebSockets();
      let restoredCount = 0;
      
      for (const ws of webSockets) {
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment?.username) {
            const found = await this._findUserInAnyRoom(attachment.username);
            if (found) {
              ws.username = attachment.username;
              ws.room = found.room;
              ws.roomname = found.room;
              ws.idtarget = attachment.username;
              ws._closing = false;
              ws._cleaning = false;
              ws._username = attachment.username;
              ws._room = found.room;
              ws._wsId = Date.now() + Math.random();
              
              const state = _wsCleanupState.get(ws);
              if (state) {
                state.cleaning = false;
                state.cleanupPromise = null;
                state.cleanupDone = false;
              } else {
                _wsCleanupState.set(ws, {
                  cleaning: false,
                  cleanupPromise: null,
                  cleanupDone: false
                });
              }
              
              const roomClients = this.roomClients.get(found.room);
              if (roomClients && !roomClients.has(ws)) {
                roomClients.add(ws);
              }
              
              let conns = this.userConnections.get(attachment.username);
              if (!conns) {
                conns = new Set();
                this.userConnections.set(attachment.username, conns);
              }
              if (!conns.has(ws)) {
                conns.add(ws);
              }
              
              if (!this.wsSet.has(ws)) {
                this.wsSet.add(ws);
              }
              
              restoredCount++;
            }
          }
        } catch(e) {
          continue;
        }
      }
      
      if (!this.closing && !this.isDestroyed) {
        try {
          await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
        } catch(e) {}
      }
      
      for (const room of ROOMS) {
        const roomBucket = this._storageCache.roomsData[room];
        let count = 0;
        if (roomBucket?.seat) {
          for (const seat in roomBucket.seat) {
            if (roomBucket.seat[seat]?.namauser) {
              count++;
            }
          }
        }
        this.broadcast(room, ["roomUserCount", room, count]);
      }
      
      for (const room of ROOMS) {
        this.broadcast(room, ["currentNumber", this.currentNumber]);
      }
      
      this._restored = true;
      this._restoreDone = true;
      
      return true;
      
    } catch(e) {
      this._restored = true;
      this._restoreDone = true;
      this._restoreFailed = true;
      this._storageCache = { roomsData: {}, currentNumber: 1 };
      this._cacheInitialized = true;
      this.currentNumber = 1;
      
      if (!this.closing && !this.isDestroyed) {
        try {
          await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
        } catch(e2) {}
      }
      
      throw e;
    }
  }

  // ============ HANDLE MESSAGE ============
  async _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    const found = await this._findUserInAnyRoom(username);
    const isMultiUser = found ? found.isMulti : false;
    if (isMultiUser && isNewUser === false) {
      return;
    }
    if (isMultiUser && isNewUser === true) {
      await this._removeUserFromRoom(username, found.room);
    }
    if (!isMultiUser && found) {
      await this._removeUserFromRoom(username, found.room);
    }
    ws.username = username;
    ws.idtarget = username;
    ws.room = null;
    ws.roomname = null;
    ws._closing = false;
    ws._username = username;
    ws._room = null;
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
    
    if (!this._restored && this._restorePromise) {
      await Promise.race([
        this._restorePromise,
        new Promise(resolve => setTimeout(resolve, C.CACHE_LOAD_TIMEOUT))
      ]);
      if (!this._restored) {
        return;
      }
    }
    
    try {
      if (ws.readyState !== 1 || ws._closing || ws._cleaning || this.closing || this.isDestroyed) {
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
      
      if (evt === "onDestroy") {
        if (!ws._username && ws.username) {
          ws._username = ws.username;
        }
        if (!ws._room && (ws.room || ws.roomname)) {
          ws._room = ws.room || ws.roomname;
        }
        
        const state = _wsCleanupState.get(ws);
        if (state && (state.cleaning || state.cleanupDone)) {
          return;
        }
        
        await this._cleanupUserCompletely(ws);
        return;
      }
      
      if (evt === "chat" || evt === "updatePoint" || evt === "gift" || evt === "rollangak") {
        const room = args[0];
        if (room && !ROOMS_SET.has(room)) return;
      }
      await this._handleEventInternal(ws, [evt, ...args]);
    } catch(e) {}
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
          const result = await this._handleMultiJoin(ws, multiUsername, multiRoomname);
          if (!result) break;
          const { room, seat } = result;
          let connections = this.userConnections.get(multiUsername);
          if (!connections) connections = new Set();
          if (!connections.has(ws)) connections.add(ws);
          this.userConnections.set(multiUsername, connections);
          ws.serializeAttachment({
            username: multiUsername,
            seatInfo: { room: room, seat: seat }
          });
          ws._username = multiUsername;
          ws._room = room;
          this.wsActiveMulti.set(ws, { username: multiUsername, room: room });
          for (const [otherRoom, clients] of this.roomClients) {
            if (otherRoom !== room && clients) {
              clients.delete(ws);
            }
          }
          const roomClients = this.roomClients.get(room);
          if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
          this.safeSend(ws, ["rooMasukMulti", seat, room]);
          await this.updateRoomCount(room);
          break;
        }
        
        case "exitMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          try {
            const found = await this._findUserInAnyRoom(targetUsername);
            const roomName = found?.room;
            const seatNumber = found?.seat;
            if (roomName && seatNumber) {
              await this._removeUserFromRoom(targetUsername, roomName);
            }
            const connections = this.userConnections.get(targetUsername);
            if (connections) {
              const toRemove = Array.from(connections);
              for (const conn of toRemove) {
                if (conn.room) {
                  const rc = this.roomClients.get(conn.room);
                  if (rc) rc.delete(conn);
                }
                if (roomName) {
                  const rc = this.roomClients.get(roomName);
                  if (rc) rc.delete(conn);
                }
                this.wsActiveMulti.delete(conn);
                try {
                  conn.serializeAttachment({});
                  conn.username = null;
                  conn.room = null;
                  conn.roomname = null;
                  conn.idtarget = null;
                  conn._username = null;
                  conn._room = null;
                } catch(e) {}
                try {
                  if (conn.readyState === 1) {
                    this.safeSend(conn, ["forceExit", "You have been exited"]);
                  }
                } catch(e) {}
                this.wsSet.delete(conn);
              }
              this.userConnections.delete(targetUsername);
            }
            const toDelete = [];
            for (const [wsKey, data] of this.wsActiveMulti) {
              if (data?.username === targetUsername) {
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
                  wsKey._username = null;
                  wsKey._room = null;
                } catch(e) {}
                try {
                  if (wsKey.readyState === 1) {
                    this.safeSend(wsKey, ["forceExit", "You have been exited"]);
                  }
                } catch(e) {}
                this.wsSet.delete(wsKey);
              }
            }
            for (const wsKey of toDelete) {
              this.wsActiveMulti.delete(wsKey);
            }
            if (roomName) {
              this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
              await this.updateRoomCount(roomName);
            }
          } catch(e) {}
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
          const found = await this._findUserInAnyRoom(targetUsername);
          if (!found) break;
          const roomName = found.room;
          const seatNumber = found.seat;
          let existingWs = null;
          for (const [wsKey, data] of this.wsActiveMulti) {
            if (data?.username === targetUsername) {
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
              existingWs._username = null;
              existingWs._room = null;
            } catch(e) {}
            this.wsSet.delete(existingWs);
            this.wsActiveMulti.delete(existingWs);
            try {
              if (existingWs.readyState === 1) {
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
          ws._username = targetUsername;
          ws._room = roomName;
          ws.serializeAttachment({
            username: targetUsername,
            seatInfo: found
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
          break;
        }
        
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          if (!kursiRoom || typeof kursiSeat !== 'number' || kursiSeat < 1 || kursiSeat > C.MAX_SEATS) {
            break;
          }
          if (!ROOMS_SET.has(kursiRoom)) break;
          if (!kursiName || typeof kursiName !== 'string' || kursiName.trim().length === 0) break;
          
          const currentUser = ws.username || ws._username;
          if (!currentUser) break;
          
          const seatData = await this._getSeatData(kursiRoom, kursiSeat);
          if (!seatData || seatData.namauser !== kursiName) break;
          
          if (seatData.namauser !== currentUser) {
            break;
          }
          
          const lockKey = `kursi_${kursiRoom}_${kursiSeat}`;
          const now = Date.now();
          const existingLock = this._kursiLocks.get(lockKey);
          if (existingLock && now - existingLock < C.LOCK_TIMEOUT) {
            break;
          }
          this._kursiLocks.set(lockKey, now);
          
          try {
            const updateData = {
              noimageUrl: String(kursiNoimg || ""),
              namauser: String(kursiName || ""),
              color: String(kursiColor || ""),
              itembawah: typeof kursiBawah === 'number' ? kursiBawah : (parseInt(kursiBawah) || 0),
              itematas: typeof kursiAtas === 'number' ? kursiAtas : (parseInt(kursiAtas) || 0),
              vip: typeof kursiVip === 'number' ? kursiVip : (parseInt(kursiVip) || 0),
              viptanda: typeof kursiVt === 'number' ? kursiVt : (parseInt(kursiVt) || 0),
              isMulti: seatData.isMulti || false
            };
            const result = await this._updateKursi(kursiRoom, kursiSeat, updateData);
            if (result.success) {
              this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, result.data]]]);
            }
          } catch(e) {} finally {
            this._kursiLocks.delete(lockKey);
          }
          break;
        }
        
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;
          const found = await this._findUserInAnyRoom(chatUser);
          if (!found || found.room !== chatRoom) break;
          const wsRoom = ws.room || ws.roomname;
          if (wsRoom !== chatRoom) break;
          this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]);
          break;
        }
        
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (!pointRoom || typeof pointSeat !== 'number') break;
          const updated = await this._updatePointDirect(pointRoom, pointSeat, pointX, pointY, pointFast === 1);
          if (updated) {
            this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]);
          }
          break;
        }
        
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (giftRoom && ROOMS_SET.has(giftRoom)) {
            const roomBucket = await this._getRoomBucket(giftRoom);
            let senderFound = false;
            let receiverFound = false;
            for (const [seat, data] of Object.entries(roomBucket.seat || {})) {
              if (data?.namauser === giftSender) senderFound = true;
              if (data?.namauser === giftReceiver) receiverFound = true;
              if (senderFound && receiverFound) break;
            }
            if (!senderFound || !receiverFound) break;
            if ((ws.room || ws.roomname) !== giftRoom) break;
            this.broadcast(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]);
          }
          break;
        }
        
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (rollRoom && ROOMS_SET.has(rollRoom)) {
            const found = await this._findUserInAnyRoom(rollUser);
            if (!found || found.room !== rollRoom) break;
            if ((ws.room || ws.roomname) !== rollRoom) break;
            this.broadcast(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, rollAngka]);
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          const currentUser = ws.username || ws._username;
          if (!currentUser) break;
          const found = await this._findUserInAnyRoom(currentUser);
          if (!found || found.room !== removeRoom) break;
          const roomBucket = await this._getRoomData(removeRoom);
          let username = null;
          if (roomBucket?.seat?.[removeSeat]) {
            username = roomBucket.seat[removeSeat].namauser;
          }
          if (username) {
            await this._removeUserFromRoom(username, removeRoom);
          }
          break;
        }
        
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (!muteRoom || !ROOMS_SET.has(muteRoom)) break;
          const currentUser = ws.username || ws._username;
          if (!currentUser) break;
          const found = await this._findUserInAnyRoom(currentUser);
          if (!found || found.room !== muteRoom) break;
          await this._updateMuteInRoom(muteRoom, !!muteVal);
          this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
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
          } catch(e) {}
          break;
        }
        
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          const found = await this._findUserInAnyRoom(onlineTarget);
          if (found) {
            if (found.isMulti === true) {
              isOnline = true;
            } else {
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
        
        case "getOnlineUsers": {
          const users = [];
          await this._ensureCacheInitialized();
          const roomsData = this._storageCache.roomsData || {};
          for (const [roomName, roomBucket] of Object.entries(roomsData)) {
            if (!roomBucket?.seat) continue;
            for (const [seat, data] of Object.entries(roomBucket.seat)) {
              if (data?.namauser) {
                const username = data.namauser;
                let isOnline = false;
                if (data.isMulti === true) {
                  isOnline = true;
                } else {
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
                if (isOnline && !users.includes(username)) {
                  users.push(username);
                }
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
            const roomBucket = this._storageCache.roomsData[room];
            let count = 0;
            if (roomBucket?.seat) {
              for (const seat in roomBucket.seat) {
                if (roomBucket.seat[seat]?.namauser) count++;
              }
            }
            counts[room] = count;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = args[0];
          if (roomName && ROOMS_SET.has(roomName)) {
            const count = await this._getRoomCount(roomName);
            this.safeSend(ws, ["roomUserCount", roomName, count]);
          }
          break;
        }
        
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (getMuteRoom && ROOMS_SET.has(getMuteRoom)) {
            await this._ensureCacheInitialized();
            const roomBucket = this._storageCache.roomsData[getMuteRoom];
            this.safeSend(ws, ["muteTypeResponse", roomBucket?.mute || false, getMuteRoom]);
          }
          break;
        }
        
        case "isInRoom": {
          let isInRoom = false;
          const currentUser = ws.username || ws._username;
          if (currentUser) {
            const found = await this._findUserInAnyRoom(currentUser);
            if (found) {
              isInRoom = true;
            }
          }
          this.safeSend(ws, ["inRoomStatus", isInRoom]);
          break;
        }
        
        case "resetRoom": {
          const resetRoomName = args[0];
          if (resetRoomName && ROOMS_SET.has(resetRoomName)) {
            await this._ensureCacheInitialized();
            const roomBucket = this._storageCache.roomsData[resetRoomName];
            if (roomBucket) {
              roomBucket.seat = {};
              roomBucket.point = {};
              roomBucket.mute = false;
              await this.db
                .prepare(`DELETE FROM ${TABLE_NAME} WHERE key LIKE ? OR key LIKE ? OR key = ?`)
                .bind(`seat_${resetRoomName}_%`, `point_${resetRoomName}_%`, `mute_${resetRoomName}`)
                .run();
              this.broadcast(resetRoomName, ["resetRoom", resetRoomName]);
              await this.updateRoomCount(resetRoomName);
            }
          }
          break;
        }
        
        case "modwarning": {
          const modRoom = args[0];
          if (modRoom && ROOMS_SET.has(modRoom)) {
            const currentUser = ws.username || ws._username;
            if (!currentUser) break;
            const found = await this._findUserInAnyRoom(currentUser);
            if (!found || found.room !== modRoom) break;
            this.broadcast(modRoom, ["modwarning", modRoom]);
          }
          break;
        }
        
        default:
          break;
      }
    } catch(e) {}
  }

  // ============ FETCH ============
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    if (!this._restored && this._restorePromise) {
      await Promise.race([
        this._restorePromise,
        new Promise(resolve => setTimeout(resolve, C.CACHE_LOAD_TIMEOUT))
      ]);
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
      server._cleaning = false;
      server._username = null;
      server._room = null;
      server._wsId = Date.now() + Math.random();
      server.serializeAttachment({});
      
      if (!this.wsSet.has(server)) this.wsSet.add(server);
      
      return new Response(null, { status: 101, webSocket: client });
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ============ DESTROY ============
  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    this._joinLocks.clear();
    this._kursiLocks.clear();
    this._userJoinLock.clear();
    
    if (this._cleanupQueue) {
      this._cleanupQueue = [];
    }
    if (this._cleaningSet) {
      this._cleaningSet.clear();
    }
    
    const wsCopy = Array.from(this.wsSet);
    const cleanupPromises = [];
    
    for (const ws of wsCopy) {
      if (!ws) continue;
      
      const isMulti = this.wsActiveMulti.has(ws);
      
      try {
        if (ws.readyState === 1) {
          ws.close(1000, "Shutdown");
        }
      } catch(e) {}
      
      if (isMulti) {
        this.wsSet.delete(ws);
        this.wsActiveMulti.delete(ws);
        const room = ws.room || ws.roomname;
        if (room) {
          const rc = this.roomClients.get(room);
          if (rc) rc.delete(ws);
        }
        try {
          ws.serializeAttachment({});
          ws.username = null;
          ws.room = null;
          ws.roomname = null;
          ws.idtarget = null;
          ws._closing = false;
          ws._cleaning = false;
          ws._username = null;
          ws._room = null;
        } catch(e) {}
        _wsCleanupState.delete(ws);
      } else {
        cleanupPromises.push(
          this._cleanupUserCompletely(ws).catch(() => {})
        );
      }
    }
    
    if (cleanupPromises.length > 0) {
      await Promise.allSettled(cleanupPromises);
    }
    
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
        let isMultiUser = false;
        for (const [wsKey, data] of this.wsActiveMulti) {
          if (data?.username === username) {
            isMultiUser = true;
            break;
          }
        }
        if (!isMultiUser) {
          this.userConnections.delete(username);
        }
      }
    }
    
    for (const [room, clients] of this.roomClients) {
      clients.clear();
    }
  }
}

export default ChatServer;
