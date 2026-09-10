// ==================== CHAT-SERVER.JS ====================
// VERSION: 14.7.1 - HEMAT DURABLE OBJECTS + FIX ERROR + FIX DECAY
// ⚠️ MULTI BEHAVIOR UNCHANGED

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_INTERVAL_MS: 900000,
  MAX_NUMBER: 6,
  LOCK_TIMEOUT: 5000,
  USER_JOIN_LOCK_TIMEOUT: 10000,
  CACHE_LOAD_TIMEOUT: 5000,
  MAX_PENDING_EVENTS: 100,
  MAX_EVENT_QUEUE_SIZE: 50,
  PROCESS_BATCH_SIZE: 50,
  PROCESS_MAX_TIME_MS: 100,
  ERROR_RESET_INTERVAL_MS: 60000,
  RATE_LIMIT_MAX: 100,
  RATE_LIMIT_WINDOW_MS: 60000,
  MAX_RESTORE_ATTEMPTS: 3,
  RESTORE_RETRY_DELAY_MS: 2000,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);
const TABLE_NAME = 'chat_data';

const _wsCleanupState = new WeakMap();

export class ChatServer {
  constructor(state, env) {
    try {
      this.state = state || null;
      this.env = env || null;
      this.ctx = state || null;
      this.closing = false;
      this.isDestroyed = false;
      this._startTime = Date.now();
      this._restored = false;
      this._restoreDone = false;
      this._restoreFailed = false;
      this._restorePromise = null;
      this._restoreStartTime = Date.now();
      this._restoreAttempts = 0;

      this._pendingEvents = [];
      this._isRestoring = false;
      this._eventQueue = [];
      this._processingQueue = false;
      this._processingPending = false;

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

      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._lastRequestDecay = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      this._reconnectAttempts = new Map();
      this._allTimers = new Set();

      // ✅ HEMAT: cache untuk getOnlineUsers & getAllRoomsUserCount
      this._onlineUsersCache = null;
      this._onlineUsersCacheTime = 0;
      this._roomCountsCache = null;
      this._roomCountsCacheTime = 0;

      if (!env || !env.DB) {
        this.db = null;
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this._restored = true;
        this._restoreDone = true;
        this._restoreFailed = true;
        for (const room of ROOMS) {
          this.roomClients.set(room, new Set());
        }
        return;
      }

      this.db = env.DB;

      this._storageCache = {
        roomsData: {},
        currentNumber: 1
      };
      this._cacheInitialized = false;
      this._cacheLoading = false;
      this._cacheLoadingPromise = null;
      this._cacheLoadAttempts = 0;

      for (const room of ROOMS) {
        this.roomClients.set(room, new Set());
      }

      this._restorePromise = this._restoreWithRetry();

      const restoreTimeout = setTimeout(() => {
        if (!this._restoreDone) {
          this._restored = true;
          this._restoreDone = true;
          this._restoreFailed = true;
          this._isRestoring = false;
          this._storageCache = { roomsData: {}, currentNumber: 1 };
          this._cacheInitialized = true;
          this.currentNumber = 1;

          if (!this.closing && !this.isDestroyed) {
            try {
              this.ctx?.storage?.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
            } catch(e) {}
          }
        }
      }, C.CACHE_LOAD_TIMEOUT || 5000);

      this._restorePromise
        .then(() => {
          clearTimeout(restoreTimeout);
          this._restored = true;
          this._restoreDone = true;
          this._isRestoring = false;
          this._restoreFailed = false;
        })
        .catch(() => {
          clearTimeout(restoreTimeout);
          this._restored = true;
          this._restoreDone = true;
          this._restoreFailed = true;
          this._isRestoring = false;
          this._storageCache = { roomsData: {}, currentNumber: 1 };
          this._cacheInitialized = true;
          this.currentNumber = 1;
        });

    } catch(e) {
      this._restored = true;
      this._restoreDone = true;
      this._isRestoring = false;
      this._storageCache = { roomsData: {}, currentNumber: 1 };
      this._cacheInitialized = true;
      this.currentNumber = 1;
      this._restoreFailed = true;
      this.closing = false;
      this.isDestroyed = false;
      this.wsSet = new Set();
      this.userConnections = new Map();
      this.roomClients = new Map();
      this.wsActiveMulti = new Map();
      this._pendingEvents = [];
      this._eventQueue = [];
      this.db = null;
      this._onlineUsersCache = null;
      this._onlineUsersCacheTime = 0;
      this._roomCountsCache = null;
      this._roomCountsCacheTime = 0;
      for (const room of ROOMS) {
        this.roomClients.set(room, new Set());
      }
    }
  }

  async _restoreWithRetry() {
    let attempts = 0;
    let lastError = null;

    while (attempts < C.MAX_RESTORE_ATTEMPTS) {
      try {
        attempts++;
        const result = await this._restoreAllState();
        this._restoreAttempts = attempts;
        return result;
      } catch(e) {
        lastError = e;
        this._restoreAttempts = attempts;

        if (attempts < C.MAX_RESTORE_ATTEMPTS) {
          await new Promise(resolve => setTimeout(resolve, C.RESTORE_RETRY_DELAY_MS));
          this._isRestoring = true;
        }
      }
    }

    this._restored = true;
    this._restoreDone = true;
    this._restoreFailed = true;
    this._isRestoring = false;
    this._storageCache = { roomsData: {}, currentNumber: 1 };
    this._cacheInitialized = true;
    this.currentNumber = 1;

    throw lastError;
  }

  async _loadFromStorage() {
    try {
      if (!this.db) {
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this.currentNumber = 1;
        return this._storageCache;
      }

      try {
        await this.db.prepare(`
          CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
          )
        `).run();
      } catch(e) {}

      let result;
      try {
        result = await this.db
          .prepare(`SELECT key, value FROM ${TABLE_NAME}`)
          .all();
      } catch(e) {
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this.currentNumber = 1;
        return this._storageCache;
      }

      const roomsData = {};
      for (const room of ROOMS) {
        roomsData[room] = { seat: {}, point: {}, mute: false };
      }

      let currentNumber = 1;
      const results = result?.results || [];

      for (const row of results) {
        try {
          const key = row?.key;
          if (!key) continue;

          let value;
          try {
            value = JSON.parse(row.value);
          } catch(e) {
            continue;
          }

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

  async _saveSeat(roomName, seatNumber, seatData) {
    try {
      if (!this.db) return;
      const key = `seat_${roomName}_${seatNumber}`;
      if (!seatData || !seatData.namauser || seatData.namauser.trim() === '') {
        await this.db.prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`).bind(key).run();
        return;
      }
      await this.db
        .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
        .bind(key, JSON.stringify(seatData))
        .run();
    } catch(e) {}
  }

  async _savePoint(roomName, seatNumber, pointData) {
    try {
      if (!this.db) return;
      const key = `point_${roomName}_${seatNumber}`;
      if (!pointData || (pointData.x === 0 && pointData.y === 0 && !pointData.fast)) {
        await this.db.prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`).bind(key).run();
        return;
      }
      await this.db
        .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
        .bind(key, JSON.stringify(pointData))
        .run();
    } catch(e) {}
  }

  async _saveMute(roomName, muted) {
    try {
      if (!this.db) return;
      const key = `mute_${roomName}`;
      if (muted === false) {
        await this.db.prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`).bind(key).run();
        return;
      }
      await this.db
        .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
        .bind(key, JSON.stringify(muted))
        .run();
    } catch(e) {}
  }

  async _saveCurrentNumber() {
    try {
      if (!this.db) return;
      await this.db
        .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
        .bind('current_number', String(this._storageCache?.currentNumber || 1))
        .run();
    } catch(e) {}
  }

  // ============ 🔥 FORCE DELETE FROM D1 - HEMAT ============
  async _forceDeleteFromD1(roomName, seatNumber, username) {
    try {
      if (!this.db) return false;

      let seat = seatNumber;
      let room = roomName;

      // 🔧 FIX: Kalau room/seat tidak diketahui, ambil dari CACHE dulu (cepat)
      if ((!seat || !room) && username) {
        try {
          const found = await this._findUserInAnyRoom(username);
          if (found && found.isMulti !== true) {
            room = found.room;
            seat = found.seat;
          }
        } catch(e) {}
      }

      // LAPIS 1: Cari seat via SELECT — DIBATASI PER ROOM
      if (!seat && room && username) {
        try {
          const result = await this.db
            .prepare(`SELECT key, value FROM ${TABLE_NAME} WHERE key LIKE ?`)
            .bind(`seat_${room}_%`)
            .all();

          const rows = result?.results || [];
          for (const row of rows) {
            try {
              const val = JSON.parse(row.value);
              if (val?.namauser === username && val.isMulti !== true) {
                const parts = row.key.split('_');
                seat = parseInt(parts[2]);
                break;
              }
            } catch(e) {}
          }
        } catch(e) {}
      }

      if (!seat || !room) return false;

      // LAPIS 3: GUARD MULTI - cek dulu sebelum hapus
      try {
        const check = await this.db
          .prepare(`SELECT value FROM ${TABLE_NAME} WHERE key = ?`)
          .bind(`seat_${room}_${seat}`)
          .first();

        if (check?.value) {
          const val = JSON.parse(check.value);
          if (val?.isMulti === true) return false;
          if (username && val?.namauser && val.namauser !== username) return false;
        }
      } catch(e) {}

      // 🔥 EKSEKUSI HAPUS
      await this.db
        .prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`)
        .bind(`seat_${room}_${seat}`)
        .run();

      await this.db
        .prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`)
        .bind(`point_${room}_${seat}`)
        .run();

      return true;
    } catch(e) {
      return false;
    }
  }

  async _deleteSeatInRoom(roomName, seatNumber) {
    try {
      const roomBucket = await this._getRoomBucket(roomName);
      if (!roomBucket) return false;

      if (roomBucket.seat) delete roomBucket.seat[seatNumber];
      if (roomBucket.point) delete roomBucket.point[seatNumber];

      if (this.db) {
        try {
          await this.db
            .prepare(`DELETE FROM ${TABLE_NAME} WHERE key IN (?, ?)`)
            .bind(`seat_${roomName}_${seatNumber}`, `point_${roomName}_${seatNumber}`)
            .run();
        } catch(e) {}
      }

      this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
      await this.updateRoomCount(roomName);
      return true;
    } catch(e) {
      return false;
    }
  }

  async _getRoomBucket(roomName) {
    try {
      await this._ensureCacheInitialized();
      if (!this._storageCache?.roomsData) {
        this._storageCache = { roomsData: {}, currentNumber: 1 };
      }
      if (!this._storageCache.roomsData[roomName]) {
        this._storageCache.roomsData[roomName] = { seat: {}, point: {}, mute: false };
      }
      return this._storageCache.roomsData[roomName];
    } catch(e) {
      return { seat: {}, point: {}, mute: false };
    }
  }

  async _updateSeatInRoom(roomName, seatNumber, seatData) {
    try {
      const roomBucket = await this._getRoomBucket(roomName);
      if (!roomBucket) return false;

      if (!seatData || !seatData.namauser || seatData.namauser.trim() === '') {
        if (roomBucket.seat) delete roomBucket.seat[seatNumber];
        await this._saveSeat(roomName, seatNumber, null);
        return true;
      }
      if (!roomBucket.seat) roomBucket.seat = {};
      roomBucket.seat[seatNumber] = seatData;
      await this._saveSeat(roomName, seatNumber, seatData);
      return true;
    } catch(e) {
      return false;
    }
  }

  async _updatePointInRoom(roomName, seatNumber, pointData) {
    try {
      const roomBucket = await this._getRoomBucket(roomName);
      if (!roomBucket) return false;

      if (!pointData || (pointData.x === 0 && pointData.y === 0 && !pointData.fast)) {
        if (roomBucket.point) delete roomBucket.point[seatNumber];
        await this._savePoint(roomName, seatNumber, null);
        return true;
      }
      if (!roomBucket.point) roomBucket.point = {};
      roomBucket.point[seatNumber] = pointData;
      await this._savePoint(roomName, seatNumber, pointData);
      return true;
    } catch(e) {
      return false;
    }
  }

  async _updateMuteInRoom(roomName, muted) {
    try {
      const roomBucket = await this._getRoomBucket(roomName);
      if (!roomBucket) return false;
      roomBucket.mute = muted;
      await this._saveMute(roomName, muted);
      return true;
    } catch(e) {
      return false;
    }
  }

  async _getRoomData(roomName) {
    try {
      await this._ensureCacheInitialized();
      return this._storageCache?.roomsData?.[roomName] || null;
    } catch(e) {
      return null;
    }
  }

  async _getSeatData(roomName, seatNumber) {
    try {
      await this._ensureCacheInitialized();
      const roomBucket = this._storageCache?.roomsData?.[roomName];
      if (!roomBucket || !roomBucket.seat) return null;
      return roomBucket.seat[seatNumber] || null;
    } catch(e) {
      return null;
    }
  }

  async _getRoomCount(roomName) {
    try {
      await this._ensureCacheInitialized();
      const roomBucket = this._storageCache?.roomsData?.[roomName];
      if (!roomBucket || !roomBucket.seat) return 0;
      let count = 0;
      for (const seat in roomBucket.seat) {
        if (roomBucket.seat[seat]?.namauser) count++;
      }
      return count;
    } catch(e) {
      return 0;
    }
  }

  async _findUserInAnyRoom(username) {
    try {
      if (!username) return null;
      await this._ensureCacheInitialized();
      const roomsData = this._storageCache?.roomsData || {};
      for (const [roomName, roomBucket] of Object.entries(roomsData)) {
        if (!roomBucket?.seat) continue;
        for (const [seat, data] of Object.entries(roomBucket.seat)) {
          if (data?.namauser === username) {
            return { room: roomName, seat: parseInt(seat), isMulti: data.isMulti || false };
          }
        }
      }
      return null;
    } catch(e) {
      return null;
    }
  }

  async _ensureCacheInitialized() {
    try {
      if (this._cacheInitialized && this._storageCache) {
        return this._storageCache;
      }

      if (this._cacheLoading) {
        if (this._cacheLoadingPromise) {
          try {
            await this._cacheLoadingPromise;
          } catch(e) {}
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
      this._cacheLoadingPromise = this._loadFromStorage()
        .then(result => {
          this._storageCache = result;
          this._cacheInitialized = true;
          return result;
        })
        .catch(e => {
          if (!this._cacheInitialized) {
            this._storageCache = { roomsData: {}, currentNumber: 1 };
            this._cacheInitialized = true;
          }
          return this._storageCache;
        })
        .finally(() => {
          this._cacheLoading = false;
          this._cacheLoadingPromise = null;
        });

      try {
        await this._cacheLoadingPromise;
      } catch(e) {}

      return this._storageCache || { roomsData: {}, currentNumber: 1 };
    } catch(e) {
      return { roomsData: {}, currentNumber: 1 };
    }
  }

  async _withLock(lockMap, key, fn, timeout = C.LOCK_TIMEOUT) {
    try {
      if (!lockMap) return await fn();

      const start = Date.now();
      let waited = 0;

      while (lockMap.has(key) && waited < timeout) {
        await new Promise(resolve => setTimeout(resolve, 50));
        waited += 50;
      }

      if (lockMap.has(key)) {
        lockMap.delete(key);
      }

      lockMap.set(key, Date.now());
      try {
        const result = await fn();
        return result;
      } finally {
        lockMap.delete(key);
      }
    } catch(e) {
      if (lockMap && lockMap.has(key)) lockMap.delete(key);
      throw e;
    }
  }

  async _updateKursi(roomName, seat, data) {
    try {
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

      const roomBucket = this._storageCache?.roomsData?.[roomName];
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
    } catch(e) {
      return { success: false, error: e.message };
    }
  }

  async _updatePointDirect(roomName, seat, x, y, fast) {
    try {
      await this._ensureCacheInitialized();
      if (!this._storageCache) {
        this._storageCache = { roomsData: {}, currentNumber: 1 };
      }
      if (!this._storageCache.roomsData[roomName]) {
        this._storageCache.roomsData[roomName] = { seat: {}, point: {}, mute: false };
      }
      const pointData = { x: x || 0, y: y || 0, fast: !!fast };
      this._storageCache.roomsData[roomName].point[seat] = pointData;
      await this._savePoint(roomName, seat, pointData);
      return true;
    } catch(e) {
      return false;
    }
  }

  async _removeUserFromRoom(username, roomName) {
    try {
      if (!username || !roomName) return false;
      await this._ensureCacheInitialized();
      const roomBucket = this._storageCache?.roomsData?.[roomName];
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
    } catch(e) {
      return false;
    }
  }

  async _handleJoin(ws, roomName) {
    try {
      if (!ws?.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
        return false;
      }

      const username = ws.username;
      const lockKey = `join_user_${username}`;

      try {
        return await this._withLock(
          this._userJoinLock,
          lockKey,
          () => this._joinInternal(ws, roomName, username),
          C.USER_JOIN_LOCK_TIMEOUT
        );
      } catch(e) {
        return false;
      }
    } catch(e) {
      return false;
    }
  }

  async _joinInternal(ws, roomName, username) {
    try {
      const existing = await this._findUserInAnyRoom(username);
      if (existing && existing.room !== roomName) {
        await this._removeUserFromRoom(username, existing.room);
      }

      await this._ensureCacheInitialized();

      let roomBucket = this._storageCache?.roomsData?.[roomName];
      if (!roomBucket) {
        roomBucket = { seat: {}, point: {}, mute: false };
        if (!this._storageCache) this._storageCache = { roomsData: {}, currentNumber: 1 };
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

      try {
        ws.serializeAttachment({
          username: username,
          seatInfo: { room: roomName, seat: seat }
        });
      } catch(e) {}

      for (const [otherRoom, clients] of this.roomClients) {
        if (otherRoom !== roomName && clients) {
          try { clients.delete(ws); } catch(e) {}
        }
      }
      const roomClients = this.roomClients.get(roomName);
      if (roomClients && !roomClients.has(ws)) {
        try { roomClients.add(ws); } catch(e) {}
      }

      this.wsActiveMulti.delete(ws);

      const muteStatus = roomBucket.mute || false;

      this.safeSend(ws, ["rooMasuk", seat, roomName]);
      this.safeSend(ws, ["numberKursiSaya", seat]);
      this.safeSend(ws, ["muteTypeResponse", muteStatus, roomName]);

      await this.updateRoomCount(roomName);

      // ✅ BIARKAN: setTimeout 1000ms untuk UI — 1x per join, bukan masalah
      setTimeout(() => {
        try {
          if (ws?.readyState === 1) {
            this.sendAllStateTo(ws, roomName, true);
          }
        } catch(e) {}
      }, 1000);

      return true;
    } catch(e) {
      return false;
    }
  }

  // ============ MULTI - ⚠️ TIDAK DIUBAH ============
  async _handleMultiJoin(ws, multiUsername, multiRoomname) {
    try {
      if (!multiUsername || !multiRoomname || !ROOMS_SET.has(multiRoomname)) return false;
      await this._ensureCacheInitialized();

      const existing = await this._findUserInAnyRoom(multiUsername);
      if (existing && !existing.isMulti) {
        await this._removeUserFromRoom(multiUsername, existing.room);
      }

      let roomBucket = this._storageCache?.roomsData?.[multiRoomname];
      if (!roomBucket) {
        roomBucket = { seat: {}, point: {}, mute: false };
        if (!this._storageCache) this._storageCache = { roomsData: {}, currentNumber: 1 };
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
    } catch(e) {
      return false;
    }
  }

  // ============ 🔥 CLEANUP - FIX RACE CONDITION ============
  async _cleanupUserCompletely(ws) {
    try {
      if (!ws) return;

      let state = _wsCleanupState.get(ws);
      if (!state) {
        state = {
          cleanupDone: false,
          cleaning: false,
          cleanupStart: null
        };
        try { _wsCleanupState.set(ws, state); } catch(e) {}
      }

      // 🔧 FIX: guard cleaning untuk cegah eksekusi paralel
      if (state.cleanupDone || state.cleaning) return;
      state.cleaning = true;
      state.cleanupStart = Date.now();

      try {
        const username = ws.username || ws._username;
        const roomName = ws.room || ws.roomname || ws._room;
        const isMulti = this.wsActiveMulti?.has(ws) || false;

        if (!username && !isMulti) {
          if (this.wsSet) try { this.wsSet.delete(ws); } catch(e) {}
          return;
        }

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
          // ⚠️ MULTI - TIDAK DIUBAH
          if (username) {
            const connections = this.userConnections?.get(username);
            if (connections) {
              try { connections.delete(ws); } catch(e) {}
              if (connections.size === 0) {
                try { this.userConnections.delete(username); } catch(e) {}
              }
            }
          }

          if (actualRoom) {
            const roomClients = this.roomClients?.get(actualRoom);
            if (roomClients) try { roomClients.delete(ws); } catch(e) {}
          }
        } else {
          // USER NORMAL

          let isActuallyMulti = false;
          try {
            if (this.wsActiveMulti?.has(ws)) {
              isActuallyMulti = true;
            }
            if (username && actualRoom) {
              const roomBucket = await this._getRoomBucket(actualRoom);
              if (roomBucket?.seat) {
                for (const [s, data] of Object.entries(roomBucket.seat)) {
                  if (data?.namauser === username && data.isMulti === true) {
                    isActuallyMulti = true;
                    break;
                  }
                }
              }
            }
          } catch(e) {}

          if (isActuallyMulti) {
            if (username) {
              const connections = this.userConnections?.get(username);
              if (connections) {
                try { connections.delete(ws); } catch(e) {}
                if (connections.size === 0) {
                  try { this.userConnections.delete(username); } catch(e) {}
                }
              }
            }
            if (actualRoom) {
              const roomClients = this.roomClients?.get(actualRoom);
              if (roomClients) try { roomClients.delete(ws); } catch(e) {}
            }
          } else {
            let roomToClean = actualRoom;
            let seatToClean = actualSeat;

            if (!roomToClean && username) {
              try {
                const found = await this._findUserInAnyRoom(username);
                if (found && found.isMulti !== true) {
                  roomToClean = found.room;
                  seatToClean = found.seat;
                }
              } catch(e) {}
            }

            if (roomToClean && !seatToClean) {
              try {
                const roomBucket = await this._getRoomBucket(roomToClean);
                if (roomBucket?.seat) {
                  for (const [s, data] of Object.entries(roomBucket.seat)) {
                    if (data?.namauser === username && data.isMulti !== true) {
                      seatToClean = parseInt(s);
                      break;
                    }
                  }
                }
              } catch(e) {}
            }

            // STEP 1: HAPUS D1 PALING PERTAMA
            await this._forceDeleteFromD1(roomToClean, seatToClean, username);

            // STEP 2: Hapus dari CACHE
            if (roomToClean && seatToClean) {
              try {
                const roomBucket = await this._getRoomBucket(roomToClean);
                if (roomBucket?.seat) delete roomBucket.seat[seatToClean];
                if (roomBucket?.point) delete roomBucket.point[seatToClean];
              } catch(e) {}
            }

            // STEP 3: Hapus dari userConnections
            if (username) {
              const connections = this.userConnections?.get(username);
              if (connections) {
                try { connections.delete(ws); } catch(e) {}
                if (connections.size === 0) {
                  try { this.userConnections.delete(username); } catch(e) {}
                }
              }
            }

            // STEP 4: Hapus dari roomClients
            if (roomToClean) {
              const roomClients = this.roomClients?.get(roomToClean);
              if (roomClients) try { roomClients.delete(ws); } catch(e) {}
            } else if (username) {
              try {
                const found = await this._findUserInAnyRoom(username);
                if (found) {
                  const roomClients = this.roomClients?.get(found.room);
                  if (roomClients) try { roomClients.delete(ws); } catch(e) {}
                }
              } catch(e) {}
            }

            // STEP 5: Hapus dari wsSet & wsActiveMulti
            if (this.wsSet) try { this.wsSet.delete(ws); } catch(e) {}
            if (this.wsActiveMulti) try { this.wsActiveMulti.delete(ws); } catch(e) {}

            // STEP 6: Broadcast removeKursi
            if (roomToClean && seatToClean) {
              this.broadcast(roomToClean, ["removeKursi", roomToClean, seatToClean]);
              await this.updateRoomCount(roomToClean);
            }
          }
        }

        if (isMulti) {
          if (this.wsSet) try { this.wsSet.delete(ws); } catch(e) {}
          if (this.wsActiveMulti) try { this.wsActiveMulti.delete(ws); } catch(e) {}
        }

        try {
          ws._closing = false;
          ws._cleaning = false;
        } catch(e) {}

      } catch(e) {} finally {
        state.cleanupDone = true;
        state.cleaning = false;
        state.cleanupStart = null;
      }
    } catch(e) {}
  }

  async webSocketClose(ws) {
    try {
      if (!ws || this.isDestroyed) return;

      if (!ws._username && ws.username) ws._username = ws.username;
      if (!ws._room && (ws.room || ws.roomname)) ws._room = ws.room || ws.roomname;

      const state = _wsCleanupState.get(ws);
      if (state && state.cleanupDone) return;

      await this._cleanupUserCompletely(ws);
    } catch(e) {}
  }

  async webSocketError(ws) {
    try {
      if (!ws || this.isDestroyed) return;

      if (!ws._username && ws.username) ws._username = ws.username;
      if (!ws._room && (ws.room || ws.roomname)) ws._room = ws.room || ws.roomname;

      const state = _wsCleanupState.get(ws);
      if (state && state.cleanupDone) return;

      await this._cleanupUserCompletely(ws);
    } catch(e) {}
  }

  async webSocketMessage(ws, msg) {
    try {
      if (!ws || ws._closing || this.closing || this.isDestroyed || ws._cleaning) return;

      let attachment = null;
      try {
        attachment = ws.deserializeAttachment ? ws.deserializeAttachment() : null;
      } catch(e) {
        attachment = null;
      }

      if (!this._restored || this._isRestoring) {
        if (!this._pendingEvents) this._pendingEvents = [];

        if (this._pendingEvents.length >= C.MAX_PENDING_EVENTS) {
          try {
            this.safeSend(ws, ["error", "Server busy"]);
          } catch(e) {}
          return;
        }

        this._pendingEvents.push({
          ws,
          message: msg,
          timestamp: Date.now(),
          wsId: ws._wsId || Date.now(),
          attachment: attachment
        });

        return;
      }

      try {
        await this.handleMessage(ws, msg);
      } catch(e) {
        this._handleError('webSocketMessage', e);
      }
    } catch(e) {
      this._handleError('webSocketMessage', e);
    }
  }

  // 🔧 FIX: guard _processingPending untuk cegah paralel
  async _processPendingEvents() {
    try {
      if (!this._pendingEvents || this._pendingEvents.length === 0) return;
      if (this._processingPending) return;

      this._processingPending = true;
      try {
        while (this._pendingEvents.length > 0 && !this.closing && !this.isDestroyed) {
          const events = this._pendingEvents.splice(0, 20);
          if (events.length === 0) break;

          for (const evt of events) {
            try {
              let ws = evt.ws;

              if (!ws || ws.readyState !== 1) {
                const wsId = evt.wsId;
                if (wsId) {
                  let found = false;
                  for (const w of (this.wsSet || new Set())) {
                    if (w._wsId === wsId || w === ws) {
                      ws = w;
                      found = true;
                      break;
                    }
                  }
                  if (!found) continue;
                } else {
                  continue;
                }
              }

              if (!ws || ws.readyState !== 1 || ws._closing || ws._cleaning) continue;

              try {
                if (evt.attachment) {
                  ws.serializeAttachment(evt.attachment);
                  if (evt.attachment.username) {
                    ws.username = evt.attachment.username;
                    ws._username = evt.attachment.username;
                  }
                  if (evt.attachment.room) {
                    ws.room = evt.attachment.room;
                    ws._room = evt.attachment.room;
                  }
                }
              } catch(e) {}

              try {
                await this.handleMessage(ws, evt.message);
              } catch(e) {
                this._handleError('processPendingEvent', e);
              }
            } catch(e) {
              this._handleError('processPendingEvent', e);
            }
          }
        }
      } finally {
        this._processingPending = false;
      }
    } catch(e) {
      this._handleError('_processPendingEvents', e);
    }
  }

  async _processEventQueue() {
    try {
      if (this._processingQueue || this._eventQueue.length === 0) return;
      this._processingQueue = true;

      try {
        const startTime = Date.now();
        let processed = 0;
        const MAX_BATCH = C.PROCESS_BATCH_SIZE || 50;
        const MAX_TIME = C.PROCESS_MAX_TIME_MS || 100;

        while (this._eventQueue.length > 0 && processed < MAX_BATCH) {
          if (Date.now() - startTime > MAX_TIME) break;
          const item = this._eventQueue.shift();
          try {
            await this.handleMessage(item.ws, item.message);
          } catch(e) {
            this._handleError('processQueue', e);
          }
          processed++;
        }

        if (this._eventQueue.length > 0 && !this.closing && !this.isDestroyed) {
          // ✅ BIARKAN: self-reschedule queue, hanya jalan kalau ada isi
          setTimeout(() => this._processEventQueue(), 10);
        }
      } finally {
        this._processingQueue = false;
      }
    } catch(e) {
      this._handleError('_processEventQueue', e);
    }
  }

  broadcast(room, msg) {
    try {
      if (this.closing || this.isDestroyed || !room || !msg) return 0;
      const msgStr = JSON.stringify(msg);
      const clients = this.roomClients?.get(room);
      if (!clients || clients.size === 0) return 0;

      const toRemove = new Set();
      let sentCount = 0;

      for (const ws of clients) {
        if (!ws) {
          try { toRemove.add(ws); } catch(e) {}
          continue;
        }

        const state = _wsCleanupState.get(ws);
        if (state && state.cleanupDone) {
          try { toRemove.add(ws); } catch(e) {}
          continue;
        }

        const wsRoom = ws.room || ws.roomname;
        if (wsRoom !== room) {
          try { toRemove.add(ws); } catch(e) {}
          continue;
        }

        try {
          if (ws.readyState === 1 && !ws._closing && !ws._cleaning) {
            ws.send(msgStr);
            sentCount++;
          } else {
            try { toRemove.add(ws); } catch(e) {}
          }
        } catch(e) {
          try { toRemove.add(ws); } catch(e) {}
        }
      }

      if (toRemove.size > 0) {
        for (const ws of toRemove) {
          try {
            clients.delete(ws);
          } catch(e) {}
        }
      }

      return sentCount;
    } catch(e) {
      return 0;
    }
  }

  safeSend(ws, msg) {
    try {
      if (!ws) return false;
      if (ws.readyState !== 1 || ws._closing || ws._cleaning || this.closing || this.isDestroyed) {
        return false;
      }
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      return false;
    }
  }

  async updateRoomCount(room) {
    try {
      if (this.closing || this.isDestroyed || !room) return 0;

      // ✅ HEMAT: invalidate cache room counts
      this._roomCountsCache = null;
      this._roomCountsCacheTime = 0;

      const count = await this._getRoomCount(room);
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) {
      return 0;
    }
  }

  async sendAllStateTo(ws, room, excludeSelf = false) {
    try {
      if (!ws?.username) return;

      try {
        if (ws.readyState !== 1 || ws._closing || ws._cleaning) {
          return;
        }
      } catch(e) {
        return;
      }

      await this._ensureCacheInitialized();
      const roomBucket = this._storageCache?.roomsData?.[room];
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
    } catch(e) {}
  }

  async alarm() {
    try {
      if (this.closing || this.isDestroyed) return;

      if (!this._restored && this._restorePromise) {
        try {
          await Promise.race([
            this._restorePromise,
            new Promise(resolve => setTimeout(resolve, C.CACHE_LOAD_TIMEOUT))
          ]);
        } catch(e) {}
      }

      await this._updateNumber();
      try {
        this.ctx?.storage?.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      } catch(e) {}
    } catch(e) {
      this._handleError('alarm', e);
    }
  }

  async _updateNumber() {
    try {
      if (this._isNumberUpdating || this.closing || this.isDestroyed) return;
      this._isNumberUpdating = true;
      try {
        this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
        if (this._storageCache) {
          this._storageCache.currentNumber = this.currentNumber;
        }
        await this._saveCurrentNumber();
        for (const [room, clients] of (this.roomClients || new Map())) {
          if (clients?.size > 0) {
            this.broadcast(room, ["currentNumber", this.currentNumber]);
          }
        }
      } catch(e) {} finally {
        this._isNumberUpdating = false;
      }
    } catch(e) {}
  }

  async _restoreAllState() {
    try {
      this._isRestoring = true;

      try {
        await this._loadFromStorage();
        await this._ensureCacheInitialized();
      } catch(e) {
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this.currentNumber = 1;
      }

      try {
        const webSockets = this.ctx?.getWebSockets?.() || [];

        const batchSize = 10;
        for (let i = 0; i < webSockets.length; i += batchSize) {
          const batch = webSockets.slice(i, i + batchSize);
          await Promise.allSettled(
            batch.map(ws => this._restoreSingleWebSocket(ws))
          );
        }

        for (const ws of webSockets) {
          try {
            const state = _wsCleanupState.get(ws);
            if (state) {
              state.cleanupDone = false;
              state.cleaning = false;
              state.cleanupStart = null;
            } else {
              _wsCleanupState.set(ws, {
                cleanupDone: false,
                cleaning: false,
                cleanupStart: null
              });
            }

            ws._closing = false;
            ws._cleaning = false;

            const attachment = ws.deserializeAttachment();
            if (attachment?.username) {
              const found = await this._findUserInAnyRoom(attachment.username);
              if (found) {
                ws.username = attachment.username;
                ws.room = found.room;
                ws.roomname = found.room;
                ws._username = attachment.username;
                ws._room = found.room;
              }
            }
          } catch(e) {}
        }
      } catch(e) {}

      if (!this.closing && !this.isDestroyed) {
        try {
          await this.ctx?.storage?.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
        } catch(e) {}
      }

      this._restored = true;
      this._restoreDone = true;
      this._restoreFailed = false;
      this._isRestoring = false;

      await this._processPendingEvents();

      return true;

    } catch(e) {
      this._restored = true;
      this._restoreDone = true;
      this._restoreFailed = true;
      this._isRestoring = false;
      this._storageCache = { roomsData: {}, currentNumber: 1 };
      this._cacheInitialized = true;
      this.currentNumber = 1;

      if (!this.closing && !this.isDestroyed) {
        try {
          await this.ctx?.storage?.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
        } catch(e2) {}
      }

      await this._processPendingEvents();

      throw e;
    }
  }

  async _restoreSingleWebSocket(ws) {
    try {
      const attachment = ws.deserializeAttachment();
      if (!attachment?.username) return;

      const found = await this._findUserInAnyRoom(attachment.username);
      if (!found) {
        try {
          ws.serializeAttachment({});
          ws.username = null;
          ws.room = null;
          ws.roomname = null;
          ws.idtarget = null;
          ws._username = null;
          ws._room = null;
          _wsCleanupState.delete(ws);
          this.wsSet?.delete(ws);
        } catch(e) {}
        return;
      }

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
        state.cleanupDone = false;
        state.cleaning = false;
        state.cleanupStart = null;
      } else {
        _wsCleanupState.set(ws, {
          cleanupDone: false,
          cleaning: false,
          cleanupStart: null
        });
      }

      const roomClients = this.roomClients?.get(found.room);
      if (roomClients && !roomClients.has(ws)) {
        try { roomClients.add(ws); } catch(e) {}
      }

      let conns = this.userConnections?.get(attachment.username);
      if (!conns) {
        conns = new Set();
        try { this.userConnections?.set(attachment.username, conns); } catch(e) {}
      }
      if (!conns.has(ws)) {
        try { conns.add(ws); } catch(e) {}
      }

      if (!this.wsSet?.has(ws)) {
        try { this.wsSet?.add(ws); } catch(e) {}
      }

      try {
        ws.serializeAttachment({
          username: attachment.username,
          seatInfo: found
        });
      } catch(e) {}

    } catch(e) {}
  }

  async _handleSetId(ws, username, isNewUser) {
    try {
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
      try { ws.serializeAttachment({ username: username }); } catch(e) {}
      let connections = this.userConnections?.get(username);
      if (!connections) {
        connections = new Set();
        try { this.userConnections?.set(username, connections); } catch(e) {}
      }
      if (!connections.has(ws)) try { connections.add(ws); } catch(e) {}
      if (!this.wsSet?.has(ws)) try { this.wsSet?.add(ws); } catch(e) {}
      try { this.wsActiveMulti?.delete(ws); } catch(e) {}
      if (isNewUser) {
        this.safeSend(ws, ["joinroomawal"]);
      } else {
        this.safeSend(ws, ["needJoinRoom"]);
      }
    } catch(e) {
      this._handleError('_handleSetId', e);
    }
  }

  async handleMessage(ws, raw) {
    try {
      if (!ws) return;

      const state = _wsCleanupState.get(ws);
      if (state && state.cleanupDone) {
        return;
      }

      if (ws._cleaning || ws._closing) {
        return;
      }

      if (!this._restored && this._restorePromise) {
        try {
          await Promise.race([
            this._restorePromise,
            new Promise(resolve => setTimeout(resolve, C.CACHE_LOAD_TIMEOUT))
          ]);
        } catch(e) {
          return;
        }
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

          if (state && state.cleanupDone) {
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
      } catch(e) {
        this._handleError('handleMessage', e);
      }
    } catch(e) {
      this._handleError('handleMessage', e);
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
          const result = await this._handleMultiJoin(ws, multiUsername, multiRoomname);
          if (!result) break;
          const { room, seat } = result;
          let connections = this.userConnections?.get(multiUsername);
          if (!connections) connections = new Set();
          if (!connections.has(ws)) try { connections.add(ws); } catch(e) {}
          try { this.userConnections?.set(multiUsername, connections); } catch(e) {}
          try {
            ws.serializeAttachment({
              username: multiUsername,
              seatInfo: { room: room, seat: seat }
            });
          } catch(e) {}
          ws._username = multiUsername;
          ws._room = room;
          try { this.wsActiveMulti?.set(ws, { username: multiUsername, room: room }); } catch(e) {}
          for (const [otherRoom, clients] of (this.roomClients || new Map())) {
            if (otherRoom !== room && clients) {
              try { clients.delete(ws); } catch(e) {}
            }
          }
          const roomClients = this.roomClients?.get(room);
          if (roomClients && !roomClients.has(ws)) try { roomClients.add(ws); } catch(e) {}
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
            const connections = this.userConnections?.get(targetUsername);
            if (connections) {
              const toRemove = Array.from(connections);
              for (const conn of toRemove) {
                if (conn.room) {
                  const rc = this.roomClients?.get(conn.room);
                  if (rc) try { rc.delete(conn); } catch(e) {}
                }
                if (roomName) {
                  const rc = this.roomClients?.get(roomName);
                  if (rc) try { rc.delete(conn); } catch(e) {}
                }
                try { this.wsActiveMulti?.delete(conn); } catch(e) {}
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
                try { this.wsSet?.delete(conn); } catch(e) {}
              }
              try { this.userConnections?.delete(targetUsername); } catch(e) {}
            }
            const toDelete = [];
            for (const [wsKey, data] of (this.wsActiveMulti || new Map())) {
              if (data?.username === targetUsername) {
                toDelete.push(wsKey);
                if (data.room) {
                  const rc = this.roomClients?.get(data.room);
                  if (rc) try { rc.delete(wsKey); } catch(e) {}
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
                try { this.wsSet?.delete(wsKey); } catch(e) {}
              }
            }
            for (const wsKey of toDelete) {
              try { this.wsActiveMulti?.delete(wsKey); } catch(e) {}
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
          for (const [wsKey, data] of (this.wsActiveMulti || new Map())) {
            if (data?.username === targetUsername) {
              existingWs = wsKey;
              break;
            }
          }
          if (existingWs && existingWs !== ws) {
            const oldRoom = this.wsActiveMulti?.get(existingWs)?.room;
            if (oldRoom) {
              const rc = this.roomClients?.get(oldRoom);
              if (rc) try { rc.delete(existingWs); } catch(e) {}
            }
            const conns = this.userConnections?.get(targetUsername);
            if (conns) {
              try { conns.delete(existingWs); } catch(e) {}
              if (conns.size === 0) {
                try { this.userConnections?.delete(targetUsername); } catch(e) {}
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
            try { this.wsSet?.delete(existingWs); } catch(e) {}
            try { this.wsActiveMulti?.delete(existingWs); } catch(e) {}
            try {
              if (existingWs.readyState === 1) {
                existingWs.close(1000, "Replaced by new connection");
              }
            } catch(e) {}
          }
          try { this.wsActiveMulti?.set(ws, { username: targetUsername, room: roomName }); } catch(e) {}
          for (const [otherRoom, clients] of (this.roomClients || new Map())) {
            if (otherRoom !== roomName && clients) {
              try { clients.delete(ws); } catch(e) {}
            }
          }
          const roomClients = this.roomClients?.get(roomName);
          if (roomClients && !roomClients.has(ws)) try { roomClients.add(ws); } catch(e) {}
          ws.username = targetUsername;
          ws.idtarget = targetUsername;
          ws.room = roomName;
          ws.roomname = roomName;
          ws._username = targetUsername;
          ws._room = roomName;
          try {
            ws.serializeAttachment({
              username: targetUsername,
              seatInfo: found
            });
          } catch(e) {}
          let connections = this.userConnections?.get(targetUsername);
          if (!connections) {
            connections = new Set();
            try { this.userConnections?.set(targetUsername, connections); } catch(e) {}
          }
          if (!connections.has(ws)) try { connections.add(ws); } catch(e) {}
          if (!this.wsSet?.has(ws)) try { this.wsSet?.add(ws); } catch(e) {}

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

          try {
            await this._withLock(
              this._kursiLocks,
              `kursi_${kursiRoom}_${kursiSeat}`,
              async () => {
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
                return result;
              },
              C.LOCK_TIMEOUT
            );
          } catch(e) {}
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
            const targetConns = this.userConnections?.get(privTarget);
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
              const targetConns = this.userConnections?.get(notifTarget);
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
              const connections = this.userConnections?.get(onlineTarget);
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

        // ✅ HEMAT: getOnlineUsers pakai cache 5 detik
        case "getOnlineUsers": {
          const now = Date.now();
          if (this._onlineUsersCache && (now - this._onlineUsersCacheTime) < 5000) {
            this.safeSend(ws, ["allOnlineUsers", this._onlineUsersCache]);
            break;
          }

          const users = [];
          const seen = new Set();
          await this._ensureCacheInitialized();
          const roomsData = this._storageCache?.roomsData || {};
          for (const [roomName, roomBucket] of Object.entries(roomsData)) {
            if (!roomBucket?.seat) continue;
            for (const [seat, data] of Object.entries(roomBucket.seat)) {
              if (data?.namauser) {
                const username = data.namauser;
                if (seen.has(username)) continue;
                let isOnline = false;
                if (data.isMulti === true) {
                  isOnline = true;
                } else {
                  const connections = this.userConnections?.get(username);
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
                  seen.add(username);
                }
              }
            }
          }

          this._onlineUsersCache = users;
          this._onlineUsersCacheTime = now;

          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }

        // ✅ HEMAT: getAllRoomsUserCount pakai cache 3 detik
        case "getAllRoomsUserCount": {
          const now = Date.now();
          if (this._roomCountsCache && (now - this._roomCountsCacheTime) < 3000) {
            this.safeSend(ws, ["allRoomsUserCount", this._roomCountsCache]);
            break;
          }

          await this._ensureCacheInitialized();
          const counts = {};
          for (const room of ROOMS) {
            const roomBucket = this._storageCache?.roomsData?.[room];
            let count = 0;
            if (roomBucket?.seat) {
              for (const seat in roomBucket.seat) {
                if (roomBucket.seat[seat]?.namauser) count++;
              }
            }
            counts[room] = count;
          }

          const entries = Object.entries(counts);
          this._roomCountsCache = entries;
          this._roomCountsCacheTime = now;

          this.safeSend(ws, ["allRoomsUserCount", entries]);
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
            const roomBucket = this._storageCache?.roomsData?.[getMuteRoom];
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
            const roomBucket = this._storageCache?.roomsData?.[resetRoomName];
            if (roomBucket) {
              roomBucket.seat = {};
              roomBucket.point = {};
              roomBucket.mute = false;
              if (this.db) {
                try {
                  await this.db
                    .prepare(`DELETE FROM ${TABLE_NAME} WHERE key LIKE ? OR key LIKE ? OR key = ?`)
                    .bind(`seat_${resetRoomName}_%`, `point_${resetRoomName}_%`, `mute_${resetRoomName}`)
                    .run();
                } catch(e) {}
              }
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
    } catch(e) {
      this._handleError('_handleEventInternal', e);
    }
  }

  // ✅ HEMAT: fetch() tanpa setTimeout rate limit
  async fetch(req) {
    try {
      if (this.closing || this.isDestroyed) {
        return new Response("Shutting down", { status: 503 });
      }

      if (this._circuitOpen) {
        const now = Date.now();
        if (now - this._lastResetTime > 60000) {
          this._circuitOpen = false;
          this._requestCount = 0;
          this._lastResetTime = now;
          this._lastRequestDecay = now;
        } else {
          return new Response("Service temporarily unavailable", {
            status: 503,
            headers: { 'Retry-After': '30', 'Content-Type': 'text/plain' }
          });
        }
      }

      // 🔧 FIX: decay rate limit berbasis waktu — TANPA setTimeout
      // Decay 1 per detik. Counter reset penuh dalam 100 detik kalau idle.
      const now = Date.now();
      const elapsed = now - this._lastRequestDecay;
      if (elapsed > 1000) {
        const elapsedSeconds = Math.floor(elapsed / 1000);
        this._requestCount = Math.max(0, this._requestCount - elapsedSeconds);
        this._lastRequestDecay = now;
      }

      this._requestCount++;
      if (this._requestCount > C.RATE_LIMIT_MAX) {
        this._circuitOpen = true;
        this._lastResetTime = now;
        return new Response("Rate limit exceeded", {
          status: 429,
          headers: { 'Retry-After': '60', 'Content-Type': 'text/plain' }
        });
      }

      // ❌ DIHAPUS: setTimeout(() => { this._requestCount -= 50; }, 60000);
      // ✅ Diganti dengan logika decay di atas

      if (!this._restored && this._restorePromise) {
        try {
          await Promise.race([
            this._restorePromise,
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error('Restore timeout')), C.CACHE_LOAD_TIMEOUT)
            )
          ]);
        } catch(e) {
          if (!this._cacheInitialized) {
            this._storageCache = { roomsData: {}, currentNumber: 1 };
            this._cacheInitialized = true;
            this.currentNumber = 1;
            this._restored = true;
            this._restoreDone = true;
            this._restoreFailed = true;
            this._isRestoring = false;
          }
        }
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

        if ((this.wsSet?.size || 0) >= C.MAX_GLOBAL_CONNECTIONS) {
          return new Response("Server full", { status: 503 });
        }

        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        try { this.ctx?.acceptWebSocket(server); }
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
        try { server.serializeAttachment({}); } catch(e) {}

        try {
          _wsCleanupState.set(server, {
            cleanupDone: false,
            cleaning: false,
            cleanupStart: null
          });
        } catch(e) {}

        if (!this.wsSet?.has(server)) try { this.wsSet?.add(server); } catch(e) {}

        return new Response(null, { status: 101, webSocket: client });
      } catch(e) {
        this._handleError('fetch', e);
        return new Response("Internal Server Error", { status: 500 });
      }
    } catch(e) {
      this._handleError('fetch', e);
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  _handleError(type, error) {
    try {
      const now = Date.now();
      if (now - this._lastErrorReset > C.ERROR_RESET_INTERVAL_MS) {
        this._errorCount = 0;
        this._lastErrorReset = now;
      }
      this._errorCount++;
      if (this._errorCount > 20) {
        this._circuitOpen = true;
        this._lastResetTime = now;
      }
    } catch(e) {}
  }

  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.closing = true;
      this.isDestroyed = true;

      if (this._joinLocks) this._joinLocks.clear();
      if (this._kursiLocks) this._kursiLocks.clear();
      if (this._userJoinLock) this._userJoinLock.clear();
      if (this._pendingEvents) this._pendingEvents = [];
      if (this._eventQueue) this._eventQueue = [];

      for (const timer of (this._allTimers || new Set())) {
        try { clearTimeout(timer); } catch(e) {}
      }
      if (this._allTimers) this._allTimers.clear();

      const wsCopy = Array.from(this.wsSet || new Set());
      const cleanupPromises = [];

      for (const ws of wsCopy) {
        if (!ws) continue;

        const isMulti = this.wsActiveMulti?.has(ws) || false;

        try {
          if (ws.readyState === 1) {
            ws.close(1000, "Shutdown");
          }
        } catch(e) {}

        if (isMulti) {
          try { this.wsSet?.delete(ws); } catch(e) {}
          try { this.wsActiveMulti?.delete(ws); } catch(e) {}
          const room = ws.room || ws.roomname;
          if (room) {
            const rc = this.roomClients?.get(room);
            if (rc) try { rc.delete(ws); } catch(e) {}
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
          try { _wsCleanupState.delete(ws); } catch(e) {}
        } else {
          cleanupPromises.push(
            this._cleanupUserCompletely(ws).catch(() => {})
          );
        }
      }

      if (cleanupPromises.length > 0) {
        await Promise.race([
          Promise.allSettled(cleanupPromises),
          new Promise(resolve => setTimeout(resolve, 3000))
        ]);
      }

      if (this.userConnections) {
        for (const [username, conns] of this.userConnections) {
          const toRemove = [];
          for (const conn of conns) {
            if (!conn || conn.readyState !== 1) {
              toRemove.push(conn);
            }
          }
          for (const conn of toRemove) {
            try { conns.delete(conn); } catch(e) {}
          }
          if (conns.size === 0) {
            let isMultiUser = false;
            if (this.wsActiveMulti) {
              for (const [wsKey, data] of this.wsActiveMulti) {
                if (data?.username === username) {
                  isMultiUser = true;
                  break;
                }
              }
            }
            if (!isMultiUser) {
              try { this.userConnections.delete(username); } catch(e) {}
            }
          }
        }
      }

      if (this.roomClients) {
        for (const [room, clients] of this.roomClients) {
          try { clients.clear(); } catch(e) {}
        }
      }
    } catch(e) {}
  }
}

export default ChatServer;
