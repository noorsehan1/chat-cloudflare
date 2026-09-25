const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 500000,
  NUMBER_INTERVAL_MS: 15 * 60 * 1000,
  MAX_NUMBER: 6,
  LOCK_TIMEOUT: 5000,
  USER_JOIN_LOCK_TIMEOUT: 10000,
  ROOM_LOCK_TIMEOUT: 15000,
  GLOBAL_LOCK_TIMEOUT: 20000,
  JOIN_COOLDOWN_MS: 800,
  CACHE_LOAD_TIMEOUT: 15000,
  MAX_PENDING_EVENTS: 100,
  MAX_EVENT_QUEUE_SIZE: 50,
  PROCESS_BATCH_SIZE: 50,
  PROCESS_MAX_TIME_MS: 100,
  ERROR_RESET_INTERVAL_MS: 60000,
  RATE_LIMIT_MAX: 100,
  RATE_LIMIT_WINDOW_MS: 60000,
  MAX_RESTORE_ATTEMPTS: 2,
  RESTORE_RETRY_DELAY_MS: 1500,
  MULTY_MIN_MS: 10 * 1000,
  MULTY_MAX_MS: 60 * 1000,
  MAX_MULTY_NUMBER: 9999,
  HISTORY_LIMIT: 100,
  HISTORY_MAX_AGE_MS: 3 * 60 * 60 * 1000,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Relax & Chat",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);
const TABLE_NAME = 'chat_data';
const TABLE_MULTY = 'chat_multy';

const DEFAULT_MULTY_ROOM = "Gacor";

const K_CHAT   = (room) => `chat_multy_${room}`;
const K_NUMBER = (room) => `number_${room}`;
const K_INDEX  = (room) => `index_${room}`;
const K_RUNNING = (room) => `multy_running_${room}`;
const K_MULTY_NOIMG_CACHE = 'multy_noimg_cache';
const K_HISTORY_TABLE = (room) => `chat_history_${String(room).replace(/[^a-zA-Z0-9_]/g, '_')}`;

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

      this._restoreRemovedSeats = [];
      this._hasBroadcastRemoveKursi = new Set();

      this.wsSet = new Set();
      this.userConnections = new Map();
      this.roomClients = new Map();
      this.wsActiveMulti = new Map();

      this._userIndex = new Map();
      this._userNoimgCache = new Map();

      // === LOCK HIERARCHY (urutan: global → room → roomWrite → user) ===
      this._globalLocks = new Map();     // lock lintas-room (pindah room, cleanup)
      this._roomLocks = new Map();       // lock per-room untuk join/leave
      this._roomWriteLocks = new Map();  // lock per-room untuk write seat
      this._userJoinLock = new Map();    // lock per-user
      this._kursiLocks = new Map();      // lock per-seat untuk updateKursi
      this._wsLock = null;

      // === JOIN COOLDOWN (anti spam) ===
      this._joinCooldown = new Map();    // username -> lastJoinTs

      this.currentNumber = 1;
      this._isNumberUpdating = false;
      this._numberUpdateStart = null;

      this._multyState = new Map();
      this._multyRestored = false;

      this._historyTableReady = new Set();

      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._lastRequestDecay = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      this._reconnectAttempts = new Map();
      this._allTimers = new Set();

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
          if (!this._cacheInitialized) {
            this._storageCache = this._storageCache || { roomsData: {}, currentNumber: 1 };
            this._cacheInitialized = true;
          }
        }
      }, C.CACHE_LOAD_TIMEOUT);

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
          if (!this._cacheInitialized) {
            this._storageCache = this._storageCache || { roomsData: {}, currentNumber: 1 };
            this._cacheInitialized = true;
          }
        });

    } catch(e) {
      this._restored = true;
      this._restoreDone = true;
      this._isRestoring = false;
      this._storageCache = { roomsData: {}, currentNumber: 1 };
      this._cacheInitialized = false;
      this.currentNumber = 1;
      this._restoreFailed = true;
      this.closing = false;
      this.isDestroyed = false;
      this._restoreRemovedSeats = [];
      this._hasBroadcastRemoveKursi = new Set();
      this.wsSet = new Set();
      this.userConnections = new Map();
      this.roomClients = new Map();
      this.wsActiveMulti = new Map();
      this._userIndex = new Map();
      this._userNoimgCache = new Map();
      this._pendingEvents = [];
      this._eventQueue = [];
      this.db = null;
      this._onlineUsersCache = null;
      this._onlineUsersCacheTime = 0;
      this._roomCountsCache = null;
      this._roomCountsCacheTime = 0;
      this._multyState = new Map();
      this._multyRestored = false;
      this._historyTableReady = new Set();
      this._globalLocks = new Map();
      this._roomLocks = new Map();
      this._roomWriteLocks = new Map();
      this._joinCooldown = new Map();
      for (const room of ROOMS) {
        this.roomClients.set(room, new Set());
      }
    }
  }

  // ============================================================
  // LOCK HELPER
  // ============================================================
  async _withLock(lockMap, key, fn, timeout = C.LOCK_TIMEOUT) {
    try {
      if (!lockMap) return await fn();

      const token = Symbol('lock_' + key);
      const start = Date.now();

      while (lockMap.has(key) && (Date.now() - start) < timeout) {
        await new Promise(resolve => setTimeout(resolve, 15));
      }

      if (lockMap.has(key)) {
        return null;
      }

      lockMap.set(key, token);
      try {
        return await fn();
      } finally {
        if (lockMap.get(key) === token) lockMap.delete(key);
      }
    } catch(e) {
      throw e;
    }
  }

  // ============================================================
  // MULTY STATE
  // ============================================================
  _getMultyState(room) {
    const r = room || DEFAULT_MULTY_ROOM;
    let st = this._multyState.get(r);
    if (!st) {
      st = {
        chatList: [],
        index: 0,
        running: false,
        numberNext: 1,
        loopTimer: null,
        tickRunning: false,
      };
      this._multyState.set(r, st);
    }
    return st;
  }

  _getRunningRooms() {
    const out = [];
    for (const [room, st] of this._multyState) {
      if (st.running) out.push(room);
    }
    return out;
  }

  _isAnyMultyRunning() {
    for (const st of this._multyState.values()) {
      if (st.running) return true;
    }
    return false;
  }

  // ============================================================
  // USER INDEX
  // ============================================================
  _rebuildUserIndex() {
    try {
      this._userIndex = new Map();
      const roomsData = this._storageCache?.roomsData || {};
      for (const [roomName, roomBucket] of Object.entries(roomsData)) {
        if (!roomBucket?.seat) continue;
        for (const [seatStr, seatData] of Object.entries(roomBucket.seat)) {
          if (seatData?.namauser) {
            const seatNum = parseInt(seatStr);
            if (!isNaN(seatNum)) {
              this._userIndex.set(seatData.namauser, {
                room: roomName,
                seat: seatNum,
                isMulti: seatData.isMulti === true
              });
            }
          }
        }
      }
    } catch(e) {
      this._userIndex = new Map();
    }
  }

  _setUserIndex(username, room, seat, isMulti) {
    if (!username) return;
    try {
      this._userIndex.set(username, { room, seat, isMulti: isMulti === true });
    } catch(e) {}
  }

  _removeUserIndex(username) {
    if (!username) return;
    try { this._userIndex.delete(username); } catch(e) {}
  }

  // ============================================================
  // NOIMG CACHE
  // ============================================================
  async _saveUserNoimgCache() {
    try {
      if (!this.db) return false;
      const obj = {};
      for (const [username, noimg] of this._userNoimgCache) {
        obj[username] = noimg;
      }
      await this.db.prepare(`
        INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
      `).bind(K_MULTY_NOIMG_CACHE, JSON.stringify(obj)).run();
      return true;
    } catch(e) {
      return false;
    }
  }

  async _loadUserNoimgCache() {
    try {
      if (!this.db) return false;
      const row = await this.db
        .prepare(`SELECT value FROM ${TABLE_MULTY} WHERE key = ?`)
        .bind(K_MULTY_NOIMG_CACHE)
        .first();

      if (!row?.value) return false;

      let obj = null;
      try { obj = JSON.parse(row.value); } catch(e) {}
      if (!obj || typeof obj !== 'object') return false;

      this._userNoimgCache = new Map();
      for (const [username, noimg] of Object.entries(obj)) {
        const n = parseInt(noimg);
        if (!isNaN(n) && n > 0) {
          this._userNoimgCache.set(username, n);
        }
      }
      return true;
    } catch(e) {
      return false;
    }
  }

  async _setUserNoimgCache(username, noimg) {
    if (!username) return;
    try {
      const n = parseInt(noimg);
      if (!isNaN(n) && n > 0) {
        this._userNoimgCache.set(username, n);
      } else {
        this._userNoimgCache.delete(username);
      }
      this._saveUserNoimgCache().catch(() => {});
    } catch(e) {}
  }

  async _deleteUserNoimgCache(username) {
    if (!username) return;
    try {
      if (this._userNoimgCache.delete(username)) {
        this._saveUserNoimgCache().catch(() => {});
      }
    } catch(e) {}
  }

  // ============================================================
  // ALARM
  // ============================================================
  async _ensureAlarm() {
    if (this.isDestroyed) return;
    try {
      if (!this.ctx || !this.ctx.storage) return;
      if (typeof this.ctx.storage.setAlarm !== 'function') return;

      if (typeof this.ctx.storage.getAlarm === 'function') {
        try {
          const existing = await this.ctx.storage.getAlarm();
          if (existing !== null && existing !== undefined) return;
        } catch(e) {}
      }

      const next = Date.now() + C.NUMBER_INTERVAL_MS;
      await this.ctx.storage.setAlarm(next);
    } catch(e) {}
  }

  async alarm() {
    try {
      if (this.isDestroyed) return;

      this._isNumberUpdating = false;
      this._numberUpdateStart = null;

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
        if (this._restoreDone && !this._isRestoring) {
          const liveWsList = [];
          for (const ws of (this.wsSet || new Set())) {
            try {
              if (ws && ws.readyState === 1) liveWsList.push(ws);
            } catch(e) {}
          }
          await this._verifyAndCleanupOrphanSeats(liveWsList);
        }
      } catch(e) {}

    } catch(e) {
      this._handleError('alarm', e);
    } finally {
      try {
        if (this.ctx && this.ctx.storage && typeof this.ctx.storage.setAlarm === 'function') {
          const next = Date.now() + C.NUMBER_INTERVAL_MS;
          await this.ctx.storage.setAlarm(next);
        }
      } catch(e) {
        try {
          await this.ctx.storage.setAlarm(Date.now() + 60000);
        } catch(e2) {}
      }
    }
  }

  async _updateNumber() {
    try {
      if (this.isDestroyed) return;

      if (this._isNumberUpdating) {
        if (this._numberUpdateStart && Date.now() - this._numberUpdateStart > 30000) {
          this._isNumberUpdating = false;
          this._numberUpdateStart = null;
        } else {
          return;
        }
      }

      this._isNumberUpdating = true;
      this._numberUpdateStart = Date.now();

      try {
        const max = C.MAX_NUMBER;
        let next = this.currentNumber + 1;
        if (next > max) next = 1;
        this.currentNumber = next;

        if (this._storageCache) {
          this._storageCache.currentNumber = this.currentNumber;
        }

        await this._saveCurrentNumber();

        if (!this._isRestoring) {
          for (const [room, clients] of (this.roomClients || new Map())) {
            if (clients?.size > 0) {
              this.broadcast(room, ["currentNumber", this.currentNumber]);
            }
          }
        }

      } catch(e) {
        this._handleError('_updateNumber', e);
      } finally {
        this._isNumberUpdating = false;
        this._numberUpdateStart = null;
      }
    } catch(e) {}
  }

  // ============================================================
  // HISTORY
  // ============================================================
  async _ensureHistoryTable(room) {
    try {
      if (!this.db) return false;
      const tableName = K_HISTORY_TABLE(room);

      if (this._historyTableReady.has(tableName)) return true;

      await this.db.prepare(`
        CREATE TABLE IF NOT EXISTS ${tableName} (
          timestamp INTEGER PRIMARY KEY,
          chat_data TEXT NOT NULL,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `).run();

      this._historyTableReady.add(tableName);
      return true;
    } catch(e) {
      return false;
    }
  }

  async _saveHistoryChat(room, chatDataArray) {
    try {
      if (!this.db) return false;
      if (!room || !Array.isArray(chatDataArray)) return false;

      await this._ensureHistoryTable(room);

      const noimg     = chatDataArray[2] ?? 10000;
      const username  = chatDataArray[3] ?? "";
      const message   = chatDataArray[4] ?? "";
      const color     = chatDataArray[5] ?? "1";
      const textColor = chatDataArray[6] ?? "1";

      const javaFormat = [
        Number(noimg) || 10000,
        parseInt(color) || 1,
        String(username),
        String(message),
        parseInt(textColor) || 1,
        0
      ];

      let timestamp = Date.now();

      try {
        const tableName = K_HISTORY_TABLE(room);
        const existing = await this.db.prepare(
          `SELECT timestamp FROM ${tableName} WHERE timestamp = ? LIMIT 1`
        ).bind(timestamp).first();
        if (existing) timestamp = timestamp + 1;
      } catch(e) {}

      const tableName = K_HISTORY_TABLE(room);

      await this.db.prepare(`
        INSERT OR REPLACE INTO ${tableName}
        (timestamp, chat_data)
        VALUES (?, ?)
      `).bind(
        timestamp,
        JSON.stringify(javaFormat)
      ).run();

      return timestamp;
    } catch(e) {
      return false;
    }
  }

  async _loadHistoryChat(room) {
    try {
      if (!this.db) return [];
      await this._ensureHistoryTable(room);

      const tableName = K_HISTORY_TABLE(room);

      const query = `
        SELECT timestamp, chat_data
        FROM ${tableName}
        ORDER BY timestamp DESC
        LIMIT ${C.HISTORY_LIMIT}
      `;

      const result = await this.db.prepare(query).all();
      const rows = result?.results || [];

      return rows.reverse().map(r => {
        let arr = null;
        try { arr = JSON.parse(r.chat_data); } catch(e) {}

        return {
          timestamp: r.timestamp,
          java: Array.isArray(arr) ? arr : [10000, 1, "", "", 1, 0]
        };
      });
    } catch(e) {
      return [];
    }
  }

  async _checkAndResetHistory(room) {
    try {
      if (!this.db) return false;
      const tableName = K_HISTORY_TABLE(room);

      const row = await this.db.prepare(`
        SELECT MIN(timestamp) AS first_ts FROM ${tableName}
      `).first();

      if (!row || !row.first_ts) return false;

      const nowTs = Date.now();
      const age = nowTs - row.first_ts;

      if (age > C.HISTORY_MAX_AGE_MS) {
        await this.db.prepare(`DROP TABLE IF EXISTS ${tableName}`).run();
        this._historyTableReady.delete(tableName);
        return true;
      }

      return false;
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // MULTY TABLE
  // ============================================================
  _randMultyDelay() {
    return Math.floor(Math.random() * (C.MULTY_MAX_MS - C.MULTY_MIN_MS + 1)) + C.MULTY_MIN_MS;
  }

  _parseMultyRow(chatRaw, numberRaw, indexRaw) {
    let chatList = [];

    if (chatRaw) {
      try {
        let arr = JSON.parse(chatRaw);

        if (Array.isArray(arr) && arr.length > 0 && Array.isArray(arr[0])) {
          const flat = [];
          for (const sub of arr) {
            if (Array.isArray(sub)) {
              for (const item of sub) {
                if (item && typeof item === 'object') flat.push(item);
              }
            } else if (sub && typeof sub === 'object') {
              flat.push(sub);
            }
          }
          arr = flat;
        }

        if (Array.isArray(arr)) {
          for (const item of arr) {
            if (!item || typeof item !== 'object') continue;
            if (!item.sender || !item.text) continue;
            chatList.push({
              noimg: item.noimg ?? 10000,
              sender: String(item.sender),
              text: String(item.text),
              color: item.color ? String(item.color) : "7",
              textColor: item.textColor ? String(item.textColor) : "1"
            });
          }
        }
      } catch(e) {}
    }

    let numberNext = 1;
    if (numberRaw) {
      const n = parseInt(numberRaw);
      if (!isNaN(n) && n >= 1) numberNext = n;
    }

    let index = 0;
    if (indexRaw) {
      const i = parseInt(indexRaw);
      if (!isNaN(i) && i >= 0) index = i;
    }

    return { chatList, numberNext, index };
  }

  async _loadAllMultyFromTable() {
    try {
      if (!this.db) return new Map();

      const result = await this.db
        .prepare(`SELECT key, value FROM ${TABLE_MULTY}`)
        .all();

      const rows = result?.results || [];

      const bucket = new Map();
      for (const room of ROOMS) {
        bucket.set(room, {
          chatRaw: null,
          numberRaw: null,
          indexRaw: null,
          runningRaw: null
        });
      }

      let legacyChatRaw = null;
      let legacyNumberRaw = null;
      let noimgCacheRaw = null;

      for (const row of rows) {
        const key = row?.key;
        const value = row?.value;
        if (!key || value === undefined || value === null) continue;

        if (key === 'chat_multy') { legacyChatRaw = value; continue; }
        if (key === 'number') { legacyNumberRaw = value; continue; }
        if (key === K_MULTY_NOIMG_CACHE) { noimgCacheRaw = value; continue; }

        if (key.startsWith('chat_multy_')) {
          const room = key.slice('chat_multy_'.length);
          if (bucket.has(room)) bucket.get(room).chatRaw = value;
          continue;
        }

        if (key.startsWith('number_')) {
          const room = key.slice('number_'.length);
          if (bucket.has(room)) bucket.get(room).numberRaw = value;
          continue;
        }

        if (key.startsWith('index_')) {
          const room = key.slice('index_'.length);
          if (bucket.has(room)) bucket.get(room).indexRaw = value;
          continue;
        }

        if (key.startsWith('multy_running_')) {
          const room = key.slice('multy_running_'.length);
          if (bucket.has(room)) bucket.get(room).runningRaw = value;
          continue;
        }
      }

      const defBucket = bucket.get(DEFAULT_MULTY_ROOM);
      if (defBucket) {
        if (!defBucket.chatRaw && legacyChatRaw) {
          defBucket.chatRaw = legacyChatRaw;
          this.db.prepare(`
            INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
          `).bind(K_CHAT(DEFAULT_MULTY_ROOM), legacyChatRaw).run().catch(() => {});
        }
        if (!defBucket.numberRaw && legacyNumberRaw) {
          defBucket.numberRaw = legacyNumberRaw;
          this.db.prepare(`
            INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
          `).bind(K_NUMBER(DEFAULT_MULTY_ROOM), legacyNumberRaw).run().catch(() => {});
        }
      }

      if (noimgCacheRaw) {
        try {
          const obj = JSON.parse(noimgCacheRaw);
          if (obj && typeof obj === 'object') {
            this._userNoimgCache = new Map();
            for (const [username, noimg] of Object.entries(obj)) {
              const n = parseInt(noimg);
              if (!isNaN(n) && n > 0) {
                this._userNoimgCache.set(username, n);
              }
            }
          }
        } catch(e) {}
      }

      const out = new Map();
      for (const [room, raw] of bucket) {
        const parsed = this._parseMultyRow(raw.chatRaw, raw.numberRaw, raw.indexRaw);
        parsed.running = raw.runningRaw === "1";

        const hasAny = parsed.chatList.length > 0
                    || parsed.running
                    || raw.numberRaw !== null
                    || raw.indexRaw !== null;

        if (hasAny) out.set(room, parsed);
      }

      return out;
    } catch(e) {
      return new Map();
    }
  }

  async _loadMultyFromTable(room) {
    const r = room || DEFAULT_MULTY_ROOM;
    try {
      if (!this.db) return { chatList: [], numberNext: 1, index: 0, running: false };

      let rowChat = null;
      let rowNum = null;
      let rowIdx = null;
      let rowRunning = null;

      try {
        rowChat = await this.db
          .prepare(`SELECT value FROM ${TABLE_MULTY} WHERE key = ?`)
          .bind(K_CHAT(r))
          .first();
      } catch(e) {}

      try {
        rowNum = await this.db
          .prepare(`SELECT value FROM ${TABLE_MULTY} WHERE key = ?`)
          .bind(K_NUMBER(r))
          .first();
      } catch(e) {}

      try {
        rowIdx = await this.db
          .prepare(`SELECT value FROM ${TABLE_MULTY} WHERE key = ?`)
          .bind(K_INDEX(r))
          .first();
      } catch(e) {}

      try {
        rowRunning = await this.db
          .prepare(`SELECT value FROM ${TABLE_MULTY} WHERE key = ?`)
          .bind(K_RUNNING(r))
          .first();
      } catch(e) {}

      if ((!rowChat || !rowChat.value) && r === DEFAULT_MULTY_ROOM) {
        try {
          const oldChat = await this.db
            .prepare(`SELECT value FROM ${TABLE_MULTY} WHERE key = 'chat_multy'`)
            .first();
          if (oldChat?.value) {
            rowChat = oldChat;
            await this.db.prepare(`
              INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
              VALUES (?, ?, CURRENT_TIMESTAMP)
            `).bind(K_CHAT(r), oldChat.value).run();
          }
        } catch(e) {}
      }

      if ((!rowNum || !rowNum.value) && r === DEFAULT_MULTY_ROOM) {
        try {
          const oldNum = await this.db
            .prepare(`SELECT value FROM ${TABLE_MULTY} WHERE key = 'number'`)
            .first();
          if (oldNum?.value) {
            rowNum = oldNum;
            await this.db.prepare(`
              INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
              VALUES (?, ?, CURRENT_TIMESTAMP)
            `).bind(K_NUMBER(r), oldNum.value).run();
          }
        } catch(e) {}
      }

      const parsed = this._parseMultyRow(
        rowChat?.value ?? null,
        rowNum?.value ?? null,
        rowIdx?.value ?? null
      );
      parsed.running = rowRunning?.value === "1";
      return parsed;
    } catch(e) {
      return { chatList: [], numberNext: 1, index: 0, running: false };
    }
  }

  async _saveMultyChatToTable(room, chatList) {
    const r = room || DEFAULT_MULTY_ROOM;
    try {
      if (!this.db) return false;
      await this.db.prepare(`
        INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
      `).bind(K_CHAT(r), JSON.stringify(chatList)).run();
      return true;
    } catch(e) {
      return false;
    }
  }

  async _saveMultyNumberToTable(room, numberNext) {
    const r = room || DEFAULT_MULTY_ROOM;
    try {
      if (!this.db) return false;
      await this.db.prepare(`
        INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
      `).bind(K_NUMBER(r), String(numberNext)).run();
      return true;
    } catch(e) {
      return false;
    }
  }

  async _saveMultyIndexToTable(room, indexNext) {
    const r = room || DEFAULT_MULTY_ROOM;
    try {
      if (!this.db) return false;
      await this.db.prepare(`
        INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
      `).bind(K_INDEX(r), String(indexNext)).run();
      return true;
    } catch(e) {
      return false;
    }
  }

  async _saveMultyRunningToTable(room, running) {
    const r = room || DEFAULT_MULTY_ROOM;
    try {
      if (!this.db) {
        return false;
      }
      await this.db.prepare(`
        INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
      `).bind(K_RUNNING(r), running ? "1" : "0").run();
      return true;
    } catch(e) {
      return false;
    }
  }

  async _loadMultyRunningFromTable(room) {
    const r = room || DEFAULT_MULTY_ROOM;
    try {
      if (!this.db) return false;
      const row = await this.db
        .prepare(`SELECT value FROM ${TABLE_MULTY} WHERE key = ?`)
        .bind(K_RUNNING(r))
        .first();
      return row?.value === "1";
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // MULTY LOOP
  // ============================================================
  _startMultyLoop(room) {
    const r = room || DEFAULT_MULTY_ROOM;
    const st = this._getMultyState(r);
    try {
      if (st.loopTimer) {
        clearTimeout(st.loopTimer);
        st.loopTimer = null;
      }

      if (!st.running || this.closing || this.isDestroyed) return;

      const clients = this.roomClients?.get(r);
      if (!clients || clients.size === 0) {
        st.loopTimer = null;
        return;
      }

      const delay = this._randMultyDelay();

      st.loopTimer = setTimeout(async () => {
        if (st.loopTimer) {
          clearTimeout(st.loopTimer);
          st.loopTimer = null;
        }

        if (!st.running || this.closing || this.isDestroyed) return;

        if (st.tickRunning) return;
        st.tickRunning = true;

        try {
          const clientsNow = this.roomClients?.get(r);
          if (!clientsNow || clientsNow.size === 0) {
            return;
          }

          try {
            await this._nextMultyChat(r);
          } catch(e) {}
        } finally {
          st.tickRunning = false;
        }

        if (st.running && !this.closing && !this.isDestroyed) {
          this._startMultyLoop(r);
        }
      }, delay);
    } catch(e) {}
  }

  _stopMultyLoop(room) {
    const r = room || DEFAULT_MULTY_ROOM;
    const st = this._getMultyState(r);
    try {
      if (st.loopTimer) {
        clearTimeout(st.loopTimer);
        st.loopTimer = null;
      }
      st.tickRunning = false;
    } catch(e) {}
  }

  _stopAllMultyLoops() {
    for (const room of this._multyState.keys()) {
      this._stopMultyLoop(room);
    }
  }

  async _nextMultyChat(room) {
    const r = room || DEFAULT_MULTY_ROOM;
    const st = this._getMultyState(r);
    try {
      if (!st.running) return false;

      const clients = this.roomClients?.get(r);
      if (!clients || clients.size === 0) {
        return false;
      }

      if (!Array.isArray(st.chatList) || st.chatList.length === 0 || st.index >= st.chatList.length) {
        st.running = false;
        st.index = 0;
        this._stopMultyLoop(r);
        this._saveMultyRunningToTable(r, false).catch(() => {});
        this._saveMultyIndexToTable(r, 0).catch(() => {});
        this.broadcast(r, ["multyStop", r]);
        return false;
      }

      const chat = st.chatList[st.index];
      if (!chat || typeof chat !== 'object') {
        st.index++;
        this._saveMultyIndexToTable(r, st.index).catch(() => {});
        return st.running;
      }

      const username = chat.sender || "";
      const chatMsg = chat.text || "";
      const chatColor = chat.color || "7";
      const chatTextColor = chat.textColor || "1";

      let chatNoimg = 10000;
      try {
        const cached = this._userNoimgCache?.get(username);
        if (cached && cached > 0) {
          chatNoimg = cached;
        }
      } catch(e) {}

      if (chatMsg) {
        const chatData = ["chat", r, chatNoimg, username, chatMsg, chatColor, chatTextColor];

        this.broadcast(r, chatData);
        this.broadcast(r, ["multyNumber", st.numberNext, r]);

        this._saveHistoryChat(r, chatData).catch(() => {});
      }

      try {
        if (this.db) {
          const num = st.numberNext;
          await this.db.prepare(`
            INSERT OR REPLACE INTO ${TABLE_MULTY} (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
          `).bind(K_NUMBER(r), String(num)).run();
        }
      } catch(e) {}

      st.numberNext++;
      if (st.numberNext > C.MAX_MULTY_NUMBER) st.numberNext = 1;

      this._saveMultyNumberToTable(r, st.numberNext).catch(() => {});

      st.index++;

      this._saveMultyIndexToTable(r, st.index).catch(() => {});

      if (st.index >= st.chatList.length) {
        st.running = false;
        st.index = 0;
        this._stopMultyLoop(r);
        this._saveMultyRunningToTable(r, false).catch(() => {});
        this._saveMultyIndexToTable(r, 0).catch(() => {});
        this.broadcast(r, ["multyStop", r]);
        return false;
      }

      return true;
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // RESTORE
  // ============================================================
  async _restoreWithRetry() {
    let attempts = 0;
    let lastError = null;

    try {
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
      if (!this._cacheInitialized) {
        this._storageCache = this._storageCache || { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
      }

      throw lastError;
    } finally {
      this._isRestoring = false;
    }
  }

  async _loadFromStorage() {
    try {
      if (!this.db) {
        this._storageCache = { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this.currentNumber = 1;
        this._userIndex = new Map();
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

      try {
        await this.db.prepare(`
          CREATE TABLE IF NOT EXISTS ${TABLE_MULTY} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
          )
        `).run();
      } catch(e) {}

      try {
        const existingRows = await this.db
          .prepare(`SELECT key FROM ${TABLE_MULTY} WHERE key LIKE 'multy_running_%'`)
          .all();

        const existingKeys = new Set(
          (existingRows?.results || []).map(r => r.key)
        );

        for (const room of ROOMS) {
          const key = K_RUNNING(room);
          if (!existingKeys.has(key)) {
            try {
              await this.db.prepare(`
                INSERT OR IGNORE INTO ${TABLE_MULTY} (key, value, updated_at)
                VALUES (?, '0', CURRENT_TIMESTAMP)
              `).bind(key).run();
            } catch(e) {}
          }
        }
      } catch(e) {}

      let result;
      try {
        result = await this.db.prepare(`SELECT key, value FROM ${TABLE_NAME}`).all();
      } catch(e) {
        if (!this._cacheInitialized) {
          this._storageCache = this._storageCache || { roomsData: {}, currentNumber: 1 };
          this._cacheInitialized = true;
        }
        return this._storageCache || { roomsData: {}, currentNumber: 1 };
      }

      const roomsData = {};
      for (const room of ROOMS) {
        roomsData[room] = { seat: {}, point: {}, mute: false };
      }

      let currentNumber = 1;
      const results = result?.results || [];

      // === DETEKSI DUPLIKAT: simpan key mana yang dipertahankan ===
      const userSeatMap = new Map(); // username -> { key, seat, room, isMulti }

      for (const row of results) {
        try {
          const key = row?.key;
          if (!key || typeof key !== 'string') continue;

          let value;
          try {
            value = JSON.parse(row.value);
          } catch(e) {
            continue;
          }

          if (key === 'current_number') {
            const n = parseInt(value);
            if (!isNaN(n)) currentNumber = n;
            continue;
          }

          const firstU = key.indexOf('_');
          if (firstU < 0) continue;
          const secondU = key.indexOf('_', firstU + 1);
          if (secondU < 0) continue;

          const type = key.slice(0, firstU);
          const roomName = key.slice(firstU + 1, secondU);
          const rest = key.slice(secondU + 1);

          if (!ROOMS_SET.has(roomName)) continue;

          if (type === 'mute') {
            roomsData[roomName].mute = value === true || value === 'true';
            continue;
          }

          const seatNumber = parseInt(rest);
          if (isNaN(seatNumber) || seatNumber < 1 || seatNumber > C.MAX_SEATS) continue;

          if (type === 'seat') {
            if (value && typeof value === 'object' && value.namauser) {
              const uname = value.namauser;
              const isMulti = value.isMulti === true;

              const prev = userSeatMap.get(uname);
              if (prev) {
                // Duplikat! Prioritaskan multi
                if (isMulti && !prev.isMulti) {
                  // Ganti: hapus yang lama, simpan yang baru (multi)
                  delete roomsData[prev.room].seat[prev.seat];
                  if (roomsData[prev.room].point) delete roomsData[prev.room].point[prev.seat];
                  userSeatMap.set(uname, {
                    key, seat: seatNumber, room: roomName, isMulti
                  });
                  roomsData[roomName].seat[seatNumber] = value;
                } else {
                  // Skip duplikat ini — jangan masukkan ke cache
                }
              } else {
                userSeatMap.set(uname, {
                  key, seat: seatNumber, room: roomName, isMulti
                });
                roomsData[roomName].seat[seatNumber] = value;
              }
            }
          } else if (type === 'point') {
            if (value && typeof value === 'object') {
              roomsData[roomName].point[seatNumber] = value;
            }
          }
        } catch(e) {
          continue;
        }
      }

      this._storageCache = { roomsData, currentNumber };
      this._cacheInitialized = true;
      this.currentNumber = currentNumber;
      this._cacheLoadAttempts = 0;

      this._rebuildUserIndex();

      return this._storageCache;

    } catch(e) {
      this._cacheLoadAttempts = (this._cacheLoadAttempts || 0) + 1;
      if (this._cacheLoadAttempts > 3) {
        this._storageCache = this._storageCache || { roomsData: {}, currentNumber: 1 };
        this._cacheInitialized = true;
        this.currentNumber = this.currentNumber || 1;
        this._restoreFailed = true;
      }
      throw e;
    }
  }

  // ============================================================
  // D1 WRITE HELPERS (WAJIB AWAIT — write-through)
  // ============================================================
  async _saveSeat(roomName, seatNumber, seatData) {
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
  }

  async _savePoint(roomName, seatNumber, pointData) {
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
  }

  async _saveMute(roomName, muted) {
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
  }

  async _saveCurrentNumber() {
    if (!this.db) return;
    const n = (typeof this.currentNumber === 'number' && this.currentNumber >= 1)
      ? this.currentNumber
      : 1;
    await this.db
      .prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
      .bind('current_number', String(n))
      .run();
  }

  async _forceDeleteFromD1(username) {
    if (!this.db) return true;
    if (!username) return true;

    const u = String(username);

    // Hapus seat user ini (kecuali multi)
    try {
      await this.db.prepare(`
        DELETE FROM ${TABLE_NAME}
        WHERE key LIKE 'seat_%'
        AND json_valid(value)
        AND json_extract(value, '$.namauser') = ?
        AND (json_extract(value, '$.isMulti') IS NULL OR json_extract(value, '$.isMulti') != 1)
      `).bind(u).run();
    } catch(e) {
      try {
        await this.db.prepare(`
          DELETE FROM ${TABLE_NAME}
          WHERE key LIKE 'seat_%'
          AND value LIKE ?
          AND value NOT LIKE '%"isMulti":true%'
        `).bind(`%"namauser":"${u}"%`).run();
      } catch(e2) {}
    }

    return true;
  }

  // ============================================================
  // ROOM BUCKET
  // ============================================================
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

  async _findAllSeatsForUser(username) {
    try {
      if (!username) return [];
      await this._ensureCacheInitialized();
      const roomsData = this._storageCache?.roomsData || {};
      const found = [];

      for (const [roomName, roomBucket] of Object.entries(roomsData)) {
        if (!roomBucket?.seat) continue;
        for (const [seatStr, data] of Object.entries(roomBucket.seat)) {
          if (data?.namauser === username) {
            const seatNum = parseInt(seatStr);
            if (!isNaN(seatNum)) {
              found.push({
                room: roomName,
                seat: seatNum,
                isMulti: data.isMulti === true
              });
            }
          }
        }
      }
      return found;
    } catch(e) {
      return [];
    }
  }

  // ============================================================
  // DELETE SEAT (write-through: D1 dulu, cache kemudian)
  // ============================================================
  async _deleteSeatInRoom(roomName, seatNumber, force = false) {
    return await this._withLock(
      this._roomWriteLocks,
      `write_${roomName}`,
      async () => {
        try {
          const roomBucket = await this._getRoomBucket(roomName);
          if (!roomBucket) return false;

          const seatData = roomBucket.seat?.[seatNumber];
          if (!force && seatData?.isMulti === true) return false;

          const removedUsername = seatData?.namauser;
          const removedIsMulti = seatData?.isMulti === true;

          // 1. Tulis D1 DULU
          if (this.db) {
            try {
              await this.db
                .prepare(`DELETE FROM ${TABLE_NAME} WHERE key IN (?, ?)`)
                .bind(`seat_${roomName}_${seatNumber}`, `point_${roomName}_${seatNumber}`)
                .run();
            } catch(e) {}
          }

          // 2. Baru update cache (real-time)
          if (roomBucket.seat) delete roomBucket.seat[seatNumber];
          if (roomBucket.point) delete roomBucket.point[seatNumber];

          if (removedUsername) {
            this._removeUserIndex(removedUsername);
            if (removedIsMulti) {
              await this._deleteUserNoimgCache(removedUsername);
            }
          }

          if (!this._isRestoring) {
            this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
          }
          await this.updateRoomCount(roomName);
          return true;
        } catch(e) {
          return false;
        }
      },
      C.LOCK_TIMEOUT
    );
  }

  // ============================================================
  // REMOVE ALL SEATS (write-through + global lock)
  // ============================================================
  async _removeAllSeatsForUser(username, options = {}) {
    if (!username) return 0;

    return await this._withLock(
      this._globalLocks,
      `global_seat_move`,
      async () => {
        try {
          const keepRoom = options.keepRoom || null;
          const keepSeat = options.keepSeat || null;

          const allSeats = await this._findAllSeatsForUser(username);
          let removed = 0;

          for (const item of allSeats) {
            if (keepRoom && item.room === keepRoom && keepSeat && item.seat === keepSeat) {
              continue;
            }

            try {
              const roomBucket = this._storageCache?.roomsData?.[item.room];
              if (!roomBucket?.seat) continue;

              // 1. Tulis D1 DULU
              if (this.db) {
                try {
                  await this.db
                    .prepare(`DELETE FROM ${TABLE_NAME} WHERE key IN (?, ?)`)
                    .bind(`seat_${item.room}_${item.seat}`, `point_${item.room}_${item.seat}`)
                    .run();
                } catch(e) {}
              }

              // 2. Baru update cache
              delete roomBucket.seat[item.seat];
              if (roomBucket.point) delete roomBucket.point[item.seat];

              if (!this._isRestoring) {
                this.broadcast(item.room, ["removeKursi", item.room, item.seat]);
                try { await this.updateRoomCount(item.room); } catch(e) {}
              }

              removed++;
            } catch(e) {}
          }

          if (removed > 0) {
            this._removeUserIndex(username);
          }

          return removed;
        } catch(e) {
          return 0;
        }
      },
      C.GLOBAL_LOCK_TIMEOUT
    );
  }

  // ============================================================
  // UPDATE SEAT (write-through + roomWrite lock)
  // ============================================================
  async _updateSeatInRoom(roomName, seatNumber, seatData) {
    return await this._withLock(
      this._roomWriteLocks,
      `write_${roomName}`,
      async () => {
        try {
          const roomBucket = await this._getRoomBucket(roomName);
          if (!roomBucket) return false;

          // === KOSONGKAN SEAT ===
          if (!seatData || !seatData.namauser || seatData.namauser.trim() === '') {
            const oldSeat = roomBucket.seat?.[seatNumber];
            const oldUser = oldSeat?.namauser;
            const oldIsMulti = oldSeat?.isMulti === true;

            // 1. Tulis D1 DULU
            try { await this._saveSeat(roomName, seatNumber, null); } catch(e) {}

            // 2. Baru update cache
            if (roomBucket.seat) delete roomBucket.seat[seatNumber];
            if (roomBucket.point) delete roomBucket.point[seatNumber];

            if (oldUser) {
              this._removeUserIndex(oldUser);
              if (oldIsMulti) {
                this._deleteUserNoimgCache(oldUser).catch(() => {});
              }
            }
            return true;
          }

          const username = seatData.namauser;

          // === HAPUS USER DARI ROOM LAIN (pindah room) ===
          await this._ensureCacheInitialized();
          const roomsData = this._storageCache?.roomsData || {};

          for (const [rName, rBucket] of Object.entries(roomsData)) {
            if (!rBucket?.seat) continue;

            for (const [sStr, data] of Object.entries(rBucket.seat)) {
              if (data?.namauser !== username) continue;

              const sNum = parseInt(sStr);
              if (isNaN(sNum)) continue;

              if (rName === roomName && sNum === seatNumber) continue;

              // 1. Tulis D1 DULU
              if (this.db) {
                try {
                  await this.db
                    .prepare(`DELETE FROM ${TABLE_NAME} WHERE key IN (?, ?)`)
                    .bind(`seat_${rName}_${sNum}`, `point_${rName}_${sNum}`)
                    .run();
                } catch(e) {}
              }

              // 2. Baru update cache
              delete rBucket.seat[sNum];
              if (rBucket.point) delete rBucket.point[sNum];

              if (!this._isRestoring) {
                this.broadcast(rName, ["removeKursi", rName, sNum]);
                this.updateRoomCount(rName).catch(() => {});
              }
            }
          }

          // === SET SEAT BARU ===
          const oldSeat = roomBucket.seat?.[seatNumber];
          const finalSeatData = { ...seatData };
          if (oldSeat?.isMulti === true && finalSeatData.isMulti !== true) {
            finalSeatData.isMulti = true;
          }

          // 1. Tulis D1 DULU
          try { await this._saveSeat(roomName, seatNumber, finalSeatData); } catch(e) {}

          // 2. Baru update cache (real-time)
          if (!roomBucket.seat) roomBucket.seat = {};
          roomBucket.seat[seatNumber] = finalSeatData;
          this._setUserIndex(finalSeatData.namauser, roomName, seatNumber, finalSeatData.isMulti);

          if (finalSeatData.isMulti === true) {
            const noimg = parseInt(finalSeatData.noimageUrl);
            if (!isNaN(noimg) && noimg > 0) {
              await this._setUserNoimgCache(finalSeatData.namauser, noimg);
            }
          }

          return true;
        } catch(e) {
          return false;
        }
      },
      C.LOCK_TIMEOUT
    );
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

  // ============================================================
  // ROOM DATA / COUNT (dari cache — real-time sinkron)
  // ============================================================
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
        const uname = roomBucket.seat[seat]?.namauser;
        if (uname && String(uname).trim() !== '') count++;
      }
      return count;
    } catch(e) {
      return 0;
    }
  }

  async _findUserInAnyRoom(username) {
    try {
      if (!username) return null;

      const cached = this._userIndex?.get(username);
      if (cached) {
        await this._ensureCacheInitialized();
        const roomBucket = this._storageCache?.roomsData?.[cached.room];
        const seatData = roomBucket?.seat?.[cached.seat];
        if (seatData?.namauser === username) {
          return {
            room: cached.room,
            seat: cached.seat,
            isMulti: seatData.isMulti === true
          };
        }
        this._userIndex.delete(username);
      }

      const allSeats = await this._findAllSeatsForUser(username);
      if (allSeats.length === 0) return null;

      const multiSeat = allSeats.find(s => s.isMulti === true);
      const result = multiSeat || allSeats[0];

      this._setUserIndex(username, result.room, result.seat, result.isMulti);

      return result;
    } catch(e) {
      return null;
    }
  }

  // ============================================================
  // CACHE INIT
  // ============================================================
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
              setTimeout(() => reject(new Error('Restore timeout')), C.CACHE_LOAD_TIMEOUT)
            )
          ]);
        } catch(e) {
          if (!this._cacheInitialized) {
            this._storageCache = this._storageCache || { roomsData: {}, currentNumber: 1 };
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
            this._storageCache = this._storageCache || { roomsData: {}, currentNumber: 1 };
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
      return this._storageCache || { roomsData: {}, currentNumber: 1 };
    }
  }

  // ============================================================
  // KURSI UPDATE (per-seat lock)
  // ============================================================
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

      const finalIsMulti = (data.isMulti === true || currentSeatData.isMulti === true);

      const updatedSeat = {
        noimageUrl: data.noimageUrl || currentSeatData.noimageUrl || "",
        namauser: data.namauser || currentSeatData.namauser || "",
        color: data.color || currentSeatData.color || "",
        itembawah: typeof data.itembawah === 'number' ? data.itembawah : (parseInt(data.itembawah) || 0),
        itematas: typeof data.itematas === 'number' ? data.itematas : (parseInt(data.itematas) || 0),
        vip: typeof data.vip === 'number' ? data.vip : (parseInt(data.vip) || 0),
        viptanda: typeof data.viptanda === 'number' ? data.viptanda : (parseInt(data.viptanda) || 0),
        isMulti: finalIsMulti
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

  async _removeUserFromRoom(username, roomName, force = false) {
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
      return await this._deleteSeatInRoom(roomName, seat, force);
    } catch(e) {
      return false;
    }
  }

  _cleanupMultiTracking(username, oldRoom, keepWs = null) {
    try {
      if (!username) return;
      for (const [wsKey, data] of (this.wsActiveMulti || new Map())) {
        if (data?.username === username && (!keepWs || wsKey !== keepWs)) {
          try { this.wsActiveMulti.delete(wsKey); } catch(e) {}
          const room = data.room || oldRoom;
          if (room) {
            const rc = this.roomClients?.get(room);
            if (rc) try { rc.delete(wsKey); } catch(e) {}
          }
        }
      }
    } catch(e) {}
  }

  // ============================================================
  // JOIN (LOCK: user → room, dengan idempotency + cooldown)
  // ============================================================
  async _handleJoin(ws, roomName) {
    try {
      if (!ws?.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
        return false;
      }

      const username = ws.username;

      // === COOLDOWN anti-spam ===
      const lastJoin = this._joinCooldown.get(username) || 0;
      if (Date.now() - lastJoin < C.JOIN_COOLDOWN_MS) {
        // Idempotent: kalau sudah di room ini, kirim state saja
        const existing = await this._findUserInAnyRoom(username);
        if (existing && existing.room === roomName) {
          this.safeSend(ws, ["rooMasuk", existing.seat, roomName]);
          return true;
        }
        return false;
      }
      this._joinCooldown.set(username, Date.now());

      // === LOCK: user → room ===
      return await this._withLock(
        this._userJoinLock,
        `join_user_${username}`,
        async () => {
          return await this._withLock(
            this._roomLocks,
            `room_${roomName}`,
            async () => {
              return await this._joinInternal(ws, roomName, username);
            },
            C.ROOM_LOCK_TIMEOUT
          );
        },
        C.USER_JOIN_LOCK_TIMEOUT
      );
    } catch(e) {
      return false;
    }
  }

  async _joinInternal(ws, roomName, username) {
    try {
      // === IDEMPOTENCY: sudah di room ini? ===
      const existing = await this._findUserInAnyRoom(username);
      const wasMulti = existing?.isMulti === true;

      if (existing && existing.room === roomName) {
        // Sudah di room ini — cukup update WS & kirim state
        ws.username = username;
        ws.room = roomName;
        ws.roomname = roomName;
        ws.idtarget = username;
        ws._room = roomName;
        ws._username = username;

        try {
          ws.serializeAttachment({
            username: username,
            seatInfo: { room: roomName, seat: existing.seat }
          });
        } catch(e) {}

        const roomClients = this.roomClients.get(roomName);
        if (roomClients && !roomClients.has(ws)) {
          try { roomClients.add(ws); } catch(e) {}
        }

        this.safeSend(ws, ["rooMasuk", existing.seat, roomName]);
        this.safeSend(ws, ["numberKursiSaya", existing.seat]);
        this.safeSend(ws, ["currentNumber", this.currentNumber]);

        return true;
      }

      // === Hapus semua seat lama user (pindah room) ===
      if (existing) {
        await this._removeAllSeatsForUser(username);
        this._cleanupMultiTracking(username, existing.room, ws);
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

      // === Cari seat yang tersedia ===
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
          isMulti: wasMulti
        };

        await this._updateSeatInRoom(roomName, seat, newSeat);
      }

      ws.username = username;
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

      if (!wasMulti) {
        this.wsActiveMulti.delete(ws);
      }

      const muteStatus = roomBucket.mute || false;

      this.safeSend(ws, ["rooMasuk", seat, roomName]);
      this.safeSend(ws, ["numberKursiSaya", seat]);
      this.safeSend(ws, ["muteTypeResponse", muteStatus, roomName]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);

      const st = this._getMultyState(roomName);

      if (st.running) {
        if (!st.loopTimer) {
          this._startMultyLoop(roomName);
        }
        this.safeSend(ws, ["multyStatus", true, st.index, st.chatList.length, roomName]);
        this.safeSend(ws, ["multyNumber", st.numberNext, roomName]);
        this.safeSend(ws, ["multyRoom", roomName]);
      }
      else if (st.chatList.length > 0) {
        this.safeSend(ws, ["multyStatus", false, st.index, st.chatList.length, roomName]);
        this.safeSend(ws, ["multyNumber", st.numberNext, roomName]);
      }

      await this.updateRoomCount(roomName);

      try {
        const att = ws.deserializeAttachment?.() || {};
        att.pendingStateSend = Date.now();
        att.pendingStateRoom = roomName;
        att.pendingStateUsername = username;
        ws.serializeAttachment(att);
      } catch(e) {}

      setTimeout(async () => {
        try {
          if (!ws || ws.readyState !== 1) return;
          const att = ws.deserializeAttachment?.() || {};
          if (!att.pendingStateSend) return;
          att.pendingStateSend = null;
          att.pendingStateRoom = null;
          att.pendingStateUsername = null;
          try { ws.serializeAttachment(att); } catch(e) {}
          await this.sendAllStateTo(ws, roomName, true);
        } catch(e) {}
      }, 1000);

      return true;
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // MULTI JOIN (LOCK: user)
  // ============================================================
  async _handleMultiJoin(ws, multiUsername, multiRoomname) {
    try {
      if (!multiUsername || !multiRoomname || !ROOMS_SET.has(multiRoomname)) return false;

      return await this._withLock(
        this._userJoinLock,
        `join_user_${multiUsername}`,
        () => this._handleMultiJoinInternal(ws, multiUsername, multiRoomname),
        C.USER_JOIN_LOCK_TIMEOUT
      );
    } catch(e) {
      return false;
    }
  }

  async _handleMultiJoinInternal(ws, multiUsername, multiRoomname) {
    try {
      await this._ensureCacheInitialized();

      await this._removeAllSeatsForUser(multiUsername);
      this._cleanupMultiTracking(multiUsername, null, ws);

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

      try {
        const currentSeat = roomBucket.seat?.[seat];
        if (currentSeat?.namauser === multiUsername) {
          const noimg = parseInt(currentSeat.noimageUrl);
          if (!isNaN(noimg) && noimg > 0) {
            await this._setUserNoimgCache(multiUsername, noimg);
          }
        }
      } catch(e) {}

      try {
        const st = this._getMultyState(multiRoomname);
        if (st.running) {
          if (!st.loopTimer) {
            this._startMultyLoop(multiRoomname);
          }
          this.broadcast(multiRoomname, ["multyStatus", true, st.index, st.chatList.length, multiRoomname]);
          this.broadcast(multiRoomname, ["multyNumber", st.numberNext, multiRoomname]);
        }
      } catch(e) {}

      try { await this.updateRoomCount(multiRoomname); } catch(e) {}

      return { room: multiRoomname, seat: seat };
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // CLEANUP USER
  // ============================================================
  async _cleanupUserCompletely(ws, options = {}) {
    const result = { removedSeats: [] };

    if (!ws) return result;

    const skipBroadcast = options.skipBroadcast === true;

    let state = _wsCleanupState.get(ws);
    if (!state) {
      state = { cleanupDone: false, cleaning: false };
      try { _wsCleanupState.set(ws, state); } catch (e) {}
    }
    if (state.cleanupDone || state.cleaning) return result;
    state.cleaning = true;

    try {
      let username = ws.username || ws._username;
      let roomName = ws.room || ws.roomname || ws._room;

      if (!ws.username && ws._username) ws.username = ws._username;
      if (!ws._username && ws.username) ws._username = ws.username;
      if (!ws.room && ws._room) ws.room = ws._room;
      if (!ws._room && ws.room) ws._room = ws.room;
      if (!ws.room && ws.roomname) ws.room = ws.roomname;
      if (!ws.roomname && ws.room) ws.roomname = ws.room;

      if (!username || !roomName) {
        try {
          const att = ws.deserializeAttachment?.();
          if (att?.username) {
            username = username || att.username;
            ws.username = ws.username || att.username;
            ws._username = ws._username || att.username;
          }
          if (att?.seatInfo?.room) {
            roomName = roomName || att.seatInfo.room;
            ws.room = ws.room || att.seatInfo.room;
            ws._room = ws._room || att.seatInfo.room;
            ws.roomname = ws.roomname || att.seatInfo.room;
          }
          if (att?.room) {
            roomName = roomName || att.room;
            ws.room = ws.room || att.room;
            ws._room = ws._room || att.room;
            ws.roomname = ws.roomname || att.room;
          }
        } catch (e) {}
      }

      if (!username) {
        for (const [user, conns] of (this.userConnections || new Map())) {
          if (conns?.has?.(ws)) {
            username = user;
            ws.username = ws.username || user;
            ws._username = ws._username || user;
            break;
          }
        }
      }

      if (!roomName) {
        for (const [rName, clients] of (this.roomClients || new Map())) {
          if (clients?.has?.(ws)) {
            roomName = rName;
            ws.room = ws.room || rName;
            ws._room = ws._room || rName;
            ws.roomname = ws.roomname || rName;
            break;
          }
        }
      }

      const isMulti = this.wsActiveMulti?.has(ws) || false;
      if (isMulti) {
        if (username) {
          const conns = this.userConnections?.get(username);
          if (conns) {
            try { conns.delete(ws); } catch (e) {}
            if (conns.size === 0) {
              try { this.userConnections.delete(username); } catch (e) {}
            }
          }
        }
        if (roomName) {
          const clients = this.roomClients?.get(roomName);
          if (clients) try { clients.delete(ws); } catch (e) {}
        }
        try { this.wsSet?.delete(ws); } catch (e) {}
        try { this.wsActiveMulti?.delete(ws); } catch (e) {}

        state.cleanupDone = true;
        return result;
      }

      let stillConnected = false;
      if (username) {
        const conns = this.userConnections?.get(username);
        if (conns) {
          for (const c of conns) {
            if (c !== ws && c?.readyState === 1) {
              stillConnected = true;
              break;
            }
          }
        }
      }

      if (username && !stillConnected && !this._isRestoring) {
        // Hapus dari D1 + cache via _removeAllSeatsForUser (write-through)
        try { await this._forceDeleteFromD1(username); } catch (e) {}
        try { await this._removeAllSeatsForUser(username); } catch (e) {}
      }

      if (this.userConnections && username) {
        const conns = this.userConnections.get(username);
        if (conns) {
          try { conns.delete(ws); } catch (e) {}
          if (conns.size === 0) {
            try { this.userConnections.delete(username); } catch (e) {}
          }
        }
      }

      if (this.roomClients) {
        for (const [, clients] of this.roomClients) {
          try { clients.delete(ws); } catch (e) {}
        }
      }

      try { this.wsSet?.delete(ws); } catch (e) {}
      try { this.wsActiveMulti?.delete(ws); } catch (e) {}

      this._onlineUsersCache = null;
      this._onlineUsersCacheTime = 0;
      this._roomCountsCache = null;
      this._roomCountsCacheTime = 0;

      try {
        if (ws.readyState === 1) ws.close(1000, "Cleanup");
      } catch (e) {}

      state.cleanupDone = true;
    } catch (e) {
      state.cleanupDone = true;
    } finally {
      state.cleaning = false;
    }

    return result;
  }

  async webSocketClose(ws) {
    try {
      if (!ws) return;
      await this._cleanupUserCompletely(ws);
    } catch (e) {}
  }

  async webSocketError(ws) {
    try {
      if (!ws) return;
      await this._cleanupUserCompletely(ws);
    } catch (e) {}
  }

  // ============================================================
  // WS MESSAGE QUEUE (per-WS serial)
  // ============================================================
  async webSocketMessage(ws, msg) {
    try {
      if (!ws) return;

      if (!ws._msgQueue) ws._msgQueue = Promise.resolve();

      ws._msgQueue = ws._msgQueue
        .then(() => this._webSocketMessageInternal(ws, msg))
        .catch(e => {
          this._handleError('wsQueue', e);
        });

      return ws._msgQueue;
    } catch(e) {
      this._handleError('webSocketMessage', e);
    }
  }

  async _webSocketMessageInternal(ws, msg) {
    try {
      if (!this._restored && !this._restorePromise) {
        this._restorePromise = this._restoreWithRetry();
      }

      if (!ws || ws._closing || this.closing || this.isDestroyed || ws._cleaning) return;

      if ((!ws.username && !ws._username) || (!ws.room && !ws.roomname && !ws._room)) {
        try {
          const att = ws.deserializeAttachment?.();
          if (att?.username) {
            const found = await this._findUserInAnyRoom(att.username);
            if (found) {
              ws.username = att.username;
              ws._username = att.username;
              ws.room = found.room;
              ws.roomname = found.room;
              ws._room = found.room;
              ws.idtarget = att.username;

              let conns = this.userConnections.get(att.username);
              if (!conns) {
                conns = new Set();
                this.userConnections.set(att.username, conns);
              }
              conns.add(ws);

              const rc = this.roomClients.get(found.room);
              if (rc) rc.add(ws);

              const st = _wsCleanupState.get(ws);
              if (st) {
                st.cleanupDone = false;
                st.cleaning = false;
              } else {
                _wsCleanupState.set(ws, { cleanupDone: false, cleaning: false });
              }
            }
          }
        } catch(e) {}
      }

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
          setTimeout(() => this._processEventQueue(), 10);
        }
      } finally {
        this._processingQueue = false;
      }
    } catch(e) {
      this._handleError('_processEventQueue', e);
    }
  }

  // ============================================================
  // BROADCAST / SEND
  // ============================================================
  broadcast(room, msg) {
    try {
      if (this.closing || this.isDestroyed || !room || !msg) return 0;
      const clients = this.roomClients?.get(room);
      if (!clients || clients.size === 0) return 0;

      const msgStr = JSON.stringify(msg);
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

        let wsRoom = ws.room || ws.roomname || ws._room;

        if (!wsRoom) {
          try {
            const att = ws.deserializeAttachment?.();
            wsRoom = att?.seatInfo?.room || att?.room;
            if (wsRoom) {
              ws.room = wsRoom;
              ws.roomname = wsRoom;
              ws._room = wsRoom;
            }
          } catch(e) {}
        }

        if (!wsRoom) {
          continue;
        }
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

      this._roomCountsCache = null;
      this._roomCountsCacheTime = 0;
      this._onlineUsersCache = null;
      this._onlineUsersCacheTime = 0;

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

        let count = 0;
        for (const s of Object.values(allSeats)) {
          if (s?.namauser && String(s.namauser).trim() !== '') count++;
        }
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
            const selfPoint = allPoints[selfSeat];
            filteredPoints = filteredPoints.filter(p => p.seat !== selfSeat);

            if (selfPoint) {
              this.safeSend(ws, [
                "pointUpdated",
                room,
                selfSeat,
                selfPoint.x || 0,
                selfPoint.y || 0,
                selfPoint.fast ? 1 : 0
              ]);
            }
          }

          if (filteredPoints.length > 0) {
            this.safeSend(ws, ["allPointsList", room, filteredPoints]);
          }
        }
      } catch(e) {}
    } catch(e) {}
  }

  // ============================================================
  // ORPHAN CLEANUP (saat alarm — bukan periodik cache reload)
  // ============================================================
  async _verifyAndCleanupOrphanSeats(liveWsList) {
    try {
      if (this._isRestoring) return 0;

      await this._ensureCacheInitialized();
      const roomsData = this._storageCache?.roomsData || {};

      const liveUsernames = new Set();
      for (const ws of (liveWsList || [])) {
        try {
          if (!ws || ws.readyState !== 1) continue;
          let uname = ws.username || ws._username;
          if (!uname) {
            try {
              const att = ws.deserializeAttachment?.();
              if (att?.username) uname = att.username;
            } catch(e) {}
          }
          if (!uname) {
            for (const [user, conns] of (this.userConnections || new Map())) {
              if (conns?.has?.(ws)) { uname = user; break; }
            }
          }
          if (uname) liveUsernames.add(uname);
        } catch(e) {}
      }

      const orphanSeats = [];
      for (const [roomName, roomBucket] of Object.entries(roomsData)) {
        if (!ROOMS_SET.has(roomName)) continue;
        if (!roomBucket?.seat) continue;

        for (const [seatStr, seatData] of Object.entries(roomBucket.seat)) {
          if (!seatData?.namauser) continue;
          const seatNum = parseInt(seatStr);
          if (isNaN(seatNum) || seatNum < 1 || seatNum > C.MAX_SEATS) continue;

          const username = seatData.namauser;
          const isMulti = seatData.isMulti === true;

          if (isMulti) continue;
          if (liveUsernames.has(username)) continue;

          orphanSeats.push({ room: roomName, seat: seatNum, username });
        }
      }

      for (const orphan of orphanSeats) {
        const { room, seat, username } = orphan;
        try {
          // Write-through: D1 dulu, cache kemudian
          if (this.db) {
            try {
              await this.db
                .prepare(`DELETE FROM ${TABLE_NAME} WHERE key IN (?, ?)`)
                .bind(`seat_${room}_${seat}`, `point_${room}_${seat}`)
                .run();
            } catch(e) {}
          }

          const roomBucket = this._storageCache?.roomsData?.[room];
          if (roomBucket?.seat) delete roomBucket.seat[seat];
          if (roomBucket?.point) delete roomBucket.point[seat];

          if (username) this._removeUserIndex(username);
        } catch(e) {}
      }

      return orphanSeats.length;
    } catch(e) {
      return 0;
    }
  }

  // ============================================================
  // RESTORE ALL STATE
  // ============================================================
  async _restoreAllState() {
    try {
      this._isRestoring = true;
      this._restoreRemovedSeats = [];
      this._hasBroadcastRemoveKursi = new Set();

      // === 1. Bersihkan duplikat di D1 SEBELUM load cache ===
      if (this.db) {
        try {
          await this.db.prepare(`
            DELETE FROM ${TABLE_NAME}
            WHERE key LIKE 'seat_%'
            AND json_valid(value)
            AND key NOT IN (
              SELECT MIN(key) FROM ${TABLE_NAME}
              WHERE key LIKE 'seat_%' AND json_valid(value)
              GROUP BY json_extract(value, '$.namauser')
            )
          `).run();
        } catch(e) {}
      }

      // === 2. Load cache dari D1 (sudah bersih) ===
      try {
        await this._loadFromStorage();
        await this._ensureCacheInitialized();
      } catch(e) {}

      // === 3. Load multy state ===
      try {
        const allMulty = await this._loadAllMultyFromTable();

        for (const room of ROOMS) {
          const data = allMulty.get(room);
          if (!data) {
            const st = this._getMultyState(room);
            st.running = false;
            continue;
          }

          const { chatList, numberNext, index, running } = data;
          const st = this._getMultyState(room);

          st.chatList = chatList;
          st.numberNext = numberNext;
          st.index = (typeof index === 'number' && index >= 0 && index < chatList.length)
                      ? index
                      : 0;
          st.running = running === true;
        }

        this._multyRestored = true;
      } catch(e) {
        this._multyRestored = false;
      }

      // === 4. Rebuild noimg cache dari seat multi ===
      try {
        if (this._userNoimgCache.size === 0) {
          await this._ensureCacheInitialized();
          const roomsData = this._storageCache?.roomsData || {};
          let rebuilt = 0;
          for (const roomBucket of Object.values(roomsData)) {
            if (!roomBucket?.seat) continue;
            for (const seatData of Object.values(roomBucket.seat)) {
              if (seatData?.namauser && seatData.isMulti === true) {
                const noimg = parseInt(seatData.noimageUrl);
                if (!isNaN(noimg) && noimg > 0) {
                  this._userNoimgCache.set(seatData.namauser, noimg);
                  rebuilt++;
                }
              }
            }
          }
          if (rebuilt > 0) {
            this._saveUserNoimgCache().catch(() => {});
          }
        }
      } catch(e) {}

      for (const room of ROOMS) {
        if (!this.roomClients.has(room)) {
          this.roomClients.set(room, new Set());
        }
      }

      // === 5. Restore live websockets ===
      try {
        const webSockets = this.ctx?.getWebSockets?.() || [];

        const deadWebSockets = [];
        const liveWebSockets = [];

        for (const ws of webSockets) {
          try {
            if (ws && ws.readyState === 1) {
              liveWebSockets.push(ws);
            } else {
              deadWebSockets.push(ws);
            }
          } catch(e) {
            deadWebSockets.push(ws);
          }
        }

        const batchSize = 10;
        for (let i = 0; i < liveWebSockets.length; i += batchSize) {
          const batch = liveWebSockets.slice(i, i + batchSize);
          await Promise.allSettled(
            batch.map(ws => this._restoreLiveWebSocket(ws))
          );
        }

        for (const ws of deadWebSockets) {
          try {
            try {
              const att = ws.deserializeAttachment?.();
              if (att?.username) {
                ws.username = att.username;
                ws._username = att.username;
              }
              const room = att?.seatInfo?.room || att?.room;
              if (room) {
                ws.room = room;
                ws.roomname = room;
                ws._room = room;
              }
            } catch(e) {}

            await this._cleanupUserCompletely(ws, { skipBroadcast: true });
            try { ws.serializeAttachment({}); } catch(e) {}
          } catch(e) {}
        }

        for (const [user, conns] of this.userConnections) {
          if (conns.size === 0) this.userConnections.delete(user);
        }

        if (this._restoreRemovedSeats?.length) {
          const affectedRooms = new Set();

          for (const item of this._restoreRemovedSeats) {
            const { room, seat } = item;
            const key = `${room}_${seat}`;
            if (this._hasBroadcastRemoveKursi.has(key)) continue;
            this._hasBroadcastRemoveKursi.add(key);
            try {
              this.broadcast(room, ["removeKursi", room, seat]);
              affectedRooms.add(room);
            } catch(e) {}
          }

          for (const room of affectedRooms) {
            try {
              const count = await this._getRoomCount(room);
              this.broadcast(room, ["roomUserCount", room, count]);
            } catch(e) {}
          }
        }

      } catch(e) {

      } finally {
        this._restoreRemovedSeats = [];
        this._hasBroadcastRemoveKursi = new Set();
      }

      this._rebuildUserIndex();

      // === 6. Alarm ===
      if (!this.isDestroyed) {
        try {
          if (this.ctx && this.ctx.storage && typeof this.ctx.storage.setAlarm === 'function') {
            const existing = typeof this.ctx.storage.getAlarm === 'function'
              ? await this.ctx.storage.getAlarm().catch(() => null)
              : null;
            if (existing === null || existing === undefined) {
              await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
            }
          }
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

      this._restoreRemovedSeats = [];
      this._hasBroadcastRemoveKursi = new Set();

      if (!this.isDestroyed) {
        try {
          if (this.ctx && this.ctx.storage && typeof this.ctx.storage.setAlarm === 'function') {
            const existing = typeof this.ctx.storage.getAlarm === 'function'
              ? await this.ctx.storage.getAlarm().catch(() => null)
              : null;
            if (existing === null || existing === undefined) {
              await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
            }
          }
        } catch(e2) {}
      }

      await this._processPendingEvents();
      throw e;
    }
  }

  async _restoreLiveWebSocket(ws) {
    try {
      if (!ws) return;

      let isAlive = false;
      try { isAlive = (ws.readyState === 1); } catch(e) { isAlive = false; }
      if (!isAlive) return;

      try { await this._ensureCacheInitialized(); } catch(e) {}

      const attachment = ws.deserializeAttachment?.();
      if (!attachment?.username) {
        try { ws.close(1000, "No attachment"); } catch(e) {}
        return;
      }

      const found = await this._findUserInAnyRoom(attachment.username);

      let finalRoom = found?.room;
      let finalSeat = found?.seat;

      if (!finalRoom) {
        const attRoom = attachment.seatInfo?.room || attachment.room;
        if (attRoom && ROOMS_SET.has(attRoom)) {
          finalRoom = attRoom;
          finalSeat = attachment.seatInfo?.seat || null;
          try {
            const roomBucket = await this._getRoomBucket(attRoom);
            if (roomBucket?.seat && finalSeat) {
              const seatData = roomBucket.seat[finalSeat];
              if (!seatData || seatData.namauser !== attachment.username) {
                try { ws.close(1000, "User not in seat"); } catch(e) {}
                return;
              }
            }
          } catch(e) {}
        }
      }

      if (!finalRoom) {
        try { ws.close(1000, "User not found"); } catch(e) {}
        return;
      }

      ws.username = attachment.username;
      ws.room = finalRoom;
      ws.roomname = finalRoom;
      ws.idtarget = attachment.username;
      ws._closing = false;
      ws._cleaning = false;
      ws._username = attachment.username;
      ws._room = finalRoom;
      ws._wsId = Date.now() + Math.random();

      const state = _wsCleanupState.get(ws);
      if (state) {
        state.cleanupDone = false;
        state.cleaning = false;
      } else {
        _wsCleanupState.set(ws, { cleanupDone: false, cleaning: false });
      }

      const roomClients = this.roomClients?.get(finalRoom);
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

      if (finalRoom && finalSeat) {
        const foundInfo = await this._findUserInAnyRoom(attachment.username);
        const isMultiUser = foundInfo?.isMulti === true;
        this._setUserIndex(attachment.username, finalRoom, finalSeat, isMultiUser);

        if (isMultiUser) {
          try {
            this.wsActiveMulti?.set(ws, { username: attachment.username, room: finalRoom });
          } catch(e) {}
        }
      }

      try {
        const st = this._getMultyState(finalRoom);
        if (st.running && !st.loopTimer) {
          this._startMultyLoop(finalRoom);
        }
      } catch(e) {}

      try {
        ws.serializeAttachment({
          username: attachment.username,
          seatInfo: { room: finalRoom, seat: finalSeat }
        });
      } catch(e) {}
    } catch(e) {}
  }

  // ============================================================
  // SET ID
  // ============================================================
  async _handleSetId(ws, username, isNewUser) {
    try {
      if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
        try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
        return;
      }

      const locked = await this._withLock(
        this._userJoinLock,
        `join_user_${username}`,
        async () => {
          const isMultiUser = this._userNoimgCache?.has(username) === true;

          if (!isNewUser && isMultiUser) {
            ws.username = username;
            ws.idtarget = username;
            ws._username = username;
            try { ws.serializeAttachment({ username: username }); } catch(e) {}
            return { skip: true };
          }

          if (this._isRestoring) {
            return { skip: true };
          }

          await this._removeAllSeatsForUser(username);

          return { skip: false };
        },
        C.USER_JOIN_LOCK_TIMEOUT
      );

      if (locked === null) {
        return;
      }
      if (locked?.skip) {
        return;
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

  // ============================================================
  // HANDLE MESSAGE
  // ============================================================
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

      try {
        const att = ws.deserializeAttachment?.();
        if (att?.pendingStateSend && (Date.now() - att.pendingStateSend) >= 1000) {
          const pendingRoom = att.pendingStateRoom;
          att.pendingStateSend = null;
          att.pendingStateRoom = null;
          att.pendingStateUsername = null;
          try { ws.serializeAttachment(att); } catch(e) {}
          if (pendingRoom) {
            this.sendAllStateTo(ws, pendingRoom, true).catch(() => {});
          }
        }
      } catch(e) {}

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

  // ============================================================
  // EVENT HANDLER
  // ============================================================
  async _handleEventInternal(ws, data) {
    try {
      if (!ws || !data || !data[0]) return;
      const [evt, ...args] = data;
      await this._ensureCacheInitialized();

      switch(evt) {
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          for (const room of ROOMS) {
            const st = this._getMultyState(room);
            if (st.running || st.chatList.length > 0) {
              this.safeSend(ws, ["multyStatus", st.running, st.index, st.chatList.length, room]);
              this.safeSend(ws, ["multyNumber", st.numberNext, room]);
            }
          }
          break;

        case "getChatHistory": {
          try {
            const room = args[0];

            if (!room || !ROOMS_SET.has(room)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }

            const reset = await this._checkAndResetHistory(room);

            if (reset) {
              this.safeSend(ws, ["chatHistory", room, "{}", true]);
              this.safeSend(ws, ["chatHistoryReset", room]);
              break;
            }

            const history = await this._loadHistoryChat(room);

            const javaJsonObject = {};
            for (const item of history) {
              javaJsonObject[String(item.timestamp)] = item.java;
            }

            const isEmpty = history.length === 0;

            this.safeSend(ws, [
              "chatHistory",
              room,
              JSON.stringify(javaJsonObject),
              isEmpty
            ]);
          } catch(e) {
            this.safeSend(ws, ["error", "Gagal load history"]);
          }
          break;
        }

        case "setIdTarget2":
          await this._handleSetId(ws, args[0], args[1]);
          break;

        case "joinRoom":
          await this._handleJoin(ws, args[0]);
          break;

        case "startMulty": {
          try {
            const startRoom = args[0] || DEFAULT_MULTY_ROOM;
            if (!ROOMS_SET.has(startRoom)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }

            const st = this._getMultyState(startRoom);

            if (st.running) {
              this.safeSend(ws, ["multyStatus", st.running, st.index, st.chatList.length, startRoom]);
              break;
            }

            const { chatList, numberNext, index } = await this._loadMultyFromTable(startRoom);
            if (!Array.isArray(chatList) || chatList.length === 0) {
              this.safeSend(ws, ["error", `Multy chat kosong untuk room ${startRoom}`]);
              break;
            }

            st.chatList = chatList;
            st.numberNext = numberNext;
            st.index = (typeof index === 'number' && index >= 0 && index < chatList.length)
                        ? index
                        : 0;
            st.running = true;

            this._saveMultyRunningToTable(startRoom, true).catch(() => {});

            this.safeSend(ws, ["multyStatus", true, st.index, chatList.length, startRoom]);
            this.safeSend(ws, ["multyNumber", st.numberNext, startRoom]);
            this.safeSend(ws, ["multyRoom", startRoom]);

            this._startMultyLoop(startRoom);
          } catch(e) {
            this._handleError('startMulty', e);
          }
          break;
        }

        case "stopMulty": {
          try {
            const stopRoom = args[0] || DEFAULT_MULTY_ROOM;
            if (!ROOMS_SET.has(stopRoom)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }

            const st = this._getMultyState(stopRoom);
            st.running = false;
            st.index = 0;
            this._stopMultyLoop(stopRoom);

            this._saveMultyRunningToTable(stopRoom, false).catch(() => {});
            this._saveMultyIndexToTable(stopRoom, 0).catch(() => {});

            this.broadcast(stopRoom, ["multyStop", stopRoom]);
            this.safeSend(ws, ["multyStatus", false, st.index, st.chatList.length, stopRoom]);
          } catch(e) {
            this._handleError('stopMulty', e);
          }
          break;
        }

        case "getMultyStatus": {
          const statusRoom = args[0] || DEFAULT_MULTY_ROOM;
          if (!ROOMS_SET.has(statusRoom)) {
            this.safeSend(ws, ["error", "Invalid room"]);
            break;
          }
          const st = this._getMultyState(statusRoom);
          this.safeSend(ws, ["multyStatus", st.running, st.index, st.chatList.length, statusRoom]);
          this.safeSend(ws, ["multyNumber", st.numberNext, statusRoom]);
          this.safeSend(ws, ["multyRoom", statusRoom]);
          break;
        }

        case "getAllMultyStatus": {
          const list = [];
          for (const room of ROOMS) {
            const st = this._getMultyState(room);
            list.push([room, st.running, st.index, st.chatList.length, st.numberNext]);
          }
          this.safeSend(ws, ["allMultyStatus", list]);
          break;
        }

        case "getMultyChatData": {
          try {
            const room = args[0] || DEFAULT_MULTY_ROOM;
            if (!ROOMS_SET.has(room)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }
            const { chatList } = await this._loadMultyFromTable(room);
            const jsonStr = JSON.stringify(chatList, null, 2);
            this.safeSend(ws, ["multyChatData", jsonStr, chatList.length, room]);
          } catch(e) {
            this.safeSend(ws, ["error", "Gagal load JSON"]);
          }
          break;
        }

        case "replaceMultyChat": {
          try {
            const room = args[0] || DEFAULT_MULTY_ROOM;
            const newArr = args[1];
            if (!ROOMS_SET.has(room)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }
            if (!Array.isArray(newArr)) {
              this.safeSend(ws, ["error", "Data bukan array"]);
              break;
            }

            const valid = [];
            for (const item of newArr) {
              if (!item || typeof item !== 'object') continue;
              if (!item.sender || !item.text) continue;
              valid.push({
                noimg: item.noimg ?? 10000,
                sender: String(item.sender),
                text: String(item.text),
                color: item.color ? String(item.color) : "7",
                textColor: item.textColor ? String(item.textColor) : "1"
              });
            }

            if (valid.length === 0) {
              this.safeSend(ws, ["error", "Tidak ada chat valid"]);
              break;
            }

            const ok = await this._saveMultyChatToTable(room, valid);
            if (!ok) {
              this.safeSend(ws, ["error", "Gagal simpan JSON"]);
              break;
            }

            const st = this._getMultyState(room);
            st.chatList = valid;
            st.index = 0;
            this._saveMultyIndexToTable(room, 0).catch(() => {});

            this.safeSend(ws, ["multyChatReloaded", valid.length, room]);
            this.broadcast(room, ["multyDataUpdated", room, valid.length, st.numberNext]);
          } catch(e) {
            this.safeSend(ws, ["error", "Gagal simpan JSON"]);
          }
          break;
        }

        case "getMultyNumberData": {
          try {
            const room = args[0] || DEFAULT_MULTY_ROOM;
            if (!ROOMS_SET.has(room)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }
            const { numberNext } = await this._loadMultyFromTable(room);
            this.safeSend(ws, ["multyNumberData", numberNext, room]);
          } catch(e) {
            this.safeSend(ws, ["error", "Gagal load number"]);
          }
          break;
        }

        case "replaceMultyNumber": {
          try {
            const room = args[0] || DEFAULT_MULTY_ROOM;
            const newNum = parseInt(args[1]);
            if (!ROOMS_SET.has(room)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }
            if (isNaN(newNum) || newNum < 1 || newNum > C.MAX_MULTY_NUMBER) {
              this.safeSend(ws, ["error", "Number invalid"]);
              break;
            }

            const ok = await this._saveMultyNumberToTable(room, newNum);
            if (!ok) {
              this.safeSend(ws, ["error", "Gagal simpan number"]);
              break;
            }

            const st = this._getMultyState(room);
            st.numberNext = newNum;

            this.safeSend(ws, ["multyNumberSaved", newNum, room]);
            this.broadcast(room, ["multyNumber", newNum, room]);
          } catch(e) {
            this.safeSend(ws, ["error", "Gagal simpan number"]);
          }
          break;
        }

        case "replaceMultyData": {
          try {
            const room = args[0] || DEFAULT_MULTY_ROOM;
            const newArr = args[1];
            const newNum = (args[2] !== undefined && args[2] !== null) ? parseInt(args[2]) : null;

            if (!ROOMS_SET.has(room)) {
              this.safeSend(ws, ["error", "Invalid room"]);
              break;
            }

            const results = { chat: null, number: null };

            if (Array.isArray(newArr)) {
              const valid = [];
              for (const item of newArr) {
                if (!item || typeof item !== 'object') continue;
                if (!item.sender || !item.text) continue;
                valid.push({
                  noimg: item.noimg ?? 10000,
                  sender: String(item.sender),
                  text: String(item.text),
                  color: item.color ? String(item.color) : "7",
                  textColor: item.textColor ? String(item.textColor) : "1"
                });
              }

              if (valid.length > 0) {
                const ok = await this._saveMultyChatToTable(room, valid);
                if (ok) {
                  const st = this._getMultyState(room);
                  st.chatList = valid;
                  st.index = 0;
                  this._saveMultyIndexToTable(room, 0).catch(() => {});
                  results.chat = valid.length;
                }
              }
            }

            if (newNum !== null && !isNaN(newNum) && newNum >= 1 && newNum <= C.MAX_MULTY_NUMBER) {
              const ok = await this._saveMultyNumberToTable(room, newNum);
              if (ok) {
                const st = this._getMultyState(room);
                st.numberNext = newNum;
                results.number = newNum;
              }
            }

            this.safeSend(ws, ["multyDataSaved", room, results]);
            this.broadcast(room, ["multyDataUpdated", room, results.chat || 0, results.number || 0]);
          } catch(e) {
            this.safeSend(ws, ["error", "Gagal simpan data"]);
          }
          break;
        }

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
              await this._deleteSeatInRoom(roomName, seatNumber, true);
            }
            this._removeUserIndex(targetUsername);
          } catch(e) {}
          break;
        }

        case "setActiveMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;

          const allSeats = await this._findAllSeatsForUser(targetUsername);
          let found = allSeats.find(s => s.isMulti === true) || allSeats[0] || null;

          if (!found) {
            const joined = await this._handleMultiJoin(ws, targetUsername, DEFAULT_MULTY_ROOM);
            if (!joined) break;
            found = { room: joined.room, seat: joined.seat, isMulti: true };
          }

          const roomName = found.room;
          const seatNumber = found.seat;

          if (allSeats.length > 1) {
            await this._removeAllSeatsForUser(targetUsername, {
              keepRoom: roomName,
              keepSeat: seatNumber
            });
          }

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

          const currentUser = ws.username || ws._username;
          if (!currentUser) break;

          const seatData = await this._getSeatData(kursiRoom, kursiSeat);
          if (!seatData || seatData.namauser !== currentUser) break;

          try {
            await this._withLock(
              this._kursiLocks,
              `kursi_${kursiRoom}_${kursiSeat}`,
              async () => {
                const updateData = {
                  noimageUrl: String(kursiNoimg || ""),
                  namauser: seatData.namauser,
                  color: String(kursiColor || ""),
                  itembawah: typeof kursiBawah === 'number' ? kursiBawah : (parseInt(kursiBawah) || 0),
                  itematas: typeof kursiAtas === 'number' ? kursiAtas : (parseInt(kursiAtas) || 0),
                  vip: typeof kursiVip === 'number' ? kursiVip : (parseInt(kursiVip) || 0),
                  viptanda: typeof kursiVt === 'number' ? kursiVt : (parseInt(kursiVt) || 0),
                  isMulti: seatData.isMulti === true
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

          const chatData = ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor];

          this.broadcast(chatRoom, chatData);
          this._saveHistoryChat(chatRoom, chatData).catch(() => {});

          break;
        }

        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;

          if (!pointRoom || typeof pointSeat !== 'number') break;
          if (!ROOMS_SET.has(pointRoom)) break;

          const currentUser = ws.username || ws._username;
          if (!currentUser) break;

          const updated = await this._updatePointDirect(
            pointRoom, pointSeat, pointX, pointY, pointFast === 1
          );

          if (updated) {
            this.broadcast(pointRoom, [
              "pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast
            ]);
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

        case "getOnlineUsers": {
          const now = Date.now();
          const users = [];
          const seen = new Set();
          await this._ensureCacheInitialized();
          const roomsData = this._storageCache?.roomsData || {};

          const userMap = new Map();
          for (const [roomName, roomBucket] of Object.entries(roomsData)) {
            if (!roomBucket?.seat) continue;
            for (const [seat, data] of Object.entries(roomBucket.seat)) {
              const username = data?.namauser;
              if (!username || String(username).trim() === '') continue;

              const prev = userMap.get(username);
              if (!prev || (data.isMulti === true && prev.isMulti !== true)) {
                userMap.set(username, { isMulti: data.isMulti === true });
              }
            }
          }

          for (const [username, info] of userMap) {
            if (seen.has(username)) continue;

            let isOnline = false;
            if (info.isMulti === true) {
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

          this._onlineUsersCache = users;
          this._onlineUsersCacheTime = now;

          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }

        case "getAllRoomsUserCount": {
          await this._ensureCacheInitialized();
          const counts = {};
          for (const room of ROOMS) {
            const roomBucket = this._storageCache?.roomsData?.[room];
            let count = 0;
            if (roomBucket?.seat) {
              for (const seat in roomBucket.seat) {
                const uname = roomBucket.seat[seat]?.namauser;
                if (uname && String(uname).trim() !== '') count++;
              }
            }
            counts[room] = count;
          }

          const entries = Object.entries(counts);
          this._roomCountsCache = entries;
          this._roomCountsCacheTime = Date.now();

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
              const removedMultiUsernames = [];
              for (const seatStr in roomBucket.seat) {
                const seatData = roomBucket.seat[seatStr];
                const uname = seatData?.namauser;
                const isMulti = seatData?.isMulti === true;
                if (uname) {
                  this._removeUserIndex(uname);
                  if (isMulti) removedMultiUsernames.push(uname);
                }
              }
              roomBucket.seat = {};
              roomBucket.point = {};
              roomBucket.mute = false;

              if (removedMultiUsernames.length > 0) {
                for (const u of removedMultiUsernames) {
                  this._userNoimgCache.delete(u);
                }
                this._saveUserNoimgCache().catch(() => {});
              }

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

  // ============================================================
  // FETCH
  // ============================================================
  async fetch(req) {
    try {
      if (!this._restored && !this._restorePromise) {
        this._restorePromise = this._restoreWithRetry();
      }

      if (this.closing) {
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
            this._storageCache = this._storageCache || { roomsData: {}, currentNumber: 1 };
            this._cacheInitialized = true;
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
          const runningRooms = this._getRunningRooms();
          const multyStatus = {};
          for (const room of ROOMS) {
            const st = this._getMultyState(room);
            multyStatus[room] = {
              running: st.running,
              index: st.index,
              total: st.chatList.length,
              numberNext: st.numberNext,
              loopActive: !!st.loopTimer,
            };
          }

          const noimgCacheObj = {};
          for (const [u, n] of this._userNoimgCache) {
            noimgCacheObj[u] = n;
          }

          // Debug: hitung total seat di cache
          let totalSeats = 0;
          for (const room of ROOMS) {
            const rb = this._storageCache?.roomsData?.[room];
            if (rb?.seat) {
              totalSeats += Object.keys(rb.seat).length;
            }
          }

          return new Response(JSON.stringify({
            currentNumber: this.currentNumber,
            alarmActive: !!(await this.ctx?.storage?.getAlarm().catch(() => null)),
            intervalMin: C.NUMBER_INTERVAL_MS / 60000,
            multyRooms: multyStatus,
            runningRooms: runningRooms,
            multyRestored: this._multyRestored,
            noimgCacheSize: this._userNoimgCache?.size || 0,
            noimgCache: noimgCacheObj,
            historyTables: Array.from(this._historyTableReady),
            totalSeatsInCache: totalSeats,
            cacheInitialized: this._cacheInitialized,
            restoreFailed: this._restoreFailed,
          }), {
            status: 200,
            headers: { "Content-Type": "application/json", "Cache-Control": "no-cache" }
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
          _wsCleanupState.set(server, { cleanupDone: false, cleaning: false });
        } catch(e) {}

        if (!this.wsSet?.has(server)) try { this.wsSet?.add(server); } catch(e) {}

        return new Response(null, { status: 101, webSocket: client });
      } catch(e) {
        if (e?.overloaded || e?.retryable) {
          return new Response("Overloaded", {
            status: 503,
            headers: { 'Retry-After': '2', 'Content-Type': 'text/plain' }
          });
        }
        this._handleError('fetch', e);
        return new Response("Internal Server Error", { status: 500 });
      }
    } catch(e) {
      if (e?.overloaded || e?.retryable) {
        return new Response("Overloaded", {
          status: 503,
          headers: { 'Retry-After': '2', 'Content-Type': 'text/plain' }
        });
      }
      this._handleError('fetch', e);
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ============================================================
  // ERROR
  // ============================================================
  _handleError(type, error) {
    try {
      if (error?.overloaded || error?.retryable) {
        return;
      }

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

  // ============================================================
  // DESTROY
  // ============================================================
  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;

    this._stopAllMultyLoops();

    const normalUsers = new Set();
    try {
      await this._ensureCacheInitialized();
      const roomsData = this._storageCache?.roomsData || {};
      for (const rBucket of Object.values(roomsData)) {
        if (!rBucket?.seat) continue;
        for (const data of Object.values(rBucket.seat)) {
          if (data?.namauser && data.isMulti !== true) normalUsers.add(data.namauser);
        }
      }
    } catch (e) {}

    const wsCopy = Array.from(this.wsSet || new Set());
    for (const ws of wsCopy) {
      if (!ws) continue;
      const isMulti = this.wsActiveMulti?.has(ws) || false;
      try { if (ws.readyState === 1) ws.close(1000, "Shutdown"); } catch (e) {}
      if (isMulti) {
        try { this.wsSet?.delete(ws); } catch (e) {}
        try { this.wsActiveMulti?.delete(ws); } catch (e) {}
        const room = ws.room || ws.roomname;
        if (room) {
          const rc = this.roomClients?.get(room);
          if (rc) try { rc.delete(ws); } catch (e) {}
        }
      }
    }

    if (this.db && normalUsers.size > 0) {
      try {
        await this.db
          .prepare(`DELETE FROM ${TABLE_NAME} WHERE key LIKE 'seat_%' AND value NOT LIKE '%"isMulti":true%'`)
          .run();
      } catch (e) {}

      try {
        await this.db
          .prepare(`DELETE FROM ${TABLE_NAME} WHERE key LIKE 'point_%'`)
          .run();
      } catch (e) {}
    }

    if (this.userConnections) {
      for (const [username, conns] of this.userConnections) {
        try { conns.clear(); } catch (e) {}
        try { this.userConnections.delete(username); } catch (e) {}
      }
    }
    if (this.roomClients) {
      for (const [, clients] of this.roomClients) {
        try { clients.clear(); } catch (e) {}
      }
    }
    try { this.wsSet?.clear(); } catch (e) {}
    try { this.wsActiveMulti?.clear(); } catch (e) {}
    this._storageCache = { roomsData: {}, currentNumber: 1 };
    this._onlineUsersCache = null;
    this._onlineUsersCacheTime = 0;
    this._roomCountsCache = null;
    this._roomCountsCacheTime = 0;
    this._restoreRemovedSeats = [];
    this._hasBroadcastRemoveKursi = new Set();
    this._userIndex = new Map();
    this._userNoimgCache = new Map();

    this._multyState = new Map();
    this._multyRestored = false;
    this._historyTableReady = new Set();

    this._globalLocks = new Map();
    this._roomLocks = new Map();
    this._roomWriteLocks = new Map();
    this._joinCooldown = new Map();

    this.isDestroyed = true;
  }
}

export default ChatServer;
