// ============================================================
// GAME-SERVER-D1-NON-HIBERNASI-FINAL.js
// VERSION: 15.0.0 - NON-HIBERNATION + FULL D1 + ALARM
// ============================================================

// ============================================================
// CONSTANTS
// ============================================================

const CONSTANTS = {
  MAX_LOWCARD_GAMES: 10,
  REGISTRATION_TIME_MS: 20000,
  DRAW_TIME_MS: 20000,
  EVALUATION_DELAY_MS: 2000,
  MAX_BOTS_PER_GAME: 4,
  MAX_BET: 100000,
  EVALUATION_TIMEOUT_MS: 30000,
  MAX_PLAYERS_PER_GAME: 45,
  MAX_WS_CLIENTS: 150,
  MAX_EVENT_QUEUE_SIZE: 50,
  ERROR_RESET_INTERVAL_MS: 60000,

  DICE_ANSWER_TIME_MS: 20000,
  DICE_TOTAL_TIME_MS: 20000,
  MAX_DICE_VALUE: 6,
  DICE_ROOM: "Quiz",

  DICE_AUTO_START_DELAY_MS: 3000,
  TIE_BREAKER_TIME_LIMIT: 20,
  TIE_BREAKER_COOLDOWN: 15000,

  BROADCAST_BATCH_SIZE: 10,
  CPU_YIELD_MS: 1,
  MAX_PROCESS_TIME_MS: 500,
  MAX_QUEUE_SIZE: 50,
  RATE_LIMIT_MAX: 100,
  RATE_LIMIT_WINDOW_MS: 60000,
  MAX_BOT_TIMEOUTS: 5,

  WEEKLY_RESET_DAY: 1,
  WEEKLY_RESET_HOUR: 0,
  WEEKLY_RESET_ALARM: 'weekly_reset',

  MAX_LEADERBOARD_LIMIT: 30,
  MIN_LEADERBOARD_LIMIT: 1,
  DEFAULT_LEADERBOARD_LIMIT: 10,

  PROCESS_BATCH_SIZE: 50,
  PROCESS_MAX_TIME_MS: 100,

  DICE_SESSION_CHECK_INTERVAL_MS: 5000,
  DICE_BROADCAST_DELAY_MS: 5000,
};

const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: "01:00", end: "02:00" },
    { start: "13:00", end: "14:00" },
    { start: "22:00", end: "23:00" }
  ],
  TIMEZONE_OFFSET: 8,
};

const TABLE_NAME = 'game_data';

function parseTime(timeStr) {
  const [hours, minutes] = timeStr.split(':').map(Number);
  return hours * 60 + minutes;
}

// ============================================================
// DATA MANAGER (Full D1 — termasuk activeGames)
// ============================================================

class DataManager {
  constructor(db) {
    this.db = db;
    this._cache = {
      recordingStatusMap: {},
      winnersMap: {},
      dicePoints: {},
      lastWeekWinner: null,
      lastResetWeek: null,
      scheduled_alarms: {},
      activeGames: {},
    };
    this._cacheInitialized = false;
    this._cacheLoading = false;
    this._restored = false;
  }

  async init() {
    try {
      await this.db.prepare(`
        CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `).run();
      return true;
    } catch (e) {
      return false;
    }
  }

  async loadAllData() {
    if (this._cacheLoading) {
      let waitCount = 0;
      while (this._cacheLoading && waitCount < 50) {
        await new Promise(resolve => setTimeout(resolve, 100));
        waitCount++;
      }
      return this._cache;
    }

    this._cacheLoading = true;
    try {
      const result = await this.db.prepare(`SELECT key, value FROM ${TABLE_NAME}`).all();

      const cache = {
        recordingStatusMap: {},
        winnersMap: {},
        dicePoints: {},
        lastWeekWinner: null,
        lastResetWeek: null,
        scheduled_alarms: {},
        activeGames: {},
      };

      const results = result.results || [];
      for (const row of results) {
        const key = row.key;
        let value;
        try { value = JSON.parse(row.value); } catch (e) { continue; }

        switch (key) {
          case 'recordingStatusMap': cache.recordingStatusMap = value || {}; break;
          case 'winnersMap': cache.winnersMap = value || {}; break;
          case 'dicePoints': cache.dicePoints = value || {}; break;
          case 'lastWeekWinner': cache.lastWeekWinner = value || null; break;
          case 'lastResetWeek': cache.lastResetWeek = value || null; break;
          case 'scheduled_alarms': cache.scheduled_alarms = value || {}; break;
          case 'activeGames': cache.activeGames = value || {}; break;
        }
      }

      this._cache = cache;
      this._cacheInitialized = true;
      this._restored = true;
      return this._cache;

    } catch (e) {
      this._cache = {
        recordingStatusMap: {},
        winnersMap: {},
        dicePoints: {},
        lastWeekWinner: null,
        lastResetWeek: null,
        scheduled_alarms: {},
        activeGames: {},
      };
      this._cacheInitialized = true;
      this._restored = true;
      return this._cache;
    } finally {
      this._cacheLoading = false;
    }
  }

  async _ensureCacheInitialized() {
    if (this._cacheInitialized && this._cache) return this._cache;
    return await this.loadAllData();
  }

  async _save(key, value) {
    try {
      if (value === null || value === undefined ||
          (typeof value === 'object' && Object.keys(value).length === 0) ||
          (Array.isArray(value) && value.length === 0)) {
        await this.db.prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`).bind(key).run();
        return;
      }
      await this.db.prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value) VALUES (?, ?)`)
        .bind(key, JSON.stringify(value)).run();
    } catch (e) {}
  }

  // ---------- recording ----------
  async getRecordingStatus(room) {
    await this._ensureCacheInitialized();
    return this._cache.recordingStatusMap[room] === true;
  }

  async setRecordingStatus(room, enabled) {
    await this._ensureCacheInitialized();
    if (enabled) this._cache.recordingStatusMap[room] = true;
    else delete this._cache.recordingStatusMap[room];
    await this._save('recordingStatusMap', this._cache.recordingStatusMap);
    return true;
  }

  // ---------- winners ----------
  async getWinners(room) {
    await this._ensureCacheInitialized();
    return this._cache.winnersMap[room] || {};
  }

  async setWinners(room, winners) {
    await this._ensureCacheInitialized();
    if (winners && Object.keys(winners).length > 0) this._cache.winnersMap[room] = winners;
    else delete this._cache.winnersMap[room];
    await this._save('winnersMap', this._cache.winnersMap);
    return true;
  }

  async addWinner(room, username) {
    await this._ensureCacheInitialized();
    const winners = await this.getWinners(room);
    let count = parseInt(String(winners[username] || "0").replace("x", "")) || 0;
    winners[username] = (count + 1) + "x";
    await this.setWinners(room, winners);
    return true;
  }

  async deleteAllWinners(room) {
    await this._ensureCacheInitialized();
    delete this._cache.winnersMap[room];
    await this._save('winnersMap', this._cache.winnersMap);
    return true;
  }

  // ---------- dice points ----------
  async getDicePoints() {
    await this._ensureCacheInitialized();
    return this._cache.dicePoints;
  }

  async setDicePoints(points) {
    await this._ensureCacheInitialized();
    const cleanPoints = {};
    for (const [username, score] of Object.entries(points || {})) {
      if (username && typeof username === 'string') {
        const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
        if (numericScore > 0) cleanPoints[username] = numericScore;
      }
    }
    this._cache.dicePoints = cleanPoints;
    await this._save('dicePoints', this._cache.dicePoints);
    return true;
  }

  async addDicePoint(username) {
    await this._ensureCacheInitialized();
    const points = await this.getDicePoints();
    points[username] = (points[username] || 0) + 1;
    await this.setDicePoints(points);
    return points;
  }

  async resetDicePoints() {
    await this._ensureCacheInitialized();
    this._cache.dicePoints = {};
    await this._save('dicePoints', this._cache.dicePoints);
    return true;
  }

  async getLeaderboard(limit = 10) {
    await this._ensureCacheInitialized();
    const points = this._cache.dicePoints;
    if (!points || Object.keys(points).length === 0) return [];
    const safeLimit = Math.min(Math.max(limit, CONSTANTS.MIN_LEADERBOARD_LIMIT), CONSTANTS.MAX_LEADERBOARD_LIMIT);
    const sorted = Object.entries(points)
      .filter(([u, s]) => u && s > 0)
      .sort((a, b) => b[1] - a[1])
      .slice(0, safeLimit);
    return sorted.map(([u, s]) => `${u}|${s}`);
  }

  // ---------- last week winner ----------
  async getLastWeekWinner() {
    await this._ensureCacheInitialized();
    return this._cache.lastWeekWinner;
  }

  async setLastWeekWinner(winnerData) {
    await this._ensureCacheInitialized();
    this._cache.lastWeekWinner = winnerData;
    await this._save('lastWeekWinner', this._cache.lastWeekWinner);
    return true;
  }

  async deleteLastWeekWinner() {
    await this._ensureCacheInitialized();
    this._cache.lastWeekWinner = null;
    await this._save('lastWeekWinner', this._cache.lastWeekWinner);
    return true;
  }

  // ---------- last reset week ----------
  async getLastResetWeek() {
    await this._ensureCacheInitialized();
    return this._cache.lastResetWeek;
  }

  async setLastResetWeek(week) {
    await this._ensureCacheInitialized();
    this._cache.lastResetWeek = week;
    await this._save('lastResetWeek', this._cache.lastResetWeek);
    return true;
  }

  // ---------- alarms ----------
  async getAlarms() {
    await this._ensureCacheInitialized();
    return this._cache.scheduled_alarms;
  }

  async setAlarms(alarms) {
    await this._ensureCacheInitialized();
    this._cache.scheduled_alarms = alarms;
    await this._save('scheduled_alarms', this._cache.scheduled_alarms);
    return true;
  }

  async deleteAlarms() {
    await this._ensureCacheInitialized();
    this._cache.scheduled_alarms = {};
    await this._save('scheduled_alarms', this._cache.scheduled_alarms);
    return true;
  }

  // ---------- activeGames (FULL D1) ----------
  async getActiveGames() {
    await this._ensureCacheInitialized();
    return this._cache.activeGames || {};
  }

  async setActiveGames(games) {
    await this._ensureCacheInitialized();
    this._cache.activeGames = games || {};
    await this._save('activeGames', this._cache.activeGames);
    return true;
  }

  // ---------- week util ----------
  getCurrentWeek() {
    const now = new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }

  isRestored() { return this._restored; }
}

// ============================================================
// ALARM SCHEDULER (tetap pakai ctx.storage.setAlarm)
// ============================================================

class AlarmScheduler {
  constructor(db, ctx) {
    this.db = db;
    this.ctx = ctx;
    this.dataManager = new DataManager(db);
    this._alarms = new Map();
  }

  async scheduleAlarms() {
    try {
      const now = new Date();
      const witaNow = this._toWITA(now);
      const currentTotal = witaNow.getHours() * 60 + witaNow.getMinutes();

      await this._clearAllAlarms();
      await this._scheduleWeeklyResetUTC();

      let currentSession = null;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        const endTotal = parseTime(session.end);
        if (currentTotal >= startTotal && currentTotal < endTotal) {
          currentSession = { ...session, startTotal, endTotal, status: 'active' };
          break;
        }
      }

      if (currentSession) {
        const endDelay = (currentSession.endTotal - currentTotal) * 60 * 1000;
        if (endDelay > 0) await this._scheduleAlarm('dice_session_end', endDelay);
        await this._scheduleAlarm('dice_session_start_immediate', 1000);
        return true;
      }

      let nextSession = null;
      let minDiff = Infinity;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) {
          minDiff = diff;
          nextSession = { ...session, startTotal, status: 'upcoming' };
        }
      }

      if (nextSession) {
        let startDelay = minDiff * 60 * 1000;
        if (startDelay < 0) startDelay = 0;
        await this._scheduleAlarm('dice_session_start', startDelay);
        const endTotal = parseTime(nextSession.end);
        const endDelay = (endTotal - currentTotal) * 60 * 1000;
        if (endDelay > 0) await this._scheduleAlarm('dice_session_end', endDelay);
      }
      return true;
    } catch (e) { return false; }
  }

  async _scheduleWeeklyResetUTC() {
    try {
      const now = new Date();
      const currentDay = now.getUTCDay();
      const currentHour = now.getUTCHours();
      const currentMinutes = now.getUTCMinutes();
      const currentSeconds = now.getUTCSeconds();
      let daysUntilReset = CONSTANTS.WEEKLY_RESET_DAY - currentDay;
      if (daysUntilReset < 0) daysUntilReset += 7;
      if (daysUntilReset === 0 && (currentHour > 0 || currentMinutes > 0 || currentSeconds > 0)) daysUntilReset = 7;
      const resetTime = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + daysUntilReset, 0, 0, 0, 0));
      const delayMs = resetTime.getTime() - now.getTime();
      if (delayMs > 0) await this._scheduleAlarm(CONSTANTS.WEEKLY_RESET_ALARM, delayMs);
      return true;
    } catch (e) { return false; }
  }

  async _scheduleAlarm(name, delayMs) {
    try {
      if (delayMs < 1000) delayMs = 1000;
      const scheduledAt = Date.now() + delayMs;
      const alarm = { name, scheduledAt, delayMs, timestamp: Date.now() };
      this._alarms.set(name, alarm);
      await this.dataManager.setAlarms(Object.fromEntries(this._alarms));
      await this._scheduleNearestAlarm();
      return true;
    } catch (e) { return false; }
  }

  async _scheduleNearestAlarm() {
    try {
      let nearestTime = Infinity;
      for (const [, alarm] of this._alarms) {
        const time = alarm.scheduledAt || alarm.timestamp + alarm.delayMs;
        if (time < nearestTime && time > Date.now()) nearestTime = time;
      }
      if (nearestTime < Infinity) {
        const delay = nearestTime - Date.now();
        if (delay > 0) await this.ctx.storage.setAlarm(Date.now() + delay);
      }
    } catch (e) {}
  }

  async _clearAllAlarms() {
    try {
      this._alarms.clear();
      await this.dataManager.deleteAlarms();
      await this.ctx.storage.deleteAlarm();
    } catch (e) {}
  }

  async getPendingAlarms() {
    try {
      await this.restoreAlarms();
      const pending = [];
      const now = Date.now();
      const expired = [];
      for (const [name, alarm] of this._alarms) {
        const scheduledTime = alarm.scheduledAt || alarm.timestamp + alarm.delayMs;
        if (scheduledTime <= now) {
          pending.push({ ...alarm, name });
          expired.push(name);
        }
      }
      for (const name of expired) this._alarms.delete(name);
      if (expired.length > 0) await this.dataManager.setAlarms(Object.fromEntries(this._alarms));
      return pending;
    } catch (e) { return []; }
  }

  async processAlarm(name) {
    try {
      const alarm = this._alarms.get(name);
      if (!alarm) {
        await this.restoreAlarms();
        const restored = this._alarms.get(name);
        if (!restored) return null;
      }
      this._alarms.delete(name);
      await this.dataManager.setAlarms(Object.fromEntries(this._alarms));
      await this._scheduleNearestAlarm();
      return alarm || this._alarms.get(name);
    } catch (e) { return null; }
  }

  async restoreAlarms() {
    try {
      const stored = await this.dataManager.getAlarms();
      if (stored && typeof stored === 'object') {
        this._alarms.clear();
        for (const [name, data] of Object.entries(stored)) {
          this._alarms.set(name, data);
        }
        await this._scheduleNearestAlarm();
        return true;
      }
      return false;
    } catch (e) { return false; }
  }

  _toWITA(date) {
    const wita = new Date(date);
    wita.setHours(wita.getHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET);
    return wita;
  }

  isDiceTime(date) {
    const wita = this._toWITA(date || new Date());
    const currentTotal = wita.getHours() * 60 + wita.getMinutes();
    for (const session of QUIZ_SCHEDULE.SESSIONS) {
      const startTotal = parseTime(session.start);
      const endTotal = parseTime(session.end);
      if (currentTotal >= startTotal && currentTotal < endTotal) return true;
    }
    return false;
  }

  getNextSessionInfo() {
    const wita = this._toWITA(new Date());
    const currentTotal = wita.getHours() * 60 + wita.getMinutes();
    let minDiff = Infinity;
    let nextSession = null;
    for (const session of QUIZ_SCHEDULE.SESSIONS) {
      const startTotal = parseTime(session.start);
      let diff = startTotal - currentTotal;
      if (diff < 0) diff += 24 * 60;
      if (diff < minDiff) {
        minDiff = diff;
        nextSession = { ...session, diff: minDiff };
      }
    }
    return nextSession;
  }

  _getTimeLeftUntilNextDice() {
    try {
      const witaTime = this._toWITA(new Date());
      const currentTotal = witaTime.getHours() * 60 + witaTime.getMinutes();
      let minDiff = Infinity;
      let nextSession = null;

      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) { minDiff = diff; nextSession = session; }
      }

      if (minDiff === Infinity) {
        return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false, nextSession: null };
      }

      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      const isRunning = this.isDiceTime();

      return {
        hours, minutes,
        totalMs: minDiff * 60 * 1000,
        text: hours + "h " + minutes + "m",
        isRunning, nextSession
      };
    } catch (e) {
      return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false, nextSession: null };
    }
  }

  async cleanup() {
    await this._clearAllAlarms();
  }
}

// ============================================================
// GAME SERVER (NON-HIBERNASI) — FULL CLASS
// ============================================================

export class GameServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      this.ctx = state;
      this.closing = false;
      this.isDestroyed = false;
      this._wsIdCounter = 0;
      this._restored = false;

      this.db = env.DB;
      this.dataManager = new DataManager(this.db);
      this.alarmScheduler = new AlarmScheduler(this.db, this.ctx);
      this.alarmScheduler.ctx = this;

      // === PENDING EVENTS QUEUE ===
      this._pendingEvents = [];
      this._isRestoring = false;
      this._restorePromise = null;

      this.activeGames = new Map();
      this.wsMap = new Map();
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.userConnections = new Map();
      this._eventQueue = [];
      this._allTimers = new Set();
      this._lastNotifTime = {};
      this._lastWinnerRequestTime = new Map();
      this._notificationTimers = new Set();

      // ✅ NON-HIBERNASI: in-memory meta (pengganti serializeAttachment)
      this._wsMeta = new Map();

      this.currentDiceRoll = null;
      this._diceLock = false;
      this._tieActive = false;
      this.diceAnswered = new Set();
      this._playerAnswers = new Map();
      this._isShowingDice = false;
      this._diceTimeUpCooldown = false;
      this._diceQuestionStartTime = null;
      this._diceStartTime = null;
      this._diceTimeout = null;
      this._diceStartTimeout = null;
      this._diceTimeUpCooldownTimer = null;
      this._diceCooldownTimer = null;
      this._diceNotificationTimeouts = [];
      this.diceAutoEnabled = false;
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.diceEndNotified = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._lastSentRemaining = -1;
      this._diceOutOfTimeShown = false;
      this._diceTaskRunning = false;
      this._canSubmitDiceAnswer = false;
      this._diceRound = 0;

      this._diceSessionActive = false;
      this._diceStartedByUser = false;
      this._diceSessionEnded = false;
      this._diceGameStarted = false;

      this._broadcastedCountdown = new Map();

      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._tieLock = false;
      this._tieNotificationTimeouts = [];

      // LOCKS
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();
      this._evaluationLocks = new Map();
      this._gameOperationLocks = new Map();
      this._drawLocks = new Map();
      this._submitLocks = new Map();
      this._checkLocks = new Map();
      this._closeLocks = new Map();

      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      this._reconnectAttempts = new Map();

      this._processingQueue = false;
      this._diceLoopCounter = 0;
      this._maxDiceLoops = 10;

      this.DICE_ROOM = CONSTANTS.DICE_ROOM;

      this._restoreAllState()
        .then(() => {
          this._restored = true;
          this._processPendingEvents();
        })
        .catch(() => {
          this._restored = true;
          this._processPendingEvents();
        });

    } catch (e) {
      this._restored = true;
    }
  }

  // ============================================================
  // WS META HELPERS (pengganti serializeAttachment)
  // ============================================================

  _setWsMeta(ws, meta) {
    try {
      const wsId = ws?._wsId;
      if (!wsId) return;
      const existing = this._wsMeta.get(wsId) || {};
      this._wsMeta.set(wsId, { ...existing, ...meta, updatedAt: Date.now() });
    } catch (e) {}
  }

  _getWsMeta(ws) {
    try {
      const wsId = ws?._wsId;
      if (!wsId) return null;
      return this._wsMeta.get(wsId) || null;
    } catch (e) { return null; }
  }

  _deleteWsMeta(ws) {
    try {
      const wsId = ws?._wsId;
      if (wsId) this._wsMeta.delete(wsId);
    } catch (e) {}
  }

  // ============================================================
  // RESTORE ALL STATE — LOAD DULU DARI D1
  // ============================================================

  async _restoreAllState() {
    if (this._isRestoring) return this._restorePromise;

    this._isRestoring = true;
    this._restorePromise = (async () => {
      try {
        await this.dataManager.init();
        await this.dataManager.loadAllData();

        await this.alarmScheduler.restoreAlarms();
        await this.alarmScheduler.scheduleAlarms();

        await this._checkAndForceResetIfMondayUTC();

        this._restoreWebSockets();
        this._syncAllRoomsComplete();

        await this._restoreActiveGames();

        this._initialized = true;

        const isDiceTime = this.alarmScheduler.isDiceTime();
        if (isDiceTime) {
          this._diceSessionActive = true;
          this._diceSessionEnded = false;
          this._diceGameStarted = false;
        } else {
          this._diceSessionActive = false;
          this._diceSessionEnded = true;
          this._diceGameStarted = false;
        }

        if (!this.closing && !this.isDestroyed) {
          try { await this.ctx.storage.setAlarm(Date.now() + 60000); } catch (e) {}
        }

        this._restored = true;
        await this._processPendingEvents();

      } catch (e) {
        this._restored = true;
        throw e;
      } finally {
        this._isRestoring = false;
        this._restorePromise = null;
      }
    })();

    return this._restorePromise;
  }

  // ============================================================
  // RESTORE WEB SOCKETS (NON-HIBERNASI = NO-OP)
  // ============================================================

  _restoreWebSockets() {
    this.wsMap.clear();
    this.wsClients.clear();
    this.clientRooms.clear();
    this.userConnections.clear();
    this._wsMeta.clear();
    this._wsIdCounter = 0;
    return true;
  }

  // ============================================================
  // SYNC ALL ROOMS COMPLETE
  // ============================================================

  _syncAllRoomsComplete() {
    try {
      const roomMap = new Map();

      for (const [room, wsIds] of this.wsClients) {
        if (!roomMap.has(room)) roomMap.set(room, new Set());
        for (const wsId of wsIds) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1 && !ws._closing) roomMap.get(room).add(wsId);
        }
      }

      for (const [wsId, room] of this.clientRooms) {
        if (!roomMap.has(room)) roomMap.set(room, new Set());
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1 && !ws._closing) roomMap.get(room).add(wsId);
      }

      for (const [, conn] of this.userConnections) {
        if (conn && conn.room && conn.wsId) {
          if (!roomMap.has(conn.room)) roomMap.set(conn.room, new Set());
          const ws = this.wsMap.get(conn.wsId);
          if (ws && ws.readyState === 1 && !ws._closing) roomMap.get(conn.room).add(conn.wsId);
        }
      }

      this.wsClients.clear();
      this.clientRooms.clear();

      for (const [room, wsIds] of roomMap) {
        if (wsIds.size > 0) {
          this.wsClients.set(room, wsIds);
          for (const wsId of wsIds) {
            this.clientRooms.set(wsId, room);
            const ws = this.wsMap.get(wsId);
            if (ws) { ws.room = room; ws.roomname = room; }
          }
        }
      }
      return true;
    } catch (e) { return false; }
  }

  // ============================================================
  // RESTORE ACTIVE GAMES — DARI D1
  // ============================================================

  async _restoreActiveGames() {
    try {
      const games = await this.dataManager.getActiveGames();
      if (!games || typeof games !== 'object') return;

      for (const [room, gameData] of Object.entries(games)) {
        if (gameData._isActive && !gameData._gameEnded) {
          const game = this._createGameObject(
            room,
            gameData.betAmount || 0,
            gameData.hostId || 'unknown',
            gameData._startedByRecording || false
          );

          if (gameData.players) {
            for (const [id, player] of Object.entries(gameData.players)) {
              game.players.set(id, player);
            }
          }

          game.round = gameData.round || 1;
          game._phase = gameData._phase || 'registration';
          game._state = gameData._state || 'registration';
          game._isActive = true;
          game._gameEnded = false;
          game._createdAt = gameData._createdAt || Date.now();
          game._drawPhaseStart = gameData._drawPhaseStart || null;

          this.activeGames.set(room, game);

          if (game._phase === 'registration') {
            this._startRegistration(room, game);
          } else if (game._phase === 'draw' || game._phase === 'evaluating') {
            this._startDrawPhase(room, game);
          }
        }
      }
    } catch (e) {}
  }

  // ============================================================
  // SAVE GAME STATE — KE D1
  // ============================================================

  async _saveGameState() {
    try {
      const games = {};
      for (const [room, game] of this.activeGames) {
        if (game._isActive && !game._gameEnded) {
          games[room] = {
            betAmount: game.betAmount,
            hostId: game.hostId,
            hostName: game.hostName,
            round: game.round,
            _phase: game._phase,
            _state: game._state,
            _isActive: game._isActive,
            _gameEnded: game._gameEnded,
            _startedByRecording: game._startedByRecording,
            players: Object.fromEntries(game.players),
            _createdAt: game._createdAt,
            _drawPhaseStart: game._drawPhaseStart
          };
        }
      }
      await this.dataManager.setActiveGames(games);
    } catch (e) {}
  }

  // ============================================================
  // FETCH — NON-HIBERNASI WEBSOCKET
  // ============================================================

  async fetch(req) {
    try {
      if (this._circuitOpen) {
        const now = Date.now();
        if (now - this._lastResetTime > 60000) {
          this._circuitOpen = false;
          this._requestCount = 0;
          this._lastResetTime = now;
        } else {
          return new Response("Service temporarily unavailable", {
            status: 503,
            headers: { 'Retry-After': '30', 'Content-Type': 'text/plain' }
          });
        }
      }
      this._requestCount++;
      if (this._requestCount > CONSTANTS.RATE_LIMIT_MAX) {
        this._circuitOpen = true;
        this._lastResetTime = Date.now();
        return new Response("Rate limit exceeded", {
          status: 429,
          headers: { 'Retry-After': '60', 'Content-Type': 'text/plain' }
        });
      }
      setTimeout(() => { this._requestCount = Math.max(0, this._requestCount - 50); }, CONSTANTS.RATE_LIMIT_WINDOW_MS);

      const url = new URL(req.url);

      if (url.pathname === "/game/ws") {
        const upgrade = req.headers.get("Upgrade");
        if (upgrade !== "websocket") return new Response("WebSocket only", { status: 400 });
        if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) return new Response("Server full", { status: 503 });
        if (this._eventQueue?.length > 500) return new Response("Server busy", { status: 503 });

        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];

        const wsId = ++this._wsIdCounter;

        // ✅ NON-HIBERNASI: tidak pakai ctx.acceptWebSocket / serializeAttachment
        server._wsId = wsId;
        server._closing = false;
        server.username = null;
        server.room = null;
        server.roomname = null;
        server._createdAt = Date.now();

        this.wsMap.set(wsId, server);
        this._setWsMeta(server, { username: null, room: null, createdAt: Date.now() });

        // ✅ Event listener manual (WAJIB non-hibernasi)
        server.addEventListener('message', (event) => {
          try { this.webSocketMessage(server, event.data); } catch (e) {}
        });
        server.addEventListener('close', (event) => {
          try { this.webSocketClose(server, event.code, event.reason, event.wasClean); } catch (e) {}
        });
        server.addEventListener('error', () => {
          try { this.webSocketClose(server, 1006, 'error', false); } catch (e) {}
        });

        return new Response(null, { status: 101, webSocket: client });
      }

      return new Response("Game Server", { status: 200 });
    } catch (e) {
      this._handleError('fetch', e);
      return new Response(JSON.stringify({ error: "Internal Server Error", message: e.message }), {
        status: 500, headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  // ============================================================
  // WEBSOCKET MESSAGE
  // ============================================================

  async webSocketMessage(ws, message) {
    if (!ws || ws._closing || this.closing || this.isDestroyed) return;

    const meta = this._getWsMeta(ws);
    if (meta) {
      ws.username = meta.username || ws.username;
      ws.room = meta.room || ws.room;
      ws.roomname = meta.room || ws.roomname;
    }

    if (!this._restored || this._isRestoring) {
      if (!this._pendingEvents) this._pendingEvents = [];
      if (this._pendingEvents.length >= 100) {
        try { ws.send(JSON.stringify(["gameLowCardError", "Server busy"])); } catch (e) {}
        return;
      }
      this._pendingEvents.push({
        ws, message,
        timestamp: Date.now(),
        wsId: ws._wsId,
        room: ws.room,
        username: ws.username
      });
      return;
    }

    try {
      const data = JSON.parse(message);
      if (Array.isArray(data) && data.length > 0) {
        await this._processWithTimeout(ws, data);
      }
    } catch (e) {}
  }

  // ============================================================
  // WEBSOCKET CLOSE
  // ============================================================

  async webSocketClose(ws, code, reason, wasClean) {
    if (!ws) return;
    try {
      const meta = this._getWsMeta(ws);
      const username = meta?.username || ws.username;
      const room = meta?.room || ws.room;
      const wsId = ws._wsId;

      if (wsId) this._broadcastedCountdown.delete(`countdown_${wsId}`);

      if (username) this.userConnections.delete(username);
      if (room && wsId) {
        const clients = this.wsClients.get(room);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) this.wsClients.delete(room);
        }
      }
      if (wsId) {
        this.wsMap.delete(wsId);
        this.clientRooms.delete(wsId);
        this._wsMeta.delete(wsId);
      }
      for (const [r, clients] of this.wsClients) {
        if (clients.has(wsId)) {
          clients.delete(wsId);
          if (clients.size === 0) this.wsClients.delete(r);
        }
      }

      if (room && username) {
        const game = this.activeGames.get(room);
        if (game && game._isActive && !game._gameEnded && game.players) {
          if (game.players.has(username)) {
            if (game.hostId === username && game._phase === 'registration') {
              this._broadcastToRoom(room, ["gameLowCardError", "Host left the game"]);
              game._gameEnded = true;
              game._isActive = false;
              this._forceCleanupGame(room, game);
              this._cleanupWsData(wsId, username, room);
              return;
            }

            if (game._phase === 'registration') {
              game.players.delete(username);
              game.playerWsId?.delete(username);
              this._broadcastToRoom(room, ["gameLowCardError", "Player left the game"]);

              if (game.hostId === username) {
                const remaining = Array.from(game.players.keys());
                if (remaining.length > 0) {
                  game.hostId = remaining[0];
                  this._broadcastToRoom(room, ["gameLowCardError", "New host: " + game.hostId]);
                } else {
                  game._gameEnded = true;
                  game._isActive = false;
                  this._forceCleanupGame(room, game);
                  this._cleanupWsData(wsId, username, room);
                  return;
                }
              }

              if (game.players.size === 0) {
                game._gameEnded = true;
                game._isActive = false;
                this._forceCleanupGame(room, game);
              }
              this._cleanupWsData(wsId, username, room);
              return;
            }

            if (game._phase === 'draw' || game._phase === 'evaluating') {
              if (!game.eliminated.has(username)) {
                game.eliminated.add(username);
                this._broadcastToRoom(room, ["gameLowCardError", "Player left the game"]);

                const active = this._getActivePlayers(game);
                if (active.length <= 1) {
                  if (active.length === 1) {
                    const winner = active[0].name;
                    const totalCoin = game.betAmount * game.players.size;
                    this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
                  } else {
                    this._broadcastToRoom(room, ["gameLowCardError", "All players disconnected"]);
                  }
                  game._gameEnded = true;
                  game._isActive = false;
                  this._forceCleanupGame(room, game);
                  this._cleanupWsData(wsId, username, room);
                  return;
                }

                if (game._phase === 'draw' && !game.evaluationLocked) {
                  const activeIds = this._getActivePlayerIds(game);
                  const hasDrawn = activeIds.every(id => game.numbers.has(id) || game.eliminated.has(id));
                  if (hasDrawn && !game._isEvaluating) this._checkAllDrawn(room, game);
                }
              }
            }
          }
        }
      }
    } catch (e) {}
  }

  _cleanupWsData(wsId, username, room) {
    try {
      if (wsId) {
        this.wsMap.delete(wsId);
        this.clientRooms.delete(wsId);
        this._wsMeta.delete(wsId);
        if (username) this.userConnections.delete(username);
        if (room) {
          const clients = this.wsClients.get(room);
          if (clients) {
            clients.delete(wsId);
            if (clients.size === 0) this.wsClients.delete(room);
          }
        }
      }
    } catch (e) {}
  }

  // ============================================================
  // PROCESS PENDING EVENTS
  // ============================================================

  async _processPendingEvents() {
    if (!this._pendingEvents || this._pendingEvents.length === 0) return;
    const events = [...this._pendingEvents];
    this._pendingEvents = [];

    for (const evt of events) {
      let ws = evt.ws;
      if (!ws || ws.readyState !== 1) {
        const wsId = evt.wsId;
        if (wsId && this.wsMap.has(wsId)) ws = this.wsMap.get(wsId);
        else continue;
      }
      if (!ws || ws.readyState !== 1 || ws._closing) continue;

      try {
        const room = evt.room || ws.room || ws.roomname;
        if (room) this._ensureClientInRoom(ws, room, evt.username || ws.username);

        const data = JSON.parse(evt.message);
        if (Array.isArray(data) && data.length > 0) await this._processWithTimeout(ws, data);
      } catch (e) {}
    }
  }

  // ============================================================
  // ENSURE CLIENT IN ROOM
  // ============================================================

  _ensureClientInRoom(ws, room, username = null) {
    try {
      if (!ws || !room) return false;
      const wsId = ws._wsId;
      if (!wsId) return false;

      if (!this.wsClients.has(room)) this.wsClients.set(room, new Set());
      const clients = this.wsClients.get(room);
      if (!clients.has(wsId)) {
        clients.add(wsId);
        this.clientRooms.set(wsId, room);
        this.wsMap.set(wsId, ws);
      }

      ws.room = room;
      ws.roomname = room;
      if (username) ws.username = username;

      this._setWsMeta(ws, { username: ws.username || null, room });

      if (ws.username) {
        let conn = this.userConnections.get(ws.username);
        if (conn) {
          conn.wsId = wsId; conn.ws = ws; conn.room = room; conn.timestamp = Date.now();
        } else {
          this.userConnections.set(ws.username, { wsId, ws, room, timestamp: Date.now() });
        }
      }
      return true;
    } catch (e) { return false; }
  }

  // ============================================================
  // GET CLIENT ROOM / USERNAME
  // ============================================================

  _getClientRoom(ws) {
    try {
      if (!ws) return null;
      let room = ws.room || ws.roomname;
      if (room) return room;
      const wsId = ws._wsId;
      if (wsId) {
        room = this.clientRooms.get(wsId);
        if (room) return room;
        const meta = this._wsMeta.get(wsId);
        if (meta?.room) return meta.room;
      }
      if (ws.username) {
        const conn = this.userConnections.get(ws.username);
        if (conn && conn.room) return conn.room;
      }
      return null;
    } catch (e) { return null; }
  }

  _getClientUsername(ws) {
    try {
      if (!ws) return null;
      if (ws.username) return ws.username;
      const wsId = ws._wsId;
      if (wsId) {
        const meta = this._wsMeta.get(wsId);
        if (meta?.username) return meta.username;
        for (const [username, conn] of this.userConnections) {
          if (conn.wsId === wsId) return username;
        }
      }
      return null;
    } catch (e) { return null; }
  }

  // ============================================================
  // SYNC / RECOVER ROOM CLIENTS
  // ============================================================

  _recoverRoomClients(room) {
    try {
      if (!room) return false;
      const recovered = new Set();
      for (const [wsId, r] of this.clientRooms) {
        if (r === room) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1 && !ws._closing) recovered.add(wsId);
        }
      }
      for (const [, conn] of this.userConnections) {
        if (conn.room === room && conn.wsId) {
          const ws = this.wsMap.get(conn.wsId);
          if (ws && ws.readyState === 1 && !ws._closing) recovered.add(conn.wsId);
        }
      }
      if (recovered.size > 0) { this.wsClients.set(room, recovered); return true; }
      return false;
    } catch (e) { return false; }
  }

  _syncRoomClients(room) {
    try {
      if (!room) return;
      let clients = this.wsClients.get(room);
      if (!clients || clients.size === 0) {
        this._recoverRoomClients(room);
        clients = this.wsClients.get(room);
        if (!clients || clients.size === 0) return;
      }

      const toRemove = [];
      for (const wsId of clients) {
        const ws = this.wsMap.get(wsId);
        if (!ws || ws.readyState !== 1 || ws._closing) toRemove.push(wsId);
        else {
          ws.room = room;
          ws.roomname = room;
          this.clientRooms.set(wsId, room);
          this._setWsMeta(ws, { room, username: ws.username || null });
        }
      }

      for (const wsId of toRemove) {
        clients.delete(wsId);
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
        this._wsMeta.delete(wsId);
        for (const [username, conn] of this.userConnections) {
          if (conn.wsId === wsId) { this.userConnections.delete(username); break; }
        }
      }

      for (const [wsId, r] of this.clientRooms) {
        if (r === room && !clients.has(wsId)) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1 && !ws._closing) clients.add(wsId);
        }
      }

      if (clients.size === 0) this.wsClients.delete(room);
      else this.wsClients.set(room, clients);
    } catch (e) {}
  }

  _syncAllRooms() {
    try {
      const roomMap = new Map();
      const clientRoomMap = new Map();
      for (const [wsId, ws] of this.wsMap) {
        if (ws && ws.room && ws.readyState === 1 && !ws._closing) {
          const room = ws.room;
          if (!roomMap.has(room)) roomMap.set(room, new Set());
          roomMap.get(room).add(wsId);
          clientRoomMap.set(wsId, room);
        }
      }
      for (const [, conn] of this.userConnections) {
        if (conn && conn.room && conn.wsId) {
          const ws = this.wsMap.get(conn.wsId);
          if (ws && ws.readyState === 1 && !ws._closing) {
            if (!roomMap.has(conn.room)) roomMap.set(conn.room, new Set());
            roomMap.get(conn.room).add(conn.wsId);
            clientRoomMap.set(conn.wsId, conn.room);
          }
        }
      }
      this.wsClients.clear();
      for (const [room, wsIds] of roomMap) this.wsClients.set(room, wsIds);
      this.clientRooms.clear();
      for (const [wsId, room] of clientRoomMap) this.clientRooms.set(wsId, room);
      return true;
    } catch (e) { return false; }
  }

  // ============================================================
  // GET ROOM USERS
  // ============================================================

  _getRoomUsers(room) {
    try {
      if (!room) return [];
      const users = [];
      let wsIds = this.wsClients.get(room);
      if (!wsIds || wsIds.size === 0) {
        wsIds = new Set();
        for (const [wsId, clientRoom] of this.clientRooms) {
          if (clientRoom === room) wsIds.add(wsId);
        }
        if (wsIds.size > 0) this.wsClients.set(room, wsIds);
      }
      if (wsIds && wsIds.size > 0) {
        const toRemove = [];
        for (const wsId of wsIds) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1 && !ws._closing) {
            if (!ws.room || ws.room !== room) {
              ws.room = room;
              ws.roomname = room;
              this._setWsMeta(ws, { room, username: ws.username || null });
            }
            users.push({ wsId, ws, username: ws.username || 'Anonymous' });
          } else toRemove.push(wsId);
        }
        for (const wsId of toRemove) {
          wsIds.delete(wsId);
          this.clientRooms.delete(wsId);
          this.wsMap.delete(wsId);
          this._wsMeta.delete(wsId);
          for (const [username, conn] of this.userConnections) {
            if (conn.wsId === wsId) { this.userConnections.delete(username); break; }
          }
        }
        if (wsIds.size === 0) this.wsClients.delete(room);
        else this.wsClients.set(room, wsIds);
      }
      return users;
    } catch (e) { return []; }
  }

  // ============================================================
  // BROADCAST TO ROOM
  // ============================================================

  _broadcastToRoom(room, message) {
    try {
      if (this.closing || this.isDestroyed || !room || !message) return 0;
      this._syncRoomClients(room);
      let clients = this.wsClients.get(room);
      if (!clients || clients.size === 0) {
        this._recoverRoomClients(room);
        clients = this.wsClients.get(room);
        if (!clients || clients.size === 0) return 0;
      }

      const msgStr = JSON.stringify(message);
      let sentCount = 0;
      const toRemove = [];
      const sentWsIds = new Set();

      for (const wsId of clients) {
        if (sentWsIds.has(wsId)) continue;
        sentWsIds.add(wsId);
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1 && !ws._closing) {
          try { ws.send(msgStr); sentCount++; }
          catch (e) { toRemove.push(wsId); }
        } else toRemove.push(wsId);
      }

      if (toRemove.length > 0) {
        for (const wsId of toRemove) {
          clients.delete(wsId);
          this.clientRooms.delete(wsId);
          this.wsMap.delete(wsId);
          this._wsMeta.delete(wsId);
          for (const [username, conn] of this.userConnections) {
            if (conn.wsId === wsId) { this.userConnections.delete(username); break; }
          }
        }
        if (clients.size === 0) this.wsClients.delete(room);
      }
      return sentCount;
    } catch (e) { return 0; }
  }

  // ============================================================
  // SAFE SEND / SEND TO USER
  // ============================================================

  _sendToUser(ws, message) {
    try {
      if (!ws || ws.readyState !== 1 || ws._closing) return false;
      const msg = typeof message === 'string' ? message : JSON.stringify(message);
      ws.send(msg);
      return true;
    } catch (e) { return false; }
  }

  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      const msg = typeof message === 'string' ? message : JSON.stringify(message);
      ws.send(msg);
      return true;
    } catch (e) { return false; }
  }

  // ============================================================
  // SEND CURRENT GAME STATE
  // ============================================================

  _sendCurrentGameState(ws, room) {
    try {
      if (!ws || !room) return;
      this._ensureClientInRoom(ws, room, ws.username);
      const game = this.activeGames.get(room);
      if (!game || !game._isActive || game._gameEnded) return;

      if (game._phase === 'registration' || game._state === 'registration') {
        this._sendToUser(ws, ["gameLowCardStart", game.betAmount]);
        this._sendToUser(ws, ["gameLowCardStartSuccess", game.hostName || game.hostId, game.betAmount]);
        const players = Array.from(game.players.keys()).filter(id => !id.startsWith('BOT_'));
        for (const player of players) this._sendToUser(ws, ["gameLowCardJoin", player, game.betAmount]);
        const elapsed = Date.now() - game._createdAt;
        const remaining = Math.max(0, CONSTANTS.REGISTRATION_TIME_MS - elapsed);
        if (remaining > 0) this._sendToUser(ws, ["gameLowCardTimeLeft", Math.ceil(remaining / 1000) + "s"]);
        return;
      }

      if (game._phase === 'draw' || game._state === 'draw' || game._phase === 'evaluating' || game._state === 'evaluating') {
        const activePlayers = this._getActivePlayers(game);
        const playersList = activePlayers.map(p => p.name);
        this._sendToUser(ws, ["gameLowCardClosed", playersList]);
        this._sendToUser(ws, ["gameLowCardNextRound", game.round]);
        for (const [id, number] of game.numbers) {
          const name = game.players.get(id)?.name || id;
          const tanda = game.tanda.get(id) || "";
          this._sendToUser(ws, ["gameLowCardPlayerDraw", name, number, tanda]);
        }
        if (game._drawPhaseStart) {
          const elapsed = Date.now() - game._drawPhaseStart;
          const remaining = Math.max(0, CONSTANTS.DRAW_TIME_MS - elapsed);
          if (remaining > 0) this._sendToUser(ws, ["gameLowCardTimeLeft", Math.ceil(remaining / 1000) + "s"]);
        }
        if (game._isEvaluating || game.evaluationLocked) this._sendToUser(ws, ["gameLowCardWait", "wait results"]);
      }
    } catch (e) {}
  }

  // ============================================================
  // BROADCAST STATE TO ALL ROOMS
  // ============================================================

  async _broadcastStateToAllRooms() {
    try {
      for (const [room, game] of this.activeGames) {
        if (game?._isActive && !game._gameEnded) {
          const clients = this.wsClients.get(room);
          if (clients && clients.size > 0) this._broadcastGameStateToAll(room, game);
        }
      }
      const diceClients = this.wsClients.get(CONSTANTS.DICE_ROOM);
      if (diceClients && diceClients.size > 0) this._broadcastDiceStateToAll();
    } catch (e) {}
  }

  _broadcastGameStateToAll(room, game) {
    try {
      if (!room || !game || !game._isActive || game._gameEnded) return;
      const clients = this.wsClients.get(room);
      if (!clients || clients.size === 0) return;

      const msgStr = JSON.stringify(["gameStatus", "true"]);
      const startMsg = JSON.stringify(["gameLowCardStart", game.betAmount]);
      const startSuccessMsg = JSON.stringify(["gameLowCardStartSuccess", game.hostName || game.hostId, game.betAmount]);

      for (const wsId of clients) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1 && !ws._closing) {
          try {
            ws.send(msgStr);
            ws.send(startMsg);
            ws.send(startSuccessMsg);
            const players = Array.from(game.players.keys()).filter(id => !id.startsWith('BOT_'));
            for (const player of players) ws.send(JSON.stringify(["gameLowCardJoin", player, game.betAmount]));
            if (game._phase === 'registration') {
              const elapsed = Date.now() - game._createdAt;
              const remaining = Math.max(0, CONSTANTS.REGISTRATION_TIME_MS - elapsed);
              if (remaining > 0) ws.send(JSON.stringify(["gameLowCardTimeLeft", Math.ceil(remaining / 1000) + "s"]));
            }
            if (game._phase === 'draw' || game._phase === 'evaluating') {
              const activePlayers = this._getActivePlayers(game);
              ws.send(JSON.stringify(["gameLowCardClosed", activePlayers.map(p => p.name)]));
              ws.send(JSON.stringify(["gameLowCardNextRound", game.round]));
              for (const [id, number] of game.numbers) {
                const name = game.players.get(id)?.name || id;
                const tanda = game.tanda.get(id) || "";
                ws.send(JSON.stringify(["gameLowCardPlayerDraw", name, number, tanda]));
              }
            }
          } catch (e) {}
        }
      }
    } catch (e) {}
  }

  _broadcastDiceStateToAll() {
    try {
      const clients = this.wsClients.get(CONSTANTS.DICE_ROOM);
      if (!clients || clients.size === 0) return;
      const isDiceTime = this.alarmScheduler.isDiceTime();
      const isGameRunning = this.currentDiceRoll && this._canSubmitDiceAnswer;
      const msgStr = JSON.stringify(["diceSessionStatus", isDiceTime ? "active" : "inactive"]);
      for (const wsId of clients) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1 && !ws._closing) {
          try {
            ws.send(msgStr);
            if (isDiceTime && isGameRunning) {
              ws.send(JSON.stringify(["diceRoll", {
                value: this.currentDiceRoll.value,
                timestamp: this.currentDiceRoll.timestamp,
                answerTime: 20, canAnswerNow: true, round: this._diceRound
              }]));
            } else if (!isDiceTime) {
              const timeInfo = this.alarmScheduler._getTimeLeftUntilNextDice();
              if (timeInfo && timeInfo.nextSession) {
                ws.send(JSON.stringify(["diceNextSession", {
                  startTime: timeInfo.nextSession.start,
                  endTime: timeInfo.nextSession.end,
                  hoursLeft: timeInfo.hours,
                  minutesLeft: timeInfo.minutes,
                  text: timeInfo.text
                }]));
              }
            }
          } catch (e) {}
        }
      }
    } catch (e) {}
  }

  // ============================================================
  // DICE ROOM STATE
  // ============================================================

  _sendDiceRoomState(ws) {
    try {
      if (!ws || ws.readyState !== 1) return;
      const wsId = ws._wsId;
      if (!wsId) return;
      this._ensureClientInRoom(ws, CONSTANTS.DICE_ROOM, ws.username);
      const isDiceTime = this.alarmScheduler.isDiceTime();

      if (isDiceTime) {
        this._safeSend(ws, ["diceSessionStatus", "active"]);
        const isGameRunning = this.currentDiceRoll && this._canSubmitDiceAnswer;
        if (!this._diceGameStarted && !this.currentDiceRoll && !this._diceLock) this._startDiceGameIfNotStarted();
        if (isGameRunning) {
          this._safeSend(ws, ["diceRoll", {
            value: this.currentDiceRoll.value,
            timestamp: this.currentDiceRoll.timestamp,
            answerTime: 20, canAnswerNow: true, round: this._diceRound
          }]);
        }
      } else {
        const timeInfo = this.alarmScheduler._getTimeLeftUntilNextDice();
        this._safeSend(ws, ["diceSessionStatus", "inactive"]);
        if (timeInfo && timeInfo.nextSession) {
          this._safeSend(ws, ["diceNextSession", {
            startTime: timeInfo.nextSession.start,
            endTime: timeInfo.nextSession.end,
            hoursLeft: timeInfo.hours, minutesLeft: timeInfo.minutes, text: timeInfo.text
          }]);
          setTimeout(() => {
            if (ws && ws.readyState === 1 && !ws._closing) {
              this._safeSend(ws, ["diceNotification", "Next dice game in: " + timeInfo.text]);
            }
          }, CONSTANTS.DICE_BROADCAST_DELAY_MS || 5000);
        }
      }
    } catch (e) {}
  }

  // ============================================================
  // START DICE GAME
  // ============================================================

  _startDiceGameIfNotStarted() {
    try {
      if (this._diceGameStarted) return;
      if (!this.alarmScheduler.isDiceTime()) return;
      if (this.currentDiceRoll || this._isShowingDice || this._diceLock) return;
      if (this._diceTimeUpCooldown) return;

      const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
      if (!clients || clients.size === 0) return;

      this._diceGameStarted = true;
      this._diceSessionActive = true;
      this._diceSessionEnded = false;
      this._diceStartedByUser = true;

      this._startDiceFast();
    } catch (e) {
      this._diceGameStarted = false;
    }
  }

  // ============================================================
  // WEEKLY RESET
  // ============================================================

  async _checkAndForceResetIfMondayUTC() {
    try {
      const now = new Date();
      const currentDay = now.getUTCDay();
      const currentWeek = this.dataManager.getCurrentWeek();
      let lastResetWeek = await this.dataManager.getLastResetWeek();
      if (!lastResetWeek) {
        await this.dataManager.setLastResetWeek(currentWeek);
        return true;
      }
      if (lastResetWeek === currentWeek) return true;
      if (currentDay === CONSTANTS.WEEKLY_RESET_DAY) {
        await this._handleWeeklyReset();
        await this.dataManager.setLastResetWeek(currentWeek);
      }
      return true;
    } catch (e) {
      const currentWeek = this.dataManager.getCurrentWeek();
      await this.dataManager.setLastResetWeek(currentWeek);
      return false;
    }
  }

  async _forceResetIfNeededUTC() {
    try {
      const now = new Date();
      const currentDay = now.getUTCDay();
      const currentWeek = this.dataManager.getCurrentWeek();
      const lastResetWeek = await this.dataManager.getLastResetWeek();
      if (currentDay === CONSTANTS.WEEKLY_RESET_DAY && lastResetWeek !== currentWeek) {
        await this._handleWeeklyReset();
        await this.dataManager.setLastResetWeek(currentWeek);
        return true;
      }
      return false;
    } catch (e) { return false; }
  }

  async _handleWeeklyReset() {
    try {
      const points = await this.dataManager.getDicePoints();
      let winner = null;
      let highestScore = 0;
      for (const [username, score] of Object.entries(points)) {
        if (username && typeof username === 'string') {
          const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
          if (numericScore > highestScore) { highestScore = numericScore; winner = username; }
        }
      }
      const currentWeek = this.dataManager.getCurrentWeek();
      if (winner && highestScore > 0) {
        await this.dataManager.setLastWeekWinner({
          username: winner, score: highestScore, week: currentWeek, timestamp: Date.now()
        });
      } else {
        await this.dataManager.deleteLastWeekWinner();
      }
      await this.dataManager.resetDicePoints();
      return true;
    } catch (e) {
      const currentWeek = this.dataManager.getCurrentWeek();
      await this.dataManager.setLastResetWeek(currentWeek);
      return false;
    }
  }

  // ============================================================
  // ALARM
  // ============================================================

  async alarm() {
    if (this.closing || this.isDestroyed) return;
    try {
      await this._forceResetIfNeededUTC();
      await this.alarmScheduler.restoreAlarms();
      const pendingAlarms = await this.alarmScheduler.getPendingAlarms();
      for (const alarm of pendingAlarms) {
        try {
          await this._processAlarm(alarm.name);
          await this.alarmScheduler.processAlarm(alarm.name);
        } catch (e) {}
      }
      await this.alarmScheduler.scheduleAlarms();
      await this._saveGameState();

      if (!this.closing && !this.isDestroyed) {
        try { await this.ctx.storage.setAlarm(Date.now() + 60000); } catch (e) {}
      }
    } catch (e) {}
  }

  async _processAlarm(name) {
    switch (name) {
      case CONSTANTS.WEEKLY_RESET_ALARM:
        await this._handleWeeklyReset();
        break;

      case 'dice_session_start':
      case 'dice_session_start_immediate':
        if (this.alarmScheduler.isDiceTime()) {
          this.diceAutoEnabled = true;
          this._diceSessionActive = true;
          this._diceSessionEnded = false;
          this._diceGameStarted = false;

          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "Dice session started!"]);

          const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
          if (clients && clients.size > 0) {
            if (!this.currentDiceRoll && !this._isShowingDice && !this._diceLock && !this._diceTimeUpCooldown) {
              this._diceStartedByUser = true;
              this._startDiceGameIfNotStarted();
            }
          } else {
            this._diceStartedByUser = false;
            this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "Waiting for players..."]);
          }
        }
        break;

      case 'dice_session_end':
        this.diceAutoEnabled = false;
        this._diceSessionActive = false;
        this._diceSessionEnded = true;
        this._diceStartedByUser = false;
        this._diceGameStarted = false;

        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "Dice session ended"]);

        if (this.currentDiceRoll || this._isShowingDice) this._endDiceRound();

        this.currentDiceRoll = null;
        this._diceLock = false;
        this._isShowingDice = false;
        this._canSubmitDiceAnswer = false;
        this.diceAnswered = new Set();
        this._playerAnswers = new Map();
        this.diceHasWinner = false;
        this.diceWinner = null;
        this._diceRound = 0;

        if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
        if (this._diceCooldownTimer) { clearTimeout(this._diceCooldownTimer); this._diceCooldownTimer = null; }
        if (this._diceTimeUpCooldownTimer) { clearTimeout(this._diceTimeUpCooldownTimer); this._diceTimeUpCooldownTimer = null; }
        for (const timeout of this._diceNotificationTimeouts) clearTimeout(timeout);
        this._diceNotificationTimeouts = [];
        if (this._tieActive) this._resetTieBreakerState(null);
        break;
    }
  }

  // ============================================================
  // EVENT PROCESSING (queue)
  // ============================================================

  async _processWithTimeout(ws, data, timeoutMs = 500) {
    try {
      const timeoutPromise = new Promise((_, reject) => {
        const timer = setTimeout(() => reject(new Error('Processing timeout')), timeoutMs);
        this._trackTimer(timer);
      });
      await Promise.race([this.handleEvent(ws, data), timeoutPromise]);
    } catch (e) {}
  }

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      if (this._eventQueue.length > CONSTANTS.MAX_EVENT_QUEUE_SIZE) {
        this._safeSend(ws, ["gameLowCardError", "Server busy"]);
        return;
      }
      this._eventQueue.push({ ws, data });
      if (!this._processingQueue) this._processEventQueue();
    } catch (e) {}
  }

  async _processEventQueue() {
    if (this._processingQueue || this._eventQueue.length === 0) return;
    this._processingQueue = true;
    try {
      const startTime = Date.now();
      let processed = 0;
      const MAX_BATCH = CONSTANTS.PROCESS_BATCH_SIZE || 50;
      const MAX_TIME = CONSTANTS.PROCESS_MAX_TIME_MS || 100;
      while (this._eventQueue.length > 0 && processed < MAX_BATCH) {
        if (Date.now() - startTime > MAX_TIME) break;
        const item = this._eventQueue.shift();
        try { await this._processEventItem(item.ws, item.data); } catch (e) {}
        processed++;
      }
      if (this._eventQueue.length > 0 && !this.closing && !this.isDestroyed) {
        setTimeout(() => this._processEventQueue(), 10);
      }
    } catch (e) {
      this._handleError('processQueue', e);
    } finally {
      this._processingQueue = false;
    }
  }

  async _processEventItem(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      const room = this._getClientRoom(ws);
      if (room) this._ensureClientInRoom(ws, room, ws.username);
      await this._handleEventInternal(ws, data);
    } catch (e) {}
  }

  // ============================================================
  // HANDLE EVENT INTERNAL
  // ============================================================

  async _handleEventInternal(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      const evt = data[0];

      // ==================== SWITCH ROOM ====================
      if (evt === "switchRoom") {
        await this.switchRoom(ws, data[1], data[2]);
        return;
      }

      // ==================== DICE SESSION STATUS ====================
      if (evt === "getDiceSessionStatus") {
        try {
          const isDiceTime = this.alarmScheduler.isDiceTime();
          const isGameRunning = this.currentDiceRoll && this._canSubmitDiceAnswer;

          if (isDiceTime) {
            this._safeSend(ws, ["diceSessionStatus", "active"]);
            if (isGameRunning) {
              this._safeSend(ws, ["diceRoll", {
                value: this.currentDiceRoll.value,
                timestamp: this.currentDiceRoll.timestamp,
                answerTime: 20, canAnswerNow: true, round: this._diceRound
              }]);
            }
          } else {
            const timeInfo = this.alarmScheduler._getTimeLeftUntilNextDice();
            this._safeSend(ws, ["diceSessionStatus", "inactive"]);
            if (timeInfo && timeInfo.nextSession) {
              this._safeSend(ws, ["diceNextSession", {
                startTime: timeInfo.nextSession.start,
                endTime: timeInfo.nextSession.end,
                hoursLeft: timeInfo.hours,
                minutesLeft: timeInfo.minutes,
                text: timeInfo.text
              }]);
              setTimeout(() => {
                if (ws && ws.readyState === 1 && !ws._closing) {
                  this._safeSend(ws, ["diceNotification", "Next dice game in: " + timeInfo.text]);
                }
              }, CONSTANTS.DICE_BROADCAST_DELAY_MS || 5000);
            }
          }
        } catch (e) {
          this._safeSend(ws, ["diceSessionStatus", "error"]);
        }
        return;
      }

      // ==================== RECORDING WINNERS ====================

      if (evt === "startRecordingWinners") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') {
          this._safeSend(ws, ["startRecordingResult", { success: false, message: "Room name required" }]);
          return;
        }
        const room = roomName.trim();
        try {
          await this.dataManager.setRecordingStatus(room, true);
          this._safeSend(ws, ["startRecordingResult", { success: true, message: "Recording started successfully" }]);
          this._broadcastToRoom(room, ["recordingStatus", true]);
        } catch (e) {
          this._safeSend(ws, ["startRecordingResult", { success: false, message: e.message || "Failed to start recording" }]);
        }
        return;
      }

      if (evt === "stopRecordingWinners") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') {
          this._safeSend(ws, ["stopRecordingResult", { success: false, message: "Room name required" }]);
          return;
        }
        const room = roomName.trim();
        try {
          await this.dataManager.setRecordingStatus(room, false);
          await this.dataManager.deleteAllWinners(room);
          this._safeSend(ws, ["stopRecordingResult", { success: true, message: "Recording stopped successfully" }]);
          this._broadcastToRoom(room, ["recordingStatus", false]);
        } catch (e) {
          this._safeSend(ws, ["stopRecordingResult", { success: false, message: e.message || "Failed to stop recording" }]);
        }
        return;
      }

      if (evt === "getRecordingStatus") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        const room = roomName.trim();
        try {
          const isRecording = await this.dataManager.getRecordingStatus(room);
          this._safeSend(ws, ["recordingStatus", isRecording]);
        } catch (e) {
          this._safeSend(ws, ["recordingError", e.message || "Failed to get recording status"]);
        }
        return;
      }

      if (evt === "sendWinnersToRoom") {
        let room = data[1] || ws.room || this.clientRooms.get(ws._wsId);
        if (!room || typeof room !== 'string' || room.trim() === '') {
          this._safeSend(ws, ["sendWinnersResult", { success: false, message: "Room required" }]);
          return;
        }
        const roomKey = room.trim();
        try {
          const isRecording = await this.dataManager.getRecordingStatus(roomKey);
          if (!isRecording) {
            this._safeSend(ws, ["sendWinnersResult", { success: false, message: "Recording is not enabled" }]);
            return;
          }
          await this._broadcastLowCardWinners(roomKey);
          this._safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners sent successfully" }]);
        } catch (e) {
          this._safeSend(ws, ["sendWinnersResult", { success: false, message: e.message || "Failed to send winners" }]);
        }
        return;
      }

      if (evt === "getRoomWinners") {
        let room = data[1] || ws.room || this.clientRooms.get(ws._wsId);
        if (!room || typeof room !== 'string' || room.trim() === '') {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        const roomKey = room.trim();
        try {
          const isRecording = await this.dataManager.getRecordingStatus(roomKey);
          const winners = await this.dataManager.getWinners(roomKey);
          this._safeSend(ws, ["roomWinnersResponse", {
            winners: winners || {}, room: roomKey, recording: isRecording || false
          }]);
        } catch (e) {
          this._safeSend(ws, ["recordingError", e.message || "Failed to get room winners"]);
        }
        return;
      }

      if (evt === "startGameWithRecording") {
        const [, room, bet, username] = data;
        if (!room || !username || typeof room !== 'string' || typeof username !== 'string') {
          this._safeSend(ws, ["recordingError", "Room and username required"]);
          return;
        }
        const roomKey = room.trim();
        const usernameClean = username.trim();
        const betAmount = parseInt(bet, 10) || 0;

        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          this._safeSend(ws, ["recordingError", "Invalid bet (0 or 100-100000)"]);
          return;
        }

        try {
          const isRecording = await this.dataManager.getRecordingStatus(roomKey);
          if (!isRecording) {
            this._safeSend(ws, ["recordingError", "Recording is not enabled in this room"]);
            return;
          }

          const existingGame = this.activeGames.get(roomKey);
          if (existingGame?._isActive && !existingGame._gameEnded) {
            this._safeSend(ws, ["recordingError", "Game is already running"]);
            return;
          }
          if (existingGame) await this._forceCleanupGame(roomKey, existingGame);

          if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
            this._safeSend(ws, ["recordingError", "Server is busy"]);
            return;
          }

          const wsId = ws._wsId;
          const game = this._createGameObject(roomKey, betAmount, usernameClean, true);
          game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
          game.playerWsId.set(usernameClean, wsId);
          this.activeGames.set(roomKey, game);
          this._addClient(roomKey, ws, usernameClean);

          this._broadcastToRoom(roomKey, ["gameLowCardStart", betAmount]);
          this._broadcastToRoom(roomKey, ["gameLowCardStartSuccess", usernameClean, betAmount]);

          this._safeSend(ws, ["recordingGameStarted", {
            room: roomKey, host: usernameClean, bet: betAmount
          }]);

          this._startRegistration(roomKey, game);
        } catch (e) {
          this._safeSend(ws, ["recordingError", e.message || "Failed to start game"]);
        }
        return;
      }

      if (evt === "addLowCardWinner") {
        const { room, username } = data[1] || {};
        if (!room || !username || typeof room !== 'string' || typeof username !== 'string') {
          this._safeSend(ws, ["error", "Room and username required"]);
          return;
        }
        const roomKey = room.trim();
        const userKey = username.trim();

        try {
          const isRecording = await this.dataManager.getRecordingStatus(roomKey);
          if (!isRecording) {
            this._safeSend(ws, ["error", "Recording is not enabled"]);
            return;
          }

          const winners = await this.dataManager.getWinners(roomKey);
          let count = parseInt(String(winners[userKey] || "0").replace("x", "")) || 0;
          winners[userKey] = (count + 1) + "x";
          await this.dataManager.setWinners(roomKey, winners);

          this._safeSend(ws, ["addWinnerResult", {
            success: true, username: userKey, count: count + 1, room: roomKey
          }]);
          this._broadcastLowCardWinners(roomKey);
        } catch (e) {
          this._safeSend(ws, ["error", e.message || "Failed to add winner"]);
        }
        return;
      }

      if (evt === "deleteAllWinners") {
        const room = data[1];
        if (!room || typeof room !== 'string' || room.trim() === '') {
          this._safeSend(ws, ["error", "Room required"]);
          return;
        }
        const roomKey = room.trim();
        try {
          await this.dataManager.deleteAllWinners(roomKey);
          this._safeSend(ws, ["deleteWinnersResult", {
            success: true, message: "All winners deleted", room: roomKey
          }]);
          this._broadcastToRoom(roomKey, ["recordingStatus", false]);
        } catch (e) {
          this._safeSend(ws, ["error", e.message || "Failed to delete winners"]);
        }
        return;
      }

      if (evt === "lowCardWinnerUpdate") {
        let room = data[1] || ws.room || this.clientRooms.get(ws._wsId);
        if (!room || typeof room !== 'string' || room.trim() === '') {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        const roomKey = room.trim();
        await this._broadcastLowCardWinners(roomKey);
        return;
      }

      // ==================== DICE EVENTS ====================

      if (evt === "submitDiceAnswer") {
        await this.submitDiceAnswer(ws, data[1], data[2]);
        return;
      }

      if (evt === "getDiceLastWeekWinner") {
        try {
          const wsId = ws._wsId;
          const now = Date.now();
          if (!this._lastWinnerRequestTime) this._lastWinnerRequestTime = new Map();
          const lastReq = this._lastWinnerRequestTime.get(wsId) || 0;
          if (now - lastReq < 5000) return;
          this._lastWinnerRequestTime.set(wsId, now);
          const winner = await this.dataManager.getLastWeekWinner();
          if (winner && typeof winner === 'object' && winner.username) {
            this._safeSend(ws, ["diceLastWeekWinner", String(winner.username), parseInt(winner.score, 10) || 0, String(winner.week || '')]);
          } else {
            this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
          }
        } catch (e) {
          this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
        }
        return;
      }

      if (evt === "deleteDiceLastWeekWinner") {
        try {
          await this.dataManager.deleteLastWeekWinner();
          this._safeSend(ws, ["diceLastWeekWinnerDeleted", true, "Deleted"]);
        } catch (e) {
          this._safeSend(ws, ["diceLastWeekWinnerDeleted", false, e.message]);
        }
        return;
      }

      if (evt === "getDiceLeaderboard") {
        try {
          let limit = CONSTANTS.DEFAULT_LEADERBOARD_LIMIT;
          if (data.length > 1 && typeof data[1] === 'number') {
            limit = Math.min(Math.max(data[1], CONSTANTS.MIN_LEADERBOARD_LIMIT), CONSTANTS.MAX_LEADERBOARD_LIMIT);
          }
          const leaderboard = await this.dataManager.getLeaderboard(limit);
          this._safeSend(ws, ["diceLeaderboard", leaderboard]);
        } catch (e) {
          this._safeSend(ws, ["diceLeaderboard", []]);
        }
        return;
      }

      if (evt === "getDicePoints") {
        try {
          const points = await this.dataManager.getDicePoints();
          this._safeSend(ws, ["dicePoints", points]);
        } catch (e) {
          this._safeSend(ws, ["dicePoints", {}]);
        }
        return;
      }

      if (evt === "getDiceStatus") {
        const isActive = !!this.currentDiceRoll && this._canSubmitDiceAnswer;
        this._safeSend(ws, ["diceStatus", isActive, this._diceRound || 1]);
        return;
      }

      if (evt === "getDiceNotification") {
        try {
          const isDiceTime = this.alarmScheduler.isDiceTime();
          const isActive = this.currentDiceRoll && this._canSubmitDiceAnswer;
          const timeLeft = this._getTimeLeftUntilNextDice();
          let notification = "";
          if (isActive) {
            const elapsed = (Date.now() - this._diceStartTime) / 1000;
            const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
            const remaining = Math.max(0, totalTime - elapsed);
            notification = Math.floor(remaining) + "s remaining";
          } else if (isDiceTime) {
            notification = "Dice game starting soon...";
          } else if (timeLeft && timeLeft.text) {
            notification = "Next dice game in: " + timeLeft.text;
          } else {
            notification = "Waiting...";
          }
          this._safeSend(ws, ["diceNotification", notification]);
        } catch (e) {
          this._safeSend(ws, ["diceNotification", "Waiting..."]);
        }
        return;
      }

      // ==================== CHECK GAME RUNNING ====================

      if (evt === "checkGameRunning") {
        await this.checkGameRunning(ws, data[1]);
        return;
      }

      // ==================== LOWCARD GAME EVENTS ====================

      const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      if (room === CONSTANTS.DICE_ROOM) {
        this._safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]);
        return;
      }

      switch (evt) {
        case "gameLowCardStart": await this.startGame(ws, data[1], data[2]); break;
        case "gameLowCardJoin": await this.joinGame(ws, data[1]); break;
        case "gameLowCardNumber": await this.submitNumber(ws, data[1], data[2] || "", data[3]); break;
        default: break;
      }
    } catch (e) {}
  }

  // ============================================================
  // GAME FACTORY
  // ============================================================

  _createGameObject(room, betAmount, username, isRecording = false) {
    return {
      room,
      players: new Map(),
      botPlayers: new Map(),
      registrationOpen: true,
      round: 1,
      numbers: new Map(),
      tanda: new Map(),
      eliminated: new Set(),
      betAmount,
      hostId: username,
      hostName: username,
      useBots: false,
      evaluationLocked: false,
      drawTimeExpired: false,
      _isActive: true,
      _gameEnded: false,
      _phase: 'registration',
      _state: 'registration',
      _botTimeouts: new Set(),
      _botTimers: new Set(),
      _botsAdded: false,
      _registrationTimer: null,
      _drawTimer: null,
      _evalTimer: null,
      _safetyTimer: null,
      _isEvaluating: false,
      _evalStartTime: null,
      _createdAt: Date.now(),
      _drawPhaseStart: null,
      _endTime: null,
      playerWsId: new Map(),
      _startedByRecording: isRecording,
      _startedBy: isRecording ? 'recording' : 'user',
      _notificationTimers: [],
      _drawNotificationTimers: [],
      _cleanupStarted: false,
      _evalLocks: new Map(),
      _gameId: `${room}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
      _version: 0
    };
  }

  // ============================================================
  // SWITCH ROOM
  // ============================================================

  async switchRoom(ws, room, username = null) {
    try {
      if (this.isDestroyed) { this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!room || typeof room !== 'string' || room.trim() === "") {
        this._safeSend(ws, ["gameLowCardError", "Invalid room name"]); return;
      }
      const roomName = room.trim();
      const wsId = ws._wsId;
      if (!wsId) { this._safeSend(ws, ["gameLowCardError", "Connection error"]); return; }
      if (ws.readyState !== 1 || ws._closing) { this._safeSend(ws, ["gameLowCardError", "Connection closed"]); return; }

      const lockKey = `switch_${wsId}`;
      if (this._switchLocks.has(lockKey)) { this._safeSend(ws, ["gameLowCardError", "Please wait"]); return; }
      this._switchLocks.set(lockKey, Date.now());

      try {
        const currentRoom = ws.room || ws.roomname || this.clientRooms.get(wsId);

        if (currentRoom === roomName) {
          if (roomName === CONSTANTS.DICE_ROOM) this._sendDiceRoomState(ws);
          this._safeSend(ws, ["switchRoomSuccess", roomName]);
          return;
        }

        if (currentRoom) {
          const clients = this.wsClients.get(currentRoom);
          if (clients) {
            clients.delete(wsId);
            if (clients.size === 0) this.wsClients.delete(currentRoom);
          }
          if (ws.username) {
            const conn = this.userConnections.get(ws.username);
            if (conn && conn.room === currentRoom) conn.room = null;
          }
        }

        if (!this.wsClients.has(roomName)) this.wsClients.set(roomName, new Set());
        this.wsClients.get(roomName).add(wsId);
        this.clientRooms.set(wsId, roomName);

        ws.room = roomName;
        ws.roomname = roomName;
        if (username) ws.username = username;

        this._setWsMeta(ws, { username: username || ws.username || null, room: roomName });

        const finalUsername = username || ws.username;
        if (finalUsername) {
          let conn = this.userConnections.get(finalUsername);
          if (conn) { conn.room = roomName; conn.wsId = wsId; conn.ws = ws; conn.timestamp = Date.now(); }
          else this.userConnections.set(finalUsername, { wsId, ws, room: roomName, timestamp: Date.now() });
        }

        this._syncRoomClients(roomName);
        this._safeSend(ws, ["switchRoomSuccess", roomName]);

        if (roomName === CONSTANTS.DICE_ROOM) {
          this._sendDiceRoomState(ws);
          if (this.alarmScheduler.isDiceTime() && !this._diceGameStarted && !this.currentDiceRoll) {
            this._startDiceGameIfNotStarted();
          }
        }
      } finally {
        setTimeout(() => { this._switchLocks.delete(lockKey); }, 2000);
      }
    } catch (e) { this._safeSend(ws, ["gameLowCardError", e.message || "Switch failed"]); }
  }

  // ============================================================
  // CHECK GAME RUNNING
  // ============================================================

  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) { this._safeSend(ws, ["gameStatus", "false"]); return; }
      let room = roomname || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameStatus", "false"]); return;
      }
      this._ensureClientInRoom(ws, room, ws.username);
      if (room === CONSTANTS.DICE_ROOM) { this._sendDiceRoomState(ws); return; }
      const game = this.activeGames.get(room);
      const isRunning = game?._isActive && !game._gameEnded && game.players?.size > 0;
      this._safeSend(ws, ["gameStatus", isRunning ? "true" : "false"]);
      if (isRunning) this._sendCurrentGameState(ws, room);
    } catch (e) {}
  }

  // ============================================================
  // ADD CLIENT
  // ============================================================

  _addClient(room, ws, username = null) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      if (!wsId) { this._safeSend(ws, ["gameLowCardError", "Connection error"]); return; }

      if (this.clientRooms.has(wsId)) {
        const oldRoom = this.clientRooms.get(wsId);
        if (oldRoom && oldRoom !== room) this._removeClientFromRoom(oldRoom, wsId);
      }

      if (!this.wsClients.has(room)) this.wsClients.set(room, new Set());
      this.wsClients.get(room).add(wsId);
      this.clientRooms.set(wsId, room);
      this.wsMap.set(wsId, ws);

      ws.room = room;
      ws.roomname = room;
      if (username) ws.username = username;

      this._setWsMeta(ws, { username: username || null, room });

      if (username) {
        let conn = this.userConnections.get(username);
        if (conn) { conn.room = room; conn.timestamp = Date.now(); conn.ws = ws; conn.wsId = wsId; }
        else this.userConnections.set(username, { wsId, ws, room, timestamp: Date.now() });
        this._reconnectAttempts.delete(username);
      }
    } catch (e) {}
  }

  _removeClientFromRoom(room, wsId) {
    try {
      if (!room || !wsId) return;
      const clients = this.wsClients.get(room);
      if (clients) {
        clients.delete(wsId);
        if (clients.size === 0) this.wsClients.delete(room);
      }
    } catch (e) {}
  }

  _getWsId(ws) { return ws?._wsId || null; }

  _isGameActuallyRunning(game) {
    return game?._isActive === true && !game?._gameEnded && game?.players?.size > 0;
  }

  _getActivePlayers(game) {
    try {
      if (!game?._isActive || game?._gameEnded || !game?.players) return [];
      return Array.from(game.players.entries())
        .filter(([id]) => !game.eliminated?.has(id))
        .map(([, p]) => p);
    } catch (e) { return []; }
  }

  _getActivePlayerIds(game) {
    try {
      if (!game?._isActive || game._gameEnded || !game?.players) return [];
      return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
    } catch (e) { return []; }
  }

  _incrementGameVersion(game) { if (game) game._version = (game._version || 0) + 1; }

  _getBotNumberByRound(round) {
    if (round <= 2) return Math.floor(Math.random() * 12) + 1;
    return Math.random() < 0.6
      ? [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)]
      : [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
  }

  _getRandomCardTanda() { return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; }
  _getRandomDrawDelay() { return (Math.floor(Math.random() * 14) + 2) * 1000; }

  // ============================================================
  // GAME: START
  // ============================================================

  async startGame(ws, bet, username) {
    try {
      if (this.isDestroyed) { this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!username || typeof username !== 'string' || !username.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]); return;
      }
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]); return;
      }
      if (room === CONSTANTS.DICE_ROOM) {
        this._safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]); return;
      }

      const lockKey = `game_start_${room}`;
      if (this._gameLocks.has(lockKey)) {
        this._safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]); return;
      }
      this._gameLocks.set(lockKey, Date.now());

      try {
        const isRecordingEnabled = await this.dataManager.getRecordingStatus(room);
        if (isRecordingEnabled) {
          this._safeSend(ws, ["gameLowCardError", "Recording is ACTIVE in this room. Users cannot start games."]);
          return;
        }

        this._syncRoomClients(room);

        const existingGame = this.activeGames.get(room);
        if (existingGame?._isActive && !existingGame._gameEnded) {
          this._safeSend(ws, ["gameLowCardError", "Game is already running"]); return;
        }
        if (existingGame) await this._forceCleanupGame(room, existingGame);

        const betAmount = parseInt(bet, 10) || 0;
        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          this._safeSend(ws, ["gameLowCardError", "Invalid bet (0 or 100-" + CONSTANTS.MAX_BET + ")"]); return;
        }
        if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
          this._safeSend(ws, ["gameLowCardError", "Server is busy"]); return;
        }

        const wsId = ws._wsId;
        const game = this._createGameObject(room, betAmount, usernameClean, false);
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        game.playerWsId.set(usernameClean, wsId);
        this.activeGames.set(room, game);
        this._syncRoomClients(room);
        this._addClient(room, ws, usernameClean);

        this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
        this._broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);

        this._startRegistration(room, game);
        await this._saveGameState();
      } finally {
        setTimeout(() => { this._gameLocks.delete(lockKey); }, 3000);
      }
    } catch (e) {}
  }

  // ============================================================
  // GAME: REGISTRATION
  // ============================================================

  _startRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      if (game._registrationTimer) { this._clearTimer(game._registrationTimer); game._registrationTimer = null; }
      if (game._notificationTimers) {
        for (const timer of game._notificationTimers) this._clearTimer(timer);
        game._notificationTimers = [];
      }

      const notifications = [
        { delay: 5000, message: "15s" },
        { delay: 10000, message: "10s" },
        { delay: 15000, message: "5s" }
      ];
      for (const notif of notifications) {
        const timer = this._trackTimer(setTimeout(() => {
          if (this._isGameActuallyRunning(game) && game.registrationOpen) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", notif.message]);
          }
        }, notif.delay));
        game._notificationTimers.push(timer);
      }

      const timer = this._trackTimer(setTimeout(() => {
        if (this._isGameActuallyRunning(game) && game.registrationOpen) {
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
          this._closeRegistration(room, game);
        }
      }, CONSTANTS.REGISTRATION_TIME_MS || 20000));
      game._registrationTimer = timer;
    } catch (e) {}
  }

  // ============================================================
  // GAME: CLOSE REGISTRATION
  // ============================================================

  _closeRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      this._cleanupGameTimers(game);
      game.registrationOpen = false;

      const humanPlayers = Array.from(game.players.keys()).filter(id => !id.startsWith('BOT_'));
      const humanCount = humanPlayers.length;

      if (!game._botsAdded) {
        if (humanCount === 1 || humanCount === 0) { this._addBots(room, 4); game._botsAdded = true; }
        else if (game.players.size < 2) {
          const needed = Math.min(4 - game.players.size, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { this._addBots(room, needed); game._botsAdded = true; }
        }
      }

      if (this._isGameActuallyRunning(game) && game.players.size >= 2) {
        this._startDrawPhase(room, game);
      } else {
        this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
        this._forceCleanupGame(room, game);
      }
    } catch (e) {}
  }

  // ============================================================
  // GAME: ADD BOTS
  // ============================================================

  _addBots(room, count) {
    try {
      const game = this.activeGames.get(room);
      if (!this._isGameActuallyRunning(game)) return;
      const botNames = ["moz1", "moz2", "moz3", "moz4"];
      const existingBots = Array.from(game.players.keys()).filter(id => id.startsWith('BOT_'));
      const existingBotCount = existingBots.length;
      const maxBotsToAdd = Math.min(count, CONSTANTS.MAX_BOTS_PER_GAME - existingBotCount);
      if (maxBotsToAdd <= 0) return;
      for (let i = 0; i < maxBotsToAdd; i++) {
        const botId = `BOT_${room}_${i}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
        const botName = botNames[(existingBotCount + i) % botNames.length];
        if (!game.players.has(botId)) {
          game.players.set(botId, { id: botId, name: botName });
          if (!game.botPlayers) game.botPlayers = new Map();
          game.botPlayers.set(botId, botName);
        }
      }
      game._botsAdded = true;
      game.useBots = true;
    } catch (e) {}
  }

  // ============================================================
  // GAME: DRAW PHASE
  // ============================================================

  async _startDrawPhase(room, game) {
    const lockKey = `startDraw_${room}`;
    if (this._gameOperationLocks.has(lockKey)) return;
    if (!this._acquireLock(this._gameOperationLocks, lockKey, 10000)) return;

    try {
      if (!this._isGameActuallyRunning(game)) {
        this._releaseLock(this._gameOperationLocks, lockKey);
        return;
      }

      game._isEvaluating = false;
      game._evalStartTime = null;
      game.evaluationLocked = false;

      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
      if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
      if (game._botTimers) {
        for (const timer of game._botTimers) this._clearTimer(timer);
        game._botTimers.clear();
      }
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) this._clearTimer(id);
        game._botTimeouts.clear();
      }

      const activePlayers = this._getActivePlayers(game);

      if (activePlayers.length < 2) {
        if (!game._botsAdded) {
          const needed = Math.min(4 - activePlayers.length, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { this._addBots(room, needed); game._botsAdded = true; }
        }
        const newActive = this._getActivePlayers(game);
        if (newActive.length < 2) {
          if (newActive.length === 1 && !game._gameEnded) {
            const winner = newActive[0]?.name || "Unknown";
            const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
            if (game._startedByRecording) {
              await this._addLowCardWinner(room, winner);
              await this._broadcastLowCardWinners(room);
            }
            game._gameEnded = true;
            game._isActive = false;
            this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
            this._releaseLock(this._gameOperationLocks, lockKey);
            this._forceCleanupGame(room, game);
            return;
          } else {
            game._gameEnded = true;
            game._isActive = false;
            this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
            this._releaseLock(this._gameOperationLocks, lockKey);
            this._forceCleanupGame(room, game);
            return;
          }
        }
      }

      game._phase = 'draw';
      game._state = 'draw';
      game.drawTimeExpired = false;
      game.evaluationLocked = false;
      game._drawPhaseStart = Date.now();

      this._incrementGameVersion(game);

      if (!game._botTimers) game._botTimers = new Set();
      if (!game._botTimeouts) game._botTimeouts = new Set();

      const playersList = this._getActivePlayers(game).map(p => p.name);
      this._broadcastToRoom(room, ["gameLowCardClosed", playersList]);
      this._broadcastToRoom(room, ["gameLowCardNextRound", game.round]);

      this._releaseLock(this._gameOperationLocks, lockKey);

      this._startDrawCountdown(room, game);

      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        this._startBotDraws(room, game);
      }
    } catch (e) {
      this._releaseLock(this._gameOperationLocks, lockKey);
    }
  }

  // ============================================================
  // GAME: DRAW COUNTDOWN
  // ============================================================

  _startDrawCountdown(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      if (game._drawNotificationTimers) {
        for (const timer of game._drawNotificationTimers) this._clearTimer(timer);
        game._drawNotificationTimers = [];
      }

      const notifications = [
        { delay: 5000, message: "15s" },
        { delay: 10000, message: "10s" },
        { delay: 15000, message: "5s" }
      ];
      for (const notif of notifications) {
        const timer = this._trackTimer(setTimeout(() => {
          if (this._isGameActuallyRunning(game) && !game.drawTimeExpired) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", notif.message]);
          }
        }, notif.delay));
        game._drawNotificationTimers.push(timer);
      }

      const timer = this._trackTimer(setTimeout(() => {
        if (this._isGameActuallyRunning(game) && !game.drawTimeExpired) {
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
          this._closeDrawPhase(room, game);
        }
      }, CONSTANTS.DRAW_TIME_MS || 20000));
      game._drawTimer = timer;
    } catch (e) {}
  }

  // ============================================================
  // GAME: BOT DRAWS
  // ============================================================

  _startBotDraws(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game.botPlayers.size === 0) return;
      if (game._state !== 'draw') return;
      if (game.evaluationLocked || game._isEvaluating) return;
      if (game.drawTimeExpired) return;

      const activeBotIds = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated.has(id) && !game.numbers.has(id));
      if (activeBotIds.length === 0) return;

      const maxPerBatch = Math.min(CONSTANTS.MAX_BOT_TIMEOUTS || 5, activeBotIds.length);
      const botsToDraw = activeBotIds.slice(0, maxPerBatch);

      for (const botId of botsToDraw) {
        if (game.evaluationLocked || game._isEvaluating) break;
        if (game.drawTimeExpired) break;
        if (game.eliminated.has(botId) || game.numbers.has(botId)) continue;

        const delay = this._getRandomDrawDelay();
        const timer = this._trackTimer(setTimeout(() => {
          if (!this._isGameActuallyRunning(game)) return;
          if (game._state !== 'draw') return;
          if (game.evaluationLocked || game._isEvaluating) return;
          if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
          if (game.drawTimeExpired) return;

          this._handleBotDraw(room, botId, game);
          game._botTimers.delete(timer);
        }, delay));

        game._botTimers.add(timer);
      }
    } catch (e) {}
  }

  // ============================================================
  // GAME: HANDLE BOT DRAW
  // ============================================================

  _handleBotDraw(room, botId, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._state !== 'draw') return;
      if (game.evaluationLocked || game._isEvaluating) return;
      if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
      if (game.drawTimeExpired) return;

      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();

      const checkLockKey = `check_${room}`;
      if (!this._acquireLock(this._checkLocks, checkLockKey, 2000)) {
        setTimeout(() => {
          if (!this._isGameActuallyRunning(game)) return;
          if (game._state !== 'draw') return;
          if (game.evaluationLocked || game._isEvaluating) return;
          if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
          if (game.drawTimeExpired) return;
          this._handleBotDraw(room, botId, game);
        }, 500);
        return;
      }

      try {
        if (game.eliminated.has(botId) || game.numbers.has(botId) || game.drawTimeExpired) return;

        game.numbers.set(botId, number);
        game.tanda.set(botId, tanda);

        const botName = game.players.get(botId)?.name || botId;
        this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);

        if (!game.evaluationLocked && !game._isEvaluating && game._state === 'draw' && !game.drawTimeExpired) {
          this._checkAllDrawn(room, game);
        }
      } finally {
        this._releaseLock(this._checkLocks, checkLockKey);
      }
    } catch (e) {}
  }

  // ============================================================
  // GAME: FORCE BOT DRAW
  // ============================================================

  _forceBotDraw(room, botId, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game.eliminated.has(botId) || game.numbers.has(botId)) return;

      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();

      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);

      const botName = game.players.get(botId)?.name || botId;
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
    } catch (e) {}
  }

  // ============================================================
  // GAME: CHECK ALL DRAWN
  // ============================================================

  _checkAllDrawn(room, game) {
    const checkLockKey = `check_${room}`;
    if (!this._acquireLock(this._checkLocks, checkLockKey, 2000)) return;

    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._state !== 'draw') return;
      if (game.evaluationLocked || game._isEvaluating) return;
      if (game.drawTimeExpired) return;

      const activeIds = this._getActivePlayerIds(game);
      const drawnCount = game.numbers.size;

      if (activeIds.length === 1) {
        const winnerId = activeIds[0];
        const winnerName = game.players.get(winnerId)?.name || winnerId;
        const totalCoin = game.betAmount * game.players.size;

        if (game._startedByRecording) {
          this._addLowCardWinner(room, winnerName);
          this._broadcastLowCardWinners(room);
        }

        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        this._forceCleanupGame(room, game);
        return;
      }

      if (drawnCount === activeIds.length && activeIds.length > 1) {
        if (game.drawTimeExpired || game.evaluationLocked || game._state !== 'draw') return;

        game.evaluationLocked = true;
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);

        if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }

        const evalTimer = this._trackTimer(setTimeout(() => {
          if (this._isGameActuallyRunning(game) && !game._gameEnded && game._state === 'draw') {
            this._evaluateRound(room, game);
          }
        }, CONSTANTS.EVALUATION_DELAY_MS));
        game._evalTimer = evalTimer;
      }
    } finally {
      this._releaseLock(this._checkLocks, checkLockKey);
    }
  }

  // ============================================================
  // GAME: CLOSE DRAW PHASE
  // ============================================================

  async _closeDrawPhase(room, game) {
    const drawLockKey = `draw_${room}`;
    if (this._drawLocks.has(drawLockKey)) return;
    if (!this._acquireLock(this._drawLocks, drawLockKey, 10000)) return;

    try {
      if (!this._isGameActuallyRunning(game)) {
        this._releaseLock(this._drawLocks, drawLockKey);
        return;
      }

      if (game.drawTimeExpired || game.evaluationLocked || game._isEvaluating) {
        this._releaseLock(this._drawLocks, drawLockKey);
        return;
      }

      const checkLockKey = `check_${room}`;
      if (!this._acquireLock(this._checkLocks, checkLockKey, 2000)) {
        await new Promise(resolve => setTimeout(resolve, 100));
        if (game.drawTimeExpired || game.evaluationLocked || game._isEvaluating) {
          this._releaseLock(this._drawLocks, drawLockKey);
          return;
        }
        if (!this._acquireLock(this._checkLocks, checkLockKey, 2000)) {
          this._releaseLock(this._drawLocks, drawLockKey);
          return;
        }
      }

      try {
        if (game.drawTimeExpired || game.evaluationLocked || game._isEvaluating) return;

        this._cleanupGameTimers(game);
        game.drawTimeExpired = true;
        game.evaluationLocked = true;

        if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
          const activeBotIds = Array.from(game.botPlayers.keys())
            .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
          for (const botId of activeBotIds) this._forceBotDraw(room, botId, game);
        }

        const activeIds = this._getActivePlayerIds(game);
        const submittedIds = new Set(game.numbers?.keys() || []);
        const notSubmitted = activeIds.filter(id => !submittedIds.has(id) && !game.eliminated?.has(id));

        if (notSubmitted.length > 0 && submittedIds.size === 0) {
          this._broadcastToRoom(room, ["gameLowCardError", "No one submitted numbers"]);
          this._releaseLock(this._drawLocks, drawLockKey);
          this._forceCleanupGame(room, game);
          return;
        }

        for (const id of notSubmitted) {
          if (!game.eliminated) game.eliminated = new Set();
          game.eliminated.add(id);
          game.numbers?.delete(id);
          game.tanda?.delete(id);
        }

        const remaining = Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));

        if (remaining.length === 1 && !game._gameEnded) {
          const winnerId = remaining[0];
          const winnerName = game.players.get(winnerId)?.name || winnerId;
          const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);

          if (game._startedByRecording) {
            await this._addLowCardWinner(room, winnerName);
            await this._broadcastLowCardWinners(room);
          }

          this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
          this._releaseLock(this._drawLocks, drawLockKey);
          this._forceCleanupGame(room, game);
          return;
        }

        if (remaining.length === 0) {
          this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
          this._releaseLock(this._drawLocks, drawLockKey);
          this._forceCleanupGame(room, game);
          return;
        }

        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        this._releaseLock(this._drawLocks, drawLockKey);

        const evalTimer = this._trackTimer(setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
              this._evaluateRound(room, game);
            }
          } catch (e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS || 2000));
        game._evalTimer = evalTimer;
      } finally {
        this._releaseLock(this._checkLocks, checkLockKey);
      }
    } catch (e) {
      this._releaseLock(this._drawLocks, drawLockKey);
    }
  }

  // ============================================================
  // GAME: EVALUATE ROUND
  // ============================================================

  async _evaluateRound(room, game) {
    const evalLockKey = `eval_${room}`;
    if (this._evaluationLocks.has(evalLockKey)) return;
    if (!this._acquireLock(this._evaluationLocks, evalLockKey, 15000)) return;

    const gameVersion = game._version;
    const gameId = game._gameId;

    try {
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game || currentGame?._gameId !== gameId || currentGame?._version !== gameVersion) {
        this._releaseLock(this._evaluationLocks, evalLockKey);
        return;
      }

      if (this.isDestroyed || !game || game._isEvaluating || !game._isActive || game._gameEnded) {
        this._releaseLock(this._evaluationLocks, evalLockKey);
        return;
      }

      this._cleanupGameTimers(game);
      game._isEvaluating = true;
      game._evalStartTime = Date.now();
      game._phase = 'evaluating';
      game._state = 'evaluating';

      const safetyTimer = this._trackTimer(setTimeout(() => {
        if (game?._isEvaluating) {
          game._isEvaluating = false;
          game.evaluationLocked = false;
          this._releaseLock(this._evaluationLocks, evalLockKey);
          this._broadcastToRoom(room, ["gameLowCardError", "Timeout, melanjutkan..."]);
          if (this._isGameActuallyRunning(game) && !game._gameEnded) {
            this._startDrawPhase(room, game);
          } else if (game && !game._isActive) {
            this._forceCleanupGame(room, game);
          }
        }
      }, CONSTANTS.EVALUATION_TIMEOUT_MS || 30000));
      game._safetyTimer = safetyTimer;

      const numbers = game.numbers || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const tanda = game.tanda || new Map();
      const entries = Array.from(numbers.entries());
      const submittedIds = new Set(numbers.keys());
      const activeIds = this._getActivePlayerIds(game);

      for (const id of activeIds) {
        if (!submittedIds.has(id) && !game.eliminated?.has(id)) {
          game.eliminated.add(id);
        }
      }

      if (entries.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);

        const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

        if (remaining.length === 1) {
          const winnerId = remaining[0];
          const winnerName = players.get(winnerId)?.name || winnerId;
          const totalCoin = (game.betAmount || 0) * players.size;

          if (game._startedByRecording) {
            await this._addLowCardWinner(room, winnerName);
            await this._broadcastLowCardWinners(room);
          }

          this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
          game._gameEnded = true;
          game._isActive = false;
          game._isEvaluating = false;
          if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
          this._releaseLock(this._evaluationLocks, evalLockKey);
          this._forceCleanupGame(room, game);
          return;
        }

        this._broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);
        this._forceCleanupGame(room, game);
        return;
      }

      const values = entries.map(([, n]) => n);
      const allSame = values.every(v => v === values[0]);
      let losers = [];

      if (!allSame && values.length > 0) {
        const lowest = Math.min(...values);
        losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
        for (const id of losers) eliminated.add(id);
      }

      const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

      if (!this._isGameActuallyRunning(game) || game._gameEnded) {
        game._isEvaluating = false;
        this._releaseLock(this._evaluationLocks, evalLockKey);
        this._forceCleanupGame(room, game);
        return;
      }

      if (allSame && remaining.length >= 2) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);

        numbers.clear();
        tanda.clear();
        game.round++;
        game.evaluationLocked = false;
        game.drawTimeExpired = false;
        game._phase = 'draw';
        game._state = 'draw';
        game.numbers = new Map();
        game.tanda = new Map();
        game._botTimeouts = new Set();
        game._botTimers = new Set();

        this._incrementGameVersion(game);

        const remainingNames = remaining.map(id => players.get(id)?.name || id);
        this._broadcastToRoom(room, ["gameLowCardRoundResult",
          game.round - 1,
          entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`),
          [],
          remainingNames,
          true
        ]);

        if (this._isGameActuallyRunning(game) && !game._gameEnded) {
          this._startDrawPhase(room, game);
        } else {
          this._forceCleanupGame(room, game);
        }
        return;
      }

      if (remaining.length === 1 && !game._gameEnded) {
        const winnerId = remaining[0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;

        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winnerName);
          await this._broadcastLowCardWinners(room);
        }

        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);
        this._forceCleanupGame(room, game);
        return;
      }

      if (remaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
        this._forceCleanupGame(room, game);
        return;
      }

      const numbersArr = entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`);
      const loserNames = [...losers].map(id => players.get(id)?.name || id);
      const remainingNames = remaining.map(id => players.get(id)?.name || id);

      this._broadcastToRoom(room, ["gameLowCardRoundResult", game.round, numbersArr, loserNames, remainingNames]);

      numbers.clear();
      tanda.clear();
      game.round++;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      game._phase = 'draw';
      game._state = 'draw';
      game.numbers = new Map();
      game.tanda = new Map();
      game._botTimeouts = new Set();
      game._botTimers = new Set();
      game._isEvaluating = false;
      game._evalStartTime = null;

      this._incrementGameVersion(game);

      if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
      this._releaseLock(this._evaluationLocks, evalLockKey);

      if (this._isGameActuallyRunning(game) && !game._gameEnded) {
        const activePlayers = this._getActivePlayers(game);
        if (activePlayers.length >= 2) {
          setTimeout(() => {
            if (this._isGameActuallyRunning(game) && !game._gameEnded) {
              this._startDrawPhase(room, game);
            } else {
              this._forceCleanupGame(room, game);
            }
          }, 500);
        } else if (activePlayers.length === 1) {
          const winner = activePlayers[0].name;
          const totalCoin = game.betAmount * game.players.size;
          game._gameEnded = true;
          game._isActive = false;
          this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
          this._forceCleanupGame(room, game);
        } else {
          this._forceCleanupGame(room, game);
        }
      } else {
        this._forceCleanupGame(room, game);
      }
    } catch (e) {
      if (game) {
        game._isEvaluating = false;
        game._evalStartTime = null;
        this._releaseLock(this._evaluationLocks, `eval_${room}`);
        if (this._isGameActuallyRunning(game) && !game._gameEnded) {
          this._broadcastToRoom(room, ["gameLowCardError", "Error, restarting round"]);
          setTimeout(() => {
            if (this._isGameActuallyRunning(game) && !game._gameEnded) {
              this._startDrawPhase(room, game);
            } else {
              this._forceCleanupGame(room, game);
            }
          }, 1000);
        } else {
          this._forceCleanupGame(room, game);
        }
      } else {
        this._releaseLock(this._evaluationLocks, `eval_${room}`);
      }
    }
  }

  // ============================================================
  // GAME: JOIN
  // ============================================================

  async joinGame(ws, username) {
    try {
      if (this.isDestroyed) { this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!username || typeof username !== 'string' || !username.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]); return;
      }

      const usernameClean = username.trim();
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);

      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]); return;
      }

      const lockKey = `join_${room}_${usernameClean}`;
      if (this._joinLocks.has(lockKey)) {
        this._safeSend(ws, ["gameLowCardError", "Please wait"]); return;
      }
      this._joinLocks.set(lockKey, Date.now());

      try {
        this._syncRoomClients(room);

        const game = this.activeGames.get(room);
        if (!game?._isActive || game._gameEnded || !game.players) {
          this._safeSend(ws, ["gameLowCardError", "No active game in this room"]); return;
        }

        if (game.players.has(usernameClean)) {
          if (game.eliminated?.has(usernameClean)) {
            this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]); return;
          }
          if (game.numbers.has(usernameClean)) {
            this._safeSend(ws, ["gameLowCardPlayerDraw", usernameClean, game.numbers.get(usernameClean), game.tanda.get(usernameClean) || ""]);
          }
          return;
        }

        if (!game.registrationOpen) {
          this._safeSend(ws, ["gameLowCardNoJoin", usernameClean, game.betAmount]);
          this._safeSend(ws, ["gameLowCardError", "Registration is closed"]);
          return;
        }

        if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
          this._safeSend(ws, ["gameLowCardError", "Game is full"]); return;
        }

        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        this._addClient(room, ws, usernameClean);
        game.playerWsId.set(usernameClean, wsId);

        this._broadcastToRoom(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
        await this._saveGameState();
      } finally {
        setTimeout(() => { this._joinLocks.delete(lockKey); }, 2000);
      }
    } catch (e) {}
  }

  // ============================================================
  // GAME: SUBMIT NUMBER
  // ============================================================

  async submitNumber(ws, number, tanda, username) {
    try {
      if (this.isDestroyed) { this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!username || typeof username !== 'string' || !username.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]); return;
      }

      const usernameClean = username.trim();
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);

      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]); return;
      }

      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]); return;
      }

      if (game.players.has(usernameClean) && game.eliminated?.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]); return;
      }

      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw' || game._isEvaluating) {
        this._safeSend(ws, ["gameLowCardError", "Cannot submit now"]); return;
      }

      if (!game.players.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game"]); return;
      }

      if (game.numbers.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You have already submitted"]); return;
      }

      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]); return;
      }

      const validTandas = ["C1", "C2", "C3", "C4", ""];
      if (!validTandas.includes(tanda)) tanda = "";

      const submitLockKey = `submit_${room}`;
      if (this._submitLocks.has(submitLockKey)) {
        await new Promise(resolve => setTimeout(resolve, 100));
        if (game.numbers.has(usernameClean)) {
          this._safeSend(ws, ["gameLowCardError", "You have already submitted"]); return;
        }
      }

      if (!this._acquireLock(this._submitLocks, submitLockKey, 2000)) {
        this._safeSend(ws, ["gameLowCardError", "System busy, please retry"]); return;
      }

      try {
        if (game.numbers.has(usernameClean)) {
          this._safeSend(ws, ["gameLowCardError", "You have already submitted"]); return;
        }
        if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw' || game._isEvaluating) {
          this._safeSend(ws, ["gameLowCardError", "Cannot submit now"]); return;
        }

        game.numbers.set(usernameClean, n);
        game.tanda.set(usernameClean, tanda);

        this._broadcastToRoom(room, ["gameLowCardPlayerDraw", usernameClean, n, tanda]);

        const activeIds = this._getActivePlayerIds(game);
        if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired &&
            this._isGameActuallyRunning(game) && game._isActive && !game._gameEnded) {

          game.evaluationLocked = true;
          if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }

          this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);

          const evalTimer = this._trackTimer(setTimeout(() => {
            try {
              const currentGame = this.activeGames.get(room);
              if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
                this._evaluateRound(room, game);
              }
            } catch (e) {}
          }, CONSTANTS.EVALUATION_DELAY_MS));
          game._evalTimer = evalTimer;
        }
      } finally {
        this._releaseLock(this._submitLocks, submitLockKey);
      }
    } catch (e) {}
  }

  // ============================================================
  // DICE GAME
  // ============================================================

  _startDiceFast() {
    try {
      if (this._diceGameStarted && this.currentDiceRoll) return;

      const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
      if (!clients || clients.size === 0) {
        this._diceLock = false;
        this._isShowingDice = false;
        this._diceGameStarted = false;
        return;
      }
      if (!this._diceSessionActive || this._diceSessionEnded) {
        this._diceLock = false;
        this._isShowingDice = false;
        this._diceGameStarted = false;
        return;
      }
      if (this._diceLock || this.currentDiceRoll || this._isShowingDice) return;
      if (this._diceTimeUpCooldown) return;

      this._diceLoopCounter = (this._diceLoopCounter || 0) + 1;
      if (this._diceLoopCounter > this._maxDiceLoops) {
        this._diceLoopCounter = 0;
        this._diceLock = false;
        this._isShowingDice = false;
        return;
      }

      this._diceLock = true;
      this._isShowingDice = true;
      this._diceGameStarted = true;

      const value = Math.floor(Math.random() * 6) + 1;
      this._diceRound = (this._diceRound || 0) + 1;
      this.currentDiceRoll = { value, timestamp: Date.now(), round: this._diceRound };
      this._diceStartTime = Date.now();
      this._diceQuestionStartTime = Date.now();
      this._canSubmitDiceAnswer = true;
      this.diceAnswered = new Set();
      this._playerAnswers = new Map();
      this.diceHasWinner = false;
      this.diceWinner = null;

      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceRoll", {
        value, timestamp: Date.now(), answerTime: 20, canAnswerNow: true, round: this._diceRound
      }]);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "clik draw"]);

      for (const timeout of this._diceNotificationTimeouts) { clearTimeout(timeout); }
      this._diceNotificationTimeouts = [];
      this._diceNotificationTimeouts.push(setTimeout(() => {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "15s remaining"]);
      }, 5000));
      this._diceNotificationTimeouts.push(setTimeout(() => {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "10s remaining"]);
      }, 10000));
      this._diceNotificationTimeouts.push(setTimeout(() => {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "5s remaining"]);
      }, 15000));
      this._diceNotificationTimeouts.push(setTimeout(() => {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "3s remaining"]);
      }, 17000));

      this._diceTimeout = this._trackTimer(setTimeout(() => { this._endDiceRound(); }, 20000));
    } catch (e) {
      this._diceLock = false;
      this._isShowingDice = false;
      this._diceGameStarted = false;
    }
  }

  async _endDiceRound() {
    try {
      if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
      for (const timeout of this._diceNotificationTimeouts) { clearTimeout(timeout); }
      this._diceNotificationTimeouts = [];
      this._canSubmitDiceAnswer = false;
      this._isShowingDice = false;

      const diceValue = this.currentDiceRoll?.value;
      const roundNumber = this._diceRound || 1;
      const correctPlayers = [];
      for (const player of this.diceAnswered) {
        if (this._playerAnswers.get(player) === diceValue) correctPlayers.push(player);
      }

      if (correctPlayers.length === 0) {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNoWinner", {
          message: "No winner", value: diceValue, round: roundNumber
        }]);
      } else if (correctPlayers.length === 1) {
        const winner = correctPlayers[0];
        try {
          const points = await this.dataManager.addDicePoint(winner);
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
            username: winner, totalPoints: points[winner] || 0, diceValue: diceValue, round: roundNumber
          }]);
        } catch (e) {
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
            username: winner, totalPoints: 0, diceValue: diceValue, round: roundNumber
          }]);
        }
      } else if (correctPlayers.length > 1 && !this._tieActive) {
        this.currentDiceRoll = null;
        this._diceLock = false;
        this._isShowingDice = false;
        await this._startTieBreaker(CONSTANTS.DICE_ROOM, correctPlayers);
        this._diceGameStarted = false;
        return;
      }

      this.currentDiceRoll = null;
      this._diceLock = false;
      this._diceGameStarted = false;
      this._diceTimeUpCooldown = true;

      if (this._diceCooldownTimer) { clearTimeout(this._diceCooldownTimer); }
      this._diceCooldownTimer = setTimeout(() => {
        this._diceTimeUpCooldown = false;
        this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
        this._lastSentRemaining = -1;
        this._diceLoopCounter = 0;
        this._diceGameStarted = false;
        if (this._diceSessionActive && !this._diceSessionEnded) {
          const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
          if (clients && clients.size > 0) {
            if (!this.currentDiceRoll && !this._isShowingDice && !this._diceLock) {
              this._startDiceGameIfNotStarted();
            }
          }
        }
      }, 15000);
    } catch (e) {
      this._diceLock = false;
      this._isShowingDice = false;
      this._diceGameStarted = false;
    }
  }

  // ============================================================
  // TIE BREAKER
  // ============================================================

  async _startTieBreaker(room, players) {
    if (this._tieLock) return;
    this._tieLock = true;
    try {
      if (!players || players.length < 2 || this._tieActive) { this._tieLock = false; return; }
      this._tieActive = true;
      this._tieRound = 0;
      this._tiePlayers = [...players];
      this._tieAnswers = new Map();
      const id = `tie_${Date.now()}`;
      this._tieBreakers.set(id, { players, round: 0, winner: null, status: 'waiting' });
      await this._runTieRound(room, id, players);
    } finally {
      setTimeout(() => { this._tieLock = false; }, 2000);
    }
  }

  async _runTieRound(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);
    for (const timeout of this._tieNotificationTimeouts) clearTimeout(timeout);
    this._tieNotificationTimeouts = [];
    this._tieRound++;
    data.round = this._tieRound;
    data.status = 'running';
    data.players = players;
    this._tiePlayers = [...players];
    this._tieAnswers = new Map();
    this._diceQuestionStartTime = Date.now();
    this._canSubmitDiceAnswer = true;
    this.diceAnswered = new Set();
    this._playerAnswers = new Map();
    this._isShowingDice = true;
    this.diceHasWinner = false;
    this.diceWinner = null;

    this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification",
      `Tie Round ${this._tieRound}: ${players.join(', ')}`
    ]);

    const timeLimit = CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20;
    let isProcessed = false;
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "15s remaining"]);
    }, 5000));
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "10s remaining"]);
    }, 10000));
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "5s remaining"]);
    }, 15000));
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "3s remaining"]);
    }, 17000));

    this._tieTimer = this._trackTimer(setTimeout(() => {
      if (!isProcessed) {
        isProcessed = true;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "TIME UP"]);
        for (const timeout of this._tieNotificationTimeouts) clearTimeout(timeout);
        this._tieNotificationTimeouts = [];
        const tieId = this._getActiveTieBreakerId();
        if (tieId) {
          this._processTieResults(room, tieId, players);
        } else {
          this._resetTieBreakerState(null);
          this._startCooldownAfterTieBreaker();
        }
      }
    }, (timeLimit * 1000) + 2000));
  }

  async _processTieResults(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;

    const entries = [];
    let answeredCount = 0;
    let highest = 0;
    let highestPlayers = [];
    for (const player of players) {
      const answer = this._tieAnswers.get(player);
      if (answer !== undefined && answer >= 1 && answer <= 6) {
        answeredCount++;
        entries.push({ player, answer });
        if (answer > highest) { highest = answer; highestPlayers = [player]; }
        else if (answer === highest) { highestPlayers.push(player); }
      }
    }

    if (answeredCount === 0) {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification",
        `No one answered in Round ${this._tieRound} - Tie breaker ended`
      ]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }

    if (answeredCount === 1) {
      const winner = entries[0].player;
      const answer = entries[0].answer;

      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification",
        `${winner} answered with ${answer} - Auto win!`
      ]);

      try {
        const points = await this.dataManager.addDicePoint(winner);
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: points[winner] || 0,
          diceValue: answer,
          round: this._diceRound || 1,
          isTieBreaker: true,
          tieBreakerRound: this._tieRound,
          finalWinner: true,
          totalTieRounds: this._tieRound
        }]);
      } catch (e) {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: 0,
          diceValue: answer,
          round: this._diceRound || 1,
          isTieBreaker: true,
          tieBreakerRound: this._tieRound,
          finalWinner: true,
          totalTieRounds: this._tieRound
        }]);
      }
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }

    const allSame = entries.every(e => e.answer === entries[0].answer);
    if (allSame) {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification",
        `All answered same value: ${entries[0].answer} - Tie again!`
      ]);
      const allPlayers = entries.map(e => e.player);
      this._tiePlayers = allPlayers;
      this._tieAnswers = new Map();
      data.players = allPlayers;
      data.status = 'waiting';
      const nextTimer = setTimeout(async () => {
        if (this._tieActive && this._tiePlayers.length > 1) {
          await this._runTieRound(room, id, this._tiePlayers);
        } else if (this._tiePlayers.length === 1) {
          await this._processSingleWinner(room, id, this._tiePlayers[0]);
        } else {
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "No players remaining"]);
          this._resetTieBreakerState(id);
          this._startCooldownAfterTieBreaker();
        }
      }, 3000);
      this._trackTimer(nextTimer);
      return;
    }

    if (highestPlayers.length === 1) {
      const winner = highestPlayers[0];

      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification",
        `${winner} wins with highest value: ${highest}`
      ]);

      try {
        const points = await this.dataManager.addDicePoint(winner);
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: points[winner] || 0,
          diceValue: highest,
          round: this._diceRound || 1,
          isTieBreaker: true,
          tieBreakerRound: this._tieRound,
          finalWinner: true,
          totalTieRounds: this._tieRound
        }]);
      } catch (e) {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: 0,
          diceValue: highest,
          round: this._diceRound || 1,
          isTieBreaker: true,
          tieBreakerRound: this._tieRound,
          finalWinner: true,
          totalTieRounds: this._tieRound
        }]);
      }
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }

    if (highestPlayers.length > 1) {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification",
        `Tie again! Round ${this._tieRound + 1} between: ${highestPlayers.join(', ')}`
      ]);
      this._tiePlayers = highestPlayers;
      this._tieAnswers = new Map();
      data.players = highestPlayers;
      data.status = 'waiting';
      const nextTimer = setTimeout(async () => {
        if (this._tieActive && this._tiePlayers.length > 1) {
          await this._runTieRound(room, id, this._tiePlayers);
        } else if (this._tiePlayers.length === 1) {
          await this._processSingleWinner(room, id, this._tiePlayers[0]);
        } else {
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "No players remaining"]);
          this._resetTieBreakerState(id);
          this._startCooldownAfterTieBreaker();
        }
      }, 3000);
      this._trackTimer(nextTimer);
      return;
    }

    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  async _processSingleWinner(room, id, winner) {
    if (!winner) { this._resetTieBreakerState(id); this._startCooldownAfterTieBreaker(); return; }
    try {
      const points = await this.dataManager.addDicePoint(winner);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: points[winner] || 0,
        diceValue: 'auto',
        round: this._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: this._tieRound,
        finalWinner: true,
        totalTieRounds: this._tieRound
      }]);
    } catch (e) {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: 0,
        diceValue: 'auto',
        round: this._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: this._tieRound,
        finalWinner: true,
        totalTieRounds: this._tieRound
      }]);
    }
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  _startCooldownAfterTieBreaker() {
    this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "wait 15s"]);
    this._diceTimeUpCooldown = true;
    this._clearTimer(this._diceTimeUpCooldownTimer);
    this._diceTimeUpCooldownTimer = this._trackTimer(setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._tieActive = false;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieRound = 0;
      this._diceLoopCounter = 0;
      this._diceGameStarted = false;
      if (this._diceSessionActive && !this._diceSessionEnded) {
        const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
        if (clients && clients.size > 0) {
          if (!this.currentDiceRoll && !this._isShowingDice && !this._diceLock) {
            this._startDiceGameIfNotStarted();
          }
        }
      }
    }, CONSTANTS.TIE_BREAKER_COOLDOWN || 15000));
  }

  _resetTieBreakerState(id) {
    if (id) this._tieBreakers.delete(id);
    this._tieActive = false;
    this._tiePlayers = [];
    this._tieAnswers = new Map();
    this._tieRound = 0;
    this._canSubmitDiceAnswer = false;
    this._isShowingDice = false;
    this.currentDiceRoll = null;
    this.diceAnswered = new Set();
    this._playerAnswers = new Map();
    this.diceHasWinner = false;
    this.diceWinner = null;
    if (this._tieTimer) { this._clearTimer(this._tieTimer); this._tieTimer = null; }
    if (this._tieInterval) { this._clearTimer(this._tieInterval); this._tieInterval = null; }
    for (const timeout of this._tieNotificationTimeouts) clearTimeout(timeout);
    this._tieNotificationTimeouts = [];
  }

  _getActiveTieBreakerId() {
    for (const [id, data] of this._tieBreakers) {
      if (data.status === 'waiting' || data.status === 'running') return id;
    }
    return null;
  }

  // ============================================================
  // SUBMIT DICE ANSWER
  // ============================================================

  async submitDiceAnswer(ws, username, guess) {
    try {
      if (!ws || !username) return;
      if (!this._canSubmitDiceAnswer) { this._safeSend(ws, ["diceError", "Round ended"]); return; }
      if (this.diceAnswered.has(username)) { this._safeSend(ws, ["diceError", "Already answered"]); return; }
      const guessValue = parseInt(guess, 10);
      if (isNaN(guessValue) || guessValue < 1 || guessValue > 6) {
        this._safeSend(ws, ["diceError", "invalid guess 1-6"]);
        return;
      }
      if (this._tieActive) {
        if (!this._tiePlayers.includes(username)) {
          this._safeSend(ws, ["diceError", "You are not in tie breaker"]);
          return;
        }
        if (this._tieAnswers.has(username)) {
          this._safeSend(ws, ["diceError", "You already answered"]);
          return;
        }
        this._tieAnswers.set(username, guessValue);
        this.diceAnswered.add(username);
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceAnswer", {
          username, guess: guessValue, isTieBreaker: true, tieRound: this._tieRound
        }]);
        if (this._tieAnswers.size === this._tiePlayers.length) {
          this._canSubmitDiceAnswer = false;
          this._isShowingDice = false;
          if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
          if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
          const tieId = this._getActiveTieBreakerId();
          if (tieId) {
            setTimeout(async () => {
              await this._processTieResults(CONSTANTS.DICE_ROOM, tieId, this._tiePlayers);
            }, 500);
          } else {
            this._resetTieBreakerState(null);
            this._startCooldownAfterTieBreaker();
          }
        }
        return;
      }
      if (!this.currentDiceRoll) { this._safeSend(ws, ["diceError", "No active round"]); return; }
      const diceValue = this.currentDiceRoll.value;
      this._playerAnswers.set(username, guessValue);
      this.diceAnswered.add(username);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceAnswer", {
        username, guess: guessValue, round: this._diceRound || 1
      }]);
      if (guessValue === diceValue && !this.diceHasWinner) {
        this.diceHasWinner = true;
        this.diceWinner = username;
      }
    } catch (e) {}
  }

  // ============================================================
  // RECORDING HELPERS
  // ============================================================

  async _broadcastLowCardWinners(room) {
    try {
      if (!room || typeof room !== 'string' || room.trim() === '') return;
      const roomKey = room.trim();
      const isRecording = await this.dataManager.getRecordingStatus(roomKey);
      if (!isRecording) return;
      const winners = await this.dataManager.getWinners(roomKey);
      const now = Date.now();
      const key = `broadcast_${roomKey}`;
      if (!this._lastNotifTime) this._lastNotifTime = {};
      if (this._lastNotifTime[key] && (now - this._lastNotifTime[key]) < 500) return;
      this._lastNotifTime[key] = now;
      this._broadcastToRoom(roomKey, ["lowCardWinnerUpdate", {
        winners: winners || {}, room: roomKey, recording: true
      }]);
    } catch (e) {}
  }

  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username || room === CONSTANTS.DICE_ROOM) return false;
      const isRecording = await this.dataManager.getRecordingStatus(room);
      if (!isRecording) return false;
      return await this.dataManager.addWinner(room, username);
    } catch (e) { return false; }
  }

  // ============================================================
  // TIMER MANAGEMENT
  // ============================================================

  _trackTimer(timer) { if (timer) this._allTimers.add(timer); return timer; }

  _clearTimer(timer) {
    if (timer) {
      if (typeof timer === 'object' && timer._destroyed) return;
      try { clearTimeout(timer); } catch (e) {}
      try { clearInterval(timer); } catch (e) {}
      this._allTimers.delete(timer);
    }
  }

  _cleanupGameTimers(game) {
    if (!game) return;
    if (game._registrationTimer) { this._clearTimer(game._registrationTimer); game._registrationTimer = null; }
    if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
    if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
    if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
    if (game._notificationTimers) {
      for (const timer of game._notificationTimers) this._clearTimer(timer);
      game._notificationTimers = [];
    }
    if (game._drawNotificationTimers) {
      for (const timer of game._drawNotificationTimers) this._clearTimer(timer);
      game._drawNotificationTimers = [];
    }
    if (game._botTimeouts) {
      for (const timeout of game._botTimeouts) this._clearTimer(timeout);
      game._botTimeouts.clear();
    }
    if (game._botTimers) {
      for (const timer of game._botTimers) this._clearTimer(timer);
      game._botTimers.clear();
    }
    game._isEvaluating = false;
    game._evalStartTime = null;
    game.evaluationLocked = false;
    game.drawTimeExpired = false;
    game.registrationOpen = false;
  }

  _clearAllGameTimers(game) { this._cleanupGameTimers(game); }

  // ============================================================
  // LOCK HELPERS
  // ============================================================

  _acquireLock(lockMap, key, timeoutMs = 5000) {
    if (lockMap.has(key)) return false;
    lockMap.set(key, Date.now());
    setTimeout(() => { if (lockMap.has(key)) lockMap.delete(key); }, timeoutMs);
    return true;
  }

  _releaseLock(lockMap, key) {
    if (lockMap.has(key)) { lockMap.delete(key); return true; }
    return false;
  }

  // ============================================================
  // UTILITY
  // ============================================================

  _handleError(type, error) {
    try {
      const now = Date.now();
      if (now - this._lastErrorReset > CONSTANTS.ERROR_RESET_INTERVAL_MS) {
        this._errorCount = 0;
        this._lastErrorReset = now;
      }
      this._errorCount++;
      if (this._errorCount > 20) { this._circuitOpen = true; this._lastResetTime = now; }
    } catch (e) {}
  }

  _getTimeLeftUntilNextDice() {
    try {
      const witaTime = this._getCurrentWITATime();
      const currentTotal = witaTime.totalMinutes;
      let minDiff = Infinity;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) minDiff = diff;
      }
      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      const isRunning = this.alarmScheduler.isDiceTime();
      return { hours, minutes, totalMs: minDiff * 60 * 1000, text: hours + "h " + minutes + "m", isRunning };
    } catch (e) {
      return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false };
    }
  }

  _getCurrentWITATime() {
    try {
      const now = new Date();
      const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
      const minutes = now.getUTCMinutes();
      return { hours, minutes, totalMinutes: (hours * 60) + minutes };
    } catch (e) { return { hours: 0, minutes: 0, totalMinutes: 0 }; }
  }

  // ============================================================
  // FORCE CLEANUP
  // ============================================================

  async _forceCleanupGame(room, game) {
    try {
      if (!game) return;
      game._gameEnded = true;
      game._isActive = false;
      this._cleanupGameTimers(game);
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);

      game.players = null;
      game.botPlayers = null;
      game.numbers = null;
      game.tanda = null;
      game.eliminated = null;
      game.playerWsId = null;
      game._isActive = false;
      game._gameEnded = true;
      game._isEvaluating = false;
      game._evalStartTime = null;
      game.registrationOpen = false;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      game._phase = null;
      game._state = null;
      game.round = 0;
      game.betAmount = 0;
      game.hostId = null;
      game.hostName = null;
      game.useBots = false;
      game._botsAdded = false;
      game._createdAt = null;
      game._drawPhaseStart = null;
      game._endTime = Date.now();
      game._startedByRecording = false;
      game._startedBy = null;
      game._notificationTimers = [];
      game._drawNotificationTimers = [];
      game._botTimeouts = null;
      game._botTimers = null;
      game._cleanupStarted = false;
      game._evalLocks = null;
      game._gameId = null;
      game._version = 0;

      this.activeGames.delete(room);
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this._evaluationLocks.delete(`eval_${room}`);
      this._drawLocks.delete(`draw_${room}`);
      this._gameOperationLocks.delete(`startDraw_${room}`);
      this._submitLocks.delete(`submit_${room}`);
      this._checkLocks.delete(`check_${room}`);
      this._closeLocks.delete(`close_${room}`);

      await this._saveGameState();
    } catch (e) {}
  }

  // ============================================================
  // DESTROY
  // ============================================================

  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;

      for (const timer of this._allTimers) { try { clearTimeout(timer); } catch (e) {} }
      this._allTimers.clear();

      if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
      if (this._diceCooldownTimer) { clearTimeout(this._diceCooldownTimer); this._diceCooldownTimer = null; }
      if (this._diceTimeUpCooldownTimer) { clearTimeout(this._diceTimeUpCooldownTimer); this._diceTimeUpCooldownTimer = null; }
      for (const timeout of this._diceNotificationTimeouts) clearTimeout(timeout);
      this._diceNotificationTimeouts = [];

      if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
      if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
      for (const timeout of this._tieNotificationTimeouts) clearTimeout(timeout);
      this._tieNotificationTimeouts = [];

      for (const [room, game] of this.activeGames) {
        if (game._isActive && !game._gameEnded) {
          game._gameEnded = true;
          game._isActive = false;
          this._broadcastToRoom(room, ["gameLowCardEnd", ["Server shutting down"]]);
        }
        this._cleanupGameTimers(game);
        this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();

      this._eventQueue = [];
      this._processingQueue = false;
      this.userConnections.clear();
      this._tieBreakers.clear();
      this._reconnectAttempts.clear();
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._switchLocks.clear();
      this._switchRetries.clear();
      this._submitLocks.clear();
      this._checkLocks.clear();
      this._closeLocks.clear();

      if (this.alarmScheduler) await this.alarmScheduler.cleanup();

      for (const [, ws] of this.wsMap) {
        try {
          if (ws && ws.readyState === 1) {
            if (typeof ws.removeAllListeners === 'function') ws.removeAllListeners();
            ws.close(1000, "Server shutting down");
          }
        } catch (e) {}
      }
      this.wsMap.clear();
      this.wsClients.clear();
      this.clientRooms.clear();
      this._wsMeta.clear();

      try { await this.ctx.storage.deleteAlarm(); } catch (e) {}

      await this._saveGameState();
    } catch (e) {}
  }
}

export default GameServer;
