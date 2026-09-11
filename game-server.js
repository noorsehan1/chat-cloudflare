// ============================================================
// GAME-SERVER.JS
// VERSION: 16.9.2 - CEK WS MATI SAAT RESTORE (SILENT)
// ============================================================

const CONSTANTS = {
  MAX_LOWCARD_GAMES: 10,
  REGISTRATION_TIME_MS: 20000,
  DRAW_TIME_MS: 20000,
  EVALUATION_DELAY_MS: 2000,
  MAX_BOTS_PER_GAME: 4,
  MAX_BET: 100000,
  MAX_BOT_DRAWS_PER_ROUND: 4,
  EVALUATION_TIMEOUT_MS: 30000,
  MAX_PLAYERS_PER_GAME: 45,
  GAME_CLEANUP_DELAY_MS: 5000,
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
  MAX_PENDING_EVENTS: 100,
  CACHE_LOAD_TIMEOUT: 5000,
  MAX_RESTORE_ATTEMPTS: 3,
  RESTORE_RETRY_DELAY_MS: 2000,
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
const _wsCleanupState = new WeakMap();

function parseTime(timeStr) {
  const [hours, minutes] = timeStr.split(':').map(Number);
  return hours * 60 + minutes;
}

// ============================================================
// D1 DATA MANAGER
// ============================================================

class DataManager {
  constructor(db) { this.db = db; }

  async init() {
    try {
      await this.db.prepare(`CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)`).run();
      return true;
    } catch(e) { return false; }
  }

  async _get(key) {
    try {
      const result = await this.db.prepare(`SELECT value FROM ${TABLE_NAME} WHERE key = ?`).bind(key).first();
      if (!result || !result.value) return null;
      try { return JSON.parse(result.value); } catch(e) { return null; }
    } catch(e) { return null; }
  }

  async _set(key, value) {
    try {
      if (value === null || value === undefined ||
          (typeof value === 'object' && Object.keys(value).length === 0) ||
          (Array.isArray(value) && value.length === 0)) {
        await this.db.prepare(`DELETE FROM ${TABLE_NAME} WHERE key = ?`).bind(key).run();
        return true;
      }
      await this.db.prepare(`INSERT OR REPLACE INTO ${TABLE_NAME} (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)`).bind(key, JSON.stringify(value)).run();
      return true;
    } catch(e) { return false; }
  }

  async getRecordingStatus(room) {
    const map = await this._get('recordingStatusMap') || {};
    return map[room] === true;
  }

  async setRecordingStatus(room, enabled) {
    const map = await this._get('recordingStatusMap') || {};
    if (enabled) map[room] = true; else delete map[room];
    await this._set('recordingStatusMap', map);
    return true;
  }

  async getWinners(room) {
    const map = await this._get('winnersMap') || {};
    return map[room] || {};
  }

  async addWinner(room, username) {
    const map = await this._get('winnersMap') || {};
    const winners = map[room] || {};
    let count = parseInt(String(winners[username] || "0").replace("x", "")) || 0;
    winners[username] = (count + 1) + "x";
    map[room] = winners;
    await this._set('winnersMap', map);
    return true;
  }

  async deleteAllWinners(room) {
    const map = await this._get('winnersMap') || {};
    delete map[room];
    await this._set('winnersMap', map);
    return true;
  }

  async getDicePoints() { return await this._get('dicePoints') || {}; }

  async setDicePoints(points) {
    const cleanPoints = {};
    for (const [username, score] of Object.entries(points || {})) {
      if (username && typeof username === 'string') {
        const s = typeof score === 'number' ? score : parseInt(score, 10) || 0;
        if (s > 0) cleanPoints[username] = s;
      }
    }
    await this._set('dicePoints', cleanPoints);
    return true;
  }

  async addDicePoint(username) {
    const points = await this.getDicePoints();
    points[username] = (points[username] || 0) + 1;
    await this.setDicePoints(points);
    return points;
  }

  async resetDicePoints() { await this._set('dicePoints', {}); return true; }

  async getLeaderboard(limit = 10) {
    const points = await this.getDicePoints();
    if (!points || Object.keys(points).length === 0) return [];
    const safeLimit = Math.min(Math.max(limit, CONSTANTS.MIN_LEADERBOARD_LIMIT), CONSTANTS.MAX_LEADERBOARD_LIMIT);
    return Object.entries(points)
      .filter(([u, s]) => u && s > 0)
      .sort((a, b) => b[1] - a[1])
      .slice(0, safeLimit)
      .map(([u, s]) => `${u}|${s}`);
  }

  async getLastWeekWinner() { return await this._get('lastWeekWinner'); }
  async setLastWeekWinner(d) { await this._set('lastWeekWinner', d); return true; }
  async deleteLastWeekWinner() { await this._set('lastWeekWinner', null); return true; }
  async getLastResetWeek() { return await this._get('lastResetWeek'); }
  async setLastResetWeek(w) { await this._set('lastResetWeek', w); return true; }
  async getAlarms() { return await this._get('scheduled_alarms') || {}; }
  async setAlarms(a) { await this._set('scheduled_alarms', a); return true; }
  async deleteAlarms() { await this._set('scheduled_alarms', {}); return true; }

  async getAllData() {
    try {
      const result = await this.db.prepare(`SELECT key, value FROM ${TABLE_NAME}`).all();
      const data = {};
      for (const row of (result.results || [])) {
        try { data[row.key] = JSON.parse(row.value); } catch(e) { data[row.key] = row.value; }
      }
      return data;
    } catch(e) { return {}; }
  }

  getCurrentWeek() {
    const now = new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }
}

// ============================================================
// ALARM SCHEDULER
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
          currentSession = { ...session, startTotal, endTotal };
          break;
        }
      }

      if (currentSession) {
        const endDelay = (currentSession.endTotal - currentTotal) * 60 * 1000;
        if (endDelay > 0) await this._scheduleAlarm('dice_session_end', endDelay);
        await this._scheduleAlarm('dice_session_start_immediate', 1000);
        return true;
      }

      let nextSession = null, minDiff = Infinity;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) { minDiff = diff; nextSession = { ...session, startTotal, endTotal: parseTime(session.end) }; }
      }

      if (nextSession) {
        let startDelay = minDiff * 60 * 1000;
        if (startDelay < 0) startDelay = 0;
        await this._scheduleAlarm('dice_session_start', startDelay);
        const dur = (nextSession.endTotal - nextSession.startTotal) * 60 * 1000;
        if (dur > 0) await this._scheduleAlarm('dice_session_end', startDelay + dur);
      }
      return true;
    } catch(e) { return false; }
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
    } catch(e) { return false; }
  }

  async _scheduleAlarm(name, delayMs) {
    try {
      if (delayMs < 1000) delayMs = 1000;
      const alarm = { name, scheduledAt: Date.now() + delayMs, delayMs, timestamp: Date.now() };
      this._alarms.set(name, alarm);
      await this.dataManager.setAlarms(Object.fromEntries(this._alarms));
      await this._scheduleNearestAlarm();
      return true;
    } catch(e) { return false; }
  }

  async _scheduleNearestAlarm() {
    try {
      let nearestTime = Infinity;
      for (const [name, alarm] of this._alarms) {
        const time = alarm.scheduledAt || alarm.timestamp + alarm.delayMs;
        if (time < nearestTime && time > Date.now()) nearestTime = time;
      }
      if (nearestTime < Infinity) {
        const delay = nearestTime - Date.now();
        if (delay > 0) await this.ctx.storage.setAlarm(Date.now() + delay);
      }
    } catch(e) {}
  }

  async _clearAllAlarms() {
    try {
      this._alarms.clear();
      await this.dataManager.deleteAlarms();
      await this.ctx.storage.deleteAlarm();
    } catch(e) {}
  }

  async getPendingAlarms() {
    try {
      await this.restoreAlarms();
      const pending = [], expired = [];
      const now = Date.now();
      for (const [name, alarm] of this._alarms) {
        const t = alarm.scheduledAt || alarm.timestamp + alarm.delayMs;
        if (t <= now) { pending.push({ ...alarm, name }); expired.push(name); }
      }
      for (const name of expired) this._alarms.delete(name);
      if (expired.length > 0) await this.dataManager.setAlarms(Object.fromEntries(this._alarms));
      return pending;
    } catch(e) { return []; }
  }

  async processAlarm(name) {
    try {
      const alarm = this._alarms.get(name);
      if (!alarm) {
        await this.restoreAlarms();
        if (!this._alarms.get(name)) return null;
      }
      this._alarms.delete(name);
      await this.dataManager.setAlarms(Object.fromEntries(this._alarms));
      await this._scheduleNearestAlarm();
      return alarm || this._alarms.get(name);
    } catch(e) { return null; }
  }

  async restoreAlarms() {
    try {
      const stored = await this.dataManager.getAlarms();
      if (stored && typeof stored === 'object') {
        this._alarms.clear();
        for (const [name, data] of Object.entries(stored)) this._alarms.set(name, data);
        await this._scheduleNearestAlarm();
        return true;
      }
      return false;
    } catch(e) { return false; }
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
      if (currentTotal >= parseTime(session.start) && currentTotal < parseTime(session.end)) return true;
    }
    return false;
  }

  _getTimeLeftUntilNextDice() {
    try {
      const witaTime = this._toWITA(new Date());
      const currentTotal = witaTime.getHours() * 60 + witaTime.getMinutes();
      let minDiff = Infinity, nextSession = null;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) { minDiff = diff; nextSession = session; }
      }
      if (minDiff === Infinity) return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false, nextSession: null };
      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      return { hours, minutes, totalMs: minDiff * 60 * 1000, text: hours + "h " + minutes + "m", isRunning: this.isDiceTime(), nextSession };
    } catch(e) { return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false, nextSession: null }; }
  }

  async cleanup() { await this._clearAllAlarms(); }
}

// ============================================================
// GAME SERVER
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
      this._restoreDone = false;
      this._restoreFailed = false;
      this._restorePromise = null;
      this._restoreAttempts = 0;
      this._isRestoring = false;
      this._initialized = false;

      this.wsSet = new Set();
      this.userConnections = new Map();
      this.roomClients = new Map();

      this._pendingEvents = [];
      this._eventQueue = [];
      this._processingQueue = false;
      this._allTimers = new Set();

      this.activeGames = new Map();

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
      this._lastNotifTime = {};
      this._lastWinnerRequestTime = new Map();
      this._cleanupTimers = new Map();
      this._gameLocks = new Map();
      this._joinLocks = new Map();

      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._lastRequestDecay = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();

      this._diceLoopCounter = 0;
      this._maxDiceLoops = 10;
      this.DICE_ROOM = CONSTANTS.DICE_ROOM;

      this.db = env.DB;
      this.dataManager = new DataManager(this.db);
      this.alarmScheduler = new AlarmScheduler(this.db, this.ctx);
      this.alarmScheduler.ctx = this;

      this._restorePromise = this._restoreWithRetry();

      const restoreTimeout = setTimeout(() => {
        if (!this._restoreDone) {
          console.log('[RESTORE] Timeout — forcing partial restore');
          this._restored = true;
          this._restoreDone = true;
          this._restoreFailed = true;
          this._isRestoring = false;
          this._initialized = true;
          if (!this.closing && !this.isDestroyed) {
            try { this._processPendingEvents(); } catch(e) {}
          }
        }
      }, CONSTANTS.CACHE_LOAD_TIMEOUT || 5000);

      this._restorePromise
        .then(() => {
          clearTimeout(restoreTimeout);
          console.log('[RESTORE] Success');
        })
        .catch((e) => {
          clearTimeout(restoreTimeout);
          console.error('[RESTORE] Failed:', e);
        });

    } catch(e) {
      console.error('[CONSTRUCTOR] Fatal:', e);
      this._restored = true;
      this._restoreDone = true;
      this._restoreFailed = true;
      this._isRestoring = false;
      this._initialized = true;
    }
  }

  // ============================================================
  // RESTORE
  // ============================================================

  async _restoreWithRetry() {
    let attempts = 0;
    let lastError = null;
    while (attempts < CONSTANTS.MAX_RESTORE_ATTEMPTS) {
      try {
        attempts++;
        console.log(`[RESTORE] Attempt ${attempts}/${CONSTANTS.MAX_RESTORE_ATTEMPTS}`);
        const result = await this._restoreAllState();
        this._restoreAttempts = attempts;
        return result;
      } catch(e) {
        lastError = e;
        this._restoreAttempts = attempts;
        console.error(`[RESTORE] Attempt ${attempts} failed:`, e);
        if (attempts < CONSTANTS.MAX_RESTORE_ATTEMPTS) {
          await new Promise(resolve => setTimeout(resolve, CONSTANTS.RESTORE_RETRY_DELAY_MS));
          this._isRestoring = true;
        }
      }
    }
    this._restored = true;
    this._restoreDone = true;
    this._restoreFailed = true;
    this._isRestoring = false;
    this._initialized = true;
    throw lastError;
  }

  async _restoreAllState() {
    try {
      this._isRestoring = true;

      try { await this.dataManager.init(); } catch(e) {}

      try {
        await this.alarmScheduler.restoreAlarms();
        await this.alarmScheduler.scheduleAlarms();
      } catch(e) {}

      try { await this._checkAndForceResetIfMondayUTC(); } catch(e) {}

      try {
        const webSockets = this.ctx.getWebSockets();
        console.log(`[RESTORE] Found ${webSockets.length} WebSockets from ctx`);

        this.wsSet.clear();
        this.roomClients.clear();
        this.userConnections.clear();

        let restoredCount = 0;
        let deadCount = 0;

        for (const ws of webSockets) {
          try {
            // 🔥 CEK WS MATI — kalau mati, tandai cleanup done & skip
            if (!ws || ws.readyState !== 1) {
              deadCount++;
              try {
                const st = _wsCleanupState.get(ws);
                if (st) {
                  st.cleanupDone = true;
                  st.cleaning = false;
                } else {
                  _wsCleanupState.set(ws, { cleanupDone: true, cleaning: false, cleanupStart: null });
                }
              } catch(e) {}
              try { this.wsSet.delete(ws); } catch(e) {}
              continue;
            }

            await this._restoreSingleWebSocket(ws);
            restoredCount++;
          } catch(e) {
            console.error('[RESTORE] WS restore failed:', e);
          }
        }

        console.log(`[RESTORE] Restored ${restoredCount} live WS, skipped ${deadCount} dead WS`);
        for (const [room, clients] of this.roomClients) {
          console.log(`[RESTORE]   Room "${room}": ${clients.size} clients`);
        }
      } catch(e) {
        console.error('[RESTORE] WS scan failed:', e);
      }

      this._restored = true;
      this._restoreDone = true;
      this._restoreFailed = false;
      this._isRestoring = false;
      this._initialized = true;

      try {
        const isDiceTime = this.alarmScheduler.isDiceTime();
        if (isDiceTime) {
          this._diceSessionActive = true;
          this._diceSessionEnded = false;
          this._diceGameStarted = false;
          const clients = this.roomClients?.get(CONSTANTS.DICE_ROOM);
          if (clients && clients.size > 0) this._startDiceGameIfNotStarted();
        } else {
          this._diceSessionActive = false;
          this._diceSessionEnded = true;
          this._diceGameStarted = false;
        }
      } catch(e) {}

      await this._processPendingEvents();
      return true;
    } catch(e) {
      this._restored = true;
      this._restoreDone = true;
      this._restoreFailed = true;
      this._isRestoring = false;
      this._initialized = true;
      await this._processPendingEvents();
      throw e;
    }
  }

  async _restoreSingleWebSocket(ws) {
    try {
      if (!ws || ws.readyState !== 1) return;

      let attachment = null;
      try { attachment = ws.deserializeAttachment(); } catch(e) {}

      const room = attachment?.room || ws.room || ws.roomname || ws._room;
      const username = attachment?.username || ws.username || ws._username;
      const wsId = attachment?.wsId || ws._wsId || (++this._wsIdCounter);

      if (!room) {
        console.log('[RESTORE] WS without room, skipping');
        return;
      }

      ws.username = username;
      ws._username = username;
      ws.room = room;
      ws.roomname = room;
      ws._room = room;
      ws._wsId = wsId;
      ws._closing = false;
      ws._cleaning = false;
      ws._createdAt = attachment?.createdAt || ws._createdAt || Date.now();
      if (ws._nextSessionSent === undefined) ws._nextSessionSent = false;

      if (!this.wsSet.has(ws)) this.wsSet.add(ws);
      if (!this.roomClients.has(room)) this.roomClients.set(room, new Set());
      this.roomClients.get(room).add(ws);

      if (username) {
        let conns = this.userConnections.get(username);
        if (!conns) { conns = new Set(); this.userConnections.set(username, conns); }
        conns.add(ws);
      }

      const state = _wsCleanupState.get(ws);
      if (state) {
        state.cleanupDone = false;
        state.cleaning = false;
        state.cleanupStart = null;
      } else {
        _wsCleanupState.set(ws, { cleanupDone: false, cleaning: false, cleanupStart: null });
      }

      if (room === CONSTANTS.DICE_ROOM) {
        try { this._sendDiceRoomState(ws); } catch(e) {}
      }
    } catch(e) {
      console.error('[RESTORE] _restoreSingleWebSocket error:', e);
    }
  }

  // ============================================================
  // PENDING EVENTS
  // ============================================================

  async _processPendingEvents() {
    try {
      if (!this._pendingEvents || this._pendingEvents.length === 0) return;
      const events = [...this._pendingEvents];
      this._pendingEvents = [];
      console.log(`[PENDING] Processing ${events.length} events`);

      for (const evt of events) {
        let ws = evt.ws;

        if (!ws || ws.readyState !== 1) {
          const wsId = evt.wsId;
          if (wsId) {
            try {
              const allWs = this.ctx.getWebSockets();
              for (const w of allWs) {
                if (w._wsId === wsId && w.readyState === 1) { ws = w; break; }
              }
            } catch(e) {}
            if (!ws) {
              for (const w of this.wsSet) {
                if (w._wsId === wsId && w.readyState === 1) { ws = w; break; }
              }
            }
          }
        }

        if (!ws || ws.readyState !== 1 || ws._closing) continue;

        try {
          if (evt.attachment && !ws.room) {
            try {
              ws.serializeAttachment(evt.attachment);
              ws._wsId = evt.attachment.wsId;
              ws.username = evt.attachment.username;
              ws._username = evt.attachment.username;
              ws.room = evt.attachment.room;
              ws.roomname = evt.attachment.room;
              ws._room = evt.attachment.room;

              if (evt.attachment.room) {
                if (!this.roomClients.has(evt.attachment.room)) this.roomClients.set(evt.attachment.room, new Set());
                this.roomClients.get(evt.attachment.room).add(ws);
                if (!this.wsSet.has(ws)) this.wsSet.add(ws);
              }
            } catch(e) {}
          }

          const data = JSON.parse(evt.message);
          if (Array.isArray(data) && data.length > 0) {
            await this._processWithTimeout(ws, data);
          }
        } catch(e) {
          console.error('[PENDING] Process failed:', e);
        }
      }
    } catch(e) {}
  }

  async _processWithTimeout(ws, data, timeoutMs = 500) {
    try {
      const timeoutPromise = new Promise((_, reject) => {
        const timer = setTimeout(() => reject(new Error('Processing timeout')), timeoutMs);
        this._trackTimer(timer);
      });
      await Promise.race([this.handleEvent(ws, data), timeoutPromise]);
    } catch(e) {}
  }

  // ============================================================
  // EVENT HANDLING
  // ============================================================

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      if (this._eventQueue.length > CONSTANTS.MAX_EVENT_QUEUE_SIZE) {
        this.safeSend(ws, ["gameLowCardError", "Server busy"]);
        return;
      }
      this._eventQueue.push({ ws, data });
      if (!this._processingQueue) this._processEventQueue();
    } catch(e) {}
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
        try { await this._processEventItem(item.ws, item.data); } catch(e) {}
        processed++;
      }
      if (this._eventQueue.length > 0 && !this.closing && !this.isDestroyed) {
        setTimeout(() => this._processEventQueue(), 10);
      }
    } catch(e) {
      this._handleError('processQueue', e);
    } finally {
      this._processingQueue = false;
    }
  }

  async _processEventItem(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;

      let room = ws.room || ws.roomname || ws._room;
      let username = ws.username || ws._username;
      let wsId = ws._wsId;

      if (!room || !username) {
        try {
          const att = ws.deserializeAttachment?.();
          if (att) {
            if (!room && att.room) room = att.room;
            if (!username && att.username) username = att.username;
            if (!wsId && att.wsId) wsId = att.wsId;
          }
        } catch(e) {}
      }

      if (room) {
        ws.room = room;
        ws.roomname = room;
        ws._room = room;
      }
      if (username) {
        ws.username = username;
        ws._username = username;
      }
      if (wsId) ws._wsId = wsId;

      if (room) {
        if (!this.roomClients.has(room)) this.roomClients.set(room, new Set());
        this.roomClients.get(room).add(ws);
        if (!this.wsSet.has(ws)) this.wsSet.add(ws);
        if (username) {
          let conns = this.userConnections.get(username);
          if (!conns) { conns = new Set(); this.userConnections.set(username, conns); }
          conns.add(ws);
        }
      }

      await this._handleEventInternal(ws, data);
    } catch(e) {}
  }

  safeSend(ws, msg) {
    try {
      if (!ws) return false;
      if (ws.readyState !== 1 || ws._closing || ws._cleaning || this.closing || this.isDestroyed) return false;
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) { return false; }
  }

  // ============================================================
  // BROADCAST
  // ============================================================

  broadcast(room, msg) {
    try {
      if (this.closing || this.isDestroyed || !room || !msg) return 0;

      let clients = this.roomClients?.get(room);
      let needScan = !clients || clients.size === 0;

      if (needScan) {
        clients = new Set();
        let allWs = [];
        try { allWs = this.ctx?.getWebSockets?.() || []; } catch(e) {}

        for (const ws of allWs) {
          try {
            if (!ws || ws.readyState !== 1) continue;
            if (ws._closing || ws._cleaning) continue;

            let wsRoom = ws.room || ws.roomname || ws._room;
            if (!wsRoom) {
              try {
                const att = ws.deserializeAttachment?.();
                if (att?.room) {
                  wsRoom = att.room;
                  ws.room = att.room;
                  ws.roomname = att.room;
                  ws._room = att.room;
                  if (att.username) { ws.username = att.username; ws._username = att.username; }
                  if (att.wsId) ws._wsId = att.wsId;
                  if (!ws._createdAt) ws._createdAt = att.createdAt || Date.now();
                }
              } catch(e) {}
            }

            if (wsRoom !== room) continue;

            clients.add(ws);
            if (!this.wsSet.has(ws)) this.wsSet.add(ws);
            if (ws.username) {
              let conns = this.userConnections.get(ws.username);
              if (!conns) { conns = new Set(); this.userConnections.set(ws.username, conns); }
              conns.add(ws);
            }
          } catch(e) {}
        }

        if (clients.size > 0) {
          try { this.roomClients.set(room, clients); } catch(e) {}
        }
      }

      if (!clients || clients.size === 0) return 0;

      const msgStr = JSON.stringify(msg);
      const toRemove = new Set();
      let sentCount = 0;

      for (const ws of clients) {
        if (!ws) { toRemove.add(ws); continue; }

        const state = _wsCleanupState.get(ws);
        if (state && state.cleanupDone) { toRemove.add(ws); continue; }

        let wsRoom = ws.room || ws.roomname || ws._room;
        if (!wsRoom) {
          try {
            const att = ws.deserializeAttachment?.();
            if (att?.room) {
              wsRoom = att.room;
              ws.room = att.room;
              ws.roomname = att.room;
              ws._room = att.room;
            }
          } catch(e) {}
        }

        if (wsRoom !== room) { toRemove.add(ws); continue; }

        try {
          if (ws.readyState === 1 && !ws._closing && !ws._cleaning) {
            ws.send(msgStr);
            sentCount++;
          } else {
            toRemove.add(ws);
          }
        } catch(e) { toRemove.add(ws); }
      }

      if (toRemove.size > 0) {
        for (const ws of toRemove) { try { clients.delete(ws); } catch(e) {} }
      }

      return sentCount;
    } catch(e) { return 0; }
  }

  // ============================================================
  // FETCH
  // ============================================================

  async fetch(req) {
    try {
      if (this.closing || this.isDestroyed) return new Response("Shutting down", { status: 503 });

      if (!this._restoreDone) {
        try {
          await Promise.race([
            this._restorePromise,
            new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
          ]);
        } catch(e) {
          console.log('[FETCH] Restore timeout — forcing restore');
          try { await this._restoreAllState(); } catch(e2) {}
        }
      }

      if (this._circuitOpen) {
        const now = Date.now();
        if (now - this._lastResetTime > 60000) {
          this._circuitOpen = false;
          this._requestCount = 0;
          this._lastResetTime = now;
          this._lastRequestDecay = now;
        } else {
          return new Response("Service temporarily unavailable", { status: 503, headers: { 'Retry-After': '30', 'Content-Type': 'text/plain' } });
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
      if (this._requestCount > CONSTANTS.RATE_LIMIT_MAX) {
        this._circuitOpen = true;
        this._lastResetTime = now;
        return new Response("Rate limit exceeded", { status: 429, headers: { 'Retry-After': '60', 'Content-Type': 'text/plain' } });
      }

      const url = new URL(req.url);
      if (url.pathname === "/game/ws") {
        const upgrade = req.headers.get("Upgrade");
        if (upgrade !== "websocket") return new Response("WebSocket only", { status: 400 });
        if (this.wsSet.size >= CONSTANTS.MAX_WS_CLIENTS) return new Response("Server full", { status: 503 });
        if (this._eventQueue?.length > 500) return new Response("Server busy", { status: 503 });

        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        const wsId = ++this._wsIdCounter;

        try {
          this.ctx.acceptWebSocket(server);
        } catch(e) {
          try { server.close(1008, "Accept failed"); } catch(err) {}
          return new Response("WebSocket acceptance failed", { status: 500 });
        }

        server.serializeAttachment({ wsId, username: null, room: null, roomname: null, createdAt: Date.now() });
        server._wsId = wsId;
        server._closing = false;
        server._cleaning = false;
        server.username = null;
        server._username = null;
        server.room = null;
        server.roomname = null;
        server._room = null;
        server._createdAt = Date.now();
        server._nextSessionSent = false;

        _wsCleanupState.set(server, { cleanupDone: false, cleaning: false, cleanupStart: null });
        this.wsSet.add(server);

        return new Response(null, { status: 101, webSocket: client });
      }
      return new Response("Game Server", { status: 200 });
    } catch(e) {
      this._handleError('fetch', e);
      return new Response(JSON.stringify({ error: "Internal Server Error" }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }
  }

  // ============================================================
  // WEBSOCKET HANDLERS
  // ============================================================

  async webSocketMessage(ws, message) {
    try {
      if (!ws || ws._closing || this.closing || this.isDestroyed || ws._cleaning) return;
      const state = _wsCleanupState.get(ws);
      if (state && state.cleanupDone) return;

      if (!ws.room && !ws.roomname && !ws._room) {
        try {
          const att = ws.deserializeAttachment?.();
          if (att?.room) {
            ws.room = att.room;
            ws.roomname = att.room;
            ws._room = att.room;
            if (att.username) { ws.username = att.username; ws._username = att.username; }
            if (att.wsId) ws._wsId = att.wsId;
            if (!ws._createdAt) ws._createdAt = att.createdAt || Date.now();
          }
        } catch(e) {}
      }

      if (ws._closing === undefined) ws._closing = false;
      if (ws._cleaning === undefined) ws._cleaning = false;
      if (!ws._createdAt) ws._createdAt = Date.now();
      if (ws._nextSessionSent === undefined) ws._nextSessionSent = false;

      const room = ws.room || ws.roomname || ws._room;
      if (room) {
        if (!this.roomClients.has(room)) this.roomClients.set(room, new Set());
        this.roomClients.get(room).add(ws);
        if (!this.wsSet.has(ws)) this.wsSet.add(ws);
        if (ws.username) {
          let conns = this.userConnections.get(ws.username);
          if (!conns) { conns = new Set(); this.userConnections.set(ws.username, conns); }
          conns.add(ws);
        }
      }

      if (!_wsCleanupState.has(ws)) {
        _wsCleanupState.set(ws, { cleanupDone: false, cleaning: false, cleanupStart: null });
      }

      if (!this._restored || this._isRestoring) {
        if (!this._pendingEvents) this._pendingEvents = [];
        if (this._pendingEvents.length >= CONSTANTS.MAX_PENDING_EVENTS) {
          try { this.safeSend(ws, ["gameLowCardError", "Server busy"]); } catch(e) {}
          return;
        }
        let attachment = null;
        try { attachment = ws.deserializeAttachment ? ws.deserializeAttachment() : null; } catch(e) {}
        this._pendingEvents.push({
          ws, message, timestamp: Date.now(), wsId: ws._wsId || Date.now(), attachment
        });
        return;
      }

      try {
        await this.handleMessage(ws, message);
      } catch(e) { this._handleError('webSocketMessage', e); }
    } catch(e) { this._handleError('webSocketMessage', e); }
  }

  async handleMessage(ws, raw) {
    try {
      if (!ws) return;
      const state = _wsCleanupState.get(ws);
      if (state && state.cleanupDone) return;
      if (ws._cleaning || ws._closing) return;

      if (!ws.room && !ws.roomname && !ws._room) {
        try {
          const att = ws.deserializeAttachment?.();
          if (att?.room) {
            ws.room = att.room;
            ws.roomname = att.room;
            ws._room = att.room;
            if (att.username) { ws.username = att.username; ws._username = att.username; }
            if (att.wsId) ws._wsId = att.wsId;
          }
        } catch(e) {}
      }

      if (!this._restored && this._restorePromise) {
        try {
          await Promise.race([
            this._restorePromise,
            new Promise(resolve => setTimeout(resolve, 3000))
          ]);
        } catch(e) {}
      }

      try {
        if (ws.readyState !== 1 || ws._closing || ws._cleaning || this.closing || this.isDestroyed) return;
      } catch(e) { return; }

      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > 5000) return;
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;

      await this._handleEventInternal(ws, data);
    } catch(e) { this._handleError('handleMessage', e); }
  }

  webSocketClose(ws) {
    try {
      if (!ws || this.isDestroyed) return;
      if (!ws._username && ws.username) ws._username = ws.username;
      if (!ws._room && (ws.room || ws.roomname)) ws._room = ws.room || ws.roomname;
      const state = _wsCleanupState.get(ws);
      if (state && state.cleanupDone) return;
      this._cleanupUserCompletely(ws).catch(() => {});
    } catch(e) {}
  }

  webSocketError(ws) {
    try {
      if (!ws || this.isDestroyed) return;
      if (!ws._username && ws.username) ws._username = ws.username;
      if (!ws._room && (ws.room || ws.roomname)) ws._room = ws.room || ws.roomname;
      const state = _wsCleanupState.get(ws);
      if (state && state.cleanupDone) return;
      this._cleanupUserCompletely(ws).catch(() => {});
    } catch(e) {}
  }

  async _cleanupUserCompletely(ws) {
    try {
      if (!ws) return;
      let state = _wsCleanupState.get(ws);
      if (!state) {
        state = { cleanupDone: false, cleaning: false, cleanupStart: null };
        try { _wsCleanupState.set(ws, state); } catch(e) {}
      }
      if (state.cleanupDone || state.cleaning) return;
      state.cleaning = true;
      state.cleanupStart = Date.now();

      try {
        const username = ws.username || ws._username;
        const roomName = ws.room || ws.roomname || ws._room;

        if (!username) {
          if (this.wsSet) try { this.wsSet.delete(ws); } catch(e) {}
          return;
        }

        if (ws && !ws._closing) ws._closing = true;
        try { if (ws && ws.readyState === 1) ws.close(1000, "Cleanup"); } catch(e) {}

        if (this.wsSet) try { this.wsSet.delete(ws); } catch(e) {}

        const connections = this.userConnections?.get(username);
        if (connections) {
          try { connections.delete(ws); } catch(e) {}
          if (connections.size === 0) {
            try { this.userConnections.delete(username); } catch(e) {}
          }
        }

        if (roomName) {
          const roomClients = this.roomClients?.get(roomName);
          if (roomClients) {
            try { roomClients.delete(ws); } catch(e) {}
            if (roomClients.size === 0) {
              try { this.roomClients.delete(roomName); } catch(e) {}
            }
          }
        }

        if (roomName && username) {
          try { await this._markPlayerLeft(roomName, username); } catch(e) {}
        }

        try { ws._closing = false; ws._cleaning = false; } catch(e) {}
      } catch(e) {} finally {
        state.cleanupDone = true;
        state.cleaning = false;
        state.cleanupStart = null;
      }
    } catch(e) {}
  }

  // ============================================================
  // MARK PLAYER LEFT — ✅ TIDAK PERNAH UBAH NAMA PLAYER
  // ============================================================

  async _markPlayerLeft(room, username) {
    try {
      if (!room || !username) return;
      if (room === CONSTANTS.DICE_ROOM) return;
      
      const game = this.activeGames.get(room);
      if (!game || !game._isActive || game._gameEnded || !game.players) return;
      if (!game.players.has(username)) return;
      if (game.eliminated?.has(username)) return;

      // ✅ HANYA set flag _left, JANGAN UBAH player.name
      const player = game.players.get(username);
      if (player) {
        player._left = true;
        player._leftAt = Date.now();
        // ❌ TIDAK ADA: player.name = "left game";
      }

      game.numbers?.delete(username);
      game.tanda?.delete(username);

      this.broadcast(room, ["gameLowCardError", `${username} left the game`]);

      if (game._phase !== 'registration' && !game._botsAdded) {
        const humanCount = this._countHumanPlayers(game);
        if (humanCount <= 1) {
          this._addBots(room, 4);
          game._botsAdded = true;
        }
      }

      if (game._phase === 'draw' && !game.evaluationLocked && !game.drawTimeExpired) {
        const activeIds = this._getActivePlayerIds(game);
        const submittedIds = Array.from(game.numbers?.keys() || []);
        const notSubmitted = activeIds.filter(id => !submittedIds.includes(id));
        if (notSubmitted.length === 0 && activeIds.length > 0 && !game._isEvaluating) {
          game.evaluationLocked = true;
          this.broadcast(room, ["gameLowCardWait", "wait results"]);
          const evalTimer = this._trackTimer(setTimeout(() => {
            try {
              const currentGame = this.activeGames.get(room);
              if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
                this._evaluateRound(room, game);
              }
            } catch(e) {}
          }, CONSTANTS.EVALUATION_DELAY_MS));
          game._evalTimer = evalTimer;
        }
      }
    } catch(e) {}
  }

  // ============================================================
  // DICE ROOM
  // ============================================================

  _sendDiceRoomState(ws) {
    try {
      if (!ws || ws.readyState !== 1) return;
      const isDiceTime = this.alarmScheduler.isDiceTime();
      
      if (isDiceTime) {
        const isGameRunning = this.currentDiceRoll && this._canSubmitDiceAnswer;
        if (!this._diceGameStarted && !this.currentDiceRoll && !this._diceLock) {
          this._startDiceGameIfNotStarted();
        }
        if (isGameRunning) {
          this.safeSend(ws, ["diceRoll", {
            value: this.currentDiceRoll.value,
            timestamp: this.currentDiceRoll.timestamp,
            answerTime: 20,
            canAnswerNow: true,
            round: this._diceRound
          }]);
        }
      } else {
        if (!ws._nextSessionSent) {
          ws._nextSessionSent = true;
          const timer = setTimeout(() => {
            try {
              if (!ws || ws.readyState !== 1 || ws._closing) return;
              const timeInfo = this.alarmScheduler._getTimeLeftUntilNextDice();
              if (timeInfo && timeInfo.text) {
                this.safeSend(ws, ["diceNotification", 
                  `Next dice game in: ${timeInfo.text}`
                ]);
              }
            } catch(e) {}
          }, 5000);
          this._trackTimer(timer);
        }
      }
    } catch(e) {}
  }

  _startDiceGameIfNotStarted() {
    try {
      if (this._diceGameStarted) return;
      if (!this.alarmScheduler.isDiceTime()) return;
      if (this.currentDiceRoll || this._isShowingDice || this._diceLock) return;
      if (this._diceTimeUpCooldown) return;
      const clients = this.roomClients?.get(CONSTANTS.DICE_ROOM);
      if (!clients || clients.size === 0) return;
      this._diceGameStarted = true;
      this._diceSessionActive = true;
      this._diceSessionEnded = false;
      this._diceStartedByUser = true;
      this._startDiceFast();
    } catch(e) { this._diceGameStarted = false; }
  }

  _startDiceFast() {
    try {
      if (this._diceGameStarted && this.currentDiceRoll) return;
      const clients = this.roomClients?.get(CONSTANTS.DICE_ROOM);
      if (!clients || clients.size === 0) { this._diceLock = false; this._isShowingDice = false; this._diceGameStarted = false; return; }
      if (!this._diceSessionActive || this._diceSessionEnded) { this._diceLock = false; this._isShowingDice = false; this._diceGameStarted = false; return; }
      if (this._diceLock || this.currentDiceRoll || this._isShowingDice) return;
      if (this._diceTimeUpCooldown) return;
      this._diceLoopCounter = (this._diceLoopCounter || 0) + 1;
      if (this._diceLoopCounter > this._maxDiceLoops) { this._diceLoopCounter = 0; this._diceLock = false; this._isShowingDice = false; return; }
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
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceRoll", { value, timestamp: Date.now(), answerTime: 20, canAnswerNow: true, round: this._diceRound }]);
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "clik draw"]);
      for (const t of this._diceNotificationTimeouts) clearTimeout(t);
      this._diceNotificationTimeouts = [];
      this._diceNotificationTimeouts.push(setTimeout(() => { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "15s remaining"]); }, 5000));
      this._diceNotificationTimeouts.push(setTimeout(() => { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "10s remaining"]); }, 10000));
      this._diceNotificationTimeouts.push(setTimeout(() => { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "5s remaining"]); }, 15000));
      this._diceNotificationTimeouts.push(setTimeout(() => { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "3s remaining"]); }, 17000));
      this._diceTimeout = this._trackTimer(setTimeout(() => { this._endDiceRound(); }, 20000));
    } catch(e) { this._diceLock = false; this._isShowingDice = false; this._diceGameStarted = false; }
  }

  async _endDiceRound() {
    try {
      if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
      for (const t of this._diceNotificationTimeouts) clearTimeout(t);
      this._diceNotificationTimeouts = [];
      this._canSubmitDiceAnswer = false;
      this._isShowingDice = false;
      const diceValue = this.currentDiceRoll?.value;
      const roundNumber = this._diceRound || 1;
      const correctPlayers = [];
      for (const p of this.diceAnswered) { if (this._playerAnswers.get(p) === diceValue) correctPlayers.push(p); }
      if (correctPlayers.length === 0) {
        this.broadcast(CONSTANTS.DICE_ROOM, ["diceNoWinner", { message: "No winner", value: diceValue, round: roundNumber }]);
      } else if (correctPlayers.length === 1) {
        const winner = correctPlayers[0];
        try {
          const points = await this.dataManager.addDicePoint(winner);
          this.broadcast(CONSTANTS.DICE_ROOM, ["diceWinner", { username: winner, totalPoints: points[winner] || 0, diceValue, round: roundNumber }]);
        } catch(e) {
          this.broadcast(CONSTANTS.DICE_ROOM, ["diceWinner", { username: winner, totalPoints: 0, diceValue, round: roundNumber }]);
        }
      } else if (correctPlayers.length > 1 && !this._tieActive) {
        this.currentDiceRoll = null; this._diceLock = false; this._isShowingDice = false;
        await this._startTieBreaker(CONSTANTS.DICE_ROOM, correctPlayers);
        this._diceGameStarted = false;
        return;
      }
      this.currentDiceRoll = null;
      this._diceLock = false;
      this._diceGameStarted = false;
      this._diceTimeUpCooldown = true;
      if (this._diceCooldownTimer) clearTimeout(this._diceCooldownTimer);
      this._diceCooldownTimer = setTimeout(() => {
        this._diceTimeUpCooldown = false;
        this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
        this._lastSentRemaining = -1;
        this._diceLoopCounter = 0;
        this._diceGameStarted = false;
        if (this._diceSessionActive && !this._diceSessionEnded) {
          const clients = this.roomClients?.get(CONSTANTS.DICE_ROOM);
          if (clients && clients.size > 0 && !this.currentDiceRoll && !this._isShowingDice && !this._diceLock) {
            this._startDiceGameIfNotStarted();
          }
        }
      }, 15000);
    } catch(e) { this._diceLock = false; this._isShowingDice = false; this._diceGameStarted = false; }
  }

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
    } finally { setTimeout(() => { this._tieLock = false; }, 2000); }
  }

  async _runTieRound(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);
    for (const t of this._tieNotificationTimeouts) clearTimeout(t);
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
    this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", `Tie Round ${this._tieRound}: ${players.join(', ')}`]);
    const timeLimit = CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20;
    let isProcessed = false;
    this._tieNotificationTimeouts.push(setTimeout(() => { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "15s remaining"]); }, 5000));
    this._tieNotificationTimeouts.push(setTimeout(() => { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "10s remaining"]); }, 10000));
    this._tieNotificationTimeouts.push(setTimeout(() => { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "5s remaining"]); }, 15000));
    this._tieNotificationTimeouts.push(setTimeout(() => { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "3s remaining"]); }, 17000));
    this._tieTimer = this._trackTimer(setTimeout(() => {
      if (!isProcessed) {
        isProcessed = true;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "TIME UP"]);
        for (const t of this._tieNotificationTimeouts) clearTimeout(t);
        this._tieNotificationTimeouts = [];
        const tieId = this._getActiveTieBreakerId();
        if (tieId) { this._processTieResults(room, tieId, players); }
        else { this._resetTieBreakerState(null); this._startCooldownAfterTieBreaker(); }
      }
    }, (timeLimit * 1000) + 2000));
  }

  async _processTieResults(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    const entries = [];
    let answeredCount = 0, highest = 0, highestPlayers = [];
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
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", `No one answered in Round ${this._tieRound} - Tie breaker ended`]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    if (answeredCount === 1) {
      const winner = entries[0].player;
      const answer = entries[0].answer;
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", `${winner} answered with ${answer} - Auto win!`]);
      try {
        const points = await this.dataManager.addDicePoint(winner);
        this.broadcast(CONSTANTS.DICE_ROOM, ["diceWinner", { username: winner, totalPoints: points[winner] || 0, diceValue: answer, round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true, totalTieRounds: this._tieRound }]);
      } catch(e) {
        this.broadcast(CONSTANTS.DICE_ROOM, ["diceWinner", { username: winner, totalPoints: 0, diceValue: answer, round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true, totalTieRounds: this._tieRound }]);
      }
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    const allSame = entries.every(e => e.answer === entries[0].answer);
    if (allSame) {
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", `All answered same value: ${entries[0].answer} - Tie again!`]);
      const allPlayers = entries.map(e => e.player);
      this._tiePlayers = allPlayers;
      this._tieAnswers = new Map();
      data.players = allPlayers;
      data.status = 'waiting';
      const nextTimer = setTimeout(async () => {
        if (this._tieActive && this._tiePlayers.length > 1) { await this._runTieRound(room, id, this._tiePlayers); }
        else if (this._tiePlayers.length === 1) { await this._processSingleWinner(room, id, this._tiePlayers[0]); }
        else { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "No players remaining"]); this._resetTieBreakerState(id); this._startCooldownAfterTieBreaker(); }
      }, 3000);
      this._trackTimer(nextTimer);
      return;
    }
    if (highestPlayers.length === 1) {
      const winner = highestPlayers[0];
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", `${winner} wins with highest value: ${highest}`]);
      try {
        const points = await this.dataManager.addDicePoint(winner);
        this.broadcast(CONSTANTS.DICE_ROOM, ["diceWinner", { username: winner, totalPoints: points[winner] || 0, diceValue: highest, round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true, totalTieRounds: this._tieRound }]);
      } catch(e) {
        this.broadcast(CONSTANTS.DICE_ROOM, ["diceWinner", { username: winner, totalPoints: 0, diceValue: highest, round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true, totalTieRounds: this._tieRound }]);
      }
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    if (highestPlayers.length > 1) {
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", `Tie again! Round ${this._tieRound + 1} between: ${highestPlayers.join(', ')}`]);
      this._tiePlayers = highestPlayers;
      this._tieAnswers = new Map();
      data.players = highestPlayers;
      data.status = 'waiting';
      const nextTimer = setTimeout(async () => {
        if (this._tieActive && this._tiePlayers.length > 1) { await this._runTieRound(room, id, this._tiePlayers); }
        else if (this._tiePlayers.length === 1) { await this._processSingleWinner(room, id, this._tiePlayers[0]); }
        else { this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "No players remaining"]); this._resetTieBreakerState(id); this._startCooldownAfterTieBreaker(); }
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
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceWinner", { username: winner, totalPoints: points[winner] || 0, diceValue: 'auto', round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true, totalTieRounds: this._tieRound }]);
    } catch(e) {
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceWinner", { username: winner, totalPoints: 0, diceValue: 'auto', round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true, totalTieRounds: this._tieRound }]);
    }
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  _startCooldownAfterTieBreaker() {
    this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "wait 15s"]);
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
        const clients = this.roomClients?.get(CONSTANTS.DICE_ROOM);
        if (clients && clients.size > 0 && !this.currentDiceRoll && !this._isShowingDice && !this._diceLock) {
          this._startDiceGameIfNotStarted();
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
    for (const t of this._tieNotificationTimeouts) clearTimeout(t);
    this._tieNotificationTimeouts = [];
  }

  _getActiveTieBreakerId() {
    for (const [id, data] of this._tieBreakers) {
      if (data.status === 'waiting' || data.status === 'running') return id;
    }
    return null;
  }

  async submitDiceAnswer(ws, username, guess) {
    try {
      if (!ws || !username) return;
      if (!this._canSubmitDiceAnswer) { this.safeSend(ws, ["diceError", "Round ended"]); return; }
      if (this.diceAnswered.has(username)) { this.safeSend(ws, ["diceError", "Already answered"]); return; }
      const guessValue = parseInt(guess, 10);
      if (isNaN(guessValue) || guessValue < 1 || guessValue > 6) { this.safeSend(ws, ["diceError", "invalid guess 1-6"]); return; }
      if (this._tieActive) {
        if (!this._tiePlayers.includes(username)) { this.safeSend(ws, ["diceError", "You are not in tie breaker"]); return; }
        if (this._tieAnswers.has(username)) { this.safeSend(ws, ["diceError", "You already answered"]); return; }
        this._tieAnswers.set(username, guessValue);
        this.diceAnswered.add(username);
        this.broadcast(CONSTANTS.DICE_ROOM, ["diceAnswer", { username, guess: guessValue, isTieBreaker: true, tieRound: this._tieRound }]);
        if (this._tieAnswers.size === this._tiePlayers.length) {
          this._canSubmitDiceAnswer = false;
          this._isShowingDice = false;
          if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
          if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
          const tieId = this._getActiveTieBreakerId();
          if (tieId) { setTimeout(async () => { await this._processTieResults(CONSTANTS.DICE_ROOM, tieId, this._tiePlayers); }, 500); }
          else { this._resetTieBreakerState(null); this._startCooldownAfterTieBreaker(); }
        }
        return;
      }
      if (!this.currentDiceRoll) { this.safeSend(ws, ["diceError", "No active round"]); return; }
      const diceValue = this.currentDiceRoll.value;
      this._playerAnswers.set(username, guessValue);
      this.diceAnswered.add(username);
      this.broadcast(CONSTANTS.DICE_ROOM, ["diceAnswer", { username, guess: guessValue, round: this._diceRound || 1 }]);
      if (guessValue === diceValue && !this.diceHasWinner) { this.diceHasWinner = true; this.diceWinner = username; }
    } catch(e) {}
  }

  // ============================================================
  // LOW CARD HELPERS
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
      this.broadcast(roomKey, ["lowCardWinnerUpdate", { winners: winners || {}, room: roomKey, recording: true }]);
    } catch(e) {}
  }

  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username || room === CONSTANTS.DICE_ROOM) return false;
      const isRecording = await this.dataManager.getRecordingStatus(room);
      if (!isRecording) return false;
      return await this.dataManager.addWinner(room, username);
    } catch(e) { return false; }
  }

  async _getRecordingStatusFromKV(roomName) {
    try { if (!roomName) return false; return await this.dataManager.getRecordingStatus(roomName); } catch(e) { return false; }
  }

  async _getLowCardWinners(room) {
    try { if (!room) return {}; return await this.dataManager.getWinners(room); } catch(e) { return {}; }
  }

  async _checkAndForceResetIfMondayUTC() {
    try {
      const now = new Date();
      const currentDay = now.getUTCDay();
      const currentWeek = this.dataManager.getCurrentWeek();
      let lastResetWeek = await this.dataManager.getLastResetWeek();
      if (!lastResetWeek) { await this.dataManager.setLastResetWeek(currentWeek); return true; }
      if (lastResetWeek === currentWeek) return true;
      if (currentDay === CONSTANTS.WEEKLY_RESET_DAY) {
        await this._handleWeeklyReset();
        await this.dataManager.setLastResetWeek(currentWeek);
      }
      return true;
    } catch(e) {
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
    } catch(e) { return false; }
  }

  async _handleWeeklyReset() {
    try {
      const points = await this.dataManager.getDicePoints();
      let winner = null, highestScore = 0;
      for (const [username, score] of Object.entries(points)) {
        if (username && typeof username === 'string') {
          const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
          if (numericScore > highestScore) { highestScore = numericScore; winner = username; }
        }
      }
      const currentWeek = this.dataManager.getCurrentWeek();
      if (winner && highestScore > 0) {
        await this.dataManager.setLastWeekWinner({ username: winner, score: highestScore, week: currentWeek, timestamp: Date.now() });
      } else {
        await this.dataManager.deleteLastWeekWinner();
      }
      await this.dataManager.resetDicePoints();
      return true;
    } catch(e) {
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
        } catch(e) {}
      }
      await this.alarmScheduler.scheduleAlarms();
    } catch(e) {}
  }

  async _processAlarm(name) {
    switch(name) {
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
          this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "Dice session started!"]);
          
          const clients = this.roomClients?.get(CONSTANTS.DICE_ROOM);
          if (clients) {
            for (const ws of clients) {
              try { ws._nextSessionSent = false; } catch(e) {}
            }
          }
          
          if (clients && clients.size > 0) {
            if (!this.currentDiceRoll && !this._isShowingDice && !this._diceLock && !this._diceTimeUpCooldown) {
              this._diceStartedByUser = true;
              this._startDiceGameIfNotStarted();
            }
          } else {
            this._diceStartedByUser = false;
            this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "Waiting for players..."]);
          }
        }
        break;
      case 'dice_session_end':
        this.diceAutoEnabled = false;
        this._diceSessionActive = false;
        this._diceSessionEnded = true;
        this._diceStartedByUser = false;
        this._diceGameStarted = false;
        this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", "Dice session ended"]);
        
        try {
          const timeInfo = this.alarmScheduler._getTimeLeftUntilNextDice();
          if (timeInfo && timeInfo.text) {
            this.broadcast(CONSTANTS.DICE_ROOM, ["diceNotification", 
              `Next dice game in: ${timeInfo.text}`
            ]);
          }
        } catch(e) {}
        
        {
          const clients = this.roomClients?.get(CONSTANTS.DICE_ROOM);
          if (clients) {
            for (const ws of clients) {
              try { ws._nextSessionSent = false; } catch(e) {}
            }
          }
        }
        
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
        for (const t of this._diceNotificationTimeouts) { clearTimeout(t); }
        this._diceNotificationTimeouts = [];
        if (this._tieActive) this._resetTieBreakerState(null);
        break;
    }
  }

  _trackTimer(timer) { if (timer) this._allTimers.add(timer); return timer; }

  _clearTimer(timer) {
    if (timer) {
      if (typeof timer === 'object' && timer._destroyed) return;
      try { clearTimeout(timer); } catch(e) {}
      try { clearInterval(timer); } catch(e) {}
      this._allTimers.delete(timer);
    }
  }

  _handleError(type, error) {
    try {
      const now = Date.now();
      if (now - this._lastErrorReset > CONSTANTS.ERROR_RESET_INTERVAL_MS) { this._errorCount = 0; this._lastErrorReset = now; }
      this._errorCount++;
      if (this._errorCount > 20) { this._circuitOpen = true; this._lastResetTime = now; }
    } catch(e) {}
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
      return { hours, minutes, totalMs: minDiff * 60 * 1000, text: hours + "h " + minutes + "m", isRunning: this.alarmScheduler.isDiceTime() };
    } catch(e) { return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false }; }
  }

  _getCurrentWITATime() {
    try {
      const now = new Date();
      const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
      const minutes = now.getUTCMinutes();
      return { hours, minutes, totalMinutes: (hours * 60) + minutes };
    } catch(e) { return { hours: 0, minutes: 0, totalMinutes: 0 }; }
  }

  // ============================================================
  // EVENT DISPATCHER
  // ============================================================

  async _handleEventInternal(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;

      let currentRoom = ws.room || ws.roomname || ws._room;
      let currentUser = ws.username || ws._username;
      let currentWsId = ws._wsId;

      if (!currentRoom || !currentUser || !currentWsId) {
        try {
          const att = ws.deserializeAttachment?.();
          if (att) {
            if (!currentRoom && att.room) currentRoom = att.room;
            if (!currentUser && att.username) currentUser = att.username;
            if (!currentWsId && att.wsId) currentWsId = att.wsId;
          }
        } catch(e) {}
      }

      if (currentRoom) {
        ws.room = currentRoom;
        ws.roomname = currentRoom;
        ws._room = currentRoom;
      }
      if (currentUser) {
        ws.username = currentUser;
        ws._username = currentUser;
      }
      if (currentWsId) ws._wsId = currentWsId;
      if (!ws._createdAt) ws._createdAt = Date.now();
      if (ws._closing === undefined) ws._closing = false;
      if (ws._cleaning === undefined) ws._cleaning = false;
      if (ws._nextSessionSent === undefined) ws._nextSessionSent = false;

      if (currentRoom) {
        if (!this.roomClients.has(currentRoom)) this.roomClients.set(currentRoom, new Set());
        this.roomClients.get(currentRoom).add(ws);
        if (!this.wsSet.has(ws)) this.wsSet.add(ws);
        if (currentUser) {
          let conns = this.userConnections.get(currentUser);
          if (!conns) { conns = new Set(); this.userConnections.set(currentUser, conns); }
          conns.add(ws);
        }
        if (!_wsCleanupState.has(ws)) {
          _wsCleanupState.set(ws, { cleanupDone: false, cleaning: false, cleanupStart: null });
        }
      }

      const evt = data[0];

      if (evt === "switchRoom") { await this.switchRoom(ws, data[1], data[2]); return; }

      if (evt === "startGameWithRecording") {
        await this.startGameWithRecording(ws, data[1], data[2], data[3]);
        return;
      }

      if (evt === "getDiceSessionStatus") {
        try {
          const isDiceTime = this.alarmScheduler.isDiceTime();
          const isGameRunning = this.currentDiceRoll && this._canSubmitDiceAnswer;
          
          if (isDiceTime && isGameRunning) {
            this.safeSend(ws, ["diceRoll", {
              value: this.currentDiceRoll.value,
              timestamp: this.currentDiceRoll.timestamp,
              answerTime: 20,
              canAnswerNow: true,
              round: this._diceRound
            }]);
          }
        } catch(e) {}
        return;
      }

      if (evt === "startRecordingWinners") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') { this.safeSend(ws, ["startRecordingResult", { success: false, message: "Room name required" }]); return; }
        const room = roomName.trim();
        try {
          await this.dataManager.setRecordingStatus(room, true);
          this.safeSend(ws, ["startRecordingResult", { success: true, message: "Recording started successfully" }]);
          this.broadcast(room, ["recordingStatus", true]);
        } catch(e) { this.safeSend(ws, ["startRecordingResult", { success: false, message: e.message || "Failed" }]); }
        return;
      }

      if (evt === "stopRecordingWinners") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') { this.safeSend(ws, ["stopRecordingResult", { success: false, message: "Room name required" }]); return; }
        const room = roomName.trim();
        try {
          await this.dataManager.setRecordingStatus(room, false);
          await this.dataManager.deleteAllWinners(room);
          this.safeSend(ws, ["stopRecordingResult", { success: true, message: "Recording stopped successfully" }]);
          this.broadcast(room, ["recordingStatus", false]);
        } catch(e) { this.safeSend(ws, ["stopRecordingResult", { success: false, message: e.message || "Failed" }]); }
        return;
      }

      if (evt === "getRecordingStatus") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') { this.safeSend(ws, ["recordingError", "Room name required"]); return; }
        try {
          const isRecording = await this.dataManager.getRecordingStatus(roomName.trim());
          this.safeSend(ws, ["recordingStatus", isRecording]);
        } catch(e) { this.safeSend(ws, ["recordingError", e.message]); }
        return;
      }

      if (evt === "sendWinnersToRoom") {
        let room = data[1] || ws.room || ws.roomname;
        if (!room) { this.safeSend(ws, ["sendWinnersResult", { success: false, message: "Room required" }]); return; }
        try {
          await this._broadcastLowCardWinners(room.trim());
          this.safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners sent" }]);
        } catch(e) { this.safeSend(ws, ["sendWinnersResult", { success: false, message: e.message }]); }
        return;
      }

      if (evt === "getRoomWinners") {
        let room = data[1] || ws.room || ws.roomname;
        if (!room) { this.safeSend(ws, ["recordingError", "Room required"]); return; }
        const roomKey = room.trim();
        try {
          const isRecording = await this.dataManager.getRecordingStatus(roomKey);
          const winners = await this.dataManager.getWinners(roomKey);
          this.safeSend(ws, ["roomWinnersResponse", { winners: winners || {}, room: roomKey, recording: isRecording || false }]);
        } catch(e) { this.safeSend(ws, ["recordingError", e.message]); }
        return;
      }

      if (evt === "getAllData") {
        try { const allData = await this.dataManager.getAllData(); this.safeSend(ws, ["allDataResponse", allData]); }
        catch(e) { this.safeSend(ws, ["allDataResponse", {}]); }
        return;
      }

      if (evt === "addLowCardWinner") {
        const { room, username } = data[1] || {};
        if (!room || !username) { this.safeSend(ws, ["error", "Room and username required"]); return; }
        try {
          const isRecording = await this.dataManager.getRecordingStatus(room.trim());
          if (!isRecording) { this.safeSend(ws, ["error", "Recording is not enabled"]); return; }
          await this.dataManager.addWinner(room.trim(), username.trim());
          const winners = await this.dataManager.getWinners(room.trim());
          const count = parseInt(String(winners[username.trim()] || "0").replace("x", "")) || 0;
          this.safeSend(ws, ["addWinnerResult", { success: true, username: username.trim(), count, room: room.trim() }]);
          this._broadcastLowCardWinners(room.trim());
        } catch(e) { this.safeSend(ws, ["error", e.message]); }
        return;
      }

      if (evt === "deleteAllWinners") {
        const room = data[1];
        if (!room) { this.safeSend(ws, ["error", "Room required"]); return; }
        try {
          await this.dataManager.deleteAllWinners(room.trim());
          this.safeSend(ws, ["deleteWinnersResult", { success: true, message: "Deleted", room: room.trim() }]);
          this.broadcast(room.trim(), ["recordingStatus", false]);
        } catch(e) { this.safeSend(ws, ["error", e.message]); }
        return;
      }

      if (evt === "submitDiceAnswer") { await this.submitDiceAnswer(ws, data[1], data[2]); return; }

      if (evt === "getDiceLastWeekWinner") {
        try {
          const wsId = ws._wsId;
          const now = Date.now();
          if (!this._lastWinnerRequestTime) this._lastWinnerRequestTime = new Map();
          const lastReq = this._lastWinnerRequestTime.get(wsId) || 0;
          if (now - lastReq < 5000) return;
          this._lastWinnerRequestTime.set(wsId, now);
          const winner = await this.dataManager.getLastWeekWinner();
          if (winner && winner.username) {
            this.safeSend(ws, ["diceLastWeekWinner", String(winner.username), parseInt(winner.score, 10) || 0, String(winner.week || '')]);
          } else {
            this.safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
          }
        } catch(e) { this.safeSend(ws, ["diceLastWeekWinner", "", 0, ""]); }
        return;
      }

      if (evt === "deleteDiceLastWeekWinner") {
        try { await this.dataManager.deleteLastWeekWinner(); this.safeSend(ws, ["diceLastWeekWinnerDeleted", true, "Deleted"]); }
        catch(e) { this.safeSend(ws, ["diceLastWeekWinnerDeleted", false, e.message]); }
        return;
      }

      if (evt === "getDiceLeaderboard") {
        try {
          let limit = CONSTANTS.DEFAULT_LEADERBOARD_LIMIT;
          if (data.length > 1 && typeof data[1] === 'number') limit = Math.min(Math.max(data[1], CONSTANTS.MIN_LEADERBOARD_LIMIT), CONSTANTS.MAX_LEADERBOARD_LIMIT);
          const leaderboard = await this.dataManager.getLeaderboard(limit);
          this.safeSend(ws, ["diceLeaderboard", leaderboard]);
        } catch(e) { this.safeSend(ws, ["diceLeaderboard", []]); }
        return;
      }

      if (evt === "getDicePoints") {
        return;
      }

      if (evt === "getDiceStatus") {
        const isActive = !!this.currentDiceRoll && this._canSubmitDiceAnswer;
        this.safeSend(ws, ["diceStatus", isActive, this._diceRound || 1]);
        return;
      }

      if (evt === "getDiceNotification") {
        try {
          const isActive = this.currentDiceRoll && this._canSubmitDiceAnswer;
          let notification = "";
          
          if (isActive) {
            const elapsed = (Date.now() - this._diceStartTime) / 1000;
            const remaining = Math.max(0, 20 - elapsed);
            notification = Math.floor(remaining) + "s remaining";
          } else if (this.alarmScheduler.isDiceTime()) {
            notification = "Dice game starting soon...";
          } else {
            notification = "Waiting...";
          }
          this.safeSend(ws, ["diceNotification", notification]);
        } catch(e) { this.safeSend(ws, ["diceNotification", "Waiting..."]); }
        return;
      }

      if (evt === "checkGameRunning") { await this.checkGameRunning(ws, data[1]); return; }

      const room = ws.room || ws.roomname;
      if (!room) { this.safeSend(ws, ["gameLowCardError", "Please switch to a room first"]); return; }
      if (room === CONSTANTS.DICE_ROOM) { this.safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]); return; }

      switch (evt) {
        case "gameLowCardStart": await this.startGame(ws, data[1], data[2]); break;
        case "gameLowCardJoin": await this.joinGame(ws, data[1]); break;
        case "gameLowCardNumber": await this.submitNumber(ws, data[1], data[2] || "", data[3]); break;
        case "gameLowCardLeave": await this.leaveGame(ws, data[1]); break;
      }
    } catch(e) {}
  }

  // ============================================================
  // SWITCH ROOM
  // ============================================================

  async switchRoom(ws, room, username = null) {
    try {
      if (this.isDestroyed) { this.safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!room || typeof room !== 'string' || room.trim() === "") { this.safeSend(ws, ["gameLowCardError", "Invalid room name"]); return; }
      const roomName = room.trim();
      const wsId = ws._wsId || (++this._wsIdCounter);
      if (!wsId) { this.safeSend(ws, ["gameLowCardError", "Connection error"]); return; }
      if (ws.readyState !== 1 || ws._closing) { this.safeSend(ws, ["gameLowCardError", "Connection closed"]); return; }

      const currentRoom = ws.room || ws.roomname;
      if (currentRoom === roomName) {
        if (roomName === CONSTANTS.DICE_ROOM) {
          this._sendDiceRoomState(ws);
        }
        this.safeSend(ws, ["switchRoomSuccess", roomName]);
        return;
      }

      if (currentRoom) {
        const clients = this.roomClients.get(currentRoom);
        if (clients) { clients.delete(ws); if (clients.size === 0) this.roomClients.delete(currentRoom); }
      }

      if (!this.roomClients.has(roomName)) this.roomClients.set(roomName, new Set());
      this.roomClients.get(roomName).add(ws);
      if (!this.wsSet.has(ws)) this.wsSet.add(ws);

      ws.room = roomName;
      ws.roomname = roomName;
      ws._room = roomName;
      ws._wsId = wsId;
      if (username) { ws.username = username; ws._username = username; }

      ws.serializeAttachment({ wsId, username: username || ws.username || null, room: roomName, roomname: roomName, createdAt: ws._createdAt || Date.now() });

      const finalUsername = username || ws.username;
      if (finalUsername) {
        let conns = this.userConnections.get(finalUsername);
        if (!conns) { conns = new Set(); this.userConnections.set(finalUsername, conns); }
        conns.add(ws);
      }

      if (!_wsCleanupState.has(ws)) {
        _wsCleanupState.set(ws, { cleanupDone: false, cleaning: false, cleanupStart: null });
      }

      this.safeSend(ws, ["switchRoomSuccess", roomName]);
      if (roomName === CONSTANTS.DICE_ROOM) {
        this._sendDiceRoomState(ws);
        if (this.alarmScheduler.isDiceTime() && !this._diceGameStarted && !this.currentDiceRoll) this._startDiceGameIfNotStarted();
      }
    } catch(e) { this.safeSend(ws, ["gameLowCardError", e.message || "Switch failed"]); }
  }

  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) { this.safeSend(ws, ["gameStatus", "false"]); return; }
      let room = roomname || ws.room || ws.roomname;
      if (!room) { this.safeSend(ws, ["gameStatus", "false"]); return; }
      if (room === CONSTANTS.DICE_ROOM) { this._sendDiceRoomState(ws); return; }
      const game = this.activeGames.get(room);
      const isRunning = game?._isActive && !game._gameEnded && game.players?.size > 0;
      this.safeSend(ws, ["gameStatus", isRunning ? "true" : "false"]);
    } catch(e) {}
  }

  // ============================================================
  // START GAME WITH RECORDING (ADMIN)
  // ============================================================

  async startGameWithRecording(ws, room, bet, username) {
    try {
      if (this.isDestroyed) { this.safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!room || typeof room !== 'string' || room.trim() === "") { this.safeSend(ws, ["gameLowCardError", "Room name required"]); return; }
      const roomName = room.trim();
      if (!username?.trim()) { this.safeSend(ws, ["gameLowCardError", "Username required"]); return; }
      const usernameClean = username.trim();
      if (roomName === CONSTANTS.DICE_ROOM) { this.safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]); return; }
      
      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
        this.safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
        return;
      }
      
      const isRecordingEnabled = await this._getRecordingStatusFromKV(roomName);
      if (!isRecordingEnabled) {
        this.safeSend(ws, ["gameLowCardError", "Recording is NOT active in this room"]);
        return;
      }
      
      const lockKey = `game_start_recording_${roomName}`;
      if (this._gameLocks.has(lockKey)) { this.safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]); return; }
      this._gameLocks.set(lockKey, Date.now());
      
      try {
        const existingGame = this.activeGames.get(roomName);
        if (existingGame?._isActive && !existingGame._gameEnded) { 
          this.safeSend(ws, ["gameLowCardError", "Game is already running"]); 
          return; 
        }
        if (existingGame) await this._forceCleanupGame(roomName, existingGame);
        
        if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) { 
          this.safeSend(ws, ["gameLowCardError", "Server is busy"]); 
          return; 
        }
        
        const wsId = ws._wsId;
        const game = {
          room: roomName, players: new Map(), botPlayers: new Map(), registrationOpen: true, round: 1,
          numbers: new Map(), tanda: new Map(), eliminated: new Set(), betAmount,
          hostId: usernameClean, hostName: usernameClean, useBots: false,
          evaluationLocked: false, drawTimeExpired: false, _isActive: true, _gameEnded: false,
          _phase: 'registration', _botTimeouts: new Set(), _botsAdded: false,
          _registrationTimer: null, _drawTimer: null, _evalTimer: null, _safetyTimer: null,
          _isEvaluating: false, _createdAt: Date.now(), _drawPhaseStart: null, _endTime: null,
          playerWsId: new Map(),
          _startedByRecording: true,
          _startedBy: 'recording',
          _roundCompleted: 0
        };
        
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean, _left: false, _leftAt: null });
        game.playerWsId.set(usernameClean, wsId);
        this.activeGames.set(roomName, game);
        
        ws.room = roomName;
        ws.roomname = roomName;
        ws._room = roomName;
        ws.username = usernameClean;
        ws._username = usernameClean;
        ws._wsId = wsId;
        ws.serializeAttachment({ wsId, username: usernameClean, room: roomName, roomname: roomName, createdAt: ws._createdAt || Date.now() });
        
        if (!this.roomClients.has(roomName)) this.roomClients.set(roomName, new Set());
        this.roomClients.get(roomName).add(ws);
        if (!this.wsSet.has(ws)) this.wsSet.add(ws);
        let conns = this.userConnections.get(usernameClean);
        if (!conns) { conns = new Set(); this.userConnections.set(usernameClean, conns); }
        conns.add(ws);
        
        this.broadcast(roomName, ["gameLowCardStart", betAmount]);
        this.broadcast(roomName, ["gameLowCardStartSuccess", usernameClean, betAmount]);
        this._startRegistration(roomName, game);
      } finally {
        setTimeout(() => { this._gameLocks.delete(lockKey); }, 3000);
      }
    } catch(e) {}
  }

  // ============================================================
  // LOW CARD GAME LOGIC
  // ============================================================

  _isGameActuallyRunning(game) { return game?._isActive === true && !game?._gameEnded && game?.players?.size > 0; }

  _getActivePlayers(game) {
    try {
      if (!game?._isActive || game?._gameEnded || !game?.players) return [];
      return Array.from(game.players.entries())
        .filter(([id]) => !game.eliminated?.has(id))
        .map(([, p]) => p);
    } catch(e) { return []; }
  }

  _getActivePlayerIds(game) {
    try {
      if (!game?._isActive || game._gameEnded || !game?.players) return [];
      return Array.from(game.players.keys())
        .filter(id => !game.eliminated?.has(id));
    } catch(e) { return []; }
  }

  _countHumanPlayers(game) {
    try {
      if (!game?.players) return 0;
      return Array.from(game.players.keys())
        .filter(id => !id.startsWith('BOT_') && !game.eliminated?.has(id))
        .length;
    } catch(e) { return 0; }
  }

  _getBotNumberByRound(round) {
    if (round <= 2) return Math.floor(Math.random() * 12) + 1;
    return Math.random() < 0.6 ? [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] : [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
  }

  _getRandomCardTanda() { return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; }
  _getRandomDrawDelay() { return (Math.floor(Math.random() * 14) + 2) * 1000; }

  async startGame(ws, bet, username) {
    try {
      if (this.isDestroyed) { this.safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!username?.trim()) { this.safeSend(ws, ["gameLowCardError", "Username is required"]); return; }
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname;
      if (!room) { this.safeSend(ws, ["gameLowCardError", "Please switch to a room first"]); return; }
      if (room === CONSTANTS.DICE_ROOM) { this.safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]); return; }
      const lockKey = `game_start_${room}`;
      if (this._gameLocks.has(lockKey)) { this.safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]); return; }
      this._gameLocks.set(lockKey, Date.now());
      try {
        const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
        if (isRecordingEnabled) { 
          this.safeSend(ws, ["gameLowCardError", "Recording is ACTIVE in this room. Users cannot start games."]); 
          return; 
        }
        
        const existingGame = this.activeGames.get(room);
        if (existingGame?._isActive && !existingGame._gameEnded) { this.safeSend(ws, ["gameLowCardError", "Game is already running"]); return; }
        if (existingGame) await this._forceCleanupGame(room, existingGame);
        const betAmount = parseInt(bet, 10) || 0;
        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          this.safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
          return;
        }
        if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) { this.safeSend(ws, ["gameLowCardError", "Server is busy"]); return; }
        const wsId = ws._wsId;
        const game = {
          room, players: new Map(), botPlayers: new Map(), registrationOpen: true, round: 1,
          numbers: new Map(), tanda: new Map(), eliminated: new Set(), betAmount,
          hostId: usernameClean, hostName: usernameClean, useBots: false,
          evaluationLocked: false, drawTimeExpired: false, _isActive: true, _gameEnded: false,
          _phase: 'registration', _botTimeouts: new Set(), _botsAdded: false,
          _registrationTimer: null, _drawTimer: null, _evalTimer: null, _safetyTimer: null,
          _isEvaluating: false, _createdAt: Date.now(), _drawPhaseStart: null, _endTime: null,
          playerWsId: new Map(), _startedByRecording: false, _startedBy: 'user',
          _roundCompleted: 0
        };
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean, _left: false, _leftAt: null });
        game.playerWsId.set(usernameClean, wsId);
        this.activeGames.set(room, game);

        ws.room = room;
        ws.roomname = room;
        ws._room = room;
        ws.username = usernameClean;
        ws._username = usernameClean;
        ws._wsId = wsId;
        ws.serializeAttachment({ wsId, username: usernameClean, room, roomname: room, createdAt: ws._createdAt || Date.now() });

        if (!this.roomClients.has(room)) this.roomClients.set(room, new Set());
        this.roomClients.get(room).add(ws);
        if (!this.wsSet.has(ws)) this.wsSet.add(ws);
        let conns = this.userConnections.get(usernameClean);
        if (!conns) { conns = new Set(); this.userConnections.set(usernameClean, conns); }
        conns.add(ws);

        this.broadcast(room, ["gameLowCardStart", betAmount]);
        this.broadcast(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
        this._startRegistration(room, game);
      } finally {
        setTimeout(() => { this._gameLocks.delete(lockKey); }, 3000);
      }
    } catch(e) {}
  }

  _startRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      if (game._registrationTimer) { this._clearTimer(game._registrationTimer); game._registrationTimer = null; }
      let timeLeft = 20;
      const timer = this._trackTimer(setInterval(() => {
        try {
          if (!this._isGameActuallyRunning(game) || !game.registrationOpen || timeLeft < 0) {
            this._clearTimer(timer);
            if (game._registrationTimer === timer) game._registrationTimer = null;
            return;
          }
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) this.broadcast(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          if (timeLeft === 0) {
            this._clearTimer(timer);
            game._registrationTimer = null;
            this.broadcast(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeRegistration(room, game);
          }
          timeLeft--;
        } catch(e) { this._clearTimer(timer); if (game._registrationTimer === timer) game._registrationTimer = null; }
      }, 1000));
      game._registrationTimer = timer;
    } catch(e) {}
  }

  _closeRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      game.registrationOpen = false;
      if (game._registrationTimer) { this._clearTimer(game._registrationTimer); game._registrationTimer = null; }
      
      if (!game._botsAdded) {
        const humanCount = this._countHumanPlayers(game);
        if (humanCount <= 1) {
          this._addBots(room, 4);
          game._botsAdded = true;
        }
      }
      
      if (this._isGameActuallyRunning(game) && game.players.size >= 2) {
        this._startDrawPhase(room, game);
      } else {
        game._gameEnded = true;
        game._isActive = false;
        this.broadcast(room, ["gameLowCardError", "Not enough players"]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }

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
        const botId = `BOT_${room}_${existingBotCount + i}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
        const botName = botNames[(existingBotCount + i) % botNames.length];
        if (!game.players.has(botId)) {
          game.players.set(botId, { id: botId, name: botName, _left: false, _leftAt: null });
          if (!game.botPlayers) game.botPlayers = new Map();
          game.botPlayers.set(botId, botName);
        }
      }
      game._botsAdded = true;
      game.useBots = true;
    } catch(e) {}
  }

  async _startDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
      if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
      if (game._botTimeouts) { for (const id of game._botTimeouts) this._clearTimer(id); game._botTimeouts.clear(); }
      game._isEvaluating = false;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      
      if (!game._botsAdded) {
        const humanCount = this._countHumanPlayers(game);
        if (humanCount <= 1) {
          this._addBots(room, 4);
          game._botsAdded = true;
        }
      }
      
      game._phase = 'draw';
      game.drawTimeExpired = false;
      game.evaluationLocked = false;
      game._drawPhaseStart = Date.now();
      if (!game._botTimeouts) game._botTimeouts = new Set();
      const playersList = this._getActivePlayers(game).map(p => p.name);
      this.broadcast(room, ["gameLowCardClosed", playersList]);
      this.broadcast(room, ["gameLowCardNextRound", game.round]);
      this._startDrawCountdown(room, game);
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) this._startBotDraws(room, game);
    } catch(e) {}
  }

  _startDrawCountdown(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      let timeLeft = 20;
      const timer = this._trackTimer(setInterval(() => {
        try {
          if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || timeLeft < 0) {
            this._clearTimer(timer);
            if (game._drawTimer === timer) game._drawTimer = null;
            return;
          }
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) this.broadcast(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          if (timeLeft === 0) {
            this._clearTimer(timer);
            game._drawTimer = null;
            this.broadcast(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeDrawPhase(room, game);
          }
          timeLeft--;
        } catch(e) { this._clearTimer(timer); if (game._drawTimer === timer) game._drawTimer = null; }
      }, 1000));
      game._drawTimer = timer;
    } catch(e) {}
  }

  _startBotDraws(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.botPlayers) return;
      if (!game._botTimeouts) game._botTimeouts = new Set();
      if (game._botTimeouts.size >= CONSTANTS.MAX_BOT_TIMEOUTS) return;
      const notDrawn = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id))
        .slice(0, Math.min(CONSTANTS.MAX_BOT_DRAWS_PER_ROUND, CONSTANTS.MAX_BOT_TIMEOUTS - game._botTimeouts.size));
      for (const botId of notDrawn) {
        const delay = this._getRandomDrawDelay();
        const timeout = this._trackTimer(setTimeout(() => {
          const currentGame = this.activeGames.get(room);
          if (this._isGameActuallyRunning(currentGame) && !currentGame.drawTimeExpired &&
              !currentGame.evaluationLocked && !currentGame.numbers?.has(botId) &&
              !currentGame.eliminated?.has(botId)) {
            this._handleBotDraw(room, botId, currentGame);
          }
          currentGame?._botTimeouts?.delete(timeout);
        }, delay));
        game._botTimeouts.add(timeout);
      }
    } catch(e) {}
  }

  _handleBotDraw(room, botId, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.numbers?.has(botId) || game.drawTimeExpired || game.evaluationLocked) return;
      if (game.eliminated?.has(botId)) return;
      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      const botName = game.players.get(botId)?.name || botId;
      this.broadcast(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
      const activeIds = this._getActivePlayerIds(game);
      if (game.numbers.size >= activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameActuallyRunning(game)) {
        game.evaluationLocked = true;
        this.broadcast(room, ["gameLowCardWait", "wait results"]);
        const evalTimer = this._trackTimer(setTimeout(() => { try { this._evaluateRound(room, game); } catch(e) {} }, CONSTANTS.EVALUATION_DELAY_MS));
        game._evalTimer = evalTimer;
      }
    } catch(e) {}
  }

  _forceBotDraw(room, botId, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.numbers?.has(botId)) return;
      if (game.eliminated?.has(botId)) return;
      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      const botName = game.players.get(botId)?.name || botId;
      this.broadcast(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
    } catch(e) {}
  }

  _closeDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || game.evaluationLocked) return;
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys()).filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) this._forceBotDraw(room, botId, game);
      }
      this.broadcast(room, ["gameLowCardWait", "wait results"]);
      if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
      const evalTimer = this._trackTimer(setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) this._evaluateRound(room, game);
        } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS));
      game._evalTimer = evalTimer;
    } catch(e) {}
  }

  async _evaluateRound(room, game) {
    try {
      if (this.isDestroyed || !game?._isActive || game._gameEnded || game._isEvaluating || !game.players) return;
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game) return;
      game._isEvaluating = true;
      const safetyTimer = this._trackTimer(setTimeout(() => {
        if (game?._isEvaluating) { game._isEvaluating = false; this._scheduleGameCleanup(room, game); }
      }, CONSTANTS.EVALUATION_TIMEOUT_MS));
      game._safetyTimer = safetyTimer;
      if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
      if (game._botTimeouts) { for (const id of game._botTimeouts) this._clearTimer(id); game._botTimeouts.clear(); }
      
      const numbers = game.numbers || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const tanda = game.tanda || new Map();
      const entries = Array.from(numbers.entries());
      const submittedIds = new Set(numbers.keys());
      const activeIds = this._getActivePlayerIds(game);
      
      for (const id of activeIds) { 
        if (!submittedIds.has(id)) eliminated.add(id); 
      }
      
      if (entries.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        if (!game._botsAdded) {
          const humanCount = this._countHumanPlayers(game);
          if (humanCount <= 1) { this._addBots(room, 4); game._botsAdded = true; }
        }
        const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
        if (remaining.length >= 2) {
          game.round++;
          game.numbers = new Map();
          game.tanda = new Map();
          game.evaluationLocked = false;
          game.drawTimeExpired = false;
          game._phase = 'draw';
          game._botTimeouts = new Set();
          if (this._isGameActuallyRunning(game)) this._startDrawPhase(room, game);
        } else {
          game._gameEnded = true;
          game._isActive = false;
          this.broadcast(room, ["gameLowCardError", "No numbers drawn"]);
          this._scheduleGameCleanup(room, game);
        }
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
      
      if (game.round === 1) game._roundCompleted = 1;
      
      if (allSame && remaining.length >= 2) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        numbers.clear();
        tanda.clear();
        game.round++;
        game.evaluationLocked = false;
        game.drawTimeExpired = false;
        game._phase = 'draw';
        game.numbers = new Map();
        game.tanda = new Map();
        game._botTimeouts = new Set();
        const remainingNames = remaining.map(id => players.get(id)?.name || id);
        this.broadcast(room, ["gameLowCardRoundResult", game.round - 1,
          entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`),
          [], remainingNames, true
        ]);
        if (this._isGameActuallyRunning(game) && !game._gameEnded) this._startDrawPhase(room, game);
        return;
      }
      
      if (remaining.length === 1 && !game._gameEnded) {
        const winnerId = remaining[0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winnerName);
          const winners = await this._getLowCardWinners(room);
          this.broadcast(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        }
        this.broadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (remaining.length === 0) {
        if (!game._botsAdded) {
          const humanCount = this._countHumanPlayers(game);
          if (humanCount <= 1) { this._addBots(room, 4); game._botsAdded = true; }
        }
        const newActive = Array.from(players.keys()).filter(id => !eliminated.has(id));
        if (newActive.length >= 2) {
          game._isEvaluating = false;
          if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
          game.round++;
          game.numbers = new Map();
          game.tanda = new Map();
          game.evaluationLocked = false;
          game.drawTimeExpired = false;
          game._phase = 'draw';
          game._botTimeouts = new Set();
          if (this._isGameActuallyRunning(game)) this._startDrawPhase(room, game);
          return;
        }
        
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        game._gameEnded = true;
        game._isActive = false;
        this.broadcast(room, ["gameLowCardError", "All players eliminated"]);
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      const numbersArr = entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`);
      const loserNames = [...losers].map(id => players.get(id)?.name || id);
      const remainingNames = remaining.map(id => players.get(id)?.name || id);
      this.broadcast(room, ["gameLowCardRoundResult", game.round, numbersArr, loserNames, remainingNames]);
      numbers.clear();
      tanda.clear();
      game.round++;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      game._phase = 'draw';
      game.numbers = new Map();
      game.tanda = new Map();
      game._botTimeouts = new Set();
      game._isEvaluating = false;
      if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
      if (this._isGameActuallyRunning(game) && !game._gameEnded) this._startDrawPhase(room, game);
    } catch(e) {}
  }

  async joinGame(ws, username) {
    try {
      if (this.isDestroyed) { this.safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!username?.trim()) { this.safeSend(ws, ["gameLowCardError", "Username is required"]); return; }
      const usernameClean = username.trim();
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname;
      if (!room) { this.safeSend(ws, ["gameLowCardError", "Please switch to a room first"]); return; }
      const lockKey = `join_${room}_${usernameClean}`;
      if (this._joinLocks.has(lockKey)) { this.safeSend(ws, ["gameLowCardError", "Please wait"]); return; }
      this._joinLocks.set(lockKey, Date.now());
      try {
        const game = this.activeGames.get(room);
        if (!game?._isActive || game._gameEnded || !game.players) { this.safeSend(ws, ["gameLowCardError", "No active game in this room"]); return; }
        
        if (game.players.has(usernameClean)) {
          if (game.eliminated?.has(usernameClean)) { 
            this.safeSend(ws, ["gameLowCardError", "You have been eliminated"]); 
            return; 
          }
          
          const player = game.players.get(usernameClean);
          if (player) {
            player.name = usernameClean;
            player._left = false;
            player._leftAt = null;
          }
          
          ws.room = room;
          ws.roomname = room;
          ws._room = room;
          ws.username = usernameClean;
          ws._username = usernameClean;
          ws._wsId = wsId;
          ws.serializeAttachment({ wsId, username: usernameClean, room, roomname: room, createdAt: ws._createdAt || Date.now() });
          
          if (!this.roomClients.has(room)) this.roomClients.set(room, new Set());
          this.roomClients.get(room).add(ws);
          if (!this.wsSet.has(ws)) this.wsSet.add(ws);
          
          let conns = this.userConnections.get(usernameClean);
          if (!conns) { conns = new Set(); this.userConnections.set(usernameClean, conns); }
          conns.add(ws);
          
          game.playerWsId.set(usernameClean, wsId);
          
          this.broadcast(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
          this.safeSend(ws, ["gameLowCardJoinSuccess", usernameClean, game.betAmount]);
          
          this.safeSend(ws, ["gameLowCardPlayerRejoin", {
            username: usernameClean,
            room: room,
            round: game.round,
            phase: game._phase,
            betAmount: game.betAmount,
            hasSubmitted: game.numbers?.has(usernameClean) || false
          }]);
          
          if (game.numbers?.has(usernameClean)) {
            this.safeSend(ws, ["gameLowCardPlayerDraw", usernameClean, game.numbers.get(usernameClean), game.tanda.get(usernameClean) || ""]);
          }
          
          return;
        }
        
        if (!game.registrationOpen) {
          this.safeSend(ws, ["gameLowCardNoJoin", usernameClean, game.betAmount]);
          this.safeSend(ws, ["gameLowCardError", "Registration is closed"]);
          return;
        }
        if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) { this.safeSend(ws, ["gameLowCardError", "Game is full"]); return; }
        
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean, _left: false, _leftAt: null });

        ws.room = room;
        ws.roomname = room;
        ws._room = room;
        ws.username = usernameClean;
        ws._username = usernameClean;
        ws._wsId = wsId;
        ws.serializeAttachment({ wsId, username: usernameClean, room, roomname: room, createdAt: ws._createdAt || Date.now() });

        if (!this.roomClients.has(room)) this.roomClients.set(room, new Set());
        this.roomClients.get(room).add(ws);
        if (!this.wsSet.has(ws)) this.wsSet.add(ws);
        let conns = this.userConnections.get(usernameClean);
        if (!conns) { conns = new Set(); this.userConnections.set(usernameClean, conns); }
        conns.add(ws);

        game.playerWsId.set(usernameClean, wsId);
        this.broadcast(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
      } finally {
        setTimeout(() => { this._joinLocks.delete(lockKey); }, 2000);
      }
    } catch(e) {}
  }

  async submitNumber(ws, number, tanda, username) {
    try {
      if (this.isDestroyed) { this.safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!username?.trim()) { this.safeSend(ws, ["gameLowCardError", "Username is required"]); return; }
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname;
      if (!room) { this.safeSend(ws, ["gameLowCardError", "Please switch to a room first"]); return; }
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) { this.safeSend(ws, ["gameLowCardError", "No active game"]); return; }
      if (game.players.has(usernameClean) && game.eliminated?.has(usernameClean)) { this.safeSend(ws, ["gameLowCardError", "You have been eliminated"]); return; }
      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw') { this.safeSend(ws, ["gameLowCardError", "Cannot submit now"]); return; }
      if (!game.players.has(usernameClean)) { this.safeSend(ws, ["gameLowCardError", "You are not in this game"]); return; }
      if (game.numbers.has(usernameClean)) { this.safeSend(ws, ["gameLowCardError", "You have already submitted"]); return; }
      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) { this.safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]); return; }
      const validTandas = ["C1", "C2", "C3", "C4", ""];
      if (!validTandas.includes(tanda)) tanda = "";
      
      const player = game.players.get(usernameClean);
      if (player) {
        player.name = usernameClean;
        player._left = false;
        player._leftAt = null;
      }
      
      game.numbers.set(usernameClean, n);
      game.tanda.set(usernameClean, tanda);
      this.broadcast(room, ["gameLowCardPlayerDraw", usernameClean, n, tanda]);
      const activeIds = this._getActivePlayerIds(game);
      if (game.numbers.size >= activeIds.length && !game.evaluationLocked && !game.drawTimeExpired &&
          this._isGameActuallyRunning(game) && game._isActive && !game._gameEnded) {
        game.evaluationLocked = true;
        if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
        this.broadcast(room, ["gameLowCardWait", "wait results"]);
        const evalTimer = this._trackTimer(setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) this._evaluateRound(room, game);
          } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS));
        game._evalTimer = evalTimer;
      }
    } catch(e) {}
  }

  async leaveGame(ws, username) {
    try {
      if (this.isDestroyed) { this.safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!username?.trim()) { this.safeSend(ws, ["gameLowCardError", "Username is required"]); return; }
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname;
      if (!room) { this.safeSend(ws, ["gameLowCardError", "Please switch to a room first"]); return; }
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) { this.safeSend(ws, ["gameLowCardError", "No active game in this room"]); return; }
      if (!game.players.has(usernameClean)) { this.safeSend(ws, ["gameLowCardError", "You are not in this game"]); return; }
      await this._markPlayerLeft(room, usernameClean);
    } catch(e) {}
  }

  _scheduleGameCleanup(room, game) {
    try {
      if (!room || !game) return;
      if (this._cleanupTimers.has(room)) { this._clearTimer(this._cleanupTimers.get(room)); this._cleanupTimers.delete(room); }
      if (!game._gameEnded) return;
      const timer = this._trackTimer(setTimeout(() => {
        const currentGame = this.activeGames.get(room);
        if (currentGame?._isActive && !currentGame._gameEnded) { this._cleanupTimers.delete(room); return; }
        this._cleanupTimers.delete(room);
        const gameToDelete = this.activeGames.get(room);
        if (gameToDelete) this._deleteGame(room, gameToDelete);
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS || 5000));
      this._cleanupTimers.set(room, timer);
    } catch(e) {}
  }

  _deleteGame(room, game) {
    try {
      if (!room || !game) return;
      if (game?._isActive && !game._gameEnded) return;
      if (this._cleanupTimers.has(room)) { this._clearTimer(this._cleanupTimers.get(room)); this._cleanupTimers.delete(room); }
      if (game) {
        game._gameEnded = true;
        game._isActive = false;
        game.playerWsId = null;
        this._cleanupGame(game);
      }
      this.activeGames.delete(room);
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this.broadcast(room, ["gameLowCardEnd", []]);
    } catch(e) {}
  }

  _cleanupGame(game) {
    try {
      if (!game) return;
      if (game._isActive && !game._gameEnded) return;
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) { if (game[key]) { this._clearTimer(game[key]); game[key] = null; } }
      if (game._botTimeouts) { for (const id of game._botTimeouts) this._clearTimer(id); game._botTimeouts.clear(); game._botTimeouts = null; }
      game.players = null;
      game.botPlayers = null;
      game.numbers = null;
      game.tanda = null;
      game.eliminated = null;
      game.playerWsId = null;
      game._isActive = false;
      game._gameEnded = true;
      game._isEvaluating = false;
    } catch(e) {}
  }

  async _forceCleanupGame(room, game) {
    try {
      if (!game) return;
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) { if (game[key]) { this._clearTimer(game[key]); game[key] = null; } }
      if (game._botTimeouts) { for (const id of game._botTimeouts) this._clearTimer(id); game._botTimeouts.clear(); }
      game._gameEnded = true;
      game._isActive = false;
      game._endTime = Date.now();
      this.broadcast(room, ["gameLowCardEnd", []]);
      this.activeGames.delete(room);
      if (this._cleanupTimers.has(room)) { this._clearTimer(this._cleanupTimers.get(room)); this._cleanupTimers.delete(room); }
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
    } catch(e) {}
  }

  // ============================================================
  // DESTROY
  // ============================================================

  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      for (const timer of this._allTimers) { try { clearTimeout(timer); } catch(e) {} }
      this._allTimers.clear();
      if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
      if (this._diceCooldownTimer) { clearTimeout(this._diceCooldownTimer); this._diceCooldownTimer = null; }
      if (this._diceTimeUpCooldownTimer) { clearTimeout(this._diceTimeUpCooldownTimer); this._diceTimeUpCooldownTimer = null; }
      for (const t of this._diceNotificationTimeouts) { clearTimeout(t); }
      this._diceNotificationTimeouts = [];
      if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
      if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
      for (const t of this._tieNotificationTimeouts) { clearTimeout(t); }
      this._tieNotificationTimeouts = [];
      for (const [room, game] of this.activeGames) {
        if (game._isActive && !game._gameEnded) {
          game._gameEnded = true;
          game._isActive = false;
          this.broadcast(room, ["gameLowCardEnd", ["Server shutting down"]]);
        }
        this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();
      this._eventQueue = [];
      this._processingQueue = false;
      this.userConnections.clear();
      this.roomClients.clear();
      this._tieBreakers.clear();
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._cleanupTimers.clear();
      if (this.alarmScheduler) { await this.alarmScheduler.cleanup(); }
      for (const ws of this.wsSet) {
        try { if (ws && ws.readyState === 1) ws.close(1000, "Server shutting down"); } catch(e) {}
      }
      this.wsSet.clear();
      try { await this.ctx.storage.deleteAlarm(); } catch(e) {}
    } catch(e) {}
  }
}

export default GameServer;
