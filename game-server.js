// ==================== GAME-SERVER-FULL.js ====================
// VERSION: 7.0.0 - STORAGE FIRST, THEN CACHE & KV

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
  LOWCARD_WINNER_KEY: 'lowcard_winner_',
  LOWCARD_RECORDING_KEY: 'lowcard_recording_status_',
  
  DICE_ANSWER_TIME_MS: 20000,
  DICE_TOTAL_TIME_MS: 20000,
  MAX_DICE_VALUE: 6,
  DICE_ROOM: "Quiz",
  DICE_POINT_KEY: 'dice_points',
  DICE_LAST_WEEK_WINNER: 'dice_last_week_winner',
  
  DICE_AUTO_START_DELAY_MS: 3000,
  TIE_BREAKER_TIME_LIMIT: 20,
  TIE_BREAKER_COOLDOWN: 15000,
  
  KV_TIMEOUT_MS: 1500,
  BROADCAST_BATCH_SIZE: 10,
  CPU_YIELD_MS: 1,
  
  MAX_PROCESS_TIME_MS: 500,
  EVENT_BATCH_SIZE: 1,
  MAX_QUEUE_SIZE: 50,
  HAND_SHAKE_TIMEOUT_MS: 3000,
  RATE_LIMIT_MAX: 100,
  RATE_LIMIT_WINDOW_MS: 60000,
  MAX_BOT_TIMEOUTS: 5,
  MAX_EVENT_ITERATIONS: 2,
  MAP_CLEANUP_AGE_MS: 1800000,
  MAX_RECONNECT_ATTEMPTS: 5,
  RECONNECT_WINDOW_MS: 30000,
  
  CACHE_TTL_MS: 60000,
  
  ALARM_STATE_KEY: 'alarm_state',
  
  WEEKLY_RESET_DAY: 1,
  WEEKLY_RESET_HOUR: 0,
  WEEKLY_RESET_ALARM: 'weekly_reset',
  
  STORAGE_WINNER_KEY: 'cachedLastWeekWinner',
  STORAGE_POINTS_KEY: 'dicePointsBackup',
  STORAGE_RECORDING_KEY: 'recordingStatusMap',
  STORAGE_WINNERS_MAP_KEY: 'winnersMap',
};

const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: "01:00", end: "02:00" },
    { start: "13:00", end: "14:00" },
    { start: "22:00", end: "23:00" }
  ],
  TIMEZONE_OFFSET: 8,
};

function parseTime(timeStr) {
  const [hours, minutes] = timeStr.split(':').map(Number);
  return hours * 60 + minutes;
}

// ============================================================
// ALARM SCHEDULER
// ============================================================
class AlarmScheduler {
  constructor(env, state) {
    this.env = env;
    this.state = state;
    this._alarms = new Map();
  }

  async scheduleAlarms() {
    try {
      const now = new Date();
      const witaNow = this._toWITA(now);
      const currentTotal = witaNow.getHours() * 60 + witaNow.getMinutes();
      
      await this._clearAllAlarms();
      await this._scheduleWeeklyReset();
      
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
        if (endDelay > 0) {
          await this._scheduleAlarm('dice_session_end', endDelay);
        }
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
        if (endDelay > 0) {
          await this._scheduleAlarm('dice_session_end', endDelay);
        }
      }
      
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async _scheduleWeeklyReset() {
    try {
      const now = new Date();
      const currentDay = now.getUTCDay();
      const currentHour = now.getUTCHours();
      const currentMinutes = now.getUTCMinutes();
      
      let daysUntilReset = CONSTANTS.WEEKLY_RESET_DAY - currentDay;
      if (daysUntilReset < 0) daysUntilReset += 7;
      if (daysUntilReset === 0 && (currentHour > 0 || currentMinutes > 0)) {
        daysUntilReset = 7;
      }
      
      const resetTime = new Date(now);
      resetTime.setUTCDate(resetTime.getUTCDate() + daysUntilReset);
      resetTime.setUTCHours(CONSTANTS.WEEKLY_RESET_HOUR, 0, 0, 0);
      
      const delayMs = resetTime.getTime() - now.getTime();
      
      if (delayMs > 0 && delayMs < 7 * 24 * 60 * 60 * 1000) {
        await this._scheduleAlarm(CONSTANTS.WEEKLY_RESET_ALARM, delayMs);
        return true;
      }
      
      return false;
    } catch(e) {
      return false;
    }
  }

  async _scheduleAlarm(name, delayMs) {
    try {
      if (delayMs < 1000) delayMs = 1000;
      if (delayMs > 86400000) delayMs = 86400000;
      
      const alarm = { 
        name, 
        scheduledAt: Date.now() + delayMs, 
        delayMs,
        timestamp: Date.now()
      };
      
      this._alarms.set(name, alarm);
      
      if (this.state) {
        await this.state.storage.setAlarm(Date.now() + delayMs);
      }
      
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async _clearAllAlarms() {
    try {
      this._alarms.clear();
      if (this.state) {
        try {
          await this.state.storage.setAlarm(null);
        } catch(e) {}
      }
    } catch(e) {}
  }

  async getPendingAlarms() {
    try {
      const pending = [];
      const now = Date.now();
      const expired = [];
      
      for (const [name, alarm] of this._alarms) {
        if (alarm.scheduledAt <= now) {
          pending.push(alarm);
          expired.push(name);
        }
      }
      
      for (const name of expired) {
        this._alarms.delete(name);
      }
      
      return pending;
    } catch(e) { 
      return []; 
    }
  }

  async processAlarm(name) {
    try {
      const alarm = this._alarms.get(name);
      if (!alarm) return null;
      
      this._alarms.delete(name);
      
      let minDelay = Infinity;
      const now = Date.now();
      for (const [n, a] of this._alarms) {
        const remaining = a.scheduledAt - now;
        if (remaining > 0 && remaining < minDelay) {
          minDelay = remaining;
        }
      }
      
      if (minDelay > 0 && minDelay < Infinity) {
        await this.state.storage.setAlarm(Date.now() + minDelay);
      }
      
      return alarm;
    } catch(e) {
      return null;
    }
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
      if (currentTotal >= startTotal && currentTotal < endTotal) {
        return true;
      }
    }
    return false;
  }

  async cleanup() {
    await this._clearAllAlarms();
  }

  async restoreAlarms() {
    try {
      await this._clearAllAlarms();
      await this.scheduleAlarms();
      return true;
    } catch(e) { return false; }
  }
}

// ============================================================
// KV CACHE
// ============================================================
class KVCache {
  constructor() {
    this.cache = new Map();
  }
  get(key) { const entry = this.cache.get(key); return entry ? entry.value : null; }
  set(key, value) { this.cache.set(key, { value }); }
  delete(key) { this.cache.delete(key); }
  clear() { this.cache.clear(); }
  has(key) { return this.cache.has(key); }
}

// ============================================================
// CACHE MANAGER
// ============================================================
class CacheManager {
  constructor() {
    this.recordingStatus = new Map();
    this.winnersCache = new Map();
    this._updateLocks = new Map();
  }

  getRecordingStatus(room) {
    if (!room) return false;
    return this.recordingStatus.has(room) ? this.recordingStatus.get(room) : false;
  }

  async setRecordingStatus(room, enabled, env) {
    if (!room || !env?.QUESTIONS) return false;
    const lockKey = `recording_${room}`;
    if (this._updateLocks.has(lockKey)) return false;
    this._updateLocks.set(lockKey, Date.now());
    try {
      this.recordingStatus.set(room, enabled);
      const key = CONSTANTS.LOWCARD_RECORDING_KEY + room;
      if (enabled) {
        await env.QUESTIONS.put(key, 'true');
      } else {
        await env.QUESTIONS.delete(key);
        this.winnersCache.delete(room);
      }
      return true;
    } catch(e) {
      this.recordingStatus.delete(room);
      return false;
    } finally {
      this._updateLocks.delete(lockKey);
    }
  }

  getWinners(room) {
    if (!room) return {};
    const cached = this.winnersCache.get(room);
    if (cached) return cached.winners;
    return {};
  }

  async addWinner(room, username, env) {
    if (!room || !username || room === CONSTANTS.DICE_ROOM) return false;
    if (!this.getRecordingStatus(room)) return false;
    if (!env?.QUESTIONS) return false;
    const lockKey = `winner_${room}`;
    if (this._updateLocks.has(lockKey)) return false;
    this._updateLocks.set(lockKey, Date.now());
    try {
      let roomWinners = this.getWinners(room);
      let count = 0;
      if (roomWinners[username]) {
        count = parseInt(String(roomWinners[username]).replace("x", "").replace("X", "")) || 0;
      }
      roomWinners[username] = (count + 1) + "x";
      this.winnersCache.set(room, { winners: roomWinners });
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      await env.QUESTIONS.put(key, JSON.stringify(roomWinners));
      return true;
    } catch(e) {
      await this._reloadWinnersFromKV(room, env);
      return false;
    } finally {
      this._updateLocks.delete(lockKey);
    }
  }

  async deleteAllWinners(room, env) {
    if (!room || !env?.QUESTIONS) return false;
    const lockKey = `winner_delete_${room}`;
    if (this._updateLocks.has(lockKey)) return false;
    this._updateLocks.set(lockKey, Date.now());
    try {
      this.winnersCache.delete(room);
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      await env.QUESTIONS.delete(key);
      return true;
    } catch(e) {
      await this._reloadWinnersFromKV(room, env);
      return false;
    } finally {
      this._updateLocks.delete(lockKey);
    }
  }

  async _reloadWinnersFromKV(room, env) {
    try {
      if (!env?.QUESTIONS) return;
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      const winners = await env.QUESTIONS.get(key, 'json') || {};
      this.winnersCache.set(room, { winners });
    } catch(e) {}
  }

  clear() {
    this.recordingStatus.clear();
    this.winnersCache.clear();
    this._updateLocks.clear();
  }
}

// ============================================================
// DICE POINTS CACHE
// ============================================================
class DicePointsCache {
  constructor() {
    this.pointsCache = new Map();
    this.leaderboardCache = new Map();
    this._updateLocks = new Map();
  }

  getPoints() {
    const cached = this.pointsCache.get('points');
    if (cached) return cached.data;
    return null;
  }

  async setPoints(points, env) {
    if (!env?.QUESTIONS) return false;
    const lockKey = 'dice_points_update';
    if (this._updateLocks.has(lockKey)) return false;
    this._updateLocks.set(lockKey, Date.now());
    try {
      this.pointsCache.set('points', { data: points });
      this.leaderboardCache.delete('leaderboard');
      await env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      return true;
    } catch(e) {
      this.pointsCache.delete('points');
      return false;
    } finally {
      this._updateLocks.delete(lockKey);
    }
  }

  async addPointToUser(username, env) {
    if (!username || !env?.QUESTIONS) return false;
    const lockKey = `dice_point_${username}`;
    if (this._updateLocks.has(lockKey)) return false;
    this._updateLocks.set(lockKey, Date.now());
    try {
      let points = this.getPoints();
      if (!points) {
        return false;
      }
      points[username] = (points[username] || 0) + 1;
      this.pointsCache.set('points', { data: points });
      this.leaderboardCache.delete('leaderboard');
      await env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      return points[username];
    } catch(e) {
      return false;
    } finally {
      this._updateLocks.delete(lockKey);
    }
  }

  getLeaderboard(limit = 10) {
    const cached = this.leaderboardCache.get('leaderboard');
    if (cached) return cached.data.slice(0, limit);
    
    const points = this.getPoints();
    if (points) {
      const sorted = Object.entries(points).sort((a, b) => b[1] - a[1]);
      this.leaderboardCache.set('leaderboard', { data: sorted });
      return sorted.slice(0, limit);
    }
    return [];
  }

  async resetPoints(env) {
    if (!env?.QUESTIONS) return false;
    const lockKey = 'dice_points_reset';
    if (this._updateLocks.has(lockKey)) return false;
    this._updateLocks.set(lockKey, Date.now());
    try {
      this.pointsCache.delete('points');
      this.leaderboardCache.delete('leaderboard');
      await env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify({}));
      return true;
    } catch(e) { return false; }
    finally { this._updateLocks.delete(lockKey); }
  }

  async loadFromKV(env) {
    try {
      if (!env?.QUESTIONS) return false;
      const points = await env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.pointsCache.set('points', { data: points });
      this.leaderboardCache.delete('leaderboard');
      return true;
    } catch(e) { return false; }
  }

  clear() {
    this.pointsCache.clear();
    this.leaderboardCache.clear();
    this._updateLocks.clear();
  }
}

// ============================================================
// DICE GAME SYSTEM
// ============================================================
class DiceGameSystem {
  constructor(gameServer) {
    this.gameServer = gameServer;
    this.env = gameServer.env;
    this.userScores = new Map();
    this._isLoaded = false;
    this.pointsCache = new DicePointsCache();
  }

  async getPoints() {
    try {
      let points = this.pointsCache.getPoints();
      if (points) {
        this.userScores.clear();
        for (const [username, score] of Object.entries(points)) {
          this.userScores.set(username, score);
        }
        return points;
      }
      return {};
    } catch(e) { return {}; }
  }

  async setPoints(points) {
    try {
      if (!this.env?.QUESTIONS) return false;
      const success = await this.pointsCache.setPoints(points, this.env);
      if (success) {
        this.userScores.clear();
        for (const [username, score] of Object.entries(points)) {
          this.userScores.set(username, score);
        }
      }
      return success;
    } catch(e) { return false; }
  }

  async addPoint(username) {
    try {
      if (!this.env?.QUESTIONS) return false;
      const newScore = await this.pointsCache.addPointToUser(username, this.env);
      if (newScore !== false) {
        this.userScores.set(username, newScore);
        return true;
      }
      return false;
    } catch(e) { return false; }
  }

  getLeaderboard(limit = 10) {
    return this.pointsCache.getLeaderboard(limit);
  }

  async resetPoints() {
    try {
      if (!this.env?.QUESTIONS) return false;
      return await this.pointsCache.resetPoints(this.env);
    } catch(e) { return false; }
  }

  async loadScores() {
    try {
      if (this._isLoaded) return true;
      if (!this.env?.QUESTIONS) return false;
      await this.pointsCache.loadFromKV(this.env);
      await this.getPoints();
      this._isLoaded = true;
      return true;
    } catch(e) { return false; }
  }

  async deleteLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return false;
      this.gameServer._cachedLastWeekWinner = null;
      await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
      return true;
    } catch(e) { return false; }
  }

  rollDice() { return Math.floor(Math.random() * 6) + 1; }
  
  clearCache() {
    this.userScores.clear();
    this._isLoaded = false;
    if (this.pointsCache) {
      this.pointsCache.clear();
    }
  }
}

// ============================================================
// ==================== GAME SERVER CLASS ====================
// ============================================================

export class GameServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      this.ctx = state;
      this.closing = false;
      this.isDestroyed = false;
      this._initialized = false;
      this._startTime = Date.now();
      this._wsIdCounter = 0;
      this._deployResetDone = false;
      
      this.cacheManager = new CacheManager();
      this.diceGameSystem = new DiceGameSystem(this);
      this.alarmScheduler = new AlarmScheduler(env, state);
      this._cachedLastWeekWinner = null;
      this._kvCache = new KVCache();
      
      this.activeGames = new Map();
      this.wsMap = new Map();
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.userConnections = new Map();
      this._eventQueue = [];
      this._allTimers = new Set();
      this._lastNotifTime = {};
      
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
      
      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._tieLock = false;
      this._tieNotificationTimeouts = [];
      
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._cleanupTimers = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();
      this._evaluationLocks = new Map();
      this._gameOperationLocks = new Map();
      this._drawLocks = new Map();
      this._cleanupLocks = new Map();
      
      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      this._reconnectAttempts = new Map();
      
      this.DICE_ROOM = CONSTANTS.DICE_ROOM;
      
      this._loadAllData().then(() => {
        this._deployResetDone = true;
        this.alarmScheduler.scheduleAlarms().catch(() => {});
        this._initLazy();
      });
      
    } catch(e) {}
  }

  // ============================================================
  // STORAGE & KV FUNCTIONS
  // ============================================================
  
  async _saveToStorage(key, data) {
    try {
      if (!this.ctx?.storage) return false;
      await this.ctx.storage.put(key, data);
      return true;
    } catch(e) { return false; }
  }

  async _saveToKV(key, data) {
    try {
      if (!this.env?.QUESTIONS) return false;
      if (data === null || data === undefined) {
        await this.env.QUESTIONS.delete(key);
        return true;
      }
      await this.env.QUESTIONS.put(key, JSON.stringify(data));
      return true;
    } catch(e) { return false; }
  }

  async _getFromStorage(key) {
    try {
      if (!this.ctx?.storage) return null;
      const data = await this.ctx.storage.get(key);
      if (data !== undefined && data !== null) return data;
      return null;
    } catch(e) { return null; }
  }

  async _getFromKV(key) {
    try {
      if (!this.env?.QUESTIONS) return null;
      const data = await this.env.QUESTIONS.get(key, 'json');
      if (data !== null && data !== undefined) return data;
      return null;
    } catch(e) { return null; }
  }

  // ============================================================
  // SAVE ALL DATA - SIMPAN KE 3 TEMPAT
  // ============================================================

  async _saveAllData() {
    try {
      // 1. SIMPAN WINNER
      if (this._cachedLastWeekWinner) {
        await this._saveToStorage(CONSTANTS.STORAGE_WINNER_KEY, this._cachedLastWeekWinner);
        await this._saveToKV(CONSTANTS.DICE_LAST_WEEK_WINNER, this._cachedLastWeekWinner);
      } else {
        await this._saveToStorage(CONSTANTS.STORAGE_WINNER_KEY, null);
        await this._saveToKV(CONSTANTS.DICE_LAST_WEEK_WINNER, null);
      }
      
      // 2. SIMPAN POINTS
      const points = this.diceGameSystem.pointsCache.getPoints();
      if (points && Object.keys(points).length > 0) {
        await this._saveToStorage(CONSTANTS.STORAGE_POINTS_KEY, points);
        await this._saveToKV(CONSTANTS.DICE_POINT_KEY, points);
      } else {
        await this._saveToStorage(CONSTANTS.STORAGE_POINTS_KEY, {});
        await this._saveToKV(CONSTANTS.DICE_POINT_KEY, {});
      }
      
      // 3. SIMPAN RECORDING STATUS
      const recordingStatusMap = {};
      for (const [room, status] of this.cacheManager.recordingStatus) {
        if (status === true) {
          recordingStatusMap[room] = status;
          await this._saveToKV(CONSTANTS.LOWCARD_RECORDING_KEY + room, 'true');
        }
      }
      if (Object.keys(recordingStatusMap).length > 0) {
        await this._saveToStorage(CONSTANTS.STORAGE_RECORDING_KEY, recordingStatusMap);
      } else {
        await this._saveToStorage(CONSTANTS.STORAGE_RECORDING_KEY, null);
      }
      
      // 4. SIMPAN WINNERS MAP
      const winnersMap = {};
      for (const [room, data] of this.cacheManager.winnersCache) {
        if (data && Object.keys(data.winners).length > 0) {
          winnersMap[room] = data.winners;
          await this._saveToKV(CONSTANTS.LOWCARD_WINNER_KEY + room, data.winners);
        }
      }
      if (Object.keys(winnersMap).length > 0) {
        await this._saveToStorage(CONSTANTS.STORAGE_WINNERS_MAP_KEY, winnersMap);
      } else {
        await this._saveToStorage(CONSTANTS.STORAGE_WINNERS_MAP_KEY, null);
      }
      
      return true;
    } catch(e) { return false; }
  }

  // ============================================================
  // LOAD ALL DATA - STORAGE FIRST, THEN KV, THEN CACHE
  // ============================================================

  async _loadAllData() {
    try {
      // ===== 1. LOAD WINNER - STORAGE FIRST =====
      let winnerData = await this._getFromStorage(CONSTANTS.STORAGE_WINNER_KEY);
      
      if (!winnerData) {
        winnerData = await this._getFromKV(CONSTANTS.DICE_LAST_WEEK_WINNER);
        if (winnerData && winnerData.username) {
          await this._saveToStorage(CONSTANTS.STORAGE_WINNER_KEY, winnerData);
        }
      }
      
      if (winnerData && winnerData.username) {
        this._cachedLastWeekWinner = winnerData;
      } else {
        this._cachedLastWeekWinner = null;
      }
      
      // ===== 2. LOAD POINTS - STORAGE FIRST =====
      let points = await this._getFromStorage(CONSTANTS.STORAGE_POINTS_KEY);
      
      if (!points || Object.keys(points).length === 0) {
        points = await this._getFromKV(CONSTANTS.DICE_POINT_KEY);
        if (points && Object.keys(points).length > 0) {
          await this._saveToStorage(CONSTANTS.STORAGE_POINTS_KEY, points);
        }
      }
      
      if (points && Object.keys(points).length > 0) {
        await this.diceGameSystem.pointsCache.setPoints(points, this.env);
        await this.diceGameSystem.getPoints();
      } else {
        const emptyPoints = {};
        await this.diceGameSystem.pointsCache.setPoints(emptyPoints, this.env);
        await this._saveToStorage(CONSTANTS.STORAGE_POINTS_KEY, emptyPoints);
      }
      
      // ===== 3. LOAD RECORDING STATUS - STORAGE FIRST =====
      let recordingMap = await this._getFromStorage(CONSTANTS.STORAGE_RECORDING_KEY);
      
      if (recordingMap) {
        for (const [room, status] of Object.entries(recordingMap)) {
          if (status === true) {
            this.cacheManager.recordingStatus.set(room, true);
          }
        }
      } else {
        try {
          const keys = await this.env.QUESTIONS.list({ prefix: CONSTANTS.LOWCARD_RECORDING_KEY });
          const newRecordingMap = {};
          
          for (const key of keys.keys) {
            const room = key.name.replace(CONSTANTS.LOWCARD_RECORDING_KEY, '');
            const status = await this.env.QUESTIONS.get(key.name);
            if (status === 'true') {
              this.cacheManager.recordingStatus.set(room, true);
              newRecordingMap[room] = true;
            }
          }
          
          if (Object.keys(newRecordingMap).length > 0) {
            await this._saveToStorage(CONSTANTS.STORAGE_RECORDING_KEY, newRecordingMap);
          }
        } catch(e) {}
      }
      
      // ===== 4. LOAD WINNERS MAP - STORAGE FIRST =====
      let winnersMap = await this._getFromStorage(CONSTANTS.STORAGE_WINNERS_MAP_KEY);
      
      if (winnersMap) {
        for (const [room, winners] of Object.entries(winnersMap)) {
          if (Object.keys(winners).length > 0) {
            this.cacheManager.winnersCache.set(room, { winners });
          }
        }
      } else {
        try {
          const keys = await this.env.QUESTIONS.list({ prefix: CONSTANTS.LOWCARD_WINNER_KEY });
          const newWinnersMap = {};
          
          for (const key of keys.keys) {
            const room = key.name.replace(CONSTANTS.LOWCARD_WINNER_KEY, '');
            const winners = await this.env.QUESTIONS.get(key.name, 'json');
            if (winners && Object.keys(winners).length > 0) {
              this.cacheManager.winnersCache.set(room, { winners });
              newWinnersMap[room] = winners;
            }
          }
          
          if (Object.keys(newWinnersMap).length > 0) {
            await this._saveToStorage(CONSTANTS.STORAGE_WINNERS_MAP_KEY, newWinnersMap);
          }
        } catch(e) {}
      }
      
      return true;
    } catch(e) { 
      return false; 
    }
  }

  // ============================================================
  // CORE FUNCTION: SIMPAN LANGSUNG KE 3 TEMPAT
  // ============================================================
  async _saveToAll(points, winnerData = null) {
    try {
      // 1️⃣ SIMPAN KE CACHE (Memory)
      if (points !== undefined && points !== null) {
        this.diceGameSystem.pointsCache.set('points', { data: points });
        this.diceGameSystem.userScores.clear();
        for (const [username, score] of Object.entries(points)) {
          this.diceGameSystem.userScores.set(username, score);
        }
      }
      
      // 2️⃣ SIMPAN KE KV (LANGSUNG!)
      if (points !== undefined && points !== null) {
        await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      }
      if (winnerData) {
        await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winnerData));
      }
      
      // 3️⃣ SIMPAN KE STORAGE (LANGSUNG!)
      if (points !== undefined && points !== null) {
        await this.ctx.storage.put(CONSTANTS.STORAGE_POINTS_KEY, points);
      }
      if (winnerData) {
        await this.ctx.storage.put(CONSTANTS.STORAGE_WINNER_KEY, winnerData);
      }
      
      return true;
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // TAMBAH POINT - LANGSUNG SIMPAN KE 3 TEMPAT
  // ============================================================
  async _addPointAndSave(username) {
    try {
      if (!username || !this.env?.QUESTIONS) return false;
      
      let points = this.diceGameSystem.pointsCache.getPoints() || {};
      points[username] = (points[username] || 0) + 1;
      
      await this._saveToAll(points, null);
      
      this.diceGameSystem.pointsCache.leaderboardCache.delete('leaderboard');
      
      return points[username];
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // FORCE WINNER - LANGSUNG SIMPAN KE 3 TEMPAT
  // ============================================================
  async _forceWinnerAndSave() {
    try {
      const points = this.diceGameSystem.pointsCache.getPoints() || {};
      
      let winner = null;
      let highestScore = 0;
      
      for (const [username, score] of Object.entries(points)) {
        const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
        if (numericScore > highestScore) {
          highestScore = numericScore;
          winner = username;
        }
      }
      
      if (winner && highestScore > 0) {
        const currentWeek = this._getCurrentWeek();
        const winnerData = { 
          username: winner, 
          score: highestScore, 
          week: currentWeek,
          timestamp: Date.now() 
        };
        
        this._cachedLastWeekWinner = winnerData;
        
        const emptyPoints = {};
        this.diceGameSystem.pointsCache.set('points', { data: emptyPoints });
        this.diceGameSystem.userScores.clear();
        
        await this._saveToAll(emptyPoints, winnerData);
        
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceLastWeekWinner", winner, highestScore, currentWeek]);
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", `🏆 Weekly Winner: ${winner} with ${highestScore} points!`]);
        
        return { success: true, winner: winnerData };
      }
      
      return { success: false, message: "No winner found" };
    } catch(e) {
      return { success: false, error: e.message };
    }
  }

  // ============================================================
  // RESET POINTS - LANGSUNG SIMPAN KE 3 TEMPAT
  // ============================================================
  async _resetPointsAndSave() {
    try {
      const emptyPoints = {};
      
      this.diceGameSystem.pointsCache.set('points', { data: emptyPoints });
      this.diceGameSystem.userScores.clear();
      this.diceGameSystem.pointsCache.leaderboardCache.delete('leaderboard');
      
      await this._saveToAll(emptyPoints, null);
      
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["dicePoints", {}]);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "🗑️ All points have been reset!"]);
      
      return { success: true, message: "Points cleared from Cache, Storage & KV" };
    } catch(e) {
      return { success: false, error: e.message };
    }
  }

  // ============================================================
  // DELETE WINNER - LANGSUNG SIMPAN KE 3 TEMPAT
  // ============================================================
  async _deleteWinnerAndSave() {
    try {
      this._cachedLastWeekWinner = null;
      
      await this._saveToAll(null, null);
      await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
      await this.ctx.storage.delete(CONSTANTS.STORAGE_WINNER_KEY);
      
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "Last week winner deleted"]);
      
      return { success: true, message: "Winner deleted from Storage & KV" };
    } catch(e) {
      return { success: false, error: e.message };
    }
  }

  // ============================================================
  // SET MANUAL WINNER - LANGSUNG SIMPAN KE 3 TEMPAT
  // ============================================================
  async _setManualWinnerAndSave(username, score) {
    try {
      if (!username) {
        return { success: false, error: "Username required" };
      }
      
      const currentWeek = this._getCurrentWeek();
      const winnerData = { 
        username, 
        score: parseInt(score) || 1, 
        week: currentWeek,
        timestamp: Date.now() 
      };
      
      this._cachedLastWeekWinner = winnerData;
      
      await this._saveToAll(null, winnerData);
      
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceLastWeekWinner", username, score, currentWeek]);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", `🏆 Manual Winner: ${username} with ${score} points!`]);
      
      return { success: true, winner: winnerData };
    } catch(e) {
      return { success: false, error: e.message };
    }
  }

  // ============================================================
  // FULL CLEANUP - LANGSUNG HAPUS DARI 3 TEMPAT
  // ============================================================
  async _fullCleanupAndSave() {
    try {
      const emptyPoints = {};
      
      this.diceGameSystem.pointsCache.set('points', { data: emptyPoints });
      this.diceGameSystem.userScores.clear();
      this.diceGameSystem.pointsCache.leaderboardCache.delete('leaderboard');
      this._cachedLastWeekWinner = null;
      this.cacheManager.clear();
      this._kvCache.clear();
      
      await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(emptyPoints));
      await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
      await this.ctx.storage.put(CONSTANTS.STORAGE_POINTS_KEY, emptyPoints);
      await this.ctx.storage.delete(CONSTANTS.STORAGE_WINNER_KEY);
      await this.ctx.storage.delete(CONSTANTS.STORAGE_RECORDING_KEY);
      await this.ctx.storage.delete(CONSTANTS.STORAGE_WINNERS_MAP_KEY);
      
      for (const [room, status] of this.cacheManager.recordingStatus) {
        if (status) {
          await this.cacheManager.setRecordingStatus(room, false, this.env);
        }
      }
      for (const [room] of this.cacheManager.winnersCache) {
        await this.cacheManager.deleteAllWinners(room, this.env);
      }
      
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["dicePoints", {}]);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "🧹 All data has been cleaned!"]);
      
      return { success: true, message: "All data cleared from Storage, KV & Cache" };
    } catch(e) {
      return { success: false, error: e.message };
    }
  }

  // ============================================================
  // DEPLOY HOOK
  // ============================================================
  async _deployResetAndLoadFromKV() {
    try {
      await this.ctx.storage.delete(CONSTANTS.STORAGE_WINNER_KEY);
      await this.ctx.storage.delete(CONSTANTS.STORAGE_POINTS_KEY);
      await this.ctx.storage.delete(CONSTANTS.STORAGE_RECORDING_KEY);
      await this.ctx.storage.delete(CONSTANTS.STORAGE_WINNERS_MAP_KEY);
      
      await this._loadAllData();
      
      if (!this.closing && !this.isDestroyed) {
        await this.alarmScheduler.restoreAlarms();
      }
      
      return true;
    } catch(e) { return false; }
  }

  // ============================================================
  // ALARM
  // ============================================================
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    try {
      await this.alarmScheduler.restoreAlarms();
      const pendingAlarms = await this.alarmScheduler.getPendingAlarms();
      
      for (const alarm of pendingAlarms) {
        try {
          switch(alarm.name) {
            case CONSTANTS.WEEKLY_RESET_ALARM:
              await this._handleWeeklyReset();
              break;
              
            case 'dice_session_start':
              if (this.alarmScheduler.isDiceTime()) {
                this.diceAutoEnabled = true;
                if (this.currentDiceRoll && this._canSubmitDiceAnswer) break;
                if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) break;
                const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
                if (clients && clients.size > 0) {
                  this._startDiceFast();
                }
              }
              break;
              
            case 'dice_session_end':
              this.diceAutoEnabled = false;
              if (this.currentDiceRoll || this._isShowingDice) {
                this._endDiceRound();
              }
              break;
          }
          
          await this.alarmScheduler.processAlarm(alarm.name);
        } catch(e) {}
      }
      
      await this.alarmScheduler.scheduleAlarms();
    } catch(e) {}
  }

  _getCurrentWeek() {
    const now = new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }

  // ============================================================
  // HANDLE WEEKLY RESET
  // ============================================================
  async _handleWeeklyReset() {
    try {
      const points = this.diceGameSystem.pointsCache.getPoints() || {};
      
      let winner = null;
      let highestScore = 0;
      
      for (const [username, score] of Object.entries(points)) {
        const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
        if (numericScore > highestScore) {
          highestScore = numericScore;
          winner = username;
        }
      }
      
      const currentWeek = this._getCurrentWeek();
      
      if (winner && highestScore > 0) {
        const winnerData = { 
          username: winner, 
          score: highestScore, 
          week: currentWeek,
          timestamp: Date.now() 
        };
        
        this._cachedLastWeekWinner = winnerData;
        await this._saveToStorage(CONSTANTS.STORAGE_WINNER_KEY, winnerData);
        await this._saveToKV(CONSTANTS.DICE_LAST_WEEK_WINNER, winnerData);
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceLastWeekWinner", winner, highestScore, currentWeek]);
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", `🏆 Weekly Winner: ${winner} with ${highestScore} points!`]);
      } else {
        this._cachedLastWeekWinner = null;
        await this._saveToStorage(CONSTANTS.STORAGE_WINNER_KEY, null);
        await this._saveToKV(CONSTANTS.DICE_LAST_WEEK_WINNER, null);
      }
      
      await this.diceGameSystem.resetPoints();
      this.diceGameSystem.clearCache();
      
      await this._saveToStorage(CONSTANTS.STORAGE_POINTS_KEY, {});
      await this._saveToKV(CONSTANTS.DICE_POINT_KEY, {});
      
    } catch(e) {}
  }

  _trackTimer(timer) {
    if (timer) this._allTimers.add(timer);
    return timer;
  }

  _clearTimer(timer) {
    if (timer) {
      if (typeof timer === 'object' && timer._destroyed) return;
      try { clearTimeout(timer); } catch(e) {}
      try { clearInterval(timer); } catch(e) {}
      this._allTimers.delete(timer);
    }
  }

  _withTimeout(promise, timeoutMs = CONSTANTS.KV_TIMEOUT_MS) {
    return Promise.race([
      promise,
      new Promise((_, reject) => {
        const timer = setTimeout(() => reject(new Error('KV timeout')), timeoutMs);
        this._trackTimer(timer);
      })
    ]);
  }

  _fireAndForget(promise) {
    promise.catch(() => {});
  }

  // ============================================================
  // FETCH - WITH ADMIN ENDPOINTS
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
      
      setTimeout(() => {
        this._requestCount = Math.max(0, this._requestCount - 50);
      }, CONSTANTS.RATE_LIMIT_WINDOW_MS);
      
      const url = new URL(req.url);
      
      // ===== ADMIN HTTP ENDPOINTS =====
      if (url.pathname === "/admin/status" && req.method === "GET") {
        return new Response(JSON.stringify({
          status: "running",
          activeGames: this.activeGames.size,
          wsClients: this.wsMap.size,
          diceActive: !!this.currentDiceRoll,
          weeklyWinner: this._cachedLastWeekWinner
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      if (url.pathname === "/admin/current-winner" && req.method === "GET") {
        try {
          const winner = await this._getFromKV(CONSTANTS.DICE_LAST_WEEK_WINNER);
          const points = await this._getFromKV(CONSTANTS.DICE_POINT_KEY) || {};
          
          return new Response(JSON.stringify({ 
            winner: winner || null,
            currentPoints: points,
            totalPlayers: Object.keys(points).length,
            storageWinner: this._cachedLastWeekWinner || null
          }), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch(e) {
          return new Response(JSON.stringify({ error: e.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // ===== FORCE WINNER VIA HTTP =====
      if (url.pathname === "/admin/force-weekly-winner" && req.method === "POST") {
        const result = await this._forceWinnerAndSave();
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // ===== RESET POINTS VIA HTTP =====
      if (url.pathname === "/admin/reset-points" && req.method === "POST") {
        const result = await this._resetPointsAndSave();
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // ===== CLEAR CACHE VIA HTTP =====
      if (url.pathname === "/admin/clear-cache" && req.method === "POST") {
        try {
          this.cacheManager.clear();
          this.diceGameSystem.clearCache();
          this._kvCache.clear();
          
          return new Response(JSON.stringify({ 
            success: true, 
            message: "Cache memory cleared" 
          }), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch(e) {
          return new Response(JSON.stringify({ 
            success: false, 
            error: e.message 
          }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // ===== FULL RESET VIA HTTP =====
      if (url.pathname === "/admin/full-reset" && req.method === "POST") {
        const result = await this._fullCleanupAndSave();
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // ===== MANUAL WINNER VIA HTTP =====
      if (url.pathname === "/admin/manual-winner" && req.method === "POST") {
        try {
          const body = await req.json();
          const result = await this._setManualWinnerAndSave(body.username, body.score);
          return new Response(JSON.stringify(result), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch(e) {
          return new Response(JSON.stringify({ 
            success: false, 
            error: e.message 
          }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // ===== DELETE LAST WEEK WINNER VIA HTTP =====
      if (url.pathname === "/admin/delete-last-week-winner" && req.method === "POST") {
        const result = await this._deleteWinnerAndSave();
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // ===== WEBSOCKET =====
      if (url.pathname === "/game/ws") {
        const upgrade = req.headers.get("Upgrade");
        if (upgrade !== "websocket") {
          return new Response("WebSocket only", { status: 400 });
        }
        
        if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
          return new Response("Server full", { status: 503 });
        }
        
        if (this._eventQueue?.length > 500) {
          return new Response("Server busy", { status: 503 });
        }
        
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        const wsId = ++this._wsIdCounter;
        
        try {
          this.ctx.acceptWebSocket(server);
        } catch(e) {
          try { server.close(1008, "Accept failed"); } catch(err) {}
          return new Response("WebSocket acceptance failed", { status: 500 });
        }
        
        server.serializeAttachment({
          wsId: wsId,
          username: null,
          room: null,
          roomname: null,
          createdAt: Date.now()
        });
        
        server._wsId = wsId;
        server._closing = false;
        server.username = null;
        server.room = null;
        server.roomname = null;
        server._createdAt = Date.now();
        
        this.wsMap.set(wsId, server);
        
        return new Response(null, { 
          status: 101, 
          webSocket: client 
        });
      }
      
      return new Response("Game Server v7.0.0 - Full Admin", { status: 200 });
      
    } catch(e) {
      this._handleError('fetch', e);
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  // ============================================================
  // WEBSOCKET EVENT HANDLERS
  // ============================================================
  
  async webSocketMessage(ws, message) {
    if (!ws || ws._closing || this.closing || this.isDestroyed) return;
    
    try {
      let attachment = null;
      try {
        attachment = ws.deserializeAttachment();
      } catch(e) {}
      
      if (attachment && attachment.wsId) {
        ws._wsId = attachment.wsId;
        ws.username = attachment.username || null;
        ws.room = attachment.room || null;
        ws.roomname = attachment.roomname || null;
        ws._createdAt = attachment.createdAt || Date.now();
        
        if (attachment.username && attachment.room) {
          this.userConnections.set(attachment.username, {
            wsId: attachment.wsId,
            ws: ws,
            room: attachment.room,
            timestamp: Date.now()
          });
        }
      }
      
      const data = JSON.parse(message);
      if (Array.isArray(data) && data.length > 0) {
        await this._processWithTimeout(ws, data);
      }
    } catch(e) {}
  }

  async webSocketClose(ws, code, reason, wasClean) {
    if (!ws) return;
    try {
      const attachment = ws.deserializeAttachment();
      const username = attachment?.username;
      const room = attachment?.room;
      const wsId = attachment?.wsId;
      
      if (username) {
        this.userConnections.delete(username);
        if (room) {
          this._broadcastToRoom(room, ["userLeftRoom", username, room]);
        }
      }
      
      if (room && wsId) {
        const clients = this.wsClients.get(room);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) {
            this.wsClients.delete(room);
          }
        }
      }
      
      if (wsId) {
        this.wsMap.delete(wsId);
        this.clientRooms.delete(wsId);
      }
      
      try {
        ws.serializeAttachment({
          wsId: wsId,
          username: null,
          room: null,
          roomname: null,
          createdAt: Date.now()
        });
      } catch(e) {}
      
    } catch(e) {}
  }

  async webSocketError(ws, error) {
    if (!ws) return;
    try {
      const attachment = ws.deserializeAttachment();
      const username = attachment?.username;
      const room = attachment?.room;
      const wsId = attachment?.wsId;
      
      if (username) {
        this.userConnections.delete(username);
        if (room) {
          this._broadcastToRoom(room, ["userLeftRoom", username, room]);
        }
      }
      
      if (room && wsId) {
        const clients = this.wsClients.get(room);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) {
            this.wsClients.delete(room);
          }
        }
      }
      
      if (wsId) {
        this.wsMap.delete(wsId);
        this.clientRooms.delete(wsId);
      }
      
      try {
        ws.serializeAttachment({
          wsId: wsId,
          username: null,
          room: null,
          roomname: null,
          createdAt: Date.now()
        });
      } catch(e) {}
      
    } catch(e) {}
  }

  // ============================================================
  // PROCESS EVENT
  // ============================================================
  
  async _processWithTimeout(ws, data, timeoutMs = 500) {
    try {
      const timeoutPromise = new Promise((_, reject) => {
        const timer = setTimeout(() => reject(new Error('Processing timeout')), timeoutMs);
        this._trackTimer(timer);
      });
      await Promise.race([
        this.handleEvent(ws, data),
        timeoutPromise
      ]);
    } catch(e) {}
  }

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      if (this._eventQueue.length > CONSTANTS.MAX_EVENT_QUEUE_SIZE) {
        this._safeSend(ws, ["gameLowCardError", "Server busy"]);
        return;
      }
      this._eventQueue.push({ ws, data });
      if (!this._isProcessingQueue) await this._processEventQueue();
    } catch(e) {}
  }

  async _processEventQueue(iteration = 0) {
    try {
      if (this._isProcessingQueue || this._eventQueue.length === 0) return;
      this._isProcessingQueue = true;
      if (iteration > CONSTANTS.MAX_EVENT_ITERATIONS) {
        this._isProcessingQueue = false;
        return;
      }
      const startTime = Date.now();
      let processed = 0;
      while (this._eventQueue.length > 0 && processed < 3) {
        if (Date.now() - startTime > CONSTANTS.MAX_PROCESS_TIME_MS) break;
        const item = this._eventQueue.shift();
        try { await this._processEventItem(item.ws, item.data); } catch(e) {}
        processed++;
      }
      if (this._eventQueue.length > 0 && iteration < CONSTANTS.MAX_EVENT_ITERATIONS) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed) {
            this._isProcessingQueue = false;
            this._processEventQueue(iteration + 1);
          }
        }, 5);
      }
    } catch(e) {
      this._handleError('processQueue', e);
    } finally {
      this._isProcessingQueue = false;
    }
  }

  async _processEventItem(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      await this._handleEventInternal(ws, data);
    } catch(e) {}
  }

  // ============================================================
  // HANDLE EVENT INTERNAL - WITH ALL ADMIN FUNCTIONS
  // ============================================================
  async _handleEventInternal(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      const evt = data[0];

      if (evt === "switchRoom") {
        await this.switchRoom(ws, data[1], data[2]);
        return;
      }

      if (evt === "submitDiceAnswer") {
        await this.submitDiceAnswer(ws, data[1], data[2]);
        return;
      }

      if (evt === "getDiceLastWeekWinner") {
        try {
          const result = this._getLastWeekWinnerAndReset();
          if (result?.username) {
            this._safeSend(ws, ["diceLastWeekWinner", result.username, result.score || 0, result.week || ""]);
          } else {
            this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
          }
        } catch(e) { 
          this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]); 
        }
        return;
      }

      if (evt === "getDiceLeaderboard") {
        try {
          let limit = data.length > 1 && typeof data[1] === 'number' ? Math.min(data[1], 30) : 10;
          
          if (!this.diceGameSystem) {
            this.diceGameSystem = new DiceGameSystem(this);
          }
          
          const result = this.getLeaderboardWithWinner(limit);
          const leaderboardData = result.leaderboard.map(([u, s]) => `${u}|${s}`);
          
          this._safeSend(ws, ["diceLeaderboard", leaderboardData]);
          
          if (result.weeklyWinner) {
            this._safeSend(ws, ["diceLastWeekWinner", 
              result.weeklyWinner.username, 
              result.weeklyWinner.score || 0, 
              result.weeklyWinner.week || ""
            ]);
          }
        } catch(e) { 
          this._safeSend(ws, ["diceLeaderboard", []]); 
        }
        return;
      }

      if (evt === "getDicePoints") {
        try {
          const points = this.diceGameSystem.pointsCache.getPoints() || {};
          this._safeSend(ws, ["dicePoints", points]);
        } catch(e) { this._safeSend(ws, ["dicePoints", {}]); }
        return;
      }

      if (evt === "getStorageStatus") {
        try {
          const storageWinner = await this._getFromStorage(CONSTANTS.STORAGE_WINNER_KEY);
          const storagePoints = await this._getFromStorage(CONSTANTS.STORAGE_POINTS_KEY);
          
          const kvWinner = await this._getFromKV(CONSTANTS.DICE_LAST_WEEK_WINNER);
          const kvPoints = await this._getFromKV(CONSTANTS.DICE_POINT_KEY);
          
          const status = {
            storageCache: {
              winner: !!storageWinner,
              points: !!storagePoints,
              winnerData: storageWinner || null
            },
            kv: {
              winner: !!kvWinner,
              points: !!kvPoints,
              winnerData: kvWinner || null
            },
            winner: this._cachedLastWeekWinner || null,
            timestamp: Date.now()
          };
          
          this._safeSend(ws, ["storageStatus", status]);
        } catch(e) {
          this._safeSend(ws, ["storageStatus", { error: e.message }]);
        }
        return;
      }

      // ============================================================
      // ADMIN FUNCTIONS VIA WEBSOCKET
      // ============================================================

      // ===== FORCE WEEKLY WINNER =====
      if (evt === "forceWeeklyWinner") {
        const result = await this._forceWinnerAndSave();
        this._safeSend(ws, ["forceWeeklyWinnerResult", result]);
        return;
      }

      // ===== RESET DICE POINTS =====
      if (evt === "resetDicePoints") {
        const result = await this._resetPointsAndSave();
        this._safeSend(ws, ["resetDicePointsResult", result]);
        return;
      }

      // ===== CLEAR CACHE =====
      if (evt === "clearCache") {
        try {
          this.cacheManager.clear();
          this.diceGameSystem.clearCache();
          this._kvCache.clear();
          
          this._safeSend(ws, ["clearCacheResult", { 
            success: true, 
            message: "Cache memory cleared" 
          }]);
        } catch(e) {
          this._safeSend(ws, ["clearCacheResult", { 
            success: false, 
            error: e.message 
          }]);
        }
        return;
      }

      // ===== FULL CLEANUP =====
      if (evt === "fullCleanup") {
        const result = await this._fullCleanupAndSave();
        this._safeSend(ws, ["fullCleanupResult", result]);
        return;
      }

      // ===== SET MANUAL WINNER =====
      if (evt === "setManualWinner") {
        const username = data[1];
        const score = data[2] || 1;
        const result = await this._setManualWinnerAndSave(username, score);
        this._safeSend(ws, ["forceWeeklyWinnerResult", result]);
        return;
      }

      // ===== DELETE LAST WEEK WINNER =====
      if (evt === "deleteDiceLastWeekWinner") {
        const result = await this._deleteWinnerAndSave();
        this._safeSend(ws, ["diceLastWeekWinnerDeleted", result.success, result.message]);
        return;
      }

      // ===== GET DICE STATUS =====
      if (evt === "getDiceStatus") {
        this._safeSend(ws, ["diceStatus", !!this.currentDiceRoll && this._canSubmitDiceAnswer, this._diceRound || 1]);
        return;
      }

      // ===== GET DICE NOTIFICATION =====
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
            notification = `${Math.floor(remaining)}s remaining`;
          } else if (isDiceTime) {
            notification = "Dice game starting soon...";
          } else {
            notification = `Next dice game in: ${timeLeft.text}`;
          }
          
          this._safeSend(ws, ["diceNotification", notification]);
        } catch(e) {
          this._safeSend(ws, ["diceNotification", "Waiting..."]);
        }
        return;
      }

      // ============================================================
      // LOWCARD GAME EVENTS
      // ============================================================

      if (evt === "startRecordingWinners") {
        const roomName = data[1];
        if (!roomName) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const success = await this._startRecordingWinners(roomName);
        this._safeSend(ws, ["startRecordingResult", { success, message: success ? "Recording enabled" : "Failed" }]);
        return;
      }

      if (evt === "stopRecordingWinners") {
        const roomName = data[1];
        if (!roomName) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const success = await this._stopRecordingWinners(roomName);
        this._safeSend(ws, ["stopRecordingResult", { success, message: success ? "Recording stopped" : "Failed" }]);
        return;
      }

      if (evt === "getRecordingStatus") {
        const roomName = data[1];
        if (!roomName) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const isRecording = this.cacheManager.getRecordingStatus(roomName);
        this._safeSend(ws, ["recordingStatus", isRecording]);
        return;
      }

      if (evt === "sendWinnersToRoom" || evt === "lowCardWinnerUpdate") {
        const room = data[1] || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
        if (!room) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        await this._broadcastLowCardWinners(room);
        this._safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners refreshed" }]);
        return;
      }

      if (evt === "getRoomWinners") {
        const room = data[1] || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
        if (!room) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const isRecording = this.cacheManager.getRecordingStatus(room);
        const winners = this.cacheManager.getWinners(room);
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: isRecording }]);
        this._safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners updated" }]);
        return;
      }

      if (evt === "startGameWithRecording") {
        const [_, room, bet, username] = data;
        await this._startGameWithRecording(ws, room, bet, username);
        return;
      }

      const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room) {
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
        case "gameLowCardLeave": await this.leaveGame(ws, data[1]); break;
        case "checkGameRunning": await this.checkGameRunning(ws, data[1]); break;
        case "getGameState": this._sendGameStateToClient(ws, data[1] || room); break;
        default: break;
      }
    } catch(e) {}
  }

  // ============================================================
  // HELPER FUNCTIONS
  // ============================================================

  _getLastWeekWinnerAndReset() {
    try {
      if (this._cachedLastWeekWinner !== null && this._cachedLastWeekWinner !== undefined) {
        return this._cachedLastWeekWinner;
      }
      return null;
    } catch(e) { 
      return null; 
    }
  }

  getLeaderboardWithWinner(limit = 10) {
    try {
      const points = this.diceGameSystem.pointsCache.getPoints() || {};
      const leaderboard = Object.entries(points)
        .sort((a, b) => b[1] - a[1])
        .slice(0, limit);
      
      const lastWeekWinner = this._getLastWeekWinnerAndReset();
      if (lastWeekWinner && lastWeekWinner.username) {
        return {
          leaderboard: leaderboard,
          weeklyWinner: lastWeekWinner
        };
      }
      
      return {
        leaderboard: leaderboard,
        weeklyWinner: null
      };
    } catch(e) {
      return { leaderboard: [], weeklyWinner: null };
    }
  }

  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  _broadcastToRoom(room, message) {
    try {
      if (this.closing || this.isDestroyed || !room || !message) return;
      const wsIds = this.wsClients.get(room);
      if (!wsIds?.size) return;
      
      const isNotification = message[0] === 'diceNotification' || 
                             message[0] === 'gameLowCardTimeLeft' ||
                             message[0] === 'gameLowCardWait';
      
      if (isNotification) {
        const now = Date.now();
        const msgKey = `${room}_${message[0]}`;
        if (!this._lastNotifTime) this._lastNotifTime = {};
        if (this._lastNotifTime[msgKey] && (now - this._lastNotifTime[msgKey]) < 2000) return;
        this._lastNotifTime[msgKey] = now;
      }
      
      const msgStr = JSON.stringify(message);
      const wsIdArray = Array.from(wsIds);
      for (let i = 0; i < wsIdArray.length; i += 20) {
        const batch = wsIdArray.slice(i, i + 20);
        for (const wsId of batch) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1 && !ws._closing) {
            try { ws.send(msgStr); } catch(e) {}
          }
        }
      }
    } catch(e) {}
  }

  _handleError(type, error) {
    try {
      const now = Date.now();
      if (now - this._lastErrorReset > CONSTANTS.ERROR_RESET_INTERVAL_MS) {
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
      
      return { 
        hours, 
        minutes, 
        totalMs: minDiff * 60 * 1000,
        text: `${hours}h ${minutes}m`, 
        isRunning: isRunning 
      };
    } catch(e) {
      return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false };
    }
  }

  _getCurrentWITATime() {
    try {
      const now = new Date();
      const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
      const minutes = now.getUTCMinutes();
      return { hours, minutes, totalMinutes: (hours * 60) + minutes };
    } catch(e) { 
      return { hours: 0, minutes: 0, totalMinutes: 0 }; 
    }
  }

  _initLazy() {
    if (this._initialized || this.closing || this.isDestroyed) return;
    this._initialized = true;
    
    try {
      if (!this.diceGameSystem) {
        this.diceGameSystem = new DiceGameSystem(this);
      }
      
      this.diceGameSystem.pointsCache.loadFromKV(this.env).then(() => {
        this.diceGameSystem.getPoints();
      }).catch(() => {});
      
      this.env?.QUESTIONS?.get(CONSTANTS.DICE_LAST_WEEK_WINNER, 'json').then((data) => {
        if (data && data.username) {
          this._cachedLastWeekWinner = data;
        }
      }).catch(() => {});
      
      this.alarmScheduler.scheduleAlarms().catch(() => {});
      
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          this._checkAndStartCurrentSession();
        }
      }, 3000);
      
    } catch(e) {
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          this._initialized = false;
          this._initLazy();
        }
      }, 30000);
    }
  }

  _checkAndStartCurrentSession() {
    try {
      if (!this.alarmScheduler.isDiceTime()) return;
      
      this.diceAutoEnabled = true;
      
      if (this.currentDiceRoll && this._canSubmitDiceAnswer) return;
      if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) return;
      
      const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
      if (clients && clients.size > 0) {
        this._startDiceFast();
      }
    } catch(e) {}
  }

  // ============================================================
  // DICE FUNCTIONS
  // ============================================================
  
  _startDiceFast() {
    try {
      if (this._diceLock || this.currentDiceRoll || this._isShowingDice) return;
      this._diceLock = true;
      this._isShowingDice = true;
      
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
        value, 
        timestamp: Date.now(),
        answerTime: 20,
        canAnswerNow: true,
        round: this._diceRound
      }]);
      
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "♡ clik draw ♡"]);
      
      for (const timeout of this._diceNotificationTimeouts) {
        clearTimeout(timeout);
      }
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
      
      this._diceTimeout = this._trackTimer(setTimeout(() => {
        this._endDiceRound();
      }, 20000));
      
    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  async _endDiceRound() {
    try {
      if (this._diceTimeout) {
        clearTimeout(this._diceTimeout);
        this._diceTimeout = null;
      }
      
      for (const timeout of this._diceNotificationTimeouts) {
        clearTimeout(timeout);
      }
      this._diceNotificationTimeouts = [];
      
      this._canSubmitDiceAnswer = false;
      this._isShowingDice = false;
      
      const diceValue = this.currentDiceRoll?.value;
      const roundNumber = this._diceRound || 1;
      
      const correctPlayers = [];
      for (const player of this.diceAnswered) {
        if (this._playerAnswers.get(player) === diceValue) {
          correctPlayers.push(player);
        }
      }
      
      if (correctPlayers.length === 0) {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNoWinner", {
          message: "No winner",
          value: diceValue,
          round: roundNumber
        }]);
      } else if (correctPlayers.length === 1) {
        const winner = correctPlayers[0];
        const newScore = await this._addPointAndSave(winner);
        
        const points = this.diceGameSystem.pointsCache.getPoints() || {};
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: points[winner] || 0,
          diceValue: diceValue,
          round: roundNumber
        }]);
      } else if (correctPlayers.length > 1 && !this._tieActive) {
        this.currentDiceRoll = null;
        this._diceLock = false;
        this._isShowingDice = false;
        await this._startTieBreaker(CONSTANTS.DICE_ROOM, correctPlayers);
        return;
      }
      
      this.currentDiceRoll = null;
      this._diceLock = false;
      this._diceTimeUpCooldown = true;
      
      if (this._diceCooldownTimer) {
        clearTimeout(this._diceCooldownTimer);
      }
      this._diceCooldownTimer = setTimeout(() => {
        this._diceTimeUpCooldown = false;
        this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
        this._lastSentRemaining = -1;
        
        if (this.alarmScheduler.isDiceTime()) {
          const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
          if (clients && clients.size > 0) {
            this._startDiceFast();
          }
        }
      }, 15000);
      
    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  async _startTieBreaker(room, players) {
    if (this._tieLock) return;
    this._tieLock = true;
    try {
      if (!players || players.length < 2 || this._tieActive) return;
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
    
    for (const timeout of this._tieNotificationTimeouts) {
      clearTimeout(timeout);
    }
    this._tieNotificationTimeouts = [];
    
    this._tieRound++;
    this._tiePlayers = [...players];
    this._tieAnswers = new Map();
    data.round = this._tieRound;
    data.status = 'running';
    data.players = players;
    this._diceQuestionStartTime = Date.now();
    this._canSubmitDiceAnswer = true;
    this.diceAnswered = new Set();
    this._playerAnswers = new Map();
    this._isShowingDice = true;
    this.diceHasWinner = false;
    this.diceWinner = null;
    
    this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", 
      `♡ Tie Round ${this._tieRound}: ${players.join(', ')}`
    ]);
    
    const timeLimit = CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20;
    let isProcessed = false;
    
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "10s remaining"]);
    }, (timeLimit - 10) * 1000));
    
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "5s remaining"]);
    }, (timeLimit - 5) * 1000));
    
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "3s remaining"]);
    }, (timeLimit - 3) * 1000));
    
    this._tieTimer = this._trackTimer(setTimeout(() => {
      if (!isProcessed) {
        isProcessed = true;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "TIME UP"]);
        
        for (const timeout of this._tieNotificationTimeouts) {
          clearTimeout(timeout);
        }
        this._tieNotificationTimeouts = [];
        
        const tieId = this._getActiveTieBreakerId();
        if (tieId) this._processTieResults(room, tieId, players);
        else { this._resetTieBreakerState(null); this._startCooldownAfterTieBreaker(); }
      }
    }, (timeLimit * 1000) + 2000));
  }

  async _processTieResults(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    let highest = 0, highestPlayers = [];
    for (const player of players) {
      const answer = this._tieAnswers.get(player);
      if (answer !== undefined && answer >= 1 && answer <= 6) {
        if (answer > highest) { highest = answer; highestPlayers = [player]; }
        else if (answer === highest) { highestPlayers.push(player); }
      }
    }
    if (highestPlayers.length === 0) {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "No one answered tie breaker"]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    if (highestPlayers.length === 1) {
      const winner = highestPlayers[0];
      const newScore = await this._addPointAndSave(winner);
      
      const points = this.diceGameSystem.pointsCache.getPoints() || {};
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: points[winner] || 0,
        diceValue: highest,
        round: this._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: this._tieRound,
        finalWinner: true
      }]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    if (highestPlayers.length > 1) {
      this._tiePlayers = highestPlayers;
      this._tieAnswers = new Map();
      data.players = highestPlayers;
      data.round = this._tieRound;
      data.status = 'waiting';
      const nextTimer = setTimeout(() => {
        if (this._tieActive && this._tiePlayers.length > 1) {
          this._runTieRound(room, id, this._tiePlayers);
        } else if (this._tiePlayers.length === 1) {
          this._processSingleWinner(room, id, this._tiePlayers[0]);
        }
      }, 2000);
      this._trackTimer(nextTimer);
      return;
    }
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  async _processSingleWinner(room, id, winner) {
    try {
      const newScore = await this._addPointAndSave(winner);
      
      const points = this.diceGameSystem.pointsCache.getPoints() || {};
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: points[winner] || 0,
        diceValue: 'auto',
        round: this._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: this._tieRound,
        finalWinner: true
      }]);
    } catch(e) {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: 0,
        diceValue: 'auto',
        round: this._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: this._tieRound,
        finalWinner: true
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
      
      if (this.alarmScheduler.isDiceTime()) {
        const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
        if (clients && clients.size > 0) {
          this._startDiceFast();
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
    
    if (this._tieTimer) {
      this._clearTimer(this._tieTimer);
      this._tieTimer = null;
    }
    if (this._tieInterval) {
      this._clearTimer(this._tieInterval);
      this._tieInterval = null;
    }
    
    for (const timeout of this._tieNotificationTimeouts) {
      clearTimeout(timeout);
    }
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
      if (!this._canSubmitDiceAnswer) return;
      if (this.diceAnswered.has(username)) return;
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
      if (!this.currentDiceRoll) return;
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
    } catch(e) {}
  }

  // ============================================================
  // SWITCH ROOM
  // ============================================================
  async switchRoom(ws, room, username = null) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!room || room.trim() === "") {
        this._safeSend(ws, ["gameLowCardError", "Invalid room name"]);
        return;
      }
      
      const roomName = room.trim();
      const wsId = ws._wsId;
      
      if (!wsId) {
        this._safeSend(ws, ["gameLowCardError", "Connection error"]);
        return;
      }
      
      const currentRoom = ws.room || ws.roomname || this.clientRooms.get(wsId);
      
      if (currentRoom === roomName) {
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
        
        if (roomName === CONSTANTS.DICE_ROOM) {
          this._sendDiceNotificationOnSwitch(ws, wsId);
          this._checkAndStartDiceIfNeeded(ws);
        }
        return;
      }
      
      const lockKey = `switch_${wsId}`;
      if (this._switchLocks.has(lockKey)) {
        const retryCount = this._switchRetries.get(lockKey) || 0;
        if (retryCount > 3) {
          this._switchLocks.delete(lockKey);
          this._switchRetries.delete(lockKey);
          this._safeSend(ws, ["switchRoomError", "Switch timeout"]);
          return;
        }
        this._switchRetries.set(lockKey, retryCount + 1);
        this._safeSend(ws, ["switchRoomSuccess", currentRoom || roomName]);
        return;
      }
      
      this._switchLocks.set(lockKey, Date.now());
      this._switchRetries.set(lockKey, 0);
      
      try {
        if (currentRoom) {
          const clients = this.wsClients.get(currentRoom);
          if (clients) {
            clients.delete(wsId);
            if (clients.size === 0) {
              this.wsClients.delete(currentRoom);
            }
          }
        }
        
        if (!this.wsClients.has(roomName)) {
          this.wsClients.set(roomName, new Set());
        }
        this.wsClients.get(roomName).add(wsId);
        this.clientRooms.set(wsId, roomName);
        
        ws.room = roomName;
        ws.roomname = roomName;
        if (username) ws.username = username;
        
        ws.serializeAttachment({
          wsId: wsId,
          username: username || null,
          room: roomName,
          roomname: roomName,
          createdAt: ws._createdAt || Date.now()
        });
        
        if (username) {
          let conn = this.userConnections.get(username);
          if (conn) { 
            conn.room = roomName; 
            conn.wsId = wsId; 
            conn.ws = ws; 
            conn.timestamp = Date.now(); 
          } else { 
            this.userConnections.set(username, { 
              wsId, 
              ws, 
              room: roomName, 
              timestamp: Date.now() 
            }); 
          }
        }
        
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
        
        if (roomName === CONSTANTS.DICE_ROOM) {
          this._sendDiceNotificationOnSwitch(ws, wsId);
          this._checkAndStartDiceIfNeeded(ws);
        }
        
        this._broadcastToRoom(roomName, ["userJoinedRoom", username, roomName]);
        if (currentRoom && currentRoom !== roomName) {
          this._broadcastToRoom(currentRoom, ["userLeftRoom", username, currentRoom]);
        }
        
      } finally {
        setTimeout(() => {
          this._switchLocks.delete(lockKey);
          this._switchRetries.delete(lockKey);
        }, 2000);
      }
    } catch(e) {}
  }

  _checkAndStartDiceIfNeeded(ws) {
    try {
      if (!this.alarmScheduler.isDiceTime()) {
        const timeLeft = this._getTimeLeftUntilNextDice();
        if (timeLeft.totalMs > 0) {
          this._safeSend(ws, ["diceNotification", `Next dice game in: ${timeLeft.text}`]);
        }
        return;
      }
      
      if (this.currentDiceRoll && this._canSubmitDiceAnswer) {
        const elapsed = (Date.now() - this._diceStartTime) / 1000;
        const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
        const remaining = Math.max(0, totalTime - elapsed);
        const remainingInt = Math.floor(remaining);
        if (remainingInt > 0) {
          this._safeSend(ws, ["diceNotification", `${remainingInt}s remaining`]);
        }
        return;
      }
      
      if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) {
        if (this._diceTimeUpCooldown) {
          this._safeSend(ws, ["diceNotification", "Game in cooldown, please wait..."]);
        }
        return;
      }
      
      this._startDiceFast();
      
    } catch(e) {}
  }

  _sendDiceNotificationOnSwitch(ws, wsId) {
    try {
      if (!ws || ws.readyState !== 1) return;
      
      const isGameActive = this.currentDiceRoll && this._canSubmitDiceAnswer;
      
      if (isGameActive) {
        const elapsed = (Date.now() - this._diceStartTime) / 1000;
        const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
        const remaining = Math.max(0, totalTime - elapsed);
        const remainingInt = Math.floor(remaining);
        if (remainingInt > 0) {
          this._safeSend(ws, ["diceNotification", `${remainingInt}s remaining`]);
        }
        return;
      }
      
      const timeLeft = this._getTimeLeftUntilNextDice();
      const isDiceTime = this.alarmScheduler.isDiceTime();
      
      if (!isDiceTime || !this.diceAutoEnabled) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed && ws && ws.readyState === 1) {
            this._safeSend(ws, ["diceNotification", `Next dice game in: ${timeLeft.text}`]);
          }
        }, 5000);
        
        const waitTime = timeLeft.totalMs + 5000;
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed && !this.currentDiceRoll && !this._isShowingDice) {
            if (this.alarmScheduler.isDiceTime()) {
              const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
              if (clients && clients.size > 0) {
                this._startDiceFast();
              }
            }
          }
        }, waitTime);
        return;
      }
      
      if (isDiceTime && !this.currentDiceRoll && this.diceAutoEnabled) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed && !this.currentDiceRoll && !this._isShowingDice) {
            const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
            if (clients && clients.size > 0) {
              this._startDiceFast();
            }
          }
        }, 5000);
      }
    } catch(e) {}
  }

  _sendGameStateToClient(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room) return;
      const game = this.activeGames.get(room);
      if (!game || !game._isActive || game._gameEnded) {
        this._safeSend(ws, ["gameState", { room, hasGame: false, gameType: 'lowcard' }]);
        return;
      }
      const activePlayers = this._getActivePlayers(game);
      const allPlayers = Array.from(game.players.values()).map(p => p.name);
      const eliminated = Array.from(game.eliminated || []);
      const submitted = Array.from(game.numbers?.keys() || []);
      this._safeSend(ws, ["gameState", {
        room, hasGame: true, gameType: 'lowcard',
        isActive: game._isActive, phase: game._phase || 'registration',
        round: game.round || 1, bet: game.betAmount || 0,
        host: game.hostName || 'Unknown',
        registrationOpen: game.registrationOpen || false,
        players: allPlayers, activePlayers: activePlayers.map(p => p.name),
        eliminated, submitted, playerCount: game.players.size,
        activeCount: activePlayers.length,
        isEvaluating: game._isEvaluating || false,
        evaluationLocked: game.evaluationLocked || false,
        drawTimeExpired: game.drawTimeExpired || false
      }]);
    } catch(e) {}
  }

  // ============================================================
  // LOWCARD GAME FUNCTIONS
  // ============================================================
  
  async _startRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      
      if (this.cacheManager.getRecordingStatus(roomName)) {
        this._broadcastToRoom(roomName, ["recordingStatus", true]);
        return true;
      }
      
      this.cacheManager.recordingStatus.set(roomName, true);
      await this._saveRecordingStatus(roomName, true);
      await this._saveAllData();
      this._broadcastToRoom(roomName, ["recordingStatus", true]);
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async _stopRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      const room = roomName.trim();
      
      if (!this.cacheManager.getRecordingStatus(room)) {
        this._broadcastToRoom(room, ["recordingStatus", false]);
        return true;
      }
      
      this.cacheManager.recordingStatus.set(room, false);
      this.cacheManager.winnersCache.delete(room);
      await this._saveRecordingStatus(room, false);
      await this._saveWinners(room, {});
      await this._saveAllData();
      this._broadcastToRoom(room, ["recordingStatus", false]);
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async _saveRecordingStatus(room, enabled) {
    try {
      if (!this.env?.QUESTIONS) return false;
      const key = CONSTANTS.LOWCARD_RECORDING_KEY + room;
      if (enabled) {
        await this.env.QUESTIONS.put(key, 'true');
      } else {
        await this.env.QUESTIONS.delete(key);
      }
      return true;
    } catch(e) { return false; }
  }

  async _saveWinners(room, winners) {
    try {
      if (!this.env?.QUESTIONS) return false;
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      await this.env.QUESTIONS.put(key, JSON.stringify(winners));
      return true;
    } catch(e) { return false; }
  }

  async _broadcastLowCardWinners(room) {
    try {
      if (!room) return;
      if (!this.cacheManager.getRecordingStatus(room)) return;
      const winners = this.cacheManager.getWinners(room);
      const now = Date.now();
      const key = `broadcast_${room}`;
      if (!this._lastNotifTime) this._lastNotifTime = {};
      if (this._lastNotifTime[key] && (now - this._lastNotifTime[key]) < 500) return;
      this._lastNotifTime[key] = now;
      this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
        winners: winners || {}, room, recording: true
      }]);
    } catch(e) {}
  }

  _isGameActuallyRunning(game) { 
    return game?._isActive === true && !game?._gameEnded; 
  }

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
      return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
    } catch(e) { return []; }
  }

  _getRandomCardTanda() { 
    return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; 
  }

  _getRandomDrawDelay() { 
    return (Math.floor(Math.random() * 14) + 2) * 1000; 
  }

  _getBotNumberByRound(round) {
    if (round <= 2) return Math.floor(Math.random() * 12) + 1;
    return Math.random() < 0.6 ?
      [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
      [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
  }

  _acquireLock(lockMap, key, timeoutMs = 5000) {
    if (lockMap.has(key)) return false;
    lockMap.set(key, Date.now());
    setTimeout(() => {
      if (lockMap.has(key)) lockMap.delete(key);
    }, timeoutMs);
    return true;
  }

  _releaseLock(lockMap, key) {
    if (lockMap.has(key)) { lockMap.delete(key); return true; }
    return false;
  }

  // ============================================================
  // LOWCARD GAME METHODS (STUBS)
  // ============================================================
  
  async startGame(ws, bet, username) {
    this._safeSend(ws, ["gameLowCardError", "Start game - full implementation needed"]);
  }

  async joinGame(ws, username) {
    this._safeSend(ws, ["gameLowCardError", "Join game - full implementation needed"]);
  }

  async submitNumber(ws, number, tanda, username) {
    this._safeSend(ws, ["gameLowCardError", "Submit number - full implementation needed"]);
  }

  async leaveGame(ws, username) {
    this._safeSend(ws, ["gameLowCardError", "Leave game - full implementation needed"]);
  }

  async checkGameRunning(ws, roomname) {
    this._safeSend(ws, ["gameStatus", "false"]);
  }

  async _startGameWithRecording(ws, room, bet, username) {
    this._safeSend(ws, ["gameLowCardError", "Start with recording - full implementation needed"]);
  }

  // ============================================================
  // DESTROY
  // ============================================================
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
      await this._saveAllData();
      
      for (const timer of this._allTimers) {
        try { clearTimeout(timer); clearInterval(timer); } catch(e) {}
      }
      this._allTimers.clear();
      
      if (this._diceTimeout) { 
        clearTimeout(this._diceTimeout); 
        this._diceTimeout = null; 
      }
      if (this._diceCooldownTimer) { 
        clearTimeout(this._diceCooldownTimer); 
        this._diceCooldownTimer = null; 
      }
      if (this._diceTimeUpCooldownTimer) {
        clearTimeout(this._diceTimeUpCooldownTimer);
        this._diceTimeUpCooldownTimer = null;
      }
      for (const timeout of this._diceNotificationTimeouts) {
        clearTimeout(timeout);
      }
      this._diceNotificationTimeouts = [];
      
      if (this._tieTimer) { 
        clearTimeout(this._tieTimer); 
        this._tieTimer = null; 
      }
      if (this._tieInterval) { 
        clearInterval(this._tieInterval); 
        this._tieInterval = null; 
      }
      for (const timeout of this._tieNotificationTimeouts) {
        clearTimeout(timeout);
      }
      this._tieNotificationTimeouts = [];
      
      for (const [room, game] of this.activeGames) {
        this._cleanupGameTimers(game);
        await this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();
      
      for (const [room, timer] of this._cleanupTimers) {
        this._clearTimer(timer);
      }
      this._cleanupTimers.clear();
      
      this._cachedLastWeekWinner = null;
      this._kvCache.clear();
      
      if (this.cacheManager) { this.cacheManager.clear(); }
      if (this.diceGameSystem) { this.diceGameSystem.clearCache(); }
      
      this._eventQueue = [];
      this._isProcessingQueue = false;
      
      this.userConnections.clear();
      this._tieBreakers.clear();
      this._reconnectAttempts.clear();
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._switchLocks.clear();
      this._switchRetries.clear();
      this._kvCache.clear();
      
      if (this.alarmScheduler) {
        await this.alarmScheduler.cleanup();
      }
      
      for (const [wsId, ws] of this.wsMap) {
        try { 
          if (ws && ws.readyState === 1) {
            ws.removeAllListeners();
            ws.close(1000, "Server shutting down"); 
          }
        } catch(e) {}
      }
      this.wsMap.clear();
      this.wsClients.clear();
      this.clientRooms.clear();
      
      try {
        await this.ctx.storage.deleteAlarm();
      } catch(e) {}
      
    } catch(e) {}
  }

  _cleanupGameTimers(game) {
    if (!game) return;
    
    const timerKeys = [
      '_registrationTimer',
      '_drawTimer', 
      '_evalTimer',
      '_safetyTimer'
    ];
    
    for (const key of timerKeys) {
      if (game[key]) {
        this._clearTimer(game[key]);
        game[key] = null;
      }
    }
    
    if (game._notificationTimers) {
      for (const timer of game._notificationTimers) {
        this._clearTimer(timer);
      }
      game._notificationTimers = [];
    }
    
    if (game._drawNotificationTimers) {
      for (const timer of game._drawNotificationTimers) {
        this._clearTimer(timer);
      }
      game._drawNotificationTimers = [];
    }
    
    if (game._botTimeouts) {
      for (const timeout of game._botTimeouts) {
        this._clearTimer(timeout);
      }
      game._botTimeouts.clear();
    }
    
    game._isEvaluating = false;
    game.evaluationLocked = false;
    game.drawTimeExpired = false;
    game.registrationOpen = false;
  }

  async _forceCleanupGame(room, game) {
    const lockKey = `cleanup_${room}`;
    if (this._cleanupLocks.has(lockKey)) return;
    if (!this._acquireLock(this._cleanupLocks, lockKey, 10000)) return;
    try {
      if (!game) { this._releaseLock(this._cleanupLocks, lockKey); return; }
      
      this._cleanupGameTimers(game);
      
      game.players = null;
      game.botPlayers = null;
      game.numbers = null;
      game.tanda = null;
      game.eliminated = null;
      game.playerWsId = null;
      game._isActive = false;
      game._gameEnded = true;
      game._isEvaluating = false;
      game.registrationOpen = false;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      game._phase = null;
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
      
      this.activeGames.delete(room);
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this._evaluationLocks.delete(`eval_${room}`);
      this._drawLocks.delete(`draw_${room}`);
      this._gameOperationLocks.delete(`startDraw_${room}`);
      
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
      this._releaseLock(this._cleanupLocks, lockKey);
    } catch(e) {
      this._releaseLock(this._cleanupLocks, lockKey);
    }
  }
}

export default GameServer;
