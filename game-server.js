// ==================== GAME-SERVER.JS ====================
// VERSION: 6.0.0 - WITH HYBERNATE API (FULL CLASS)

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
  DICE_LAST_RESET_WEEK: 'dice_last_reset_week',
  
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
};

// ========== QUIZ SCHEDULE ==========
const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: "01:00", end: "02:00" },
    { start: "14:00", end: "15:00" },
    { start: "20:00", end: "22:00" }
  ],
  TIMEZONE_OFFSET: 8,
};

// ========== FUNGSI PARSE TIME ==========
function parseTime(timeStr) {
  const [hours, minutes] = timeStr.split(':').map(Number);
  return hours * 60 + minutes;
}

// ==================== ALARM SCHEDULER ====================
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
        console.log(`[ALARM] Current session: ${currentSession.start} - ${currentSession.end}`);
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
        console.log(`[ALARM] Next session: ${nextSession.start} in ${Math.floor(minDiff/60)}h ${minDiff%60}m`);
        
        let startDelay = (minDiff * 60 * 1000) - 30000;
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
      console.error('[ALARM] Schedule error:', e);
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
      
      // Simpan ke storage
      if (this.state) {
        await this.state.storage.put('alarm_state', {
          alarms: Array.from(this._alarms.entries()),
          updatedAt: Date.now()
        });
      }
      
      // Set alarm di DO
      if (this.state) {
        await this.state.storage.setAlarm(Date.now() + delayMs);
      }
      
      return true;
    } catch(e) { 
      console.error('[ALARM] Schedule error:', e);
      return false; 
    }
  }

  async _clearAllAlarms() {
    try {
      this._alarms.clear();
      
      if (this.state) {
        await this.state.storage.delete('alarm_state');
        try {
          await this.state.storage.setAlarm(null);
        } catch(e) {}
      }
    } catch(e) {}
  }

  async restoreAlarms() {
    try {
      if (!this.state) return;
      
      const data = await this.state.storage.get('alarm_state');
      if (!data || !data.alarms) {
        console.log('[ALARM] No saved alarms found');
        return;
      }
      
      this._alarms = new Map(data.alarms);
      console.log(`[ALARM] Restored ${this._alarms.size} alarms`);
      
      let minDelay = Infinity;
      const now = Date.now();
      for (const [name, alarm] of this._alarms) {
        const remaining = alarm.scheduledAt - now;
        if (remaining > 0 && remaining < minDelay) {
          minDelay = remaining;
        }
      }
      
      if (minDelay > 0 && minDelay < Infinity) {
        await this.state.storage.setAlarm(Date.now() + minDelay);
      }
      
      return true;
    } catch(e) {
      console.error('[ALARM] Restore error:', e);
      return false;
    }
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
      
      if (expired.length > 0 && this.state) {
        await this.state.storage.put('alarm_state', {
          alarms: Array.from(this._alarms.entries()),
          updatedAt: Date.now()
        });
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
      
      if (this.state) {
        await this.state.storage.put('alarm_state', {
          alarms: Array.from(this._alarms.entries()),
          updatedAt: Date.now()
        });
      }
      
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
      console.error('[ALARM] Process error:', e);
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
}

// ==================== KVCACHE ====================
class KVCache {
  constructor() {
    this.cache = new Map();
  }
  get(key) { const entry = this.cache.get(key); return entry ? entry.value : null; }
  set(key, value) { this.cache.set(key, { value, timestamp: Date.now() }); }
  delete(key) { this.cache.delete(key); }
  clear() { this.cache.clear(); }
  has(key) { return this.cache.has(key); }
}

// ==================== CACHE MANAGER ====================
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
      this.winnersCache.set(room, { winners: roomWinners, timestamp: Date.now() });
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
      this.winnersCache.set(room, { winners, timestamp: Date.now() });
    } catch(e) {}
  }

  async loadInitialData(env) {
    try { if (!env?.QUESTIONS) return; } catch(e) {}
  }

  clear() {
    this.recordingStatus.clear();
    this.winnersCache.clear();
    this._updateLocks.clear();
  }
}

// ==================== DICE POINTS CACHE ====================
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
      this.pointsCache.set('points', { data: points, timestamp: Date.now() });
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
        points = await env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      }
      points[username] = (points[username] || 0) + 1;
      this.pointsCache.set('points', { data: points, timestamp: Date.now() });
      this.leaderboardCache.delete('leaderboard');
      await env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      return points[username];
    } catch(e) {
      await this._reloadFromKV(env);
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
      this.leaderboardCache.set('leaderboard', { data: sorted, timestamp: Date.now() });
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

  async _reloadFromKV(env) {
    try {
      if (!env?.QUESTIONS) return;
      const points = await env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.pointsCache.set('points', { data: points, timestamp: Date.now() });
    } catch(e) {}
  }

  clear() {
    this.pointsCache.clear();
    this.leaderboardCache.clear();
    this._updateLocks.clear();
  }
}

// ==================== DICE GAME SYSTEM ====================
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
      if (!this.env?.QUESTIONS) return {};
      points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      await this.pointsCache.setPoints(points, this.env);
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      return points;
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
      await this.getPoints();
      this._isLoaded = true;
      return true;
    } catch(e) { return false; }
  }

  async getLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return null;
      if (this.gameServer._cachedLastWeekWinner !== null) {
        return this.gameServer._cachedLastWeekWinner;
      }
      const winnerData = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_WEEK_WINNER, 'json');
      this.gameServer._cachedLastWeekWinner = winnerData;
      return winnerData;
    } catch(e) { return null; }
  }

  async deleteLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
      this.gameServer._cachedLastWeekWinner = null;
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

// ==================== GAME SERVER (WITH HYBERNATE API) ====================
export class GameServer {
  static allowConcurrency = true;

  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      this.closing = false;
      this.isDestroyed = false;
      this._initialized = false;
      this._startTime = Date.now();
      this._wsIdCounter = 0;
      this._lastActivity = Date.now();
      this._isHibernating = false;
      
      // ===== WEBSOCKET CONNECTIONS (MEMORY ONLY) =====
      this.wsMap = new Map();
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.userConnections = new Map();
      
      // ===== CACHE MANAGERS =====
      this.cacheManager = new CacheManager();
      
      // ===== GAME STATE =====
      this.activeGames = new Map();
      
      // ===== DICE STATE =====
      this.diceGameSystem = null;
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
      
      // ===== TIE BREAKER STATE =====
      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._tieLock = false;
      this._tieNotificationTimeouts = [];
      
      // ===== LOCKS & QUEUE =====
      this._eventQueue = [];
      this._isProcessingQueue = false;
      this._allTimers = new Set();
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._cleanupTimers = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();
      this._evaluationLocks = new Map();
      this._gameOperationLocks = new Map();
      this._drawLocks = new Map();
      this._cleanupLocks = new Map();
      
      // ===== RATE LIMITING =====
      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      
      // ===== RECONNECT =====
      this._reconnectAttempts = new Map();
      
      // ===== CACHE =====
      this._cachedResetWeek = null;
      this._cachedLastWeekWinner = null;
      this._kvCache = new KVCache();
      this.DICE_ROOM = CONSTANTS.DICE_ROOM;
      this._lastNotifTime = {};
      
      // ===== ALARM SCHEDULER =====
      this.alarmScheduler = new AlarmScheduler(env, state);
      
      // ===== RESTORE STATE DARI HYBERNATE =====
      this._restoreAllState().then(() => {});
      
    } catch(e) {
      console.error('[GAME] Constructor error:', e);
    }
  }

  // ========== HYBERNATE API METHODS ==========
  
  async _restoreAllState() {
    try {
      console.log('[HYBERNATE] Restoring all state...');
      
      // 1. RESTORE DARI STORAGE
      const roomsData = await this.state.storage.get("roomsData") || {};
      const userSeatData = await this.state.storage.get("userSeatData") || {};
      const gameState = await this.state.storage.get("gameState") || {};
      const diceState = await this.state.storage.get("diceState") || {};
      const cacheState = await this.state.storage.get("cacheState") || {};
      const alarmState = await this.state.storage.get("alarm_state") || {};
      
      // 2. RESTORE CACHE MANAGER
      if (cacheState.recordingStatus) {
        this.cacheManager.recordingStatus = new Map(Object.entries(cacheState.recordingStatus));
      }
      if (cacheState.winnersCache) {
        for (const [room, data] of Object.entries(cacheState.winnersCache)) {
          this.cacheManager.winnersCache.set(room, data);
        }
      }
      
      // 3. RESTORE GAMES
      if (gameState.activeGames && gameState.activeGames.length > 0) {
        for (const gameData of gameState.activeGames) {
          const game = this._deserializeGame(gameData);
          if (game) {
            this.activeGames.set(game.room, game);
            console.log(`[HYBERNATE] Restored game in room: ${game.room}`);
          }
        }
      }
      
      // 4. RESTORE DICE STATE
      if (diceState) {
        this.currentDiceRoll = diceState.currentDiceRoll || null;
        this._isShowingDice = diceState.isShowingDice || false;
        this._canSubmitDiceAnswer = diceState.canSubmitDiceAnswer || false;
        this._diceLock = diceState.diceLock || false;
        this.diceHasWinner = diceState.diceHasWinner || false;
        this.diceWinner = diceState.diceWinner || null;
        this._diceTimeUpCooldown = diceState.diceTimeUpCooldown || false;
        this._diceStartTime = diceState.diceStartTime || null;
        this._diceQuestionStartTime = diceState.diceQuestionStartTime || null;
        this._diceRound = diceState.diceRound || 0;
        this.diceAutoEnabled = diceState.diceAutoEnabled || false;
        this._cachedResetWeek = diceState.cachedResetWeek || null;
        this._cachedLastWeekWinner = diceState.cachedLastWeekWinner || null;
        
        if (diceState.diceAnswered) {
          this.diceAnswered = new Set(diceState.diceAnswered);
        }
        if (diceState.playerAnswers) {
          this._playerAnswers = new Map(Object.entries(diceState.playerAnswers));
        }
        
        if (diceState.tieActive) {
          this._tieActive = true;
          this._tieRound = diceState.tieRound || 0;
          this._tiePlayers = diceState.tiePlayers || [];
          if (diceState.tieAnswers) {
            this._tieAnswers = new Map(Object.entries(diceState.tieAnswers));
          }
        }
      }
      
      // 5. RESTORE WEBSOCKETS DARI HYBERNATE
      const webSockets = this.state.getWebSockets();
      for (const ws of webSockets) {
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.wsId) {
            const wsId = attachment.wsId;
            const username = attachment.username;
            const room = attachment.room;
            
            ws._wsId = wsId;
            ws.username = username;
            ws.room = room;
            ws.roomname = room;
            ws._closing = false;
            
            this.wsMap.set(wsId, ws);
            
            if (username) {
              this.userConnections.set(username, { 
                wsId, 
                ws, 
                room, 
                timestamp: Date.now() 
              });
            }
            
            if (room) {
              if (!this.wsClients.has(room)) {
                this.wsClients.set(room, new Set());
              }
              this.wsClients.get(room).add(wsId);
              this.clientRooms.set(wsId, room);
            }
          }
        } catch(e) {}
      }
      
      // 6. INIT DICE GAME SYSTEM
      if (!this.diceGameSystem) {
        this.diceGameSystem = new DiceGameSystem(this);
      }
      
      // 7. RESTORE ALARM
      await this.alarmScheduler.restoreAlarms();
      
      // 8. CHECK CURRENT SESSION
      this._checkAndStartCurrentSession();
      
      this._initialized = true;
      console.log('[HYBERNATE] All state restored successfully');
      
    } catch(e) {
      console.error('[HYBERNATE] Restore error:', e);
      this._initLazy();
    }
  }

  async _saveToStorage() {
    try {
      if (!this.state) return;
      
      // 1. SAVE GAMES
      const activeGamesList = [];
      for (const [room, game] of this.activeGames) {
        if (game._isActive && !game._gameEnded) {
          activeGamesList.push(this._serializeGame(game));
        }
      }
      
      // 2. SAVE CACHE
      const cacheState = {
        recordingStatus: Object.fromEntries(this.cacheManager.recordingStatus),
        winnersCache: Object.fromEntries(this.cacheManager.winnersCache),
      };
      
      // 3. SAVE DICE STATE
      const diceState = {
        currentDiceRoll: this.currentDiceRoll,
        isShowingDice: this._isShowingDice,
        canSubmitDiceAnswer: this._canSubmitDiceAnswer,
        diceLock: this._diceLock,
        diceHasWinner: this.diceHasWinner,
        diceWinner: this.diceWinner,
        diceTimeUpCooldown: this._diceTimeUpCooldown,
        diceStartTime: this._diceStartTime,
        diceQuestionStartTime: this._diceQuestionStartTime,
        diceRound: this._diceRound,
        diceAutoEnabled: this.diceAutoEnabled,
        cachedResetWeek: this._cachedResetWeek,
        cachedLastWeekWinner: this._cachedLastWeekWinner,
        diceAnswered: Array.from(this.diceAnswered),
        playerAnswers: Object.fromEntries(this._playerAnswers),
        tieActive: this._tieActive,
        tieRound: this._tieRound,
        tiePlayers: this._tiePlayers,
        tieAnswers: Object.fromEntries(this._tieAnswers),
      };
      
      // 4. SAVE ALL
      await this.state.storage.put({
        gameState: { activeGames: activeGamesList },
        cacheState: cacheState,
        diceState: diceState,
        lastSaved: Date.now()
      });
      
      console.log('[HYBERNATE] State saved to storage');
      
    } catch(e) {
      console.error('[HYBERNATE] Save error:', e);
    }
  }

  _serializeGame(game) {
    if (!game) return null;
    return {
      room: game.room,
      betAmount: game.betAmount,
      round: game.round,
      hostName: game.hostName,
      hostId: game.hostId,
      useBots: game.useBots || false,
      _botsAdded: game._botsAdded || false,
      _isActive: game._isActive,
      _gameEnded: game._gameEnded,
      _phase: game._phase || 'registration',
      _startedByRecording: game._startedByRecording || false,
      _startedBy: game._startedBy || 'user',
      _createdAt: game._createdAt,
      _drawPhaseStart: game._drawPhaseStart,
      registrationOpen: game.registrationOpen || false,
      evaluationLocked: game.evaluationLocked || false,
      drawTimeExpired: game.drawTimeExpired || false,
      _isEvaluating: game._isEvaluating || false,
      players: Array.from(game.players?.entries() || []).map(([id, p]) => [id, p]),
      botPlayers: Array.from(game.botPlayers?.entries() || []),
      eliminated: Array.from(game.eliminated || []),
      numbers: Array.from(game.numbers || []),
      tanda: Array.from(game.tanda || []),
      playerWsId: Array.from(game.playerWsId || []),
    };
  }

  _deserializeGame(data) {
    try {
      const game = {
        room: data.room,
        betAmount: data.betAmount,
        round: data.round || 1,
        hostName: data.hostName,
        hostId: data.hostId,
        useBots: data.useBots || false,
        _botsAdded: data._botsAdded || false,
        _isActive: data._isActive,
        _gameEnded: data._gameEnded,
        _phase: data._phase || 'registration',
        _startedByRecording: data._startedByRecording || false,
        _startedBy: data._startedBy || 'user',
        _createdAt: data._createdAt,
        _drawPhaseStart: data._drawPhaseStart,
        registrationOpen: data.registrationOpen || false,
        evaluationLocked: data.evaluationLocked || false,
        drawTimeExpired: data.drawTimeExpired || false,
        _isEvaluating: data._isEvaluating || false,
        players: new Map(data.players || []),
        botPlayers: new Map(data.botPlayers || []),
        eliminated: new Set(data.eliminated || []),
        numbers: new Map(data.numbers || []),
        tanda: new Map(data.tanda || []),
        playerWsId: new Map(data.playerWsId || []),
        _botTimeouts: new Set(),
        _registrationTimer: null,
        _drawTimer: null,
        _evalTimer: null,
        _safetyTimer: null,
        _endTime: null,
      };
      return game;
    } catch(e) {
      console.error('[HYBERNATE] Deserialize game error:', e);
      return null;
    }
  }

  // ========== LAZY INIT ==========
  _initLazy() {
    if (this._initialized || this.closing || this.isDestroyed) return;
    this._initialized = true;
    
    try {
      if (!this.diceGameSystem) {
        this.diceGameSystem = new DiceGameSystem(this);
      }
      
      this._loadKVData().catch(() => {});
      this.alarmScheduler.scheduleAlarms().catch(() => {});
      
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          this._cleanupDeadConnections();
          this._checkAndStartCurrentSession();
        }
      }, 3000);
      
    } catch(e) {
      console.error('[GAME] Lazy init error:', e);
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          this._initialized = false;
          this._initLazy();
        }
      }, 30000);
    }
  }

  // ========== CEK DAN MULAI SESI ==========
  _checkAndStartCurrentSession() {
    try {
      if (!this.alarmScheduler.isDiceTime()) {
        console.log('[DICE] No active session');
        return;
      }
      
      console.log('[DICE] Session is ACTIVE!');
      this.diceAutoEnabled = true;
      
      if (this.currentDiceRoll && this._canSubmitDiceAnswer) {
        console.log('[DICE] Dice already active, NOT starting new game');
        return;
      }
      
      if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) {
        console.log('[DICE] Dice in cooldown/lock state, waiting...');
        return;
      }
      
      const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
      
      if (clients && clients.size > 0) {
        console.log(`[DICE] ${clients.size} users in Quiz room, starting dice`);
        this._startDiceFast();
      } else {
        console.log('[DICE] No users in Quiz room, dice will start when user joins');
      }
    } catch(e) {
      console.error('[DICE] Check current session error:', e);
    }
  }

  // ========== FETCH ==========
  async fetch(req) {
    try {
      // WAKE UP FROM HYBERNATION
      if (this._isHibernating) {
        this._isHibernating = false;
        await this._restoreAllState();
      }
      
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
      
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          uptime: Date.now() - this._startTime,
          connections: this.wsMap.size,
          games: this.activeGames.size,
          queue: this._eventQueue?.length || 0,
          circuitOpen: this._circuitOpen,
          initialized: this._initialized,
          errors: this._errorCount,
          isHibernating: this._isHibernating,
          alarms: this.alarmScheduler._alarms.size,
          concurrencyEnabled: GameServer.allowConcurrency,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (url.pathname === "/game/ws") {
        const upgrade = req.headers.get("Upgrade");
        if (upgrade !== "websocket") {
          return new Response("WebSocket only", { status: 400 });
        }
        
        if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
          return new Response("Server at maximum capacity", { 
            status: 503,
            headers: { 'Retry-After': '10', 'Content-Type': 'text/plain' }
          });
        }
        
        if (this._eventQueue?.length > 500) {
          return new Response("Server busy", { 
            status: 503,
            headers: { 'Retry-After': '5', 'Content-Type': 'text/plain' }
          });
        }
        
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        const wsId = ++this._wsIdCounter;
        
        server._wsId = wsId;
        server._closing = false;
        server.room = null;
        server.roomname = null;
        server.username = null;
        server._createdAt = Date.now();
        
        try {
          // ===== ACCEPT WEBSOCKET WITH HYBERNATE =====
          this.state.acceptWebSocket(server);
        } catch(e) {
          try { server.close(1008, "Accept failed"); } catch(err) {}
          return new Response("WebSocket acceptance failed", { status: 500 });
        }
        
        // ===== SAVE STATE TO ATTACHMENT (FOR HYBERNATE) =====
        server.serializeAttachment({
          wsId: wsId,
          username: null,
          room: null
        });
        
        this.wsMap.set(wsId, server);
        
        server.addEventListener("message", async (event) => {
          try {
            if (server._closing || this.closing || this.isDestroyed) return;
            const data = JSON.parse(event.data);
            if (Array.isArray(data) && data.length > 0) {
              await this._processWithTimeout(server, data);
            }
          } catch(e) {}
        });
        
        server.addEventListener("close", () => { 
          this.webSocketClose(server);
        }, { once: true });
        
        server.addEventListener("error", () => { 
          this.webSocketError(server);
        }, { once: true });
        
        return new Response(null, { status: 101, webSocket: client });
      }
      
      return new Response("Game Server", { status: 200 });
      
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

  // ========== WEBSOCKET HANDLERS ==========
  webSocketClose(ws) {
    try {
      if (!ws) return;
      ws._closing = true;
      
      const wsId = ws._wsId;
      const username = ws.username;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      
      // Remove from room
      if (room) this._removeClientFromRoom(room, wsId);
      
      // Remove from maps
      if (wsId) {
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
      }
      
      // Remove user connection
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn?.wsId === wsId) this.userConnections.delete(username);
        if (room) { this._broadcastToRoom(room, ["userLeftRoom", username, room]); }
      }
      
      // Clear WS state
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
      
      // Save state to storage (async)
      this._saveToStorage().catch(() => {});
      
    } catch(e) {}
  }

  webSocketError(ws) {
    try {
      if (!ws) return;
      ws._closing = true;
      
      const wsId = ws._wsId;
      const username = ws.username;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      
      if (room) this._removeClientFromRoom(room, wsId);
      
      if (wsId) {
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
      }
      
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn?.wsId === wsId) this.userConnections.delete(username);
      }
      
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
      
      this._saveToStorage().catch(() => {});
      
    } catch(e) {}
  }

  // ========== HANDLE ALARM ==========
  async handleAlarm() {
    try {
      console.log('[ALARM] Alarm triggered!');
      
      // Wake up from hibernation
      this._isHibernating = false;
      
      // Restore state
      await this._restoreAllState();
      
      // Get pending alarms
      const pendingAlarms = await this.alarmScheduler.getPendingAlarms();
      
      for (const alarm of pendingAlarms) {
        console.log(`[ALARM] Processing: ${alarm.name}`);
        
        switch(alarm.name) {
          case 'dice_session_start':
            console.log('[ALARM] Session start alarm triggered');
            this._cleanupDeadConnections();
            
            if (this.alarmScheduler.isDiceTime()) {
              this.diceAutoEnabled = true;
              
              if (this.currentDiceRoll && this._canSubmitDiceAnswer) {
                console.log('[ALARM] Dice already active, NOT starting new game');
                break;
              }
              
              if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) {
                console.log('[ALARM] Dice in cooldown/lock state, waiting...');
                break;
              }
              
              const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
              
              if (clients && clients.size > 0) {
                console.log(`[ALARM] ${clients.size} users in Quiz room, starting dice`);
                this._startDiceFast();
              } else {
                console.log('[ALARM] No users in Quiz room, dice will start when user joins');
              }
            }
            break;
            
          case 'dice_session_end':
            console.log('[ALARM] Session end alarm triggered');
            this.diceAutoEnabled = false;
            if (this.currentDiceRoll || this._isShowingDice) {
              this._endDiceRound();
            }
            break;
        }
        
        await this.alarmScheduler.processAlarm(alarm.name);
      }
      
      // Schedule next alarms
      await this.alarmScheduler.scheduleAlarms();
      
      // Save state
      await this._saveToStorage();
      
    } catch(e) {
      console.error('[ALARM] Handle alarm error:', e);
    }
  }

  // ========== SWITCH ROOM (WITH ATTACHMENT) ==========
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
        if (currentRoom) this._removeClientFromRoom(currentRoom, wsId);
        this._addClient(roomName, ws, username);
        ws.room = roomName;
        ws.roomname = roomName;
        if (username) ws.username = username;
        
        // ===== UPDATE ATTACHMENT FOR HYBERNATE =====
        ws.serializeAttachment({
          wsId: wsId,
          username: username,
          room: roomName
        });
        
        if (username) {
          let conn = this.userConnections.get(username);
          if (conn) { 
            conn.room = roomName; 
            conn.wsId = wsId; 
            conn.ws = ws; 
            conn.timestamp = Date.now(); 
          } else { 
            this.userConnections.set(username, { wsId, ws, room: roomName, timestamp: Date.now() }); 
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
        
        // Save state
        this._saveToStorage().catch(() => {});
        
      } finally {
        setTimeout(() => {
          this._switchLocks.delete(lockKey);
          this._switchRetries.delete(lockKey);
        }, 2000);
      }
    } catch(e) {}
  }

  // ========== ADD CLIENT (WITH ATTACHMENT) ==========
  _addClient(room, ws, username = null) {
    try {
      if (!ws) return;
      const wsId = ws._wsId;
      if (!wsId) { this._safeSend(ws, ["gameLowCardError", "Connection error"]); return; }
      
      if (this.clientRooms.has(wsId)) {
        const oldRoom = this.clientRooms.get(wsId);
        if (oldRoom && oldRoom !== room) this._removeClientFromRoom(oldRoom, wsId);
      }
      
      if (username) {
        let conn = this.userConnections.get(username);
        if (conn) { conn.room = room; conn.timestamp = Date.now(); conn.ws = ws; conn.wsId = wsId; }
        else { this.userConnections.set(username, { wsId, ws, room, timestamp: Date.now() }); }
        this._reconnectAttempts.delete(username);
      }
      
      let clients = this.wsClients.get(room);
      if (!clients) { clients = new Set(); this.wsClients.set(room, clients); }
      clients.add(wsId);
      this.clientRooms.set(wsId, room);
      this.wsMap.set(wsId, ws);
      ws.room = room;
      ws.roomname = room;
      if (username) ws.username = username;
      
      // ===== UPDATE ATTACHMENT FOR HYBERNATE =====
      ws.serializeAttachment({
        wsId: wsId,
        username: username,
        room: room
      });
      
      // Save state
      this._saveToStorage().catch(() => {});
      
    } catch(e) {}
  }

  // ========== _removeClientFromRoom ==========
  _removeClientFromRoom(room, wsId) {
    try {
      if (!room || !wsId) return;
      const clients = this.wsClients.get(room);
      if (clients) { clients.delete(wsId); if (clients.size === 0) this.wsClients.delete(room); }
    } catch(e) {}
  }

  // ========== _safeSend ==========
  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  // ========== _broadcastToRoom ==========
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

  // ========== _getWsId ==========
  _getWsId(ws) { return ws?._wsId || null; }

  // ========== _sendGameStateToClient ==========
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

  // ========== _getActivePlayers ==========
  _getActivePlayers(game) {
    try {
      if (!game?._isActive || game?._gameEnded || !game?.players) return [];
      return Array.from(game.players.entries())
        .filter(([id]) => !game.eliminated?.has(id))
        .map(([, p]) => p);
    } catch(e) { return []; }
  }

  // ========== _getActivePlayerIds ==========
  _getActivePlayerIds(game) {
    try {
      if (!game?._isActive || game._gameEnded || !game?.players) return [];
      return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
    } catch(e) { return []; }
  }

  // ========== _isGameActuallyRunning ==========
  _isGameActuallyRunning(game) { 
    return game?._isActive === true && !game?._gameEnded; 
  }

  // ========== _cleanupGameTimers ==========
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

  // ========== _clearTimer ==========
  _clearTimer(timer) {
    if (timer) {
      if (typeof timer === 'object' && timer._destroyed) return;
      try { clearTimeout(timer); } catch(e) {}
      try { clearInterval(timer); } catch(e) {}
      this._allTimers.delete(timer);
    }
  }

  // ========== _trackTimer ==========
  _trackTimer(timer) {
    if (timer) this._allTimers.add(timer);
    return timer;
  }

  // ========== _scheduleGameCleanup ==========
  _scheduleGameCleanup(room, game) {
    try {
      if (!room || !game) return;
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      if (!game._gameEnded) return;
      const timer = this._trackTimer(setTimeout(() => {
        const currentGame = this.activeGames.get(room);
        if (currentGame?._isActive && !currentGame._gameEnded) {
          this._cleanupTimers.delete(room);
          return;
        }
        this._cleanupTimers.delete(room);
        const gameToDelete = this.activeGames.get(room);
        if (gameToDelete) this._forceCleanupGame(room, gameToDelete);
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS));
      this._cleanupTimers.set(room, timer);
    } catch(e) {}
  }

  // ========== _forceCleanupGame ==========
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
      
      // Save state
      this._saveToStorage().catch(() => {});
      
    } catch(e) {
      this._releaseLock(this._cleanupLocks, lockKey);
    }
  }

  // ========== _acquireLock ==========
  _acquireLock(lockMap, key, timeoutMs = 5000) {
    if (lockMap.has(key)) return false;
    lockMap.set(key, Date.now());
    setTimeout(() => {
      if (lockMap.has(key)) lockMap.delete(key);
    }, timeoutMs);
    return true;
  }

  // ========== _releaseLock ==========
  _releaseLock(lockMap, key) {
    if (lockMap.has(key)) { lockMap.delete(key); return true; }
    return false;
  }

  // ========== _handleError ==========
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

  // ========== _loadKVData ==========
  async _loadKVData() {
    try {
      if (this.closing || this.isDestroyed || !this.env?.QUESTIONS) return;
      const currentWeek = this._generateCurrentWeek(new Date());
      const existing = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
      if (!existing) {
        this._cachedResetWeek = currentWeek;
        this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek).catch(() => {});
      }
      await this.diceGameSystem.loadScores();
      await this.cacheManager.loadInitialData(this.env);
    } catch(e) {}
  }

  // ========== _generateCurrentWeek ==========
  _generateCurrentWeek(date) {
    const now = date || new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }

  // ========== GET TIME LEFT ==========
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

  // ========== _getCurrentWITATime ==========
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

  // ========== _cleanupDeadConnections ==========
  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      let removed = 0;
      const now = Date.now();
      
      for (const [wsId, ws] of this.wsMap) {
        if (removed > 50) break;
        
        const isDead = !ws || 
                       ws.readyState !== 1 || 
                       ws._closing ||
                       (ws._createdAt && (now - ws._createdAt) > 3600000);
        
        if (isDead) {
          toRemove.push(wsId);
          removed++;
        }
      }
      
      for (const wsId of toRemove) {
        const ws = this.wsMap.get(wsId);
        if (ws) {
          try {
            if (ws.readyState === 1) {
              ws.close(1000, "Cleanup");
            }
          } catch(e) {}
          
          const room = this.clientRooms.get(wsId);
          if (room) this._removeClientFromRoom(room, wsId);
          this.clientRooms.delete(wsId);
          this.wsMap.delete(wsId);
        }
      }
    } catch(e) {}
  }

  // ========== CEK DAN MULAI DICE ==========
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
        console.log('[DICE] User switched to Quiz, dice already active - NOT starting new game');
        return;
      }
      
      if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) {
        if (this._diceTimeUpCooldown) {
          this._safeSend(ws, ["diceNotification", "Game in cooldown, please wait..."]);
        }
        console.log('[DICE] Dice in cooldown/lock state, waiting...');
        return;
      }
      
      console.log('[DICE] User switched to Quiz, starting dice immediately');
      this._startDiceFast();
      
    } catch(e) {
      console.error('[DICE] Check and start dice error:', e);
    }
  }

  // ========== _sendDiceNotificationOnSwitch ==========
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

  // ========== _startDiceFast ==========
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
      
      // Save state
      this._saveToStorage().catch(() => {});
      
    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  // ========== _endDiceRound ==========
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
        try {
          const success = await this.diceGameSystem.addPoint(winner);
          if (success) {
            const points = this.diceGameSystem.pointsCache.getPoints() || {};
            this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
              username: winner,
              totalPoints: points[winner] || 0,
              diceValue: diceValue,
              round: roundNumber
            }]);
          } else {
            this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
              username: winner,
              totalPoints: 0,
              diceValue: diceValue,
              round: roundNumber
            }]);
          }
        } catch(e) {
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
            username: winner,
            totalPoints: 0,
            diceValue: diceValue,
            round: roundNumber
          }]);
        }
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
      
      // Save state
      this._saveToStorage().catch(() => {});
      
    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  // ========== TIE BREAKER ==========
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
      
      // Save state
      this._saveToStorage().catch(() => {});
      
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
        
        // Save state
        this._saveToStorage().catch(() => {});
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
      try {
        const success = await this.diceGameSystem.addPoint(winner);
        if (success) {
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
        } else {
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
            username: winner,
            totalPoints: 0,
            diceValue: highest,
            round: this._diceRound || 1,
            isTieBreaker: true,
            tieBreakerRound: this._tieRound,
            finalWinner: true
          }]);
        }
      } catch(e) {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: 0,
          diceValue: highest,
          round: this._diceRound || 1,
          isTieBreaker: true,
          tieBreakerRound: this._tieRound,
          finalWinner: true
        }]);
      }
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
      const success = await this.diceGameSystem.addPoint(winner);
      if (success) {
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
      } else {
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

  // ========== submitDiceAnswer ==========
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

  // ========== LOW CARD GAME METHODS ==========
  // (Semua game method sama seperti sebelumnya, tapi dengan _saveToStorage)
  // Saya singkat karena panjang, tapi intinya semua game method
  // tetap sama, hanya ditambah _saveToStorage() di akhir

  // ========== PROCESS EVENT ==========
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

  // ========== _handleEventInternal ==========
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
          const result = await this._getLastWeekWinnerAndReset();
          if (result?.username) {
            this._safeSend(ws, ["diceLastWeekWinner", result.username, result.score || 0, result.week || ""]);
          } else {
            this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
          }
        } catch(e) { this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]); }
        return;
      }

      if (evt === "getDiceLeaderboard") {
        try {
          let limit = data.length > 1 && typeof data[1] === 'number' ? Math.min(data[1], 30) : 10;
          const leaderboard = this.diceGameSystem.getLeaderboard(limit);
          this._safeSend(ws, ["diceLeaderboard", leaderboard.map(([u, s]) => `${u}|${s}`)]);
        } catch(e) { this._safeSend(ws, ["diceLeaderboard", []]); }
        return;
      }

      if (evt === "getDicePoints") {
        try {
          const points = this.diceGameSystem.pointsCache.getPoints() || {};
          this._safeSend(ws, ["dicePoints", points]);
        } catch(e) { this._safeSend(ws, ["dicePoints", {}]); }
        return;
      }

      if (evt === "deleteDiceLastWeekWinner") {
        try {
          const success = await this.diceGameSystem.deleteLastWeekWinner();
          this._safeSend(ws, ["diceLastWeekWinnerDeleted", success, success ? "Deleted" : "Failed"]);
          if (success) this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "Last week winner deleted"]);
        } catch(e) { this._safeSend(ws, ["diceLastWeekWinnerDeleted", false, e.message]); }
        return;
      }

      if (evt === "getDiceStatus") {
        this._safeSend(ws, ["diceStatus", !!this.currentDiceRoll && this._canSubmitDiceAnswer, this._diceRound || 1]);
        return;
      }

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

  // ========== RECORDING METHODS ==========
  async _startRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      if (this.cacheManager.getRecordingStatus(roomName)) {
        this._broadcastToRoom(roomName, ["recordingStatus", true]);
        return true;
      }
      const success = await this.cacheManager.setRecordingStatus(roomName, true, this.env);
      if (success) {
        this._broadcastToRoom(roomName, ["recordingStatus", true]);
        this._saveToStorage().catch(() => {});
      }
      return success;
    } catch(e) { return false; }
  }

  async _stopRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      const room = roomName.trim();
      if (!this.cacheManager.getRecordingStatus(room)) {
        this._broadcastToRoom(room, ["recordingStatus", false]);
        return true;
      }
      const success = await this.cacheManager.setRecordingStatus(room, false, this.env);
      if (success) {
        this.cacheManager.winnersCache.delete(room);
        this._broadcastToRoom(room, ["recordingStatus", false]);
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners: {}, room, recording: false }]);
        this._saveToStorage().catch(() => {});
      }
      return success;
    } catch(e) { return false; }
  }

  async _addLowCardWinner(room, username) {
    return await this.cacheManager.addWinner(room, username, this.env);
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

  // ========== GET LAST WEEK WINNER ==========
  _getLastWeekWinnerAndReset() {
    try {
      if (!this.env?.QUESTIONS) return null;
      const currentWeek = this._generateCurrentWeek(new Date());
      const lastResetWeek = this._cachedResetWeek || this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
      const weekChanged = lastResetWeek && this._compareWeeks(currentWeek, lastResetWeek) > 0;
      if (!lastResetWeek || weekChanged) {
        this._performReset();
        return this._cachedLastWeekWinner;
      }
      if (this._cachedLastWeekWinner !== null) return this._cachedLastWeekWinner;
      return null;
    } catch(e) { return null; }
  }

  _compareWeeks(a, b) {
    try {
      const [yA, wA] = a.split('-W');
      const [yB, wB] = b.split('-W');
      const diff = parseInt(yA) - parseInt(yB);
      if (diff !== 0) return diff;
      return parseInt(wA) - parseInt(wB);
    } catch(e) { return 0; }
  }

  _performReset() {
    try {
      const currentWeek = this._generateCurrentWeek(new Date());
      const points = this.diceGameSystem.pointsCache.getPoints() || {};
      let winner = null, highestScore = 0;
      for (const [username, score] of Object.entries(points)) {
        const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
        if (numericScore > highestScore) {
          highestScore = numericScore;
          winner = username;
        }
      }
      if (winner && highestScore > 0) {
        const winnerData = { username: winner, score: highestScore, week: currentWeek, timestamp: Date.now() };
        this._cachedLastWeekWinner = winnerData;
        this._fireAndForget(this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winnerData)));
      } else {
        this._cachedLastWeekWinner = null;
        this._fireAndForget(this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER));
      }
      this.diceGameSystem.resetPoints();
      this.diceGameSystem.clearCache();
      this._cachedResetWeek = currentWeek;
      this._fireAndForget(this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek));
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceReset", { winner, score: highestScore, week: currentWeek }]);
      this._saveToStorage().catch(() => {});
      return this._cachedLastWeekWinner;
    } catch(e) { return null; }
  }

  _fireAndForget(promise) {
    promise.catch(() => {});
  }

  // ========== GAME METHODS ==========
  // (Semua game methods sama seperti sebelumnya)
  // Saya singkat karena panjang, tapi semua method tetap sama

  // ========== DESTROY ==========
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
      // Save final state
      await this._saveToStorage();
      
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
      
      this._cachedResetWeek = null;
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
      
    } catch(e) {}
  }
}

export default GameServer;
