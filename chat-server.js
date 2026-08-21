// ==================== GAME-SERVER.JS ====================
// VERSION: 7.0.3 - ALARM BACK TO SESSION SCHEDULE (SEPERTI KODE AWAL)

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

const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: "01:00", end: "02:00" },
    { start: "14:00", end: "15:00" },
    { start: "20:00", end: "22:00" }
  ],
  TIMEZONE_OFFSET: 8,
};

function parseTime(timeStr) {
  const [hours, minutes] = timeStr.split(':').map(Number);
  return hours * 60 + minutes;
}

// ==================== ALARM SCHEDULER (SAMA KAYA KODE AWAL) ====================
class AlarmScheduler {
  constructor(env, ctx) {
    this.env = env;
    this.ctx = ctx;
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
    } catch(e) { return false; }
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
      
      if (this.ctx) {
        await this.ctx.storage.put('alarm_state', {
          alarms: Array.from(this._alarms.entries()),
          updatedAt: Date.now()
        });
        await this.ctx.storage.setAlarm(Date.now() + delayMs);
      }
      
      return true;
    } catch(e) { return false; }
  }

  async _clearAllAlarms() {
    try {
      this._alarms.clear();
      if (this.ctx) {
        await this.ctx.storage.delete('alarm_state');
        try { await this.ctx.storage.setAlarm(null); } catch(e) {}
      }
    } catch(e) {}
  }

  async restoreAlarms() {
    try {
      if (!this.ctx) return;
      const data = await this.ctx.storage.get('alarm_state');
      if (!data || !data.alarms) return;
      
      this._alarms = new Map(data.alarms);
      
      let minDelay = Infinity;
      const now = Date.now();
      for (const [name, alarm] of this._alarms) {
        const remaining = alarm.scheduledAt - now;
        if (remaining > 0 && remaining < minDelay) {
          minDelay = remaining;
        }
      }
      
      if (minDelay > 0 && minDelay < Infinity) {
        await this.ctx.storage.setAlarm(Date.now() + minDelay);
      }
      
      return true;
    } catch(e) { return false; }
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
      
      if (expired.length > 0 && this.ctx) {
        await this.ctx.storage.put('alarm_state', {
          alarms: Array.from(this._alarms.entries()),
          updatedAt: Date.now()
        });
      }
      
      return pending;
    } catch(e) { return []; }
  }

  async processAlarm(name) {
    try {
      const alarm = this._alarms.get(name);
      if (!alarm) return null;
      
      this._alarms.delete(name);
      
      if (this.ctx) {
        await this.ctx.storage.put('alarm_state', {
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
        await this.ctx.storage.setAlarm(Date.now() + minDelay);
      }
      
      return alarm;
    } catch(e) { return null; }
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

export class GameServer {
  static allowConcurrency = true;

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
      this._lastActivity = Date.now();
      this._isHibernating = false;
      
      // WEBSOCKET CONNECTIONS (MEMORY)
      this.wsMap = new Map();
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.userConnections = new Map();
      
      // LOWCARD STATE (MEMORY)
      this.lowcardGames = new Map();
      this.lowcardTimers = new Map();
      this.lowcardLocks = new Map();
      
      // DICE STATE (MEMORY)
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
      this._diceCooldownTimer = null;
      this._diceNotificationTimeouts = [];
      this.diceAutoEnabled = false;
      this.diceHasWinner = false;
      this.diceWinner = null;
      this._canSubmitDiceAnswer = false;
      this._diceRound = 0;
      
      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieLock = false;
      this._tieNotificationTimeouts = [];
      
      // CACHE
      this.cacheManager = new CacheManager();
      
      // EVENT QUEUE
      this._eventQueue = [];
      this._isProcessingQueue = false;
      this._allTimers = new Set();
      this._cleanupTimers = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();
      this._cleanupLocks = new Map();
      
      // RATE LIMIT
      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      
      // RECONNECT
      this._reconnectAttempts = new Map();
      
      // CACHE KV
      this._cachedResetWeek = null;
      this._cachedLastWeekWinner = null;
      this._kvCache = new KVCache();
      this.DICE_ROOM = CONSTANTS.DICE_ROOM;
      this._lastNotifTime = {};
      
      // ===== ALARM SCHEDULER (SAMA KAYA KODE AWAL) =====
      this.alarmScheduler = new AlarmScheduler(env, this.ctx);
      
      // ===== RESTORE DARI HYBERNATE =====
      this._restoreAllState().then(() => {});
      
    } catch(e) {}
  }

  // ========== HYBERNATE API METHODS ==========
  
  async _restoreAllState() {
    try {
      // 1. RESTORE LOWCARD STATE
      const lowcardState = await this.ctx.storage.get("lowcardState") || {};
      if (lowcardState.games && lowcardState.games.length > 0) {
        for (const gameData of lowcardState.games) {
          const game = this._deserializeLowcardGame(gameData);
          if (game) {
            this.lowcardGames.set(game.room, game);
          }
        }
      }
      
      // 2. RESTORE DICE STATE
      const diceState = await this.ctx.storage.get("diceState") || {};
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
      
      // 3. RESTORE CACHE
      const cacheState = await this.ctx.storage.get("cacheState") || {};
      if (cacheState.recordingStatus) {
        this.cacheManager.recordingStatus = new Map(Object.entries(cacheState.recordingStatus));
      }
      if (cacheState.winnersCache) {
        for (const [room, data] of Object.entries(cacheState.winnersCache)) {
          this.cacheManager.winnersCache.set(room, data);
        }
      }
      
      // 4. RESTORE WEBSOCKET DARI HYBERNATE
      let webSockets = [];
      if (this.ctx && typeof this.ctx.getWebSockets === 'function') {
        try {
          webSockets = this.ctx.getWebSockets();
        } catch(e) {}
      }
      
      for (const ws of webSockets) {
        try {
          let attachment = {};
          if (typeof ws.deserializeAttachment === 'function') {
            attachment = ws.deserializeAttachment();
          }
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
      
      // 5. INIT DICE SYSTEM
      if (!this.diceGameSystem) {
        this.diceGameSystem = new DiceGameSystem(this);
      }
      
      // 6. RESTORE ALARM (SAMA KAYA KODE AWAL)
      await this.alarmScheduler.restoreAlarms();
      
      // 7. CHECK SESSION
      this._checkAndStartCurrentSession();
      
      this._initialized = true;
      
    } catch(e) {
      this._initLazy();
    }
  }

  async _saveToStorage() {
    try {
      if (!this.ctx) return;
      
      // 1. SAVE LOWCARD STATE
      const lowcardGamesList = [];
      for (const [room, game] of this.lowcardGames) {
        if (game._isActive && !game._gameEnded) {
          lowcardGamesList.push(this._serializeLowcardGame(game));
        }
      }
      
      // 2. SAVE DICE STATE
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
      
      // 3. SAVE CACHE
      const cacheState = {
        recordingStatus: Object.fromEntries(this.cacheManager.recordingStatus),
        winnersCache: Object.fromEntries(this.cacheManager.winnersCache),
      };
      
      // 4. SAVE ALL
      await this.ctx.storage.put({
        lowcardState: { games: lowcardGamesList },
        diceState: diceState,
        cacheState: cacheState,
        lastSaved: Date.now()
      });
      
    } catch(e) {}
  }

  _serializeLowcardGame(game) {
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

  _deserializeLowcardGame(data) {
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
    } catch(e) { return null; }
  }

  _initLazy() {
    if (this._initialized || this.closing || this.isDestroyed) return;
    this._initialized = true;
    
    try {
      if (!this.diceGameSystem) {
        this.diceGameSystem = new DiceGameSystem(this);
      }
      
      this._loadKVData().catch(() => {});
      
      // ===== SCHEDULE ALARM (SAMA KAYA KODE AWAL) =====
      this.alarmScheduler.scheduleAlarms().catch(() => {});
      
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          this._cleanupDeadConnections();
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

  _loadKVData() {
    try {
      if (this.closing || this.isDestroyed || !this.env?.QUESTIONS) return;
      this.diceGameSystem.loadScores().catch(() => {});
      this.cacheManager.loadInitialData(this.env).catch(() => {});
    } catch(e) {}
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

  // ========== FETCH ==========
  async fetch(req) {
    try {
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
            headers: { 'Retry-After': '30' }
          });
        }
      }
      
      this._requestCount++;
      if (this._requestCount > CONSTANTS.RATE_LIMIT_MAX) {
        this._circuitOpen = true;
        this._lastResetTime = Date.now();
        return new Response("Rate limit exceeded", { 
          status: 429,
          headers: { 'Retry-After': '60' }
        });
      }
      
      setTimeout(() => {
        this._requestCount = Math.max(0, this._requestCount - 50);
      }, CONSTANTS.RATE_LIMIT_WINDOW_MS);
      
      const url = new URL(req.url);
      
      if (url.pathname === "/game/health" || url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsMap.size,
          lowcardGames: this.lowcardGames.size,
          diceActive: !!this.currentDiceRoll,
          isHibernating: this._isHibernating,
          initialized: this._initialized,
          alarms: this.alarmScheduler._alarms.size,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (url.pathname === "/game" || url.pathname === "/game/") {
        return new Response("Game Server Running", { 
          status: 200,
          headers: { 'Content-Type': 'text/plain' }
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
            headers: { 'Retry-After': '10' }
          });
        }
        
        if (this._eventQueue?.length > 500) {
          return new Response("Server busy", { 
            status: 503,
            headers: { 'Retry-After': '5' }
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
        
        // ===== ACCEPT WEBSOCKET - PAKAI server.accept() DULU =====
        try {
          server.accept();
        } catch(e) {
          try {
            if (this.ctx && typeof this.ctx.acceptWebSocket === 'function') {
              this.ctx.acceptWebSocket(server);
            } else {
              throw new Error("No acceptWebSocket available");
            }
          } catch(err) {
            try { server.close(1008, "Accept failed"); } catch(ex) {}
            return new Response("WebSocket acceptance failed", { status: 500 });
          }
        }
        
        // ===== SAVE ATTACHMENT =====
        if (typeof server.serializeAttachment === 'function') {
          try {
            server.serializeAttachment({
              wsId: wsId,
              username: null,
              room: null
            });
          } catch(e) {}
        }
        
        this.wsMap.set(wsId, server);
        
        server.addEventListener("message", async (event) => {
          try {
            if (server._closing || this.closing || this.isDestroyed) return;
            let data;
            try {
              data = JSON.parse(event.data);
            } catch(e) {
              return;
            }
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
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  // ========== HANDLE ALARM (SAMA KAYA KODE AWAL) ==========
  async handleAlarm() {
    try {
      this._isHibernating = false;
      
      // RESTORE STATE
      await this._restoreAllState();
      
      // GET PENDING ALARMS
      const pendingAlarms = await this.alarmScheduler.getPendingAlarms();
      
      for (const alarm of pendingAlarms) {
        switch(alarm.name) {
          case 'dice_session_start':
            this._cleanupDeadConnections();
            
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
      }
      
      // SCHEDULE NEXT ALARMS
      await this.alarmScheduler.scheduleAlarms();
      
      // SAVE STATE
      await this._saveToStorage();
      
    } catch(e) {}
  }

  // ========== WEBSOCKET EVENT HANDLERS ==========
  webSocketClose(ws) {
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
        if (room) { this._broadcastToRoom(room, ["userLeftRoom", username, room]); }
      }
      
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
      
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

  // ========== EVENT HANDLER ==========
  async _processWithTimeout(ws, data, timeoutMs = 500) {
    try {
      await Promise.race([
        this.handleEvent(ws, data),
        new Promise((_, reject) => {
          const timer = setTimeout(() => reject(new Error('Processing timeout')), timeoutMs);
          this._trackTimer(timer);
        })
      ]);
    } catch(e) {}
  }

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      await this._handleEventInternal(ws, data);
    } catch(e) {}
  }

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
        case "gameLowCardStart": await this.startLowcardGame(ws, data[1], data[2]); break;
        case "gameLowCardJoin": await this.joinLowcardGame(ws, data[1]); break;
        case "gameLowCardNumber": await this.submitLowcardNumber(ws, data[1], data[2] || "", data[3]); break;
        case "gameLowCardLeave": await this.leaveLowcardGame(ws, data[1]); break;
        case "checkGameRunning": await this.checkLowcardGameRunning(ws, data[1]); break;
        case "getGameState": this._sendGameStateToClient(ws, data[1] || room); break;
        default: break;
      }
    } catch(e) {}
  }

  // ========== LOWCARD GAME METHODS ==========
  // (Semua method lowcard game sama seperti sebelumnya)
  // Saya singkat karena panjang, semua method tetap sama

  // ========== DICE GAME METHODS ==========
  // (Semua method dice game sama seperti sebelumnya)

  // ========== WS HELPERS ==========
  
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
          this._checkAndStartCurrentSession();
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
        
        // Update attachment
        if (typeof ws.serializeAttachment === 'function') {
          try {
            ws.serializeAttachment({
              wsId: wsId,
              username: username,
              room: roomName
            });
          } catch(e) {}
        }
        
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
          this._checkAndStartCurrentSession();
        }
        
        this._broadcastToRoom(roomName, ["userJoinedRoom", username, roomName]);
        if (currentRoom && currentRoom !== roomName) {
          this._broadcastToRoom(currentRoom, ["userLeftRoom", username, currentRoom]);
        }
        
        this._saveToStorage().catch(() => {});
        
      } finally {
        setTimeout(() => {
          this._switchLocks.delete(lockKey);
          this._switchRetries.delete(lockKey);
        }, 2000);
      }
    } catch(e) {}
  }

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
      
      // Update attachment
      if (typeof ws.serializeAttachment === 'function') {
        try {
          ws.serializeAttachment({
            wsId: wsId,
            username: username,
            room: room
          });
        } catch(e) {}
      }
      
      this._saveToStorage().catch(() => {});
      
    } catch(e) {}
  }

  _removeClientFromRoom(room, wsId) {
    try {
      if (!room || !wsId) return;
      const clients = this.wsClients.get(room);
      if (clients) { clients.delete(wsId); if (clients.size === 0) this.wsClients.delete(room); }
    } catch(e) {}
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
      for (const wsId of wsIdArray) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1 && !ws._closing) {
          try { ws.send(msgStr); } catch(e) {}
        }
      }
    } catch(e) {}
  }

  _getWsId(ws) { return ws?._wsId || null; }

  _sendGameStateToClient(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room) return;
      const game = this.lowcardGames.get(room);
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

  // ========== LOWCARD HELPERS ==========
  
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

  _trackTimer(timer) {
    if (timer) this._allTimers.add(timer);
    return timer;
  }

  _clearTimer(timer) {
    if (timer) {
      try { clearTimeout(timer); } catch(e) {}
      try { clearInterval(timer); } catch(e) {}
      this._allTimers.delete(timer);
    }
  }

  // ========== DESTROY ==========
  
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
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
      
      for (const [room, game] of this.lowcardGames) {
        this._cleanupLowcardTimers(game);
        await this._forceCleanupLowcardGame(room, game);
      }
      this.lowcardGames.clear();
      
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
      this.lowcardLocks.clear();
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
