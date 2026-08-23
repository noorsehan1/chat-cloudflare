// ==================== GAME-SERVER-HIBERNATION-FULL-FIXED.js ====================
// VERSION: 6.6.0 - FULL HIBERNATION API WITH CACHE PERSISTENCE (STORAGE ONLY)

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
};

const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: "01:00", end: "02:00" },
    { start: "18:19", end: "19:00" },
    { start: "20:00", end: "22:00" }
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
      
      if (delayMs > 0) {
        await this._scheduleAlarm(CONSTANTS.WEEKLY_RESET_ALARM, delayMs);
      }
      
      return true;
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

  async loadInitialData(env) {
    try { if (!env?.QUESTIONS) return; } catch(e) {}
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
      
      // ===== CACHE =====
      this.cacheManager = new CacheManager();
      this.diceGameSystem = new DiceGameSystem(this);
      this.alarmScheduler = new AlarmScheduler(env, state);
      this._cachedLastWeekWinner = null;
      this._kvCache = new KVCache();
      
      // ===== MEMORY CACHE =====
      this.activeGames = new Map();
      this.wsMap = new Map();
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.userConnections = new Map();
      this._eventQueue = [];
      this._allTimers = new Set();
      this._lastNotifTime = {};
      
      // ===== DICE STATE =====
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
      
      // ===== TIE STATE =====
      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._tieLock = false;
      this._tieNotificationTimeouts = [];
      
      // ===== LOCKS =====
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._cleanupTimers = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();
      this._evaluationLocks = new Map();
      this._gameOperationLocks = new Map();
      this._drawLocks = new Map();
      this._cleanupLocks = new Map();
      
      // ===== RATE LIMIT =====
      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      this._reconnectAttempts = new Map();
      
      this.DICE_ROOM = CONSTANTS.DICE_ROOM;
      
      // ============================================================
      // ✅ RESTORE SEMUA CACHE DARI STORAGE
      // ============================================================
      this._restoreAllCache().then(() => {
        this.alarmScheduler.scheduleAlarms().catch(() => {});
        this._initLazy();
      });
      
    } catch(e) {}
  }

  // ============================================================
  // ✅ SAVE ALL CACHE KE STORAGE (HANYA CACHE, BUKAN STATE GAME)
  // ============================================================
  async _saveAllCache() {
    try {
      // 1. Save recording status
      const recordingStatusMap = {};
      for (const [room, status] of this.cacheManager.recordingStatus) {
        recordingStatusMap[room] = status;
      }
      await this.ctx.storage.put('recordingStatusMap', recordingStatusMap);
      
      // 2. Save winners
      const winnersMap = {};
      for (const [room, data] of this.cacheManager.winnersCache) {
        winnersMap[room] = data.winners;
      }
      await this.ctx.storage.put('winnersMap', winnersMap);
      
      // 3. Save dice points (backup)
      const points = this.diceGameSystem.pointsCache.getPoints();
      if (points) {
        await this.ctx.storage.put('dicePointsBackup', points);
      }
      
      // 4. Save last week winner
      if (this._cachedLastWeekWinner) {
        await this.ctx.storage.put('cachedLastWeekWinner', this._cachedLastWeekWinner);
      }
      
      // ❌ HAPUS diceState - TIDAK PERLU DISIMPAN
      // State dice aktif = tidak hibernasi
      
    } catch(e) {
      console.error('Failed to save cache:', e);
    }
  }

  // ============================================================
  // ✅ RESTORE ALL CACHE - HANYA DARI STORAGE
  // ============================================================
  async _restoreAllCache() {
    try {
      // 1. RESTORE WEBSOCKET (OTOMATIS DARI ATTACHMENT)
      const webSockets = this.ctx.getWebSockets();
      for (const ws of webSockets) {
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.wsId) {
            ws._wsId = attachment.wsId;
            ws._closing = false;
            ws.username = attachment.username || null;
            ws.room = attachment.room || null;
            ws.roomname = attachment.roomname || null;
            ws._createdAt = attachment.createdAt || Date.now();
            
            this.wsMap.set(attachment.wsId, ws);
            
            if (attachment.room) {
              if (!this.wsClients.has(attachment.room)) {
                this.wsClients.set(attachment.room, new Set());
              }
              this.wsClients.get(attachment.room).add(attachment.wsId);
              this.clientRooms.set(attachment.wsId, attachment.room);
            }
            
            if (attachment.username) {
              this.userConnections.set(attachment.username, {
                wsId: attachment.wsId,
                ws: ws,
                room: attachment.room,
                timestamp: Date.now()
              });
            }
          }
        } catch(e) {}
      }
      
      // ============================================================
      // ✅ RESTORE RECORDING STATUS - DARI STORAGE
      // ============================================================
      const recordingStatusMap = await this.ctx.storage.get('recordingStatusMap');
      if (recordingStatusMap) {
        for (const [room, status] of Object.entries(recordingStatusMap)) {
          this.cacheManager.recordingStatus.set(room, status);
        }
      }
      
      // ============================================================
      // ✅ RESTORE WINNERS - DARI STORAGE
      // ============================================================
      const winnersMap = await this.ctx.storage.get('winnersMap');
      if (winnersMap) {
        for (const [room, winners] of Object.entries(winnersMap)) {
          if (Object.keys(winners).length > 0) {
            this.cacheManager.winnersCache.set(room, { winners });
          }
        }
      }
      
      // ============================================================
      // ✅ RESTORE DICE POINTS - DARI STORAGE
      // ============================================================
      const dicePoints = await this.ctx.storage.get('dicePointsBackup');
      if (dicePoints) {
        await this.diceGameSystem.pointsCache.setPoints(dicePoints, this.env);
        await this.diceGameSystem.getPoints();
      }
      
      // ============================================================
      // ✅ RESTORE LAST WEEK WINNER - DARI STORAGE
      // ============================================================
      const lastWeekWinner = await this.ctx.storage.get('cachedLastWeekWinner');
      if (lastWeekWinner) {
        this._cachedLastWeekWinner = lastWeekWinner;
      } else {
        this._cachedLastWeekWinner = null;
      }
      
      // ❌ HAPUS diceState restore - TIDAK PERLU
      // State dice direstore dari memory awal (null)
      
      // ============================================================
      // ✅ RESTORE ALARM
      // ============================================================
      if (!this.closing && !this.isDestroyed) {
        const existingAlarm = await this.ctx.storage.getAlarm();
        if (!existingAlarm) {
          await this.alarmScheduler.scheduleAlarms();
        }
      }
      
    } catch(e) {
      console.error('Restore cache error:', e);
    }
  }

  // ============================================================
  // ✅ SAVE CACHE KE STORAGE (UTILITY)
  // ============================================================
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
        if (data) {
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
      if (!this.alarmScheduler.isDiceTime()) {
        return;
      }
      
      this.diceAutoEnabled = true;
      
      if (this.currentDiceRoll && this._canSubmitDiceAnswer) {
        return;
      }
      
      if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) {
        return;
      }
      
      const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
      if (clients && clients.size > 0) {
        this._startDiceFast();
      }
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

  // ============================================================
  // ✅ ALARM
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
                if (this.currentDiceRoll && this._canSubmitDiceAnswer) {
                  break;
                }
                if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) {
                  break;
                }
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
        } catch(e) {
          console.error(`Alarm ${alarm.name} error:`, e);
        }
      }
      
      await this.alarmScheduler.scheduleAlarms();
      
    } catch(e) {
      console.error('Alarm processing error:', e);
      try {
        await this.alarmScheduler.scheduleAlarms();
      } catch(err) {
        console.error('Failed to reschedule alarms:', err);
      }
    }
  }

  _getCurrentWeek() {
    const now = new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }

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
        await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winnerData));
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceLastWeekWinner", winner, highestScore, currentWeek]);
      } else {
        this._cachedLastWeekWinner = null;
        await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
      }
      
      await this.diceGameSystem.resetPoints();
      this.diceGameSystem.clearCache();
      
      // ✅ SAVE ALL CACHE
      await this._saveAllCache();
      
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
  // ✅ FETCH - DENGAN HIBERNATION API
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

  // ============================================================
  // ✅ WEBSOCKET EVENT HANDLERS
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
    } catch(e) {
      console.error('WebSocket message error:', e);
    }
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
  // ✅ SWITCH ROOM - UPDATE ATTACHMENT
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

  // ============================================================
  // ✅ SEMUA METHOD LAINNYA (SAMA PERSIS DENGAN KODE ASLI)
  // ============================================================
  
  // ... semua method dari kode asli tetap sama ...
  // (startGame, joinGame, submitNumber, leaveGame, dice, tie breaker, dll)
  // HANYA _saveAllCache() dan _restoreAllCache() yang diubah

  // ============================================================
  // ✅ DICE - HAPUS PANGGILAN _saveAllCache() YANG TIDAK PERLU
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
      
      // ❌ HAPUS _saveAllCache() - tidak perlu
      
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
      
      // ❌ HAPUS _saveAllCache() - tidak perlu
      
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

  // ============================================================
  // ✅ RECORDING - DENGAN STORAGE
  // ============================================================
  async _startRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      if (this.cacheManager.getRecordingStatus(roomName)) {
        this._broadcastToRoom(roomName, ["recordingStatus", true]);
        return true;
      }
      
      // 1️⃣ UPDATE CACHE
      this.cacheManager.recordingStatus.set(roomName, true);
      
      // 2️⃣ SIMPAN KE KV
      await this._saveRecordingStatus(roomName, true);
      
      // 3️⃣ SIMPAN KE STORAGE
      await this._saveAllCache();
      
      this._broadcastToRoom(roomName, ["recordingStatus", true]);
      return true;
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
      
      // 1️⃣ UPDATE CACHE
      this.cacheManager.recordingStatus.set(room, false);
      this.cacheManager.winnersCache.delete(room);
      
      // 2️⃣ SIMPAN KE KV
      await this._saveRecordingStatus(room, false);
      await this._saveWinners(room, {});
      
      // 3️⃣ SIMPAN KE STORAGE
      await this._saveAllCache();
      
      // ✅ HANYA KIRIM STATUS RECORDING, TANPA SCORE
      this._broadcastToRoom(room, ["recordingStatus", false]);
      
      return true;
    } catch(e) { return false; }
  }

  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username || room === CONSTANTS.DICE_ROOM) return false;
      if (!this.cacheManager.getRecordingStatus(room)) return false;
      if (!this.env?.QUESTIONS) return false;
      
      // 1️⃣ UPDATE CACHE
      let roomWinners = this.cacheManager.getWinners(room);
      let count = 0;
      if (roomWinners[username]) {
        count = parseInt(String(roomWinners[username]).replace("x", "").replace("X", "")) || 0;
      }
      roomWinners[username] = (count + 1) + "x";
      this.cacheManager.winnersCache.set(room, { winners: roomWinners });
      
      // 2️⃣ SIMPAN KE KV
      await this._saveWinners(room, roomWinners);
      
      // 3️⃣ SIMPAN KE STORAGE
      await this._saveAllCache();
      
      return true;
    } catch(e) { return false; }
  }

  // ============================================================
  // ✅ DESTROY
  // ============================================================
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
      // ✅ SAVE ALL CACHE TERAKHIR
      await this._saveAllCache();
      
      // ... rest of destroy code ...
      
    } catch(e) {}
  }
}

export default GameServer;
