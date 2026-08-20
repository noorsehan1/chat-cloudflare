// ==================== GAME-SERVER.JS ====================
// VERSION: 4.3.0 - FULL CLASS + ALARM JADWAL + CLEANUP

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
  STALE_GAME_TIMEOUT_MS: 600000,
  STUCK_DRAW_TIMEOUT_MS: 60000,
  STUCK_REGISTRATION_TIMEOUT_MS: 30000,
  MAX_WS_CLIENTS: 50,
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
  CLEANUP_INTERVAL_MS: 60000,
  HAND_SHAKE_TIMEOUT_MS: 3000,
  RATE_LIMIT_MAX: 100,
  RATE_LIMIT_WINDOW_MS: 60000,
  MAX_BOT_TIMEOUTS: 5,
  MAX_EVENT_ITERATIONS: 2,
  DICE_TIMER_INTERVAL_MS: 1000,
  MAP_CLEANUP_AGE_MS: 1800000,
  BAN_DURATION_MS: 180000,
  MAX_RECONNECT_ATTEMPTS: 5,
  RECONNECT_WINDOW_MS: 30000,
  
  DICE_CHECK_INTERVAL_MS: 10000,
  DICE_START_WINDOW_MS: 60000,
  
  CACHE_TTL_MS: 60000,
};

const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: 1, end: 2 },
    { start: 14, end: 15 },
    { start: 22, end: 23 }
  ],
  TIMEZONE_OFFSET: 8,
};

const DICE_ROOM = "Quiz";

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(name, data = null) {
    this.name = name;
    this.players = new Map();
    this.botPlayers = new Map();
    this.numbers = new Map();
    this.tanda = new Map();
    this.eliminated = new Set();
    this.playerWsId = new Map();
    this.betAmount = 0;
    this.round = 1;
    this.registrationOpen = false;
    this.evaluationLocked = false;
    this.drawTimeExpired = false;
    this._isActive = false;
    this._gameEnded = false;
    this._phase = 'idle';
    this._createdAt = Date.now();
    this._drawPhaseStart = null;
    this._endTime = null;
    this._startedByRecording = false;
    this._startedBy = null;
    this.hostId = null;
    this.hostName = null;
    this.useBots = false;
    this._botsAdded = false;
    
    if (data) {
      if (data.players) {
        for (const [key, val] of Object.entries(data.players)) {
          this.players.set(key, val);
        }
      }
      if (data.botPlayers) {
        for (const [key, val] of Object.entries(data.botPlayers)) {
          this.botPlayers.set(key, val);
        }
      }
      if (data.numbers) {
        for (const [key, val] of Object.entries(data.numbers)) {
          this.numbers.set(parseInt(key), val);
        }
      }
      if (data.tanda) {
        for (const [key, val] of Object.entries(data.tanda)) {
          this.tanda.set(key, val);
        }
      }
      if (data.eliminated) {
        for (const val of data.eliminated) {
          this.eliminated.add(val);
        }
      }
      if (data.playerWsId) {
        for (const [key, val] of Object.entries(data.playerWsId)) {
          this.playerWsId.set(key, val);
        }
      }
      this.betAmount = data.betAmount || 0;
      this.round = data.round || 1;
      this.registrationOpen = data.registrationOpen || false;
      this.evaluationLocked = data.evaluationLocked || false;
      this.drawTimeExpired = data.drawTimeExpired || false;
      this._isActive = data._isActive || false;
      this._gameEnded = data._gameEnded || false;
      this._phase = data._phase || 'idle';
      this._createdAt = data._createdAt || Date.now();
      this._drawPhaseStart = data._drawPhaseStart || null;
      this._endTime = data._endTime || null;
      this._startedByRecording = data._startedByRecording || false;
      this._startedBy = data._startedBy || null;
      this.hostId = data.hostId || null;
      this.hostName = data.hostName || null;
      this.useBots = data.useBots || false;
      this._botsAdded = data._botsAdded || false;
    }
  }

  toJSON() {
    const players = {};
    for (const [key, val] of this.players) {
      players[key] = val;
    }
    const botPlayers = {};
    for (const [key, val] of this.botPlayers) {
      botPlayers[key] = val;
    }
    const numbers = {};
    for (const [key, val] of this.numbers) {
      numbers[key] = val;
    }
    const tanda = {};
    for (const [key, val] of this.tanda) {
      tanda[key] = val;
    }
    const eliminated = Array.from(this.eliminated);
    const playerWsId = {};
    for (const [key, val] of this.playerWsId) {
      playerWsId[key] = val;
    }
    
    return {
      players, botPlayers, numbers, tanda, eliminated, playerWsId,
      betAmount: this.betAmount,
      round: this.round,
      registrationOpen: this.registrationOpen,
      evaluationLocked: this.evaluationLocked,
      drawTimeExpired: this.drawTimeExpired,
      _isActive: this._isActive,
      _gameEnded: this._gameEnded,
      _phase: this._phase,
      _createdAt: this._createdAt,
      _drawPhaseStart: this._drawPhaseStart,
      _endTime: this._endTime,
      _startedByRecording: this._startedByRecording,
      _startedBy: this._startedBy,
      hostId: this.hostId,
      hostName: this.hostName,
      useBots: this.useBots,
      _botsAdded: this._botsAdded
    };
  }

  getActivePlayers() {
    return Array.from(this.players.entries())
      .filter(([id]) => !this.eliminated.has(id))
      .map(([, p]) => p);
  }

  getActivePlayerIds() {
    return Array.from(this.players.keys())
      .filter(id => !this.eliminated.has(id));
  }

  getCount() { return this.players.size; }
  getActiveCount() { return this.getActivePlayers().length; }
}

// ==================== KV CACHE ====================
class KVCache {
  constructor() {
    this.cache = new Map();
  }

  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;
    return entry.value;
  }

  set(key, value) {
    this.cache.set(key, { value, timestamp: Date.now() });
  }

  delete(key) { this.cache.delete(key); }
  clear() { this.cache.clear(); }
  
  update(key, value) {
    if (this.cache.has(key)) {
      this.cache.set(key, { value, timestamp: Date.now() });
      return true;
    }
    return false;
  }
  
  has(key) {
    return this.cache.has(key);
  }
  
  keys() {
    return Array.from(this.cache.keys());
  }
}

// ==================== CACHE MANAGER ====================
class CacheManager {
  constructor() {
    this.recordingStatus = new Map();
    this.winnersCache = new Map();
    this.cacheTTL = CONSTANTS.CACHE_TTL_MS || 60000;
    this._updateLocks = new Map();
    this._restored = false;
    this._restorePromise = null;
  }

  async restore(env) {
    if (this._restored) return;
    if (!env?.QUESTIONS) return;
    
    this._restorePromise = (async () => {
      try {
        const rooms = ["LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", 
                       "Birthday Party", "Sweet Memories", "Lounge Talk", 
                       "Noxxeliverothcifsa", "BESTIES", "Happy Vibes", "The Chatter Room"];
        
        for (const room of rooms) {
          try {
            const key = CONSTANTS.LOWCARD_RECORDING_KEY + room;
            const status = await env.QUESTIONS.get(key);
            if (status === 'true') {
              this.recordingStatus.set(room, true);
            }
          } catch(e) {}
        }
        
        for (const room of rooms) {
          try {
            const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
            const winners = await env.QUESTIONS.get(key, 'json');
            if (winners) {
              this.winnersCache.set(room, {
                winners: winners,
                timestamp: Date.now()
              });
            }
          } catch(e) {}
        }
        
        this._restored = true;
      } catch(e) {}
    })();
    
    await this._restorePromise;
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
    if (cached) {
      if (Date.now() - cached.timestamp < this.cacheTTL) {
        return cached.winners;
      }
      return cached.winners;
    }
    return {};
  }

  async addWinner(room, username, env) {
    if (!room || !username || room === DICE_ROOM) return false;
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
      
      this.winnersCache.set(room, {
        winners: roomWinners,
        timestamp: Date.now()
      });
      
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
      this.winnersCache.set(room, {
        winners: winners,
        timestamp: Date.now()
      });
    } catch(e) {}
  }

  cleanup() {
    const now = Date.now();
    for (const [room, data] of this.winnersCache) {
      if (now - data.timestamp > this.cacheTTL * 2) {
        this.winnersCache.delete(room);
      }
    }
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
    this.cacheTTL = 30000;
    this.leaderboardCache = new Map();
    this.leaderboardTTL = 60000;
    this._updateLocks = new Map();
  }

  getPoints() {
    const cached = this.pointsCache.get('points');
    if (cached) {
      if (Date.now() - cached.timestamp < this.cacheTTL) {
        return cached.data;
      }
      return cached.data;
    }
    return null;
  }

  async setPoints(points, env) {
    if (!env?.QUESTIONS) return false;
    
    const lockKey = 'dice_points_update';
    if (this._updateLocks.has(lockKey)) return false;
    
    this._updateLocks.set(lockKey, Date.now());
    
    try {
      this.pointsCache.set('points', {
        data: points,
        timestamp: Date.now()
      });
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
      this.pointsCache.set('points', {
        data: points,
        timestamp: Date.now()
      });
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
    if (cached) {
      if (Date.now() - cached.timestamp < this.leaderboardTTL) {
        return cached.data.slice(0, limit);
      }
      return cached.data.slice(0, limit);
    }
    
    const points = this.getPoints();
    if (points) {
      const sorted = Object.entries(points)
        .sort((a, b) => b[1] - a[1]);
      this.leaderboardCache.set('leaderboard', {
        data: sorted,
        timestamp: Date.now()
      });
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
    } catch(e) {
      return false;
    } finally {
      this._updateLocks.delete(lockKey);
    }
  }

  async _reloadFromKV(env) {
    try {
      if (!env?.QUESTIONS) return;
      const points = await env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.pointsCache.set('points', {
        data: points,
        timestamp: Date.now()
      });
    } catch(e) {}
  }

  cleanup() {
    const now = Date.now();
    for (const [key, data] of this.pointsCache) {
      if (now - data.timestamp > this.cacheTTL * 2) {
        this.pointsCache.delete(key);
      }
    }
    for (const [key, data] of this.leaderboardCache) {
      if (now - data.timestamp > this.leaderboardTTL * 2) {
        this.leaderboardCache.delete(key);
      }
    }
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
      const points = await this.getPoints();
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
  
  cleanup() {
    if (this.pointsCache) {
      this.pointsCache.cleanup();
    }
  }
  
  clearCache() {
    this.userScores.clear();
    this._isLoaded = false;
    if (this.pointsCache) {
      this.pointsCache.clear();
    }
  }
}

// ==================== GAME SERVER ====================
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
      this._lastActivity = Date.now();
      this._restored = false;
      this._restorePromise = null;
      
      // ========== CACHE ==========
      this.cacheManager = new CacheManager();
      this._kvCache = new KVCache();
      this._cachedResetWeek = null;
      this._cachedLastWeekWinner = null;
      
      // ========== CORE MAPS ==========
      this.activeGames = new Map();
      this.wsMap = new Map();
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.userConnections = new Map();
      
      // ========== DICE ==========
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
      this._diceTimerInterval = null;
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
      
      // ========== TIE BREAKER ==========
      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._tieLock = false;
      
      // ========== QUEUE ==========
      this._eventQueue = [];
      this._isProcessingQueue = false;
      this._allTimers = new Set();
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._cleanupTimers = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();
      
      // ========== ANTI RACE ==========
      this._evaluationLocks = new Map();
      this._gameOperationLocks = new Map();
      this._drawLocks = new Map();
      this._cleanupLocks = new Map();
      
      // ========== CIRCUIT BREAKER ==========
      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      
      // ========== RECONNECT ==========
      this._reconnectAttempts = new Map();
      this._bannedUsers = new Map();
      
      // ========== THROTTLE ==========
      this._lastNotifTime = {};
      
      // ========== ALARM ==========
      this._alarmScheduled = false;
      this._lastAlarmCheck = Date.now();
      
      // ========== DICE ROOM ==========
      this.DICE_ROOM = DICE_ROOM;
      
      // ========== MULAI RESTORE ==========
      this._restorePromise = this._restoreAllState();
      
    } catch(e) {}
  }

  // ========== RESTORE ALL STATE ==========
  async _restoreAllState() {
    try {
      await this.cacheManager.restore(this.env);
      
      if (!this.diceGameSystem) {
        this.diceGameSystem = new DiceGameSystem(this);
      }
      await this.diceGameSystem.loadScores();
      
      const webSockets = this.ctx.getWebSockets();
      for (const ws of webSockets) {
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username) {
            const wsId = ++this._wsIdCounter;
            ws._wsId = wsId;
            ws._closing = false;
            ws.username = attachment.username;
            ws.room = attachment.room || null;
            ws.roomname = attachment.room || null;
            
            this.wsMap.set(wsId, ws);
            
            if (attachment.room) {
              const clients = this.wsClients.get(attachment.room);
              if (clients) clients.add(wsId);
              else this.wsClients.set(attachment.room, new Set([wsId]));
              this.clientRooms.set(wsId, attachment.room);
            }
            
            if (attachment.username) {
              this.userConnections.set(attachment.username, {
                wsId, ws, room: attachment.room, timestamp: Date.now()
              });
            }
          }
        } catch(e) {}
      }
      
      this._restored = true;
      this._scheduleAlarm();
      
    } catch(e) {}
  }

  // ========== SCHEDULE ALARM ==========
  _scheduleAlarm() {
    if (this.closing || this.isDestroyed) return;
    
    const now = Date.now();
    const nextSchedule = this._getNextScheduleTime();
    
    if (nextSchedule) {
      const delay = nextSchedule - now;
      if (delay > 0) {
        this.ctx.storage.setAlarm(nextSchedule);
        this._alarmScheduled = true;
        this._lastAlarmCheck = now;
      }
    }
  }

  // ========== GET NEXT SCHEDULE TIME ==========
  _getNextScheduleTime() {
    try {
      const now = new Date();
      const witaTime = this._getCurrentWITATime();
      const currentTotal = witaTime.totalMinutes;
      
      let nextStart = null;
      let nextEnd = null;
      let foundFuture = false;
      
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = session.start * 60;
        const endTotal = session.end * 60;
        
        if (currentTotal < startTotal) {
          const minutesUntilStart = startTotal - currentTotal;
          const scheduleTime = new Date(now);
          scheduleTime.setUTCHours(0, 0, 0, 0);
          scheduleTime.setUTCMinutes(minutesUntilStart);
          scheduleTime.setUTCHours(session.start);
          if (!nextStart || scheduleTime < nextStart) {
            nextStart = scheduleTime;
          }
          foundFuture = true;
        } else if (currentTotal >= startTotal && currentTotal < endTotal) {
          const minutesUntilEnd = endTotal - currentTotal;
          const scheduleTime = new Date(now);
          scheduleTime.setUTCHours(0, 0, 0, 0);
          scheduleTime.setUTCMinutes(minutesUntilEnd);
          scheduleTime.setUTCHours(session.end);
          nextEnd = scheduleTime;
          break;
        }
      }
      
      if (!foundFuture && !nextEnd) {
        const tomorrow = new Date(now);
        tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
        tomorrow.setUTCHours(0, 0, 0, 0);
        
        const firstSession = QUIZ_SCHEDULE.SESSIONS[0];
        tomorrow.setUTCHours(firstSession.start);
        tomorrow.setUTCMinutes(0, 0, 0);
        nextStart = tomorrow;
      }
      
      return nextStart || nextEnd;
      
    } catch(e) {
      return null;
    }
  }

  // ========== ALARM HANDLER ==========
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    if (!this._restored) {
      await this._restorePromise;
    }
    
    const witaTime = this._getCurrentWITATime();
    const currentTotal = witaTime.totalMinutes;
    
    let inSchedule = false;
    for (const session of QUIZ_SCHEDULE.SESSIONS) {
      const startTotal = session.start * 60;
      const endTotal = session.end * 60;
      if (currentTotal >= startTotal && currentTotal < endTotal) {
        inSchedule = true;
        break;
      }
    }
    
    if (inSchedule) {
      // ===== DALAM JADWAL QUIZ =====
      // ✅ CLEANUP
      this._cleanupDeadConnections();
      this._cleanupUserConnections();
      this._cleanupReconnectAttempts();
      this._cleanupStaleLocks();
      this._cleanupMemory();
      if (this.cacheManager) this.cacheManager.cleanup();
      if (this.diceGameSystem) this.diceGameSystem.cleanup();
      
      // ✅ CHECK DICE
      this._checkDice();
      
      // ✅ CHECK STUCK GAMES
      this._checkStuckGames();
      
    } else {
      // ===== DI LUAR JADWAL QUIZ =====
      // HANYA STALE LOCKS (ringan)
      this._cleanupStaleLocks();
    }
    
    // ✅ JADWALKAN ALARM BERIKUTNYA
    this._scheduleAlarm();
  }

  // ========== LAZY INIT ==========
  _initLazy() {
    if (this._initialized || this.closing || this.isDestroyed) return;
    this._initialized = true;
    
    try {
      if (!this.diceGameSystem) {
        this.diceGameSystem = new DiceGameSystem(this);
      }
    } catch(e) {
      // ❌ TIDAK ADA setTimeout!
      this._initialized = false;
    }
  }

  // ========== UTILITY ==========
  _getCurrentWITATime() {
    try {
      const now = new Date();
      const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
      const minutes = now.getUTCMinutes();
      return { hours, minutes, totalMinutes: (hours * 60) + minutes };
    } catch(e) { return { hours: 0, minutes: 0, totalMinutes: 0 }; }
  }

  _isDiceTime() {
    try {
      const witaTime = this._getCurrentWITATime();
      const currentTotal = witaTime.totalMinutes;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = session.start * 60;
        const endTotal = session.end * 60;
        if (currentTotal >= startTotal && currentTotal < endTotal) return true;
      }
      return false;
    } catch(e) { return false; }
  }

  _getTimeLeftUntilNextDice() {
    try {
      const witaTime = this._getCurrentWITATime();
      const currentTotal = witaTime.totalMinutes;
      let minDiff = Infinity;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        let startTotal = session.start * 60;
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) minDiff = diff;
      }
      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      const isRunning = this._isDiceTime();
      return { 
        hours, minutes, totalMs: minDiff * 60 * 1000,
        text: `${hours}h ${minutes}m`,
        isRunning: isRunning
      };
    } catch(e) {
      return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false };
    }
  }

  // ========== CHECK DICE ==========
  _checkDice() {
    try {
      if (!this._isDiceTime()) return;
      if (this._tieActive || this._isShowingDice || this._diceTimeUpCooldown) return;
      if (this.currentDiceRoll || this._diceTimeout || this._diceLock) return;
      
      const clients = this.wsClients?.get(DICE_ROOM);
      if (!clients || clients.size === 0) return;
      
      this._startDiceFast();
    } catch(e) {}
  }

  // ========== START DICE ==========
  _startDiceFast() {
    try {
      if (this._diceLock || this.currentDiceRoll || this._isShowingDice) return;
      if (!this._isDiceTime()) return;
      
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
      
      this._broadcastToRoom(DICE_ROOM, ["diceRoll", { 
        value, timestamp: Date.now(),
        answerTime: 20,
        canAnswerNow: true,
        round: this._diceRound
      }]);
      
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "♡ clik draw ♡"]);
      
      let timeLeft = 20;
      let notifSent = { 15: false, 10: false, 5: false };
      
      const timerInterval = setInterval(() => {
        timeLeft--;
        if (timeLeft === 15 && !notifSent[15]) {
          notifSent[15] = true;
          this._broadcastToRoom(DICE_ROOM, ["diceNotification", `${timeLeft}s remaining`]);
        } else if (timeLeft === 10 && !notifSent[10]) {
          notifSent[10] = true;
          this._broadcastToRoom(DICE_ROOM, ["diceNotification", `${timeLeft}s remaining`]);
        } else if (timeLeft === 5 && !notifSent[5]) {
          notifSent[5] = true;
          this._broadcastToRoom(DICE_ROOM, ["diceNotification", `${timeLeft}s remaining`]);
        }
        if (timeLeft <= 0) {
          clearInterval(timerInterval);
          this._endDiceRound();
        }
      }, 1000);
      
      this._diceTimerInterval = timerInterval;
      this._allTimers.add(timerInterval);
      
      this._diceTimeout = setTimeout(() => {
        this._endDiceRound();
      }, 20000);
      this._allTimers.add(this._diceTimeout);
      
    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  // ========== END DICE ROUND ==========
  async _endDiceRound() {
    try {
      if (this._diceTimerInterval) {
        clearInterval(this._diceTimerInterval);
        this._diceTimerInterval = null;
      }
      if (this._diceTimeout) {
        clearTimeout(this._diceTimeout);
        this._diceTimeout = null;
      }
      
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
        this._broadcastToRoom(DICE_ROOM, ["diceNoWinner", {
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
            this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
              username: winner,
              totalPoints: points[winner] || 0,
              diceValue: diceValue,
              round: roundNumber
            }]);
          } else {
            this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
              username: winner,
              totalPoints: 0,
              diceValue: diceValue,
              round: roundNumber
            }]);
          }
        } catch(e) {
          this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
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
        await this._startTieBreaker(DICE_ROOM, correctPlayers);
        return;
      }
      
      this.currentDiceRoll = null;
      this._diceLock = false;
      this._diceTimeUpCooldown = true;
      
      setTimeout(() => {
        this._diceTimeUpCooldown = false;
        this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
        this._lastSentRemaining = -1;
        this._checkDice();
      }, 15000);
      
    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  // ========== SUBMIT DICE ANSWER ==========
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
        this._broadcastToRoom(DICE_ROOM, ["diceAnswer", {
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
              await this._processTieResults(DICE_ROOM, tieId, this._tiePlayers);
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
      this._broadcastToRoom(DICE_ROOM, ["diceAnswer", {
        username, guess: guessValue, round: this._diceRound || 1
      }]);
      
      if (guessValue === diceValue && !this.diceHasWinner) {
        this.diceHasWinner = true;
        this.diceWinner = username;
      }
    } catch(e) {}
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
    } finally {
      setTimeout(() => { this._tieLock = false; }, 2000);
    }
  }

  async _runTieRound(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    
    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);
    
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
    
    this._broadcastToRoom(DICE_ROOM, ["diceNotification", 
      `♡ Tie Round ${this._tieRound}: ${players.join(', ')}`
    ]);
    
    this._startTieTimer(room, id, players);
  }

  _startTieTimer(room, id, players) {
    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);
    
    let timeLeft = CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20;
    let notified10 = false, notified5 = false, isProcessed = false;
    
    this._tieInterval = this._trackTimer(setInterval(() => {
      timeLeft--;
      if (timeLeft === 10 && !notified10) { notified10 = true; this._broadcastToRoom(DICE_ROOM, ["diceNotification", "10s remaining"]); }
      if (timeLeft === 5 && !notified5) { notified5 = true; this._broadcastToRoom(DICE_ROOM, ["diceNotification", "5s remaining"]); }
      if (timeLeft === 3) { this._broadcastToRoom(DICE_ROOM, ["diceNotification", "3s remaining"]); }
      if (timeLeft <= 0 && !isProcessed) {
        isProcessed = true;
        this._clearTimer(this._tieInterval);
        this._tieInterval = null;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "TIME UP"]);
        const tieId = this._getActiveTieBreakerId();
        if (tieId) this._processTieResults(room, tieId, players);
        else { this._resetTieBreakerState(null); this._startCooldownAfterTieBreaker(); }
      }
    }, 1000));
    
    this._tieTimer = this._trackTimer(setTimeout(() => {
      if (!isProcessed) {
        isProcessed = true;
        this._clearTimer(this._tieInterval);
        this._tieInterval = null;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "TIME UP"]);
        const tieId = this._getActiveTieBreakerId();
        if (tieId) this._processTieResults(room, tieId, players);
        else { this._resetTieBreakerState(null); this._startCooldownAfterTieBreaker(); }
      }
    }, (CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20) * 1000 + 2000));
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
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "No one answered tie breaker"]);
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
          this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
            username: winner, totalPoints: points[winner] || 0,
            diceValue: highest, round: this._diceRound || 1,
            isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
          }]);
        } else {
          this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
            username: winner, totalPoints: 0,
            diceValue: highest, round: this._diceRound || 1,
            isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
          }]);
        }
      } catch(e) {
        this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
          username: winner, totalPoints: 0,
          diceValue: highest, round: this._diceRound || 1,
          isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
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
        this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
          username: winner, totalPoints: points[winner] || 0,
          diceValue: 'auto', round: this._diceRound || 1,
          isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
        }]);
      } else {
        this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
          username: winner, totalPoints: 0,
          diceValue: 'auto', round: this._diceRound || 1,
          isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
        }]);
      }
    } catch(e) {
      this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
        username: winner, totalPoints: 0,
        diceValue: 'auto', round: this._diceRound || 1,
        isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
      }]);
    }
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  _startCooldownAfterTieBreaker() {
    this._broadcastToRoom(DICE_ROOM, ["diceNotification", "wait 15s"]);
    this._diceTimeUpCooldown = true;
    this._clearTimer(this._diceTimeUpCooldownTimer);
    this._diceTimeUpCooldownTimer = this._trackTimer(setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._checkDice();
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
    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);
    this._tieTimer = null;
    this._tieInterval = null;
  }

  _getActiveTieBreakerId() {
    for (const [id, data] of this._tieBreakers) {
      if (data.status === 'waiting' || data.status === 'running') return id;
    }
    return null;
  }

  // ========== CLEANUP METHODS ==========
  
  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      let removed = 0;
      for (const [wsId, ws] of this.wsMap) {
        if (removed > 10) break;
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(wsId);
          removed++;
        }
      }
      for (const wsId of toRemove) {
        const ws = this.wsMap.get(wsId);
        if (ws) {
          const room = this.clientRooms.get(wsId);
          if (room) this._removeClientFromRoom(room, wsId);
          this.clientRooms.delete(wsId);
          this.wsMap.delete(wsId);
        }
      }
    } catch(e) {}
  }

  _cleanupUserConnections() {
    try {
      let cleaned = 0;
      for (const [username, conn] of this.userConnections) {
        if (cleaned > 10) break;
        if (!conn?.ws || conn.ws.readyState !== 1) {
          this.userConnections.delete(username);
          cleaned++;
        }
      }
    } catch(e) {}
  }

  _cleanupReconnectAttempts() {
    try {
      const now = Date.now();
      let cleaned = 0;
      for (const [username, data] of this._reconnectAttempts) {
        if (cleaned > 10) break;
        if (now - (data.lastAttempt || 0) > 300000) {
          this._reconnectAttempts.delete(username);
          cleaned++;
        }
      }
    } catch(e) {}
  }

  _cleanupStaleLocks() {
    try {
      const now = Date.now();
      const staleTimeout = 30000;
      for (const [key, time] of this._gameLocks) {
        if (now - time > staleTimeout) this._gameLocks.delete(key);
      }
      for (const [key, time] of this._joinLocks) {
        if (now - time > staleTimeout) this._joinLocks.delete(key);
      }
      for (const [key, time] of this._evaluationLocks) {
        if (now - time > staleTimeout) this._evaluationLocks.delete(key);
      }
      for (const [key, time] of this._gameOperationLocks) {
        if (now - time > staleTimeout) this._gameOperationLocks.delete(key);
      }
      for (const [key, time] of this._drawLocks) {
        if (now - time > staleTimeout) this._drawLocks.delete(key);
      }
      for (const [key, time] of this._cleanupLocks) {
        if (now - time > staleTimeout) this._cleanupLocks.delete(key);
      }
    } catch(e) {}
  }

  _cleanupMemory() {
    try {
      if (this.wsMap) {
        const toRemove = [];
        let removed = 0;
        for (const [id, ws] of this.wsMap) {
          if (removed > 10) break;
          if (!ws || ws.readyState !== 1 || ws._closing) {
            toRemove.push(id);
            removed++;
          }
        }
        for (const id of toRemove) {
          this.wsMap.delete(id);
        }
      }
      if (this._eventQueue && this._eventQueue.length > 50) {
        this._eventQueue.splice(0, this._eventQueue.length - 50);
      }
    } catch(e) {}
  }

  _checkStuckGames() {
    try {
      const now = Date.now();
      let checked = 0;
      for (const [room, game] of this.activeGames) {
        checked++;
        if (checked > 5) break;
        if (!game?._isActive || game._gameEnded) continue;
        if (game._phase === 'draw' && game._drawPhaseStart &&
            (now - game._drawPhaseStart) > CONSTANTS.STUCK_DRAW_TIMEOUT_MS) {
          this._closeDrawPhase(room, game);
        }
        if (game._phase === 'registration' && game.registrationOpen &&
            game._createdAt && (now - game._createdAt) > CONSTANTS.STUCK_REGISTRATION_TIMEOUT_MS) {
          this._closeRegistration(room, game);
        }
      }
    } catch(e) {}
  }

  // ========== BROADCAST ==========
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
        if (this._lastNotifTime[msgKey] && (now - this._lastNotifTime[msgKey]) < 2000) {
          return;
        }
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

  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  _removeClientFromRoom(room, wsId) {
    try {
      if (!room || !wsId) return;
      const clients = this.wsClients.get(room);
      if (clients) { clients.delete(wsId); if (clients.size === 0) this.wsClients.delete(room); }
    } catch(e) {}
  }

  // ========== TIMER MANAGEMENT ==========
  _trackTimer(timer) {
    if (timer) this._allTimers.add(timer);
    return timer;
  }

  _clearTimer(timer) {
    if (timer) {
      clearTimeout(timer);
      clearInterval(timer);
      this._allTimers.delete(timer);
    }
  }

  _acquireLock(lockMap, key, timeoutMs = 5000) {
    if (lockMap.has(key)) return false;
    lockMap.set(key, Date.now());
    // ❌ TIDAK ADA setTimeout auto-release!
    return true;
  }

  _releaseLock(lockMap, key) {
    if (lockMap.has(key)) {
      lockMap.delete(key);
      return true;
    }
    return false;
  }

  // ========== WS HANDLERS ==========
  webSocketClose(ws) {
    try {
      if (!ws) return;
      ws._closing = true;
      try { ws.removeAllListeners(); } catch(e) {}
      
      const username = ws.username;
      if (username) {
        const attempts = this._reconnectAttempts.get(username) || { count: 0, lastAttempt: 0 };
        attempts.count++;
        attempts.lastAttempt = Date.now();
        this._reconnectAttempts.set(username, attempts);
        if (attempts.count > CONSTANTS.MAX_RECONNECT_ATTEMPTS) {
          const now = Date.now();
          if (now - attempts.lastAttempt < CONSTANTS.RECONNECT_WINDOW_MS) {
            this._bannedUsers.set(username, now + CONSTANTS.BAN_DURATION_MS);
            return;
          }
        }
      }
      
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      if (room) this._removeClientFromRoom(room, wsId);
      if (wsId) {
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
      }
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn?.wsId === wsId) this.userConnections.delete(username);
        if (room) this._broadcastToRoom(room, ["userLeftRoom", username, room]);
      }
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
    } catch(e) {}
  }

  webSocketError(ws) {
    try {
      if (!ws) return;
      ws._closing = true;
      try { ws.removeAllListeners(); } catch(e) {}
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
    } catch(e) {}
  }

  // ========== FETCH ==========
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
      
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          uptime: Date.now() - this._startTime,
          connections: this.wsMap.size,
          games: this.activeGames.size,
          queue: this._eventQueue?.length || 0,
          circuitOpen: this._circuitOpen,
          initialized: this._initialized,
          restored: this._restored,
          errors: this._errorCount,
          isDiceTime: this._isDiceTime(),
          alarmScheduled: this._alarmScheduled,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (url.pathname === "/metrics") {
        return new Response(JSON.stringify({
          connections: this.wsMap.size,
          games: this.activeGames.size,
          queue: this._eventQueue?.length || 0,
          errors: this._errorCount,
          circuitOpen: this._circuitOpen,
          uptime: Date.now() - this._startTime,
          restored: this._restored,
          diceActive: !!this.currentDiceRoll,
          diceRound: this._diceRound || 0,
          tieActive: this._tieActive,
          isDiceTime: this._isDiceTime(),
          alarmScheduled: this._alarmScheduled,
          cacheSize: this.cacheManager?.winnersCache?.size || 0,
          pointsCacheSize: this.diceGameSystem?.pointsCache?.pointsCache?.size || 0
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
          this.ctx.acceptWebSocket(server);
        } catch(e) {
          try { server.close(1008, "Accept failed"); } catch(err) {}
          return new Response("WebSocket acceptance failed", { status: 500 });
        }
        
        server.serializeAttachment({});
        
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

  // ========== PROCESS EVENT ==========
  async _processWithTimeout(ws, data, timeoutMs = 500) {
    try {
      const timeoutPromise = new Promise((_, reject) => {
        const timer = setTimeout(() => {
          reject(new Error('Processing timeout'));
        }, timeoutMs);
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
        try {
          await this._processEventItem(item.ws, item.data);
        } catch(e) { this._handleError('processQueue', e); }
        processed++;
      }
      
      if (this._eventQueue.length > 0 && iteration < CONSTANTS.MAX_EVENT_ITERATIONS) {
        // ✅ LANGSUNG PROSES, TANPA setTimeout
        this._isProcessingQueue = false;
        await this._processEventQueue(iteration + 1);
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

  // ========== HANDLE EVENT INTERNAL ==========
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
          if (success) this._broadcastToRoom(DICE_ROOM, ["diceNotification", "Last week winner deleted"]);
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
      if (room === DICE_ROOM) {
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

  // ========== SWITCH ROOM ==========
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
      const wsId = this._getWsId(ws);
      if (!wsId) {
        this._safeSend(ws, ["gameLowCardError", "Connection error"]);
        return;
      }
      
      const currentRoom = ws.room || ws.roomname || this.clientRooms.get(wsId);
      if (currentRoom === roomName) {
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
        if (roomName === DICE_ROOM) {
          this._sendDiceNotificationOnSwitch(ws, wsId);
        }
        return;
      }
      
      const lockKey = `switch_${wsId}`;
      if (this._switchLocks.has(lockKey)) {
        this._safeSend(ws, ["switchRoomError", "Switch in progress"]);
        return;
      }
      
      this._switchLocks.set(lockKey, Date.now());
      
      try {
        if (currentRoom) this._removeClientFromRoom(currentRoom, wsId);
        
        ws.room = roomName;
        ws.roomname = roomName;
        if (username) ws.username = username;
        
        ws.serializeAttachment({
          username: ws.username || username,
          room: roomName
        });
        
        if (!this.wsClients.has(roomName)) {
          this.wsClients.set(roomName, new Set());
        }
        this.wsClients.get(roomName).add(wsId);
        this.clientRooms.set(wsId, roomName);
        this.wsMap.set(wsId, ws);
        
        if (username) {
          this.userConnections.set(username, { wsId, ws, room: roomName, timestamp: Date.now() });
        }
        
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
        
        if (roomName === DICE_ROOM) {
          this._sendDiceNotificationOnSwitch(ws, wsId);
        }
        
        this._broadcastToRoom(roomName, ["userJoinedRoom", username, roomName]);
        if (currentRoom && currentRoom !== roomName) {
          this._broadcastToRoom(currentRoom, ["userLeftRoom", username, currentRoom]);
        }
      } finally {
        setTimeout(() => {
          this._switchLocks.delete(lockKey);
        }, 2000);
      }
    } catch(e) {}
  }

  _getWsId(ws) { return ws?._wsId || null; }

  _sendGameStateToClient(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room) return;
      const game = this.activeGames.get(room);
      if (!game || !game._isActive || game._gameEnded) {
        this._safeSend(ws, ["gameState", { room, hasGame: false, gameType: 'lowcard' }]);
        return;
      }
      
      const activePlayers = game.getActivePlayers();
      const allPlayers = Array.from(game.players.values()).map(p => p.name);
      const eliminated = Array.from(game.eliminated);
      const submitted = Array.from(game.numbers.keys());
      
      this._safeSend(ws, ["gameState", {
        room, hasGame: true, gameType: 'lowcard',
        isActive: game._isActive, phase: game._phase || 'registration',
        round: game.round || 1, bet: game.betAmount || 0,
        host: game.hostName || 'Unknown',
        registrationOpen: game.registrationOpen || false,
        players: allPlayers, activePlayers: activePlayers.map(p => p.name),
        eliminated, submitted, playerCount: game.players.size,
        activeCount: activePlayers.length,
        isEvaluating: false,
        evaluationLocked: game.evaluationLocked || false,
        drawTimeExpired: game.drawTimeExpired || false
      }]);
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
      const isDiceTime = this._isDiceTime();
      
      // ✅ KIRIM LANGSUNG, TANPA DELAY
      if (!isDiceTime) {
        this._safeSend(ws, ["diceNotification", `Next dice game in: ${timeLeft.text}`]);
      }
      
    } catch(e) {}
  }

  forceStartDice() {
    try {
      if (this._tieActive) return false;
      if (this._isShowingDice) return false;
      if (this._diceTimeUpCooldown) return false;
      if (!this._isDiceTime() || this.currentDiceRoll || this._diceTimeout || this._diceStartTimeout) {
        return false;
      }
      this.diceAutoEnabled = true;
      this._startDiceFast();
      return true;
    } catch(e) { return false; }
  }

  // ========== RECORDING ==========
  async _startRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      if (this.cacheManager.getRecordingStatus(roomName)) {
        this._broadcastToRoom(roomName, ["recordingStatus", true]);
        return true;
      }
      const success = await this.cacheManager.setRecordingStatus(roomName, true, this.env);
      if (success) this._broadcastToRoom(roomName, ["recordingStatus", true]);
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
      }
      return success;
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
        winners: winners || {},
        room: room,
        recording: true
      }]);
    } catch(e) {}
  }

  async _addLowCardWinner(room, username) {
    return await this.cacheManager.addWinner(room, username, this.env);
  }

  // ========== GET LAST WEEK WINNER ==========
  async _getLastWeekWinnerAndReset() {
    try {
      if (!this.env?.QUESTIONS) return null;
      const currentWeek = this._generateCurrentWeek(new Date());
      const lastResetWeek = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
      const weekChanged = lastResetWeek && this._compareWeeks(currentWeek, lastResetWeek) > 0;
      
      if (!lastResetWeek || weekChanged) {
        await this._performReset();
        return this._cachedLastWeekWinner;
      }
      
      if (this._cachedLastWeekWinner !== null) return this._cachedLastWeekWinner;
      const savedWinner = await this.diceGameSystem.getLastWeekWinner();
      return savedWinner;
    } catch(e) { return null; }
  }

  async _performReset() {
    try {
      const currentWeek = this._generateCurrentWeek(new Date());
      const lastResetWeek = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
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
        const winnerData = {
          username: winner,
          score: highestScore,
          week: lastResetWeek || currentWeek,
          timestamp: Date.now()
        };
        await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winnerData));
        this._cachedLastWeekWinner = winnerData;
      } else {
        await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
        this._cachedLastWeekWinner = null;
      }
      
      await this.diceGameSystem.resetPoints();
      this.diceGameSystem.clearCache();
      this._cachedResetWeek = currentWeek;
      await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek);
      this._broadcastToRoom(DICE_ROOM, ["diceReset", { winner, score: highestScore, week: currentWeek }]);
      return this._cachedLastWeekWinner;
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

  _generateCurrentWeek(date) {
    const now = date || new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }

  // ========== LOW CARD GAME METHODS ==========
  
  // ========== GAME START - KODE AWAL YANG BISA ==========
  async startGame(ws, bet, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username?.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]);
        return;
      }
      
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      if (room === DICE_ROOM) {
        this._safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]);
        return;
      }

      const lockKey = `game_start_${room}`;
      if (this._gameLocks.has(lockKey)) {
        this._safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]);
        return;
      }
      
      this._gameLocks.set(lockKey, Date.now());

      try {
        const isRecordingEnabled = this.cacheManager.getRecordingStatus(room);
        
        const existingGame = this.activeGames.get(room);
        if (existingGame?._isActive && !existingGame._gameEnded) {
          this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
          return;
        }
        if (existingGame) await this._forceCleanupGame(room, existingGame);
        
        const betAmount = parseInt(bet, 10) || 0;
        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          this._safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
          return;
        }
        
        if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
          this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
          return;
        }
        
        const wsId = ws._wsId;
        const game = {
          room, players: new Map(), botPlayers: new Map(), registrationOpen: true,
          round: 1, numbers: new Map(), tanda: new Map(), eliminated: new Set(),
          betAmount, hostId: usernameClean, hostName: usernameClean, useBots: false,
          evaluationLocked: false, drawTimeExpired: false,
          _isActive: true, _gameEnded: false, _phase: 'registration',
          _botTimeouts: new Set(), _botsAdded: false,
          _registrationTimer: null, _drawTimer: null, _evalTimer: null, _safetyTimer: null,
          _isEvaluating: false, _createdAt: Date.now(), _drawPhaseStart: null, _endTime: null,
          playerWsId: new Map(),
          _startedByRecording: isRecordingEnabled,
          _startedBy: 'user'
        };
        
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        game.playerWsId.set(usernameClean, wsId);
        this.activeGames.set(room, game);
        this._addClient(room, ws, usernameClean);
        this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
        this._broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
        this._startRegistration(room, game);
        
      } finally {
        setTimeout(() => {
          this._gameLocks.delete(lockKey);
        }, 3000);
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }

  // ========== JOIN GAME ==========
  async joinGame(ws, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username?.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]);
        return;
      }
      
      const usernameClean = username.trim();
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      
      const lockKey = `join_${room}_${usernameClean}`;
      if (this._joinLocks.has(lockKey)) {
        this._safeSend(ws, ["gameLowCardError", "Please wait"]);
        return;
      }
      
      this._joinLocks.set(lockKey, Date.now());
      
      try {
        const game = this.activeGames.get(room);
        if (!game?._isActive || game._gameEnded || !game.players) {
          this._safeSend(ws, ["gameLowCardError", "No active game in this room"]);
          return;
        }
        if (game.players.has(usernameClean)) {
          if (game.eliminated?.has(usernameClean)) {
            this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
            return;
          }
          if (game.numbers.has(usernameClean)) {
            this._safeSend(ws, ["gameLowCardPlayerDraw", usernameClean, game.numbers.get(usernameClean), game.tanda.get(usernameClean) || ""]);
          }
          this._sendGameStateToClient(ws, room);
          return;
        }
        if (!game.registrationOpen) {
          this._safeSend(ws, ["gameLowCardNoJoin", usernameClean, game.betAmount]);
          this._safeSend(ws, ["gameLowCardError", "Registration is closed"]);
          return;
        }
        if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
          this._safeSend(ws, ["gameLowCardError", "Game is full"]);
          return;
        }
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        this._addClient(room, ws, usernameClean);
        game.playerWsId.set(usernameClean, wsId);
        this._broadcastToRoom(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
      } finally {
        setTimeout(() => {
          this._joinLocks.delete(lockKey);
        }, 2000);
      }
    } catch(e) {}
  }

  // ========== SUBMIT NUMBER ==========
  async submitNumber(ws, number, tanda, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username?.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]);
        return;
      }
      
      const usernameClean = username.trim();
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      if (game.players.has(usernameClean) && game.eliminated?.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
        return;
      }
      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw') {
        this._safeSend(ws, ["gameLowCardError", "Cannot submit now"]);
        return;
      }
      if (!game.players.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game"]);
        return;
      }
      if (game.numbers.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You have already submitted"]);
        return;
      }
      
      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
        return;
      }
      const validTandas = ["C1", "C2", "C3", "C4", ""];
      if (!validTandas.includes(tanda)) tanda = "";
      
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
          } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS));
        game._evalTimer = evalTimer;
      }
    } catch(e) {}
  }

  // ========== LEAVE GAME ==========
  async leaveGame(ws, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username?.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]);
        return;
      }
      
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        this._safeSend(ws, ["gameLowCardError", "No active game in this room"]);
        return;
      }
      if (!game.players.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game"]);
        return;
      }
      this._removePlayerFromGame(usernameClean, room);
    } catch(e) {}
  }

  // ========== CHECK GAME RUNNING ==========
  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameStatus", "false"]);
        return;
      }
      let room = roomname || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room) {
        this._safeSend(ws, ["gameStatus", "false"]);
        return;
      }
      const game = this.activeGames.get(room);
      const isRunning = game?._isActive && !game._gameEnded && game.players?.size > 0;
      this._safeSend(ws, ["gameStatus", isRunning ? "true" : "false"]);
      if (isRunning) this._sendGameStateToClient(ws, room);
    } catch(e) {}
  }

  _isGameActuallyRunning(game) { 
    return game?._isActive === true && !game?._gameEnded; 
  }

  _getActivePlayerIds(game) {
    try {
      if (!game?._isActive || game._gameEnded || !game?.players) return [];
      return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
    } catch(e) { return []; }
  }

  _removePlayerFromGame(username, room) {
    try {
      const game = this.activeGames.get(room);
      if (!game || !game.players?.has(username) || !game._isActive || game._gameEnded || game._isEvaluating || game.evaluationLocked) return false;
      if (!game.eliminated) game.eliminated = new Set();
      game.eliminated.add(username);
      game.numbers?.delete(username);
      game.tanda?.delete(username);
      this._broadcastToRoom(room, ["gameLowCardError", `${username} has been eliminated`]);
      const checkTimer = setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && !game._gameEnded) this._checkGameCanContinue(room, game);
        } catch(e) {}
      }, 1000);
      this._trackTimer(checkTimer);
      return true;
    } catch(e) { return false; }
  }

  async _checkGameCanContinue(room, game) {
    try {
      if (!game?._isActive || game._gameEnded || !game.players || game._isEvaluating || game.evaluationLocked || game.registrationOpen) return;
      const activePlayers = game.getActivePlayers();
      if (activePlayers.length === 0) {
        const allPlayers = Array.from(game.players.keys());
        const submitted = Array.from(game.numbers?.keys() || []);
        const notSubmitted = allPlayers.filter(id => !submitted.includes(id) && !game.eliminated?.has(id));
        if (notSubmitted.length > 0) return;
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardEnd", []]);
        this._forceCleanupGame(room, game);
        return;
      }
      if (activePlayers.length === 1 && !game._gameEnded) {
        const activeIds = this._getActivePlayerIds(game);
        const submittedIds = Array.from(game.numbers?.keys() || []);
        const notSubmitted = activeIds.filter(id => !submittedIds.includes(id));
        if (notSubmitted.length > 0) {
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", `Waiting for ${notSubmitted.length} player(s)`]);
          return;
        }
        const winner = activePlayers[0]?.name || "Unknown";
        const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winner);
          await this._broadcastLowCardWinners(room);
        }
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
        this._forceCleanupGame(room, game);
      }
    } catch(e) {}
  }

  // ========== FORCE CLEANUP GAME ==========
  async _forceCleanupGame(room, game) {
    const lockKey = `cleanup_${room}`;
    if (this._cleanupLocks.has(lockKey)) {
      let waitCount = 0;
      while (this._cleanupLocks.has(lockKey) && waitCount < 10) {
        await new Promise(resolve => setTimeout(resolve, 100));
        waitCount++;
      }
      if (this._cleanupLocks.has(lockKey)) {
        this._cleanupLocks.delete(lockKey);
      }
    }
    
    if (!this._acquireLock(this._cleanupLocks, lockKey, 10000)) {
      return;
    }
    
    try {
      if (!game) {
        this._releaseLock(this._cleanupLocks, lockKey);
        return;
      }
      
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) { 
          this._clearTimer(game[key]); 
          game[key] = null; 
        }
      }
      
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) {
          this._clearTimer(id);
        }
        game._botTimeouts.clear();
        game._botTimeouts = null;
      }
      
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

  // ========== START REGISTRATION ==========
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
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          if (timeLeft === 0) {
            this._clearTimer(timer);
            game._registrationTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeRegistration(room, game);
          }
          timeLeft--;
        } catch(e) { this._clearTimer(timer); if (game._registrationTimer === timer) game._registrationTimer = null; }
      }, 1000));
      game._registrationTimer = timer;
    } catch(e) {}
  }

  // ========== CLOSE REGISTRATION ==========
  _closeRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      game.registrationOpen = false;
      if (game._registrationTimer) { this._clearTimer(game._registrationTimer); game._registrationTimer = null; }
      
      const humanPlayers = Array.from(game.players.keys()).filter(id => !id.startsWith('BOT_'));
      const humanCount = humanPlayers.length;
      
      if (!game._botsAdded) {
        if (humanCount === 1 || humanCount === 0) {
          this._addBots(room, 4);
          game._botsAdded = true;
        } else if (game.players.size < 2) {
          const needed = Math.min(4 - game.players.size, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { this._addBots(room, needed); game._botsAdded = true; }
        }
      }
      
      if (this._isGameActuallyRunning(game) && game.players.size >= 2) {
        this._startDrawPhase(room, game);
      } else {
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }

  // ========== SCHEDULE GAME CLEANUP ==========
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

  // ========== ADD BOTS ==========
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
    } catch(e) {}
  }

  // ========== START DRAW PHASE ==========
  async _startDrawPhase(room, game) {
    const lockKey = `startDraw_${room}`;
    if (this._gameOperationLocks.has(lockKey)) {
      return;
    }
    
    if (!this._acquireLock(this._gameOperationLocks, lockKey, 10000)) {
      return;
    }
    
    try {
      if (!this._isGameActuallyRunning(game)) {
        this._releaseLock(this._gameOperationLocks, lockKey);
        return;
      }
      
      if (game._drawTimer) { 
        this._clearTimer(game._drawTimer); 
        game._drawTimer = null; 
      }
      if (game._evalTimer) { 
        this._clearTimer(game._evalTimer); 
        game._evalTimer = null; 
      }
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) {
          this._clearTimer(id);
        }
        game._botTimeouts.clear();
      }
      
      const activePlayers = game.getActivePlayers();
      
      if (activePlayers.length < 2) {
        if (!game._botsAdded) {
          const needed = Math.min(4 - activePlayers.length, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { 
            this._addBots(room, needed); 
            game._botsAdded = true; 
          }
        }
        
        const newActive = game.getActivePlayers();
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
      game.drawTimeExpired = false;
      game.evaluationLocked = false;
      game._drawPhaseStart = Date.now();
      if (!game._botTimeouts) game._botTimeouts = new Set();
      
      const playersList = game.getActivePlayers().map(p => p.name);
      this._broadcastToRoom(room, ["gameLowCardClosed", playersList]);
      this._broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
      
      this._releaseLock(this._gameOperationLocks, lockKey);
      
      this._startDrawCountdown(room, game);
      
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        this._startBotDraws(room, game);
      }
      
    } catch(e) {
      this._releaseLock(this._gameOperationLocks, lockKey);
    }
  }

  // ========== START DRAW COUNTDOWN ==========
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
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          if (timeLeft === 0) {
            this._clearTimer(timer);
            game._drawTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeDrawPhase(room, game);
          }
          timeLeft--;
        } catch(e) { this._clearTimer(timer); if (game._drawTimer === timer) game._drawTimer = null; }
      }, 1000));
      game._drawTimer = timer;
    } catch(e) {}
  }

  // ========== CLOSE DRAW PHASE ==========
  async _closeDrawPhase(room, game) {
    const drawLockKey = `draw_${room}`;
    if (this._drawLocks.has(drawLockKey)) {
      return;
    }
    
    if (!this._acquireLock(this._drawLocks, drawLockKey, 10000)) {
      return;
    }
    
    try {
      if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || game.evaluationLocked) {
        this._releaseLock(this._drawLocks, drawLockKey);
        return;
      }
      
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      
      if (game._drawTimer) { 
        this._clearTimer(game._drawTimer); 
        game._drawTimer = null; 
      }
      
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) {
          this._forceBotDraw(room, botId, game);
        }
      }
      
      const activeIds = this._getActivePlayerIds(game);
      const submittedIds = new Set(game.numbers?.keys() || []);
      const notSubmitted = activeIds.filter(id => !submittedIds.has(id) && !game.eliminated?.has(id));
      
      if (notSubmitted.length > 0 && submittedIds.size === 0) {
        this._broadcastToRoom(room, ["gameLowCardError", "No one submitted numbers"]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
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
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._releaseLock(this._drawLocks, drawLockKey);
        this._forceCleanupGame(room, game);
        return;
      }
      
      if (remaining.length === 0) {
        this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._releaseLock(this._drawLocks, drawLockKey);
        this._forceCleanupGame(room, game);
        return;
      }
      
      this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      if (game._evalTimer) { 
        this._clearTimer(game._evalTimer); 
        game._evalTimer = null; 
      }
      
      this._releaseLock(this._drawLocks, drawLockKey);
      
      const evalTimer = this._trackTimer(setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            this._evaluateRound(room, game);
          }
        } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS));
      game._evalTimer = evalTimer;
      
    } catch(e) {
      this._releaseLock(this._drawLocks, drawLockKey);
    }
  }

  // ========== EVALUATE ROUND ==========
  async _evaluateRound(room, game) {
    const evalLockKey = `eval_${room}`;
    if (this._evaluationLocks.has(evalLockKey)) {
      return;
    }
    
    if (!this._acquireLock(this._evaluationLocks, evalLockKey, 15000)) {
      return;
    }
    
    try {
      if (this.isDestroyed || !game?._isActive || game._gameEnded || game._isEvaluating || !game.players) {
        this._releaseLock(this._evaluationLocks, evalLockKey);
        return;
      }
      
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game) {
        this._releaseLock(this._evaluationLocks, evalLockKey);
        return;
      }
      
      if (game._isEvaluating) {
        this._releaseLock(this._evaluationLocks, evalLockKey);
        return;
      }
      
      game._isEvaluating = true;
      
      const safetyTimer = this._trackTimer(setTimeout(() => {
        if (game?._isEvaluating) { 
          game._isEvaluating = false; 
          this._scheduleGameCleanup(room, game); 
        }
        this._releaseLock(this._evaluationLocks, evalLockKey);
      }, CONSTANTS.EVALUATION_TIMEOUT_MS));
      game._safetyTimer = safetyTimer;
      
      if (game._evalTimer) { 
        this._clearTimer(game._evalTimer); 
        game._evalTimer = null; 
      }
      
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) {
          this._clearTimer(id);
        }
        game._botTimeouts.clear();
      }
      
      const numbers = game.numbers || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const tanda = game.tanda || new Map();
      const entries = Array.from(numbers.entries());
      const submittedIds = new Set(numbers.keys());
      const activeIds = this._getActivePlayerIds(game);
      
      for (const id of activeIds) {
        if (!submittedIds.has(id)) {
          eliminated.add(id);
        }
      }
      
      if (entries.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
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
          if (game._safetyTimer) { 
            this._clearTimer(game._safetyTimer); 
            game._safetyTimer = null; 
          }
          this._forceCleanupGame(room, game);
          return;
        }
        
        this._broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        this._forceCleanupGame(room, game);
        return;
      }
      
      const values = entries.map(([, n]) => n);
      const allSame = values.every(v => v === values[0]);
      let losers = [];
      
      if (!allSame && values.length > 0) {
        const lowest = Math.min(...values);
        losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
        for (const id of losers) {
          eliminated.add(id);
        }
      }
      
      const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
      
      if (allSame && remaining.length >= 2) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        this._releaseLock(this._evaluationLocks, evalLockKey);
        
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
        this._broadcastToRoom(room, ["gameLowCardRoundResult", game.round - 1,
          entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`),
          [], remainingNames, true
        ]);
        
        if (this._isGameActuallyRunning(game) && !game._gameEnded) {
          this._startDrawPhase(room, game);
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
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        this._releaseLock(this._evaluationLocks, evalLockKey);
        this._forceCleanupGame(room, game);
        return;
      }
      
      if (remaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
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
      game.numbers = new Map();
      game.tanda = new Map();
      game._botTimeouts = new Set();
      game._isEvaluating = false;
      
      if (game._safetyTimer) { 
        this._clearTimer(game._safetyTimer); 
        game._safetyTimer = null; 
      }
      this._releaseLock(this._evaluationLocks, evalLockKey);
      
      if (this._isGameActuallyRunning(game) && !game._gameEnded) {
        this._startDrawPhase(room, game);
      }
      
    } catch(e) {
      if (game) {
        game._isEvaluating = false;
      }
      this._releaseLock(this._evaluationLocks, `eval_${room}`);
    }
  }

  // ========== START BOT DRAWS ==========
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
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
      const activeIds = this._getActivePlayerIds(game);
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameActuallyRunning(game)) {
        game.evaluationLocked = true;
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        const evalTimer = this._trackTimer(setTimeout(() => {
          try { this._evaluateRound(room, game); } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS));
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
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
    } catch(e) {}
  }

  _getBotNumberByRound(round) {
    if (round <= 2) return Math.floor(Math.random() * 12) + 1;
    return Math.random() < 0.6 ?
      [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
      [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
  }

  _getRandomCardTanda() { 
    return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; 
  }

  _getRandomDrawDelay() { 
    return (Math.floor(Math.random() * 14) + 2) * 1000; 
  }

  // ========== START GAME WITH RECORDING ==========
  async _startGameWithRecording(ws, room, bet, username) {
    try {
      if (!room || !username) {
        this._safeSend(ws, ["gameLowCardError", "Room and username required"]);
        return;
      }

      const isRecordingEnabled = this.cacheManager.getRecordingStatus(room);
      if (!isRecordingEnabled) {
        this._safeSend(ws, ["gameLowCardError", "Recording is not enabled in this room"]);
        return;
      }

      const existingGame = this.activeGames.get(room);
      if (existingGame?._isActive && !existingGame._gameEnded) {
        this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
        return;
      }
      if (existingGame) await this._forceCleanupGame(room, existingGame);

      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
        this._safeSend(ws, ["gameLowCardError", "Invalid bet (0 or 100-100000)"]);
        return;
      }
      if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
        this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
        return;
      }

      if (isRecordingEnabled) {
        this.cacheManager.getWinners(room);
      }

      const wsId = ws._wsId;
      const game = {
        room, players: new Map(), botPlayers: new Map(), registrationOpen: true,
        round: 1, numbers: new Map(), tanda: new Map(), eliminated: new Set(),
        betAmount, hostId: username, hostName: username, useBots: false,
        evaluationLocked: false, drawTimeExpired: false,
        _isActive: true, _gameEnded: false, _phase: 'registration',
        _botTimeouts: new Set(), _botsAdded: false,
        _registrationTimer: null, _drawTimer: null, _evalTimer: null, _safetyTimer: null,
        _isEvaluating: false, _createdAt: Date.now(), _drawPhaseStart: null, _endTime: null,
        playerWsId: new Map(),
        _startedByRecording: true, _startedBy: 'recording'
      };

      game.players.set(username, { id: username, name: username });
      game.playerWsId.set(username, wsId);
      this.activeGames.set(room, game);
      this._addClient(room, ws, username);
      this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
      this._broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount]);
      this._startRegistration(room, game);
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }

  // ========== ADD CLIENT ==========
  _addClient(room, ws, username = null) {
    try {
      if (!ws) return;
      
      if (username && this._bannedUsers.has(username)) {
        const banUntil = this._bannedUsers.get(username);
        if (Date.now() < banUntil) {
          this._safeSend(ws, ["gameLowCardError", "You are temporarily banned"]);
          return;
        }
        this._bannedUsers.delete(username);
      }
      
      const wsId = this._getWsId(ws);
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
    } catch(e) {}
  }

  // ========== HANDLE ERROR ==========
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

  // ========== DESTROY ==========
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
      for (const timer of this._allTimers) {
        try { clearTimeout(timer); clearInterval(timer); } catch(e) {}
      }
      this._allTimers.clear();
      
      this._clearTimer(this._diceTimeout);
      this._clearTimer(this._diceStartTimeout);
      this._clearTimer(this._diceTimeUpCooldownTimer);
      if (this._diceTimerInterval) {
        clearInterval(this._diceTimerInterval);
        this._diceTimerInterval = null;
      }
      
      this._cachedResetWeek = null;
      this._cachedLastWeekWinner = null;
      this._kvCache.clear();
      
      if (this.cacheManager) this.cacheManager.clear();
      if (this.diceGameSystem) this.diceGameSystem.clearCache();
      
      this._eventQueue = [];
      this._isProcessingQueue = false;
      
      for (const [room, game] of this.activeGames) {
        await this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();
      
      for (const [room, timer] of this._cleanupTimers) {
        this._clearTimer(timer);
      }
      this._cleanupTimers.clear();
      
      this.userConnections.clear();
      this._tieBreakers.clear();
      this._reconnectAttempts.clear();
      this._bannedUsers.clear();
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._switchLocks.clear();
      this._switchRetries.clear();
      this._kvCache.clear();
      
      if (this.diceGameSystem) this.diceGameSystem.clearCache();
      
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
