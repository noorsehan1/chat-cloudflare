// ==================== GAME-SERVER-HYBRID.JS ====================
// Hybrid approach: Event-driven + Alarm-based scheduling
// Uses Durable Object Alarm API for efficient resource usage

const CONSTANTS = {
  MAX_LOWCARD_GAMES: 10,
  REGISTRATION_TIME_MS: 20000,
  DRAW_TIME_MS: 20000,
  EVALUATION_DELAY_MS: 2000,
  MAX_BOTS_PER_GAME: 4,
  MAX_BET: 100000,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 15,
  MAX_BOT_DRAWS_PER_ROUND: 4,
  EVALUATION_TIMEOUT_MS: 30000,
  START_LOCK_DURATION_MS: 3000,
  MAX_PLAYERS_PER_GAME: 45,
  GAME_CLEANUP_DELAY_MS: 5000,
  BATCH_SIZE: 2,
  CLEANUP_TIK: 90,
  STALE_GAME_TIMEOUT_MS: 600000,
  STUCK_DRAW_TIMEOUT_MS: 60000,
  STUCK_REGISTRATION_TIMEOUT_MS: 30000,
  MAX_RETRY_INIT_QUIZ: 2,
  MAX_SHUTDOWN_WAIT_MS: 5000,
  MAX_WS_CLIENTS: 50,
  MAX_ARRAY_SIZE: 50,
  QUIZ_SWITCH_DELAY_MS: 5000,
  SCHEDULER_INTERVAL_MS: 60000,
  QUIZ_BATCH_SIZE: 100,
  MAX_QUESTIONS: 10000,
  CF_SUBREQUEST_LIMIT: 50,
  DEEPLX_TIMEOUT_MS: 8000,
  DEEPLX_MAX_RETRIES: 5,
  TRANSLATE_TIMEOUT_MS: 10000,
  QUIZ_KEEP_ALIVE_INTERVAL_MS: 5000,
  QUIZ_NEXT_QUESTION_DELAY_MS: 5000,
  CPU_TIME_LIMIT_MS: 10,
  CPU_YIELD_DELAY_MS: 1,
  CPU_CHECK_INTERVAL_MS: 100,
  MAX_EVENTS_PER_TICK: 5,
  BROADCAST_BATCH_SIZE: 5,
  MAX_RESTART_ATTEMPTS: 3,
  RESTART_COOLDOWN_MS: 30000,
  HEALTH_CHECK_INTERVAL_MS: 10000,
  MAX_IDLE_TIME_MS: 300000,
  RECONNECT_DELAY_MS: 2000,
  MAX_EVENT_QUEUE_SIZE: 1000,
  ERROR_RECOVERY_DELAY_MS: 5000,
  MAX_UNHANDLED_ERRORS: 5,
  ERROR_RESET_INTERVAL_MS: 60000,
  LOWCARD_WINNER_KEY: 'lowcard_winner_',
  LOWCARD_RECORDING_KEY: 'lowcard_recording_status_',
  SCHEDULER_LOOP_INTERVAL_MS: 50,
  
  MAX_DICE_GAMES: 10,
  DICE_ROLL_TIME_MS: 0,
  DICE_READING_TIME_MS: 0,
  DICE_ANSWER_TIME_MS: 20000,
  DICE_TOTAL_TIME_MS: 20000,
  DICE_BREAK_MS: 15000,
  DICE_AFTER_TIMEOUT_BREAK_MS: 15000,
  MAX_DICE_VALUE: 6,
  DICE_ROOM: "Quiz",
  DICE_POINT_KEY: 'dice_points',
  DICE_LAST_WEEK_WINNER: 'dice_last_week_winner',
  DICE_WINNER_KEY: 'dice_winner_',
  DICE_RECORDING_KEY: 'dice_recording_status_',
  QUIZ_START_DELAY_MS: 5000,
  
  DICE_AUTO_START_DELAY_MS: 3000,
  DICE_MIN_PLAYERS_TO_AUTO_START: 1,
  DICE_CHECK_INTERVAL_MS: 5000,
  
  DICE_LAST_RESET_WEEK: 'dice_last_reset_week',
  WEEKLY_RESET_CHECK_INTERVAL_MS: 300000,
  
  TIE_BREAKER_TIME_LIMIT: 20,
  TIE_BREAKER_COOLDOWN: 15000,
  
  // ==================== ALARM CONSTANTS ====================
  ALARM_INTERVAL_MS: 15000,        // Main alarm interval (15 seconds)
  DICE_TICK_INTERVAL_MS: 1000,     // Dice timer tick (per second)
  CLEANUP_INTERVAL_MS: 30000,      // Cleanup every 30 seconds
  HEALTH_CHECK_INTERVAL_MS: 30000, // Health check every 30 seconds
  DICE_ACTIVITY_TIMEOUT_MS: 5000,  // Inactivity timeout
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

// ==================== KV CACHE CLASS ====================
class KVCache {
  constructor() {
    this.cache = new Map();
    this.ttl = 30000;
  }

  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(key);
      return null;
    }
    return entry.value;
  }

  set(key, value, customTtl = null) {
    const ttl = customTtl || this.ttl;
    this.cache.set(key, {
      value: value,
      timestamp: Date.now(),
      ttl: ttl
    });
  }

  delete(key) {
    this.cache.delete(key);
  }

  clear() {
    this.cache.clear();
  }
}

// ==================== CPU PROTECTION CLASS ====================
class CPUProtection {
  constructor() {
    this._cpuStartTime = 0;
    this._cpuTotalTime = 0;
    this._cpuCheckCount = 0;
    this._isThrottled = false;
    this._pendingOperations = [];
    this._isProcessingPending = false;
    this._cpuHistory = [];
    this._cpuAverage = 0;
    this._eventQueue = [];
    this._isProcessingQueue = false;
    this._rateLimitMap = new Map();
  }

  _startCPUTimer() {
    this._cpuStartTime = performance.now ? performance.now() : Date.now();
    return this._cpuStartTime;
  }

  _checkCPULimit() {
    try {
      const now = performance.now ? performance.now() : Date.now();
      const elapsed = now - this._cpuStartTime;
      if (elapsed >= CONSTANTS.CPU_TIME_LIMIT_MS) {
        this._cpuTotalTime += elapsed;
        this._cpuCheckCount++;
        this._cpuHistory.push(elapsed);
        if (this._cpuHistory.length > 10) this._cpuHistory.shift();
        const sum = this._cpuHistory.reduce((a, b) => a + b, 0);
        this._cpuAverage = sum / this._cpuHistory.length;
        return true;
      }
      return false;
    } catch(e) { return false; }
  }

  async _cpuYield() {
    try {
      if (this._isThrottled) {
        await this._sleep(CONSTANTS.CPU_YIELD_DELAY_MS * 2);
        return;
      }
      if (this._cpuAverage > CONSTANTS.CPU_TIME_LIMIT_MS * 0.8) {
        await this._sleep(CONSTANTS.CPU_YIELD_DELAY_MS * 3);
        this._isThrottled = true;
        setTimeout(() => { this._isThrottled = false; }, 100);
      } else {
        await this._sleep(CONSTANTS.CPU_YIELD_DELAY_MS);
      }
    } catch(e) {}
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async _safeExecute(fn, ...args) {
    this._startCPUTimer();
    try {
      const result = await fn(...args);
      if (this._checkCPULimit()) await this._cpuYield();
      return result;
    } catch(e) {
      if (this._checkCPULimit()) await this._cpuYield();
      throw e;
    }
  }

  _isRateLimited(wsId, eventType) {
    try {
      const now = Date.now();
      const key = `${wsId}_${eventType}`;
      const data = this._rateLimitMap.get(key);
      if (!data) {
        this._rateLimitMap.set(key, { count: 1, resetTime: now + 1000 });
        return false;
      }
      if (now > data.resetTime) {
        data.count = 1;
        data.resetTime = now + 1000;
        return false;
      }
      data.count++;
      return data.count > 10;
    } catch(e) { return false; }
  }

  _cleanupRateLimitMap() {
    try {
      const now = Date.now();
      for (const [key, data] of this._rateLimitMap) {
        if (now - data.resetTime > 1000) this._rateLimitMap.delete(key);
      }
    } catch(e) {}
  }
}

// ==================== DICE GAME SYSTEM ====================
class DiceGameSystem {
  constructor(gameServer) {
    this.gameServer = gameServer;
    this.env = gameServer.env;
    this.userScores = new Map();
    this._isLoaded = false;
    this._loading = false;
    this._usedDiceValues = new Set();
    this._lastLoadTime = 0;
    this._cacheTTL = 5000;
  }

  async loadScores() {
    try {
      const now = Date.now();
      if (this._isLoaded && (now - this._lastLoadTime) < this._cacheTTL) {
        return true;
      }
      
      if (this._loading) return this._isLoaded;
      this._loading = true;
      
      const env = this.env;
      if (!env?.QUESTIONS) {
        this._loading = false;
        return false;
      }
      
      const points = await env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      
      this._isLoaded = true;
      this._loading = false;
      this._lastLoadTime = Date.now();
      return true;
    } catch(e) {
      this._loading = false;
      return false;
    }
  }

  async getPoints() {
    try {
      if (!this.env?.QUESTIONS) return {};
      const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      return points;
    } catch(e) {
      return {};
    }
  }

  async setPoints(points) {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      return true;
    } catch(e) {
      return false;
    }
  }

  async getLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return null;
      
      if (this.gameServer._cachedLastWeekWinner !== null) {
        return this.gameServer._cachedLastWeekWinner;
      }
      
      const winnerData = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_WEEK_WINNER, 'json');
      this.gameServer._cachedLastWeekWinner = winnerData;
      this.gameServer._cachedLastWeekWinnerTimestamp = Date.now();
      return winnerData;
    } catch(e) {
      return null;
    }
  }

  async setLastWeekWinner(winner) {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winner));
      this.gameServer._cachedLastWeekWinner = winner;
      this.gameServer._cachedLastWeekWinnerTimestamp = Date.now();
      return true;
    } catch(e) {
      return false;
    }
  }

  async deleteLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
      this.gameServer._cachedLastWeekWinner = null;
      this.gameServer._cachedLastWeekWinnerTimestamp = 0;
      return true;
    } catch(e) {
      return false;
    }
  }

  generateCurrentWeek() {
    const now = new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(year, 0, 1);
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }

  rollDice() {
    return Math.floor(Math.random() * 6) + 1;
  }

  clearCache() {
    this.userScores.clear();
    this._usedDiceValues.clear();
  }
}

// ==================== GAME SERVER CLASS - HYBRID VERSION ====================
export class GameServer extends CPUProtection {
  constructor(state, env) {
    try {
      super();
      this.state = state;
      this.env = env;
      this.closing = false;
      this.isDestroyed = false;
      this._initialized = false;
      this._initializing = false;

      this._restartCount = 0;
      this._lastRestartTime = 0;
      this._isRestarting = false;
      this._startTime = Date.now();
      this._lastHeartbeat = Date.now();
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      this._isRecovering = false;
      this._recoveryAttempts = 0;
      this._maxRecoveryAttempts = 3;
      this._lastRecoveryTime = 0;

      this._winnerProcessed = false;

      this.activeGames = new Map();
      this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();

      this._wsIdCounter = 0;
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.wsMap = new Map();
      this.roomViewers = new Map();
      this.userConnections = new Map();
      this._cleanupTimers = new Map();
      this._roomBroadcastCount = new Map();
      this._roomBroadcastReset = new Map();
      this._tikCounter = 0;
      this._gameStartFlags = new Map();

      this.diceAnswered = new Set();
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.diceTimer = null;
      this.currentDiceRoll = null;
      this._diceStartTime = null;
      this._diceTimeout = null;
      this._diceBreakTimeout = null;
      this._diceStartTimeout = null;
      this.diceAutoEnabled = false;
      this._diceKeepAliveInterval = null;
      this._lastActivityTime = Date.now();
      this._isDiceIdle = false;
      this._isShowingDice = false;
      this._diceInitAttempts = 0;
      this._maxDiceInitAttempts = 3;

      this.diceEndedToday = false;
      this.diceEndMessageShown = false;
      this.diceEndNotified = false;

      this._diceTimeLeftNotified = new Map();
      this._nextDiceNotified = new Map();
      this._diceJoinedNotified = new Map();
      this._diceTimeLeftBroadcastCooldown = 1000;
      this._lastDiceTimeLeftBroadcast = 0;

      this._diceQuestionStartTime = null;
      this._canSubmitDiceAnswer = false;

      this._recordingEnabled = new Map();

      this._weeklyResetTimer = null;
      this._lastResetWeek = null;
      this._lastResetCheck = null;

      this._diceTimerInterval = null;
      this._diceNotified20 = false;
      this._diceNotified10 = false;
      this._diceNotified5 = false;
      this._diceNotified3 = false;

      this._diceOutOfTimeShown = false;
      this._diceRemainingShown = false;
      this._diceTimeUpShown = false;

      this._diceRound = 0;

      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      
      this._diceTimeUpCooldown = false;
      this._diceTimeUpCooldownTimer = null;
      
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };

      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tieActive = false;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._playerAnswers = new Map();
      this._processingTieResults = false;
      this._lastTieLog = '';

      this._cachedResetWeek = null;
      this._cachedResetWeekTimestamp = 0;
      this._cachedLastWeekWinner = null;
      this._cachedLastWeekWinnerTimestamp = 0;

      this._kvCache = new KVCache();

      this.diceGameSystem = new DiceGameSystem(this);

      // ==================== ALARM-BASED SCHEDULING ====================
      // NO setInterval! Menggunakan Alarm API untuk menghemat resource
      this._alarmScheduled = false;
      this._diceTickCounter = 0;
      this._lastDiceTickTime = 0;
      
      // State persistence key
      this._stateKey = 'game_server_state';

      // ==================== LOAD STATE DARI DURABLE ====================
      this._loadState().then(() => {
        // Schedule first alarm if not already scheduled
        if (!this._alarmScheduled) {
          this._scheduleAlarm(CONSTANTS.ALARM_INTERVAL_MS);
          this._alarmScheduled = true;
        }
      });

      // Init async tasks
      this._initAsync();

      setTimeout(async () => {
        try {
          if (!this.closing && !this.isDestroyed) {
            await this._initResetWeek();
          }
        } catch(e) {}
      }, 1000);

      setTimeout(async () => {
        try {
          if (!this.closing && !this.isDestroyed) {
            await this.diceGameSystem.loadScores();
          }
        } catch(e) {}
      }, 5000);

      setTimeout(() => {
        if (!this.closing && !this.isDestroyed && !this._isShowingDice) {
          this.forceStartDice();
        }
      }, 8000);

    } catch(e) {}
  }

  // ==================== STATE PERSISTENCE ====================
  
  async _saveState() {
    try {
      if (this.closing || this.isDestroyed) return;
      
      // Only save critical state, not WebSocket connections
      const state = {
        _diceRound: this._diceRound || 0,
        _diceStartTime: this._diceStartTime,
        _diceQuestionStartTime: this._diceQuestionStartTime,
        currentDiceRoll: this.currentDiceRoll,
        _isShowingDice: this._isShowingDice,
        _canSubmitDiceAnswer: this._canSubmitDiceAnswer,
        _diceTimeUpCooldown: this._diceTimeUpCooldown,
        diceAutoEnabled: this.diceAutoEnabled,
        _diceNotifiedFlags: this._diceNotifiedFlags,
        _lastSentRemaining: this._lastSentRemaining,
        _diceRound: this._diceRound,
        // Tie breaker state
        _tieActive: this._tieActive,
        _tieRound: this._tieRound,
        _tiePlayers: this._tiePlayers,
        // Game state summaries
        activeGamesCount: this.activeGames.size,
        wsConnectionsCount: this.wsMap.size,
        _lastHeartbeat: Date.now()
      };
      
      await this.state.storage.put(this._stateKey, state);
    } catch(e) {
      // Silent fail - state persistence is best effort
    }
  }

  async _loadState() {
    try {
      const state = await this.state.storage.get(this._stateKey);
      if (state) {
        this._diceRound = state._diceRound || 0;
        this._diceStartTime = state._diceStartTime || null;
        this._diceQuestionStartTime = state._diceQuestionStartTime || null;
        this.currentDiceRoll = state.currentDiceRoll || null;
        this._isShowingDice = state._isShowingDice || false;
        this._canSubmitDiceAnswer = state._canSubmitDiceAnswer || false;
        this._diceTimeUpCooldown = state._diceTimeUpCooldown || false;
        this.diceAutoEnabled = state.diceAutoEnabled || false;
        if (state._diceNotifiedFlags) {
          this._diceNotifiedFlags = state._diceNotifiedFlags;
        }
        this._lastSentRemaining = state._lastSentRemaining || -1;
        this._tieActive = state._tieActive || false;
        this._tieRound = state._tieRound || 0;
        this._tiePlayers = state._tiePlayers || [];
        
        // Restore dice timer if dice is active
        if (this.currentDiceRoll && this._diceQuestionStartTime) {
          this._restoreDiceTimer();
        }
      }
    } catch(e) {
      // Silent fail
    }
  }

  // ==================== ALARM HANDLING ====================
  
  async alarm() {
    try {
      if (this.closing || this.isDestroyed) {
        this._alarmScheduled = false;
        return;
      }

      // Reset alarm flag before processing
      this._alarmScheduled = false;

      // ====== RUN ALL PERIODIC TASKS ======
      
      // 1. CPU Monitor cleanup
      this._cleanupRateLimitMap();
      
      // 2. Dice timer tick (if dice is active)
      this._processDiceTick();
      
      // 3. Health check
      this._performHealthCheck();
      
      // 4. Check stuck games
      this._checkStuckGames();
      
      // 5. Cleanup stale games
      this._cleanupStaleGames();
      
      // 6. Cleanup dead connections
      this._cleanupDeadConnections();
      
      // 7. Dice auto task
      await this._diceAutoTask();
      
      // 8. Dice keep alive
      this._diceKeepAliveTask();
      
      // 9. Check weekly reset
      await this._checkAndResetWeeklyDice();
      
      // 10. Save state
      await this._saveState();

      // ====== SCHEDULE NEXT ALARM ======
      if (!this.closing && !this.isDestroyed) {
        this._scheduleAlarm(CONSTANTS.ALARM_INTERVAL_MS);
        this._alarmScheduled = true;
      }

    } catch(e) {
      // Schedule next alarm even on error
      if (!this.closing && !this.isDestroyed) {
        this._scheduleAlarm(CONSTANTS.ALARM_INTERVAL_MS);
        this._alarmScheduled = true;
      }
    }
  }

  _scheduleAlarm(delayMs) {
    try {
      const currentAlarm = this.state.storage.getAlarm();
      // Don't schedule if already scheduled
      if (currentAlarm) return;
      this.state.storage.setAlarm(Date.now() + delayMs);
    } catch(e) {
      // Silent fail
    }
  }

  // ==================== DICE TIMER - TICK BASED ====================
  
  _restoreDiceTimer() {
    // Restart dice timer notifications from saved state
    this._diceNotifiedFlags = {
      20: false,
      10: false,
      5: false,
      timeup: false
    };
    // The alarm tick will handle remaining time
  }

  _processDiceTick() {
    try {
      if (this._tieActive) return;
      
      // Only process if dice is active
      if (!this.currentDiceRoll || !this._diceQuestionStartTime) {
        return;
      }

      if (this._isDiceTime()) {
        const elapsed = (Date.now() - this._diceQuestionStartTime) / 1000;
        const remaining = Math.max(0, CONSTANTS.DICE_ANSWER_TIME_MS / 1000 - elapsed);
        const remainingInt = Math.floor(remaining);
        
        let shouldSend = false;
        let message = "";
        
        // Check notification thresholds
        if (remainingInt === 20 && !this._diceNotifiedFlags[20]) {
          this._diceNotifiedFlags[20] = true;
          shouldSend = true;
          message = "20s remaining";
        } else if (remainingInt === 10 && !this._diceNotifiedFlags[10]) {
          this._diceNotifiedFlags[10] = true;
          shouldSend = true;
          message = "10s remaining";
        } else if (remainingInt === 5 && !this._diceNotifiedFlags[5]) {
          this._diceNotifiedFlags[5] = true;
          shouldSend = true;
          message = "5s remaining";
        } else if (remainingInt <= 0 && !this._diceNotifiedFlags.timeup) {
          this._diceNotifiedFlags.timeup = true;
          shouldSend = true;
          message = "TIME UP";
          this._startTimeUpCooldown();
        }
        
        if (shouldSend) {
          this._broadcastDiceNotification("diceError", {
            remaining: remainingInt,
            message: message,
            round: this._diceRound || 1,
            isDiceTime: true,
            isActive: true
          });
        }
      }
    } catch(e) {}
  }

  _startTimeUpCooldown() {
    if (this._diceTimeUpCooldown) return;
    
    this._diceTimeUpCooldown = true;
    
    this._broadcastDiceNotification("diceError", {
      message: "wait 15s",
      remaining: 15,
      isDiceTime: true,
      isActive: false,
      cooldown: true
    });
    
    // Reset cooldown after 15 seconds (will be handled by alarm)
    // But we also set a fallback timeout
    if (this._diceTimeUpCooldownTimer) {
      clearTimeout(this._diceTimeUpCooldownTimer);
    }
    this._diceTimeUpCooldownTimer = setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      
      // Also handle dice timeout/cleanup here
      this._handleDiceTimeout();
    }, 15000);
  }

  async _handleDiceTimeout() {
    try {
      if (this.closing || this.isDestroyed) { 
        this._isShowingDice = false;
        this._canSubmitDiceAnswer = false;
        return; 
      }
      
      if (this._tieActive) return;
      
      const currentClients = this.wsClients.get(DICE_ROOM);
      if (!currentClients?.size) { 
        this.currentDiceRoll = null;
        this._isShowingDice = false;
        this._canSubmitDiceAnswer = false;
        return; 
      }
      
      const diceValue = this.currentDiceRoll?.value;
      const roundNumber = this._diceRound || 1;
      
      if (this.diceHasWinner && this.diceWinner) {
        const correctPlayers = [];
        for (const player of this.diceAnswered) {
          const answer = this._playerAnswers.get(player);
          if (answer === this.currentDiceRoll?.value) {
            correctPlayers.push(player);
          }
        }
        
        if (correctPlayers.length > 1 && !this._tieActive) {
          this.currentDiceRoll = null;
          this._isShowingDice = false;
          this._canSubmitDiceAnswer = false;
          await this._startTieBreaker(DICE_ROOM, correctPlayers);
          return;
        }
        
        const points = await this._getDicePoints();
        this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
          username: this.diceWinner,
          totalPoints: points[this.diceWinner] || 0,
          diceValue: diceValue,
          round: roundNumber
        }]);
        
        this._broadcastDiceNotification("diceError", {
          username: this.diceWinner,
          totalPoints: points[this.diceWinner] || 0,
          diceValue: diceValue,
          round: roundNumber,
          remaining: -1,
          message: `${this.diceWinner} won with value ${diceValue}`
        });
      } else {
        this._broadcastToRoom(DICE_ROOM, ["diceNoWinner", {
          message: `No winner`,
          value: diceValue,
          round: roundNumber
        }]);
      }
      
      this.currentDiceRoll = null;
      this._isShowingDice = false;
      this._canSubmitDiceAnswer = false;
      
    } catch(e) {}
  }

  // ==================== ORIGINAL METHODS (with modifications) ====================
  
  // [Keep all original methods from the provided code]
  // Only remove setInterval-based scheduling and replace with alarm-based
  
  // ==================== CACHE METHODS ====================

  async _updateCachedResetWeek(week) {
    this._cachedResetWeek = week;
    this._cachedResetWeekTimestamp = Date.now();
  }

  async _getCachedResetWeek() {
    try {
      if (this._cachedResetWeek !== null) {
        return this._cachedResetWeek;
      }
      
      if (this.env?.QUESTIONS) {
        const lastResetWeek = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
        if (lastResetWeek) {
          this._cachedResetWeek = lastResetWeek;
          this._cachedResetWeekTimestamp = Date.now();
          return lastResetWeek;
        }
      }
      
      return null;
    } catch(e) {
      return null;
    }
  }

  async _getRecordingStatusFromKV(roomName) {
    try {
      if (!roomName) return false;
      
      if (this._recordingEnabled.has(roomName)) {
        return this._recordingEnabled.get(roomName);
      }
      
      if (this.env?.QUESTIONS) {
        const kvValue = await this.env.QUESTIONS.get(
          CONSTANTS.LOWCARD_RECORDING_KEY + roomName
        );
        const isRecording = kvValue === 'true';
        this._recordingEnabled.set(roomName, isRecording);
        return isRecording;
      }
      
      return false;
    } catch(e) {
      return false;
    }
  }

  async _initResetWeek() {
    try {
      if (!this.env?.QUESTIONS) return;

      const existingResetWeek = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
      const currentWeek = this._generateCurrentWeek(new Date());

      if (!existingResetWeek) {
        await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek);
        await this._updateCachedResetWeek(currentWeek);
        return;
      }

      await this._updateCachedResetWeek(existingResetWeek);

    } catch(e) {}
  }

  _isWeekGreater(weekA, weekB) {
    try {
      const numA = parseInt(weekA.split('-W')[1]);
      const numB = parseInt(weekB.split('-W')[1]);
      return numA > numB;
    } catch(e) {
      return false;
    }
  }

  async _checkAndResetWeeklyDice() {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      const now = new Date();
      const currentWeek = this._generateCurrentWeek(now);
      
      let lastResetWeek = await this._getCachedResetWeek();
      
      if (!lastResetWeek) {
        await this._updateCachedResetWeek(currentWeek);
        await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek);
        return false;
      }
      
      const weekChanged = lastResetWeek !== currentWeek;
      
      if (!weekChanged) {
        return false;
      }
      
      const dayOfWeek = now.getUTCDay();
      const hours = now.getUTCHours();
      const minutes = now.getUTCMinutes();
      
      const isMonday = dayOfWeek === 1;
      const isResetTime = hours === 0 && minutes === 0;
      
      if (weekChanged && isMonday && isResetTime) {
        const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
        
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
          const winnerData = {
            username: winner,
            score: highestScore,
            week: lastResetWeek,
            timestamp: Date.now()
          };
          
          await this.env.QUESTIONS.put(
            CONSTANTS.DICE_LAST_WEEK_WINNER,
            JSON.stringify(winnerData)
          );
          
          this._cachedLastWeekWinner = winnerData;
          this._cachedLastWeekWinnerTimestamp = Date.now();
          
        } else {
          await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
          this._cachedLastWeekWinner = null;
          this._cachedLastWeekWinnerTimestamp = 0;
        }
        
        await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify({}));
        
        await this._updateCachedResetWeek(currentWeek);
        await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek);
        
        this.diceGameSystem.userScores.clear();
        
        return true;
      }
      
      return false;
      
    } catch(e) {
      return false;
    }
  }

  _generateCurrentWeek(date) {
    try {
      const now = date || new Date();
      const year = now.getUTCFullYear();
      const startOfYear = new Date(Date.UTC(year, 0, 1));
      const diff = now - startOfYear;
      const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
      return `${year}-W${String(week).padStart(2, '0')}`;
    } catch(e) {
      return '2026-W01';
    }
  }

  _healthCheckTask() {
    // Removed - handled by alarm
  }

  _diceKeepAliveTask() {
    try {
      this._lastHeartbeat = Date.now();
      
      if (!this._isDiceTime()) {
        if (!this._diceOutOfTimeShown) {
          const timeLeft = this._getTimeLeftUntilNextDice();
          this._broadcastDiceNotification("diceError", {
            message: `Next dice game in: ${timeLeft.text}`,
            timeLeft: timeLeft.text,
            hours: timeLeft.hours,
            minutes: timeLeft.minutes,
            remaining: -1,
            isDiceTime: false,
            isActive: false
          });
          this._diceOutOfTimeShown = true;
        }
        return;
      }
      
      if (this._diceOutOfTimeShown) {
        this._diceOutOfTimeShown = false;
        this._diceRemainingShown = false;
        this._diceTimeUpShown = false;
      }
      
    } catch(e) {}
  }

  async _diceAutoTask() {
    try {
      if (this._tieActive) {
        return;
      }
      
      await this._checkDiceAutoStatus();
      
      // Don't use setInterval - alarm handles periodic checks
      const isDiceTime = this._isDiceTime();
      const clients = this.wsClients.get(DICE_ROOM);
      const hasPlayers = clients && clients.size > 0;
      
      if (isDiceTime) {
        this._diceOutOfTimeShown = false;
        this._diceRemainingShown = false;
        this._diceTimeUpShown = false;
        this._diceJoinedNotified.clear();
        
        this.diceEndedToday = false;
        this.diceEndMessageShown = false;
        this.diceEndNotified = false;
        this._nextDiceNotified.clear();
        
        if (hasPlayers && !this.currentDiceRoll && !this._diceTimeout && 
            !this._diceStartTimeout && !this._isShowingDice && !this._diceTimeUpCooldown) {
          
          if (!this.diceAutoEnabled) {
            this.diceAutoEnabled = true;
            await this.startDiceWithDelay(CONSTANTS.DICE_AUTO_START_DELAY_MS || 3000);
          }
          
          // Use setTimeout for immediate start, not interval
          setTimeout(() => {
            if (!this.closing && !this.isDestroyed && 
                !this.currentDiceRoll && !this._diceTimeout && 
                !this._isShowingDice && !this._diceTimeUpCooldown) {
              this.forceStartDice();
            }
          }, CONSTANTS.DICE_AUTO_START_DELAY_MS || 3000);
        }
        
      } else {
        if (this.diceAutoEnabled && !this.diceEndNotified) {
          this.diceAutoEnabled = false;
          this.diceEndedToday = true;
          this.diceEndMessageShown = false;
          await this.resetDice();
          this._clearDiceData();
          this._diceTimeLeftNotified.clear();
          this._nextDiceNotified.clear();
          this._diceJoinedNotified.clear();
          this._sendDiceEndNotificationOnce();
        }
      }
      
    } catch(e) {}
  }

  _isDiceTime() {
    try {
      const witaTime = this._getCurrentWITATime();
      const currentTotal = witaTime.totalMinutes;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = session.start * 60;
        const endTotal = session.end * 60;
        if (currentTotal >= startTotal && currentTotal < endTotal) {
          return true;
        }
      }
      return false;
    } catch(e) { 
      return false; 
    }
  }

  _getCurrentWITATime() {
    try {
      const now = new Date();
      const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
      const minutes = now.getUTCMinutes();
      return {
        hours,
        minutes,
        totalMinutes: (hours * 60) + minutes,
        formatted: `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}`
      };
    } catch(e) {
      return { hours: 0, minutes: 0, totalMinutes: 0, formatted: '00:00' };
    }
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
        if (diff < minDiff) {
          minDiff = diff;
        }
      }
      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      const isRunning = this._isDiceTime();
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

  _getTimeLeftUntilNextDiceEvent() {
    try {
      const witaTime = this._getCurrentWITATime();
      const currentTotal = witaTime.totalMinutes;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = session.start * 60;
        const endTotal = session.end * 60;
        if (currentTotal >= startTotal && currentTotal < endTotal) {
          const remaining = endTotal - currentTotal;
          const hours = Math.floor(remaining / 60);
          const minutes = Math.floor(remaining % 60);
          return {
            minutes: remaining,
            seconds: 0,
            isRunning: true,
            hours: hours,
            totalMinutes: remaining,
            status: 'running',
            remainingText: `${hours}h ${minutes}m`
          };
        }
      }
      let minDiff = Infinity;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        let startTotal = session.start * 60;
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) {
          minDiff = diff;
        }
      }
      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      return {
        hours: hours,
        minutes: minutes,
        seconds: 0,
        totalMinutes: minDiff,
        totalSeconds: minDiff * 60,
        isRunning: false,
        status: 'waiting',
        remainingText: `${hours}h ${minutes}m`
      };
    } catch(e) {
      return { hours: 0, minutes: 0, isRunning: false, status: 'unknown' };
    }
  }

  async _startRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      
      const currentStatus = await this._getRecordingStatusFromKV(roomName);
      if (currentStatus) {
        this._broadcastToRoom(roomName, ["recordingStatus", true]);
        return true;
      }
      
      this._recordingEnabled.set(roomName, true);
      
      if (this.env?.QUESTIONS) {
        await this.env.QUESTIONS.put(
          CONSTANTS.LOWCARD_RECORDING_KEY + roomName, 
          'true'
        );
      }
      
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
      
      const currentStatus = await this._getRecordingStatusFromKV(room);
      if (!currentStatus) {
        this._broadcastToRoom(room, ["recordingStatus", false]);
        return true;
      }
      
      this._recordingEnabled.set(room, false);
      
      if (this.env?.QUESTIONS) {
        const statusKey = CONSTANTS.LOWCARD_RECORDING_KEY + room;
        const winnerKey = CONSTANTS.LOWCARD_WINNER_KEY + room;
        
        await this.env.QUESTIONS.delete(statusKey);
        await this.env.QUESTIONS.delete(winnerKey);
        
        const prefixes = [
          CONSTANTS.LOWCARD_WINNER_KEY,
          CONSTANTS.LOWCARD_RECORDING_KEY
        ];
        
        for (const prefix of prefixes) {
          try {
            const list = await this.env.QUESTIONS.list({ prefix: prefix });
            for (const key of list.keys) {
              if (key.name === prefix + room || key.name.includes(room)) {
                await this.env.QUESTIONS.delete(key.name);
              }
            }
          } catch(e) {}
        }
      }
      
      this._broadcastToRoom(room, ["recordingStatus", false]);
      
      return true;
    } catch(e) {
      return false;
    }
  }

  async _sendWinnersToRoom(room) {
    try {
      if (!room) return;
      
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      const winners = await this._getLowCardWinners(room);
      
      this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
        winners: winners,
        room: room,
        recording: isRecordingEnabled
      }]);
      
    } catch(e) {}
  }

  async _getLowCardWinners(room) {
    try {
      if (!room) return {};
      if (!this.env?.QUESTIONS) return {};
      
      const isRecording = await this._getRecordingStatusFromKV(room);
      if (!isRecording) {
        return {};
      }
      
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      const winners = await this.env.QUESTIONS.get(key, 'json');
      
      if (winners && typeof winners === 'object' && Object.keys(winners).length > 0) {
        return winners;
      }
      
      return {};
    } catch(e) {
      return {};
    }
  }

  async _broadcastLowCardWinners(room) {
    try {
      if (!room) return;
      
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled) {
        return;
      }
      
      const winners = await this._getLowCardWinners(room);
      
      if (Object.keys(winners).length === 0) {
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
          winners: {},
          room: room,
          recording: true,
          message: "No winner"
        }]);
      } else {
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
          winners: winners,
          room: room,
          recording: true
        }]);
      }
      
    } catch(e) {}
  }

  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username) return false;
      
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled) {
        return false;
      }
      
      if (room === DICE_ROOM) {
        return false;
      }
      
      if (!this.env?.QUESTIONS) return false;
      
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      
      let roomWinners = await this.env.QUESTIONS.get(key, 'json') || {};
      
      let currentCount = 0;
      if (roomWinners[username]) {
        const valStr = String(roomWinners[username]);
        currentCount = parseInt(valStr.replace("x", "").replace("X", "")) || 0;
      }
      const newCount = currentCount + 1;
      
      roomWinners[username] = newCount + "x";
      
      await this.env.QUESTIONS.put(key, JSON.stringify(roomWinners));
      
      return true;
    } catch(e) {
      return false;
    }
  }

  async _handleDiceWinner(username, diceValue) {
    try {
      if (this._winnerProcessed) return;
      
      if (!this.currentDiceRoll || !this._canSubmitDiceAnswer) {
        return;
      }
      
      this._winnerProcessed = true;
      
      const points = await this.diceGameSystem.getPoints();
      points[username] = (points[username] || 0) + 1;
      await this.diceGameSystem.setPoints(points);
      
      this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
        username: username,
        totalPoints: points[username] || 0,
        diceValue: diceValue,
        round: this._diceRound || 1
      }]);
      
      this._broadcastDiceNotification("diceError", {
        username: username,
        totalPoints: points[username] || 0,
        diceValue: diceValue,
        round: this._diceRound || 1,
        remaining: -1,
        message: `${username} won with value ${diceValue}`
      });
      
      setTimeout(() => {
        this._winnerProcessed = false;
      }, 1000);
      
    } catch(e) {}
  }

  async _getDicePoints() {
    try {
      if (!this.env?.QUESTIONS) return {};
      const points = await this.diceGameSystem.getPoints();
      return points;
    } catch(e) {
      return {};
    }
  }

  _sendDiceEndNotificationOnce() {
    try {
      if (this.diceEndNotified) return;
      const timeLeft = this._getTimeLeftUntilNextDice();
      this._broadcastToRoom(DICE_ROOM, ["diceEnded", { 
        timeLeft: timeLeft.text, 
        status: "ended"
      }]);
      this._broadcastDiceNotification("diceError", { 
        timeLeft: timeLeft.text,
        remaining: -1,
        message: `Dice game ended. Next session in: ${timeLeft.text}`
      });
      this.diceEndNotified = true;
    } catch(e) {}
  }

  _sendDiceNotification(ws, type, data) {
    try {
      if (!ws || ws.readyState !== 1) return;
      const message = data.message || "";
      this._safeSend(ws, ["diceNotification", message]);
    } catch(e) {}
  }

  _broadcastDiceNotification(type, data) {
    try {
      if (this._tieActive && !data?.isTieBreaker) {
        return;
      }
      
      const wsIds = this.wsClients.get(DICE_ROOM);
      if (!wsIds?.size) return;
      
      const now = Date.now();
      const message = data.message || "";
      const remaining = data.remaining !== undefined ? data.remaining : -1;
      
      let key = `dice_${remaining}`;
      if (remaining === -1) {
        key = `dice_msg_${message.substring(0, 30)}`;
      }
      
      if (message === "TIME UP") {
        key = "dice_timeup";
      }
      
      if (data.cooldown) {
        key = `cooldown_${remaining}`;
      }
      
      if (message !== "TIME UP") {
        if (this._lastNotificationKey === key && (now - this._lastNotificationTime) < 3000) {
          return;
        }
        
        if (remaining > 0 && this._lastSentRemaining === remaining && !data.cooldown) {
          return;
        }
      }
      
      this._lastNotificationKey = key;
      this._lastNotificationTime = now;
      if (remaining > 0) {
        this._lastSentRemaining = remaining;
      }
      
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", message]);
      
    } catch(e) {}
  }

  async _initAsync() {
    try {
      if (this._initializing) return;
      if (this._initialized && !this._isRecovering) return;
      this._initializing = true;
      
      await this.diceGameSystem.loadScores();
      await this._initDice();
      
      // Use setTimeout instead of interval for initial start
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          if (this._isDiceTime()) {
            const clients = this.wsClients.get(DICE_ROOM);
            if (clients && clients.size > 0) {
              this.diceAutoEnabled = true;
              this._broadcastDiceNotification("diceError", {
                message: "Dice game is starting now",
                isDiceTime: true,
                remaining: -1,
                timestamp: Date.now()
              });
              
              setTimeout(() => {
                if (!this.closing && !this.isDestroyed) {
                  this.forceStartDice();
                }
              }, CONSTANTS.DICE_AUTO_START_DELAY_MS || 3000);
            }
          }
        }
      }, 2000);
      
      this._initialized = true;
      this._initializing = false;
      this._errorCount = 0;
      this._isRecovering = false;
      this._diceInitAttempts = 0;
    } catch(e) {
      this._initializing = false;
      this._handleError('initAsync', e);
      if (this._diceInitAttempts < this._maxDiceInitAttempts && !this.closing && !this.isDestroyed) {
        this._diceInitAttempts++;
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed) {
            this._initAsync();
          }
        }, 5000 * this._diceInitAttempts);
      }
    }
  }

  async _checkDiceAutoStatus() {
    try {
      const isDiceTime = this._isDiceTime();
      if (isDiceTime) {
        this._diceOutOfTimeShown = false;
        this._diceRemainingShown = false;
        this._diceTimeUpShown = false;
        this._diceJoinedNotified.clear();
        
        this.diceEndedToday = false;
        this.diceEndMessageShown = false;
        this.diceEndNotified = false;
        this._nextDiceNotified.clear();
        
        if (!this.diceAutoEnabled) {
          this.diceAutoEnabled = true;
          const wsIds = this.wsClients.get(DICE_ROOM);
          if (wsIds?.size > 0) {
            let hasUnnotified = false;
            for (const wsId of wsIds) {
              if (!this._diceTimeLeftNotified.has(wsId) && !this._nextDiceNotified.has(wsId)) {
                hasUnnotified = true;
                break;
              }
            }
            if (hasUnnotified) {
              this._broadcastDiceTimeLeft();
            }
          }
          await this.startDiceWithDelay(CONSTANTS.QUIZ_START_DELAY_MS);
          if (!this._diceStartTimeout && !this._isShowingDice && !this._diceTimeUpCooldown) {
            this.forceStartDice();
          }
        } else if (!this.currentDiceRoll && !this._diceTimeout && !this._diceStartTimeout && !this._isShowingDice && !this._diceTimeUpCooldown) {
          const clients = this.wsClients.get(DICE_ROOM);
          if (clients?.size > 0) {
            await this._showDiceQuestion();
          }
        }
        return false;
      } else {
        if (this.diceAutoEnabled && !this.diceEndNotified) {
          this.diceAutoEnabled = false;
          this.diceEndedToday = true;
          this.diceEndMessageShown = false;
          await this.resetDice();
          this._clearDiceData();
          this._diceTimeLeftNotified.clear();
          this._nextDiceNotified.clear();
          this._diceJoinedNotified.clear();
          this._sendDiceEndNotificationOnce();
        }
        return true;
      }
    } catch(e) { return true; }
  }

  forceStartDice() {
    try {
      if (this._tieActive) {
        return false;
      }
      
      if (this._isShowingDice) return false;
      if (this._diceTimeUpCooldown) return false;
      if (!this._isDiceTime() || this.currentDiceRoll || this._diceTimeout || this._diceStartTimeout) {
        return false;
      }
      this.diceAutoEnabled = true;
      this._showDiceQuestion();
      return true;
    } catch(e) { return false; }
  }

  ensureDiceRunning() {
    try {
      if (this._isShowingDice) return;
      if (this._diceTimeUpCooldown) return;
      this._forceStartDiceIfTime();
      if (!this.currentDiceRoll && !this._diceTimeout && !this._diceStartTimeout && !this._isShowingDice) {
        this.forceStartDice();
      }
    } catch(e) {}
  }

  _forceStartDiceIfTime() {
    try {
      if (this._isShowingDice) return;
      if (this._diceTimeUpCooldown) return;
      if (!this._isDiceTime() || this.currentDiceRoll || this._diceTimeout || this._diceStartTimeout) {
        return;
      }
      this.diceAutoEnabled = true;
      this._showDiceQuestion();
    } catch(e) {}
  }

  _startDiceTimerNotifications() {
    try {
      this._diceNotified20 = false;
      this._diceNotified10 = false;
      this._diceNotified5 = false;
      this._diceNotified3 = false;
      this._diceTimeUpShown = false;
      this._diceRemainingShown = false;
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
      // Timer handled by alarm ticks
    } catch(e) {}
  }

  _stopDiceTimerNotifications() {
    try {
      this._diceNotified20 = false;
      this._diceNotified10 = false;
      this._diceNotified5 = false;
      this._diceNotified3 = false;
      this._diceTimeUpShown = false;
      this._diceRemainingShown = false;
      this._lastSentRemaining = -1;
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
    } catch(e) {}
  }

  async _showDiceQuestionSilent() {
    try {
      if (this._tieActive) {
        return;
      }
      
      if (this._isShowingDice) return;
      if (this._diceTimeUpCooldown) return;
      this._lastActivityTime = Date.now();
      this._isDiceIdle = false;
      
      this._diceRemainingShown = false;
      this._diceTimeUpShown = false;
      this._diceOutOfTimeShown = false;
      
      if (!this._isDiceTime()) {
        const clients = this.wsClients.get(DICE_ROOM);
        if (clients?.size > 0) {
          if (!this.diceEndNotified) {
            this._sendDiceEndNotificationOnce();
          }
        }
        return;
      }
      
      if (!this.diceAutoEnabled) {
        this.diceAutoEnabled = true;
        const clients = this.wsClients.get(DICE_ROOM);
        if (clients?.size > 0) {
          this._broadcastDiceNotification("diceError", {
            message: "Dice game is starting soon",
            isDiceTime: true,
            isActive: false,
            remaining: -1
          });
        }
        return;
      }
      
      if (this.isDestroyed || this._diceStartTimeout || this.currentDiceRoll) return;
      
      this._isShowingDice = true;
      
      try {
        this._diceRound = (this._diceRound || 0) + 1;
        
        const diceValue = this.diceGameSystem.rollDice();
        
        this.currentDiceRoll = {
          value: diceValue,
          timestamp: Date.now(),
          round: this._diceRound
        };
        this._diceStartTime = Date.now();
        this._diceQuestionStartTime = Date.now();
        
        this._canSubmitDiceAnswer = true;
        
        this.diceAnswered = new Set();
        this.diceHasWinner = false;
        this.diceWinner = null;
        this._winnerProcessed = false;
        this._diceRemainingShown = false;
        this._diceTimeUpShown = false;
        
        this._diceNotified20 = false;
        this._diceNotified10 = false;
        this._diceNotified5 = false;
        this._diceNotified3 = false;
        this._lastSentRemaining = -1;
        this._diceNotifiedFlags = {
          20: false,
          10: false,
          5: false,
          timeup: false
        };
        
        await this._broadcastDiceRoll(diceValue);
        
        this._broadcastDiceNotification("diceError", {
          answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000,
          remaining: 20,
          message: "♡ clik draw ♡",
          round: this._diceRound
        });
        
        this._startDiceTimerNotifications();
        
        if (this._diceTimeout) clearTimeout(this._diceTimeout);
        if (this._diceBreakTimeout) clearTimeout(this._diceBreakTimeout);
        
        this._diceTimeout = setTimeout(async () => {
          try {
            if (this.closing || this.isDestroyed) { 
              this._diceTimeout = null; 
              this._isShowingDice = false;
              this._canSubmitDiceAnswer = false;
              this._stopDiceTimerNotifications();
              return; 
            }
            
            if (this._tieActive) {
              this._diceTimeout = null;
              return;
            }
            
            const currentClients = this.wsClients.get(DICE_ROOM);
            if (!currentClients?.size) { 
              this._diceTimeout = null; 
              this.currentDiceRoll = null;
              this._isShowingDice = false;
              this._canSubmitDiceAnswer = false;
              this._stopDiceTimerNotifications();
              return; 
            }
            
            const diceValue = this.currentDiceRoll?.value;
            const roundNumber = this._diceRound || 1;
            
            this._stopDiceTimerNotifications();
            
            if (this.diceHasWinner && this.diceWinner) {
              const correctPlayers = [];
              for (const player of this.diceAnswered) {
                const answer = this._playerAnswers.get(player);
                if (answer === this.currentDiceRoll?.value) {
                  correctPlayers.push(player);
                }
              }
              
              if (correctPlayers.length > 1 && !this._tieActive) {
                this._diceTimeout = null;
                this.currentDiceRoll = null;
                this._isShowingDice = false;
                this._canSubmitDiceAnswer = false;
                this._stopDiceTimerNotifications();
                
                await this._startTieBreaker(DICE_ROOM, correctPlayers);
                return;
              }
              
              const points = await this._getDicePoints();
              this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
                username: this.diceWinner,
                totalPoints: points[this.diceWinner] || 0,
                diceValue: diceValue,
                round: roundNumber
              }]);
              
              this._broadcastDiceNotification("diceError", {
                username: this.diceWinner,
                totalPoints: points[this.diceWinner] || 0,
                diceValue: diceValue,
                round: roundNumber,
                remaining: -1,
                message: `${this.diceWinner} won with value ${diceValue}`
              });
            } else {
              this._broadcastToRoom(DICE_ROOM, ["diceNoWinner", {
                message: `No winner`,
                value: diceValue,
                round: roundNumber
              }]);
            }
            
            this._diceTimeout = null;
            this.currentDiceRoll = null;
            this._isShowingDice = false;
            this._canSubmitDiceAnswer = false;
            
            this._startTimeUpCooldown();
            
          } catch(e) {}
        }, CONSTANTS.DICE_TOTAL_TIME_MS);
        
      } catch(e) {
        this._isShowingDice = false;
        this.currentDiceRoll = null;
        this._canSubmitDiceAnswer = false;
        this._stopDiceTimerNotifications();
      }
    } catch(e) {}
  }

  async _showDiceQuestion() {
    try {
      if (this._tieActive) {
        return;
      }
      
      if (this._isShowingDice) return;
      if (this._diceTimeUpCooldown) return;
      this._lastActivityTime = Date.now();
      this._isDiceIdle = false;
      
      this._diceRemainingShown = false;
      this._diceTimeUpShown = false;
      this._diceOutOfTimeShown = false;
      
      if (!this._isDiceTime()) {
        const clients = this.wsClients.get(DICE_ROOM);
        if (clients?.size > 0) {
          if (!this.diceEndNotified) {
            this._sendDiceEndNotificationOnce();
          }
        }
        return;
      }
      
      if (!this.diceAutoEnabled) {
        this.diceAutoEnabled = true;
        const clients = this.wsClients.get(DICE_ROOM);
        if (clients?.size > 0) {
          this._broadcastDiceNotification("diceError", {
            message: "Dice game is starting soon",
            isDiceTime: true,
            isActive: false,
            remaining: -1
          });
        }
        return;
      }
      
      if (this.isDestroyed || this._diceStartTimeout || this.currentDiceRoll) return;
      
      this._isShowingDice = true;
      
      try {
        this._diceRound = (this._diceRound || 0) + 1;
        
        const diceValue = this.diceGameSystem.rollDice();
        
        this.currentDiceRoll = {
          value: diceValue,
          timestamp: Date.now(),
          round: this._diceRound
        };
        this._diceStartTime = Date.now();
        this._diceQuestionStartTime = Date.now();
        
        this._canSubmitDiceAnswer = true;
        
        this.diceAnswered = new Set();
        this.diceHasWinner = false;
        this.diceWinner = null;
        this._winnerProcessed = false;
        this._diceRemainingShown = false;
        this._diceTimeUpShown = false;
        
        this._diceNotified20 = false;
        this._diceNotified10 = false;
        this._diceNotified5 = false;
        this._diceNotified3 = false;
        this._lastSentRemaining = -1;
        this._diceNotifiedFlags = {
          20: false,
          10: false,
          5: false,
          timeup: false
        };
        
        await this._broadcastDiceRoll(diceValue);
        
        this._broadcastDiceNotification("diceError", {
          answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000,
          remainingTime: "20s remaining",
          remaining: 20,
          message: "♡ clik draw ♡",
          round: this._diceRound
        });
        
        this._startDiceTimerNotifications();
        
        if (this._diceTimeout) clearTimeout(this._diceTimeout);
        if (this._diceBreakTimeout) clearTimeout(this._diceBreakTimeout);
        
        this._diceTimeout = setTimeout(async () => {
          try {
            if (this.closing || this.isDestroyed) { 
              this._diceTimeout = null; 
              this._isShowingDice = false;
              this._canSubmitDiceAnswer = false;
              this._stopDiceTimerNotifications();
              return; 
            }
            
            if (this._tieActive) {
              this._diceTimeout = null;
              return;
            }
            
            const currentClients = this.wsClients.get(DICE_ROOM);
            if (!currentClients?.size) { 
              this._diceTimeout = null; 
              this.currentDiceRoll = null;
              this._isShowingDice = false;
              this._canSubmitDiceAnswer = false;
              this._stopDiceTimerNotifications();
              return; 
            }
            
            const diceValue = this.currentDiceRoll?.value;
            const roundNumber = this._diceRound || 1;
            
            this._stopDiceTimerNotifications();
            
            if (this.diceHasWinner && this.diceWinner) {
              const correctPlayers = [];
              for (const player of this.diceAnswered) {
                const answer = this._playerAnswers.get(player);
                if (answer === this.currentDiceRoll?.value) {
                  correctPlayers.push(player);
                }
              }
              
              if (correctPlayers.length > 1 && !this._tieActive) {
                this._diceTimeout = null;
                this.currentDiceRoll = null;
                this._isShowingDice = false;
                this._canSubmitDiceAnswer = false;
                this._stopDiceTimerNotifications();
                
                await this._startTieBreaker(DICE_ROOM, correctPlayers);
                return;
              }
              
              const points = await this._getDicePoints();
              this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
                username: this.diceWinner,
                totalPoints: points[this.diceWinner] || 0,
                diceValue: diceValue,
                round: roundNumber
              }]);
              
              this._broadcastDiceNotification("diceError", {
                username: this.diceWinner,
                totalPoints: points[this.diceWinner] || 0,
                diceValue: diceValue,
                round: roundNumber,
                remaining: -1,
                message: `${this.diceWinner} won with value ${diceValue}`
              });
            } else {
              this._broadcastToRoom(DICE_ROOM, ["diceNoWinner", {
                message: `No winner`,
                value: diceValue,
                round: roundNumber
              }]);
            }
            
            this._diceTimeout = null;
            this.currentDiceRoll = null;
            this._isShowingDice = false;
            this._canSubmitDiceAnswer = false;
            
            this._startTimeUpCooldown();
            
          } catch(e) {}
        }, CONSTANTS.DICE_TOTAL_TIME_MS);
        
      } catch(e) {
        this._isShowingDice = false;
        this.currentDiceRoll = null;
        this._canSubmitDiceAnswer = false;
        this._stopDiceTimerNotifications();
      }
    } catch(e) {}
  }

  // ==================== SUBMIT DICE ANSWER WITH TIE BREAKER ====================
  async submitDiceAnswer(ws, username, guess) {
    try {
      if (!ws || !username) return;
      
      const room = this._ensureRoomConsistency(ws);
      if (room !== DICE_ROOM) return;
      if (!this._isDiceTime()) return;
      
      const guessValue = parseInt(guess, 10);
      if (isNaN(guessValue) || guessValue < 1 || guessValue > 6) {
        this._safeSend(ws, ["diceError", "invalid guess 1-6"]);
        return;
      }
      
      // ============ TIE BREAKER MODE ============
      if (this._tieActive) {
        if (!this._tiePlayers.includes(username)) {
          return;
        }
        
        if (this._tieAnswers.has(username)) {
          return;
        }
        
        if (!this._canSubmitDiceAnswer) {
          return;
        }
        
        this._tieAnswers.set(username, guessValue);
        this.diceAnswered.add(username);
        
        this._broadcastToRoom(DICE_ROOM, ["diceAnswer", {
          username: username,
          guess: guessValue,
          round: this._diceRound || 1,
          isTieBreaker: true,
          tieRound: this._tieRound
        }]);
        
        if (this._tieAnswers.size === this._tiePlayers.length) {
          if (this._tieTimer) {
            clearTimeout(this._tieTimer);
            this._tieTimer = null;
          }
          if (this._tieInterval) {
            clearInterval(this._tieInterval);
            this._tieInterval = null;
          }
          
          this._canSubmitDiceAnswer = false;
          this._isShowingDice = false;
          
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
      
      // ============ DICE NORMAL MODE ============
      if (this.diceAnswered.has(username)) return;
      
      const diceValue = this.currentDiceRoll?.value;
      const remaining = this._getDiceAnswerRemainingTime();
      if (remaining <= 0) {
        this.diceAnswered.add(username);
        return;
      }
      
      const isCorrect = guessValue === diceValue;
      this._playerAnswers.set(username, guessValue);
      this.diceAnswered.add(username);
      
      this._broadcastToRoom(DICE_ROOM, ["diceAnswer", {
        username: username,
        guess: guessValue,
        round: this._diceRound || 1
      }]);
      
      if (isCorrect && !this.diceHasWinner) {
        this.diceHasWinner = true;
        this.diceWinner = username;
        
        const points = await this._getDicePoints();
        points[username] = (points[username] || 0) + 1;
        await this.diceGameSystem.setPoints(points);
      }
      
    } catch(e) {}
  }

  // ==================== TIE BREAKER METHODS ====================

  async _startTieBreaker(room, players) {
    if (!players || players.length < 2) return;
    if (this._tieActive) return;
    
    this._tieActive = true;
    this._tieRound = 0;
    this._tiePlayers = [...players];
    this._tieAnswers = new Map();
    
    const id = `tie_${Date.now()}`;
    this._tieBreakers.set(id, {
      players: players,
      round: 0,
      winner: null,
      status: 'waiting'
    });
    
    await this._runTieRound(room, id, players);
  }

  async _runTieRound(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    
    if (this._tieTimer) {
      clearTimeout(this._tieTimer);
      this._tieTimer = null;
    }
    if (this._tieInterval) {
      clearInterval(this._tieInterval);
      this._tieInterval = null;
    }
    
    this._tieRound++;
    this._tiePlayers = [...players];
    this._tieAnswers = new Map();
    data.round = this._tieRound;
    data.status = 'running';
    data.players = players;
    
    const playerNames = players.join(', ');
    
    this._broadcastToRoom(DICE_ROOM, ["diceNotification", 
      `♡ Round ${this._tieRound}: ${playerNames}`
    ]);
    
    this._canSubmitDiceAnswer = true;
    this._diceQuestionStartTime = Date.now();
    this.diceAnswered = new Set();
    this._isShowingDice = true;
    
    this._startTieTimer(room, id, players);
  }

  _startTieTimer(room, id, players) {
    if (this._tieTimer) {
      clearTimeout(this._tieTimer);
      this._tieTimer = null;
    }
    if (this._tieInterval) {
      clearInterval(this._tieInterval);
      this._tieInterval = null;
    }
    
    let timeLeft = CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20;
    let notified10 = false;
    let notified5 = false;
    let isProcessed = false;
    
    this._tieInterval = setInterval(() => {
      timeLeft--;
      
      if (timeLeft === 10 && !notified10) {
        notified10 = true;
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", `10s remaining`]);
      }
      
      if (timeLeft === 5 && !notified5) {
        notified5 = true;
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", `5s remaining`]);
      }
      
      if (timeLeft === 3) {
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", `3s remaining`]);
      }
      
      if (timeLeft <= 0 && !isProcessed) {
        isProcessed = true;
        clearInterval(this._tieInterval);
        this._tieInterval = null;
        
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", `TIME UP`]);
        
        const tieId = this._getActiveTieBreakerId();
        if (tieId) {
          this._processTieResults(room, tieId, players);
        } else {
          this._resetTieBreakerState(null);
          this._startCooldownAfterTieBreaker();
        }
      }
    }, 1000);
    
    this._tieTimer = setTimeout(() => {
      if (!isProcessed) {
        isProcessed = true;
        if (this._tieInterval) {
          clearInterval(this._tieInterval);
          this._tieInterval = null;
        }
        
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", `TIME UP`]);
        
        const tieId = this._getActiveTieBreakerId();
        if (tieId) {
          this._processTieResults(room, tieId, players);
        } else {
          this._resetTieBreakerState(null);
          this._startCooldownAfterTieBreaker();
        }
      }
    }, (CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20) * 1000 + 2000);
  }

  async _processTieResults(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    
    const results = [];
    let highest = 0;
    let highestPlayers = [];
    let answeredPlayers = [];
    
    for (const player of players) {
      const answer = this._tieAnswers.get(player);
      if (answer !== undefined && answer >= 1 && answer <= 6) {
        results.push({ player, answer });
        answeredPlayers.push(player);
        if (answer > highest) {
          highest = answer;
          highestPlayers = [player];
        } else if (answer === highest) {
          highestPlayers.push(player);
        }
      }
    }
    
    if (answeredPlayers.length === 0) {
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", 
        `No one answered`
      ]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    
    // ============ CASE 1: HANYA 1 PEMENANG ============
    if (highestPlayers.length === 1) {
      const winner = highestPlayers[0];
      
      const points = await this._getDicePoints();
      points[winner] = (points[winner] || 0) + 1;
      await this.diceGameSystem.setPoints(points);
      
      this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
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
    
    // ============ CASE 2: MASIH TIE -> LANGSUNG ROUND BERIKUTNYA ============
    if (highestPlayers.length > 1) {
      this._tiePlayers = highestPlayers;
      this._tieAnswers = new Map();
      data.players = highestPlayers;
      data.round = this._tieRound;
      data.status = 'waiting';
      data.tieValue = highest;
      
      setTimeout(() => {
        if (this._tieActive && this._tiePlayers.length > 1) {
          this._runTieRound(room, id, this._tiePlayers);
        } else if (this._tiePlayers.length === 1) {
          const winner = this._tiePlayers[0];
          this._processSingleWinner(room, id, winner);
        }
      }, 2000);
      
      return;
    }
    
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  async _processSingleWinner(room, id, winner) {
    const points = await this._getDicePoints();
    points[winner] = (points[winner] || 0) + 1;
    await this.diceGameSystem.setPoints(points);
    
    this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
      username: winner,
      totalPoints: points[winner] || 0,
      diceValue: 'auto',
      round: this._diceRound || 1,
      isTieBreaker: true,
      tieBreakerRound: this._tieRound,
      finalWinner: true
    }]);
    
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  _startCooldownAfterTieBreaker() {
    this._broadcastDiceNotification("diceError", {
      message: "wait 15s",
      remaining: 15,
      isDiceTime: true,
      isActive: false,
      cooldown: true
    });
    
    this._diceTimeUpCooldown = true;
    
    if (this._diceTimeUpCooldownTimer) {
      clearTimeout(this._diceTimeUpCooldownTimer);
      this._diceTimeUpCooldownTimer = null;
    }
    
    this._diceTimeUpCooldownTimer = setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      
      this._showDiceQuestionSilent();
    }, CONSTANTS.TIE_BREAKER_COOLDOWN || 15000);
  }

  _resetTieBreakerState(id) {
    if (id) {
      this._tieBreakers.delete(id);
    }
    this._tieActive = false;
    this._tiePlayers = [];
    this._tieAnswers = new Map();
    this._tieRound = 0;
    this._canSubmitDiceAnswer = false;
    this._isShowingDice = false;
    this.currentDiceRoll = null;
    this.diceAnswered = new Set();
    this._processingTieResults = false;
    
    if (this._tieTimer) {
      clearTimeout(this._tieTimer);
      this._tieTimer = null;
    }
    if (this._tieInterval) {
      clearInterval(this._tieInterval);
      this._tieInterval = null;
    }
  }

  _getActiveTieBreakerId() {
    for (const [id, data] of this._tieBreakers) {
      if (data.status === 'waiting' || data.status === 'running') {
        return id;
      }
    }
    return null;
  }

  async resetDice() {
    try {
      if (this._diceTimeout) clearTimeout(this._diceTimeout);
      if (this._diceBreakTimeout) clearTimeout(this._diceBreakTimeout);
      if (this._diceStartTimeout) clearTimeout(this._diceStartTimeout);
      if (this._diceTimeUpCooldownTimer) {
        clearTimeout(this._diceTimeUpCooldownTimer);
        this._diceTimeUpCooldownTimer = null;
      }
      
      this._stopDiceTimerNotifications();
      
      this.currentDiceRoll = null;
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.diceAnswered = new Set();
      this._diceStartTime = null;
      this.diceEndNotified = false;
      this._isShowingDice = false;
      this._winnerProcessed = false;
      this._canSubmitDiceAnswer = false;
      this._diceQuestionStartTime = null;
      this._diceOutOfTimeShown = false;
      this._diceRemainingShown = false;
      this._diceTimeUpShown = false;
      this._lastSentRemaining = -1;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
      
      this._diceTimeLeftNotified.clear();
      this._nextDiceNotified.clear();
      this._diceJoinedNotified.clear();
      
      this._playerAnswers = new Map();
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieActive = false;
      this._tieBreakers.clear();
      this._tieRound = 0;
      this._processingTieResults = false;
      if (this._tieTimer) {
        clearTimeout(this._tieTimer);
        this._tieTimer = null;
      }
      if (this._tieInterval) {
        clearInterval(this._tieInterval);
        this._tieInterval = null;
      }
    } catch(e) {}
  }

  // ==================== CONTINUE WITH REMAINING METHODS ====================
  
  async startDiceWithDelay(delayMs) {
    try {
      if (this._diceStartTimeout) return;
      this._diceStartTimeout = setTimeout(() => {
        try {
          if (this.closing || this.isDestroyed) { 
            this._diceStartTimeout = null; 
            return; 
          }
          this._diceStartTimeout = null;
          if (!this.currentDiceRoll && this.diceAutoEnabled && !this._isShowingDice && !this._diceTimeUpCooldown) {
            this.forceStartDice();
          }
        } catch(e) {}
      }, delayMs);
    } catch(e) {}
  }

  async _initDice(retryCount = 0) {
    try {
      await this.diceGameSystem.loadScores();
      return true;
    } catch(e) {
      if (retryCount < CONSTANTS.MAX_RETRY_INIT_QUIZ && !this.closing && !this.isDestroyed) {
        setTimeout(() => this._initDice(retryCount + 1), 5000);
      }
      return false;
    }
  }

  _clearDiceData() {
    try {
      if (this._diceTimeUpCooldownTimer) {
        clearTimeout(this._diceTimeUpCooldownTimer);
        this._diceTimeUpCooldownTimer = null;
      }
      
      this.currentDiceRoll = null;
      this._diceStartTime = null;
      this.diceAnswered = new Set();
      this.diceHasWinner = false;
      this.diceWinner = null;
      this._isShowingDice = false;
      this._winnerProcessed = false;
      this._canSubmitDiceAnswer = false;
      this._diceQuestionStartTime = null;
      this._diceOutOfTimeShown = false;
      this._diceRemainingShown = false;
      this._diceTimeUpShown = false;
      this._lastSentRemaining = -1;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
      
      this._stopDiceTimerNotifications();
      
      if (this._diceTimeout) {
        clearTimeout(this._diceTimeout);
        this._diceTimeout = null;
      }
      if (this._diceBreakTimeout) {
        clearTimeout(this._diceBreakTimeout);
        this._diceBreakTimeout = null;
      }
      if (this._diceStartTimeout) {
        clearTimeout(this._diceStartTimeout);
        this._diceStartTimeout = null;
      }
      
      this._playerAnswers = new Map();
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieActive = false;
      this._tieBreakers.clear();
      this._tieRound = 0;
      this._processingTieResults = false;
      if (this._tieTimer) {
        clearTimeout(this._tieTimer);
        this._tieTimer = null;
      }
      if (this._tieInterval) {
        clearInterval(this._tieInterval);
        this._tieInterval = null;
      }
      
      this._broadcastDiceNotification("diceError", {
        message: "Dice game has ended",
        remaining: -1,
        clearUI: true
      });
    } catch(e) {}
  }

  async _broadcastDiceResult(type, data) {
    try {
      const wsIds = this.wsClients.get(DICE_ROOM);
      if (!wsIds || wsIds.size === 0) return;
      
      const msgStr = JSON.stringify([type, data]);
      const wsIdArray = Array.from(wsIds);
      const BATCH_SIZE = CONSTANTS.BROADCAST_BATCH_SIZE || 5;
      let startTime = Date.now();
      
      for (let i = 0; i < wsIdArray.length; i += BATCH_SIZE) {
        const batch = wsIdArray.slice(i, i + BATCH_SIZE);
        
        for (const wsId of batch) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1) {
            try { 
              ws.send(msgStr); 
            } catch(e) {}
          }
        }
        
        if (Date.now() - startTime > 8) {
          await this._cpuYield();
          startTime = Date.now();
        }
      }
      
    } catch(e) {}
  }

  _broadcastDiceTimeLeft() {
    try {
      const wsIds = this.wsClients.get(DICE_ROOM);
      if (!wsIds?.size) return;
      const now = Date.now();
      if (now - this._lastDiceTimeLeftBroadcast < this._diceTimeLeftBroadcastCooldown) {
        return;
      }
      
      if (this.currentDiceRoll && this._diceStartTime) {
        const elapsed = (Date.now() - this._diceStartTime) / 1000;
        const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
        const remaining = Math.max(0, totalTime - elapsed);
        const remainingInt = Math.floor(remaining);
        
        if (remainingInt > 0) {
          const minutes = Math.floor(remainingInt / 60);
          const seconds = remainingInt % 60;
          const message = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
          
          this._broadcastDiceNotification("diceError", {
            message: message,
            remaining: remainingInt,
            isDiceTime: true,
            isActive: true,
            round: this._diceRound || 1
          });
          this._lastDiceTimeLeftBroadcast = now;
          return;
        }
      }
      
      const timeLeft = this._getTimeLeftUntilNextDice();
      this._broadcastDiceNotification("diceError", {
        message: `Next dice game in: ${timeLeft.text}`,
        timeLeft: timeLeft.text,
        hours: timeLeft.hours,
        minutes: timeLeft.minutes,
        remaining: -1,
        isDiceTime: this._isDiceTime(),
        isActive: false
      });
      this._lastDiceTimeLeftBroadcast = now;
      
    } catch(e) {}
  }

  _sendDiceTimeLeftToUser(ws) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      const wsId = this._getWsId(ws);
      if (!wsId) return false;
      
      if (this._diceTimeLeftNotified.has(wsId)) {
        return false;
      }
      
      if (this.currentDiceRoll && this._diceStartTime) {
        const elapsed = (Date.now() - this._diceStartTime) / 1000;
        const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
        const remaining = Math.max(0, totalTime - elapsed);
        const remainingInt = Math.floor(remaining);
        
        if (remainingInt > 0) {
          const minutes = Math.floor(remainingInt / 60);
          const seconds = remainingInt % 60;
          const timeText = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
          
          let displayTime = "";
          if (remainingInt >= 20) {
            displayTime = "20s remaining";
          } else if (remainingInt >= 10) {
            displayTime = "10s remaining";
          } else if (remainingInt > 0) {
            displayTime = `${timeText} remaining`;
          }
          
          if (displayTime) {
            this._sendDiceNotification(ws, "diceError", {
              message: displayTime,
              remaining: remainingInt,
              isDiceTime: true,
              isActive: true,
              round: this._diceRound || 1
            });
            this._diceTimeLeftNotified.set(wsId, Date.now());
            return true;
          }
        }
      }
      
      const timeLeft = this._getTimeLeftUntilNextDice();
      this._sendDiceNotification(ws, "diceError", {
        message: `Next dice game in: ${timeLeft.text}`,
        timeLeft: timeLeft.text,
        hours: timeLeft.hours,
        minutes: timeLeft.minutes,
        remaining: -1,
        isDiceTime: this._isDiceTime(),
        isActive: false
      });
      this._diceTimeLeftNotified.set(wsId, Date.now());
      return true;
      
    } catch(e) { return false; }
  }

  _sendDiceErrorWithTime(ws, errorType, customMessage = null) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      const timeLeft = this._getTimeLeftUntilNextDice();
      let message = "";
      
      switch(errorType) {
        case "NOT_DICE_TIME":
          message = `Dice game will start in ${timeLeft.text}`;
          break;
        case "DICE_DISABLED": 
          message = `Dice game is disabled. Next session: ${timeLeft.text}`; 
          break;
        case "DICE_ENDED":
          message = `Dice game ended. Next session: ${timeLeft.text}`;
          break;
        case "DICE_NOT_STARTED": 
          message = `Dice game not started. Next session: ${timeLeft.text}`; 
          break;
        default: 
          message = customMessage || `Next dice game: ${timeLeft.text}`;
      }
      
      this._sendDiceNotification(ws, "diceError", {
        message: message,
        timeLeft: timeLeft.text,
        remaining: -1,
        errorType: errorType,
        isDiceTime: this._isDiceTime()
      });
      
      return true;
    } catch(e) { return false; }
  }

  _getDiceQuestionRemainingTime() {
    try {
      if (!this.currentDiceRoll || !this._diceStartTime) return 0;
      const elapsed = (Date.now() - this._diceStartTime) / 1000;
      return Math.max(0, Math.round((CONSTANTS.DICE_TOTAL_TIME_MS / 1000) - elapsed));
    } catch(e) { return 0; }
  }

  _getDiceAnswerRemainingTime() {
    try {
      if (!this.currentDiceRoll || !this._diceQuestionStartTime) return 0;
      const elapsed = (Date.now() - this._diceQuestionStartTime) / 1000;
      return Math.max(0, Math.round((CONSTANTS.DICE_ANSWER_TIME_MS / 1000) - elapsed));
    } catch(e) { return 0; }
  }

  async _broadcastDiceRoll(diceValue) {
    try {
      if (this._tieActive) {
        return;
      }
      
      const wsIds = this.wsClients.get(DICE_ROOM);
      if (!wsIds?.size) return;

      const msgData = {
        value: diceValue,
        timestamp: Date.now(),
        answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000,
        canAnswerNow: true,
        message: "♡ clik draw ♡",
        round: this._diceRound || 1,
        timerNotifications: [20, 10, 5]
      };
      
      const msgStr = JSON.stringify(["diceRoll", msgData]);
      const wsIdArray = Array.from(wsIds);
      const BATCH_SIZE = CONSTANTS.BROADCAST_BATCH_SIZE || 5;
      let startTime = Date.now();
      
      for (let i = 0; i < wsIdArray.length; i += BATCH_SIZE) {
        const batch = wsIdArray.slice(i, i + BATCH_SIZE);
        
        for (const wsId of batch) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1) {
            try { ws.send(msgStr); } catch(e) {}
          }
        }
        
        if (Date.now() - startTime > 8) {
          await this._cpuYield();
          startTime = Date.now();
        }
      }
      
    } catch(e) {}
  }

  // ==================== LOW CARD GAME METHODS ====================

  _isGameActuallyRunning(game) { 
    try { 
      return game?._isActive === true && !game?._gameEnded; 
    } catch(e) { 
      return false; 
    } 
  }

  _isGameValid(game) { 
    try { 
      return game?._isActive === true && !game?._gameEnded && game?.players?.size > 0; 
    } catch(e) { 
      return false; 
    } 
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
    try { 
      return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; 
    } catch(e) { 
      return "C1"; 
    } 
  }

  _getRandomDrawDelay() { 
    try { 
      return (Math.floor(Math.random() * 14) + 2) * 1000; 
    } catch(e) { 
      return 5000; 
    } 
  }

  _getBotNumberByRound(round) {
    try {
      if (round <= 2) return Math.floor(Math.random() * 12) + 1;
      return Math.random() < 0.6 ?
        [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
        [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
    } catch(e) { return 5; }
  }

  _safeGetGame(room) {
    try {
      if (this.isDestroyed || !room) return null;
      const game = this.activeGames.get(room);
      if (game?._isActive && !game?._gameEnded && game?.players) return game;
      return null;
    } catch(e) { return null; }
  }

  _scheduleGameCleanup(room, game) {
    try {
      if (!room || !game) return;
      if (this._cleanupTimers.has(room)) {
        const oldTimer = this._cleanupTimers.get(room);
        if (oldTimer) clearTimeout(oldTimer);
        this._cleanupTimers.delete(room);
      }
      if (!game._gameEnded) return;
      const timer = setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame?._isActive && !currentGame._gameEnded) { 
            this._cleanupTimers.delete(room); 
            return; 
          }
          this._cleanupTimers.delete(room);
          const gameToDelete = this.activeGames.get(room);
          if (gameToDelete) this._deleteGame(room, gameToDelete);
        } catch(e) {}
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS);
      this._cleanupTimers.set(room, timer);
    } catch(e) {}
  }

  _cleanupGame(game) {
    try {
      if (!game) return;
      if (game._isActive && !game._gameEnded) return;
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) { 
          clearTimeout(game[key]); 
          clearInterval(game[key]); 
          game[key] = null; 
        }
      }
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) clearTimeout(id);
        game._botTimeouts.clear();
        game._botTimeouts = null;
      }
      game.players = null;
      game.botPlayers = null;
      game.numbers = null;
      game.tanda = null;
      game.eliminated = null;
      game._isActive = false;
      game._gameEnded = true;
      game._isEvaluating = false;
    } catch(e) {}
  }

  _deleteGame(room, game) {
    try {
      if (!room || !game) return;
      if (game?._isActive && !game._gameEnded) return;
      if (this._cleanupTimers.has(room)) { 
        clearTimeout(this._cleanupTimers.get(room)); 
        this._cleanupTimers.delete(room); 
      }
      this._roomBroadcastCount.delete(room);
      this._roomBroadcastReset.delete(room);
      if (game) {
        game._gameEnded = true;
        game._isActive = false;
        game.playerWsId = null;
        this._cleanupGame(game);
      }
      this.activeGames.delete(room);
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this._gameStartFlags.delete(room);
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
    } catch(e) {}
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
      setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && !game._gameEnded) this._checkGameCanContinue(room, game);
        } catch(e) {}
      }, 1000);
      return true;
    } catch(e) { return false; }
  }

  async _checkGameCanContinue(room, game) {
    try {
      if (!game?._isActive || game._gameEnded || !game.players || game._isEvaluating || game.evaluationLocked || game.registrationOpen) return;
      const activePlayers = this._getActivePlayers(game);
      if (activePlayers.length === 0) {
        const allPlayers = Array.from(game.players.keys());
        const submitted = Array.from(game.numbers?.keys() || []);
        const notSubmitted = allPlayers.filter(id => !submitted.includes(id) && !game.eliminated?.has(id));
        if (notSubmitted.length > 0) return;
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._broadcastToRoom(room, ["gameLowCardEnd", []]);
        this._scheduleGameCleanup(room, game);
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
          const allWinners = await this._getLowCardWinners(room);
          this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
            winners: allWinners,
            room: room,
            recording: true
          }]);
        }
        
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
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

  _startBotDraws(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.botPlayers) return;
      if (!game._botTimeouts) game._botTimeouts = new Set();
      const notDrawn = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id))
        .slice(0, CONSTANTS.MAX_BOT_DRAWS_PER_ROUND);
      for (const botId of notDrawn) {
        const delay = this._getRandomDrawDelay();
        const timeout = setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (this._isGameActuallyRunning(currentGame) && !currentGame.drawTimeExpired &&
                !currentGame.evaluationLocked && !currentGame.numbers?.has(botId) && !currentGame.eliminated?.has(botId)) {
              this._handleBotDraw(room, botId, currentGame);
            }
            currentGame?._botTimeouts?.delete(timeout);
          } catch(e) {}
        }, delay);
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
        game._evalTimer = setTimeout(() => { 
          try { 
            this._evaluateRound(room, game); 
          } catch(e) {} 
        }, CONSTANTS.EVALUATION_DELAY_MS);
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

  _startRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      if (game._registrationTimer) { 
        clearInterval(game._registrationTimer); 
        game._registrationTimer = null; 
      }
      let timeLeft = 20;
      const timer = setInterval(() => {
        try {
          if (!this._isGameActuallyRunning(game) || !game.registrationOpen || timeLeft < 0) {
            clearInterval(timer);
            if (game._registrationTimer === timer) game._registrationTimer = null;
            return;
          }
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          if (timeLeft === 0) {
            clearInterval(timer);
            game._registrationTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeRegistration(room, game);
          }
          timeLeft--;
        } catch(e) { 
          clearInterval(timer); 
          if (game._registrationTimer === timer) game._registrationTimer = null; 
        }
      }, 1000);
      game._registrationTimer = timer;
    } catch(e) {}
  }

  _closeRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      game.registrationOpen = false;
      if (game._registrationTimer) { 
        clearInterval(game._registrationTimer); 
        game._registrationTimer = null; 
      }
      const humanPlayers = Array.from(game.players.keys()).filter(id => !id.startsWith('BOT_'));
      const humanCount = humanPlayers.length;
      if (!game._botsAdded) {
        if (humanCount === 1 || humanCount === 0) { 
          this._addBots(room, 4); 
          game._botsAdded = true; 
        }
        else if (game.players.size < 2) {
          const needed = Math.min(4 - game.players.size, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { 
            this._addBots(room, needed); 
            game._botsAdded = true; 
          }
        }
      }
      if (this._isGameActuallyRunning(game) && game.players.size >= 2) {
        this._startDrawPhase(room, game);
      } else {
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }

  async _startDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { 
        clearInterval(game._drawTimer); 
        game._drawTimer = null; 
      }
      if (game._evalTimer) { 
        clearTimeout(game._evalTimer); 
        game._evalTimer = null; 
      }
      if (game._botTimeouts) { 
        for (const id of game._botTimeouts) clearTimeout(id); 
        game._botTimeouts.clear(); 
      }
      const activePlayers = this._getActivePlayers(game);
      if (activePlayers.length < 2) {
        if (!game._botsAdded) {
          const needed = Math.min(4 - activePlayers.length, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { 
            this._addBots(room, needed); 
            game._botsAdded = true; 
          }
        }
        const newActive = this._getActivePlayers(game);
        if (newActive.length < 2) {
          if (newActive.length === 1 && !game._gameEnded) {
            const winner = newActive[0]?.name || "Unknown";
            const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
            
            if (game._startedByRecording) {
              await this._addLowCardWinner(room, winner);
              const allWinners = await this._getLowCardWinners(room);
              this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
                winners: allWinners,
                room: room,
                recording: true
              }]);
            }
            
            game._gameEnded = true;
            game._isActive = false;
            game._endTime = Date.now();
            this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
            this._scheduleGameCleanup(room, game);
          } else {
            game._gameEnded = true;
            game._isActive = false;
            game._endTime = Date.now();
            this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
            this._scheduleGameCleanup(room, game);
          }
          return;
        }
      }
      game._phase = 'draw';
      game.drawTimeExpired = false;
      game.evaluationLocked = false;
      game._drawPhaseStart = Date.now();
      if (!game._botTimeouts) game._botTimeouts = new Set();
      const playersList = this._getActivePlayers(game).map(p => p.name);
      this._broadcastToRoom(room, ["gameLowCardClosed", playersList]);
      this._broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
      this._startDrawCountdown(room, game);
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) this._startBotDraws(room, game);
    } catch(e) {}
  }

  _startDrawCountdown(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { 
        clearInterval(game._drawTimer); 
        game._drawTimer = null; 
      }
      let timeLeft = 20;
      const timer = setInterval(() => {
        try {
          if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || timeLeft < 0) {
            clearInterval(timer);
            if (game._drawTimer === timer) game._drawTimer = null;
            return;
          }
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          if (timeLeft === 0) {
            clearInterval(timer);
            game._drawTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeDrawPhase(room, game);
          }
          timeLeft--;
        } catch(e) { 
          clearInterval(timer); 
          if (game._drawTimer === timer) game._drawTimer = null; 
        }
      }, 1000);
      game._drawTimer = timer;
    } catch(e) {}
  }

  _closeDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || game.evaluationLocked) return;
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      if (game._drawTimer) { 
        clearInterval(game._drawTimer); 
        game._drawTimer = null; 
      }
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) this._forceBotDraw(room, botId, game);
      }
      this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      if (game._evalTimer) { 
        clearTimeout(game._evalTimer); 
        game._evalTimer = null; 
      }
      game._evalTimer = setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            this._evaluateRound(room, game);
          }
        } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS);
    } catch(e) {}
  }

  // ==================== EVALUATE ROUND ====================
  async _evaluateRound(room, game) {
    try {
      if (this.isDestroyed || !game?._isActive || game._gameEnded || game._isEvaluating || !game.players) return;
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game) return;
      
      game._isEvaluating = true;
      game._safetyTimer = setTimeout(() => {
        try { 
          if (game?._isEvaluating) { 
            game._isEvaluating = false; 
            this._scheduleGameCleanup(room, game); 
          } 
        } catch(e) {}
      }, CONSTANTS.EVALUATION_TIMEOUT_MS);
      
      if (game._evalTimer) { 
        clearTimeout(game._evalTimer); 
        game._evalTimer = null; 
      }
      if (game._botTimeouts) { 
        for (const id of game._botTimeouts) clearTimeout(id); 
        game._botTimeouts.clear(); 
      }
      
      const numbers = game.numbers || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const tanda = game.tanda || new Map();
      const entries = Array.from(numbers.entries());
      const submittedIds = new Set(numbers.keys());
      const activeIds = this._getActivePlayerIds(game);
      
      if (this._checkCPULimit()) await this._cpuYield();
      
      for (const id of activeIds) {
        if (!submittedIds.has(id)) eliminated.add(id);
      }
      
      if (entries.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          clearTimeout(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        this._broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (entries.length === 1 && eliminated.size >= activeIds.length - 1) {
        const winnerId = entries[0][0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        
        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winnerName);
          const allWinners = await this._getLowCardWinners(room);
          this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
            winners: allWinners,
            room: room,
            recording: true
          }]);
        }
        
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          clearTimeout(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      const activePlayerIds = this._getActivePlayerIds(game);
      if (game.numbers.size < activePlayerIds.length) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          clearTimeout(game._safetyTimer); 
          game._safetyTimer = null; 
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
      
      if (allSame && remaining.length >= 2) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          clearTimeout(game._safetyTimer); 
          game._safetyTimer = null; 
        }
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
          const allWinners = await this._getLowCardWinners(room);
          this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
            winners: allWinners,
            room: room,
            recording: true
          }]);
        }
        
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          clearTimeout(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (remaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          clearTimeout(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
        this._scheduleGameCleanup(room, game);
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
        clearTimeout(game._safetyTimer); 
        game._safetyTimer = null; 
      }
      
      if (this._isGameActuallyRunning(game) && !game._gameEnded) {
        this._startDrawPhase(room, game);
      }
      
    } catch(e) {}
  }

  // ==================== GAME START METHODS ====================

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

      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (isRecordingEnabled) {
        this._safeSend(ws, ["gameLowCardError", 
          "Recording is ACTIVE in this room. Users cannot start games."
        ]);
        return;
      }

      const existingGame = this.activeGames.get(room);
      if (existingGame?._isActive && !existingGame._gameEnded) {
        this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
        return;
      }
      
      if (existingGame) {
        await this._forceCleanupGame(room, existingGame);
      }
      
      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
        this._safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
        return;
      }
      
      if (this.activeGames.size >= this._maxGames) {
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
        _startedByRecording: false,
        _startedBy: 'user'
      };
      
      game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
      game.playerWsId.set(usernameClean, wsId);
      this.activeGames.set(room, game);
      this._addClient(room, ws, usernameClean, false);
      this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
      this._broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
      
      const allWsIds = this.wsClients.get(room);
      if (allWsIds) {
        for (const id of allWsIds) {
          const client = this.wsMap.get(id);
          if (client && client.readyState === 1) {
            this._sendGameStateToClient(client, room);
          }
        }
      }
      
      this._startRegistration(room, game);
      
    } catch(e) {}
  }

  async _forceCleanupGame(room, game) {
    try {
      if (!game) return;
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) { 
          clearTimeout(game[key]); 
          clearInterval(game[key]); 
          game[key] = null; 
        }
      }
      if (game._botTimeouts) { 
        for (const id of game._botTimeouts) clearTimeout(id); 
        game._botTimeouts.clear(); 
      }
      game._gameEnded = true;
      game._isActive = false;
      game._endTime = Date.now();
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
      this.activeGames.delete(room);
      if (this._cleanupTimers.has(room)) { 
        clearTimeout(this._cleanupTimers.get(room)); 
        this._cleanupTimers.delete(room); 
      }
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this._gameStartFlags.delete(`start_${room}`);
    } catch(e) {}
  }

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
        const finalWsId = this._ensureSingleConnection(room, usernameClean, ws, wsId);
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
      this._addClient(room, ws, usernameClean, false);
      game.playerWsId.set(usernameClean, wsId);
      this._broadcastToRoom(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
      
      const allWsIds = this.wsClients.get(room);
      if (allWsIds) {
        for (const id of allWsIds) {
          const client = this.wsMap.get(id);
          if (client && client.readyState === 1) {
            this._sendGameStateToClient(client, room);
          }
        }
      }
      
    } catch(e) {}
  }

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
      if (game.players.has(usernameClean)) {
        if (game.eliminated?.has(usernameClean)) {
          this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
          return;
        }
        const existingWsId = game.playerWsId.get(usernameClean);
        if (existingWsId && existingWsId !== wsId) this._ensureSingleConnection(room, usernameClean, ws, wsId);
      }
      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw') {
        this._safeSend(ws, ["gameLowCardError", "Cannot submit now"]);
        return;
      }
      if (!game.players.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game"]);
        return;
      }
      if (game.eliminated.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
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
        if (game._evalTimer) { 
          clearTimeout(game._evalTimer); 
          game._evalTimer = null; 
        }
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        game._evalTimer = setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
              this._evaluateRound(room, game);
            }
          } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS);
      }
    } catch(e) {}
  }

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
      
      const allWsIds = this.wsClients.get(room);
      if (allWsIds) {
        for (const id of allWsIds) {
          const client = this.wsMap.get(id);
          if (client && client.readyState === 1) {
            this._sendGameStateToClient(client, room);
          }
        }
      }
      
    } catch(e) {}
  }

  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) { 
        this._safeSend(ws, ["gameStatus", "false"]);
        return; 
      }
      let room = roomname;
      if (!room) room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room) { 
        this._safeSend(ws, ["gameStatus", "false"]);
        return; 
      }
      const game = this.activeGames.get(room);
      const isRunning = game?._isActive && !game._gameEnded && game.players?.size > 0;
      this._safeSend(ws, ["gameStatus", isRunning ? "true" : "false"]);
      
      if (isRunning) {
        this._sendGameStateToClient(ws, room);
      }
    } catch(e) {}
  }

  getGame(room) { return this.activeGames.get(room); }

  isGameRunning(room) {
    try {
      if (this.isDestroyed || !room) return { running: false, message: this.isDestroyed ? "System destroyed" : "Invalid room" };
      const game = this.activeGames.get(room);
      if (!game?.players) return { running: false, message: "No game in this room" };
      return { running: game._isActive && !game._gameEnded, message: "Game is " + (game._isActive && !game._gameEnded ? "running" : "not active") };
    } catch(e) {
      return { running: false, message: "Error checking game" };
    }
  }

  _ensureSingleConnection(room, username, newWs, newWsId) {
    try {
      const game = this.activeGames.get(room);
      if (!game) return newWsId;
      const existingWsId = game.playerWsId?.get(username);
      if (existingWsId && existingWsId !== newWsId) {
        const oldWs = this.wsMap.get(existingWsId);
        if (oldWs) {
          try { oldWs.close(1000, "Duplicate connection"); } catch(e) {}
          this._removeClient(room, oldWs);
        }
        if (game.playerWsId) game.playerWsId.set(username, newWsId);
      }
      return newWsId;
    } catch(e) { return newWsId; }
  }

  _shuffleArray(array) {
    try {
      if (!array?.length) return array || [];
      const arr = array.length > CONSTANTS.MAX_ARRAY_SIZE ? array.slice(0, CONSTANTS.MAX_ARRAY_SIZE) : [...array];
      for (let i = arr.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
      }
      return arr;
    } catch(e) { return array || []; }
  }

  // ==================== EVENT HANDLING ====================

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      this._eventQueue.push({ ws, data });
      if (!this._isProcessingQueue) {
        await this._safeExecute(async () => {
          await this._processEventQueue();
        });
      }
    } catch(e) {}
  }

  async _processEventQueue() {
    try {
      if (this._isProcessingQueue || this._eventQueue.length === 0) return;
      if (this._eventQueue.length > CONSTANTS.MAX_EVENT_QUEUE_SIZE) {
        this._eventQueue.splice(0, this._eventQueue.length - CONSTANTS.MAX_EVENT_QUEUE_SIZE);
      }
      this._isProcessingQueue = true;
      this._startCPUTimer();
      const batchSize = CONSTANTS.MAX_EVENTS_PER_TICK;
      const batch = this._eventQueue.splice(0, batchSize);
      for (const item of batch) {
        try {
          await this._processEventItem(item.ws, item.data);
        } catch(e) {
          this._handleError('processEvent', e);
        }
        if (this._checkCPULimit()) {
          await this._cpuYield();
          this._startCPUTimer();
        }
      }
      if (this._eventQueue.length > 0) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed) {
            this._processEventQueue();
          }
        }, CONSTANTS.CPU_YIELD_DELAY_MS);
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
      const evt = data[0];
      const wsId = ws._wsId;
      if (wsId && this._isRateLimited(wsId, evt)) {
        this._safeSend(ws, ["gameLowCardError", "Too many requests"]);
        return;
      }
      await this._safeExecute(async () => {
        await this._handleEventInternal(ws, data);
      });
    } catch(e) {}
  }

  async _handleEventInternal(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      const evt = data[0];

      if (evt === "switchRoom") {
        const [_, room, username] = data;
        await this.switchRoom(ws, room, username);
        return;
      }

      if (evt === "submitDiceAnswer") {
        const [_, username, guess] = data;
        await this.submitDiceAnswer(ws, username, guess);
        return;
      }

      if (evt === "getDiceLastWeekWinner") {
        try {
          const winner = await this.diceGameSystem.getLastWeekWinner();
          if (winner && winner.username) {
            this._safeSend(ws, ["diceLastWeekWinner", winner.username, winner.score || 0, winner.week || ""]);
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
          const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
          const sorted = Object.entries(points)
            .sort((a, b) => b[1] - a[1])
            .slice(0, limit);
          const result = sorted.map(([username, score]) => `${username}|${score}`);
          this._safeSend(ws, ["diceLeaderboard", result]);
        } catch(e) {
          this._safeSend(ws, ["diceLeaderboard", []]);
        }
        return;
      }

      if (evt === "deleteDiceLastWeekWinner") {
        try {
          if (this.env?.QUESTIONS) {
            const success = await this.diceGameSystem.deleteLastWeekWinner();
            if (success) {
              this._safeSend(ws, ["diceLastWeekWinnerDeleted", true, "Last week winner deleted successfully"]);
              this._broadcastToRoom(DICE_ROOM, ["diceNotification", "Last week winner data has been deleted"]);
            } else {
              this._safeSend(ws, ["diceLastWeekWinnerDeleted", false, "Failed to delete"]);
            }
          } else {
            this._safeSend(ws, ["diceLastWeekWinnerDeleted", false, "KV not available"]);
          }
        } catch(e) {
          this._safeSend(ws, ["diceLastWeekWinnerDeleted", false, e.message]);
        }
        return;
      }

      if (evt === "getDiceNotification") {
        const remaining = this._getDiceQuestionRemainingTime();
        const timeLeft = this._getTimeLeftUntilNextDice();
        const answerRemaining = this._getDiceAnswerRemainingTime();
        const notification = {
          type: "diceError",
          timestamp: Date.now(),
          diceValue: this.currentDiceRoll?.value || null,
          remaining: remaining,
          data: {
            isDiceTime: this._isDiceTime(),
            isActive: !!this.currentDiceRoll,
            hasWinner: this.diceHasWinner,
            winner: this.diceWinner,
            timeLeft: timeLeft.text,
            canSubmit: this._canSubmitDiceAnswer,
            answerTimeLeft: this._canSubmitDiceAnswer ? answerRemaining : 0,
            totalTimeLeft: Math.max(0, Math.round((CONSTANTS.DICE_TOTAL_TIME_MS - (Date.now() - this._diceStartTime)) / 1000)),
            round: this._diceRound || 1
          }
        };
        this._safeSend(ws, ["diceNotification", notification]);
        return;
      }

      if (evt === "getDiceStatus") {
        const isActive = !!this.currentDiceRoll && this._canSubmitDiceAnswer;
        this._safeSend(ws, ["diceStatus", isActive, this._diceRound || 1]);
        return;
      }

      if (evt === "getCachedResetStatus") {
        try {
          const now = new Date();
          const currentWeek = this._generateCurrentWeek(now);
          const lastResetWeek = await this._getCachedResetWeek();
          const cacheAge = this._cachedResetWeekTimestamp ? 
            Math.round((Date.now() - this._cachedResetWeekTimestamp) / 1000) : 0;
          
          this._safeSend(ws, ["cachedResetStatus", {
            currentWeek: currentWeek,
            lastResetWeek: lastResetWeek || 'never',
            needsReset: lastResetWeek !== currentWeek,
            cacheAge: cacheAge + 's',
            fromCache: true,
            timestamp: Date.now()
          }]);
        } catch(e) {
          this._safeSend(ws, ["cachedResetStatus", { error: e.message }]);
        }
        return;
      }

      if (evt === "startRecordingWinners") {
        const roomName = data[1];
        if (!roomName) {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        
        const success = await this._startRecordingWinners(roomName);
        
        this._safeSend(ws, ["startRecordingResult", {
          success: success,
          message: success ? "Recording enabled" : "Failed to enable recording"
        }]);
        return;
      }

      if (evt === "stopRecordingWinners") {
        const roomName = data[1];
        if (!roomName) {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        
        const success = await this._stopRecordingWinners(roomName);
        
        this._safeSend(ws, ["stopRecordingResult", {
          success: success,
          message: success ? "Recording stopped" : "Failed to stop recording"
        }]);
        return;
      }

      if (evt === "getRecordingStatus") {
        const roomName = data[1];
        if (!roomName) {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        
        const isRecordingEnabled = await this._getRecordingStatusFromKV(roomName);
        this._safeSend(ws, ["recordingStatus", isRecordingEnabled]);
        return;
      }

      if (evt === "sendWinnersToRoom") {
        const room = data[1];
        if (!room) {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        
        await this._sendWinnersToRoom(room);
        
        this._safeSend(ws, ["sendWinnersResult", {
          success: true,
          message: "Winners data sent to room"
        }]);
        return;
      }

      if (evt === "getRoomWinners") {
        const room = data[1] || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
        if (!room) {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        
        const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
        const winners = await this._getLowCardWinners(room);
        
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
          winners: winners,
          room: room,
          recording: isRecordingEnabled
        }]);
        
        this._safeSend(ws, ["sendWinnersResult", {
          success: true,
          message: "Winners updated to room"
        }]);
        return;
      }

      if (evt === "lowCardWinnerUpdate") {
        const room = data[1] || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
        if (!room) {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        
        await this._broadcastLowCardWinners(room);
        
        this._safeSend(ws, ["sendWinnersResult", {
          success: true,
          message: "Winners refreshed"
        }]);
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
        case "gameLowCardStart":
          await this.startGame(ws, data[1], data[2]);
          break;
        case "gameLowCardJoin":
          await this.joinGame(ws, data[1]);
          break;
        case "gameLowCardNumber":
          await this.submitNumber(ws, data[1], data[2] || "", data[3]);
          break;
        case "gameLowCardLeave":
          await this.leaveGame(ws, data[1]);
          break;
        case "checkGameRunning":
          await this.checkGameRunning(ws, data[1]);
          break;
        case "getGameState":
          const targetRoom = data[1] || room;
          this._sendGameStateToClient(ws, targetRoom);
          break;
        default:
          break;
      }
      
    } catch(e) {}
  }

  async _startGameWithRecording(ws, room, bet, username) {
    try {
      if (!room || !username) {
        this._safeSend(ws, ["gameLowCardError", "Room and username required"]);
        return;
      }

      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled) {
        this._safeSend(ws, ["gameLowCardError", "Recording is not enabled in this room"]);
        return;
      }

      const existingGame = this.activeGames.get(room);
      if (existingGame && existingGame._isActive && !existingGame._gameEnded) {
        this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
        return;
      }

      if (existingGame) {
        await this._forceCleanupGame(room, existingGame);
      }

      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
        this._safeSend(ws, ["gameLowCardError", "Invalid bet (0 or 100-100000)"]);
        return;
      }

      if (this.activeGames.size >= this._maxGames) {
        this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
        return;
      }

      const wsId = ws._wsId;
      const game = {
        room: room,
        players: new Map(),
        botPlayers: new Map(),
        registrationOpen: true,
        round: 1,
        numbers: new Map(),
        tanda: new Map(),
        eliminated: new Set(),
        betAmount: betAmount,
        hostId: username,
        hostName: username,
        useBots: false,
        evaluationLocked: false,
        drawTimeExpired: false,
        _isActive: true,
        _gameEnded: false,
        _phase: 'registration',
        _botTimeouts: new Set(),
        _botsAdded: false,
        _registrationTimer: null,
        _drawTimer: null,
        _evalTimer: null,
        _safetyTimer: null,
        _isEvaluating: false,
        _createdAt: Date.now(),
        _drawPhaseStart: null,
        _endTime: null,
        playerWsId: new Map(),
        _startedByRecording: true,
        _startedBy: 'recording'
      };

      game.players.set(username, { id: username, name: username });
      game.playerWsId.set(username, wsId);
      this.activeGames.set(room, game);
      this._addClient(room, ws, username, false);
      this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
      this._broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount]);

      const allWsIds = this.wsClients.get(room);
      if (allWsIds) {
        for (const id of allWsIds) {
          const client = this.wsMap.get(id);
          if (client && client.readyState === 1) {
            this._sendGameStateToClient(client, room);
          }
        }
      }

      this._startRegistration(room, game);

    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }

  // ==================== STALE GAME CLEANUP ====================

  _checkStuckGames() {
    try {
      const now = Date.now();
      const toEvaluate = [];
      const toClose = [];
      
      for (const [room, game] of this.activeGames) {
        if (!game?._isActive || game._gameEnded) continue;
        
        if (game._phase === 'draw' && game._drawPhaseStart &&
            (now - game._drawPhaseStart) > CONSTANTS.STUCK_DRAW_TIMEOUT_MS) {
          toEvaluate.push({ room, game });
        }
        
        if (game._phase === 'registration' && game.registrationOpen &&
            game._createdAt && (now - game._createdAt) > CONSTANTS.STUCK_REGISTRATION_TIMEOUT_MS) {
          toClose.push({ room, game });
        }
        
        if (game._phase !== 'registration' && !game.registrationOpen) {
          const activePlayers = this._getActivePlayers(game);
          if (activePlayers.length === 0 && !game._gameEnded) {
            game._gameEnded = true;
            game._isActive = false;
            game._endTime = Date.now();
            this._broadcastToRoom(room, ["gameLowCardEnd", []]);
            this._scheduleGameCleanup(room, game);
          }
        }
      }
      
      for (const item of toEvaluate) {
        this._closeDrawPhase(item.room, item.game);
      }
      
      for (const item of toClose) {
        this._closeRegistration(item.room, item.game);
      }
      
    } catch(e) {}
  }

  _cleanupStaleGames() {
    try {
      const now = Date.now();
      for (const [room, game] of this.activeGames) {
        if (!game) continue;
        if (game._isActive && !game._gameEnded) continue;
        if (game._gameEnded) {
          const endTime = game._endTime || game._createdAt || now;
          if ((now - endTime) > CONSTANTS.STALE_GAME_TIMEOUT_MS) this._scheduleGameCleanup(room, game);
          continue;
        }
        if (!game._isActive && !game._gameEnded && game._createdAt && (now - game._createdAt) > 300000) {
          game._gameEnded = true;
          game._endTime = now;
          this._scheduleGameCleanup(room, game);
        }
      }
    } catch(e) {}
  }

  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      for (const [wsId, ws] of this.wsMap) {
        if (!ws || ws.readyState !== 1 || ws._closing) toRemove.push(wsId);
      }
      for (const wsId of toRemove) {
        const ws = this.wsMap.get(wsId);
        if (ws) {
          const room = this.clientRooms.get(wsId);
          if (room) this._removeClientFromRoom(room, wsId);
          this.clientRooms.delete(wsId);
          this.wsMap.delete(wsId);
          this._diceTimeLeftNotified.delete(wsId);
          this._nextDiceNotified.delete(wsId);
          this._diceJoinedNotified.delete(wsId);
          for (const [username, conn] of this.userConnections) {
            if (conn?.wsId === wsId) { this.userConnections.delete(username); break; }
          }
        }
      }
    } catch(e) {}
  }

  _startWeeklyResetChecker() {}

  _setupErrorHandlers() {
    try {
      const self = this;
      if (typeof process !== 'undefined' && process.on) {
        process.on('unhandledRejection', (reason) => {
          self._handleError('unhandledRejection', reason);
        });
        process.on('uncaughtException', (error) => {
          self._handleError('uncaughtException', error);
        });
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
      if (this._errorCount > CONSTANTS.MAX_UNHANDLED_ERRORS && 
          !this._isRecovering && 
          this._recoveryAttempts < this._maxRecoveryAttempts) {
        this._isRecovering = true;
        this._recoveryAttempts++;
        this._lastRecoveryTime = now;
        setTimeout(() => {
          this._forceRecovery();
          this._isRecovering = false;
        }, CONSTANTS.ERROR_RECOVERY_DELAY_MS);
      }
    } catch(e) {}
  }

  _startHealthCheck() {}

  _performHealthCheck() {
    try {
      const now = Date.now();
      this._lastHeartbeat = now;
      if (this._isProcessingQueue && this._eventQueue.length > 0) {
        const queueAge = now - (this._lastHeartbeat || now);
        if (queueAge > 30000) {
          this._isProcessingQueue = false;
          this._eventQueue = [];
        }
      }
      if (this._isDiceTime() && this.currentDiceRoll && this._diceStartTime) {
        const elapsed = (now - this._diceStartTime) / 1000;
        if (elapsed > (CONSTANTS.DICE_TOTAL_TIME_MS / 1000) + 30) {
          this.currentDiceRoll = null;
          this._diceTimeout = null;
          this._isShowingDice = false;
          this._canSubmitDiceAnswer = false;
          this._diceRemainingShown = false;
          this._diceTimeUpShown = false;
          this._stopDiceTimerNotifications();
        }
      }
      const deadConnections = [];
      for (const [wsId, ws] of this.wsMap) {
        if (!ws || ws.readyState !== 1) {
          deadConnections.push(wsId);
        }
      }
      for (const wsId of deadConnections) {
        try {
          const ws = this.wsMap.get(wsId);
          if (ws) {
            const room = this.clientRooms.get(wsId);
            if (room) this._removeClientFromRoom(room, wsId);
            this.clientRooms.delete(wsId);
            this.wsMap.delete(wsId);
          }
        } catch(e) {}
      }
    } catch(e) {}
  }

  _forceRecovery() {
    try {
      if (this.closing || this.isDestroyed) return;
      if (this._recoveryAttempts >= this._maxRecoveryAttempts) return;
      this._resetCriticalState();
      this._cleanupResources();
      
      this._startWeeklyResetChecker();
      
      if (!this._initialized && !this._initializing) {
        this._initAsync();
      }
      if (this._isDiceTime()) {
        this.diceAutoEnabled = true;
      }
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "Server has recovered"]);
    } catch(e) {}
  }

  _resetCriticalState() {
    try {
      this.currentDiceRoll = null;
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.diceAnswered = new Set();
      this._diceStartTime = null;
      this._isShowingDice = false;
      this._winnerProcessed = false;
      this._canSubmitDiceAnswer = false;
      this._diceQuestionStartTime = null;
      this._diceOutOfTimeShown = false;
      this._diceRemainingShown = false;
      this._diceTimeUpShown = false;
      this._lastSentRemaining = -1;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
      this._stopDiceTimerNotifications();
      
      this._playerAnswers = new Map();
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieActive = false;
      this._tieBreakers.clear();
      this._tieRound = 0;
      this._processingTieResults = false;
      if (this._tieTimer) {
        clearTimeout(this._tieTimer);
        this._tieTimer = null;
      }
      if (this._tieInterval) {
        clearInterval(this._tieInterval);
        this._tieInterval = null;
      }
      
      if (this._eventQueue) {
        this._eventQueue = [];
      }
      if (this._rateLimitMap) {
        this._rateLimitMap.clear();
      }
    } catch(e) {}
  }

  _cleanupResources() {
    try {
      if (this._diceTimeout) {
        clearTimeout(this._diceTimeout);
        this._diceTimeout = null;
      }
      if (this._diceBreakTimeout) {
        clearTimeout(this._diceBreakTimeout);
        this._diceBreakTimeout = null;
      }
      if (this._diceStartTimeout) {
        clearTimeout(this._diceStartTimeout);
        this._diceStartTimeout = null;
      }
      if (this.diceTimer) {
        clearInterval(this.diceTimer);
        this.diceTimer = null;
      }
      if (this.diceAutoTimer) {
        clearInterval(this.diceAutoTimer);
        this.diceAutoTimer = null;
      }
      if (this._diceTimeUpCooldownTimer) {
        clearTimeout(this._diceTimeUpCooldownTimer);
        this._diceTimeUpCooldownTimer = null;
      }
      this._stopDiceTimerNotifications();
      
      if (this._tieTimer) {
        clearTimeout(this._tieTimer);
        this._tieTimer = null;
      }
      if (this._tieInterval) {
        clearInterval(this._tieInterval);
        this._tieInterval = null;
      }
    } catch(e) {}
  }

  // ==================== WS HELPER METHODS ====================

  _getWsId(ws) { return ws?._wsId || null; }

  _getRoomForWs(ws) {
    if (!ws) return null;
    return ws.room || ws.roomname || null;
  }

  _ensureRoomConsistency(ws) {
    try {
      if (!ws) return null;
      const wsId = this._getWsId(ws);
      if (!wsId) return null;
      
      let room = ws.room || ws.roomname || null;
      
      if (!room) {
        room = this.clientRooms.get(wsId) || null;
      }
      
      if (!room && ws.username) {
        const conn = this.userConnections.get(ws.username);
        if (conn) room = conn.room || null;
      }
      
      if (room) {
        ws.room = room;
        ws.roomname = room;
        
        if (!this.wsClients.has(room)) {
          this.wsClients.set(room, new Set());
        }
        
        if (!this.wsClients.get(room).has(wsId)) {
          this.wsClients.get(room).add(wsId);
          this.clientRooms.set(wsId, room);
          this.wsMap.set(wsId, ws);
        }
        
        return room;
      }
      
      return null;
    } catch(e) { 
      return null; 
    }
  }

  _addClient(room, ws, username = null, isNewConnection = false) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      if (!wsId) { 
        this._safeSend(ws, ["gameLowCardError", "Connection error"]); 
        return; 
      }
      
      if (this.clientRooms.has(wsId)) {
        const oldRoom = this.clientRooms.get(wsId);
        if (oldRoom && oldRoom !== room) {
          this._removeClientFromRoom(oldRoom, wsId);
        }
      }
      
      if (username) {
        let conn = this.userConnections.get(username);
        if (conn) { 
          conn.room = room; 
          conn.timestamp = Date.now(); 
          conn.ws = ws; 
          conn.wsId = wsId;
        } else { 
          this.userConnections.set(username, { 
            wsId, 
            ws, 
            room, 
            timestamp: Date.now() 
          }); 
        }
      }
      
      let clients = this.wsClients.get(room);
      if (!clients) {
        clients = new Set();
        this.wsClients.set(room, clients);
      }
      clients.add(wsId);
      
      this.clientRooms.set(wsId, room);
      this.wsMap.set(wsId, ws);
      ws.room = room;
      ws.roomname = room;
      if (username) ws.username = username;
      
      if (username) {
        if (!this.roomViewers.has(room)) {
          this.roomViewers.set(room, new Set());
        }
        this.roomViewers.get(room).add(username);
      }
      
    } catch(e) {}
  }

  _removeClientFromRoom(room, wsId) {
    try {
      if (!room || !wsId) return;
      const clients = this.wsClients.get(room);
      if (clients) {
        clients.delete(wsId);
        if (clients.size === 0) {
          this.wsClients.delete(room);
        }
      }
    } catch(e) {}
  }

  _removeClient(room, ws) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      if (!wsId) return;
      const username = ws.username;
      this._diceTimeLeftNotified.delete(wsId);
      this._nextDiceNotified.delete(wsId);
      this._diceJoinedNotified.delete(wsId);
      this._removeClientFromRoom(room, wsId);
      this.clientRooms.delete(wsId);
      this.wsMap.delete(wsId);
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn?.wsId === wsId) this.userConnections.delete(username);
        if (this.roomViewers.has(room)) {
          this.roomViewers.get(room).delete(username);
          if (this.roomViewers.get(room).size === 0) this.roomViewers.delete(room);
        }
      }
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
    } catch(e) {}
  }

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
          this._removeClientFromRoom(currentRoom, wsId);
        }
        
        this._addClient(roomName, ws, username, false);
        ws.room = roomName;
        ws.roomname = roomName;
        if (username) ws.username = username;
        
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
          this._switchRetries.delete(lockKey);
        }, 2000);
      }
    } catch(e) {}
  }

  _sendGameStateToClient(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room) return;
      
      const game = this.activeGames.get(room);
      if (!game || !game._isActive || game._gameEnded) {
        this._safeSend(ws, ["gameState", {
          room: room,
          hasGame: false,
          gameType: 'lowcard'
        }]);
        return;
      }
      
      const activePlayers = this._getActivePlayers(game);
      const allPlayers = Array.from(game.players.values()).map(p => p.name);
      const eliminated = Array.from(game.eliminated || []);
      const submitted = Array.from(game.numbers?.keys() || []);
      
      const state = {
        room: room,
        hasGame: true,
        gameType: 'lowcard',
        isActive: game._isActive,
        phase: game._phase || 'registration',
        round: game.round || 1,
        bet: game.betAmount || 0,
        host: game.hostName || 'Unknown',
        registrationOpen: game.registrationOpen || false,
        players: allPlayers,
        activePlayers: activePlayers.map(p => p.name),
        eliminated: eliminated,
        submitted: submitted,
        playerCount: game.players.size,
        activeCount: activePlayers.length,
        isEvaluating: game._isEvaluating || false,
        evaluationLocked: game.evaluationLocked || false,
        drawTimeExpired: game.drawTimeExpired || false
      };
      
      this._safeSend(ws, ["gameState", state]);
      
      if (game._phase === 'draw' && ws.username) {
        const userNumber = game.numbers.get(ws.username);
        if (userNumber !== undefined) {
          const userTanda = game.tanda.get(ws.username) || '';
          this._safeSend(ws, ["gameLowCardPlayerDraw", ws.username, userNumber, userTanda]);
        }
      }
      
      this._getRecordingStatusFromKV(room).then(isRecording => {
        if (isRecording !== undefined) {
          this._safeSend(ws, ["recordingStatus", isRecording]);
        }
      }).catch(() => {});
      
    } catch(e) {}
  }

  _sendDiceNotificationOnSwitch(ws, wsId) {
    try {
      this._diceTimeLeftNotified.delete(wsId);
      this._nextDiceNotified.delete(wsId);
      this._diceJoinedNotified.delete(wsId);
      
      const isGameActive = this.currentDiceRoll && this._canSubmitDiceAnswer;
      
      if (isGameActive) {
        const elapsed = (Date.now() - this._diceStartTime) / 1000;
        const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
        const remaining = Math.max(0, totalTime - elapsed);
        const remainingInt = Math.floor(remaining);
        
        if (remainingInt > 0) {
          let displayTime = "";
          if (remainingInt >= 20) {
            displayTime = "20s remaining";
          } else if (remainingInt >= 10) {
            displayTime = "10s remaining";
          } else if (remainingInt >= 5) {
            displayTime = "5s remaining";
          } else {
            displayTime = `${remainingInt}s remaining`;
          }
          
          this._sendDiceNotification(ws, "diceError", {
            message: displayTime,
            remaining: remainingInt,
            isDiceTime: true,
            isActive: true
          });
        }
      } else {
        const timeLeft = this._getTimeLeftUntilNextDice();
        
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed && ws && ws.readyState === 1) {
            this._sendDiceNotification(ws, "diceError", {
              message: `Dice game ended. Next session in: ${timeLeft.text}`,
              timeLeft: timeLeft.text,
              hours: timeLeft.hours,
              minutes: timeLeft.minutes,
              remaining: -1,
              isDiceTime: this._isDiceTime(),
              isActive: false
            });
          }
        }, 5000);
        
        this._diceJoinedNotified.set(wsId, true);
      }
      
      if (this.currentDiceRoll && this._canSubmitDiceAnswer) {
        this._safeSend(ws, ["diceRoll", {
          value: this.currentDiceRoll.value,
          timestamp: this.currentDiceRoll.timestamp,
          answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000,
          canAnswerNow: true,
          round: this._diceRound || 1
        }]);
      }
    } catch(e) {}
  }

  async _broadcastToRoom(room, message) {
    try {
      if (this.closing || this.isDestroyed || !room || !message) return;
      const wsIds = this.wsClients.get(room);
      if (!wsIds?.size) return;
      
      const msgStr = JSON.stringify(message);
      const wsIdArray = Array.from(wsIds);
      const BATCH_SIZE = CONSTANTS.BROADCAST_BATCH_SIZE || 5;
      let startTime = Date.now();
      
      for (let i = 0; i < wsIdArray.length; i += BATCH_SIZE) {
        const batch = wsIdArray.slice(i, i + BATCH_SIZE);
        
        for (const wsId of batch) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1) {
            try { ws.send(msgStr); } catch(e) {}
          }
        }
        
        if (Date.now() - startTime > 8) {
          await this._cpuYield();
          startTime = Date.now();
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

  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
      // Cancel any pending alarms
      try {
        await this.state.storage.setAlarm(null);
      } catch(e) {}
      
      // Cleanup all timers
      if (this._diceTimeout) {
        clearTimeout(this._diceTimeout);
        this._diceTimeout = null;
      }
      if (this._diceBreakTimeout) {
        clearTimeout(this._diceBreakTimeout);
        this._diceBreakTimeout = null;
      }
      if (this._diceStartTimeout) {
        clearTimeout(this._diceStartTimeout);
        this._diceStartTimeout = null;
      }
      if (this._diceTimeUpCooldownTimer) {
        clearTimeout(this._diceTimeUpCooldownTimer);
        this._diceTimeUpCooldownTimer = null;
      }
      if (this._tieTimer) {
        clearTimeout(this._tieTimer);
        this._tieTimer = null;
      }
      if (this._tieInterval) {
        clearInterval(this._tieInterval);
        this._tieInterval = null;
      }
      
      this._stopDiceTimerNotifications();
      
      // Cleanup games
      for (const [room, game] of this.activeGames) {
        await this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();
      
      await this.resetDice();
      this.diceGameSystem.clearCache();
      
      // Close all connections
      for (const [wsId, ws] of this.wsMap) {
        try {
          if (ws && ws.readyState === 1) {
            ws.close(1000, "Server shutting down");
          }
        } catch(e) {}
      }
      this.wsMap.clear();
      this.wsClients.clear();
      this.clientRooms.clear();
      
      // Save final state
      await this._saveState();
      
      this._alarmScheduled = false;
      
    } catch(e) {}
  }

  async fetch(req) {
    try {
      if (this.closing || this.isDestroyed) {
        return new Response("Server is shutting down", { status: 503 });
      }
      
      const url = new URL(req.url);
      
      if (url.pathname === "/health") {
        try {
          const status = {
            status: "ok",
            uptime: Date.now() - this._startTime,
            restartCount: this._restartCount,
            isRestarting: this._isRestarting,
            isRecovering: this._isRecovering,
            diceActive: !!this.currentDiceRoll,
            diceRound: this._diceRound || 0,
            diceCooldown: this._diceTimeUpCooldown,
            gamesRunning: this.activeGames.size,
            wsConnections: this.wsMap.size,
            eventQueueSize: this._eventQueue?.length || 0,
            errorCount: this._errorCount,
            timestamp: Date.now(),
            diceSchedule: QUIZ_SCHEDULE.SESSIONS.map(s => `${s.start}:00-${s.end}:00`),
            currentWITATime: this._getCurrentWITATime().formatted,
            lastResetWeek: this._cachedResetWeek || 'unknown',
            currentWeek: this._generateCurrentWeek(new Date()),
            tieActive: this._tieActive,
            tieRound: this._tieRound,
            tiePlayers: this._tiePlayers.length,
            alarmScheduled: this._alarmScheduled
          };
          return new Response(JSON.stringify(status), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch(e) {
          return new Response(JSON.stringify({ status: "degraded", error: e.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }
      
      if (url.pathname === "/game/ws") {
        const upgrade = req.headers.get("Upgrade");
        if (upgrade !== "websocket") {
          return new Response("WebSocket only", { status: 400 });
        }
        
        if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
          return new Response("Server at maximum capacity", { status: 503 });
        }
        
        try {
          const pair = new WebSocketPair();
          const [client, server] = [pair[0], pair[1]];
          const wsId = ++this._wsIdCounter;
          
          server._wsId = wsId;
          server._closing = false;
          server.room = null;
          server.roomname = null;
          server._createdAt = Date.now();
          server.username = null;
          server._cf = req.cf || {};
          server._country = req.cf?.country || 'US';
          server._language = 'en';
          
          try { 
            this.state.acceptWebSocket(server); 
          } catch(e) { 
            return new Response("WebSocket acceptance failed", { status: 500 }); 
          }
          
          server.addEventListener("message", async (event) => {
            try {
              const data = JSON.parse(event.data);
              if (Array.isArray(data) && data.length > 0) {
                await this.handleEvent(server, data);
              }
            } catch(e) { 
              this._safeSend(server, ["gameLowCardError", e.message || "Error"]); 
            }
          });
          
          server.addEventListener("close", () => {
            this.webSocketClose(server);
          });
          
          server.addEventListener("error", () => {
            this.webSocketError(server);
          });
          
          return new Response(null, { status: 101, webSocket: client });
        } catch(e) {
          return new Response("WebSocket creation failed", { status: 500 });
        }
      }
      
      return new Response("Game Server", { status: 200 });
    } catch(e) {
      this._handleError('fetch', e);
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  webSocketClose(ws) {
    try {
      if (!ws) return;
      
      ws._closing = true;
      
      const wsId = ws._wsId;
      const username = ws.username;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      
      if (room) {
        this._removeClientFromRoom(room, wsId);
      }
      
      if (wsId) {
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
        this._diceTimeLeftNotified.delete(wsId);
        this._nextDiceNotified.delete(wsId);
        this._diceJoinedNotified.delete(wsId);
      }
      
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn?.wsId === wsId) {
          this.userConnections.delete(username);
        }
      }
      
      if (room && username) {
        const viewers = this.roomViewers.get(room);
        if (viewers) {
          viewers.delete(username);
          if (viewers.size === 0) {
            this.roomViewers.delete(room);
          }
        }
      }
      
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
      
      const clients = this.wsClients.get(DICE_ROOM);
      if (clients?.size > 0) {
        this.ensureDiceRunning();
      }
    } catch(e) {}
  }

  async webSocketError(ws) {
    try {
      if (!ws) return;
      
      ws._closing = true;
      
      const wsId = ws._wsId;
      const username = ws.username;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      
      if (room) {
        this._removeClientFromRoom(room, wsId);
      }
      
      if (wsId) {
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
        this._diceTimeLeftNotified.delete(wsId);
        this._nextDiceNotified.delete(wsId);
        this._diceJoinedNotified.delete(wsId);
      }
      
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn?.wsId === wsId) {
          this.userConnections.delete(username);
        }
      }
      
      if (room && username) {
        const viewers = this.roomViewers.get(room);
        if (viewers) {
          viewers.delete(username);
          if (viewers.size === 0) {
            this.roomViewers.delete(room);
          }
        }
      }
      
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
    } catch(e) {}
  }

  async webSocketMessage(ws, msg) {
    try {
      if (!ws || ws._closing || this.closing || this.isDestroyed || !ws._wsId) return;
      const data = JSON.parse(msg);
      if (Array.isArray(data) && data.length > 0) {
        await this.handleEvent(ws, data);
      }
    } catch(e) {
      this._handleError('webSocketMessage', e);
      this._safeSend(ws, ["gameLowCardError", "Server is recovering"]);
    }
  }
}
