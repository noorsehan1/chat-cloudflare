// ==================== GAME-SERVER.JS - FULL CLASS ====================

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
  
  // ==================== DICE GAME CONSTANTS ====================
  MAX_DICE_GAMES: 10,
  DICE_ROLL_TIME_MS: 3000,
  DICE_READING_TIME_MS: 2000,
  DICE_ANSWER_TIME_MS: 20000,
  DICE_TOTAL_TIME_MS: 22000,
  DICE_BREAK_MS: 2000,
  MAX_DICE_VALUE: 6,
  DICE_ROOM: "Quiz",
  DICE_POINT_KEY: 'dice_points',
  DICE_LAST_WEEK_WINNER: 'dice_last_week_winner',
  DICE_WINNER_KEY: 'dice_winner_',
  DICE_RECORDING_KEY: 'dice_recording_status_',
  QUIZ_START_DELAY_MS: 5000,
};

const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: 1, end: 2 },
    { start: 11, end: 12 },
    { start: 23, end: 24 }
  ],
  TIMEZONE_OFFSET: 8,
};

const QUIZ_ROOM = "Quiz";
const DICE_ROOM = "Quiz";

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
    this._cpuMonitorInterval = null;
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

  _cpuMonitorTask() {
    try {
      const now = Date.now();
      for (const [key, data] of this._rateLimitMap) {
        if (now - data.resetTime > 1000) this._rateLimitMap.delete(key);
      }
      if (this._cpuHistory.length > 0) {
        const avg = this._cpuHistory.reduce((a, b) => a + b, 0) / this._cpuHistory.length;
        if (avg > CONSTANTS.CPU_TIME_LIMIT_MS * 0.9) {
          this._isThrottled = true;
          setTimeout(() => { this._isThrottled = false; }, 500);
        }
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
  }

  async loadScores() {
    try {
      if (this._loading) return this._isLoaded;
      this._loading = true;
      
      const env = this.env;
      if (!env?.QUESTIONS) {
        this._loading = false;
        return false;
      }
      
      const points = await env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      
      this._isLoaded = true;
      this._loading = false;
      return true;
    } catch(e) {
      this._loading = false;
      return false;
    }
  }

  async getPoints() {
    try {
      if (!this.env?.QUESTIONS) return {};
      const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json');
      return points || {};
    } catch(e) {
      return {};
    }
  }

  async setPoints(points) {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
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
      return await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_WEEK_WINNER, 'json');
    } catch(e) {
      return null;
    }
  }

  async setLastWeekWinner(winner) {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winner));
      return true;
    } catch(e) {
      return false;
    }
  }

  async deleteLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
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

  getDiceEmoji(value) {
    const emojis = ['⚀', '⚁', '⚂', '⚃', '⚄', '⚅'];
    return emojis[value - 1] || '🎲';
  }

  clearCache() {
    this.userScores.clear();
    this._usedDiceValues.clear();
  }
}

// ==================== CENTRALIZED SCHEDULER ====================
class CentralizedScheduler {
  constructor() {
    this.tasks = [];
    this.isRunning = false;
    this._lastRun = Date.now();
    this._taskQueue = [];
    this._loopInterval = null;
  }

  registerTask(name, interval, fn, options = {}) {
    this.tasks.push({
      name,
      interval,
      fn,
      lastRun: 0,
      options,
      isRunning: false,
      errorCount: 0
    });
  }

  async run() {
    if (this.isRunning) return;
    this.isRunning = true;

    try {
      const now = Date.now();
      
      const dueTasks = this.tasks.filter(task => {
        if (task.isRunning) return false;
        const elapsed = now - task.lastRun;
        return elapsed >= task.interval;
      });

      for (const task of dueTasks) {
        task.isRunning = true;
        task.lastRun = now;
        
        try {
          if (this._shouldYieldCPU()) {
            await this._yield();
          }
          
          await task.fn();
          task.errorCount = 0;
          
        } catch(e) {
          task.errorCount++;
          if (task.errorCount > 5) {
            task.interval = task.interval * 2;
            task.errorCount = 0;
          }
        } finally {
          task.isRunning = false;
        }
        
        await this._yield();
      }

    } finally {
      this.isRunning = false;
    }
  }

  _shouldYieldCPU() {
    const elapsed = Date.now() - this._lastRun;
    return elapsed > 8;
  }

  async _yield() {
    return new Promise(resolve => setTimeout(resolve, 1));
  }

  start(intervalMs = 50) {
    if (this._loopInterval) {
      clearInterval(this._loopInterval);
    }
    this._loopInterval = setInterval(() => {
      this.run();
    }, intervalMs);
  }

  stop() {
    if (this._loopInterval) {
      clearInterval(this._loopInterval);
      this._loopInterval = null;
    }
  }
}

// ==================== GAME SERVER CLASS ====================
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
      this._healthCheckInterval = null;
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

      // ==================== LOWCARD GAME STATE ====================
      this.activeGames = new Map();
      this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._switchLocks = new Map();

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

      // ==================== DICE GAME STATE ====================
      this.diceAnswered = new Set();
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.diceTimer = null;
      this.isDiceRunning = false;
      this.currentDiceRoll = null;
      this.isDiceWaiting = false;
      this._diceStartTime = null;
      this._diceTimeout = null;
      this._diceBreakTimeout = null;
      this._diceStartTimeout = null;
      this.diceAutoEnabled = false;
      this.diceAutoTimer = null;
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

      // ==================== FLAGS ====================
      this._diceOutOfTimeShown = false;
      this._diceRemainingShown = false;
      this._diceTimeUpShown = false;

      // DICE SYSTEM
      this.diceGameSystem = new DiceGameSystem(this);

      // ==================== CENTRALIZED SCHEDULER ====================
      this._scheduler = new CentralizedScheduler();
      this._setupScheduler();

      this._initAsync();
      this._startHealthCheck();

      this._initRecordingStatusFromKV();

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

  // ==================== SETUP SCHEDULER ====================
  _setupScheduler() {
    this._scheduler.registerTask('cpuMonitor', 100, () => {
      this._cpuMonitorTask();
    });

    this._scheduler.registerTask('healthCheck', 10000, () => {
      this._healthCheckTask();
    });

    this._scheduler.registerTask('weeklyReset', 3600000, async () => {
      await this._weeklyResetTask();
    });

    this._scheduler.registerTask('diceKeepAlive', 1000, () => {
      this._diceKeepAliveTask();
    });

    this._scheduler.registerTask('diceAuto', 60000, async () => {
      await this._diceAutoTask();
    });

    this._scheduler.registerTask('diceTimer', 30000, () => {
      this._diceTimerTask();
    });

    this._scheduler.registerTask('stuckGamesCheck', 15000, () => {
      this._checkStuckGames();
    });

    this._scheduler.registerTask('staleGamesCleanup', 60000, () => {
      this._cleanupStaleGames();
    });

    this._scheduler.registerTask('deadConnectionsCleanup', 30000, () => {
      this._cleanupDeadConnections();
    });

    this._scheduler.start(CONSTANTS.SCHEDULER_LOOP_INTERVAL_MS || 50);
  }

  // ==================== SCHEDULER TASKS ====================
  _healthCheckTask() {
    try {
      this._performHealthCheck();
    } catch(e) {}
  }

  async _weeklyResetTask() {
    await this._checkAndResetWeeklyDice();
  }

  _diceKeepAliveTask() {
    try {
      this._lastHeartbeat = Date.now();
      
      if (!this._isDiceTime()) {
        if (!this._diceOutOfTimeShown) {
          const timeLeft = this._getTimeLeftUntilNextDice();
          this._broadcastToRoom(DICE_ROOM, ["diceTimeLeft", 
            `Next dice game in: ${timeLeft.text}`, 
            true, 
            false
          ]);
          this._diceOutOfTimeShown = true;
        }
        return;
      }
      
      if (this._diceOutOfTimeShown) {
        this._diceOutOfTimeShown = false;
        this._diceRemainingShown = false;
        this._diceTimeUpShown = false;
      }
      
      if (this.currentDiceRoll && this._diceStartTime) {
        const now = Date.now();
        const elapsed = (now - this._diceStartTime) / 1000;
        const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
        const remaining = Math.max(0, totalTime - elapsed);
        const remainingInt = Math.floor(remaining);
        const minutes = Math.floor(remainingInt / 60);
        const seconds = remainingInt % 60;
        const timeText = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
        
        if (this._canSubmitDiceAnswer && remaining > 0) {
          const isTargetTime = (remainingInt === 20 || remainingInt === 10 || remainingInt === 5);
          
          if (isTargetTime && !this._diceTimeUpShown) {
            this._broadcastToRoom(DICE_ROOM, ["diceTimeLeft", 
              `${timeText} remaining`, 
              false, 
              true
            ]);
          }
        }
        
        if (remaining <= 0 && !this._diceTimeUpShown) {
          this._broadcastToRoom(DICE_ROOM, ["diceTimeLeft", 
            "TIME UP!", 
            false, 
            true
          ]);
          this._diceTimeUpShown = true;
          
          if (!this._diceTimeout) {
            this._forceEvaluateDice();
          }
        }
        
        if (remaining <= 2 && !this._diceTimeout && !this.diceHasWinner) {
          this._forceEvaluateDice();
        }
        
        if (elapsed > totalTime + 10) {
          this.currentDiceRoll = null;
          this._diceTimeout = null;
          this._isShowingDice = false;
          this._canSubmitDiceAnswer = false;
          this._diceRemainingShown = false;
          this._diceTimeUpShown = false;
        }
      }
    } catch(e) {}
  }

  async _diceAutoTask() {
    try {
      await this._checkDiceAutoStatus();
      await this._checkAndRestartDice();
      
      const timeInfo = this._getTimeLeftUntilNextDiceEvent();
      
      if (!timeInfo.isRunning && !this.diceEndNotified) {
        if (!this._diceOutOfTimeShown) {
          const timeLeft = this._getTimeLeftUntilNextDice();
          this._broadcastToRoom(DICE_ROOM, ["diceTimeLeft", 
            `Next dice game in: ${timeLeft.text}`, 
            true, 
            false
          ]);
          this._diceOutOfTimeShown = true;
        }
        this._sendDiceEndNotificationOnce();
        this._broadcastDiceTimeLeft();
      } else if (timeInfo.isRunning) {
        this._diceOutOfTimeShown = false;
      }
    } catch(e) {}
  }

  _diceTimerTask() {
    try {
      if (this._isDiceTime()) {
        if (!this.currentDiceRoll && !this._diceTimeout && 
            !this.isDiceWaiting && !this._isShowingDice) {
          this._showDiceQuestion();
        }
      }
    } catch(e) {}
  }

  // ==================== DICE TIME FUNCTIONS ====================

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

  // ==================== RECORDING WINNERS (LOWCARD) ====================

  async _initRecordingStatusFromKV() {
    try {
      if (!this.env?.QUESTIONS) return;
    } catch(e) {}
  }

  async _getRecordingStatusFromKV(roomName) {
    try {
      if (!roomName) return false;
      
      if (this.env?.QUESTIONS) {
        try {
          const kvValue = await this.env.QUESTIONS.get(
            CONSTANTS.LOWCARD_RECORDING_KEY + roomName
          );
          const isRecording = kvValue === 'true';
          this._recordingEnabled.set(roomName, isRecording);
          return isRecording;
        } catch(e) {
          return this._recordingEnabled.get(roomName) || false;
        }
      }
      return this._recordingEnabled.get(roomName) || false;
    } catch(e) {
      return false;
    }
  }

  async _startRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      
      this._recordingEnabled.set(roomName, true);
      
      if (this.env?.QUESTIONS) {
        await this.env.QUESTIONS.put(
          CONSTANTS.LOWCARD_RECORDING_KEY + roomName, 
          'true'
        );
      }
      
      this._broadcastToRoom(roomName, ["recordingStatus", true]);
      this._broadcastToRoom(roomName, ["systemMessage", "📢 Recording ENABLED for this room!"]);
      
      return true;
    } catch(e) {
      return false;
    }
  }

  async _stopRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      
      const room = roomName.trim();
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

  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username) return false;
      
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled) {
        return false;
      }
      
      if (room === QUIZ_ROOM) {
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

  async _sendWinnersToRoom(room) {
    try {
      if (!room) return;
      
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled) {
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
          winners: {},
          room: room,
          recording: false,
          updatedAt: new Date().toISOString(),
          type: 'sendWinnersToRoom',
          message: "Recording disabled for this room"
        }]);
        return;
      }
      
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      const winners = await this.env.QUESTIONS.get(key, 'json') || {};
      
      if (Object.keys(winners).length === 0) {
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
          winners: {},
          room: room,
          recording: true,
          updatedAt: new Date().toISOString(),
          type: 'sendWinnersToRoom',
          message: "No winners yet"
        }]);
        return;
      }
      
      this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
        winners: winners,
        room: room,
        recording: true,
        updatedAt: new Date().toISOString(),
        type: 'sendWinnersToRoom'
      }]);
      
    } catch(e) {}
  }

  async _getLowCardWinners(room) {
    try {
      if (!room) return {};
      if (!this.env?.QUESTIONS) return {};
      
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      const winners = await this.env.QUESTIONS.get(key, 'json');
      
      if (winners && typeof winners === 'object') {
        return winners;
      }
      
      return {};
    } catch(e) {
      return {};
    }
  }

  // ==================== GET RECORDING STATUS ====================

  async getRecordingStatus(ws, roomName) {
    try {
      if (!roomName || roomName.trim() === "") {
        this._safeSend(ws, ["recordingStatus", false]);
        return;
      }

      const isRecordingEnabled = await this._getRecordingStatusFromKV(roomName);
      
      this._safeSend(ws, ["recordingStatus", isRecordingEnabled]);

    } catch(e) {
      this._safeSend(ws, ["recordingStatus", false]);
    }
  }

  // ==================== START GAME WITH RECORDING (ADMIN) ====================

  async _startGameWithRecording(ws, room, bet, username) {
    try {
      if (!room || !username) {
        this._safeSend(ws, ["gameLowCardError", "Room and username required"]);
        return;
      }
      
      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
        this._safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
        return;
      }
      
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled) {
        this._safeSend(ws, ["gameLowCardError", "Recording must be enabled first! Use startRecordingWinners"]);
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
      
      const now = Date.now();
      const lockTime = this._gameLocks.get(room);
      if (lockTime && (now - lockTime) < CONSTANTS.START_LOCK_DURATION_MS) {
        this._safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]);
        return;
      }
      this._gameLocks.set(room, now);
      
      try {
        const game = {
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
          _startedBy: 'admin'
        };
        
        game.players.set(username, { id: username, name: username });
        const wsId = this._getWsId(ws);
        if (wsId) {
          game.playerWsId.set(username, wsId);
          this._addClient(room, ws, username, false);
        }
        
        this.activeGames.set(room, game);
        
        this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
        this._broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount]);
        this._broadcastToRoom(room, ["recordingGameStarted", {
          room: room,
          startedBy: 'admin',
          host: username,
          bet: betAmount,
          timestamp: Date.now()
        }]);
        
        this._broadcastToRoom(room, ["systemMessage", `🎮 ADMIN started a RECORDING game! Bet: ${betAmount} coins`]);
        
        this._safeSend(ws, ["gameStartWithRecordingSuccess", {
          room: room,
          bet: betAmount,
          host: username,
          message: "Game started with recording"
        }]);
        
        this._startRegistration(room, game);
        
      } catch(e) {
        this._deleteGame(room, this.activeGames.get(room));
        this._safeSend(ws, ["gameLowCardError", "Failed to start game: " + e.message]);
        this._gameLocks.delete(room);
      }
      
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to start game with recording"]);
    }
  }

  // ==================== DICE GAME METHODS ====================

  async _handleDiceWinner(username, diceValue) {
    try {
      if (this._winnerProcessed) return;
      
      this._winnerProcessed = true;
      
      const points = await this.diceGameSystem.getPoints();
      points[username] = (points[username] || 0) + 1;
      await this.diceGameSystem.setPoints(points);
      
      this._broadcastDiceNotification("diceWinner", {
        username: username,
        totalPoints: points[username] || 0,
        diceValue: diceValue
      });
      
      this._broadcastDiceResult("diceWinner", {
        username: username,
        totalPoints: points[username] || 0,
        diceValue: diceValue
      });
      
      setTimeout(() => {
        this._winnerProcessed = false;
      }, 5000);
      
    } catch(e) {
      this._winnerProcessed = false;
    }
  }

  async _checkAndResetWeeklyDice() {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      const currentWeek = this.diceGameSystem.generateCurrentWeek();
      const lastResetWeek = await this._getLastResetWeek();
      
      if (lastResetWeek !== currentWeek) {
        const points = await this.diceGameSystem.getPoints();
        
        if (points && Object.keys(points).length > 0) {
          let winner = null;
          let highestScore = 0;
          
          for (const [username, score] of Object.entries(points)) {
            if (score > highestScore) {
              highestScore = score;
              winner = username;
            }
          }
          
          if (winner) {
            const winnerData = {
              username: winner,
              score: highestScore,
              week: lastResetWeek || currentWeek
            };
            await this.diceGameSystem.setLastWeekWinner(winnerData);
            
            this._broadcastToRoom(DICE_ROOM, [
              "diceLastWeekWinner", 
              winner, 
              highestScore, 
              lastResetWeek || currentWeek
            ]);
          }
        }
        
        await this.diceGameSystem.setPoints({});
        await this._setLastResetWeek(currentWeek);
        
        this._broadcastToRoom(DICE_ROOM, [
          "systemMessage", 
          `📊 Weekly Dice Reset! New week: ${currentWeek}`
        ]);
        
        this._broadcastToRoom(DICE_ROOM, [
          "diceReset", 
          {
            week: currentWeek,
            message: "Points reset for new week! Good luck!"
          }
        ]);
        
        return true;
      }
      
      return false;
    } catch(e) {
      return false;
    }
  }

  async _getLastResetWeek() {
    try {
      if (!this.env?.QUESTIONS) return null;
      return await this.env.QUESTIONS.get('dice_last_reset_week', 'json');
    } catch(e) { 
      return null; 
    }
  }

  async _setLastResetWeek(week) {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.put('dice_last_reset_week', JSON.stringify(week));
      return true;
    } catch(e) { 
      return false; 
    }
  }

  _sendDiceEndNotificationOnce() {
    try {
      if (this.diceEndNotified) return;
      const timeLeft = this._getTimeLeftUntilNextDice();
      const message = `${timeLeft.text}`;
      this._broadcastToRoom(DICE_ROOM, ["diceEnded", { 
        timeLeft: timeLeft.text, 
        status: "ended"
      }]);
      this._broadcastToRoom(DICE_ROOM, ["diceTimeLeft", message, true, false]);
      this._broadcastDiceNotification("diceEnded", { 
        timeLeft: timeLeft.text
      });
      this.diceEndNotified = true;
    } catch(e) {}
  }

  _sendDiceNotification(ws, type, data) {
    try {
      if (!ws || ws.readyState !== 1) return;
      const remaining = this._getDiceQuestionRemainingTime();
      const remainingText = `${remaining}s remaining`;
      const notification = {
        type: type,
        timestamp: Date.now(),
        remainingTime: remainingText,
        diceValue: this.currentDiceRoll?.value || null,
        data: data || {}
      };
      this._safeSend(ws, ["diceNotification", notification]);
    } catch(e) {}
  }

  _broadcastDiceNotification(type, data) {
    try {
      const wsIds = this.wsClients.get(DICE_ROOM);
      if (!wsIds?.size) return;
      const remaining = this._getDiceQuestionRemainingTime();
      const remainingText = `${remaining}s remaining`;
      const notification = {
        type: type,
        timestamp: Date.now(),
        remainingTime: remainingText,
        diceValue: this.currentDiceRoll?.value || null,
        data: data || {}
      };
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", notification]);
    } catch(e) {}
  }

  async _initAsync() {
    try {
      if (this._initializing) return;
      if (this._initialized && !this._isRecovering) return;
      this._initializing = true;
      
      await this.diceGameSystem.loadScores();
      await this._initDice();
      
      this._startWeeklyResetChecker();
      
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

  _incrementSubRequest() {
    try {
      this._subRequestCount++;
      this._requestCount++;
    } catch(e) {}
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
          if (!this._diceStartTimeout && !this._isShowingDice) {
            this.forceStartDice();
          }
        } else if (!this.currentDiceRoll && !this._diceTimeout && !this.isDiceWaiting && !this._diceStartTimeout && !this._isShowingDice) {
          await this._showDiceQuestion();
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
      if (this._isShowingDice) return false;
      if (!this._isDiceTime() || this.currentDiceRoll || this._diceTimeout || this.isDiceWaiting || this._diceStartTimeout) {
        return false;
      }
      this.diceAutoEnabled = true;
      this._showDiceQuestion();
      return true;
    } catch(e) { return false; }
  }

  _checkAndRestartDice() {
    try {
      if (!this._isDiceTime()) return;
      if (!this.currentDiceRoll && !this.isDiceWaiting && !this._diceTimeout && !this._diceBreakTimeout && !this._isShowingDice) {
        this.diceAutoEnabled = true;
        this._showDiceQuestion();
      }
    } catch(e) {}
  }

  ensureDiceRunning() {
    try {
      if (this._isShowingDice) return;
      this._forceStartDiceIfTime();
      if (!this.currentDiceRoll && !this._diceTimeout && !this.isDiceWaiting && !this._diceStartTimeout && !this._isShowingDice) {
        this.forceStartDice();
      }
      if (!this._diceKeepAliveInterval) {
        this._startDiceKeepAlive();
      }
    } catch(e) {}
  }

  _forceStartDiceIfTime() {
    try {
      if (this._isShowingDice) return;
      if (!this._isDiceTime() || this.currentDiceRoll || this._diceTimeout || this.isDiceWaiting || this._diceStartTimeout) {
        return;
      }
      this.diceAutoEnabled = true;
      this._showDiceQuestion();
    } catch(e) {}
  }

  // ==================== _showDiceQuestion ====================
  async _showDiceQuestion() {
    try {
      await this._checkAndResetWeeklyDice();

      if (this._isShowingDice) return;
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
        if (clients?.size > 0) this._broadcastToRoom(DICE_ROOM, ["diceTimeLeft", "Dice game is starting soon!", true, true]);
        return;
      }
      
      if (this.isDestroyed || this.isDiceWaiting || this._diceStartTimeout || this.currentDiceRoll) return;
      
      this._isShowingDice = true;
      
      try {
        const diceValue = this.diceGameSystem.rollDice();
        const diceEmoji = this.diceGameSystem.getDiceEmoji(diceValue);
        
        this.currentDiceRoll = {
          value: diceValue,
          emoji: diceEmoji,
          timestamp: Date.now()
        };
        this._diceStartTime = Date.now();
        this._diceQuestionStartTime = Date.now();
        this._canSubmitDiceAnswer = false;
        this.diceAnswered = new Set();
        this.diceHasWinner = false;
        this.diceWinner = null;
        this._winnerProcessed = false;
        this._diceRemainingShown = false;
        this._diceTimeUpShown = false;
        
        await this._broadcastDiceRoll(diceValue, diceEmoji);
        
        this._broadcastDiceNotification("diceRolled", {
          value: diceValue,
          emoji: diceEmoji,
          readingTime: CONSTANTS.DICE_READING_TIME_MS / 1000
        });
        
        setTimeout(() => {
          if (this.closing || this.isDestroyed) { 
            this._isShowingDice = false;
            return; 
          }
          
          this._canSubmitDiceAnswer = true;
          
          this._broadcastDiceNotification("diceCanAnswer", {
            answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000,
            remainingTime: `${CONSTANTS.DICE_ANSWER_TIME_MS / 1000}s remaining`,
            message: "You can now guess the dice value!"
          });
          
          this._broadcastToRoom(DICE_ROOM, [
            "diceTimeLeft", 
            "20s remaining", 
            false,
            true
          ]);
          
        }, CONSTANTS.DICE_READING_TIME_MS);
        
        if (this._diceTimeout) clearTimeout(this._diceTimeout);
        if (this._diceBreakTimeout) clearTimeout(this._diceBreakTimeout);
        
        this._diceTimeout = setTimeout(async () => {
          try {
            if (this.closing || this.isDestroyed) { 
              this._diceTimeout = null; 
              this._isShowingDice = false;
              this._canSubmitDiceAnswer = false;
              return; 
            }
            
            const currentClients = this.wsClients.get(DICE_ROOM);
            if (!currentClients?.size) { 
              this._diceTimeout = null; 
              this.currentDiceRoll = null;
              this._isShowingDice = false;
              this._canSubmitDiceAnswer = false;
              return; 
            }
            
            const diceValue = this.currentDiceRoll?.value;
            
            if (this.diceHasWinner && this.diceWinner) {
              await this._handleDiceWinner(this.diceWinner, diceValue);
            } else {
              this._broadcastDiceNotification("diceNoWinner", `No winner this round! The value was: ${diceValue} ${this.diceGameSystem.getDiceEmoji(diceValue)}`);
            }
            
            this._diceTimeout = null;
            this.isDiceWaiting = true;
            this._isShowingDice = false;
            this._canSubmitDiceAnswer = false;
            
            this._diceBreakTimeout = setTimeout(() => {
              if (this.closing || this.isDestroyed) { 
                this._diceBreakTimeout = null; 
                return; 
              }
              this.isDiceWaiting = false;
              this._diceBreakTimeout = null;
              this.currentDiceRoll = null;
            }, CONSTANTS.DICE_BREAK_MS);
            
          } catch(e) {
            this._diceTimeout = null;
            this.currentDiceRoll = null;
            this.isDiceWaiting = false;
            this._isShowingDice = false;
            this._canSubmitDiceAnswer = false;
          }
        }, CONSTANTS.DICE_TOTAL_TIME_MS);
        
      } catch(e) {
        this._isShowingDice = false;
        this.currentDiceRoll = null;
        this.isDiceWaiting = false;
        this._diceTimeout = null;
        this._canSubmitDiceAnswer = false;
      }
    } catch(e) {
      this._isShowingDice = false;
      this.currentDiceRoll = null;
      this.isDiceWaiting = false;
      this._diceTimeout = null;
      this._canSubmitDiceAnswer = false;
    }
  }

  async _forceEvaluateDice() {
    try {
      if (!this.currentDiceRoll || this._diceTimeout) return;
      
      const currentClients = this.wsClients.get(DICE_ROOM);
      if (!currentClients?.size) { 
        this.currentDiceRoll = null;
        this._isShowingDice = false;
        this._canSubmitDiceAnswer = false;
        return; 
      }
      
      const diceValue = this.currentDiceRoll?.value;
      
      if (this.diceHasWinner && this.diceWinner) {
        await this._handleDiceWinner(this.diceWinner, diceValue);
      } else {
        this._broadcastDiceNotification("diceNoWinner", `No winner this round! The value was: ${diceValue} ${this.diceGameSystem.getDiceEmoji(diceValue)}`);
      }
      
      this.currentDiceRoll = null;
      this.isDiceWaiting = true;
      this._isShowingDice = false;
      this._canSubmitDiceAnswer = false;
      
      this._diceBreakTimeout = setTimeout(() => {
        if (this.closing || this.isDestroyed) { 
          this._diceBreakTimeout = null; 
          return; 
        }
        this.isDiceWaiting = false;
        this._diceBreakTimeout = null;
      }, CONSTANTS.DICE_BREAK_MS);
      
    } catch(e) {
      this.currentDiceRoll = null;
      this.isDiceWaiting = false;
      this._isShowingDice = false;
      this._canSubmitDiceAnswer = false;
    }
  }

  // ==================== submitDiceAnswer ====================
  async submitDiceAnswer(ws, username, guess) {
    try {
      if (!ws || !username) {
        this._sendDiceErrorWithTime(ws, "ERROR", "Invalid request");
        return;
      }
      
      const room = this._ensureRoomConsistency(ws);
      if (room !== DICE_ROOM) {
        this._safeSend(ws, ["diceError", "Dice game only available in Dice room"]);
        return;
      }
      
      if (!this._isDiceTime()) {
        this._sendDiceErrorWithTime(ws, "NOT_DICE_TIME");
        return;
      }
      
      if (!this.diceAutoEnabled) {
        this._sendDiceErrorWithTime(ws, "DICE_DISABLED");
        return;
      }
      
      const clients = this.wsClients.get(DICE_ROOM);
      if (!clients?.size) {
        this._sendDiceErrorWithTime(ws, "ERROR", "Dice game is paused");
        return;
      }
      
      if (!this.currentDiceRoll) {
        this._startDiceIfNeeded();
        if (!this.currentDiceRoll) {
          this._sendDiceErrorWithTime(ws, "DICE_NOT_STARTED");
          return;
        }
      }
      
      if (this.diceAnswered.has(username)) {
        this._safeSend(ws, ["diceError", "You already guessed!"]);
        return;
      }
      
      const isReadingTime = !this._canSubmitDiceAnswer;
      let readingTimeLeft = 0;
      
      if (isReadingTime) {
        const elapsed = (Date.now() - (this._diceQuestionStartTime || 0)) / 1000;
        readingTimeLeft = Math.max(0, Math.round((CONSTANTS.DICE_READING_TIME_MS / 1000) - elapsed));
      }
      
      const guessValue = parseInt(guess, 10);
      const isValidGuess = !isNaN(guessValue) && guessValue >= 1 && guessValue <= 6;
      const wsId = this._getWsId(ws);
      
      const hasWinner = this.diceHasWinner && this.diceWinner;
      const diceValue = this.currentDiceRoll?.value;
      
      if (!isReadingTime && this._getDiceAnswerRemainingTime() <= 0) {
        this._broadcastDiceNotification("diceAnswerLate", {
          username: username,
          guess: guessValue || "?",
          isCorrect: false,
          remainingTime: "0s (Time's up!)",
          message: "⏰ Time's up! Guess seen but not counted.",
          hasWinner: hasWinner,
          winner: hasWinner ? this.diceWinner : null
        });
        
        this.diceAnswered.add(username);
        
        if (hasWinner) {
          this._safeSend(ws, ["diceError", `Time's up! Winner: ${this.diceWinner}`]);
        } else {
          this._safeSend(ws, ["diceError", "Time's up!"]);
        }
        return;
      }
      
      if (isReadingTime) {
        this._broadcastDiceNotification("diceAnswerReading", {
          username: username,
          guess: guessValue || "?",
          remainingTime: `${readingTimeLeft}s reading time left`,
          status: "reading",
          message: hasWinner ? `⏳ Guess seen, but already have winner: ${this.diceWinner}` : "⏳ Guess during reading NOT counted. Resubmit during answer time!",
          hasWinner: hasWinner,
          winner: hasWinner ? this.diceWinner : null
        });
        
        this._safeSend(ws, ["diceAnswerReading", {
          username: username,
          guess: guessValue || "?",
          readingTimeLeft: readingTimeLeft,
          message: hasWinner ? 
            `⏳ "${guessValue}" seen! But already have winner: ${this.diceWinner}` :
            `⏳ "${guessValue}" NOT counted! Resubmit during answer time (${CONSTANTS.DICE_ANSWER_TIME_MS / 1000}s).`,
          status: "waiting",
          notCounted: true
        }]);
        
        this.diceAnswered.add(username);
        return;
      }
      
      const isCorrect = isValidGuess && guessValue === diceValue;
      const answerRemaining = this._getDiceAnswerRemainingTime();
      const remainingText = `${answerRemaining}s remaining`;
      
      if (hasWinner) {
        this._broadcastDiceNotification("diceAnswer", {
          username: username,
          guess: guessValue || "?",
          isCorrect: isCorrect,
          remainingTime: remainingText,
          gotPoint: false,
          hasWinner: true,
          winner: this.diceWinner,
          message: `⚠️ Guess seen, but already have winner: ${this.diceWinner}`
        });
        
        this._broadcastDiceResult("diceAnswer", {
          username: username,
          guess: guessValue || "?"
        });
        
        this.diceAnswered.add(username);
        return;
      }
      
      if (isCorrect && !this.diceHasWinner) {
        this.diceHasWinner = true;
        this.diceWinner = username;
        
        const points = await this.diceGameSystem.getPoints();
        points[username] = (points[username] || 0) + 1;
        await this.diceGameSystem.setPoints(points);
        
        this._broadcastDiceNotification("diceWinner", {
          username: username,
          totalPoints: points[username] || 0,
          diceValue: diceValue
        });
      }
      
      this.diceAnswered.add(username);
      
      this._broadcastDiceNotification("diceAnswer", {
        username: username,
        guess: guessValue || "?",
        isCorrect: isCorrect,
        remainingTime: remainingText,
        gotPoint: isCorrect,
        diceValue: diceValue
      });
      
      this._broadcastDiceResult("diceAnswer", {
        username: username,
        guess: guessValue || "?"
      });
      
    } catch(e) {
      this._safeSend(ws, ["diceError", e.message]);
    }
  }

  _startDiceLoop() {
    // Dice loop already handled by scheduler
  }

  async resetDice() {
    try {
      if (this._diceTimeout) clearTimeout(this._diceTimeout);
      if (this._diceBreakTimeout) clearTimeout(this._diceBreakTimeout);
      if (this._diceStartTimeout) clearTimeout(this._diceStartTimeout);
      if (this._diceKeepAliveInterval) clearInterval(this._diceKeepAliveInterval);
      this.currentDiceRoll = null;
      this.isDiceWaiting = false;
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
      
      this._diceTimeLeftNotified.clear();
      this._nextDiceNotified.clear();
      this._diceJoinedNotified.clear();
    } catch(e) {}
  }

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
          if (!this.currentDiceRoll && this.diceAutoEnabled && !this._isShowingDice) {
            this.forceStartDice();
          }
        } catch(e) {}
      }, delayMs);
    } catch(e) {}
  }

  _startDiceIfNeeded() {
    try {
      const clients = this.wsClients.get(DICE_ROOM);
      if (!clients?.size) return;
      if (!this.currentDiceRoll && !this._diceTimeout && !this.isDiceWaiting && !this._diceStartTimeout && !this._isShowingDice) {
        this._showDiceQuestion();
      }
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

  _startDiceKeepAlive() {
    // Keep alive already handled by scheduler
  }

  _clearDiceData() {
    try {
      this.currentDiceRoll = null;
      this._diceStartTime = null;
      this.diceAnswered = new Set();
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.isDiceWaiting = false;
      this._isShowingDice = false;
      this._winnerProcessed = false;
      this._canSubmitDiceAnswer = false;
      this._diceQuestionStartTime = null;
      this._diceOutOfTimeShown = false;
      this._diceRemainingShown = false;
      this._diceTimeUpShown = false;
      
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
      
      this._broadcastToRoom(DICE_ROOM, ["diceClear", {
        message: "Dice game has ended. Come back tomorrow!",
        timestamp: Date.now()
      }]);
      this._broadcastDiceNotification("diceCleared", {
        message: "Dice game has ended. Come back tomorrow!",
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
      const timeInfo = this._getTimeLeftUntilNextDiceEvent();
      const timeLeft = this._getTimeLeftUntilNextDice();
      let message = "", canType = true, isDiceTime = timeInfo.isRunning;
      if (isDiceTime) {
        if (this.currentDiceRoll && this._diceStartTime) {
          const elapsed = (Date.now() - this._diceStartTime) / 1000;
          const left = Math.max(0, (CONSTANTS.DICE_TOTAL_TIME_MS / 1000) - elapsed);
          const minutes = Math.floor(left / 60), seconds = Math.floor(left % 60);
          message = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
          canType = false;
        } else {
          message = `Dice game is starting soon!`;
          canType = true;
        }
      } else {
        message = `${timeLeft.text}`;
        canType = true;
      }
      
      this._broadcastToRoom(DICE_ROOM, ["diceTimeLeft", message, canType, isDiceTime]);
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
      if (this._nextDiceNotified.has(wsId)) {
        return false;
      }
      
      const timeInfo = this._getTimeLeftUntilNextDiceEvent();
      const timeLeft = this._getTimeLeftUntilNextDice();
      let message = "", canType = true, isDiceTime = timeInfo.isRunning;
      
      if (isDiceTime) {
        if (this.currentDiceRoll && this._diceStartTime) {
          const elapsed = (Date.now() - this._diceStartTime) / 1000;
          const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
          const remaining = Math.max(0, totalTime - elapsed);
          const remainingInt = Math.floor(remaining);
          const minutes = Math.floor(remainingInt / 60);
          const seconds = remainingInt % 60;
          const timeText = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
          
          let displayTime = "";
          if (remainingInt >= 20) {
            displayTime = "20s remaining";
          } else if (remainingInt >= 10) {
            displayTime = "10s remaining";
          } else if (remainingInt >= 5) {
            displayTime = "5s remaining";
          } else if (remainingInt > 0) {
            displayTime = `${timeText} remaining`;
          } else {
            displayTime = "TIME UP!";
          }
          
          message = displayTime;
          canType = false;
        } else {
          message = `Dice game is starting soon!`;
          canType = true;
        }
      } else {
        if (!this._diceJoinedNotified.has(wsId)) {
          message = `Next dice game in: ${timeLeft.text}`;
          this._diceJoinedNotified.set(wsId, true);
        } else {
          return false;
        }
        canType = true;
      }
      
      this._safeSend(ws, ["diceTimeLeft", message, canType, isDiceTime]);
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
      this._safeSend(ws, ["diceError", message]);
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
      const readingEnd = this._diceQuestionStartTime + CONSTANTS.DICE_READING_TIME_MS;
      const now = Date.now();
      if (now < readingEnd) return 0;
      const elapsed = (now - readingEnd) / 1000;
      return Math.max(0, Math.round((CONSTANTS.DICE_ANSWER_TIME_MS / 1000) - elapsed));
    } catch(e) { return 0; }
  }

  // ==================== OPTIMIZED BROADCAST DICE ROLL ====================
  async _broadcastDiceRoll(diceValue, diceEmoji) {
    try {
      const wsIds = this.wsClients.get(DICE_ROOM);
      if (!wsIds?.size) return;

      const msgData = {
        value: diceValue,
        emoji: diceEmoji,
        timestamp: Date.now(),
        readingTime: CONSTANTS.DICE_READING_TIME_MS / 1000,
        answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000
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

  // ==================== LOWCARD GAME METHODS ====================

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
      let room = this.clientRooms.get(wsId);
      if (!room) room = ws.room || ws.roomname || null;
      if (!room) {
        if (ws.username) {
          const conn = this.userConnections.get(ws.username);
          if (conn) room = conn.room;
        }
      }
      if (!room) return null;
      ws.room = room;
      ws.roomname = room;
      if (!this.wsClients.has(room)) this.wsClients.set(room, new Set());
      if (!this.wsClients.get(room).has(wsId)) {
        this.wsClients.get(room).add(wsId);
        this.clientRooms.set(wsId, room);
        this.wsMap.set(wsId, ws);
      }
      return room;
    } catch(e) { return null; }
  }

  _addClient(room, ws, username = null, isNewConnection = false) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      if (!wsId) { this._safeSend(ws, ["gameLowCardError", "Connection error"]); return; }
      if (this.clientRooms.has(wsId)) {
        const oldRoom = this.clientRooms.get(wsId);
        if (oldRoom && oldRoom !== room) this._removeClientFromRoom(oldRoom, wsId);
      }
      if (username && isNewConnection) {
        this.userConnections.set(username, { wsId, ws, room, timestamp: Date.now() });
      } else if (username) {
        const conn = this.userConnections.get(username);
        if (conn) { conn.room = room; conn.timestamp = Date.now(); conn.ws = ws; }
        else { this.userConnections.set(username, { wsId, ws, room, timestamp: Date.now() }); }
      }
      if (this.clientRooms.has(wsId)) {
        const oldRoom = this.clientRooms.get(wsId);
        if (oldRoom !== room) this._removeClientFromRoom(oldRoom, wsId);
      }
      if (!this.wsClients.has(room)) this.wsClients.set(room, new Set());
      this.wsClients.get(room).add(wsId);
      this.clientRooms.set(wsId, room);
      this.wsMap.set(wsId, ws);
      ws.room = room;
      ws.roomname = room;
      ws.username = username;
      if (username) {
        if (!this.roomViewers.has(room)) this.roomViewers.set(room, new Set());
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
        if (clients.size === 0) this.wsClients.delete(room);
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
      if (this.isDestroyed) { this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]); return; }
      if (!room || room.trim() === "") { this._safeSend(ws, ["gameLowCardError", "Invalid room name"]); return; }
      const roomName = room.trim();
      const wsId = this._getWsId(ws);
      if (!wsId) { this._safeSend(ws, ["gameLowCardError", "Connection error"]); return; }
      const lockKey = `switch_${wsId}`;
      if (this._switchLocks.has(lockKey)) { this._safeSend(ws, ["gameLowCardError", "Switch in progress"]); return; }
      this._switchLocks.set(lockKey, Date.now());
      try {
        const oldRoom = this.clientRooms.get(wsId);
        if (oldRoom === roomName) {
          ws.room = roomName;
          ws.roomname = roomName;
          if (roomName === DICE_ROOM) {
            this._diceTimeLeftNotified.delete(wsId);
            this._nextDiceNotified.delete(wsId);
            this._diceJoinedNotified.delete(wsId);
            
            if (this._isDiceTime()) {
              if (!this.diceAutoEnabled) this.diceAutoEnabled = true;
              this.forceStartDice();
              
              if (this.currentDiceRoll) {
                const elapsed = (Date.now() - this._diceStartTime) / 1000;
                const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
                const remaining = Math.max(0, totalTime - elapsed);
                const remainingInt = Math.floor(remaining);
                
                let displayTime = "";
                if (remainingInt >= 20) {
                  displayTime = "20s remaining";
                } else if (remainingInt >= 10) {
                  displayTime = "10s remaining";
                } else if (remainingInt >= 5) {
                  displayTime = "5s remaining";
                } else if (remainingInt > 0) {
                  const minutes = Math.floor(remainingInt / 60);
                  const seconds = remainingInt % 60;
                  displayTime = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s remaining`;
                } else {
                  displayTime = "TIME UP!";
                }
                
                this._safeSend(ws, ["diceTimeLeft", 
                  displayTime, 
                  false, 
                  true
                ]);
                
                this._safeSend(ws, ["diceRoll", {
                  value: this.currentDiceRoll.value,
                  emoji: this.currentDiceRoll.emoji,
                  timestamp: this.currentDiceRoll.timestamp,
                  readingTime: CONSTANTS.DICE_READING_TIME_MS / 1000,
                  answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000
                }]);
                
                if (this._canSubmitDiceAnswer) {
                  this._safeSend(ws, ["diceCanAnswer", {
                    answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000,
                    remainingTime: `${this._getDiceAnswerRemainingTime()}s remaining`,
                    message: "You can now guess the dice value!"
                  }]);
                }
                
              } else {
                this._safeSend(ws, ["diceTimeLeft", "Dice game is starting soon!", true, true]);
              }
              
            } else {
              if (!this._diceJoinedNotified.has(wsId)) {
                const timeLeft = this._getTimeLeftUntilNextDice();
                this._safeSend(ws, ["diceTimeLeft", 
                  `Next dice game in: ${timeLeft.text}`, 
                  true, 
                  false
                ]);
                this._diceJoinedNotified.set(wsId, true);
              }
            }
            
            setTimeout(() => {
              if (!this.closing && !this.isDestroyed) {
                this._sendDiceTimeLeftToUser(ws);
                this._sendDiceNotification(ws, "diceStatus", {
                  isDiceTime: this._isDiceTime(),
                  isActive: !!this.currentDiceRoll,
                  remainingTime: `${this._getDiceQuestionRemainingTime()}s remaining`,
                  hasWinner: this.diceHasWinner,
                  winner: this.diceWinner,
                  diceValue: this.currentDiceRoll?.value || null,
                  canSubmit: this._canSubmitDiceAnswer
                });
              }
            }, CONSTANTS.QUIZ_SWITCH_DELAY_MS);
          }
          this._safeSend(ws, ["switchRoomSuccess", roomName]);
          return;
        }
        if (oldRoom) this._removeClientFromRoom(oldRoom, wsId);
        this._addClient(roomName, ws, username, false);
        ws.room = roomName;
        ws.roomname = roomName;
        ws.username = username;
        if (username) {
          let conn = this.userConnections.get(username);
          if (conn) { conn.room = roomName; conn.wsId = wsId; conn.ws = ws; conn.timestamp = Date.now(); }
          else { this.userConnections.set(username, { wsId, ws, room: roomName, timestamp: Date.now() }); }
        }
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        if (roomName === DICE_ROOM) {
          this._diceTimeLeftNotified.delete(wsId);
          this._nextDiceNotified.delete(wsId);
          this._diceJoinedNotified.delete(wsId);
          
          if (this._isDiceTime()) {
            if (!this.diceAutoEnabled) this.diceAutoEnabled = true;
            this.forceStartDice();
            
            if (this.currentDiceRoll) {
              const elapsed = (Date.now() - this._diceStartTime) / 1000;
              const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
              const remaining = Math.max(0, totalTime - elapsed);
              const remainingInt = Math.floor(remaining);
              
              let displayTime = "";
              if (remainingInt >= 20) {
                displayTime = "20s remaining";
              } else if (remainingInt >= 10) {
                displayTime = "10s remaining";
              } else if (remainingInt >= 5) {
                displayTime = "5s remaining";
              } else if (remainingInt > 0) {
                const minutes = Math.floor(remainingInt / 60);
                const seconds = remainingInt % 60;
                displayTime = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s remaining`;
              } else {
                displayTime = "TIME UP!";
              }
              
              this._safeSend(ws, ["diceTimeLeft", 
                displayTime, 
                false, 
                true
              ]);
              
              this._safeSend(ws, ["diceRoll", {
                value: this.currentDiceRoll.value,
                emoji: this.currentDiceRoll.emoji,
                timestamp: this.currentDiceRoll.timestamp,
                readingTime: CONSTANTS.DICE_READING_TIME_MS / 1000,
                answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000
              }]);
              
              if (this._canSubmitDiceAnswer) {
                this._safeSend(ws, ["diceCanAnswer", {
                  answerTime: CONSTANTS.DICE_ANSWER_TIME_MS / 1000,
                  remainingTime: `${this._getDiceAnswerRemainingTime()}s remaining`,
                  message: "You can now guess the dice value!"
                }]);
              }
              
            } else {
              this._safeSend(ws, ["diceTimeLeft", "Dice game is starting soon!", true, true]);
            }
            
          } else {
            if (!this._diceJoinedNotified.has(wsId)) {
              const timeLeft = this._getTimeLeftUntilNextDice();
              this._safeSend(ws, ["diceTimeLeft", 
                `Next dice game in: ${timeLeft.text}`, 
                true, 
                false
              ]);
              this._diceJoinedNotified.set(wsId, true);
            }
          }
          
          setTimeout(() => {
            if (!this.closing && !this.isDestroyed) {
              this._sendDiceTimeLeftToUser(ws);
              this._sendDiceNotification(ws, "diceStatus", {
                isDiceTime: this._isDiceTime(),
                isActive: !!this.currentDiceRoll,
                remainingTime: `${this._getDiceQuestionRemainingTime()}s remaining`,
                hasWinner: this.diceHasWinner,
                winner: this.diceWinner,
                diceValue: this.currentDiceRoll?.value || null,
                canSubmit: this._canSubmitDiceAnswer
              });
            }
          }, CONSTANTS.QUIZ_SWITCH_DELAY_MS);
        }
        this._broadcastToRoom(roomName, ["userJoinedRoom", username, roomName]);
        if (oldRoom) this._broadcastToRoom(oldRoom, ["userLeftRoom", username, oldRoom]);
      } finally {
        this._switchLocks.delete(lockKey);
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Switch failed"]);
    }
  }

  // ==================== OPTIMIZED BROADCAST TO ROOM ====================
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
            recording: true,
            updatedAt: new Date().toISOString(),
            type: 'winnerUpdate'
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
        this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
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
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
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
                recording: true,
                updatedAt: new Date().toISOString(),
                type: 'winnerUpdate'
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
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
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
      this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
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
      
      if (entries.length === 1 && eliminated.size === activeIds.length - 1) {
        const winnerId = entries[0][0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        
        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winnerName);
          const allWinners = await this._getLowCardWinners(room);
          this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
            winners: allWinners,
            room: room,
            recording: true,
            updatedAt: new Date().toISOString(),
            type: 'winnerUpdate'
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
        if (this._isGameActuallyRunning(game) && !game._gameEnded) this._startDrawPhase(room, game);
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
            recording: true,
            updatedAt: new Date().toISOString(),
            type: 'winnerUpdate'
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
      if (this._isGameActuallyRunning(game) && !game._gameEnded) this._startDrawPhase(room, game);
    } catch(e) {
      if (game) { 
        game._isEvaluating = false; 
        if (game._safetyTimer) { 
          clearTimeout(game._safetyTimer); 
          game._safetyTimer = null; 
        } 
      }
      this._scheduleGameCleanup(room, game);
    }
  }

  // ==================== START GAME (USER) ====================

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
      const room = this._ensureRoomConsistency(ws);
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]);
        return;
      }
      if (room === QUIZ_ROOM) {
        this._safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]);
        return;
      }

      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (isRecordingEnabled) {
        this._safeSend(ws, ["gameLowCardError", 
          "Recording is ACTIVE in this room! Users cannot start games."
        ]);
        return;
      }

      const startKey = `start_${room}`;
      if (this._gameStartFlags.has(startKey)) {
        this._safeSend(ws, ["gameLowCardError", "Game is already starting..."]);
        return;
      }
      
      const existingGame = this.activeGames.get(room);
      if (existingGame?._isActive && !existingGame._gameEnded) {
        this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
        return;
      }
      
      this._gameStartFlags.set(startKey, Date.now());
      
      if (existingGame) {
        await this._forceCleanupGame(room, existingGame);
      }
      
      const now = Date.now();
      const lockTime = this._gameLocks.get(room);
      if (lockTime && (now - lockTime) < CONSTANTS.START_LOCK_DURATION_MS) {
        this._safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]);
        this._gameStartFlags.delete(startKey);
        return;
      }
      this._gameLocks.set(room, now);
      
      try {
        if (this.activeGames.size >= this._maxGames) {
          this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
          this._gameLocks.delete(room);
          this._gameStartFlags.delete(startKey);
          return;
        }
        
        const betAmount = parseInt(bet, 10) || 0;
        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          this._safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
          this._gameLocks.delete(room);
          this._gameStartFlags.delete(startKey);
          return;
        }
        
        const wsId = this._getWsId(ws);
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
        this._startRegistration(room, game);
        
        setTimeout(() => {
          try {
            this._gameStartFlags.delete(startKey);
            if (this._gameLocks.get(room) === now) this._gameLocks.delete(room);
          } catch(e) {}
        }, CONSTANTS.START_LOCK_DURATION_MS + 1000);
        
      } catch(e) {
        this._deleteGame(room, this.activeGames.get(room));
        this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
        this._gameLocks.delete(room);
        this._gameStartFlags.delete(startKey);
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
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

  // ==================== JOIN GAME ====================

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
      const wsId = this._getWsId(ws);
      const room = this._ensureRoomConsistency(ws);
      if (!room) { 
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]); 
        return; 
      }
      const lockKey = `join_${room}_${usernameClean}`;
      if (this._joinLocks.has(lockKey)) { 
        this._safeSend(ws, ["gameLowCardError", "Join in progress, please wait"]); 
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
          const finalWsId = this._ensureSingleConnection(room, usernameClean, ws, wsId);
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
          this._safeSend(ws, ["gameLowCardError", "Game is full"]);
          return;
        }
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        this._addClient(room, ws, usernameClean, false);
        game.playerWsId.set(usernameClean, wsId);
        this._broadcastToRoom(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
      } finally {
        this._joinLocks.delete(lockKey);
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to join game"]);
    }
  }

  // ==================== SUBMIT NUMBER ====================

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
      const wsId = this._getWsId(ws);
      const room = this._ensureRoomConsistency(ws);
      if (!room) { 
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]); 
        return; 
      }
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      if (game.players.has(usernameClean)) {
        if (game.eliminated?.has(usernameClean)) {
          this._safeSend(ws, ["gameLowCardError", "You have been eliminated from this game"]);
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
        this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
        game._evalTimer = setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
              this._evaluateRound(room, game);
            }
          } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS);
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to submit number"]);
    }
  }

  // ==================== LEAVE GAME ====================

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
      const room = this._ensureRoomConsistency(ws);
      if (!room) { 
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]); 
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
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to leave game"]);
    }
  }

  // ==================== CHECK GAME RUNNING ====================

  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) { 
        this._safeSend(ws, ["gameStatus", { running: "false" }]); 
        return; 
      }
      let room = roomname;
      if (!room) room = this._ensureRoomConsistency(ws);
      if (!room) { 
        this._safeSend(ws, ["gameStatus", { running: "false" }]); 
        return; 
      }
      const game = this.activeGames.get(room);
      const isRunning = game?._isActive && !game._gameEnded && game.players?.size > 0;
      this._safeSend(ws, ["gameStatus", { running: isRunning ? "true" : "false" }]);
    } catch(e) {
      this._safeSend(ws, ["gameStatus", { running: "false" }]);
    }
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

  // ==================== HANDLE EVENT ====================

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      this._eventQueue.push({ ws, data });
      if (!this._isProcessingQueue) {
        await this._safeExecute(async () => {
          await this._processEventQueue();
        });
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Server is recovering, please try again"]);
    }
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
      const wsId = this._getWsId(ws);
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

      // ==================== RECORDING EVENTS ====================
      if (evt === "startRecordingWinners") {
        const roomName = data[1];
        if (!roomName) {
          this._safeSend(ws, ["startRecordingResult", {
            success: false,
            enabled: false,
            message: "Room name required"
          }]);
          return;
        }
        
        const success = await this._startRecordingWinners(roomName);
        
        this._broadcastToRoom(roomName, ["startRecordingResult", {
          success: success,
          enabled: true,
          room: roomName,
          message: success ? "Recording ENABLED for " + roomName : "Failed to enable recording",
          timestamp: Date.now()
        }]);
        
        this._safeSend(ws, ["startRecordingResult", {
          success: success,
          enabled: true,
          room: roomName,
          message: success ? "Recording enabled for " + roomName : "Failed to enable recording"
        }]);
        
        if (success) {
          this._broadcastToRoom(roomName, ["recordingStatus", true]);
          this._broadcastToRoom(roomName, ["systemMessage", "📢 ADMIN has ENABLED winner recording for this room!"]);
        }
        return;
      }

      if (evt === "stopRecordingWinners") {
        const roomName = data[1];
        if (!roomName) {
          this._safeSend(ws, ["stopRecordingResult", {
            success: false,
            enabled: false,
            message: "Room name required"
          }]);
          return;
        }
        
        const success = await this._stopRecordingWinners(roomName);
        
        this._broadcastToRoom(roomName, ["stopRecordingResult", {
          success: success,
          enabled: false,
          room: roomName,
          message: success ? "Recording STOPPED and winners DELETED for " + roomName : "Failed to stop recording",
          timestamp: Date.now()
        }]);
        
        this._safeSend(ws, ["stopRecordingResult", {
          success: success,
          enabled: false,
          room: roomName,
          message: success ? "Recording stopped and winners deleted for " + roomName : "Failed to stop recording"
        }]);
        
        if (success) {
          this._broadcastToRoom(roomName, ["recordingStatus", false]);
          this._broadcastToRoom(roomName, ["systemMessage", "📢 ADMIN has DISABLED winner recording for this room!"]);
        }
        return;
      }

      if (evt === "getRoomWinners") {
        const room = data[1];
        if (!room) {
          this._safeSend(ws, ["lowCardWinnerUpdate", {
            error: "Room name required",
            success: false,
            message: "Room name required"
          }]);
          return;
        }
        
        const winners = await this._getLowCardWinners(room);
        const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
        
        if (Object.keys(winners).length === 0) {
          this._safeSend(ws, ["lowCardWinnerUpdate", {
            winners: {},
            room: room,
            recording: isRecordingEnabled,
            updatedAt: new Date().toISOString(),
            type: 'getRoomWinners',
            message: "No winners yet"
          }]);
        } else {
          this._safeSend(ws, ["lowCardWinnerUpdate", {
            winners: winners,
            room: room,
            recording: isRecordingEnabled,
            updatedAt: new Date().toISOString(),
            type: 'getRoomWinners'
          }]);
        }
        return;
      }

      if (evt === "getRecordingStatus") {
        const roomName = data[1];
        await this.getRecordingStatus(ws, roomName);
        return;
      }

      if (evt === "sendWinnersToRoom") {
        const room = data[1];
        if (!room) {
          this._safeSend(ws, ["sendWinnersResult", {
            success: false,
            message: "Room name required"
          }]);
          return;
        }
        
        await this._sendWinnersToRoom(room);
        
        this._safeSend(ws, ["sendWinnersResult", {
          success: true,
          room: room,
          message: "Winners data sent to room"
        }]);
        return;
      }

      if (evt === "startGameWithRecording") {
        const [_, room, bet, username] = data;
        await this._startGameWithRecording(ws, room, bet, username);
        return;
      }

      if (evt === "lowCardWinnerUpdate") {
        const room = data[1] || this._ensureRoomConsistency(ws);
        if (!room) {
          this._safeSend(ws, ["lowCardWinnerUpdate", {
            error: "Room name required",
            success: false
          }]);
          return;
        }
        
        await this._sendWinnersToRoom(room);
        
        const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
        const winners = await this._getLowCardWinners(room);
        
        if (Object.keys(winners).length === 0) {
          this._safeSend(ws, ["lowCardWinnerUpdate", {
            winners: {},
            room: room,
            recording: isRecordingEnabled,
            updatedAt: new Date().toISOString(),
            type: 'refreshResponse',
            message: "No winners yet"
          }]);
        } else {
          this._safeSend(ws, ["lowCardWinnerUpdate", {
            winners: winners,
            room: room,
            recording: isRecordingEnabled,
            updatedAt: new Date().toISOString(),
            type: 'refreshResponse'
          }]);
        }
        
        return;
      }

      // ==================== DICE GAME EVENTS ====================
      if (evt === "submitDiceAnswer") {
        const [_, username, guess] = data;
        await this.submitDiceAnswer(ws, username, guess);
        return;
      }

      if (evt === "getDiceLastWeekWinner") {
        try {
          const winner = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_WEEK_WINNER, 'json');
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
          const result = sorted.map(([username, score]) => 
            `${username}|${score}`
          );
          this._safeSend(ws, ["diceLeaderboard", result]);
        } catch(e) {
          this._safeSend(ws, ["diceLeaderboard", []]);
        }
        return;
      }

      if (evt === "deleteDiceLastWeekWinner") {
        try {
          if (this.env?.QUESTIONS) {
            this._incrementSubRequest();
            await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
            const check = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_WEEK_WINNER, 'json');
            if (!check) {
              this._safeSend(ws, ["diceLastWeekWinnerDeleted", true, "Last week winner deleted successfully"]);
              this._broadcastToRoom(DICE_ROOM, ["systemMessage", "📢 Last week winner data has been deleted"]);
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
        const remainingText = `${remaining}s remaining`;
        const timeLeft = this._getTimeLeftUntilNextDice();
        const answerRemaining = this._getDiceAnswerRemainingTime();
        const notification = {
          type: "diceStatus",
          timestamp: Date.now(),
          remainingTime: remainingText,
          diceValue: this.currentDiceRoll?.value || null,
          data: {
            isDiceTime: this._isDiceTime(),
            isActive: !!this.currentDiceRoll,
            hasWinner: this.diceHasWinner,
            winner: this.diceWinner,
            timeLeft: timeLeft.text,
            canSubmit: this._canSubmitDiceAnswer,
            readingTimeLeft: this._canSubmitDiceAnswer ? 0 : Math.max(0, Math.round((CONSTANTS.DICE_READING_TIME_MS - (Date.now() - this._diceQuestionStartTime)) / 1000)),
            answerTimeLeft: this._canSubmitDiceAnswer ? answerRemaining : 0,
            totalTimeLeft: Math.max(0, Math.round((CONSTANTS.DICE_TOTAL_TIME_MS - (Date.now() - this._diceStartTime)) / 1000))
          }
        };
        this._safeSend(ws, ["diceNotification", notification]);
        return;
      }

      if (evt === "getDiceStatus") {
        const isActive = !!this.currentDiceRoll && this._canSubmitDiceAnswer;
        this._safeSend(ws, ["diceStatus", isActive]);
        return;
      }

      // ==================== ROOM SWITCH ====================
      if (evt === "switchRoom") {
        const [_, room, username] = data;
        await this.switchRoom(ws, room, username);
        return;
      }

      // ==================== LOWCARD GAME EVENTS ====================
      const room = this._ensureRoomConsistency(ws);
      if (!room) { 
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]); 
        return; 
      }
      if (room === QUIZ_ROOM) { 
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
        default:
          break;
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Error processing event"]);
    }
  }

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

  _startWeeklyResetChecker() {
    // Weekly reset already handled by scheduler
  }

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

  _startHealthCheck() {
    // Health check already handled by scheduler
  }

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
      this._broadcastToRoom(DICE_ROOM, ["serverRecovered", {
        timestamp: Date.now(),
        message: "Server has recovered"
      }]);
    } catch(e) {}
  }

  _resetCriticalState() {
    try {
      this.currentDiceRoll = null;
      this.isDiceWaiting = false;
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
      if (this._diceKeepAliveInterval) {
        clearInterval(this._diceKeepAliveInterval);
        this._diceKeepAliveInterval = null;
      }
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
            gamesRunning: this.activeGames.size,
            wsConnections: this.wsMap.size,
            eventQueueSize: this._eventQueue?.length || 0,
            errorCount: this._errorCount,
            timestamp: Date.now(),
            diceSchedule: QUIZ_SCHEDULE.SESSIONS.map(s => `${s.start}:00-${s.end}:00`),
            currentWITATime: this._getCurrentWITATime().formatted,
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
        if (upgrade !== "websocket") return new Response("WebSocket only", { status: 400 });
        if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) return new Response("Server at maximum capacity", { status: 503 });
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
          const cf = req.cf || {};
          const country = cf?.country || 'US';
          server._cf = cf;
          server._country = country;
          server._language = 'en';
          try { this.state.acceptWebSocket(server); } catch(e) { return new Response("WebSocket acceptance failed", { status: 500 }); }
          server.addEventListener("message", async (event) => {
            try {
              const data = JSON.parse(event.data);
              if (Array.isArray(data) && data.length > 0) await this.handleEvent(server, data);
            } catch(e) { this._safeSend(server, ["gameLowCardError", e.message || "Error"]); }
          });
          server.addEventListener("close", () => {
            try {
              if (server.room || server.roomname) {
                const room = server.room || server.roomname;
                const wsId = this._getWsId(server);
                const username = server.username;
                this._removeClient(room, server);
                this._diceTimeLeftNotified.delete(wsId);
                this._nextDiceNotified.delete(wsId);
                this._diceJoinedNotified.delete(wsId);
                if (username) {
                  const conn = this.userConnections.get(username);
                  if (conn?.wsId === wsId) this.userConnections.delete(username);
                }
              }
              const clients = this.wsClients.get(DICE_ROOM);
              if (clients?.size > 0) this.ensureDiceRunning();
            } catch(e) {}
          });
          server.addEventListener("error", () => {
            try {
              if (server.room || server.roomname) {
                const room = server.room || server.roomname;
                const wsId = this._getWsId(server);
                const username = server.username;
                this._removeClient(room, server);
                this._diceTimeLeftNotified.delete(wsId);
                this._nextDiceNotified.delete(wsId);
                this._diceJoinedNotified.delete(wsId);
                if (username) {
                  const conn = this.userConnections.get(username);
                  if (conn?.wsId === wsId) this.userConnections.delete(username);
                }
              }
            } catch(e) {}
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

  async webSocketClose(ws) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      const username = ws.username;
      if (ws.room || ws.roomname) {
        const room = ws.room || ws.roomname;
        this._removeClient(room, ws);
      }
      this._diceTimeLeftNotified.delete(wsId);
      this._nextDiceNotified.delete(wsId);
      this._diceJoinedNotified.delete(wsId);
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn?.wsId === wsId) this.userConnections.delete(username);
      }
      if (wsId) { this.clientRooms.delete(wsId); this.wsMap.delete(wsId); }
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      const clients = this.wsClients.get(DICE_ROOM);
      if (clients?.size > 0) this.ensureDiceRunning();
    } catch(e) {}
  }

  async webSocketError(ws) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      const username = ws.username;
      if (ws.room || ws.roomname) {
        const room = ws.room || ws.roomname;
        this._removeClient(room, ws);
      }
      this._diceTimeLeftNotified.delete(wsId);
      this._nextDiceNotified.delete(wsId);
      this._diceJoinedNotified.delete(wsId);
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn?.wsId === wsId) this.userConnections.delete(username);
      }
      if (wsId) { this.clientRooms.delete(wsId); this.wsMap.delete(wsId); }
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
    } catch(e) {}
  }
}
