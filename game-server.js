// ==================== GAME-SERVER-FINAL.js ====================
// ✅ VERSI FINAL - HAPUS SEMUA PENYEBAB LIMIT EXCEEDED
// ✅ TANPA CPU PROTECTION - TANPA TTL - TANPA RATE LIMITING
// ✅ HANYA 3 INTERVAL - SEMUA PAKAI CACHE PERMANEN
// ✅ SIAP DEPLOY - HEMAT - DURABLE - ANTI HYBERNATE
// ✅ SUPPORT startGameWithRecording DARI ANDROID

const CONSTANTS = {
  REGISTRATION_TIME_MS: 20000,
  DRAW_TIME_MS: 20000,
  EVALUATION_DELAY_MS: 2000,
  MAX_BOTS_PER_GAME: 4,
  MAX_BET: 100000,
  
  DICE_ANSWER_TIME_MS: 20000,
  DICE_TOTAL_TIME_MS: 20000,
  DICE_BREAK_MS: 15000,
  DICE_ROOM: "Quiz",
  
  ALARM_INTERVAL_MS: 15000,
  ALARM_MAX_IDLE_MS: 60000,
  ALARM_HEARTBEAT_MS: 30000,
  ALARM_FORCE_WAKEUP_MS: 120000,
  
  STALE_GAME_TIMEOUT_MS: 300000,
  STUCK_DRAW_TIMEOUT_MS: 30000,
  STUCK_REGISTRATION_TIMEOUT_MS: 30000,
  ERROR_RECOVERY_DELAY_MS: 5000,
  ERROR_RESET_INTERVAL_MS: 60000,
  MAX_UNHANDLED_ERRORS: 10,
  
  DICE_POINT_KEY: 'dice_points',
  DICE_LAST_WEEK_WINNER: 'dice_last_week_winner',
  DICE_WINNER_KEY: 'dice_winner_',
  DICE_RECORDING_KEY: 'dice_recording_status_',
  LOWCARD_WINNER_KEY: 'lowcard_winner_',
  LOWCARD_RECORDING_KEY: 'lowcard_recording_status_',
  DICE_LAST_RESET_WEEK: 'dice_last_reset_week',
  
  TIE_BREAKER_TIME_LIMIT: 20,
  TIE_BREAKER_COOLDOWN: 15000,
  
  GAME_CLEANUP_DELAY_MS: 5000,
  EVALUATION_TIMEOUT_MS: 30000,
  MAX_BOT_DRAWS_PER_ROUND: 4,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 15,
  QUIZ_START_DELAY_MS: 5000,
  DICE_AUTO_START_DELAY_MS: 3000,
};

const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: 1, end: 3 },
    { start: 14, end: 15 },
    { start: 22, end: 23 }
  ],
  TIMEZONE_OFFSET: 8,
};

const DICE_ROOM = "Quiz";

// ==================== KV CACHE - TANPA TTL, TANPA CLEANUP ====================
class KVCache {
  constructor() {
    this.cache = new Map();
  }

  get(key) {
    return this.cache.get(key) || null;
  }

  set(key, value) {
    this.cache.set(key, value);
  }

  delete(key) {
    this.cache.delete(key);
  }

  clear() {
    this.cache.clear();
  }

  has(key) {
    return this.cache.has(key);
  }
}

// ==================== DICE GAME SYSTEM - TANPA TTL ====================
class DiceGameSystem {
  constructor(gameServer) {
    this.gameServer = gameServer;
    this.env = gameServer.env;
    this.userScores = new Map();
    this._isLoaded = false;
    this._leaderboardCache = null;
  }

  async getPoints() {
    try {
      if (!this.env?.QUESTIONS) return {};
      
      if (this._isLoaded && this.userScores.size > 0) {
        const result = {};
        for (const [username, score] of this.userScores) {
          result[username] = score;
        }
        return result;
      }
      
      const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      this._isLoaded = true;
      this._leaderboardCache = null;
      return points;
    } catch(e) {
      return {};
    }
  }

  async getLeaderboard(limit = 10) {
    try {
      if (!this.env?.QUESTIONS) return [];
      
      if (this._leaderboardCache !== null) {
        const cached = this._leaderboardCache.slice(0, limit);
        return cached.map(([username, score]) => `${username}|${score}`);
      }
      
      const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      const sorted = Object.entries(points).sort((a, b) => b[1] - a[1]);
      this._leaderboardCache = sorted;
      
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      this._isLoaded = true;
      
      return sorted.slice(0, limit).map(([username, score]) => `${username}|${score}`);
    } catch(e) {
      return [];
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
      this._isLoaded = true;
      this._leaderboardCache = null;
      return true;
    } catch(e) {
      return false;
    }
  }

  async updatePoint(username, increment = 1) {
    try {
      const points = await this.getPoints();
      points[username] = (points[username] || 0) + increment;
      return await this.setPoints(points);
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
      return true;
    } catch(e) {
      return false;
    }
  }

  rollDice() {
    return Math.floor(Math.random() * 6) + 1;
  }

  clearCache() {
    this.userScores.clear();
    this._isLoaded = false;
    this._leaderboardCache = null;
  }
}

// ==================== GAME SERVER - TANPA SEMUA PENYEBAB LIMIT ====================
export class GameServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      this.closing = false;
      this.isDestroyed = false;
      this._initialized = false;
      this._initializing = false;
      
      this.activeGames = new Map();
      this._wsIdCounter = 0;
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.wsMap = new Map();
      this.userConnections = new Map();
      this._cleanupTimers = new Map();
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();
      
      this.diceAnswered = new Set();
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.currentDiceRoll = null;
      this._diceStartTime = null;
      this._diceTimeout = null;
      this._diceBreakTimeout = null;
      this._diceStartTimeout = null;
      this.diceAutoEnabled = false;
      this._isShowingDice = false;
      this._canSubmitDiceAnswer = false;
      this._diceQuestionStartTime = null;
      this._diceRound = 0;
      this._winnerProcessed = false;
      
      this._diceTimeUpCooldown = false;
      this._diceTimeUpCooldownTimer = null;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      
      this._diceTimeLeftNotified = new Map();
      this._nextDiceNotified = new Map();
      this._diceJoinedNotified = new Map();
      this._diceTimeLeftBroadcastCooldown = 1000;
      this._lastDiceTimeLeftBroadcast = 0;
      
      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tieActive = false;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._playerAnswers = new Map();
      this._processingTieResults = false;
      
      this._recordingEnabled = new Map();
      this._kvCache = new KVCache();
      this._cachedLastWeekWinner = null;
      this._resetWeekCache = null;
      
      this.diceGameSystem = new DiceGameSystem(this);
      
      this._lastActivity = Date.now();
      this._lastAlarm = Date.now();
      this._alarmCount = 0;
      this._isHibernating = false;
      this._wakeupAttempts = 0;
      this._maxWakeupAttempts = 3;
      
      this._eventQueue = [];
      this._isProcessingQueue = false;
      
      this._startTime = Date.now();
      this._lastHeartbeat = Date.now();
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      this._isRecovering = false;
      this._recoveryAttempts = 0;
      this._maxRecoveryAttempts = 3;

      // ✅ HANYA 3 INTERVAL
      this._alarmInterval = setInterval(() => {
        if (this.closing || this.isDestroyed) {
          clearInterval(this._alarmInterval);
          this._alarmInterval = null;
          return;
        }
        this._alarmTick();
      }, CONSTANTS.ALARM_INTERVAL_MS || 15000);

      this._forceWakeupTimer = setInterval(() => {
        const now = Date.now();
        if (now - this._lastActivity > CONSTANTS.ALARM_MAX_IDLE_MS) {
          this._forceWakeup();
        }
      }, CONSTANTS.ALARM_FORCE_WAKEUP_MS || 120000);

      this._heartbeatInterval = setInterval(() => {
        if (this.closing || this.isDestroyed) {
          clearInterval(this._heartbeatInterval);
          this._heartbeatInterval = null;
          return;
        }
        this._sendHeartbeat();
      }, CONSTANTS.ALARM_HEARTBEAT_MS || 30000);
      
      this._initAsync();
      
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) this._alarmTick();
      }, 1000);

      setTimeout(async () => {
        if (!this.closing && !this.isDestroyed) await this._initResetWeek();
      }, 1000);

      setTimeout(async () => {
        if (!this.closing && !this.isDestroyed) await this.diceGameSystem.getPoints();
      }, 5000);

      setTimeout(() => {
        if (!this.closing && !this.isDestroyed && !this._isShowingDice) {
          this.forceStartDice();
        }
      }, 8000);

      this._startDiceAutoChecker();

    } catch(e) {}
  }

  // ==================== ALARM TICK - SEMUA TASK DALAM 1 INTERVAL ====================
  
  _alarmTick() {
    try {
      const now = Date.now();
      this._alarmCount++;
      this._lastAlarm = now;
      
      const idleTime = now - this._lastActivity;
      
      if (idleTime > CONSTANTS.ALARM_MAX_IDLE_MS) {
        if (!this._isHibernating) this._isHibernating = true;
        this._wakeup();
        return;
      }
      
      if (idleTime > CONSTANTS.ALARM_INTERVAL_MS) {
        this._sendKeepAlive();
      }
      
      if (this._isHibernating && idleTime < CONSTANTS.ALARM_INTERVAL_MS) {
        this._isHibernating = false;
        this._wakeupAttempts = 0;
      }
      
      this._diceKeepAliveTask();
      this._healthCheckTask();
      this._checkStuckGames();
      this._cleanupStaleGames();
      this._cleanupDeadConnections();
      this._diceTimerTask();
      this._diceAutoTask().catch(() => {});
      
      this._lastActivity = Date.now();
    } catch(e) {}
  }

  _sendKeepAlive() {
    try {
      for (const [room, clients] of this.wsClients) {
        if (clients && clients.size > 0) {
          this._broadcastToRoom(room, ["keepAlive", {
            timestamp: Date.now(),
            alarmCount: this._alarmCount,
            isHibernating: this._isHibernating
          }]);
        }
      }
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "♡ Server is active"]);
    } catch(e) {}
  }

  _sendHeartbeat() {
    try {
      this._lastHeartbeat = Date.now();
      
      const deadConnections = [];
      for (const [wsId, ws] of this.wsMap) {
        if (!ws || ws.readyState !== 1) deadConnections.push(wsId);
      }
      
      for (const wsId of deadConnections) {
        const ws = this.wsMap.get(wsId);
        if (ws) {
          const room = this.clientRooms.get(wsId);
          if (room) this._removeClientFromRoom(room, wsId);
          this.clientRooms.delete(wsId);
          this.wsMap.delete(wsId);
        }
      }
      
      if (this._isDiceTime() && !this.currentDiceRoll && !this._isShowingDice) {
        const clients = this.wsClients.get(DICE_ROOM);
        if (clients && clients.size > 0) {
          this.diceAutoEnabled = true;
          this._showDiceQuestionSilent();
        }
      }
    } catch(e) {}
  }

  _wakeup() {
    try {
      if (this._wakeupAttempts > this._maxWakeupAttempts) {
        this._forceWakeup();
        return;
      }
      
      this._wakeupAttempts++;
      this._lastActivity = Date.now();
      this._isHibernating = false;
      
      if (this._isDiceTime()) {
        this.diceAutoEnabled = true;
        if (!this.currentDiceRoll && !this._isShowingDice) {
          this._showDiceQuestionSilent();
        }
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "♡ Dice game recovered"]);
      }
      
      for (const [room, game] of this.activeGames) {
        if (game._isActive && !game._gameEnded) {
          if (game.registrationOpen && game._createdAt) {
            const elapsed = (Date.now() - game._createdAt) / 1000;
            if (elapsed > (CONSTANTS.REGISTRATION_TIME_MS / 1000)) {
              this._closeRegistration(room, game);
            }
          }
          if (game._phase === 'draw' && game._drawPhaseStart) {
            const elapsed = (Date.now() - game._drawPhaseStart) / 1000;
            if (elapsed > (CONSTANTS.DRAW_TIME_MS / 1000)) {
              this._closeDrawPhase(room, game);
            }
          }
          this._broadcastToRoom(room, ["gameState", {
            room: room,
            hasGame: true,
            phase: game._phase,
            round: game.round,
            recovered: true
          }]);
        }
      }
      
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._switchLocks.clear();
      
      if (this._eventQueue && this._eventQueue.length > 0) {
        this._eventQueue = [];
        this._isProcessingQueue = false;
      }
      
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "♡ Server is awake"]);
    } catch(e) {}
  }

  _forceWakeup() {
    try {
      this._lastActivity = Date.now();
      this._isHibernating = false;
      this._wakeupAttempts = 0;
      this._alarmCount = 0;
      
      this._resetCriticalState();
      
      if (this._isDiceTime()) {
        this.diceAutoEnabled = true;
        this._showDiceQuestionSilent();
      }
      
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._switchLocks.clear();
      this._switchRetries.clear();
      
      this._cleanupDeadConnections();
      
      if (this._eventQueue) {
        this._eventQueue = [];
        this._isProcessingQueue = false;
      }
      
      this._diceTimeLeftNotified.clear();
      this._nextDiceNotified.clear();
      this._diceJoinedNotified.clear();
      
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "♡ Server fully recovered"]);
      
      this._alarmCount = 0;
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
      this._lastSentRemaining = -1;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      
      if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
      if (this._diceBreakTimeout) { clearTimeout(this._diceBreakTimeout); this._diceBreakTimeout = null; }
      if (this._diceStartTimeout) { clearTimeout(this._diceStartTimeout); this._diceStartTimeout = null; }
      if (this._diceTimeUpCooldownTimer) { clearTimeout(this._diceTimeUpCooldownTimer); this._diceTimeUpCooldownTimer = null; }
      
      this._playerAnswers = new Map();
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieActive = false;
      this._tieBreakers.clear();
      this._tieRound = 0;
      this._processingTieResults = false;
      
      if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
      if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
      
      if (this._eventQueue) this._eventQueue = [];
      if (this._isProcessingQueue) this._isProcessingQueue = false;
    } catch(e) {}
  }

  // ==================== DICE METHODS ====================

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

  _getCurrentWITATime() {
    try {
      const now = new Date();
      const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
      const minutes = now.getUTCMinutes();
      return { hours, minutes, totalMinutes: (hours * 60) + minutes };
    } catch(e) { return { hours: 0, minutes: 0, totalMinutes: 0 }; }
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
      return { hours: Math.floor(minDiff / 60), minutes: Math.floor(minDiff % 60) };
    } catch(e) { return { hours: 0, minutes: 0 }; }
  }

  forceStartDice() {
    try {
      if (this._tieActive) return false;
      if (this._isShowingDice) return false;
      if (this._diceTimeUpCooldown) return false;
      if (!this._isDiceTime() || this.currentDiceRoll || this._diceTimeout || this._diceStartTimeout) return false;
      this.diceAutoEnabled = true;
      this._showDiceQuestion();
      return true;
    } catch(e) { return false; }
  }

  async _showDiceQuestion() {
    try {
      if (this._tieActive) return;
      if (this._isShowingDice) return;
      if (this._diceTimeUpCooldown) return;
      if (!this._isDiceTime()) return;
      if (this.isDestroyed || this._diceStartTimeout || this.currentDiceRoll) return;
      
      this._isShowingDice = true;
      
      try {
        this._diceRound = (this._diceRound || 0) + 1;
        const diceValue = this.diceGameSystem.rollDice();
        
        this.currentDiceRoll = { value: diceValue, timestamp: Date.now(), round: this._diceRound };
        this._diceStartTime = Date.now();
        this._diceQuestionStartTime = Date.now();
        this._canSubmitDiceAnswer = true;
        
        this.diceAnswered = new Set();
        this.diceHasWinner = false;
        this.diceWinner = null;
        this._winnerProcessed = false;
        this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
        this._lastSentRemaining = -1;
        
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
                this._diceTimeout = null;
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
      }
    } catch(e) {}
  }

  async _showDiceQuestionSilent() {
    try {
      if (this._tieActive) return;
      if (this._isShowingDice) return;
      if (this._diceTimeUpCooldown) return;
      if (!this._isDiceTime()) return;
      if (this.isDestroyed || this._diceStartTimeout || this.currentDiceRoll) return;
      
      this._isShowingDice = true;
      
      try {
        this._diceRound = (this._diceRound || 0) + 1;
        const diceValue = this.diceGameSystem.rollDice();
        
        this.currentDiceRoll = { value: diceValue, timestamp: Date.now(), round: this._diceRound };
        this._diceStartTime = Date.now();
        this._diceQuestionStartTime = Date.now();
        this._canSubmitDiceAnswer = true;
        
        this.diceAnswered = new Set();
        this.diceHasWinner = false;
        this.diceWinner = null;
        this._winnerProcessed = false;
        this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
        this._lastSentRemaining = -1;
        
        await this._broadcastDiceRoll(diceValue);
        
        this._startDiceTimerNotifications();
        
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
                this._diceTimeout = null;
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
    
    this._diceTimeUpCooldownTimer = setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._showDiceQuestionSilent();
    }, 15000);
  }

  _startDiceTimerNotifications() {
    this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
    this._lastSentRemaining = -1;
    this._lastNotificationKey = "";
    this._lastNotificationTime = 0;
    this._diceTimerTick();
  }

  _diceTimerTick() {
    try {
      if (!this.currentDiceRoll || !this._diceQuestionStartTime) return;
      
      const elapsed = (Date.now() - this._diceQuestionStartTime) / 1000;
      const remaining = Math.max(0, CONSTANTS.DICE_ANSWER_TIME_MS / 1000 - elapsed);
      const remainingInt = Math.floor(remaining);
      
      let shouldSend = false;
      let message = "";
      
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
      
      if (remainingInt > 0 && !this._diceNotifiedFlags.timeup) {
        setTimeout(() => { this._diceTimerTick(); }, 1000);
      }
      
    } catch(e) {}
  }

  async _broadcastDiceRoll(diceValue) {
    try {
      if (this._tieActive) return;
      
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
      
      for (const wsId of wsIds) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1) {
          try { ws.send(msgStr); } catch(e) {}
        }
      }
    } catch(e) {}
  }

  _broadcastDiceNotification(type, data) {
    try {
      if (this._tieActive && !data?.isTieBreaker) return;
      
      const wsIds = this.wsClients.get(DICE_ROOM);
      if (!wsIds?.size) return;
      
      const now = Date.now();
      const message = data.message || "";
      const remaining = data.remaining !== undefined ? data.remaining : -1;
      
      let key = `dice_${remaining}`;
      if (remaining === -1) key = `dice_msg_${message.substring(0, 30)}`;
      if (message === "TIME UP") key = "dice_timeup";
      if (data.cooldown) key = `cooldown_${remaining}`;
      
      if (message !== "TIME UP") {
        if (this._lastNotificationKey === key && (now - this._lastNotificationTime) < 3000) return;
        if (remaining > 0 && this._lastSentRemaining === remaining && !data.cooldown) return;
      }
      
      this._lastNotificationKey = key;
      this._lastNotificationTime = now;
      if (remaining > 0) this._lastSentRemaining = remaining;
      
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", message]);
    } catch(e) {}
  }

  async _getDicePoints() {
    try {
      if (!this.env?.QUESTIONS) return {};
      return await this.diceGameSystem.getPoints();
    } catch(e) { return {}; }
  }

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
      
      if (this._tieActive) {
        if (!this._tiePlayers.includes(username)) return;
        if (this._tieAnswers.has(username)) return;
        if (!this._canSubmitDiceAnswer) return;
        
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
          if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
          if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
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
      
      if (this.diceAnswered.has(username)) return;
      
      const diceValue = this.currentDiceRoll?.value;
      if (!diceValue) return;
      
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

  // ==================== TIE BREAKER ====================

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
    
    if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
    if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
    
    this._tieRound++;
    this._tiePlayers = [...players];
    this._tieAnswers = new Map();
    data.round = this._tieRound;
    data.status = 'running';
    data.players = players;
    
    const playerNames = players.join(', ');
    this._broadcastToRoom(DICE_ROOM, ["diceNotification", `♡ Round ${this._tieRound}: ${playerNames}`]);
    
    this._canSubmitDiceAnswer = true;
    this._diceQuestionStartTime = Date.now();
    this.diceAnswered = new Set();
    this._isShowingDice = true;
    
    this._startTieTimer(room, id, players);
  }

  _startTieTimer(room, id, players) {
    if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
    if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
    
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
        if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
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
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", `No one answered`]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    
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
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._showDiceQuestionSilent();
    }, CONSTANTS.TIE_BREAKER_COOLDOWN || 15000);
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
    this._processingTieResults = false;
    
    if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
    if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
  }

  _getActiveTieBreakerId() {
    for (const [id, data] of this._tieBreakers) {
      if (data.status === 'waiting' || data.status === 'running') return id;
    }
    return null;
  }

  // ==================== DICE KEEP ALIVE ====================

  _diceKeepAliveTask() {
    try {
      this._lastHeartbeat = Date.now();
      if (!this._isDiceTime()) {
        const timeLeft = this._getTimeLeftUntilNextDice();
        this._broadcastDiceNotification("diceError", {
          message: `Next dice game in: ${timeLeft.hours}h ${timeLeft.minutes}m`,
          timeLeft: `${timeLeft.hours}h ${timeLeft.minutes}m`,
          hours: timeLeft.hours,
          minutes: timeLeft.minutes,
          remaining: -1,
          isDiceTime: false,
          isActive: false
        });
        return;
      }
    } catch(e) {}
  }

  _diceTimerTask() {
    try {
      if (this._tieActive) return;
      if (this._isDiceTime()) {
        if (!this.currentDiceRoll && !this._diceTimeout && !this._isShowingDice && !this._diceTimeUpCooldown) {
          const clients = this.wsClients.get(DICE_ROOM);
          if (clients?.size > 0) {
            this._showDiceQuestion();
          }
        }
      }
    } catch(e) {}
  }

  _startDiceAutoChecker() {
    try {
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          this._diceAutoTask().catch(() => {});
        }
      }, 1000);
    } catch(e) {}
  }

  async _diceAutoTask() {
    try {
      if (this._tieActive) return;
      
      await this._checkDiceAutoStatus();
      await this._checkAndRestartDice();
      
      const isDiceTime = this._isDiceTime();
      const clients = this.wsClients.get(DICE_ROOM);
      const hasPlayers = clients && clients.size > 0;
      
      if (isDiceTime) {
        if (hasPlayers && !this.currentDiceRoll && !this._diceTimeout && 
            !this._diceStartTimeout && !this._isShowingDice && !this._diceTimeUpCooldown) {
          
          if (!this.diceAutoEnabled) {
            this.diceAutoEnabled = true;
            await this.startDiceWithDelay(CONSTANTS.DICE_AUTO_START_DELAY_MS || 3000);
          }
          
          setTimeout(() => {
            if (!this.closing && !this.isDestroyed && 
                !this.currentDiceRoll && !this._diceTimeout && 
                !this._isShowingDice && !this._diceTimeUpCooldown) {
              this.forceStartDice();
            }
          }, CONSTANTS.DICE_AUTO_START_DELAY_MS || 3000);
        }
      }
    } catch(e) {}
  }

  async _checkDiceAutoStatus() {
    try {
      const isDiceTime = this._isDiceTime();
      if (isDiceTime) {
        this.diceEndedToday = false;
        this.diceEndMessageShown = false;
        this.diceEndNotified = false;
        this._nextDiceNotified.clear();
        
        if (!this.diceAutoEnabled) {
          this.diceAutoEnabled = true;
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
        }
        return true;
      }
    } catch(e) { return true; }
  }

  _checkAndRestartDice() {
    try {
      if (!this._isDiceTime()) return;
      if (this._diceTimeUpCooldown) return;
      if (!this.currentDiceRoll && !this._diceTimeout && !this._diceBreakTimeout && !this._isShowingDice) {
        const clients = this.wsClients.get(DICE_ROOM);
        if (clients?.size > 0) {
          this.diceAutoEnabled = true;
          this._showDiceQuestion();
        }
      }
    } catch(e) {}
  }

  async startDiceWithDelay(delayMs) {
    try {
      if (this._diceStartTimeout) return;
      this._diceStartTimeout = setTimeout(() => {
        try {
          if (this.closing || this.isDestroyed) { this._diceStartTimeout = null; return; }
          this._diceStartTimeout = null;
          if (!this.currentDiceRoll && this.diceAutoEnabled && !this._isShowingDice && !this._diceTimeUpCooldown) {
            this.forceStartDice();
          }
        } catch(e) {}
      }, delayMs);
    } catch(e) {}
  }

  async resetDice() {
    try {
      if (this._diceTimeout) clearTimeout(this._diceTimeout);
      if (this._diceBreakTimeout) clearTimeout(this._diceBreakTimeout);
      if (this._diceStartTimeout) clearTimeout(this._diceStartTimeout);
      if (this._diceTimeUpCooldownTimer) clearTimeout(this._diceTimeUpCooldownTimer);
      
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
      this._diceTimeUpCooldown = false;
      this._lastSentRemaining = -1;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      
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
      
      if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
      if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
    } catch(e) {}
  }

  _clearDiceData() {
    try {
      if (this._diceTimeUpCooldownTimer) { clearTimeout(this._diceTimeUpCooldownTimer); this._diceTimeUpCooldownTimer = null; }
      this.currentDiceRoll = null;
      this._diceStartTime = null;
      this.diceAnswered = new Set();
      this.diceHasWinner = false;
      this.diceWinner = null;
      this._isShowingDice = false;
      this._winnerProcessed = false;
      this._canSubmitDiceAnswer = false;
      this._diceQuestionStartTime = null;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      
      if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
      if (this._diceBreakTimeout) { clearTimeout(this._diceBreakTimeout); this._diceBreakTimeout = null; }
      if (this._diceStartTimeout) { clearTimeout(this._diceStartTimeout); this._diceStartTimeout = null; }
      
      this._playerAnswers = new Map();
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieActive = false;
      this._tieBreakers.clear();
      this._tieRound = 0;
      this._processingTieResults = false;
      
      if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
      if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
    } catch(e) {}
  }

  // ==================== CACHE METHODS - SEMUA PAKAI CACHE ====================

  async _getRecordingStatusFromKV(roomName) {
    try {
      if (!roomName) return false;
      if (this._recordingEnabled.has(roomName)) {
        return this._recordingEnabled.get(roomName);
      }
      if (this.env?.QUESTIONS) {
        const kvValue = await this.env.QUESTIONS.get(CONSTANTS.LOWCARD_RECORDING_KEY + roomName);
        const isRecording = kvValue === 'true';
        this._recordingEnabled.set(roomName, isRecording);
        return isRecording;
      }
      return false;
    } catch(e) { return false; }
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
        await this.env.QUESTIONS.put(CONSTANTS.LOWCARD_RECORDING_KEY + roomName, 'true');
      }
      this._broadcastToRoom(roomName, ["recordingStatus", true]);
      return true;
    } catch(e) { return false; }
  }

  async _stopRecordingWinners(roomName) {
    try {
      if (!roomName) return false;
      const room = roomName.trim();
      this._recordingEnabled.set(room, false);
      if (this.env?.QUESTIONS) {
        await this.env.QUESTIONS.delete(CONSTANTS.LOWCARD_RECORDING_KEY + room);
        await this.env.QUESTIONS.delete(CONSTANTS.LOWCARD_WINNER_KEY + room);
        this._kvCache.delete(CONSTANTS.LOWCARD_WINNER_KEY + room);
      }
      this._broadcastToRoom(room, ["recordingStatus", false]);
      return true;
    } catch(e) { return false; }
  }

  async _getLowCardWinners(room) {
    try {
      if (!room) return {};
      if (!this.env?.QUESTIONS) return {};
      
      const cacheKey = CONSTANTS.LOWCARD_WINNER_KEY + room;
      const cached = this._kvCache.get(cacheKey);
      if (cached) return cached;
      
      const isRecording = await this._getRecordingStatusFromKV(room);
      if (!isRecording) return {};
      
      const winners = await this.env.QUESTIONS.get(cacheKey, 'json');
      if (winners && typeof winners === 'object' && Object.keys(winners).length > 0) {
        this._kvCache.set(cacheKey, winners);
        return winners;
      }
      return {};
    } catch(e) { return {}; }
  }

  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username) return false;
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled || room === DICE_ROOM) return false;
      if (!this.env?.QUESTIONS) return false;
      
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      let roomWinners = await this._getLowCardWinners(room);
      if (!roomWinners || typeof roomWinners !== 'object') {
        roomWinners = {};
      }
      
      let currentCount = 0;
      if (roomWinners[username]) {
        const valStr = String(roomWinners[username]);
        currentCount = parseInt(valStr.replace("x", "").replace("X", "")) || 0;
      }
      roomWinners[username] = (currentCount + 1) + "x";
      
      await this.env.QUESTIONS.put(key, JSON.stringify(roomWinners));
      this._kvCache.set(key, roomWinners);
      return true;
    } catch(e) { return false; }
  }

  async _getCachedResetWeek() {
    try {
      if (this._resetWeekCache !== null) return this._resetWeekCache;
      if (this.env?.QUESTIONS) {
        const lastResetWeek = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
        if (lastResetWeek) {
          this._resetWeekCache = lastResetWeek;
          return lastResetWeek;
        }
      }
      return null;
    } catch(e) { return null; }
  }

  async _updateCachedResetWeek(week) {
    this._resetWeekCache = week;
    if (this.env?.QUESTIONS) {
      await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, week);
    }
  }

  async _initResetWeek() {
    try {
      if (!this.env?.QUESTIONS) return;
      const existingResetWeek = await this._getCachedResetWeek();
      const currentWeek = this._generateCurrentWeek(new Date());
      if (!existingResetWeek) {
        await this._updateCachedResetWeek(currentWeek);
      }
    } catch(e) {}
  }

  _generateCurrentWeek(date) {
    try {
      const now = date || new Date();
      const year = now.getUTCFullYear();
      const startOfYear = new Date(Date.UTC(year, 0, 1));
      const diff = now - startOfYear;
      const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
      return `${year}-W${String(week).padStart(2, '0')}`;
    } catch(e) { return '2026-W01'; }
  }

  async _checkAndResetWeeklyDice() {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      const now = new Date();
      const currentWeek = this._generateCurrentWeek(now);
      let lastResetWeek = await this._getCachedResetWeek();
      
      if (!lastResetWeek) {
        await this._updateCachedResetWeek(currentWeek);
        return false;
      }
      
      const weekChanged = lastResetWeek !== currentWeek;
      if (!weekChanged) return false;
      
      const dayOfWeek = now.getUTCDay();
      const hours = now.getUTCHours();
      const minutes = now.getUTCMinutes();
      
      const isMonday = dayOfWeek === 1;
      const isResetTime = hours === 0 && minutes === 0;
      
      if (weekChanged && isMonday && isResetTime) {
        const points = await this.diceGameSystem.getPoints();
        
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
          await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winnerData));
          this._cachedLastWeekWinner = winnerData;
        } else {
          await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
          this._cachedLastWeekWinner = null;
        }
        
        await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify({}));
        await this._updateCachedResetWeek(currentWeek);
        this.diceGameSystem.clearCache();
        return true;
      }
      
      return false;
    } catch(e) { return false; }
  }

  async _handleDiceWinner(username, diceValue) {
    try {
      if (this._winnerProcessed) return;
      if (!this.currentDiceRoll || !this._canSubmitDiceAnswer) return;
      
      this._winnerProcessed = true;
      const success = await this.diceGameSystem.updatePoint(username, 1);
      
      if (success) {
        const points = await this.diceGameSystem.getPoints();
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
      }
      
      setTimeout(() => { this._winnerProcessed = false; }, 1000);
    } catch(e) {}
  }

  // ==================== INIT ====================

  async _initAsync() {
    try {
      if (this._initializing) return;
      if (this._initialized && !this._isRecovering) return;
      this._initializing = true;
      
      await this.diceGameSystem.getPoints();
      
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          if (this._isDiceTime()) {
            const clients = this.wsClients.get(DICE_ROOM);
            if (clients && clients.size > 0) {
              this.diceAutoEnabled = true;
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
    } catch(e) {
      this._initializing = false;
    }
  }

  // ==================== HEALTH CHECK ====================

  _healthCheckTask() {
    try {
      this._performHealthCheck();
    } catch(e) {}
  }

  _performHealthCheck() {
    try {
      const now = Date.now();
      this._lastHeartbeat = now;
      
      if (this._isDiceTime() && this.currentDiceRoll && this._diceStartTime) {
        const elapsed = (now - this._diceStartTime) / 1000;
        if (elapsed > (CONSTANTS.DICE_TOTAL_TIME_MS / 1000) + 30) {
          this.currentDiceRoll = null;
          this._diceTimeout = null;
          this._isShowingDice = false;
          this._canSubmitDiceAnswer = false;
        }
      }
    } catch(e) {}
  }

  // ==================== GAME METHODS ====================

  async startGame(ws, bet, username, forceStart = false) {
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

      // ✅ CEK RECORDING - IZINKAN JIKA forceStart = true
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (isRecordingEnabled && !forceStart) {
        this._safeSend(ws, ["gameLowCardError", "Recording is ACTIVE in this room"]);
        return;
      }
      
      // ✅ KIRIM INFO KE CLIENT JIKA forceStart
      if (isRecordingEnabled && forceStart) {
        this._safeSend(ws, ["gameLowCardInfo", "Game started with recording enabled"]);
      }

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
        _startedByRecording: isRecordingEnabled && forceStart,
        _startedBy: forceStart ? 'recording' : 'user'
      };
      
      game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
      game.playerWsId.set(usernameClean, wsId);
      this.activeGames.set(room, game);
      this._addClient(room, ws, usernameClean, false);
      this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
      this._broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
      
      this._startRegistration(room, game);
      
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
      
      game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
      this._addClient(room, ws, usernameClean, false);
      game.playerWsId.set(usernameClean, wsId);
      this._broadcastToRoom(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
      
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
      
      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw') {
        this._safeSend(ws, ["gameLowCardError", "Cannot submit now"]);
        return;
      }
      
      if (!game.players.has(usernameClean) || game.eliminated.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game or eliminated"]);
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
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameActuallyRunning(game)) {
        game.evaluationLocked = true;
        if (game._evalTimer) { clearTimeout(game._evalTimer); game._evalTimer = null; }
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        game._evalTimer = setTimeout(() => {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            this._evaluateRound(room, game);
          }
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
      
      if (isRunning) this._sendGameStateToClient(ws, room);
    } catch(e) {}
  }

  // ==================== GAME HELPERS ====================

  _isGameActuallyRunning(game) {
    try { return game?._isActive === true && !game?._gameEnded; }
    catch(e) { return false; }
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
    try { return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; }
    catch(e) { return "C1"; }
  }

  _getRandomDrawDelay() {
    try { return (Math.floor(Math.random() * 14) + 2) * 1000; }
    catch(e) { return 5000; }
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

  // ==================== GAME REGISTRATION ====================

  _startRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      if (game._registrationTimer) { clearInterval(game._registrationTimer); game._registrationTimer = null; }
      
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
        } catch(e) { clearInterval(timer); if (game._registrationTimer === timer) game._registrationTimer = null; }
      }, 1000);
      game._registrationTimer = timer;
    } catch(e) {}
  }

  _closeRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      game.registrationOpen = false;
      if (game._registrationTimer) { clearInterval(game._registrationTimer); game._registrationTimer = null; }
      
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
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
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

  // ==================== GAME DRAW PHASE ====================

  async _startDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      
      if (game._drawTimer) { clearInterval(game._drawTimer); game._drawTimer = null; }
      if (game._evalTimer) { clearTimeout(game._evalTimer); game._evalTimer = null; }
      if (game._botTimeouts) { for (const id of game._botTimeouts) clearTimeout(id); game._botTimeouts.clear(); }
      
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
      if (game._drawTimer) { clearInterval(game._drawTimer); game._drawTimer = null; }
      
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
        } catch(e) { clearInterval(timer); if (game._drawTimer === timer) game._drawTimer = null; }
      }, 1000);
      game._drawTimer = timer;
    } catch(e) {}
  }

  _closeDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || game.evaluationLocked) return;
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      
      if (game._drawTimer) { clearInterval(game._drawTimer); game._drawTimer = null; }
      
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) this._forceBotDraw(room, botId, game);
      }
      
      this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      if (game._evalTimer) { clearTimeout(game._evalTimer); game._evalTimer = null; }
      
      game._evalTimer = setTimeout(() => {
        const currentGame = this.activeGames.get(room);
        if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
          this._evaluateRound(room, game);
        }
      }, CONSTANTS.EVALUATION_DELAY_MS);
    } catch(e) {}
  }

  // ==================== BOT METHODS ====================

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
          try { this._evaluateRound(room, game); } catch(e) {}
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

  // ==================== EVALUATE ROUND ====================

  async _evaluateRound(room, game) {
    try {
      if (this.isDestroyed || !game?._isActive || game._gameEnded || game._isEvaluating || !game.players) return;
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game) return;
      
      game._isEvaluating = true;
      game._safetyTimer = setTimeout(() => {
        try { if (game?._isEvaluating) { game._isEvaluating = false; this._scheduleGameCleanup(room, game); } }
        catch(e) {}
      }, CONSTANTS.EVALUATION_TIMEOUT_MS);
      
      if (game._evalTimer) { clearTimeout(game._evalTimer); game._evalTimer = null; }
      if (game._botTimeouts) { for (const id of game._botTimeouts) clearTimeout(id); game._botTimeouts.clear(); }
      
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
        if (game._safetyTimer) { clearTimeout(game._safetyTimer); game._safetyTimer = null; }
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
        if (game._safetyTimer) { clearTimeout(game._safetyTimer); game._safetyTimer = null; }
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      const activePlayerIds = this._getActivePlayerIds(game);
      if (game.numbers.size < activePlayerIds.length) {
        game._isEvaluating = false;
        if (game._safetyTimer) { clearTimeout(game._safetyTimer); game._safetyTimer = null; }
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
        if (game._safetyTimer) { clearTimeout(game._safetyTimer); game._safetyTimer = null; }
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
        if (game._safetyTimer) { clearTimeout(game._safetyTimer); game._safetyTimer = null; }
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (remaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { clearTimeout(game._safetyTimer); game._safetyTimer = null; }
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
      
      if (game._safetyTimer) { clearTimeout(game._safetyTimer); game._safetyTimer = null; }
      
      if (this._isGameActuallyRunning(game) && !game._gameEnded) {
        this._startDrawPhase(room, game);
      }
      
    } catch(e) {}
  }

  // ==================== GAME CLEANUP ====================

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
          if (currentGame?._isActive && !currentGame._gameEnded) { this._cleanupTimers.delete(room); return; }
          this._cleanupTimers.delete(room);
          const gameToDelete = this.activeGames.get(room);
          if (gameToDelete) this._deleteGame(room, gameToDelete);
        } catch(e) {}
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS);
      this._cleanupTimers.set(room, timer);
    } catch(e) {}
  }

  _deleteGame(room, game) {
    try {
      if (!room || !game) return;
      if (game?._isActive && !game._gameEnded) return;
      if (this._cleanupTimers.has(room)) { clearTimeout(this._cleanupTimers.get(room)); this._cleanupTimers.delete(room); }
      
      if (game) {
        game._gameEnded = true;
        game._isActive = false;
        game.playerWsId = null;
        this._cleanupGame(game);
      }
      this.activeGames.delete(room);
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
    } catch(e) {}
  }

  _cleanupGame(game) {
    try {
      if (!game) return;
      if (game._isActive && !game._gameEnded) return;
      
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) { clearTimeout(game[key]); clearInterval(game[key]); game[key] = null; }
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

  async _forceCleanupGame(room, game) {
    try {
      if (!game) return;
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) { clearTimeout(game[key]); clearInterval(game[key]); game[key] = null; }
      }
      if (game._botTimeouts) { for (const id of game._botTimeouts) clearTimeout(id); game._botTimeouts.clear(); }
      game._gameEnded = true;
      game._isActive = false;
      game._endTime = Date.now();
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
      this.activeGames.delete(room);
      if (this._cleanupTimers.has(room)) { clearTimeout(this._cleanupTimers.get(room)); this._cleanupTimers.delete(room); }
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
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
        const currentGame = this.activeGames.get(room);
        if (currentGame && currentGame === game && !game._gameEnded) this._checkGameCanContinue(room, game);
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

  // ==================== CLEANUP TASKS ====================

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
      
      for (const item of toEvaluate) this._closeDrawPhase(item.room, item.game);
      for (const item of toClose) this._closeRegistration(item.room, item.game);
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
        if (!ws || ws.readyState !== 1) toRemove.push(wsId);
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

  // ==================== WEBSOCKET METHODS ====================

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
      if (!room) room = this.clientRooms.get(wsId) || null;
      if (!room && ws.username) {
        const conn = this.userConnections.get(ws.username);
        if (conn) room = conn.room || null;
      }
      
      if (room) {
        ws.room = room;
        ws.roomname = room;
        if (!this.wsClients.has(room)) this.wsClients.set(room, new Set());
        if (!this.wsClients.get(room).has(wsId)) {
          this.wsClients.get(room).add(wsId);
          this.clientRooms.set(wsId, room);
          this.wsMap.set(wsId, ws);
        }
        return room;
      }
      return null;
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
      
      if (username) {
        let conn = this.userConnections.get(username);
        if (conn) { conn.room = room; conn.timestamp = Date.now(); conn.ws = ws; conn.wsId = wsId; }
        else { this.userConnections.set(username, { wsId, ws, room, timestamp: Date.now() }); }
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
      
      const currentRoom = ws.room || ws.roomname || this.clientRooms.get(wsId);
      if (currentRoom === roomName) {
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
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
        this._addClient(roomName, ws, username, false);
        ws.room = roomName;
        ws.roomname = roomName;
        if (username) ws.username = username;
        
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
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
        this._safeSend(ws, ["gameState", { room, hasGame: false, gameType: 'lowcard' }]);
        return;
      }
      
      const activePlayers = this._getActivePlayers(game);
      const allPlayers = Array.from(game.players.values()).map(p => p.name);
      const eliminated = Array.from(game.eliminated || []);
      const submitted = Array.from(game.numbers?.keys() || []);
      
      const state = {
        room, hasGame: true, gameType: 'lowcard',
        isActive: game._isActive, phase: game._phase || 'registration',
        round: game.round || 1, bet: game.betAmount || 0,
        host: game.hostName || 'Unknown',
        registrationOpen: game.registrationOpen || false,
        players: allPlayers, activePlayers: activePlayers.map(p => p.name),
        eliminated, submitted,
        playerCount: game.players.size, activeCount: activePlayers.length,
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
    } catch(e) {}
  }

  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  async _broadcastToRoom(room, message) {
    try {
      if (this.closing || this.isDestroyed || !room || !message) return;
      
      this._lastActivity = Date.now();
      
      const wsIds = this.wsClients.get(room);
      if (!wsIds?.size) return;
      
      const msgStr = JSON.stringify(message);
      
      for (const wsId of wsIds) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1) {
          try { ws.send(msgStr); } catch(e) {}
        }
      }
    } catch(e) {}
  }

  // ==================== EVENT HANDLING - TANPA RATE LIMITING ====================

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      
      this._lastActivity = Date.now();
      if (this._isHibernating) this._wakeup();
      
      this._eventQueue.push({ ws, data });
      if (!this._isProcessingQueue) {
        await this._processEventQueue();
      }
    } catch(e) {}
  }

  async _processEventQueue() {
    try {
      if (this._isProcessingQueue || this._eventQueue.length === 0) return;
      
      this._isProcessingQueue = true;
      
      const batch = this._eventQueue.splice(0);
      
      for (const item of batch) {
        try {
          await this._processEventItem(item.ws, item.data);
        } catch(e) {}
      }
      
      if (this._eventQueue.length > 0) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed) this._processEventQueue();
        }, 1);
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

  async _handleEventInternal(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      const evt = data[0];

      if (evt === "switchRoom") {
        const [_, room, username] = data;
        await this.switchRoom(ws, room, username);
        return;
      }

      // ========== START GAME WITH RECORDING (DARI ANDROID) ==========
      if (evt === "startGameWithRecording") {
        const [_, room, bet, username] = data;
        if (!room || !username) {
          this._safeSend(ws, ["gameLowCardError", "Room and username required"]);
          return;
        }
        await this.startGame(ws, bet, username, true);
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
          const result = await this.diceGameSystem.getLeaderboard(limit);
          this._safeSend(ws, ["diceLeaderboard", result]);
        } catch(e) {
          this._safeSend(ws, ["diceLeaderboard", []]);
        }
        return;
      }

      if (evt === "deleteDiceLastWeekWinner") {
        try {
          const success = await this.diceGameSystem.deleteLastWeekWinner();
          if (success) {
            this._safeSend(ws, ["diceLastWeekWinnerDeleted", true]);
            this._broadcastToRoom(DICE_ROOM, ["diceNotification", "Last week winner data deleted"]);
          } else {
            this._safeSend(ws, ["diceLastWeekWinnerDeleted", false]);
          }
        } catch(e) {
          this._safeSend(ws, ["diceLastWeekWinnerDeleted", false]);
        }
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
          
          this._safeSend(ws, ["cachedResetStatus", {
            currentWeek: currentWeek,
            lastResetWeek: lastResetWeek || 'never',
            needsReset: lastResetWeek !== currentWeek,
            fromCache: this._resetWeekCache !== null,
            timestamp: Date.now()
          }]);
        } catch(e) {
          this._safeSend(ws, ["cachedResetStatus", { error: e.message }]);
        }
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
        const isRecordingEnabled = await this._getRecordingStatusFromKV(roomName);
        this._safeSend(ws, ["recordingStatus", isRecordingEnabled]);
        return;
      }

      if (evt === "getRoomWinners") {
        const room = data[1] || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
        if (!room) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const winners = await this._getLowCardWinners(room);
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room }]);
        return;
      }

      if (evt === "lowCardWinnerUpdate") {
        const room = data[1] || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
        if (!room) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        await this._broadcastLowCardWinners(room);
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

  async _broadcastLowCardWinners(room) {
    try {
      if (!room) return;
      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled) return;
      const winners = await this._getLowCardWinners(room);
      this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
    } catch(e) {}
  }

  // ==================== WEBSOCKET EVENTS ====================

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
        this._diceTimeLeftNotified.delete(wsId);
        this._nextDiceNotified.delete(wsId);
        this._diceJoinedNotified.delete(wsId);
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

  async webSocketError(ws) {
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
        this._diceTimeLeftNotified.delete(wsId);
        this._nextDiceNotified.delete(wsId);
        this._diceJoinedNotified.delete(wsId);
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

  async webSocketMessage(ws, msg) {
    try {
      if (!ws || ws._closing || this.closing || this.isDestroyed || !ws._wsId) return;
      this._lastActivity = Date.now();
      if (this._isHibernating) this._wakeup();
      
      const data = JSON.parse(msg);
      if (Array.isArray(data) && data.length > 0) {
        await this.handleEvent(ws, data);
      }
    } catch(e) {
      this._handleError('webSocketMessage', e);
      this._safeSend(ws, ["gameLowCardError", "Server is recovering"]);
    }
  }

  // ==================== ERROR HANDLING ====================

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

  _forceRecovery() {
    try {
      if (this.closing || this.isDestroyed) return;
      if (this._recoveryAttempts >= this._maxRecoveryAttempts) return;
      
      this._resetCriticalState();
      this._cleanupDeadConnections();
      
      if (!this._initialized && !this._initializing) {
        this._initAsync();
      }
      if (this._isDiceTime()) {
        this.diceAutoEnabled = true;
      }
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "Server has recovered"]);
    } catch(e) {}
  }

  // ==================== FETCH ====================

  async fetch(req) {
    try {
      if (this.closing || this.isDestroyed) {
        return new Response("Server is shutting down", { status: 503 });
      }
      
      this._lastActivity = Date.now();
      
      const url = new URL(req.url);
      
      if (url.pathname === "/health") {
        try {
          const status = {
            status: "ok",
            uptime: Date.now() - this._startTime,
            alarm: {
              lastAlarm: this._lastAlarm,
              alarmCount: this._alarmCount,
              isHibernating: this._isHibernating,
              idleTime: Date.now() - this._lastActivity,
              wakeupAttempts: this._wakeupAttempts
            },
            cache: {
              kvCacheSize: this._kvCache?.cache?.size || 0,
              recordingCacheSize: this._recordingEnabled.size,
              resetWeekCached: this._resetWeekCache !== null,
              lastWeekWinnerCached: this._cachedLastWeekWinner !== null,
              diceScoresLoaded: this.diceGameSystem._isLoaded,
              diceScoresCount: this.diceGameSystem.userScores.size,
              leaderboardCached: this.diceGameSystem._leaderboardCache !== null
            },
            games: {
              running: this.activeGames.size,
              diceActive: !!this.currentDiceRoll,
              diceRound: this._diceRound || 0,
              diceCooldown: this._diceTimeUpCooldown,
              tieActive: this._tieActive,
              tieRound: this._tieRound
            },
            connections: {
              ws: this.wsMap.size,
              eventQueue: this._eventQueue?.length || 0
            },
            timestamp: Date.now()
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
          
          try { this.state.acceptWebSocket(server); } 
          catch(e) { return new Response("WebSocket acceptance failed", { status: 500 }); }
          
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
          
          server.addEventListener("close", () => { this.webSocketClose(server); });
          server.addEventListener("error", () => { this.webSocketError(server); });
          
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

  // ==================== DESTROY ====================

  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
      if (this._alarmInterval) { clearInterval(this._alarmInterval); this._alarmInterval = null; }
      if (this._forceWakeupTimer) { clearInterval(this._forceWakeupTimer); this._forceWakeupTimer = null; }
      if (this._heartbeatInterval) { clearInterval(this._heartbeatInterval); this._heartbeatInterval = null; }
      
      for (const [room, game] of this.activeGames) {
        await this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();
      
      for (const [wsId, ws] of this.wsMap) {
        try { if (ws && ws.readyState === 1) ws.close(1000, "Server shutting down"); } catch(e) {}
      }
      this.wsMap.clear();
      this.wsClients.clear();
      this.clientRooms.clear();
      
      if (this._kvCache) this._kvCache.clear();
      this._recordingEnabled.clear();
      this._cachedLastWeekWinner = null;
      this._resetWeekCache = null;
      
      await this.resetDice();
      this.diceGameSystem.clearCache();
      
    } catch(e) {}
  }
}
