// ==================== GAME-SERVER-HEMAT-DURABLE.JS ====================
// ✅ TANPA CPU CHECK
// ✅ SINGLE MASTER INTERVAL
// ✅ BROADCAST BUFFERING
// ✅ FULL CACHE (TANPA TTL - SINKRON DENGAN KV)
// ✅ THROTTLED NOTIFICATIONS
// ✅ GLOBAL GAME SCHEDULER
// ✅ OPTIMAL UNTUK DURABLE OBJECT
// ✅ PERBAIKAN WEEKLY RESET
// ✅ PERBAIKAN CRASH
// ✅ PERBAIKAN ROUND TIDAK HILANG
// ✅ TANPA LOG / CONSOLE

// ==================== CONSTANTS ====================
const C = {
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
  MASTER_INTERVAL_MS: 10000,
  BROADCAST_BUFFER_FLUSH_MS: 100,
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

// ==================== BROADCAST BUFFER ====================
class BroadcastBuffer {
  constructor() {
    this.buffers = new Map();
    this.flushTimers = new Map();
  }

  add(room, message) {
    if (!this.buffers.has(room)) {
      this.buffers.set(room, []);
    }
    this.buffers.get(room).push(message);
    
    if (!this.flushTimers.has(room)) {
      const timer = setTimeout(() => this.flush(room), C.BROADCAST_BUFFER_FLUSH_MS);
      this.flushTimers.set(room, timer);
    }
  }

  flush(room) {
    const messages = this.buffers.get(room);
    if (!messages || messages.length === 0) {
      this.flushTimers.delete(room);
      return;
    }
    
    this.buffers.delete(room);
    this.flushTimers.delete(room);
    return messages;
  }

  flushAll() {
    const result = new Map();
    for (const [room, messages] of this.buffers) {
      if (messages.length > 0) {
        result.set(room, messages);
      }
    }
    this.buffers.clear();
    for (const [room, timer] of this.flushTimers) {
      clearTimeout(timer);
    }
    this.flushTimers.clear();
    return result;
  }

  clear() {
    this.buffers.clear();
    for (const timer of this.flushTimers.values()) {
      clearTimeout(timer);
    }
    this.flushTimers.clear();
  }
}

// ==================== CACHE (TANPA TTL - SINKRON DENGAN KV) ====================
class GameCache {
  constructor() {
    this.cache = new Map();
  }

  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;
    return entry.value;
  }

  set(key, value) {
    this.cache.set(key, {
      value: value,
      timestamp: Date.now()
    });
  }

  delete(key) {
    this.cache.delete(key);
  }

  has(key) {
    return this.cache.has(key);
  }

  clear() {
    this.cache.clear();
  }
}

// ==================== THROTTLED NOTIFICATIONS ====================
class ThrottledNotifier {
  constructor() {
    this.lastNotification = new Map();
    this.cooldown = 3000;
  }

  canSend(key) {
    const now = Date.now();
    const last = this.lastNotification.get(key);
    if (!last) {
      this.lastNotification.set(key, now);
      return true;
    }
    if (now - last < this.cooldown) {
      return false;
    }
    this.lastNotification.set(key, now);
    return true;
  }

  reset(key) {
    this.lastNotification.delete(key);
  }

  clear() {
    this.lastNotification.clear();
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
    this._cache = gameServer._cache;
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
      
      const cached = this._cache.get('dice_points');
      if (cached) {
        this.userScores.clear();
        for (const [username, score] of Object.entries(cached)) {
          this.userScores.set(username, score);
        }
        this._isLoaded = true;
        this._loading = false;
        return true;
      }
      
      const points = await env.QUESTIONS.get(C.DICE_POINT_KEY, 'json') || {};
      this._cache.set('dice_points', points);
      
      this.userScores.clear();
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
      
      const cached = this._cache.get('dice_points');
      if (cached) {
        this.userScores.clear();
        for (const [username, score] of Object.entries(cached)) {
          this.userScores.set(username, score);
        }
        return cached;
      }
      
      const points = await this.env.QUESTIONS.get(C.DICE_POINT_KEY, 'json') || {};
      this._cache.set('dice_points', points);
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
      
      // 1. UPDATE KV
      await this.env.QUESTIONS.put(C.DICE_POINT_KEY, JSON.stringify(points));
      
      // 2. UPDATE CACHE (langsung, sama dengan KV)
      this._cache.set('dice_points', points);
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
      
      const cached = this._cache.get('dice_last_week_winner');
      if (cached) {
        this.gameServer._cachedLastWeekWinner = cached;
        return cached;
      }
      
      const winnerData = await this.env.QUESTIONS.get(C.DICE_LAST_WEEK_WINNER, 'json');
      if (winnerData) {
        this._cache.set('dice_last_week_winner', winnerData);
        this.gameServer._cachedLastWeekWinner = winnerData;
      }
      return winnerData;
    } catch(e) {
      return null;
    }
  }

  async setLastWeekWinner(winner) {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      // 1. UPDATE KV
      await this.env.QUESTIONS.put(C.DICE_LAST_WEEK_WINNER, JSON.stringify(winner));
      
      // 2. UPDATE CACHE (langsung, sama dengan KV)
      this._cache.set('dice_last_week_winner', winner);
      this.gameServer._cachedLastWeekWinner = winner;
      return true;
    } catch(e) {
      return false;
    }
  }

  async deleteLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      // 1. DELETE KV
      await this.env.QUESTIONS.delete(C.DICE_LAST_WEEK_WINNER);
      
      // 2. DELETE CACHE (langsung)
      this._cache.delete('dice_last_week_winner');
      this.gameServer._cachedLastWeekWinner = null;
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
    this._cache.delete('dice_points');
    this._cache.delete('dice_last_week_winner');
  }
}

// ==================== GAME SERVER ====================
export class GameServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    this._initialized = false;
    this._initializing = false;

    this._restartCount = 0;
    this._lastRestartTime = 0;
    this._startTime = Date.now();
    this._lastHeartbeat = Date.now();
    this._errorCount = 0;
    this._lastErrorReset = Date.now();
    this._isRecovering = false;
    this._recoveryAttempts = 0;
    this._maxRecoveryAttempts = 3;
    this._lastRecoveryTime = 0;

    this._winnerProcessed = false;

    // Active games
    this.activeGames = new Map();
    this._maxGames = C.MAX_LOWCARD_GAMES;
    this._gameLocks = new Map();
    this._joinLocks = new Map();
    this._switchLocks = new Map();
    this._switchRetries = new Map();

    // WebSocket management
    this._wsIdCounter = 0;
    this.wsClients = new Map();
    this.clientRooms = new Map();
    this.wsMap = new Map();
    this.roomViewers = new Map();
    this.userConnections = new Map();
    this._cleanupTimers = new Map();
    this._gameStartFlags = new Map();

    // Dice state
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
    this.diceAutoTimer = null;
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

    this._diceNotified20 = false;
    this._diceNotified10 = false;
    this._diceNotified5 = false;
    this._diceNotified3 = false;

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

    // Tie breaker
    this._tieBreakers = new Map();
    this._tieRound = 0;
    this._tieActive = false;
    this._tiePlayers = [];
    this._tieAnswers = new Map();
    this._tieTimer = null;
    this._tieInterval = null;
    this._playerAnswers = new Map();
    this._processingTieResults = false;

    // Cache
    this._cachedResetWeek = null;
    this._cachedResetWeekTimestamp = 0;
    this._cachedLastWeekWinner = null;
    this._cachedLastWeekWinnerTimestamp = 0;

    // Components
    this._cache = new GameCache();
    this._broadcastBuffer = new BroadcastBuffer();
    this._notifier = new ThrottledNotifier();
    this.diceGameSystem = new DiceGameSystem(this);

    // === SINGLE MASTER INTERVAL ===
    this._masterInterval = setInterval(() => {
      if (this.closing || this.isDestroyed) {
        clearInterval(this._masterInterval);
        this._masterInterval = null;
        return;
      }
      
      this._flushBroadcastBuffer();
      this._performHealthCheck();
      this._diceKeepAliveTask();
      this._checkStuckGames();
      this._cleanupStaleGames();
      this._cleanupDeadConnections();
      this._diceTimerTask();
      this._checkAndResetWeeklyDice();
      
    }, C.MASTER_INTERVAL_MS);

    // Init async
    this._initAsync();

    // Start dice
    setTimeout(() => {
      if (!this.closing && !this.isDestroyed && !this._isShowingDice) {
        this._forceStartDiceIfTime();
      }
    }, 5000);

    // Load scores
    setTimeout(() => {
      if (!this.closing && !this.isDestroyed) {
        this.diceGameSystem.loadScores();
      }
    }, 3000);

    // Weekly reset init
    setTimeout(() => {
      if (!this.closing && !this.isDestroyed) {
        this._initResetWeek();
      }
    }, 1000);
  }

  // ==================== CACHE METHODS (SINKRON DENGAN KV) ====================

  async _getRecordingStatus(roomName) {
    if (!roomName) return false;
    
    // 1. Cek CACHE
    const cached = this._cache.get(`recording_${roomName}`);
    if (cached !== null) {
      return cached;
    }
    
    // 2. Baca dari KV
    if (this.env?.QUESTIONS) {
      const kvValue = await this.env.QUESTIONS.get(C.LOWCARD_RECORDING_KEY + roomName);
      const isRecording = kvValue === 'true';
      
      // 3. Simpan ke CACHE
      this._cache.set(`recording_${roomName}`, isRecording);
      return isRecording;
    }
    return false;
  }

  async _setRecordingStatus(roomName, value) {
    if (!roomName) return false;
    
    // 1. UPDATE KV
    if (this.env?.QUESTIONS) {
      await this.env.QUESTIONS.put(
        C.LOWCARD_RECORDING_KEY + roomName,
        value ? 'true' : 'false'
      );
    }
    
    // 2. UPDATE CACHE (langsung, sama dengan KV)
    this._cache.set(`recording_${roomName}`, value);
    
    return true;
  }

  async _getLowCardWinners(room) {
    if (!room || !this.env?.QUESTIONS) return {};
    
    // 1. Cek CACHE
    const cached = this._cache.get(`winners_${room}`);
    if (cached) {
      return cached;
    }
    
    // 2. Baca dari KV
    const key = C.LOWCARD_WINNER_KEY + room;
    const winners = await this.env.QUESTIONS.get(key, 'json') || {};
    
    // 3. Simpan ke CACHE
    if (Object.keys(winners).length > 0) {
      this._cache.set(`winners_${room}`, winners);
    }
    return winners;
  }

  async _addLowCardWinner(room, username) {
    if (!room || !username) return false;
    const isRecording = await this._getRecordingStatus(room);
    if (!isRecording || room === DICE_ROOM) return false;
    if (!this.env?.QUESTIONS) return false;
    
    const key = C.LOWCARD_WINNER_KEY + room;
    
    // 1. Baca dari KV
    let roomWinners = await this.env.QUESTIONS.get(key, 'json') || {};
    
    // 2. Update data
    let currentCount = 0;
    if (roomWinners[username]) {
      const valStr = String(roomWinners[username]);
      currentCount = parseInt(valStr.replace("x", "").replace("X", "")) || 0;
    }
    roomWinners[username] = (currentCount + 1) + "x";
    
    // 3. UPDATE KV
    await this.env.QUESTIONS.put(key, JSON.stringify(roomWinners));
    
    // 4. UPDATE CACHE (langsung, sama dengan KV)
    this._cache.set(`winners_${room}`, roomWinners);
    return true;
  }

  async _getCachedResetWeek() {
    try {
      if (this._cachedResetWeek !== null) {
        return this._cachedResetWeek;
      }
      
      if (this.env?.QUESTIONS) {
        const lastResetWeek = await this.env.QUESTIONS.get(C.DICE_LAST_RESET_WEEK);
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

  async _updateCachedResetWeek(week) {
    // 1. UPDATE CACHE
    this._cachedResetWeek = week;
    this._cachedResetWeekTimestamp = Date.now();
    
    // 2. UPDATE KV
    if (this.env?.QUESTIONS) {
      await this.env.QUESTIONS.put(C.DICE_LAST_RESET_WEEK, week);
    }
  }

  // ==================== BROADCAST BUFFER ====================

  _flushBroadcastBuffer() {
    const messages = this._broadcastBuffer.flushAll();
    for (const [room, msgList] of messages) {
      if (msgList.length === 0) continue;
      const wsIds = this.wsClients.get(room);
      if (!wsIds?.size) continue;
      
      const wsIdArray = Array.from(wsIds);
      const BATCH_SIZE = C.BROADCAST_BATCH_SIZE;
      
      for (let i = 0; i < wsIdArray.length; i += BATCH_SIZE) {
        const batch = wsIdArray.slice(i, i + BATCH_SIZE);
        for (const wsId of batch) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1) {
            for (const msg of msgList) {
              try { ws.send(JSON.stringify(msg)); } catch(e) {}
            }
          }
        }
      }
    }
  }

  _broadcastToRoom(room, message) {
    if (this.closing || this.isDestroyed || !room || !message) return;
    this._broadcastBuffer.add(room, message);
  }

  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  _broadcastDiceNotification(type, data) {
    if (this._tieActive && !data?.isTieBreaker) return;
    
    const message = data.message || "";
    const remaining = data.remaining !== undefined ? data.remaining : -1;
    
    let key = `dice_${remaining}`;
    if (remaining === -1) {
      key = `dice_msg_${message.substring(0, 30)}`;
    }
    if (message === "TIME UP") key = "dice_timeup";
    
    if (!this._notifier.canSend(key)) return;
    
    if (remaining > 0 && this._lastSentRemaining === remaining && !data.cooldown) {
      return;
    }
    
    if (remaining > 0) this._lastSentRemaining = remaining;
    this._broadcastToRoom(DICE_ROOM, ["diceNotification", message]);
  }

  // ==================== DICE KEEP ALIVE ====================

  _diceKeepAliveTask() {
    this._lastHeartbeat = Date.now();
    
    if (!this._isDiceTime()) {
      if (!this.diceEndNotified) {
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
        this.diceEndNotified = true;
      }
      return;
    }
    
    if (this.diceEndNotified) {
      this.diceEndNotified = false;
      this._broadcastDiceNotification("diceError", {
        message: "Dice game is starting",
        isDiceTime: true,
        isActive: false,
        remaining: -1
      });
    }
  }

  _diceTimerTask() {
    if (this._tieActive) return;
    if (!this._isDiceTime()) return;
    
    if (!this.currentDiceRoll && !this._diceTimeout && 
        !this._isShowingDice && !this._diceTimeUpCooldown) {
      const clients = this.wsClients.get(DICE_ROOM);
      if (clients?.size > 0) {
        this._showDiceQuestion();
      }
    }
  }

  // ==================== DICE METHODS ====================

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
        if (diff < minDiff) minDiff = diff;
      }
      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      return { 
        hours, 
        minutes, 
        totalMs: minDiff * 60 * 1000,
        text: `${hours}h ${minutes}m`,
        isRunning: this._isDiceTime()
      };
    } catch(e) {
      return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false };
    }
  }

  _forceStartDiceIfTime() {
    if (this._isShowingDice) return;
    if (this._diceTimeUpCooldown) return;
    if (!this._isDiceTime() || this.currentDiceRoll || this._diceTimeout || this._diceStartTimeout) {
      return;
    }
    this.diceAutoEnabled = true;
    this._showDiceQuestion();
  }

  async _showDiceQuestion() {
    if (this._tieActive || this._isShowingDice || this._diceTimeUpCooldown) return;
    if (!this._isDiceTime()) return;
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
      
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
      this._lastSentRemaining = -1;
      
      await this._broadcastDiceRoll(diceValue);
      
      this._broadcastDiceNotification("diceError", {
        answerTime: C.DICE_ANSWER_TIME_MS / 1000,
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
            
            const points = await this.diceGameSystem.getPoints();
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
      }, C.DICE_TOTAL_TIME_MS);
      
    } catch(e) {
      this._isShowingDice = false;
      this.currentDiceRoll = null;
      this._canSubmitDiceAnswer = false;
      this._stopDiceTimerNotifications();
    }
  }

  async _broadcastDiceRoll(diceValue) {
    if (this._tieActive) return;
    
    const wsIds = this.wsClients.get(DICE_ROOM);
    if (!wsIds?.size) return;

    const msgData = {
      value: diceValue,
      timestamp: Date.now(),
      answerTime: C.DICE_ANSWER_TIME_MS / 1000,
      canAnswerNow: true,
      message: "♡ clik draw ♡",
      round: this._diceRound || 1,
      timerNotifications: [20, 10, 5]
    };
    
    this._broadcastToRoom(DICE_ROOM, ["diceRoll", msgData]);
  }

  _startDiceTimerNotifications() {
    if (this._diceTimerTimeout) {
      clearTimeout(this._diceTimerTimeout);
      this._diceTimerTimeout = null;
    }
    
    this._diceNotifiedFlags = {
      20: false,
      10: false,
      5: false,
      timeup: false
    };
    
    this._diceTimerTick();
  }

  _diceTimerTick() {
    if (!this.currentDiceRoll || !this._diceQuestionStartTime) return;
    
    const elapsed = (Date.now() - this._diceQuestionStartTime) / 1000;
    const remaining = Math.max(0, C.DICE_ANSWER_TIME_MS / 1000 - elapsed);
    const remainingInt = Math.floor(remaining);
    
    let message = "";
    let shouldSend = false;
    
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
      this._stopDiceTimerNotifications();
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
    
    if (remainingInt > 0 && !this._diceNotifiedFlags.timeup) {
      this._diceTimerTimeout = setTimeout(() => {
        this._diceTimerTick();
      }, 1000);
    }
  }

  _stopDiceTimerNotifications() {
    if (this._diceTimerTimeout) {
      clearTimeout(this._diceTimerTimeout);
      this._diceTimerTimeout = null;
    }
    this._diceNotifiedFlags = {
      20: false,
      10: false,
      5: false,
      timeup: false
    };
    this._lastSentRemaining = -1;
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
      this._diceNotifiedFlags = {
        20: false,
        10: false,
        5: false,
        timeup: false
      };
      this._lastSentRemaining = -1;
      this._forceStartDiceIfTime();
    }, C.TIE_BREAKER_COOLDOWN || 15000);
  }

  // ==================== TIE BREAKER ====================

  async _startTieBreaker(room, players) {
    if (!players || players.length < 2 || this._tieActive) return;
    
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
    
    let timeLeft = C.TIE_BREAKER_TIME_LIMIT || 20;
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
          this._startTimeUpCooldown();
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
          this._startTimeUpCooldown();
        }
      }
    }, (C.TIE_BREAKER_TIME_LIMIT || 20) * 1000 + 2000);
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
      this._startTimeUpCooldown();
      return;
    }
    
    if (highestPlayers.length === 1) {
      const winner = highestPlayers[0];
      const points = await this.diceGameSystem.getPoints();
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
      this._startTimeUpCooldown();
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
    this._startTimeUpCooldown();
  }

  async _processSingleWinner(room, id, winner) {
    const points = await this.diceGameSystem.getPoints();
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
    this._startTimeUpCooldown();
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

  // ==================== SUBMIT DICE ANSWER ====================

  async submitDiceAnswer(ws, username, guess) {
    if (!ws || !username) return;
    
    const room = this._ensureRoomConsistency(ws);
    if (room !== DICE_ROOM) return;
    if (!this._isDiceTime()) return;
    
    const guessValue = parseInt(guess, 10);
    if (isNaN(guessValue) || guessValue < 1 || guessValue > 6) {
      this._safeSend(ws, ["diceError", "invalid guess 1-6"]);
      return;
    }
    
    // Tie breaker mode
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
          this._startTimeUpCooldown();
        }
      }
      return;
    }
    
    // Normal mode
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
      
      const points = await this.diceGameSystem.getPoints();
      points[username] = (points[username] || 0) + 1;
      await this.diceGameSystem.setPoints(points);
    }
  }

  _getDiceAnswerRemainingTime() {
    try {
      if (!this.currentDiceRoll || !this._diceQuestionStartTime) return 0;
      const elapsed = (Date.now() - this._diceQuestionStartTime) / 1000;
      return Math.max(0, Math.round((C.DICE_ANSWER_TIME_MS / 1000) - elapsed));
    } catch(e) { return 0; }
  }

  // ==================== LOW CARD GAME ====================

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

  _isGameActuallyRunning(game) {
    try { 
      return game?._isActive === true && !game?._gameEnded; 
    } catch(e) { 
      return false; 
    }
  }

  _getRandomCardTanda() {
    try { 
      return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; 
    } catch(e) { 
      return "C1"; 
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

  _getRandomDrawDelay() {
    try { 
      return (Math.floor(Math.random() * 14) + 2) * 1000; 
    } catch(e) { 
      return 5000; 
    }
  }

  // ==================== GAME START ====================

  async startGame(ws, bet, username) {
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

    const isRecordingEnabled = await this._getRecordingStatus(room);
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
    if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > C.MAX_BET) {
      this._safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${C.MAX_BET})`]);
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
  }

  async joinGame(ws, username) {
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
    if (game.players.size >= C.MAX_PLAYERS_PER_GAME) {
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
  }

  async submitNumber(ws, number, tanda, username) {
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
      }, C.EVALUATION_DELAY_MS);
    }
  }

  async leaveGame(ws, username) {
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
  }

  // ==================== REGISTRATION & DRAW ====================

  _startRegistration(room, game) {
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
  }

  _closeRegistration(room, game) {
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
        const needed = Math.min(4 - game.players.size, C.MAX_BOTS_PER_GAME);
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
  }

  _addBots(room, count) {
    const game = this.activeGames.get(room);
    if (!this._isGameActuallyRunning(game)) return;
    
    const botNames = ["moz1", "moz2", "moz3", "moz4"];
    const existingBots = Array.from(game.players.keys()).filter(id => id.startsWith('BOT_'));
    const existingBotCount = existingBots.length;
    const maxBotsToAdd = Math.min(count, C.MAX_BOTS_PER_GAME - existingBotCount);
    
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
  }

  _startDrawPhase(room, game) {
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
        const needed = Math.min(4 - activePlayers.length, C.MAX_BOTS_PER_GAME);
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
            this._addLowCardWinner(room, winner);
            const allWinners = this._getLowCardWinners(room);
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
    
    if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
      this._startBotDraws(room, game);
    }
  }

  _startDrawCountdown(room, game) {
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
  }

  _closeDrawPhase(room, game) {
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
    }, C.EVALUATION_DELAY_MS);
  }

  _startBotDraws(room, game) {
    if (!this._isGameActuallyRunning(game) || !game.botPlayers) return;
    if (!game._botTimeouts) game._botTimeouts = new Set();
    
    const notDrawn = Array.from(game.botPlayers.keys())
      .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id))
      .slice(0, C.MAX_BOT_DRAWS_PER_ROUND);
    
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
  }

  _handleBotDraw(room, botId, game) {
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
      }, C.EVALUATION_DELAY_MS);
    }
  }

  _forceBotDraw(room, botId, game) {
    if (!this._isGameActuallyRunning(game) || game.numbers?.has(botId)) return;
    if (game.eliminated?.has(botId)) return;
    
    const number = this._getBotNumberByRound(game.round);
    const tanda = this._getRandomCardTanda();
    game.numbers.set(botId, number);
    game.tanda.set(botId, tanda);
    const botName = game.players.get(botId)?.name || botId;
    this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
  }

  // ==================== EVALUATE ROUND ====================

  async _evaluateRound(room, game) {
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
    }, C.EVALUATION_TIMEOUT_MS);
    
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
  }

  // ==================== UTILITY METHODS ====================

  _removePlayerFromGame(username, room) {
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
  }

  async _checkGameCanContinue(room, game) {
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
  }

  _scheduleGameCleanup(room, game) {
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
    }, C.GAME_CLEANUP_DELAY_MS);
    this._cleanupTimers.set(room, timer);
  }

  _deleteGame(room, game) {
    if (!room || !game) return;
    if (game?._isActive && !game._gameEnded) return;
    if (this._cleanupTimers.has(room)) { 
      clearTimeout(this._cleanupTimers.get(room)); 
      this._cleanupTimers.delete(room); 
    }
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
  }

  _cleanupGame(game) {
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
  }

  async _forceCleanupGame(room, game) {
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
  }

  // ==================== WS MANAGEMENT ====================

  _addClient(room, ws, username = null, isNewConnection = false) {
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
  }

  _removeClientFromRoom(room, wsId) {
    if (!room || !wsId) return;
    const clients = this.wsClients.get(room);
    if (clients) {
      clients.delete(wsId);
      if (clients.size === 0) {
        this.wsClients.delete(room);
      }
    }
  }

  _removeClient(room, ws) {
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
  }

  _getWsId(ws) { return ws?._wsId || null; }

  _ensureRoomConsistency(ws) {
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
  }

  _ensureSingleConnection(room, username, newWs, newWsId) {
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
  }

  async switchRoom(ws, room, username = null) {
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
  }

  _sendDiceNotificationOnSwitch(ws, wsId) {
    this._diceTimeLeftNotified.delete(wsId);
    this._nextDiceNotified.delete(wsId);
    this._diceJoinedNotified.delete(wsId);
    
    const isGameActive = this.currentDiceRoll && this._canSubmitDiceAnswer;
    
    if (isGameActive) {
      const elapsed = (Date.now() - this._diceStartTime) / 1000;
      const totalTime = C.DICE_TOTAL_TIME_MS / 1000;
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
        answerTime: C.DICE_ANSWER_TIME_MS / 1000,
        canAnswerNow: true,
        round: this._diceRound || 1
      }]);
    }
  }

  _sendDiceNotification(ws, type, data) {
    if (!ws || ws.readyState !== 1) return;
    const message = data.message || "";
    this._safeSend(ws, ["diceNotification", message]);
  }

  _sendGameStateToClient(ws, room) {
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
    
    this._getRecordingStatus(room).then(isRecording => {
      if (isRecording !== undefined) {
        this._safeSend(ws, ["recordingStatus", isRecording]);
      }
    }).catch(() => {});
  }

  // ==================== CLEANUP TASKS ====================

  _checkStuckGames() {
    const now = Date.now();
    const toEvaluate = [];
    const toClose = [];
    
    for (const [room, game] of this.activeGames) {
      if (!game?._isActive || game._gameEnded) continue;
      
      if (game._phase === 'draw' && game._drawPhaseStart &&
          (now - game._drawPhaseStart) > C.STUCK_DRAW_TIMEOUT_MS) {
        toEvaluate.push({ room, game });
      }
      
      if (game._phase === 'registration' && game.registrationOpen &&
          game._createdAt && (now - game._createdAt) > C.STUCK_REGISTRATION_TIMEOUT_MS) {
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
  }

  _cleanupStaleGames() {
    const now = Date.now();
    for (const [room, game] of this.activeGames) {
      if (!game) continue;
      if (game._isActive && !game._gameEnded) continue;
      if (game._gameEnded) {
        const endTime = game._endTime || game._createdAt || now;
        if ((now - endTime) > C.STALE_GAME_TIMEOUT_MS) this._scheduleGameCleanup(room, game);
        continue;
      }
      if (!game._isActive && !game._gameEnded && game._createdAt && (now - game._createdAt) > 300000) {
        game._gameEnded = true;
        game._endTime = now;
        this._scheduleGameCleanup(room, game);
      }
    }
  }

  _cleanupDeadConnections() {
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
  }

  _performHealthCheck() {
    const now = Date.now();
    this._lastHeartbeat = now;
    
    if (this._isDiceTime() && this.currentDiceRoll && this._diceStartTime) {
      const elapsed = (now - this._diceStartTime) / 1000;
      if (elapsed > (C.DICE_TOTAL_TIME_MS / 1000) + 30) {
        this.currentDiceRoll = null;
        this._diceTimeout = null;
        this._isShowingDice = false;
        this._canSubmitDiceAnswer = false;
        this._stopDiceTimerNotifications();
      }
    }
  }

  // ==================== WEEKLY RESET ====================

  async _initResetWeek() {
    try {
      if (!this.env?.QUESTIONS) return;
      
      const existingResetWeek = await this.env.QUESTIONS.get(C.DICE_LAST_RESET_WEEK);
      const currentWeek = this.diceGameSystem.generateCurrentWeek();
      
      if (!existingResetWeek) {
        await this.env.QUESTIONS.put(C.DICE_LAST_RESET_WEEK, currentWeek);
        this._cachedResetWeek = currentWeek;
        this._cachedResetWeekTimestamp = Date.now();
        return;
      }
      
      this._cachedResetWeek = existingResetWeek;
      this._cachedResetWeekTimestamp = Date.now();
      
    } catch(e) {}
  }

  async _checkAndResetWeeklyDice() {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      const now = new Date();
      const currentWeek = this.diceGameSystem.generateCurrentWeek();
      
      let lastResetWeek = this._cachedResetWeek;
      if (!lastResetWeek) {
        lastResetWeek = await this.env.QUESTIONS.get(C.DICE_LAST_RESET_WEEK);
        if (lastResetWeek) {
          this._cachedResetWeek = lastResetWeek;
          this._cachedResetWeekTimestamp = Date.now();
        } else {
          await this.env.QUESTIONS.put(C.DICE_LAST_RESET_WEEK, currentWeek);
          this._cachedResetWeek = currentWeek;
          this._cachedResetWeekTimestamp = Date.now();
          return false;
        }
      }
      
      if (lastResetWeek === currentWeek) return false;
      
      const dayOfWeek = now.getUTCDay();
      const hours = now.getUTCHours();
      const minutes = now.getUTCMinutes();
      const isMonday = dayOfWeek === 1;
      const isResetTime = hours === 0 && minutes === 0;
      
      if (isMonday && isResetTime) {
        const points = await this.env.QUESTIONS.get(C.DICE_POINT_KEY, 'json') || {};
        
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
          
          // UPDATE KV & CACHE
          await this.diceGameSystem.setLastWeekWinner(winnerData);
          
        } else {
          await this.diceGameSystem.deleteLastWeekWinner();
        }
        
        // RESET POIN - UPDATE KV & CACHE
        await this.diceGameSystem.setPoints({});
        
        // UPDATE RESET WEEK - UPDATE KV & CACHE
        await this._updateCachedResetWeek(currentWeek);
        
        return true;
      }
      
      return false;
      
    } catch(e) {
      return false;
    }
  }

  // ==================== INIT ====================

  async _initAsync() {
    try {
      if (this._initializing) return;
      if (this._initialized && !this._isRecovering) return;
      this._initializing = true;
      
      await this.diceGameSystem.loadScores();
      await this._initResetWeek();
      
      this._initialized = true;
      this._initializing = false;
      this._errorCount = 0;
      this._isRecovering = false;
      this._diceInitAttempts = 0;
    } catch(e) {
      this._initializing = false;
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

  // ==================== DESTROY ====================

  async destroy() {
    if (this.isDestroyed) return;
    this.isDestroyed = true;
    this.closing = true;
    
    if (this._masterInterval) {
      clearInterval(this._masterInterval);
      this._masterInterval = null;
    }
    
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
    if (this._diceTimerTimeout) {
      clearTimeout(this._diceTimerTimeout);
      this._diceTimerTimeout = null;
    }
    if (this._tieTimer) {
      clearTimeout(this._tieTimer);
      this._tieTimer = null;
    }
    if (this._tieInterval) {
      clearInterval(this._tieInterval);
      this._tieInterval = null;
    }
    
    this._cache.clear();
    this._broadcastBuffer.clear();
    this._notifier.clear();
    this.diceGameSystem.clearCache();
    
    for (const [room, game] of this.activeGames) {
      await this._forceCleanupGame(room, game);
    }
    this.activeGames.clear();
    
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
    this.userConnections.clear();
    this.roomViewers.clear();
    this._cleanupTimers.clear();
  }

  // ==================== FETCH ====================

  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Server is shutting down", { status: 503 });
    }
    
    const url = new URL(req.url);
    
    if (url.pathname === "/health") {
      const status = {
        status: "ok",
        uptime: Date.now() - this._startTime,
        restartCount: this._restartCount,
        isRestarting: this._isRecovering,
        diceActive: !!this.currentDiceRoll,
        diceRound: this._diceRound || 0,
        diceCooldown: this._diceTimeUpCooldown,
        gamesRunning: this.activeGames.size,
        wsConnections: this.wsMap.size,
        errorCount: this._errorCount,
        timestamp: Date.now(),
        diceSchedule: QUIZ_SCHEDULE.SESSIONS.map(s => `${s.start}:00-${s.end}:00`),
        currentWITATime: this._getCurrentWITATime().formatted,
        lastResetWeek: this._cachedResetWeek || 'unknown',
        currentWeek: this.diceGameSystem.generateCurrentWeek(),
        tieActive: this._tieActive,
        tieRound: this._tieRound,
        tiePlayers: this._tiePlayers.length
      };
      return new Response(JSON.stringify(status), {
        headers: { 'Content-Type': 'application/json' }
      });
    }
    
    if (url.pathname === "/game/ws") {
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("WebSocket only", { status: 400 });
      }
      
      if (this.wsMap.size >= C.MAX_WS_CLIENTS) {
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
        
        this.state.acceptWebSocket(server);
        
        server.addEventListener("message", async (event) => {
          try {
            const data = JSON.parse(event.data);
            if (Array.isArray(data) && data.length > 0) {
              await this._handleEvent(ws, data);
            }
          } catch(e) {}
        });
        
        server.addEventListener("close", () => {
          this._webSocketClose(server);
        });
        
        server.addEventListener("error", () => {
          this._webSocketError(server);
        });
        
        return new Response(null, { status: 101, webSocket: client });
      } catch(e) {
        return new Response("WebSocket creation failed", { status: 500 });
      }
    }
    
    return new Response("Game Server", { status: 200 });
  }

  // ==================== WS EVENT HANDLING ====================

  async _handleEvent(ws, data) {
    if (this.isDestroyed || !ws || !data?.[0]) return;
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
        const points = await this.env.QUESTIONS.get(C.DICE_POINT_KEY, 'json') || {};
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

    if (evt === "getDiceStatus") {
      const isActive = !!this.currentDiceRoll && this._canSubmitDiceAnswer;
      this._safeSend(ws, ["diceStatus", isActive, this._diceRound || 1]);
      return;
    }

    if (evt === "startRecordingWinners") {
      const roomName = data[1];
      if (!roomName) {
        this._safeSend(ws, ["recordingError", "Room name required"]);
        return;
      }
      
      const success = await this._setRecordingStatus(roomName, true);
      
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
      
      const success = await this._setRecordingStatus(roomName, false);
      
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
      
      const isRecordingEnabled = await this._getRecordingStatus(roomName);
      this._safeSend(ws, ["recordingStatus", isRecordingEnabled]);
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
  }

  async _startGameWithRecording(ws, room, bet, username) {
    if (!room || !username) {
      this._safeSend(ws, ["gameLowCardError", "Room and username required"]);
      return;
    }

    const isRecordingEnabled = await this._getRecordingStatus(room);
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
    if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > C.MAX_BET) {
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
  }

  checkGameRunning(ws, roomname) {
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
  }

  // ==================== WS CLOSE/ERROR ====================

  _webSocketClose(ws) {
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
  }

  _webSocketError(ws) {
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
  }

  async webSocketMessage(ws, msg) {
    if (!ws || ws._closing || this.closing || this.isDestroyed || !ws._wsId) return;
    try {
      const data = JSON.parse(msg);
      if (Array.isArray(data) && data.length > 0) {
        await this._handleEvent(ws, data);
      }
    } catch(e) {}
  }
}
