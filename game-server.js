// ==================== GAME-SERVER-HIBERNATION-FIXED.js ====================
// ✅ WEBSOCKET HIBERNATION API
// ✅ DO BISA TIDUR SAAT TIDAK AKTIF
// ✅ HEMAT KUOTA DURATION
// ✅ TANPA addEventListener
// ✅ PAKAI ctx.acceptWebSocket()
// ✅ FIX: Game bisa start setelah selesai

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
  MAX_WS_CLIENTS: 200,
  MAX_EVENT_QUEUE_SIZE: 1000,
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
  
  KV_TIMEOUT_MS: 2000,
  BROADCAST_BATCH_SIZE: 20,
  ALARM_INTERVAL_MS: 60000,
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

// ==================== KV CACHE - SYNC SYSTEM ====================
class KVCache {
  constructor() {
    this.cache = new Map();
    this._initialized = false;
  }

  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;
    return entry.value;
  }

  set(key, value) {
    this.cache.set(key, { value });
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

  getAll() {
    const result = {};
    for (const [key, entry] of this.cache) {
      result[key] = entry.value;
    }
    return result;
  }

  replaceAll(data) {
    this.cache.clear();
    for (const [key, value] of Object.entries(data)) {
      this.cache.set(key, { value });
    }
  }

  get size() {
    return this.cache.size;
  }

  keys() {
    return Array.from(this.cache.keys());
  }

  values() {
    const result = [];
    for (const [, entry] of this.cache) {
      result.push(entry.value);
    }
    return result;
  }

  entries() {
    const result = [];
    for (const [key, entry] of this.cache) {
      result.push([key, entry.value]);
    }
    return result;
  }
}

// ==================== DICE GAME SYSTEM - SYNC ====================
class DiceGameSystem {
  constructor(gameServer) {
    this.gameServer = gameServer;
    this.env = gameServer.env;
    this._isLoaded = false;
    this._dataVersion = 0;
  }

  async loadScores() {
    try {
      if (this._isLoaded) return true;
      if (!this.env?.QUESTIONS) return false;
      
      const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.gameServer._kvCache.replaceAll(points);
      
      this._isLoaded = true;
      this._dataVersion++;
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async getPoints() {
    if (!this._isLoaded) {
      await this.loadScores();
    }
    return this.gameServer._kvCache.getAll();
  }

  async getUserScore(username) {
    if (!this._isLoaded) {
      await this.loadScores();
    }
    const allPoints = this.gameServer._kvCache.getAll();
    return allPoints[username] || 0;
  }

  async setPoints(points) {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      this.gameServer._kvCache.replaceAll(points);
      await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      
      this._isLoaded = true;
      this._dataVersion++;
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async updateUserScore(username, points) {
    try {
      if (!this.env?.QUESTIONS) return false;
      if (!username) return false;
      
      let allPoints = this.gameServer._kvCache.getAll();
      allPoints[username] = (allPoints[username] || 0) + points;
      
      this.gameServer._kvCache.replaceAll(allPoints);
      await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(allPoints));
      
      this._dataVersion++;
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
      return winnerData;
    } catch(e) { 
      return null; 
    }
  }

  async setLastWeekWinner(winnerData) {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      this.gameServer._cachedLastWeekWinner = winnerData;
      await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winnerData));
      
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async deleteLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return false;
      
      this.gameServer._cachedLastWeekWinner = null;
      await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
      
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async getResetWeek() {
    if (this.gameServer._cachedResetWeek !== null) {
      return this.gameServer._cachedResetWeek;
    }
    
    if (this.env?.QUESTIONS) {
      const week = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
      if (week) {
        this.gameServer._cachedResetWeek = week;
      }
      return week;
    }
    return null;
  }

  async setResetWeek(week) {
    this.gameServer._cachedResetWeek = week;
    if (this.env?.QUESTIONS) {
      await this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, week);
    }
    return true;
  }

  rollDice() { 
    return Math.floor(Math.random() * 6) + 1; 
  }
  
  clearCache() { 
    this.gameServer._kvCache.clear();
    this._isLoaded = false;
    this._dataVersion++;
  }

  async reload() {
    this._isLoaded = false;
    await this.loadScores();
  }
}

// ==================== RECORDING SYSTEM - SYNC ====================
class RecordingSystem {
  constructor(gameServer) {
    this.gameServer = gameServer;
    this.env = gameServer.env;
    this._recordingEnabled = new Map();
    this._isLoaded = false;
  }

  async getStatus(roomName) {
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
  }

  async setStatus(roomName, enabled) {
    if (!roomName) return false;
    
    this._recordingEnabled.set(roomName, enabled);
    if (this.env?.QUESTIONS) {
      await this.env.QUESTIONS.put(
        CONSTANTS.LOWCARD_RECORDING_KEY + roomName, 
        enabled ? 'true' : 'false'
      );
    }
    return true;
  }

  async deleteStatus(roomName) {
    if (!roomName) return false;
    
    this._recordingEnabled.delete(roomName);
    if (this.env?.QUESTIONS) {
      await this.env.QUESTIONS.delete(CONSTANTS.LOWCARD_RECORDING_KEY + roomName);
    }
    return true;
  }

  async getWinners(room) {
    if (!room || !this.env?.QUESTIONS) return {};
    
    const cacheKey = CONSTANTS.LOWCARD_WINNER_KEY + room;
    
    if (this.gameServer._kvCache.has(cacheKey)) {
      return this.gameServer._kvCache.get(cacheKey) || {};
    }
    
    const isRecording = await this.getStatus(room);
    if (!isRecording) return {};
    
    const winners = await this.env.QUESTIONS.get(cacheKey, 'json');
    const result = winners && typeof winners === 'object' ? winners : {};
    this.gameServer._kvCache.set(cacheKey, result);
    
    return result;
  }

  async addWinner(room, username) {
    try {
      if (!room || !username || room === DICE_ROOM) return false;
      
      const isRecording = await this.getStatus(room);
      if (!isRecording) return false;
      if (!this.env?.QUESTIONS) return false;
      
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      let roomWinners = this.gameServer._kvCache.get(key) || {};
      
      let count = 0;
      if (roomWinners[username]) {
        count = parseInt(String(roomWinners[username]).replace("x", "").replace("X", "")) || 0;
      }
      roomWinners[username] = (count + 1) + "x";
      
      this.gameServer._kvCache.set(key, roomWinners);
      await this.env.QUESTIONS.put(key, JSON.stringify(roomWinners));
      
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async deleteWinners(room) {
    if (!room) return false;
    
    const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
    this.gameServer._kvCache.delete(key);
    if (this.env?.QUESTIONS) {
      await this.env.QUESTIONS.delete(key);
    }
    return true;
  }
}

// ==================== GAME SERVER - HIBERNATION ====================
export class GameServer {
  constructor(state, env) {
    try {
      // ==================== STATE ====================
      this.state = state;
      this.env = env;
      this.ctx = state;  // ✅ UNTUK HIBERNATION API
      this.closing = false;
      this.isDestroyed = false;

      // ==================== GAME ====================
      this.activeGames = new Map();
      this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;

      // ==================== WEBSOCKET ====================
      this._wsIdCounter = 0;
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.wsMap = new Map();
      this.userConnections = new Map();
      this._cleanupTimers = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();

      // ==================== DICE ====================
      this.diceAnswered = new Set();
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.currentDiceRoll = null;
      this._diceStartTime = null;
      this._diceTimeout = null;
      this._diceStartTimeout = null;
      this.diceAutoEnabled = false;
      this._isShowingDice = false;
      this._canSubmitDiceAnswer = false;
      this._diceQuestionStartTime = null;
      this._diceRound = 0;
      this._diceTimeUpCooldown = false;
      this._diceTimeUpCooldownTimer = null;
      this._diceOutOfTimeShown = false;
      this.diceEndNotified = false;
      
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._lastSentRemaining = -1;

      // ==================== TIE BREAKER ====================
      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tieActive = false;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._playerAnswers = new Map();

      // ==================== CACHE SYSTEM ====================
      this._cachedResetWeek = null;
      this._cachedLastWeekWinner = null;
      this._kvCache = new KVCache();
      
      this.diceGameSystem = new DiceGameSystem(this);
      this.recordingSystem = new RecordingSystem(this);

      // ==================== EVENT QUEUE ====================
      this._eventQueue = [];
      this._isProcessingQueue = false;
      this._diceTaskRunning = false;

      // ==================== TIMER MANAGEMENT ====================
      this._allTimers = new Set();
      this._lastHeartbeat = Date.now();
      this._startTime = Date.now();
      this._tickCount = 0;
      this._lastErrorReset = Date.now();
      this._errorCount = 0;

      // ==================== LOAD KV KE CACHE ====================
      this._loadAllCache();

      // ==================== START DICE ====================
      const startDiceTimer = setTimeout(() => {
        if (!this.closing && !this.isDestroyed && !this._isShowingDice) {
          this.forceStartDice();
        }
      }, 3000);
      this._trackTimer(startDiceTimer);

      // ==================== SET ALARM PERTAMA ====================
      this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_INTERVAL_MS);

    } catch(e) {}
  }

  // ==================== ALARM - GANTI SEMUA INTERVAL ====================
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    try {
      // 1. Health check (batasi 20 WS per alarm)
      this._performHealthCheckLight();
      
      // 2. Dice - cek hanya jika ada player
      const clients = this.wsClients.get(DICE_ROOM);
      if (clients?.size > 0) {
        const isDiceTime = this._isDiceTime();
        if (isDiceTime && !this.currentDiceRoll && !this._isShowingDice && !this._diceTimeUpCooldown) {
          this.forceStartDice();
        } else if (!isDiceTime && this.currentDiceRoll) {
          this.resetDice();
        }
      }
      
      // 3. Cleanup game (hanya kadang-kadang)
      this._tickCount++;
      if (this._tickCount % 6 === 0) {
        this._cleanupStaleGamesLight();
        this._cleanupDeadConnectionsLight();
      }
      
      // 4. Check stuck games
      if (this._tickCount % 3 === 0) {
        this._checkStuckGamesLight();
      }
      
      // 5. Schedule next alarm
      this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_INTERVAL_MS);
      
    } catch(e) {
      // Jika error, tetap set alarm
      try {
        this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_INTERVAL_MS);
      } catch(e2) {}
    }
  }

  // ==================== LOAD ALL DATA TO CACHE ====================
  async _loadAllCache() {
    try {
      if (this.closing || this.isDestroyed) return;
      
      const currentWeek = this._generateCurrentWeek(new Date());
      if (this._cachedResetWeek === null) {
        const existing = await this.diceGameSystem.getResetWeek();
        if (!existing && this.env?.QUESTIONS) {
          this._cachedResetWeek = currentWeek;
          this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek).catch(() => {});
        }
      }
      
      await this.diceGameSystem.loadScores();
      
      if (this._cachedLastWeekWinner === null) {
        await this.diceGameSystem.getLastWeekWinner();
      }
      
    } catch(e) {}
  }

  // ==================== TIMER MANAGEMENT ====================
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

  // ==================== KV TIMEOUT ====================
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

  // ==================== GENERATE WEEK ====================
  _generateCurrentWeek(date) {
    const now = date || new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
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

  // ==================== HEALTH CHECK RINGAN ====================
  _performHealthCheckLight() {
    try {
      const now = Date.now();
      let checked = 0;
      const dead = [];
      
      for (const [wsId, ws] of this.wsMap) {
        if (checked > 20) break;
        checked++;
        if (!ws || ws.readyState !== 1 || ws._closing) {
          dead.push(wsId);
        }
      }
      
      for (const wsId of dead) {
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

  // ==================== CLEANUP RINGAN ====================
  _cleanupStaleGamesLight() {
    try {
      const now = Date.now();
      let cleaned = 0;
      for (const [room, game] of this.activeGames) {
        if (cleaned > 5) break;
        cleaned++;
        if (game._gameEnded) {
          const endTime = game._endTime || game._createdAt || now;
          if ((now - endTime) > CONSTANTS.STALE_GAME_TIMEOUT_MS) {
            this._scheduleGameCleanup(room, game);
          }
        }
      }
    } catch(e) {}
  }

  _cleanupDeadConnectionsLight() {
    try {
      let removed = 0;
      const toRemove = [];
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

  _checkStuckGamesLight() {
    try {
      const now = Date.now();
      let checked = 0;
      const toEvaluate = [], toClose = [];
      
      for (const [room, game] of this.activeGames) {
        if (checked > 5) break;
        checked++;
        if (!game?._isActive || game._gameEnded) continue;
        
        if (game._phase === 'draw' && game._drawPhaseStart &&
            (now - game._drawPhaseStart) > CONSTANTS.STUCK_DRAW_TIMEOUT_MS) {
          toEvaluate.push({ room, game });
        }
        if (game._phase === 'registration' && game.registrationOpen &&
            game._createdAt && (now - game._createdAt) > CONSTANTS.STUCK_REGISTRATION_TIMEOUT_MS) {
          toClose.push({ room, game });
        }
      }
      
      for (const item of toEvaluate) this._closeDrawPhase(item.room, item.game);
      for (const item of toClose) this._closeRegistration(item.room, item.game);
    } catch(e) {}
  }

  // ==================== DICE TIME ====================
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
      const currentTotal = this._getCurrentWITATime().totalMinutes;
      let minDiff = Infinity;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        let diff = session.start * 60 - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) minDiff = diff;
      }
      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      return { hours, minutes, text: `${hours}h ${minutes}m`, isRunning: this._isDiceTime() };
    } catch(e) { return { hours: 0, minutes: 0, text: '0h 0m', isRunning: false }; }
  }

  // ==================== DICE CORE ====================
  
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
      if (this._tieActive || this._isShowingDice || this._diceTimeUpCooldown) return;
      if (!this._isDiceTime() || this.isDestroyed || this._diceStartTimeout || this.currentDiceRoll) return;
      
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
        this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
        this._lastSentRemaining = -1;
        
        await this._broadcastDiceRoll(diceValue);
        
        this._broadcastDiceNotification({
          message: "♡ clik draw ♡",
          remaining: 20,
          round: this._diceRound
        });
        
        this._startDiceTimerNotifications();
        
        this._clearTimer(this._diceTimeout);
        this._diceTimeout = this._trackTimer(setTimeout(async () => {
          try {
            if (this.closing || this.isDestroyed || this._tieActive) {
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
            const roundNumber = this._diceRound || 1;
            
            this._stopDiceTimerNotifications();
            
            if (this.diceHasWinner && this.diceWinner) {
              const correctPlayers = [];
              for (const player of this.diceAnswered) {
                if (this._playerAnswers.get(player) === this.currentDiceRoll?.value) {
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
              points[this.diceWinner] = (points[this.diceWinner] || 0) + 1;
              
              this._kvCache.replaceAll(points);
              await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
              
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
        }, CONSTANTS.DICE_TOTAL_TIME_MS));
        
      } catch(e) {
        this._isShowingDice = false;
        this.currentDiceRoll = null;
        this._canSubmitDiceAnswer = false;
      }
    } catch(e) {}
  }

  // ==================== DICE TIMER NOTIFICATIONS ====================
  
  _startDiceTimerNotifications() {
    this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
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
        this._broadcastDiceNotification({ remaining: remainingInt, message, round: this._diceRound || 1 });
      }
      
      if (remainingInt <= 0) {
        this._stopDiceTimerNotifications();
        this._startTimeUpCooldown();
      } else {
        const timer = setTimeout(() => this._diceTimerTick(), 1000);
        this._trackTimer(timer);
      }
      
    } catch(e) {}
  }

  _stopDiceTimerNotifications() {
    this._diceNotifiedFlags = { 20: true, 10: true, 5: true, timeup: true };
  }

  _startTimeUpCooldown() {
    if (this._diceTimeUpCooldown) return;
    
    this._diceTimeUpCooldown = true;
    this._broadcastDiceNotification({ message: "wait 15s", remaining: 15, cooldown: true });
    
    this._clearTimer(this._diceTimeUpCooldownTimer);
    this._diceTimeUpCooldownTimer = this._trackTimer(setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._showDiceQuestion();
    }, 15000));
  }

  // ==================== GET DICE POINTS - DARI CACHE ====================
  async _getDicePoints() {
    try {
      if (!this.env?.QUESTIONS) return {};
      return this._kvCache.getAll();
    } catch(e) { return {}; }
  }

  // ==================== SUBMIT DICE ANSWER ====================
  async submitDiceAnswer(ws, username, guess) {
    try {
      if (!ws || !username) return;
      const room = this._ensureRoomConsistency(ws);
      if (room !== DICE_ROOM || !this._isDiceTime()) return;
      
      const guessValue = parseInt(guess, 10);
      if (isNaN(guessValue) || guessValue < 1 || guessValue > 6) {
        this._safeSend(ws, ["diceError", "invalid guess 1-6"]);
        return;
      }
      
      if (this._tieActive) {
        if (!this._tiePlayers.includes(username) || this._tieAnswers.has(username) || !this._canSubmitDiceAnswer) return;
        
        this._tieAnswers.set(username, guessValue);
        this.diceAnswered.add(username);
        this._broadcastToRoom(DICE_ROOM, ["diceAnswer", {
          username, guess: guessValue, round: this._diceRound || 1,
          isTieBreaker: true, tieRound: this._tieRound
        }]);
        
        if (this._tieAnswers.size === this._tiePlayers.length) {
          this._clearTimer(this._tieTimer);
          this._clearTimer(this._tieInterval);
          this._canSubmitDiceAnswer = false;
          this._isShowingDice = false;
          
          const tieId = this._getActiveTieBreakerId();
          if (tieId) {
            const timer = setTimeout(async () => {
              await this._processTieResults(DICE_ROOM, tieId, this._tiePlayers);
            }, 500);
            this._trackTimer(timer);
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
      
      this._playerAnswers.set(username, guessValue);
      this.diceAnswered.add(username);
      this._broadcastToRoom(DICE_ROOM, ["diceAnswer", {
        username, guess: guessValue, round: this._diceRound || 1
      }]);
      
      if (guessValue === diceValue && !this.diceHasWinner) {
        this.diceHasWinner = true;
        this.diceWinner = username;
        
        const points = await this._getDicePoints();
        points[username] = (points[username] || 0) + 1;
        
        this._kvCache.replaceAll(points);
        await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      }
    } catch(e) {}
  }

  // ==================== TIE BREAKER ====================

  async _startTieBreaker(room, players) {
    if (!players || players.length < 2 || this._tieActive) return;
    
    this._tieActive = true;
    this._tieRound = 0;
    this._tiePlayers = [...players];
    this._tieAnswers = new Map();
    
    const id = `tie_${Date.now()}`;
    this._tieBreakers.set(id, { players, round: 0, winner: null, status: 'waiting' });
    await this._runTieRound(room, id, players);
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
    
    this._broadcastToRoom(DICE_ROOM, ["diceNotification", `♡ Round ${this._tieRound}: ${players.join(', ')}`]);
    
    this._canSubmitDiceAnswer = true;
    this._diceQuestionStartTime = Date.now();
    this.diceAnswered = new Set();
    this._isShowingDice = true;
    
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
      if (timeLeft === 3) this._broadcastToRoom(DICE_ROOM, ["diceNotification", "3s remaining"]);
      
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
        else if (answer === highest) highestPlayers.push(player);
      }
    }
    
    if (highestPlayers.length === 0) {
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "No one answered"]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    
    if (highestPlayers.length === 1) {
      const winner = highestPlayers[0];
      
      const points = await this._getDicePoints();
      points[winner] = (points[winner] || 0) + 1;
      
      this._kvCache.replaceAll(points);
      await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      
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
    const points = await this._getDicePoints();
    points[winner] = (points[winner] || 0) + 1;
    
    this._kvCache.replaceAll(points);
    await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
    
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
    this._broadcastDiceNotification({ message: "wait 15s", remaining: 15, cooldown: true });
    this._diceTimeUpCooldown = true;
    
    this._clearTimer(this._diceTimeUpCooldownTimer);
    this._diceTimeUpCooldownTimer = this._trackTimer(setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._showDiceQuestion();
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

  // ==================== BROADCAST DICE ====================
  
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
        round: this._diceRound || 1
      };
      
      await this._broadcastToRoom(DICE_ROOM, ["diceRoll", msgData]);
    } catch(e) {}
  }

  _broadcastDiceNotification(data) {
    try {
      if (this._tieActive && !data?.isTieBreaker) return;
      
      const wsIds = this.wsClients.get(DICE_ROOM);
      if (!wsIds?.size) return;
      
      const now = Date.now();
      const message = data.message || "";
      const remaining = data.remaining !== undefined ? data.remaining : -1;
      
      let key = `dice_${remaining}`;
      if (remaining === -1) key = `dice_msg_${message.substring(0, 20)}`;
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

  // ==================== RESET DICE ====================
  async resetDice() {
    try {
      this._clearTimer(this._diceTimeout);
      this._clearTimer(this._diceStartTimeout);
      this._clearTimer(this._diceTimeUpCooldownTimer);
      this._stopDiceTimerNotifications();
      
      this.currentDiceRoll = null;
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.diceAnswered = new Set();
      this._diceStartTime = null;
      this.diceEndNotified = false;
      this._isShowingDice = false;
      this._canSubmitDiceAnswer = false;
      this._diceQuestionStartTime = null;
      this._diceOutOfTimeShown = false;
      this._diceTimeUpCooldown = false;
      this._lastSentRemaining = -1;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      
      this._playerAnswers = new Map();
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieActive = false;
      this._tieBreakers.clear();
      this._tieRound = 0;
      
      this._clearTimer(this._tieTimer);
      this._clearTimer(this._tieInterval);
      this._tieTimer = null;
      this._tieInterval = null;
    } catch(e) {}
  }

  // ==================== WS HELPERS ====================

  _getWsId(ws) { return ws?._wsId || null; }

  _ensureRoomConsistency(ws) {
    try {
      if (!ws) return null;
      const wsId = this._getWsId(ws);
      if (!wsId) return null;
      let room = ws.room || ws.roomname || this.clientRooms.get(wsId) || null;
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

  _addClient(room, ws, username = null) {
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
      if (clients) { clients.delete(wsId); if (clients.size === 0) this.wsClients.delete(room); }
    } catch(e) {}
  }

  _removeClient(room, ws) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      if (!wsId) return;
      const username = ws.username;
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

  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  // ==================== BROADCAST DENGAN BATCH ====================
  async _broadcastToRoom(room, message) {
    try {
      if (this.closing || this.isDestroyed || !room || !message) return;
      const wsIds = this.wsClients.get(room);
      if (!wsIds?.size) return;
      
      const msgStr = JSON.stringify(message);
      const wsIdArray = Array.from(wsIds);
      const BATCH_SIZE = CONSTANTS.BROADCAST_BATCH_SIZE || 20;
      
      for (let i = 0; i < wsIdArray.length; i += BATCH_SIZE) {
        const batch = wsIdArray.slice(i, i + BATCH_SIZE);
        for (const wsId of batch) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1) {
            try { ws.send(msgStr); } catch(e) {}
          }
        }
      }
    } catch(e) {}
  }

  // ==================== SWITCH ROOM ====================
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
        if (roomName === DICE_ROOM) this._sendDiceNotificationOnSwitch(ws, wsId);
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
        
        if (username) {
          let conn = this.userConnections.get(username);
          if (conn) { conn.room = roomName; conn.wsId = wsId; conn.ws = ws; conn.timestamp = Date.now(); }
          else { this.userConnections.set(username, { wsId, ws, room: roomName, timestamp: Date.now() }); }
        }
        
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
        if (roomName === DICE_ROOM) this._sendDiceNotificationOnSwitch(ws, wsId);
        this._broadcastToRoom(roomName, ["userJoinedRoom", username, roomName]);
        if (currentRoom && currentRoom !== roomName) {
          this._broadcastToRoom(currentRoom, ["userLeftRoom", username, currentRoom]);
        }
      } finally {
        const unlockTimer = setTimeout(() => {
          this._switchLocks.delete(lockKey);
          this._switchRetries.delete(lockKey);
        }, 2000);
        this._trackTimer(unlockTimer);
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

  _sendDiceNotificationOnSwitch(ws, wsId) {
    try {
      const isGameActive = this.currentDiceRoll && this._canSubmitDiceAnswer;
      if (isGameActive) {
        const elapsed = (Date.now() - this._diceStartTime) / 1000;
        const remaining = Math.max(0, Math.floor(CONSTANTS.DICE_TOTAL_TIME_MS / 1000 - elapsed));
        if (remaining > 0) {
          let displayTime = "";
          if (remaining >= 20) displayTime = "20s remaining";
          else if (remaining >= 10) displayTime = "10s remaining";
          else if (remaining >= 5) displayTime = "5s remaining";
          else displayTime = `${remaining}s remaining`;
          this._safeSend(ws, ["diceNotification", displayTime]);
        }
      } else {
        const timeLeft = this._getTimeLeftUntilNextDice();
        const timer = setTimeout(() => {
          if (!this.closing && !this.isDestroyed && ws && ws.readyState === 1) {
            this._safeSend(ws, ["diceNotification", `Dice game ended. Next session in: ${timeLeft.text}`]);
          }
        }, 5000);
        this._trackTimer(timer);
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

  // ==================== LOW CARD GAME ====================

  _isGameActuallyRunning(game) { return game?._isActive === true && !game?._gameEnded; }

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

  _getRandomCardTanda() { return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; }

  _getRandomDrawDelay() { return (Math.floor(Math.random() * 14) + 2) * 1000; }

  _getBotNumberByRound(round) {
    if (round <= 2) return Math.floor(Math.random() * 12) + 1;
    return Math.random() < 0.6 ?
      [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
      [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
  }

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
        if (gameToDelete) this._deleteGame(room, gameToDelete);
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS));
      this._cleanupTimers.set(room, timer);
    } catch(e) {}
  }

  // ✅ FIX: _deleteGame - HAPUS GAME DENGAN BENAR
  _deleteGame(room, game) {
    try {
      if (!room || !game) return;
      if (game?._isActive && !game._gameEnded) return;
      
      // Hapus cleanup timer
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      
      // Cleanup game
      if (game) {
        game._gameEnded = true;
        game._isActive = false;
        game.playerWsId = null;
        this._cleanupGame(game);
      }
      
      // ✅ HAPUS DARI activeGames
      this.activeGames.delete(room);
      
      // ✅ BROADCAST GAME ENDED
      this._broadcastToRoom(room, ["gameLowCardEnd", "Game has ended"]);
      
      // ✅ RESET STATE PLAYER - BIAR BISA START LAGI!
      for (const wsId of this.wsClients.get(room) || []) {
        const ws = this.wsMap.get(wsId);
        if (ws) {
          ws.inGame = false;
          ws.currentGame = null;
          this._safeSend(ws, ["gameLowCardIdle", "Game ended. Start new with /start [bet]"]);
        }
      }
      
    } catch(e) {}
  }

  // ✅ FIX: _forceCleanupGame - FORCE HAPUS GAME
  async _forceCleanupGame(room, game) {
    try {
      if (!game) return;
      
      // Hapus semua timer
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) { this._clearTimer(game[key]); game[key] = null; }
      }
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) this._clearTimer(id);
        game._botTimeouts.clear();
        game._botTimeouts = null;
      }
      
      // Tandai game selesai
      game._gameEnded = true;
      game._isActive = false;
      game._endTime = Date.now();
      
      // ✅ HAPUS DARI activeGames
      this.activeGames.delete(room);
      
      // ✅ BROADCAST
      this._broadcastToRoom(room, ["gameLowCardEnd", "Game force ended"]);
      
      // ✅ RESET STATE PLAYER
      for (const wsId of this.wsClients.get(room) || []) {
        const ws = this.wsMap.get(wsId);
        if (ws) {
          ws.inGame = false;
          ws.currentGame = null;
          this._safeSend(ws, ["gameLowCardIdle", "Game ended. Start new with /start [bet]"]);
        }
      }
      
      // Hapus cleanup timer
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      
    } catch(e) {}
  }

  _cleanupGame(game) {
    try {
      if (!game) return;
      if (game._isActive && !game._gameEnded) return;
      
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) { this._clearTimer(game[key]); game[key] = null; }
      }
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) this._clearTimer(id);
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
        const timeout = this._trackTimer(setTimeout(() => {
          const currentGame = this.activeGames.get(room);
          if (this._isGameActuallyRunning(currentGame) && !currentGame.drawTimeExpired &&
              !currentGame.evaluationLocked && !currentGame.numbers?.has(botId) && !currentGame.eliminated?.has(botId)) {
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

  _closeRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      game.registrationOpen = false;
      if (game._registrationTimer) { this._clearTimer(game._registrationTimer); game._registrationTimer = null; }
      
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
        this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }

  async _startDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) this._clearTimer(id);
        game._botTimeouts.clear();
      }
      
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
            
            await this.recordingSystem.addWinner(room, winner);
            const winners = await this.recordingSystem.getWinners(room);
            this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
            
            game._gameEnded = true;
            game._isActive = false;
            this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
            this._scheduleGameCleanup(room, game);
          } else {
            game._gameEnded = true;
            game._isActive = false;
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

  _closeDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || game.evaluationLocked) return;
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) this._forceBotDraw(room, botId, game);
      }
      
      this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
      const evalTimer = this._trackTimer(setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            this._evaluateRound(room, game);
          }
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
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) this._clearTimer(id);
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
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
        game._gameEnded = true;
        game._isActive = false;
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (entries.length === 1 && eliminated.size >= activeIds.length - 1) {
        const winnerId = entries[0][0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        
        await this.recordingSystem.addWinner(room, winnerName);
        const winners = await this.recordingSystem.getWinners(room);
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      const activePlayerIds = this._getActivePlayerIds(game);
      if (game.numbers.size < activePlayerIds.length) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
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
        
        await this.recordingSystem.addWinner(room, winnerName);
        const winners = await this.recordingSystem.getWinners(room);
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (remaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        game._gameEnded = true;
        game._isActive = false;
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
      
      if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
      if (this._isGameActuallyRunning(game) && !game._gameEnded) this._startDrawPhase(room, game);
      
    } catch(e) {}
  }

  // ==================== GAME START - FIXED ====================

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

      const isRecordingEnabled = await this.recordingSystem.getStatus(room);
      if (isRecordingEnabled) {
        this._safeSend(ws, ["gameLowCardError", "Recording is ACTIVE in this room. Users cannot start games."]);
        return;
      }

      // ✅ FIX: CEK DAN CLEANUP GAME LAMA
      const existingGame = this.activeGames.get(room);
      if (existingGame) {
        // Jika game sudah selesai, force cleanup dulu
        if (existingGame._gameEnded || !existingGame._isActive) {
          await this._forceCleanupGame(room, existingGame);
        } else if (existingGame._isActive) {
          // Game masih berjalan
          this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
          return;
        }
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
        _startedByRecording: false, _startedBy: 'user'
      };
      
      game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
      game.playerWsId.set(usernameClean, wsId);
      this.activeGames.set(room, game);
      this._addClient(room, ws, usernameClean);
      this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
      this._broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
      this._startRegistration(room, game);
      
    } catch(e) {}
  }

  async _forceCleanupGame(room, game) {
    try {
      if (!game) return;
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) { this._clearTimer(game[key]); game[key] = null; }
      }
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) this._clearTimer(id);
        game._botTimeouts.clear();
      }
      game._gameEnded = true;
      game._isActive = false;
      game._endTime = Date.now();
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
      this.activeGames.delete(room);
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
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
      if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
        this._safeSend(ws, ["gameLowCardError", "Game is full"]);
        return;
      }
      game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
      this._addClient(room, ws, usernameClean);
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
      const activePlayers = this._getActivePlayers(game);
      if (activePlayers.length === 0) {
        const allPlayers = Array.from(game.players.keys());
        const submitted = Array.from(game.numbers?.keys() || []);
        const notSubmitted = allPlayers.filter(id => !submitted.includes(id) && !game.eliminated?.has(id));
        if (notSubmitted.length > 0) return;
        game._gameEnded = true;
        game._isActive = false;
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
        
        await this.recordingSystem.addWinner(room, winner);
        const winners = await this.recordingSystem.getWinners(room);
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }

  // ==================== EVENT HANDLING ====================

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      if (this._eventQueue.length > CONSTANTS.MAX_EVENT_QUEUE_SIZE) {
        this._eventQueue.splice(0, this._eventQueue.length - CONSTANTS.MAX_EVENT_QUEUE_SIZE);
      }
      this._eventQueue.push({ ws, data });
      if (!this._isProcessingQueue) await this._processEventQueue();
    } catch(e) {}
  }

  async _processEventQueue() {
    try {
      if (this._isProcessingQueue || this._eventQueue.length === 0) return;
      this._isProcessingQueue = true;
      
      const batchSize = 5;
      const batch = this._eventQueue.splice(0, batchSize);
      
      for (const item of batch) {
        try { await this._processEventItem(item.ws, item.data); } catch(e) {}
      }
      
      if (this._eventQueue.length > 0) {
        const nextTimer = setTimeout(() => {
          if (!this.closing && !this.isDestroyed) this._processEventQueue();
        }, 1);
        this._trackTimer(nextTimer);
      }
    } catch(e) { } 
    finally { this._isProcessingQueue = false; }
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

      if (evt === "submitDiceAnswer") {
        const [_, username, guess] = data;
        await this.submitDiceAnswer(ws, username, guess);
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
          const points = await this._getDicePoints();
          const sorted = Object.entries(points).sort((a, b) => b[1] - a[1]).slice(0, limit);
          this._safeSend(ws, ["diceLeaderboard", sorted.map(([u, s]) => `${u}|${s}`)]);
        } catch(e) { this._safeSend(ws, ["diceLeaderboard", []]); }
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
        const success = await this.recordingSystem.setStatus(roomName, true);
        this._safeSend(ws, ["startRecordingResult", { success, message: success ? "Recording enabled" : "Failed" }]);
        if (success) this._broadcastToRoom(roomName, ["recordingStatus", true]);
        return;
      }

      if (evt === "stopRecordingWinners") {
        const roomName = data[1];
        if (!roomName) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const success = await this.recordingSystem.setStatus(roomName, false);
        this._safeSend(ws, ["stopRecordingResult", { success, message: success ? "Recording stopped" : "Failed" }]);
        if (success) this._broadcastToRoom(roomName, ["recordingStatus", false]);
        return;
      }

      if (evt === "getRecordingStatus") {
        const roomName = data[1];
        if (!roomName) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const isRecording = await this.recordingSystem.getStatus(roomName);
        this._safeSend(ws, ["recordingStatus", isRecording]);
        return;
      }

      if (evt === "sendWinnersToRoom" || evt === "lowCardWinnerUpdate") {
        const room = data[1] || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
        if (!room) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const winners = await this.recordingSystem.getWinners(room);
        this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        this._safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners refreshed" }]);
        return;
      }

      if (evt === "getRoomWinners") {
        const room = data[1] || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
        if (!room) { this._safeSend(ws, ["recordingError", "Room name required"]); return; }
        const isRecording = await this.recordingSystem.getStatus(room);
        const winners = await this.recordingSystem.getWinners(room);
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

  async _startGameWithRecording(ws, room, bet, username) {
    try {
      if (!room || !username) {
        this._safeSend(ws, ["gameLowCardError", "Room and username required"]);
        return;
      }

      const isRecordingEnabled = await this.recordingSystem.getStatus(room);
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
      if (this.activeGames.size >= this._maxGames) {
        this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
        return;
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

  // ==================== LAST WEEK WINNER ====================
  async _getLastWeekWinnerAndReset() {
    try {
      if (!this.env?.QUESTIONS) return null;
      
      const currentWeek = this._generateCurrentWeek(new Date());
      const lastResetWeek = await this.diceGameSystem.getResetWeek();
      const weekChanged = lastResetWeek && this._compareWeeks(currentWeek, lastResetWeek) > 0;
      
      if (!lastResetWeek || weekChanged) {
        const points = await this.diceGameSystem.getPoints();
        
        let winner = null, highestScore = 0;
        let participants = 0;
        for (const [username, score] of Object.entries(points)) {
          const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
          participants++;
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
            participants: participants,
            timestamp: Date.now()
          };
          
          this._cachedLastWeekWinner = winnerData;
          await this._withTimeout(
            this.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winnerData)),
            3000
          );
          
          this._broadcastToRoom(DICE_ROOM, ["diceNotification", 
            `🏆 Last week winner: ${winner} with ${highestScore} points!`
          ]);
        } else {
          this._cachedLastWeekWinner = null;
          await this._withTimeout(this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER), 3000);
        }
        
        const emptyPoints = {};
        this._kvCache.replaceAll(emptyPoints);
        await this._withTimeout(
          this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(emptyPoints)),
          3000
        );
        
        await this.diceGameSystem.setResetWeek(currentWeek);
        
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "🔄 Weekly points have been reset!"]);
        
        return this._cachedLastWeekWinner;
      }
      
      return this._cachedLastWeekWinner;
      
    } catch(e) {
      return null;
    }
  }

  // ==================== FETCH - DENGAN HIBERNATION ====================

  async fetch(req) {
    try {
      if (this.closing || this.isDestroyed) {
        return new Response("Server is shutting down", { status: 503 });
      }
      
      const url = new URL(req.url);
      
      if (url.pathname === "/health") {
        const status = {
          status: "ok",
          uptime: Date.now() - this._startTime,
          diceActive: !!this.currentDiceRoll,
          diceRound: this._diceRound || 0,
          gamesRunning: this.activeGames.size,
          wsConnections: this.wsMap.size,
          eventQueueSize: this._eventQueue?.length || 0,
          timestamp: Date.now(),
          currentWITATime: this._getCurrentWITATime().hours + ":" + String(this._getCurrentWITATime().minutes).padStart(2, '0'),
          lastResetWeek: this._cachedResetWeek || 'unknown',
          tieActive: this._tieActive,
          cacheSize: this._kvCache.size
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
        
        // ✅ PAKAI wsMap.size LANGSUNG
        if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
          return new Response("Server at maximum capacity", { status: 503 });
        }
        
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        const wsId = ++this._wsIdCounter;
        
        server._wsId = wsId;
        server._closing = false;
        server.room = null;
        server.roomname = null;
        server._createdAt = Date.now();
        server.username = null;
        
        // ✅ HIBERNATION API: pakai ctx.acceptWebSocket()
        try { 
          this.ctx.acceptWebSocket(server); 
        } catch(e) { 
          return new Response("WebSocket acceptance failed", { status: 500 }); 
        }
        
        // ✅ TANPA addEventListener!
        // WebSocket events handled by:
        //    - webSocketMessage() 
        //    - webSocketClose()
        //    - webSocketError()
        // Ini membuat DO bisa hibernasi dan menghemat kuota duration!
        
        this.wsMap.set(server._wsId, server);
        
        return new Response(null, { status: 101, webSocket: client });
      }
      
      return new Response("Game Server", { status: 200 });
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ==================== WEBSOCKET HANDLERS (HIBERNATION) ====================

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
      }
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
      
      const clients = this.wsClients.get(DICE_ROOM);
      if (clients?.size > 0) this.ensureDiceRunning();
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
      this._safeSend(ws, ["gameLowCardError", "Server is recovering"]);
    }
  }

  ensureDiceRunning() {
    try {
      if (this._isShowingDice || this._diceTimeUpCooldown) return;
      this._forceStartDiceIfTime();
      if (!this.currentDiceRoll && !this._diceTimeout && !this._diceStartTimeout && !this._isShowingDice) {
        this.forceStartDice();
      }
    } catch(e) {}
  }

  _forceStartDiceIfTime() {
    try {
      if (this._isShowingDice || this._diceTimeUpCooldown) return;
      if (!this._isDiceTime() || this.currentDiceRoll || this._diceTimeout || this._diceStartTimeout) return;
      this.diceAutoEnabled = true;
      this._showDiceQuestion();
    } catch(e) {}
  }

  // ==================== DESTROY ====================
  
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
      this._stopDiceTimerNotifications();
      
      this._clearTimer(this._tieTimer);
      this._clearTimer(this._tieInterval);
      
      this._cachedResetWeek = null;
      this._cachedLastWeekWinner = null;
      this._kvCache.clear();
      
      for (const [room, game] of this.activeGames) {
        await this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();
      
      for (const [room, timer] of this._cleanupTimers) {
        this._clearTimer(timer);
      }
      this._cleanupTimers.clear();
      
      await this.resetDice();
      this.diceGameSystem.clearCache();
      
      for (const [wsId, ws] of this.wsMap) {
        try { if (ws && ws.readyState === 1) ws.close(1000, "Server shutting down"); } catch(e) {}
      }
      this.wsMap.clear();
      this.wsClients.clear();
      this.clientRooms.clear();
      
    } catch(e) {}
  }
}
