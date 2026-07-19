// ==================== GAME-SERVER.JS (REALTIME ONLY - NO HISTORY) ====================

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
  BATCH_SIZE: 3,
  ALARM_10_DETIK: 10000,
  CLEANUP_TIK: 90,
  STALE_GAME_TIMEOUT_MS: 600000,
  STUCK_DRAW_TIMEOUT_MS: 60000,
  STUCK_REGISTRATION_TIMEOUT_MS: 30000,
  QUIZ_INTERVAL_MS: 20000,
  QUIZ_TIME_LIMIT_MS: 15000,
  TRANSLATE_LIMIT: 30,
  QUIZ_BREAK_MS: 2000,
  QUIZ_START_DELAY_MS: 5000,
  MAX_RETRY_INIT_QUIZ: 3,
  MAX_BROADCAST_BATCH: 5,
  MAX_SHUTDOWN_WAIT_MS: 5000,
  MAX_WS_CLIENTS: 100,
  MAX_ARRAY_SIZE: 100,
  CIRCUIT_BREAKER_THRESHOLD: 2,
  CIRCUIT_BREAKER_TIMEOUT_MS: 30000,
  QUIZ_SWITCH_DELAY_MS: 3000,
  QUIZ_POINT_KEY: 'quiz_points',
  QUIZ_WEEK_KEY: 'quiz_current_week',
  QUIZ_LAST_WEEK_WINNER: 'quiz_last_week_winner',
  SCHEDULER_INTERVAL_MS: 120000,
};

const QUIZ_SCHEDULE = {
  SESSION1: { start: 0, end: 2 },
  SESSION2: { start: 4, end: 6 },
  SESSION3: { start: 8, end: 10 },
  SESSION4: { start: 12, end: 14 },
  SESSION5: { start: 16, end: 18 },
  SESSION6: { start: 20, end: 22 },
};

const QUIZ_ROOM = "Quiz";

export class GameServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      this.closing = false;
      this.isDestroyed = false;
      this._initialized = false;
      
      // GAME MAPS
      this.activeGames = new Map();
      this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._switchLocks = new Map();
      
      // WEBSOCKET MAPS
      this._wsIdCounter = 0;
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.wsMap = new Map();
      this.roomViewers = new Map();
      this.userConnections = new Map();
      this.connectionLocks = new Map();
      this._cleanupTimers = new Map();
      this._roomBroadcastCount = new Map();
      this._roomBroadcastReset = new Map();
      this._tikCounter = 0;
      this._gameStartFlags = new Map();
      
      // QUIZ
      this.quizAnswered = new Set();
      this.quizHasWinner = false;
      this.quizWinner = null;
      this.quizTimer = null;
      this.isQuizRunning = false;
      this.currentQuestion = null;
      this.isQuizWaiting = false;
      this.quizQuestionCache = {};
      this.questionTranslations = new Map();
      
      // TRANSLATION
      this.translateCount = 0;
      this.translateDate = new Date().toUTCString();
      this.translateLimitReached = false;
      this.userLanguage = new Map();
      this.userCountry = new Map();
      
      // TIMERS
      this._quizTimeout = null;
      this._translateResetInterval = null;
      this._quizBreakTimeout = null;
      this._quizStartTimeout = null;
      
      this._translationCircuitBreaker = {
        failures: 0,
        lastFailureTime: 0,
        isOpen: false
      };
      
      this.quizAutoEnabled = false;
      this.quizAutoTimer = null;
      
      // ✅ INIT VIA SETTIMEOUT
      setTimeout(() => {
        this._initAsync();
      }, 0);
      
      // ALARM
      if (this.state && this.state.storage) {
        this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_10_DETIK);
      }
      
    } catch(e) {
      console.error("Constructor error:", e);
      throw e;
    }
  }
  
  // ==================== ASYNC INIT ====================
  
  async _initAsync() {
    if (this._initialized) return;
    this._initialized = true;
    
    try {
      await this._initQuiz();
      this._startQuizScheduler();
      await this._checkAndResetWeeklyPoints();
      
      setTimeout(() => {
        this.ensureQuizRunning();
      }, 2000);
      
    } catch(e) {
      console.error("Init async error:", e);
    }
  }
  
  // ==================== UTC TIME HELPERS ====================
  
  _getCurrentUTCTime() {
    return new Date();
  }
  
  _getCurrentUTCHours() {
    return new Date().getUTCHours();
  }
  
  // ==================== WEEKLY HELPERS ====================
  
  _getCurrentWeek() {
    const now = new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(year, 0, 1);
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }
  
  async _getQuizPoints() {
    try {
      if (!this.env || !this.env.QUESTIONS) return {};
      const points = await this.env.QUESTIONS.get(CONSTANTS.QUIZ_POINT_KEY, 'json');
      return points || {};
    } catch(e) {
      return {};
    }
  }
  
  async _getLastWeekWinner() {
    try {
      if (!this.env || !this.env.QUESTIONS) return null;
      const winner = await this.env.QUESTIONS.get(CONSTANTS.QUIZ_LAST_WEEK_WINNER, 'json');
      return winner || null;
    } catch(e) {
      return null;
    }
  }
  
  async _checkAndResetWeeklyPoints() {
    try {
      if (!this.env || !this.env.QUESTIONS) return false;
      
      const currentWeek = this._getCurrentWeek();
      const savedWeek = await this.env.QUESTIONS.get(CONSTANTS.QUIZ_WEEK_KEY);
      
      if (savedWeek !== currentWeek) {
        const points = await this._getQuizPoints();
        
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
            week: savedWeek || currentWeek
          };
          
          await this.env.QUESTIONS.put(
            CONSTANTS.QUIZ_LAST_WEEK_WINNER,
            JSON.stringify(winnerData)
          );
          
          this._broadcastToRoom(QUIZ_ROOM, [
            "quizLastWeekWinner",
            {
              username: winner,
              score: highestScore,
              week: savedWeek || currentWeek
            }
          ]);
        }
        
        await this.env.QUESTIONS.put(CONSTANTS.QUIZ_POINT_KEY, JSON.stringify({}));
        await this.env.QUESTIONS.put(CONSTANTS.QUIZ_WEEK_KEY, currentWeek);
        
        this._broadcastToRoom(QUIZ_ROOM, [
          "quizWeekReset",
          {
            week: currentWeek,
            message: "New quiz week started! Points reset."
          }
        ]);
        
        return true;
      }
      return false;
    } catch(e) {
      return false;
    }
  }
  
  // ==================== QUIZ SCHEDULE ====================
  
  _isQuizTime() {
    const hour = this._getCurrentUTCHours();
    const schedules = [
      QUIZ_SCHEDULE.SESSION1,
      QUIZ_SCHEDULE.SESSION2,
      QUIZ_SCHEDULE.SESSION3,
      QUIZ_SCHEDULE.SESSION4,
      QUIZ_SCHEDULE.SESSION5,
      QUIZ_SCHEDULE.SESSION6
    ];
    for (const schedule of schedules) {
      if (hour >= schedule.start && hour < schedule.end) {
        return true;
      }
    }
    return false;
  }
  
  _getNextQuizStartTime() {
    const now = this._getCurrentUTCTime();
    const schedules = [
      QUIZ_SCHEDULE.SESSION1,
      QUIZ_SCHEDULE.SESSION2,
      QUIZ_SCHEDULE.SESSION3,
      QUIZ_SCHEDULE.SESSION4,
      QUIZ_SCHEDULE.SESSION5,
      QUIZ_SCHEDULE.SESSION6
    ];
    for (const schedule of schedules) {
      const startTime = new Date(now);
      startTime.setUTCHours(schedule.start, 0, 0, 0);
      if (startTime > now) {
        return startTime;
      }
    }
    const tomorrow = new Date(now);
    tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
    tomorrow.setUTCHours(QUIZ_SCHEDULE.SESSION1.start, 0, 0, 0);
    return tomorrow;
  }
  
  _getTimeLeftUntilNextEvent() {
    const now = Date.now();
    const nextStart = this._getNextQuizStartTime();
    const timeLeft = nextStart.getTime() - now;
    
    if (timeLeft <= 0) {
      return { minutes: 0, seconds: 0, isRunning: this._isQuizTime() };
    }
    
    const totalSeconds = Math.floor(timeLeft / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    
    return { minutes, seconds, isRunning: false };
  }
  
  // ==================== QUIZ AUTO SCHEDULER ====================
  
  _startQuizScheduler() {
    if (this.quizAutoTimer) {
      clearInterval(this.quizAutoTimer);
      this.quizAutoTimer = null;
    }
    this.quizAutoTimer = setInterval(() => {
      try {
        if (this.closing || this.isDestroyed) {
          clearInterval(this.quizAutoTimer);
          this.quizAutoTimer = null;
          return;
        }
        this._checkQuizAutoStatus();
      } catch(e) {}
    }, CONSTANTS.SCHEDULER_INTERVAL_MS);
  }
  
  async _checkQuizAutoStatus() {
    try {
      const isQuizTime = this._isQuizTime();
      if (isQuizTime) {
        if (!this.quizAutoEnabled) {
          this.quizAutoEnabled = true;
          await this.startQuizWithDelay(CONSTANTS.QUIZ_START_DELAY_MS);
        }
      } else {
        if (this.quizAutoEnabled) {
          this.quizAutoEnabled = false;
          await this.resetQuiz();
        }
      }
    } catch(e) {}
  }
  
  // ==================== SEND TO USER ====================
  
  _sendQuizTimeLeftToUser(ws) {
    if (!ws || ws.readyState !== 1) return false;
    
    try {
      const isQuizTime = this._isQuizTime();
      const timeLeft = this._getTimeLeftUntilNextEvent();
      const isQuizActive = this.currentQuestion !== null || this._quizTimeout !== null;
      
      let message = "";
      let canType = true;
      
      if (isQuizTime && isQuizActive) {
        message = "Quiz is currently running!";
        canType = false;
      } else if (isQuizTime && !isQuizActive) {
        message = "Quiz will start soon!";
        canType = true;
      } else {
        const minutes = timeLeft.minutes;
        const seconds = timeLeft.seconds;
        
        if (minutes >= 60) {
          const hours = Math.floor(minutes / 60);
          const remainingMinutes = minutes % 60;
          message = `Quiz starts in ${hours}h ${remainingMinutes}m ${seconds}s`;
        } else if (minutes > 0) {
          message = `Quiz starts in ${minutes}m ${seconds}s`;
        } else {
          message = `Quiz starts in ${seconds}s!`;
        }
        canType = true;
      }
      
      this._safeSend(ws, ["quizTimeLeft", message, canType]);
      return true;
    } catch(e) {
      return false;
    }
  }
  
  _sendQuizErrorWithTime(ws, errorType, customMessage = null) {
    if (!ws || ws.readyState !== 1) return false;
    
    try {
      const timeLeft = this._getTimeLeftUntilNextEvent();
      let message = "";
      
      switch(errorType) {
        case "NOT_QUIZ_TIME":
          message = "Quiz is currently offline";
          break;
        case "QUIZ_DISABLED":
          message = "Quiz is not available right now";
          break;
        case "QUIZ_ENDED":
          message = "Quiz session has ended";
          break;
        case "QUIZ_NOT_STARTED":
          const timeStr = timeLeft.minutes > 0 ? 
            `${timeLeft.minutes}m ${timeLeft.seconds}s` : 
            `${timeLeft.seconds}s`;
          message = `Quiz hasn't started yet. Starting in: ${timeStr}`;
          break;
        default:
          message = customMessage || "Quiz error occurred";
      }
      
      this._safeSend(ws, ["quizError", message]);
      return true;
    } catch(e) {
      return false;
    }
  }
  
  // ==================== SWITCH ROOM ====================
  
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
    
    const lockKey = `switch_${wsId}`;
    if (this._switchLocks.has(lockKey)) return;
    this._switchLocks.set(lockKey, Date.now());
    
    try {
      const oldRoom = this.clientRooms.get(wsId);
      
      if (oldRoom === roomName) {
        if (roomName === QUIZ_ROOM) {
          if (!this.quizQuestionCache['en'] || this.quizQuestionCache['en'].length === 0) {
            await this._initQuiz();
          }
          setTimeout(() => {
            try {
              if (this.closing || this.isDestroyed) return;
              this._sendQuizTimeLeftToUser(ws);
            } catch(e) {}
          }, CONSTANTS.QUIZ_SWITCH_DELAY_MS);
          this._startQuizIfNeeded();
        }
        return;
      }
      
      if (oldRoom) {
        this._removeClientFromRoom(oldRoom, wsId);
      }
      
      this._addClient(roomName, ws, username, false);
      ws.room = roomName;
      ws.roomname = roomName;
      ws.username = username;
      
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn) conn.room = roomName;
      }
      
      if (roomName === QUIZ_ROOM) {
        if (!this.quizQuestionCache['en'] || this.quizQuestionCache['en'].length === 0) {
          await this._initQuiz();
        }
        setTimeout(() => {
          try {
            if (this.closing || this.isDestroyed) return;
            this._sendQuizTimeLeftToUser(ws);
          } catch(e) {}
        }, CONSTANTS.QUIZ_SWITCH_DELAY_MS);
        this._startQuizIfNeeded();
      }
      
    } finally {
      this._switchLocks.delete(lockKey);
    }
  }
  
  // ==================== QUIZ CORE ====================
  
  async _showQuestion() {
    try {
      if (!this._isQuizTime() || !this.quizAutoEnabled) return;
      if (this.isDestroyed || this.isQuizWaiting || this._quizStartTimeout || this.currentQuestion) return;

      const clients = this.wsClients.get(QUIZ_ROOM);
      if (!clients || clients.size === 0) return;

      let questions = this.quizQuestionCache['en'];
      if (!questions || questions.length === 0) {
        const loaded = await this._loadQuestionsFromKV();
        if (!loaded) return;
        questions = this.quizQuestionCache['en'];
        if (!questions || questions.length === 0) return;
      }

      const maxIndex = Math.min(questions.length, 30);
      const randomIndex = Math.floor(Math.random() * maxIndex);
      const q = questions[randomIndex];
      if (!q || !q.options) return;

      const shuffled = this._shuffleQuestionOptions(q);
      
      this.currentQuestion = {
        ...q,
        options: shuffled.options,
        correct: shuffled.correct
      };
      
      this.quizAnswered = new Set();
      this.quizHasWinner = false;
      this.quizWinner = null;

      await this._broadcastQuizQuestion(
        this.currentQuestion.question,
        this.currentQuestion.options
      );

      if (this._quizTimeout) clearTimeout(this._quizTimeout);
      if (this._quizBreakTimeout) clearTimeout(this._quizBreakTimeout);

      this._quizTimeout = setTimeout(async () => {
        try {
          if (this.closing || this.isDestroyed) {
            this._quizTimeout = null;
            return;
          }

          const currentClients = this.wsClients.get(QUIZ_ROOM);
          if (!currentClients || currentClients.size === 0) {
            this._quizTimeout = null;
            this.currentQuestion = null;
            return;
          }

          if (this.quizHasWinner && this.quizWinner) {
            const points = await this._getQuizPoints();
            points[this.quizWinner] = (points[this.quizWinner] || 0) + 1;
            
            if (this.env && this.env.QUESTIONS) {
              await this.env.QUESTIONS.put(
                CONSTANTS.QUIZ_POINT_KEY,
                JSON.stringify(points)
              );
            }
            
            const totalPoints = points[this.quizWinner] || 0;
            
            this._broadcastToRoom(QUIZ_ROOM, [
              "quizWinner",
              { username: this.quizWinner, totalPoints: totalPoints }
            ]);
          } else {
            this._broadcastToRoom(QUIZ_ROOM, [
              "quizNoWinner",
              { message: "No one answered correctly!" }
            ]);
          }

          this._quizTimeout = null;
          this.isQuizWaiting = true;

          this._quizBreakTimeout = setTimeout(() => {
            try {
              if (this.closing || this.isDestroyed) {
                this._quizBreakTimeout = null;
                return;
              }
              this.isQuizWaiting = false;
              this._quizBreakTimeout = null;
              this.currentQuestion = null;
              if (!this.closing && !this.isDestroyed) {
                this.ensureQuizRunning();
              }
            } catch(e) {
              this.isQuizWaiting = false;
              this._quizBreakTimeout = null;
            }
          }, CONSTANTS.QUIZ_BREAK_MS);

        } catch(e) {
          this._quizTimeout = null;
          this.currentQuestion = null;
          this.isQuizWaiting = false;
        }
      }, CONSTANTS.QUIZ_TIME_LIMIT_MS);

    } catch(e) {
      this.currentQuestion = null;
      this.isQuizWaiting = false;
      this._quizTimeout = null;
    }
  }
  
  async submitQuizAnswer(ws, username, answer) {
    try {
      if (!ws || !username) {
        this._sendQuizErrorWithTime(ws, "ERROR", "Invalid request");
        return;
      }
      
      const room = this._ensureRoomConsistency(ws);
      if (room !== QUIZ_ROOM) {
        this._sendQuizErrorWithTime(ws, "ERROR", "Quiz only available in Quiz room");
        return;
      }
      
      if (!this._isQuizTime()) {
        this._sendQuizErrorWithTime(ws, "NOT_QUIZ_TIME");
        return;
      }
      
      if (!this.quizAutoEnabled) {
        this._sendQuizErrorWithTime(ws, "QUIZ_DISABLED");
        return;
      }
      
      const clients = this.wsClients.get(QUIZ_ROOM);
      if (!clients || clients.size === 0) {
        this._sendQuizErrorWithTime(ws, "ERROR", "Quiz is paused");
        return;
      }
      
      if (!this.currentQuestion) {
        this._startQuizIfNeeded();
        if (!this.currentQuestion) {
          this._sendQuizErrorWithTime(ws, "QUIZ_NOT_STARTED");
          return;
        }
      }
      
      if (this.quizHasWinner) {
        this._safeSend(ws, ["quizError", "Someone already answered correctly!"]);
        return;
      }
      
      if (this.quizAnswered.has(username)) {
        this._safeSend(ws, ["quizError", "You already answered!"]);
        return;
      }
      
      const answerKey = answer ? answer.toUpperCase().trim() : '';
      const isValidAnswer = ['A', 'B', 'C', 'D'].includes(answerKey);
      const isCorrect = isValidAnswer && (answerKey === this.currentQuestion.correct);
      
      const resultObj = {
        username: username,
        answer: isValidAnswer ? answerKey : "?",
        isCorrect: isCorrect,
        correctAnswer: this.currentQuestion.correct
      };
      
      this._broadcastToRoom(QUIZ_ROOM, ["quizAnswerResult", resultObj]);
      this.quizAnswered.add(username);
      
      if (isCorrect && !this.quizHasWinner) {
        this.quizHasWinner = true;
        this.quizWinner = username;
      }
      
    } catch(e) {
      this._sendQuizErrorWithTime(ws, "ERROR", e.message);
    }
  }
  
  _startQuizLoop() {
    if (this.quizTimer) {
      clearInterval(this.quizTimer);
      this.quizTimer = null;
    }
    this.quizTimer = setInterval(() => {
      try {
        if (this.closing || this.isDestroyed) {
          clearInterval(this.quizTimer);
          this.quizTimer = null;
          return;
        }
        const clients = this.wsClients.get(QUIZ_ROOM);
        if (!clients || clients.size === 0) return;
        if (!this._isQuizTime()) return;
        if (!this.quizAutoEnabled) return;
        if (this.currentQuestion || this._quizTimeout || this.isQuizWaiting || this._quizStartTimeout) return;
        this._showQuestion();
      } catch(e) {}
    }, CONSTANTS.QUIZ_INTERVAL_MS);
  }
  
  // ==================== HANDLE EVENT ====================
  
  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      const evt = data[0];
      
      if (evt === "switchRoom") {
        const [_, room, username] = data;
        await this.switchRoom(ws, room, username);
        return;
      }
      
      if (evt === "submitQuizAnswer") {
        const [_, username, answer] = data;
        await this.submitQuizAnswer(ws, username, answer);
        return;
      }
      
      if (evt === "getQuizLastWeekWinner") {
        const winner = await this._getLastWeekWinner();
        if (winner) {
          this._safeSend(ws, ["quizLastWeekWinner", winner.username, winner.score, winner.week]);
        } else {
          this._safeSend(ws, ["quizLastWeekWinner", "", 0, ""]);
        }
        return;
      }
      
      if (evt === "getQuizLeaderboard") {
        let limit = 10;
        if (data.length > 1 && typeof data[1] === 'number') {
          limit = Math.min(data[1], 30);
        }
        
        const points = await this._getQuizPoints();
        
        const sorted = Object.entries(points)
          .map(([username, score]) => ({ username, score }))
          .sort((a, b) => b.score - a.score)
          .slice(0, limit);
        
        const result = sorted.map(item => `${item.username}|${item.score}`);
        this._safeSend(ws, ["quizLeaderboard", result]);
        return;
      }
      
      if (evt === "getQuizUserPoints") {
        let username = "";
        if (data.length > 1 && typeof data[1] === 'string') {
          username = data[1];
        }
        
        const points = await this._getQuizPoints();
        const userPoints = points[username] || 0;
        this._safeSend(ws, ["quizUserPoints", username, userPoints]);
        return;
      }
      
      if (evt === "getRoomUsers") {
        return;
      }
      
      const room = this._ensureRoomConsistency(ws);
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]);
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
          this._safeSend(ws, ["gameLowCardError", `Unknown event: ${evt}`]);
          break;
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Game error: " + (e.message || "Unknown")]);
    }
  }
  
  // ==================== ALARM ====================
  
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    try {
      this._tikCounter++;
      if (this._tikCounter % 6 === 0) {
        this._checkStuckGames();
      }
      if (this._tikCounter >= CONSTANTS.CLEANUP_TIK) {
        this._cleanupStaleGames();
        this._cleanupDeadConnections();
        this._cleanupStaleBroadcastCounters();
        this._cleanupStaleSwitchLocks();
        this._tikCounter = 0;
      }
    } catch(e) {}
    try {
      if (this.state && this.state.storage) {
        await this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_10_DETIK);
      }
    } catch(e) {}
  }
  
  // ==================== TRANSLATION ====================
  
  _resetTranslateCounterDaily() {
    if (this._translateResetInterval) {
      clearInterval(this._translateResetInterval);
      this._translateResetInterval = null;
    }
    this._translateResetInterval = setInterval(() => {
      try {
        if (this.closing || this.isDestroyed) {
          clearInterval(this._translateResetInterval);
          this._translateResetInterval = null;
          return;
        }
        const now = new Date().toUTCString();
        if (now !== this.translateDate) {
          this.translateDate = now;
          this.translateCount = 0;
          this.translateLimitReached = false;
          this.questionTranslations.clear();
          this._translationCircuitBreaker.isOpen = false;
          this._translationCircuitBreaker.failures = 0;
        }
      } catch(e) {}
    }, 60000);
  }
  
  _countryToLanguage(countryCode) {
    if (!countryCode) return 'en';
    const map = {
      'ID': 'id', 'MY': 'id', 'SG': 'id', 'PH': 'id',
      'JP': 'ja', 'CN': 'zh', 'TW': 'zh', 'HK': 'zh',
      'KR': 'ko', 'IN': 'hi', 'TH': 'th', 'VN': 'vi',
      'GB': 'en', 'US': 'en', 'AU': 'en', 'CA': 'en',
      'NZ': 'en', 'FR': 'fr', 'DE': 'de', 'ES': 'es',
      'IT': 'it', 'PT': 'pt', 'NL': 'nl', 'RU': 'ru',
      'UA': 'ru', 'PL': 'pl', 'TR': 'tr',
      'QA': 'ar', 'SA': 'ar', 'AE': 'ar', 'KW': 'ar', 'BH': 'ar',
      'OM': 'ar', 'YE': 'ar', 'SY': 'ar', 'LB': 'ar', 'PS': 'ar',
      'SD': 'ar', 'LY': 'ar', 'TN': 'ar', 'DZ': 'ar', 'MA': 'ar',
      'MR': 'ar', 'IQ': 'ar', 'JO': 'ar', 'EG': 'ar',
      'MX': 'es', 'BR': 'pt', 'AR': 'es', 'CO': 'es',
      'CL': 'es', 'PE': 'es',
      'ZA': 'en', 'NG': 'en', 'KE': 'en',
    };
    return map[countryCode.toUpperCase()] || 'en';
  }
  
  _getUserLanguage(ws) {
    if (!ws) return 'en';
    const wsId = this._getWsId(ws);
    if (!wsId) return 'en';
    return this.userLanguage.get(wsId) || 'en';
  }
  
  async _translateText(text, targetLang) {
    if (targetLang === 'en') return text;
    if (this.translateLimitReached) return text;
    if (!text || typeof text !== 'string') return text;
    
    if (this._translationCircuitBreaker.isOpen) {
      const now = Date.now();
      if (now - this._translationCircuitBreaker.lastFailureTime > CONSTANTS.CIRCUIT_BREAKER_TIMEOUT_MS) {
        this._translationCircuitBreaker.isOpen = false;
        this._translationCircuitBreaker.failures = 0;
      } else {
        return text;
      }
    }
    
    const cacheKey = `${text.substring(0, 30)}_${targetLang}`;
    if (this.questionTranslations.has(cacheKey)) {
      return this.questionTranslations.get(cacheKey);
    }
    
    if (this.translateCount >= CONSTANTS.TRANSLATE_LIMIT) {
      this.translateLimitReached = true;
      return text;
    }
    
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 1500);
      
      const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=${targetLang}&dt=t&q=${encodeURIComponent(text)}`;
      const response = await fetch(url, { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (data && data[0] && data[0][0] && data[0][0][0]) {
        const translated = data[0][0][0];
        this.questionTranslations.set(cacheKey, translated);
        this.translateCount++;
        this._translationCircuitBreaker.failures = 0;
        return translated;
      }
    } catch(e) {
      this._translationCircuitBreaker.failures++;
      this._translationCircuitBreaker.lastFailureTime = Date.now();
      if (this._translationCircuitBreaker.failures >= CONSTANTS.CIRCUIT_BREAKER_THRESHOLD) {
        this._translationCircuitBreaker.isOpen = true;
      }
    }
    return text;
  }
  
  async _translateOptions(options, targetLang) {
    if (targetLang === 'en' || this.translateLimitReached || !options) {
      return options;
    }
    const translatedOptions = {};
    const keys = ['A', 'B', 'C', 'D'];
    for (const key of keys) {
      if (options[key] && typeof options[key] === 'string') {
        try {
          translatedOptions[key] = await this._translateText(options[key], targetLang);
        } catch(e) {
          translatedOptions[key] = options[key];
        }
      } else {
        translatedOptions[key] = options[key] || '';
      }
    }
    return translatedOptions;
  }
  
  // ==================== LOAD QUESTIONS ====================
  
  async _loadQuestionsFromKV() {
    try {
      if (!this.env || !this.env.QUESTIONS) return false;
      const cached = await this.env.QUESTIONS.get('quiz_questions', 'json');
      if (cached && cached.questions && Array.isArray(cached.questions) && cached.questions.length > 0) {
        const questions = cached.questions.slice(0, CONSTANTS.MAX_ARRAY_SIZE);
        this.quizQuestionCache['en'] = questions.map(q => ({
          question: q.question || '',
          options: q.options || { A: '', B: '', C: '', D: '' },
          correct: q.correct || 'A',
          category: q.category || 'General',
          difficulty: q.difficulty || 'medium'
        }));
        return true;
      }
      return false;
    } catch(e) {
      return false;
    }
  }
  
  async _initQuiz(retryCount = 0) {
    try {
      const loaded = await this._loadQuestionsFromKV();
      if (loaded) {
        this._startQuizLoop();
        this._resetTranslateCounterDaily();
        return true;
      }
      if (retryCount < CONSTANTS.MAX_RETRY_INIT_QUIZ && !this.closing && !this.isDestroyed) {
        setTimeout(() => this._initQuiz(retryCount + 1), 5000);
      }
      return false;
    } catch(e) {
      if (retryCount < CONSTANTS.MAX_RETRY_INIT_QUIZ && !this.closing && !this.isDestroyed) {
        setTimeout(() => this._initQuiz(retryCount + 1), 5000);
      }
      return false;
    }
  }
  
  ensureQuizRunning() {
    try {
      const clients = this.wsClients.get(QUIZ_ROOM);
      if (!clients || clients.size === 0) return;
      if (!this.quizQuestionCache['en'] || this.quizQuestionCache['en'].length === 0) {
        this._initQuiz().then(() => {
          if (!this.closing && !this.isDestroyed) {
            this._startQuizIfNeeded();
          }
        });
        return;
      }
      this._startQuizIfNeeded();
    } catch(e) {}
  }
  
  _startQuizIfNeeded() {
    try {
      const clients = this.wsClients.get(QUIZ_ROOM);
      if (!clients || clients.size === 0) return;
      if (!this.currentQuestion && !this._quizTimeout && !this.isQuizWaiting && !this._quizStartTimeout) {
        if (!this.quizQuestionCache['en'] || this.quizQuestionCache['en'].length === 0) {
          this._initQuiz().then(() => {
            if (!this.closing && !this.isDestroyed) {
              this._showQuestion();
            }
          });
          return;
        }
        this._showQuestion();
      }
    } catch(e) {}
  }
  
  async forceStartQuiz() {
    try {
      if (!this.quizQuestionCache['en'] || this.quizQuestionCache['en'].length === 0) {
        await this._initQuiz();
      }
      if (!this.quizQuestionCache['en'] || this.quizQuestionCache['en'].length === 0) {
        return { success: false, message: "No questions available" };
      }
      if (this.currentQuestion || this._quizTimeout || this.isQuizWaiting) {
        return { 
          success: true, 
          message: "Quiz already running",
          questions: this.quizQuestionCache['en'].length 
        };
      }
      this.quizAnswered = new Set();
      this.quizHasWinner = false;
      this.quizWinner = null;
      this.currentQuestion = null;
      if (this._quizTimeout) clearTimeout(this._quizTimeout);
      if (this._quizBreakTimeout) clearTimeout(this._quizBreakTimeout);
      if (this._quizStartTimeout) clearTimeout(this._quizStartTimeout);
      this.isQuizWaiting = false;
      await this._showQuestion();
      return { 
        success: true, 
        message: "Quiz started!",
        questions: this.quizQuestionCache['en'].length 
      };
    } catch(e) {
      return { success: false, message: e.message };
    }
  }
  
  async startQuizWithDelay(delayMs = CONSTANTS.QUIZ_START_DELAY_MS) {
    try {
      if (!this.quizQuestionCache['en'] || this.quizQuestionCache['en'].length === 0) {
        await this._initQuiz();
      }
      if (!this.quizQuestionCache['en'] || this.quizQuestionCache['en'].length === 0) {
        return { success: false, message: "No questions available" };
      }
      if (this.currentQuestion || this._quizTimeout || this.isQuizWaiting) {
        return { 
          success: true, 
          message: "Quiz already running",
          questions: this.quizQuestionCache['en'].length 
        };
      }
      this.quizAnswered = new Set();
      this.quizHasWinner = false;
      this.quizWinner = null;
      this.currentQuestion = null;
      if (this._quizTimeout) clearTimeout(this._quizTimeout);
      if (this._quizBreakTimeout) clearTimeout(this._quizBreakTimeout);
      if (this._quizStartTimeout) clearTimeout(this._quizStartTimeout);
      this.isQuizWaiting = true;
      this._quizStartTimeout = setTimeout(() => {
        try {
          if (this.closing || this.isDestroyed) {
            this._quizStartTimeout = null;
            return;
          }
          this.isQuizWaiting = false;
          this._quizStartTimeout = null;
          this._showQuestion();
        } catch(e) {
          this._quizStartTimeout = null;
          this.isQuizWaiting = false;
        }
      }, delayMs);
      return { 
        success: true, 
        message: `Quiz will start in ${delayMs/1000} seconds`,
        questions: this.quizQuestionCache['en'].length 
      };
    } catch(e) {
      return { success: false, message: e.message };
    }
  }
  
  async resetQuiz() {
    try {
      if (this._quizTimeout) clearTimeout(this._quizTimeout);
      if (this._quizBreakTimeout) clearTimeout(this._quizBreakTimeout);
      if (this._quizStartTimeout) clearTimeout(this._quizStartTimeout);
      this.isQuizWaiting = false;
      this.currentQuestion = null;
      this.quizAnswered = new Set();
      this.quizHasWinner = false;
      this.quizWinner = null;
      return { success: true, message: "Quiz reset successfully" };
    } catch(e) {
      return { success: false, message: e.message };
    }
  }
  
  // ==================== OPTIMIZED BROADCAST ====================
  
  _broadcastToRoom(room, message) {
    if (this.closing || this.isDestroyed || !room || !message) return;
    const wsIds = this.wsClients.get(room);
    if (!wsIds || wsIds.size === 0) return;
    
    const now = Date.now();
    const reset = this._roomBroadcastReset.get(room) || 0;
    const count = this._roomBroadcastCount.get(room) || 0;
    
    if (now > reset) {
      this._roomBroadcastReset.set(room, now + 1000);
      this._roomBroadcastCount.set(room, 1);
    } else {
      if (count > CONSTANTS.MAX_BROADCAST_BATCH) return;
      this._roomBroadcastCount.set(room, count + 1);
    }
    
    const msgStr = JSON.stringify(message);
    const wsIdArray = Array.from(wsIds);
    const batchSize = Math.min(CONSTANTS.BATCH_SIZE, 3);
    const disconnected = [];
    
    for (let i = 0; i < wsIdArray.length && i < 15; i += batchSize) {
      const batch = wsIdArray.slice(i, i + batchSize);
      for (const wsId of batch) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1) {
          try {
            ws.send(msgStr);
          } catch(e) {
            disconnected.push(wsId);
          }
        } else {
          disconnected.push(wsId);
        }
      }
    }
    
    if (disconnected.length > 0) {
      for (const wsId of disconnected) {
        const ws = this.wsMap.get(wsId);
        if (ws) {
          this._removeClient(room, ws);
        } else {
          this._removeClientFromRoom(room, wsId);
          this.clientRooms.delete(wsId);
        }
      }
    }
  }
  
  _safeSend(ws, message) {
    if (!ws || ws.readyState !== 1) return false;
    try {
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) {
      return false;
    }
  }
  
  // ==================== SHUFFLE HELPERS ====================
  
  _shuffleQuestionOptions(question) {
    if (!question || !question.options) {
      return { options: { A: '', B: '', C: '', D: '' }, correct: 'A' };
    }
    const options = question.options;
    const keys = ['A', 'B', 'C', 'D'];
    const entries = keys.map(key => ({
      key: key,
      text: options[key] || '',
      isCorrect: key === question.correct
    }));
    const shuffled = this._shuffleArray(entries);
    const newOptions = {};
    const newKeys = ['A', 'B', 'C', 'D'];
    let newCorrect = '';
    shuffled.forEach((item, index) => {
      const newKey = newKeys[index];
      newOptions[newKey] = item.text;
      if (item.isCorrect) newCorrect = newKey;
    });
    return {
      options: newOptions,
      correct: newCorrect || 'A'
    };
  }
  
  async _broadcastQuizQuestion(question, options) {
    const wsIds = this.wsClients.get(QUIZ_ROOM);
    if (!wsIds) return;
    const wsIdArray = Array.from(wsIds);
    for (const wsId of wsIdArray) {
      try {
        const ws = this.wsMap.get(wsId);
        if (!ws || ws.readyState !== 1) continue;
        const lang = this._getUserLanguage(ws);
        let finalQuestion = question;
        let finalOptions = options;
        if (lang !== 'en' && !this.translateLimitReached && finalQuestion && typeof finalQuestion === 'string') {
          try {
            finalQuestion = await this._translateText(question, lang);
            finalOptions = await this._translateOptions(options, lang);
          } catch(e) {}
        }
        const questionObj = {
          question: finalQuestion || '',
          options: finalOptions || { A: '', B: '', C: '', D: '' }
        };
        this._safeSend(ws, ["quizQuestion", questionObj]);
      } catch(e) {}
    }
  }
  
  _shuffleArray(array) {
    if (!array || !Array.isArray(array) || array.length === 0) return array || [];
    const arr = array.length > CONSTANTS.MAX_ARRAY_SIZE ? array.slice(0, CONSTANTS.MAX_ARRAY_SIZE) : [...array];
    for (let i = arr.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
  }
  
  // ==================== WEBSOCKET HELPERS ====================
  
  _getWsId(ws) {
    return ws ? ws._wsId : null;
  }
  
  _getRoomForWs(ws) {
    if (!ws) return null;
    return ws.room || ws.roomname || null;
  }
  
  _ensureRoomConsistency(ws) {
    if (!ws) return null;
    const wsId = this._getWsId(ws);
    if (!wsId) return null;
    let room = this._getRoomForWs(ws);
    if (!room) return null;
    const clientRoom = this.clientRooms.get(wsId);
    if (clientRoom && clientRoom !== room) {
      room = clientRoom;
      ws.room = room;
      ws.roomname = room;
    }
    if (!this.wsClients.has(room)) {
      this.wsClients.set(room, new Set());
    }
    if (!this.wsClients.get(room).has(wsId)) {
      this.wsClients.get(room).add(wsId);
      this.clientRooms.set(wsId, room);
    }
    return room;
  }
  
  _lockUserConnection(username) {
    if (this.connectionLocks.has(username)) return false;
    this.connectionLocks.set(username, true);
    return true;
  }
  
  _unlockUserConnection(username) {
    this.connectionLocks.delete(username);
  }
  
  _forceCleanupUserConnections(username, excludeWsId = null) {
    const conn = this.userConnections.get(username);
    if (!conn) return;
    if (excludeWsId !== null && conn.wsId === excludeWsId) return;
    const oldWs = this.wsMap.get(conn.wsId);
    if (oldWs && oldWs.readyState === 1) {
      try { oldWs.close(1000, "Replaced by new connection"); } catch(e) {}
    }
    if (conn.room) this._removeClientFromRoom(conn.room, conn.wsId);
    this.wsMap.delete(conn.wsId);
    this.clientRooms.delete(conn.wsId);
    this.userLanguage.delete(conn.wsId);
    this.userCountry.delete(conn.wsId);
    if (conn.room && this.roomViewers.has(conn.room)) {
      this.roomViewers.get(conn.room).delete(username);
      if (this.roomViewers.get(conn.room).size === 0) this.roomViewers.delete(conn.room);
    }
    this.userConnections.delete(username);
  }
  
  _addClient(room, ws, username = null, isNewConnection = false) {
    if (!ws) return;
    const wsId = this._getWsId(ws);
    if (!wsId) {
      this._safeSend(ws, ["gameLowCardError", "Connection error, please reconnect"]);
      return;
    }
    if (username && isNewConnection) {
      this._forceCleanupUserConnections(username, wsId);
      this.userConnections.set(username, {
        wsId: wsId,
        ws: ws,
        room: room,
        timestamp: Date.now()
      });
    }
    if (username && !isNewConnection) {
      const conn = this.userConnections.get(username);
      if (conn) {
        conn.room = room;
        conn.timestamp = Date.now();
      } else {
        this.userConnections.set(username, {
          wsId: wsId,
          ws: ws,
          room: room,
          timestamp: Date.now()
        });
      }
    }
    if (this.clientRooms.has(wsId)) {
      const oldRoom = this.clientRooms.get(wsId);
      if (oldRoom !== room) this._removeClientFromRoom(oldRoom, wsId);
    }
    const clients = this.wsClients.get(room);
    if (clients) clients.delete(wsId);
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
  }
  
  _removeClientFromRoom(room, wsId) {
    if (!room || !wsId) return;
    const clients = this.wsClients.get(room);
    if (clients) {
      clients.delete(wsId);
      if (clients.size === 0) this.wsClients.delete(room);
    }
  }
  
  _removeClient(room, ws) {
    if (!ws) return;
    const wsId = this._getWsId(ws);
    if (!wsId) return;
    const username = ws.username;
    this._removeClientFromRoom(room, wsId);
    this.clientRooms.delete(wsId);
    this.wsMap.delete(wsId);
    this.userLanguage.delete(wsId);
    this.userCountry.delete(wsId);
    if (username) {
      const conn = this.userConnections.get(username);
      if (conn && conn.wsId === wsId) this.userConnections.delete(username);
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
  
  _ensureSingleConnection(room, username, newWs, newWsId) {
    if (!newWs) return newWsId;
    const game = this.activeGames.get(room);
    if (!game) return newWsId;
    this._forceCleanupUserConnections(username, newWsId);
    game.playerWsId.set(username, newWsId);
    this._addClient(room, newWs, username, true);
    return newWsId;
  }
  
  _sendGameStatusToWs(ws, room) {}
  
  // ==================== GAME LOWCARD METHODS ====================
  
  _isGameActuallyRunning(game) {
    if (!game) return false;
    return game._isActive === true && !game._gameEnded;
  }
  
  _isGameValid(game) {
    if (!game) return false;
    return game._isActive === true && !game._gameEnded && game.players && game.players.size > 0;
  }
  
  _getActivePlayers(game) {
    if (!game || !game._isActive || game._gameEnded || !game.players) return [];
    return Array.from(game.players.entries())
      .filter(([id]) => !game.eliminated?.has(id))
      .map(([, p]) => p);
  }
  
  _getActivePlayerIds(game) {
    if (!game || !game._isActive || game._gameEnded || !game.players) return [];
    return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
  }
  
  _getRandomCardTanda() {
    return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)];
  }
  
  _getRandomDrawDelay() {
    return (Math.floor(Math.random() * 14) + 2) * 1000;
  }
  
  _getBotNumberByRound(round) {
    if (round <= 2) {
      return Math.floor(Math.random() * 12) + 1;
    } else {
      return Math.random() < 0.6 ? 
        [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
        [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
    }
  }
  
  _safeGetGame(room) {
    if (this.isDestroyed || !room) return null;
    const game = this.activeGames.get(room);
    if (game && game._isActive === true && !game._gameEnded && game.players) {
      return game;
    }
    return null;
  }
  
  // ==================== GAME CLEANUP ====================
  
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
        if (currentGame && currentGame._isActive && !currentGame._gameEnded) {
          this._cleanupTimers.delete(room);
          return;
        }
        this._cleanupTimers.delete(room);
        const gameToDelete = this.activeGames.get(room);
        if (gameToDelete) this._deleteGame(room, gameToDelete);
      } catch(e) {}
    }, CONSTANTS.GAME_CLEANUP_DELAY_MS);
    this._cleanupTimers.set(room, timer);
  }
  
  _cleanupGame(game) {
    if (!game) return;
    if (game._isActive === true && !game._gameEnded) return;
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
  
  _deleteGame(room, game) {
    if (!room || !game) return;
    if (game && game._isActive === true && !game._gameEnded) return;
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
  }
  
  _removePlayerFromGame(username, room) {
    try {
      const game = this.activeGames.get(room);
      if (!game) return false;
      if (!game.players || !game.players.has(username)) return false;
      if (!game._isActive || game._gameEnded) return false;
      if (game._isEvaluating || game.evaluationLocked) return false;
      if (!game.eliminated) game.eliminated = new Set();
      game.eliminated.add(username);
      this._broadcastToRoom(room, ["gameLowCardPlayerEliminated", username, "Disconnected"]);
      game.numbers?.delete(username);
      game.tanda?.delete(username);
      setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && !game._gameEnded) {
            this._checkGameCanContinue(room, game);
          }
        } catch(e) {}
      }, 1000);
      return true;
    } catch(e) {
      return false;
    }
  }
  
  _checkGameCanContinue(room, game) {
    try {
      if (!game || game._gameEnded || !game.players || !game._isActive) return;
      if (game._isEvaluating || game.evaluationLocked) return;
      if (game.registrationOpen) return;
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
          this._broadcastToRoom(room, ["gameLowCardInfo", `Waiting for ${notSubmitted.length} player(s)`]);
          return;
        }
        const winner = activePlayers[0]?.name || "Unknown";
        const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }
  
  _findAllGamesByUsername(username) {
    if (!username) return [];
    const result = [];
    for (const [room, game] of this.activeGames) {
      if (game && game._isActive && !game._gameEnded && game.players) {
        if (game.players.has(username)) result.push({ game, room });
      }
    }
    return result;
  }
  
  // ==================== BOT METHODS ====================
  
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
            if (this._isGameActuallyRunning(currentGame) && 
                !currentGame.drawTimeExpired &&
                !currentGame.evaluationLocked &&
                !currentGame.numbers?.has(botId) &&
                !currentGame.eliminated?.has(botId)) {
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
  
  // ==================== GAME PHASE METHODS ====================
  
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
        } else if (game.players.size < 2) {
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
  
  _startDrawPhase(room, game) {
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
    } catch(e) {}
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
      for (const botId of activeBotIds) {
        this._forceBotDraw(room, botId, game);
      }
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
  }
  
  _evaluateRound(room, game) {
    try {
      if (this.isDestroyed || !game || game._gameEnded || !game._isActive || game._isEvaluating) return;
      if (!game.players) return;
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game) return;
      game._isEvaluating = true;
      game._safetyTimer = setTimeout(() => {
        try {
          if (game && game._isEvaluating) {
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
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        game._isEvaluating = false;
        if (game._safetyTimer) {
          clearTimeout(game._safetyTimer);
          game._safetyTimer = null;
        }
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
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
        this._broadcastToRoom(room, [
          "gameLowCardRoundResult", 
          game.round - 1, 
          entries.map(([id, n]) => {
            const name = players.get(id)?.name || id;
            const t = tanda.get(id) || "";
            return `${name}:${n}${t ? `(${t})` : ''}`;
          }),
          [],
          remainingNames,
          true
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
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        game._isEvaluating = false;
        if (game._safetyTimer) {
          clearTimeout(game._safetyTimer);
          game._safetyTimer = null;
        }
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
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
      const numbersArr = entries.map(([id, n]) => {
        const name = players.get(id)?.name || id;
        const t = tanda.get(id) || "";
        return `${name}:${n}${t ? `(${t})` : ''}`;
      });
      const loserNames = [...losers].map(id => players.get(id)?.name || id);
      const remainingNames = remaining.map(id => players.get(id)?.name || id);
      this._broadcastToRoom(room, [
        "gameLowCardRoundResult", game.round, numbersArr, loserNames, remainingNames
      ]);
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
  
  // ==================== START GAME ====================
  
  async startGame(ws, bet, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username || username.trim() === "") {
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
      const startKey = `start_${room}`;
      if (this._gameStartFlags.has(startKey)) {
        this._safeSend(ws, ["gameLowCardError", "Game is already starting..."]);
        return;
      }
      const existingGame = this.activeGames.get(room);
      if (existingGame && existingGame._isActive && !existingGame._gameEnded) {
        this._safeSend(ws, ["gameLowCardInfo", "Game is already running"]);
        return;
      }
      this._gameStartFlags.set(startKey, Date.now());
      if (existingGame) await this._forceCleanupGame(room, existingGame);
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
          room,
          players: new Map(),
          botPlayers: new Map(),
          registrationOpen: true,
          round: 1,
          numbers: new Map(),
          tanda: new Map(),
          eliminated: new Set(),
          betAmount,
          hostId: usernameClean,
          hostName: usernameClean,
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
          playerWsId: new Map()
        };
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        game.playerWsId.set(usernameClean, wsId);
        this.activeGames.set(room, game);
        this._addClient(room, ws, usernameClean, false);
        this._broadcastToRoom(room, ["gameLowCardStart", game.betAmount, usernameClean]);
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
    if (!game) return;
    try {
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
      if (!username || username.trim() === "") {
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
        if (!game || !game._isActive || game._gameEnded || !game.players) {
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
            const number = game.numbers.get(usernameClean);
            const tanda = game.tanda.get(usernameClean) || "";
            this._safeSend(ws, ["gameLowCardPlayerDraw", usernameClean, number, tanda]);
          }
          return;
        }
        if (!game.registrationOpen) {
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
      if (!username || username.trim() === "") {
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
      if (!game || !game._isActive || game._gameEnded || !game.players) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      if (game.players.has(usernameClean)) {
        if (game.eliminated?.has(usernameClean)) {
          this._safeSend(ws, ["gameLowCardError", "You have been eliminated from this game"]);
          return;
        }
        const existingWsId = game.playerWsId.get(usernameClean);
        if (existingWsId && existingWsId !== wsId) {
          this._ensureSingleConnection(room, usernameClean, ws, wsId);
        }
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
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameActuallyRunning(game) && game._isActive && !game._gameEnded) {
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
      if (!username || username.trim() === "") {
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
      if (!game || !game._isActive || game._gameEnded || !game.players) {
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
  
  // ==================== CHECK GAME ====================
  
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
      const isRunning = game && game._isActive && !game._gameEnded && game.players && game.players.size > 0;
      this._safeSend(ws, ["gameStatus", { running: isRunning ? "true" : "false" }]);
    } catch(e) {
      this._safeSend(ws, ["gameStatus", { running: "false" }]);
    }
  }
  
  // ==================== GETTERS ====================
  
  getGame(room) {
    return this.activeGames.get(room);
  }
  
  isGameRunning(room) {
    try {
      if (this.isDestroyed || !room) {
        return { running: false, message: this.isDestroyed ? "System destroyed" : "Invalid room" };
      }
      const game = this.activeGames.get(room);
      if (!game || !game.players) {
        return { running: false, message: "No game in this room" };
      }
      const isRunning = game._isActive === true && !game._gameEnded;
      return { running: isRunning, message: isRunning ? "Game is running" : "Game is not active" };
    } catch(e) {
      return { running: false, message: "Error checking game" };
    }
  }
  
  // ==================== CLEANUP ====================
  
  _checkStuckGames() {
    try {
      const now = Date.now();
      for (const [room, game] of this.activeGames) {
        if (!game || !game._isActive || game._gameEnded) continue;
        if (game._phase === 'draw' && game._drawPhaseStart) {
          if ((now - game._drawPhaseStart) > CONSTANTS.STUCK_DRAW_TIMEOUT_MS) {
            this._broadcastToRoom(room, ["gameLowCardError", "Game stuck, forcing evaluation..."]);
            this._closeDrawPhase(room, game);
          }
        }
        if (game._phase === 'registration' && game.registrationOpen) {
          if (game._createdAt && (now - game._createdAt) > CONSTANTS.STUCK_REGISTRATION_TIMEOUT_MS) {
            this._broadcastToRoom(room, ["gameLowCardError", "Registration timeout"]);
            this._closeRegistration(room, game);
          }
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
    } catch(e) {}
  }
  
  _cleanupStaleGames() {
    try {
      const now = Date.now();
      for (const [room, game] of this.activeGames) {
        if (!game) continue;
        if (game._isActive === true && !game._gameEnded) continue;
        if (game._gameEnded === true) {
          const endTime = game._endTime || game._createdAt || now;
          if ((now - endTime) > CONSTANTS.STALE_GAME_TIMEOUT_MS) {
            this._scheduleGameCleanup(room, game);
          }
          continue;
        }
        if (game._isActive === false && !game._gameEnded) {
          if (game._createdAt && (now - game._createdAt) > 300000) {
            game._gameEnded = true;
            game._endTime = now;
            this._scheduleGameCleanup(room, game);
          }
        }
      }
    } catch(e) {}
  }
  
  _cleanupStaleBroadcastCounters() {
    try {
      const now = Date.now();
      for (const [room, resetTime] of this._roomBroadcastReset) {
        if ((now - resetTime) > 60000) {
          this._roomBroadcastCount.delete(room);
          this._roomBroadcastReset.delete(room);
        }
      }
    } catch(e) {}
  }
  
  _cleanupStaleSwitchLocks() {
    try {
      const now = Date.now();
      for (const [key, time] of this._switchLocks) {
        if ((now - time) > 5000) this._switchLocks.delete(key);
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
          this.userLanguage.delete(wsId);
          this.userCountry.delete(wsId);
          for (const [username, conn] of this.userConnections) {
            if (conn && conn.wsId === wsId) {
              this.userConnections.delete(username);
              break;
            }
          }
        }
      }
    } catch(e) {}
  }
  
  // ==================== FETCH ====================
  
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    try {
      const url = new URL(req.url);
      
      if (url.pathname === "/game/health") {
        return new Response(JSON.stringify({
          status: this.isDestroyed ? 'down' : 'healthy',
          games: this.activeGames ? this.activeGames.size : 0,
          connections: this.wsMap ? this.wsMap.size : 0,
          questions: this.quizQuestionCache && this.quizQuestionCache['en'] ? 
                     this.quizQuestionCache['en'].length : 0,
          kvAvailable: !!(this.env && this.env.QUESTIONS),
          quizAutoEnabled: this.quizAutoEnabled || false,
          isQuizRunning: !!this.currentQuestion,
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
          
          const cf = req.cf;
          let country = 'US';
          if (cf && cf.country) {
            country = cf.country;
          }
          server._country = country;
          const lang = this._countryToLanguage(country);
          this.userLanguage.set(wsId, lang);
          this.userCountry.set(wsId, country);
          
          try { 
            this.state.acceptWebSocket(server);
          } catch(e) { 
            return new Response("WebSocket acceptance failed", { status: 500 }); 
          }
          
          server.addEventListener("message", async (event) => {
            try {
              const data = JSON.parse(event.data);
              if (!Array.isArray(data) || data.length === 0) return;
              await this.handleEvent(server, data);
            } catch(e) {
              this._safeSend(server, ["gameLowCardError", e.message || "Error"]);
            }
          });
          
          server.addEventListener("close", () => {
            try {
              if (server.room || server.roomname) {
                const room = server.room || server.roomname;
                const wsId = this._getWsId(server);
                const username = server.username;
                this._removeClient(room, server);
                this.userLanguage.delete(wsId);
                this.userCountry.delete(wsId);
                if (username) {
                  const conn = this.userConnections.get(username);
                  if (conn && conn.wsId === wsId) {
                    this.userConnections.delete(username);
                  }
                }
              }
              const clients = this.wsClients.get(QUIZ_ROOM);
              if (clients && clients.size > 0) {
                this.ensureQuizRunning();
              }
            } catch(e) {}
          });
          
          server.addEventListener("error", () => {
            try {
              if (server.room || server.roomname) {
                const room = server.room || server.roomname;
                const wsId = this._getWsId(server);
                const username = server.username;
                this._removeClient(room, server);
                this.userLanguage.delete(wsId);
                this.userCountry.delete(wsId);
                if (username) {
                  const conn = this.userConnections.get(username);
                  if (conn && conn.wsId === wsId) {
                    this.userConnections.delete(username);
                  }
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
      return new Response("Internal Server Error: " + e.message, { status: 500 });
    }
  }
  
  // ==================== WEBSOCKET EVENTS ====================
  
  async webSocketMessage(ws, msg) {
    try {
      if (!ws || ws._closing || this.closing || this.isDestroyed) return;
      if (!ws._wsId) return;
      const data = JSON.parse(msg);
      if (!Array.isArray(data) || data.length === 0) return;
      await this.handleEvent(ws, data);
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", e.message || "Error"]);
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
      this.userLanguage.delete(wsId);
      this.userCountry.delete(wsId);
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn && conn.wsId === wsId) {
          this.userConnections.delete(username);
        }
      }
      if (wsId) {
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
      }
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      const clients = this.wsClients.get(QUIZ_ROOM);
      if (clients && clients.size > 0) {
        this.ensureQuizRunning();
      }
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
      this.userLanguage.delete(wsId);
      this.userCountry.delete(wsId);
      if (username) {
        const conn = this.userConnections.get(username);
        if (conn && conn.wsId === wsId) {
          this.userConnections.delete(username);
        }
      }
      if (wsId) {
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
      }
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
    } catch(e) {}
  }
  
  // ==================== DESTROY ====================
  
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.closing = true;
      this.isDestroyed = true;
      
      if (this.quizAutoTimer) {
        clearInterval(this.quizAutoTimer);
        this.quizAutoTimer = null;
      }
      if (this.quizTimer) {
        clearInterval(this.quizTimer);
        this.quizTimer = null;
      }
      if (this._translateResetInterval) {
        clearInterval(this._translateResetInterval);
        this._translateResetInterval = null;
      }
      if (this._quizTimeout) {
        clearTimeout(this._quizTimeout);
        this._quizTimeout = null;
      }
      if (this._quizBreakTimeout) {
        clearTimeout(this._quizBreakTimeout);
        this._quizBreakTimeout = null;
      }
      if (this._quizStartTimeout) {
        clearTimeout(this._quizStartTimeout);
        this._quizStartTimeout = null;
      }
      
      for (const [room, game] of this.activeGames) {
        if (game) {
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
          game.playerWsId = null;
        }
      }
      
      for (const [room, timer] of this._cleanupTimers) {
        clearTimeout(timer);
      }
      this._cleanupTimers.clear();
      
      this.quizQuestionCache = {};
      this.questionTranslations.clear();
      this.userLanguage.clear();
      this.userCountry.clear();
      this.wsClients.clear();
      this.clientRooms.clear();
      this.wsMap.clear();
      this.roomViewers.clear();
      this.userConnections.clear();
      this.connectionLocks.clear();
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._switchLocks.clear();
      this._gameStartFlags.clear();
      this._roomBroadcastCount.clear();
      this._roomBroadcastReset.clear();
      this.quizAnswered.clear();
      this.activeGames.clear();
      
      for (const [room, wsIds] of this.wsClients) {
        for (const wsId of wsIds) {
          const ws = this.wsMap.get(wsId);
          if (ws) {
            try {
              ws.close(1000, "Game server shutting down");
            } catch(e) {}
          }
        }
      }
    } catch(e) {}
  }
}
