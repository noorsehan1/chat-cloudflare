// ==================== GAME SERVER - QUIZ OTOMATIS ====================

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
  BATCH_SIZE: 20,
  ALARM_10_DETIK: 10000,
  CLEANUP_TIK: 90,
  STALE_GAME_TIMEOUT_MS: 600000,
  STUCK_DRAW_TIMEOUT_MS: 60000,
  STUCK_REGISTRATION_TIMEOUT_MS: 30000,
  
  // ✅ QUIZ
  QUIZ_INTERVAL_MS: 15000,
  QUIZ_QUESTION_TIME_MS: 15000,
};

const QUIZ_ROOM = "LowCard 2";

export class GameServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    
    // ==================== GAME LOWCARD ====================
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
    this.connectionLocks = new Map();
    this._cleanupTimers = new Map();
    this._roomBroadcastCount = new Map();
    this._roomBroadcastReset = new Map();
    this._tikCounter = 0;
    this._gameStartFlags = new Map();
    
    // ==================== QUIZ ====================
    this.quizActive = false;
    this.quizQuestions = [];
    this.quizCurrentIndex = 0;
    this.quizWinner = null;
    this.quizHasWinner = false;
    this.quizAnswered = new Set();
    this.quizTimer = null;
    
    // ✅ QUIZ LANGSUNG JALAN!
    this._startQuizLoop();
    
    this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_10_DETIK);
  }
  
  // ==================== ALARM ====================
  
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    try {
      this._tikCounter++;
      if (this._tikCounter % 6 === 0) this._checkStuckGames();
      if (this._tikCounter >= CONSTANTS.CLEANUP_TIK) {
        this._cleanupStaleGames();
        this._cleanupDeadConnections();
        this._cleanupStaleBroadcastCounters();
        this._cleanupStaleSwitchLocks();
        this._tikCounter = 0;
      }
    } catch(e) {}
    try {
      this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_10_DETIK);
    } catch(e) {}
  }
  
  // ==================== QUIZ OTOMATIS ====================
  
  _startQuizLoop() {
    if (this.quizTimer) {
      clearInterval(this.quizTimer);
      this.quizTimer = null;
    }
    
    // ✅ QUIZ JALAN TERUS SETIAP 15 DETIK
    this.quizTimer = setInterval(() => {
      try {
        if (this.closing || this.isDestroyed) return;
        if (this.quizActive) return;
        
        const clients = this.wsClients.get(QUIZ_ROOM);
        if (!clients || clients.size === 0) return;
        
        this._runQuiz();
        
      } catch(e) {}
    }, CONSTANTS.QUIZ_INTERVAL_MS);
  }
  
  async _runQuiz() {
    try {
      // ✅ AMBIL SOAL
      const questions = await this._fetchQuestions(5);
      if (!questions || questions.length === 0) return;
      
      this.quizActive = true;
      this.quizQuestions = questions;
      this.quizCurrentIndex = 0;
      this.quizWinner = null;
      this.quizHasWinner = false;
      this.quizAnswered = new Set();
      
      // ✅ KIRIM QUIZ START
      this._broadcastToRoom(QUIZ_ROOM, ["quizStarted", {
        total: questions.length
      }]);
      
      // ✅ TAMPILKAN SOAL PERTAMA
      this._showQuestion();
      
    } catch(e) {}
  }
  
  _showQuestion() {
    if (!this.quizActive) return;
    if (this.quizCurrentIndex >= this.quizQuestions.length) {
      this._endQuiz();
      return;
    }
    
    const q = this.quizQuestions[this.quizCurrentIndex];
    this.quizHasWinner = false;
    this.quizWinner = null;
    this.quizAnswered = new Set();
    
    // ✅ KIRIM PERTANYAAN + TIMER 15 DETIK
    this._broadcastToRoom(QUIZ_ROOM, ["quizQuestion", {
      index: this.quizCurrentIndex + 1,
      total: this.quizQuestions.length,
      question: q.question,
      options: q.options,
      timeLimit: 15
    }]);
    
    // ✅ TIMER 15 DETIK
    setTimeout(() => {
      try {
        if (!this.quizActive) return;
        
        if (this.quizHasWinner && this.quizWinner) {
          // ✅ ADA PEMENANG
          this._broadcastToRoom(QUIZ_ROOM, ["quizWinner", {
            username: this.quizWinner,
            message: `🏆 ${this.quizWinner} is the first to answer correctly!`
          }]);
        } else {
          // ✅ TIDAK ADA PEMENANG
          this._broadcastToRoom(QUIZ_ROOM, ["quizNoWinner", {
            message: "⏰ Time's up! No one answered correctly."
          }]);
        }
        
        // ✅ LANJUT SOAL BERIKUTNYA
        this.quizCurrentIndex++;
        if (this.quizCurrentIndex >= this.quizQuestions.length) {
          this._endQuiz();
        } else {
          this._showQuestion();
        }
        
      } catch(e) {}
    }, CONSTANTS.QUIZ_QUESTION_TIME_MS);
  }
  
  async _fetchQuestions(amount = 5) {
    try {
      const url = `https://opentdb.com/api.php?amount=${amount}&type=multiple`;
      const response = await fetch(url);
      const data = await response.json();
      
      if (data.response_code === 0 && data.results) {
        return data.results.map((q) => {
          const answers = [
            { text: q.correct_answer, isCorrect: true },
            { text: q.incorrect_answers[0] || "N/A", isCorrect: false },
            { text: q.incorrect_answers[1] || "N/A", isCorrect: false },
            { text: q.incorrect_answers[2] || "N/A", isCorrect: false }
          ];
          
          const shuffled = this._shuffleArray(answers);
          const options = {};
          const keys = ['A', 'B', 'C', 'D'];
          let correctKey = '';
          
          shuffled.forEach((item, i) => {
            const key = keys[i];
            options[key] = item.text;
            if (item.isCorrect) correctKey = key;
          });
          
          return {
            question: q.question,
            options: options,
            correct: correctKey
          };
        });
      }
    } catch(e) {}
    return [];
  }
  
  _shuffleArray(array) {
    const arr = [...array];
    for (let i = arr.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
  }
  
  _endQuiz() {
    this.quizActive = false;
    this._broadcastToRoom(QUIZ_ROOM, ["quizEnded", {
      reason: "Quiz completed!",
      totalQuestions: this.quizQuestions.length
    }]);
  }
  
  // ==================== SUBMIT ANSWER ====================
  
  async submitQuizAnswer(ws, username, questionIndex, answer) {
    try {
      const room = this._ensureRoomConsistency(ws);
      if (room !== QUIZ_ROOM) {
        this._safeSend(ws, ["quizError", "Quiz only in LowCard 2"]);
        return;
      }
      
      if (!this.quizActive) {
        this._safeSend(ws, ["quizError", "No active quiz"]);
        return;
      }
      
      if (this.quizAnswered.has(username)) {
        this._safeSend(ws, ["quizError", "You already answered!"]);
        return;
      }
      
      if (questionIndex !== this.quizCurrentIndex) {
        this._safeSend(ws, ["quizError", "Invalid question"]);
        return;
      }
      
      const q = this.quizQuestions[this.quizCurrentIndex];
      const isCorrect = answer === q.correct;
      
      this.quizAnswered.add(username);
      
      let isFirstCorrect = false;
      if (isCorrect && !this.quizHasWinner) {
        this.quizHasWinner = true;
        this.quizWinner = username;
        isFirstCorrect = true;
        
        // ✅ LANGSUNG KIRIM WINNER
        this._broadcastToRoom(QUIZ_ROOM, ["quizWinner", {
          username: username,
          message: `🏆 ${username} is the first to answer correctly!`
        }]);
      }
      
      this._safeSend(ws, ["quizAnswerResult", {
        isCorrect: isCorrect,
        correctAnswer: q.correct,
        isFirstCorrect: isFirstCorrect
      }]);
      
      this._broadcastToRoom(QUIZ_ROOM, ["quizPlayerAnswered", {
        username: username,
        isCorrect: isCorrect,
        isFirstCorrect: isFirstCorrect
      }]);
      
    } catch(e) {
      this._safeSend(ws, ["quizError", e.message]);
    }
  }
  
  // ==================== WEB SOCKET HELPERS ====================
  
  _getWsId(ws) { return ws ? ws._wsId : null; }
  _getRoomForWs(ws) { if (!ws) return null; return ws.room || ws.roomname || null; }
  
  _ensureRoomConsistency(ws) {
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
    if (!this.wsClients.has(room)) this.wsClients.set(room, new Set());
    if (!this.wsClients.get(room).has(wsId)) {
      this.wsClients.get(room).add(wsId);
      this.clientRooms.set(wsId, room);
    }
    return room;
  }
  
  _addClient(room, ws, username = null, isNewConnection = false) {
    const wsId = this._getWsId(ws);
    if (!wsId) return;
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
    const clients = this.wsClients.get(room);
    if (clients) {
      clients.delete(wsId);
      if (clients.size === 0) this.wsClients.delete(room);
    }
  }
  
  _removeClient(room, ws) {
    const wsId = this._getWsId(ws);
    if (!wsId) return;
    const username = ws.username;
    this._removeClientFromRoom(room, wsId);
    this.clientRooms.delete(wsId);
    this.wsMap.delete(wsId);
    if (username) {
      const conn = this.userConnections.get(username);
      if (conn && conn.wsId === wsId) this.userConnections.delete(username);
      if (this.roomViewers.has(room)) {
        this.roomViewers.get(room).delete(username);
        if (this.roomViewers.get(room).size === 0) this.roomViewers.delete(room);
      }
    }
    if (ws) { ws.room = null; ws.roomname = null; ws._wsId = null; ws.username = null; }
  }
  
  _broadcastToRoom(room, message) {
    if (this.closing || this.isDestroyed || !room || !message) return;
    const wsIds = this.wsClients.get(room);
    if (!wsIds || wsIds.size === 0) return;
    const msgStr = JSON.stringify(message);
    const disconnected = new Set();
    const wsIdArray = Array.from(wsIds);
    for (let i = 0; i < wsIdArray.length; i += 10) {
      const batch = wsIdArray.slice(i, i + 10);
      for (const wsId of batch) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1) {
          try { ws.send(msgStr); } catch(e) { disconnected.add(wsId); }
        } else { disconnected.add(wsId); }
      }
    }
    if (disconnected.size > 0) {
      for (const wsId of disconnected) {
        const ws = this.wsMap.get(wsId);
        if (ws) this._removeClient(room, ws);
        else { this._removeClientFromRoom(room, wsId); this.clientRooms.delete(wsId); }
      }
    }
  }
  
  _safeSend(ws, message) {
    if (!ws || ws.readyState !== 1) return false;
    try { ws.send(JSON.stringify(message)); return true; } catch(e) { return false; }
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
    if (this._switchLocks.has(lockKey)) {
      this._safeSend(ws, ["switchRoomBusy", "Please wait..."]);
      return;
    }
    this._switchLocks.set(lockKey, Date.now());
    try {
      const oldRoom = this.clientRooms.get(wsId);
      if (oldRoom) this._removeClientFromRoom(oldRoom, wsId);
      this._addClient(roomName, ws, username, false);
      ws.room = roomName;
      ws.roomname = roomName;
      ws.username = username;
      this._broadcastToRoom(roomName, ["roomUserJoined", username || "Anonymous"]);
      this._safeSend(ws, ["switchRoomSuccess", roomName]);
      if (roomName === QUIZ_ROOM) {
        this._safeSend(ws, ["quizInfo", "🎯 Welcome to LowCard 2! Quiz is running!"]);
      }
    } finally {
      this._switchLocks.delete(lockKey);
    }
  }
  
  // ==================== GAME LOWCARD ====================
  
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
      
      // ✅ CEK APAKAH ROOM LowCard 2?
      if (room === QUIZ_ROOM) {
        this._safeSend(ws, ["gameLowCardError", "❌ Cannot start game in LowCard 2. This room is for Quiz only!"]);
        return;
      }
      
      // ========== GAME LOWCARD START ==========
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
        this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
        
        this._startRegistration(room, game);
        
        setTimeout(() => {
          try {
            this._gameStartFlags.delete(startKey);
            if (this._gameLocks.get(room) === now) {
              this._gameLocks.delete(room);
            }
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
  
  // ==================== EVENT HANDLER ====================
  
  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      const evt = data[0];
      
      if (evt === "switchRoom") {
        await this.switchRoom(ws, data[1], data[2]);
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
        // ✅ QUIZ
        case "submitQuizAnswer":
          await this.submitQuizAnswer(ws, data[1], data[2], data[3]);
          break;
        case "getQuizStatus":
          this._sendQuizStatusToWs(ws);
          break;
        default:
          this._safeSend(ws, ["gameLowCardError", `Unknown event: ${evt}`]);
          break;
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Error: " + (e.message || "Unknown")]);
    }
  }
  
  _sendQuizStatusToWs(ws) {
    if (this.quizActive) {
      this._safeSend(ws, ["quizStatus", {
        active: true,
        currentQuestion: this.quizCurrentIndex + 1,
        totalQuestions: this.quizQuestions.length
      }]);
    } else {
      this._safeSend(ws, ["quizStatus", {
        active: false,
        message: "Waiting for next quiz..."
      }]);
    }
  }
  
  // ==================== WEB SOCKET ====================
  
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    try {
      const url = new URL(req.url);
      if (url.pathname === "/game/ws") {
        const upgrade = req.headers.get("Upgrade");
        if (upgrade !== "websocket") {
          return new Response("WebSocket only", { status: 400 });
        }
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        const timeoutId = setTimeout(() => {
          try {
            if (server.readyState === 0) server.close(1000, "Timeout");
          } catch(e) {}
        }, 5000);
        server._timeoutId = timeoutId;
        try { this.state.acceptWebSocket(server); }
        catch(e) { clearTimeout(timeoutId); return new Response("WebSocket acceptance failed", { status: 500 }); }
        const wsId = ++this._wsIdCounter;
        server._wsId = wsId;
        server._closing = false;
        server.room = null;
        server.roomname = null;
        server.username = null;
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
              this._removeClient(room, server);
            }
          } catch(e) {}
        });
        server.addEventListener("error", () => {
          try {
            if (server.room || server.roomname) {
              const room = server.room || server.roomname;
              this._removeClient(room, server);
            }
          } catch(e) {}
        });
        return new Response(null, { status: 101, webSocket: client });
      }
      return new Response("Game Server", { status: 200 });
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
  
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
      if (ws.room || ws.roomname) {
        const room = ws.room || ws.roomname;
        this._removeClient(room, ws);
      }
      if (wsId) { this.clientRooms.delete(wsId); this.wsMap.delete(wsId); }
      ws.room = null; ws.roomname = null; ws._wsId = null; ws.username = null;
    } catch(e) {}
  }
  
  async webSocketError(ws) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      if (ws.room || ws.roomname) {
        const room = ws.room || ws.roomname;
        this._removeClient(room, ws);
      }
      if (wsId) { this.clientRooms.delete(wsId); this.wsMap.delete(wsId); }
      ws.room = null; ws.roomname = null; ws._wsId = null; ws.username = null;
    } catch(e) {}
  }
  
  // ==================== DESTROY ====================
  
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.closing = true;
      this.isDestroyed = true;
      if (this.quizTimer) { clearInterval(this.quizTimer); this.quizTimer = null; }
      this.quizActive = false;
      // Cleanup lainnya...
    } catch(e) {}
  }
  
  // ==================== GAME LOWCARD METHODS (DUMMY) ====================
  
  _checkStuckGames() {}
  _cleanupStaleGames() {}
  _cleanupStaleBroadcastCounters() {}
  _cleanupStaleSwitchLocks() {}
  _cleanupDeadConnections() {}
  
  _startRegistration(room, game) {}
  _closeRegistration(room, game) {}
  _startDrawPhase(room, game) {}
  _startDrawCountdown(room, game) {}
  _closeDrawPhase(room, game) {}
  _evaluateRound(room, game) {}
  _getActivePlayers(game) { return []; }
  _getActivePlayerIds(game) { return []; }
  _isGameActuallyRunning(game) { return false; }
  _scheduleGameCleanup(room, game) {}
  _deleteGame(room, game) {}
  _forceCleanupGame(room, game) {}
  
  async joinGame(ws, username) {
    this._safeSend(ws, ["gameLowCardError", "Game not implemented"]);
  }
  async submitNumber(ws, number, tanda, username) {
    this._safeSend(ws, ["gameLowCardError", "Game not implemented"]);
  }
  async leaveGame(ws, username) {
    this._safeSend(ws, ["gameLowCardError", "Game not implemented"]);
  }
  async checkGameRunning(ws, roomname) {
    this._safeSend(ws, ["gameStatus", { running: "false" }]);
  }
}
