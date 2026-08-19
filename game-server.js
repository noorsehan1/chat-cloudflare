// ==================== GAME-SERVER.JS ====================
// VERSION: 4.0.2 - WEBSOCKET + D1 (TANPA DO)
// PISAH DARI CHAT SERVER - PERSIS KODE AWAL

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
const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

// ==================== WEBSOCKET STORAGE ====================
const wsConnections = new Map();

export class GameServer {
  constructor(env) {
    this.env = env;
    this.wsConnections = wsConnections;
    this.ROOMS = ROOMS;
    this.DICE_ROOM = DICE_ROOM;
    this.CONSTANTS = CONSTANTS;
    this.QUIZ_SCHEDULE = QUIZ_SCHEDULE;
    
    // ========== GAME STATE ==========
    this.activeGames = new Map();
    this.gameTimers = new Map();
    this._gameLocks = new Map();
    this._joinLocks = new Map();
    this._cleanupTimers = new Map();
    
    // ========== DICE STATE ==========
    this.currentDiceRoll = null;
    this._diceLock = false;
    this._tieActive = false;
    this.diceAnswered = new Set();
    this._playerAnswers = new Map();
    this._isShowingDice = false;
    this._diceTimeUpCooldown = false;
    this._diceTimeout = null;
    this._diceTimerInterval = null;
    this.diceHasWinner = false;
    this.diceWinner = null;
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
    this._switchLocks = new Map();
    this._switchRetries = new Map();
    
    // ========== RECORDING ==========
    this._recordingEnabled = new Map();
    this._kvCache = new Map();
    
    // ========== INTERVAL ==========
    this._mainInterval = null;
    this._cleanupInterval = null;
    this._tickCount = 0;
    this._initialized = false;
    this.closing = false;
    this.isDestroyed = false;
    
    // ✅ START
    setTimeout(() => {
      if (!this.closing && !this.isDestroyed) {
        this._initLazy();
      }
    }, 2000);
  }

  // ========== LAZY INIT ==========
  _initLazy() {
    if (this._initialized || this.closing || this.isDestroyed) return;
    this._initialized = true;
    
    try {
      this._startMainInterval();
    } catch(e) {
      setTimeout(() => {
        if (!this.closing && !this.isDestroyed) {
          this._initialized = false;
          this._initLazy();
        }
      }, 30000);
    }
  }

  // ========== MAIN INTERVAL ==========
  _startMainInterval() {
    if (this.closing || this.isDestroyed) return;
    
    this._mainInterval = setInterval(() => {
      if (this.closing || this.isDestroyed) {
        clearInterval(this._mainInterval);
        return;
      }
      this._tickCount++;
      this._doTick();
    }, 10000);
    
    this._allTimers.add(this._mainInterval);
    
    this._cleanupInterval = setInterval(() => {
      if (this.closing || this.isDestroyed) {
        clearInterval(this._cleanupInterval);
        return;
      }
      this._performCleanup();
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
    
    this._allTimers.add(this._cleanupInterval);
  }

  // ========== TICK ==========
  _doTick() {
    try {
      const tick = this._tickCount % 3;
      switch(tick) {
        case 0: 
          this._checkDice();
          break;
        case 1: 
          this._checkStuckGames();
          break;
        case 2: 
          this._cleanupMemory();
          break;
      }
    } catch(e) {}
  }

  // ========== FETCH ==========
  async fetch(request) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }

    const url = new URL(request.url);
    const pathname = url.pathname;

    // ============================================================
    // WEBSOCKET
    // ============================================================
    if (pathname === "/game/ws" || pathname === "/game") {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Game Server", { 
          status: 200,
          headers: { "Cache-Control": "no-cache" }
        });
      }

      if (this.wsConnections.size >= CONSTANTS.MAX_WS_CLIENTS) {
        return new Response("Server at maximum capacity", { 
          status: 503,
          headers: { 'Retry-After': '10' }
        });
      }

      const room = url.searchParams.get("room") || "LowCard";
      const username = url.searchParams.get("username") || "Anonymous";

      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];

      try {
        server.accept();
      } catch(e) {
        try { server.close(1008, "Accept failed"); } catch(err) {}
        return new Response("WebSocket acceptance failed", { status: 500 });
      }

      const wsId = crypto.randomUUID();

      this.wsConnections.set(wsId, {
        ws: server,
        username: username,
        room: room,
        connectedAt: Date.now()
      });

      // ✅ SIMPAN KE D1
      const env = this.env;
      const existing = await env.DB.prepare(
        "SELECT username FROM users WHERE username = ?"
      ).bind(username).first();

      if (existing) {
        await env.DB.prepare(
          "UPDATE users SET ws_id = ?, room = ?, active = 1, last_active = ? WHERE username = ?"
        ).bind(wsId, room, Date.now(), username).run();
      } else {
        await env.DB.prepare(
          "INSERT INTO users (username, ws_id, room, active, last_active) VALUES (?, ?, ?, 1, ?)"
        ).bind(username, wsId, room, Date.now()).run();
      }

      // ✅ WEBSOCKET MESSAGE HANDLER
      server.addEventListener("message", async (event) => {
        try {
          if (server._closing || this.closing || this.isDestroyed) return;
          const data = JSON.parse(event.data);
          if (Array.isArray(data) && data.length > 0) {
            await this.handleEvent(server, data);
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

    // ============================================================
    // HEALTH
    // ============================================================
    if (pathname === "/game/health") {
      return new Response(JSON.stringify({
        status: "ok",
        connections: this.wsConnections.size,
        games: this.activeGames.size,
        timers: this.gameTimers.size,
        initialized: this._initialized,
        timestamp: Date.now()
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response("Game Server", { status: 200 });
  }

  // ========== HANDLE EVENT ==========
  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      
      const [evt, ...args] = data;
      const env = this.env;

      switch(evt) {
        // ============================================================
        // SWITCH ROOM
        // ============================================================
        case "switchRoom": {
          const [roomName, username] = args;
          await this.switchRoom(ws, roomName, username);
          break;
        }

        // ============================================================
        // GAME LOWCARD
        // ============================================================
        case "gameLowCardStart": {
          const [bet, username] = args;
          await this.startGame(ws, bet, username);
          break;
        }

        case "gameLowCardJoin": {
          const [username] = args;
          await this.joinGame(ws, username);
          break;
        }

        case "gameLowCardNumber": {
          const [number, tanda, username] = args;
          await this.submitNumber(ws, number, tanda, username);
          break;
        }

        case "gameLowCardLeave": {
          const [username] = args;
          await this.leaveGame(ws, username);
          break;
        }

        case "checkGameRunning": {
          const [roomName] = args;
          await this.checkGameRunning(ws, roomName);
          break;
        }

        case "getGameState": {
          const [roomName] = args;
          this._sendGameStateToClient(ws, roomName || ws.room || "LowCard");
          break;
        }

        // ============================================================
        // DICE
        // ============================================================
        case "submitDiceAnswer": {
          const [username, guess] = args;
          await this.submitDiceAnswer(ws, username, guess);
          break;
        }

        case "getDiceLastWeekWinner": {
          try {
            const winner = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_WEEK_WINNER, 'json');
            if (winner) {
              ws.send(JSON.stringify(["diceLastWeekWinner", winner.username, winner.score || 0, winner.week || ""]));
            } else {
              ws.send(JSON.stringify(["diceLastWeekWinner", "", 0, ""]));
            }
          } catch(e) { 
            ws.send(JSON.stringify(["diceLastWeekWinner", "", 0, ""])); 
          }
          break;
        }

        case "getDiceLeaderboard": {
          try {
            const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
            const sorted = Object.entries(points).sort((a, b) => b[1] - a[1]).slice(0, 10);
            ws.send(JSON.stringify(["diceLeaderboard", sorted.map(([u, s]) => `${u}|${s}`)]));
          } catch(e) { 
            ws.send(JSON.stringify(["diceLeaderboard", []])); 
          }
          break;
        }

        case "getDiceStatus": {
          ws.send(JSON.stringify(["diceStatus", !!this.currentDiceRoll && this._canSubmitDiceAnswer, this._diceRound || 1]));
          break;
        }

        // ============================================================
        // RECORDING
        // ============================================================
        case "startRecordingWinners": {
          const [roomName] = args;
          if (!roomName) { 
            ws.send(JSON.stringify(["recordingError", "Room name required"])); 
            break;
          }
          const success = await this._startRecordingWinners(roomName);
          ws.send(JSON.stringify(["startRecordingResult", { success, message: success ? "Recording enabled" : "Failed" }]));
          break;
        }

        case "stopRecordingWinners": {
          const [roomName] = args;
          if (!roomName) { 
            ws.send(JSON.stringify(["recordingError", "Room name required"])); 
            break;
          }
          const success = await this._stopRecordingWinners(roomName);
          ws.send(JSON.stringify(["stopRecordingResult", { success, message: success ? "Recording stopped" : "Failed" }]));
          break;
        }

        case "getRecordingStatus": {
          const [roomName] = args;
          if (!roomName) { 
            ws.send(JSON.stringify(["recordingError", "Room name required"])); 
            break;
          }
          const isRecording = await this._getRecordingStatusFromKV(roomName);
          ws.send(JSON.stringify(["recordingStatus", isRecording]));
          break;
        }

        case "sendWinnersToRoom": {
          const [roomName] = args;
          const roomToSend = roomName || ws.room || "LowCard";
          if (!roomToSend) { 
            ws.send(JSON.stringify(["recordingError", "Room name required"])); 
            break;
          }
          await this._broadcastLowCardWinners(roomToSend);
          ws.send(JSON.stringify(["sendWinnersResult", { success: true, message: "Winners refreshed" }]));
          break;
        }

        case "getRoomWinners": {
          const [roomName] = args;
          const roomToSend = roomName || ws.room || "LowCard";
          if (!roomToSend) { 
            ws.send(JSON.stringify(["recordingError", "Room name required"])); 
            break;
          }
          const isRecording = await this._getRecordingStatusFromKV(roomToSend);
          const winners = await this._getLowCardWinners(roomToSend);
          this._broadcastToRoom(roomToSend, ["lowCardWinnerUpdate", { winners, room: roomToSend, recording: isRecording }]);
          ws.send(JSON.stringify(["sendWinnersResult", { success: true, message: "Winners updated" }]));
          break;
        }

        case "startGameWithRecording": {
          const [_, roomName, bet, username] = args;
          await this._startGameWithRecording(ws, roomName, bet, username);
          break;
        }

        default:
          ws.send(JSON.stringify(["error", `Unknown event: ${evt}`]));
          break;
      }
    } catch(e) {
      console.error("Handle event error:", e);
    }
  }

  // ========== SWITCH ROOM ==========
  async switchRoom(ws, room, username) {
    if (!room || !ROOMS.includes(room)) {
      ws.send(JSON.stringify(["gameLowCardError", "Invalid room"]));
      return;
    }

    await this.env.DB.prepare(
      "UPDATE users SET room = ?, last_active = ? WHERE username = ?"
    ).bind(room, Date.now(), username).run();

    ws.send(JSON.stringify(["switchRoomSuccess", room]));
    this._sendGameStateToClient(ws, room);
  }

  // ========== START GAME ==========
  async startGame(ws, bet, username) {
    try {
      if (this.isDestroyed) {
        ws.send(JSON.stringify(["gameLowCardError", "Server is shutting down"]));
        return;
      }
      if (!username?.trim()) {
        ws.send(JSON.stringify(["gameLowCardError", "Username is required"]));
        return;
      }
      
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || "LowCard";
      
      const lockKey = `game_start_${room}`;
      if (this._gameLocks.has(lockKey)) {
        ws.send(JSON.stringify(["gameLowCardError", "Game is starting, please wait"]));
        return;
      }
      
      this._gameLocks.set(lockKey, Date.now());

      try {
        const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
        if (isRecordingEnabled) {
          ws.send(JSON.stringify(["gameLowCardError", "Recording is ACTIVE in this room. Users cannot start games."]));
          return;
        }

        const existingGame = this.activeGames.get(room);
        if (existingGame?._isActive && !existingGame._gameEnded) {
          ws.send(JSON.stringify(["gameLowCardError", "Game is already running"]));
          return;
        }
        if (existingGame) await this._forceCleanupGame(room, existingGame);
        
        const betAmount = parseInt(bet, 10) || 0;
        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          ws.send(JSON.stringify(["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]));
          return;
        }
        
        if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
          ws.send(JSON.stringify(["gameLowCardError", "Server is busy"]));
          return;
        }
        
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
          playerWsId: new Map(),
          _startedByRecording: false, 
          _startedBy: 'user'
        };
        
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        this.activeGames.set(room, game);
        this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
        this._broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
        
        // ✅ START REGISTRATION TIMER (20 DETIK)
        this._startRegistration(room, game);
        
      } finally {
        setTimeout(() => {
          this._gameLocks.delete(lockKey);
        }, 3000);
      }
    } catch(e) {}
  }

  // ========== START REGISTRATION ==========
  _startRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      if (game._registrationTimer) { 
        this._clearTimer(game._registrationTimer); 
        game._registrationTimer = null; 
      }
      
      let timeLeft = 20;
      this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
      
      const timer = setInterval(() => {
        try {
          if (!this._isGameActuallyRunning(game) || !game.registrationOpen || timeLeft < 0) {
            this._clearTimer(timer);
            if (game._registrationTimer === timer) game._registrationTimer = null;
            return;
          }
          
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          
          if (timeLeft > 0 && timeLeft !== 15 && timeLeft !== 10 && timeLeft !== 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          
          if (timeLeft === 0) {
            this._clearTimer(timer);
            game._registrationTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeRegistration(room, game);
          }
          timeLeft--;
        } catch(e) { 
          this._clearTimer(timer); 
          if (game._registrationTimer === timer) game._registrationTimer = null; 
        }
      }, 1000);
      
      game._registrationTimer = timer;
    } catch(e) {}
  }

  // ========== CLOSE REGISTRATION ==========
  _closeRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      game.registrationOpen = false;
      if (game._registrationTimer) { 
        this._clearTimer(game._registrationTimer); 
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

  // ========== START DRAW PHASE ==========
  _startDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { 
        this._clearTimer(game._drawTimer); 
        game._drawTimer = null; 
      }
      if (game._evalTimer) { 
        this._clearTimer(game._evalTimer); 
        game._evalTimer = null; 
      }
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
            
            if (game._startedByRecording) {
              this._addLowCardWinner(room, winner);
              const winners = this._getLowCardWinners(room);
              this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
            }
            
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
      
      // ✅ START DRAW COUNTDOWN (20 DETIK)
      this._startDrawCountdown(room, game);
      
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        this._startBotDraws(room, game);
      }
    } catch(e) {}
  }

  // ========== START DRAW COUNTDOWN ==========
  _startDrawCountdown(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { 
        this._clearTimer(game._drawTimer); 
        game._drawTimer = null; 
      }
      
      let timeLeft = 20;
      this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
      
      const timer = setInterval(() => {
        try {
          if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || timeLeft < 0) {
            this._clearTimer(timer);
            if (game._drawTimer === timer) game._drawTimer = null;
            return;
          }
          
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          
          if (timeLeft > 0 && timeLeft !== 15 && timeLeft !== 10 && timeLeft !== 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          
          if (timeLeft === 0) {
            this._clearTimer(timer);
            game._drawTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeDrawPhase(room, game);
          }
          timeLeft--;
        } catch(e) { 
          this._clearTimer(timer); 
          if (game._drawTimer === timer) game._drawTimer = null; 
        }
      }, 1000);
      
      game._drawTimer = timer;
    } catch(e) {}
  }

  // ========== CLOSE DRAW PHASE ==========
  async _closeDrawPhase(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || game.evaluationLocked) return;
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      if (game._drawTimer) { 
        this._clearTimer(game._drawTimer); 
        game._drawTimer = null; 
      }
      
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) this._forceBotDraw(room, botId, game);
      }
      
      const activeIds = this._getActivePlayerIds(game);
      const submittedIds = new Set(game.numbers?.keys() || []);
      const notSubmitted = activeIds.filter(id => !submittedIds.has(id) && !game.eliminated?.has(id));
      
      if (notSubmitted.length > 0 && submittedIds.size === 0) {
        this._broadcastToRoom(room, ["gameLowCardError", "No one submitted numbers"]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._scheduleGameCleanup(room, game);
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
          const winners = await this._getLowCardWinners(room);
          this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        }
        
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (remaining.length === 0) {
        this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      if (game._evalTimer) { 
        this._clearTimer(game._evalTimer); 
        game._evalTimer = null; 
      }
      const evalTimer = setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            this._evaluateRound(room, game);
          }
        } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS);
      game._evalTimer = evalTimer;
      
    } catch(e) {}
  }

  // ========== JOIN GAME ==========
  async joinGame(ws, username) {
    try {
      if (this.isDestroyed) {
        ws.send(JSON.stringify(["gameLowCardError", "Server is shutting down"]));
        return;
      }
      if (!username?.trim()) {
        ws.send(JSON.stringify(["gameLowCardError", "Username is required"]));
        return;
      }
      
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || "LowCard";
      
      const lockKey = `join_${room}_${usernameClean}`;
      if (this._joinLocks.has(lockKey)) {
        ws.send(JSON.stringify(["gameLowCardError", "Please wait"]));
        return;
      }
      
      this._joinLocks.set(lockKey, Date.now());
      
      try {
        const game = this.activeGames.get(room);
        if (!game?._isActive || game._gameEnded || !game.players) {
          ws.send(JSON.stringify(["gameLowCardError", "No active game in this room"]));
          return;
        }
        if (game.players.has(usernameClean)) {
          if (game.eliminated?.has(usernameClean)) {
            ws.send(JSON.stringify(["gameLowCardError", "You have been eliminated"]));
            return;
          }
          if (game.numbers.has(usernameClean)) {
            ws.send(JSON.stringify(["gameLowCardPlayerDraw", usernameClean, game.numbers.get(usernameClean), game.tanda.get(usernameClean) || ""]));
          }
          this._sendGameStateToClient(ws, room);
          return;
        }
        if (!game.registrationOpen) {
          ws.send(JSON.stringify(["gameLowCardNoJoin", usernameClean, game.betAmount]));
          ws.send(JSON.stringify(["gameLowCardError", "Registration is closed"]));
          return;
        }
        if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
          ws.send(JSON.stringify(["gameLowCardError", "Game is full"]));
          return;
        }
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        game.playerWsId.set(usernameClean, ws._wsId);
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
        ws.send(JSON.stringify(["gameLowCardError", "Server is shutting down"]));
        return;
      }
      if (!username?.trim()) {
        ws.send(JSON.stringify(["gameLowCardError", "Username is required"]));
        return;
      }
      
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || "LowCard";
      
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        ws.send(JSON.stringify(["gameLowCardError", "No active game"]));
        return;
      }
      if (game.players.has(usernameClean) && game.eliminated?.has(usernameClean)) {
        ws.send(JSON.stringify(["gameLowCardError", "You have been eliminated"]));
        return;
      }
      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw') {
        ws.send(JSON.stringify(["gameLowCardError", "Cannot submit now"]));
        return;
      }
      if (!game.players.has(usernameClean)) {
        ws.send(JSON.stringify(["gameLowCardError", "You are not in this game"]));
        return;
      }
      if (game.numbers.has(usernameClean)) {
        ws.send(JSON.stringify(["gameLowCardError", "You have already submitted"]));
        return;
      }
      
      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        ws.send(JSON.stringify(["gameLowCardError", "Invalid number (1-12)"]));
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
          this._clearTimer(game._evalTimer); 
          game._evalTimer = null; 
        }
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        const evalTimer = setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
              this._evaluateRound(room, game);
            }
          } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS);
        game._evalTimer = evalTimer;
      }
    } catch(e) {}
  }

  // ========== LEAVE GAME ==========
  async leaveGame(ws, username) {
    try {
      if (this.isDestroyed) {
        ws.send(JSON.stringify(["gameLowCardError", "Server is shutting down"]));
        return;
      }
      if (!username?.trim()) {
        ws.send(JSON.stringify(["gameLowCardError", "Username is required"]));
        return;
      }
      
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || "LowCard";
      
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        ws.send(JSON.stringify(["gameLowCardError", "No active game in this room"]));
        return;
      }
      if (!game.players.has(usernameClean)) {
        ws.send(JSON.stringify(["gameLowCardError", "You are not in this game"]));
        return;
      }
      this._removePlayerFromGame(usernameClean, room);
    } catch(e) {}
  }

  // ========== CHECK GAME RUNNING ==========
  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) {
        ws.send(JSON.stringify(["gameStatus", "false"]));
        return;
      }
      let room = roomname || ws.room || ws.roomname || "LowCard";
      if (!room) {
        ws.send(JSON.stringify(["gameStatus", "false"]));
        return;
      }
      const game = this.activeGames.get(room);
      const isRunning = game?._isActive && !game._gameEnded && game.players?.size > 0;
      ws.send(JSON.stringify(["gameStatus", isRunning ? "true" : "false"]));
      if (isRunning) this._sendGameStateToClient(ws, room);
    } catch(e) {}
  }

  // ============================================================
  // HELPER FUNCTIONS
  // ============================================================

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

  _clearTimer(timer) {
    if (timer) {
      clearTimeout(timer);
      clearInterval(timer);
    }
  }

  _scheduleGameCleanup(room, game) {
    try {
      if (!room || !game) return;
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      if (!game._gameEnded) return;
      const timer = setTimeout(() => {
        const currentGame = this.activeGames.get(room);
        if (currentGame?._isActive && !currentGame._gameEnded) {
          this._cleanupTimers.delete(room);
          return;
        }
        this._cleanupTimers.delete(room);
        const gameToDelete = this.activeGames.get(room);
        if (gameToDelete) this._deleteGame(room, gameToDelete);
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS);
      this._cleanupTimers.set(room, timer);
    } catch(e) {}
  }

  _deleteGame(room, game) {
    try {
      if (!room || !game) return;
      if (game?._isActive && !game._gameEnded) return;
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      if (game) {
        game._gameEnded = true;
        game._isActive = false;
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
      game.playerWsId = null;
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
      
      if (game._botTimeouts.size >= CONSTANTS.MAX_BOT_TIMEOUTS) return;
      
      const notDrawn = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id))
        .slice(0, Math.min(CONSTANTS.MAX_BOT_DRAWS_PER_ROUND, CONSTANTS.MAX_BOT_TIMEOUTS - game._botTimeouts.size));
      
      for (const botId of notDrawn) {
        const delay = (Math.floor(Math.random() * 14) + 2) * 1000;
        const timeout = setTimeout(() => {
          const currentGame = this.activeGames.get(room);
          if (this._isGameActuallyRunning(currentGame) && !currentGame.drawTimeExpired &&
              !currentGame.evaluationLocked && !currentGame.numbers?.has(botId) && 
              !currentGame.eliminated?.has(botId)) {
            this._handleBotDraw(room, botId, currentGame);
          }
          currentGame?._botTimeouts?.delete(timeout);
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
        const evalTimer = setTimeout(() => {
          try { this._evaluateRound(room, game); } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS);
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

  // ========== EVALUATE ROUND ==========
  async _evaluateRound(room, game) {
    try {
      if (this.isDestroyed || !game?._isActive || game._gameEnded || game._isEvaluating || !game.players) return;
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game) return;
      
      game._isEvaluating = true;
      const safetyTimer = setTimeout(() => {
        if (game?._isEvaluating) { 
          game._isEvaluating = false; 
          this._scheduleGameCleanup(room, game); 
        }
      }, CONSTANTS.EVALUATION_TIMEOUT_MS);
      game._safetyTimer = safetyTimer;
      
      if (game._evalTimer) { 
        this._clearTimer(game._evalTimer); 
        game._evalTimer = null; 
      }
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
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        
        const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
        
        if (remaining.length === 1) {
          const winnerId = remaining[0];
          const winnerName = players.get(winnerId)?.name || winnerId;
          const totalCoin = (game.betAmount || 0) * players.size;
          
          if (game._startedByRecording) {
            await this._addLowCardWinner(room, winnerName);
            const winners = await this._getLowCardWinners(room);
            this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
          }
          
          this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
          game._gameEnded = true;
          game._isActive = false;
          game._isEvaluating = false;
          if (game._safetyTimer) { 
            this._clearTimer(game._safetyTimer); 
            game._safetyTimer = null; 
          }
          this._scheduleGameCleanup(room, game);
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
          this._clearTimer(game._safetyTimer); 
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
          const winners = await this._getLowCardWinners(room);
          this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        }
        
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (remaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
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
      
      if (game._safetyTimer) { 
        this._clearTimer(game._safetyTimer); 
        game._safetyTimer = null; 
      }
      if (this._isGameActuallyRunning(game) && !game._gameEnded) {
        this._startDrawPhase(room, game);
      }
      
    } catch(e) {}
  }

  // ========== REMOVE PLAYER FROM GAME ==========
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
        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winner);
          const winners = await this._getLowCardWinners(room);
          this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        }
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }

  // ========== BROADCAST ==========
  _broadcastToRoom(room, message) {
    const msgStr = JSON.stringify(message);
    const users = this.env.DB.prepare(
      "SELECT ws_id FROM users WHERE room = ? AND active = 1"
    ).bind(room).all();

    for (const user of users.results || []) {
      const conn = this.wsConnections.get(user.ws_id);
      if (conn && conn.ws && conn.ws.readyState === 1) {
        try {
          conn.ws.send(msgStr);
        } catch(e) {}
      }
    }
  }

  // ========== SEND GAME STATE ==========
  _sendGameStateToClient(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room) return;
      const game = this.activeGames.get(room);
      if (!game || !game._isActive || game._gameEnded) {
        ws.send(JSON.stringify(["gameState", { room, hasGame: false, gameType: 'lowcard' }]));
        return;
      }
      
      const activePlayers = this._getActivePlayers(game);
      const allPlayers = Array.from(game.players.values()).map(p => p.name);
      const eliminated = Array.from(game.eliminated || []);
      const submitted = Array.from(game.numbers?.keys() || []);
      
      ws.send(JSON.stringify(["gameState", {
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
      }]));
    } catch(e) {}
  }

  // ========== RECORDING ==========
  async _getRecordingStatusFromKV(roomName) {
    try {
      if (!roomName) return false;
      if (this._recordingEnabled.has(roomName)) return this._recordingEnabled.get(roomName);
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
      if (await this._getRecordingStatusFromKV(roomName)) {
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
      
      const isRecording = await this._getRecordingStatusFromKV(room);
      if (!isRecording) {
        this._broadcastToRoom(room, ["recordingStatus", false]);
        return true;
      }
      
      this._recordingEnabled.set(room, false);
      
      if (this.env?.QUESTIONS) {
        await this.env.QUESTIONS.delete(CONSTANTS.LOWCARD_RECORDING_KEY + room);
        await this.env.QUESTIONS.delete(CONSTANTS.LOWCARD_WINNER_KEY + room);
        this._kvCache.delete(CONSTANTS.LOWCARD_WINNER_KEY + room);
        this._kvCache.delete(CONSTANTS.LOWCARD_RECORDING_KEY + room);
      }
      
      this._broadcastToRoom(room, ["recordingStatus", false]);
      return true;
    } catch(e) { return false; }
  }

  async _getLowCardWinners(room) {
    try {
      if (!room || !this.env?.QUESTIONS) return {};
      if (!await this._getRecordingStatusFromKV(room)) return {};
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      const winners = await this.env.QUESTIONS.get(key, 'json');
      return winners && typeof winners === 'object' ? winners : {};
    } catch(e) { return {}; }
  }

  async _broadcastLowCardWinners(room) {
    try {
      if (!room) return;
      if (!await this._getRecordingStatusFromKV(room)) return;
      const winners = await this._getLowCardWinners(room);
      this._broadcastToRoom(room, ["lowCardWinnerUpdate", {
        winners: winners || {},
        room: room,
        recording: true
      }]);
    } catch(e) {}
  }

  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username || room === DICE_ROOM) return false;
      if (!await this._getRecordingStatusFromKV(room)) return false;
      if (!this.env?.QUESTIONS) return false;
      
      const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
      let roomWinners = await this.env.QUESTIONS.get(key, 'json') || {};
      
      let count = 0;
      if (roomWinners[username]) {
        count = parseInt(String(roomWinners[username]).replace("x", "").replace("X", "")) || 0;
      }
      roomWinners[username] = (count + 1) + "x";
      
      await this.env.QUESTIONS.put(key, JSON.stringify(roomWinners));
      return true;
    } catch(e) { return false; }
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
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
    } catch(e) {}
  }

  async _startGameWithRecording(ws, room, bet, username) {
    try {
      if (!room || !username) {
        ws.send(JSON.stringify(["gameLowCardError", "Room and username required"]));
        return;
      }

      const isRecordingEnabled = await this._getRecordingStatusFromKV(room);
      if (!isRecordingEnabled) {
        ws.send(JSON.stringify(["gameLowCardError", "Recording is not enabled in this room"]));
        return;
      }

      const existingGame = this.activeGames.get(room);
      if (existingGame?._isActive && !existingGame._gameEnded) {
        ws.send(JSON.stringify(["gameLowCardError", "Game is already running"]));
        return;
      }
      if (existingGame) await this._forceCleanupGame(room, existingGame);

      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
        ws.send(JSON.stringify(["gameLowCardError", "Invalid bet (0 or 100-100000)"]));
        return;
      }
      if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
        ws.send(JSON.stringify(["gameLowCardError", "Server is busy"]));
        return;
      }

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
        _startedBy: 'recording'
      };

      game.players.set(username, { id: username, name: username });
      game.playerWsId.set(username, ws._wsId);
      this.activeGames.set(room, game);
      this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
      this._broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount]);
      this._startRegistration(room, game);
    } catch(e) {
      ws.send(JSON.stringify(["gameLowCardError", "Failed to start game"]));
    }
  }

  // ============================================================
  // WEBSOCKET CLOSE
  // ============================================================
  webSocketClose(ws) {
    try {
      if (!ws) return;
      ws._closing = true;
      
      // Hapus dari connections
      for (const [id, conn] of this.wsConnections) {
        if (conn.ws === ws) {
          this.wsConnections.delete(id);
          break;
        }
      }
    } catch(e) {}
  }

  webSocketError(ws) {
    try {
      if (!ws) return;
      ws._closing = true;
      
      for (const [id, conn] of this.wsConnections) {
        if (conn.ws === ws) {
          this.wsConnections.delete(id);
          break;
        }
      }
    } catch(e) {}
  }

  // ============================================================
  // CHECK STUCK GAMES
  // ============================================================
  _checkStuckGames() {
    try {
      const now = Date.now();
      for (const [room, game] of this.activeGames) {
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

  // ============================================================
  // CLEANUP
  // ============================================================
  _performCleanup() {
    try {
      this._cleanupDeadConnections();
      this._cleanupMemory();
    } catch(e) {}
  }

  _cleanupDeadConnections() {
    try {
      for (const [id, conn] of this.wsConnections) {
        if (!conn.ws || conn.ws.readyState !== 1) {
          this.wsConnections.delete(id);
        }
      }
    } catch(e) {}
  }

  _cleanupMemory() {
    try {
      // Hapus game yang sudah selesai
      for (const [room, game] of this.activeGames) {
        if (game._gameEnded && game._endTime && (Date.now() - game._endTime) > 60000) {
          this.activeGames.delete(room);
          this._gameLocks.delete(room);
          this._joinLocks.delete(room);
        }
      }
    } catch(e) {}
  }

  // ============================================================
  // DICE - CONTINUED
  // ============================================================
  _getDicePoints() {
    try {
      return this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
    } catch(e) { return {}; }
  }

  _setDicePoints(points) {
    try {
      return this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
    } catch(e) {}
  }

  // ============================================================
  // DESTROY
  // ============================================================
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
      for (const timer of this._allTimers) {
        try { clearTimeout(timer); clearInterval(timer); } catch(e) {}
      }
      this._allTimers.clear();
      
      if (this._mainInterval) { clearInterval(this._mainInterval); this._mainInterval = null; }
      if (this._cleanupInterval) { clearInterval(this._cleanupInterval); this._cleanupInterval = null; }
      
      if (this._diceTimerInterval) {
        clearInterval(this._diceTimerInterval);
        this._diceTimerInterval = null;
      }
      
      this._clearTimer(this._diceTimeout);
      
      for (const [room, game] of this.activeGames) {
        await this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();
      
      this.wsConnections.clear();
      
    } catch(e) {}
  }
}
