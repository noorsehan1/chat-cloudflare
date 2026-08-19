// ==================== GAME-SERVER.JS - PURE WORKER ====================
// VERSION: 4.0.0

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

// ==================== GAME SERVER - PURE WORKER ====================
export class GameServer {
  constructor(env, savedState = null) {
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    this._initialized = true;
    this._startTime = Date.now();
    this._wsIdCounter = 0;
    this._lastActivity = Date.now();
    
    // CORE MAPS (Shared)
    this.activeGames = new Map();
    this.wsMap = new Map();
    this.wsClients = new Map();
    this.clientRooms = new Map();
    this.userConnections = new Map();
    this.userRooms = new Map();
    
    // DICE VARIABLES
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
    this.diceAutoEnabled = true;
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
    this._dicePoints = new Map();
    
    // TIE BREAKER
    this._tieBreakers = new Map();
    this._tieRound = 0;
    this._tiePlayers = [];
    this._tieAnswers = new Map();
    this._tieTimer = null;
    this._tieInterval = null;
    this._tieLock = false;
    
    // QUEUE
    this._eventQueue = [];
    this._isProcessingQueue = false;
    this._allTimers = new Set();
    this._gameLocks = new Map();
    this._joinLocks = new Map();
    this._cleanupTimers = new Map();
    this._switchLocks = new Map();
    this._switchRetries = new Map();
    
    // CIRCUIT BREAKER
    this._requestCount = 0;
    this._lastResetTime = Date.now();
    this._circuitOpen = false;
    this._errorCount = 0;
    this._lastErrorReset = Date.now();
    this._tickCount = 0;
    
    // RECONNECT PROTECTION
    this._reconnectAttempts = new Map();
    this._bannedUsers = new Map();
    
    // CACHE
    this._cachedResetWeek = null;
    this._cachedLastWeekWinner = null;
    this._recordingEnabled = new Map();
    this._kvCache = new Map();
    this.DICE_ROOM = DICE_ROOM;
    
    // Restore state
    if (savedState) {
      this.restoreState(savedState);
    }
    
    // INTERVALS
    this._mainInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._tickCount++;
        this._doTick();
      }
    }, 10000);
    this._allTimers.add(this._mainInterval);
    
    this._cleanupInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._performCleanup();
      }
    }, 60000);
    this._allTimers.add(this._cleanupInterval);
    
    this._diceCheckInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._checkDice();
      }
    }, 10000);
    this._allTimers.add(this._diceCheckInterval);
    
    // Load KV
    this._loadKVData().catch(() => {});
    
    // Start dice
    setTimeout(() => {
      if (!this.closing && !this.isDestroyed) {
        this._checkDice();
      }
    }, 5000);
  }

  // ========== RESTORE STATE ==========
  restoreState(state) {
    try {
      if (state.activeGames) {
        for (const [room, gameData] of Object.entries(state.activeGames)) {
          const game = {
            room: gameData.room,
            players: new Map(Object.entries(gameData.players || {})),
            botPlayers: new Map(Object.entries(gameData.botPlayers || {})),
            registrationOpen: gameData.registrationOpen || false,
            round: gameData.round || 1,
            numbers: new Map(Object.entries(gameData.numbers || {})),
            tanda: new Map(Object.entries(gameData.tanda || {})),
            eliminated: new Set(gameData.eliminated || []),
            betAmount: gameData.betAmount || 0,
            hostId: gameData.hostId || '',
            hostName: gameData.hostName || '',
            useBots: gameData.useBots || false,
            evaluationLocked: gameData.evaluationLocked || false,
            drawTimeExpired: gameData.drawTimeExpired || false,
            _isActive: gameData._isActive || false,
            _gameEnded: gameData._gameEnded || false,
            _phase: gameData._phase || 'registration',
            _botTimeouts: new Set(),
            _botsAdded: gameData._botsAdded || false,
            _registrationTimer: null,
            _drawTimer: null,
            _evalTimer: null,
            _safetyTimer: null,
            _isEvaluating: false,
            _createdAt: gameData._createdAt || Date.now(),
            _drawPhaseStart: null,
            _endTime: null,
            playerWsId: new Map(Object.entries(gameData.playerWsId || {})),
            _startedByRecording: gameData._startedByRecording || false,
            _startedBy: gameData._startedBy || 'user'
          };
          this.activeGames.set(room, game);
        }
      }
      
      if (state.dicePoints) {
        this._dicePoints = new Map(Object.entries(state.dicePoints));
      }
      
      if (state.currentDiceRoll) {
        this.currentDiceRoll = state.currentDiceRoll;
      }
      
      if (state._diceRound) {
        this._diceRound = state._diceRound;
      }
      
      console.log(`[GameServer] Restored: ${this.activeGames.size} games`);
    } catch(e) {
      console.error('[GameServer] Restore error:', e);
    }
  }

  // ========== SAVE STATE ==========
  async saveState() {
    try {
      const state = {
        activeGames: {},
        dicePoints: Object.fromEntries(this._dicePoints),
        currentDiceRoll: this.currentDiceRoll,
        _diceRound: this._diceRound,
        timestamp: Date.now()
      };
      
      for (const [room, game] of this.activeGames) {
        if (!game._isActive || game._gameEnded) continue;
        state.activeGames[room] = {
          room: game.room,
          players: Object.fromEntries(game.players || new Map()),
          botPlayers: Object.fromEntries(game.botPlayers || new Map()),
          registrationOpen: game.registrationOpen,
          round: game.round,
          numbers: Object.fromEntries(game.numbers || new Map()),
          tanda: Object.fromEntries(game.tanda || new Map()),
          eliminated: Array.from(game.eliminated || new Set()),
          betAmount: game.betAmount,
          hostId: game.hostId,
          hostName: game.hostName,
          useBots: game.useBots,
          evaluationLocked: game.evaluationLocked,
          drawTimeExpired: game.drawTimeExpired,
          _isActive: game._isActive,
          _gameEnded: game._gameEnded,
          _phase: game._phase,
          _botsAdded: game._botsAdded,
          _createdAt: game._createdAt,
          playerWsId: Object.fromEntries(game.playerWsId || new Map()),
          _startedByRecording: game._startedByRecording,
          _startedBy: game._startedBy
        };
      }
      
      const cache = await caches.open('app_state_cache');
      const response = new Response(JSON.stringify(state), {
        headers: { 'Content-Type': 'application/json' }
      });
      await cache.put('game_server_state', response);
      
      return true;
    } catch(e) {
      return false;
    }
  }

  // ========== DO TICK ==========
  _doTick() {
    try {
      const tick = this._tickCount % 3;
      switch(tick) {
        case 0: 
          this._cleanupDeadConnections();
          this._checkDice();
          break;
        case 1: 
          this._checkStuckGames();
          break;
        case 2: 
          this._cleanupMemory();
          break;
      }
      
      if (this._tickCount % 30 === 0) {
        this.saveState().catch(() => {});
      }
    } catch(e) {}
  }

  // ========== HANDLE WEBSOCKET ==========
  async handleWebSocket(request) {
    try {
      const url = new URL(request.url);
      const room = url.searchParams.get("room") || "default";
      
      if (this._circuitOpen) {
        const now = Date.now();
        if (now - this._lastResetTime > 60000) {
          this._circuitOpen = false;
          this._requestCount = 0;
          this._lastResetTime = now;
        } else {
          return new Response("Service temporarily unavailable", { 
            status: 503,
            headers: { 'Retry-After': '30' }
          });
        }
      }
      
      this._requestCount++;
      if (this._requestCount > CONSTANTS.RATE_LIMIT_MAX) {
        this._circuitOpen = true;
        this._lastResetTime = Date.now();
        return new Response("Rate limit exceeded", { 
          status: 429,
          headers: { 'Retry-After': '60' }
        });
      }
      
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("WebSocket only", { status: 400 });
      }
      
      if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
        return new Response("Server full", { 
          status: 503,
          headers: { 'Retry-After': '10' }
        });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      const wsId = ++this._wsIdCounter;
      
      server._wsId = wsId;
      server._closing = false;
      server.room = room;
      server.roomname = room;
      server.username = null;
      server._createdAt = Date.now();
      
      try {
        server.accept();
      } catch(e) {
        try { server.close(1008, "Accept failed"); } catch(err) {}
        return new Response("WebSocket acceptance failed", { status: 500 });
      }
      
      this.wsMap.set(wsId, server);
      
      let clients = this.wsClients.get(room);
      if (!clients) {
        clients = new Set();
        this.wsClients.set(room, clients);
      }
      clients.add(wsId);
      this.clientRooms.set(wsId, room);
      
      console.log(`[${room}] New connection: ${wsId}, total: ${clients.size}`);
      
      // EVENT HANDLERS
      server.addEventListener("message", async (event) => {
        try {
          if (server._closing || this.closing || this.isDestroyed) return;
          
          let data;
          try {
            data = JSON.parse(event.data);
          } catch(e) {
            return;
          }
          
          if (Array.isArray(data) && data.length > 0) {
            if (data[0] === "joinGame" || data[0] === "startGame" || data[0] === "switchRoom") {
              const username = data[data.length - 1];
              if (username && typeof username === 'string') {
                server.username = username;
                this.userRooms.set(username, room);
              }
            }
            
            await this._processWithTimeout(server, data);
          }
        } catch(e) {}
      });
      
      server.addEventListener("close", () => {
        console.log(`[${room}] Connection closed: ${wsId}`);
        this.webSocketClose(server);
      });
      
      server.addEventListener("error", () => {
        console.log(`[${room}] Connection error: ${wsId}`);
        this.webSocketError(server);
      });
      
      // SEND INITIAL STATE
      if (room === DICE_ROOM) {
        this._sendDiceNotificationOnSwitch(server, wsId);
      }
      
      this._sendGameStateToClient(server, room);
      this._safeSend(server, ["switchRoomSuccess", room]);
      this._broadcastToRoom(room, ["userJoinedRoom", server.username || "Anonymous", room]);
      
      const usersInRoom = [];
      for (const [id, ws] of this.wsMap) {
        if (this.clientRooms.get(id) === room && ws.username) {
          usersInRoom.push(ws.username);
        }
      }
      this._safeSend(server, ["roomUsers", usersInRoom]);
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      console.error('[GameServer] handleWebSocket error:', e);
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ========== HANDLE HEALTH ==========
  handleHealth() {
    return new Response(JSON.stringify({
      status: "ok",
      uptime: Date.now() - this._startTime,
      connections: this.wsMap.size,
      games: this.activeGames.size,
      queue: this._eventQueue?.length || 0,
      circuitOpen: this._circuitOpen,
      errors: this._errorCount,
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // ========== BROADCAST TO ROOM ==========
  _broadcastToRoom(room, message) {
    try {
      if (this.closing || this.isDestroyed || !room || !message) return;
      const wsIds = this.wsClients.get(room);
      if (!wsIds?.size) return;
      
      const msgStr = JSON.stringify(message);
      const wsIdArray = Array.from(wsIds);
      
      for (let i = 0; i < wsIdArray.length; i += 10) {
        const batch = wsIdArray.slice(i, i + 10);
        for (const wsId of batch) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1 && !ws._closing) {
            try { ws.send(msgStr); } catch(e) {}
          }
        }
      }
    } catch(e) {}
  }

  // ========== SAFE SEND ==========
  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  // ========== GET WS ID ==========
  _getWsId(ws) {
    return ws?._wsId || null;
  }

  // ========== REMOVE CLIENT FROM ROOM ==========
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

  // ========== CLEANUP DEAD CONNECTIONS ==========
  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      for (const [wsId, ws] of this.wsMap) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(wsId);
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

  // ========== CLEANUP MEMORY ==========
  _cleanupMemory() {
    try {
      if (this._eventQueue && this._eventQueue.length > 50) {
        this._eventQueue.splice(0, this._eventQueue.length - 50);
      }
      
      const now = Date.now();
      for (const [key, time] of this._gameLocks) {
        if (now - time > 30000) this._gameLocks.delete(key);
      }
      for (const [key, time] of this._joinLocks) {
        if (now - time > 30000) this._joinLocks.delete(key);
      }
    } catch(e) {}
  }

  // ========== PERFORM CLEANUP ==========
  _performCleanup() {
    try {
      this._cleanupDeadConnections();
      this._cleanupUserConnections();
      this._cleanupReconnectAttempts();
      this._cleanupBannedUsers();
      
      if (Date.now() - this._lastErrorReset > CONSTANTS.ERROR_RESET_INTERVAL_MS) {
        this._errorCount = 0;
        this._lastErrorReset = Date.now();
      }
    } catch(e) {}
  }

  // ========== CLEANUP USER CONNECTIONS ==========
  _cleanupUserConnections() {
    try {
      let cleaned = 0;
      for (const [username, conn] of this.userConnections) {
        if (cleaned > 20) break;
        if (!conn?.ws || conn.ws.readyState !== 1) {
          this.userConnections.delete(username);
          cleaned++;
        }
      }
    } catch(e) {}
  }

  // ========== CLEANUP RECONNECT ATTEMPTS ==========
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

  // ========== CLEANUP BANNED USERS ==========
  _cleanupBannedUsers() {
    try {
      const now = Date.now();
      let cleaned = 0;
      for (const [username, banUntil] of this._bannedUsers) {
        if (cleaned > 10) break;
        if (now > banUntil) {
          this._bannedUsers.delete(username);
          cleaned++;
        }
      }
    } catch(e) {}
  }

  // ========== CHECK STUCK GAMES ==========
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

  // ========== TIMER MANAGEMENT ==========
  _clearTimer(timer) {
    if (timer) {
      clearTimeout(timer);
      clearInterval(timer);
      this._allTimers.delete(timer);
    }
  }

  // ========== PROCESS WITH TIMEOUT ==========
  _processWithTimeout(ws, data, timeoutMs = 500) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => resolve(), timeoutMs);
      this._allTimers.add(timer);
      
      this.handleEvent(ws, data).finally(() => {
        clearTimeout(timer);
        this._allTimers.delete(timer);
        resolve();
      });
    });
  }

  // ========== HANDLE EVENT ==========
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

  // ========== PROCESS EVENT QUEUE ==========
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
        } catch(e) {}
        processed++;
      }
      
      if (this._eventQueue.length > 0 && iteration < CONSTANTS.MAX_EVENT_ITERATIONS) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed) {
            this._isProcessingQueue = false;
            this._processEventQueue(iteration + 1);
          }
        }, 5);
      }
      
    } catch(e) {
    } finally {
      this._isProcessingQueue = false;
    }
  }

  // ========== PROCESS EVENT ITEM ==========
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

      if (evt === "getDiceLeaderboard") {
        try {
          const sorted = Array.from(this._dicePoints.entries())
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10);
          this._safeSend(ws, ["diceLeaderboard", sorted.map(([u, s]) => `${u}|${s}`)]);
        } catch(e) { this._safeSend(ws, ["diceLeaderboard", []]); }
        return;
      }

      if (evt === "getDiceStatus") {
        this._safeSend(ws, ["diceStatus", !!this.currentDiceRoll && this._canSubmitDiceAnswer, this._diceRound || 1]);
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

  // ========== GET ACTIVE PLAYERS ==========
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

  // ========== IS GAME ACTUALLY RUNNING ==========
  _isGameActuallyRunning(game) { 
    return game?._isActive === true && !game?._gameEnded; 
  }

  // ========== SEND GAME STATE TO CLIENT ==========
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
        this._safeSend(ws, ["switchRoomSuccess", currentRoom || roomName]);
        return;
      }
      
      this._switchLocks.set(lockKey, Date.now());
      
      try {
        if (currentRoom) this._removeClientFromRoom(currentRoom, wsId);
        
        let clients = this.wsClients.get(roomName);
        if (!clients) {
          clients = new Set();
          this.wsClients.set(roomName, clients);
        }
        clients.add(wsId);
        this.clientRooms.set(wsId, roomName);
        ws.room = roomName;
        ws.roomname = roomName;
        if (username) ws.username = username;
        
        if (username) {
          this.userRooms.set(username, roomName);
        }
        
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
        
        if (roomName === DICE_ROOM) {
          this._sendDiceNotificationOnSwitch(ws, wsId);
        }
        
        const usersInRoom = [];
        for (const [id, w] of this.wsMap) {
          if (this.clientRooms.get(id) === roomName && w.username) {
            usersInRoom.push(w.username);
          }
        }
        this._broadcastToRoom(roomName, ["roomUsers", usersInRoom]);
        
      } finally {
        setTimeout(() => {
          this._switchLocks.delete(lockKey);
        }, 2000);
      }
    } catch(e) {}
  }

  // ========== DICE METHODS ==========
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

  _checkDice() {
    try {
      if (this._tieActive || this._isShowingDice || this._diceTimeUpCooldown) return;
      if (!this._isDiceTime()) return;
      if (this.currentDiceRoll || this._diceTimeout || this._diceLock) return;
      
      const clients = this.wsClients?.get(DICE_ROOM);
      if (clients?.size > 0) {
        this._startDiceFast();
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
      
      this._broadcastToRoom(DICE_ROOM, ["diceRoll", { 
        value, 
        timestamp: Date.now(),
        answerTime: 20,
        canAnswerNow: true,
        round: this._diceRound
      }]);
      
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "♡ clik draw ♡"]);
      
      let timeLeft = 20;
      const timerInterval = setInterval(() => {
        timeLeft--;
        if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
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
        const points = this._dicePoints.get(winner) || 0;
        this._dicePoints.set(winner, points + 1);
        
        this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: points + 1,
          diceValue: diceValue,
          round: roundNumber
        }]);
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
      this._tieBreakers.set(id, { 
        players, 
        round: 0, 
        winner: null, 
        status: 'waiting' 
      });
      
      await this._runTieRound(room, id, players);
      
    } finally {
      setTimeout(() => {
        this._tieLock = false;
      }, 2000);
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
    
    this._tieInterval = setInterval(() => {
      timeLeft--;
      if (timeLeft === 10 && !notified10) { 
        notified10 = true; 
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "10s remaining"]); 
      }
      if (timeLeft === 5 && !notified5) { 
        notified5 = true; 
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "5s remaining"]); 
      }
      if (timeLeft === 3) {
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "3s remaining"]);
      }
      
      if (timeLeft <= 0 && !isProcessed) {
        isProcessed = true;
        clearInterval(this._tieInterval);
        this._tieInterval = null;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "TIME UP"]);
        
        const tieId = this._getActiveTieBreakerId();
        if (tieId) this._processTieResults(room, tieId, players);
        else { 
          this._resetTieBreakerState(null); 
          this._startCooldownAfterTieBreaker(); 
        }
      }
    }, 1000);
    this._allTimers.add(this._tieInterval);
    
    this._tieTimer = setTimeout(() => {
      if (!isProcessed) {
        isProcessed = true;
        clearInterval(this._tieInterval);
        this._tieInterval = null;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "TIME UP"]);
        
        const tieId = this._getActiveTieBreakerId();
        if (tieId) this._processTieResults(room, tieId, players);
        else { 
          this._resetTieBreakerState(null); 
          this._startCooldownAfterTieBreaker(); 
        }
      }
    }, (CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20) * 1000 + 2000);
    this._allTimers.add(this._tieTimer);
  }

  async _processTieResults(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    
    let highest = 0, highestPlayers = [];
    for (const player of players) {
      const answer = this._tieAnswers.get(player);
      if (answer !== undefined && answer >= 1 && answer <= 6) {
        if (answer > highest) { 
          highest = answer; 
          highestPlayers = [player]; 
        } else if (answer === highest) {
          highestPlayers.push(player);
        }
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
      const points = this._dicePoints.get(winner) || 0;
      this._dicePoints.set(winner, points + 1);
      
      this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: points + 1,
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
      this._allTimers.add(nextTimer);
      return;
    }
    
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  async _processSingleWinner(room, id, winner) {
    const points = this._dicePoints.get(winner) || 0;
    this._dicePoints.set(winner, points + 1);
    
    this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
      username: winner,
      totalPoints: points + 1,
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
    this._broadcastToRoom(DICE_ROOM, ["diceNotification", "wait 15s"]);
    this._diceTimeUpCooldown = true;
    
    this._clearTimer(this._diceTimeUpCooldownTimer);
    this._diceTimeUpCooldownTimer = setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._checkDice();
    }, CONSTANTS.TIE_BREAKER_COOLDOWN || 15000);
    this._allTimers.add(this._diceTimeUpCooldownTimer);
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
          username,
          guess: guessValue,
          isTieBreaker: true,
          tieRound: this._tieRound
        }]);
        
        if (this._tieAnswers.size === this._tiePlayers.length) {
          this._canSubmitDiceAnswer = false;
          this._isShowingDice = false;
          
          if (this._tieTimer) {
            clearTimeout(this._tieTimer);
            this._tieTimer = null;
          }
          if (this._tieInterval) {
            clearInterval(this._tieInterval);
            this._tieInterval = null;
          }
          
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
        username,
        guess: guessValue,
        round: this._diceRound || 1
      }]);
      
      if (guessValue === diceValue && !this.diceHasWinner) {
        this.diceHasWinner = true;
        this.diceWinner = username;
      }
      
    } catch(e) {}
  }

  // ========== SEND DICE NOTIFICATION ==========
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
      
      if (!isDiceTime || !this.diceAutoEnabled) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed && ws && ws.readyState === 1) {
            this._safeSend(ws, ["diceNotification", `Next dice game in: ${timeLeft.text}`]);
          }
        }, 5000);
        return;
      }
      
      if (isDiceTime && !this.currentDiceRoll && this.diceAutoEnabled) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed && !this.currentDiceRoll && !this._isShowingDice) {
            this.forceStartDice();
          }
        }, 5000);
      }
      
    } catch(e) {}
  }

  // ========== LOAD KV DATA ==========
  async _loadKVData() {
    try {
      if (this.closing || this.isDestroyed) return;
      
      if (this.env?.QUESTIONS) {
        const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
        this._dicePoints = new Map(Object.entries(points));
      }
    } catch(e) {}
  }

  // ========== GAME START METHODS ==========
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
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
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
        this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
        this._broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
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
      if (game._registrationTimer) { this._clearTimer(game._registrationTimer); game._registrationTimer = null; }
      
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
      this._allTimers.add(timer);
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
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        this._startBotDraws(room, game);
      }
    } catch(e) {}
  }

  // ========== START DRAW COUNTDOWN ==========
  _startDrawCountdown(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      
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
      this._allTimers.add(timer);
    } catch(e) {}
  }

  // ========== CLOSE DRAW PHASE ==========
  async _closeDrawPhase(room, game) {
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
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
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
      if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
      const evalTimer = setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            this._evaluateRound(room, game);
          }
        } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS);
      game._evalTimer = evalTimer;
      this._allTimers.add(evalTimer);
      
    } catch(e) {}
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
      this._allTimers.add(safetyTimer);
      
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
        
        const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
        
        if (remaining.length === 1) {
          const winnerId = remaining[0];
          const winnerName = players.get(winnerId)?.name || winnerId;
          const totalCoin = (game.betAmount || 0) * players.size;
          game._gameEnded = true;
          game._isActive = false;
          game._isEvaluating = false;
          if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
          this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
          this._scheduleGameCleanup(room, game);
          return;
        }
        
        this._broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
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
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
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
      if (this._isGameActuallyRunning(game) && !game._gameEnded) {
        this._startDrawPhase(room, game);
      }
      
    } catch(e) {}
  }

  // ========== BOT DRAW METHODS ==========
  _getRandomCardTanda() { 
    return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; 
  }

  _getRandomDrawDelay() { 
    return (Math.floor(Math.random() * 14) + 2) * 1000; 
  }

  _getBotNumberByRound(round) {
    if (round <= 2) return Math.floor(Math.random() * 12) + 1;
    return Math.random() < 0.6 ?
      [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
      [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
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
        const delay = this._getRandomDrawDelay();
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
        this._allTimers.add(timeout);
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
        this._allTimers.add(evalTimer);
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
        const evalTimer = setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
              this._evaluateRound(room, game);
            }
          } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS);
        game._evalTimer = evalTimer;
        this._allTimers.add(evalTimer);
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
      this._allTimers.add(checkTimer);
      return true;
    } catch(e) { return false; }
  }

  // ========== CHECK GAME CAN CONTINUE ==========
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
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
        this._scheduleGameCleanup(room, game);
      }
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

  // ========== SCHEDULE GAME CLEANUP ==========
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
      this._allTimers.add(timer);
    } catch(e) {}
  }

  // ========== DELETE GAME ==========
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
        game.playerWsId = null;
        this._cleanupGame(game);
      }
      this.activeGames.delete(room);
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
    } catch(e) {}
  }

  // ========== CLEANUP GAME ==========
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

  // ========== FORCE CLEANUP GAME ==========
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

  // ========== WEBSOCKET CLOSE ==========
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
        this.userRooms.delete(username);
        this.userConnections.delete(username);
        if (room) {
          this._broadcastToRoom(room, ["userLeftRoom", username, room]);
        }
      }
      
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
    } catch(e) {}
  }

  // ========== WEBSOCKET ERROR ==========
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
        this.userRooms.delete(username);
        this.userConnections.delete(username);
      }
      
      ws.room = null;
      ws.roomname = null;
      ws._wsId = null;
      ws.username = null;
      ws._closing = true;
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
      
      if (this._mainInterval) { clearInterval(this._mainInterval); this._mainInterval = null; }
      if (this._cleanupInterval) { clearInterval(this._cleanupInterval); this._cleanupInterval = null; }
      if (this._diceCheckInterval) { clearInterval(this._diceCheckInterval); this._diceCheckInterval = null; }
      
      if (this._diceTimerInterval) {
        clearInterval(this._diceTimerInterval);
        this._diceTimerInterval = null;
      }
      
      this._clearTimer(this._diceTimeout);
      this._clearTimer(this._diceStartTimeout);
      this._clearTimer(this._diceTimeUpCooldownTimer);
      
      this._cachedResetWeek = null;
      this._cachedLastWeekWinner = null;
      this._recordingEnabled.clear();
      this._kvCache.clear();
      
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
      this.userRooms.clear();
      this._tieBreakers.clear();
      this._reconnectAttempts.clear();
      this._bannedUsers.clear();
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._switchLocks.clear();
      this._switchRetries.clear();
      this._recordingEnabled.clear();
      this._kvCache.clear();
      this._dicePoints.clear();
      
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
      
      await this.saveState();
      
    } catch(e) {}
  }
}
