// ==================== GAME SERVER - OPTIMIZED ====================

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
  MAX_RETRIES: 5,
  RETRY_DELAY_MS: 100,
  MAX_CONNECTION_AGE_MS: 300000, // 5 minutes
  CLEANUP_CHUNK_SIZE: 50,
};

export class GameServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    
    this.activeGames = new Map();
    this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
    this._gameLocks = new Map();
    this._joinLocks = new Map();
    
    this._wsIdCounter = 0;
    this.wsClients = new Map();
    this.clientRooms = new Map();
    this.wsMap = new Map();
    this.roomViewers = new Map();
    
    this.userConnections = new Map();
    this.connectionLocks = new Map();
    
    this._cleanupTimers = new Map();
    this._cleaningUp = false;
    
    // ✅ OPTIMIZED INTERVAL - 30 seconds instead of 60
    this._mainInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        try {
          this._doMainTask();
        } catch(e) {
          // Silent error
        }
      }
    }, 30000);
    
    // ✅ Separate cleanup interval
    this._cleanupInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        try {
          this._cleanupStaleGames();
          this._cleanupDeadConnections();
        } catch(e) {
          // Silent error
        }
      }
    }, 60000);
    
    this._lastActivityTime = Date.now();
  }
  
  // ==================== KEEP-ALIVE (NON-BLOCKING) ====================
  
  _doMainTask() {
    try {
      this._lastActivityTime = Date.now();
      
      // ✅ NON-BLOCKING - Use microtask queue
      for (const [room, game] of this.activeGames) {
        if (game && game._isActive && !game._gameEnded) {
          queueMicrotask(() => {
            if (!this.closing && !this.isDestroyed) {
              this._broadcastToRoom(room, ["_keepAlive", Date.now()]);
            }
          });
        }
      }
    } catch(e) {
      // Silent error
    }
  }
  
  // ==================== CLEANUP DEAD CONNECTIONS (CHUNKED) ====================
  
  _cleanupDeadConnections() {
    if (this._cleaningUp) return;
    this._cleaningUp = true;
    
    try {
      const toRemove = [];
      const now = Date.now();
      
      for (const [wsId, ws] of this.wsMap) {
        const isDead = !ws || 
                      ws.readyState !== 1 || 
                      ws._closing ||
                      (ws._createdAt && (now - ws._createdAt) > CONSTANTS.MAX_CONNECTION_AGE_MS);
        
        if (isDead) {
          toRemove.push(wsId);
        }
      }
      
      if (toRemove.length === 0) {
        this._cleaningUp = false;
        return;
      }
      
      // ✅ PROCESS IN CHUNKS (NON-BLOCKING)
      const chunkSize = CONSTANTS.CLEANUP_CHUNK_SIZE || 50;
      
      const processChunk = (index) => {
        if (index >= toRemove.length) {
          this._cleaningUp = false;
          return;
        }
        
        const chunk = toRemove.slice(index, index + chunkSize);
        
        for (const wsId of chunk) {
          const ws = this.wsMap.get(wsId);
          if (ws) {
            const room = this.clientRooms.get(wsId);
            if (room) {
              this._removeClientFromRoom(room, wsId);
            }
            this.clientRooms.delete(wsId);
            this.wsMap.delete(wsId);
            
            for (const [username, conn] of this.userConnections) {
              if (conn.wsId === wsId) {
                this.userConnections.delete(username);
                break;
              }
            }
          }
        }
        
        // ✅ Process next chunk asynchronously
        setImmediate(() => processChunk(index + chunkSize));
      };
      
      processChunk(0);
      
    } catch(e) {
      this._cleaningUp = false;
    }
  }
  
  // ==================== WEB SOCKET MANAGEMENT ====================
  
  _getWsId(ws) {
    return ws ? ws._wsId : null;
  }
  
  _lockUserConnection(username) {
    if (this.connectionLocks.has(username)) {
      return false;
    }
    this.connectionLocks.set(username, Date.now());
    return true;
  }
  
  _unlockUserConnection(username) {
    this.connectionLocks.delete(username);
  }
  
  _forceCleanupUserConnections(username, excludeWsId = null) {
    const conn = this.userConnections.get(username);
    if (!conn) {
      this._unlockUserConnection(username);
      return;
    }
    
    // ✅ FIX: Release lock if excluded
    if (excludeWsId !== null && conn.wsId === excludeWsId) {
      this._unlockUserConnection(username);
      return;
    }
    
    const oldWs = this.wsMap.get(conn.wsId);
    if (oldWs && oldWs.readyState === 1) {
      try {
        this._safeSend(oldWs, ["gameLowCardReplaced", "New connection established"]);
        oldWs.close(1000, "Replaced by new connection");
      } catch(e) {}
    }
    
    if (conn.room) {
      this._removeClientFromRoom(conn.room, conn.wsId);
    }
    
    this.wsMap.delete(conn.wsId);
    this.clientRooms.delete(conn.wsId);
    
    if (conn.room && this.roomViewers.has(conn.room)) {
      this.roomViewers.get(conn.room).delete(username);
      if (this.roomViewers.get(conn.room).size === 0) {
        this.roomViewers.delete(conn.room);
      }
    }
    
    this.userConnections.delete(username);
    this._unlockUserConnection(username);
  }
  
  // ==================== ADD/REMOVE CLIENT (WITH RETRY) ====================
  
  _addClient(room, ws, username = null, isNewConnection = false, retryCount = 0) {
    const wsId = this._getWsId(ws);
    if (!wsId) {
      this._safeSend(ws, ["gameLowCardError", "Connection error, please reconnect"]);
      return;
    }
    
    // ✅ FIX: Retry limit
    if (retryCount > CONSTANTS.MAX_RETRIES) {
      this._safeSend(ws, ["gameLowCardError", "Connection timeout, please try again"]);
      return;
    }
    
    if (username && isNewConnection) {
      if (!this._lockUserConnection(username)) {
        setTimeout(() => {
          this._addClient(room, ws, username, isNewConnection, retryCount + 1);
        }, CONSTANTS.RETRY_DELAY_MS * (retryCount + 1));
        return;
      }
      
      try {
        this._forceCleanupUserConnections(username, wsId);
        this.userConnections.set(username, {
          wsId: wsId,
          ws: ws,
          room: room,
          timestamp: Date.now()
        });
      } finally {
        this._unlockUserConnection(username);
      }
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
      if (oldRoom !== room) {
        this._removeClientFromRoom(oldRoom, wsId);
      }
    }
    
    const clients = this.wsClients.get(room);
    if (clients) {
      clients.delete(wsId);
    }
    
    if (!this.wsClients.has(room)) {
      this.wsClients.set(room, new Set());
    }
    this.wsClients.get(room).add(wsId);
    this.clientRooms.set(wsId, room);
    this.wsMap.set(wsId, ws);
    ws.room = room;
    ws.username = username;
    
    if (username) {
      if (!this.roomViewers.has(room)) {
        this.roomViewers.set(room, new Set());
      }
      this.roomViewers.get(room).add(username);
    }
  }
  
  _removeClientFromRoom(room, wsId) {
    const clients = this.wsClients.get(room);
    if (clients) {
      clients.delete(wsId);
      if (clients.size === 0) {
        this.wsClients.delete(room);
      }
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
      if (conn && conn.wsId === wsId) {
        this.userConnections.delete(username);
      }
      
      if (this.roomViewers.has(room)) {
        this.roomViewers.get(room).delete(username);
        if (this.roomViewers.get(room).size === 0) {
          this.roomViewers.delete(room);
        }
      }
    }
    
    if (ws) {
      ws.room = null;
      ws._wsId = null;
      ws.username = null;
    }
  }
  
  _getRoomForWs(ws) {
    const wsId = this._getWsId(ws);
    if (!wsId) return null;
    return this.clientRooms.get(wsId) || null;
  }
  
  // ==================== SINGLE CONNECTION (WITH RETRY) ====================
  
  _ensureSingleConnection(room, username, newWs, newWsId, retryCount = 0) {
    const game = this.activeGames.get(room);
    if (!game) return newWsId;
    
    if (retryCount > CONSTANTS.MAX_RETRIES) {
      return newWsId;
    }
    
    if (this._lockUserConnection(username)) {
      try {
        this._forceCleanupUserConnections(username, newWsId);
        game.playerWsId.set(username, newWsId);
        this._addClient(room, newWs, username, true);
      } finally {
        this._unlockUserConnection(username);
      }
    } else {
      setTimeout(() => {
        this._ensureSingleConnection(room, username, newWs, newWsId, retryCount + 1);
      }, CONSTANTS.RETRY_DELAY_MS * (retryCount + 1));
    }
    
    return newWsId;
  }
  
  // ==================== ROOM MANAGEMENT ====================
  
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
    
    const oldRoom = this.clientRooms.get(wsId);
    
    if (oldRoom === roomName) {
      this._safeSend(ws, ["switchRoomSuccess", roomName]);
      this._sendGameStatusToWs(ws, roomName);
      return;
    }
    
    if (oldRoom) {
      this._removeClientFromRoom(oldRoom, wsId);
    }
    
    this._addClient(roomName, ws, username, false);
    ws.username = username;
    
    if (username) {
      const conn = this.userConnections.get(username);
      if (conn) {
        conn.room = roomName;
      }
    }
    
    this._broadcastToRoom(roomName, ["roomUserJoined", username || "Anonymous"]);
    this._safeSend(ws, ["switchRoomSuccess", roomName]);
    this._sendGameStatusToWs(ws, roomName);
  }
  
  _sendGameStatusToWs(ws, room) {
    const roomGame = this.activeGames.get(room);
    if (roomGame && roomGame._isActive && !roomGame._gameEnded) {
      this._safeSend(ws, ["gameLowCardStatus", {
        room: room,
        running: true,
        phase: roomGame._phase || 'idle',
        round: roomGame.round || 0,
        betAmount: roomGame.betAmount || 0,
        registrationOpen: roomGame.registrationOpen || false,
        players: Array.from(roomGame.players?.values() || []).map(p => p.name),
        eliminated: Array.from(roomGame.eliminated || []),
        numbers: Array.from(roomGame.numbers?.entries() || []).map(([name, num]) => ({ name, num })),
        totalPlayers: roomGame.players?.size || 0,
        activePlayers: this._getActivePlayers(roomGame).length
      }]);
    } else {
      this._safeSend(ws, ["gameLowCardStatus", {
        room: room,
        running: false,
        phase: 'idle',
        round: 0,
        betAmount: 0,
        registrationOpen: false,
        players: [],
        eliminated: [],
        numbers: [],
        totalPlayers: 0,
        activePlayers: 0
      }]);
    }
  }
  
  // ==================== OPTIMIZED BROADCAST ====================
  
  _broadcastToRoom(room, message) {
    if (this.closing || this.isDestroyed || !room || !message) return;
    
    const wsIds = this.wsClients.get(room);
    if (!wsIds || wsIds.size === 0) return;
    
    const msgStr = JSON.stringify(message);
    const BATCH_SIZE = CONSTANTS.BATCH_SIZE || 20;
    const wsIdArray = Array.from(wsIds);
    const disconnected = new Set();
    
    // ✅ SEND IN BATCHES
    for (let i = 0; i < wsIdArray.length; i += BATCH_SIZE) {
      const batch = wsIdArray.slice(i, i + BATCH_SIZE);
      
      for (const wsId of batch) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1) {
          try {
            ws.send(msgStr);
          } catch(e) {
            disconnected.add(wsId);
          }
        } else {
          disconnected.add(wsId);
        }
      }
    }
    
    // ✅ CLEANUP DISCONNECTED ASYNC
    if (disconnected.size > 0) {
      setImmediate(() => {
        for (const wsId of disconnected) {
          const ws = this.wsMap.get(wsId);
          if (ws) {
            this._removeClient(room, ws);
          } else {
            this._removeClientFromRoom(room, wsId);
            this.clientRooms.delete(wsId);
          }
        }
      });
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
  
  // ==================== PLAYER MANAGEMENT ====================
  
  _removePlayerFromGame(username, room) {
    try {
      const game = this.activeGames.get(room);
      if (!game) return false;
      
      if (!game.players || !game.players.has(username)) return false;
      if (!game._isActive || game._gameEnded) return false;
      
      if (!game.eliminated) game.eliminated = new Set();
      game.eliminated.add(username);
      
      this._broadcastToRoom(room, ["gameLowCardPlayerEliminated", username, "Disconnected"]);
      
      game.numbers?.delete(username);
      game.tanda?.delete(username);
      
      this._checkGameCanContinue(room, game);
      return true;
    } catch(e) {
      return false;
    }
  }
  
  _checkGameCanContinue(room, game) {
    try {
      if (!game || game._gameEnded || !game.players || !game._isActive) return;
      
      const activePlayers = this._getActivePlayers(game);
      
      if (activePlayers.length >= 2) return;
      
      if (activePlayers.length === 1 && !game._gameEnded) {
        const winner = activePlayers[0]?.name || "Unknown";
        const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
        
        game._gameEnded = true;
        game._isActive = false;
        
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (activePlayers.length === 0) {
        game._gameEnded = true;
        game._isActive = false;
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }
  
  _findAllGamesByUsername(username) {
    if (!username) return [];
    const result = [];
    for (const [room, game] of this.activeGames) {
      if (game._isActive && !game._gameEnded && game.players) {
        if (game.players.has(username)) {
          result.push({ game, room });
        }
      }
    }
    return result;
  }
  
  // ==================== HELPERS ====================
  
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
      return Math.random() < 0.8 ? 
        [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
        [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
    }
  }
  
  _getActivePlayers(game) {
    if (!game?._isActive || game._gameEnded || !game.players) return [];
    return Array.from(game.players.entries())
      .filter(([id]) => !game.eliminated?.has(id))
      .map(([, p]) => p);
  }
  
  _getActivePlayerIds(game) {
    if (!game?._isActive || game._gameEnded || !game.players) return [];
    return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
  }
  
  _isGameRunning(game) {
    return game && game._isActive === true && !game._gameEnded && !this.isDestroyed && game.players;
  }
  
  _safeGetGame(room) {
    if (this.isDestroyed || !room) return null;
    const game = this.activeGames.get(room);
    return (game?._isActive && !game._gameEnded && game.players) ? game : null;
  }
  
  // ==================== GAME CLEANUP ====================
  
  _scheduleGameCleanup(room, game) {
    if (this._cleanupTimers.has(room)) {
      clearTimeout(this._cleanupTimers.get(room));
      this._cleanupTimers.delete(room);
    }
    
    const timer = setTimeout(() => {
      try {
        this._cleanupTimers.delete(room);
        this._deleteGame(room, game);
      } catch(e) {
        // ✅ Prevent memory leak - ensure cleanup
        this._cleanupTimers.delete(room);
      }
    }, CONSTANTS.GAME_CLEANUP_DELAY_MS);
    
    this._cleanupTimers.set(room, timer);
  }
  
  _cleanupGame(game) {
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
      for (const id of game._botTimeouts) {
        clearTimeout(id);
      }
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
    if (this._cleanupTimers.has(room)) {
      clearTimeout(this._cleanupTimers.get(room));
      this._cleanupTimers.delete(room);
    }
    
    if (game) {
      game.playerWsId = null;
      this._cleanupGame(game);
    }
    this.activeGames.delete(room);
    this._gameLocks.delete(room);
    this._joinLocks.delete(room);
    
    this._broadcastToRoom(room, ["gameLowCardEnd", []]);
  }
  
  // ==================== REGISTRATION ====================
  
  _startRegistration(room, game) {
    if (!this._isGameRunning(game) || !game.registrationOpen) return;
    
    if (game._registrationTimer) {
      clearInterval(game._registrationTimer);
      game._registrationTimer = null;
    }
    
    let timeLeft = 20;
    
    const timer = setInterval(() => {
      try {
        if (!this._isGameRunning(game) || !game.registrationOpen || timeLeft < 0) {
          clearInterval(timer);
          game._registrationTimer = null;
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
        game._registrationTimer = null;
      }
    }, 1000);
    
    game._registrationTimer = timer;
  }
  
  _closeRegistration(room, game) {
    try {
      if (!this._isGameRunning(game) || !game.registrationOpen) return;
      game.registrationOpen = false;
      
      if (game._registrationTimer) {
        clearInterval(game._registrationTimer);
        game._registrationTimer = null;
      }
      
      const humanPlayers = Array.from(game.players.keys()).filter(id => !id.startsWith('BOT_'));
      const humanCount = humanPlayers.length;
      
      if (humanCount === 1 && !game._botsAdded) {
        this._addBots(room, 4);
      }
      
      if (humanCount === 0) {
        this._addBots(room, 4);
      }
      
      if (game.players.size < 2) {
        const needed = Math.min(4 - game.players.size, CONSTANTS.MAX_BOTS_PER_GAME);
        if (needed > 0) {
          this._addBots(room, needed);
        }
      }
      
      if (this._isGameRunning(game) && game.players.size >= 2) {
        this._startDrawPhase(room, game);
      } else {
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }
  
  _addBots(room, count) {
    try {
      const game = this._safeGetGame(room);
      if (!this._isGameRunning(game)) return;
      
      const botNames = ["moz1", "moz2", "moz3", "moz4"];
      
      const existingBots = Array.from(game.players.keys()).filter(id => id.startsWith('BOT_'));
      const existingBotCount = existingBots.length;
      
      const maxBotsToAdd = Math.min(count, CONSTANTS.MAX_BOTS_PER_GAME - existingBotCount);
      
      for (let i = 0; i < maxBotsToAdd; i++) {
        // ✅ FIX: Better bot ID generation
        const timestamp = Date.now();
        const random = Math.random().toString(36).substring(2, 6);
        const botId = `BOT_${room}_${i}_${timestamp}_${random}`;
        const botName = botNames[(existingBotCount + i) % botNames.length];
        
        if (!game.players.has(botId)) {
          game.players.set(botId, { id: botId, name: botName });
          game.botPlayers.set(botId, botName);
        }
      }
      
      game._botsAdded = true;
      game.useBots = true;
    } catch(e) {}
  }
  
  // ==================== DRAW PHASE ====================
  
  _startDrawPhase(room, game) {
    try {
      if (!this._isGameRunning(game)) return;
      
      if (game._drawTimer) {
        clearInterval(game._drawTimer);
        game._drawTimer = null;
      }
      
      if (game._evalTimer) {
        clearTimeout(game._evalTimer);
        game._evalTimer = null;
      }
      
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) {
          clearTimeout(id);
        }
        game._botTimeouts.clear();
      }
      
      const activePlayers = this._getActivePlayers(game);
      
      if (activePlayers.length < 2) {
        const needed = Math.min(4 - activePlayers.length, CONSTANTS.MAX_BOTS_PER_GAME);
        if (needed > 0) {
          this._addBots(room, needed);
        }
        
        const newActive = this._getActivePlayers(game);
        if (newActive.length < 2) {
          if (newActive.length === 1 && !game._gameEnded) {
            const winner = newActive[0]?.name || "Unknown";
            const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
            game._gameEnded = true;
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
      
      if (!game._botTimeouts) game._botTimeouts = new Set();
      
      const playersList = this._getActivePlayers(game).map(p => p.name);
      
      this._broadcastToRoom(room, ["gameLowCardClosed", playersList]);
      this._broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
      
      this._startDrawCountdown(room, game);
      
      if (game.botPlayers?.size > 0 && this._isGameRunning(game)) {
        this._startBotDraws(room, game);
      }
    } catch(e) {}
  }
  
  _startDrawCountdown(room, game) {
    if (!this._isGameRunning(game)) return;
    
    if (game._drawTimer) {
      clearInterval(game._drawTimer);
      game._drawTimer = null;
    }
    
    let timeLeft = 20;
    
    const timer = setInterval(() => {
      try {
        if (!this._isGameRunning(game) || game.drawTimeExpired || timeLeft < 0) {
          clearInterval(timer);
          game._drawTimer = null;
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
        game._drawTimer = null;
      }
    }, 1000);
    
    game._drawTimer = timer;
  }
  
  _closeDrawPhase(room, game) {
    try {
      if (!this._isGameRunning(game) || game.drawTimeExpired || game.evaluationLocked) return;
      
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      
      if (game._drawTimer) {
        clearInterval(game._drawTimer);
        game._drawTimer = null;
      }
      
      if (game.botPlayers?.size > 0 && this._isGameRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) {
          this._forceBotDraw(room, botId, game);
        }
      }
      
      this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
      
      // ✅ FIX: Only schedule if not already evaluating
      if (!game._isEvaluating) {
        game._evalTimer = setTimeout(() => {
          try {
            this._evaluateRound(room, game);
          } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS);
      }
    } catch(e) {}
  }
  
  // ==================== BOT DRAWS ====================
  
  _startBotDraws(room, game) {
    try {
      if (!this._isGameRunning(game) || !game.botPlayers) return;
      
      if (!game._botTimeouts) game._botTimeouts = new Set();
      
      const notDrawn = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id))
        .slice(0, CONSTANTS.MAX_BOT_DRAWS_PER_ROUND);
      
      for (const botId of notDrawn) {
        const timeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (this._isGameRunning(currentGame) && 
                !currentGame.drawTimeExpired &&
                !currentGame.evaluationLocked &&
                !currentGame.numbers?.has(botId) &&
                !currentGame.eliminated?.has(botId)) {
              this._handleBotDraw(room, botId, currentGame);
            }
            currentGame?._botTimeouts?.delete(timeout);
          } catch(e) {}
        }, this._getRandomDrawDelay());
        
        game._botTimeouts.add(timeout);
      }
    } catch(e) {}
  }
  
  _handleBotDraw(room, botId, game) {
    try {
      if (!this._isGameRunning(game) || game.numbers?.has(botId) || game.drawTimeExpired || game.evaluationLocked) return;
      if (game.eliminated?.has(botId)) return;
      
      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      
      const botName = game.players.get(botId)?.name || botId;
      
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
      
      const activeIds = this._getActivePlayerIds(game);
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameRunning(game)) {
        game.evaluationLocked = true;
        this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
        if (!game._isEvaluating) {
          game._evalTimer = setTimeout(() => {
            try {
              this._evaluateRound(room, game);
            } catch(e) {}
          }, CONSTANTS.EVALUATION_DELAY_MS);
        }
      }
    } catch(e) {}
  }
  
  _forceBotDraw(room, botId, game) {
    try {
      if (!this._isGameRunning(game) || game.numbers?.has(botId)) return;
      if (game.eliminated?.has(botId)) return;
      
      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      
      const botName = game.players.get(botId)?.name || botId;
      
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
    } catch(e) {}
  }
  
  // ==================== EVALUATION ====================
  
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
            this._scheduleGameCleanup(room
