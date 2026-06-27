// ==================== GAME SERVER - DURABLE OBJECT ====================

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
    this.wsClients = new Map(); // room -> Set(wsId)
    this.clientRooms = new Map(); // wsId -> room
    this.wsMap = new Map(); // wsId -> ws
    this.roomViewers = new Map(); // room -> Set(username)
    
    // Track per user
    this.userConnections = new Map(); // username -> { wsId, ws, room, timestamp }
    this.connectionLocks = new Map(); // username -> true (sedang proses connect)
    
    this._cleanupTimers = new Map();
    
    this._cleanupInterval = setInterval(() => {
      this._cleanupStaleGames();
    }, 60000);
  }
  
  // ==================== WEB SOCKET MANAGEMENT ====================
  
  _getWsId(ws) {
    return ws ? ws._wsId : null;
  }
  
  // ==================== CORE: MENCEGAH DUPLIKAT EVENT ====================
  
  _lockUserConnection(username) {
    if (this.connectionLocks.has(username)) {
      return false;
    }
    this.connectionLocks.set(username, true);
    return true;
  }
  
  _unlockUserConnection(username) {
    this.connectionLocks.delete(username);
  }
  
  // Hanya untuk koneksi BARU (reconnect), BUKAN switch room
  _forceCleanupUserConnections(username, excludeWsId = null) {
    const conn = this.userConnections.get(username);
    if (!conn) return;
    
    // Jika excludeWsId sama dengan yang ada, berarti ini switch room, jangan hapus
    if (excludeWsId !== null && conn.wsId === excludeWsId) {
      return; // JANGAN hapus koneksi sendiri!
    }
    
    // Hapus koneksi lama (ini untuk reconnect)
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
  }
  
  // ==================== ADD/REMOVE CLIENT ====================
  
  _addClient(room, ws, username = null, isNewConnection = false) {
    const wsId = this._getWsId(ws);
    if (!wsId) {
      this._safeSend(ws, ["gameLowCardError", "Connection error, please reconnect"]);
      return;
    }
    
    // Hanya cleanup jika ini koneksi BARU (bukan switch room)
    if (username && isNewConnection) {
      if (!this._lockUserConnection(username)) {
        setTimeout(() => {
          this._addClient(room, ws, username, isNewConnection);
        }, 100);
        return;
      }
      
      try {
        // Hapus koneksi lama hanya jika koneksi baru
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
    
    // Update atau tambahkan ke room
    if (username && !isNewConnection) {
      // Ini switch room - update userConnections dengan room baru
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
    
    // Hapus dari room lama jika ada
    if (this.clientRooms.has(wsId)) {
      const oldRoom = this.clientRooms.get(wsId);
      if (oldRoom !== room) {
        this._removeClientFromRoom(oldRoom, wsId);
      }
    }
    
    // Tambahkan ke room baru
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
    
    // Track viewer
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
  
  // ==================== SINGLE CONNECTION (untuk JOIN/REJOIN) ====================
  
  _ensureSingleConnection(room, username, newWs, newWsId) {
    const game = this.activeGames.get(room);
    if (!game) return newWsId;
    
    // Ini adalah koneksi BARU (rejoin), jadi cleanup koneksi lama
    if (this._lockUserConnection(username)) {
      try {
        this._forceCleanupUserConnections(username, newWsId);
        game.playerWsId.set(username, newWsId);
        this._addClient(room, newWs, username, true); // isNewConnection = true
      } finally {
        this._unlockUserConnection(username);
      }
    } else {
      setTimeout(() => {
        this._ensureSingleConnection(room, username, newWs, newWsId);
      }, 100);
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
    
    // SWITCH ROOM - JANGAN hapus koneksi!
    // Hanya pindahkan dari room lama ke room baru
    if (oldRoom) {
      this._removeClientFromRoom(oldRoom, wsId);
    }
    
    // Tambahkan ke room baru - isNewConnection = FALSE
    this._addClient(roomName, ws, username, false);
    ws.username = username;
    
    // Update userConnections dengan room baru
    if (username) {
      const conn = this.userConnections.get(username);
      if (conn) {
        conn.room = roomName;
      }
    }
    
    this._sendGameStatusToWs(ws, roomName);
    this._broadcastToRoom(roomName, ["roomUserJoined", username || "Anonymous"]);
    this._safeSend(ws, ["switchRoomSuccess", roomName]);
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
  
  // ==================== BROADCAST ====================
  
  _broadcastToRoom(room, message) {
    if (this.closing || this.isDestroyed || !room || !message) return;
    
    const wsIds = this.wsClients.get(room);
    if (!wsIds || wsIds.size === 0) return;
    
    const msgStr = JSON.stringify(message);
    const disconnected = new Set();
    
    for (const wsId of wsIds) {
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
    
    if (disconnected.size > 0) {
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
      
      if (activePlayers.length >= 2) {
        return;
      }
      
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
    } catch(e) {
      // Silent error
    }
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
  
  _checkAndCleanupGame(room) {
    const game = this.activeGames.get(room);
    if (!game) return;
    
    if (game._gameEnded || !game._isActive || !game.players || game.players.size === 0) {
      this._scheduleGameCleanup(room, game);
    }
  }
  
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
        // Silent error
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
    
    let timeLeft = 20;
    
    const timer = setInterval(() => {
      try {
        if (!this._isGameRunning(game) || !game.registrationOpen) {
          clearInterval(timer);
          game._registrationTimer = null;
          return;
        }
        
        // Kurangi timeLeft di awal
        timeLeft--;
        
        // Broadcast di detik-detik tertentu
        if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
        
        // Saat timeLeft mencapai 0, tutup registrasi
        if (timeLeft <= 0) {
          clearInterval(timer);
          game._registrationTimer = null;
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          this._closeRegistration(room, game);
        }
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
    } catch(e) {
      // Silent error
    }
  }
  
  _addBots(room, count) {
    try {
      const game = this._safeGetGame(room);
      if (!this._isGameRunning(game)) return;
      
      const botNames = ["moz1", "moz2", "moz3", "moz4"];
      let added = 0;
      
      const existingBots = Array.from(game.players.keys()).filter(id => id.startsWith('BOT_'));
      const existingBotCount = existingBots.length;
      
      const maxBotsToAdd = Math.min(count, CONSTANTS.MAX_BOTS_PER_GAME - existingBotCount);
      
      for (let i = 0; i < maxBotsToAdd; i++) {
        const botId = `BOT_${room}_${i}_${Date.now()}`;
        const botName = botNames[(existingBotCount + i) % botNames.length];
        
        if (!game.players.has(botId)) {
          game.players.set(botId, { id: botId, name: botName });
          game.botPlayers.set(botId, botName);
          added++;
        }
      }
      
      game._botsAdded = true;
      game.useBots = true;
    } catch(e) {
      // Silent error
    }
  }
  
  // ==================== DRAW PHASE ====================
  
  _startDrawPhase(room, game) {
    try {
      if (!this._isGameRunning(game)) return;
      
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
    } catch(e) {
      // Silent error
    }
  }
  
  _startDrawCountdown(room, game) {
    if (!this._isGameRunning(game)) return;
    
    let timeLeft = 20;
    
    const timer = setInterval(() => {
      try {
        if (!this._isGameRunning(game) || game.drawTimeExpired) {
          clearInterval(timer);
          game._drawTimer = null;
          return;
        }
        
        // Kurangi timeLeft di awal
        timeLeft--;
        
        // Broadcast di detik-detik tertentu
        if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
        
        // Saat timeLeft mencapai 0, tutup fase draw
        if (timeLeft <= 0) {
          clearInterval(timer);
          game._drawTimer = null;
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          this._closeDrawPhase(room, game);
        }
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
      
      game._evalTimer = setTimeout(() => {
        try {
          this._evaluateRound(room, game);
        } catch(e) {
          // Silent error
        }
      }, CONSTANTS.EVALUATION_DELAY_MS);
    } catch(e) {
      // Silent error
    }
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
          } catch(e) {
            // Silent error
          }
        }, this._getRandomDrawDelay());
        
        game._botTimeouts.add(timeout);
      }
    } catch(e) {
      // Silent error
    }
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
        game._evalTimer = setTimeout(() => {
          try {
            this._evaluateRound(room, game);
          } catch(e) {
            // Silent error
          }
        }, CONSTANTS.EVALUATION_DELAY_MS);
      }
    } catch(e) {
      // Silent error
    }
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
    } catch(e) {
      // Silent error
    }
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
            this._scheduleGameCleanup(room, game);
          }
        } catch(e) {
          // Silent error
        }
      }, CONSTANTS.EVALUATION_TIMEOUT_MS);
      
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
      
      const numbers = game.numbers || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const tanda = game.tanda || new Map();
      
      const entries = Array.from(numbers.entries());
      const submittedIds = new Set(numbers.keys());
      const activeIds = this._getActivePlayerIds(game);
      
      for (const id of activeIds) {
        if (!submittedIds.has(id)) {
          eliminated.add(id);
        }
      }
      
      if (entries.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) {
          clearTimeout(game._safetyTimer);
          game._safetyTimer = null;
        }
        
        this._broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (entries.length === 1 && eliminated.size === activeIds.length - 1) {
        const winnerId = entries[0][0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        
        game._gameEnded = true;
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
      
      // JIKA SEMUA NILAI SAMA -> TIDAK ADA YANG TERELIMINASI
      if (!allSame && values.length > 0) {
        const lowest = Math.min(...values);
        losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
        for (const id of losers) {
          eliminated.add(id);
        }
      }
      // Jika allSame (semua nilai sama), tidak ada yang dieliminasi
      
      const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
      
      // CEK: Jika semua nilai sama (berapapun jumlah pemainnya) -> SERI, lanjut ronde berikutnya
      if (allSame && remaining.length >= 2) {
        // Tidak ada eliminasi, lanjut ke ronde berikutnya
        game._isEvaluating = false;
        
        if (game._safetyTimer) {
          clearTimeout(game._safetyTimer);
          game._safetyTimer = null;
        }
        
        // Reset untuk ronde berikutnya
        numbers.clear();
        tanda.clear();
        game.round++;
        game.evaluationLocked = false;
        game.drawTimeExpired = false;
        game._phase = 'draw';
        game.numbers = new Map();
        game.tanda = new Map();
        game._botTimeouts = new Set();
        
        // Broadcast hasil seri
        const remainingNames = remaining.map(id => players.get(id)?.name || id);
        this._broadcastToRoom(room, [
          "gameLowCardRoundResult", 
          game.round - 1, 
          entries.map(([id, n]) => {
            const name = players.get(id)?.name || id;
            const t = tanda.get(id) || "";
            return `${name}:${n}${t ? `(${t})` : ''}`;
          }),
          [], // Tidak ada yang tereliminasi
          remainingNames,
          true // Flag untuk menunjukkan ini seri
        ]);
        
        if (this._isGameRunning(game) && !game._gameEnded) {
          this._startDrawPhase(room, game);
        }
        return;
      }
      
      if (remaining.length === 1 && !game._gameEnded) {
        const winnerId = remaining[0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        
        game._gameEnded = true;
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
      
      if (this._isGameRunning(game) && !game._gameEnded) {
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
  
  // ==================== CHECK GAME RUNNING ====================
  
  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      
      let room = roomname;
      
      if (!room) {
        const wsId = this._getWsId(ws);
        if (wsId && this.clientRooms.has(wsId)) {
          room = this.clientRooms.get(wsId);
        }
      }
      
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Room name is required"]);
        return;
      }
      
      const game = this.activeGames.get(room);
      
      if (!game || !game._isActive || game._gameEnded || !game.players) {
        this._safeSend(ws, ["gameStatus", { running: false }]);
        return;
      }
      
      this._safeSend(ws, ["gameStatus", { 
        running: true,
        phase: game._phase || 'idle',
        round: game.round || 0,
        players: Array.from(game.players?.values() || []).map(p => p.name),
        betAmount: game.betAmount || 0,
        registrationOpen: game.registrationOpen || false,
        eliminated: Array.from(game.eliminated || []),
        totalPlayers: game.players?.size || 0,
        activePlayers: this._getActivePlayers(game).length
      }]);
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Error checking game"]);
    }
  }
  
  // ==================== PUBLIC METHODS ====================
  
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
      
      const existingGames = this._findAllGamesByUsername(usernameClean);
      if (existingGames.length > 0) {
        const roomList = existingGames.map(g => g.room).join(', ');
        this._safeSend(ws, ["gameLowCardInfo", `You are currently playing`]);
      }
      
      const room = this._getRoomForWs(ws);
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]);
        return;
      }
      
      const existingRoomGame = this.activeGames.get(room);
      if (existingRoomGame && existingRoomGame._isActive && !existingRoomGame._gameEnded && existingRoomGame.players) {
        if (existingRoomGame.players.has(usernameClean) && !existingRoomGame.eliminated?.has(usernameClean)) {
          this._safeSend(ws, ["gameLowCardInfo", `Game already running`]);
          this._safeSend(ws, ["gameLowCardStartSuccess", existingRoomGame.hostName, existingRoomGame.betAmount]);
          return;
        } else if (existingRoomGame.eliminated?.has(usernameClean)) {
          this._safeSend(ws, ["gameLowCardError", `You are eliminated`]);
          return;
        } else {
          this._safeSend(ws, ["gameLowCardError", `Game already running`]);
          return;
        }
      }
      
      const now = Date.now();
      const lockTime = this._gameLocks.get(room);
      if (lockTime && (now - lockTime) < CONSTANTS.START_LOCK_DURATION_MS) {
        this._safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]);
        return;
      }
      
      this._gameLocks.set(room, now);
      
      try {
        if (this.activeGames.size >= this._maxGames) {
          this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
          this._gameLocks.delete(room);
          return;
        }
        
        if (existingRoomGame) {
          await this.forceEndGame(room);
          await new Promise(r => setTimeout(r, 300));
        }
        
        const betAmount = parseInt(bet, 10) || 0;
        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          this._safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
          this._gameLocks.delete(room);
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
          playerWsId: new Map()
        };
        
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        game.playerWsId.set(usernameClean, wsId);
        
        this.activeGames.set(room, game);
        
        // Ini koneksi BARU (start game), tapi kita pakai addClient biasa
        // Karena ini WebSocket yang sama, bukan reconnect
        this._addClient(room, ws, usernameClean, false);
        
        this._broadcastToRoom(room, ["gameLowCardStart", game.betAmount, usernameClean]);
        this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
        
        this._startRegistration(room, game);
        
        setTimeout(() => {
          try {
            if (this._gameLocks.get(room) === now) {
              this._gameLocks.delete(room);
            }
          } catch(e) {
            // Silent error
          }
        }, CONSTANTS.START_LOCK_DURATION_MS);
        
      } catch(e) {
        this._deleteGame(room, this.activeGames.get(room));
        this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
        this._gameLocks.delete(room);
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }
  
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
      
      const room = this._getRoomForWs(ws);
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
        const existingGames = this._findAllGamesByUsername(usernameClean);
        if (existingGames.length > 0) {
          const roomList = existingGames.map(g => g.room).join(', ');
          this._safeSend(ws, ["gameLowCardInfo", `You are currently playing`]);
        }
        
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
          
          // Ini REJOIN - koneksi BARU, jadi cleanup koneksi lama
          const finalWsId = this._ensureSingleConnection(room, usernameClean, ws, wsId);
          
          this._safeSend(ws, ["gameLowCardRejoinSuccess", usernameClean]);
          this._safeSend(ws, ["gameLowCardStatus", {
            room: room,
            running: true,
            phase: game._phase || 'idle',
            round: game.round || 0,
            betAmount: game.betAmount || 0,
            registrationOpen: game.registrationOpen || false,
            players: Array.from(game.players?.values() || []).map(p => p.name)
          }]);
          
          if (game.numbers.has(usernameClean)) {
            const number = game.numbers.get(usernameClean);
            const tanda = game.tanda.get(usernameClean) || "";
            this._safeSend(ws, ["gameLowCardPlayerDraw", usernameClean, number, tanda]);
          }
          
          this._safeSend(ws, ["gameLowCardRejoinComplete", usernameClean]);
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
        // Ini JOIN - koneksi yang sama, bukan reconnect
        this._addClient(room, ws, usernameClean, false);
        game.playerWsId.set(usernameClean, wsId);
        
        this._broadcastToRoom(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
        this._safeSend(ws, ["gameLowCardJoinSuccess", usernameClean, game.betAmount]);
        
      } finally {
        this._joinLocks.delete(lockKey);
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to join game"]);
    }
  }
  
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
      
      const room = this._getRoomForWs(ws);
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
          // Ini RECONNECT saat submit - cleanup koneksi lama
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
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameRunning(game)) {
        game.evaluationLocked = true;
        this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
        game._evalTimer = setTimeout(() => {
          try {
            this._evaluateRound(room, game);
          } catch(e) {
            // Silent error
          }
        }, CONSTANTS.EVALUATION_DELAY_MS);
      }
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to submit number"]);
    }
  }
  
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
      const room = this._getRoomForWs(ws);
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
      this._safeSend(ws, ["gameLowCardLeaveSuccess", usernameClean]);
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to leave game"]);
    }
  }
  
  async forceEndGame(room) {
    try {
      const game = this.activeGames.get(room);
      if (game) {
        const players = Array.from(game.players?.values() || []).map(p => p.name);
        if (players.length > 0) {
          this._broadcastToRoom(room, ["gameLowCardEnd", players]);
        }
        this._deleteGame(room, game);
      }
    } catch(e) {
      // Silent error
    }
  }
  
  getGame(room) {
    return this.activeGames.get(room);
  }
  
  isGameRunning(room) {
    try {
      if (this.isDestroyed || !room) {
        return {
          running: false,
          message: this.isDestroyed ? "System destroyed" : "Invalid room"
        };
      }
      
      const game = this.activeGames.get(room);
      
      if (!game || !game.players) {
        return {
          running: false,
          message: "No game in this room"
        };
      }
      
      const isRunning = game._isActive === true && !game._gameEnded;
      
      return {
        running: isRunning,
        message: isRunning ? "Game is running" : "Game is not active"
      };
    } catch(e) {
      return { running: false, message: "Error checking game" };
    }
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
      
      const room = this._getRoomForWs(ws);
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]);
        return;
      }
      
      if (ws.room !== room) ws.room = room;
      
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
  
  // ==================== FETCH ====================
  
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    try {
      const url = new URL(req.url);
      
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "alive",
          games: this.activeGames.size,
          wsClients: this.wsClients.size,
          clients: this.clientRooms.size,
          wsMap: this.wsMap.size,
          roomViewers: Array.from(this.roomViewers.entries()).map(([room, viewers]) => ({ room, viewers: Array.from(viewers) })),
          userConnections: Array.from(this.userConnections.entries()).map(([user, conn]) => ({ 
            user, 
            wsId: conn.wsId,
            room: conn.room,
            timestamp: conn.timestamp
          })),
          connectionLocks: Array.from(this.connectionLocks.keys()),
          cleanupTimers: this._cleanupTimers.size,
          wsIdCounter: this._wsIdCounter,
          joinLocks: this._joinLocks.size
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      
      if (url.pathname === "/game/ws") {
        const upgrade = req.headers.get("Upgrade");
        if (upgrade !== "websocket") {
          return new Response("WebSocket only", { status: 400 });
        }
        
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        
        try { 
          this.state.acceptWebSocket(server); 
        } catch(e) { 
          return new Response("WebSocket acceptance failed", { status: 500 }); 
        }
        
        const wsId = ++this._wsIdCounter;
        server._wsId = wsId;
        server._closing = false;
        server.room = null;
        server._createdAt = Date.now();
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
            if (server.room) {
              const room = server.room;
              const wsId = this._getWsId(server);
              const username = server.username;
              
              this._removeClient(server.room, server);
              
              if (username) {
                const conn = this.userConnections.get(username);
                if (conn && conn.wsId === wsId) {
                  this.userConnections.delete(username);
                }
              }
            }
          } catch(e) {
            // Silent error
          }
        });
        
        server.addEventListener("error", () => {
          try {
            if (server.room) {
              const room = server.room;
              const wsId = this._getWsId(server);
              const username = server.username;
              
              this._removeClient(server.room, server);
              
              if (username) {
                const conn = this.userConnections.get(username);
                if (conn && conn.wsId === wsId) {
                  this.userConnections.delete(username);
                }
              }
            }
          } catch(e) {
            // Silent error
          }
        });
        
        return new Response(null, { status: 101, webSocket: client });
      }
      
      return new Response("Game Server", { status: 200 });
      
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
  
  // ==================== WEB SOCKET EVENTS ====================
  
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
      
      if (ws.room) {
        this._removeClient(ws.room, ws);
      }
      
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
      ws._wsId = null;
      ws.username = null;
    } catch(e) {
      // Silent error
    }
  }
  
  async webSocketError(ws) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      const username = ws.username;
      
      if (ws.room) {
        this._removeClient(ws.room, ws);
      }
      
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
      ws._wsId = null;
      ws.username = null;
    } catch(e) {
      // Silent error
    }
  }
  
  // ==================== DESTROY ====================
  
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.closing = true;
      this.isDestroyed = true;
      
      if (this._cleanupInterval) {
        clearInterval(this._cleanupInterval);
        this._cleanupInterval = null;
      }
      
      for (const [room, game] of this.activeGames) {
        this._cleanupGame(game);
      }
      
      for (const [room, timer] of this._cleanupTimers) {
        clearTimeout(timer);
      }
      this._cleanupTimers.clear();
      
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
      
      this.wsClients.clear();
      this.clientRooms.clear();
      this.wsMap.clear();
      this.roomViewers.clear();
      this.userConnections.clear();
      this.connectionLocks.clear();
      this._gameLocks.clear();
      this._joinLocks.clear();
      
      for (const [room, game] of this.activeGames) {
        this._deleteGame(room, game);
      }
      this.activeGames.clear();
    } catch(e) {
      // Silent error
    }
  }
}
