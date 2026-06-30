// ==================== GAME SERVER - DURABLE OBJECT (FULLY OPTIMIZED & CRASH-PROOF) ====================

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
  KEEP_ALIVE_INTERVAL_MS: 900000,
  CLEANUP_INTERVAL_MS: 300000,
  MAX_REQUESTS_PER_SECOND: 10,
  MESSAGE_CACHE_MAX_SIZE: 200,
  MESSAGE_CACHE_TTL_MS: 30000,
  WS_BUFFER_LIMIT: 1024 * 1024,
};

const GAME_PHASE = {
  IDLE: 'idle',
  REGISTRATION: 'registration',
  DRAW: 'draw',
  EVALUATING: 'evaluating',
  ENDED: 'ended'
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
    
    this._activeSessions = new Map();
    this._processedRequests = new Map();
    this._userRateLimit = new Map();
    this._userRoomIndex = new Map();
    this._messageCache = new Map();
    this._messageCacheMaxSize = CONSTANTS.MESSAGE_CACHE_MAX_SIZE;
    
    this._cleanupTimers = new Map();
    
    this._cleanupInterval = setInterval(() => {
      this._cleanupStaleGames();
      this._cleanupExpiredSessions();
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
    
    this._mainInterval = null;
    this._lastActivityTime = Date.now();
    this._startMainInterval();
  }
  
  _startMainInterval() {
    if (this._mainInterval) {
      clearInterval(this._mainInterval);
    }
    
    this._mainInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._doMainTask();
      }
    }, CONSTANTS.KEEP_ALIVE_INTERVAL_MS);
  }
  
  _doMainTask() {
    try {
      this._lastActivityTime = Date.now();
      
      for (const [room, game] of this.activeGames) {
        if (game && game._isActive && !game._gameEnded) {
          this._broadcastToRoom(room, ["_keepAlive", Date.now()]);
          this._checkGameHealth(room, game);
        }
      }
      
      this._cleanupStaleGames();
      this._cleanupExpiredSessions();
      this._clearMessageCache();
    } catch(e) {}
  }
  
  _getCachedMessage(message) {
    try {
      const key = JSON.stringify(message);
      if (!this._messageCache.has(key)) {
        if (this._messageCache.size > this._messageCacheMaxSize) {
          this._messageCache.clear();
        }
        this._messageCache.set(key, key);
        setTimeout(() => {
          this._messageCache.delete(key);
        }, CONSTANTS.MESSAGE_CACHE_TTL_MS);
      }
      return this._messageCache.get(key);
    } catch(e) {
      return JSON.stringify(message);
    }
  }
  
  _clearMessageCache() {
    try {
      if (this._messageCache.size > this._messageCacheMaxSize) {
        this._messageCache.clear();
      }
    } catch(e) {}
  }
  
  _cleanupExpiredSessions() {
    try {
      const toDelete = [];
      for (const [username, wsId] of this._activeSessions) {
        const ws = this.wsMap.get(wsId);
        if (!ws || ws.readyState !== 1) {
          toDelete.push(username);
        }
      }
      for (const username of toDelete) {
        this._activeSessions.delete(username);
        this._userRoomIndex.delete(username);
        this._userRateLimit.delete(username);
        this._processedRequests.delete(username);
      }
    } catch(e) {}
  }
  
  _updateUserRoomIndex(username, room) {
    if (username && room) {
      this._userRoomIndex.set(username, room);
    } else if (username) {
      this._userRoomIndex.delete(username);
    }
  }
  
  _getWsId(ws) {
    return ws ? ws._wsId : null;
  }
  
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
  
  _checkRateLimit(username) {
    try {
      const now = Date.now();
      const data = this._userRateLimit.get(username);
      
      if (!data) {
        this._userRateLimit.set(username, { count: 1, timestamp: now });
        return true;
      }
      
      if (now - data.timestamp > 1000) {
        this._userRateLimit.set(username, { count: 1, timestamp: now });
        return true;
      }
      
      if (data.count >= CONSTANTS.MAX_REQUESTS_PER_SECOND) {
        return false;
      }
      
      data.count++;
      return true;
    } catch(e) {
      return true;
    }
  }
  
  _isDuplicateRequest(username, requestId) {
    if (!requestId) return false;
    try {
      const key = `${username}_${requestId}`;
      if (this._processedRequests.has(key)) {
        return true;
      }
      this._processedRequests.set(key, Date.now());
      setTimeout(() => {
        this._processedRequests.delete(key);
      }, 5000);
      return false;
    } catch(e) {
      return false;
    }
  }
  
  _ensureSingleSession(username, newWs, newWsId) {
    try {
      const existingWsId = this._activeSessions.get(username);
      if (existingWsId && existingWsId !== newWsId) {
        const existingWs = this.wsMap.get(existingWsId);
        if (existingWs && existingWs.readyState === 1) {
          this._safeSend(existingWs, ["gameLowCardReplaced", "New connection established"]);
          existingWs._duplicate = true;
          try {
            existingWs.close(1000, "Replaced by new connection");
          } catch(e) {}
        }
        this._removeClient(existingWs?.room || null, existingWs);
        this._activeSessions.delete(username);
      }
      
      this._activeSessions.set(username, newWsId);
      newWs._duplicate = false;
    } catch(e) {}
  }
  
  _forceCleanupUserConnections(username, excludeWsId = null) {
    try {
      const conn = this.userConnections.get(username);
      if (!conn) return;
      
      if (excludeWsId !== null && conn.wsId === excludeWsId) {
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
      
      if (this._activeSessions.get(username) === conn.wsId) {
        this._activeSessions.delete(username);
        this._userRoomIndex.delete(username);
        this._userRateLimit.delete(username);
        this._processedRequests.delete(username);
      }
      
      if (conn.room && this.roomViewers.has(conn.room)) {
        this.roomViewers.get(conn.room).delete(username);
        if (this.roomViewers.get(conn.room).size === 0) {
          this.roomViewers.delete(conn.room);
        }
      }
      
      this.userConnections.delete(username);
    } catch(e) {}
  }
  
  _addClient(room, ws, username = null, isNewConnection = false) {
    try {
      const wsId = this._getWsId(ws);
      if (!wsId) {
        this._safeSend(ws, ["gameLowCardError", "Connection error, please reconnect"]);
        return;
      }
      
      if (username) {
        this._ensureSingleSession(username, ws, wsId);
        this._updateUserRoomIndex(username, room);
      }
      
      if (username && isNewConnection) {
        if (!this._lockUserConnection(username)) {
          setTimeout(() => {
            this._addClient(room, ws, username, isNewConnection);
          }, 100);
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
    } catch(e) {}
  }
  
  _removeClientFromRoom(room, wsId) {
    try {
      const clients = this.wsClients.get(room);
      if (clients) {
        clients.delete(wsId);
        if (clients.size === 0) {
          this.wsClients.delete(room);
        }
      }
    } catch(e) {}
  }
  
  _removeClient(room, ws) {
    try {
      const wsId = this._getWsId(ws);
      if (!wsId) return;
      
      const username = ws.username;
      
      this._removeClientFromRoom(room, wsId);
      this.clientRooms.delete(wsId);
      this.wsMap.delete(wsId);
      
      if (username) {
        if (this._activeSessions.get(username) === wsId) {
          this._activeSessions.delete(username);
          this._userRoomIndex.delete(username);
          this._userRateLimit.delete(username);
          this._processedRequests.delete(username);
        }
        
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
        ws._duplicate = true;
        ws._cleanedUp = true;
      }
      
      this._clearMessageCache();
    } catch(e) {}
  }
  
  _getRoomForWs(ws) {
    try {
      const wsId = this._getWsId(ws);
      if (!wsId) return null;
      return this.clientRooms.get(wsId) || null;
    } catch(e) {
      return null;
    }
  }
  
  _ensureSingleConnection(room, username, newWs, newWsId) {
    try {
      const game = this.activeGames.get(room);
      if (!game) return newWsId;
      
      this._ensureSingleSession(username, newWs, newWsId);
      
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
          this._ensureSingleConnection(room, username, newWs, newWsId);
        }, 100);
      }
      
      return newWsId;
    } catch(e) {
      return newWsId;
    }
  }
  
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
        this._updateUserRoomIndex(username, roomName);
      }
      
      this._sendGameStatusToWs(ws, roomName);
      this._broadcastToRoom(roomName, ["roomUserJoined", username || "Anonymous"]);
      this._safeSend(ws, ["switchRoomSuccess", roomName]);
      
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to switch room"]);
    }
  }
  
  _sendGameStatusToWs(ws, room) {
    try {
      const roomGame = this.activeGames.get(room);
      if (roomGame && roomGame._isActive && !roomGame._gameEnded && roomGame.players && roomGame.players.size > 0) {
        this._safeSend(ws, ["gameStatus", {
          running: true,
          room: room,
          phase: roomGame._phase || 'idle',
          round: roomGame.round || 0,
          betAmount: roomGame.betAmount || 0,
          registrationOpen: roomGame.registrationOpen || false,
          players: Array.from(roomGame.players?.values() || []).map(p => p.name),
          eliminated: Array.from(roomGame.eliminated || []),
          totalPlayers: roomGame.players?.size || 0,
          activePlayers: this._getActivePlayers(roomGame).length
        }]);
      } else {
        this._safeSend(ws, ["gameStatus", {
          running: false,
          room: room,
          phase: 'idle',
          round: 0,
          betAmount: 0,
          registrationOpen: false,
          players: [],
          eliminated: [],
          totalPlayers: 0,
          activePlayers: 0
        }]);
      }
    } catch(e) {}
  }
  
  _broadcastToRoom(room, message) {
    try {
      if (this.closing || this.isDestroyed || !room || !message) return;
      
      const wsIds = this.wsClients.get(room);
      if (!wsIds || wsIds.size === 0) return;
      
      const msgStr = this._getCachedMessage(message);
      const disconnected = new Set();
      
      for (const wsId of wsIds) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1 && !ws._duplicate) {
          try {
            if (ws.bufferedAmount && ws.bufferedAmount > CONSTANTS.WS_BUFFER_LIMIT) {
              continue;
            }
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
    } catch(e) {}
  }
  
  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1 || ws._duplicate) return false;
      if (ws.bufferedAmount && ws.bufferedAmount > CONSTANTS.WS_BUFFER_LIMIT) return false;
      
      const msgStr = this._getCachedMessage(message);
      ws.send(msgStr);
      return true;
    } catch(e) {
      return false;
    }
  }
  
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
        game._phase = GAME_PHASE.ENDED;
        
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (activePlayers.length === 0) {
        game._gameEnded = true;
        game._isActive = false;
        game._phase = GAME_PHASE.ENDED;
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }
  
  _findAllGamesByUsername(username) {
    try {
      if (!username) return [];
      
      const room = this._userRoomIndex.get(username);
      if (room) {
        const game = this.activeGames.get(room);
        if (game && game._isActive && !game._gameEnded && game.players && game.players.has(username)) {
          return [{ game, room }];
        }
      }
      
      const result = [];
      for (const [r, g] of this.activeGames) {
        if (g._isActive && !g._gameEnded && g.players && g.players.has(username)) {
          result.push({ game: g, room: r });
        }
      }
      return result;
    } catch(e) {
      return [];
    }
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
      return Math.random() < 0.8 ? 
        [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
        [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
    }
  }
  
  _getActivePlayers(game) {
    try {
      if (!game?._isActive || game._gameEnded || !game.players) return [];
      return Array.from(game.players.entries())
        .filter(([id]) => !game.eliminated?.has(id))
        .map(([, p]) => p);
    } catch(e) {
      return [];
    }
  }
  
  _getActivePlayerIds(game) {
    try {
      if (!game?._isActive || game._gameEnded || !game.players) return [];
      return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
    } catch(e) {
      return [];
    }
  }
  
  _isGameRunning(game) {
    return game && game._isActive === true && !game._gameEnded && !this.isDestroyed && game.players;
  }
  
  _safeGetGame(room) {
    try {
      if (this.isDestroyed || !room) return null;
      const game = this.activeGames.get(room);
      return (game?._isActive && !game._gameEnded && game.players) ? game : null;
    } catch(e) {
      return null;
    }
  }
  
  _checkAndCleanupGame(room) {
    try {
      const game = this.activeGames.get(room);
      if (!game) return;
      
      if (game._gameEnded || !game._isActive || !game.players || game.players.size === 0) {
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }
  
  _scheduleGameCleanup(room, game) {
    try {
      if (this._cleanupTimers.has(room)) {
        clearTimeout(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      
      const timer = setTimeout(() => {
        try {
          this._cleanupTimers.delete(room);
          this._deleteGame(room, game);
        } catch(e) {}
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS);
      
      this._cleanupTimers.set(room, timer);
    } catch(e) {}
  }
  
  _cleanupGame(game) {
    try {
      if (!game) return;
      
      this._clearAllTimers(game);
      this._cleanupBotTimeouts(game);
      
      if (game.players) {
        for (const [username] of game.players) {
          if (this._userRoomIndex.get(username) === game.room) {
            this._userRoomIndex.delete(username);
            this._userRateLimit.delete(username);
            this._processedRequests.delete(username);
          }
        }
      }
      
      game.players = null;
      game.botPlayers = null;
      game.numbers = null;
      game.tanda = null;
      game.eliminated = null;
      game._isActive = false;
      game._gameEnded = true;
      game._phase = GAME_PHASE.ENDED;
      game.playerWsId = null;
    } catch(e) {}
  }
  
  _deleteGame(room, game) {
    try {
      if (this._cleanupTimers.has(room)) {
        clearTimeout(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      
      if (game) {
        if (game.players) {
          for (const [username] of game.players) {
            if (this._userRoomIndex.get(username) === room) {
              this._userRoomIndex.delete(username);
              this._userRateLimit.delete(username);
              this._processedRequests.delete(username);
            }
          }
        }
        this._cleanupGame(game);
      }
      this.activeGames.delete(room);
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this.roomViewers.delete(room);
      
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
    } catch(e) {}
  }
  
  _clearAllTimers(game) {
    try {
      if (!game) return;
      const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
      for (const key of timers) {
        if (game[key]) {
          clearInterval(game[key]);
          clearTimeout(game[key]);
          game[key] = null;
        }
      }
    } catch(e) {}
  }
  
  _cleanupBotTimeouts(game) {
    try {
      if (game._botTimeouts) {
        for (const timeout of game._botTimeouts) {
          clearTimeout(timeout);
        }
        game._botTimeouts.clear();
        game._botTimeouts = null;
      }
    } catch(e) {}
  }
  
  _startRegistration(room, game) {
    try {
      if (!this._isGameRunning(game) || !game.registrationOpen) return;
      
      this._clearAllTimers(game);
      game._phase = GAME_PHASE.REGISTRATION;
      
      let timeLeft = 20;
      
      const tick = () => {
        try {
          if (!this._isGameRunning(game) || !game.registrationOpen || timeLeft < 0 || game._gameEnded) {
            game._registrationTimer = null;
            return;
          }
          
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          
          if (timeLeft === 0) {
            game._registrationTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
            this._closeRegistration(room, game);
            return;
          }
          
          timeLeft--;
          if (!game._gameEnded) {
            game._registrationTimer = setTimeout(tick, 1000);
          }
        } catch(e) {
          game._registrationTimer = null;
        }
      };
      
      game._registrationTimer = setTimeout(tick, 1000);
    } catch(e) {}
  }
  
  _closeRegistration(room, game) {
    try {
      if (!this._isGameRunning(game) || !game.registrationOpen) return;
      game.registrationOpen = false;
      
      this._clearAllTimers(game);
      
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
        game._phase = GAME_PHASE.ENDED;
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
        const botId = `BOT_${room}_${i}_${Date.now()}`;
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
  
  _startDrawPhase(room, game) {
    try {
      if (!this._isGameRunning(game)) return;
      
      this._clearAllTimers(game);
      this._cleanupBotTimeouts(game);
      
      game._phase = GAME_PHASE.DRAW;
      game.drawTimeExpired = false;
      game.evaluationLocked = false;
      game._evalLocked = false;
      game._closingDrawLock = false;
      game._drawStartTime = Date.now();
      
      if (!game._botTimeouts) game._botTimeouts = new Set();
      if (!game.numbers) game.numbers = new Map();
      if (!game.tanda) game.tanda = new Map();
      
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
            game._phase = GAME_PHASE.ENDED;
            this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
            this._scheduleGameCleanup(room, game);
          } else {
            game._gameEnded = true;
            game._isActive = false;
            game._phase = GAME_PHASE.ENDED;
            this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
            this._scheduleGameCleanup(room, game);
          }
          return;
        }
      }
      
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
    try {
      if (!this._isGameRunning(game)) return;
      
      this._clearAllTimers(game);
      
      let timeLeft = 20;
      
      const tick = () => {
        try {
          if (!this._isGameRunning(game) || game.drawTimeExpired || timeLeft < 0 || game._gameEnded) {
            game._drawTimer = null;
            return;
          }
          
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          
          if (timeLeft === 0) {
            game._drawTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
            this._closeDrawPhase(room, game);
            return;
          }
          
          timeLeft--;
          if (!game._gameEnded) {
            game._drawTimer = setTimeout(tick, 1000);
          }
        } catch(e) {
          game._drawTimer = null;
        }
      };
      
      game._drawTimer = setTimeout(tick, 1000);
    } catch(e) {}
  }
  
  _closeDrawPhase(room, game) {
    try {
      if (!this._isGameRunning(game) || game.drawTimeExpired || game.evaluationLocked) return;
      
      if (game._closingDrawLock) return;
      game._closingDrawLock = true;
      
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      
      this._clearAllTimers(game);
      
      const activeIds = this._getActivePlayerIds(game);
      for (const id of activeIds) {
        if (!game.numbers.has(id)) {
          game.eliminated.add(id);
          const playerName = game.players.get(id)?.name || id;
          this._broadcastToRoom(room, ["gameLowCardPlayerEliminated", playerName, "Did not submit"]);
        }
      }
      
      if (game.botPlayers?.size > 0 && this._isGameRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) {
          this._forceBotDraw(room, botId, game);
        }
      }
      
      const remainingActive = this._getActivePlayers(game);
      if (remainingActive.length < 2) {
        if (remainingActive.length === 1 && !game._gameEnded) {
          const winner = remainingActive[0]?.name || "Unknown";
          const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
          game._gameEnded = true;
          game._phase = GAME_PHASE.ENDED;
          this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
          this._scheduleGameCleanup(room, game);
        } else {
          game._gameEnded = true;
          game._isActive = false;
          game._phase = GAME_PHASE.ENDED;
          this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
          this._scheduleGameCleanup(room, game);
        }
        game._closingDrawLock = false;
        return;
      }
      
      this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
      
      game._evalTimer = setTimeout(() => {
        try {
          this._evaluateRound(room, game);
        } catch(e) {
          if (game) {
            game._isEvaluating = false;
            game._evalLocked = false;
          }
        }
      }, CONSTANTS.EVALUATION_DELAY_MS);
      
      game._closingDrawLock = false;
    } catch(e) {
      if (game) game._closingDrawLock = false;
    }
  }
  
  _startBotDraws(room, game) {
    try {
      if (!this._isGameRunning(game) || !game.botPlayers) return;
      
      if (!game._botTimeouts) game._botTimeouts = new Set();
      
      const notDrawn = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
      
      for (const botId of notDrawn) {
        const timeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (this._isGameRunning(currentGame) && 
                !currentGame._gameEnded &&
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
      
      this._checkAllPlayersDrawn(room, game);
    } catch(e) {}
  }
  
  _forceBotDraw(room, botId, game) {
    try {
      if (!this._isGameRunning(game) || game.numbers?.has(botId)) return;
      if (game.eliminated?.has(botId)) return;
      if (game._gameEnded) return;
      
      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      
      const botName = game.players.get(botId)?.name || botId;
      
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
    } catch(e) {}
  }
  
  _checkAllPlayersDrawn(room, game) {
    try {
      if (game._closingDrawLock) return;
      
      const activeIds = this._getActivePlayerIds(game);
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameRunning(game)) {
        game._closingDrawLock = true;
        try {
          game.evaluationLocked = true;
          this._clearAllTimers(game);
          this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
          game._evalTimer = setTimeout(() => {
            try {
              this._evaluateRound(room, game);
            } catch(e) {
              if (game) {
                game._isEvaluating = false;
                game._evalLocked = false;
              }
            }
          }, CONSTANTS.EVALUATION_DELAY_MS);
        } finally {
          game._closingDrawLock = false;
        }
      }
    } catch(e) {}
  }
  
  _checkGameHealth(room, game) {
    try {
      if (!this._isGameRunning(game)) return;
      
      if (game._phase === GAME_PHASE.DRAW && !game.drawTimeExpired && !game.evaluationLocked) {
        const now = Date.now();
        const timeSinceDrawStart = now - (game._drawStartTime || 0);
        
        if (timeSinceDrawStart > 30000) {
          this._closeDrawPhase(room, game);
        }
      }
      
      if (game._phase === GAME_PHASE.EVALUATING && game._isEvaluating) {
        const now = Date.now();
        const timeSinceEvalStart = now - (game._evalStartTime || 0);
        
        if (timeSinceEvalStart > 30000) {
          game._isEvaluating = false;
          game._evalLocked = false;
          this._broadcastToRoom(room, ["gameLowCardError", "Evaluation timeout"]);
          this._scheduleGameCleanup(room, game);
        }
      }
    } catch(e) {}
  }
  
  _evaluateRound(room, game) {
    try {
      if (game._evalLocked) return;
      game._evalLocked = true;
      
      if (this.isDestroyed || !game || game._gameEnded || !game._isActive || game._isEvaluating) {
        game._evalLocked = false;
        return;
      }
      
      if (!game.players || !game.numbers || !game.tanda || !game.eliminated) {
        this._scheduleGameCleanup(room, game);
        game._evalLocked = false;
        return;
      }
      
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game) {
        game._evalLocked = false;
        return;
      }
      
      game._isEvaluating = true;
      game._phase = GAME_PHASE.EVALUATING;
      game._evalStartTime = Date.now();
      
      this._clearAllTimers(game);
      this._cleanupBotTimeouts(game);
      
      game._safetyTimer = setTimeout(() => {
        try {
          if (game && game._isEvaluating) {
            game._isEvaluating = false;
            game._evalLocked = false;
            this._broadcastToRoom(room, ["gameLowCardError", "Evaluation timeout"]);
            this._scheduleGameCleanup(room, game);
          }
        } catch(e) {}
      }, CONSTANTS.EVALUATION_TIMEOUT_MS);
      
      const numbers = game.numbers;
      const players = game.players;
      const eliminated = game.eliminated;
      const tanda = game.tanda;
      
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
        game._evalLocked = false;
        if (game._safetyTimer) {
          clearTimeout(game._safetyTimer);
          game._safetyTimer = null;
        }
        
        this._broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (entries.length === 1) {
        const winnerId = entries[0][0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        
        game._gameEnded = true;
        game._isActive = false;
        game._phase = GAME_PHASE.ENDED;
        game._isEvaluating = false;
        game._evalLocked = false;
        
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
        for (const id of losers) {
          eliminated.add(id);
        }
      }
      
      const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
      
      if (allSame && remaining.length >= 2) {
        game._isEvaluating = false;
        game._evalLocked = false;
        
        if (game._safetyTimer) {
          clearTimeout(game._safetyTimer);
          game._safetyTimer = null;
        }
        
        numbers.clear();
        tanda.clear();
        game.round++;
        game.evaluationLocked = false;
        game.drawTimeExpired = false;
        game._phase = GAME_PHASE.DRAW;
        game._botTimeouts = new Set();
        game._evalStartTime = null;
        
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
        game._isActive = false;
        game._phase = GAME_PHASE.ENDED;
        game._isEvaluating = false;
        game._evalLocked = false;
        
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
        game._evalLocked = false;
        
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
      game._phase = GAME_PHASE.DRAW;
      game._botTimeouts = new Set();
      game._isEvaluating = false;
      game._evalLocked = false;
      game._evalStartTime = null;
      
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
        game._evalLocked = false;
        if (game._safetyTimer) {
          clearTimeout(game._safetyTimer);
          game._safetyTimer = null;
        }
      }
      this._broadcastToRoom(room, ["gameLowCardError", "Evaluation error"]);
      this._scheduleGameCleanup(room, game);
    }
  }
  
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
        this._safeSend(ws, ["gameStatus", { 
          running: false,
          room: "",
          phase: "idle",
          round: 0,
          players: [],
          betAmount: 0,
          registrationOpen: false,
          eliminated: [],
          totalPlayers: 0,
          activePlayers: 0
        }]);
        return;
      }
      
      const game = this.activeGames.get(room);
      
      if (!game || !game._isActive || game._gameEnded || !game.players || game.players.size === 0) {
        this._safeSend(ws, ["gameStatus", { 
          running: false,
          room: room,
          phase: "idle",
          round: 0,
          players: [],
          betAmount: 0,
          registrationOpen: false,
          eliminated: [],
          totalPlayers: 0,
          activePlayers: 0
        }]);
        return;
      }
      
      this._safeSend(ws, ["gameStatus", { 
        running: true,
        room: room,
        phase: game._phase || 'idle',
        round: game.round || 0,
        betAmount: game.betAmount || 0,
        registrationOpen: game.registrationOpen || false,
        players: Array.from(game.players?.values() || []).map(p => p.name),
        eliminated: Array.from(game.eliminated || []),
        totalPlayers: game.players?.size || 0,
        activePlayers: this._getActivePlayers(game).length
      }]);
      
    } catch(e) {
      this._safeSend(ws, ["gameStatus", { 
        running: false,
        room: roomname || "",
        phase: "idle",
        round: 0,
        players: [],
        betAmount: 0,
        registrationOpen: false,
        eliminated: [],
        totalPlayers: 0,
        activePlayers: 0
      }]);
    }
  }
  
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
          _phase: GAME_PHASE.REGISTRATION,
          _botTimeouts: new Set(),
          _botsAdded: false,
          _registrationTimer: null,
          _drawTimer: null,
          _evalTimer: null,
          _safetyTimer: null,
          _isEvaluating: false,
          _evalLocked: false,
          _closingDrawLock: false,
          _createdAt: Date.now(),
          _drawStartTime: null,
          _evalStartTime: null,
          playerWsId: new Map()
        };
        
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        game.playerWsId.set(usernameClean, wsId);
        
        this.activeGames.set(room, game);
        
        this._addClient(room, ws, usernameClean, false);
        this._updateUserRoomIndex(usernameClean, room);
        
        this._broadcastToRoom(room, ["gameLowCardStart", game.betAmount, usernameClean]);
        this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
        
        this._startRegistration(room, game);
        
        setTimeout(() => {
          try {
            if (this._gameLocks.get(room) === now) {
              this._gameLocks.delete(room);
            }
          } catch(e) {}
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
          this._sendGameStatusToWs(ws, room);
          return;
        }
        
        if (game._phase === GAME_PHASE.EVALUATING || game._phase === GAME_PHASE.ENDED) {
          this._safeSend(ws, ["gameLowCardError", "Cannot join now"]);
          this._sendGameStatusToWs(ws, room);
          return;
        }
        
        if (game.players.has(usernameClean)) {
          if (game.eliminated?.has(usernameClean)) {
            this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
            this._sendGameStatusToWs(ws, room);
            return;
          }
          
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
          this._sendGameStatusToWs(ws, room);
          return;
        }
        
        if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
          this._safeSend(ws, ["gameLowCardError", "Game is full"]);
          this._sendGameStatusToWs(ws, room);
          return;
        }
        
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        this._addClient(room, ws, usernameClean, false);
        game.playerWsId.set(usernameClean, wsId);
        this._updateUserRoomIndex(usernameClean, room);
        
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
          this._ensureSingleConnection(room, usernameClean, ws, wsId);
        }
      }
      
      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== GAME_PHASE.DRAW) {
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
      
      this._checkAllPlayersDrawn(room, game);
      
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
    } catch(e) {}
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
        message: isRunning ? "Game is running" : "Game is not active",
        room: room,
        phase: game._phase || 'idle',
        round: game.round || 0,
        players: Array.from(game.players?.values() || []).map(p => p.name),
        betAmount: game.betAmount || 0,
        registrationOpen: game.registrationOpen || false,
        eliminated: Array.from(game.eliminated || []),
        totalPlayers: game.players?.size || 0,
        activePlayers: this._getActivePlayers(game).length
      };
    } catch(e) {
      return { 
        running: false, 
        message: "Error checking game",
        room: room || "unknown"
      };
    }
  }
  
  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      
      if (ws._duplicate) {
        return;
      }
      
      const username = ws.username;
      if (username) {
        const activeWsId = this._activeSessions.get(username);
        if (activeWsId && activeWsId !== ws._wsId) {
          ws._duplicate = true;
          try {
            ws.close(1000, "Duplicate connection");
          } catch(e) {}
          return;
        }
      }
      
      const evt = data[0];
      
      if (username && (evt === "gameLowCardNumber" || evt === "gameLowCardStart" || evt === "gameLowCardJoin")) {
        if (!this._checkRateLimit(username)) {
          this._safeSend(ws, ["gameLowCardError", "Too many requests"]);
          return;
        }
      }
      
      if (evt === "gameLowCardNumber" && username) {
        const requestId = data[data.length - 1];
        if (this._isDuplicateRequest(username, requestId)) {
          return;
        }
      }
      
      if (evt === "switchRoom") {
        const [_, room, user] = data;
        await this.switchRoom(ws, room, user);
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
        server._duplicate = false;
        server._cleanedUp = false;
        
        server.addEventListener("message", async (event) => {
          try {
            if (server._duplicate || server._cleanedUp) return;
            
            let data;
            try {
              data = JSON.parse(event.data);
            } catch(e) {
              this._safeSend(server, ["gameLowCardError", "Invalid message format"]);
              return;
            }
            
            if (!Array.isArray(data) || data.length === 0) return;
            await this.handleEvent(server, data);
          } catch(e) {
            this._safeSend(server, ["gameLowCardError", e.message || "Error"]);
          }
        });
        
        server.addEventListener("close", () => {
          if (server._cleanedUp) return;
          server._cleanedUp = true;
          
          try {
            const username = server.username;
            const wsId = server._wsId;
            
            if (username && this._activeSessions.get(username) === wsId) {
              this._activeSessions.delete(username);
              this._userRoomIndex.delete(username);
              this._userRateLimit.delete(username);
              this._processedRequests.delete(username);
            }
            
            if (server.room) {
              this._removeClient(server.room, server);
            }
            
            if (username) {
              const conn = this.userConnections.get(username);
              if (conn && conn.wsId === wsId) {
                this.userConnections.delete(username);
              }
            }
            
            this.clientRooms.delete(wsId);
            this.wsMap.delete(wsId);
          } catch(e) {}
        });
        
        server.addEventListener("error", () => {
          if (server._cleanedUp) return;
          server._cleanedUp = true;
          
          try {
            const username = server.username;
            const wsId = server._wsId;
            
            if (username && this._activeSessions.get(username) === wsId) {
              this._activeSessions.delete(username);
              this._userRoomIndex.delete(username);
              this._userRateLimit.delete(username);
              this._processedRequests.delete(username);
            }
            
            if (server.room) {
              this._removeClient(server.room, server);
            }
            
            if (username) {
              const conn = this.userConnections.get(username);
              if (conn && conn.wsId === wsId) {
                this.userConnections.delete(username);
              }
            }
            
            this.clientRooms.delete(wsId);
            this.wsMap.delete(wsId);
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
      if (!ws._wsId || ws._duplicate || ws._cleanedUp) return;
      
      let data;
      try {
        data = JSON.parse(msg);
      } catch(e) {
        this._safeSend(ws, ["gameLowCardError", "Invalid message format"]);
        return;
      }
      
      if (!Array.isArray(data) || data.length === 0) return;
      await this.handleEvent(ws, data);
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", e.message || "Error"]);
    }
  }
  
  async webSocketClose(ws) {
    try {
      if (!ws) return;
      if (ws._cleanedUp) return;
      ws._cleanedUp = true;
      
      const wsId = this._getWsId(ws);
      const username = ws.username;
      
      if (username && this._activeSessions.get(username) === wsId) {
        this._activeSessions.delete(username);
        this._userRoomIndex.delete(username);
        this._userRateLimit.delete(username);
        this._processedRequests.delete(username);
      }
      
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
      ws._duplicate = true;
    } catch(e) {}
  }
  
  async webSocketError(ws) {
    try {
      if (!ws) return;
      if (ws._cleanedUp) return;
      ws._cleanedUp = true;
      
      const wsId = this._getWsId(ws);
      const username = ws.username;
      
      if (username && this._activeSessions.get(username) === wsId) {
        this._activeSessions.delete(username);
        this._userRoomIndex.delete(username);
        this._userRateLimit.delete(username);
        this._processedRequests.delete(username);
      }
      
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
      ws._duplicate = true;
    } catch(e) {}
  }
  
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.closing = true;
      this.isDestroyed = true;
      
      if (this._mainInterval) {
        clearInterval(this._mainInterval);
        this._mainInterval = null;
      }
      
      if (this._cleanupInterval) {
        clearInterval(this._cleanupInterval);
        this._cleanupInterval = null;
      }
      
      this._messageCache.clear();
      this._userRoomIndex.clear();
      this._processedRequests.clear();
      this._userRateLimit.clear();
      this._activeSessions.clear();
      
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
          if (ws && !ws._cleanedUp) {
            try {
              ws._cleanedUp = true;
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
    } catch(e) {}
  }
  
  _cleanupStaleGames() {
    try {
      const now = Date.now();
      for (const [room, game] of this.activeGames) {
        if (!game._isActive || game._gameEnded) {
          if (game._createdAt && (now - game._createdAt) > 600000) {
            this._scheduleGameCleanup(room, game);
          }
        }
      }
    } catch(e) {}
  }
}
