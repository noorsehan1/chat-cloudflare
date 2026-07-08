// ==================== GAME SERVER - FULL CODE (FIXED) ====================

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
  CLEANUP_TIK: 90,  // 90 TIK = 15 MENIT
  STALE_GAME_TIMEOUT_MS: 600000, // 10 menit
  STUCK_DRAW_TIMEOUT_MS: 60000, // 1 menit
  STUCK_REGISTRATION_TIMEOUT_MS: 30000, // 30 detik
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
    
    // RATE LIMIT BROADCAST
    this._roomBroadcastCount = new Map();
    this._roomBroadcastReset = new Map();
    
    // COUNTER UNTUK CLEANUP
    this._tikCounter = 0;
    this._lastActivityTime = Date.now();
    
    // SET ALARM PERTAMA
    this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_10_DETIK);
  }
  
  // ==================== ALARM & CLEANUP ====================
  
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    try {
      this._tikCounter++;
      
      // CEK GAME STUCK SETIAP MENIT (6 TIK = 60 DETIK)
      if (this._tikCounter % 6 === 0) {
        this._checkStuckGames();
      }
      
      // CLEANUP 15 MENIT SEKALI
      if (this._tikCounter >= CONSTANTS.CLEANUP_TIK) {
        this._cleanupStaleGames();
        this._cleanupDeadConnections();
        this._cleanupStaleBroadcastCounters();
        this._tikCounter = 0;
      }
      
    } catch(e) {
      // Silent error
    }
    
    this.state.storage.setAlarm(Date.now() + CONSTANTS.ALARM_10_DETIK);
  }
  
  _checkStuckGames() {
    try {
      const now = Date.now();
      for (const [room, game] of this.activeGames) {
        if (!game._isActive || game._gameEnded) continue;
        
        // CEK GAME STUCK DI DRAW PHASE
        if (game._phase === 'draw' && game._drawPhaseStart) {
          if ((now - game._drawPhaseStart) > CONSTANTS.STUCK_DRAW_TIMEOUT_MS) {
            console.log(`[STUCK] Game ${room} stuck in draw phase, forcing evaluation`);
            this._broadcastToRoom(room, ["gameLowCardError", "Game stuck, forcing evaluation..."]);
            this._closeDrawPhase(room, game);
          }
        }
        
        // CEK GAME STUCK DI REGISTRATION
        if (game._phase === 'registration' && game.registrationOpen) {
          if (game._createdAt && (now - game._createdAt) > CONSTANTS.STUCK_REGISTRATION_TIMEOUT_MS) {
            console.log(`[STUCK] Game ${room} stuck in registration, forcing close`);
            this._broadcastToRoom(room, ["gameLowCardError", "Registration timeout"]);
            this._closeRegistration(room, game);
          }
        }
        
        // CEK GAME TANPA PEMAIN AKTIF
        if (game._phase !== 'registration' && !game.registrationOpen) {
          const activePlayers = this._getActivePlayers(game);
          if (activePlayers.length === 0 && !game._gameEnded) {
            console.log(`[STUCK] Game ${room} has no active players, ending`);
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
        // SKIP JIKA GAME MASIH BERJALAN
        if (game._isActive === true && !game._gameEnded) {
          continue;
        }
        
        // HANYA HAPUS JIKA SUDAH ENDED DAN SUDAH LAMA
        if (game._gameEnded === true) {
          const endTime = game._endTime || game._createdAt || now;
          if ((now - endTime) > CONSTANTS.STALE_GAME_TIMEOUT_MS) {
            console.log(`[CLEANUP] Removing stale game: ${room}`);
            this._scheduleGameCleanup(room, game);
          }
          continue;
        }
        
        // GAME DALAM STATUS ANEH (TIDAK AKTIF TAPI TIDAK ENDED)
        if (game._isActive === false && !game._gameEnded) {
          if (game._createdAt && (now - game._createdAt) > 300000) { // 5 menit
            console.log(`[CLEANUP] Removing orphaned game: ${room}`);
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
        if ((now - resetTime) > 60000) { // 1 menit tidak ada aktivitas
          this._roomBroadcastCount.delete(room);
          this._roomBroadcastReset.delete(room);
        }
      }
    } catch(e) {}
  }
  
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
    } catch(e) {}
  }
  
  // ==================== WEB SOCKET HELPERS ====================
  
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
  
  _forceCleanupUserConnections(username, excludeWsId = null) {
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
    
    if (conn.room && this.roomViewers.has(conn.room)) {
      this.roomViewers.get(conn.room).delete(username);
      if (this.roomViewers.get(conn.room).size === 0) {
        this.roomViewers.delete(conn.room);
      }
    }
    
    this.userConnections.delete(username);
  }
  
  _addClient(room, ws, username = null, isNewConnection = false) {
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
    return ws ? ws.room : null;
  }
  
  _ensureSingleConnection(room, username, newWs, newWsId) {
    const game = this.activeGames.get(room);
    if (!game) return newWsId;
    
    this._forceCleanupUserConnections(username, newWsId);
    game.playerWsId.set(username, newWsId);
    this._addClient(room, newWs, username, true);
    
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
    
    ws.room = roomName;
    ws.roomname = roomName;
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
  
  // ==================== BROADCAST ====================
  
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
      if (count > 50) {
        return;
      }
      this._roomBroadcastCount.set(room, count + 1);
    }
    
    const msgStr = JSON.stringify(message);
    const disconnected = new Set();
    
    const wsIdArray = Array.from(wsIds);
    const BATCH_SIZE = 10;
    
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
  
  // ==================== GAME HELPERS ====================
  
  _isGameActuallyRunning(game) {
    if (!game) return false;
    return game._isActive === true && !game._gameEnded;
  }
  
  _isGameValid(game) {
    if (!game) return false;
    return game._isActive === true && 
           !game._gameEnded && 
           game.players && 
           game.players.size > 0;
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
  
  _getActivePlayersWithSubmitStatus(game) {
    if (!game?._isActive || game._gameEnded || !game.players) return [];
    const submittedIds = new Set(game.numbers?.keys() || []);
    return Array.from(game.players.entries())
      .filter(([id]) => !game.eliminated?.has(id))
      .map(([id, p]) => ({
        ...p,
        hasSubmitted: submittedIds.has(id)
      }));
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
      // Lebih random: 60% angka 8-12, 40% angka 1-7
      return Math.random() < 0.6 ? 
        [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
        [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
    }
  }
  
  _safeGetGame(room) {
    if (this.isDestroyed || !room) return null;
    const game = this.activeGames.get(room);
    // HAPUS pengecekan players.size > 0
    if (game && game._isActive === true && !game._gameEnded && game.players) {
      return game;
    }
    return null;
  }
  
  // ==================== GAME CLEANUP ====================
  
  _scheduleGameCleanup(room, game) {
    if (this._cleanupTimers.has(room)) {
      clearTimeout(this._cleanupTimers.get(room));
      this._cleanupTimers.delete(room);
    }
    
    // PASTIKAN game benar-benar sudah ended
    if (!game._gameEnded) {
      return;
    }
    
    const timer = setTimeout(() => {
      try {
        const currentGame = this.activeGames.get(room);
        if (currentGame && currentGame._isActive && !currentGame._gameEnded) {
          // Game ternyata masih berjalan, batalkan cleanup
          this._cleanupTimers.delete(room);
          return;
        }
        
        this._cleanupTimers.delete(room);
        this._deleteGame(room, game);
      } catch(e) {}
    }, CONSTANTS.GAME_CLEANUP_DELAY_MS);
    
    this._cleanupTimers.set(room, timer);
  }
  
  _cleanupGame(game) {
    if (!game) return;
    
    // JANGAN hapus jika game masih berjalan
    if (game._isActive === true && !game._gameEnded) {
      return;
    }
    
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
    // CEK apakah game benar-benar harus dihapus
    if (game && game._isActive === true && !game._gameEnded) {
      // Game masih berjalan, jangan hapus!
      return;
    }
    
    if (this._cleanupTimers.has(room)) {
      clearTimeout(this._cleanupTimers.get(room));
      this._cleanupTimers.delete(room);
    }
    
    // Clear broadcast counters
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
    
    this._broadcastToRoom(room, ["gameLowCardEnd", []]);
  }
  
  // ==================== PLAYER MANAGEMENT ====================
  
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
      
      // DELAY sebelum cek untuk menghindari race condition
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
      
      // Jangan cek jika sedang evaluasi atau draw
      if (game._isEvaluating || game.evaluationLocked) return;
      if (game.registrationOpen) return;
      
      const activePlayers = this._getActivePlayers(game);
      
      if (activePlayers.length === 0) {
        // Cek apakah masih ada yang belum submit
        const allPlayers = Array.from(game.players.keys());
        const submitted = Array.from(game.numbers?.keys() || []);
        const notSubmitted = allPlayers.filter(id => !submitted.includes(id) && !game.eliminated?.has(id));
        
        if (notSubmitted.length > 0) {
          // Masih ada yang belum submit, tunggu
          return;
        }
        
        // Benar-benar tidak ada pemain
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._broadcastToRoom(room, ["gameLowCardEnd", []]);
        this._scheduleGameCleanup(room, game);
        return;
      }
      
      if (activePlayers.length === 1 && !game._gameEnded) {
        // Cek apakah semua pemain sudah submit
        const activeIds = this._getActivePlayerIds(game);
        const submittedIds = Array.from(game.numbers?.keys() || []);
        const notSubmitted = activeIds.filter(id => !submittedIds.includes(id));
        
        if (notSubmitted.length > 0) {
          // Masih ada yang belum submit, jangan akhiri
          this._broadcastToRoom(room, ["gameLowCardInfo", `Waiting for ${notSubmitted.length} player(s)`]);
          return;
        }
        
        // Semua sudah submit, declare winner
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
      if (game._isActive && !game._gameEnded && game.players) {
        if (game.players.has(username)) {
          result.push({ game, room });
        }
      }
    }
    return result;
  }
  
  // ==================== BOT MANAGEMENT ====================
  
  _addBots(room, count) {
    try {
      const game = this.activeGames.get(room); // Langsung ambil
      if (!this._isGameActuallyRunning(game)) return;
      
      const botNames = ["moz1", "moz2", "moz3", "moz4"];
      
      const existingBots = Array.from(game.players.keys()).filter(id => id.startsWith('BOT_'));
      const existingBotCount = existingBots.length;
      
      const maxBotsToAdd = Math.min(count, CONSTANTS.MAX_BOTS_PER_GAME - existingBotCount);
      
      for (let i = 0; i < maxBotsToAdd; i++) {
        const botId = `BOT_${room}_${i}_${Date.now()}`;
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
      
      // Bot draw dengan delay yang lebih bervariasi
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
          try {
            this._evaluateRound(room, game);
          } catch(e) {}
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
  
  // ==================== GAME PHASES ====================
  
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
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
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
      game._drawPhaseStart = Date.now(); // ✅ TAMBAHKAN TIMESTAMP
      
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
      
      game._evalTimer = setTimeout(() => {
        try {
          this._evaluateRound(room, game);
        } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS);
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
            this._scheduleGameCleanup(room, game);
          }
        } catch(e) {}
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
      
      // Eliminasi pemain yang tidak submit
      for (const id of activeIds) {
        if (!submittedIds.has(id)) {
          eliminated.add(id);
        }
      }
      
      // Jika tidak ada yang submit
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
      
      // Jika hanya 1 yang submit dan semua lain eliminated
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
        for (const id of losers) {
          eliminated.add(id);
        }
      }
      
      const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
      
      // Jika semua angka sama dan masih ada ≥2 pemain
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
      
      // Jika hanya 1 pemain tersisa
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
      
      // Jika tidak ada pemain tersisa
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
      
      // Normal round result
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
  
  // ==================== GAME COMMANDS ====================
  
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
      const room = ws.room;
      
      if (!room) {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first!"]);
        return;
      }
      
      if (!this.wsClients.has(room)) {
        this._safeSend(ws, ["gameLowCardError", "Invalid room, please switch room"]);
        return;
      }
      
      const existingGames = this._findAllGamesByUsername(usernameClean);
      if (existingGames.length > 0) {
        this._safeSend(ws, ["gameLowCardInfo", `You are currently playing`]);
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
      const room = ws.room;
      
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
      const room = ws.room;
      
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
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameActuallyRunning(game)) {
        game.evaluationLocked = true;
        this._broadcastToRoom(room, ["gameLowCardWait", "Please wait for results..."]);
        game._evalTimer = setTimeout(() => {
          try {
            this._evaluateRound(room, game);
          } catch(e) {}
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
      const room = ws.room;
      
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
  
  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameStatus", { running: "false" }]);
        return;
      }
      
      let room = roomname;
      
      if (!room) {
        room = ws.room;
      }
      
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
  
  // ==================== EVENT HANDLER ====================
  
  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      
      const evt = data[0];
      
      if (evt === "switchRoom") {
        const [_, room, username] = data;
        await this.switchRoom(ws, room, username);
        return;
      }
      
      const room = ws.room;
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
            if (server.readyState === 0) {
              server.close(1000, "Timeout");
            }
          } catch(e) {}
        }, 5000);
        
        server._timeoutId = timeoutId;
        
        try { 
          this.state.acceptWebSocket(server);
        } catch(e) { 
          clearTimeout(timeoutId);
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
          } catch(e) {}
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
    } catch(e) {}
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
    } catch(e) {}
  }
  
  // ==================== DESTROY ====================
  
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.closing = true;
      this.isDestroyed = true;
      
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
      this._roomBroadcastCount.clear();
      this._roomBroadcastReset.clear();
      
      for (const [room, game] of this.activeGames) {
        this._deleteGame(room, game);
      }
      this.activeGames.clear();
    } catch(e) {}
  }
}
