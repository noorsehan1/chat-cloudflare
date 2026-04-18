// ==================== LOWCARDGAMEMANAGER.js - ZERO BUG FINAL ====================

const CONSTANTS = Object.freeze({
  MAX_LOWCARD_GAMES: 50,
  GAME_TIMEOUT_HOURS: 6,
  CLEANUP_INTERVAL_MS: 600000,
  REGISTRATION_TIME: 20,
  DRAW_TIME: 20,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 10,
  MASTER_TICK_INTERVAL_MS: 1000,
  EVALUATION_DELAY_MS: 3000,
  MAX_EVALUATION_TIME_MS: 10000,
  MAX_DRAW_WAIT_MS: 30000,
  LOCK_TIMEOUT_MS: 5000,
  MAX_GAME_AGE_MS: 6 * 60 * 60 * 1000,
  MAX_RETRY_ATTEMPTS: 3,
});

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
    this._destroyed = false;
    this._cleanupInterval = null;
    this._gameLocks = new Map();
    this._isShuttingDown = false;
    
    this._stats = {
      totalGamesStarted: 0,
      totalGamesEnded: 0,
      totalErrors: 0,
      lastError: null,
      lastErrorTime: null,
      totalLockTimeouts: 0
    };
    
    this._startCleanupInterval();
  }
  
  _startCleanupInterval() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this._cleanupInterval = setInterval(() => {
      if (!this._destroyed && !this._isShuttingDown) this.cleanupStaleGames();
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
  }
  
  async _acquireGameLock(room, timeoutMs = CONSTANTS.LOCK_TIMEOUT_MS) {
    if (this._destroyed || this._isShuttingDown) {
      throw new Error("Game manager destroyed");
    }
    
    if (!this._gameLocks.has(room)) {
      this._gameLocks.set(room, { locked: false, queue: [] });
    }
    
    const lock = this._gameLocks.get(room);
    
    if (!lock.locked) {
      lock.locked = true;
      return () => this._releaseGameLock(room);
    }
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = lock.queue.findIndex(item => item.resolve === resolve);
        if (index !== -1) {
          lock.queue.splice(index, 1);
        }
        this._stats.totalLockTimeouts++;
        reject(new Error(`Game lock timeout: ${room}`));
      }, timeoutMs);
      
      lock.queue.push({
        resolve: () => {
          clearTimeout(timeout);
          resolve(() => this._releaseGameLock(room));
        },
        reject
      });
    });
  }
  
  _releaseGameLock(room) {
    if (this._destroyed || this._isShuttingDown) return;
    
    const lock = this._gameLocks.get(room);
    if (!lock) return;
    
    if (lock.queue.length > 0) {
      const next = lock.queue.shift();
      if (next) next.resolve();
    } else {
      lock.locked = false;
    }
  }

  masterTick() {
    if (this._destroyed || this._isShuttingDown) return;
    const now = Date.now();
    
    const snapshot = Array.from(this.activeGames.entries());
    for (const [room, game] of snapshot) {
      if (!game || !game._isActive) {
        this.activeGames.delete(room);
        continue;
      }
      this._processGameTick(room, game, now);
    }
  }
  
  _processGameTick(room, game, now) {
    if (!game || !game._isActive) return;
    
    if (game._phase === 'evaluating' && game._evalStartTime) {
      if (now - game._evalStartTime > CONSTANTS.MAX_EVALUATION_TIME_MS) {
        this._forceNextRound(room);
      }
      return;
    }
    
    if (game._phase === 'draw' && game.drawStartTime) {
      if (now - game.drawStartTime > CONSTANTS.MAX_DRAW_WAIT_MS) {
        this._forceEvaluateRound(room);
      }
    }
    
    if (game._phase === 'registration') {
      this._handleRegistrationTick(game, room);
    } else if (game._phase === 'draw') {
      this._handleDrawTick(game, room);
    }
  }
  
  _forceNextRound(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || game.evaluationLocked) return;
    if (game._phase !== 'evaluating') return;
    this._safeBroadcast(room, ["gameLowCardInfo", "Processing next round..."]);
    this._evaluateRound(room);
  }
  
  _forceEvaluateRound(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || game.drawTimeExpired || game.evaluationLocked) return;
    if (game._phase !== 'draw') return;
    game.drawTimeExpired = true;
    this._scheduleEvaluation(room, game);
  }

  _scheduleEvaluation(room, game) {
    if (!game || !game._isActive) return;
    if (game._phase !== 'draw') return;
    if (game.evaluationLocked || game._phase === 'evaluating') return;
    
    if (game._evalTimeout) {
      clearTimeout(game._evalTimeout);
      game._evalTimeout = null;
    }
    
    game.evaluationLocked = true;
    game._phase = 'evaluating';
    game._evalStartTime = Date.now();

    this._clearGameTimeouts(game);
    this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);

    const roomName = room;
    game._evalTimeout = setTimeout(() => {
      if (this._destroyed || this._isShuttingDown) return;
      
      const currentGame = this._safeGetGame(roomName);
      if (currentGame && currentGame._isActive && 
          currentGame.evaluationLocked && 
          currentGame._phase === 'evaluating') {
        this._evaluateRound(roomName);
      }
      
      if (game._evalTimeout) {
        clearTimeout(game._evalTimeout);
        game._evalTimeout = null;
      }
    }, CONSTANTS.EVALUATION_DELAY_MS);
  }

  _safeBroadcast(room, message) {
    try {
      if (this._destroyed || this._isShuttingDown) return;
      if (this.chatServer?.broadcastToRoom && !this.chatServer._isClosing) {
        this.chatServer.broadcastToRoom(room, message);
      }
    } catch (e) {}
  }

  _safeSend(ws, message) {
    try {
      if (this._destroyed || this._isShuttingDown) return false;
      if (ws?.readyState === 1 && this.chatServer?.safeSend && !this.chatServer._isClosing) {
        return this.chatServer.safeSend(ws, message);
      }
      return false;
    } catch (e) {
      return false;
    }
  }

  _safeGetGame(room) {
    if (this._destroyed || this._isShuttingDown || !room) return null;
    const game = this.activeGames.get(room);
    return (game && game._isActive === true) ? game : null;
  }
  
  _logError(message) {
    this._stats.totalErrors++;
    this._stats.lastError = message;
    this._stats.lastErrorTime = Date.now();
  }

  cleanupStaleGames() {
    if (this._destroyed || this._isShuttingDown) return;
    const now = Date.now();
    const staleGames = [];
    
    if (this.activeGames.size > this._maxGames) {
      const entries = Array.from(this.activeGames.entries());
      entries.sort((a, b) => (a[1]._createdAt || 0) - (b[1]._createdAt || 0));
      const toDelete = entries.slice(0, this.activeGames.size - this._maxGames);
      for (const [room] of toDelete) staleGames.push(room);
    }
    
    for (const [room, game] of this.activeGames.entries()) {
      if (!game) {
        staleGames.push(room);
        continue;
      }
      
      if (game._createdAt && (now - game._createdAt) > CONSTANTS.MAX_GAME_AGE_MS) {
        staleGames.push(room);
        continue;
      }
      
      if (!game.players || game.players.size === 0) {
        staleGames.push(room);
        continue;
      }
      
      if (game._phase === 'evaluating' && game._evalStartTime && 
          (now - game._evalStartTime) > CONSTANTS.MAX_EVALUATION_TIME_MS * 2) {
        staleGames.push(room);
        continue;
      }
      
      if (game._phase === 'draw' && game.drawStartTime && 
          (now - game.drawStartTime) > CONSTANTS.MAX_DRAW_WAIT_MS * 2) {
        staleGames.push(room);
        continue;
      }
    }
    
    for (const room of staleGames) {
      const game = this.activeGames.get(room);
      if (game && game._isActive) {
        this.endGame(room);
      }
    }
  }

  getRandomCardTanda() {
    const tandaOptions = ["C1", "C2", "C3", "C4"];
    return tandaOptions[Math.floor(Math.random() * tandaOptions.length)];
  }

  getRandomDrawTime() {
    return Math.floor(Math.random() * (CONSTANTS.BOT_DRAW_MAX_SECONDS - CONSTANTS.BOT_DRAW_MIN_SECONDS + 1)) + CONSTANTS.BOT_DRAW_MIN_SECONDS;
  }

  getBotNumberByRound(round) {
    if (round <= 2) {
      return Math.floor(Math.random() * 12) + 1;
    }
    if (round >= 3) {
      const isGetHighNumber = Math.random() < 0.6;
      if (isGetHighNumber) {
        const bigNumbers = [8, 9, 10, 11, 12];
        return bigNumbers[Math.floor(Math.random() * bigNumbers.length)];
      } else {
        const smallNumbers = [1, 2, 3, 4, 5, 6, 7];
        return smallNumbers[Math.floor(Math.random() * smallNumbers.length)];
      }
    }
    return Math.floor(Math.random() * 12) + 1;
  }

  async handleEvent(ws, data) {
    if (this._destroyed || this._isShuttingDown || !ws || !data || !data[0]) return;
    const evt = data[0];
    
    try {
      switch (evt) {
        case "gameLowCardStart":
          await this.startGame(ws, data[1]);
          break;
        case "gameLowCardJoin":
          await this.joinGame(ws);
          break;
        case "gameLowCardNumber":
          await this.submitNumber(ws, data[1], data[2] || "");
          break;
        case "gameLowCardEnd":
          if (ws?.roomname) await this.endGame(ws.roomname);
          break;
      }
    } catch (error) {
      this._logError(`HandleEvent error for ${evt}: ${error.message}`);
      this._safeSend(ws, ["gameLowCardError", "Game error occurred"]);
    }
  }

  async startGame(ws, bet) {
    if (this._destroyed || this._isShuttingDown) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.roomname;
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const existingGame = this.activeGames.get(room);
      if (existingGame && existingGame._isActive) {
        this._safeSend(ws, ["gameLowCardError", "Game already running in this room"]);
        return;
      }

      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0) {
        this._safeSend(ws, ["gameLowCardError", "Invalid bet amount"]);
        return;
      }
      
      if (betAmount !== 0 && betAmount < 100) {
        this._safeSend(ws, ["gameLowCardError", "Bet must be 0 or at least 100"]);
        return;
      }

      const game = {
        room: room,
        players: new Map(),
        botPlayers: new Map(),
        registrationOpen: true,
        round: 1,
        numbers: new Map(),
        tanda: new Map(),
        eliminated: new Set(),
        winner: null,
        betAmount: betAmount,
        registrationTimeLeft: CONSTANTS.REGISTRATION_TIME,
        drawTimeLeft: CONSTANTS.DRAW_TIME,
        hostId: ws.idtarget,
        hostName: ws.username || ws.idtarget,
        useBots: false,
        evaluationLocked: false,
        drawTimeExpired: false,
        _createdAt: Date.now(),
        _isActive: true,
        _phase: 'registration',
        _pendingBotDraws: new Map(),
        _hasBroadcastInitial: false,
        _evalTimeout: null,
        _evalStartTime: null,
        drawStartTime: null
      };

      game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });
      this.activeGames.set(room, game);
      this._stats.totalGamesStarted++;
      
      this._safeBroadcast(room, ["gameLowCardStart", game.betAmount]);
      this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
      
    } catch (e) {
      this._logError(`StartGame error in ${room}: ${e.message}`);
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    } finally {
      if (release) release();
    }
  }

  _clearGameTimeouts(game) {
    if (game && game._evalTimeout) {
      clearTimeout(game._evalTimeout);
      game._evalTimeout = null;
    }
  }

  _handleRegistrationTick(game, room) {
    if (!game || !game._isActive) return;
    
    const timesToNotify = [20, 15, 10, 5, 0];
    
    if (timesToNotify.includes(game.registrationTimeLeft)) {
      if (game.registrationTimeLeft === 0) {
        this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
        if (game.players && game.players.size === 1) {
          this._addFourMozBots(room);
        }
        this._closeRegistration(room);
        return;
      } else {
        this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.registrationTimeLeft}s`]);
      }
    }
    
    game.registrationTimeLeft--;
    if (game.registrationTimeLeft < 0 && game.registrationOpen) {
      this._closeRegistration(room);
    }
  }

  _handleDrawTick(game, room) {
    if (!game || !game._isActive) return;
    
    const timesToNotify = [20, 15, 10, 5, 0];

    if (game.drawStartTime === null && game._phase === 'draw') {
      game.drawStartTime = Date.now();
    }

    if (timesToNotify.includes(game.drawTimeLeft)) {
      if (game.drawTimeLeft === 0) {
        if (game.drawTimeExpired) return;
        
        this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
        game.drawTimeExpired = true;

        const activePlayers = game.players ? 
          Array.from(game.players.keys()).filter(id => !game.eliminated.has(id)) : [];
        const allDrawn = game.numbers ? game.numbers.size === activePlayers.length : false;

        if (!allDrawn) {
          this._safeBroadcast(room, ["gameLowCardInfo", "Time is up, processing current draws..."]);
        }

        this._scheduleEvaluation(room, game);
        return;
      } else {
        if (game.drawTimeLeft !== CONSTANTS.DRAW_TIME || game._hasBroadcastInitial !== true) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.drawTimeLeft}s`]);
        }
        game._hasBroadcastInitial = true;
      }
    }

    game.drawTimeLeft--;

    if (game.useBots && game._pendingBotDraws && game._pendingBotDraws.size > 0) {
      const toDraw = [];
      const snapshot = Array.from(game._pendingBotDraws.entries());
      const newMap = new Map();
      
      for (const [botId, timeRemaining] of snapshot) {
        if (timeRemaining <= 0) {
          toDraw.push(botId);
        } else {
          newMap.set(botId, timeRemaining - 1);
        }
      }
      
      game._pendingBotDraws = newMap;
      
      for (const botId of toDraw) {
        if (!game.drawTimeExpired && !game.evaluationLocked && 
            game.eliminated && !game.eliminated.has(botId) && 
            game.numbers && !game.numbers.has(botId)) {
          this._handleBotDraw(room, botId);
        }
      }
    }
  }

  _addFourMozBots(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || this._destroyed || this._isShuttingDown) return;
    if (game.useBots || (game.botPlayers && game.botPlayers.size > 0)) return;
    
    game.useBots = true;
    if (game._pendingBotDraws) game._pendingBotDraws.clear();
    game._pendingBotDraws = new Map();
    
    const mozNames = ["Moz1", "Moz2", "Moz3", "Moz4"];
    
    for (let i = 0; i < 4; i++) {
      const randomSuffix = Math.random().toString(36).substring(7);
      const botId = `BOT_${room}_${i}_${Date.now()}_${randomSuffix}`;
      const botName = mozNames[i];
      
      if (!game.players) game.players = new Map();
      if (!game.botPlayers) game.botPlayers = new Map();
      
      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
    }
  }

  async _closeRegistration(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || this._destroyed || this._isShuttingDown) return;
    
    if (!game.players) {
      this.activeGames.delete(room);
      return;
    }
    
    const playerCount = game.players.size;
    
    if (playerCount < 2) {
      const hostId = game.hostId;
      const hostName = game.hostName;
      const betAmount = game.betAmount;
      
      const hostConnections = this.chatServer?.userConnections?.get(hostId);
      if (hostConnections) {
        const snapshot = Array.from(hostConnections);
        for (const client of snapshot) {
          if (client && client.readyState === 1 && !client._isClosing) {
            this._safeSend(client, ["gameLowCardNoJoin", hostName, betAmount]);
            break;
          }
        }
      }
      this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players", hostId]);
      this.activeGames.delete(room);
      return;
    }

    game.registrationOpen = false;
    game._phase = 'draw';
    game.drawTimeLeft = CONSTANTS.DRAW_TIME;
    game.drawTimeExpired = false;
    game._hasBroadcastInitial = false;
    game.drawStartTime = null;

    const playersList = Array.from(game.players.values())
      .filter(p => p && p.name)
      .map(p => p.name);
    
    this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
    this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._safeBroadcast(room, ["gameLowCardNextRound", 1]);

    if (game.useBots && game.botPlayers) {
      if (game._pendingBotDraws) game._pendingBotDraws.clear();
      game._pendingBotDraws = new Map();
      const activeBots = Array.from(game.botPlayers.keys())
        .filter(botId => !game.eliminated.has(botId));
      for (const botId of activeBots) {
        game._pendingBotDraws.set(botId, this.getRandomDrawTime());
      }
    }
  }

  _handleBotDraw(room, botId) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || this._destroyed || this._isShuttingDown) return;
    
    if (!game.eliminated || !game.numbers) return;
    if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
    if (game.drawTimeExpired || game.evaluationLocked) return;
    
    const botNumber = this.getBotNumberByRound(game.round);
    const tanda = this.getRandomCardTanda();
    
    if (!game.numbers) game.numbers = new Map();
    if (!game.tanda) game.tanda = new Map();
    
    game.numbers.set(botId, botNumber);
    game.tanda.set(botId, tanda);
    
    const botPlayer = game.players ? game.players.get(botId) : null;
    const botName = botPlayer?.name || botId;
    this._safeBroadcast(room, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
    
    const activePlayers = game.players ? 
      Array.from(game.players.keys()).filter(id => !game.eliminated.has(id)) : [];
    const allDrawn = game.numbers.size === activePlayers.length;
    
    if (!game.evaluationLocked && allDrawn && game._phase !== 'evaluating') {
      this._scheduleEvaluation(room, game);
    }
  }

  async joinGame(ws) {
    if (this._destroyed || this._isShuttingDown) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.roomname;
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const game = this._safeGetGame(room);
      
      if (!game || !game._isActive) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      
      if (game.evaluationLocked) {
        this._safeSend(ws, ["gameLowCardError", "Game in progress, please wait"]);
        return;
      }
      
      if (!game.registrationOpen) {
        this._safeSend(ws, ["gameLowCardError", "Registration closed"]);
        return;
      }
      
      if (game.players.has(ws.idtarget)) {
        this._safeSend(ws, ["gameLowCardError", "Already joined"]);
        return;
      }

      game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });
      this._safeBroadcast(room, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
      
    } catch (e) {
      this._logError(`JoinGame error in ${room}: ${e.message}`);
      this._safeSend(ws, ["gameLowCardError", "Failed to join game"]);
    } finally {
      if (release) release();
    }
  }

  async submitNumber(ws, number, tanda = "") {
    if (this._destroyed || this._isShuttingDown) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.roomname;
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const game = this._safeGetGame(room);
      
      if (!game || !game._isActive) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      
      if (game.evaluationLocked) {
        this._safeSend(ws, ["gameLowCardError", "Please wait, results are being processed..."]);
        return;
      }
      
      if (game.registrationOpen) {
        this._safeSend(ws, ["gameLowCardError", "Registration still open"]);
        return;
      }
      
      if (!game.players.has(ws.idtarget) || game.eliminated.has(ws.idtarget)) {
        this._safeSend(ws, ["gameLowCardError", "Not in game or eliminated"]);
        return;
      }
      
      if (game.numbers.has(ws.idtarget)) {
        this._safeSend(ws, ["gameLowCardError", "Already submitted number"]);
        return;
      }

      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (allDrawn) {
        this._safeSend(ws, ["gameLowCardError", "All players have already drawn, please wait for results..."]);
        return;
      }

      if (game.drawTimeExpired) {
        this._safeSend(ws, ["gameLowCardError", "Draw time has expired!"]);
        return;
      }

      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
        return;
      }

      game.numbers.set(ws.idtarget, n);
      game.tanda.set(ws.idtarget, tanda);
      
      const player = game.players.get(ws.idtarget);
      const playerName = player?.name || ws.idtarget;
      this._safeBroadcast(room, ["gameLowCardPlayerDraw", playerName, n, tanda]);

      const newActivePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const nowAllDrawn = game.numbers.size === newActivePlayers.length;
      
      if (!game.evaluationLocked && nowAllDrawn && game._phase !== 'evaluating') {
        this._scheduleEvaluation(room, game);
      }
      
    } catch (e) {
      this._logError(`SubmitNumber error in ${room}: ${e.message}`);
      this._safeSend(ws, ["gameLowCardError", "Failed to submit number"]);
    } finally {
      if (release) release();
    }
  }

  async _evaluateRound(room) {
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed || this._isShuttingDown) return;
      
      this._clearGameTimeouts(game);
      
      if (!game.players || game.players.size === 0) {
        this.activeGames.delete(room);
        return;
      }
      
      const numbers = game.numbers || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const round = game.round || 1;
      const betAmount = game.betAmount || 0;
      
      const entries = Array.from(numbers.entries());
      const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
      
      if (entries.length === 0) {
        this._safeBroadcast(room, ["gameLowCardError", "Game ended - no submissions"]);
        this.activeGames.delete(room);
        return;
      }
      
      const submittedIds = new Set(numbers.keys());
      const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
      noSubmit.forEach(id => eliminated.add(id));

      if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
        const winnerId = entries[0][0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        game.winner = winnerId;
        
        this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
        
        this._cleanupGameResources(game);
        this.activeGames.delete(room);
        this._stats.totalGamesEnded++;
        return;
      }

      const values = entries.map(([, n]) => n);
      const allSame = values.length > 0 && values.every(v => v === values[0]);
      let losers = [];

      if (!allSame && values.length > 0) {
        const lowest = Math.min(...values);
        losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
        losers.forEach(id => eliminated.add(id));
      }

      const newRemaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

      if (newRemaining.length === 1) {
        const winnerId = newRemaining[0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        game.winner = winnerId;
        
        this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
        
        this._cleanupGameResources(game);
        this.activeGames.delete(room);
        this._stats.totalGamesEnded++;
        return;
      }
      
      if (newRemaining.length === 0) {
        this.activeGames.delete(room);
        return;
      }

      const numbersArr = entries.map(([id, n]) => {
        const player = players.get(id);
        const playerName = player?.name || id;
        const playerTanda = game.tanda?.get(id) || "";
        return `${playerName}:${n}(${playerTanda})`;
      });
      
      const loserNames = [...new Set([...losers, ...noSubmit])].map(id => {
        const player = players.get(id);
        return player?.name || id;
      });
      
      const remainingNames = newRemaining.map(id => {
        const player = players.get(id);
        return player?.name || id;
      });

      this._safeBroadcast(room, [
        "gameLowCardRoundResult",
        round,
        numbersArr,
        loserNames,
        remainingNames
      ]);

      numbers.clear();
      if (game.tanda) game.tanda.clear();
      
      game.round++;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      game._phase = 'draw';
      game.drawTimeLeft = CONSTANTS.DRAW_TIME;
      game._hasBroadcastInitial = false;
      game.drawStartTime = null;
      
      if (game.useBots && game.botPlayers) {
        if (game._pendingBotDraws) game._pendingBotDraws.clear();
        game._pendingBotDraws = new Map();
        const activeBots = Array.from(game.botPlayers.keys())
          .filter(botId => !game.eliminated.has(botId));
        for (const botId of activeBots) {
          game._pendingBotDraws.set(botId, this.getRandomDrawTime());
        }
      }
      
      this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
      
    } catch (e) {
      this._logError(`EvaluateRound error in ${room}: ${e.message}`);
      const game = this.activeGames.get(room);
      if (game && game._isActive) {
        this._safeBroadcast(room, ["gameLowCardError", "Game error, ending game"]);
        this._cleanupGameResources(game);
        this.activeGames.delete(room);
      }
    } finally {
      if (release) release();
    }
  }

  _cleanupGameResources(game) {
    if (!game) return;
    
    if (game._evalTimeout) {
      clearTimeout(game._evalTimeout);
      game._evalTimeout = null;
    }
    
    if (game.players) { game.players.clear(); game.players = null; }
    if (game.botPlayers) { game.botPlayers.clear(); game.botPlayers = null; }
    if (game.numbers) { game.numbers.clear(); game.numbers = null; }
    if (game.tanda) { game.tanda.clear(); game.tanda = null; }
    if (game.eliminated) { game.eliminated.clear(); game.eliminated = null; }
    if (game._pendingBotDraws) { 
      game._pendingBotDraws.clear(); 
      game._pendingBotDraws = null;
    }
  }

  async endGame(room) {
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const game = this.activeGames.get(room);
      if (!game || !game._isActive) return;
      
      const playersList = [];
      if (game.players) {
        for (const player of game.players.values()) {
          if (player && player.name) playersList.push(player.name);
        }
      }
      
      game._isActive = false;
      this._cleanupGameResources(game);
      
      if (playersList.length > 0) {
        this._safeBroadcast(room, ["gameLowCardEnd", playersList]);
      }
      
      this.activeGames.delete(room);
      this._stats.totalGamesEnded++;
      
    } catch (e) {
      this._logError(`EndGame error in ${room}: ${e.message}`);
    } finally {
      if (release) release();
    }
  }
  
  getGame(room) {
    if (this._destroyed || this._isShuttingDown || !room) return null;
    const game = this.activeGames.get(room);
    return (game && game._isActive) ? game : null;
  }
  
  healthCheck() {
    return {
      destroyed: this._destroyed,
      activeGames: this.activeGames.size,
      maxGames: this._maxGames,
      totalPlayers: Array.from(this.activeGames.values()).reduce(
        (sum, game) => sum + (game.players?.size || 0), 0
      ),
      pendingLocks: Array.from(this._gameLocks.values()).reduce(
        (sum, lock) => sum + lock.queue.length, 0
      ),
      stats: { ...this._stats }
    };
  }
  
  destroy() {
    this._isShuttingDown = true;
    this._destroyed = true;
    
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    const snapshot = Array.from(this.activeGames.entries());
    for (const [room, game] of snapshot) {
      if (game) {
        this._clearGameTimeouts(game);
        this.endGame(room).catch(() => {});
      }
    }
    
    this.activeGames.clear();
    
    for (const lock of this._gameLocks.values()) {
      for (const waiter of lock.queue) {
        waiter.reject(new Error("Game manager destroyed"));
      }
    }
    this._gameLocks.clear();
    
    this.chatServer = null;
  }
}
