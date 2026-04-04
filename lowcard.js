// ============================
// LowCardGameManager (FIXED VERSION)
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._cleanupInterval = null;
    this._destroyed = false;
    this._errorLogs = [];
    
    // Error handler untuk prevent crash
    this._errorHandler = (error, context) => {
      const errorMsg = error?.message || String(error);
      this._errorLogs.push({ time: Date.now(), context, error: errorMsg });
      if (this._errorLogs.length > 100) this._errorLogs.shift();
      console.error(`[LowCardGame] ${context}:`, errorMsg);
    };
    
    // Auto cleanup setiap 5 menit
    this._cleanupInterval = setInterval(() => {
      if (!this._destroyed) this.cleanupStaleGames();
    }, 300000);
  }

  // ========== SAFE HELPER METHODS ==========
  _safeBroadcast(room, message) {
    try {
      if (this._destroyed) return;
      if (this.chatServer && typeof this.chatServer.broadcastToRoom === 'function') {
        this.chatServer.broadcastToRoom(room, message);
      }
    } catch (error) {
      this._errorHandler(error, `broadcast ${message?.[0] || 'unknown'}`);
    }
  }

  _safeSend(ws, message) {
    try {
      if (this._destroyed) return false;
      if (ws && ws.readyState === 1 && this.chatServer && typeof this.chatServer.safeSend === 'function') {
        return this.chatServer.safeSend(ws, message);
      }
      return false;
    } catch (error) {
      this._errorHandler(error, `send ${message?.[0] || 'unknown'}`);
      return false;
    }
  }

  _safeGetGame(room) {
    try {
      if (this._destroyed || !room) return null;
      const game = this.activeGames.get(room);
      return (game && game._isActive) ? game : null;
    } catch (error) {
      this._errorHandler(error, `getGame ${room}`);
      return null;
    }
  }

  // ========== CLEANUP METHODS ==========
  cleanupStaleGames() {
    try {
      if (this._destroyed) return;
      const now = Date.now();
      const staleGames = [];
      
      for (const [room, game] of this.activeGames.entries()) {
        if (!game) {
          staleGames.push(room);
          continue;
        }
        
        if (game._createdAt && (now - game._createdAt) > 3600000) {
          staleGames.push(room);
        }
        
        if (game.players && game.players.size === 0) {
          staleGames.push(room);
        }
      }
      
      for (const room of staleGames) {
        this.endGame(room);
      }
    } catch (error) {
      this._errorHandler(error, 'cleanupStaleGames');
    }
  }

  // ========== TIMER MANAGEMENT ==========
  _clearAllTimers(game) {
    try {
      if (!game) return;
      
      if (game._regInterval) {
        clearInterval(game._regInterval);
        game._regInterval = null;
      }
      
      if (game._drawInterval) {
        clearInterval(game._drawInterval);
        game._drawInterval = null;
      }
      
      if (game.countdownTimers && Array.isArray(game.countdownTimers)) {
        for (const timer of game.countdownTimers) {
          if (timer) {
            if (timer.interval) clearInterval(timer.interval);
            if (timer.timeout) clearTimeout(timer.timeout);
          }
        }
        game.countdownTimers = [];
      }
      
      if (game._botTimers && Array.isArray(game._botTimers)) {
        for (const timer of game._botTimers) {
          if (timer) clearTimeout(timer);
        }
        game._botTimers = [];
      }
      
      if (game._botDrawTimeouts) {
        for (const timeout of game._botDrawTimeouts) {
          try { clearTimeout(timeout); } catch (e) {}
        }
        game._botDrawTimeouts.clear();
      }
      
    } catch (error) {
      this._errorHandler(error, 'clearAllTimers');
    }
  }

  // ========== GAME UTILITIES ==========
  getRandomCardTanda() {
    try {
      const tandaOptions = ["C1", "C2", "C3", "C4"];
      return tandaOptions[Math.floor(Math.random() * tandaOptions.length)];
    } catch {
      return "C1";
    }
  }

  getRandomDrawTime() {
    try {
      return Math.floor(Math.random() * 23) + 3;
    } catch {
      return 10;
    }
  }

  getBotNumberByRound(round) {
    try {
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
    } catch {
      return 7;
    }
  }

  // ========== GAME CORE METHODS ==========
  handleEvent(ws, data) {
    try {
      if (this._destroyed || !ws || !data || !Array.isArray(data) || data.length === 0) return;

      const evt = data[0];
      if (typeof evt !== 'string') return;

      switch (evt) {
        case "gameLowCardStart":
          this.startGame(ws, data[1]);
          break;
        case "gameLowCardJoin":
          this.joinGame(ws);
          break;
        case "gameLowCardNumber":
          this.submitNumber(ws, data[1], data[2] || "");
          break;
        case "gameLowCardEnd":
          if (ws && ws.roomname) this.endGame(ws.roomname);
          break;
        default:
          break;
      }
    } catch (error) {
      this._errorHandler(error, 'handleEvent');
    }
  }

  startGame(ws, bet) {
    try {
      if (this._destroyed) return;
      if (!ws || !ws.roomname || !ws.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }

      const room = ws.roomname;
      
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
        countdownTimers: [],
        _botTimers: [],
        _botDrawTimeouts: new Set(),
        registrationTime: 25,
        drawTime: 30,
        hostId: ws.idtarget,
        hostName: ws.username || ws.idtarget,
        useBots: false,
        evaluationLocked: false,
        drawTimeExpired: false,
        _createdAt: Date.now(),
        _isActive: true,
        _regInterval: null,
        _drawInterval: null
      };

      game.players.set(ws.idtarget, { 
        id: ws.idtarget, 
        name: ws.username || ws.idtarget 
      });

      this.activeGames.set(room, game);

      this._safeBroadcast(room, ["gameLowCardStart", game.betAmount]);
      this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);

      this._startRegistrationCountdown(room);
      
    } catch (error) {
      this._errorHandler(error, 'startGame');
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }

  _startRegistrationCountdown(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      this._clearAllTimers(game);

      let timeLeft = game.registrationTime;
      const timesToNotify = [20, 10, 5, 0];

      game._regInterval = setInterval(() => {
        try {
          const currentGame = this._safeGetGame(room);
          if (this._destroyed || !currentGame || !currentGame._isActive) {
            if (game._regInterval) {
              clearInterval(game._regInterval);
              game._regInterval = null;
            }
            return;
          }

          if (timesToNotify.includes(timeLeft)) {
            if (timeLeft === 0) {
              this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
              
              if (game.players && game.players.size === 1) {
                this._addFourMozBots(room);
              }
              
              this._closeRegistration(room);
              
              if (game._regInterval) {
                clearInterval(game._regInterval);
                game._regInterval = null;
              }
            } else {
              this._safeBroadcast(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
            }
          }

          timeLeft--;
          if (timeLeft < 0 && game._regInterval) {
            clearInterval(game._regInterval);
            game._regInterval = null;
          }
        } catch (error) {
          this._errorHandler(error, 'registration interval');
          if (game._regInterval) {
            clearInterval(game._regInterval);
            game._regInterval = null;
          }
        }
      }, 1000);

      if (!game.countdownTimers) game.countdownTimers = [];
      game.countdownTimers.push({ interval: game._regInterval });
      
    } catch (error) {
      this._errorHandler(error, 'startRegistrationCountdown');
    }
  }

  _addFourMozBots(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      if (game.useBots || (game.botPlayers && game.botPlayers.size > 0)) return;
      
      game.useBots = true;
      
      const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
      
      for (let i = 0; i < 4; i++) {
        // FIXED: Add random suffix to prevent duplicate bot IDs
        const randomSuffix = Math.random().toString(36).substring(7);
        const botId = `BOT_MOZ_${room}_${i}_${Date.now()}_${randomSuffix}`;
        const botName = mozNames[i];
        
        if (!game.players) game.players = new Map();
        if (!game.botPlayers) game.botPlayers = new Map();
        
        game.players.set(botId, { id: botId, name: botName });
        game.botPlayers.set(botId, botName);
        
        this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
      }
    } catch (error) {
      this._errorHandler(error, 'addFourMozBots');
    }
  }

  _closeRegistration(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;

      if (!game.players) {
        this.activeGames.delete(room);
        return;
      }
      
      const playerCount = game.players.size;
      
      if (playerCount < 2) {
        if (this.chatServer && this.chatServer.clients) {
          for (const client of this.chatServer.clients) {
            if (client && client.idtarget === game.hostId && client.readyState === 1) {
              this._safeSend(client, ["gameLowCardNoJoin", game.hostName, game.betAmount]);
              break;
            }
          }
        }

        this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players", game.hostId]);
        
        // FIXED: Clear timers before deleting game
        this._clearAllTimers(game);
        
        this.activeGames.delete(room);
        return;
      }

      game.registrationOpen = false;

      const playersList = Array.from(game.players.values())
        .filter(p => p && p.name)
        .map(p => p.name);

      this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
      this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
      this._safeBroadcast(room, ["gameLowCardNextRound", 1]);

      this._startDrawCountdown(room);
    } catch (error) {
      this._errorHandler(error, 'closeRegistration');
    }
  }

  _startDrawCountdown(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      this._clearAllTimers(game);
      game.evaluationLocked = false;
      game.drawTimeExpired = false;

      let timeLeft = game.drawTime;
      const timesToNotify = [20, 10, 5, 0];

      game._drawInterval = setInterval(() => {
        try {
          const currentGame = this._safeGetGame(room);
          if (this._destroyed || !currentGame || !currentGame._isActive) {
            if (game._drawInterval) {
              clearInterval(game._drawInterval);
              game._drawInterval = null;
            }
            return;
          }

          if (timesToNotify.includes(timeLeft)) {
            if (timeLeft === 0) {
              this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
              
              game.drawTimeExpired = true;
              
              const activePlayers = Array.from(game.players.keys())
                .filter(id => !game.eliminated.has(id));
              const allDrawn = game.numbers.size === activePlayers.length;
              
              if (!allDrawn) {
                this._safeBroadcast(room, ["gameLowCardInfo", "Time is up, processing current draws..."]);
              }
              
              game.evaluationLocked = true;
              this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
              
              const evalTimeout = setTimeout(() => {
                try {
                  const currentGame = this._safeGetGame(room);
                  if (currentGame && currentGame._isActive && !this._destroyed) {
                    this._evaluateRound(room);
                  }
                } catch (evalError) {
                  this._errorHandler(evalError, 'evaluateRound timeout');
                }
              }, 2000);
              
              if (!game.countdownTimers) game.countdownTimers = [];
              game.countdownTimers.push({ timeout: evalTimeout });
              
              if (game._drawInterval) {
                clearInterval(game._drawInterval);
                game._drawInterval = null;
              }
            } else {
              this._safeBroadcast(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
            }
          }

          timeLeft--;
          if (timeLeft < 0 && game._drawInterval) {
            clearInterval(game._drawInterval);
            game._drawInterval = null;
          }
        } catch (error) {
          this._errorHandler(error, 'draw interval');
          if (game._drawInterval) {
            clearInterval(game._drawInterval);
            game._drawInterval = null;
          }
        }
      }, 1000);

      if (!game.countdownTimers) game.countdownTimers = [];
      game.countdownTimers.push({ interval: game._drawInterval });

      if (game.useBots && game.botPlayers) {
        const activeBots = Array.from(game.botPlayers.keys())
          .filter(botId => !game.eliminated.has(botId) && !game.numbers.has(botId));
        
        for (const botId of activeBots) {
          const drawTime = this.getRandomDrawTime();
          
          const botTimeout = setTimeout(() => {
            try {
              const currentGame = this._safeGetGame(room);
              if (currentGame && currentGame._isActive && !currentGame.drawTimeExpired && !currentGame.evaluationLocked && !this._destroyed) {
                this._handleBotDraw(room, botId);
              }
            } catch (botError) {
              this._errorHandler(botError, `bot draw ${botId}`);
            }
          }, drawTime * 1000);
          
          if (!game._botTimers) game._botTimers = [];
          game._botTimers.push(botTimeout);
          if (!game._botDrawTimeouts) game._botDrawTimeouts = new Set();
          game._botDrawTimeouts.add(botTimeout);
          if (!game.countdownTimers) game.countdownTimers = [];
          game.countdownTimers.push({ timeout: botTimeout });
        }
      }
      
    } catch (error) {
      this._errorHandler(error, 'startDrawCountdown');
    }
  }

  _handleBotDraw(room, botId) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
      if (game.drawTimeExpired || game.evaluationLocked) return;
      
      const botNumber = this.getBotNumberByRound(game.round);
      const tanda = this.getRandomCardTanda();
      
      if (!game.numbers) game.numbers = new Map();
      if (!game.tanda) game.tanda = new Map();
      
      game.numbers.set(botId, botNumber);
      game.tanda.set(botId, tanda);
      
      const botPlayer = game.players.get(botId);
      const botName = botPlayer?.name || botId;
      
      this._safeBroadcast(room, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
      
      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (!game.evaluationLocked && allDrawn) {
        game.evaluationLocked = true;
        this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
        
        const evalTimeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this._evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound after bot draw');
          }
        }, 2000);
        
        if (!game.countdownTimers) game.countdownTimers = [];
        game.countdownTimers.push({ timeout: evalTimeout });
      }
    } catch (error) {
      this._errorHandler(error, 'handleBotDraw');
    }
  }

  joinGame(ws) {
    try {
      if (this._destroyed) return;
      if (!ws || !ws.roomname || !ws.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }

      const room = ws.roomname;
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

      game.players.set(ws.idtarget, { 
        id: ws.idtarget, 
        name: ws.username || ws.idtarget 
      });

      this._safeBroadcast(room, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
      
    } catch (error) {
      this._errorHandler(error, 'joinGame');
      this._safeSend(ws, ["gameLowCardError", "Failed to join game"]);
    }
  }

  submitNumber(ws, number, tanda = "") {
    try {
      if (this._destroyed) return;
      if (!ws || !ws.roomname || !ws.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }

      const room = ws.roomname;
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
      
      if (!game.evaluationLocked && nowAllDrawn) {
        game.evaluationLocked = true;
        this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
        
        const evalTimeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this._evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound after submit');
          }
        }, 2000);
        
        if (!game.countdownTimers) game.countdownTimers = [];
        game.countdownTimers.push({ timeout: evalTimeout });
      }
      
    } catch (error) {
      this._errorHandler(error, 'submitNumber');
      this._safeSend(ws, ["gameLowCardError", "Failed to submit number"]);
    }
  }

  // ========== FIX: EVALUATE ROUND DENGAN SAFE DESTRUCTURING ==========
  _evaluateRound(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      // VALIDASI LENGKAP
      if (!game.players || game.players.size === 0) {
        this._clearAllTimers(game);
        this.activeGames.delete(room);
        return;
      }
      
      // Safe destructuring dengan default
      const numbers = game.numbers || new Map();
      const tanda = game.tanda || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const round = game.round || 1;
      const betAmount = game.betAmount || 0;
      
      // Validasi numbers
      if (!numbers || typeof numbers.entries !== 'function') {
        this._errorHandler(new Error('Invalid numbers map'), 'evaluateRound');
        this._clearAllTimers(game);
        this.activeGames.delete(room);
        return;
      }
      
      this._clearAllTimers(game);
      
      let entries = [];
      try {
        entries = Array.from(numbers.entries());
      } catch (e) {
        this._errorHandler(e, 'evaluateRound entries');
        this.activeGames.delete(room);
        return;
      }
      
      if (entries.length === 0) {
        const remainingPlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
        if (remainingPlayers.length === 0) {
          this.activeGames.delete(room);
          return;
        }
        
        game.round++;
        game.evaluationLocked = false;
        game.drawTimeExpired = false;
        this._startDrawCountdown(room);
        return;
      }
      
      const submittedIds = new Set(numbers.keys());
      const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
      const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
      noSubmit.forEach(id => eliminated.add(id));

      if (entries.length === 0) {
        this._safeBroadcast(room, ["gameLowCardError", "No numbers drawn this round"]);
        this.activeGames.delete(room);
        return;
      }

      const remainingPlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));

      // Only 1 player submitted and others eliminated = winner
      if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
        const winnerId = entries[0][0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        game.winner = winnerId;
        
        this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
        this.activeGames.delete(room);
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

      // Only 1 player left = winner
      if (newRemaining.length === 1) {
        const winnerId = newRemaining[0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        game.winner = winnerId;
        
        this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
        this.activeGames.delete(room);
        return;
      }

      // Format numbers for broadcast
      const numbersArr = entries.map(([id, n]) => {
        const player = players.get(id);
        const playerName = player?.name || id;
        const playerTanda = tanda.get(id) || "";
        return `${playerName}:${n}(${playerTanda})`;
      });
      
      const loserNames = losers.concat(noSubmit).map(id => {
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

      // Clear for next round
      numbers.clear();
      tanda.clear();
      game.round++;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      
      this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
      this._startDrawCountdown(room);
      
    } catch (error) {
      this._errorHandler(error, 'evaluateRound');
      try {
        this.activeGames.delete(room);
      } catch (e) {}
    }
  }

  endGame(room) {
    try {
      const game = this.activeGames.get(room);
      if (!game) return;
      
      const playersList = [];
      if (game.players) {
        for (const player of game.players.values()) {
          if (player && player.name) playersList.push(player.name);
        }
      }
      
      game._isActive = false;
      
      this._clearAllTimers(game);
      
      if (game.players) game.players.clear();
      if (game.botPlayers) game.botPlayers.clear();
      if (game.numbers) game.numbers.clear();
      if (game.tanda) game.tanda.clear();
      if (game.eliminated) game.eliminated.clear();
      if (game._botDrawTimeouts) {
        for (const timeout of game._botDrawTimeouts) {
          try { clearTimeout(timeout); } catch (e) {}
        }
        game._botDrawTimeouts.clear();
      }
      
      game.countdownTimers = null;
      game._botTimers = null;
      game._botDrawTimeouts = null;
      game._regInterval = null;
      game._drawInterval = null;
      
      if (playersList.length > 0) {
        this._safeBroadcast(room, ["gameLowCardEnd", playersList]);
      }
      
      this.activeGames.delete(room);
      
    } catch (error) {
      this._errorHandler(error, 'endGame');
      this.activeGames.delete(room);
    }
  }
  
  getGame(room) {
    try {
      if (this._destroyed || !room) return null;
      const game = this.activeGames.get(room);
      return (game && game._isActive) ? game : null;
    } catch {
      return null;
    }
  }
  
  destroy() {
    this._destroyed = true;
    
    const rooms = Array.from(this.activeGames.keys());
    for (const room of rooms) {
      this.endGame(room);
    }
    this.activeGames.clear();
    
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    this.chatServer = null;
    this._errorLogs = [];
  }
}
