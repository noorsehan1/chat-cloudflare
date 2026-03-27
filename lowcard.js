// ============================
// LowCardGameManager (FULLY FIXED - NO MEMORY LEAK)
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._cleanupInterval = null;
    this._destroyed = false;
    
    // Error handler untuk prevent crash
    this._errorHandler = (error, context) => {
      console.error(`[LowCardGame] ${context}:`, error?.message || error);
    };
    
    // Auto cleanup setiap 5 menit
    this._cleanupInterval = setInterval(() => {
      if (!this._destroyed) this.cleanupStaleGames();
    }, 300000);
  }

  // ========== SAFE METHODS ==========
  _safeBroadcast(room, message) {
    try {
      if (this.chatServer && this.chatServer.broadcastToRoom) {
        this.chatServer.broadcastToRoom(room, message);
      }
    } catch (error) {
      this._errorHandler(error, `broadcast ${message?.[0] || 'unknown'}`);
    }
  }

  _safeGetGame(room) {
    try {
      if (this._destroyed) return null;
      return room ? this.activeGames.get(room) || null : null;
    } catch (error) {
      this._errorHandler(error, `getGame ${room}`);
      return null;
    }
  }

  cleanupStaleGames() {
    try {
      if (this._destroyed) return;
      const now = Date.now();
      for (const [room, game] of this.activeGames.entries()) {
        if (game && game._createdAt && (now - game._createdAt) > 3600000) {
          this.endGame(room);
        }
      }
    } catch (error) {
      this._errorHandler(error, 'cleanupStaleGames');
    }
  }

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
          if (ws?.roomname) this.endGame(ws.roomname);
          break;
      }
    } catch (error) {
      this._errorHandler(error, 'handleEvent');
    }
  }

  clearAllTimers(game) {
    try {
      if (!game) return;
      
      // Bersihkan registration interval
      if (game._regInterval) {
        clearInterval(game._regInterval);
        game._regInterval = null;
      }
      
      // Bersihkan draw interval
      if (game._drawInterval) {
        clearInterval(game._drawInterval);
        game._drawInterval = null;
      }
      
      // Bersihkan countdown timers
      if (game.countdownTimers && Array.isArray(game.countdownTimers)) {
        for (const timer of game.countdownTimers) {
          try {
            if (timer?.interval) clearInterval(timer.interval);
            if (timer?.timeout) clearTimeout(timer.timeout);
          } catch (e) {}
        }
        game.countdownTimers = [];
      }
      
      // Bersihkan bot timers
      if (game._botTimers && Array.isArray(game._botTimers)) {
        for (const timer of game._botTimers) {
          try { clearTimeout(timer); } catch (e) {}
        }
        game._botTimers = [];
      }
      
      // Bersihkan bot draw timeouts
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

  getGame(room) {
    try {
      if (this._destroyed) return null;
      return room ? this.activeGames.get(room) || null : null;
    } catch {
      return null;
    }
  }

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

  startGame(ws, bet) {
    try {
      if (this._destroyed || !ws?.roomname || !ws?.idtarget) return;

      const room = ws.roomname;
      
      if (this.activeGames.has(room)) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Game already running in this room"]);
        return;
      }

      const betAmount = parseInt(bet, 10) || 0;
      
      if (betAmount < 0) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Invalid bet amount"]);
        return;
      }
      
      if (betAmount !== 0 && betAmount < 100) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Bet must be 0 or at least 100"]);
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
        winner: null,
        betAmount,
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
        _regInterval: null,      // ✅ TAMBAHKAN
        _drawInterval: null      // ✅ TAMBAHKAN
      };

      game.players.set(ws.idtarget, { 
        id: ws.idtarget, 
        name: ws.username || ws.idtarget 
      });

      this.activeGames.set(room, game);

      this._safeBroadcast(room, ["gameLowCardStart", game.betAmount]);
      this.chatServer?.safeSend?.(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);

      this.startRegistrationCountdown(room);
    } catch (error) {
      this._errorHandler(error, 'startGame');
    }
  }

  startRegistrationCountdown(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      this.clearAllTimers(game);

      let timeLeft = game.registrationTime;
      const timesToNotify = [20, 10, 5, 0];

      // ✅ SIMPAN REFERENCE INTERVAL
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
              
              try {
                if (game.players.size === 1) {
                  this.addFourMozBots(room);
                }
              } catch (botError) {
                this._errorHandler(botError, 'addFourMozBots');
              }
              
              try {
                this.closeRegistration(room);
              } catch (closeError) {
                this._errorHandler(closeError, 'closeRegistration');
              }
              
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

  addFourMozBots(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      if (game.useBots || game.botPlayers.size > 0) return;
      
      game.useBots = true;
      
      const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
      
      for (let i = 0; i < 4; i++) {
        const botId = `BOT_MOZ_${room}_${i}_${Date.now()}_${Math.random()}`;
        const botName = mozNames[i];
        
        game.players.set(botId, { id: botId, name: botName });
        game.botPlayers.set(botId, botName);
        
        this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
      }
    } catch (error) {
      this._errorHandler(error, 'addFourMozBots');
    }
  }

  startDrawCountdown(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      this.clearAllTimers(game);
      game.evaluationLocked = false;
      game.drawTimeExpired = false;

      let timeLeft = game.drawTime;
      const timesToNotify = [20, 10, 5, 0];

      // ✅ SIMPAN REFERENCE INTERVAL
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
              
              const timeout = setTimeout(() => {
                try {
                  const currentGame = this._safeGetGame(room);
                  if (currentGame && currentGame._isActive && !this._destroyed) {
                    this.evaluateRound(room);
                  }
                } catch (evalError) {
                  this._errorHandler(evalError, 'evaluateRound timeout');
                }
              }, 2000);
              
              if (!game.countdownTimers) game.countdownTimers = [];
              game.countdownTimers.push({ timeout });
              
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

      if (game.useBots) {
        const activeBots = Array.from(game.botPlayers.keys())
          .filter(botId => !game.eliminated.has(botId) && !game.numbers.has(botId));
        
        for (const botId of activeBots) {
          const drawTime = this.getRandomDrawTime();
          
          const timeout = setTimeout(() => {
            try {
              const currentGame = this._safeGetGame(room);
              if (currentGame && currentGame._isActive && !currentGame.drawTimeExpired && !currentGame.evaluationLocked && !this._destroyed) {
                this.handleBotDraw(room, botId);
              }
            } catch (botError) {
              this._errorHandler(botError, `bot draw ${botId}`);
            }
          }, drawTime * 1000);
          
          if (!game._botTimers) game._botTimers = [];
          game._botTimers.push(timeout);
          if (!game._botDrawTimeouts) game._botDrawTimeouts = new Set();
          game._botDrawTimeouts.add(timeout);
          if (!game.countdownTimers) game.countdownTimers = [];
          game.countdownTimers.push({ timeout });
        }
      }
      
    } catch (error) {
      this._errorHandler(error, 'startDrawCountdown');
    }
  }

  handleBotDraw(room, botId) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
      if (game.drawTimeExpired || game.evaluationLocked) return;
      
      const botNumber = this.getBotNumberByRound(game.round);
      const tanda = this.getRandomCardTanda();
      
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
        
        const timeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this.evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound after bot draw');
          }
        }, 2000);
        
        if (!game.countdownTimers) game.countdownTimers = [];
        game.countdownTimers.push({ timeout });
      }
    } catch (error) {
      this._errorHandler(error, 'handleBotDraw');
    }
  }

  joinGame(ws) {
    try {
      if (this._destroyed || !ws?.roomname || !ws?.idtarget) return;

      const room = ws.roomname;
      const game = this._safeGetGame(room);
      
      if (!game || !game._isActive) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      
      if (game?.evaluationLocked) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Game in progress, please wait"]);
        return;
      }
      
      if (!game?.registrationOpen) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Registration closed or no game"]);
        return;
      }
      
      if (game.players.has(ws.idtarget)) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Already joined"]);
        return;
      }

      game.players.set(ws.idtarget, { 
        id: ws.idtarget, 
        name: ws.username || ws.idtarget 
      });

      this._safeBroadcast(room, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
    } catch (error) {
      this._errorHandler(error, 'joinGame');
    }
  }

  closeRegistration(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;

      const playerCount = game.players.size;
      
      if (playerCount < 2) {
        const hostSocket = Array.from(this.chatServer?.clients || [])
          .find(ws => ws?.idtarget === game.hostId);
        
        if (hostSocket) {
          this.chatServer?.safeSend?.(hostSocket, ["gameLowCardNoJoin", game.hostName, game.betAmount]);
        }

        this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players", game.hostId]);
        this.activeGames.delete(room);
        return;
      }

      game.registrationOpen = false;

      const playersList = Array.from(game.players.values()).map(p => p.name || p.id);

      this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
      this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
      this._safeBroadcast(room, ["gameLowCardNextRound", 1]);

      this.startDrawCountdown(room);
    } catch (error) {
      this._errorHandler(error, 'closeRegistration');
    }
  }

  submitNumber(ws, number, tanda = "") {
    try {
      if (this._destroyed || !ws?.roomname || !ws?.idtarget) return;

      const room = ws.roomname;
      const game = this._safeGetGame(room);
      
      if (!game || !game._isActive) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      
      if (game.evaluationLocked) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Please wait, results are being processed..."]);
        return;
      }
      
      if (game.registrationOpen) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Registration still open"]);
        return;
      }
      
      if (!game.players.has(ws.idtarget) || game.eliminated.has(ws.idtarget)) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Not in game or eliminated"]);
        return;
      }
      
      if (game.numbers.has(ws.idtarget)) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Already submitted number"]);
        return;
      }

      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (allDrawn) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "All players have already drawn, please wait for results..."]);
        return;
      }

      if (game.drawTimeExpired) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Draw time has expired!"]);
        return;
      }

      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        this.chatServer?.safeSend?.(ws, ["gameLowCardError", "Invalid number (1-12)"]);
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
        
        const timeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this.evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound after submit');
          }
        }, 2000);
        
        if (!game.countdownTimers) game.countdownTimers = [];
        game.countdownTimers.push({ timeout });
      }
    } catch (error) {
      this._errorHandler(error, 'submitNumber');
    }
  }

  evaluateRound(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      if (!game.players || game.players.size === 0) {
        this.activeGames.delete(room);
        return;
      }
      
      this.clearAllTimers(game);

      const { numbers, tanda, players, eliminated, round, betAmount } = game;
      
      if (!numbers || numbers.size === 0) {
        const remainingPlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
        if (remainingPlayers.length === 0) {
          this.activeGames.delete(room);
          return;
        }
        
        game.round++;
        game.evaluationLocked = false;
        game.drawTimeExpired = false;
        this.startDrawCountdown(room);
        return;
      }
      
      const entries = Array.from(numbers.entries());

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

      const values = Array.from(numbers.values());
      const allSame = values.every(v => v === values[0]);
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
        this.activeGames.delete(room);
        return;
      }

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

      numbers.clear();
      tanda.clear();
      game.round++;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      
      this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
      this.startDrawCountdown(room);
    } catch (error) {
      this._errorHandler(error, 'evaluateRound');
      try {
        this.activeGames.delete(room);
      } catch (e) {}
    }
  }

  endGame(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game) return;
      
      const playersList = Array.from(game.players?.values() || []).map(p => p.name || p.id);
      
      game._isActive = false;
      
      // Bersihkan semua timer
      this.clearAllTimers(game);
      
      // Bersihkan semua Map
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
      
      // Hapus references
      game.countdownTimers = null;
      game._botTimers = null;
      game._botDrawTimeouts = null;
      game._regInterval = null;
      game._drawInterval = null;
      
      if (this.chatServer && this.chatServer.broadcastToRoom) {
        try {
          this.chatServer.broadcastToRoom(room, ["gameLowCardEnd", playersList]);
        } catch (e) {}
      }
      
      this.activeGames.delete(room);
    } catch (error) {
      this._errorHandler(error, 'endGame');
    }
  }
  
  destroy() {
    this._destroyed = true;
    
    // End all active games
    for (const [room, game] of this.activeGames.entries()) {
      this.endGame(room);
    }
    this.activeGames.clear();
    
    // Bersihkan cleanup interval
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    this.chatServer = null;
  }
}
