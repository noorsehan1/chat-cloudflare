// ============================
// LowCardGameManager (Fixed - No Memory Leak)
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._cleanupInterval = null;
    this._destroyed = false;
    
    // Auto cleanup setiap 5 menit
    if (!this._destroyed) {
      this._cleanupInterval = setInterval(() => {
        if (!this._destroyed) this.cleanupStaleGames();
      }, 300000);
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
    } catch (error) {}
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
    } catch (error) {}
  }

  clearAllTimers(game) {
    try {
      if (!game) return;
      
      if (game.countdownTimers && Array.isArray(game.countdownTimers)) {
        game.countdownTimers.forEach(timer => {
          try {
            if (timer?.interval) clearInterval(timer.interval);
            if (timer?.timeout) clearTimeout(timer.timeout);
          } catch (e) {}
        });
        game.countdownTimers = [];
      }
      
      if (game._botTimers) {
        game._botTimers.forEach(timer => {
          try { clearTimeout(timer); } catch (e) {}
        });
        game._botTimers = [];
      }
      
    } catch (error) {}
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
        registrationTime: 25,
        drawTime: 30,
        hostId: ws.idtarget,
        hostName: ws.username || ws.idtarget,
        useBots: false,
        evaluationLocked: false,
        drawTimeExpired: false,
        _createdAt: Date.now(),
        _isActive: true
      };

      game.players.set(ws.idtarget, { 
        id: ws.idtarget, 
        name: ws.username || ws.idtarget 
      });

      this.activeGames.set(room, game);

      this.chatServer?.broadcastToRoom?.(room, ["gameLowCardStart", game.betAmount]);
      this.chatServer?.safeSend?.(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);

      this.startRegistrationCountdown(room);
    } catch (error) {}
  }

  startRegistrationCountdown(room) {
    try {
      const game = this.getGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      this.clearAllTimers(game);

      let timeLeft = game.registrationTime;
      const timesToNotify = [20, 10, 5, 0];

      const interval = setInterval(() => {
        try {
          if (this._destroyed || !this.activeGames.has(room) || !game._isActive) {
            clearInterval(interval);
            return;
          }

          if (timesToNotify.includes(timeLeft)) {
            if (timeLeft === 0) {
              this.chatServer?.broadcastToRoom?.(room, ["gameLowCardTimeLeft", "TIME UP!"]);
              
              if (game.players.size === 1) {
                this.addFourMozBots(room);
              }
              
              this.closeRegistration(room);
              clearInterval(interval);
            } else {
              this.chatServer?.broadcastToRoom?.(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
            }
          }

          timeLeft--;
          if (timeLeft < 0) clearInterval(interval);
        } catch {
          clearInterval(interval);
        }
      }, 1000);

      game.countdownTimers.push({ interval });
    } catch {}
  }

  addFourMozBots(room) {
    try {
      const game = this.getGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      game.useBots = true;
      
      const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
      
      for (let i = 0; i < 4; i++) {
        const botId = `BOT_MOZ_${room}_${i}_${Date.now()}`;
        const botName = mozNames[i];
        
        game.players.set(botId, { id: botId, name: botName });
        game.botPlayers.set(botId, botName);
        
        this.chatServer?.broadcastToRoom?.(room, ["gameLowCardJoin", botName, game.betAmount]);
      }
    } catch {}
  }

  startDrawCountdown(room) {
    try {
      const game = this.getGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      this.clearAllTimers(game);
      game.evaluationLocked = false;
      game.drawTimeExpired = false;

      let timeLeft = game.drawTime;
      const timesToNotify = [20, 10, 5, 0];

      const interval = setInterval(() => {
        try {
          if (this._destroyed || !this.activeGames.has(room) || !game._isActive) {
            clearInterval(interval);
            return;
          }

          if (timesToNotify.includes(timeLeft)) {
            if (timeLeft === 0) {
              this.chatServer?.broadcastToRoom?.(room, ["gameLowCardTimeLeft", "TIME UP!"]);
              
              game.drawTimeExpired = true;
              
              const activePlayers = Array.from(game.players.keys())
                .filter(id => !game.eliminated.has(id));
              const allDrawn = game.numbers.size === activePlayers.length;
              
              if (!allDrawn) {
                this.chatServer?.broadcastToRoom?.(room, ["gameLowCardInfo", "Time is up, processing current draws..."]);
              }
              
              game.evaluationLocked = true;
              this.chatServer?.broadcastToRoom?.(room, ["gameLowCardWait", "Please wait for results..."]);
              
              const timeout = setTimeout(() => {
                try {
                  const currentGame = this.getGame(room);
                  if (currentGame && currentGame._isActive && !this._destroyed) {
                    this.evaluateRound(room);
                  }
                } catch {}
              }, 2000);
              
              game.countdownTimers.push({ timeout });
              clearInterval(interval);
            } else {
              this.chatServer?.broadcastToRoom?.(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
            }
          }

          timeLeft--;
          if (timeLeft < 0) clearInterval(interval);
        } catch {
          clearInterval(interval);
        }
      }, 1000);

      game.countdownTimers.push({ interval });

      if (game.useBots) {
        const activeBots = Array.from(game.botPlayers.keys())
          .filter(botId => !game.eliminated.has(botId) && !game.numbers.has(botId));
        
        activeBots.forEach(botId => {
          const drawTime = this.getRandomDrawTime();
          
          const timeout = setTimeout(() => {
            try {
              const currentGame = this.getGame(room);
              if (currentGame && currentGame._isActive && !currentGame.drawTimeExpired && !currentGame.evaluationLocked && !this._destroyed) {
                this.handleBotDraw(room, botId);
              }
            } catch {}
          }, drawTime * 1000);
          
          game._botTimers.push(timeout);
          game.countdownTimers.push({ timeout });
        });
      }
    } catch {}
  }

  handleBotDraw(room, botId) {
    try {
      const game = this.getGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
      if (game.drawTimeExpired || game.evaluationLocked) return;
      
      const botNumber = this.getBotNumberByRound(game.round);
      const tanda = this.getRandomCardTanda();
      
      game.numbers.set(botId, botNumber);
      game.tanda.set(botId, tanda);
      
      const botPlayer = game.players.get(botId);
      const botName = botPlayer?.name || botId;
      
      this.chatServer?.broadcastToRoom?.(room, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
      
      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (!game.evaluationLocked && allDrawn) {
        game.evaluationLocked = true;
        this.chatServer?.broadcastToRoom?.(room, ["gameLowCardWait", "Please wait for results..."]);
        
        const timeout = setTimeout(() => {
          try {
            const currentGame = this.getGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this.evaluateRound(room);
            }
          } catch {}
        }, 2000);
        
        game.countdownTimers.push({ timeout });
      }
    } catch {}
  }

  joinGame(ws) {
    try {
      if (this._destroyed || !ws?.roomname || !ws?.idtarget) return;

      const room = ws.roomname;
      const game = this.getGame(room);
      
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

      this.chatServer?.broadcastToRoom?.(room, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
    } catch {}
  }

  closeRegistration(room) {
    try {
      const game = this.getGame(room);
      if (!game || !game._isActive || this._destroyed) return;

      const playerCount = game.players.size;
      
      if (playerCount < 2) {
        const hostSocket = Array.from(this.chatServer?.clients || [])
          .find(ws => ws?.idtarget === game.hostId);
        
        if (hostSocket) {
          this.chatServer?.safeSend?.(hostSocket, ["gameLowCardNoJoin", game.hostName, game.betAmount]);
        }

        this.chatServer?.broadcastToRoom?.(room, ["gameLowCardError", "Need at least 2 players", game.hostId]);
        this.activeGames.delete(room);
        return;
      }

      game.registrationOpen = false;

      const playersList = Array.from(game.players.values()).map(p => p.name || p.id);

      this.chatServer?.broadcastToRoom?.(room, ["gameLowCardClosed", playersList]);
      this.chatServer?.broadcastToRoom?.(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
      this.chatServer?.broadcastToRoom?.(room, ["gameLowCardNextRound", 1]);

      this.startDrawCountdown(room);
    } catch {}
  }

  submitNumber(ws, number, tanda = "") {
    try {
      if (this._destroyed || !ws?.roomname || !ws?.idtarget) return;

      const room = ws.roomname;
      const game = this.getGame(room);
      
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
      
      this.chatServer?.broadcastToRoom?.(room, ["gameLowCardPlayerDraw", playerName, n, tanda]);

      const newActivePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const nowAllDrawn = game.numbers.size === newActivePlayers.length;
      
      if (!game.evaluationLocked && nowAllDrawn) {
        game.evaluationLocked = true;
        this.chatServer?.broadcastToRoom?.(room, ["gameLowCardWait", "Please wait for results..."]);
        
        const timeout = setTimeout(() => {
          try {
            const currentGame = this.getGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this.evaluateRound(room);
            }
          } catch {}
        }, 2000);
        
        game.countdownTimers.push({ timeout });
      }
    } catch {}
  }

  evaluateRound(room) {
    try {
      const game = this.getGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      // Safety: jika tidak ada players, hapus game
      if (!game.players || game.players.size === 0) {
        this.activeGames.delete(room);
        return;
      }
      
      this.clearAllTimers(game);

      const { numbers, tanda, players, eliminated, round, betAmount } = game;
      
      // Safety: jika tidak ada numbers, lanjutkan ke round berikutnya
      if (!numbers || numbers.size === 0) {
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
        this.chatServer?.broadcastToRoom?.(room, ["gameLowCardError", "No numbers drawn this round"]);
        this.activeGames.delete(room);
        return;
      }

      if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
        const winnerId = entries[0][0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        game.winner = winnerId;
        
        this.chatServer?.broadcastToRoom?.(room, ["gameLowCardWinner", winnerName, totalCoin]);
        this.activeGames.delete(room);
        return;
      }

      const values = Array.from(numbers.values());
      const allSame = values.every(v => v === values[0]);
      let losers = [];

      if (!allSame) {
        const lowest = Math.min(...values);
        losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
        losers.forEach(id => eliminated.add(id));
      }

      const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

      if (remaining.length === 1) {
        const winnerId = remaining[0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        game.winner = winnerId;
        
        this.chatServer?.broadcastToRoom?.(room, ["gameLowCardWinner", winnerName, totalCoin]);
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
      
      const remainingNames = remaining.map(id => {
        const player = players.get(id);
        return player?.name || id;
      });

      this.chatServer?.broadcastToRoom?.(room, [
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
      
      this.chatServer?.broadcastToRoom?.(room, ["gameLowCardNextRound", game.round]);
      this.startDrawCountdown(room);
    } catch (error) {
      // Safety: jika error, hapus game
      try {
        this.activeGames.delete(room);
      } catch (e) {}
    }
  }

  endGame(room) {
    try {
      const game = this.getGame(room);
      if (!game) return;
      
      // Mark game as inactive first
      game._isActive = false;
      
      // Clear all timers
      this.clearAllTimers(game);
      
      // Clear all maps and sets
      if (game.players) game.players.clear();
      if (game.botPlayers) game.botPlayers.clear();
      if (game.numbers) game.numbers.clear();
      if (game.tanda) game.tanda.clear();
      if (game.eliminated) game.eliminated.clear();
      
      const playersList = Array.from(game.players?.values() || []).map(p => p.name || p.id);
      
      if (this.chatServer && this.chatServer.broadcastToRoom) {
        try {
          this.chatServer.broadcastToRoom(room, ["gameLowCardEnd", playersList]);
        } catch (e) {}
      }
      
      // Delete from active games
      this.activeGames.delete(room);
    } catch (error) {}
  }
  
  destroy() {
    this._destroyed = true;
    
    // Cleanup all games
    for (const [room, game] of this.activeGames.entries()) {
      this.endGame(room);
    }
    this.activeGames.clear();
    
    // Clear cleanup interval
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    // Clear reference to chatServer
    this.chatServer = null;
  }
}
