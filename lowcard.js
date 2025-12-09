// ============================
// LowCardGameManager (Fixed Timer Management)
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this.countdownIntervals = new Map();
  }

  handleEvent(ws, data) {
    const evt = data[0];
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
        this.endGame(ws.roomname);
        break;
    }
  }

  clearAllTimers(room) {
    // Clear countdown interval
    const intervalId = this.countdownIntervals.get(room);
    if (intervalId) {
      clearInterval(intervalId);
      this.countdownIntervals.delete(room);
    }
    
    // Clear any bot timeouts
    const game = this.activeGames.get(room);
    if (game && game.botTimeouts) {
      game.botTimeouts.forEach(timeout => clearTimeout(timeout));
      game.botTimeouts = [];
    }
  }

  getGame(room) {
    return this.activeGames.get(room);
  }

  getRandomCardTanda() {
    const tandaOptions = ["C1", "C2", "C3", "C4"];
    return tandaOptions[Math.floor(Math.random() * tandaOptions.length)];
  }

  getRandomDrawTime() {
    return Math.floor(Math.random() * 23) + 3;
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

  startGame(ws, bet) {
    const room = ws.roomname;
    if (!room) return;
    
    if (this.activeGames.has(room)) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Game already running in this room"]);
      return;
    }

    const betAmount = parseInt(bet, 10) || 0;
    if (betAmount <= 0) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Invalid bet amount"]);
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
      registrationTime: 40,
      drawTime: 30,
      hostId: ws.idtarget,
      hostName: ws.username || ws.idtarget,
      useBots: false,
      botDrawTimes: new Map(),
      botAlreadyDrawInFirstRound: false,
      botTimeouts: [],
      countdownEndTime: null,
      countdownType: null,
      broadcastedSeconds: new Set() // Track already broadcasted seconds
    };

    // Host auto join
    game.players.set(ws.idtarget, { 
      id: ws.idtarget, 
      name: ws.username || ws.idtarget 
    });

    this.activeGames.set(room, game);

    // Broadcast to room
    this.chatServer.broadcastToRoom(room, [
      "gameLowCardStart",
      game.betAmount
    ]);

    // Private event to host
    this.chatServer.safeSend(ws, [
      "gameLowCardStartSuccess",
      game.hostName,
      game.betAmount
    ]);

    this.startRegistrationCountdown(room);
  }

  startRegistrationCountdown(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    this.clearAllTimers(room);
    game.broadcastedSeconds.clear();
    
    game.countdownType = 'registration';
    game.countdownEndTime = Date.now() + (game.registrationTime * 1000);
    
    const notifySeconds = [30, 20, 10];
    let lastProcessedSecond = -1;
    
    const tick = () => {
      const game = this.getGame(room);
      if (!game) {
        this.clearAllTimers(room);
        return;
      }
      
      const remainingMs = game.countdownEndTime - Date.now();
      const timeLeft = Math.max(0, Math.ceil(remainingMs / 1000));
      
      // Only process if we've moved to a new second
      if (timeLeft !== lastProcessedSecond) {
        lastProcessedSecond = timeLeft;
        
        if (timeLeft === 0) {
          // Time's up
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          
          if (game.players.size === 1) {
            this.addFourMozBots(room);
          }
          
          this.closeRegistration(room);
          this.clearAllTimers(room);
          return;
        }
        
        // Check if we should broadcast this second
        if (notifySeconds.includes(timeLeft) && !game.broadcastedSeconds.has(timeLeft)) {
          game.broadcastedSeconds.add(timeLeft);
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
      }
      
      // If time is up, stop the interval
      if (remainingMs <= 0) {
        this.clearAllTimers(room);
        return;
      }
    };
    
    // Start the interval
    const intervalId = setInterval(tick, 100); // Check every 100ms for better accuracy
    this.countdownIntervals.set(room, intervalId);
    
    // Do an initial tick
    tick();
  }

  addFourMozBots(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    game.useBots = true;
    game.botTimeouts = [];
    
    const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
    
    for (let i = 0; i < 4; i++) {
      const botId = `BOT_MOZ_${room}_${i}`;
      const botName = mozNames[i];
      
      game.players.set(botId, { 
        id: botId, 
        name: botName 
      });
      game.botPlayers.set(botId, botName);
      
      // Stagger bot join messages
      setTimeout(() => {
        this.chatServer.broadcastToRoom(room, [
          "gameLowCardJoin",
          botName,
          game.betAmount
        ]);
      }, i * 100);
    }
  }

  startDrawCountdown(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    this.clearAllTimers(room);
    game.broadcastedSeconds.clear();
    
    game.countdownType = 'draw';
    game.countdownEndTime = Date.now() + (game.drawTime * 1000);
    
    // Schedule bot draws
    if (game.useBots) {
      this.scheduleBotDraws(room);
    }
    
    const notifySeconds = [20, 10];
    let lastProcessedSecond = -1;
    
    const tick = () => {
      const game = this.getGame(room);
      if (!game) {
        this.clearAllTimers(room);
        return;
      }
      
      const remainingMs = game.countdownEndTime - Date.now();
      const timeLeft = Math.max(0, Math.ceil(remainingMs / 1000));
      
      // Only process if we've moved to a new second
      if (timeLeft !== lastProcessedSecond) {
        lastProcessedSecond = timeLeft;
        
        if (timeLeft === 0) {
          // Time's up
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          this.evaluateRound(room);
          this.clearAllTimers(room);
          return;
        }
        
        // Check if we should broadcast this second
        if (notifySeconds.includes(timeLeft) && !game.broadcastedSeconds.has(timeLeft)) {
          game.broadcastedSeconds.add(timeLeft);
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
      }
      
      // If time is up, stop the interval
      if (remainingMs <= 0) {
        this.clearAllTimers(room);
        return;
      }
    };
    
    // Start the interval
    const intervalId = setInterval(tick, 100);
    this.countdownIntervals.set(room, intervalId);
    
    // Do an initial tick
    tick();
  }

  scheduleBotDraws(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    // Clear any existing bot timeouts
    if (game.botTimeouts) {
      game.botTimeouts.forEach(timeout => clearTimeout(timeout));
      game.botTimeouts = [];
    }
    
    // Schedule draws for active bots
    const activeBots = Array.from(game.botPlayers.keys())
      .filter(botId => !game.eliminated.has(botId) && !game.numbers.has(botId));
    
    activeBots.forEach(botId => {
      const drawTime = this.getRandomDrawTime();
      game.botDrawTimes.set(botId, drawTime);
      
      const timeout = setTimeout(() => {
        this.handleBotDraw(room, botId);
      }, drawTime * 1000);
      
      game.botTimeouts.push(timeout);
    });
  }

  handleBotDraw(room, botId) {
    const game = this.getGame(room);
    if (!game || game.eliminated.has(botId) || game.numbers.has(botId)) {
      return;
    }
    
    const botNumber = this.getBotNumberByRound(game.round);
    const tanda = this.getRandomCardTanda();
    
    game.numbers.set(botId, botNumber);
    game.tanda.set(botId, tanda);
    
    const botPlayer = game.players.get(botId);
    const botName = botPlayer ? botPlayer.name : botId;
    
    // Broadcast bot draw
    this.chatServer.broadcastToRoom(room, [
      "gameLowCardPlayerDraw",
      botName,
      botNumber,
      tanda
    ]);
    
    // Check if all players have drawn
    if (game.numbers.size === game.players.size - game.eliminated.size) {
      // Small delay to ensure all draws are processed
      setTimeout(() => this.evaluateRound(room), 300);
    }
  }

  closeRegistration(room) {
    const game = this.getGame(room);
    if (!game) return;

    const playerCount = game.players.size;
    
    if (playerCount < 2) {
      const hostSocket = Array.from(this.chatServer.clients)
        .find(ws => ws.idtarget === game.hostId);
      if (hostSocket) {
        this.chatServer.safeSend(hostSocket, ["gameLowCardNoJoin", game.hostName, game.betAmount]);
      }

      this.chatServer.broadcastToRoom(room, ["gameLowCardError", "Need at least 2 players", game.hostId]);
      this.activeGames.delete(room);
      this.clearAllTimers(room);
      return;
    }

    game.registrationOpen = false;
    const playersList = Array.from(game.players.values()).map(p => p.name || p.id);

    // Broadcast game info
    this.chatServer.broadcastToRoom(room, ["gameLowCardClosed", playersList]);
    this.chatServer.broadcastToRoom(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", 1]);

    this.startDrawCountdown(room);
  }

  joinGame(ws) {
    const room = ws.roomname;
    const game = this.getGame(room);
    if (!game || !game.registrationOpen) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Registration closed or no game"]);
      return;
    }
    if (game.players.has(ws.idtarget)) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Already joined"]);
      return;
    }

    game.players.set(ws.idtarget, { 
      id: ws.idtarget, 
      name: ws.username || ws.idtarget 
    });

    this.chatServer.broadcastToRoom(room, [
      "gameLowCardJoin",
      ws.username || ws.idtarget,
      game.betAmount
    ]);
  }

  submitNumber(ws, number, tanda = "") {
    const room = ws.roomname;
    const game = this.getGame(room);
    if (!game) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "No active game"]);
      return;
    }
    
    if (game.registrationOpen) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Registration still open"]);
      return;
    }
    
    if (!game.players.has(ws.idtarget) || game.eliminated.has(ws.idtarget)) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Not in game or eliminated"]);
      return;
    }
    
    if (game.numbers.has(ws.idtarget)) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Already submitted number"]);
      return;
    }

    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 12) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
      return;
    }

    game.numbers.set(ws.idtarget, n);
    game.tanda.set(ws.idtarget, tanda);
    
    const player = game.players.get(ws.idtarget);
    const playerName = player ? player.name : ws.idtarget;
    
    this.chatServer.broadcastToRoom(room, [
      "gameLowCardPlayerDraw",
      playerName,
      n,
      tanda
    ]);

    // Check if all players have drawn
    if (game.numbers.size === game.players.size - game.eliminated.size) {
      setTimeout(() => this.evaluateRound(room), 300);
    }
  }

  evaluateRound(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    this.clearAllTimers(room);

    const { numbers, tanda, players, eliminated, round, betAmount } = game;
    const entries = Array.from(numbers.entries());

    // Auto-eliminate players who didn't submit
    const submittedIds = new Set(numbers.keys());
    const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
    const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
    noSubmit.forEach(id => eliminated.add(id));

    if (entries.length === 0) {
      this.chatServer.broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
      this.activeGames.delete(room);
      return;
    }

    if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
      const winnerId = entries[0][0];
      const winnerPlayer = players.get(winnerId);
      const winnerName = winnerPlayer ? winnerPlayer.name : winnerId;
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
      
      setTimeout(() => {
        this.activeGames.delete(room);
      }, 1000);
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
      const winnerName = winnerPlayer ? winnerPlayer.name : winnerId;
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
      
      setTimeout(() => {
        this.activeGames.delete(room);
      }, 1000);
      return;
    }

    // Format numbers array for display
    const numbersArr = entries.map(([id, n]) => {
      const player = players.get(id);
      const playerName = player ? player.name : id;
      const playerTanda = tanda.get(id) || "";
      return `${playerName}:${n}(${playerTanda})`;
    });
    
    const loserNames = losers.concat(noSubmit).map(id => {
      const player = players.get(id);
      return player ? player.name : id;
    });
    
    const remainingNames = remaining.map(id => {
      const player = players.get(id);
      return player ? player.name : id;
    });

    this.chatServer.broadcastToRoom(room, [
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
    
    // Announce next round
    setTimeout(() => {
      this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
    }, 1000);
    
    // Start next round countdown
    setTimeout(() => {
      this.startDrawCountdown(room);
    }, 1500);
  }

  endGame(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    const playersList = Array.from(game.players.values()).map(p => p.name || p.id);
    
    this.chatServer.broadcastToRoom(room, ["gameLowCardEnd", playersList]);
    this.clearAllTimers(room);
    this.activeGames.delete(room);
  }
}
