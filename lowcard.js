// ============================
// LowCardGameManager (Optimized Version)
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map(); // key: room, value: game state
    this.bots = new Map(); // key: room, value: array of bot timers
    this.countdownIntervals = new Map(); // key: room, value: intervalId (SIMPAN interval ID saja)
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
    // Hapus interval countdown
    const intervalId = this.countdownIntervals.get(room);
    if (intervalId) {
      clearInterval(intervalId);
      this.countdownIntervals.delete(room);
    }
    
    // Hapus timer bot
    const botTimers = this.bots.get(room);
    if (botTimers) {
      botTimers.forEach(timer => clearTimeout(timer));
      this.bots.delete(room);
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
    // Random waktu antara 3-25 detik (kurangi kemungkinan mepet TIME UP)
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
      // **OPTIMASI: Simpan timestamp untuk tracking countdown**
      countdownEndTime: null,
      countdownType: null // 'registration' atau 'draw'
    };

    // Host auto join
    game.players.set(ws.idtarget, { 
      id: ws.idtarget, 
      name: ws.username || ws.idtarget 
    });

    this.activeGames.set(room, game);

    // Broadcast ke semua orang di room
    this.chatServer.broadcastToRoom(room, [
      "gameLowCardStart",
      game.betAmount
    ]);

    // Event private ke host
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
    game.countdownType = 'registration';
    game.countdownEndTime = Date.now() + (game.registrationTime * 1000);

    let lastBroadcastSecond = -1;
    const notifySeconds = [30, 20, 10, 5, 3, 2, 1, 0];

    const intervalId = setInterval(() => {
      const game = this.getGame(room);
      if (!game) {
        clearInterval(intervalId);
        this.countdownIntervals.delete(room);
        return;
      }

      const timeLeft = Math.max(0, Math.ceil((game.countdownEndTime - Date.now()) / 1000));
      
      // **OPTIMASI: Hanya broadcast pada detik tertentu**
      if (notifySeconds.includes(timeLeft) && timeLeft !== lastBroadcastSecond) {
        lastBroadcastSecond = timeLeft;
        
        if (timeLeft === 0) {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          
          if (game.players.size === 1) {
            this.addFourMozBots(room);
          }
          
          this.closeRegistration(room);
          clearInterval(intervalId);
          this.countdownIntervals.delete(room);
        } else {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
      }
      
      // Jika waktu habis, bersihkan interval
      if (timeLeft <= 0) {
        clearInterval(intervalId);
        this.countdownIntervals.delete(room);
      }
    }, 500); // **OPTIMASI: Check setiap 500ms, bukan 1000ms**

    this.countdownIntervals.set(room, intervalId);
  }

  addFourMozBots(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    game.useBots = true;
    this.bots.set(room, []);
    
    const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
    
    for (let i = 0; i < 4; i++) {
      const botId = `BOT_MOZ_${room}_${i}`;
      const botName = mozNames[i];
      
      game.players.set(botId, { 
        id: botId, 
        name: botName 
      });
      game.botPlayers.set(botId, botName);
      game.botDrawTimes.set(botId, this.getRandomDrawTime());
      
      this.chatServer.broadcastToRoom(room, [
        "gameLowCardJoin",
        botName,
        game.betAmount
      ]);
    }
  }

  startDrawCountdown(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    this.clearAllTimers(room);
    game.countdownType = 'draw';
    game.countdownEndTime = Date.now() + (game.drawTime * 1000);

    // Reset dan schedule timer bot
    if (game.useBots) {
      game.botDrawTimes.clear();
      
      // **OPTIMASI: Schedule semua bot sekaligus di awal**
      const botTimers = [];
      const activeBots = Array.from(game.botPlayers.keys())
        .filter(botId => !game.eliminated.has(botId) && !game.numbers.has(botId));
      
      activeBots.forEach(botId => {
        const drawTime = this.getRandomDrawTime();
        game.botDrawTimes.set(botId, drawTime);
        
        const timer = setTimeout(() => {
          this.handleBotDraw(room, botId);
        }, drawTime * 1000);
        
        botTimers.push(timer);
      });
      
      if (botTimers.length > 0) {
        this.bots.set(room, botTimers);
      }
    }

    let lastBroadcastSecond = -1;
    const notifySeconds = [20, 10, 5, 3, 2, 1, 0];

    const intervalId = setInterval(() => {
      const game = this.getGame(room);
      if (!game) {
        clearInterval(intervalId);
        this.countdownIntervals.delete(room);
        return;
      }

      const timeLeft = Math.max(0, Math.ceil((game.countdownEndTime - Date.now()) / 1000));
      
      // **OPTIMASI: Hanya broadcast pada detik tertentu**
      if (notifySeconds.includes(timeLeft) && timeLeft !== lastBroadcastSecond) {
        lastBroadcastSecond = timeLeft;
        
        if (timeLeft === 0) {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          this.evaluateRound(room);
          clearInterval(intervalId);
          this.countdownIntervals.delete(room);
        } else {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
      }
      
      // Jika waktu habis, bersihkan interval
      if (timeLeft <= 0) {
        clearInterval(intervalId);
        this.countdownIntervals.delete(room);
      }
    }, 500); // **OPTIMASI: Check setiap 500ms**

    this.countdownIntervals.set(room, intervalId);
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
    
    this.chatServer.broadcastToRoom(room, [
      "gameLowCardPlayerDraw",
      botName,
      botNumber,
      tanda
    ]);
    
    // Cek jika semua sudah draw
    if (game.numbers.size === game.players.size - game.eliminated.size) {
      this.evaluateRound(room);
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

    this.chatServer.broadcastToRoom(room, ["gameLowCardClosed", playersList]);
    this.chatServer.broadcastToRoom(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", 1]);

    // Schedule bot draw untuk round pertama
    if (game.useBots && game.round === 1) {
      game.botAlreadyDrawInFirstRound = true;
      
      const botTimers = [];
      Array.from(game.botPlayers.keys()).forEach(botId => {
        const drawTime = game.botDrawTimes.get(botId);
        if (drawTime) {
          const timer = setTimeout(() => {
            this.handleBotDraw(room, botId);
          }, drawTime * 1000);
          
          botTimers.push(timer);
        }
      });
      
      if (botTimers.length > 0) {
        this.bots.set(room, botTimers);
      }
    }

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

    // Cek jika semua sudah draw
    if (game.numbers.size === game.players.size - game.eliminated.size) {
      this.evaluateRound(room);
    } else if (game.round === 1 && game.useBots && !game.botAlreadyDrawInFirstRound) {
      // Schedule bot draw jika round pertama
      game.botAlreadyDrawInFirstRound = true;
      
      const botTimers = [];
      Array.from(game.botPlayers.keys()).forEach(botId => {
        if (!game.eliminated.has(botId) && !game.numbers.has(botId)) {
          const drawTime = this.getRandomDrawTime();
          game.botDrawTimes.set(botId, drawTime);
          
          const timer = setTimeout(() => {
            this.handleBotDraw(room, botId);
          }, drawTime * 1000);
          
          botTimers.push(timer);
        }
      });
      
      if (botTimers.length > 0) {
        const existingTimers = this.bots.get(room) || [];
        this.bots.set(room, [...existingTimers, ...botTimers]);
      }
    }
  }

  evaluateRound(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    this.clearAllTimers(room);

    const { numbers, tanda, players, eliminated, round, betAmount } = game;
    const entries = Array.from(numbers.entries());

    // Eliminasi otomatis yang tidak submit
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
      const winnerName = winnerPlayer ? winnerPlayer.name : winnerId;
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
      this.activeGames.delete(room);
      return;
    }

    // Format numbers array
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

    numbers.clear();
    tanda.clear();
    game.round++;
    
    if (game.round > 1) {
      game.botAlreadyDrawInFirstRound = false;
    }
    
    this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
    this.startDrawCountdown(room);
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
