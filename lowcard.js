// ============================
// LowCardGameManager with AUTO-BOT
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map(); // key: room, value: game state
    this.botNames = ["AlphaBot", "BetaBot", "GammaBot", "DeltaBot"];
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

  clearAllTimers(game) {
    if (game?.countdownTimers) {
      game.countdownTimers.forEach(clearInterval);
      game.countdownTimers = [];
    }
    
    if (game?.botTimers) {
      game.botTimers.forEach(clearTimeout);
      game.botTimers = [];
    }
  }

  getGame(room) {
    return this.activeGames.get(room);
  }

  startGame(ws, bet) {
    const room = ws.roomname;
    if (!room) return;
    if (this.activeGames.has(room)) return; // game sudah berjalan

    const betAmount = parseInt(bet, 10) || 0;

    const game = {
      room,
      players: new Map(),
      botPlayers: new Map(),
      registrationOpen: true,
      round: 1,
      numbers: new Map(),
      eliminated: new Set(),
      winner: null,
      betAmount,
      countdownTimers: [],
      botTimers: [],
      registrationTime: 10, // Cuma 10 detik tunggu
      drawTime: 30,
      hostId: ws.idtarget,
      hostName: ws.username || ws.idtarget,
      useBots: false
    };

    // Host auto join
    game.players.set(ws.idtarget, { 
      id: ws.idtarget,
      name: ws.username || ws.idtarget,
      isBot: false 
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

    // Langsung mulai countdown registrasi singkat
    this.startRegistrationCountdown(room);
  }

  startRegistrationCountdown(room) {
    const game = this.getGame(room);
    if (!game) return;
    this.clearAllTimers(game);

    let timeLeft = game.registrationTime;
    const timesToNotify = [5, 3, 2, 1, 0];

    const interval = setInterval(() => {
      if (!this.activeGames.has(room)) {
        clearInterval(interval);
        return;
      }

      if (timesToNotify.includes(timeLeft)) {
        if (timeLeft === 0) {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          
          // TAMBAH BOT OTOMATIS JIKA HANYA HOST SAJA
          if (game.players.size === 1) {
            // Tambah 3 bot otomatis
            this.addBotsToGame(game, 3);
            game.useBots = true;
          }
          
          this.closeRegistration(room);
          clearInterval(interval);
        } else {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
      }

      timeLeft--;
      if (timeLeft < 0) clearInterval(interval);
    }, 1000);

    game.countdownTimers.push(interval);
  }

  addBotsToGame(game, count) {
    for (let i = 0; i < count; i++) {
      const botId = `BOT_${Date.now()}_${i}`;
      const botName = this.botNames[i];
      
      game.botPlayers.set(botId, {
        id: botId,
        name: botName,
        isBot: true
      });

      game.players.set(botId, {
        id: botId,
        name: botName,
        isBot: true
      });

      // Broadcast bot join seperti player biasa
      setTimeout(() => {
        if (this.activeGames.has(game.room)) {
          this.chatServer.broadcastToRoom(game.room, [
            "gameLowCardJoin",
            botName,
            game.betAmount
          ]);
        }
      }, 500 * (i + 1));
    }
  }

  startDrawCountdown(room) {
    const game = this.getGame(room);
    if (!game) return;
    this.clearAllTimers(game);

    let timeLeft = game.drawTime;
    const timesToNotify = [20, 10, 5, 0];

    const interval = setInterval(() => {
      if (!this.activeGames.has(room)) {
        clearInterval(interval);
        return;
      }

      if (timesToNotify.includes(timeLeft)) {
        if (timeLeft === 0) {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          this.makeBotsSubmitNumbers(room);
          setTimeout(() => this.evaluateRound(room), 1000);
          clearInterval(interval);
        } else {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
      }

      timeLeft--;
      if (timeLeft < 0) clearInterval(interval);
    }, 1000);

    game.countdownTimers.push(interval);
  }

  makeBotsSubmitNumbers(room) {
    const game = this.getGame(room);
    if (!game || !game.useBots) return;

    const activePlayers = Array.from(game.players.keys())
      .filter(id => !game.eliminated.has(id) && !game.numbers.has(id));

    activePlayers.forEach((playerId, index) => {
      const player = game.players.get(playerId);
      if (player.isBot) {
        const delay = this.getRandomInt(1000, 5000);
        
        const botTimer = setTimeout(() => {
          if (!this.activeGames.has(room) || 
              !game.players.has(playerId) || 
              game.eliminated.has(playerId) || 
              game.numbers.has(playerId)) {
            return;
          }

          const number = this.getRandomInt(1, 12);
          this.botSubmitNumber(room, playerId, number);
        }, delay);

        game.botTimers.push(botTimer);
      }
    });
  }

  botSubmitNumber(room, botId, number) {
    const game = this.getGame(room);
    if (!game || game.numbers.has(botId)) return;

    const bot = game.players.get(botId);
    const tanda = this.getRandomTanda();
    
    game.numbers.set(botId, number);
    
    this.chatServer.broadcastToRoom(room, [
      "gameLowCardPlayerDraw",
      bot.name,
      number,
      tanda
    ]);

    // Check if all have submitted
    const remainingPlayers = Array.from(game.players.keys())
      .filter(id => !game.eliminated.has(id));
    
    if (game.numbers.size === remainingPlayers.length) {
      setTimeout(() => this.evaluateRound(room), 1000);
    }
  }

  getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  getRandomTanda() {
    const tandaList = ["", "✓", "⚡", "🎯"];
    return Math.random() > 0.5 ? tandaList[this.getRandomInt(0, tandaList.length-1)] : "";
  }

  joinGame(ws) {
    const room = ws.roomname;
    const game = this.getGame(room);
    if (!game || !game.registrationOpen) return;
    if (game.players.has(ws.idtarget)) return;

    game.players.set(ws.idtarget, { 
      id: ws.idtarget, 
      name: ws.username || ws.idtarget,
      isBot: false 
    });

    this.chatServer.broadcastToRoom(room, [
      "gameLowCardJoin",
      ws.username || ws.idtarget,
      game.betAmount
    ]);
  }

  closeRegistration(room) {
    const game = this.getGame(room);
    if (!game) return;

    game.registrationOpen = false;

    const playersList = Array.from(game.players.values()).map(p => p.name);

    this.chatServer.broadcastToRoom(room, ["gameLowCardClosed", playersList]);
    this.chatServer.broadcastToRoom(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    
    setTimeout(() => {
      if (this.activeGames.has(room)) {
        this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", 1]);
        this.startDrawCountdown(room);
      }
    }, 2000);
  }

  submitNumber(ws, number, tanda = "") {
    const room = ws.roomname;
    const game = this.getGame(room);
    if (!game || game.registrationOpen) return;
    if (!game.players.has(ws.idtarget) || game.eliminated.has(ws.idtarget)) return;
    if (game.numbers.has(ws.idtarget)) return;

    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 12) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Invalid number"]);
      return;
    }

    game.numbers.set(ws.idtarget, n);
    this.chatServer.broadcastToRoom(room, [
      "gameLowCardPlayerDraw",
      ws.username || ws.idtarget,
      n,
      tanda
    ]);

    const remainingPlayers = Array.from(game.players.keys())
      .filter(id => !game.eliminated.has(id));
    
    if (game.numbers.size === remainingPlayers.length) {
      setTimeout(() => this.evaluateRound(room), 1000);
    }
  }

  evaluateRound(room) {
    const game = this.getGame(room);
    if (!game) return;
    this.clearAllTimers(game);

    const { numbers, players, eliminated, round, betAmount } = game;
    const entries = Array.from(numbers.entries());

    // Eliminasi otomatis yang tidak submit
    const submittedIds = new Set(numbers.keys());
    const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
    const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
    noSubmit.forEach(id => eliminated.add(id));

    if (entries.length === 0) {
      this.chatServer.broadcastToRoom(room, ["gameLowCardError", "No numbers drawn"]);
      this.activeGames.delete(room);
      return;
    }

    if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
      // hanya 1 orang yang draw → langsung pemenang
      const winnerId = entries[0][0];
      const winner = players.get(winnerId);
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winner.name, totalCoin]);
      this.endGameWithDelay(room);
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

    // sisa pemain
    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

    if (remaining.length === 1) {
      const winnerId = remaining[0];
      const winner = players.get(winnerId);
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winner.name, totalCoin]);
      this.endGameWithDelay(room);
      return;
    }

    const numbersArr = entries.map(([id, n]) => {
      const player = players.get(id);
      return `${player.name}:${n}`;
    });
    
    const loserNames = losers.concat(noSubmit).map(id => {
      const player = players.get(id);
      return player?.name || id;
    });
    
    const remainingNames = remaining.map(id => {
      const player = players.get(id);
      return player?.name || id;
    });

    this.chatServer.broadcastToRoom(room, [
      "gameLowCardRoundResult",
      round,
      numbersArr,
      loserNames,
      remainingNames
    ]);

    numbers.clear();
    game.round++;
    
    setTimeout(() => {
      if (this.activeGames.has(room)) {
        this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
        this.startDrawCountdown(room);
      }
    }, 3000);
  }

  endGameWithDelay(room) {
    setTimeout(() => {
      this.endGame(room);
    }, 3000);
  }

  endGame(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    const playerNames = Array.from(game.players.values()).map(p => p.name);
    this.chatServer.broadcastToRoom(room, ["gameLowCardEnd", playerNames]);
    
    this.clearAllTimers(game);
    this.activeGames.delete(room);
  }
}
