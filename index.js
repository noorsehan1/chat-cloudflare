// ============================
// LowCardGameManager (Sinkron ChatServer, Multi-room)
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map(); // key: room, value: game state
    this.bots = new Map(); // key: room, value: array of bot timers
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
  }

  clearBotTimers(room) {
    const botTimers = this.bots.get(room);
    if (botTimers) {
      botTimers.forEach(clearTimeout);
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

  startGame(ws, bet) {
    const room = ws.roomname;
    if (!room) return;
    if (this.activeGames.has(room)) return; // game sudah berjalan

    const betAmount = parseInt(bet, 10) || 0;

    const game = {
      room,
      players: new Map(),
      botPlayers: new Map(), // untuk tracking bot
      registrationOpen: true,
      round: 1,
      numbers: new Map(),
      eliminated: new Set(),
      winner: null,
      betAmount,
      countdownTimers: [],
      registrationTime: 40,
      drawTime: 30,
      hostId: ws.idtarget,
      hostName: ws.username || ws.idtarget,
      useBots: false,
      botDrawTriggered: false // flag untuk memastikan bot sudah draw di awal
    };

    // Host auto join
    game.players.set(ws.idtarget, { id: ws.idtarget });

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
    this.clearAllTimers(game);

    let timeLeft = game.registrationTime;
    const timesToNotify = [30, 20, 10, 0];

    const interval = setInterval(() => {
      if (!this.activeGames.has(room)) {
        clearInterval(interval);
        return;
      }

      if (timesToNotify.includes(timeLeft)) {
        if (timeLeft === 0) {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          
          // Cek apakah perlu menggunakan bot
          if (game.players.size < 2) {
            this.addBots(room);
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

  addBots(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    const neededBots = 4 - (game.players.size - 1); // -1 untuk host yang sudah ada
    if (neededBots <= 0) return;
    
    game.useBots = true;
    this.bots.set(room, []);
    
    for (let i = 0; i < neededBots; i++) {
      const botId = `BOT_${room}_${i}`;
      const botName = `Bot${i+1}`;
      
      // Simulasi bot join
      game.players.set(botId, { 
        id: botId, 
        name: botName, 
        isBot: true 
      });
      game.botPlayers.set(botId, botName);
      
      // Broadcast bot join
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
    this.clearAllTimers(game);
    this.clearBotTimers(room);
    
    // Reset bot draw flag setiap round baru
    game.botDrawTriggered = false;

    let timeLeft = game.drawTime;
    const timesToNotify = [20, 10, 0];

    const interval = setInterval(() => {
      if (!this.activeGames.has(room)) {
        clearInterval(interval);
        return;
      }

      if (timesToNotify.includes(timeLeft)) {
        if (timeLeft === 0) {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          this.evaluateRound(room);
          clearInterval(interval);
        } else {
          this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
      }

      // Bot auto draw di awal (saat waktu masih 29 detik)
      if (game.useBots && !game.botDrawTriggered && timeLeft === 29) {
        this.triggerAllBotDraws(room);
        game.botDrawTriggered = true;
      }

      timeLeft--;
      if (timeLeft < 0) clearInterval(interval);
    }, 1000);

    game.countdownTimers.push(interval);
  }

  triggerAllBotDraws(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    const activePlayers = Array.from(game.players.keys()).filter(id => 
      !game.eliminated.has(id) && !game.numbers.has(id)
    );
    
    const botPlayers = activePlayers.filter(id => id.startsWith('BOT_'));
    
    if (botPlayers.length === 0) return;
    
    // Trigger semua bot untuk draw dengan delay bertahap
    botPlayers.forEach((botId, index) => {
      const timer = setTimeout(() => {
        if (!this.activeGames.has(room) || 
            game.eliminated.has(botId) || 
            game.numbers.has(botId)) {
          return;
        }
        
        // Random number 1-12
        const botNumber = Math.floor(Math.random() * 12) + 1;
        
        // Random tanda C1-C4
        const tanda = this.getRandomCardTanda();
        
        // Simulasikan bot draw menggunakan event yang sama
        game.numbers.set(botId, botNumber);
        this.chatServer.broadcastToRoom(room, [
          "gameLowCardPlayerDraw",
          botId,
          botNumber,
          tanda
        ]);
        
        // Check if all have drawn
        if (game.numbers.size === game.players.size - game.eliminated.size) {
          this.evaluateRound(room);
        }
      }, index * 1000); // Delay 1 detik antar bot untuk efek bertahap
      
      this.bots.get(room)?.push(timer);
    });
  }

  joinGame(ws) {
    const room = ws.roomname;
    const game = this.getGame(room);
    if (!game || !game.registrationOpen) return;
    if (game.players.has(ws.idtarget)) return;

    game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });

    this.chatServer.broadcastToRoom(room, [
      "gameLowCardJoin",
      ws.username || ws.idtarget,
      game.betAmount
    ]);
  }

  closeRegistration(room) {
    const game = this.getGame(room);
    if (!game) return;

    const playerCount = game.players.size;
    if (playerCount < 2) {
      const onlyPlayer = playerCount === 1 ? Array.from(game.players.keys())[0] : null;

      if (onlyPlayer) {
        const hostSocket = Array.from(this.chatServer.clients)
          .find(ws => ws.idtarget === game.hostId);
        if (hostSocket) {
          this.chatServer.safeSend(hostSocket, ["gameLowCardNoJoin", game.hostName, game.betAmount]);
        }
      }

      this.chatServer.broadcastToRoom(room, ["gameLowCardError", "Need at least 2 players", onlyPlayer]);
      this.activeGames.delete(room);
      this.clearBotTimers(room);
      return;
    }

    game.registrationOpen = false;

    const playersList = Array.from(game.players.keys());

    this.chatServer.broadcastToRoom(room, ["gameLowCardClosed", playersList]);
    this.chatServer.broadcastToRoom(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", 1]);

    this.startDrawCountdown(room);
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
    this.chatServer.broadcastToRoom(room, ["gameLowCardPlayerDraw", ws.idtarget, n, tanda]);

    if (game.numbers.size === game.players.size - game.eliminated.size) {
      this.evaluateRound(room);
    }
  }

  evaluateRound(room) {
    const game = this.getGame(room);
    if (!game) return;
    this.clearAllTimers(game);
    this.clearBotTimers(room);

    const { numbers, players, eliminated, round, betAmount } = game;
    const entries = Array.from(numbers.entries());

    // --- Eliminasi otomatis yang tidak submit ---
    const submittedIds = new Set(numbers.keys());
    const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
    const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
    noSubmit.forEach(id => eliminated.add(id));

    if (entries.length === 0) {
      this.chatServer.broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
      this.activeGames.delete(room);
      this.clearBotTimers(room);
      return;
    }

    if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
      // hanya 1 orang yang draw → langsung pemenang
      const winnerId = entries[0][0];
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerId, totalCoin]);
      this.activeGames.delete(room);
      this.clearBotTimers(room);
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
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerId, totalCoin]);
      this.activeGames.delete(room);
      this.clearBotTimers(room);
      return;
    }

    const numbersArr = entries.map(([id, n]) => `${id}:${n}`);
    this.chatServer.broadcastToRoom(room, [
      "gameLowCardRoundResult",
      round,
      numbersArr,
      losers.concat(noSubmit), // kalah karena angka rendah + tidak draw
      remaining
    ]);

    numbers.clear();
    game.round++;
    this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
    this.startDrawCountdown(room);
  }

  endGame(room) {
    const game = this.getGame(room);
    if (!game) return;
    this.chatServer.broadcastToRoom(room, ["gameLowCardEnd", Array.from(game.players.keys())]);
    this.clearAllTimers(game);
    this.clearBotTimers(room);
    this.activeGames.delete(room);
  }
}
