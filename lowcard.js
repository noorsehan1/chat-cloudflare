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

  getRandomDrawTime() {
    // Random waktu antara 1-25 detik (agar tidak terlalu mepet ke TIME UP)
    return Math.floor(Math.random() * 25) + 1;
  }

  getRandomNumber() {
    // Random number 1-12
    return Math.floor(Math.random() * 12) + 1;
  }

  startGame(ws, bet) {
    const room = ws.roomname;
    if (!room) return;
    if (this.activeGames.has(room)) return; // game sudah berjalan

    const betAmount = parseInt(bet, 10) || 0;

    const game = {
      room,
      players: new Map(), // menyimpan info player: {id, name}
      botPlayers: new Map(), // untuk tracking bot: botId -> botName
      registrationOpen: true,
      round: 1,
      numbers: new Map(), // menyimpan angka yang sudah di-submit: playerId -> number
      tanda: new Map(), // menyimpan tanda yang sudah di-submit: playerId -> tanda
      eliminated: new Set(),
      winner: null,
      betAmount,
      countdownTimers: [],
      registrationTime: 40,
      drawTime: 30,
      hostId: ws.idtarget,
      hostName: ws.username || ws.idtarget,
      useBots: false,
      botDrawTimes: new Map(), // menyimpan waktu draw masing-masing bot
      botAlreadyDrawInFirstRound: false // flag khusus untuk round pertama
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
          
          // **LOGIKA: HANYA TAMBAH BOT JIKA HANYA HOST SAJA**
          if (game.players.size === 1) {
            // Hanya host yang join, tambah 4 bot moz
            this.addFourMozBots(room);
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

  // **MODIFIKASI: TAMBAH 4 BOT MOZ DENGAN NAMA YANG BENAR**
  addFourMozBots(room) {
    const game = this.getGame(room);
    if (!game) return;
    
    game.useBots = true;
    this.bots.set(room, []);
    
    // **BUAT 4 BOT MOZ (moz 1, moz 2, moz 3, moz 4)**
    const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
    
    for (let i = 0; i < 4; i++) {
      const botId = `BOT_MOZ_${room}_${i}`;
      const botName = mozNames[i];
      
      // Simulasi bot join
      game.players.set(botId, { 
        id: botId, 
        name: botName, 
        isBot: true 
      });
      game.botPlayers.set(botId, botName);
      
      // Generate waktu random untuk bot draw
      game.botDrawTimes.set(botId, this.getRandomDrawTime());
      
      // **BROADCAST BOT JOIN (seperti kode awal)**
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
    
    // Reset timer bot untuk round baru
    if (game.useBots && game.round > 1) {
      game.botDrawTimes.clear();
      Array.from(game.botPlayers.keys()).forEach(botId => {
        if (!game.eliminated.has(botId)) {
          game.botDrawTimes.set(botId, this.getRandomDrawTime());
        }
      });
    }

    let timeLeft = game.drawTime;
    const timesToNotify = [20, 10, 0];
    
    // Map untuk melacak bot yang sudah dijadwalkan
    const scheduledBots = new Set();

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

      // Cek apakah ada bot yang harus draw pada waktu ini
      if (game.useBots) {
        const activeBots = Array.from(game.botPlayers.keys()).filter(botId => 
          !game.eliminated.has(botId) && 
          !game.numbers.has(botId) &&
          !scheduledBots.has(botId)
        );
        
        activeBots.forEach(botId => {
          const drawTime = game.botDrawTimes.get(botId);
          // Bot draw jika waktu tersisa sama dengan waktu draw yang ditentukan
          if (drawTime && timeLeft === (game.drawTime - drawTime)) {
            scheduledBots.add(botId);
            
            const timer = setTimeout(() => {
              if (!this.activeGames.has(room) || 
                  game.eliminated.has(botId) || 
                  game.numbers.has(botId)) {
                return;
              }
              
              // **BOT INPUT: (number random 1-12, tanda random C1-C4)**
              const botNumber = this.getRandomNumber(); // Random 1-12
              const tanda = this.getRandomCardTanda(); // Random C1-C4
              
              // Simpan data
              game.numbers.set(botId, botNumber);
              game.tanda.set(botId, tanda);
              
              // Dapatkan nama bot dari game.players
              const botPlayer = game.players.get(botId);
              const botName = botPlayer ? botPlayer.name : botId;
              
              // Broadcast dengan format yang benar
              this.chatServer.broadcastToRoom(room, [
                "gameLowCardPlayerDraw",
                botName,
                botNumber,
                tanda
              ]);
              
              // Check if all have drawn
              if (game.numbers.size === game.players.size - game.eliminated.size) {
                this.evaluateRound(room);
              }
            }, 0);
            
            this.bots.get(room)?.push(timer);
          }
        });
      }

      timeLeft--;
      if (timeLeft < 0) clearInterval(interval);
    }, 1000);

    game.countdownTimers.push(interval);
  }

  closeRegistration(room) {
    const game = this.getGame(room);
    if (!game) return;

    const playerCount = game.players.size;
    
    // **LOGIKA BARU: CEK JUMLAH PEMAIN MINIMAL**
    if (playerCount < 2) {
      // Hanya host saja (tidak ada user lain join dan tidak ada bot)
      const hostSocket = Array.from(this.chatServer.clients)
        .find(ws => ws.idtarget === game.hostId);
      if (hostSocket) {
        this.chatServer.safeSend(hostSocket, ["gameLowCardNoJoin", game.hostName, game.betAmount]);
      }

      this.chatServer.broadcastToRoom(room, ["gameLowCardError", "Need at least 2 players", game.hostId]);
      this.activeGames.delete(room);
      this.clearBotTimers(room);
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
      
      Array.from(game.botPlayers.keys()).forEach(botId => {
        const drawTime = game.botDrawTimes.get(botId);
        if (drawTime) {
          const timer = setTimeout(() => {
            if (!this.activeGames.has(room) || 
                game.eliminated.has(botId) || 
                game.numbers.has(botId)) {
              return;
            }
            
            // **BOT INPUT: (number random 1-12, tanda random C1-C4)**
            const botNumber = this.getRandomNumber(); // Random 1-12
            const tanda = this.getRandomCardTanda(); // Random C1-C4
            
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
            
            if (game.numbers.size === game.players.size - game.eliminated.size) {
              this.evaluateRound(room);
            }
          }, drawTime * 1000);
          
          this.bots.get(room)?.push(timer);
        }
      });
    }

    this.startDrawCountdown(room);
  }

  joinGame(ws) {
    const room = ws.roomname;
    const game = this.getGame(room);
    if (!game || !game.registrationOpen) return;
    if (game.players.has(ws.idtarget)) return;

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
    if (!game || game.registrationOpen) return;
    if (!game.players.has(ws.idtarget) || game.eliminated.has(ws.idtarget)) return;
    if (game.numbers.has(ws.idtarget)) return;

    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 12) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Invalid number"]);
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
      // Jika round pertama dan bot belum draw, schedule bot draw dengan waktu random
      game.botAlreadyDrawInFirstRound = true;
      
      // Schedule draw untuk 4 bot moz
      Array.from(game.botPlayers.keys()).forEach(botId => {
        if (!game.eliminated.has(botId) && !game.numbers.has(botId)) {
          const drawTime = this.getRandomDrawTime();
          game.botDrawTimes.set(botId, drawTime);
          
          const timer = setTimeout(() => {
            if (!this.activeGames.has(room) || 
                game.eliminated.has(botId) || 
                game.numbers.has(botId)) {
              return;
            }
            
            // **BOT INPUT: (number random 1-12, tanda random C1-C4)**
            const botNumber = this.getRandomNumber(); // Random 1-12
            const tanda = this.getRandomCardTanda(); // Random C1-C4
            
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
            
            if (game.numbers.size === game.players.size - game.eliminated.size) {
              this.evaluateRound(room);
            }
          }, drawTime * 1000);
          
          this.bots.get(room)?.push(timer);
        }
      });
    }
  }

  evaluateRound(room) {
    const game = this.getGame(room);
    if (!game) return;
    this.clearAllTimers(game);
    this.clearBotTimers(room);

    const { numbers, tanda, players, eliminated, round, betAmount } = game;
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
      const winnerPlayer = players.get(winnerId);
      const winnerName = winnerPlayer ? winnerPlayer.name : winnerId;
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
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
      const winnerPlayer = players.get(winnerId);
      const winnerName = winnerPlayer ? winnerPlayer.name : winnerId;
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
      this.activeGames.delete(room);
      this.clearBotTimers(room);
      return;
    }

    // Format numbers array dengan format: "nama:angka(tanda)"
    const numbersArr = entries.map(([id, n]) => {
      const player = players.get(id);
      const playerName = player ? player.name : id;
      const playerTanda = tanda.get(id) || "";
      return `${playerName}:${n}(${playerTanda})`;
    });
    
    // Format losers dengan nama
    const loserNames = losers.concat(noSubmit).map(id => {
      const player = players.get(id);
      return player ? player.name : id;
    });
    
    // Format remaining dengan nama
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
    
    // Reset flag bot draw untuk round pertama
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
    this.clearAllTimers(game);
    this.clearBotTimers(room);
    this.activeGames.delete(room);
  }
}
