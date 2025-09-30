// ============================
// LowCardGameManager (Sinkron ChatServer, Multi-room)
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map(); // key: room, value: game state
  }

  handleEvent(ws, data) {
    const evt = data[0];
    switch (evt) {
      case "gameLowCardStart":
        this.startGame(ws, data[1]);
        break;
       // return;

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
      game.countdownTimers.forEach(clearTimeout);
      game.countdownTimers = [];
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
      hostName: ws.username || ws.idtarget
    };

    // Host auto join
    game.players.set(ws.idtarget, { id: ws.idtarget });

    this.activeGames.set(room, game);

    // Broadcast ke semua orang di room
    this.chatServer.broadcastToRoom(room, [
  "gameLowCardStart",
  game.betAmount
]);

   // --- Event private ke host ---
// Kirim langsung hostName dan betAmount sebagai elemen array
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

  const timesToNotify = [30, 20, 10, 5, 0];
  timesToNotify.forEach(t => {
    const delay = (game.registrationTime - t) * 1000;
    const timer = setTimeout(() => {
      if (!this.activeGames.has(room)) return;

      if (t === 0) {
        this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
        this.closeRegistration(room);
      } else {
        this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${t}s`]);
      }

    }, delay);
    game.countdownTimers.push(timer);
  });
}

  joinGame(ws) {
    const room = ws.roomname;
    const game = this.getGame(room);
    if (!game || !game.registrationOpen) return;
    if (game.players.has(ws.idtarget)) return;

    // Simpan player
    game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });

    // Broadcast hanya nama + bet
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
      return;
    }

    game.registrationOpen = false;
    this.chatServer.broadcastToRoom(room, ["gameLowCardClosed", Array.from(game.players.keys())]);
    this.startDrawCountdown(room);
  }

startDrawCountdown(room) {
  const game = this.getGame(room);
  if (!game) return;
  this.clearAllTimers(game);

  const timesToNotify = [20, 10, 5, 0];
  timesToNotify.forEach(t => {
    const delay = (game.drawTime - t) * 1000;
    const timer = setTimeout(() => {
      if (!this.activeGames.has(room)) return;

      if (t === 0) {
        this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
        this.evaluateRound(room);
      } else {
        this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", `${t}s`]);
      }

    }, delay);
    game.countdownTimers.push(timer);
  });
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

    const { numbers, players, eliminated, round, betAmount } = game;
    const entries = Array.from(numbers.entries());

    if (entries.length === 0) {
      this.chatServer.broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
      this.activeGames.delete(room);
      return;
    }

    if (entries.length === 1) {
      const winnerId = entries[0][0];
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerId, totalCoin]);
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
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      this.chatServer.broadcastToRoom(room, ["gameLowCardWinner", winnerId, totalCoin]);
      this.activeGames.delete(room);
      return;
    }

    const numbersArr = entries.map(([id, n]) => `${id}:${n}`);
    this.chatServer.broadcastToRoom(room, ["gameLowCardRoundResult", round, numbersArr, losers, remaining]);

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
    this.activeGames.delete(room);
  }
}



