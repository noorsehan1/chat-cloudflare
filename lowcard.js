export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map(); // key: room, value: game state
  }

  handleEvent(ws, data) {
    const evt = data[0];
    switch (evt) {
      case "gameLowCardStart": this.startGame(ws, data[1]); break;
      case "gameLowCardJoin": this.joinGame(ws); break;
      case "gameLowCardNumber": this.submitNumber(ws, data[1], data[2] || ""); break;
      case "gameLowCardEnd": this.endGame(ws.roomname); break;
    }
  }

  getGame(room) {
    return this.activeGames.get(room);
  }

  clearGameInterval(game) {
    if (game.intervalId) {
      clearInterval(game.intervalId);
      game.intervalId = null;
    }
  }

  startGame(ws, bet) {
    const room = ws.roomname;
    if (!room || this.activeGames.has(room)) return;

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
      hostId: ws.idtarget,
      hostName: ws.username || ws.idtarget,
      countdown: 0,
      intervalId: null,
      phase: "registration", // registration/draw
      registrationTime: 40,
      drawTime: 30
    };

    game.players.set(ws.idtarget, { id: ws.idtarget });
    this.activeGames.set(room, game);

    // Broadcast start
    this.chatServer.broadcastToRoom(room, ["gameLowCardStart", betAmount]);
    this.chatServer.safeSend(ws, ["gameLowCardStartSuccess", game.hostName, betAmount]);

    // Start registration countdown
    this.startRoomCountdown(game);
  }

  startRoomCountdown(game) {
    this.clearGameInterval(game);

    if (game.phase === "registration") game.countdown = game.registrationTime;
    else if (game.phase === "draw") game.countdown = game.drawTime;

    game.intervalId = setInterval(() => {
      if (!this.activeGames.has(game.room)) {
        this.clearGameInterval(game);
        return;
      }

      if (game.countdown <= 0) {
        this.chatServer.broadcastToRoom(game.room, ["gameLowCardTimeLeft", "TIME UP!"]);

        if (game.phase === "registration") this.closeRegistration(game.room);
        else if (game.phase === "draw") this.evaluateRound(game.room);

        this.clearGameInterval(game);
      } else {
        this.chatServer.broadcastToRoom(game.room, ["gameLowCardTimeLeft", `${game.countdown}s`]);
        game.countdown--;
      }
    }, 1000);
  }

  joinGame(ws) {
    const room = ws.roomname;
    const game = this.getGame(room);
    if (!game || !game.registrationOpen) return;
    if (game.players.has(ws.idtarget)) return;

    game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });
    this.chatServer.broadcastToRoom(room, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
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
    const playersList = Array.from(game.players.keys());
    this.chatServer.broadcastToRoom(room, ["gameLowCardClosed", playersList]);
    this.chatServer.broadcastToRoom(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);

    // Mulai ronde 1
    game.phase = "draw";
    this.chatServer.broadcastToRoom(room, ["gameLowCardNextRound", 1]);
    this.startRoomCountdown(game);
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

    if (game.numbers.size === game.players.size - game.eliminated.size) this.evaluateRound(room);
  }

  evaluateRound(room) {
    const game = this.getGame(room);
    if (!game) return;
    this.clearGameInterval(game);

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
    game.phase = "draw";
    this.startRoomCountdown(game);
  }

  endGame(room) {
    const game = this.getGame(room);
    if (!game) return;
    this.chatServer.broadcastToRoom(room, ["gameLowCardEnd", Array.from(game.players.keys())]);
    this.clearGameInterval(game);
    this.activeGames.delete(room);
  }
}
