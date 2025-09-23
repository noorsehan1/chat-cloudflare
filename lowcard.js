// ============================
// LowCardGameManager
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGame = null;
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
        this.submitNumber(ws, data[1]);
        break;
    }
  }

  startGame(ws, betAmount) {
    if (!ws.roomname || !ws.idtarget) return;
    if (this.activeGame && this.activeGame.registrationOpen) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Game registration already open"]);
      return;
    }

    const bet = parseInt(betAmount, 10) || 0;

    this.activeGame = {
      room: ws.roomname,
      players: new Map(),
      registrationOpen: true,
      round: 1,
      numbers: new Map(),
      eliminated: new Set(),
      winner: null,
      betAmount: bet,
      countdownTimers: []
    };

    this.startCountdown(ws.roomname);

    this.chatServer.broadcastToRoom(ws.roomname, [
      "gameLowCardStart",
      `Registration open 20s. Bet: ${bet}`
    ]);
  }

  startCountdown(room) {
    const times = [20, 10, 5, 0];
    this.activeGame.countdownTimers = times.map((t, i) =>
      setTimeout(() => {
        this.chatServer.broadcastToRoom(room, ["gameLowCardTimeLeft", t]);
        if (t === 0) this.closeRegistration();
      }, i === 0 ? 0 : (20 - t) * 1000)
    );
  }

  joinGame(ws) {
    if (!this.activeGame || !this.activeGame.registrationOpen) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "No open registration"]);
      return;
    }
    if (!ws.idtarget || this.activeGame.players.has(ws.idtarget)) return;

    this.activeGame.players.set(ws.idtarget, { id: ws.idtarget });
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardJoin", ws.idtarget]);
  }

  closeRegistration() {
    if (!this.activeGame) return;
    const playerCount = this.activeGame.players.size;

    if (playerCount < 2) {
      const onlyPlayer = playerCount === 1 ? Array.from(this.activeGame.players.keys())[0] : null;
      this.chatServer.broadcastToRoom(this.activeGame.room, [
        "gameLowCardError",
        "Need at least 2 players to start the game",
        onlyPlayer
      ]);
      this.activeGame = null;
      return;
    }

    this.activeGame.registrationOpen = false;
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardClosed",
      Array.from(this.activeGame.players.keys())
    ]);
  }

  submitNumber(ws, number) {
    if (!this.activeGame || this.activeGame.registrationOpen) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Game not active"]);
      return;
    }
    if (!this.activeGame.players.has(ws.idtarget) || this.activeGame.eliminated.has(ws.idtarget)) return;

    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 11) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Invalid number"]);
      return;
    }

    this.activeGame.numbers.set(ws.idtarget, n);
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardPlayerDraw", ws.idtarget, n]);

    if (this.activeGame.numbers.size === this.activeGame.players.size - this.activeGame.eliminated.size) {
      this.evaluateRound();
    }
  }

  evaluateRound() {
    if (!this.activeGame) return;
    const { numbers, players, eliminated, round, betAmount } = this.activeGame;

    const entries = Array.from(numbers.entries());

    // Jika tidak ada pemain yang draw angka → game dibatalkan
    if (entries.length === 0) {
      this.activeGame = null;
      this.chatServer.broadcastToRoom(this.activeGame.room, [
        "gameLowCardError",
        "No numbers drawn this round"
      ]);
      return;
    }

    // Ambil semua angka ronde ini
    const values = Array.from(numbers.values());
    const allSame = values.every(v => v === values[0]);

    let losers = [];

    if (!allSame) {
      // Cari angka terendah
      const lowest = Math.min(...values);
      // Eliminasi pemain dengan angka terendah
      losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
      for (const id of losers) eliminated.add(id);
    }

    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

    // Jika tersisa 1 pemain → otomatis jadi pemenang
    if (remaining.length === 1) {
      const winnerId = remaining[0];
      const totalCoin = betAmount * players.size;
      this.activeGame.winner = winnerId;
      this.chatServer.broadcastToRoom(this.activeGame.room, [
        "gameLowCardWinner",
        winnerId,
        totalCoin
      ]);
      this.activeGame = null;
      return;
    }

    // Broadcast ronde result
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardRoundResult",
      round,
      JSON.stringify(Object.fromEntries(entries)),
      JSON.stringify(losers),
      JSON.stringify(remaining)
    ]);

    // Clear numbers untuk ronde berikutnya
    numbers.clear();
    this.activeGame.round++;
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardNextRound", this.activeGame.round]);
  }
}
