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
      case "gameLowCardStart": this.startGame(ws, data[1]); break;
      case "gameLowCardJoin": this.joinGame(ws); break;
      case "gameLowCardNumber": this.submitNumber(ws, data[1]); break;
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
      countdownTimers: [] // untuk countdown 20→10→5
    };

    // Start countdown sequence
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
    if (entries.length === 0) return;

    let lowest = Infinity;
    for (const [, n] of entries) if (n < lowest) lowest = n;

    const losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
    for (const id of losers) eliminated.add(id);

    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardRoundResult",
      round,
      JSON.stringify(Object.fromEntries(entries)),
      JSON.stringify(losers),
      JSON.stringify(remaining)
    ]);

    numbers.clear();

    if (remaining.length <= 1) {
      const winnerId = remaining[0] || null;
      const totalCoin = betAmount * players.size;
      this.activeGame.winner = winnerId;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardWinner", winnerId, totalCoin]);
      this.activeGame = null;
    } else {
      this.activeGame.round++;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardNextRound", this.activeGame.round]);
    }
  }
}
