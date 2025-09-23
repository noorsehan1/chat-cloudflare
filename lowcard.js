export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGame = null;
  }

  handleEvent(ws, data) {
    const evt = data[0];
    switch (evt) {
      case "gameLowCardStart": this.startGame(ws); break;
      case "gameLowCardJoin": this.joinGame(ws); break;
      case "gameLowCardNumber": this.submitNumber(ws, data[1]); break;
      case "gameLowCardWinner": this.announceWinner(ws); break;
    }
  }

  startGame(ws) {
    if (!ws.roomname || !ws.idtarget) return;
    if (this.activeGame && this.activeGame.registrationOpen) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Game registration already open"]);
      return;
    }

    this.activeGame = {
      room: ws.roomname,
      players: new Map(),
      registrationOpen: true,
      timer: setTimeout(() => this.closeRegistration(), 20000),
      round: 1,
      numbers: new Map(),
      eliminated: new Set(),
      winner: null
    };

    this.chatServer.broadcastToRoom(ws.roomname, ["gameLowCardStart", "Registration open for 20s"]);
  }

  joinGame(ws) {
    if (!this.activeGame || !this.activeGame.registrationOpen) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "No open registration"]);
      return;
    }
    if (!ws.idtarget) return;
    if (this.activeGame.players.has(ws.idtarget)) return;

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
    if (!this.activeGame.players.has(ws.idtarget)) return;
    if (this.activeGame.eliminated.has(ws.idtarget)) return;

    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 11) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "Invalid number"]);
      return;
    }

    this.activeGame.numbers.set(ws.idtarget, n);

    // 🔹 Real-time broadcast setiap submit
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardNumberSubmitted",
      ws.idtarget,
      n
    ]);

    // 🔹 Jika semua tersisa sudah submit, evaluasi ronde
    if (this.activeGame.numbers.size === this.activeGame.players.size - this.activeGame.eliminated.size) {
      this.evaluateRound();
    }
  }

  evaluateRound() {
    if (!this.activeGame) return;
    const { numbers, players, eliminated, round } = this.activeGame;

    const entries = Array.from(numbers.entries());
    if (entries.length === 0) return;

    let lowest = Math.min(...entries.map(([_, n]) => n));
    const losers = entries.filter(([_, n]) => n === lowest).map(([id]) => id);

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
      this.activeGame.winner = remaining[0] || null;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardWinner", this.activeGame.winner]);
      this.activeGame = null;
    } else {
      this.activeGame.round++;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardNextRound", this.activeGame.round]);
    }
  }

  announceWinner(ws) {
    if (this.activeGame && this.activeGame.winner) {
      this.chatServer.safeSend(ws, ["gameLowCardWinner", this.activeGame.winner]);
    } else {
      this.chatServer.safeSend(ws, ["gameLowCardError", "No winner yet"]);
    }
  }
}
