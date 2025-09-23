// ============================
// LowCardGameManager
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGame = null; 
    this.defaultBetCoin = 1000; // coin per pemain
  }

  handleEvent(ws, data) {
    const evt = data[0];
    switch (evt) {
      case "gameLowCardStart": this.startGame(ws); break;
      case "gameLowCardJoin": this.joinGame(ws); break;
      case "gameLowCardNumber": this.submitNumber(ws, data[1]); break;
      default: break;
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
    if (!this.activeGame || !this.activeGame.registrationOpen) return;
    if (!ws.idtarget) return;
    if (this.activeGame.players.has(ws.idtarget)) return;

    this.activeGame.players.set(ws.idtarget, { id: ws.idtarget });
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardJoin", ws.idtarget]);
  }

  closeRegistration() {
    if (!this.activeGame) return;
    this.activeGame.registrationOpen = false;

    const playerList = Array.from(this.activeGame.players.keys());
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardClosed", playerList.join(",")]);
  }

  submitNumber(ws, number) {
    if (!this.activeGame || this.activeGame.registrationOpen) return;
    if (!this.activeGame.players.has(ws.idtarget)) return;
    if (this.activeGame.eliminated.has(ws.idtarget)) return;

    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 11) return;

    this.activeGame.numbers.set(ws.idtarget, n);

    if (this.activeGame.numbers.size === this.activeGame.players.size - this.activeGame.eliminated.size) {
      this.evaluateRound();
    }
  }

  evaluateRound() {
    if (!this.activeGame) return;
    const { numbers, players, eliminated, round } = this.activeGame;
    const entries = Array.from(numbers.entries());
    if (entries.length === 0) return;

    let lowest = Infinity;
    for (const [, n] of entries) if (n < lowest) lowest = n;

    const losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
    for (const id of losers) eliminated.add(id);

    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardRoundResult",
      String(round),
      JSON.stringify(Object.fromEntries(entries)),
      JSON.stringify(losers),
      JSON.stringify(remaining)
    ]);

    numbers.clear();

    if (remaining.length <= 1) {
      const winner = remaining[0] || null;
      this.activeGame.winner = winner;

      // Hitung total coin pemenang
      const totalCoin = players.size * this.defaultBetCoin;

      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardWinner", winner, totalCoin]);
      this.activeGame = null;
    } else {
      this.activeGame.round++;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardNextRound", String(this.activeGame.round)]);
    }
  }
}
