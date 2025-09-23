// ============================
// LowCardGameManager
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGame = null; // Status game saat ini
  }

  // ===== Tangani semua event masuk =====
  handleEvent(ws, data) {
    const evt = data[0];
    switch (evt) {
      case "gameLowCardStart": this.startGame(ws); break;
      case "gameLowCardJoin": this.joinGame(ws); break;
      case "gameLowCardNumber": this.submitNumber(ws, data[1]); break;
      case "gameLowCardWinner": this.announceWinner(ws); break;
    }
  }

  // ===== Start Game =====
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
      timer: setTimeout(() => this.closeRegistration(), 20000), // 20 detik registrasi
      round: 1,
      numbers: new Map(),
      eliminated: new Set(),
      winner: null
    };

    // 🔔 Kirim event real-time ke semua user di room
    this.chatServer.broadcastToRoom(ws.roomname, ["gameLowCardStart", "Registration open for 20s"]);
  }

  // ===== Join Game =====
  joinGame(ws) {
    if (!this.activeGame || !this.activeGame.registrationOpen) {
      this.chatServer.safeSend(ws, ["gameLowCardError", "No open registration"]);
      return;
    }
    if (!ws.idtarget) return;
    if (this.activeGame.players.has(ws.idtarget)) return;

    this.activeGame.players.set(ws.idtarget, { id: ws.idtarget });

    // 🔔 Event real-time user join
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardJoin", ws.idtarget]);
  }

  // ===== Close Registration =====
  closeRegistration() {
    if (!this.activeGame) return;
    this.activeGame.registrationOpen = false;

    // 🔔 Event daftar pemain ditutup
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardClosed",
      Array.from(this.activeGame.players.keys())
    ]);
  }

  // ===== Submit Number =====
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

    // Simpan angka user
    this.activeGame.numbers.set(ws.idtarget, n);

    // Jika semua pemain submit, evaluasi ronde
    if (this.activeGame.numbers.size === this.activeGame.players.size - this.activeGame.eliminated.size) {
      this.evaluateRound();
    }
  }

  // ===== Evaluasi Round =====
  evaluateRound() {
    if (!this.activeGame) return;
    const { numbers, players, eliminated, round } = this.activeGame;

    const entries = Array.from(numbers.entries());
    if (entries.length === 0) return;

    // Cari angka terendah
    let lowest = Infinity;
    for (const [, n] of entries) if (n < lowest) lowest = n;

    // Tentukan yang kalah ronde ini
    const losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
    for (const id of losers) eliminated.add(id);

    // Sisa pemain
    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

    // 🔔 Event hasil ronde
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardRoundResult",
      round,
      JSON.stringify(Object.fromEntries(entries)), // numbersJson
      JSON.stringify(losers),                     // losersJson
      JSON.stringify(remaining)                   // remainingJson
    ]);

    numbers.clear();

    // Jika hanya tersisa 1 pemain, dia pemenang
    if (remaining.length <= 1) {
      this.activeGame.winner = remaining[0] || null;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardWinner", this.activeGame.winner]);
      this.activeGame = null;
    } else {
      // Next round
      this.activeGame.round++;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardNextRound", this.activeGame.round]);
    }
  }

  // ===== Announce Winner =====
  announceWinner(ws) {
    if (this.activeGame && this.activeGame.winner) {
      this.chatServer.safeSend(ws, ["gameLowCardWinner", this.activeGame.winner]);
    } else {
      this.chatServer.safeSend(ws, ["gameLowCardError", "No winner yet"]);
    }
  }
}
