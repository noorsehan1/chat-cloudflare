// ============================
// LowCardGameManager
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGame = null;
  }

  // Router event
  handleEvent(ws, type, data) {
    switch (type) {
      case "gameLowCardStart":
        this.startGame(ws, data[0]);
        break;
      case "gameLowCardJoin":
        this.joinGame(ws);
        break;
      case "gameLowCardDraw":
        this.submitNumber(ws, data[0]);
        break;
    }
  }

  // ============================
  // START GAME
  // ============================
  startGame(ws, bet) {
    if (this.activeGame && this.activeGame.room === ws.roomname) return; // ❌ tidak bisa 2x start

    const betAmount = parseInt(bet, 10) || 0;

    this.activeGame = {
      room: ws.roomname,
      players: new Map(),
      registrationOpen: true,
      round: 1,
      numbers: new Map(),
      eliminated: new Set(),
      winner: null,
      betAmount,
      countdownTimers: [],
      registrationTime: 40,
      drawTime: 30
    };

    // ✅ host auto join
    this.activeGame.players.set(ws.idtarget, { id: ws.idtarget });

    // 🔔 broadcast start
    this.chatServer.broadcastToRoom(ws.roomname, [
      "gameLowCardStart",
      `Game is starting!\nType .ij to join in ${this.activeGame.registrationTime}s.\nBet: ${betAmount} Starting!`,
      ws.idtarget
    ]);

    this.startRegistrationCountdown();
  }

  // ============================
  // JOIN
  // ============================
  joinGame(ws) {
    if (!this.activeGame || !this.activeGame.registrationOpen) return;
    if (this.activeGame.players.has(ws.idtarget)) return; // ❌ sudah join → silent

    this.activeGame.players.set(ws.idtarget, { id: ws.idtarget });

    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardJoin",
      ws.idtarget
    ]);
  }

  // ============================
  // DRAW
  // ============================
  submitNumber(ws, number) {
    if (!this.activeGame) return;
    if (!this.activeGame.players.has(ws.idtarget)) return;
    if (!this.activeGame.registrationOpen && this.activeGame.numbers.has(ws.idtarget)) return; // ❌ sudah draw

    this.activeGame.numbers.set(ws.idtarget, number);

    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardPlayerDraw",
      ws.idtarget,
      number
    ]);
  }

  // ============================
  // REGISTRATION COUNTDOWN
  // ============================
  startRegistrationCountdown() {
    let timeLeft = this.activeGame.registrationTime;

    const interval = setInterval(() => {
      timeLeft--;

      if (timeLeft > 0) {
        this.chatServer.broadcastToRoom(this.activeGame.room, [
          "gameLowCardCountdownJoin",
          timeLeft
        ]);
      } else {
        clearInterval(interval);
        this.activeGame.countdownTimers =
          this.activeGame.countdownTimers.filter(t => t !== interval);

        // 🔔 Timer Over
        this.chatServer.broadcastToRoom(this.activeGame.room, [
          "gameLowCardCountdownJoin",
          "Timer Over"
        ]);

        this.endRegistration();
      }
    }, 1000);

    this.activeGame.countdownTimers.push(interval);
  }

  // ============================
  // END REGISTRATION
  // ============================
  endRegistration() {
    this.activeGame.registrationOpen = false;

    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardRegistrationClosed"
    ]);

    // 🔔 Round 1 mulai
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardRoundStart",
      this.activeGame.round
    ]);

    this.startDrawCountdown();
  }

  // ============================
  // DRAW COUNTDOWN
  // ============================
  startDrawCountdown() {
    let timeLeft = this.activeGame.drawTime;

    const interval = setInterval(() => {
      timeLeft--;

      if (timeLeft > 0) {
        this.chatServer.broadcastToRoom(this.activeGame.room, [
          "gameLowCardCountdownDraw",
          timeLeft
        ]);
      } else {
        clearInterval(interval);
        this.activeGame.countdownTimers =
          this.activeGame.countdownTimers.filter(t => t !== interval);

        // 🔔 Timer Over
        this.chatServer.broadcastToRoom(this.activeGame.room, [
          "gameLowCardCountdownDraw",
          "Timer Over"
        ]);

        this.evaluateRound();
      }
    }, 1000);

    this.activeGame.countdownTimers.push(interval);
  }

  // ============================
  // EVALUATE ROUND
  // ============================
  evaluateRound() {
    const numbers = Array.from(this.activeGame.numbers.entries());

    if (numbers.length === 0) {
      this.chatServer.broadcastToRoom(this.activeGame.room, [
        "gameLowCardNoDraws"
      ]);
      return this.endGame();
    }

    // cari nilai terendah
    let min = Infinity;
    let losers = [];

    for (const [playerId, number] of numbers) {
      if (number < min) {
        min = number;
        losers = [playerId];
      } else if (number === min) {
        losers.push(playerId);
      }
    }

    losers.forEach(id => this.activeGame.eliminated.add(id));

    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardRoundResult",
      this.activeGame.round,
      losers,
      min
    ]);

    const activePlayers = Array.from(this.activeGame.players.keys()).filter(
      id => !this.activeGame.eliminated.has(id)
    );

    if (activePlayers.length <= 1) {
      const winner = activePlayers[0] || null;
      this.chatServer.broadcastToRoom(this.activeGame.room, [
        "gameLowCardWinner",
        winner
      ]);
      return this.endGame();
    }

    // reset untuk round berikutnya
    this.activeGame.round++;
    this.activeGame.numbers.clear();

    // 🔔 Notif round baru
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardRoundStart",
      this.activeGame.round
    ]);

    this.startDrawCountdown();
  }

  // ============================
  // END GAME
  // ============================
  endGame() {
    this.clearAllTimers();
    this.activeGame = null;
  }

  clearAllTimers() {
    if (!this.activeGame) return;
    this.activeGame.countdownTimers.forEach(clearInterval);
    this.activeGame.countdownTimers = [];
  }
}
