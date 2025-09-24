// ============================
// LowCardGameManager (Sinkron ChatServer)
// ============================
export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGame = null;
  }

  handleEvent(ws, data) {
    const evt = data[0];
    switch(evt){
      case "gameLowCardStart": this.startGame(ws, data[1]); break;
      case "gameLowCardJoin": this.joinGame(ws); break;
      case "gameLowCardNumber": this.submitNumber(ws, data[1], data[2] || ""); break;
      case "gameLowCardEnd": this.endGame(); break;
    }
  }

  clearAllTimers() {
    if (this.activeGame?.countdownTimers) {
      this.activeGame.countdownTimers.forEach(clearTimeout);
      this.activeGame.countdownTimers = [];
    }
  }

  startGame(ws, betAmount){
    if(!ws.roomname || !ws.idtarget) return;
    const bet = parseInt(betAmount,10)||0;

    this.clearAllTimers();

    this.activeGame = {
      room: ws.roomname,
      players: new Map(),
      registrationOpen: true,
      round: 1,
      numbers: new Map(),
      eliminated: new Set(),
      winner: null,
      betAmount: bet,
      countdownTimers: [],
      registrationTime: 40, // join 40 detik
      drawTime: 30          // draw 30 detik
    };

    this.chatServer.broadcastToRoom(ws.roomname, [
      "gameLowCardStart",
      `Game is starting!\nType .ij to join in ${this.activeGame.registrationTime}s.\nBet: ${bet} Starting!`
    ]);

    this.startRegistrationCountdown();
  }

  startRegistrationCountdown() {
    if(!this.activeGame) return;
    this.clearAllTimers();

    const timesToNotify = [30,20,10,5,0];
    timesToNotify.forEach(t => {
      const delay = (this.activeGame.registrationTime - t) * 1000;
      const timer = setTimeout(() => {
        if(!this.activeGame) return;
        this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardTimeLeft", `${t}s`]);
        if(t===0) this.closeRegistration();
      }, delay);
      this.activeGame.countdownTimers.push(timer);
    });
  }

  joinGame(ws){
    if(!this.activeGame || !this.activeGame.registrationOpen){
      return; // silent kalau tidak ada registrasi
    }
    if(!ws.idtarget || this.activeGame.players.has(ws.idtarget)) return;

    this.activeGame.players.set(ws.idtarget,{id:ws.idtarget});
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardJoin",
      ws.idtarget,
      `${ws.idtarget} joined!`
    ]);
  }

  closeRegistration(){
    if(!this.activeGame) return;
    const playerCount = this.activeGame.players.size;
    if(playerCount < 2){
      this.activeGame = null;
      return;
    }
    this.activeGame.registrationOpen = false;
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardClosed",
      Array.from(this.activeGame.players.keys()),
      "All players locked in! Type .id to draw cards."
    ]);
    this.startDrawCountdown();
  }

  startDrawCountdown() {
    if(!this.activeGame) return;
    this.clearAllTimers();

    const timesToNotify = [20,10,5,0];
    timesToNotify.forEach(t => {
      const delay = (this.activeGame.drawTime - t) * 1000;
      const timer = setTimeout(() => {
        if(!this.activeGame) return;
        this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardTimeLeft", `${t}s`]);
        if(t===0) this.evaluateRound();
      }, delay);
      this.activeGame.countdownTimers.push(timer);
    });
  }

  submitNumber(ws, number, tanda=""){
    if(!this.activeGame || this.activeGame.registrationOpen) {
      return; // game belum aktif → abaikan
    }
    if(!this.activeGame.players.has(ws.idtarget) || this.activeGame.eliminated.has(ws.idtarget)) return;

    // ❌ Sudah pernah draw di ronde ini → abaikan silent
    if(this.activeGame.numbers.has(ws.idtarget)) {
      return;
    }

    const n = parseInt(number,10);
    if(isNaN(n) || n < 1 || n > 12){
      return; // invalid → silent
    }

    // ✅ Simpan draw pertama
    this.activeGame.numbers.set(ws.idtarget,n);

    // ✅ Broadcast hanya draw pertama
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardPlayerDraw",
      ws.idtarget,
      n,
      tanda
    ]);
  }

  evaluateRound(){
    if(!this.activeGame) return;
    const {numbers, players, eliminated, round, betAmount} = this.activeGame;
    const entries = Array.from(numbers.entries());
    if(entries.length === 0){
      this.activeGame = null;
      return;
    }

    const values = Array.from(numbers.values());
    const allSame = values.every(v => v === values[0]);
    let losers = [];
    if(!allSame){
      const lowest = Math.min(...values);
      losers = entries.filter(([,n]) => n === lowest).map(([id]) => id);
      losers.forEach(id => eliminated.add(id));
    }

    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
    if(remaining.length === 1){
      const winnerId = remaining[0];
      const totalCoin = betAmount * players.size;
      this.activeGame.winner = winnerId;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardWinner", winnerId, totalCoin]);
      this.activeGame = null;
      return;
    }

    const numbersArr = entries.map(([id,n]) => `${id}:${n}`);
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardRoundResult", round, numbersArr, losers, remaining]);

    numbers.clear();
    this.activeGame.round++;
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardNextRound", this.activeGame.round]);

    this.startDrawCountdown(); // ⬅️ lanjut ronde berikutnya
  }

  endGame(){
    if(!this.activeGame) return;
    this.clearAllTimers();
    this.activeGame = null;
  }
}
