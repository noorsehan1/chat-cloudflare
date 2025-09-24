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
      case "gameLowCardNumber": this.submitNumber(ws, data[1], data[2] || ""); break; // tambahan tanda
      case "gameLowCardEnd": this.endGame(); break;
    }
  }

  startGame(ws, betAmount){
    if(!ws.roomname || !ws.idtarget) return;
    const bet = parseInt(betAmount,10)||0;

    if(this.activeGame?.countdownTimers)
      this.activeGame.countdownTimers.forEach(clearTimeout);

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
      registrationTime: 30,
      drawTime: 20
    };

    this.chatServer.broadcastToRoom(ws.roomname, [
  "gameLowCardStart",
  `Game is starting!\nType .ij to join in ${this.activeGame.registrationTime}s.\nBet: ${bet} Starting!`
]);

    this.startRegistrationCountdown();
  }

  startRegistrationCountdown() {
    if(!this.activeGame) return;
    const timesToNotify = [20,10,5,0];
    this.activeGame.countdownTimers.forEach(clearTimeout);
    this.activeGame.countdownTimers = [];

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
      this.chatServer.safeSend(ws, ["gameLowCardError","No open registration"]);
      return;
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
      const onlyPlayer = playerCount===1 ? Array.from(this.activeGame.players.keys())[0] : null;
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardError","Need at least 2 players", onlyPlayer]);
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
    const timesToNotify = [20,10,5,0];
    this.activeGame.countdownTimers.forEach(clearTimeout);
    this.activeGame.countdownTimers = [];

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
      this.chatServer.safeSend(ws, ["gameLowCardError","Game not active"]);
      return;
    }
    if(!this.activeGame.players.has(ws.idtarget) || this.activeGame.eliminated.has(ws.idtarget)) return;

    const n = parseInt(number,10);
    if(isNaN(n) || n < 1 || n > 12){
      this.chatServer.safeSend(ws, ["gameLowCardError","Invalid number"]);
      return;
    }

    this.activeGame.numbers.set(ws.idtarget,n);

    // broadcast ke room, termasuk tanda
    this.chatServer.broadcastToRoom(this.activeGame.room, [
      "gameLowCardPlayerDraw",
      ws.idtarget,
      n,
      tanda
    ]);

    if(this.activeGame.numbers.size === this.activeGame.players.size - this.activeGame.eliminated.size){
      this.evaluateRound();
    }
  }

  evaluateRound(){
    if(!this.activeGame) return;
    const {numbers, players, eliminated, round, betAmount} = this.activeGame;
    const entries = Array.from(numbers.entries());
    if(entries.length === 0){
      this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardError","No numbers drawn this round"]);
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
  }

  endGame(){
    if(!this.activeGame) return;
    this.chatServer.broadcastToRoom(this.activeGame.room, ["gameLowCardEnd", Array.from(this.activeGame.players.keys())]);
    this.activeGame.countdownTimers.forEach(clearTimeout);
    this.activeGame = null;
  }
}


