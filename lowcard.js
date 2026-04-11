// lowcard.js - LowCardGameManager FINAL - FIXED VERSION
// Timer: 20s registration, 20s draw
// Notifikasi: 20s, 10s, 5s
// Logika bot PERSIS seperti kode awal (normal, tidak diseting menang terus)

const CONSTANTS = {
  GAME_TIMEOUT_HOURS: 1,
  REGISTRATION_TIME: 20,     // 20 detik join
  DRAW_TIME: 20,             // 20 detik draw
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 10,
};

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._destroyed = false;
    this._errorLogs = [];
    this._errorHandler = (error, context) => {
      const errorMsg = error?.message || String(error);
      this._errorLogs.push({ time: Date.now(), context, error: errorMsg });
      if (this._errorLogs.length > 100) this._errorLogs.shift();
      console.error(`[LowCardGame] ${context}:`, errorMsg);
    };
  }

  _safeBroadcast(room, message) {
    try {
      if (this._destroyed) return;
      if (this.chatServer && typeof this.chatServer.broadcastToRoom === 'function') {
        this.chatServer.broadcastToRoom(room, message);
      }
    } catch (error) { 
      this._errorHandler(error, `broadcast to ${room}`); 
    }
  }

  _safeSend(ws, message) {
    try {
      if (this._destroyed) return false;
      if (ws && ws.readyState === 1 && this.chatServer && typeof this.chatServer.safeSend === 'function') {
        return this.chatServer.safeSend(ws, message);
      }
      return false;
    } catch (error) { 
      return false; 
    }
  }

  _safeGetGame(room) {
    try {
      if (this._destroyed || !room) return null;
      const game = this.activeGames.get(room);
      return (game && game._active && !game._ended) ? game : null;
    } catch (error) { 
      return null; 
    }
  }

  cleanupStaleGames() {
    if (this._destroyed) return;
    const now = Date.now();
    const staleGames = [];
    for (const [room, game] of this.activeGames) {
      if (!game || !game._active) staleGames.push(room);
      else if (game.createdAt && (now - game.createdAt) > CONSTANTS.GAME_TIMEOUT_HOURS * 3600000) staleGames.push(room);
      else if (game.players && game.players.size === 0) staleGames.push(room);
    }
    for (const room of staleGames) {
      this.endGame(room);
    }
  }

  _clearAllTimers(game) {
    if (!game) return;
    
    if (game._regInterval) { 
      clearInterval(game._regInterval); 
      game._regInterval = null; 
    }
    if (game._drawInterval) { 
      clearInterval(game._drawInterval); 
      game._drawInterval = null; 
    }
    
    if (game.countdownTimers) {
      for (const timer of game.countdownTimers) {
        if (timer) {
          if (timer.interval) clearInterval(timer.interval);
          if (timer.timeout) clearTimeout(timer.timeout);
        }
      }
      game.countdownTimers = null;
    }
    
    if (game._botTimers) { 
      for (const timer of game._botTimers) {
        if (timer) clearTimeout(timer);
      }
      game._botTimers = null; 
    }
    
    if (game._botDrawTimeouts) {
      for (const timeout of game._botDrawTimeouts) {
        try { clearTimeout(timeout); } catch(e) {}
      }
      game._botDrawTimeouts.clear();
      game._botDrawTimeouts = null;
    }
    
    if (game._timeouts) { 
      for (const tid of game._timeouts) {
        if (tid) clearTimeout(tid);
      }
      game._timeouts = null; 
    }
  }

  getRandomCardTanda() { 
    const tandaOptions = ["C1", "C2", "C3", "C4"]; 
    return tandaOptions[Math.floor(Math.random() * tandaOptions.length)]; 
  }
  
  getRandomDrawTime() { 
    return Math.floor(Math.random() * (CONSTANTS.BOT_DRAW_MAX_SECONDS - CONSTANTS.BOT_DRAW_MIN_SECONDS + 1)) + CONSTANTS.BOT_DRAW_MIN_SECONDS; 
  }
  
  // ========== LOGIKA BOT PERSIS KODE AWAL (NORMAL) ==========
  getBotNumberByRound(round) {
    // Round 1-2: random 1-12 (normal, tidak diatur)
    if (round <= 2) {
      return Math.floor(Math.random() * 12) + 1;
    }
    
    // Round 3 ke atas: 70% angka besar (9-12), 30% angka kecil (1-8)
    // Ini sama persis dengan kode awal Anda!
    if (round >= 3) {
      const isGetHighNumber = Math.random() < 0.7;
      
      if (isGetHighNumber) {
        const bigNumbers = [9, 10, 11, 12];
        return bigNumbers[Math.floor(Math.random() * bigNumbers.length)];
      } else {
        const smallNumbers = [1, 2, 3, 4, 5, 6, 7, 8];
        return smallNumbers[Math.floor(Math.random() * smallNumbers.length)];
      }
    }
    
    return Math.floor(Math.random() * 12) + 1;
  }

  // ========== MASTER TICK (dipanggil dari index.js setiap detik) ==========
  masterTick() {
    if (this._destroyed) return;
    
    for (const [room, game] of this.activeGames) {
      if (!game || !game._active || game._ended) continue;
      
      // REGISTRATION COUNTDOWN (20 detik) - notifikasi di 20s, 10s, 5s
      if (game.registrationOpen && game.regTimeLeft > 0) {
        game.regTimeLeft--;
        
        if (game.regTimeLeft === 20 || game.regTimeLeft === 10 || game.regTimeLeft === 5) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.regTimeLeft}s`]);
        }
        
        if (game.regTimeLeft <= 0) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          this._closeRegistration(room);
        }
      }
      
      // DRAW COUNTDOWN (20 detik) - notifikasi di 20s, 10s, 5s
      if (!game.registrationOpen && !game.drawTimeExpired && game.drawTimeLeft > 0) {
        game.drawTimeLeft--;
        
        if (game.drawTimeLeft === 20 || game.drawTimeLeft === 10 || game.drawTimeLeft === 5) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.drawTimeLeft}s`]);
        }
        
        if (game.drawTimeLeft <= 0 && !game.drawTimeExpired) {
          game.drawTimeExpired = true;
          this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          
          if (!game.evaluationLocked) {
            game.evaluationLocked = true;
            this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
            
            const evalTimeout = setTimeout(() => { 
              if (!this._destroyed && !game._ended) this._evaluateRound(room); 
            }, 2000);
            
            if (!game._timeouts) game._timeouts = [];
            game._timeouts.push(evalTimeout);
          }
        }
      }
    }
  }

  // ========== HANDLE EVENT ==========
  async handleEvent(ws, data) {
    if (this._destroyed || !ws || !data) return;
    const evt = data[0];
    try {
      switch (evt) {
        case "gameLowCardStart": 
          await this.startGame(ws, data[1]); 
          break;
        case "gameLowCardJoin": 
          await this.joinGame(ws); 
          break;
        case "gameLowCardNumber": 
          await this.submitNumber(ws, data[1], data[2] || ""); 
          break;
        case "gameLowCardEnd": 
          if (ws.roomname) this.endGame(ws.roomname); 
          break;
      }
    } catch (error) {
      this._errorHandler(error, `handleEvent:${evt}`);
      this._safeSend(ws, ["gameLowCardError", "Internal error"]);
    }
  }

  // ========== START GAME ==========
  async startGame(ws, bet) {
    if (!ws?.roomname || !ws?.idtarget) { 
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]); 
      return; 
    }
    
    const room = ws.roomname;
    
    if (this.activeGames.has(room)) { 
      this._safeSend(ws, ["gameLowCardError", "Game already running in this room"]); 
      return; 
    }
    
    const betAmount = parseInt(bet) || 0;
    if (betAmount < 0 || (betAmount !== 0 && betAmount < 100)) { 
      this._safeSend(ws, ["gameLowCardError", "Bet must be 0 or at least 100"]); 
      return; 
    }
    
    const game = {
      room, 
      players: new Map(), 
      botPlayers: new Map(), 
      registrationOpen: true, 
      round: 1,
      numbers: new Map(), 
      tanda: new Map(), 
      eliminated: new Set(), 
      winner: null, 
      betAmount,
      regTimeLeft: CONSTANTS.REGISTRATION_TIME, 
      drawTimeLeft: CONSTANTS.DRAW_TIME,
      drawTimeExpired: false, 
      evaluationLocked: false, 
      hostId: ws.idtarget,
      hostName: ws.username || ws.idtarget, 
      useBots: false, 
      createdAt: Date.now(),
      _active: true,
      _ended: false,
      _timeouts: [], 
      _botTimers: [], 
      _botDrawTimeouts: new Set(),
      countdownTimers: [], 
      _regInterval: null, 
      _drawInterval: null
    };
    
    game.players.set(ws.idtarget, { 
      id: ws.idtarget, 
      name: ws.username || ws.idtarget 
    });
    
    this.activeGames.set(room, game);
    
    this._safeBroadcast(room, ["gameLowCardStart", betAmount]);
    this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, betAmount]);
    this._safeBroadcast(room, ["gameLowCardTimeLeft", `${CONSTANTS.REGISTRATION_TIME}s`]);
  }

  // ========== TAMBAH BOT MOZ ==========
  _addFourMozBots(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._active || game._ended) return;
    if (game.useBots || game.botPlayers.size > 0) return;
    
    game.useBots = true;
    const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
    
    for (let i = 0; i < 4; i++) {
      const botId = `BOT_MOZ_${room}_${i}_${Date.now()}_${Math.random().toString(36).substring(7)}`;
      const botName = mozNames[i];
      
      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      
      this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
    }
  }

  // ========== CLOSE REGISTRATION ==========
  _closeRegistration(room) {
    const game = this._safeGetGame(room);
    if (!game || !game.registrationOpen) return;
    
    if (game.players.size === 1) {
      this._addFourMozBots(room);
    }
    
    if (game.players.size < 2) { 
      this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players", game.hostId]); 
      this.endGame(room); 
      return; 
    }
    
    game.registrationOpen = false;
    
    const playersList = Array.from(game.players.values())
      .filter(p => p && p.name)
      .map(p => p.name);
    
    this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
    this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._safeBroadcast(room, ["gameLowCardNextRound", 1]);
    this._safeBroadcast(room, ["gameLowCardTimeLeft", `${CONSTANTS.DRAW_TIME}s`]);
    
    this._startBotDraws(room);
  }

  // ========== START BOT DRAWS ==========
  _startBotDraws(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._active || game._ended) return;
    if (!game.useBots || !game.botPlayers) return;
    
    if (game._botTimers) { 
      for (const timer of game._botTimers) clearTimeout(timer); 
      game._botTimers = []; 
    }
    if (game._botDrawTimeouts) { 
      for (const timeout of game._botDrawTimeouts) clearTimeout(timeout); 
      game._botDrawTimeouts.clear(); 
    }
    
    const activeBots = Array.from(game.botPlayers.keys())
      .filter(botId => !game.eliminated.has(botId) && !game.numbers.has(botId));
    
    for (const botId of activeBots) {
      const drawTime = this.getRandomDrawTime();
      
      const botTimeout = setTimeout(() => {
        if (this._destroyed) return;
        const currentGame = this._safeGetGame(room);
        if (currentGame && currentGame._active && !currentGame._ended && 
            !currentGame.drawTimeExpired && !currentGame.evaluationLocked) {
          this._handleBotDraw(room, botId);
        }
      }, drawTime * 1000);
      
      if (!game._botTimers) game._botTimers = [];
      game._botTimers.push(botTimeout);
      game._botDrawTimeouts.add(botTimeout);
    }
  }

  // ========== HANDLE BOT DRAW ==========
  _handleBotDraw(room, botId) {
    const game = this._safeGetGame(room);
    if (!game || !game._active || game._ended) return;
    if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
    if (game.drawTimeExpired || game.evaluationLocked) return;
    
    const botNumber = this.getBotNumberByRound(game.round);
    const tanda = this.getRandomCardTanda();
    
    game.numbers.set(botId, botNumber);
    game.tanda.set(botId, tanda);
    
    const botPlayer = game.players.get(botId);
    const botName = botPlayer?.name || botId;
    
    this._safeBroadcast(room, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
    
    const activePlayers = Array.from(game.players.keys())
      .filter(id => !game.eliminated.has(id));
    const allDrawn = game.numbers.size === activePlayers.length;
    
    if (!game.evaluationLocked && allDrawn && !game.drawTimeExpired) {
      game.evaluationLocked = true;
      this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
      
      const evalTimeout = setTimeout(() => { 
        if (!this._destroyed && !game._ended) this._evaluateRound(room); 
      }, 2000);
      
      if (!game._timeouts) game._timeouts = [];
      game._timeouts.push(evalTimeout);
    }
  }

  // ========== JOIN GAME ==========
  async joinGame(ws) {
    if (!ws?.roomname || !ws?.idtarget) { 
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]); 
      return; 
    }
    
    const game = this._safeGetGame(ws.roomname);
    if (!game) { 
      this._safeSend(ws, ["gameLowCardError", "No active game"]); 
      return; 
    }
    
    if (game.evaluationLocked) { 
      this._safeSend(ws, ["gameLowCardError", "Game in progress, please wait"]); 
      return; 
    }
    
    if (!game.registrationOpen) { 
      this._safeSend(ws, ["gameLowCardError", "Registration closed"]); 
      return; 
    }
    
    if (game.players.has(ws.idtarget)) { 
      this._safeSend(ws, ["gameLowCardError", "Already joined"]); 
      return; 
    }
    
    game.players.set(ws.idtarget, { 
      id: ws.idtarget, 
      name: ws.username || ws.idtarget 
    });
    
    this._safeBroadcast(ws.roomname, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
  }

  // ========== SUBMIT NUMBER ==========
  async submitNumber(ws, number, tanda = "") {
    if (!ws?.roomname || !ws?.idtarget) { 
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]); 
      return; 
    }
    
    const game = this._safeGetGame(ws.roomname);
    if (!game) { 
      this._safeSend(ws, ["gameLowCardError", "No active game"]); 
      return; 
    }
    
    if (game.evaluationLocked) { 
      this._safeSend(ws, ["gameLowCardError", "Please wait, results are being processed..."]); 
      return; 
    }
    
    if (game.registrationOpen) { 
      this._safeSend(ws, ["gameLowCardError", "Registration still open"]); 
      return; 
    }
    
    if (!game.players.has(ws.idtarget) || game.eliminated.has(ws.idtarget)) { 
      this._safeSend(ws, ["gameLowCardError", "Not in game or eliminated"]); 
      return; 
    }
    
    if (game.numbers.has(ws.idtarget)) { 
      this._safeSend(ws, ["gameLowCardError", "Already submitted number"]); 
      return; 
    }
    
    if (game.drawTimeExpired) { 
      this._safeSend(ws, ["gameLowCardError", "Draw time has expired!"]); 
      return; 
    }
    
    const n = parseInt(number);
    if (isNaN(n) || n < 1 || n > 12) { 
      this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]); 
      return; 
    }
    
    game.numbers.set(ws.idtarget, n);
    game.tanda.set(ws.idtarget, tanda || this.getRandomCardTanda());
    
    const player = game.players.get(ws.idtarget);
    this._safeBroadcast(ws.roomname, ["gameLowCardPlayerDraw", player?.name || ws.idtarget, n, game.tanda.get(ws.idtarget)]);
    
    const activePlayers = Array.from(game.players.keys())
      .filter(id => !game.eliminated.has(id));
    const allDrawn = game.numbers.size === activePlayers.length;
    
    if (!game.evaluationLocked && allDrawn && !game.drawTimeExpired) {
      game.evaluationLocked = true;
      this._safeBroadcast(ws.roomname, ["gameLowCardWait", "Please wait for results..."]);
      
      const evalTimeout = setTimeout(() => { 
        if (!this._destroyed && !game._ended) this._evaluateRound(ws.roomname); 
      }, 2000);
      
      if (!game._timeouts) game._timeouts = [];
      game._timeouts.push(evalTimeout);
    }
  }

  // ========== EVALUATE ROUND ==========
  _evaluateRound(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._active || game._ended) return;
    
    if (game.numbers.size === 0) { 
      this.endGame(room); 
      return; 
    }
    
    let lowest = 13;
    for (const n of game.numbers.values()) {
      if (n < lowest) lowest = n;
    }
    
    const losers = [];
    for (const [id, n] of game.numbers) {
      if (n === lowest) {
        game.eliminated.add(id);
        losers.push(id);
      }
    }
    
    const remaining = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    
    if (remaining.length === 1) {
      const winnerId = remaining[0];
      const winner = game.players.get(winnerId);
      const totalCoin = game.betAmount * game.players.size;
      this._safeBroadcast(room, ["gameLowCardWinner", winner?.name || winnerId, totalCoin]);
      this.endGame(room);
      return;
    }
    
    if (remaining.length === 0) { 
      this.endGame(room); 
      return; 
    }
    
    const numbersArr = Array.from(game.numbers.entries()).map(([id, n]) => {
      const player = game.players.get(id);
      const playerTanda = game.tanda.get(id) || "";
      return `${player?.name}:${n}(${playerTanda})`;
    });
    
    const loserNames = losers.map(id => game.players.get(id)?.name || id);
    const remainingNames = remaining.map(id => game.players.get(id)?.name || id);
    
    this._safeBroadcast(room, [
      "gameLowCardRoundResult",
      game.round,
      numbersArr,
      loserNames,
      remainingNames
    ]);
    
    game.round++;
    game.numbers.clear();
    game.tanda.clear();
    game.drawTimeLeft = CONSTANTS.DRAW_TIME;
    game.drawTimeExpired = false;
    game.evaluationLocked = false;
    
    this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
    this._safeBroadcast(room, ["gameLowCardTimeLeft", `${CONSTANTS.DRAW_TIME}s`]);
    
    if (game.useBots && game.botPlayers && game.botPlayers.size > 0) {
      this._startBotDraws(room);
    }
  }

  // ========== END GAME ==========
  endGame(room) {
    const game = this.activeGames.get(room);
    if (!game) return;
    
    if (game._ended) return;
    game._ended = true;
    game._active = false;
    
    this._clearAllTimers(game);
    
    const playersList = [];
    if (game.players) { 
      for (const player of game.players.values()) { 
        if (player && player.name) playersList.push(player.name); 
      } 
    }
    
    // Clean up all maps and sets
    if (game.players) { 
      game.players.clear(); 
    }
    if (game.botPlayers) { 
      game.botPlayers.clear(); 
    }
    if (game.numbers) { 
      game.numbers.clear(); 
    }
    if (game.tanda) { 
      game.tanda.clear(); 
    }
    if (game.eliminated) { 
      game.eliminated.clear(); 
    }
    
    // Broadcast end game
    if (playersList.length > 0) {
      this._safeBroadcast(room, ["gameLowCardEnd", playersList]);
    }
    
    this.activeGames.delete(room);
  }

  // ========== DESTROY ==========
  destroy() {
    this._destroyed = true;
    for (const [room] of this.activeGames) {
      this.endGame(room);
    }
    this.activeGames.clear();
    this.chatServer = null;
    this._errorLogs = [];
  }
}
