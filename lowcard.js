// lowcard.js - LowCardGameManager FINAL - FULL CLASS
// Timer: 20s registration, 20s draw
// Notifikasi: 20s, 10s, 5s
// Logika bot PERSIS seperti kode awal
// LOGIKA LOWCARD: Yang draw angka TERENDAH yang KALAH (eliminasi)
// FIXED: Winner detection - User hanya menang jika TIDAK ADA bot tersisa

const CONSTANTS = {
  GAME_TIMEOUT_HOURS: 1,
  REGISTRATION_TIME: 20,
  DRAW_TIME: 20,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 10,
};

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._destroyed = false;
    this._cleanupInterval = null;
    
    this._startPeriodicCleanup();
  }

  _startPeriodicCleanup() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this._cleanupInterval = setInterval(() => {
      this.cleanupStaleGames();
    }, 60000);
  }

  _safeBroadcast(room, message) {
    try {
      if (this._destroyed) return;
      if (this.chatServer && typeof this.chatServer.broadcastToRoom === 'function') {
        this.chatServer.broadcastToRoom(room, message);
      }
    } catch (error) { }
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

  _isHumanPlayer(playerId) {
    return !playerId.includes('BOT_MOZ_');
  }

  _hasHumanPlayer(game) {
    if (!game || !game.players) return false;
    for (const playerId of game.players.keys()) {
      if (this._isHumanPlayer(playerId)) return true;
    }
    return false;
  }

  _isBotGame(game) {
    if (!game) return false;
    return game.useBots === true && game.botPlayers.size > 0;
  }

  cleanupStaleGames() {
    if (this._destroyed) return;
    const now = Date.now();
    const staleGames = [];
    
    for (const [room, game] of this.activeGames) {
      if (!game || !game._active) {
        staleGames.push(room);
      } else if (game.createdAt && (now - game.createdAt) > CONSTANTS.GAME_TIMEOUT_HOURS * 3600000) {
        staleGames.push(room);
      } else if (game.players && game.players.size === 0) {
        staleGames.push(room);
      }
    }
    
    for (const room of staleGames) {
      this.endGame(room, "Game timeout");
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
  
  getBotNumberByRound(round) {
    if (round <= 2) {
      return Math.floor(Math.random() * 12) + 1;
    }
    
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

  masterTick() {
    if (this._destroyed) return;
    
    for (const [room, game] of this.activeGames) {
      if (!game || !game._active || game._ended) continue;
      
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
          if (ws.roomname) this.endGame(ws.roomname, "Game ended by player"); 
          break;
      }
    } catch (error) { }
  }

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

  _closeRegistration(room) {
    const game = this._safeGetGame(room);
    if (!game || !game.registrationOpen) return;
    
    if (game.players.size === 1) {
      this._addFourMozBots(room);
    }
    
    if (game.players.size < 2) { 
      this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players", game.hostId]); 
      this.endGame(room, "Not enough players"); 
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
          if (!currentGame.eliminated.has(botId) && !currentGame.numbers.has(botId)) {
            this._handleBotDraw(room, botId);
          }
        }
      }, drawTime * 1000);
      
      if (!game._botTimers) game._botTimers = [];
      game._botTimers.push(botTimeout);
      game._botDrawTimeouts.add(botTimeout);
    }
  }

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
      game.eliminated.add(ws.idtarget);
      this._safeSend(ws, ["gameLowCardError", "Draw time has expired! You are eliminated!"]);
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

  _evaluateRound(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._active || game._ended) return;
    
    const activePlayers = Array.from(game.players.keys())
      .filter(id => !game.eliminated.has(id));
    
    if (activePlayers.length === 0) {
      this.endGame(room, "All players eliminated");
      return;
    }
    
    const submittedPlayers = [];
    const notSubmittedPlayers = [];
    
    for (const playerId of activePlayers) {
      if (game.numbers.has(playerId)) {
        submittedPlayers.push(playerId);
      } else {
        notSubmittedPlayers.push(playerId);
      }
    }
    
    const allLosers = [];
    const loserNames = [];
    
    // YANG TIDAK DRAW LANGSUNG KALAH
    if (notSubmittedPlayers.length > 0) {
      for (const playerId of notSubmittedPlayers) {
        game.eliminated.add(playerId);
        allLosers.push(playerId);
        const player = game.players.get(playerId);
        if (player && this._isHumanPlayer(playerId)) {
          loserNames.push(player.name);
        }
      }
      const notSubmittedNames = notSubmittedPlayers.map(id => game.players.get(id)?.name || id);
      this._safeBroadcast(room, ["gameLowCardRoundResultEliminated", notSubmittedNames]);
    }
    
    // JIKA TIDAK ADA YANG DRAW (SEMUA TIDAK DRAW)
    if (submittedPlayers.length === 0) {
      this.endGame(room, "No one drew cards");
      return;
    }
    
    // ========== LOGIKA LOWCARD: YANG ANGKA TERENDAH KALAH ==========
    let lowest = 13;
    for (const id of submittedPlayers) {
      const n = game.numbers.get(id);
      if (n < lowest) lowest = n;
    }
    
    const lowestPlayers = [];
    for (const id of submittedPlayers) {
      const n = game.numbers.get(id);
      if (n === lowest) {
        lowestPlayers.push(id);
      }
    }
    
    // JIKA SEMUA PLAYER DRAW ANGKA SAMA
    if (lowestPlayers.length === submittedPlayers.length && submittedPlayers.length > 1) {
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
      return;
    }
    
    // ELIMINASI YANG DRAW ANGKA TERENDAH
    for (const id of lowestPlayers) {
      game.eliminated.add(id);
      allLosers.push(id);
      const player = game.players.get(id);
      if (player && this._isHumanPlayer(id)) {
        loserNames.push(player.name);
      }
    }
    
    // HITUNG REMAINING
    const remaining = Array.from(game.players.keys())
      .filter(id => !game.eliminated.has(id));
    
    const remainingHumans = remaining.filter(id => this._isHumanPlayer(id));
    const remainingBots = remaining.filter(id => !this._isHumanPlayer(id));
    const isBotGame = this._isBotGame(game);
    
    // ========== WINNER DETECTION YANG BENAR ==========
    // User MENANG hanya jika:
    // 1. Hanya 1 user manusia yang tersisa
    // 2. DAN TIDAK ADA BOT yang tersisa
    if (remainingHumans.length === 1 && remainingBots.length === 0) {
      const winnerId = remainingHumans[0];
      const winner = game.players.get(winnerId);
      const totalCoin = game.betAmount * game.players.size;
      
      this._safeBroadcast(room, ["gameLowCardWinner", winner?.name || winnerId, totalCoin]);
      this.endGame(room, `${winner?.name} won the game`);
      return;
    }
    
    // Jika TIDAK ADA user manusia yang tersisa (semua user sudah kalah)
    if (remainingHumans.length === 0 && loserNames.length > 0) {
      if (isBotGame) {
        this._safeBroadcast(room, ["gameLowCardTimeLeft", "Player lost"]);
      } else {
        this._safeBroadcast(room, ["gameLowCardTimeLeft", `Player(s) ${loserNames.join(", ")} lost`]);
      }
      this.endGame(room, "Game ended");
      return;
    }
    
    // Jika TIDAK ADA user manusia yang tersisa
    if (remainingHumans.length === 0) {
      this.endGame(room, "No human players remaining");
      return;
    }
    
    // JIKA MASIH ADA LEBIH DARI 1 USER MANUSIA ATAU MASIH ADA BOT
    if (remaining.length >= 2) {
      const numbersArr = Array.from(game.numbers.entries()).map(([id, n]) => {
        const player = game.players.get(id);
        const playerTanda = game.tanda.get(id) || "";
        return `${player?.name}:${n}(${playerTanda})`;
      });
      
      const allLoserNames = allLosers.map(id => game.players.get(id)?.name || id);
      const remainingNames = remaining.map(id => game.players.get(id)?.name || id);
      
      this._safeBroadcast(room, [
        "gameLowCardRoundResult",
        game.round,
        numbersArr,
        allLoserNames,
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
      return;
    }
    
    // JIKA TIDAK ADA YANG TERSISA
    if (remaining.length === 0) {
      this.endGame(room, "All players eliminated");
      return;
    }
  }

  endGame(room, reason = "") {
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
    
    if (game.players) { game.players.clear(); }
    if (game.botPlayers) { game.botPlayers.clear(); }
    if (game.numbers) { game.numbers.clear(); }
    if (game.tanda) { game.tanda.clear(); }
    if (game.eliminated) { game.eliminated.clear(); }
    
    this.activeGames.delete(room);
  }

  destroy() {
    this._destroyed = true;
    
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    for (const [room] of this.activeGames) {
      this.endGame(room, "Server destroyed");
    }
    this.activeGames.clear();
    this.chatServer = null;
  }
}
