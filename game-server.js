// ==================== GAME-SERVER.JS - FULL COMPLETE VERSION (NO LOGS) ====================

// ==================== CONSTANTS ====================
const CONSTANTS = {
  MAX_LOWCARD_GAMES: 10,
  REGISTRATION_TIME_MS: 20000,
  DRAW_TIME_MS: 20000,
  EVALUATION_DELAY_MS: 2000,
  MAX_BOTS_PER_GAME: 4,
  MAX_BET: 100000,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 15,
  MAX_BOT_DRAWS_PER_ROUND: 4,
  EVALUATION_TIMEOUT_MS: 30000,
  START_LOCK_DURATION_MS: 3000,
  MAX_PLAYERS_PER_GAME: 45,
  GAME_CLEANUP_DELAY_MS: 5000,
  BATCH_SIZE: 2,
  CLEANUP_TIK: 90,
  STALE_GAME_TIMEOUT_MS: 600000,
  STUCK_DRAW_TIMEOUT_MS: 60000,
  STUCK_REGISTRATION_TIMEOUT_MS: 30000,
  MAX_RETRY_INIT_QUIZ: 2,
  MAX_SHUTDOWN_WAIT_MS: 5000,
  MAX_WS_CLIENTS: 50,
  MAX_ARRAY_SIZE: 50,
  QUIZ_SWITCH_DELAY_MS: 5000,
  SCHEDULER_INTERVAL_MS: 60000,
  QUIZ_BATCH_SIZE: 100,
  MAX_QUESTIONS: 10000,
  CF_SUBREQUEST_LIMIT: 50,
  DEEPLX_TIMEOUT_MS: 8000,
  DEEPLX_MAX_RETRIES: 5,
  TRANSLATE_TIMEOUT_MS: 10000,
  QUIZ_KEEP_ALIVE_INTERVAL_MS: 5000,
  QUIZ_NEXT_QUESTION_DELAY_MS: 5000,
  MAX_EVENTS_PER_TICK: 5,
  BROADCAST_BATCH_SIZE: 5,
  MAX_RESTART_ATTEMPTS: 3,
  RESTART_COOLDOWN_MS: 30000,
  HEALTH_CHECK_INTERVAL_MS: 10000,
  MAX_IDLE_TIME_MS: 300000,
  RECONNECT_DELAY_MS: 2000,
  MAX_EVENT_QUEUE_SIZE: 1000,
  ERROR_RECOVERY_DELAY_MS: 5000,
  MAX_UNHANDLED_ERRORS: 5,
  ERROR_RESET_INTERVAL_MS: 60000,
  LOWCARD_WINNER_KEY: 'lowcard_winner_',
  LOWCARD_RECORDING_KEY: 'lowcard_recording_status_',
  SCHEDULER_LOOP_INTERVAL_MS: 50,
  
  MAX_DICE_GAMES: 10,
  DICE_ROLL_TIME_MS: 0,
  DICE_READING_TIME_MS: 0,
  DICE_ANSWER_TIME_MS: 20000,
  DICE_TOTAL_TIME_MS: 20000,
  DICE_BREAK_MS: 15000,
  DICE_AFTER_TIMEOUT_BREAK_MS: 15000,
  MAX_DICE_VALUE: 6,
  DICE_ROOM: "Quiz",
  DICE_POINT_KEY: 'dice_points',
  DICE_LAST_WEEK_WINNER: 'dice_last_week_winner',
  DICE_WINNER_KEY: 'dice_winner_',
  DICE_RECORDING_KEY: 'dice_recording_status_',
  QUIZ_START_DELAY_MS: 5000,
  
  DICE_AUTO_START_DELAY_MS: 3000,
  DICE_MIN_PLAYERS_TO_AUTO_START: 1,
  DICE_CHECK_INTERVAL_MS: 5000,
  
  DICE_LAST_RESET_WEEK: 'dice_last_reset_week',
  WEEKLY_RESET_CHECK_INTERVAL_MS: 300000,
  
  TIE_BREAKER_TIME_LIMIT: 20,
  TIE_BREAKER_COOLDOWN: 15000,
};

// ==================== QUIZ SCHEDULE ====================
const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: 1, end: 2 },
    { start: 14, end: 15 },
    { start: 22, end: 23 }
  ],
  TIMEZONE_OFFSET: 8,
};

const DICE_ROOM = "Quiz";

// ==================== KV CACHE CLASS ====================
class KVCache {
  constructor() {
    this.cache = new Map();
    this.persistent = new Map();
  }

  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;
    return entry.value;
  }

  set(key, value) {
    this.cache.set(key, {
      value: value,
      timestamp: Date.now()
    });
  }

  delete(key) {
    this.cache.delete(key);
  }

  clear() {
    this.cache.clear();
  }

  setPersistent(key, value) {
    this.persistent.set(key, value);
  }

  getPersistent(key) {
    return this.persistent.get(key) || null;
  }

  getAllKeys() {
    return Array.from(this.cache.keys());
  }

  size() {
    return this.cache.size;
  }
}

// ==================== PLAYER CLASS ====================
class Player {
  constructor(id, name, ws) {
    this.id = id;
    this.name = name;
    this.ws = ws;
    this.connected = true;
    this.lastActivity = Date.now();
    this.score = 0;
    this.gamesPlayed = 0;
    this.gamesWon = 0;
    this.totalWinnings = 0;
    this.isBot = false;
    this.room = null;
    this.currentGame = null;
    this.points = 0;
    this.level = 1;
    this.experience = 0;
  }

  updateActivity() {
    this.lastActivity = Date.now();
  }

  isIdle(timeout = CONSTANTS.MAX_IDLE_TIME_MS) {
    return Date.now() - this.lastActivity > timeout;
  }

  addPoints(points) {
    this.points += points;
    this.experience += points;
    this.checkLevelUp();
  }

  checkLevelUp() {
    const expNeeded = this.level * 100;
    if (this.experience >= expNeeded) {
      this.level++;
      this.experience = 0;
      return true;
    }
    return false;
  }

  toJSON() {
    return {
      id: this.id,
      name: this.name,
      score: this.score,
      points: this.points,
      level: this.level,
      experience: this.experience,
      gamesPlayed: this.gamesPlayed,
      gamesWon: this.gamesWon,
      totalWinnings: this.totalWinnings,
      isBot: this.isBot,
      connected: this.connected
    };
  }
}

// ==================== BOT CLASS ====================
class Bot extends Player {
  constructor(id, name) {
    super(id, name, null);
    this.isBot = true;
    this.strategy = this.getRandomStrategy();
    this.responseDelay = Math.random() * 3000 + 1000;
    this.difficulty = this.getRandomDifficulty();
  }

  getRandomStrategy() {
    const strategies = ['aggressive', 'conservative', 'random', 'adaptive', 'balanced'];
    return strategies[Math.floor(Math.random() * strategies.length)];
  }

  getRandomDifficulty() {
    const difficulties = ['easy', 'medium', 'hard'];
    return difficulties[Math.floor(Math.random() * difficulties.length)];
  }

  async makeDecision(gameState) {
    await this.delay(this.responseDelay);

    switch (this.strategy) {
      case 'aggressive':
        return this.aggressiveDecision(gameState);
      case 'conservative':
        return this.conservativeDecision(gameState);
      case 'adaptive':
        return this.adaptiveDecision(gameState);
      case 'balanced':
        return this.balancedDecision(gameState);
      default:
        return this.randomDecision(gameState);
    }
  }

  aggressiveDecision(gameState) {
    const maxBet = gameState.maxBet || 1000;
    const bet = Math.min(Math.floor(Math.random() * maxBet * 0.8) + (maxBet * 0.2), CONSTANTS.MAX_BET);
    return {
      action: 'draw',
      bet: bet,
      raiseAmount: Math.floor(bet * 0.5)
    };
  }

  conservativeDecision(gameState) {
    const shouldDraw = Math.random() > 0.4;
    const bet = Math.floor(Math.random() * 500) + 50;
    return {
      action: shouldDraw ? 'draw' : 'fold',
      bet: bet,
      raiseAmount: 0
    };
  }

  adaptiveDecision(gameState) {
    const round = gameState.round || 1;
    const playerCount = gameState.playerCount || 2;
    const potSize = gameState.pot || 0;
    
    if (round > 3 || potSize > 5000) {
      return { action: 'fold', bet: 0, raiseAmount: 0 };
    }
    
    if (round === 1 && playerCount > 4) {
      return {
        action: 'draw',
        bet: Math.floor(Math.random() * 1000) + 200,
        raiseAmount: 0
      };
    }
    
    return {
      action: 'draw',
      bet: Math.floor(Math.random() * 1500) + 300,
      raiseAmount: Math.floor(Math.random() * 500)
    };
  }

  balancedDecision(gameState) {
    const shouldDraw = Math.random() > 0.3;
    const baseBet = 500;
    const variance = Math.floor(Math.random() * 1000);
    
    return {
      action: shouldDraw ? 'draw' : 'fold',
      bet: Math.max(baseBet + variance, 100),
      raiseAmount: Math.floor(Math.random() * 300)
    };
  }

  randomDecision(gameState) {
    const actions = ['draw', 'fold', 'raise'];
    const action = actions[Math.floor(Math.random() * actions.length)];
    const bet = Math.floor(Math.random() * 3000) + 100;
    
    return {
      action: action,
      bet: action === 'fold' ? 0 : bet,
      raiseAmount: action === 'raise' ? Math.floor(Math.random() * 500) : 0
    };
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// ==================== LOWCARD GAME CLASS ====================
class LowcardGame {
  constructor(gameId, hostId) {
    this.id = gameId;
    this.hostId = hostId;
    this.players = new Map();
    this.bots = new Map();
    this.status = 'waiting';
    this.round = 0;
    this.maxRounds = 5;
    this.currentDraws = new Map();
    this.bets = new Map();
    this.pot = 0;
    this.startTime = Date.now();
    this.lastActivity = Date.now();
    this.winner = null;
    this.history = [];
    this.maxPlayers = CONSTANTS.MAX_PLAYERS_PER_GAME;
    this.isLocked = false;
    this.registrationEndTime = null;
    this.drawEndTime = null;
    this.totalRounds = 0;
    this.currentPlayers = new Set();
    this.foldedPlayers = new Set();
    this.minBet = 100;
    this.currentBet = 100;
    this.lastRaiser = null;
  }

  addPlayer(player) {
    if (this.players.size >= this.maxPlayers) {
      throw new Error('Game is full');
    }
    if (this.players.has(player.id)) {
      throw new Error('Player already in game');
    }
    this.players.set(player.id, player);
    this.currentPlayers.add(player.id);
    this.lastActivity = Date.now();
    return true;
  }

  removePlayer(playerId) {
    const removed = this.players.delete(playerId);
    this.bots.delete(playerId);
    this.currentPlayers.delete(playerId);
    this.foldedPlayers.delete(playerId);
    this.lastActivity = Date.now();
    return removed;
  }

  addBot(bot) {
    if (this.bots.size >= CONSTANTS.MAX_BOTS_PER_GAME) {
      throw new Error('Max bots reached');
    }
    this.bots.set(bot.id, bot);
    this.players.set(bot.id, bot);
    this.currentPlayers.add(bot.id);
    this.lastActivity = Date.now();
    return true;
  }

  getTotalPlayers() {
    return this.players.size;
  }

  getActivePlayers() {
    return Array.from(this.players.values()).filter(p => p.connected && !this.foldedPlayers.has(p.id));
  }

  getRemainingPlayers() {
    return Array.from(this.currentPlayers).filter(id => !this.foldedPlayers.has(id));
  }

  startRegistration() {
    this.status = 'registration';
    this.registrationEndTime = Date.now() + CONSTANTS.REGISTRATION_TIME_MS;
    this.lastActivity = Date.now();
  }

  startDrawing() {
    this.status = 'drawing';
    this.round++;
    this.totalRounds++;
    this.currentDraws = new Map();
    this.foldedPlayers = new Set();
    this.drawEndTime = Date.now() + CONSTANTS.DRAW_TIME_MS;
    this.lastActivity = Date.now();
  }

  evaluateRound() {
    this.status = 'evaluating';
    this.lastActivity = Date.now();
  }

  completeGame() {
    this.status = 'completed';
    this.lastActivity = Date.now();
  }

  isStale() {
    return Date.now() - this.lastActivity > CONSTANTS.STALE_GAME_TIMEOUT_MS;
  }

  isRegistrationStuck() {
    return this.status === 'registration' && 
           Date.now() > this.registrationEndTime + CONSTANTS.STUCK_REGISTRATION_TIMEOUT_MS;
  }

  isDrawStuck() {
    return this.status === 'drawing' && 
           Date.now() > this.drawEndTime + CONSTANTS.STUCK_DRAW_TIMEOUT_MS;
  }

  placeBet(playerId, amount) {
    if (amount > CONSTANTS.MAX_BET) {
      throw new Error('Bet exceeds maximum');
    }
    this.bets.set(playerId, amount);
    this.pot += amount;
    this.currentBet = Math.max(this.currentBet, amount);
    this.lastRaiser = playerId;
    this.lastActivity = Date.now();
    return true;
  }

  fold(playerId) {
    this.foldedPlayers.add(playerId);
    this.lastActivity = Date.now();
    return true;
  }

  getPlayerBet(playerId) {
    return this.bets.get(playerId) || 0;
  }

  resetBets() {
    this.bets.clear();
    this.currentBet = this.minBet;
    this.lastRaiser = null;
  }

  toJSON() {
    return {
      id: this.id,
      hostId: this.hostId,
      status: this.status,
      round: this.round,
      totalRounds: this.totalRounds,
      players: Array.from(this.players.values()).map(p => p.toJSON()),
      bots: Array.from(this.bots.values()).map(b => b.toJSON()),
      pot: this.pot,
      winner: this.winner,
      startTime: this.startTime,
      lastActivity: this.lastActivity,
      currentBet: this.currentBet,
      minBet: this.minBet,
      playerCount: this.players.size,
      activePlayers: this.getActivePlayers().length
    };
  }
}

// ==================== DICE GAME SYSTEM ====================
class DiceGameSystem {
  constructor(kvCache, wsServer) {
    this.kv = kvCache;
    this.ws = wsServer;
    this.activeGames = new Map();
    this.gameTimers = new Map();
    this.isProcessing = false;
    this.gameHistory = [];
    this.stats = {
      totalGames: 0,
      totalWinners: 0,
      lastGameId: null,
      lastWinner: null,
      totalPlayers: 0,
      totalRolls: 0
    };
    this.isRunning = false;
    this.scheduleInterval = null;
  }

  async startDiceGame(gameId) {
    try {
      const result = await this.rollDice(gameId);
      const winner = await this.determineWinner(gameId, result);
      await this.announceResult(gameId, winner, result);
      await this.waitForBreak(CONSTANTS.DICE_BREAK_MS);
      await this.cleanupGame(gameId);
    } catch (error) {
      await this.handleGameError(gameId);
    }
  }

  async rollDice(gameId) {
    const game = this.activeGames.get(gameId);
    if (!game) throw new Error('Game not found');

    const results = {};
    const players = game.players;
    
    for (const playerId of players) {
      const value = Math.floor(Math.random() * CONSTANTS.MAX_DICE_VALUE) + 1;
      results[playerId] = value;
    }

    game.results = results;
    game.rolledAt = Date.now();
    this.stats.totalRolls++;
    this.activeGames.set(gameId, game);

    this.broadcastToRoom({
      type: 'DICE_ROLL_RESULT',
      gameId: gameId,
      results: results,
      playerNames: game.playerNames,
      timestamp: Date.now()
    });

    return results;
  }

  async determineWinner(gameId, results) {
    const sixes = Object.entries(results)
      .filter(([_, value]) => value === 6)
      .map(([playerId]) => playerId);

    if (sixes.length === 0) {
      return null;
    }

    if (sixes.length === 1) {
      return sixes[0];
    }

    return await this.handleTieBreaker(gameId, sixes);
  }

  async handleTieBreaker(gameId, players) {
    const tieResults = {};
    for (const playerId of players) {
      const value = Math.floor(Math.random() * CONSTANTS.MAX_DICE_VALUE) + 1;
      tieResults[playerId] = value;
    }

    this.broadcastToRoom({
      type: 'DICE_TIE_BREAKER',
      gameId: gameId,
      results: tieResults,
      timestamp: Date.now()
    });

    let highestValue = 0;
    let winner = null;
    let isTie = false;

    for (const [playerId, value] of Object.entries(tieResults)) {
      if (value > highestValue) {
        highestValue = value;
        winner = playerId;
        isTie = false;
      } else if (value === highestValue) {
        isTie = true;
      }
    }

    if (isTie) {
      const tiedPlayers = Object.entries(tieResults)
        .filter(([_, value]) => value === highestValue)
        .map(([playerId]) => playerId);
      
      await this.waitForBreak(CONSTANTS.TIE_BREAKER_COOLDOWN);
      return this.handleTieBreaker(gameId, tiedPlayers);
    }

    return winner;
  }

  async announceResult(gameId, winner, results) {
    const game = this.activeGames.get(gameId);
    if (!game) return;

    const announcement = {
      type: 'DICE_RESULT',
      gameId: gameId,
      results: results,
      winner: winner,
      winnerName: winner ? game.playerNames[winner] : null,
      hasWinner: !!winner,
      timestamp: Date.now()
    };

    if (winner) {
      const weekKey = this.getWeekKey();
      const winnerKey = `${CONSTANTS.DICE_WINNER_KEY}${weekKey}`;
      
      const weeklyData = this.kv.get(winnerKey) || { winners: [] };
      weeklyData.winners.push({
        playerId: winner,
        playerName: game.playerNames[winner],
        timestamp: Date.now(),
        gameId: gameId
      });
      
      if (weeklyData.winners.length > 100) {
        weeklyData.winners = weeklyData.winners.slice(-100);
      }
      
      this.kv.set(winnerKey, weeklyData);
      
      this.stats.totalWinners++;
      this.stats.lastWinner = winner;
      
      if (game.playersData && game.playersData[winner]) {
        game.playersData[winner].gamesWon = (game.playersData[winner].gamesWon || 0) + 1;
        game.playersData[winner].lastWin = Date.now();
      }
    }

    this.stats.lastGameId = gameId;
    this.stats.totalGames++;
    game.announcedAt = Date.now();
    game.winner = winner;
    this.activeGames.set(gameId, game);

    this.gameHistory.push({
      gameId: gameId,
      winner: winner,
      winnerName: winner ? game.playerNames[winner] : null,
      results: results,
      timestamp: Date.now(),
      playerCount: game.players.length
    });

    if (this.gameHistory.length > 100) {
      this.gameHistory.shift();
    }

    this.broadcastToRoom(announcement);
  }

  async waitForBreak(duration) {
    return new Promise(resolve => {
      setTimeout(resolve, duration);
    });
  }

  async cleanupGame(gameId) {
    const game = this.activeGames.get(gameId);
    if (!game) return;

    if (this.gameTimers.has(gameId)) {
      clearTimeout(this.gameTimers.get(gameId));
      this.gameTimers.delete(gameId);
    }

    this.activeGames.delete(gameId);

    this.broadcastToRoom({
      type: 'DICE_GAME_CLEANUP',
      gameId: gameId,
      timestamp: Date.now()
    });

    this.scheduleNextGame();
  }

  async handleGameError(gameId) {
    await this.waitForBreak(CONSTANTS.ERROR_RECOVERY_DELAY_MS);
    await this.cleanupGame(gameId);
  }

  scheduleNextGame() {
    if (this.activeGames.size >= CONSTANTS.MAX_DICE_GAMES) {
      return;
    }

    setTimeout(() => {
      this.startNewGame();
    }, CONSTANTS.DICE_AUTO_START_DELAY_MS);
  }

  async startNewGame() {
    if (this.isProcessing) return;
    if (this.activeGames.size >= CONSTANTS.MAX_DICE_GAMES) return;
    if (!this.isRunning) return;

    this.isProcessing = true;

    try {
      const gameId = this.generateGameId();
      const players = this.getAvailablePlayers();
      
      if (players.length < CONSTANTS.DICE_MIN_PLAYERS_TO_AUTO_START) {
        this.isProcessing = false;
        return;
      }

      const game = {
        id: gameId,
        players: players.map(p => p.id),
        playerNames: Object.fromEntries(players.map(p => [p.id, p.name])),
        playersData: Object.fromEntries(players.map(p => [p.id, { 
          name: p.name, 
          gamesPlayed: 0,
          gamesWon: 0,
          lastWin: null
        }])),
        startedAt: Date.now(),
        status: 'active',
        results: null,
        rolledAt: null,
        announcedAt: null,
        winner: null
      };

      this.activeGames.set(gameId, game);
      this.stats.totalPlayers += players.length;

      this.broadcastToRoom({
        type: 'DICE_GAME_START',
        gameId: gameId,
        players: players,
        timestamp: Date.now()
      });

      await this.startDiceGame(gameId);

    } catch (error) {
      // Silent error handling
    } finally {
      this.isProcessing = false;
    }
  }

  broadcastToRoom(message) {
    if (this.ws && this.ws.broadcastToRoom) {
      this.ws.broadcastToRoom(CONSTANTS.DICE_ROOM, message);
    }
  }

  generateGameId() {
    return `dice_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  getAvailablePlayers() {
    if (this.ws && this.ws.getClientsInRoom) {
      const clients = this.ws.getClientsInRoom(CONSTANTS.DICE_ROOM) || [];
      return clients.map(client => ({
        id: client.id || `player_${Math.random().toString(36).substr(2, 6)}`,
        name: client.name || `Player_${Math.random().toString(36).substr(2, 6)}`
      }));
    }
    
    return [
      { id: 'player1', name: 'Player 1' },
      { id: 'player2', name: 'Player 2' }
    ];
  }

  getWeekKey() {
    const now = new Date();
    const year = now.getFullYear();
    const week = this.getWeekNumber(now);
    return `${year}_W${String(week).padStart(2, '0')}`;
  }

  getWeekNumber(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    d.setDate(d.getDate() + 3 - (d.getDay() + 6) % 7);
    const week1 = new Date(d.getFullYear(), 0, 4);
    return 1 + Math.round(((d - week1) / 86400000 - 3 + (week1.getDay() + 6) % 7) / 7);
  }

  checkWeeklyReset() {
    const weekKey = this.getWeekKey();
    const lastResetKey = CONSTANTS.DICE_LAST_RESET_WEEK;
    const lastReset = this.kv.get(lastResetKey);

    if (lastReset !== weekKey) {
      const winnerKey = `${CONSTANTS.DICE_WINNER_KEY}${weekKey}`;
      this.kv.set(winnerKey, { winners: [] });
      this.kv.set(lastResetKey, weekKey);
    }
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    
    this.checkWeeklyReset();
    
    this.scheduleInterval = setInterval(() => {
      this.checkWeeklyReset();
      if (this.activeGames.size === 0) {
        this.startNewGame();
      }
    }, CONSTANTS.WEEKLY_RESET_CHECK_INTERVAL_MS);

    setTimeout(() => {
      this.startNewGame();
    }, CONSTANTS.DICE_AUTO_START_DELAY_MS);
  }

  stop() {
    this.isRunning = false;
    if (this.scheduleInterval) {
      clearInterval(this.scheduleInterval);
      this.scheduleInterval = null;
    }
    for (const [gameId, game] of this.activeGames) {
      this.cleanupGame(gameId);
    }
  }

  getStats() {
    return {
      ...this.stats,
      activeGames: this.activeGames.size,
      historyLength: this.gameHistory.length,
      isRunning: this.isRunning
    };
  }

  getHistory() {
    return this.gameHistory;
  }

  getActiveGames() {
    return Array.from(this.activeGames.values()).map(game => ({
      id: game.id,
      players: game.players.length,
      startedAt: game.startedAt,
      status: game.status,
      winner: game.winner
    }));
  }

  getPlayerStats(playerId) {
    let totalGames = 0;
    let totalWins = 0;
    let totalRolls = 0;
    
    for (const game of this.gameHistory) {
      if (game.results && game.results[playerId]) {
        totalGames++;
        totalRolls++;
        if (game.winner === playerId) {
          totalWins++;
        }
      }
    }
    
    return {
      playerId: playerId,
      totalGames: totalGames,
      totalWins: totalWins,
      winRate: totalGames > 0 ? (totalWins / totalGames) * 100 : 0,
      totalRolls: totalRolls
    };
  }
}

// ==================== QUIZ SYSTEM ====================
class QuizSystem {
  constructor(kvCache, wsServer) {
    this.kv = kvCache;
    this.ws = wsServer;
    this.questions = [];
    this.currentQuestion = null;
    this.isRunning = false;
    this.currentSession = null;
    this.scores = new Map();
    this.answeredPlayers = new Set();
    this.questionIndex = 0;
    this.timer = null;
    this.isProcessing = false;
    this.totalQuestionsAsked = 0;
    this.correctAnswers = 0;
    this.wrongAnswers = 0;
    this.sessionStartTime = null;
    this.questionStartTime = null;
  }

  async initQuestions() {
    const cachedQuestions = this.kv.get('quiz_questions');
    if (cachedQuestions && cachedQuestions.length > 0) {
      this.questions = cachedQuestions;
    } else {
      this.questions = this.getDefaultQuestions();
      this.kv.set('quiz_questions', this.questions);
    }
  }

  getDefaultQuestions() {
    return [
      {
        id: 1,
        question: 'Apa ibu kota Indonesia?',
        options: ['Jakarta', 'Bandung', 'Surabaya', 'Medan'],
        correct: 0,
        category: 'Geografi',
        difficulty: 'easy',
        points: 10
      },
      {
        id: 2,
        question: 'Berapa hasil dari 2 + 2?',
        options: ['3', '4', '5', '6'],
        correct: 1,
        category: 'Matematika',
        difficulty: 'easy',
        points: 10
      },
      {
        id: 3,
        question: 'Siapa presiden pertama Indonesia?',
        options: ['Soeharto', 'Soekarno', 'Habibie', 'Gus Dur'],
        correct: 1,
        category: 'Sejarah',
        difficulty: 'medium',
        points: 15
      },
      {
        id: 4,
        question: 'Apa warna bendera Indonesia?',
        options: ['Merah-Putih', 'Merah-Kuning', 'Biru-Putih', 'Hijau-Kuning'],
        correct: 0,
        category: 'Pengetahuan Umum',
        difficulty: 'easy',
        points: 10
      },
      {
        id: 5,
        question: 'Berapa jumlah provinsi di Indonesia?',
        options: ['34', '33', '35', '32'],
        correct: 0,
        category: 'Geografi',
        difficulty: 'medium',
        points: 15
      },
      {
        id: 6,
        question: 'Apa bahasa resmi Indonesia?',
        options: ['Bahasa Indonesia', 'Bahasa Jawa', 'Bahasa Sunda', 'Bahasa Inggris'],
        correct: 0,
        category: 'Bahasa',
        difficulty: 'easy',
        points: 10
      },
      {
        id: 7,
        question: 'Siapa penemu lampu?',
        options: ['Thomas Edison', 'Nikola Tesla', 'Albert Einstein', 'Alexander Bell'],
        correct: 0,
        category: 'Sains',
        difficulty: 'medium',
        points: 15
      },
      {
        id: 8,
        question: 'Berapa diameter bumi?',
        options: ['12.742 km', '10.000 km', '15.000 km', '8.000 km'],
        correct: 0,
        category: 'Sains',
        difficulty: 'hard',
        points: 20
      },
      {
        id: 9,
        question: 'Apa gunung tertinggi di Indonesia?',
        options: ['Puncak Jaya', 'Gunung Merapi', 'Gunung Semeru', 'Gunung Rinjani'],
        correct: 0,
        category: 'Geografi',
        difficulty: 'medium',
        points: 15
      },
      {
        id: 10,
        question: 'Siapa penulis novel "Laskar Pelangi"?',
        options: ['Andrea Hirata', 'Pramoedya', 'Tere Liye', 'Dewi Lestari'],
        correct: 0,
        category: 'Sastra',
        difficulty: 'medium',
        points: 15
      }
    ];
  }

  getCurrentSession() {
    const now = new Date();
    const hours = now.getHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET;
    
    for (const session of QUIZ_SCHEDULE.SESSIONS) {
      if (hours >= session.start && hours < session.end) {
        return session;
      }
    }
    return null;
  }

  async startQuiz() {
    if (this.isRunning) return;
    
    const session = this.getCurrentSession();
    if (!session) {
      this.scheduleNextQuiz();
      return;
    }
    
    this.isRunning = true;
    this.currentSession = session;
    this.questionIndex = 0;
    this.scores.clear();
    this.answeredPlayers.clear();
    this.totalQuestionsAsked = 0;
    this.correctAnswers = 0;
    this.wrongAnswers = 0;
    this.sessionStartTime = Date.now();
    
    await this.broadcastQuizStart();
    setTimeout(() => {
      this.nextQuestion();
    }, CONSTANTS.QUIZ_START_DELAY_MS);
  }

  async nextQuestion() {
    if (this.questionIndex >= this.questions.length) {
      await this.endQuiz();
      return;
    }

    this.currentQuestion = this.questions[this.questionIndex];
    this.answeredPlayers.clear();
    this.questionStartTime = Date.now();
    this.totalQuestionsAsked++;
    
    await this.broadcastQuestion(this.currentQuestion);
    
    this.timer = setTimeout(() => {
      this.handleQuestionTimeout();
    }, CONSTANTS.QUIZ_NEXT_QUESTION_DELAY_MS);
  }

  async handleQuestionTimeout() {
    if (!this.currentQuestion) return;
    
    const correctAnswer = this.currentQuestion.options[this.currentQuestion.correct];
    
    await this.broadcastToRoom({
      type: 'QUIZ_ANSWER_REVEAL',
      questionId: this.currentQuestion.id,
      correctAnswer: correctAnswer,
      answeredCount: this.answeredPlayers.size,
      totalPlayers: this.getTotalPlayers()
    });
    
    this.questionIndex++;
    
    setTimeout(() => {
      this.nextQuestion();
    }, CONSTANTS.QUIZ_SWITCH_DELAY_MS);
  }

  async submitAnswer(playerId, answerIndex) {
    if (!this.currentQuestion) return;
    if (this.answeredPlayers.has(playerId)) return;
    
    this.answeredPlayers.add(playerId);
    const isCorrect = answerIndex === this.currentQuestion.correct;
    
    if (isCorrect) {
      const points = this.currentQuestion.points || 10;
      const currentScore = this.scores.get(playerId) || 0;
      this.scores.set(playerId, currentScore + points);
      this.correctAnswers++;
    } else {
      this.wrongAnswers++;
    }
    
    await this.broadcastToRoom({
      type: 'QUIZ_ANSWER_SUBMITTED',
      playerId: playerId,
      isCorrect: isCorrect,
      score: this.scores.get(playerId) || 0,
      points: isCorrect ? (this.currentQuestion.points || 10) : 0
    });
  }

  async endQuiz() {
    this.isRunning = false;
    this.currentQuestion = null;
    
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    
    const winner = this.getWinner();
    const allScores = Array.from(this.scores.entries()).map(([id, score]) => ({
      playerId: id,
      score: score
    }));
    
    await this.broadcastToRoom({
      type: 'QUIZ_END',
      scores: allScores,
      winner: winner,
      totalQuestions: this.totalQuestionsAsked,
      correctAnswers: this.correctAnswers,
      wrongAnswers: this.wrongAnswers,
      duration: Date.now() - this.sessionStartTime,
      timestamp: Date.now()
    });
    
    if (winner) {
      const weekKey = this.getWeekKey();
      const winnerKey = `quiz_winner_${weekKey}`;
      const weeklyData = this.kv.get(winnerKey) || { winners: [] };
      weeklyData.winners.push({
        playerId: winner,
        score: this.scores.get(winner) || 0,
        timestamp: Date.now()
      });
      this.kv.set(winnerKey, weeklyData);
    }
    
    this.scheduleNextQuiz();
  }

  getWinner() {
    let highestScore = 0;
    let winner = null;
    
    for (const [playerId, score] of this.scores) {
      if (score > highestScore) {
        highestScore = score;
        winner = playerId;
      }
    }
    
    return winner;
  }

  getTotalPlayers() {
    if (this.ws && this.ws.getClientsInRoom) {
      return this.ws.getClientsInRoom(CONSTANTS.DICE_ROOM).length;
    }
    return 0;
  }

  async broadcastQuizStart() {
    await this.broadcastToRoom({
      type: 'QUIZ_START',
      totalQuestions: this.questions.length,
      session: this.currentSession,
      timestamp: Date.now()
    });
  }

  async broadcastQuestion(question) {
    await this.broadcastToRoom({
      type: 'QUIZ_QUESTION',
      questionId: question.id,
      question: question.question,
      options: question.options,
      category: question.category,
      difficulty: question.difficulty,
      points: question.points,
      timestamp: Date.now()
    });
  }

  async broadcastToRoom(message) {
    if (this.ws && this.ws.broadcastToRoom) {
      this.ws.broadcastToRoom(CONSTANTS.DICE_ROOM, message);
    }
  }

  scheduleNextQuiz() {
    setTimeout(() => {
      this.startQuiz();
    }, CONSTANTS.QUIZ_START_DELAY_MS);
  }

  getWeekKey() {
    const now = new Date();
    const year = now.getFullYear();
    const week = this.getWeekNumber(now);
    return `${year}_W${String(week).padStart(2, '0')}`;
  }

  getWeekNumber(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    d.setDate(d.getDate() + 3 - (d.getDay() + 6) % 7);
    const week1 = new Date(d.getFullYear(), 0, 4);
    return 1 + Math.round(((d - week1) / 86400000 - 3 + (week1.getDay() + 6) % 7) / 7);
  }

  start() {
    this.initQuestions();
    this.startQuiz();
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  getQuestions() {
    return this.questions;
  }

  addQuestion(question) {
    this.questions.push({
      id: this.questions.length + 1,
      ...question
    });
    this.kv.set('quiz_questions', this.questions);
  }

  removeQuestion(questionId) {
    this.questions = this.questions.filter(q => q.id !== questionId);
    this.kv.set('quiz_questions', this.questions);
  }

  getStats() {
    return {
      isRunning: this.isRunning,
      totalQuestions: this.questions.length,
      currentQuestion: this.currentQuestion ? this.currentQuestion.id : null,
      totalQuestionsAsked: this.totalQuestionsAsked,
      correctAnswers: this.correctAnswers,
      wrongAnswers: this.wrongAnswers,
      playersAnswered: this.answeredPlayers.size,
      scores: Array.from(this.scores.entries()).map(([id, score]) => ({ playerId: id, score }))
    };
  }
}

// ==================== WEB SOCKET SERVER ====================
class WebSocketServer {
  constructor() {
    this.clients = new Map();
    this.rooms = new Map();
    this.eventQueue = [];
    this.isProcessing = false;
    this.messageHandlers = new Map();
    this.maxClients = CONSTANTS.MAX_WS_CLIENTS;
  }

  addClient(clientId, ws, name = null) {
    if (this.clients.size >= this.maxClients) {
      throw new Error('Max clients reached');
    }
    
    this.clients.set(clientId, {
      id: clientId,
      ws: ws,
      name: name || `Player_${clientId.substr(0, 6)}`,
      connected: true,
      room: null,
      lastActivity: Date.now(),
      messagesReceived: 0,
      messagesSent: 0
    });
  }

  removeClient(clientId) {
    const client = this.clients.get(clientId);
    if (client && client.room) {
      this.leaveRoom(clientId, client.room);
    }
    this.clients.delete(clientId);
  }

  joinRoom(clientId, roomName) {
    const client = this.clients.get(clientId);
    if (!client) return false;
    
    if (client.room) {
      this.leaveRoom(clientId, client.room);
    }
    
    if (!this.rooms.has(roomName)) {
      this.rooms.set(roomName, new Set());
    }
    
    this.rooms.get(roomName).add(clientId);
    client.room = roomName;
    client.lastActivity = Date.now();
    
    return true;
  }

  leaveRoom(clientId, roomName) {
    const client = this.clients.get(clientId);
    if (client) {
      client.room = null;
      client.lastActivity = Date.now();
    }
    
    const room = this.rooms.get(roomName);
    if (room) {
      room.delete(clientId);
      if (room.size === 0) {
        this.rooms.delete(roomName);
      }
    }
  }

  broadcastToRoom(roomName, message) {
    const room = this.rooms.get(roomName);
    if (!room) return;
    
    const messageStr = JSON.stringify(message);
    
    for (const clientId of room) {
      const client = this.clients.get(clientId);
      if (client && client.ws && client.connected) {
        try {
          client.ws.send(messageStr);
          client.lastActivity = Date.now();
          client.messagesSent++;
        } catch (error) {
          // Silent error
        }
      }
    }
  }

  sendToClient(clientId, message) {
    const client = this.clients.get(clientId);
    if (!client || !client.ws || !client.connected) return false;
    
    try {
      client.ws.send(JSON.stringify(message));
      client.lastActivity = Date.now();
      client.messagesSent++;
      return true;
    } catch (error) {
      return false;
    }
  }

  getClientsInRoom(roomName) {
    const room = this.rooms.get(roomName);
    if (!room) return [];
    
    return Array.from(room)
      .map(clientId => this.clients.get(clientId))
      .filter(client => client && client.connected);
  }

  getClient(clientId) {
    return this.clients.get(clientId) || null;
  }

  getClientCount() {
    return this.clients.size;
  }

  getRoomCount() {
    return this.rooms.size;
  }

  getRoomSize(roomName) {
    const room = this.rooms.get(roomName);
    return room ? room.size : 0;
  }

  isClientConnected(clientId) {
    const client = this.clients.get(clientId);
    return client && client.connected;
  }

  updateClientName(clientId, newName) {
    const client = this.clients.get(clientId);
    if (client) {
      client.name = newName;
      client.lastActivity = Date.now();
      return true;
    }
    return false;
  }

  handleDisconnect(clientId) {
    this.removeClient(clientId);
  }

  handleMessage(clientId, message) {
    const client = this.clients.get(clientId);
    if (!client) return;
    
    client.messagesReceived++;
    client.lastActivity = Date.now();
    
    const handler = this.messageHandlers.get(message.type);
    if (handler) {
      try {
        handler(clientId, message.data);
      } catch (error) {
        // Silent error
      }
    }
  }

  registerMessageHandler(type, handler) {
    this.messageHandlers.set(type, handler);
  }

  cleanupIdleClients(timeout = CONSTANTS.MAX_IDLE_TIME_MS) {
    const now = Date.now();
    const toRemove = [];
    
    for (const [clientId, client] of this.clients) {
      if (now - client.lastActivity > timeout) {
        toRemove.push(clientId);
      }
    }
    
    for (const clientId of toRemove) {
      this.removeClient(clientId);
    }
  }

  getStats() {
    return {
      totalClients: this.clients.size,
      totalRooms: this.rooms.size,
      maxClients: this.maxClients,
      rooms: Array.from(this.rooms.entries()).map(([name, clients]) => ({
        name: name,
        clientCount: clients.size
      })),
      totalMessagesReceived: Array.from(this.clients.values()).reduce((sum, c) => sum + c.messagesReceived, 0),
      totalMessagesSent: Array.from(this.clients.values()).reduce((sum, c) => sum + c.messagesSent, 0)
    };
  }
}

// ==================== GAME SERVER MAIN CLASS ====================
class GameServer {
  constructor() {
    this.kv = new KVCache();
    this.ws = new WebSocketServer();
    this.diceSystem = new DiceGameSystem(this.kv, this.ws);
    this.quizSystem = new QuizSystem(this.kv, this.ws);
    this.lowcardGames = new Map();
    this.isRunning = false;
    this.cleanupInterval = null;
    this.healthCheckInterval = null;
    this.errorCount = 0;
    this.lastErrorReset = Date.now();
    this.startTime = null;
    this.totalErrors = 0;
    this.uptime = 0;
  }

  start() {
    if (this.isRunning) return;
    
    this.isRunning = true;
    this.startTime = Date.now();
    
    this.diceSystem.start();
    this.quizSystem.start();
    
    this.cleanupInterval = setInterval(() => {
      this.cleanup();
    }, CONSTANTS.GAME_CLEANUP_DELAY_MS);
    
    this.healthCheckInterval = setInterval(() => {
      this.healthCheck();
    }, CONSTANTS.HEALTH_CHECK_INTERVAL_MS);
  }

  stop() {
    this.isRunning = false;
    this.uptime = Date.now() - this.startTime;
    
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
    
    this.diceSystem.stop();
    this.quizSystem.stop();
    
    this.cleanup();
  }

  cleanup() {
    this.ws.cleanupIdleClients();
    
    for (const [gameId, game] of this.lowcardGames) {
      if (game.isStale() || game.isRegistrationStuck() || game.isDrawStuck()) {
        this.lowcardGames.delete(gameId);
      }
    }
    
    if (this.diceSystem.gameHistory.length > 100) {
      this.diceSystem.gameHistory = this.diceSystem.gameHistory.slice(-100);
    }
  }

  healthCheck() {
    try {
      const now = Date.now();
      
      if (now - this.lastErrorReset > CONSTANTS.ERROR_RESET_INTERVAL_MS) {
        this.errorCount = 0;
        this.lastErrorReset = now;
      }
      
      if (this.errorCount > CONSTANTS.MAX_UNHANDLED_ERRORS) {
        this.recover();
      }
      
    } catch (error) {
      this.errorCount++;
      this.totalErrors++;
    }
  }

  recover() {
    try {
      this.stop();
      setTimeout(() => {
        this.start();
      }, CONSTANTS.ERROR_RECOVERY_DELAY_MS);
    } catch (error) {
      // Silent error
    }
  }

  createLowcardGame(hostId) {
    const gameId = `lowcard_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    
    if (this.lowcardGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
      throw new Error('Max lowcard games reached');
    }
    
    const game = new LowcardGame(gameId, hostId);
    this.lowcardGames.set(gameId, game);
    
    return game;
  }

  joinLowcardGame(gameId, player) {
    const game = this.lowcardGames.get(gameId);
    if (!game) {
      throw new Error('Game not found');
    }
    
    return game.addPlayer(player);
  }

  leaveLowcardGame(gameId, playerId) {
    const game = this.lowcardGames.get(gameId);
    if (game) {
      game.removePlayer(playerId);
      if (game.getTotalPlayers() === 0) {
        this.lowcardGames.delete(gameId);
      }
    }
  }

  addBotToLowcardGame(gameId) {
    const game = this.lowcardGames.get(gameId);
    if (!game) {
      throw new Error('Game not found');
    }
    
    const botId = `bot_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
    const bot = new Bot(botId, `Bot_${Math.random().toString(36).substr(2, 4)}`);
    
    return game.addBot(bot);
  }

  getLowcardGame(gameId) {
    return this.lowcardGames.get(gameId) || null;
  }

  getAllLowcardGames() {
    return Array.from(this.lowcardGames.values());
  }

  getActiveLowcardGames() {
    return Array.from(this.lowcardGames.values())
      .filter(game => game.status === 'registration' || game.status === 'drawing');
  }

  getDiceStats() {
    return this.diceSystem.getStats();
  }

  getDiceHistory() {
    return this.diceSystem.getHistory();
  }

  getDiceActiveGames() {
    return this.diceSystem.getActiveGames();
  }

  getQuizQuestions() {
    return this.quizSystem.getQuestions();
  }

  addQuizQuestion(question) {
    this.quizSystem.addQuestion(question);
  }

  removeQuizQuestion(questionId) {
    this.quizSystem.removeQuestion(questionId);
  }

  getCurrentQuizQuestion() {
    return this.quizSystem.currentQuestion;
  }

  getQuizScores() {
    return Array.from(this.quizSystem.scores.entries()).map(([id, score]) => ({
      playerId: id,
      score: score
    }));
  }

  getWinners(limit = 10) {
    const allWinners = {};
    const weekKey = this.getWeekKey();
    
    // Dice winners
    const diceWinnerKey = `${CONSTANTS.DICE_WINNER_KEY}${weekKey}`;
    const diceWinners = this.kv.get(diceWinnerKey) || { winners: [] };
    allWinners.dice = diceWinners.winners.slice(-limit);
    
    // Quiz winners
    const quizWinnerKey = `quiz_winner_${weekKey}`;
    const quizWinners = this.kv.get(quizWinnerKey) || { winners: [] };
    allWinners.quiz = quizWinners.winners.slice(-limit);
    
    // Lowcard winners
    const lowcardWinnerKey = `${CONSTANTS.LOWCARD_WINNER_KEY}${weekKey}`;
    const lowcardWinners = this.kv.get(lowcardWinnerKey) || { winners: [] };
    allWinners.lowcard = lowcardWinners.winners.slice(-limit);
    
    return allWinners;
  }

  getServerStats() {
    return {
      isRunning: this.isRunning,
      uptime: this.isRunning ? Date.now() - this.startTime : this.uptime,
      wsClients: this.ws.getClientCount(),
      wsRooms: this.ws.getRoomCount(),
      lowcardGames: this.lowcardGames.size,
      diceGames: this.diceSystem.activeGames.size,
      diceTotalGames: this.diceSystem.stats.totalGames,
      diceTotalWinners: this.diceSystem.stats.totalWinners,
      quizRunning: this.quizSystem.isRunning,
      quizQuestions: this.quizSystem.questions.length,
      errorCount: this.errorCount,
      totalErrors: this.totalErrors,
      startTime: this.startTime
    };
  }

  getWeekKey() {
    const now = new Date();
    const year = now.getFullYear();
    const week = this.getWeekNumber(now);
    return `${year}_W${String(week).padStart(2, '0')}`;
  }

  getWeekNumber(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    d.setDate(d.getDate() + 3 - (d.getDay() + 6) % 7);
    const week1 = new Date(d.getFullYear(), 0, 4);
    return 1 + Math.round(((d - week1) / 86400000 - 3 + (week1.getDay() + 6) % 7) / 7);
  }

  broadcastToRoom(roomName, message) {
    this.ws.broadcastToRoom(roomName, message);
  }

  sendToClient(clientId, message) {
    return this.ws.sendToClient(clientId, message);
  }

  registerMessageHandler(type, handler) {
    this.ws.registerMessageHandler(type, handler);
  }

  getClient(clientId) {
    return this.ws.getClient(clientId);
  }

  getClientsInRoom(roomName) {
    return this.ws.getClientsInRoom(roomName);
  }
}

// ==================== EXPORT ====================
module.exports = {
  CONSTANTS,
  KVCache,
  Player,
  Bot,
  LowcardGame,
  DiceGameSystem,
  QuizSystem,
  WebSocketServer,
  GameServer,
  QUIZ_SCHEDULE,
  DICE_ROOM
};
