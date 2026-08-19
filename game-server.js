// ==================== GAME-SERVER.JS ====================
// VERSION: 4.0.0 - PURE WORKER (NO DO, TETAP KV)

const CONSTANTS = {
  MAX_LOWCARD_GAMES: 10,
  REGISTRATION_TIME_MS: 20000,
  DRAW_TIME_MS: 20000,
  EVALUATION_DELAY_MS: 2000,
  MAX_BOTS_PER_GAME: 4,
  MAX_BET: 100000,
  MAX_BOT_DRAWS_PER_ROUND: 4,
  EVALUATION_TIMEOUT_MS: 30000,
  MAX_PLAYERS_PER_GAME: 45,
  GAME_CLEANUP_DELAY_MS: 5000,
  STALE_GAME_TIMEOUT_MS: 600000,
  STUCK_DRAW_TIMEOUT_MS: 60000,
  STUCK_REGISTRATION_TIMEOUT_MS: 30000,
  MAX_WS_CLIENTS: 50,
  MAX_EVENT_QUEUE_SIZE: 50,
  ERROR_RESET_INTERVAL_MS: 60000,
  LOWCARD_WINNER_KEY: 'lowcard_winner_',
  LOWCARD_RECORDING_KEY: 'lowcard_recording_status_',
  
  DICE_ANSWER_TIME_MS: 20000,
  DICE_TOTAL_TIME_MS: 20000,
  MAX_DICE_VALUE: 6,
  DICE_ROOM: "Quiz",
  DICE_POINT_KEY: 'dice_points',
  DICE_LAST_WEEK_WINNER: 'dice_last_week_winner',
  DICE_LAST_RESET_WEEK: 'dice_last_reset_week',
  
  DICE_AUTO_START_DELAY_MS: 3000,
  TIE_BREAKER_TIME_LIMIT: 20,
  TIE_BREAKER_COOLDOWN: 15000,
  
  KV_TIMEOUT_MS: 1500,
  BROADCAST_BATCH_SIZE: 10,
  CPU_YIELD_MS: 1,
  
  MAX_PROCESS_TIME_MS: 500,
  EVENT_BATCH_SIZE: 1,
  MAX_QUEUE_SIZE: 50,
  CLEANUP_INTERVAL_MS: 60000,
  HAND_SHAKE_TIMEOUT_MS: 3000,
  RATE_LIMIT_MAX: 100,
  RATE_LIMIT_WINDOW_MS: 60000,
  MAX_BOT_TIMEOUTS: 5,
  MAX_EVENT_ITERATIONS: 2,
  DICE_TIMER_INTERVAL_MS: 1000,
  MAP_CLEANUP_AGE_MS: 1800000,
  BAN_DURATION_MS: 180000,
  MAX_RECONNECT_ATTEMPTS: 5,
  RECONNECT_WINDOW_MS: 30000,
  
  DICE_CHECK_INTERVAL_MS: 10000,
  DICE_START_WINDOW_MS: 60000,
};

const QUIZ_SCHEDULE = {
  SESSIONS: [
    { start: 1, end: 2 },
    { start: 14, end: 15 },
    { start: 22, end: 23 }
  ],
  TIMEZONE_OFFSET: 8,
};

const DICE_ROOM = "Quiz";

// ==================== KV CACHE ====================
class KVCache {
  constructor() {
    this.cache = new Map();
  }

  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;
    return entry.value;
  }

  set(key, value) {
    this.cache.set(key, { value, timestamp: Date.now() });
  }

  delete(key) { this.cache.delete(key); }
  clear() { this.cache.clear(); }
  
  update(key, value) {
    if (this.cache.has(key)) {
      this.cache.set(key, { value, timestamp: Date.now() });
      return true;
    }
    return false;
  }
  
  has(key) {
    return this.cache.has(key);
  }
  
  keys() {
    return Array.from(this.cache.keys());
  }
}

// ==================== DICE GAME SYSTEM ====================
class DiceGameSystem {
  constructor(gameServer) {
    this.gameServer = gameServer;
    this.env = gameServer.env;
    this.userScores = new Map();
    this._isLoaded = false;
  }

  async loadScores() {
    try {
      if (this._isLoaded) return true;
      if (!this.env?.QUESTIONS) return false;
      
      const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      this._isLoaded = true;
      return true;
    } catch(e) { return false; }
  }

  async getPoints() {
    try {
      if (!this.env?.QUESTIONS) return {};
      const points = await this.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json') || {};
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      return points;
    } catch(e) { return {}; }
  }

  async setPoints(points) {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify(points));
      this.userScores.clear();
      for (const [username, score] of Object.entries(points)) {
        this.userScores.set(username, score);
      }
      return true;
    } catch(e) { return false; }
  }

  async getLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return null;
      if (this.gameServer._cachedLastWeekWinner !== null) {
        return this.gameServer._cachedLastWeekWinner;
      }
      const winnerData = await this.env.QUESTIONS.get(CONSTANTS.DICE_LAST_WEEK_WINNER, 'json');
      this.gameServer._cachedLastWeekWinner = winnerData;
      return winnerData;
    } catch(e) { return null; }
  }

  async deleteLastWeekWinner() {
    try {
      if (!this.env?.QUESTIONS) return false;
      await this.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER);
      this.gameServer._cachedLastWeekWinner = null;
      return true;
    } catch(e) { return false; }
  }

  rollDice() { return Math.floor(Math.random() * 6) + 1; }
  clearCache() { this.userScores.clear(); this._isLoaded = false; }
}

// ==================== 🔥 GLOBAL STATE (IN-MEMORY) ====================
const STATE = {
  // Core maps
  activeGames: new Map(),
  wsMap: new Map(),
  wsClients: new Map(),
  clientRooms: new Map(),
  userConnections: new Map(),
  
  // Dice state
  diceGameSystem: null,
  currentDiceRoll: null,
  _diceLock: false,
  _tieActive: false,
  diceAnswered: new Set(),
  _playerAnswers: new Map(),
  _isShowingDice: false,
  _diceTimeUpCooldown: false,
  _diceQuestionStartTime: null,
  _diceStartTime: null,
  _diceTimeout: null,
  _diceStartTimeout: null,
  _diceTimeUpCooldownTimer: null,
  _diceTimerInterval: null,
  diceAutoEnabled: false,
  diceHasWinner: false,
  diceWinner: null,
  diceEndNotified: false,
  _diceNotifiedFlags: { 20: false, 10: false, 5: false, timeup: false },
  _lastNotificationKey: "",
  _lastNotificationTime: 0,
  _lastSentRemaining: -1,
  _diceOutOfTimeShown: false,
  _diceTaskRunning: false,
  _canSubmitDiceAnswer: false,
  _diceRound: 0,
  
  // Tie breaker
  _tieBreakers: new Map(),
  _tieRound: 0,
  _tiePlayers: [],
  _tieAnswers: new Map(),
  _tieTimer: null,
  _tieInterval: null,
  _tieLock: false,
  
  // Queue
  _eventQueue: [],
  _isProcessingQueue: false,
  _allTimers: new Set(),
  _gameLocks: new Map(),
  _joinLocks: new Map(),
  _cleanupTimers: new Map(),
  _switchLocks: new Map(),
  _switchRetries: new Map(),
  
  // Circuit breaker
  _requestCount: 0,
  _lastResetTime: Date.now(),
  _circuitOpen: false,
  _errorCount: 0,
  _lastErrorReset: Date.now(),
  _tickCount: 0,
  
  // Reconnect protection
  _reconnectAttempts: new Map(),
  _bannedUsers: new Map(),
  
  // Cache
  _cachedResetWeek: null,
  _cachedLastWeekWinner: null,
  _recordingEnabled: new Map(),
  _kvCache: new KVCache(),
  
  // Counters
  _wsIdCounter: 0,
  _startTime: Date.now(),
  _lastActivity: Date.now(),
  _initialized: false,
  
  // Intervals
  _mainInterval: null,
  _cleanupInterval: null,
  
  // Dice timers tracking
  _diceTimeLeftNotified: new Map(),
  _nextDiceNotified: new Map(),
  _diceJoinedNotified: new Map(),
};

// ==================== INIT DICE SYSTEM ====================
STATE.diceGameSystem = new DiceGameSystem(STATE);

// ==================== BACKGROUND TASKS ====================
setInterval(() => {
  STATE._tickCount++;
  doTick();
}, 10000);

setInterval(() => {
  performCleanup();
}, 60000);

// ==================== DO TICK ====================
function doTick() {
  try {
    const tick = STATE._tickCount % 3;
    switch(tick) {
      case 0: 
        cleanupDeadConnections();
        checkDice();
        break;
      case 1: 
        checkStuckGames();
        break;
      case 2: 
        cleanupMemory();
        break;
    }
  } catch(e) {}
}

// ==================== CHECK DICE ====================
function checkDice() {
  try {
    if (STATE._tieActive || STATE._isShowingDice || STATE._diceTimeUpCooldown) return;
    if (!isDiceTime()) return;
    if (STATE.currentDiceRoll || STATE._diceTimeout || STATE._diceLock) return;
    
    const clients = STATE.wsClients?.get(DICE_ROOM);
    if (clients?.size > 0) {
      startDiceFast();
    }
  } catch(e) {}
}

// ==================== IS DICE TIME ====================
function isDiceTime() {
  try {
    const witaTime = getCurrentWITATime();
    const currentTotal = witaTime.totalMinutes;
    for (const session of QUIZ_SCHEDULE.SESSIONS) {
      const startTotal = session.start * 60;
      const endTotal = session.end * 60;
      if (currentTotal >= startTotal && currentTotal < endTotal) return true;
    }
    return false;
  } catch(e) { return false; }
}

function getCurrentWITATime() {
  try {
    const now = new Date();
    const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
    const minutes = now.getUTCMinutes();
    return { hours, minutes, totalMinutes: (hours * 60) + minutes };
  } catch(e) { return { hours: 0, minutes: 0, totalMinutes: 0 }; }
}

// ==================== START DICE ====================
function startDiceFast() {
  try {
    if (STATE._diceLock || STATE.currentDiceRoll || STATE._isShowingDice) return;
    STATE._diceLock = true;
    STATE._isShowingDice = true;
    
    const value = Math.floor(Math.random() * 6) + 1;
    STATE._diceRound = (STATE._diceRound || 0) + 1;
    STATE.currentDiceRoll = { value, timestamp: Date.now(), round: STATE._diceRound };
    STATE._diceStartTime = Date.now();
    STATE._diceQuestionStartTime = Date.now();
    STATE._canSubmitDiceAnswer = true;
    STATE.diceAnswered = new Set();
    STATE._playerAnswers = new Map();
    STATE.diceHasWinner = false;
    STATE.diceWinner = null;
    
    broadcastToRoom(DICE_ROOM, ["diceRoll", { 
      value, 
      timestamp: Date.now(),
      answerTime: 20,
      canAnswerNow: true,
      round: STATE._diceRound
    }]);
    
    broadcastToRoom(DICE_ROOM, ["diceNotification", "♡ clik draw ♡"]);
    
    let timeLeft = 20;
    const timerInterval = setInterval(() => {
      timeLeft--;
      if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
        broadcastToRoom(DICE_ROOM, ["diceNotification", `${timeLeft}s remaining`]);
      }
      if (timeLeft <= 0) {
        clearInterval(timerInterval);
        endDiceRound();
      }
    }, 1000);
    
    STATE._diceTimerInterval = timerInterval;
    STATE._allTimers.add(timerInterval);
    
    STATE._diceTimeout = setTimeout(() => {
      endDiceRound();
    }, 20000);
    STATE._allTimers.add(STATE._diceTimeout);
    
  } catch(e) {
    STATE._diceLock = false;
    STATE._isShowingDice = false;
  }
}

// ==================== END DICE ROUND ====================
async function endDiceRound() {
  try {
    if (STATE._diceTimerInterval) {
      clearInterval(STATE._diceTimerInterval);
      STATE._diceTimerInterval = null;
    }
    if (STATE._diceTimeout) {
      clearTimeout(STATE._diceTimeout);
      STATE._diceTimeout = null;
    }
    
    STATE._canSubmitDiceAnswer = false;
    STATE._isShowingDice = false;
    
    const diceValue = STATE.currentDiceRoll?.value;
    const roundNumber = STATE._diceRound || 1;
    
    const correctPlayers = [];
    for (const player of STATE.diceAnswered) {
      if (STATE._playerAnswers.get(player) === diceValue) {
        correctPlayers.push(player);
      }
    }
    
    if (correctPlayers.length === 0) {
      broadcastToRoom(DICE_ROOM, ["diceNoWinner", {
        message: "No winner",
        value: diceValue,
        round: roundNumber
      }]);
    } else if (correctPlayers.length === 1) {
      const winner = correctPlayers[0];
      
      try {
        const points = await getDicePoints();
        points[winner] = (points[winner] || 0) + 1;
        await STATE.diceGameSystem.setPoints(points);
        
        broadcastToRoom(DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: points[winner] || 0,
          diceValue: diceValue,
          round: roundNumber
        }]);
      } catch(e) {
        broadcastToRoom(DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: 0,
          diceValue: diceValue,
          round: roundNumber
        }]);
      }
    } else if (correctPlayers.length > 1 && !STATE._tieActive) {
      STATE.currentDiceRoll = null;
      STATE._diceLock = false;
      STATE._isShowingDice = false;
      
      await startTieBreaker(DICE_ROOM, correctPlayers);
      return;
    }
    
    STATE.currentDiceRoll = null;
    STATE._diceLock = false;
    STATE._diceTimeUpCooldown = true;
    
    setTimeout(() => {
      STATE._diceTimeUpCooldown = false;
      STATE._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      STATE._lastSentRemaining = -1;
      checkDice();
    }, 15000);
    
  } catch(e) {
    STATE._diceLock = false;
    STATE._isShowingDice = false;
  }
}

// ==================== TIE BREAKER ====================
async function startTieBreaker(room, players) {
  if (STATE._tieLock) return;
  STATE._tieLock = true;
  
  try {
    if (!players || players.length < 2 || STATE._tieActive) return;
    
    STATE._tieActive = true;
    STATE._tieRound = 0;
    STATE._tiePlayers = [...players];
    STATE._tieAnswers = new Map();
    
    const id = `tie_${Date.now()}`;
    STATE._tieBreakers.set(id, { 
      players, 
      round: 0, 
      winner: null, 
      status: 'waiting' 
    });
    
    await runTieRound(room, id, players);
    
  } finally {
    setTimeout(() => {
      STATE._tieLock = false;
    }, 2000);
  }
}

async function runTieRound(room, id, players) {
  const data = STATE._tieBreakers.get(id);
  if (!data) return;
  
  clearTimer(STATE._tieTimer);
  clearTimer(STATE._tieInterval);
  
  STATE._tieRound++;
  STATE._tiePlayers = [...players];
  STATE._tieAnswers = new Map();
  data.round = STATE._tieRound;
  data.status = 'running';
  data.players = players;
  
  STATE._diceQuestionStartTime = Date.now();
  STATE._canSubmitDiceAnswer = true;
  STATE.diceAnswered = new Set();
  STATE._playerAnswers = new Map();
  STATE._isShowingDice = true;
  STATE.diceHasWinner = false;
  STATE.diceWinner = null;
  
  broadcastToRoom(DICE_ROOM, ["diceNotification", 
    `♡ Tie Round ${STATE._tieRound}: ${players.join(', ')}`
  ]);
  
  startTieTimer(room, id, players);
}

function startTieTimer(room, id, players) {
  clearTimer(STATE._tieTimer);
  clearTimer(STATE._tieInterval);
  
  let timeLeft = CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20;
  let notified10 = false, notified5 = false, isProcessed = false;
  
  STATE._tieInterval = trackTimer(setInterval(() => {
    timeLeft--;
    if (timeLeft === 10 && !notified10) { 
      notified10 = true; 
      broadcastToRoom(DICE_ROOM, ["diceNotification", "10s remaining"]); 
    }
    if (timeLeft === 5 && !notified5) { 
      notified5 = true; 
      broadcastToRoom(DICE_ROOM, ["diceNotification", "5s remaining"]); 
    }
    if (timeLeft === 3) {
      broadcastToRoom(DICE_ROOM, ["diceNotification", "3s remaining"]);
    }
    
    if (timeLeft <= 0 && !isProcessed) {
      isProcessed = true;
      clearTimer(STATE._tieInterval);
      STATE._tieInterval = null;
      STATE._canSubmitDiceAnswer = false;
      STATE._isShowingDice = false;
      broadcastToRoom(DICE_ROOM, ["diceNotification", "TIME UP"]);
      
      const tieId = getActiveTieBreakerId();
      if (tieId) processTieResults(room, tieId, players);
      else { 
        resetTieBreakerState(null); 
        startCooldownAfterTieBreaker(); 
      }
    }
  }, 1000));
  
  STATE._tieTimer = trackTimer(setTimeout(() => {
    if (!isProcessed) {
      isProcessed = true;
      clearTimer(STATE._tieInterval);
      STATE._tieInterval = null;
      STATE._canSubmitDiceAnswer = false;
      STATE._isShowingDice = false;
      broadcastToRoom(DICE_ROOM, ["diceNotification", "TIME UP"]);
      
      const tieId = getActiveTieBreakerId();
      if (tieId) processTieResults(room, tieId, players);
      else { 
        resetTieBreakerState(null); 
        startCooldownAfterTieBreaker(); 
      }
    }
  }, (CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20) * 1000 + 2000));
}

async function processTieResults(room, id, players) {
  const data = STATE._tieBreakers.get(id);
  if (!data) return;
  
  let highest = 0, highestPlayers = [];
  for (const player of players) {
    const answer = STATE._tieAnswers.get(player);
    if (answer !== undefined && answer >= 1 && answer <= 6) {
      if (answer > highest) { 
        highest = answer; 
        highestPlayers = [player]; 
      } else if (answer === highest) {
        highestPlayers.push(player);
      }
    }
  }
  
  if (highestPlayers.length === 0) {
    broadcastToRoom(DICE_ROOM, ["diceNotification", "No one answered tie breaker"]);
    resetTieBreakerState(id);
    startCooldownAfterTieBreaker();
    return;
  }
  
  if (highestPlayers.length === 1) {
    const winner = highestPlayers[0];
    
    try {
      const points = await getDicePoints();
      points[winner] = (points[winner] || 0) + 1;
      await STATE.diceGameSystem.setPoints(points);
      
      broadcastToRoom(DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: points[winner] || 0,
        diceValue: highest,
        round: STATE._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: STATE._tieRound,
        finalWinner: true
      }]);
    } catch(e) {
      broadcastToRoom(DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: 0,
        diceValue: highest,
        round: STATE._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: STATE._tieRound,
        finalWinner: true
      }]);
    }
    
    resetTieBreakerState(id);
    startCooldownAfterTieBreaker();
    return;
  }
  
  if (highestPlayers.length > 1) {
    STATE._tiePlayers = highestPlayers;
    STATE._tieAnswers = new Map();
    data.players = highestPlayers;
    data.round = STATE._tieRound;
    data.status = 'waiting';
    
    const nextTimer = setTimeout(() => {
      if (STATE._tieActive && STATE._tiePlayers.length > 1) {
        runTieRound(room, id, STATE._tiePlayers);
      } else if (STATE._tiePlayers.length === 1) {
        processSingleWinner(room, id, STATE._tiePlayers[0]);
      }
    }, 2000);
    trackTimer(nextTimer);
    return;
  }
  
  resetTieBreakerState(id);
  startCooldownAfterTieBreaker();
}

async function processSingleWinner(room, id, winner) {
  try {
    const points = await getDicePoints();
    points[winner] = (points[winner] || 0) + 1;
    await STATE.diceGameSystem.setPoints(points);
    
    broadcastToRoom(DICE_ROOM, ["diceWinner", {
      username: winner,
      totalPoints: points[winner] || 0,
      diceValue: 'auto',
      round: STATE._diceRound || 1,
      isTieBreaker: true,
      tieBreakerRound: STATE._tieRound,
      finalWinner: true
    }]);
  } catch(e) {
    broadcastToRoom(DICE_ROOM, ["diceWinner", {
      username: winner,
      totalPoints: 0,
      diceValue: 'auto',
      round: STATE._diceRound || 1,
      isTieBreaker: true,
      tieBreakerRound: STATE._tieRound,
      finalWinner: true
    }]);
  }
  
  resetTieBreakerState(id);
  startCooldownAfterTieBreaker();
}

function startCooldownAfterTieBreaker() {
  broadcastToRoom(DICE_ROOM, ["diceNotification", "wait 15s"]);
  STATE._diceTimeUpCooldown = true;
  
  clearTimer(STATE._diceTimeUpCooldownTimer);
  STATE._diceTimeUpCooldownTimer = trackTimer(setTimeout(() => {
    STATE._diceTimeUpCooldownTimer = null;
    STATE._diceTimeUpCooldown = false;
    STATE._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
    STATE._lastSentRemaining = -1;
    STATE._lastNotificationKey = "";
    STATE._lastNotificationTime = 0;
    checkDice();
  }, CONSTANTS.TIE_BREAKER_COOLDOWN || 15000));
}

function resetTieBreakerState(id) {
  if (id) STATE._tieBreakers.delete(id);
  STATE._tieActive = false;
  STATE._tiePlayers = [];
  STATE._tieAnswers = new Map();
  STATE._tieRound = 0;
  STATE._canSubmitDiceAnswer = false;
  STATE._isShowingDice = false;
  STATE.currentDiceRoll = null;
  STATE.diceAnswered = new Set();
  STATE._playerAnswers = new Map();
  STATE.diceHasWinner = false;
  STATE.diceWinner = null;
  
  clearTimer(STATE._tieTimer);
  clearTimer(STATE._tieInterval);
  STATE._tieTimer = null;
  STATE._tieInterval = null;
}

function getActiveTieBreakerId() {
  for (const [id, data] of STATE._tieBreakers) {
    if (data.status === 'waiting' || data.status === 'running') return id;
  }
  return null;
}

// ==================== SUBMIT DICE ANSWER ====================
async function submitDiceAnswer(ws, username, guess) {
  try {
    if (!ws || !username) return;
    if (!STATE._canSubmitDiceAnswer) return;
    if (STATE.diceAnswered.has(username)) return;
    
    const guessValue = parseInt(guess, 10);
    if (isNaN(guessValue) || guessValue < 1 || guessValue > 6) {
      safeSend(ws, ["diceError", "invalid guess 1-6"]);
      return;
    }
    
    if (STATE._tieActive) {
      if (!STATE._tiePlayers.includes(username)) {
        safeSend(ws, ["diceError", "You are not in tie breaker"]);
        return;
      }
      if (STATE._tieAnswers.has(username)) {
        safeSend(ws, ["diceError", "You already answered"]);
        return;
      }
      
      STATE._tieAnswers.set(username, guessValue);
      STATE.diceAnswered.add(username);
      
      broadcastToRoom(DICE_ROOM, ["diceAnswer", {
        username,
        guess: guessValue,
        isTieBreaker: true,
        tieRound: STATE._tieRound
      }]);
      
      if (STATE._tieAnswers.size === STATE._tiePlayers.length) {
        STATE._canSubmitDiceAnswer = false;
        STATE._isShowingDice = false;
        
        if (STATE._tieTimer) {
          clearTimeout(STATE._tieTimer);
          STATE._tieTimer = null;
        }
        if (STATE._tieInterval) {
          clearInterval(STATE._tieInterval);
          STATE._tieInterval = null;
        }
        
        const tieId = getActiveTieBreakerId();
        if (tieId) {
          setTimeout(async () => {
            await processTieResults(DICE_ROOM, tieId, STATE._tiePlayers);
          }, 500);
        } else {
          resetTieBreakerState(null);
          startCooldownAfterTieBreaker();
        }
      }
      return;
    }
    
    if (!STATE.currentDiceRoll) return;
    
    const diceValue = STATE.currentDiceRoll.value;
    
    STATE._playerAnswers.set(username, guessValue);
    STATE.diceAnswered.add(username);
    
    broadcastToRoom(DICE_ROOM, ["diceAnswer", {
      username,
      guess: guessValue,
      round: STATE._diceRound || 1
    }]);
    
    if (guessValue === diceValue && !STATE.diceHasWinner) {
      STATE.diceHasWinner = true;
      STATE.diceWinner = username;
    }
    
  } catch(e) {}
}

// ==================== GET DICE POINTS ====================
async function getDicePoints() {
  try {
    if (!STATE.env?.QUESTIONS) return {};
    return await withTimeout(STATE.diceGameSystem.getPoints(), 1500);
  } catch(e) { return {}; }
}

function getTimeLeftUntilNextDice() {
  try {
    const witaTime = getCurrentWITATime();
    const currentTotal = witaTime.totalMinutes;
    let minDiff = Infinity;
    for (const session of QUIZ_SCHEDULE.SESSIONS) {
      let startTotal = session.start * 60;
      let diff = startTotal - currentTotal;
      if (diff < 0) diff += 24 * 60;
      if (diff < minDiff) minDiff = diff;
    }
    const hours = Math.floor(minDiff / 60);
    const minutes = Math.floor(minDiff % 60);
    const isRunning = isDiceTime();
    return { 
      hours, 
      minutes, 
      totalMs: minDiff * 60 * 1000,
      text: `${hours}h ${minutes}m`,
      isRunning: isRunning
    };
  } catch(e) {
    return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false };
  }
}

// ==================== LOAD KV DATA ====================
async function loadKVData() {
  try {
    if (!STATE.env?.QUESTIONS) return;
    
    const currentWeek = generateCurrentWeek(new Date());
    const existing = await STATE.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
    if (!existing) {
      STATE._cachedResetWeek = currentWeek;
      STATE.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek).catch(() => {});
    }
    
    await STATE.diceGameSystem.loadScores();
    
  } catch(e) {}
}

function generateCurrentWeek(date) {
  const now = date || new Date();
  const year = now.getUTCFullYear();
  const startOfYear = new Date(Date.UTC(year, 0, 1));
  const diff = now - startOfYear;
  const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
  return `${year}-W${String(week).padStart(2, '0')}`;
}

// ==================== CLEANUP ====================
function cleanupMemory() {
  try {
    if (STATE.wsMap) {
      const toRemove = [];
      for (const [id, ws] of STATE.wsMap) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(id);
        }
      }
      for (const id of toRemove) {
        STATE.wsMap.delete(id);
      }
    }
    
    if (STATE._eventQueue && STATE._eventQueue.length > 50) {
      STATE._eventQueue.splice(0, STATE._eventQueue.length - 50);
    }
    
    const now = Date.now();
    for (const [key, time] of STATE._gameLocks) {
      if (now - time > 30000) STATE._gameLocks.delete(key);
    }
    for (const [key, time] of STATE._joinLocks) {
      if (now - time > 30000) STATE._joinLocks.delete(key);
    }
  } catch(e) {}
}

function performCleanup() {
  try {
    cleanupDeadConnections();
    cleanupUserConnections();
    cleanupReconnectAttempts();
    cleanupBannedUsers();
    
    if (Date.now() - STATE._lastErrorReset > CONSTANTS.ERROR_RESET_INTERVAL_MS) {
      STATE._errorCount = 0;
      STATE._lastErrorReset = Date.now();
    }
  } catch(e) {}
}

function cleanupDeadConnections() {
  try {
    const toRemove = [];
    for (const [wsId, ws] of STATE.wsMap) {
      if (!ws || ws.readyState !== 1 || ws._closing) {
        toRemove.push(wsId);
      }
    }
    for (const wsId of toRemove) {
      const ws = STATE.wsMap.get(wsId);
      if (ws) {
        const room = STATE.clientRooms.get(wsId);
        if (room) removeClientFromRoom(room, wsId);
        STATE.clientRooms.delete(wsId);
        STATE.wsMap.delete(wsId);
      }
    }
  } catch(e) {}
}

function cleanupUserConnections() {
  try {
    let cleaned = 0;
    for (const [username, conn] of STATE.userConnections) {
      if (cleaned > 20) break;
      if (!conn?.ws || conn.ws.readyState !== 1) {
        STATE.userConnections.delete(username);
        cleaned++;
      }
    }
  } catch(e) {}
}

function cleanupReconnectAttempts() {
  try {
    const now = Date.now();
    let cleaned = 0;
    for (const [username, data] of STATE._reconnectAttempts) {
      if (cleaned > 10) break;
      if (now - (data.lastAttempt || 0) > 300000) {
        STATE._reconnectAttempts.delete(username);
        cleaned++;
      }
    }
  } catch(e) {}
}

function cleanupBannedUsers() {
  try {
    const now = Date.now();
    let cleaned = 0;
    for (const [username, banUntil] of STATE._bannedUsers) {
      if (cleaned > 10) break;
      if (now > banUntil) {
        STATE._bannedUsers.delete(username);
        cleaned++;
      }
    }
  } catch(e) {}
}

// ==================== CHECK STUCK GAMES ====================
function checkStuckGames() {
  try {
    const now = Date.now();
    for (const [room, game] of STATE.activeGames) {
      if (!game?._isActive || game._gameEnded) continue;
      
      if (game._phase === 'draw' && game._drawPhaseStart &&
          (now - game._drawPhaseStart) > CONSTANTS.STUCK_DRAW_TIMEOUT_MS) {
        closeDrawPhase(room, game);
      }
      
      if (game._phase === 'registration' && game.registrationOpen &&
          game._createdAt && (now - game._createdAt) > CONSTANTS.STUCK_REGISTRATION_TIMEOUT_MS) {
        closeRegistration(room, game);
      }
    }
  } catch(e) {}
}

// ==================== TIMER MANAGEMENT ====================
function trackTimer(timer) {
  if (timer) STATE._allTimers.add(timer);
  return timer;
}

function clearTimer(timer) {
  if (timer) {
    clearTimeout(timer);
    clearInterval(timer);
    STATE._allTimers.delete(timer);
  }
}

function withTimeout(promise, timeoutMs = CONSTANTS.KV_TIMEOUT_MS) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      const timer = setTimeout(() => reject(new Error('KV timeout')), timeoutMs);
      trackTimer(timer);
    })
  ]);
}

function fireAndForget(promise) {
  promise.catch(() => {});
}

// ==================== MAIN HANDLER ====================
export default {
  async fetch(request, env) {
    STATE.env = env;
    
    // ✅ LOAD KV DATA ON STARTUP
    if (!STATE._initialized) {
      STATE._initialized = true;
      await loadKVData();
    }
    
    try {
      if (STATE._circuitOpen) {
        const now = Date.now();
        if (now - STATE._lastResetTime > 60000) {
          STATE._circuitOpen = false;
          STATE._requestCount = 0;
          STATE._lastResetTime = now;
        } else {
          return new Response("Service temporarily unavailable", { 
            status: 503,
            headers: { 'Retry-After': '30', 'Content-Type': 'text/plain' }
          });
        }
      }
      
      STATE._requestCount++;
      if (STATE._requestCount > CONSTANTS.RATE_LIMIT_MAX) {
        STATE._circuitOpen = true;
        STATE._lastResetTime = Date.now();
        return new Response("Rate limit exceeded", { 
          status: 429,
          headers: { 'Retry-After': '60', 'Content-Type': 'text/plain' }
        });
      }
      
      setTimeout(() => {
        STATE._requestCount = Math.max(0, STATE._requestCount - 50);
      }, CONSTANTS.RATE_LIMIT_WINDOW_MS);
      
      const url = new URL(request.url);
      
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          uptime: Date.now() - STATE._startTime,
          connections: STATE.wsMap.size,
          games: STATE.activeGames.size,
          queue: STATE._eventQueue?.length || 0,
          circuitOpen: STATE._circuitOpen,
          initialized: STATE._initialized,
          errors: STATE._errorCount,
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      if (url.pathname === "/metrics") {
        return new Response(JSON.stringify({
          connections: STATE.wsMap.size,
          games: STATE.activeGames.size,
          queue: STATE._eventQueue?.length || 0,
          errors: STATE._errorCount,
          circuitOpen: STATE._circuitOpen,
          uptime: Date.now() - STATE._startTime,
          diceActive: !!STATE.currentDiceRoll,
          diceRound: STATE._diceRound || 0,
          tieActive: STATE._tieActive
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      // ========== WEBSOCKET ==========
      if (url.pathname === "/game/ws") {
        const upgrade = request.headers.get("Upgrade");
        if (upgrade !== "websocket") {
          return new Response("WebSocket only", { status: 400 });
        }
        
        if (STATE.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
          return new Response("Server at maximum capacity", { 
            status: 503,
            headers: { 'Retry-After': '10', 'Content-Type': 'text/plain' }
          });
        }
        
        if (STATE._eventQueue?.length > 500) {
          return new Response("Server busy", { 
            status: 503,
            headers: { 'Retry-After': '5', 'Content-Type': 'text/plain' }
          });
        }
        
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        const wsId = ++STATE._wsIdCounter;
        
        server._wsId = wsId;
        server._closing = false;
        server.room = null;
        server.roomname = null;
        server.username = null;
        server._createdAt = Date.now();
        
        try {
          server.accept();
        } catch(e) {
          try { server.close(1008, "Accept failed"); } catch(err) {}
          return new Response("WebSocket acceptance failed", { status: 500 });
        }
        
        STATE.wsMap.set(wsId, server);
        
        server.addEventListener("message", async (event) => {
          try {
            if (server._closing) return;
            const data = JSON.parse(event.data);
            if (Array.isArray(data) && data.length > 0) {
              await processWithTimeout(server, data);
            }
          } catch(e) {}
        });
        
        server.addEventListener("close", () => { 
          webSocketClose(server);
        }, { once: true });
        
        server.addEventListener("error", () => { 
          webSocketError(server);
        }, { once: true });
        
        return new Response(null, { status: 101, webSocket: client });
      }
      
      return new Response("Game Server", { status: 200 });
      
    } catch(e) {
      handleError('fetch', e);
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};

// ==================== PROCESS WITH TIMEOUT ====================
async function processWithTimeout(ws, data, timeoutMs = 500) {
  try {
    const timeoutPromise = new Promise((_, reject) => {
      const timer = setTimeout(() => {
        reject(new Error('Processing timeout'));
      }, timeoutMs);
      trackTimer(timer);
    });
    
    await Promise.race([
      handleEvent(ws, data),
      timeoutPromise
    ]);
  } catch(e) {}
}

// ==================== HANDLE EVENT ====================
async function handleEvent(ws, data) {
  try {
    if (!ws || !data?.[0]) return;
    if (STATE._eventQueue.length > CONSTANTS.MAX_EVENT_QUEUE_SIZE) {
      safeSend(ws, ["gameLowCardError", "Server busy"]);
      return;
    }
    STATE._eventQueue.push({ ws, data });
    if (!STATE._isProcessingQueue) await processEventQueue();
  } catch(e) {}
}

async function processEventQueue(iteration = 0) {
  try {
    if (STATE._isProcessingQueue || STATE._eventQueue.length === 0) return;
    STATE._isProcessingQueue = true;
    
    if (iteration > CONSTANTS.MAX_EVENT_ITERATIONS) {
      STATE._isProcessingQueue = false;
      return;
    }
    
    const startTime = Date.now();
    let processed = 0;
    
    while (STATE._eventQueue.length > 0 && processed < 3) {
      if (Date.now() - startTime > CONSTANTS.MAX_PROCESS_TIME_MS) break;
      
      const item = STATE._eventQueue.shift();
      try {
        await processEventItem(item.ws, item.data);
      } catch(e) {
        handleError('processQueue', e);
      }
      processed++;
    }
    
    if (STATE._eventQueue.length > 0 && iteration < CONSTANTS.MAX_EVENT_ITERATIONS) {
      setTimeout(() => {
        STATE._isProcessingQueue = false;
        processEventQueue(iteration + 1);
      }, 5);
    }
    
  } catch(e) {
    handleError('processQueue', e);
  } finally {
    STATE._isProcessingQueue = false;
  }
}

async function processEventItem(ws, data) {
  try {
    if (!ws || !data || !data[0]) return;
    await handleEventInternal(ws, data);
  } catch(e) {}
}

// ==================== HANDLE EVENT INTERNAL ====================
async function handleEventInternal(ws, data) {
  try {
    if (!ws || !data || !data[0]) return;
    const evt = data[0];

    if (evt === "switchRoom") {
      await switchRoom(ws, data[1], data[2]);
      return;
    }

    if (evt === "submitDiceAnswer") {
      await submitDiceAnswer(ws, data[1], data[2]);
      return;
    }

    if (evt === "getDiceLastWeekWinner") {
      try {
        const result = await getLastWeekWinnerAndReset();
        if (result?.username) {
          safeSend(ws, ["diceLastWeekWinner", result.username, result.score || 0, result.week || ""]);
        } else {
          safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
        }
      } catch(e) { safeSend(ws, ["diceLastWeekWinner", "", 0, ""]); }
      return;
    }

    if (evt === "getDiceLeaderboard") {
      try {
        let limit = data.length > 1 && typeof data[1] === 'number' ? Math.min(data[1], 30) : 10;
        const points = await withTimeout(
          STATE.env.QUESTIONS.get(CONSTANTS.DICE_POINT_KEY, 'json'),
          1500
        ) || {};
        const sorted = Object.entries(points).sort((a, b) => b[1] - a[1]).slice(0, limit);
        safeSend(ws, ["diceLeaderboard", sorted.map(([u, s]) => `${u}|${s}`)]);
      } catch(e) { safeSend(ws, ["diceLeaderboard", []]); }
      return;
    }

    if (evt === "deleteDiceLastWeekWinner") {
      try {
        const success = await STATE.diceGameSystem.deleteLastWeekWinner();
        safeSend(ws, ["diceLastWeekWinnerDeleted", success, success ? "Deleted" : "Failed"]);
        if (success) broadcastToRoom(DICE_ROOM, ["diceNotification", "Last week winner deleted"]);
      } catch(e) { safeSend(ws, ["diceLastWeekWinnerDeleted", false, e.message]); }
      return;
    }

    if (evt === "getDiceStatus") {
      safeSend(ws, ["diceStatus", !!STATE.currentDiceRoll && STATE._canSubmitDiceAnswer, STATE._diceRound || 1]);
      return;
    }

    if (evt === "startRecordingWinners") {
      const roomName = data[1];
      if (!roomName) { safeSend(ws, ["recordingError", "Room name required"]); return; }
      const success = await startRecordingWinners(roomName);
      safeSend(ws, ["startRecordingResult", { success, message: success ? "Recording enabled" : "Failed" }]);
      return;
    }

    if (evt === "stopRecordingWinners") {
      const roomName = data[1];
      if (!roomName) { safeSend(ws, ["recordingError", "Room name required"]); return; }
      const success = await stopRecordingWinners(roomName);
      safeSend(ws, ["stopRecordingResult", { success, message: success ? "Recording stopped" : "Failed" }]);
      return;
    }

    if (evt === "getRecordingStatus") {
      const roomName = data[1];
      if (!roomName) { safeSend(ws, ["recordingError", "Room name required"]); return; }
      const isRecording = await getRecordingStatusFromKV(roomName);
      safeSend(ws, ["recordingStatus", isRecording]);
      return;
    }

    if (evt === "sendWinnersToRoom" || evt === "lowCardWinnerUpdate") {
      const room = data[1] || ws.room || ws.roomname || STATE.clientRooms.get(ws._wsId);
      if (!room) { safeSend(ws, ["recordingError", "Room name required"]); return; }
      await broadcastLowCardWinners(room);
      safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners refreshed" }]);
      return;
    }

    if (evt === "getRoomWinners") {
      const room = data[1] || ws.room || ws.roomname || STATE.clientRooms.get(ws._wsId);
      if (!room) { safeSend(ws, ["recordingError", "Room name required"]); return; }
      const isRecording = await getRecordingStatusFromKV(room);
      const winners = await getLowCardWinners(room);
      broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: isRecording }]);
      safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners updated" }]);
      return;
    }

    if (evt === "startGameWithRecording") {
      const [_, room, bet, username] = data;
      await startGameWithRecording(ws, room, bet, username);
      return;
    }

    const room = ws.room || ws.roomname || STATE.clientRooms.get(ws._wsId);
    if (!room) {
      safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
      return;
    }
    if (room === DICE_ROOM) {
      safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]);
      return;
    }

    switch (evt) {
      case "gameLowCardStart": await startGame(ws, data[1], data[2]); break;
      case "gameLowCardJoin": await joinGame(ws, data[1]); break;
      case "gameLowCardNumber": await submitNumber(ws, data[1], data[2] || "", data[3]); break;
      case "gameLowCardLeave": await leaveGame(ws, data[1]); break;
      case "checkGameRunning": await checkGameRunning(ws, data[1]); break;
      case "getGameState": sendGameStateToClient(ws, data[1] || room); break;
      default: break;
    }
  } catch(e) {}
}

// ==================== RECORDING ====================
async function getRecordingStatusFromKV(roomName) {
  try {
    if (!roomName) return false;
    if (STATE._recordingEnabled.has(roomName)) return STATE._recordingEnabled.get(roomName);
    if (STATE.env?.QUESTIONS) {
      const kvValue = await withTimeout(
        STATE.env.QUESTIONS.get(CONSTANTS.LOWCARD_RECORDING_KEY + roomName), 1500
      );
      const isRecording = kvValue === 'true';
      STATE._recordingEnabled.set(roomName, isRecording);
      return isRecording;
    }
    return false;
  } catch(e) { return false; }
}

async function startRecordingWinners(roomName) {
  try {
    if (!roomName) return false;
    if (await getRecordingStatusFromKV(roomName)) {
      broadcastToRoom(roomName, ["recordingStatus", true]);
      return true;
    }
    STATE._recordingEnabled.set(roomName, true);
    if (STATE.env?.QUESTIONS) {
      await withTimeout(
        STATE.env.QUESTIONS.put(CONSTANTS.LOWCARD_RECORDING_KEY + roomName, 'true'), 1500
      );
    }
    broadcastToRoom(roomName, ["recordingStatus", true]);
    return true;
  } catch(e) { return false; }
}

async function stopRecordingWinners(roomName) {
  try {
    if (!roomName) return false;
    const room = roomName.trim();
    
    const isRecording = await getRecordingStatusFromKV(room);
    if (!isRecording) {
      broadcastToRoom(room, ["recordingStatus", false]);
      return true;
    }
    
    STATE._recordingEnabled.set(room, false);
    
    if (STATE.env?.QUESTIONS) {
      await withTimeout(
        STATE.env.QUESTIONS.delete(CONSTANTS.LOWCARD_RECORDING_KEY + room), 1500
      );
      await withTimeout(
        STATE.env.QUESTIONS.delete(CONSTANTS.LOWCARD_WINNER_KEY + room), 1500
      );
      STATE._kvCache.delete(CONSTANTS.LOWCARD_WINNER_KEY + room);
      STATE._kvCache.delete(CONSTANTS.LOWCARD_RECORDING_KEY + room);
    }
    
    broadcastToRoom(room, ["recordingStatus", false]);
    return true;
  } catch(e) { return false; }
}

async function getLowCardWinners(room) {
  try {
    if (!room || !STATE.env?.QUESTIONS) return {};
    if (!await getRecordingStatusFromKV(room)) return {};
    const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
    const winners = await withTimeout(STATE.env.QUESTIONS.get(key, 'json'), 1500);
    return winners && typeof winners === 'object' ? winners : {};
  } catch(e) { return {}; }
}

async function broadcastLowCardWinners(room) {
  try {
    if (!room) return;
    if (!await getRecordingStatusFromKV(room)) return;
    const winners = await getLowCardWinners(room);
    broadcastToRoom(room, ["lowCardWinnerUpdate", {
      winners: winners || {},
      room: room,
      recording: true
    }]);
  } catch(e) {}
}

async function addLowCardWinner(room, username) {
  try {
    if (!room || !username || room === DICE_ROOM) return false;
    if (!await getRecordingStatusFromKV(room)) return false;
    if (!STATE.env?.QUESTIONS) return false;
    
    const key = CONSTANTS.LOWCARD_WINNER_KEY + room;
    let roomWinners = await withTimeout(STATE.env.QUESTIONS.get(key, 'json'), 1500) || {};
    
    let count = 0;
    if (roomWinners[username]) {
      count = parseInt(String(roomWinners[username]).replace("x", "").replace("X", "")) || 0;
    }
    roomWinners[username] = (count + 1) + "x";
    
    await withTimeout(STATE.env.QUESTIONS.put(key, JSON.stringify(roomWinners)), 1500);
    return true;
  } catch(e) { return false; }
}

// ==================== WS HELPERS ====================
function getWsId(ws) { return ws?._wsId || null; }

function ensureRoomConsistency(ws) {
  try {
    if (!ws) return null;
    const wsId = getWsId(ws);
    if (!wsId) return null;
    let room = ws.room || ws.roomname || STATE.clientRooms.get(wsId) || null;
    if (!room && ws.username) {
      const conn = STATE.userConnections.get(ws.username);
      if (conn) room = conn.room || null;
    }
    if (room) {
      ws.room = room;
      ws.roomname = room;
      if (!STATE.wsClients.has(room)) STATE.wsClients.set(room, new Set());
      if (!STATE.wsClients.get(room).has(wsId)) {
        STATE.wsClients.get(room).add(wsId);
        STATE.clientRooms.set(wsId, room);
        STATE.wsMap.set(wsId, ws);
      }
      return room;
    }
    return null;
  } catch(e) { return null; }
}

function addClient(room, ws, username = null) {
  try {
    if (!ws) return;
    
    if (username && STATE._bannedUsers.has(username)) {
      const banUntil = STATE._bannedUsers.get(username);
      if (Date.now() < banUntil) {
        safeSend(ws, ["gameLowCardError", "You are temporarily banned"]);
        return;
      }
      STATE._bannedUsers.delete(username);
    }
    
    const wsId = getWsId(ws);
    if (!wsId) { safeSend(ws, ["gameLowCardError", "Connection error"]); return; }
    
    if (STATE.clientRooms.has(wsId)) {
      const oldRoom = STATE.clientRooms.get(wsId);
      if (oldRoom && oldRoom !== room) removeClientFromRoom(oldRoom, wsId);
    }
    
    if (username) {
      let conn = STATE.userConnections.get(username);
      if (conn) { conn.room = room; conn.timestamp = Date.now(); conn.ws = ws; conn.wsId = wsId; }
      else { STATE.userConnections.set(username, { wsId, ws, room, timestamp: Date.now() }); }
      STATE._reconnectAttempts.delete(username);
    }
    
    let clients = STATE.wsClients.get(room);
    if (!clients) { clients = new Set(); STATE.wsClients.set(room, clients); }
    clients.add(wsId);
    STATE.clientRooms.set(wsId, room);
    STATE.wsMap.set(wsId, ws);
    ws.room = room;
    ws.roomname = room;
    if (username) ws.username = username;
  } catch(e) {}
}

function removeClientFromRoom(room, wsId) {
  try {
    if (!room || !wsId) return;
    const clients = STATE.wsClients.get(room);
    if (clients) { clients.delete(wsId); if (clients.size === 0) STATE.wsClients.delete(room); }
  } catch(e) {}
}

function removeClient(room, ws) {
  try {
    if (!ws) return;
    const wsId = getWsId(ws);
    if (!wsId) return;
    const username = ws.username;
    removeClientFromRoom(room, wsId);
    STATE.clientRooms.delete(wsId);
    STATE.wsMap.delete(wsId);
    if (username) {
      const conn = STATE.userConnections.get(username);
      if (conn?.wsId === wsId) STATE.userConnections.delete(username);
    }
    ws.room = null;
    ws.roomname = null;
    ws._wsId = null;
    ws.username = null;
  } catch(e) {}
}

function safeSend(ws, message) {
  try {
    if (!ws || ws.readyState !== 1) return false;
    ws.send(JSON.stringify(message));
    return true;
  } catch(e) { return false; }
}

// ==================== BROADCAST ====================
function broadcastToRoom(room, message) {
  try {
    if (!room || !message) return;
    const wsIds = STATE.wsClients.get(room);
    if (!wsIds?.size) return;
    
    const msgStr = JSON.stringify(message);
    const wsIdArray = Array.from(wsIds);
    
    for (let i = 0; i < wsIdArray.length; i += 10) {
      const batch = wsIdArray.slice(i, i + 10);
      for (const wsId of batch) {
        const ws = STATE.wsMap.get(wsId);
        if (ws && ws.readyState === 1 && !ws._closing) {
          try { ws.send(msgStr); } catch(e) {}
        }
      }
    }
  } catch(e) {}
}

// ==================== SWITCH ROOM ====================
async function switchRoom(ws, room, username = null) {
  try {
    if (!room || room.trim() === "") {
      safeSend(ws, ["gameLowCardError", "Invalid room name"]);
      return;
    }
    
    const roomName = room.trim();
    const wsId = getWsId(ws);
    if (!wsId) {
      safeSend(ws, ["gameLowCardError", "Connection error"]);
      return;
    }
    
    const currentRoom = ws.room || ws.roomname || STATE.clientRooms.get(wsId);
    if (currentRoom === roomName) {
      safeSend(ws, ["switchRoomSuccess", roomName]);
      sendGameStateToClient(ws, roomName);
      
      if (roomName === DICE_ROOM) {
        sendDiceNotificationOnSwitch(ws, wsId);
      }
      return;
    }
    
    const lockKey = `switch_${wsId}`;
    if (STATE._switchLocks.has(lockKey)) {
      const retryCount = STATE._switchRetries.get(lockKey) || 0;
      if (retryCount > 3) {
        STATE._switchLocks.delete(lockKey);
        STATE._switchRetries.delete(lockKey);
        safeSend(ws, ["switchRoomError", "Switch timeout"]);
        return;
      }
      STATE._switchRetries.set(lockKey, retryCount + 1);
      safeSend(ws, ["switchRoomSuccess", currentRoom || roomName]);
      return;
    }
    
    STATE._switchLocks.set(lockKey, Date.now());
    STATE._switchRetries.set(lockKey, 0);
    
    try {
      if (currentRoom) removeClientFromRoom(currentRoom, wsId);
      addClient(roomName, ws, username);
      ws.room = roomName;
      ws.roomname = roomName;
      if (username) ws.username = username;
      
      if (username) {
        let conn = STATE.userConnections.get(username);
        if (conn) { conn.room = roomName; conn.wsId = wsId; conn.ws = ws; conn.timestamp = Date.now(); }
        else { STATE.userConnections.set(username, { wsId, ws, room: roomName, timestamp: Date.now() }); }
      }
      
      safeSend(ws, ["switchRoomSuccess", roomName]);
      sendGameStateToClient(ws, roomName);
      
      if (roomName === DICE_ROOM) {
        sendDiceNotificationOnSwitch(ws, wsId);
      }
      
      broadcastToRoom(roomName, ["userJoinedRoom", username, roomName]);
      if (currentRoom && currentRoom !== roomName) {
        broadcastToRoom(currentRoom, ["userLeftRoom", username, currentRoom]);
      }
    } finally {
      setTimeout(() => {
        STATE._switchLocks.delete(lockKey);
        STATE._switchRetries.delete(lockKey);
      }, 2000);
    }
  } catch(e) {}
}

function sendGameStateToClient(ws, room) {
  try {
    if (!ws || ws.readyState !== 1 || !room) return;
    const game = STATE.activeGames.get(room);
    if (!game || !game._isActive || game._gameEnded) {
      safeSend(ws, ["gameState", { room, hasGame: false, gameType: 'lowcard' }]);
      return;
    }
    
    const activePlayers = getActivePlayers(game);
    const allPlayers = Array.from(game.players.values()).map(p => p.name);
    const eliminated = Array.from(game.eliminated || []);
    const submitted = Array.from(game.numbers?.keys() || []);
    
    safeSend(ws, ["gameState", {
      room, hasGame: true, gameType: 'lowcard',
      isActive: game._isActive, phase: game._phase || 'registration',
      round: game.round || 1, bet: game.betAmount || 0,
      host: game.hostName || 'Unknown',
      registrationOpen: game.registrationOpen || false,
      players: allPlayers, activePlayers: activePlayers.map(p => p.name),
      eliminated, submitted, playerCount: game.players.size,
      activeCount: activePlayers.length,
      isEvaluating: game._isEvaluating || false,
      evaluationLocked: game.evaluationLocked || false,
      drawTimeExpired: game.drawTimeExpired || false
    }]);
  } catch(e) {}
}

// ==================== SEND DICE NOTIFICATION ON SWITCH ====================
function sendDiceNotificationOnSwitch(ws, wsId) {
  try {
    if (!ws || ws.readyState !== 1) return;
    
    STATE._diceTimeLeftNotified?.delete(wsId);
    STATE._nextDiceNotified?.delete(wsId);
    STATE._diceJoinedNotified?.delete(wsId);
    
    const isGameActive = STATE.currentDiceRoll && STATE._canSubmitDiceAnswer;
    
    if (isGameActive) {
      const elapsed = (Date.now() - STATE._diceStartTime) / 1000;
      const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
      const remaining = Math.max(0, totalTime - elapsed);
      const remainingInt = Math.floor(remaining);
      
      if (remainingInt > 0) {
        safeSend(ws, ["diceNotification", `${remainingInt}s remaining`]);
      }
      return;
    }
    
    const timeLeft = getTimeLeftUntilNextDice();
    const isDiceTime = isDiceTime();
    
    if (!isDiceTime || !STATE.diceAutoEnabled) {
      setTimeout(() => {
        if (ws && ws.readyState === 1) {
          safeSend(ws, ["diceNotification", `Next dice game in: ${timeLeft.text}`]);
        }
      }, 5000);
      
      const waitTime = timeLeft.totalMs + 5000;
      setTimeout(() => {
        if (!STATE.currentDiceRoll && !STATE._isShowingDice) {
          if (isDiceTime()) {
            forceStartDice();
          }
        }
      }, waitTime);
      
      return;
    }
    
    if (isDiceTime && !STATE.currentDiceRoll && STATE.diceAutoEnabled) {
      setTimeout(() => {
        if (!STATE.currentDiceRoll && !STATE._isShowingDice) {
          forceStartDice();
        }
      }, 5000);
    }
    
  } catch(e) {}
}

// ==================== GET LAST WEEK WINNER ====================
async function getLastWeekWinnerAndReset() {
  try {
    if (!STATE.env?.QUESTIONS) return null;
    
    const currentWeek = generateCurrentWeek(new Date());
    const lastResetWeek = await STATE.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
    const weekChanged = lastResetWeek && compareWeeks(currentWeek, lastResetWeek) > 0;
    
    if (!lastResetWeek || weekChanged) {
      await performReset();
      return STATE._cachedLastWeekWinner;
    }
    
    if (STATE._cachedLastWeekWinner !== null) return STATE._cachedLastWeekWinner;
    
    const savedWinner = await withTimeout(STATE.diceGameSystem.getLastWeekWinner(), 1500);
    return savedWinner;
    
  } catch(e) { return null; }
}

async function performReset() {
  try {
    const currentWeek = generateCurrentWeek(new Date());
    const lastResetWeek = await STATE.env.QUESTIONS.get(CONSTANTS.DICE_LAST_RESET_WEEK);
    
    const points = await withTimeout(STATE.diceGameSystem.getPoints(), 1500);
    
    let winner = null, highestScore = 0;
    for (const [username, score] of Object.entries(points)) {
      const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
      if (numericScore > highestScore) {
        highestScore = numericScore;
        winner = username;
      }
    }
    
    if (winner && highestScore > 0) {
      const winnerData = {
        username: winner,
        score: highestScore,
        week: lastResetWeek || currentWeek,
        timestamp: Date.now()
      };
      await withTimeout(
        STATE.env.QUESTIONS.put(CONSTANTS.DICE_LAST_WEEK_WINNER, JSON.stringify(winnerData)),
        1500
      );
      STATE._cachedLastWeekWinner = winnerData;
    } else {
      await withTimeout(STATE.env.QUESTIONS.delete(CONSTANTS.DICE_LAST_WEEK_WINNER), 1500);
      STATE._cachedLastWeekWinner = null;
    }
    
    await withTimeout(
      STATE.env.QUESTIONS.put(CONSTANTS.DICE_POINT_KEY, JSON.stringify({})),
      1500
    );
    STATE.diceGameSystem.clearCache();
    
    STATE._cachedResetWeek = currentWeek;
    fireAndForget(
      STATE.env.QUESTIONS.put(CONSTANTS.DICE_LAST_RESET_WEEK, currentWeek)
    );
    
    broadcastToRoom(DICE_ROOM, [
      "diceReset", 
      { 
        winner: winner, 
        score: highestScore, 
        week: currentWeek 
      }
    ]);
    
    return STATE._cachedLastWeekWinner;
    
  } catch(e) {
    return null;
  }
}

function compareWeeks(a, b) {
  try {
    const [yA, wA] = a.split('-W');
    const [yB, wB] = b.split('-W');
    const diff = parseInt(yA) - parseInt(yB);
    if (diff !== 0) return diff;
    return parseInt(wA) - parseInt(wB);
  } catch(e) { return 0; }
}

// ==================== LOW CARD GAME - FULL METHODS ====================

function isGameActuallyRunning(game) { 
  return game?._isActive === true && !game?._gameEnded; 
}

function getActivePlayers(game) {
  try {
    if (!game?._isActive || game?._gameEnded || !game?.players) return [];
    return Array.from(game.players.entries())
      .filter(([id]) => !game.eliminated?.has(id))
      .map(([, p]) => p);
  } catch(e) { return []; }
}

function getActivePlayerIds(game) {
  try {
    if (!game?._isActive || game._gameEnded || !game?.players) return [];
    return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
  } catch(e) { return []; }
}

function getRandomCardTanda() { 
  return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; 
}

function getRandomDrawDelay() { 
  return (Math.floor(Math.random() * 14) + 2) * 1000; 
}

function getBotNumberByRound(round) {
  if (round <= 2) return Math.floor(Math.random() * 12) + 1;
  return Math.random() < 0.6 ?
    [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
    [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
}

function scheduleGameCleanup(room, game) {
  try {
    if (!room || !game) return;
    if (STATE._cleanupTimers.has(room)) {
      clearTimer(STATE._cleanupTimers.get(room));
      STATE._cleanupTimers.delete(room);
    }
    if (!game._gameEnded) return;
    const timer = trackTimer(setTimeout(() => {
      const currentGame = STATE.activeGames.get(room);
      if (currentGame?._isActive && !currentGame._gameEnded) {
        STATE._cleanupTimers.delete(room);
        return;
      }
      STATE._cleanupTimers.delete(room);
      const gameToDelete = STATE.activeGames.get(room);
      if (gameToDelete) deleteGame(room, gameToDelete);
    }, CONSTANTS.GAME_CLEANUP_DELAY_MS));
    STATE._cleanupTimers.set(room, timer);
  } catch(e) {}
}

function deleteGame(room, game) {
  try {
    if (!room || !game) return;
    if (game?._isActive && !game._gameEnded) return;
    if (STATE._cleanupTimers.has(room)) {
      clearTimer(STATE._cleanupTimers.get(room));
      STATE._cleanupTimers.delete(room);
    }
    if (game) {
      game._gameEnded = true;
      game._isActive = false;
      game.playerWsId = null;
      cleanupGame(game);
    }
    STATE.activeGames.delete(room);
    STATE._gameLocks.delete(room);
    STATE._joinLocks.delete(room);
    broadcastToRoom(room, ["gameLowCardEnd", []]);
  } catch(e) {}
}

function cleanupGame(game) {
  try {
    if (!game) return;
    if (game._isActive && !game._gameEnded) return;
    const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
    for (const key of timers) {
      if (game[key]) { clearTimer(game[key]); game[key] = null; }
    }
    if (game._botTimeouts) {
      for (const id of game._botTimeouts) clearTimer(id);
      game._botTimeouts.clear();
      game._botTimeouts = null;
    }
    game.players = null;
    game.botPlayers = null;
    game.numbers = null;
    game.tanda = null;
    game.eliminated = null;
    game.playerWsId = null;
    game._isActive = false;
    game._gameEnded = true;
    game._isEvaluating = false;
  } catch(e) {}
}

function addBots(room, count) {
  try {
    const game = STATE.activeGames.get(room);
    if (!isGameActuallyRunning(game)) return;
    const botNames = ["moz1", "moz2", "moz3", "moz4"];
    const existingBots = Array.from(game.players.keys()).filter(id => id.startsWith('BOT_'));
    const existingBotCount = existingBots.length;
    const maxBotsToAdd = Math.min(count, CONSTANTS.MAX_BOTS_PER_GAME - existingBotCount);
    if (maxBotsToAdd <= 0) return;
    for (let i = 0; i < maxBotsToAdd; i++) {
      const botId = `BOT_${room}_${i}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
      const botName = botNames[(existingBotCount + i) % botNames.length];
      if (!game.players.has(botId)) {
        game.players.set(botId, { id: botId, name: botName });
        if (!game.botPlayers) game.botPlayers = new Map();
        game.botPlayers.set(botId, botName);
      }
    }
    game._botsAdded = true;
    game.useBots = true;
  } catch(e) {}
}

function startBotDraws(room, game) {
  try {
    if (!isGameActuallyRunning(game) || !game.botPlayers) return;
    if (!game._botTimeouts) game._botTimeouts = new Set();
    
    if (game._botTimeouts.size >= CONSTANTS.MAX_BOT_TIMEOUTS) return;
    
    const notDrawn = Array.from(game.botPlayers.keys())
      .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id))
      .slice(0, Math.min(CONSTANTS.MAX_BOT_DRAWS_PER_ROUND, CONSTANTS.MAX_BOT_TIMEOUTS - game._botTimeouts.size));
    
    for (const botId of notDrawn) {
      const delay = getRandomDrawDelay();
      const timeout = trackTimer(setTimeout(() => {
        const currentGame = STATE.activeGames.get(room);
        if (isGameActuallyRunning(currentGame) && !currentGame.drawTimeExpired &&
            !currentGame.evaluationLocked && !currentGame.numbers?.has(botId) && 
            !currentGame.eliminated?.has(botId)) {
          handleBotDraw(room, botId, currentGame);
        }
        currentGame?._botTimeouts?.delete(timeout);
      }, delay));
      game._botTimeouts.add(timeout);
    }
  } catch(e) {}
}

function handleBotDraw(room, botId, game) {
  try {
    if (!isGameActuallyRunning(game) || game.numbers?.has(botId) || game.drawTimeExpired || game.evaluationLocked) return;
    if (game.eliminated?.has(botId)) return;
    const number = getBotNumberByRound(game.round);
    const tanda = getRandomCardTanda();
    game.numbers.set(botId, number);
    game.tanda.set(botId, tanda);
    const botName = game.players.get(botId)?.name || botId;
    broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
    const activeIds = getActivePlayerIds(game);
    if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && isGameActuallyRunning(game)) {
      game.evaluationLocked = true;
      broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      const evalTimer = trackTimer(setTimeout(() => {
        try { evaluateRound(room, game); } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS));
      game._evalTimer = evalTimer;
    }
  } catch(e) {}
}

function forceBotDraw(room, botId, game) {
  try {
    if (!isGameActuallyRunning(game) || game.numbers?.has(botId)) return;
    if (game.eliminated?.has(botId)) return;
    const number = getBotNumberByRound(game.round);
    const tanda = getRandomCardTanda();
    game.numbers.set(botId, number);
    game.tanda.set(botId, tanda);
    const botName = game.players.get(botId)?.name || botId;
    broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
  } catch(e) {}
}

function startRegistration(room, game) {
  try {
    if (!isGameActuallyRunning(game) || !game.registrationOpen) return;
    if (game._registrationTimer) { clearTimer(game._registrationTimer); game._registrationTimer = null; }
    let timeLeft = 20;
    const timer = trackTimer(setInterval(() => {
      try {
        if (!isGameActuallyRunning(game) || !game.registrationOpen || timeLeft < 0) {
          clearTimer(timer);
          if (game._registrationTimer === timer) game._registrationTimer = null;
          return;
        }
        if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
          broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
        if (timeLeft === 0) {
          clearTimer(timer);
          game._registrationTimer = null;
          broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
          closeRegistration(room, game);
        }
        timeLeft--;
      } catch(e) { clearTimer(timer); if (game._registrationTimer === timer) game._registrationTimer = null; }
    }, 1000));
    game._registrationTimer = timer;
  } catch(e) {}
}

function closeRegistration(room, game) {
  try {
    if (!isGameActuallyRunning(game) || !game.registrationOpen) return;
    game.registrationOpen = false;
    if (game._registrationTimer) { clearTimer(game._registrationTimer); game._registrationTimer = null; }
    
    const humanPlayers = Array.from(game.players.keys()).filter(id => !id.startsWith('BOT_'));
    const humanCount = humanPlayers.length;
    
    if (!game._botsAdded) {
      if (humanCount === 1 || humanCount === 0) {
        addBots(room, 4);
        game._botsAdded = true;
      } else if (game.players.size < 2) {
        const needed = Math.min(4 - game.players.size, CONSTANTS.MAX_BOTS_PER_GAME);
        if (needed > 0) { addBots(room, needed); game._botsAdded = true; }
      }
    }
    
    if (isGameActuallyRunning(game) && game.players.size >= 2) {
      startDrawPhase(room, game);
    } else {
      game._gameEnded = true;
      game._isActive = false;
      broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
      scheduleGameCleanup(room, game);
    }
  } catch(e) {}
}

async function startDrawPhase(room, game) {
  try {
    if (!isGameActuallyRunning(game)) return;
    if (game._drawTimer) { clearTimer(game._drawTimer); game._drawTimer = null; }
    if (game._evalTimer) { clearTimer(game._evalTimer); game._evalTimer = null; }
    if (game._botTimeouts) {
      for (const id of game._botTimeouts) clearTimer(id);
      game._botTimeouts.clear();
    }
    
    const activePlayers = getActivePlayers(game);
    if (activePlayers.length < 2) {
      if (!game._botsAdded) {
        const needed = Math.min(4 - activePlayers.length, CONSTANTS.MAX_BOTS_PER_GAME);
        if (needed > 0) { addBots(room, needed); game._botsAdded = true; }
      }
      const newActive = getActivePlayers(game);
      if (newActive.length < 2) {
        if (newActive.length === 1 && !game._gameEnded) {
          const winner = newActive[0]?.name || "Unknown";
          const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
          
          if (game._startedByRecording) {
            await addLowCardWinner(room, winner);
            const winners = await getLowCardWinners(room);
            broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
          }
          
          game._gameEnded = true;
          game._isActive = false;
          broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
          scheduleGameCleanup(room, game);
        } else {
          game._gameEnded = true;
          game._isActive = false;
          broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
          scheduleGameCleanup(room, game);
        }
        return;
      }
    }
    
    game._phase = 'draw';
    game.drawTimeExpired = false;
    game.evaluationLocked = false;
    game._drawPhaseStart = Date.now();
    if (!game._botTimeouts) game._botTimeouts = new Set();
    
    const playersList = getActivePlayers(game).map(p => p.name);
    broadcastToRoom(room, ["gameLowCardClosed", playersList]);
    broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
    startDrawCountdown(room, game);
    if (game.botPlayers?.size > 0 && isGameActuallyRunning(game)) {
      startBotDraws(room, game);
    }
  } catch(e) {}
}

function startDrawCountdown(room, game) {
  try {
    if (!isGameActuallyRunning(game)) return;
    if (game._drawTimer) { clearTimer(game._drawTimer); game._drawTimer = null; }
    let timeLeft = 20;
    const timer = trackTimer(setInterval(() => {
      try {
        if (!isGameActuallyRunning(game) || game.drawTimeExpired || timeLeft < 0) {
          clearTimer(timer);
          if (game._drawTimer === timer) game._drawTimer = null;
          return;
        }
        if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
          broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
        if (timeLeft === 0) {
          clearTimer(timer);
          game._drawTimer = null;
          broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
          closeDrawPhase(room, game);
        }
        timeLeft--;
      } catch(e) { clearTimer(timer); if (game._drawTimer === timer) game._drawTimer = null; }
    }, 1000));
    game._drawTimer = timer;
  } catch(e) {}
}

// ==================== CLOSE DRAW PHASE ====================
async function closeDrawPhase(room, game) {
  try {
    if (!isGameActuallyRunning(game) || game.drawTimeExpired || game.evaluationLocked) return;
    game.drawTimeExpired = true;
    game.evaluationLocked = true;
    if (game._drawTimer) { clearTimer(game._drawTimer); game._drawTimer = null; }
    
    if (game.botPlayers?.size > 0 && isGameActuallyRunning(game)) {
      const activeBotIds = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
      for (const botId of activeBotIds) forceBotDraw(room, botId, game);
    }
    
    const activeIds = getActivePlayerIds(game);
    const submittedIds = new Set(game.numbers?.keys() || []);
    const notSubmitted = activeIds.filter(id => !submittedIds.has(id) && !game.eliminated?.has(id));
    
    if (notSubmitted.length > 0 && submittedIds.size === 0) {
      broadcastToRoom(room, ["gameLowCardError", "No one submitted numbers"]);
      game._gameEnded = true;
      game._isActive = false;
      game._endTime = Date.now();
      scheduleGameCleanup(room, game);
      return;
    }
    
    for (const id of notSubmitted) {
      if (!game.eliminated) game.eliminated = new Set();
      game.eliminated.add(id);
      game.numbers?.delete(id);
      game.tanda?.delete(id);
    }
    
    const remaining = Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
    
    if (remaining.length === 1 && !game._gameEnded) {
      const winnerId = remaining[0];
      const winnerName = game.players.get(winnerId)?.name || winnerId;
      const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
      
      if (game._startedByRecording) {
        await addLowCardWinner(room, winnerName);
        const winners = await getLowCardWinners(room);
        broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
      }
      
      broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
      game._gameEnded = true;
      game._isActive = false;
      game._endTime = Date.now();
      scheduleGameCleanup(room, game);
      return;
    }
    
    if (remaining.length === 0) {
      broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
      game._gameEnded = true;
      game._isActive = false;
      game._endTime = Date.now();
      scheduleGameCleanup(room, game);
      return;
    }
    
    broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
    if (game._evalTimer) { clearTimer(game._evalTimer); game._evalTimer = null; }
    const evalTimer = trackTimer(setTimeout(() => {
      try {
        const currentGame = STATE.activeGames.get(room);
        if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
          evaluateRound(room, game);
        }
      } catch(e) {}
    }, CONSTANTS.EVALUATION_DELAY_MS));
    game._evalTimer = evalTimer;
    
  } catch(e) {}
}

// ==================== EVALUATE ROUND ====================
async function evaluateRound(room, game) {
  try {
    if (!game?._isActive || game._gameEnded || game._isEvaluating || !game.players) return;
    const currentGame = STATE.activeGames.get(room);
    if (currentGame !== game) return;
    
    game._isEvaluating = true;
    const safetyTimer = trackTimer(setTimeout(() => {
      if (game?._isEvaluating) { 
        game._isEvaluating = false; 
        scheduleGameCleanup(room, game); 
      }
    }, CONSTANTS.EVALUATION_TIMEOUT_MS));
    game._safetyTimer = safetyTimer;
    
    if (game._evalTimer) { clearTimer(game._evalTimer); game._evalTimer = null; }
    if (game._botTimeouts) {
      for (const id of game._botTimeouts) clearTimer(id);
      game._botTimeouts.clear();
    }
    
    const numbers = game.numbers || new Map();
    const players = game.players || new Map();
    const eliminated = game.eliminated || new Set();
    const tanda = game.tanda || new Map();
    const entries = Array.from(numbers.entries());
    const submittedIds = new Set(numbers.keys());
    const activeIds = getActivePlayerIds(game);
    
    for (const id of activeIds) {
      if (!submittedIds.has(id)) eliminated.add(id);
    }
    
    if (entries.length === 0) {
      game._isEvaluating = false;
      if (game._safetyTimer) { clearTimer(game._safetyTimer); game._safetyTimer = null; }
      
      const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
      
      if (remaining.length === 1) {
        const winnerId = remaining[0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        
        if (game._startedByRecording) {
          await addLowCardWinner(room, winnerName);
          const winners = await getLowCardWinners(room);
          broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
        }
        
        broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { clearTimer(game._safetyTimer); game._safetyTimer = null; }
        scheduleGameCleanup(room, game);
        return;
      }
      
      broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
      game._gameEnded = true;
      game._isActive = false;
      game._isEvaluating = false;
      if (game._safetyTimer) { clearTimer(game._safetyTimer); game._safetyTimer = null; }
      scheduleGameCleanup(room, game);
      return;
    }
    
    const values = entries.map(([, n]) => n);
    const allSame = values.every(v => v === values[0]);
    let losers = [];
    
    if (!allSame && values.length > 0) {
      const lowest = Math.min(...values);
      losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
      for (const id of losers) eliminated.add(id);
    }
    
    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
    
    if (allSame && remaining.length >= 2) {
      game._isEvaluating = false;
      if (game._safetyTimer) { clearTimer(game._safetyTimer); game._safetyTimer = null; }
      numbers.clear();
      tanda.clear();
      game.round++;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      game._phase = 'draw';
      game.numbers = new Map();
      game.tanda = new Map();
      game._botTimeouts = new Set();
      
      const remainingNames = remaining.map(id => players.get(id)?.name || id);
      broadcastToRoom(room, ["gameLowCardRoundResult", game.round - 1,
        entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`),
        [], remainingNames, true
      ]);
      if (isGameActuallyRunning(game) && !game._gameEnded) {
        startDrawPhase(room, game);
      }
      return;
    }
    
    if (remaining.length === 1 && !game._gameEnded) {
      const winnerId = remaining[0];
      const winnerName = players.get(winnerId)?.name || winnerId;
      const totalCoin = (game.betAmount || 0) * players.size;
      
      if (game._startedByRecording) {
        await addLowCardWinner(room, winnerName);
        const winners = await getLowCardWinners(room);
        broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
      }
      
      broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
      game._gameEnded = true;
      game._isActive = false;
      game._isEvaluating = false;
      if (game._safetyTimer) { clearTimer(game._safetyTimer); game._safetyTimer = null; }
      scheduleGameCleanup(room, game);
      return;
    }
    
    if (remaining.length === 0) {
      game._isEvaluating = false;
      if (game._safetyTimer) { clearTimer(game._safetyTimer); game._safetyTimer = null; }
      game._gameEnded = true;
      game._isActive = false;
      broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
      scheduleGameCleanup(room, game);
      return;
    }
    
    const numbersArr = entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`);
    const loserNames = [...losers].map(id => players.get(id)?.name || id);
    const remainingNames = remaining.map(id => players.get(id)?.name || id);
    
    broadcastToRoom(room, ["gameLowCardRoundResult", game.round, numbersArr, loserNames, remainingNames]);
    
    numbers.clear();
    tanda.clear();
    game.round++;
    game.evaluationLocked = false;
    game.drawTimeExpired = false;
    game._phase = 'draw';
    game.numbers = new Map();
    game.tanda = new Map();
    game._botTimeouts = new Set();
    game._isEvaluating = false;
    
    if (game._safetyTimer) { clearTimer(game._safetyTimer); game._safetyTimer = null; }
    if (isGameActuallyRunning(game) && !game._gameEnded) {
      startDrawPhase(room, game);
    }
    
  } catch(e) {}
}

// ==================== GAME START METHODS ====================

async function startGame(ws, bet, username) {
  try {
    if (!username?.trim()) {
      safeSend(ws, ["gameLowCardError", "Username is required"]);
      return;
    }
    
    const usernameClean = username.trim();
    const room = ws.room || ws.roomname || STATE.clientRooms.get(ws._wsId);
    if (!room) {
      safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
      return;
    }
    if (room === DICE_ROOM) {
      safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]);
      return;
    }

    const lockKey = `game_start_${room}`;
    if (STATE._gameLocks.has(lockKey)) {
      safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]);
      return;
    }
    
    STATE._gameLocks.set(lockKey, Date.now());

    try {
      const isRecordingEnabled = await getRecordingStatusFromKV(room);
      if (isRecordingEnabled) {
        safeSend(ws, ["gameLowCardError", "Recording is ACTIVE in this room. Users cannot start games."]);
        return;
      }

      const existingGame = STATE.activeGames.get(room);
      if (existingGame?._isActive && !existingGame._gameEnded) {
        safeSend(ws, ["gameLowCardError", "Game is already running"]);
        return;
      }
      if (existingGame) await forceCleanupGame(room, existingGame);
      
      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
        safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
        return;
      }
      
      if (STATE.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
        safeSend(ws, ["gameLowCardError", "Server is busy"]);
        return;
      }
      
      const wsId = ws._wsId;
      const game = {
        room, players: new Map(), botPlayers: new Map(), registrationOpen: true,
        round: 1, numbers: new Map(), tanda: new Map(), eliminated: new Set(),
        betAmount, hostId: usernameClean, hostName: usernameClean, useBots: false,
        evaluationLocked: false, drawTimeExpired: false,
        _isActive: true, _gameEnded: false, _phase: 'registration',
        _botTimeouts: new Set(), _botsAdded: false,
        _registrationTimer: null, _drawTimer: null, _evalTimer: null, _safetyTimer: null,
        _isEvaluating: false, _createdAt: Date.now(), _drawPhaseStart: null, _endTime: null,
        playerWsId: new Map(),
        _startedByRecording: false, _startedBy: 'user'
      };
      
      game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
      game.playerWsId.set(usernameClean, wsId);
      STATE.activeGames.set(room, game);
      addClient(room, ws, usernameClean);
      broadcastToRoom(room, ["gameLowCardStart", betAmount]);
      broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
      startRegistration(room, game);
      
    } finally {
      setTimeout(() => {
        STATE._gameLocks.delete(lockKey);
      }, 3000);
    }
  } catch(e) {}
}

async function forceCleanupGame(room, game) {
  try {
    if (!game) return;
    const timers = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
    for (const key of timers) {
      if (game[key]) { clearTimer(game[key]); game[key] = null; }
    }
    if (game._botTimeouts) {
      for (const id of game._botTimeouts) clearTimer(id);
      game._botTimeouts.clear();
    }
    game._gameEnded = true;
    game._isActive = false;
    game._endTime = Date.now();
    broadcastToRoom(room, ["gameLowCardEnd", []]);
    STATE.activeGames.delete(room);
    if (STATE._cleanupTimers.has(room)) {
      clearTimer(STATE._cleanupTimers.get(room));
      STATE._cleanupTimers.delete(room);
    }
    STATE._gameLocks.delete(room);
    STATE._joinLocks.delete(room);
  } catch(e) {}
}

async function joinGame(ws, username) {
  try {
    if (!username?.trim()) {
      safeSend(ws, ["gameLowCardError", "Username is required"]);
      return;
    }
    
    const usernameClean = username.trim();
    const wsId = ws._wsId;
    const room = ws.room || ws.roomname || STATE.clientRooms.get(wsId);
    if (!room) {
      safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
      return;
    }
    
    const lockKey = `join_${room}_${usernameClean}`;
    if (STATE._joinLocks.has(lockKey)) {
      safeSend(ws, ["gameLowCardError", "Please wait"]);
      return;
    }
    
    STATE._joinLocks.set(lockKey, Date.now());
    
    try {
      const game = STATE.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        safeSend(ws, ["gameLowCardError", "No active game in this room"]);
        return;
      }
      if (game.players.has(usernameClean)) {
        if (game.eliminated?.has(usernameClean)) {
          safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
          return;
        }
        if (game.numbers.has(usernameClean)) {
          safeSend(ws, ["gameLowCardPlayerDraw", usernameClean, game.numbers.get(usernameClean), game.tanda.get(usernameClean) || ""]);
        }
        sendGameStateToClient(ws, room);
        return;
      }
      if (!game.registrationOpen) {
        safeSend(ws, ["gameLowCardNoJoin", usernameClean, game.betAmount]);
        safeSend(ws, ["gameLowCardError", "Registration is closed"]);
        return;
      }
      if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
        safeSend(ws, ["gameLowCardError", "Game is full"]);
        return;
      }
      game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
      addClient(room, ws, usernameClean);
      game.playerWsId.set(usernameClean, wsId);
      broadcastToRoom(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
    } finally {
      setTimeout(() => {
        STATE._joinLocks.delete(lockKey);
      }, 2000);
    }
  } catch(e) {}
}

async function submitNumber(ws, number, tanda, username) {
  try {
    if (!username?.trim()) {
      safeSend(ws, ["gameLowCardError", "Username is required"]);
      return;
    }
    
    const usernameClean = username.trim();
    const wsId = ws._wsId;
    const room = ws.room || ws.roomname || STATE.clientRooms.get(wsId);
    if (!room) {
      safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
      return;
    }
    
    const game = STATE.activeGames.get(room);
    if (!game?._isActive || game._gameEnded || !game.players) {
      safeSend(ws, ["gameLowCardError", "No active game"]);
      return;
    }
    if (game.players.has(usernameClean) && game.eliminated?.has(usernameClean)) {
      safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
      return;
    }
    if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw') {
      safeSend(ws, ["gameLowCardError", "Cannot submit now"]);
      return;
    }
    if (!game.players.has(usernameClean)) {
      safeSend(ws, ["gameLowCardError", "You are not in this game"]);
      return;
    }
    if (game.numbers.has(usernameClean)) {
      safeSend(ws, ["gameLowCardError", "You have already submitted"]);
      return;
    }
    
    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 12) {
      safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
      return;
    }
    const validTandas = ["C1", "C2", "C3", "C4", ""];
    if (!validTandas.includes(tanda)) tanda = "";
    
    game.numbers.set(usernameClean, n);
    game.tanda.set(usernameClean, tanda);
    broadcastToRoom(room, ["gameLowCardPlayerDraw", usernameClean, n, tanda]);
    
    const activeIds = getActivePlayerIds(game);
    if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired &&
        isGameActuallyRunning(game) && game._isActive && !game._gameEnded) {
      game.evaluationLocked = true;
      if (game._evalTimer) { clearTimer(game._evalTimer); game._evalTimer = null; }
      broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      const evalTimer = trackTimer(setTimeout(() => {
        try {
          const currentGame = STATE.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            evaluateRound(room, game);
          }
        } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS));
      game._evalTimer = evalTimer;
    }
  } catch(e) {}
}

async function leaveGame(ws, username) {
  try {
    if (!username?.trim()) {
      safeSend(ws, ["gameLowCardError", "Username is required"]);
      return;
    }
    
    const usernameClean = username.trim();
    const room = ws.room || ws.roomname || STATE.clientRooms.get(ws._wsId);
    if (!room) {
      safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
      return;
    }
    
    const game = STATE.activeGames.get(room);
    if (!game?._isActive || game._gameEnded || !game.players) {
      safeSend(ws, ["gameLowCardError", "No active game in this room"]);
      return;
    }
    if (!game.players.has(usernameClean)) {
      safeSend(ws, ["gameLowCardError", "You are not in this game"]);
      return;
    }
    removePlayerFromGame(usernameClean, room);
  } catch(e) {}
}

async function checkGameRunning(ws, roomname) {
  try {
    let room = roomname || ws.room || ws.roomname || STATE.clientRooms.get(ws._wsId);
    if (!room) {
      safeSend(ws, ["gameStatus", "false"]);
      return;
    }
    const game = STATE.activeGames.get(room);
    const isRunning = game?._isActive && !game._gameEnded && game.players?.size > 0;
    safeSend(ws, ["gameStatus", isRunning ? "true" : "false"]);
    if (isRunning) sendGameStateToClient(ws, room);
  } catch(e) {}
}

function removePlayerFromGame(username, room) {
  try {
    const game = STATE.activeGames.get(room);
    if (!game || !game.players?.has(username) || !game._isActive || game._gameEnded || game._isEvaluating || game.evaluationLocked) return false;
    if (!game.eliminated) game.eliminated = new Set();
    game.eliminated.add(username);
    game.numbers?.delete(username);
    game.tanda?.delete(username);
    broadcastToRoom(room, ["gameLowCardError", `${username} has been eliminated`]);
    const checkTimer = setTimeout(() => {
      try {
        const currentGame = STATE.activeGames.get(room);
        if (currentGame && currentGame === game && !game._gameEnded) checkGameCanContinue(room, game);
      } catch(e) {}
    }, 1000);
    trackTimer(checkTimer);
    return true;
  } catch(e) { return false; }
}

async function checkGameCanContinue(room, game) {
  try {
    if (!game?._isActive || game._gameEnded || !game.players || game._isEvaluating || game.evaluationLocked || game.registrationOpen) return;
    const activePlayers = getActivePlayers(game);
    if (activePlayers.length === 0) {
      const allPlayers = Array.from(game.players.keys());
      const submitted = Array.from(game.numbers?.keys() || []);
      const notSubmitted = allPlayers.filter(id => !submitted.includes(id) && !game.eliminated?.has(id));
      if (notSubmitted.length > 0) return;
      game._gameEnded = true;
      game._isActive = false;
      broadcastToRoom(room, ["gameLowCardEnd", []]);
      scheduleGameCleanup(room, game);
      return;
    }
    if (activePlayers.length === 1 && !game._gameEnded) {
      const activeIds = getActivePlayerIds(game);
      const submittedIds = Array.from(game.numbers?.keys() || []);
      const notSubmitted = activeIds.filter(id => !submittedIds.includes(id));
      if (notSubmitted.length > 0) {
        broadcastToRoom(room, ["gameLowCardTimeLeft", `Waiting for ${notSubmitted.length} player(s)`]);
        return;
      }
      const winner = activePlayers[0]?.name || "Unknown";
      const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
      if (game._startedByRecording) {
        await addLowCardWinner(room, winner);
        const winners = await getLowCardWinners(room);
        broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room, recording: true }]);
      }
      game._gameEnded = true;
      game._isActive = false;
      broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
      scheduleGameCleanup(room, game);
    }
  } catch(e) {}
}

async function startGameWithRecording(ws, room, bet, username) {
  try {
    if (!room || !username) {
      safeSend(ws, ["gameLowCardError", "Room and username required"]);
      return;
    }

    const isRecordingEnabled = await getRecordingStatusFromKV(room);
    if (!isRecordingEnabled) {
      safeSend(ws, ["gameLowCardError", "Recording is not enabled in this room"]);
      return;
    }

    const existingGame = STATE.activeGames.get(room);
    if (existingGame?._isActive && !existingGame._gameEnded) {
      safeSend(ws, ["gameLowCardError", "Game is already running"]);
      return;
    }
    if (existingGame) await forceCleanupGame(room, existingGame);

    const betAmount = parseInt(bet, 10) || 0;
    if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
      safeSend(ws, ["gameLowCardError", "Invalid bet (0 or 100-100000)"]);
      return;
    }
    if (STATE.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
      safeSend(ws, ["gameLowCardError", "Server is busy"]);
      return;
    }

    const wsId = ws._wsId;
    const game = {
      room, players: new Map(), botPlayers: new Map(), registrationOpen: true,
      round: 1, numbers: new Map(), tanda: new Map(), eliminated: new Set(),
      betAmount, hostId: username, hostName: username, useBots: false,
      evaluationLocked: false, drawTimeExpired: false,
      _isActive: true, _gameEnded: false, _phase: 'registration',
      _botTimeouts: new Set(), _botsAdded: false,
      _registrationTimer: null, _drawTimer: null, _evalTimer: null, _safetyTimer: null,
      _isEvaluating: false, _createdAt: Date.now(), _drawPhaseStart: null, _endTime: null,
      playerWsId: new Map(),
      _startedByRecording: true, _startedBy: 'recording'
    };

    game.players.set(username, { id: username, name: username });
    game.playerWsId.set(username, wsId);
    STATE.activeGames.set(room, game);
    addClient(room, ws, username);
    broadcastToRoom(room, ["gameLowCardStart", betAmount]);
    broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount]);
    startRegistration(room, game);
  } catch(e) {
    safeSend(ws, ["gameLowCardError", "Failed to start game"]);
  }
}

// ==================== WEBSOCKET HANDLERS ====================
function webSocketClose(ws) {
  try {
    if (!ws) return;
    ws._closing = true;
    
    try { ws.removeAllListeners(); } catch(e) {}
    
    const username = ws.username;
    if (username) {
      const attempts = STATE._reconnectAttempts.get(username) || { count: 0, lastAttempt: 0 };
      attempts.count++;
      attempts.lastAttempt = Date.now();
      STATE._reconnectAttempts.set(username, attempts);
      
      if (attempts.count > CONSTANTS.MAX_RECONNECT_ATTEMPTS) {
        const now = Date.now();
        if (now - attempts.lastAttempt < CONSTANTS.RECONNECT_WINDOW_MS) {
          STATE._bannedUsers.set(username, now + CONSTANTS.BAN_DURATION_MS);
          return;
        }
      }
    }
    
    const wsId = ws._wsId;
    const room = ws.room || ws.roomname || STATE.clientRooms.get(wsId);
    
    if (room) removeClientFromRoom(room, wsId);
    if (wsId) {
      STATE.clientRooms.delete(wsId);
      STATE.wsMap.delete(wsId);
    }
    if (username) {
      const conn = STATE.userConnections.get(username);
      if (conn?.wsId === wsId) STATE.userConnections.delete(username);
      
      if (room) {
        broadcastToRoom(room, ["userLeftRoom", username, room]);
      }
    }
    
    ws.room = null;
    ws.roomname = null;
    ws._wsId = null;
    ws.username = null;
    ws._closing = true;
  } catch(e) {}
}

function webSocketError(ws) {
  try {
    if (!ws) return;
    ws._closing = true;
    
    try { ws.removeAllListeners(); } catch(e) {}
    
    const wsId = ws._wsId;
    const username = ws.username;
    const room = ws.room || ws.roomname || STATE.clientRooms.get(wsId);
    
    if (room) removeClientFromRoom(room, wsId);
    if (wsId) {
      STATE.clientRooms.delete(wsId);
      STATE.wsMap.delete(wsId);
    }
    if (username) {
      const conn = STATE.userConnections.get(username);
      if (conn?.wsId === wsId) STATE.userConnections.delete(username);
    }
    
    ws.room = null;
    ws.roomname = null;
    ws._wsId = null;
    ws.username = null;
    ws._closing = true;
  } catch(e) {}
}

function forceStartDice() {
  try {
    if (STATE._tieActive) return false;
    if (STATE._isShowingDice) return false;
    if (STATE._diceTimeUpCooldown) return false;
    if (!isDiceTime() || STATE.currentDiceRoll || STATE._diceTimeout || STATE._diceStartTimeout) {
      return false;
    }
    STATE.diceAutoEnabled = true;
    startDiceFast();
    return true;
  } catch(e) { return false; }
}

// ==================== HANDLE ERROR ====================
function handleError(type, error) {
  try {
    const now = Date.now();
    if (now - STATE._lastErrorReset > CONSTANTS.ERROR_RESET_INTERVAL_MS) {
      STATE._errorCount = 0;
      STATE._lastErrorReset = now;
    }
    STATE._errorCount++;
    
    if (STATE._errorCount > 20) {
      STATE._circuitOpen = true;
      STATE._lastResetTime = now;
    }
  } catch(e) {}
}