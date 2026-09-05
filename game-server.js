// ============================================================
// GAME-SERVER-STORAGE-ONLY.js
// VERSION: 9.0.3 - SWITCH ROOM FIXED
// ============================================================

// ============================================================
// CONSTANTS
// ============================================================

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
  MAX_WS_CLIENTS: 150,
  MAX_EVENT_QUEUE_SIZE: 50,
  ERROR_RESET_INTERVAL_MS: 60000,
  
  DICE_ANSWER_TIME_MS: 20000,
  DICE_TOTAL_TIME_MS: 20000,
  MAX_DICE_VALUE: 6,
  DICE_ROOM: "Quiz",
  
  DICE_AUTO_START_DELAY_MS: 3000,
  TIE_BREAKER_TIME_LIMIT: 20,
  TIE_BREAKER_COOLDOWN: 15000,
  
  BROADCAST_BATCH_SIZE: 10,
  CPU_YIELD_MS: 1,
  MAX_PROCESS_TIME_MS: 500,
  MAX_QUEUE_SIZE: 50,
  RATE_LIMIT_MAX: 100,
  RATE_LIMIT_WINDOW_MS: 60000,
  MAX_BOT_TIMEOUTS: 5,
  MAX_EVENT_ITERATIONS: 2,
  
  WEEKLY_RESET_DAY: 1,
  WEEKLY_RESET_HOUR: 0,
  WEEKLY_RESET_ALARM: 'weekly_reset',
  
  MAX_LEADERBOARD_LIMIT: 30,
  MIN_LEADERBOARD_LIMIT: 1,
  DEFAULT_LEADERBOARD_LIMIT: 10,
};

const QUIZ_SCHEDULE = {
  SESSIONS: [
  
  ],
  TIMEZONE_OFFSET: 8,
};

function parseTime(timeStr) {
  const [hours, minutes] = timeStr.split(':').map(Number);
  return hours * 60 + minutes;
}

// ============================================================
// DATA MANAGER - STORAGE ONLY (NO CACHE, NO KV)
// ============================================================

class DataManager {
  constructor(state) {
    this.state = state;
  }

  // ============================================================
  // RECORDING STATUS
  // ============================================================
  
  async getRecordingStatus(room) {
    try {
      const map = await this.state.storage.get('recordingStatusMap');
      return map && map[room] === true;
    } catch(e) {
      return false;
    }
  }

  async setRecordingStatus(room, enabled) {
    try {
      const map = await this.state.storage.get('recordingStatusMap') || {};
      if (enabled) {
        map[room] = true;
      } else {
        delete map[room];
      }
      await this.state.storage.put('recordingStatusMap', map);
      return true;
    } catch(e) {
      return false;
    }
  }

  async getAllRecordingStatus() {
    try {
      return await this.state.storage.get('recordingStatusMap') || {};
    } catch(e) {
      return {};
    }
  }

  // ============================================================
  // WINNERS
  // ============================================================
  
  async getWinners(room) {
    try {
      const map = await this.state.storage.get('winnersMap');
      return map && map[room] ? map[room] : {};
    } catch(e) {
      return {};
    }
  }

  async setWinners(room, winners) {
    try {
      const map = await this.state.storage.get('winnersMap') || {};
      if (winners && Object.keys(winners).length > 0) {
        map[room] = winners;
      } else {
        delete map[room];
      }
      await this.state.storage.put('winnersMap', map);
      return true;
    } catch(e) {
      return false;
    }
  }

  async addWinner(room, username) {
    try {
      const winners = await this.getWinners(room);
      let count = parseInt(String(winners[username] || "0").replace("x", "")) || 0;
      winners[username] = (count + 1) + "x";
      await this.setWinners(room, winners);
      return true;
    } catch(e) {
      return false;
    }
  }

  async deleteAllWinners(room) {
    return await this.setWinners(room, {});
  }

  async getAllWinners() {
    try {
      return await this.state.storage.get('winnersMap') || {};
    } catch(e) {
      return {};
    }
  }

  // ============================================================
  // DICE POINTS
  // ============================================================
  
  async getDicePoints() {
    try {
      const points = await this.state.storage.get('dicePoints');
      return points || {};
    } catch(e) {
      return {};
    }
  }

  async setDicePoints(points) {
    try {
      const cleanPoints = {};
      for (const [username, score] of Object.entries(points || {})) {
        if (username && typeof username === 'string') {
          const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
          if (numericScore > 0) {
            cleanPoints[username] = numericScore;
          }
        }
      }
      
      if (Object.keys(cleanPoints).length > 0) {
        await this.state.storage.put('dicePoints', cleanPoints);
      } else {
        await this.state.storage.delete('dicePoints');
      }
      return true;
    } catch(e) {
      return false;
    }
  }

  async addDicePoint(username) {
    try {
      const points = await this.getDicePoints();
      points[username] = (points[username] || 0) + 1;
      await this.setDicePoints(points);
      return points;
    } catch(e) {
      return null;
    }
  }

  async resetDicePoints() {
    try {
      await this.state.storage.delete('dicePoints');
      return true;
    } catch(e) {
      return false;
    }
  }

  async getLeaderboard(limit = 10) {
    try {
      const points = await this.getDicePoints();
      if (!points || Object.keys(points).length === 0) {
        return [];
      }
      
      const safeLimit = Math.min(
        Math.max(limit, CONSTANTS.MIN_LEADERBOARD_LIMIT),
        CONSTANTS.MAX_LEADERBOARD_LIMIT
      );
      
      const sorted = Object.entries(points)
        .filter(([username, score]) => username && score > 0)
        .sort((a, b) => b[1] - a[1])
        .slice(0, safeLimit);
      
      return sorted.map(([u, s]) => `${u}|${s}`);
    } catch(e) {
      return [];
    }
  }

  // ============================================================
  // LAST WEEK WINNER
  // ============================================================
  
  async getLastWeekWinner() {
    try {
      return await this.state.storage.get('lastWeekWinner') || null;
    } catch(e) {
      return null;
    }
  }

  async setLastWeekWinner(winnerData) {
    try {
      if (winnerData && winnerData.username) {
        await this.state.storage.put('lastWeekWinner', winnerData);
      } else {
        await this.state.storage.delete('lastWeekWinner');
      }
      return true;
    } catch(e) {
      return false;
    }
  }

  async deleteLastWeekWinner() {
    try {
      await this.state.storage.delete('lastWeekWinner');
      return true;
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // RESET WEEK TRACKING
  // ============================================================
  
  async getLastResetWeek() {
    try {
      return await this.state.storage.get('lastResetWeek') || null;
    } catch(e) {
      return null;
    }
  }

  async setLastResetWeek(week) {
    try {
      await this.state.storage.put('lastResetWeek', week);
      return true;
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // ALARMS
  // ============================================================
  
  async getAlarms() {
    try {
      return await this.state.storage.get('scheduled_alarms') || {};
    } catch(e) {
      return {};
    }
  }

  async setAlarms(alarms) {
    try {
      await this.state.storage.put('scheduled_alarms', alarms);
      return true;
    } catch(e) {
      return false;
    }
  }

  async deleteAlarms() {
    try {
      await this.state.storage.delete('scheduled_alarms');
      return true;
    } catch(e) {
      return false;
    }
  }

  // ============================================================
  // UTILITY
  // ============================================================
  
  getCurrentWeek() {
    const now = new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }

  async clearAllData() {
    try {
      await this.state.storage.delete('recordingStatusMap');
      await this.state.storage.delete('winnersMap');
      await this.state.storage.delete('dicePoints');
      await this.state.storage.delete('lastWeekWinner');
      await this.state.storage.delete('lastResetWeek');
      await this.state.storage.delete('scheduled_alarms');
      return true;
    } catch(e) {
      return false;
    }
  }

  async getStorageSize() {
    try {
      const keys = await this.state.storage.list();
      let totalSize = 0;
      for (const key of keys.keys) {
        const value = await this.state.storage.get(key.name);
        totalSize += JSON.stringify(value).length;
      }
      return {
        keyCount: keys.keys.length,
        totalSizeBytes: totalSize,
        totalSizeKB: (totalSize / 1024).toFixed(2),
        keys: keys.keys.map(k => k.name)
      };
    } catch(e) {
      return { error: e.message };
    }
  }
}

// ============================================================
// ALARM SCHEDULER - STORAGE ONLY
// ============================================================

class AlarmScheduler {
  constructor(state) {
    this.state = state;
    this._alarms = new Map();
    this._alarmsKey = 'scheduled_alarms';
  }

  async scheduleAlarms() {
    try {
      const now = new Date();
      const witaNow = this._toWITA(now);
      const currentTotal = witaNow.getHours() * 60 + witaNow.getMinutes();
      
      await this._clearAllAlarms();
      await this._scheduleWeeklyReset();
      
      let currentSession = null;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        const endTotal = parseTime(session.end);
        if (currentTotal >= startTotal && currentTotal < endTotal) {
          currentSession = { ...session, startTotal, endTotal, status: 'active' };
          break;
        }
      }
      
      if (currentSession) {
        const endDelay = (currentSession.endTotal - currentTotal) * 60 * 1000;
        if (endDelay > 0) {
          await this._scheduleAlarm('dice_session_end', endDelay);
        }
        return true;
      }
      
      let nextSession = null;
      let minDiff = Infinity;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) {
          minDiff = diff;
          nextSession = { ...session, startTotal, status: 'upcoming' };
        }
      }
      
      if (nextSession) {
        let startDelay = minDiff * 60 * 1000;
        if (startDelay < 0) startDelay = 0;
        await this._scheduleAlarm('dice_session_start', startDelay);
        
        const endTotal = parseTime(nextSession.end);
        const endDelay = (endTotal - currentTotal) * 60 * 1000;
        if (endDelay > 0) {
          await this._scheduleAlarm('dice_session_end', endDelay);
        }
      }
      
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async _scheduleWeeklyReset() {
    try {
      const now = new Date();
      const currentDay = now.getUTCDay();
      const currentHour = now.getUTCHours();
      const currentMinutes = now.getUTCMinutes();
      const currentSeconds = now.getUTCSeconds();
      
      let daysUntilReset = CONSTANTS.WEEKLY_RESET_DAY - currentDay;
      if (daysUntilReset < 0) daysUntilReset += 7;
      if (daysUntilReset === 0 && (currentHour > 0 || currentMinutes > 0 || currentSeconds > 0)) {
        daysUntilReset = 7;
      }
      
      const resetTime = new Date(Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate() + daysUntilReset,
        0, 0, 0, 0
      ));
      
      const delayMs = resetTime.getTime() - now.getTime();
      
      if (delayMs > 0) {
        await this._scheduleAlarm(CONSTANTS.WEEKLY_RESET_ALARM, delayMs);
      }
      
      return true;
    } catch(e) {
      return false;
    }
  }

  async _scheduleAlarm(name, delayMs) {
    try {
      if (delayMs < 1000) delayMs = 1000;
      
      const scheduledAt = Date.now() + delayMs;
      const alarm = { 
        name, 
        scheduledAt,
        delayMs,
        timestamp: Date.now()
      };
      
      this._alarms.set(name, alarm);
      await this._saveAlarms();
      await this._scheduleNearestAlarm();
      
      return true;
    } catch(e) { 
      return false; 
    }
  }

  async _saveAlarms() {
    try {
      const data = {};
      for (const [name, alarm] of this._alarms) {
        data[name] = alarm;
      }
      await this.state.storage.put(this._alarmsKey, data);
    } catch(e) {}
  }

  async _scheduleNearestAlarm() {
    try {
      let nearestTime = Infinity;
      for (const [name, alarm] of this._alarms) {
        const time = alarm.scheduledAt || alarm.timestamp + alarm.delayMs;
        if (time < nearestTime && time > Date.now()) {
          nearestTime = time;
        }
      }
      
      if (nearestTime < Infinity) {
        const delay = nearestTime - Date.now();
        if (delay > 0) {
          await this.state.storage.setAlarm(Date.now() + delay);
        }
      }
    } catch(e) {}
  }

  async _clearAllAlarms() {
    try {
      this._alarms.clear();
      await this.state.storage.delete(this._alarmsKey);
      await this.state.storage.deleteAlarm();
    } catch(e) {}
  }

  async getPendingAlarms() {
    try {
      await this.restoreAlarms();
      
      const pending = [];
      const now = Date.now();
      const expired = [];
      
      for (const [name, alarm] of this._alarms) {
        const scheduledTime = alarm.scheduledAt || alarm.timestamp + alarm.delayMs;
        if (scheduledTime <= now) {
          pending.push({ ...alarm, name });
          expired.push(name);
        }
      }
      
      for (const name of expired) {
        this._alarms.delete(name);
      }
      
      if (expired.length > 0) {
        await this._saveAlarms();
      }
      
      return pending;
    } catch(e) { 
      return []; 
    }
  }

  async processAlarm(name) {
    try {
      const alarm = this._alarms.get(name);
      if (!alarm) {
        await this.restoreAlarms();
        const restored = this._alarms.get(name);
        if (!restored) return null;
      }
      
      this._alarms.delete(name);
      await this._saveAlarms();
      await this._scheduleNearestAlarm();
      
      return alarm || this._alarms.get(name);
    } catch(e) {
      return null;
    }
  }

  async restoreAlarms() {
    try {
      const stored = await this.state.storage.get(this._alarmsKey);
      if (stored && typeof stored === 'object') {
        this._alarms.clear();
        for (const [name, data] of Object.entries(stored)) {
          this._alarms.set(name, data);
        }
        await this._scheduleNearestAlarm();
        return true;
      }
      return false;
    } catch(e) { return false; }
  }

  _toWITA(date) {
    const wita = new Date(date);
    wita.setHours(wita.getHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET);
    return wita;
  }

  isDiceTime(date) {
    const wita = this._toWITA(date || new Date());
    const currentTotal = wita.getHours() * 60 + wita.getMinutes();
    
    for (const session of QUIZ_SCHEDULE.SESSIONS) {
      const startTotal = parseTime(session.start);
      const endTotal = parseTime(session.end);
      if (currentTotal >= startTotal && currentTotal < endTotal) {
        return true;
      }
    }
    return false;
  }

  async cleanup() {
    await this._clearAllAlarms();
  }
}

// ============================================================
// GAME SERVER - STORAGE ONLY (SWITCH ROOM FIXED)
// ============================================================

export class GameServer {
  constructor(state, env) {
    try {
      this.state = state;
      this.env = env;
      this.ctx = state;
      this.closing = false;
      this.isDestroyed = false;
      this._initialized = false;
      this._startTime = Date.now();
      this._wsIdCounter = 0;
      
      // ONLY DATA MANAGER - NO CACHE
      this.dataManager = new DataManager(state);
      
      this.alarmScheduler = new AlarmScheduler(state);
      
      // Game state
      this.activeGames = new Map();
      this.wsMap = new Map();
      this.wsClients = new Map();
      this.clientRooms = new Map();
      this.userConnections = new Map();
      this._eventQueue = [];
      this._allTimers = new Set();
      this._lastNotifTime = {};
      this._lastWinnerRequestTime = new Map();
      
      // NOTIFICATION TIMERS
      this._notificationTimers = new Set();
      
      // Dice state
      this.currentDiceRoll = null;
      this._diceLock = false;
      this._tieActive = false;
      this.diceAnswered = new Set();
      this._playerAnswers = new Map();
      this._isShowingDice = false;
      this._diceTimeUpCooldown = false;
      this._diceQuestionStartTime = null;
      this._diceStartTime = null;
      this._diceTimeout = null;
      this._diceStartTimeout = null;
      this._diceTimeUpCooldownTimer = null;
      this._diceCooldownTimer = null;
      this._diceNotificationTimeouts = [];
      this.diceAutoEnabled = false;
      this.diceHasWinner = false;
      this.diceWinner = null;
      this.diceEndNotified = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      this._lastSentRemaining = -1;
      this._diceOutOfTimeShown = false;
      this._diceTaskRunning = false;
      this._canSubmitDiceAnswer = false;
      this._diceRound = 0;
      
      // Tie breaker state
      this._tieBreakers = new Map();
      this._tieRound = 0;
      this._tiePlayers = [];
      this._tieAnswers = new Map();
      this._tieTimer = null;
      this._tieInterval = null;
      this._tieLock = false;
      this._tieNotificationTimeouts = [];
      
      // Locks
      this._gameLocks = new Map();
      this._joinLocks = new Map();
      this._cleanupTimers = new Map();
      this._switchLocks = new Map();
      this._switchRetries = new Map();
      this._evaluationLocks = new Map();
      this._gameOperationLocks = new Map();
      this._drawLocks = new Map();
      this._cleanupLocks = new Map();
      
      // Rate limiting
      this._requestCount = 0;
      this._lastResetTime = Date.now();
      this._circuitOpen = false;
      this._errorCount = 0;
      this._lastErrorReset = Date.now();
      this._reconnectAttempts = new Map();
      
      this.DICE_ROOM = CONSTANTS.DICE_ROOM;
      
      // Inisialisasi
      this._init();
      
    } catch(e) {
      console.error('Constructor error:', e);
    }
  }

  // ============================================================
  // INIT - STORAGE ONLY
  // ============================================================
  async _init() {
    try {
      await this.alarmScheduler.restoreAlarms();
      await this.alarmScheduler.scheduleAlarms();
      await this._checkAndForceResetIfMonday();
      this._initialized = true;
      
      if (this.alarmScheduler.isDiceTime()) {
        const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
        if (clients && clients.size > 0) {
          this._startDiceFast();
        }
      }
    } catch(e) {
      console.error('Init error:', e);
      this._initialized = true;
    }
  }

  // ============================================================
  // WEEKLY RESET - STORAGE ONLY
  // ============================================================
  async _checkAndForceResetIfMonday() {
    try {
      const now = new Date();
      const currentDay = now.getUTCDay();
      const currentWeek = this.dataManager.getCurrentWeek();
      
      let lastResetWeek = await this.dataManager.getLastResetWeek();
      if (!lastResetWeek) {
        await this.dataManager.setLastResetWeek(currentWeek);
        return true;
      }
      
      if (lastResetWeek === currentWeek) return true;
      
      if (currentDay === CONSTANTS.WEEKLY_RESET_DAY) {
        await this._handleWeeklyReset();
        await this.dataManager.setLastResetWeek(currentWeek);
      }
      
      return true;
    } catch(e) {
      const currentWeek = this.dataManager.getCurrentWeek();
      await this.dataManager.setLastResetWeek(currentWeek);
      return false;
    }
  }

  async _handleWeeklyReset() {
    try {
      const points = await this.dataManager.getDicePoints();
      
      let winner = null;
      let highestScore = 0;
      for (const [username, score] of Object.entries(points)) {
        if (username && typeof username === 'string') {
          const numericScore = typeof score === 'number' ? score : parseInt(score, 10) || 0;
          if (numericScore > highestScore) {
            highestScore = numericScore;
            winner = username;
          }
        }
      }
      
      const currentWeek = this.dataManager.getCurrentWeek();
      
      if (winner && highestScore > 0) {
        const winnerData = { 
          username: winner, 
          score: highestScore, 
          week: currentWeek,
          timestamp: Date.now() 
        };
        await this.dataManager.setLastWeekWinner(winnerData);
      } else {
        await this.dataManager.deleteLastWeekWinner();
      }
      
      await this.dataManager.resetDicePoints();
      return true;
    } catch(e) {
      console.error('Weekly reset error:', e);
      const currentWeek = this.dataManager.getCurrentWeek();
      await this.dataManager.setLastResetWeek(currentWeek);
      return false;
    }
  }

  // ============================================================
  // ALARM HANDLER
  // ============================================================
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    try {
      await this.alarmScheduler.restoreAlarms();
      const pendingAlarms = await this.alarmScheduler.getPendingAlarms();
      
      for (const alarm of pendingAlarms) {
        try {
          await this._processAlarm(alarm.name);
          await this.alarmScheduler.processAlarm(alarm.name);
        } catch(e) {}
      }
      
      await this.alarmScheduler.scheduleAlarms();
    } catch(e) {}
  }

  // ============================================================
  // PROCESS ALARM
  // ============================================================
  async _processAlarm(name) {
    switch(name) {
      case CONSTANTS.WEEKLY_RESET_ALARM:
        await this._handleWeeklyReset();
        break;
      case 'dice_session_start':
        if (this.alarmScheduler.isDiceTime()) {
          this.diceAutoEnabled = true;
          if (!this.currentDiceRoll && !this._isShowingDice && !this._diceLock && !this._diceTimeUpCooldown) {
            const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
            if (clients && clients.size > 0) {
              this._startDiceFast();
            }
          }
        }
        break;
      case 'dice_session_end':
        this.diceAutoEnabled = false;
        
        const timeUntilNext = this._getTimeLeftUntilNextDice();
        if (timeUntilNext.totalMs > 0) {
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", `⏰ Dice session ended. Next game in: ${timeUntilNext.text}`]);
        } else {
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "⏰ Dice session ended"]);
        }
        
        if (this.currentDiceRoll || this._isShowingDice) {
          this._endDiceRound();
        }
        break;
    }
  }

  // ============================================================
  // FETCH / WEBSOCKET
  // ============================================================
  async fetch(req) {
    try {
      if (this._circuitOpen) {
        const now = Date.now();
        if (now - this._lastResetTime > 60000) {
          this._circuitOpen = false;
          this._requestCount = 0;
          this._lastResetTime = now;
        } else {
          return new Response("Service temporarily unavailable", { 
            status: 503,
            headers: { 'Retry-After': '30', 'Content-Type': 'text/plain' }
          });
        }
      }
      
      this._requestCount++;
      if (this._requestCount > CONSTANTS.RATE_LIMIT_MAX) {
        this._circuitOpen = true;
        this._lastResetTime = Date.now();
        return new Response("Rate limit exceeded", { 
          status: 429,
          headers: { 'Retry-After': '60', 'Content-Type': 'text/plain' }
        });
      }
      
      setTimeout(() => {
        this._requestCount = Math.max(0, this._requestCount - 50);
      }, CONSTANTS.RATE_LIMIT_WINDOW_MS);
      
      const url = new URL(req.url);
      
      if (url.pathname === "/game/ws") {
        const upgrade = req.headers.get("Upgrade");
        if (upgrade !== "websocket") {
          return new Response("WebSocket only", { status: 400 });
        }
        
        if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
          return new Response("Server full", { status: 503 });
        }
        
        if (this._eventQueue?.length > 500) {
          return new Response("Server busy", { status: 503 });
        }
        
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        const wsId = ++this._wsIdCounter;
        
        try {
          this.ctx.acceptWebSocket(server);
        } catch(e) {
          try { server.close(1008, "Accept failed"); } catch(err) {}
          return new Response("WebSocket acceptance failed", { status: 500 });
        }
        
        server.serializeAttachment({
          wsId: wsId,
          username: null,
          room: null,
          roomname: null,
          createdAt: Date.now()
        });
        
        server._wsId = wsId;
        server._closing = false;
        server.username = null;
        server.room = null;
        server.roomname = null;
        server._createdAt = Date.now();
        
        this.wsMap.set(wsId, server);
        
        return new Response(null, { 
          status: 101, 
          webSocket: client 
        });
      }
      
      return new Response("Game Server", { status: 200 });
    } catch(e) {
      this._handleError('fetch', e);
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  // ============================================================
  // WEBSOCKET HANDLERS
  // ============================================================
  async webSocketMessage(ws, message) {
    if (!ws || ws._closing || this.closing || this.isDestroyed) return;
    
    try {
      let attachment = null;
      try {
        attachment = ws.deserializeAttachment();
      } catch(e) {}
      
      if (attachment && attachment.wsId) {
        ws._wsId = attachment.wsId;
        ws.username = attachment.username || null;
        ws.room = attachment.room || null;
        ws.roomname = attachment.roomname || null;
        ws._createdAt = attachment.createdAt || Date.now();
        
        if (attachment.username && attachment.room) {
          this.userConnections.set(attachment.username, {
            wsId: attachment.wsId,
            ws: ws,
            room: attachment.room,
            timestamp: Date.now()
          });
        }
      }
      
      const data = JSON.parse(message);
      if (Array.isArray(data) && data.length > 0) {
        await this._processWithTimeout(ws, data);
        
        // Re-sync state setelah event tertentu
        const evt = data[0];
        const syncEvents = ['switchRoom', 'gameLowCardJoin', 'gameLowCardStart', 'gameLowCardNumber', 'checkGameRunning', 'gameLowCardLeave'];
        if (syncEvents.includes(evt)) {
          const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
          if (room) {
            setTimeout(() => {
              if (ws && ws.readyState === 1) {
                this._sendGameStateToClient(ws, room);
              }
            }, 100);
          }
        }
      }
    } catch(e) {}
  }

  async webSocketClose(ws, code, reason, wasClean) {
    if (!ws) return;
    try {
      const attachment = ws.deserializeAttachment();
      const username = attachment?.username;
      const room = attachment?.room;
      const wsId = attachment?.wsId;
      
      if (username) {
        this.userConnections.delete(username);
        if (room) {
          this._broadcastToRoom(room, ["userLeftRoom", username, room]);
        }
      }
      
      if (room && wsId) {
        const clients = this.wsClients.get(room);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) {
            this.wsClients.delete(room);
          }
        }
      }
      
      if (wsId) {
        this.wsMap.delete(wsId);
        this.clientRooms.delete(wsId);
      }
      
      try {
        ws.serializeAttachment({
          wsId: wsId,
          username: null,
          room: null,
          roomname: null,
          createdAt: Date.now()
        });
      } catch(e) {}
    } catch(e) {}
  }

  async webSocketError(ws, error) {
    if (!ws) return;
    try {
      const attachment = ws.deserializeAttachment();
      const username = attachment?.username;
      const room = attachment?.room;
      const wsId = attachment?.wsId;
      
      if (username) {
        this.userConnections.delete(username);
        if (room) {
          this._broadcastToRoom(room, ["userLeftRoom", username, room]);
        }
      }
      
      if (room && wsId) {
        const clients = this.wsClients.get(room);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) {
            this.wsClients.delete(room);
          }
        }
      }
      
      if (wsId) {
        this.wsMap.delete(wsId);
        this.clientRooms.delete(wsId);
      }
      
      try {
        ws.serializeAttachment({
          wsId: wsId,
          username: null,
          room: null,
          roomname: null,
          createdAt: Date.now()
        });
      } catch(e) {}
    } catch(e) {}
  }

  // ============================================================
  // EVENT PROCESSING
  // ============================================================
  async _processWithTimeout(ws, data, timeoutMs = 500) {
    try {
      const timeoutPromise = new Promise((_, reject) => {
        const timer = setTimeout(() => reject(new Error('Processing timeout')), timeoutMs);
        this._trackTimer(timer);
      });
      await Promise.race([
        this.handleEvent(ws, data),
        timeoutPromise
      ]);
    } catch(e) {}
  }

  async handleEvent(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data?.[0]) return;
      if (this._eventQueue.length > CONSTANTS.MAX_EVENT_QUEUE_SIZE) {
        this._safeSend(ws, ["gameLowCardError", "Server busy"]);
        return;
      }
      this._eventQueue.push({ ws, data });
      if (!this._isProcessingQueue) await this._processEventQueue();
    } catch(e) {}
  }

  async _processEventQueue(iteration = 0) {
    try {
      if (this._isProcessingQueue || this._eventQueue.length === 0) return;
      this._isProcessingQueue = true;
      if (iteration > CONSTANTS.MAX_EVENT_ITERATIONS) {
        this._isProcessingQueue = false;
        return;
      }
      const startTime = Date.now();
      let processed = 0;
      while (this._eventQueue.length > 0 && processed < 3) {
        if (Date.now() - startTime > CONSTANTS.MAX_PROCESS_TIME_MS) break;
        const item = this._eventQueue.shift();
        try { await this._processEventItem(item.ws, item.data); } catch(e) {}
        processed++;
      }
      if (this._eventQueue.length > 0 && iteration < CONSTANTS.MAX_EVENT_ITERATIONS) {
        setTimeout(() => {
          if (!this.closing && !this.isDestroyed) {
            this._isProcessingQueue = false;
            this._processEventQueue(iteration + 1);
          }
        }, 5);
      }
    } catch(e) {
      this._handleError('processQueue', e);
    } finally {
      this._isProcessingQueue = false;
    }
  }

  async _processEventItem(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      await this._handleEventInternal(ws, data);
    } catch(e) {}
  }

  // ============================================================
  // HANDLE EVENT INTERNAL - ALL EVENTS
  // ============================================================
  async _handleEventInternal(ws, data) {
    try {
      if (this.isDestroyed || !ws || !data || !data[0]) return;
      const evt = data[0];

      // SWITCH ROOM - FIXED
      if (evt === "switchRoom") {
        await this.switchRoom(ws, data[1], data[2]);
        return;
      }

      // RECORDING - STORAGE ONLY
      if (evt === "startRecordingWinners") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        const room = roomName.trim();
        await this.dataManager.setRecordingStatus(room, true);
        this._broadcastToRoom(room, ["recordingStatus", true]);
        this._safeSend(ws, ["startRecordingResult", { success: true, message: "Recording enabled" }]);
        return;
      }

      if (evt === "stopRecordingWinners") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        const room = roomName.trim();
        await this.dataManager.setRecordingStatus(room, false);
        await this.dataManager.deleteAllWinners(room);
        this._broadcastToRoom(room, ["recordingStatus", false]);
        this._safeSend(ws, ["stopRecordingResult", { success: true, message: "Recording stopped and winners deleted" }]);
        return;
      }

      if (evt === "getRecordingStatus") {
        const roomName = data[1];
        if (!roomName || typeof roomName !== 'string' || roomName.trim() === '') {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        const room = roomName.trim();
        const isRecording = await this.dataManager.getRecordingStatus(room);
        this._safeSend(ws, ["recordingStatus", isRecording]);
        return;
      }

      // WINNERS - STORAGE ONLY
      if (evt === "addLowCardWinner") {
        const { room, username } = data[1] || {};
        if (!room || !username || typeof room !== 'string' || typeof username !== 'string') {
          this._safeSend(ws, ["error", "Room and username required"]);
          return;
        }
        const roomKey = room.trim();
        const userKey = username.trim();
        
        const winners = await this.dataManager.getWinners(roomKey);
        let count = parseInt(String(winners[userKey] || "0").replace("x", "")) || 0;
        winners[userKey] = (count + 1) + "x";
        await this.dataManager.setWinners(roomKey, winners);
        
        this._broadcastLowCardWinners(roomKey);
        this._safeSend(ws, ["addWinnerResult", { success: true }]);
        return;
      }

      if (evt === "deleteAllWinners") {
        const room = data[1];
        if (!room || typeof room !== 'string' || room.trim() === '') {
          this._safeSend(ws, ["error", "Room required"]);
          return;
        }
        const roomKey = room.trim();
        await this.dataManager.deleteAllWinners(roomKey);
        this._broadcastToRoom(roomKey, ["recordingStatus", false]);
        this._safeSend(ws, ["deleteWinnersResult", { success: true }]);
        return;
      }

      if (evt === "getRoomWinners") {
        let room = data[1] || ws.room || this.clientRooms.get(ws._wsId);
        if (!room || typeof room !== 'string' || room.trim() === '') {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        const roomKey = room.trim();
        const isRecording = await this.dataManager.getRecordingStatus(roomKey);
        const winners = await this.dataManager.getWinners(roomKey);
        this._safeSend(ws, ["roomWinners", { 
          winners: winners || {}, 
          room: roomKey, 
          recording: isRecording || false 
        }]);
        return;
      }

      if (evt === "sendWinnersToRoom" || evt === "lowCardWinnerUpdate") {
        let room = data[1] || ws.room || this.clientRooms.get(ws._wsId);
        if (!room || typeof room !== 'string' || room.trim() === '') {
          this._safeSend(ws, ["recordingError", "Room name required"]);
          return;
        }
        const roomKey = room.trim();
        await this._broadcastLowCardWinners(roomKey);
        this._safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners refreshed" }]);
        return;
      }

      // DICE ANSWER
      if (evt === "submitDiceAnswer") {
        await this.submitDiceAnswer(ws, data[1], data[2]);
        return;
      }

      // GET DICE LAST WEEK WINNER - STORAGE ONLY
      if (evt === "getDiceLastWeekWinner") {
        try {
          const wsId = ws._wsId;
          const now = Date.now();
          if (!this._lastWinnerRequestTime) this._lastWinnerRequestTime = new Map();
          const lastReq = this._lastWinnerRequestTime.get(wsId) || 0;
          if (now - lastReq < 5000) return;
          this._lastWinnerRequestTime.set(wsId, now);
          
          const winner = await this.dataManager.getLastWeekWinner();
          
          if (winner && typeof winner === 'object' && winner.username) {
            this._safeSend(ws, [
              "diceLastWeekWinner", 
              String(winner.username), 
              parseInt(winner.score, 10) || 0, 
              String(winner.week || '')
            ]);
          } else {
            this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
          }
        } catch(e) {
          this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
        }
        return;
      }

      if (evt === "deleteDiceLastWeekWinner") {
        try {
          await this.dataManager.deleteLastWeekWinner();
          this._safeSend(ws, ["diceLastWeekWinnerDeleted", true, "Deleted"]);
        } catch(e) { 
          this._safeSend(ws, ["diceLastWeekWinnerDeleted", false, e.message]); 
        }
        return;
      }

      // GET DICE LEADERBOARD - STORAGE ONLY
      if (evt === "getDiceLeaderboard") {
        try {
          let limit = CONSTANTS.DEFAULT_LEADERBOARD_LIMIT;
          if (data.length > 1 && typeof data[1] === 'number') {
            limit = Math.min(Math.max(data[1], CONSTANTS.MIN_LEADERBOARD_LIMIT), CONSTANTS.MAX_LEADERBOARD_LIMIT);
          }
          
          const leaderboard = await this.dataManager.getLeaderboard(limit);
          this._safeSend(ws, ["diceLeaderboard", leaderboard]);
        } catch(e) { 
          this._safeSend(ws, ["diceLeaderboard", []]);
        }
        return;
      }

      // GET DICE POINTS - STORAGE ONLY
      if (evt === "getDicePoints") {
        try {
          const points = await this.dataManager.getDicePoints();
          this._safeSend(ws, ["dicePoints", points]);
        } catch(e) { 
          this._safeSend(ws, ["dicePoints", {}]);
        }
        return;
      }

      if (evt === "getDiceStatus") {
        const isActive = !!this.currentDiceRoll && this._canSubmitDiceAnswer;
        this._safeSend(ws, ["diceStatus", isActive, this._diceRound || 1]);
        return;
      }

      if (evt === "getDiceNotification") {
        try {
          const isDiceTime = this.alarmScheduler.isDiceTime();
          const isActive = this.currentDiceRoll && this._canSubmitDiceAnswer;
          const timeLeft = this._getTimeLeftUntilNextDice();
          let notification = "";
          if (isActive) {
            const elapsed = (Date.now() - this._diceStartTime) / 1000;
            const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
            const remaining = Math.max(0, totalTime - elapsed);
            notification = `${Math.floor(remaining)}s remaining`;
          } else if (isDiceTime) {
            notification = "Dice game starting soon...";
          } else if (timeLeft && timeLeft.text) {
            notification = `Next dice game in: ${timeLeft.text}`;
          } else {
            notification = "Waiting...";
          }
          this._safeSend(ws, ["diceNotification", notification]);
        } catch(e) {
          this._safeSend(ws, ["diceNotification", "Waiting..."]);
        }
        return;
      }

      if (evt === "startGameWithRecording") {
        const [_, room, bet, username] = data;
        if (!room || !username || typeof room !== 'string' || typeof username !== 'string') {
          this._safeSend(ws, ["gameLowCardError", "Room and username required"]);
          return;
        }
        await this._startGameWithRecording(ws, room.trim(), bet, username.trim());
        return;
      }

      // LOW CARD GAME
      const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      if (room === CONSTANTS.DICE_ROOM) {
        this._safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]);
        return;
      }

      switch (evt) {
        case "gameLowCardStart": 
          await this.startGame(ws, data[1], data[2]); 
          break;
        case "gameLowCardJoin": 
          await this.joinGame(ws, data[1]); 
          break;
        case "gameLowCardNumber": 
          await this.submitNumber(ws, data[1], data[2] || "", data[3]); 
          break;
        case "gameLowCardLeave": 
          await this.leaveGame(ws, data[1]); 
          break;
        case "checkGameRunning": 
          await this.checkGameRunning(ws, data[1]); 
          break;
        case "getGameState": 
          this._sendGameStateToClient(ws, data[1] || room); 
          break;
        default: 
          break;
      }
    } catch(e) {}
  }

  // ============================================================
  // ROOM SWITCHING - FIXED (NO BLOCK)
  // ============================================================
  async switchRoom(ws, room, username = null) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!room || typeof room !== 'string' || room.trim() === "") {
        this._safeSend(ws, ["gameLowCardError", "Invalid room name"]);
        return;
      }
      
      const roomName = room.trim();
      const wsId = ws._wsId;
      
      if (!wsId) {
        this._safeSend(ws, ["gameLowCardError", "Connection error"]);
        return;
      }
      
      const currentRoom = ws.room || ws.roomname || this.clientRooms.get(wsId);
      
      // Jika sudah di room yang sama, langsung kirim state
      if (currentRoom === roomName) {
        this._safeSend(ws, ["switchRoomSuccess", roomName]);
        this._sendGameStateToClient(ws, roomName);
        if (roomName === CONSTANTS.DICE_ROOM) {
          this._sendDiceNotificationOnSwitch(ws, wsId);
          this._checkAndStartDiceIfNeeded(ws);
        }
        this._sendWinnersToClient(ws, roomName);
        return;
      }
      
      // Hapus dari room lama
      if (currentRoom) {
        const clients = this.wsClients.get(currentRoom);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) {
            this.wsClients.delete(currentRoom);
          }
        }
      }
      
      // Tambah ke room baru
      if (!this.wsClients.has(roomName)) {
        this.wsClients.set(roomName, new Set());
      }
      this.wsClients.get(roomName).add(wsId);
      this.clientRooms.set(wsId, roomName);
      
      // Update ws
      ws.room = roomName;
      ws.roomname = roomName;
      if (username) ws.username = username;
      
      // Update attachment
      ws.serializeAttachment({
        wsId: wsId,
        username: username || null,
        room: roomName,
        roomname: roomName,
        createdAt: ws._createdAt || Date.now()
      });
      
      // Update user connections
      if (username) {
        let conn = this.userConnections.get(username);
        if (conn) { 
          conn.room = roomName; 
          conn.wsId = wsId; 
          conn.ws = ws; 
          conn.timestamp = Date.now(); 
        } else { 
          this.userConnections.set(username, { 
            wsId, 
            ws, 
            room: roomName, 
            timestamp: Date.now() 
          }); 
        }
      }
      
      // Kirim response success
      this._safeSend(ws, ["switchRoomSuccess", roomName]);
      
      // Kirim state lengkap room
      this._sendGameStateToClient(ws, roomName);
      
      // Kirim winners
      this._sendWinnersToClient(ws, roomName);
      
      // Handle dice room
      if (roomName === CONSTANTS.DICE_ROOM) {
        this._sendDiceNotificationOnSwitch(ws, wsId);
        this._checkAndStartDiceIfNeeded(ws);
      }
      
      // Broadcast ke room
      if (username) {
        this._broadcastToRoom(roomName, ["userJoinedRoom", username, roomName]);
        if (currentRoom && currentRoom !== roomName) {
          this._broadcastToRoom(currentRoom, ["userLeftRoom", username, currentRoom]);
        }
      }
      
    } catch(e) {
      this._safeSend(ws, ["switchRoomError", e.message || "Switch failed"]);
    }
  }

  // ============================================================
  // SEND WINNERS TO CLIENT
  // ============================================================
  async _sendWinnersToClient(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room) return;
      if (room === CONSTANTS.DICE_ROOM) return;
      
      const isRecording = await this.dataManager.getRecordingStatus(room);
      const winners = await this.dataManager.getWinners(room);
      
      this._safeSend(ws, ["roomWinners", { 
        winners: winners || {}, 
        room: room, 
        recording: isRecording || false 
      }]);
    } catch(e) {}
  }

  // ============================================================
  // GAME: START
  // ============================================================
  async startGame(ws, bet, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username || typeof username !== 'string' || !username.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]);
        return;
      }
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      if (room === CONSTANTS.DICE_ROOM) {
        this._safeSend(ws, ["gameLowCardError", "Cannot start game in Quiz room"]);
        return;
      }
      
      const lockKey = `game_start_${room}`;
      if (this._gameLocks.has(lockKey)) {
        this._safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]);
        return;
      }
      this._gameLocks.set(lockKey, Date.now());
      
      try {
        const isRecordingEnabled = await this.dataManager.getRecordingStatus(room);
        if (isRecordingEnabled) {
          this._safeSend(ws, ["gameLowCardError", "Recording is ACTIVE in this room. Users cannot start games."]);
          return;
        }
        const existingGame = this.activeGames.get(room);
        if (existingGame?._isActive && !existingGame._gameEnded) {
          this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
          return;
        }
        if (existingGame) await this._forceCleanupGame(room, existingGame);
        const betAmount = parseInt(bet, 10) || 0;
        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          this._safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
          return;
        }
        if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
          this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
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
          _startedByRecording: false, _startedBy: 'user',
          _notificationTimers: [],
          _drawNotificationTimers: []
        };
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        game.playerWsId.set(usernameClean, wsId);
        this.activeGames.set(room, game);
        this._addClient(room, ws, usernameClean);
        this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
        this._broadcastToRoom(room, ["gameLowCardStartSuccess", usernameClean, betAmount]);
        this._startRegistration(room, game);
        
        setTimeout(() => {
          this._broadcastGameStateToRoom(room);
        }, 100);
        
      } finally {
        setTimeout(() => { this._gameLocks.delete(lockKey); }, 3000);
      }
    } catch(e) {}
  }

  _startRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      if (game._registrationTimer) {
        this._clearTimer(game._registrationTimer);
        game._registrationTimer = null;
      }
      if (game._notificationTimers) {
        for (const timer of game._notificationTimers) {
          this._clearTimer(timer);
        }
        game._notificationTimers = [];
      } else {
        game._notificationTimers = [];
      }
      const notifications = [
        { delay: 5000, message: "15s" },
        { delay: 10000, message: "10s" },
        { delay: 15000, message: "5s" }
      ];
      for (const notif of notifications) {
        const timer = this._trackTimer(setTimeout(() => {
          if (this._isGameActuallyRunning(game) && game.registrationOpen) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", notif.message]);
          }
        }, notif.delay));
        game._notificationTimers.push(timer);
      }
      const timer = this._trackTimer(setTimeout(() => {
        if (this._isGameActuallyRunning(game) && game.registrationOpen) {
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
          this._closeRegistration(room, game);
        }
      }, 20000));
      game._registrationTimer = timer;
    } catch(e) {}
  }

  // ============================================================
  // GAME: CLOSE REGISTRATION
  // ============================================================
  _closeRegistration(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.registrationOpen) return;
      this._cleanupGameTimers(game);
      game.registrationOpen = false;
      const humanPlayers = Array.from(game.players.keys()).filter(id => !id.startsWith('BOT_'));
      const humanCount = humanPlayers.length;
      if (!game._botsAdded) {
        if (humanCount === 1 || humanCount === 0) {
          this._addBots(room, 4);
          game._botsAdded = true;
        } else if (game.players.size < 2) {
          const needed = Math.min(4 - game.players.size, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { this._addBots(room, needed); game._botsAdded = true; }
        }
      }
      if (this._isGameActuallyRunning(game) && game.players.size >= 2) {
        this._startDrawPhase(room, game);
        
        setTimeout(() => {
          this._broadcastGameStateToRoom(room);
        }, 500);
        
      } else {
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
        this._scheduleGameCleanup(room, game);
      }
    } catch(e) {}
  }

  _addBots(room, count) {
    try {
      const game = this.activeGames.get(room);
      if (!this._isGameActuallyRunning(game)) return;
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

  // ============================================================
  // GAME: DRAW PHASE
  // ============================================================
  async _startDrawPhase(room, game) {
    const lockKey = `startDraw_${room}`;
    if (this._gameOperationLocks.has(lockKey)) return;
    if (!this._acquireLock(this._gameOperationLocks, lockKey, 10000)) return;
    try {
      if (!this._isGameActuallyRunning(game)) {
        this._releaseLock(this._gameOperationLocks, lockKey);
        return;
      }
      if (game._drawTimer) { this._clearTimer(game._drawTimer); game._drawTimer = null; }
      if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
      if (game._botTimeouts) {
        for (const id of game._botTimeouts) {
          this._clearTimer(id);
        }
        game._botTimeouts.clear();
      }
      
      const activePlayers = this._getActivePlayers(game);
      if (activePlayers.length < 2) {
        if (!game._botsAdded) {
          const needed = Math.min(4 - activePlayers.length, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { this._addBots(room, needed); game._botsAdded = true; }
        }
        const newActive = this._getActivePlayers(game);
        if (newActive.length < 2) {
          if (newActive.length === 1 && !game._gameEnded) {
            const winner = newActive[0]?.name || "Unknown";
            const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
            if (game._startedByRecording) {
              await this._addLowCardWinner(room, winner);
              await this._broadcastLowCardWinners(room);
            }
            game._gameEnded = true;
            game._isActive = false;
            this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
            this._releaseLock(this._gameOperationLocks, lockKey);
            this._forceCleanupGame(room, game);
            return;
          } else {
            game._gameEnded = true;
            game._isActive = false;
            this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
            this._releaseLock(this._gameOperationLocks, lockKey);
            this._forceCleanupGame(room, game);
            return;
          }
        }
      }
      
      game._phase = 'draw';
      game.drawTimeExpired = false;
      game.evaluationLocked = false;
      game._drawPhaseStart = Date.now();
      if (!game._botTimeouts) game._botTimeouts = new Set();
      const playersList = this._getActivePlayers(game).map(p => p.name);
      this._broadcastToRoom(room, ["gameLowCardClosed", playersList]);
      this._broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
      this._releaseLock(this._gameOperationLocks, lockKey);
      this._startDrawCountdown(room, game);
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        this._startBotDraws(room, game);
      }
    } catch(e) {
      this._releaseLock(this._gameOperationLocks, lockKey);
    }
  }

  _startDrawCountdown(room, game) {
    try {
      if (!this._isGameActuallyRunning(game)) return;
      if (game._drawTimer) {
        this._clearTimer(game._drawTimer);
        game._drawTimer = null;
      }
      if (game._drawNotificationTimers) {
        for (const timer of game._drawNotificationTimers) {
          this._clearTimer(timer);
        }
        game._drawNotificationTimers = [];
      } else {
        game._drawNotificationTimers = [];
      }
      const notifications = [
        { delay: 5000, message: "15s" },
        { delay: 10000, message: "10s" },
        { delay: 15000, message: "5s" }
      ];
      for (const notif of notifications) {
        const timer = this._trackTimer(setTimeout(() => {
          if (this._isGameActuallyRunning(game) && !game.drawTimeExpired) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", notif.message]);
          }
        }, notif.delay));
        game._drawNotificationTimers.push(timer);
      }
      const timer = this._trackTimer(setTimeout(() => {
        if (this._isGameActuallyRunning(game) && !game.drawTimeExpired) {
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
          this._closeDrawPhase(room, game);
        }
      }, 20000));
      game._drawTimer = timer;
    } catch(e) {}
  }

  _startBotDraws(room, game) {
    try {
      if (!this._isGameActuallyRunning(game) || !game.botPlayers) return;
      if (!game._botTimeouts) game._botTimeouts = new Set();
      if (game._botTimeouts.size >= CONSTANTS.MAX_BOT_TIMEOUTS) return;
      const notDrawn = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id))
        .slice(0, Math.min(CONSTANTS.MAX_BOT_DRAWS_PER_ROUND, CONSTANTS.MAX_BOT_TIMEOUTS - game._botTimeouts.size));
      for (const botId of notDrawn) {
        const delay = this._getRandomDrawDelay();
        const timeout = this._trackTimer(setTimeout(() => {
          const currentGame = this.activeGames.get(room);
          if (this._isGameActuallyRunning(currentGame) && !currentGame.drawTimeExpired &&
              !currentGame.evaluationLocked && !currentGame.numbers?.has(botId) && 
              !currentGame.eliminated?.has(botId)) {
            this._handleBotDraw(room, botId, currentGame);
          }
          currentGame?._botTimeouts?.delete(timeout);
        }, delay));
        game._botTimeouts.add(timeout);
      }
    } catch(e) {}
  }

  _handleBotDraw(room, botId, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.numbers?.has(botId) || game.drawTimeExpired || game.evaluationLocked) return;
      if (game.eliminated?.has(botId)) return;
      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      const botName = game.players.get(botId)?.name || botId;
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
      const activeIds = this._getActivePlayerIds(game);
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && this._isGameActuallyRunning(game)) {
        game.evaluationLocked = true;
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        const evalTimer = this._trackTimer(setTimeout(() => {
          try { this._evaluateRound(room, game); } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS));
        game._evalTimer = evalTimer;
      }
    } catch(e) {}
  }

  _forceBotDraw(room, botId, game) {
    try {
      if (!this._isGameActuallyRunning(game) || game.numbers?.has(botId)) return;
      if (game.eliminated?.has(botId)) return;
      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      const botName = game.players.get(botId)?.name || botId;
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);
    } catch(e) {}
  }

  // ============================================================
  // GAME: CLOSE DRAW PHASE
  // ============================================================
  async _closeDrawPhase(room, game) {
    const drawLockKey = `draw_${room}`;
    if (this._drawLocks.has(drawLockKey)) return;
    if (!this._acquireLock(this._drawLocks, drawLockKey, 10000)) return;
    try {
      if (!this._isGameActuallyRunning(game) || game.drawTimeExpired || game.evaluationLocked) {
        this._releaseLock(this._drawLocks, drawLockKey);
        return;
      }
      this._cleanupGameTimers(game);
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      if (game.botPlayers?.size > 0 && this._isGameActuallyRunning(game)) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) {
          this._forceBotDraw(room, botId, game);
        }
      }
      const activeIds = this._getActivePlayerIds(game);
      const submittedIds = new Set(game.numbers?.keys() || []);
      const notSubmitted = activeIds.filter(id => !submittedIds.has(id) && !game.eliminated?.has(id));
      if (notSubmitted.length > 0 && submittedIds.size === 0) {
        this._broadcastToRoom(room, ["gameLowCardError", "No one submitted numbers"]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._releaseLock(this._drawLocks, drawLockKey);
        this._forceCleanupGame(room, game);
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
          await this._addLowCardWinner(room, winnerName);
          await this._broadcastLowCardWinners(room);
        }
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._releaseLock(this._drawLocks, drawLockKey);
        this._forceCleanupGame(room, game);
        return;
      }
      if (remaining.length === 0) {
        this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
        game._gameEnded = true;
        game._isActive = false;
        game._endTime = Date.now();
        this._releaseLock(this._drawLocks, drawLockKey);
        this._forceCleanupGame(room, game);
        return;
      }
      this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      this._releaseLock(this._drawLocks, drawLockKey);
      const evalTimer = this._trackTimer(setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            this._evaluateRound(room, game);
          }
        } catch(e) {}
      }, CONSTANTS.EVALUATION_DELAY_MS));
      game._evalTimer = evalTimer;
    } catch(e) {
      this._releaseLock(this._drawLocks, drawLockKey);
    }
  }

  // ============================================================
  // GAME: EVALUATE ROUND
  // ============================================================
  async _evaluateRound(room, game) {
    const evalLockKey = `eval_${room}`;
    if (this._evaluationLocks.has(evalLockKey)) return;
    if (!this._acquireLock(this._evaluationLocks, evalLockKey, 15000)) return;
    try {
      if (this.isDestroyed || !game?._isActive || game._gameEnded || game._isEvaluating || !game.players) {
        this._releaseLock(this._evaluationLocks, evalLockKey);
        return;
      }
      const currentGame = this.activeGames.get(room);
      if (currentGame !== game) {
        this._releaseLock(this._evaluationLocks, evalLockKey);
        return;
      }
      if (game._isEvaluating) {
        this._releaseLock(this._evaluationLocks, evalLockKey);
        return;
      }
      this._cleanupGameTimers(game);
      game._isEvaluating = true;
      const safetyTimer = this._trackTimer(setTimeout(() => {
        if (game?._isEvaluating) { game._isEvaluating = false; this._scheduleGameCleanup(room, game); }
        this._releaseLock(this._evaluationLocks, evalLockKey);
      }, CONSTANTS.EVALUATION_TIMEOUT_MS));
      game._safetyTimer = safetyTimer;
      const numbers = game.numbers || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const tanda = game.tanda || new Map();
      const entries = Array.from(numbers.entries());
      const submittedIds = new Set(numbers.keys());
      const activeIds = this._getActivePlayerIds(game);
      for (const id of activeIds) {
        if (!submittedIds.has(id)) { eliminated.add(id); }
      }
      if (entries.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);
        const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
        if (remaining.length === 1) {
          const winnerId = remaining[0];
          const winnerName = players.get(winnerId)?.name || winnerId;
          const totalCoin = (game.betAmount || 0) * players.size;
          if (game._startedByRecording) {
            await this._addLowCardWinner(room, winnerName);
            await this._broadcastLowCardWinners(room);
          }
          this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
          game._gameEnded = true;
          game._isActive = false;
          game._isEvaluating = false;
          if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
          this._forceCleanupGame(room, game);
          return;
        }
        this._broadcastToRoom(room, ["gameLowCardError", "No numbers drawn this round"]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._forceCleanupGame(room, game);
        return;
      }
      const values = entries.map(([, n]) => n);
      const allSame = values.every(v => v === values[0]);
      let losers = [];
      if (!allSame && values.length > 0) {
        const lowest = Math.min(...values);
        losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
        for (const id of losers) { eliminated.add(id); }
      }
      const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
      if (allSame && remaining.length >= 2) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);
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
        this._broadcastToRoom(room, ["gameLowCardRoundResult", game.round - 1,
          entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`),
          [], remainingNames, true
        ]);
        if (this._isGameActuallyRunning(game) && !game._gameEnded) {
          this._startDrawPhase(room, game);
        }
        return;
      }
      if (remaining.length === 1 && !game._gameEnded) {
        const winnerId = remaining[0];
        const winnerName = players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * players.size;
        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winnerName);
          await this._broadcastLowCardWinners(room);
        }
        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);
        this._forceCleanupGame(room, game);
        return;
      }
      if (remaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
        this._releaseLock(this._evaluationLocks, evalLockKey);
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
        this._forceCleanupGame(room, game);
        return;
      }
      const numbersArr = entries.map(([id, n]) => `${players.get(id)?.name || id}:${n}${tanda.get(id) ? `(${tanda.get(id)})` : ''}`);
      const loserNames = [...losers].map(id => players.get(id)?.name || id);
      const remainingNames = remaining.map(id => players.get(id)?.name || id);
      this._broadcastToRoom(room, ["gameLowCardRoundResult", game.round, numbersArr, loserNames, remainingNames]);
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
      if (game._safetyTimer) { this._clearTimer(game._safetyTimer); game._safetyTimer = null; }
      this._releaseLock(this._evaluationLocks, evalLockKey);
      if (this._isGameActuallyRunning(game) && !game._gameEnded) {
        this._startDrawPhase(room, game);
      }
    } catch(e) {
      if (game) { game._isEvaluating = false; }
      this._releaseLock(this._evaluationLocks, `eval_${room}`);
    }
  }

  // ============================================================
  // GAME: JOIN
  // ============================================================
  async joinGame(ws, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username || typeof username !== 'string' || !username.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]);
        return;
      }
      const usernameClean = username.trim();
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      const lockKey = `join_${room}_${usernameClean}`;
      if (this._joinLocks.has(lockKey)) {
        this._safeSend(ws, ["gameLowCardError", "Please wait"]);
        return;
      }
      this._joinLocks.set(lockKey, Date.now());
      try {
        const game = this.activeGames.get(room);
        if (!game?._isActive || game._gameEnded || !game.players) {
          this._safeSend(ws, ["gameLowCardError", "No active game in this room"]);
          return;
        }
        if (game.players.has(usernameClean)) {
          if (game.eliminated?.has(usernameClean)) {
            this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
            return;
          }
          if (game.numbers.has(usernameClean)) {
            this._safeSend(ws, ["gameLowCardPlayerDraw", usernameClean, game.numbers.get(usernameClean), game.tanda.get(usernameClean) || ""]);
          }
          this._sendGameStateToClient(ws, room);
          return;
        }
        if (!game.registrationOpen) {
          this._safeSend(ws, ["gameLowCardNoJoin", usernameClean, game.betAmount]);
          this._safeSend(ws, ["gameLowCardError", "Registration is closed"]);
          return;
        }
        if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
          this._safeSend(ws, ["gameLowCardError", "Game is full"]);
          return;
        }
        game.players.set(usernameClean, { id: usernameClean, name: usernameClean });
        this._addClient(room, ws, usernameClean);
        game.playerWsId.set(usernameClean, wsId);
        this._broadcastToRoom(room, ["gameLowCardJoin", usernameClean, game.betAmount]);
        
        setTimeout(() => {
          this._broadcastGameStateToRoom(room);
        }, 100);
        
      } finally {
        setTimeout(() => { this._joinLocks.delete(lockKey); }, 2000);
      }
    } catch(e) {}
  }

  // ============================================================
  // GAME: SUBMIT NUMBER
  // ============================================================
  async submitNumber(ws, number, tanda, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username || typeof username !== 'string' || !username.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]);
        return;
      }
      const usernameClean = username.trim();
      const wsId = ws._wsId;
      const room = ws.room || ws.roomname || this.clientRooms.get(wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      if (game.players.has(usernameClean) && game.eliminated?.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You have been eliminated"]);
        return;
      }
      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw') {
        this._safeSend(ws, ["gameLowCardError", "Cannot submit now"]);
        return;
      }
      if (!game.players.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game"]);
        return;
      }
      if (game.numbers.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You have already submitted"]);
        return;
      }
      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
        return;
      }
      const validTandas = ["C1", "C2", "C3", "C4", ""];
      if (!validTandas.includes(tanda)) tanda = "";
      game.numbers.set(usernameClean, n);
      game.tanda.set(usernameClean, tanda);
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", usernameClean, n, tanda]);
      const activeIds = this._getActivePlayerIds(game);
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired &&
          this._isGameActuallyRunning(game) && game._isActive && !game._gameEnded) {
        game.evaluationLocked = true;
        if (game._evalTimer) { this._clearTimer(game._evalTimer); game._evalTimer = null; }
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        const evalTimer = this._trackTimer(setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
              this._evaluateRound(room, game);
            }
          } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS));
        game._evalTimer = evalTimer;
      }
    } catch(e) {}
  }

  // ============================================================
  // GAME: LEAVE
  // ============================================================
  async leaveGame(ws, username) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }
      if (!username || typeof username !== 'string' || !username.trim()) {
        this._safeSend(ws, ["gameLowCardError", "Username is required"]);
        return;
      }
      const usernameClean = username.trim();
      const room = ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameLowCardError", "Please switch to a room first"]);
        return;
      }
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded || !game.players) {
        this._safeSend(ws, ["gameLowCardError", "No active game in this room"]);
        return;
      }
      if (!game.players.has(usernameClean)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game"]);
        return;
      }
      this._removePlayerFromGame(usernameClean, room);
      
      setTimeout(() => {
        this._broadcastGameStateToRoom(room);
      }, 100);
      
    } catch(e) {}
  }

  _removePlayerFromGame(username, room) {
    try {
      const game = this.activeGames.get(room);
      if (!game || !game.players?.has(username) || !game._isActive || game._gameEnded || game._isEvaluating || game.evaluationLocked) return false;
      if (!game.eliminated) game.eliminated = new Set();
      game.eliminated.add(username);
      game.numbers?.delete(username);
      game.tanda?.delete(username);
      this._broadcastToRoom(room, ["gameLowCardError", `${username} has been eliminated`]);
      const checkTimer = setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && !game._gameEnded) this._checkGameCanContinue(room, game);
        } catch(e) {}
      }, 1000);
      this._trackTimer(checkTimer);
      return true;
    } catch(e) { return false; }
  }

  async _checkGameCanContinue(room, game) {
    try {
      if (!game?._isActive || game._gameEnded || !game.players || game._isEvaluating || game.evaluationLocked || game.registrationOpen) return;
      const activePlayers = this._getActivePlayers(game);
      if (activePlayers.length === 0) {
        const allPlayers = Array.from(game.players.keys());
        const submitted = Array.from(game.numbers?.keys() || []);
        const notSubmitted = allPlayers.filter(id => !submitted.includes(id) && !game.eliminated?.has(id));
        if (notSubmitted.length > 0) return;
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardEnd", []]);
        this._forceCleanupGame(room, game);
        return;
      }
      if (activePlayers.length === 1 && !game._gameEnded) {
        const activeIds = this._getActivePlayerIds(game);
        const submittedIds = Array.from(game.numbers?.keys() || []);
        const notSubmitted = activeIds.filter(id => !submittedIds.includes(id));
        if (notSubmitted.length > 0) {
          this._broadcastToRoom(room, ["gameLowCardTimeLeft", `Waiting for ${notSubmitted.length} player(s)`]);
          return;
        }
        const winner = activePlayers[0]?.name || "Unknown";
        const totalCoin = (game.betAmount || 0) * (game.players?.size || 0);
        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winner);
          await this._broadcastLowCardWinners(room);
        }
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
        this._forceCleanupGame(room, game);
      }
    } catch(e) {}
  }

  // ============================================================
  // GAME: CHECK GAME RUNNING
  // ============================================================
  async checkGameRunning(ws, roomname) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameStatus", "false"]);
        this._safeSend(ws, ["gameState", { 
          room: roomname || 'unknown', 
          hasGame: false, 
          gameType: 'lowcard',
          isActive: false 
        }]);
        return;
      }
      let room = roomname || ws.room || ws.roomname || this.clientRooms.get(ws._wsId);
      if (!room || typeof room !== 'string' || room.trim() === '') {
        this._safeSend(ws, ["gameStatus", "false"]);
        this._safeSend(ws, ["gameState", { 
          room: room || 'unknown', 
          hasGame: false, 
          gameType: 'lowcard',
          isActive: false 
        }]);
        return;
      }
      
      const game = this.activeGames.get(room);
      const isRunning = game?._isActive && !game._gameEnded && game.players?.size > 0;
      this._safeSend(ws, ["gameStatus", isRunning ? "true" : "false"]);
      
      if (isRunning) {
        this._sendGameStateToClient(ws, room);
      } else {
        this._safeSend(ws, ["gameState", { 
          room: room, 
          hasGame: false, 
          gameType: 'lowcard',
          isActive: false 
        }]);
      }
    } catch(e) {}
  }

  // ============================================================
  // GAME: START WITH RECORDING
  // ============================================================
  async _startGameWithRecording(ws, room, bet, username) {
    try {
      if (!room || !username || typeof room !== 'string' || typeof username !== 'string') {
        this._safeSend(ws, ["gameLowCardError", "Room and username required"]);
        return;
      }
      const isRecordingEnabled = await this.dataManager.getRecordingStatus(room);
      if (!isRecordingEnabled) {
        this._safeSend(ws, ["gameLowCardError", "Recording is not enabled in this room"]);
        return;
      }
      const existingGame = this.activeGames.get(room);
      if (existingGame?._isActive && !existingGame._gameEnded) {
        this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
        return;
      }
      if (existingGame) await this._forceCleanupGame(room, existingGame);
      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
        this._safeSend(ws, ["gameLowCardError", "Invalid bet (0 or 100-100000)"]);
        return;
      }
      if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
        this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
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
        _startedByRecording: true, _startedBy: 'recording',
        _notificationTimers: [],
        _drawNotificationTimers: []
      };
      game.players.set(username, { id: username, name: username });
      game.playerWsId.set(username, wsId);
      this.activeGames.set(room, game);
      this._addClient(room, ws, username);
      this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
      this._broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount]);
      this._startRegistration(room, game);
      
      setTimeout(() => {
        this._broadcastGameStateToRoom(room);
      }, 100);
      
    } catch(e) {
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }

  // ============================================================
  // DICE GAME
  // ============================================================
  _startDiceFast() {
    try {
      if (this._diceLock || this.currentDiceRoll || this._isShowingDice) return;
      this._diceLock = true;
      this._isShowingDice = true;
      const value = Math.floor(Math.random() * 6) + 1;
      this._diceRound = (this._diceRound || 0) + 1;
      this.currentDiceRoll = { value, timestamp: Date.now(), round: this._diceRound };
      this._diceStartTime = Date.now();
      this._diceQuestionStartTime = Date.now();
      this._canSubmitDiceAnswer = true;
      this.diceAnswered = new Set();
      this._playerAnswers = new Map();
      this.diceHasWinner = false;
      this.diceWinner = null;
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceRoll", { 
        value, timestamp: Date.now(), answerTime: 20, canAnswerNow: true, round: this._diceRound
      }]);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "♡ clik draw ♡"]);
      for (const timeout of this._diceNotificationTimeouts) { clearTimeout(timeout); }
      this._diceNotificationTimeouts = [];
      this._diceNotificationTimeouts.push(setTimeout(() => {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "15s remaining"]);
      }, 5000));
      this._diceNotificationTimeouts.push(setTimeout(() => {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "10s remaining"]);
      }, 10000));
      this._diceNotificationTimeouts.push(setTimeout(() => {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "5s remaining"]);
      }, 15000));
      this._diceNotificationTimeouts.push(setTimeout(() => {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "3s remaining"]);
      }, 17000));
      this._diceTimeout = this._trackTimer(setTimeout(() => { this._endDiceRound(); }, 20000));
    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  async _endDiceRound() {
    try {
      if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
      for (const timeout of this._diceNotificationTimeouts) { clearTimeout(timeout); }
      this._diceNotificationTimeouts = [];
      this._canSubmitDiceAnswer = false;
      this._isShowingDice = false;
      
      const diceValue = this.currentDiceRoll?.value;
      const roundNumber = this._diceRound || 1;
      const correctPlayers = [];
      
      for (const player of this.diceAnswered) {
        if (this._playerAnswers.get(player) === diceValue) {
          correctPlayers.push(player);
        }
      }
      
      if (correctPlayers.length === 0) {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNoWinner", {
          message: "No winner", value: diceValue, round: roundNumber
        }]);
      } else if (correctPlayers.length === 1) {
        const winner = correctPlayers[0];
        try {
          const points = await this.dataManager.addDicePoint(winner);
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
            username: winner, totalPoints: points[winner] || 0, diceValue: diceValue, round: roundNumber
          }]);
        } catch(e) {
          this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
            username: winner, totalPoints: 0, diceValue: diceValue, round: roundNumber
          }]);
        }
      } else if (correctPlayers.length > 1 && !this._tieActive) {
        this.currentDiceRoll = null;
        this._diceLock = false;
        this._isShowingDice = false;
        await this._startTieBreaker(CONSTANTS.DICE_ROOM, correctPlayers);
        return;
      }
      
      this.currentDiceRoll = null;
      this._diceLock = false;
      this._diceTimeUpCooldown = true;
      if (this._diceCooldownTimer) { clearTimeout(this._diceCooldownTimer); }
      this._diceCooldownTimer = setTimeout(() => {
        this._diceTimeUpCooldown = false;
        this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
        this._lastSentRemaining = -1;
        if (this.alarmScheduler.isDiceTime()) {
          const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
          if (clients && clients.size > 0) { this._startDiceFast(); }
        }
      }, 15000);
    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  // ============================================================
  // TIE BREAKER
  // ============================================================
  async _startTieBreaker(room, players) {
    if (this._tieLock) return;
    this._tieLock = true;
    try {
      if (!players || players.length < 2 || this._tieActive) return;
      this._tieActive = true;
      this._tieRound = 0;
      this._tiePlayers = [...players];
      this._tieAnswers = new Map();
      const id = `tie_${Date.now()}`;
      this._tieBreakers.set(id, { players, round: 0, winner: null, status: 'waiting' });
      await this._runTieRound(room, id, players);
    } finally {
      setTimeout(() => { this._tieLock = false; }, 2000);
    }
  }

  async _runTieRound(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);
    for (const timeout of this._tieNotificationTimeouts) { clearTimeout(timeout); }
    this._tieNotificationTimeouts = [];
    this._tieRound++;
    this._tiePlayers = [...players];
    this._tieAnswers = new Map();
    data.round = this._tieRound;
    data.status = 'running';
    data.players = players;
    this._diceQuestionStartTime = Date.now();
    this._canSubmitDiceAnswer = true;
    this.diceAnswered = new Set();
    this._playerAnswers = new Map();
    this._isShowingDice = true;
    this.diceHasWinner = false;
    this.diceWinner = null;
    this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", `♡ Tie Round ${this._tieRound}: ${players.join(', ')}`]);
    const timeLimit = CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20;
    let isProcessed = false;
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "10s remaining"]);
    }, (timeLimit - 10) * 1000));
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "5s remaining"]);
    }, (timeLimit - 5) * 1000));
    this._tieNotificationTimeouts.push(setTimeout(() => {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "3s remaining"]);
    }, (timeLimit - 3) * 1000));
    this._tieTimer = this._trackTimer(setTimeout(() => {
      if (!isProcessed) {
        isProcessed = true;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "TIME UP"]);
        for (const timeout of this._tieNotificationTimeouts) { clearTimeout(timeout); }
        this._tieNotificationTimeouts = [];
        const tieId = this._getActiveTieBreakerId();
        if (tieId) this._processTieResults(room, tieId, players);
        else { this._resetTieBreakerState(null); this._startCooldownAfterTieBreaker(); }
      }
    }, (timeLimit * 1000) + 2000));
  }

  async _processTieResults(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;
    let highest = 0, highestPlayers = [];
    for (const player of players) {
      const answer = this._tieAnswers.get(player);
      if (answer !== undefined && answer >= 1 && answer <= 6) {
        if (answer > highest) { highest = answer; highestPlayers = [player]; }
        else if (answer === highest) { highestPlayers.push(player); }
      }
    }
    if (highestPlayers.length === 0) {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "No one answered tie breaker"]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    if (highestPlayers.length === 1) {
      const winner = highestPlayers[0];
      try {
        const points = await this.dataManager.addDicePoint(winner);
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
          username: winner, totalPoints: points[winner] || 0, diceValue: highest,
          round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
        }]);
      } catch(e) {
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
          username: winner, totalPoints: 0, diceValue: highest,
          round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
        }]);
      }
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }
    if (highestPlayers.length > 1) {
      this._tiePlayers = highestPlayers;
      this._tieAnswers = new Map();
      data.players = highestPlayers;
      data.round = this._tieRound;
      data.status = 'waiting';
      const nextTimer = setTimeout(() => {
        if (this._tieActive && this._tiePlayers.length > 1) {
          this._runTieRound(room, id, this._tiePlayers);
        } else if (this._tiePlayers.length === 1) {
          this._processSingleWinner(room, id, this._tiePlayers[0]);
        }
      }, 2000);
      this._trackTimer(nextTimer);
      return;
    }
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  async _processSingleWinner(room, id, winner) {
    try {
      const points = await this.dataManager.addDicePoint(winner);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
        username: winner, totalPoints: points[winner] || 0, diceValue: 'auto',
        round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
      }]);
    } catch(e) {
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceWinner", {
        username: winner, totalPoints: 0, diceValue: 'auto',
        round: this._diceRound || 1, isTieBreaker: true, tieBreakerRound: this._tieRound, finalWinner: true
      }]);
    }
    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  _startCooldownAfterTieBreaker() {
    this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceNotification", "wait 15s"]);
    this._diceTimeUpCooldown = true;
    this._clearTimer(this._diceTimeUpCooldownTimer);
    this._diceTimeUpCooldownTimer = this._trackTimer(setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      this._diceNotifiedFlags = { 20: false, 10: false, 5: false, timeup: false };
      this._lastSentRemaining = -1;
      this._lastNotificationKey = "";
      this._lastNotificationTime = 0;
      if (this.alarmScheduler.isDiceTime()) {
        const clients = this.wsClients?.get(CONSTANTS.DICE_ROOM);
        if (clients && clients.size > 0) { this._startDiceFast(); }
      }
    }, CONSTANTS.TIE_BREAKER_COOLDOWN || 15000));
  }

  _resetTieBreakerState(id) {
    if (id) this._tieBreakers.delete(id);
    this._tieActive = false;
    this._tiePlayers = [];
    this._tieAnswers = new Map();
    this._tieRound = 0;
    this._canSubmitDiceAnswer = false;
    this._isShowingDice = false;
    this.currentDiceRoll = null;
    this.diceAnswered = new Set();
    this._playerAnswers = new Map();
    this.diceHasWinner = false;
    this.diceWinner = null;
    if (this._tieTimer) { this._clearTimer(this._tieTimer); this._tieTimer = null; }
    if (this._tieInterval) { this._clearTimer(this._tieInterval); this._tieInterval = null; }
    for (const timeout of this._tieNotificationTimeouts) { clearTimeout(timeout); }
    this._tieNotificationTimeouts = [];
  }

  _getActiveTieBreakerId() {
    for (const [id, data] of this._tieBreakers) {
      if (data.status === 'waiting' || data.status === 'running') return id;
    }
    return null;
  }

  // ============================================================
  // DICE: SUBMIT ANSWER
  // ============================================================
  async submitDiceAnswer(ws, username, guess) {
    try {
      if (!ws || !username) return;
      if (!this._canSubmitDiceAnswer) return;
      if (this.diceAnswered.has(username)) return;
      const guessValue = parseInt(guess, 10);
      if (isNaN(guessValue) || guessValue < 1 || guessValue > 6) {
        this._safeSend(ws, ["diceError", "invalid guess 1-6"]);
        return;
      }
      if (this._tieActive) {
        if (!this._tiePlayers.includes(username)) {
          this._safeSend(ws, ["diceError", "You are not in tie breaker"]);
          return;
        }
        if (this._tieAnswers.has(username)) {
          this._safeSend(ws, ["diceError", "You already answered"]);
          return;
        }
        this._tieAnswers.set(username, guessValue);
        this.diceAnswered.add(username);
        this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceAnswer", {
          username, guess: guessValue, isTieBreaker: true, tieRound: this._tieRound
        }]);
        if (this._tieAnswers.size === this._tiePlayers.length) {
          this._canSubmitDiceAnswer = false;
          this._isShowingDice = false;
          if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
          if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
          const tieId = this._getActiveTieBreakerId();
          if (tieId) {
            setTimeout(async () => {
              await this._processTieResults(CONSTANTS.DICE_ROOM, tieId, this._tiePlayers);
            }, 500);
          } else {
            this._resetTieBreakerState(null);
            this._startCooldownAfterTieBreaker();
          }
        }
        return;
      }
      if (!this.currentDiceRoll) return;
      const diceValue = this.currentDiceRoll.value;
      this._playerAnswers.set(username, guessValue);
      this.diceAnswered.add(username);
      this._broadcastToRoom(CONSTANTS.DICE_ROOM, ["diceAnswer", {
        username, guess: guessValue, round: this._diceRound || 1
      }]);
      if (guessValue === diceValue && !this.diceHasWinner) {
        this.diceHasWinner = true;
        this.diceWinner = username;
      }
    } catch(e) {}
  }

  // ============================================================
  // DICE: NOTIFICATION HELPERS
  // ============================================================
  _sendDiceNotificationOnSwitch(ws, wsId) {
    try {
      if (!ws || ws.readyState !== 1) return;
      
      const isGameActive = this.currentDiceRoll && this._canSubmitDiceAnswer;
      if (isGameActive) {
        const elapsed = (Date.now() - this._diceStartTime) / 1000;
        const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
        const remaining = Math.max(0, totalTime - elapsed);
        const remainingInt = Math.floor(remaining);
        if (remainingInt > 0) {
          this._safeSend(ws, ["diceNotification", `${remainingInt}s remaining`]);
        }
        return;
      }
      
      const isDiceTime = this.alarmScheduler.isDiceTime();
      const timeLeft = this._getTimeLeftUntilNextDice();
      
      if (!isDiceTime && !isGameActive && timeLeft.totalMs > 0) {
        const notificationKey = `nextgame_${wsId}`;
        
        for (const timer of this._notificationTimers) {
          if (timer._key === notificationKey) {
            clearTimeout(timer);
            this._notificationTimers.delete(timer);
          }
        }
        
        const timer = setTimeout(() => {
          if (ws && ws.readyState === 1) {
            const currentTimeLeft = this._getTimeLeftUntilNextDice();
            if (currentTimeLeft.totalMs > 0 && !this.alarmScheduler.isDiceTime()) {
              this._safeSend(ws, ["diceNotification", `Next dice game in: ${currentTimeLeft.text}`]);
            }
          }
          this._notificationTimers.delete(timer);
        }, 5000);
        
        timer._key = notificationKey;
        this._notificationTimers.add(timer);
        this._trackTimer(timer);
        
      } else if (isDiceTime && !this.currentDiceRoll && !this._isShowingDice && !this._diceLock && !this._diceTimeUpCooldown) {
        this._safeSend(ws, ["diceNotification", "Dice game starting soon..."]);
        if (this.alarmScheduler.isDiceTime()) {
          this._startDiceFast();
        }
      }
    } catch(e) {}
  }

  _checkAndStartDiceIfNeeded(ws) {
    try {
      if (!this.alarmScheduler.isDiceTime()) {
        return;
      }

      if (this.currentDiceRoll && this._canSubmitDiceAnswer) {
        const elapsed = (Date.now() - this._diceStartTime) / 1000;
        const totalTime = CONSTANTS.DICE_TOTAL_TIME_MS / 1000;
        const remaining = Math.max(0, totalTime - elapsed);
        const remainingInt = Math.floor(remaining);
        if (remainingInt > 0) {
          this._safeSend(ws, ["diceNotification", `${remainingInt}s remaining`]);
        }
        return;
      }
      if (this._isShowingDice || this._diceLock || this._diceTimeUpCooldown) {
        if (this._diceTimeUpCooldown) {
          this._safeSend(ws, ["diceNotification", "Game in cooldown, please wait..."]);
        }
        return;
      }
      if (this.alarmScheduler.isDiceTime()) { this._startDiceFast(); }
    } catch(e) {}
  }

  // ============================================================
  // BROADCAST HELPERS
  // ============================================================
  async _broadcastLowCardWinners(room) {
    try {
      if (!room || typeof room !== 'string' || room.trim() === '') return;
      const roomKey = room.trim();
      const isRecording = await this.dataManager.getRecordingStatus(roomKey);
      if (!isRecording) return;
      const winners = await this.dataManager.getWinners(roomKey);
      const now = Date.now();
      const key = `broadcast_${roomKey}`;
      if (!this._lastNotifTime) this._lastNotifTime = {};
      if (this._lastNotifTime[key] && (now - this._lastNotifTime[key]) < 500) return;
      this._lastNotifTime[key] = now;
      this._broadcastToRoom(roomKey, ["lowCardWinnerUpdate", {
        winners: winners || {}, 
        room: roomKey, 
        recording: true
      }]);
    } catch(e) {}
  }

  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username || room === CONSTANTS.DICE_ROOM) return false;
      const isRecording = await this.dataManager.getRecordingStatus(room);
      if (!isRecording) return false;
      return await this.dataManager.addWinner(room, username);
    } catch(e) { 
      return false; 
    }
  }

  // ============================================================
  // BROADCAST GAME STATE TO ROOM
  // ============================================================
  _broadcastGameStateToRoom(room) {
    try {
      if (!room) return;
      const wsIds = this.wsClients.get(room);
      if (!wsIds?.size) return;
      
      for (const wsId of wsIds) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1) {
          this._sendGameStateToClient(ws, room);
        }
      }
    } catch(e) {}
  }

  // ============================================================
  // SEND GAME STATE TO CLIENT
  // ============================================================
  _sendGameStateToClient(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room) return;
      
      const game = this.activeGames.get(room);
      
      if (!game || !game._isActive || game._gameEnded) {
        this._safeSend(ws, ["gameState", { 
          room, 
          hasGame: false, 
          gameType: 'lowcard',
          isActive: false 
        }]);
        return;
      }
      
      const activePlayers = this._getActivePlayers(game);
      const allPlayers = Array.from(game.players?.values() || []).map(p => p.name);
      const eliminated = Array.from(game.eliminated || []);
      const submitted = Array.from(game.numbers?.keys() || []);
      
      this._safeSend(ws, ["gameState", {
        room, 
        hasGame: true, 
        gameType: 'lowcard',
        isActive: game._isActive || false,
        phase: game._phase || 'registration',
        round: game.round || 1, 
        bet: game.betAmount || 0,
        host: game.hostName || 'Unknown',
        registrationOpen: game.registrationOpen || false,
        players: allPlayers, 
        activePlayers: activePlayers.map(p => p.name),
        eliminated, 
        submitted, 
        playerCount: game.players?.size || 0,
        activeCount: activePlayers.length,
        isEvaluating: game._isEvaluating || false,
        evaluationLocked: game.evaluationLocked || false,
        drawTimeExpired: game.drawTimeExpired || false,
        gameStatus: game._gameEnded ? 'ended' : (game._isActive ? 'active' : 'inactive'),
        totalPlayers: game.players?.size || 0
      }]);
    } catch(e) {
      try {
        this._safeSend(ws, ["gameState", { 
          room: room || 'unknown', 
          hasGame: false, 
          gameType: 'lowcard',
          isActive: false,
          error: true
        }]);
      } catch(err) {}
    }
  }

  _trackTimer(timer) {
    if (timer) this._allTimers.add(timer);
    return timer;
  }

  _clearTimer(timer) {
    if (timer) {
      if (typeof timer === 'object' && timer._destroyed) return;
      try { clearTimeout(timer); } catch(e) {}
      try { clearInterval(timer); } catch(e) {}
      this._allTimers.delete(timer);
    }
  }

  _safeSend(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  _broadcastToRoom(room, message) {
    try {
      if (this.closing || this.isDestroyed || !room || !message) return;
      const wsIds = this.wsClients.get(room);
      if (!wsIds?.size) return;
      
      const isNotification = message[0] === 'diceNotification' || 
                             message[0] === 'gameLowCardTimeLeft' ||
                             message[0] === 'gameLowCardWait';
      
      if (isNotification) {
        const now = Date.now();
        const msgKey = `${room}_${message[0]}`;
        if (!this._lastNotifTime) this._lastNotifTime = {};
        if (this._lastNotifTime[msgKey] && (now - this._lastNotifTime[msgKey]) < 2000) return;
        this._lastNotifTime[msgKey] = now;
      }
      
      const msgStr = JSON.stringify(message);
      const wsIdArray = Array.from(wsIds);
      for (let i = 0; i < wsIdArray.length; i += 20) {
        const batch = wsIdArray.slice(i, i + 20);
        for (const wsId of batch) {
          const ws = this.wsMap.get(wsId);
          if (ws && ws.readyState === 1 && !ws._closing) {
            try { ws.send(msgStr); } catch(e) {}
          }
        }
      }
    } catch(e) {}
  }

  _handleError(type, error) {
    try {
      const now = Date.now();
      if (now - this._lastErrorReset > CONSTANTS.ERROR_RESET_INTERVAL_MS) {
        this._errorCount = 0;
        this._lastErrorReset = now;
      }
      this._errorCount++;
      if (this._errorCount > 20) {
        this._circuitOpen = true;
        this._lastResetTime = now;
      }
    } catch(e) {}
  }

  _getTimeLeftUntilNextDice() {
    try {
      const witaTime = this._getCurrentWITATime();
      const currentTotal = witaTime.totalMinutes;
      let minDiff = Infinity;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = parseTime(session.start);
        let diff = startTotal - currentTotal;
        if (diff < 0) diff += 24 * 60;
        if (diff < minDiff) minDiff = diff;
      }
      const hours = Math.floor(minDiff / 60);
      const minutes = Math.floor(minDiff % 60);
      const isRunning = this.alarmScheduler.isDiceTime();
      return { 
        hours, minutes, totalMs: minDiff * 60 * 1000,
        text: `${hours}h ${minutes}m`, isRunning 
      };
    } catch(e) {
      return { hours: 0, minutes: 0, totalMs: 0, text: '0h 0m', isRunning: false };
    }
  }

  _getCurrentWITATime() {
    try {
      const now = new Date();
      const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
      const minutes = now.getUTCMinutes();
      return { hours, minutes, totalMinutes: (hours * 60) + minutes };
    } catch(e) { 
      return { hours: 0, minutes: 0, totalMinutes: 0 }; 
    }
  }

  _isGameActuallyRunning(game) { 
    return game?._isActive === true && !game?._gameEnded; 
  }

  _getActivePlayers(game) {
    try {
      if (!game?._isActive || game?._gameEnded || !game?.players) return [];
      return Array.from(game.players.entries())
        .filter(([id]) => !game.eliminated?.has(id))
        .map(([, p]) => p);
    } catch(e) { return []; }
  }

  _getActivePlayerIds(game) {
    try {
      if (!game?._isActive || game._gameEnded || !game?.players) return [];
      return Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
    } catch(e) { return []; }
  }

  _getRandomCardTanda() { 
    return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)]; 
  }

  _getRandomDrawDelay() { 
    return (Math.floor(Math.random() * 14) + 2) * 1000; 
  }

  _getBotNumberByRound(round) {
    if (round <= 2) return Math.floor(Math.random() * 12) + 1;
    return Math.random() < 0.6 ?
      [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
      [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
  }

  // ============================================================
  // CLIENT MANAGEMENT
  // ============================================================
  _addClient(room, ws, username = null) {
    try {
      if (!ws) return;
      const wsId = this._getWsId(ws);
      if (!wsId) { this._safeSend(ws, ["gameLowCardError", "Connection error"]); return; }
      if (this.clientRooms.has(wsId)) {
        const oldRoom = this.clientRooms.get(wsId);
        if (oldRoom && oldRoom !== room) this._removeClientFromRoom(oldRoom, wsId);
      }
      ws.serializeAttachment({
        wsId: wsId,
        username: username,
        room: room,
        roomname: room,
        createdAt: ws._createdAt || Date.now()
      });
      if (username) {
        let conn = this.userConnections.get(username);
        if (conn) { conn.room = room; conn.timestamp = Date.now(); conn.ws = ws; conn.wsId = wsId; }
        else { this.userConnections.set(username, { wsId, ws, room, timestamp: Date.now() }); }
        this._reconnectAttempts.delete(username);
      }
      let clients = this.wsClients.get(room);
      if (!clients) { clients = new Set(); this.wsClients.set(room, clients); }
      clients.add(wsId);
      this.clientRooms.set(wsId, room);
      this.wsMap.set(wsId, ws);
      ws.room = room;
      ws.roomname = room;
      if (username) ws.username = username;
    } catch(e) {}
  }

  _removeClientFromRoom(room, wsId) {
    try {
      if (!room || !wsId) return;
      const clients = this.wsClients.get(room);
      if (clients) { clients.delete(wsId); if (clients.size === 0) this.wsClients.delete(room); }
    } catch(e) {}
  }

  _getWsId(ws) { return ws?._wsId || null; }

  // ============================================================
  // GAME: CLEANUP
  // ============================================================
  _scheduleGameCleanup(room, game) {
    try {
      if (!room || !game) return;
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      if (!game._gameEnded) return;
      const timer = this._trackTimer(setTimeout(() => {
        const currentGame = this.activeGames.get(room);
        if (currentGame?._isActive && !currentGame._gameEnded) {
          this._cleanupTimers.delete(room);
          return;
        }
        this._cleanupTimers.delete(room);
        const gameToDelete = this.activeGames.get(room);
        if (gameToDelete) this._forceCleanupGame(room, gameToDelete);
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS));
      this._cleanupTimers.set(room, timer);
    } catch(e) {}
  }

  _cleanupGameTimers(game) {
    if (!game) return;
    const timerKeys = ['_registrationTimer', '_drawTimer', '_evalTimer', '_safetyTimer'];
    for (const key of timerKeys) {
      if (game[key]) {
        this._clearTimer(game[key]);
        game[key] = null;
      }
    }
    if (game._notificationTimers) {
      for (const timer of game._notificationTimers) {
        this._clearTimer(timer);
      }
      game._notificationTimers = [];
    }
    if (game._drawNotificationTimers) {
      for (const timer of game._drawNotificationTimers) {
        this._clearTimer(timer);
      }
      game._drawNotificationTimers = [];
    }
    if (game._botTimeouts) {
      for (const timeout of game._botTimeouts) {
        this._clearTimer(timeout);
      }
      game._botTimeouts.clear();
    }
    game._isEvaluating = false;
    game.evaluationLocked = false;
    game.drawTimeExpired = false;
    game.registrationOpen = false;
  }

  async _forceCleanupGame(room, game) {
    const lockKey = `cleanup_${room}`;
    if (this._cleanupLocks.has(lockKey)) return;
    if (!this._acquireLock(this._cleanupLocks, lockKey, 10000)) return;
    try {
      if (!game) { this._releaseLock(this._cleanupLocks, lockKey); return; }
      this._cleanupGameTimers(game);
      game.players = null;
      game.botPlayers = null;
      game.numbers = null;
      game.tanda = null;
      game.eliminated = null;
      game.playerWsId = null;
      game._isActive = false;
      game._gameEnded = true;
      game._isEvaluating = false;
      game.registrationOpen = false;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      game._phase = null;
      game.round = 0;
      game.betAmount = 0;
      game.hostId = null;
      game.hostName = null;
      game.useBots = false;
      game._botsAdded = false;
      game._createdAt = null;
      game._drawPhaseStart = null;
      game._endTime = Date.now();
      game._startedByRecording = false;
      game._startedBy = null;
      this.activeGames.delete(room);
      this._gameLocks.delete(room);
      this._joinLocks.delete(room);
      this._evaluationLocks.delete(`eval_${room}`);
      this._drawLocks.delete(`draw_${room}`);
      this._gameOperationLocks.delete(`startDraw_${room}`);
      if (this._cleanupTimers.has(room)) {
        this._clearTimer(this._cleanupTimers.get(room));
        this._cleanupTimers.delete(room);
      }
      
      this._broadcastToRoom(room, ["gameLowCardEnd", []]);
      this._broadcastGameStateToRoom(room);
      
      this._releaseLock(this._cleanupLocks, lockKey);
    } catch(e) {
      this._releaseLock(this._cleanupLocks, lockKey);
    }
  }

  // ============================================================
  // LOCK HELPERS
  // ============================================================
  _acquireLock(lockMap, key, timeoutMs = 5000) {
    if (lockMap.has(key)) return false;
    lockMap.set(key, Date.now());
    setTimeout(() => {
      if (lockMap.has(key)) lockMap.delete(key);
    }, timeoutMs);
    return true;
  }

  _releaseLock(lockMap, key) {
    if (lockMap.has(key)) { lockMap.delete(key); return true; }
    return false;
  }

  // ============================================================
  // DESTROY
  // ============================================================
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;
      
      if (this._notificationTimers) {
        for (const timer of this._notificationTimers) {
          clearTimeout(timer);
        }
        this._notificationTimers.clear();
      }
      
      for (const timer of this._allTimers) {
        try { clearTimeout(timer); } catch(e) {}
      }
      this._allTimers.clear();
      
      if (this._diceTimeout) { clearTimeout(this._diceTimeout); this._diceTimeout = null; }
      if (this._diceCooldownTimer) { clearTimeout(this._diceCooldownTimer); this._diceCooldownTimer = null; }
      if (this._diceTimeUpCooldownTimer) { clearTimeout(this._diceTimeUpCooldownTimer); this._diceTimeUpCooldownTimer = null; }
      for (const timeout of this._diceNotificationTimeouts) { clearTimeout(timeout); }
      this._diceNotificationTimeouts = [];
      
      if (this._tieTimer) { clearTimeout(this._tieTimer); this._tieTimer = null; }
      if (this._tieInterval) { clearInterval(this._tieInterval); this._tieInterval = null; }
      for (const timeout of this._tieNotificationTimeouts) { clearTimeout(timeout); }
      this._tieNotificationTimeouts = [];
      
      for (const [room, game] of this.activeGames) {
        this._cleanupGameTimers(game);
        await this._forceCleanupGame(room, game);
      }
      this.activeGames.clear();
      
      for (const [room, timer] of this._cleanupTimers) {
        this._clearTimer(timer);
      }
      this._cleanupTimers.clear();
      
      this._eventQueue = [];
      this._isProcessingQueue = false;
      this.userConnections.clear();
      this._tieBreakers.clear();
      this._reconnectAttempts.clear();
      this._gameLocks.clear();
      this._joinLocks.clear();
      this._switchLocks.clear();
      this._switchRetries.clear();
      
      if (this.alarmScheduler) { await this.alarmScheduler.cleanup(); }
      
      for (const [wsId, ws] of this.wsMap) {
        try { 
          if (ws && ws.readyState === 1) {
            ws.removeAllListeners();
            ws.close(1000, "Server shutting down"); 
          }
        } catch(e) {}
      }
      this.wsMap.clear();
      this.wsClients.clear();
      this.clientRooms.clear();
      
      try { await this.ctx.storage.deleteAlarm(); } catch(e) {}
    } catch(e) {}
  }
}

export default GameServer;
