// ==================== GAME-SERVER.JS ====================
// VERSION: 6.0.0 - FULL CHAT + GAME (MEMORY ONLY)
// 🔥 SEMUA USER DI 1 INSTANCE → REALTIME!

const CONSTANTS = {
  MAX_ROOMS: 100,
  MAX_PLAYERS_PER_ROOM: 50,
  MAX_LOWCARD_GAMES: 10,
  REGISTRATION_TIME_MS: 20000,
  DRAW_TIME_MS: 20000,
  EVALUATION_DELAY_MS: 2000,
  MAX_BOTS_PER_GAME: 4,
  MAX_BET: 100000,
  MAX_WS_CLIENTS: 200,
  DICE_ROOM: "Quiz",
  TIE_BREAKER_TIME_LIMIT: 20,
  TIE_BREAKER_COOLDOWN: 15000,
  GAME_CLEANUP_DELAY_MS: 5000,
  EVALUATION_TIMEOUT_MS: 30000,
  MAX_BOT_DRAWS_PER_ROUND: 4,
  MAX_BOT_TIMEOUTS: 5,
  MAX_PLAYERS_PER_GAME: 45,
  STUCK_DRAW_TIMEOUT_MS: 60000,
  STUCK_REGISTRATION_TIMEOUT_MS: 30000,
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

// ============================================================
// ✅ GAME SERVER CLASS
// ============================================================
export class GameServer {
  constructor(env) {
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    this._wsIdCounter = 0;

    // ========== 🔥 SEMUA DATA DI MEMORY ==========
    // Semua user di 1 instance → SALING TERLIHAT!

    // ========== WEBSOCKET ==========
    this.wsMap = new Map();           // wsId → WebSocket
    this.wsClients = new Map();       // room → Set(wsId)
    this.clientRooms = new Map();     // wsId → room
    this.userConnections = new Map(); // username → {wsId, ws, room}

    // ========== CHAT ==========
    this.roomUsers = new Map();       // room → Map(username → userData)
    this.roomSeats = new Map();       // room → Map(seat → username)
    this.roomPoints = new Map();      // room → Map(seat → {x, y, fast})
    this.roomMutes = new Map();       // room → boolean

    // ========== GAME ==========
    this.activeGames = new Map();     // room → game data

    // ========== DICE ==========
    this.currentDiceRoll = null;
    this._tieActive = false;
    this.diceAnswered = new Set();
    this._playerAnswers = new Map();
    this._isShowingDice = false;
    this._diceTimeUpCooldown = false;
    this._canSubmitDiceAnswer = false;
    this._diceRound = 0;
    this.dicePoints = new Map();      // 🔥 DICE POINTS IN MEMORY!
    this.lastWeekWinner = null;
    this.lastResetWeek = null;
    this._diceLock = false;
    this._diceTimerInterval = null;
    this._diceTimeout = null;
    this._diceTimeUpCooldownTimer = null;
    this.diceAutoEnabled = true;

    // ========== TIE BREAKER ==========
    this._tieBreakers = new Map();
    this._tieRound = 0;
    this._tiePlayers = [];
    this._tieAnswers = new Map();
    this._tieLock = false;
    this._tieTimer = null;
    this._tieInterval = null;

    // ========== RECORDING ==========
    this.recordingEnabled = new Map();   // room → boolean
    this.lowcardWinners = new Map();     // room → {username: wins}

    // ========== LOCKS ==========
    this._gameLocks = new Map();
    this._joinLocks = new Map();
    this._cleanupTimers = new Map();

    // ========== TIMERS ==========
    this._allTimers = new Set();
    this._mainInterval = null;
    this._cleanupInterval = null;

    // ========== START ==========
    this._startMainInterval();
    this._checkWeekReset();

    console.log('🎮 GameServer started - SINGLE INSTANCE (Memory Only)');
    console.log(`📊 All users share 1 instance → REALTIME!`);
  }

  // ============================================================
  // ✅ MAIN INTERVAL
  // ============================================================
  _startMainInterval() {
    this._mainInterval = setInterval(() => {
      if (this.closing || this.isDestroyed) {
        clearInterval(this._mainInterval);
        return;
      }
      this._doTick();
    }, 5000);
    this._allTimers.add(this._mainInterval);

    this._cleanupInterval = setInterval(() => {
      this._performCleanup();
    }, 30000);
    this._allTimers.add(this._cleanupInterval);
  }

  _doTick() {
    try {
      this._cleanupDeadConnections();
      this._checkDice();
      this._checkStuckGames();
      this._cleanupMemory();
    } catch(e) {}
  }

  // ============================================================
  // ✅ CLEANUP
  // ============================================================
  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      for (const [wsId, ws] of this.wsMap) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(wsId);
        }
      }
      for (const wsId of toRemove) {
        const ws = this.wsMap.get(wsId);
        if (ws) {
          const room = this.clientRooms.get(wsId);
          if (room) {
            const clients = this.wsClients.get(room);
            if (clients) {
              clients.delete(wsId);
              if (clients.size === 0) this.wsClients.delete(room);
            }
          }
          this.clientRooms.delete(wsId);
          this.wsMap.delete(wsId);
        }
      }
    } catch(e) {}
  }

  _cleanupMemory() {
    try {
      for (const [room, clients] of this.wsClients) {
        if (clients.size === 0) this.wsClients.delete(room);
      }

      const now = Date.now();
      for (const [key, time] of this._gameLocks) {
        if (now - time > 30000) this._gameLocks.delete(key);
      }
      for (const [key, time] of this._joinLocks) {
        if (now - time > 30000) this._joinLocks.delete(key);
      }
    } catch(e) {}
  }

  _performCleanup() {
    try {
      this._cleanupDeadConnections();
      this._cleanupMemory();
    } catch(e) {}
  }

  // ============================================================
  // ✅ HANDLE CHAT WEBSOCKET (Untuk Java App Inventor)
  // ============================================================
  async handleChatWebSocket(request) {
    try {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("WebSocket only", { status: 400 });
      }

      if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
        return new Response("Server full", { 
          status: 503,
          headers: { 'Retry-After': '10' }
        });
      }

      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      const wsId = ++this._wsIdCounter;

      server._wsId = wsId;
      server._closing = false;
      server.room = null;
      server.username = null;
      server.idTarget = null;
      server.seatNumber = -1;
      server.noimageUrl = '';
      server.color = '#FFFFFF';
      server.itembawah = 0;
      server.itematas = 0;
      server.vip = 0;
      server.viptanda = 0;

      try {
        server.accept();
      } catch(e) {
        return new Response("WebSocket failed", { status: 500 });
      }

      this.wsMap.set(wsId, server);

      server.addEventListener("message", async (event) => {
        try {
          if (server._closing || this.closing) return;
          const data = JSON.parse(event.data);
          if (Array.isArray(data) && data.length > 0) {
            await this._handleChatMessage(server, data);
          }
        } catch(e) {
          console.error('Chat message error:', e);
        }
      });

      server.addEventListener("close", () => {
        this._handleChatClose(server);
      }, { once: true });

      server.addEventListener("error", () => {
        this._handleChatClose(server);
      }, { once: true });

      return new Response(null, { status: 101, webSocket: client });

    } catch(e) {
      console.error('Chat WS error:', e);
      return new Response("Error", { status: 500 });
    }
  }

  // ============================================================
  // ✅ CHAT MESSAGE HANDLER
  // ============================================================
  async _handleChatMessage(ws, data) {
    try {
      const evt = data[0];

      switch(evt) {
        // ========== ROOM MANAGEMENT ==========
        case "joinRoom":
        case "setIdTarget": {
          const idTarget = data[1] || "";
          const room = data[2] || "default";
          ws.idTarget = idTarget;
          ws.username = idTarget;
          await this._chatJoinRoom(ws, room);
          break;
        }

        case "setIdTarget2": {
          ws.idTarget = data[1] || "";
          ws.username = ws.idTarget;
          break;
        }

        case "isInRoom": {
          const room = ws.room || "default";
          const isInRoom = this.wsClients.has(room) && this.wsClients.get(room).has(ws._wsId);
          this._safeSend(ws, ["inRoomStatus", isInRoom]);
          break;
        }

        // ========== CHAT ==========
        case "chat": {
          const room = data[1] || ws.room || "default";
          const noImage = data[2] || ws.noimageUrl || "";
          const username = data[3] || ws.username || "Unknown";
          const message = data[4] || "";
          const userColor = data[5] || ws.color || "#FFFFFF";
          const textColor = data[6] || "#000000";

          this._broadcastToRoom(room, ["chat", room, noImage, username, message, userColor, textColor]);
          break;
        }

        // ========== PRIVATE ==========
        case "private": {
          const targetId = data[1] || "";
          const noImage = data[2] || ws.noimageUrl || "";
          const message = data[3] || "";
          const sender = data[4] || ws.username || "Unknown";

          for (const [wsId, targetWs] of this.wsMap) {
            if ((targetWs.idTarget === targetId || targetWs.username === targetId) && targetWs.readyState === 1) {
              this._safeSend(targetWs, ["private", sender, noImage, message, Date.now(), sender]);
              break;
            }
          }
          break;
        }

        // ========== NOTIFICATION ==========
        case "sendnotif": {
          const targetId = data[1] || "";
          const noImage = data[2] || ws.noimageUrl || "";
          const username = data[3] || ws.username || "Unknown";
          const deskripsi = data[4] || "";

          for (const [wsId, targetWs] of this.wsMap) {
            if ((targetWs.idTarget === targetId || targetWs.username === targetId) && targetWs.readyState === 1) {
              this._safeSend(targetWs, ["notif", noImage, username, deskripsi, Date.now()]);
              break;
            }
          }
          break;
        }

        // ========== KURSI (SEAT) ==========
        case "updateKursi": {
          const room = data[1] || ws.room || "default";
          const seat = data[2] || 0;
          const noImage = data[3] || "";
          const namauser = data[4] || "";
          const color = data[5] || "#FFFFFF";
          const itembawah = data[6] || 0;
          const itematas = data[7] || 0;
          const vip = data[8] || 0;
          const viptanda = data[9] || 0;

          ws.noimageUrl = noImage;
          ws.username = namauser || ws.idTarget;
          ws.color = color;
          ws.itembawah = itembawah;
          ws.itematas = itematas;
          ws.vip = vip;
          ws.viptanda = viptanda;
          ws.seatNumber = seat;

          // Save to memory
          if (!this.roomUsers.has(room)) this.roomUsers.set(room, new Map());
          const users = this.roomUsers.get(room);
          users.set(ws.username || ws.idTarget, {
            wsId: ws._wsId,
            seat: seat,
            noimageUrl: noImage,
            color: color,
            itembawah: itembawah,
            itematas: itematas,
            vip: vip,
            viptanda: viptanda
          });

          if (!this.roomSeats.has(room)) this.roomSeats.set(room, new Map());
          this.roomSeats.get(room).set(seat, ws.username || ws.idTarget);

          this._broadcastToRoom(room, ["kursiUpdated", room, seat, noImage, namauser, color, itembawah, itematas, vip, viptanda]);
          break;
        }

        // ========== POINT ==========
        case "updatePoint": {
          const room = data[1] || ws.room || "default";
          const seat = data[2] || 0;
          const x = data[3] || 0;
          const y = data[4] || 0;
          const fast = data[5] || 0;

          if (!this.roomPoints.has(room)) this.roomPoints.set(room, new Map());
          this.roomPoints.get(room).set(seat, { x, y, fast });

          this._broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        // ========== REMOVE KURSI ==========
        case "removeKursiAndPoint": {
          const room = data[1] || ws.room || "default";
          const seat = data[2] || 0;

          if (this.roomSeats.has(room)) {
            this.roomSeats.get(room).delete(seat);
          }
          if (this.roomPoints.has(room)) {
            this.roomPoints.get(room).delete(seat);
          }
          if (this.roomUsers.has(room)) {
            for (const [username, userData] of this.roomUsers.get(room)) {
              if (userData.seat === seat) {
                this.roomUsers.get(room).delete(username);
                break;
              }
            }
          }

          this._broadcastToRoom(room, ["removeKursi", room, seat]);
          break;
        }

        case "resetRoom": {
          const room = data[1] || ws.room || "default";
          this.roomUsers.delete(room);
          this.roomSeats.delete(room);
          this.roomPoints.delete(room);
          this._broadcastToRoom(room, ["resetRoom", room]);
          break;
        }

        // ========== GET ONLINE USERS ==========
        case "getOnlineUsers": {
          const users = [];
          for (const [wsId, client] of this.wsMap) {
            if (client.username && client.readyState === 1) {
              users.push(client.username);
            }
          }
          this._safeSend(ws, ["allOnlineUsers", users]);
          break;
        }

        // ========== IS USER ONLINE ==========
        case "isUserOnline": {
          const userId = data[1] || "";
          const tanda = data[2] || "";
          let online = false;

          for (const [wsId, client] of this.wsMap) {
            if ((client.idTarget === userId || client.username === userId) && client.readyState === 1) {
              online = true;
              break;
            }
          }

          this._safeSend(ws, ["userOnlineStatus", userId, online, tanda]);
          break;
        }

        // ========== GET ALL ROOMS USER COUNT ==========
        case "getAllRoomsUserCount": {
          const rooms = [];
          for (const [room, clients] of this.wsClients) {
            rooms.push({ roomName: room, userCount: clients.size });
          }
          this._safeSend(ws, ["allRoomsUserCount", rooms]);
          break;
        }

        // ========== GET CURRENT NUMBER ==========
        case "getCurrentNumber": {
          const room = ws.room || "default";
          const game = this.activeGames.get(room);
          if (game && game._phase === 'draw' && game.numbers) {
            const myNumber = game.numbers.get(ws.username || ws.idTarget);
            if (myNumber) {
              this._safeSend(ws, ["currentNumber", myNumber]);
            } else {
              this._safeSend(ws, ["currentNumber", 0]);
            }
          } else {
            this._safeSend(ws, ["currentNumber", 0]);
          }
          break;
        }

        // ========== GIFT ==========
        case "gift": {
          const room = data[1] || ws.room || "default";
          const sender = data[2] || ws.username || "Unknown";
          const receiver = data[3] || "";
          const giftName = data[4] || "";

          this._broadcastToRoom(room, ["gift", room, sender, receiver, giftName, Date.now()]);
          break;
        }

        // ========== MOD WARNING ==========
        case "modwarning": {
          const room = data[1] || ws.room || "default";
          this._broadcastToRoom(room, ["modwarning", room]);
          break;
        }

        // ========== MUTE ==========
        case "setMuteType": {
          const isMuted = data[1] || false;
          const room = data[2] || ws.room || "default";
          this.roomMutes.set(room, isMuted);
          this._safeSend(ws, ["muteTypeResponse", isMuted, room]);
          break;
        }

        case "getMuteType": {
          const room = data[1] || ws.room || "default";
          const isMuted = this.roomMutes.get(room) || false;
          this._safeSend(ws, ["muteTypeResponse", isMuted, room]);
          break;
        }

        // ========== ROLL ANGKA ==========
        case "rollangak": {
          const room = data[1] || ws.room || "default";
          const username = data[2] || ws.username || "Unknown";
          const angka = data[3] || 0;
          this._broadcastToRoom(room, ["rollangakBroadcast", room, username, angka]);
          break;
        }

        // ========== ON DESTROY ==========
        case "onDestroy": {
          this._handleChatClose(ws);
          break;
        }

        default:
          break;
      }

    } catch(e) {
      console.error('Chat message error:', e);
    }
  }

  // ============================================================
  // ✅ CHAT: JOIN ROOM
  // ============================================================
  async _chatJoinRoom(ws, room) {
    try {
      const wsId = ws._wsId;
      if (!wsId) return;

      const currentRoom = ws.room || this.clientRooms.get(wsId);

      // Leave current room
      if (currentRoom && currentRoom !== room) {
        const clients = this.wsClients.get(currentRoom);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) this.wsClients.delete(currentRoom);
        }
        this._broadcastToRoom(currentRoom, ["userLeftRoom", ws.username || ws.idTarget, currentRoom]);
      }

      // Join new room
      ws.room = room;

      let clients = this.wsClients.get(room);
      if (!clients) {
        clients = new Set();
        this.wsClients.set(room, clients);
      }
      clients.add(wsId);
      this.clientRooms.set(wsId, room);

      // Get available seat
      let seat = 1;
      if (this.roomSeats.has(room)) {
        const usedSeats = new Set(this.roomSeats.get(room).keys());
        while (usedSeats.has(seat) && seat <= 45) seat++;
      }
      ws.seatNumber = seat;

      // Save to memory
      if (!this.roomUsers.has(room)) this.roomUsers.set(room, new Map());
      const users = this.roomUsers.get(room);
      users.set(ws.username || ws.idTarget, {
        wsId: wsId,
        seat: seat,
        noimageUrl: ws.noimageUrl || '',
        color: ws.color || '#FFFFFF'
      });

      if (!this.roomSeats.has(room)) this.roomSeats.set(room, new Map());
      this.roomSeats.get(room).set(seat, ws.username || ws.idTarget);

      // Send response
      this._safeSend(ws, ["rooMasuk", seat, room]);
      this._safeSend(ws, ["numberKursiSaya", seat]);

      // Broadcast to room
      this._broadcastToRoom(room, ["userJoinedRoom", ws.username || ws.idTarget, room]);

      // Send all kursi data to new user
      if (this.roomUsers.has(room)) {
        const allUsers = this.roomUsers.get(room);
        const kursiList = [];
        for (const [username, userData] of allUsers) {
          if (username !== (ws.username || ws.idTarget)) {
            kursiList.push([userData.seat, {
              noimageUrl: userData.noimageUrl || '',
              namauser: username,
              color: userData.color || '#FFFFFF',
              itembawah: userData.itembawah || 0,
              itematas: userData.itematas || 0,
              vip: userData.vip || 0,
              viptanda: userData.viptanda || 0
            }]);
          }
        }
        if (kursiList.length > 0) {
          this._safeSend(ws, ["kursiBatchUpdate", room, kursiList]);
        }
      }

      // Send all points data to new user
      if (this.roomPoints.has(room)) {
        const points = this.roomPoints.get(room);
        const pointList = [];
        for (const [seat, pointData] of points) {
          pointList.push({ seat, x: pointData.x, y: pointData.y, fast: pointData.fast });
        }
        if (pointList.length > 0) {
          this._safeSend(ws, ["allPointsList", room, pointList]);
        }
      }

    } catch(e) {
      console.error('Join room error:', e);
    }
  }

  // ============================================================
  // ✅ CHAT CLOSE
  // ============================================================
  _handleChatClose(ws) {
    try {
      if (!ws) return;
      ws._closing = true;

      const wsId = ws._wsId;
      const room = ws.room || this.clientRooms.get(wsId);
      const username = ws.username || ws.idTarget;

      if (room) {
        // Remove from memory
        if (this.roomUsers.has(room)) {
          this.roomUsers.get(room).delete(username);
          if (this.roomUsers.get(room).size === 0) this.roomUsers.delete(room);
        }

        if (this.roomSeats.has(room) && ws.seatNumber) {
          this.roomSeats.get(room).delete(ws.seatNumber);
          if (this.roomSeats.get(room).size === 0) this.roomSeats.delete(room);
        }

        if (this.roomPoints.has(room) && ws.seatNumber) {
          this.roomPoints.get(room).delete(ws.seatNumber);
          if (this.roomPoints.get(room).size === 0) this.roomPoints.delete(room);
        }

        // Remove from WebSocket clients
        const clients = this.wsClients.get(room);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) this.wsClients.delete(room);
        }

        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);

        this._broadcastToRoom(room, ["userLeftRoom", username, room]);
        this._broadcastToRoom(room, ["removeKursi", room, ws.seatNumber || 0]);
      }

      ws.room = null;
      ws.username = null;
      ws._wsId = null;
      ws._closing = true;

      try { ws.close(1000, "Closed"); } catch(e) {}

    } catch(e) {
      console.error('Chat close error:', e);
    }
  }

  // ============================================================
  // ✅ HANDLE GAME WEBSOCKET
  // ============================================================
  async handleGameWebSocket(request) {
    try {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("WebSocket only", { status: 400 });
      }

      if (this.wsMap.size >= CONSTANTS.MAX_WS_CLIENTS) {
        return new Response("Server full", { status: 503 });
      }

      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      const wsId = ++this._wsIdCounter;

      server._wsId = wsId;
      server._closing = false;
      server.room = null;
      server.username = null;

      try {
        server.accept();
      } catch(e) {
        return new Response("WebSocket failed", { status: 500 });
      }

      this.wsMap.set(wsId, server);

      server.addEventListener("message", async (event) => {
        try {
          if (server._closing || this.closing) return;
          const data = JSON.parse(event.data);
          if (Array.isArray(data) && data.length > 0) {
            await this._handleGameMessage(server, data);
          }
        } catch(e) {
          console.error('Game message error:', e);
        }
      });

      server.addEventListener("close", () => {
        this._handleGameClose(server);
      }, { once: true });

      server.addEventListener("error", () => {
        this._handleGameClose(server);
      }, { once: true });

      return new Response(null, { status: 101, webSocket: client });

    } catch(e) {
      console.error('Game WS error:', e);
      return new Response("Error", { status: 500 });
    }
  }

  // ============================================================
  // ✅ GAME MESSAGE HANDLER
  // ============================================================
  async _handleGameMessage(ws, data) {
    try {
      const evt = data[0];
      const room = ws.room || "default";

      switch(evt) {
        // ========== SWITCH ROOM ==========
        case "switchRoom": {
          const newRoom = data[1] || "default";
          const username = data[2] || `user_${ws._wsId}`;
          ws.room = newRoom;
          ws.username = username;

          // Add to room
          let clients = this.wsClients.get(newRoom);
          if (!clients) {
            clients = new Set();
            this.wsClients.set(newRoom, clients);
          }
          clients.add(ws._wsId);
          this.clientRooms.set(ws._wsId, newRoom);

          this._safeSend(ws, ["switchRoomSuccess", newRoom]);
          this._sendGameState(ws, newRoom);

          if (newRoom === DICE_ROOM) {
            this._sendDiceStatus(ws);
          }
          break;
        }

        // ========== DICE ==========
        case "submitDiceAnswer": {
          await this._submitDiceAnswer(ws, data[1], data[2]);
          break;
        }

        case "getDiceLastWeekWinner": {
          this._checkWeekReset();
          if (this.lastWeekWinner) {
            this._safeSend(ws, ["diceLastWeekWinner", 
              this.lastWeekWinner.username, 
              this.lastWeekWinner.score,
              this.lastWeekWinner.week
            ]);
          } else {
            this._safeSend(ws, ["diceLastWeekWinner", "", 0, ""]);
          }
          break;
        }

        case "getDiceLeaderboard": {
          const limit = data[1] || 10;
          const sorted = [...this.dicePoints.entries()]
            .sort((a, b) => b[1] - a[1])
            .slice(0, limit);
          this._safeSend(ws, ["diceLeaderboard", sorted.map(([u, s]) => `${u}|${s}`)]);
          break;
        }

        case "deleteDiceLastWeekWinner": {
          this.lastWeekWinner = null;
          this._safeSend(ws, ["diceLastWeekWinnerDeleted", true, "Deleted"]);
          this._broadcastToRoom(DICE_ROOM, ["diceNotification", "Last week winner deleted"]);
          break;
        }

        case "getDiceStatus": {
          this._sendDiceStatus(ws);
          break;
        }

        // ========== RECORDING ==========
        case "startRecordingWinners": {
          const roomName = data[1];
          if (roomName) {
            this.recordingEnabled.set(roomName, true);
            this._broadcastToRoom(roomName, ["recordingStatus", true]);
            this._safeSend(ws, ["startRecordingResult", { success: true, message: "Recording enabled" }]);
          }
          break;
        }

        case "stopRecordingWinners": {
          const roomName = data[1];
          if (roomName) {
            this.recordingEnabled.set(roomName, false);
            this.lowcardWinners.delete(roomName);
            this._broadcastToRoom(roomName, ["recordingStatus", false]);
            this._safeSend(ws, ["stopRecordingResult", { success: true, message: "Recording stopped" }]);
          }
          break;
        }

        case "getRecordingStatus": {
          const roomName = data[1];
          if (roomName) {
            const isRecording = this.recordingEnabled.get(roomName) || false;
            this._safeSend(ws, ["recordingStatus", isRecording]);
          }
          break;
        }

        case "getRoomWinners": {
          const roomName = data[1] || room;
          const winners = this.lowcardWinners.get(roomName) || {};
          const isRecording = this.recordingEnabled.get(roomName) || false;
          this._safeSend(ws, ["roomWinnersResponse", { winners, room: roomName, recording: isRecording }]);
          break;
        }

        case "sendWinnersToRoom": {
          const roomName = data[1] || room;
          const winners = this.lowcardWinners.get(roomName) || {};
          this._broadcastToRoom(roomName, ["lowCardWinnerUpdate", { winners, room: roomName }]);
          this._safeSend(ws, ["sendWinnersResult", { success: true, message: "Winners refreshed" }]);
          break;
        }

        case "startGameWithRecording": {
          const roomName = data[1];
          const bet = data[2] || 0;
          const username = data[3] || ws.username || "Unknown";

          const isRecording = this.recordingEnabled.get(roomName) || false;
          if (!isRecording) {
            this._safeSend(ws, ["recordingError", "Recording is not enabled in this room"]);
            return;
          }

          await this._startGame(ws, bet, username, roomName, true);
          break;
        }

        // ========== LOWCARD GAME ==========
        case "gameLowCardStart": {
          await this._startGame(ws, data[1], data[2], room, false);
          break;
        }

        case "gameLowCardJoin": {
          await this._joinGame(ws, data[1], room);
          break;
        }

        case "gameLowCardNumber": {
          await this._submitNumber(ws, data[1], data[2] || "", data[3], room);
          break;
        }

        case "gameLowCardLeave": {
          await this._leaveGame(ws, data[1], room);
          break;
        }

        case "checkGameRunning": {
          const game = this.activeGames.get(room);
          const isRunning = game?._isActive && !game._gameEnded;
          this._safeSend(ws, ["gameStatus", isRunning ? "true" : "false"]);
          if (isRunning) this._sendGameState(ws, room);
          break;
        }

        case "getGameState": {
          this._sendGameState(ws, room);
          break;
        }

        default:
          break;
      }

    } catch(e) {
      console.error('Game message error:', e);
    }
  }

  // ============================================================
  // ✅ GAME CLOSE
  // ============================================================
  _handleGameClose(ws) {
    try {
      if (!ws) return;
      ws._closing = true;

      const wsId = ws._wsId;
      const room = ws.room || this.clientRooms.get(wsId);

      if (room) {
        const clients = this.wsClients.get(room);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) this.wsClients.delete(room);
        }
        this.clientRooms.delete(wsId);
        this.wsMap.delete(wsId);
      }

      ws.room = null;
      ws.username = null;
      ws._wsId = null;
      ws._closing = true;

      try { ws.close(1000, "Closed"); } catch(e) {}

    } catch(e) {
      console.error('Game close error:', e);
    }
  }

  // ============================================================
  // ✅ DICE SYSTEM
  // ============================================================
  _checkDice() {
    try {
      if (this._tieActive || this._isShowingDice || this._diceTimeUpCooldown) return;
      if (!this._isDiceTime()) return;
      if (this.currentDiceRoll || this._diceLock) return;

      const clients = this.wsClients?.get(DICE_ROOM);
      if (clients?.size > 0) {
        this._startDice();
      }
    } catch(e) {}
  }

  _isDiceTime() {
    try {
      const witaTime = this._getCurrentWITATime();
      const currentTotal = witaTime.totalMinutes;
      for (const session of QUIZ_SCHEDULE.SESSIONS) {
        const startTotal = session.start * 60;
        const endTotal = session.end * 60;
        if (currentTotal >= startTotal && currentTotal < endTotal) return true;
      }
      return false;
    } catch(e) { return false; }
  }

  _getCurrentWITATime() {
    try {
      const now = new Date();
      const hours = (now.getUTCHours() + QUIZ_SCHEDULE.TIMEZONE_OFFSET) % 24;
      const minutes = now.getUTCMinutes();
      return { hours, minutes, totalMinutes: (hours * 60) + minutes };
    } catch(e) { return { hours: 0, minutes: 0, totalMinutes: 0 }; }
  }

  _startDice() {
    try {
      if (this._diceLock || this.currentDiceRoll || this._isShowingDice) return;
      this._diceLock = true;
      this._isShowingDice = true;

      const value = Math.floor(Math.random() * 6) + 1;
      this._diceRound = (this._diceRound || 0) + 1;
      this.currentDiceRoll = { value, timestamp: Date.now(), round: this._diceRound };
      this._diceStartTime = Date.now();
      this._canSubmitDiceAnswer = true;
      this.diceAnswered = new Set();
      this._playerAnswers = new Map();
      this.diceHasWinner = false;
      this.diceWinner = null;

      this._broadcastToRoom(DICE_ROOM, ["diceRoll", { 
        value, 
        timestamp: Date.now(),
        answerTime: 20,
        canAnswerNow: true,
        round: this._diceRound
      }]);

      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "♡ clik draw ♡"]);

      let timeLeft = 20;
      const timerInterval = setInterval(() => {
        timeLeft--;
        if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
          this._broadcastToRoom(DICE_ROOM, ["diceNotification", `${timeLeft}s remaining`]);
        }
        if (timeLeft <= 0) {
          clearInterval(timerInterval);
          this._endDiceRound();
        }
      }, 1000);

      this._diceTimerInterval = timerInterval;
      this._allTimers.add(timerInterval);

      this._diceTimeout = setTimeout(() => {
        this._endDiceRound();
      }, 20000);
      this._allTimers.add(this._diceTimeout);

    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  async _endDiceRound() {
    try {
      if (this._diceTimerInterval) {
        clearInterval(this._diceTimerInterval);
        this._diceTimerInterval = null;
      }
      if (this._diceTimeout) {
        clearTimeout(this._diceTimeout);
        this._diceTimeout = null;
      }

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
        this._broadcastToRoom(DICE_ROOM, ["diceNoWinner", {
          message: "No winner",
          value: diceValue,
          round: roundNumber
        }]);
      } else if (correctPlayers.length === 1) {
        const winner = correctPlayers[0];
        const currentPoints = this.dicePoints.get(winner) || 0;
        this.dicePoints.set(winner, currentPoints + 1);

        this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
          username: winner,
          totalPoints: currentPoints + 1,
          diceValue: diceValue,
          round: roundNumber
        }]);
      } else if (correctPlayers.length > 1 && !this._tieActive) {
        this.currentDiceRoll = null;
        this._diceLock = false;
        this._isShowingDice = false;

        await this._startTieBreaker(DICE_ROOM, correctPlayers);
        return;
      }

      this.currentDiceRoll = null;
      this._diceLock = false;
      this._diceTimeUpCooldown = true;

      setTimeout(() => {
        this._diceTimeUpCooldown = false;
        this._checkDice();
      }, 15000);

    } catch(e) {
      this._diceLock = false;
      this._isShowingDice = false;
    }
  }

  // ============================================================
  // ✅ SUBMIT DICE ANSWER
  // ============================================================
  async _submitDiceAnswer(ws, username, guess) {
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

        this._broadcastToRoom(DICE_ROOM, ["diceAnswer", {
          username,
          guess: guessValue,
          isTieBreaker: true,
          tieRound: this._tieRound
        }]);

        if (this._tieAnswers.size === this._tiePlayers.length) {
          this._canSubmitDiceAnswer = false;
          this._isShowingDice = false;

          if (this._tieTimer) {
            clearTimeout(this._tieTimer);
            this._tieTimer = null;
          }
          if (this._tieInterval) {
            clearInterval(this._tieInterval);
            this._tieInterval = null;
          }

          const tieId = this._getActiveTieBreakerId();
          if (tieId) {
            setTimeout(async () => {
              await this._processTieResults(DICE_ROOM, tieId, this._tiePlayers);
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

      this._broadcastToRoom(DICE_ROOM, ["diceAnswer", {
        username,
        guess: guessValue,
        round: this._diceRound || 1
      }]);

      if (guessValue === diceValue && !this.diceHasWinner) {
        this.diceHasWinner = true;
        this.diceWinner = username;
      }

    } catch(e) {
      console.error('Submit dice answer error:', e);
    }
  }

  // ============================================================
  // ✅ TIE BREAKER
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
      this._tieBreakers.set(id, { 
        players, 
        round: 0, 
        winner: null, 
        status: 'waiting' 
      });

      await this._runTieRound(room, id, players);

    } finally {
      setTimeout(() => {
        this._tieLock = false;
      }, 2000);
    }
  }

  async _runTieRound(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;

    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);

    this._tieRound++;
    this._tiePlayers = [...players];
    this._tieAnswers = new Map();
    data.round = this._tieRound;
    data.status = 'running';
    data.players = players;

    this._canSubmitDiceAnswer = true;
    this.diceAnswered = new Set();
    this._playerAnswers = new Map();
    this._isShowingDice = true;
    this.diceHasWinner = false;
    this.diceWinner = null;

    this._broadcastToRoom(DICE_ROOM, ["diceNotification", 
      `♡ Tie Round ${this._tieRound}: ${players.join(', ')}`
    ]);

    this._startTieTimer(room, id, players);
  }

  _startTieTimer(room, id, players) {
    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);

    let timeLeft = CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20;
    let notified10 = false, notified5 = false, isProcessed = false;

    this._tieInterval = this._trackTimer(setInterval(() => {
      timeLeft--;
      if (timeLeft === 10 && !notified10) { 
        notified10 = true; 
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "10s remaining"]); 
      }
      if (timeLeft === 5 && !notified5) { 
        notified5 = true; 
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "5s remaining"]); 
      }
      if (timeLeft === 3) {
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "3s remaining"]);
      }

      if (timeLeft <= 0 && !isProcessed) {
        isProcessed = true;
        this._clearTimer(this._tieInterval);
        this._tieInterval = null;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "TIME UP"]);

        const tieId = this._getActiveTieBreakerId();
        if (tieId) this._processTieResults(room, tieId, players);
        else { 
          this._resetTieBreakerState(null); 
          this._startCooldownAfterTieBreaker(); 
        }
      }
    }, 1000));

    this._tieTimer = this._trackTimer(setTimeout(() => {
      if (!isProcessed) {
        isProcessed = true;
        this._clearTimer(this._tieInterval);
        this._tieInterval = null;
        this._canSubmitDiceAnswer = false;
        this._isShowingDice = false;
        this._broadcastToRoom(DICE_ROOM, ["diceNotification", "TIME UP"]);

        const tieId = this._getActiveTieBreakerId();
        if (tieId) this._processTieResults(room, tieId, players);
        else { 
          this._resetTieBreakerState(null); 
          this._startCooldownAfterTieBreaker(); 
        }
      }
    }, (CONSTANTS.TIE_BREAKER_TIME_LIMIT || 20) * 1000 + 2000));
  }

  async _processTieResults(room, id, players) {
    const data = this._tieBreakers.get(id);
    if (!data) return;

    let highest = 0, highestPlayers = [];
    for (const player of players) {
      const answer = this._tieAnswers.get(player);
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
      this._broadcastToRoom(DICE_ROOM, ["diceNotification", "No one answered tie breaker"]);
      this._resetTieBreakerState(id);
      this._startCooldownAfterTieBreaker();
      return;
    }

    if (highestPlayers.length === 1) {
      const winner = highestPlayers[0];
      const currentPoints = this.dicePoints.get(winner) || 0;
      this.dicePoints.set(winner, currentPoints + 1);

      this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: currentPoints + 1,
        diceValue: highest,
        round: this._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: this._tieRound,
        finalWinner: true
      }]);

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
      const currentPoints = this.dicePoints.get(winner) || 0;
      this.dicePoints.set(winner, currentPoints + 1);

      this._broadcastToRoom(DICE_ROOM, ["diceWinner", {
        username: winner,
        totalPoints: currentPoints + 1,
        diceValue: 'auto',
        round: this._diceRound || 1,
        isTieBreaker: true,
        tieBreakerRound: this._tieRound,
        finalWinner: true
      }]);
    } catch(e) {
      console.error('Process single winner error:', e);
    }

    this._resetTieBreakerState(id);
    this._startCooldownAfterTieBreaker();
  }

  _startCooldownAfterTieBreaker() {
    this._broadcastToRoom(DICE_ROOM, ["diceNotification", "wait 15s"]);
    this._diceTimeUpCooldown = true;

    this._clearTimer(this._diceTimeUpCooldownTimer);
    this._diceTimeUpCooldownTimer = this._trackTimer(setTimeout(() => {
      this._diceTimeUpCooldownTimer = null;
      this._diceTimeUpCooldown = false;
      this._checkDice();
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

    this._clearTimer(this._tieTimer);
    this._clearTimer(this._tieInterval);
    this._tieTimer = null;
    this._tieInterval = null;
  }

  _getActiveTieBreakerId() {
    for (const [id, data] of this._tieBreakers) {
      if (data.status === 'waiting' || data.status === 'running') return id;
    }
    return null;
  }

  // ============================================================
  // ✅ WEEK RESET
  // ============================================================
  _checkWeekReset() {
    const currentWeek = this._generateCurrentWeek(new Date());
    if (this.lastResetWeek !== currentWeek) {
      const points = Object.fromEntries(this.dicePoints);

      let winner = null, highestScore = 0;
      for (const [username, score] of Object.entries(points)) {
        if (score > highestScore) {
          highestScore = score;
          winner = username;
        }
      }

      if (winner && highestScore > 0) {
        this.lastWeekWinner = {
          username: winner,
          score: highestScore,
          week: this.lastResetWeek || currentWeek
        };
      } else {
        this.lastWeekWinner = null;
      }

      this.dicePoints.clear();
      this.lastResetWeek = currentWeek;

      this._broadcastToRoom(DICE_ROOM, ["diceReset", { 
        winner: winner, 
        score: highestScore, 
        week: currentWeek 
      }]);
    }
  }

  _generateCurrentWeek(date) {
    const now = date || new Date();
    const year = now.getUTCFullYear();
    const startOfYear = new Date(Date.UTC(year, 0, 1));
    const diff = now - startOfYear;
    const week = Math.ceil((diff / 86400000 + startOfYear.getUTCDay() + 1) / 7);
    return `${year}-W${String(week).padStart(2, '0')}`;
  }

  // ============================================================
  // ✅ LOWCARD GAME
  // ============================================================
  async _startGame(ws, bet, username, room, isRecording = false) {
    try {
      if (this.isDestroyed) {
        this._safeSend(ws, ["gameLowCardError", "Server is shutting down"]);
        return;
      }

      const lockKey = `game_start_${room}`;
      if (this._gameLocks.has(lockKey)) {
        this._safeSend(ws, ["gameLowCardError", "Game is starting, please wait"]);
        return;
      }

      this._gameLocks.set(lockKey, Date.now());

      try {
        // Cek game existing
        const existingGame = this.activeGames.get(room);
        if (existingGame?._isActive && !existingGame._gameEnded) {
          this._safeSend(ws, ["gameLowCardError", "Game is already running"]);
          return;
        }

        // Cleanup
        if (existingGame) {
          const timers = ['_registrationTimer', '_drawTimer', '_evalTimer'];
          for (const key of timers) {
            if (existingGame[key]) { 
              this._clearTimer(existingGame[key]); 
              existingGame[key] = null; 
            }
          }
          existingGame._gameEnded = true;
          existingGame._isActive = false;
          this.activeGames.delete(room);
        }

        const betAmount = parseInt(bet, 10) || 0;
        if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > CONSTANTS.MAX_BET) {
          this._safeSend(ws, ["gameLowCardError", `Invalid bet (0 or 100-${CONSTANTS.MAX_BET})`]);
          return;
        }

        if (this.activeGames.size >= CONSTANTS.MAX_LOWCARD_GAMES) {
          this._safeSend(ws, ["gameLowCardError", "Server is busy"]);
          return;
        }

        // Create game
        const game = {
          room,
          players: new Map(),
          botPlayers: new Map(),
          registrationOpen: true,
          round: 1,
          numbers: new Map(),
          tanda: new Map(),
          eliminated: new Set(),
          betAmount,
          hostName: username,
          useBots: false,
          evaluationLocked: false,
          drawTimeExpired: false,
          _isActive: true,
          _gameEnded: false,
          _phase: 'registration',
          _botTimeouts: new Set(),
          _botsAdded: false,
          _registrationTimer: null,
          _drawTimer: null,
          _evalTimer: null,
          _safetyTimer: null,
          _isEvaluating: false,
          _createdAt: Date.now(),
          _drawPhaseStart: null,
          _startedByRecording: isRecording
        };

        game.players.set(username, { id: username, name: username });
        this.activeGames.set(room, game);

        this._broadcastToRoom(room, ["gameLowCardStart", betAmount]);
        this._broadcastToRoom(room, ["gameLowCardStartSuccess", username, betAmount]);

        if (isRecording) {
          this._broadcastToRoom(room, ["recordingGameStarted", room, username, betAmount]);
        }

        // Start registration
        this._startRegistration(room, game);

      } finally {
        setTimeout(() => {
          this._gameLocks.delete(lockKey);
        }, 3000);
      }

    } catch(e) {
      console.error('Start game error:', e);
      this._safeSend(ws, ["gameLowCardError", e.message || "Failed to start game"]);
    }
  }

  _startRegistration(room, game) {
    try {
      if (!game?._isActive || !game.registrationOpen) return;
      if (game._registrationTimer) { 
        this._clearTimer(game._registrationTimer); 
        game._registrationTimer = null; 
      }

      let timeLeft = 20;
      const timer = this._trackTimer(setInterval(() => {
        try {
          if (!game?._isActive || !game.registrationOpen || timeLeft < 0) {
            this._clearTimer(timer);
            if (game._registrationTimer === timer) game._registrationTimer = null;
            return;
          }
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          if (timeLeft === 0) {
            this._clearTimer(timer);
            game._registrationTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeRegistration(room, game);
          }
          timeLeft--;
        } catch(e) { 
          this._clearTimer(timer); 
          if (game._registrationTimer === timer) game._registrationTimer = null; 
        }
      }, 1000));
      game._registrationTimer = timer;

    } catch(e) {
      console.error('Start registration error:', e);
    }
  }

  async _closeRegistration(room, game) {
    try {
      if (!game?._isActive || !game.registrationOpen) return;
      game.registrationOpen = false;
      if (game._registrationTimer) { 
        this._clearTimer(game._registrationTimer); 
        game._registrationTimer = null; 
      }

      // Add bots if needed
      if (!game._botsAdded && game.players.size < 2) {
        this._addBots(room, game, 4);
        game._botsAdded = true;
      }

      if (game._isActive && game.players.size >= 2) {
        this._startDrawPhase(room, game);
      } else {
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
        this._scheduleGameCleanup(room, game);
      }

    } catch(e) {
      console.error('Close registration error:', e);
    }
  }

  _addBots(room, game, count) {
    try {
      if (!game?._isActive) return;
      const botNames = ["moz1", "moz2", "moz3", "moz4"];
      const existingBots = Array.from(game.players.keys()).filter(id => id.startsWith('BOT_'));
      const existingBotCount = existingBots.length;
      const maxBotsToAdd = Math.min(count, CONSTANTS.MAX_BOTS_PER_GAME - existingBotCount);

      for (let i = 0; i < maxBotsToAdd; i++) {
        const botId = `BOT_${room}_${i}_${Date.now()}`;
        const botName = botNames[(existingBotCount + i) % botNames.length];
        if (!game.players.has(botId)) {
          game.players.set(botId, { id: botId, name: botName });
          game.botPlayers.set(botId, botName);
        }
      }
      game._botsAdded = true;
      game.useBots = true;

    } catch(e) {
      console.error('Add bots error:', e);
    }
  }

  async _startDrawPhase(room, game) {
    try {
      if (!game?._isActive || game._gameEnded) return;

      const activePlayers = Array.from(game.players.values()).filter(p => !game.eliminated?.has(p.id));
      
      if (activePlayers.length < 2) {
        if (!game._botsAdded) {
          const needed = Math.min(4 - activePlayers.length, CONSTANTS.MAX_BOTS_PER_GAME);
          if (needed > 0) { 
            this._addBots(room, game, needed); 
            game._botsAdded = true; 
          }
        }
        const newActive = Array.from(game.players.values()).filter(p => !game.eliminated?.has(p.id));
        if (newActive.length < 2) {
          if (newActive.length === 1 && !game._gameEnded) {
            const winner = newActive[0]?.name || "Unknown";
            const totalCoin = (game.betAmount || 0) * game.players.size;

            if (game._startedByRecording) {
              await this._addLowCardWinner(room, winner);
            }

            this._broadcastToRoom(room, ["gameLowCardWinner", winner, totalCoin]);
            game._gameEnded = true;
            game._isActive = false;
            this._scheduleGameCleanup(room, game);
          } else {
            game._gameEnded = true;
            game._isActive = false;
            this._broadcastToRoom(room, ["gameLowCardError", "Not enough players"]);
            this._scheduleGameCleanup(room, game);
          }
          return;
        }
      }

      game._phase = 'draw';
      game.drawTimeExpired = false;
      game.evaluationLocked = false;
      game._drawPhaseStart = Date.now();
      if (!game._botTimeouts) game._botTimeouts = new Set();

      const playersList = Array.from(game.players.values())
        .filter(p => !game.eliminated?.has(p.id))
        .map(p => p.name);
      
      this._broadcastToRoom(room, ["gameLowCardClosed", playersList]);
      this._broadcastToRoom(room, ["gameLowCardNextRound", game.round]);
      this._startDrawCountdown(room, game);

      if (game.botPlayers.size > 0 && game._isActive) {
        this._startBotDraws(room, game);
      }

    } catch(e) {
      console.error('Start draw phase error:', e);
    }
  }

  _startDrawCountdown(room, game) {
    try {
      if (!game?._isActive) return;
      if (game._drawTimer) { 
        this._clearTimer(game._drawTimer); 
        game._drawTimer = null; 
      }

      let timeLeft = 20;
      const timer = this._trackTimer(setInterval(() => {
        try {
          if (!game?._isActive || game.drawTimeExpired || timeLeft < 0) {
            this._clearTimer(timer);
            if (game._drawTimer === timer) game._drawTimer = null;
            return;
          }
          if (timeLeft === 15 || timeLeft === 10 || timeLeft === 5) {
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
          if (timeLeft === 0) {
            this._clearTimer(timer);
            game._drawTimer = null;
            this._broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP"]);
            this._closeDrawPhase(room, game);
          }
          timeLeft--;
        } catch(e) { 
          this._clearTimer(timer); 
          if (game._drawTimer === timer) game._drawTimer = null; 
        }
      }, 1000));
      game._drawTimer = timer;

    } catch(e) {
      console.error('Start draw countdown error:', e);
    }
  }

  async _closeDrawPhase(room, game) {
    try {
      if (!game?._isActive || game.drawTimeExpired || game.evaluationLocked) return;
      game.drawTimeExpired = true;
      game.evaluationLocked = true;
      if (game._drawTimer) { 
        this._clearTimer(game._drawTimer); 
        game._drawTimer = null; 
      }

      // Force bot draws
      if (game.botPlayers.size > 0 && game._isActive) {
        const activeBotIds = Array.from(game.botPlayers.keys())
          .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id));
        for (const botId of activeBotIds) {
          this._forceBotDraw(room, botId, game);
        }
      }

      // Check submissions
      const activeIds = Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
      const submittedIds = new Set(game.numbers.keys());
      const notSubmitted = activeIds.filter(id => !submittedIds.has(id));

      // If no one submitted
      if (notSubmitted.length > 0 && submittedIds.size === 0) {
        this._broadcastToRoom(room, ["gameLowCardError", "No one submitted numbers"]);
        game._gameEnded = true;
        game._isActive = false;
        this._scheduleGameCleanup(room, game);
        return;
      }

      // Eliminate players who didn't submit
      for (const id of notSubmitted) {
        game.eliminated.add(id);
        game.numbers.delete(id);
        game.tanda.delete(id);
      }

      // Check remaining players
      const remaining = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));

      if (remaining.length === 1 && !game._gameEnded) {
        const winnerId = remaining[0];
        const winnerName = game.players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * game.players.size;

        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winnerName);
        }

        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        this._scheduleGameCleanup(room, game);
        return;
      }

      if (remaining.length === 0) {
        this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
        game._gameEnded = true;
        game._isActive = false;
        this._scheduleGameCleanup(room, game);
        return;
      }

      // Evaluate
      this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
      if (game._evalTimer) { 
        this._clearTimer(game._evalTimer); 
        game._evalTimer = null; 
      }

      const evalTimer = this._trackTimer(setTimeout(() => {
        try {
          const currentGame = this.activeGames.get(room);
          if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
            this._evaluateRound(room, game);
          }
        } catch(e) {
          console.error('Evaluation timeout error:', e);
        }
      }, CONSTANTS.EVALUATION_DELAY_MS));
      game._evalTimer = evalTimer;

    } catch(e) {
      console.error('Close draw phase error:', e);
    }
  }

  async _evaluateRound(room, game) {
    try {
      if (!game?._isActive || game._gameEnded || game._isEvaluating) return;
      game._isEvaluating = true;

      // Safety timer
      const safetyTimer = this._trackTimer(setTimeout(() => {
        if (game?._isEvaluating) { 
          game._isEvaluating = false; 
          this._scheduleGameCleanup(room, game); 
        }
      }, CONSTANTS.EVALUATION_TIMEOUT_MS));
      game._safetyTimer = safetyTimer;

      const numbers = game.numbers || new Map();
      const entries = Array.from(numbers.entries());
      const submittedIds = new Set(numbers.keys());
      const activeIds = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));

      // Eliminate players who didn't submit
      for (const id of activeIds) {
        if (!submittedIds.has(id)) {
          game.eliminated.add(id);
        }
      }

      const remaining = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));

      if (entries.length === 0 || remaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }

        if (remaining.length === 1 && !game._gameEnded) {
          const winnerId = remaining[0];
          const winnerName = game.players.get(winnerId)?.name || winnerId;
          const totalCoin = (game.betAmount || 0) * game.players.size;

          if (game._startedByRecording) {
            await this._addLowCardWinner(room, winnerName);
          }

          this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
          game._gameEnded = true;
          game._isActive = false;
          this._scheduleGameCleanup(room, game);
        } else {
          this._broadcastToRoom(room, ["gameLowCardError", "No valid entries"]);
          game._gameEnded = true;
          game._isActive = false;
          this._scheduleGameCleanup(room, game);
        }
        return;
      }

      // Find lowest number
      const values = entries.map(([, n]) => n);
      const lowest = Math.min(...values);
      const losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);

      for (const id of losers) {
        game.eliminated.add(id);
      }

      const newRemaining = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));

      // All same numbers
      if (values.every(v => v === values[0]) && newRemaining.length >= 2) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        game.numbers = new Map();
        game.tanda = new Map();
        game.round++;
        game.evaluationLocked = false;
        game.drawTimeExpired = false;
        game._phase = 'draw';

        this._broadcastToRoom(room, ["gameLowCardRoundResult", game.round - 1,
          entries.map(([id, n]) => `${game.players.get(id)?.name || id}:${n}`),
          [], Array.from(newRemaining).map(id => game.players.get(id)?.name || id), true
        ]);

        if (game._isActive && !game._gameEnded) {
          this._startDrawPhase(room, game);
        }
        return;
      }

      // One winner
      if (newRemaining.length === 1 && !game._gameEnded) {
        const winnerId = newRemaining[0];
        const winnerName = game.players.get(winnerId)?.name || winnerId;
        const totalCoin = (game.betAmount || 0) * game.players.size;

        if (game._startedByRecording) {
          await this._addLowCardWinner(room, winnerName);
        }

        this._broadcastToRoom(room, ["gameLowCardWinner", winnerName, totalCoin]);
        game._gameEnded = true;
        game._isActive = false;
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        this._scheduleGameCleanup(room, game);
        return;
      }

      // No remaining players
      if (newRemaining.length === 0) {
        game._isEvaluating = false;
        if (game._safetyTimer) { 
          this._clearTimer(game._safetyTimer); 
          game._safetyTimer = null; 
        }
        game._gameEnded = true;
        game._isActive = false;
        this._broadcastToRoom(room, ["gameLowCardError", "All players eliminated"]);
        this._scheduleGameCleanup(room, game);
        return;
      }

      // Continue to next round
      const numbersArr = entries.map(([id, n]) => `${game.players.get(id)?.name || id}:${n}`);
      const loserNames = losers.map(id => game.players.get(id)?.name || id);
      const remainingNames = newRemaining.map(id => game.players.get(id)?.name || id);

      this._broadcastToRoom(room, ["gameLowCardRoundResult", game.round, numbersArr, loserNames, remainingNames]);

      game.numbers = new Map();
      game.tanda = new Map();
      game.round++;
      game.evaluationLocked = false;
      game.drawTimeExpired = false;
      game._phase = 'draw';
      game._isEvaluating = false;

      if (game._safetyTimer) { 
        this._clearTimer(game._safetyTimer); 
        game._safetyTimer = null; 
      }

      if (game._isActive && !game._gameEnded) {
        this._startDrawPhase(room, game);
      }

    } catch(e) {
      console.error('Evaluate round error:', e);
      game._isEvaluating = false;
      if (game._safetyTimer) { 
        this._clearTimer(game._safetyTimer); 
        game._safetyTimer = null; 
      }
    }
  }

  async _joinGame(ws, username, room) {
    try {
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }

      if (game.players.has(username)) {
        this._safeSend(ws, ["gameLowCardError", "Already in game"]);
        return;
      }

      if (!game.registrationOpen) {
        this._safeSend(ws, ["gameLowCardNoJoin", username, game.betAmount]);
        this._safeSend(ws, ["gameLowCardError", "Registration is closed"]);
        return;
      }

      if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
        this._safeSend(ws, ["gameLowCardError", "Game is full"]);
        return;
      }

      game.players.set(username, { id: username, name: username });
      this._broadcastToRoom(room, ["gameLowCardJoin", username, game.betAmount]);

    } catch(e) {
      console.error('Join game error:', e);
    }
  }

  async _submitNumber(ws, number, tanda, username, room) {
    try {
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }

      if (!game.players.has(username)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game"]);
        return;
      }

      if (game.registrationOpen || game.evaluationLocked || game.drawTimeExpired || game._phase !== 'draw') {
        this._safeSend(ws, ["gameLowCardError", "Cannot submit now"]);
        return;
      }

      if (game.numbers.has(username)) {
        this._safeSend(ws, ["gameLowCardError", "Already submitted"]);
        return;
      }

      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
        return;
      }

      const validTandas = ["C1", "C2", "C3", "C4", ""];
      const t = validTandas.includes(tanda) ? tanda : "";

      game.numbers.set(username, n);
      game.tanda.set(username, t);
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", username, n, t]);

      // Check if all players have submitted
      const activeIds = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired) {
        game.evaluationLocked = true;
        if (game._evalTimer) { 
          this._clearTimer(game._evalTimer); 
          game._evalTimer = null; 
        }
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        const evalTimer = this._trackTimer(setTimeout(() => {
          try {
            const currentGame = this.activeGames.get(room);
            if (currentGame && currentGame === game && currentGame._isActive && !currentGame._gameEnded) {
              this._evaluateRound(room, game);
            }
          } catch(e) {
            console.error('Evaluation timer error:', e);
          }
        }, CONSTANTS.EVALUATION_DELAY_MS));
        game._evalTimer = evalTimer;
      }

    } catch(e) {
      console.error('Submit number error:', e);
    }
  }

  async _leaveGame(ws, username, room) {
    try {
      const game = this.activeGames.get(room);
      if (!game?._isActive || game._gameEnded) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }

      if (!game.players.has(username)) {
        this._safeSend(ws, ["gameLowCardError", "You are not in this game"]);
        return;
      }

      game.eliminated.add(username);
      game.numbers.delete(username);
      game.tanda.delete(username);
      this._broadcastToRoom(room, ["gameLowCardError", `${username} has been eliminated`]);

    } catch(e) {
      console.error('Leave game error:', e);
    }
  }

  // ============================================================
  // ✅ START BOT DRAWS
  // ============================================================
  _startBotDraws(room, game) {
    try {
      if (!game?._isActive || !game.botPlayers) return;
      if (!game._botTimeouts) game._botTimeouts = new Set();
      if (game._botTimeouts.size >= CONSTANTS.MAX_BOT_TIMEOUTS) return;

      const notDrawn = Array.from(game.botPlayers.keys())
        .filter(id => !game.eliminated?.has(id) && !game.numbers?.has(id))
        .slice(0, Math.min(CONSTANTS.MAX_BOT_DRAWS_PER_ROUND, CONSTANTS.MAX_BOT_TIMEOUTS - game._botTimeouts.size));

      for (const botId of notDrawn) {
        const delay = (Math.floor(Math.random() * 14) + 2) * 1000;
        const timeout = this._trackTimer(setTimeout(() => {
          const currentGame = this.activeGames.get(room);
          if (currentGame?._isActive && !currentGame.drawTimeExpired &&
              !currentGame.evaluationLocked && !currentGame.numbers?.has(botId) &&
              !currentGame.eliminated?.has(botId)) {
            this._handleBotDraw(room, botId, currentGame);
          }
          currentGame?._botTimeouts?.delete(timeout);
        }, delay));
        game._botTimeouts.add(timeout);
      }

    } catch(e) {
      console.error('Start bot draws error:', e);
    }
  }

  _handleBotDraw(room, botId, game) {
    try {
      if (!game?._isActive || game.numbers?.has(botId) || game.drawTimeExpired || game.evaluationLocked) return;
      if (game.eliminated?.has(botId)) return;

      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      const botName = game.players.get(botId)?.name || botId;
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);

      const activeIds = Array.from(game.players.keys()).filter(id => !game.eliminated?.has(id));
      if (game.numbers.size === activeIds.length && !game.evaluationLocked && !game.drawTimeExpired && game._isActive) {
        game.evaluationLocked = true;
        this._broadcastToRoom(room, ["gameLowCardWait", "wait results"]);
        const evalTimer = this._trackTimer(setTimeout(() => {
          try { this._evaluateRound(room, game); } catch(e) {}
        }, CONSTANTS.EVALUATION_DELAY_MS));
        game._evalTimer = evalTimer;
      }

    } catch(e) {
      console.error('Handle bot draw error:', e);
    }
  }

  _forceBotDraw(room, botId, game) {
    try {
      if (!game?._isActive || game.numbers?.has(botId)) return;
      if (game.eliminated?.has(botId)) return;
      const number = this._getBotNumberByRound(game.round);
      const tanda = this._getRandomCardTanda();
      game.numbers.set(botId, number);
      game.tanda.set(botId, tanda);
      const botName = game.players.get(botId)?.name || botId;
      this._broadcastToRoom(room, ["gameLowCardPlayerDraw", botName, number, tanda]);

    } catch(e) {
      console.error('Force bot draw error:', e);
    }
  }

  _getBotNumberByRound(round) {
    if (round <= 2) return Math.floor(Math.random() * 12) + 1;
    return Math.random() < 0.6 ?
      [8, 9, 10, 11, 12][Math.floor(Math.random() * 5)] :
      [1, 2, 3, 4, 5, 6, 7][Math.floor(Math.random() * 7)];
  }

  _getRandomCardTanda() {
    return ["C1", "C2", "C3", "C4"][Math.floor(Math.random() * 4)];
  }

  // ============================================================
  // ✅ CHECK STUCK GAMES
  // ============================================================
  _checkStuckGames() {
    try {
      const now = Date.now();
      for (const [room, game] of this.activeGames) {
        if (!game?._isActive || game._gameEnded) continue;

        if (game._phase === 'draw' && game._drawPhaseStart &&
            (now - game._drawPhaseStart) > CONSTANTS.STUCK_DRAW_TIMEOUT_MS) {
          this._closeDrawPhase(room, game);
        }

        if (game._phase === 'registration' && game.registrationOpen &&
            game._createdAt && (now - game._createdAt) > CONSTANTS.STUCK_REGISTRATION_TIMEOUT_MS) {
          this._closeRegistration(room, game);
        }
      }

    } catch(e) {
      console.error('Check stuck games error:', e);
    }
  }

  // ============================================================
  // ✅ SCHEDULE GAME CLEANUP
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
        this.activeGames.delete(room);
        this._gameLocks.delete(room);
        this._joinLocks.delete(room);
        this._broadcastToRoom(room, ["gameLowCardEnd", []]);
      }, CONSTANTS.GAME_CLEANUP_DELAY_MS));
      this._cleanupTimers.set(room, timer);

    } catch(e) {
      console.error('Schedule game cleanup error:', e);
    }
  }

  // ============================================================
  // ✅ RECORDING WINNERS
  // ============================================================
  async _addLowCardWinner(room, username) {
    try {
      if (!room || !username) return false;
      if (!this.recordingEnabled.get(room)) return false;

      if (!this.lowcardWinners.has(room)) {
        this.lowcardWinners.set(room, {});
      }
      const winners = this.lowcardWinners.get(room);
      winners[username] = (winners[username] || 0) + 1;

      this._broadcastToRoom(room, ["lowCardWinnerUpdate", { winners, room }]);
      return true;

    } catch(e) {
      console.error('Add lowcard winner error:', e);
      return false;
    }
  }

  // ============================================================
  // ✅ SEND GAME STATE
  // ============================================================
  _sendGameState(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room) return;
      const game = this.activeGames.get(room);
      if (!game || !game._isActive || game._gameEnded) {
        this._safeSend(ws, ["gameState", { room, hasGame: false }]);
        return;
      }

      const activePlayers = Array.from(game.players.values())
        .filter(p => !game.eliminated?.has(p.id))
        .map(p => p.name);
      const allPlayers = Array.from(game.players.values()).map(p => p.name);
      const eliminated = Array.from(game.eliminated || []);
      const submitted = Array.from(game.numbers?.keys() || []);

      this._safeSend(ws, ["gameState", {
        room,
        hasGame: true,
        gameType: 'lowcard',
        isActive: game._isActive,
        phase: game._phase || 'registration',
        round: game.round || 1,
        bet: game.betAmount || 0,
        host: game.hostName || 'Unknown',
        registrationOpen: game.registrationOpen || false,
        players: allPlayers,
        activePlayers: activePlayers,
        eliminated: eliminated,
        submitted: submitted,
        playerCount: game.players.size,
        activeCount: activePlayers.length,
        isEvaluating: game._isEvaluating || false,
        evaluationLocked: game.evaluationLocked || false,
        drawTimeExpired: game.drawTimeExpired || false
      }]);

    } catch(e) {
      console.error('Send game state error:', e);
    }
  }

  // ============================================================
  // ✅ SEND DICE STATUS
  // ============================================================
  _sendDiceStatus(ws) {
    try {
      if (!ws || ws.readyState !== 1) return;
      const isActive = !!this.currentDiceRoll && this._canSubmitDiceAnswer;
      this._safeSend(ws, ["diceStatus", isActive, this._diceRound || 1]);
    } catch(e) {
      console.error('Send dice status error:', e);
    }
  }

  // ============================================================
  // ✅ UTILITY METHODS
  // ============================================================
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

      const msgStr = JSON.stringify(message);
      const wsIdArray = Array.from(wsIds);

      for (const wsId of wsIdArray) {
        const ws = this.wsMap.get(wsId);
        if (ws && ws.readyState === 1 && !ws._closing) {
          try { ws.send(msgStr); } catch(e) {}
        }
      }

    } catch(e) {
      console.error('Broadcast error:', e);
    }
  }

  _trackTimer(timer) {
    if (timer) this._allTimers.add(timer);
    return timer;
  }

  _clearTimer(timer) {
    if (timer) {
      clearTimeout(timer);
      clearInterval(timer);
      this._allTimers.delete(timer);
    }
  }

  // ============================================================
  // ✅ DESTROY
  // ============================================================
  async destroy() {
    try {
      if (this.isDestroyed) return;
      this.isDestroyed = true;
      this.closing = true;

      for (const timer of this._allTimers) {
        try { clearTimeout(timer); clearInterval(timer); } catch(e) {}
      }
      this._allTimers.clear();

      if (this._mainInterval) { clearInterval(this._mainInterval); this._mainInterval = null; }
      if (this._cleanupInterval) { clearInterval(this._cleanupInterval); this._cleanupInterval = null; }

      if (this._diceTimerInterval) {
        clearInterval(this._diceTimerInterval);
        this._diceTimerInterval = null;
      }

      this._clearTimer(this._diceTimeout);
      this._clearTimer(this._diceTimeUpCooldownTimer);

      for (const [wsId, ws] of this.wsMap) {
        try { 
          if (ws && ws.readyState === 1) {
            ws.close(1000, "Server shutting down"); 
          }
        } catch(e) {}
      }

      this.wsMap.clear();
      this.wsClients.clear();
      this.clientRooms.clear();

      console.log('🛑 GameServer destroyed');

    } catch(e) {
      console.error('Destroy error:', e);
    }
  }
}
