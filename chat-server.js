// ==================== CHAT-SERVER.JS ====================
// VERSION: 8.0.0 - PURE WORKER (NO DO, NO KV)
// SEMUA USER DI ROOM BISA SALING LIHAT

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 300,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_UPDATE_TIK: 6,
  MAX_NUMBER: 6,
  BATCH_SIZE: 10,
  LOCK_TIMEOUT: 5000,
  CLEANUP_INTERVAL: 60000,
  MAX_EVENT_QUEUE: 50,
  MAX_PROCESS_TIME_MS: 50,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();
    this.points = new Map();
    this.muted = false;
    this.number = 1;
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= C.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addSeat(userId, noimageUrl, color, itembawah, itematas, vip, viptanda) {
    if (!userId) return null;
    
    for (const [seat, data] of this.seats) {
      if (data && data.namauser === userId) return seat;
    }
    
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    
    this.seats.set(seat, {
      noimageUrl: noimageUrl || "",
      namauser: userId,
      color: color || "",
      itembawah: itembawah || 0,
      itematas: itematas || 0,
      vip: vip || 0,
      viptanda: viptanda || 0,
    });
    return seat;
  }

  updateSeat(seat, data) {
    if (!this.seats.has(seat) || !data) return false;
    
    this.seats.set(seat, {
      noimageUrl: data.noimageUrl || "",
      namauser: data.namauser || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0
    });
    return true;
  }

  removeSeat(seat) {
    this.points.delete(seat);
    return this.seats.delete(seat);
  }
  
  getSeat(seat) { 
    const data = this.seats.get(seat);
    return data ? { ...data } : null;
  }
  
  getCount() { return this.seats.size; }
  
  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      if (data) result[seat] = { ...data };
    }
    return result;
  }

  setMuted(val) { 
    this.muted = !!val; 
    return this.muted; 
  }
  
  getMuted() { return this.muted; }
  
  setNumber(n) { 
    this.number = n || 1; 
  }
  getNumber() { return this.number; }

  updatePoint(seat, x, y, fast) {
    if (!this.seats.has(seat)) return false;
    this.points.set(seat, { x: x || 0, y: y || 0, fast: !!fast });
    return true;
  }

  getPoint(seat) { 
    const point = this.points.get(seat);
    return point ? { ...point } : null;
  }
  
  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      if (this.seats.has(seat) && point) {
        result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
      }
    }
    return result;
  }
}

// ==================== CHAT SERVER CLASS ====================
export class ChatServer {
  constructor(deployVersion) {
    this._deployVersion = deployVersion || Date.now();
    this._lastDeployVersion = null;
    
    // ========== STATE ==========
    this.rooms = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.wsActiveMulti = new Map();
    this.wsSet = new Set();
    this.wsIdCounter = 0;
    this.currentNumber = 1;
    this.tikCounter = 0;
    this.joinLocks = new Map();
    this.kursiLocks = new Map();
    this._processingMessages = new Set();
    this._cleaningUp = new Set();
    this._eventQueue = [];
    this._isProcessingQueue = false;
    this._startTime = Date.now();
    this._intervals = [];
    this._started = false;
    this._stats = { messages: 0 };
    
    // ========== INIT ROOMS ==========
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
  }

  // ==================== CHECK DEPLOY VERSION ====================
  _checkDeployVersion() {
    if (this._lastDeployVersion !== this._deployVersion) {
      this._lastDeployVersion = this._deployVersion;
      this._resetState();
      return true;
    }
    return false;
  }

  // ==================== RESET STATE ====================
  _resetState() {
    for (const interval of this._intervals) {
      clearInterval(interval);
    }
    this._intervals = [];
    this._started = false;
    
    for (const ws of this.wsSet) {
      try {
        if (ws.readyState === 1) {
          ws.close(1000, "Server reset");
        }
      } catch(e) {}
    }
    
    this.wsSet.clear();
    this.rooms.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this.userConnections.clear();
    this.roomClients.clear();
    this.wsActiveMulti.clear();
    this.joinLocks.clear();
    this.kursiLocks.clear();
    this._eventQueue = [];
    this._processingMessages.clear();
    this._cleaningUp.clear();
    this.wsIdCounter = 0;
    this.currentNumber = 1;
    this.tikCounter = 0;
    this._startTime = Date.now();
    this._stats.messages = 0;
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
  }

  // ==================== START BACKGROUND TASKS ====================
  start() {
    this._checkDeployVersion();
    if (this._started) return;
    this._started = true;
    
    this._intervals.push(setInterval(() => {
      this._cleanupDeadConnections();
    }, C.CLEANUP_INTERVAL));

    this._intervals.push(setInterval(() => {
      this._cleanupStaleLocks();
    }, 30000));

    this._intervals.push(setInterval(() => {
      this._updateNumber();
    }, 900000));
  }

  // ==================== UPDATE NUMBER ====================
  _updateNumber() {
    this.tikCounter++;
    
    if (this.tikCounter >= C.NUMBER_UPDATE_TIK) {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      for (const room of this.rooms.values()) {
        if (room) room.setNumber(this.currentNumber);
      }
      
      const msg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const room of ROOMS) {
        this._broadcastToRoom(room, msg);
      }
      
      this.tikCounter = 0;
    }
  }

  // ==================== CLEANUP DEAD CONNECTIONS ====================
  _cleanupDeadConnections() {
    const toRemove = [];
    for (const [wsId, conn] of this.userConnections) {
      if (!conn || conn._closing) {
        toRemove.push(wsId);
      }
    }
    for (const wsId of toRemove) {
      this._cleanup(wsId);
    }
  }

  // ==================== CLEANUP STALE LOCKS ====================
  _cleanupStaleLocks() {
    const now = Date.now();
    for (const [key, time] of this.joinLocks) {
      if (now - time > C.LOCK_TIMEOUT) {
        this.joinLocks.delete(key);
      }
    }
    for (const [key, time] of this.kursiLocks) {
      if (now - time > C.LOCK_TIMEOUT) {
        this.kursiLocks.delete(key);
      }
    }
  }

  // ==================== MAIN FETCH ====================
  async fetch(request) {
    this._checkDeployVersion();
    this.start();
    
    const url = new URL(request.url);
    const pathname = url.pathname;
    
    if (pathname === "/ws" || pathname === "/chat" || pathname === "/") {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade === "websocket") {
        return this._handleWebSocket(request);
      }
      return new Response("Chat Server", { 
        status: 200,
        headers: { "Cache-Control": "no-cache" }
      });
    }
    
    if (pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        deployVersion: this._deployVersion,
        connections: this.wsSet.size,
        rooms: this.rooms.size,
        users: this.userSeat.size,
        messages: this._stats?.messages || 0,
        uptime: Math.floor((Date.now() - this._startTime) / 1000)
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }
    
    return new Response("Chat Server", { status: 200 });
  }

  // ==================== WEBSOCKET HANDLER ====================
  _handleWebSocket(request) {
    if (this.wsSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }
    
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    
    try {
      server.accept();
    } catch(e) {
      return new Response("WebSocket acceptance failed", { status: 500 });
    }
    
    const wsId = ++this.wsIdCounter;
    
    server._wsId = wsId;
    server.username = null;
    server.room = null;
    server.roomname = null;
    server.idtarget = null;
    server._closing = false;
    
    this.wsSet.add(server);
    
    server.addEventListener("message", async (event) => {
      try {
        if (server._closing) return;
        await this._handleMessage(server, event.data);
      } catch(e) {}
    });
    
    server.addEventListener("close", () => {
      if (server._closing) return;
      this._cleanup(server);
    });
    
    server.addEventListener("error", () => {
      if (server._closing) return;
      this._cleanup(server);
    });
    
    return new Response(null, { status: 101, webSocket: client });
  }

  // ==================== HANDLE MESSAGE ====================
  async _handleMessage(ws, raw) {
    if (!ws) return;
    if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws)) return;
    
    if (this._processingMessages.has(ws)) return;
    this._processingMessages.add(ws);
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MESSAGE_SIZE) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      if (evt === "chat" || evt === "updatePoint" || evt === "gift" || evt === "rollangak") {
        const room = args[0];
        if (room && !ROOMS_SET.has(room)) return;
      }
      
      if (this._eventQueue.length < C.MAX_EVENT_QUEUE) {
        this._eventQueue.push({ ws, data: [evt, ...args] });
        if (!this._isProcessingQueue) {
          this._processEventQueue();
        }
      }
      
    } catch(e) {} finally {
      this._processingMessages.delete(ws);
    }
  }

  // ==================== PROCESS EVENT QUEUE ====================
  _processEventQueue() {
    if (this._isProcessingQueue || this._eventQueue.length === 0) return;
    this._isProcessingQueue = true;
    
    const startTime = Date.now();
    let processed = 0;
    
    while (this._eventQueue.length > 0 && processed < 5) {
      if (Date.now() - startTime > C.MAX_PROCESS_TIME_MS) break;
      
      const item = this._eventQueue.shift();
      try {
        this._handleEventInternal(item.ws, item.data);
      } catch(e) {}
      processed++;
    }
    
    this._isProcessingQueue = false;
  }

  // ==================== HANDLE EVENT INTERNAL ====================
  _handleEventInternal(ws, data) {
    if (!ws || !data || !data[0]) return;
    const [evt, ...args] = data;
    
    switch(evt) {
      case "setIdTarget2":
        this._handleSetId(ws, args[0], args[1]);
        break;
      
      case "joinRoom":
        this._handleJoin(ws, args[0]);
        break;
      
      case "multiJoin":
        this._handleMultiJoin(ws, args[0], args[1]);
        break;
      
      case "exitMulti":
        this._handleExitMulti(ws, args[0]);
        break;
      
      case "setActiveMulti":
        this._handleSetActiveMulti(ws, args[0]);
        break;
      
      case "updateKursi":
        this._handleUpdateKursi(ws, args);
        break;
      
      case "chat":
        this._handleChat(ws, args);
        break;
      
      case "updatePoint":
        this._handleUpdatePoint(ws, args);
        break;
      
      case "gift":
        this._handleGift(ws, args);
        break;
      
      case "rollangak":
        this._handleRollAngak(ws, args);
        break;
      
      case "private":
        this._handlePrivate(ws, args);
        break;
      
      case "sendnotif":
        this._handleSendNotif(ws, args);
        break;
      
      case "removeKursiAndPoint":
        this._handleRemoveKursi(ws, args);
        break;
      
      case "setMuteType":
        this._handleSetMute(ws, args);
        break;
      
      case "getMuteType":
        this._handleGetMute(ws, args[0]);
        break;
      
      case "getCurrentNumber":
        this._safeSend(ws, ["currentNumber", this.currentNumber]);
        break;
      
      case "isUserOnline":
        this._handleIsUserOnline(ws, args[0], args[1]);
        break;
      
      case "getOnlineUsers":
        this._handleGetOnlineUsers(ws);
        break;
      
      case "getAllRoomsUserCount":
        this._handleGetAllRoomsUserCount(ws);
        break;
      
      case "getRoomUserCount":
        this._handleGetRoomUserCount(ws, args[0]);
        break;
      
      case "modwarning":
        this._handleModWarning(ws, args);
        break;
      
      case "onDestroy":
        this._cleanup(ws);
        break;
      
      default:
        this._safeSend(ws, ["error", `Unknown event: ${evt}`]);
        break;
    }
  }

  // ==================== HANDLE SET ID ====================
  _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    
    if (ws.readyState !== 1) {
      try { this._cleanup(ws); } catch(e) {}
      return;
    }
    
    const existingSeatInfo = this.userSeat.get(username);
    if (existingSeatInfo?.isMulti === true && isNewUser === false) {
      try {
        const oldConnections = this.userConnections.get(username);
        if (oldConnections && oldConnections.ws) {
          if (oldConnections.ws.readyState !== 1) {
            this.userConnections.delete(username);
          }
        }
        
        let connections = this.userConnections.get(username);
        if (!connections) {
          connections = { ws, isMulti: true };
          this.userConnections.set(username, connections);
        } else {
          connections.ws = ws;
        }
        
        if (!this.wsSet.has(ws)) {
          this.wsSet.add(ws);
        }
        
        ws.username = username;
        ws.idtarget = username;
        ws.room = null;
        ws.roomname = null;
        ws._closing = false;
        
        this._safeSend(ws, ["multiUserActive", username]);
      } catch(e) {}
      return;
    }
    
    try {
      let existingSeatInfo2 = this.userSeat.get(username);
      
      if (!existingSeatInfo2) {
        for (const [roomName, roomMan] of this.rooms) {
          if (!roomMan) continue;
          for (const [seat, seatData] of roomMan.seats) {
            if (seatData?.namauser === username) {
              existingSeatInfo2 = { room: roomName, seat: seat, isMulti: false };
              this.userSeat.set(username, existingSeatInfo2);
              this.userRoom.set(username, roomName);
              break;
            }
          }
          if (existingSeatInfo2) break;
        }
      }
      
      if (existingSeatInfo2) {
        try {
          const oldRoom = existingSeatInfo2.room;
          const oldSeat = existingSeatInfo2.seat;
          const oldRoomMan = this.rooms.get(oldRoom);
          if (oldRoomMan) {
            const seatData = oldRoomMan.getSeat(oldSeat);
            if (seatData?.namauser === username) {
              oldRoomMan.removeSeat(oldSeat);
              this._broadcastToRoom(oldRoom, JSON.stringify(["removeKursi", oldRoom, oldSeat]));
              this._updateRoomCount(oldRoom);
            }
          }
          this.userSeat.delete(username);
          this.userRoom.delete(username);
        } catch(e) {}
      }
      
      try {
        for (const [roomName, roomMan] of this.rooms) {
          if (!roomMan) continue;
          let found = false;
          for (const [seat, seatData] of roomMan.seats) {
            if (seatData?.namauser === username) {
              roomMan.removeSeat(seat);
              this._broadcastToRoom(roomName, JSON.stringify(["removeKursi", roomName, seat]));
              this._updateRoomCount(roomName);
              found = true;
              break;
            }
          }
          if (found) break;
        }
      } catch(e) {}
      
      try {
        this.userSeat.delete(username);
        this.userRoom.delete(username);
      } catch(e) {}
      
      try {
        ws.username = username;
        ws.idtarget = username;
        ws.room = null;
        ws.roomname = null;
        ws._closing = false;
        
        let connections = this.userConnections.get(username);
        if (!connections) {
          connections = { ws, isMulti: false };
          this.userConnections.set(username, connections);
        } else {
          connections.ws = ws;
        }
        
        if (!this.wsSet.has(ws)) {
          this.wsSet.add(ws);
        }
      } catch(e) {}
      
      try {
        if (isNewUser) {
          this._safeSend(ws, ["joinroomawal"]);
        } else {
          this._safeSend(ws, ["needJoinRoom"]);
        }
      } catch(e) {}
      
    } catch(e) {}
  }

  // ==================== HANDLE JOIN ====================
  _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName)) {
      return false;
    }
    
    const username = ws.username;
    const lockKey = `join_${roomName}_${username}`;
    
    if (this.joinLocks.has(lockKey)) {
      this._safeSend(ws, ["roomFull", roomName]);
      return false;
    }
    
    this.joinLocks.set(lockKey, Date.now());
    
    try {
      const oldRoom = ws.room;
      
      if (oldRoom && oldRoom !== roomName) {
        try {
          const oldMan = this.rooms.get(oldRoom);
          if (oldMan) {
            const oldSeat = this.userSeat.get(username)?.seat;
            if (oldSeat) {
              oldMan.removeSeat(oldSeat);
              this._broadcastToRoom(oldRoom, JSON.stringify(["removeKursi", oldRoom, oldSeat]));
              this._updateRoomCount(oldRoom);
            }
          }
          const oldClients = this.roomClients.get(oldRoom);
          if (oldClients) oldClients.delete(ws);
          this.userSeat.delete(username);
          this.userRoom.delete(username);
        } catch(e) {}
        ws.room = null;
        ws.roomname = null;
      }
      
      const roomMan = this.rooms.get(roomName);
      if (!roomMan) return false;
      
      let seat = null;
      for (const [s, data] of roomMan.seats) {
        if (data?.namauser === username) {
          seat = s;
          break;
        }
      }
      
      if (!seat) {
        if (roomMan.getCount() >= C.MAX_SEATS) {
          this._safeSend(ws, ["roomFull", roomName]);
          return false;
        }
        seat = roomMan.getAvailableSeat();
        if (!seat) {
          this._safeSend(ws, ["roomFull", roomName]);
          return false;
        }
        roomMan.addSeat(username, "", "", 0, 0, 0, 0);
      }
      
      try {
        this.userSeat.set(username, { room: roomName, seat, isMulti: false });
        this.userRoom.set(username, roomName);
        ws.room = roomName;
        ws.roomname = roomName;
        ws.idtarget = username;
        
        // ✅ PASTIKAN USER MASUK KE roomClients
        const roomClients = this.roomClients.get(roomName);
        if (roomClients && !roomClients.has(ws)) {
          roomClients.add(ws);
        }
        
        this._safeSend(ws, ["rooMasuk", seat, roomName]);
        this._safeSend(ws, ["numberKursiSaya", seat]);
        this._safeSend(ws, ["muteTypeResponse", roomMan.getMuted(), roomName]);
        this._safeSend(ws, ["roomUserCount", roomName, roomMan.getCount()]);
        
        this._updateRoomCount(roomName);
        
        // 🔥 BROADCAST USER JOIN KE SEMUA USER DI ROOM
        this._broadcastToRoom(roomName, JSON.stringify([
          "userJoinedRoom", username, roomName
        ]));
        
        // 🔥 SEND ALL STATE KE USER BARU
        setTimeout(() => {
          try {
            if (ws && ws.readyState === 1) {
              this._sendAllState(ws, roomName, true);
            }
          } catch(e) {}
        }, 500);
        
      } catch(e) {}
      return true;
      
    } finally {
      this.joinLocks.delete(lockKey);
    }
  }

  // ==================== HANDLE MULTI JOIN ====================
  _handleMultiJoin(ws, multiUsername, multiRoomname) {
    if (!multiUsername || !multiRoomname || !ROOMS_SET.has(multiRoomname)) {
      this._safeSend(ws, ["error", "Invalid room"]);
      return;
    }
    
    try {
      let existingSeat = null, existingRoom = null;
      for (const [roomName, roomMan] of this.rooms) {
        if (!roomMan) continue;
        for (const [seat, seatData] of roomMan.seats) {
          if (seatData?.namauser === multiUsername) {
            existingSeat = seat;
            existingRoom = roomName;
            break;
          }
        }
        if (existingSeat) break;
      }
      
      if (existingSeat && existingRoom) {
        const oldRoomMan = this.rooms.get(existingRoom);
        if (oldRoomMan) {
          oldRoomMan.removeSeat(existingSeat);
          this._broadcastToRoom(existingRoom, JSON.stringify(["removeKursi", existingRoom, existingSeat]));
          this._updateRoomCount(existingRoom);
        }
        this.userSeat.delete(multiUsername);
        this.userRoom.delete(multiUsername);
      }
    } catch(e) {}
    
    const roomMan = this.rooms.get(multiRoomname);
    if (!roomMan || roomMan.getCount() >= C.MAX_SEATS) {
      this._safeSend(ws, ["error", "Room full"]);
      return;
    }
    
    const seat = roomMan.addSeat(multiUsername, "", "", 0, 0, 0, 0);
    if (!seat) {
      this._safeSend(ws, ["error", "Room full"]);
      return;
    }
    
    try {
      this.userSeat.set(multiUsername, { room: multiRoomname, seat, isMulti: true });
      this.userRoom.set(multiUsername, multiRoomname);
      
      let connections = this.userConnections.get(multiUsername);
      if (!connections) {
        connections = { ws, isMulti: true };
        this.userConnections.set(multiUsername, connections);
      }
      
      this.wsActiveMulti.set(ws, { username: multiUsername, room: multiRoomname });
      const roomClients = this.roomClients.get(multiRoomname);
      if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
      
      this._safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
      this._broadcastToRoom(multiRoomname, JSON.stringify(["roomUserCount", multiRoomname, roomMan.getCount()]));
      
      // 🔥 BROADCAST USER JOIN KE SEMUA USER DI ROOM
      this._broadcastToRoom(multiRoomname, JSON.stringify([
        "userJoinedRoom", multiUsername, multiRoomname
      ]));
      
    } catch(e) {}
  }

  // ==================== HANDLE EXIT MULTI ====================
  _handleExitMulti(ws, targetUsername) {
    if (!targetUsername) return;
    
    try {
      const seatInfo = this.userSeat.get(targetUsername);
      if (!seatInfo) return;
      
      const roomName = seatInfo.room;
      const seatNumber = seatInfo.seat;
      
      const activeData = this.wsActiveMulti.get(ws);
      if (activeData?.username === targetUsername) {
        const roomClients = this.roomClients.get(roomName);
        if (roomClients) roomClients.delete(ws);
        this.wsActiveMulti.delete(ws);
      }
      
      const roomMan = this.rooms.get(roomName);
      if (roomMan) {
        roomMan.removeSeat(seatNumber);
        this._broadcastToRoom(roomName, JSON.stringify(["removeKursi", roomName, seatNumber]));
        this._broadcastToRoom(roomName, JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
        
        // 🔥 BROADCAST USER LEFT KE SEMUA USER DI ROOM
        this._broadcastToRoom(roomName, JSON.stringify([
          "userLeftRoom", targetUsername, roomName
        ]));
      }
      
      this.userSeat.delete(targetUsername);
      this.userRoom.delete(targetUsername);
      
      const connections = this.userConnections.get(targetUsername);
      if (connections) {
        this.userConnections.delete(targetUsername);
      }
      
      if (ws.username === targetUsername) {
        ws.username = null;
        ws.idtarget = null;
      }
    } catch(e) {}
  }

  // ==================== HANDLE SET ACTIVE MULTI ====================
  _handleSetActiveMulti(ws, targetUsername) {
    if (!targetUsername) return;
    
    try {
      const seatInfo = this.userSeat.get(targetUsername);
      if (!seatInfo) return;
      
      const roomName = seatInfo.room;
      const seatNumber = seatInfo.seat;
      
      const oldActive = this.wsActiveMulti.get(ws);
      if (oldActive?.room) {
        const oldClients = this.roomClients.get(oldActive.room);
        if (oldClients) oldClients.delete(ws);
      }
      
      this.wsActiveMulti.set(ws, { username: targetUsername, room: roomName });
      const roomClients = this.roomClients.get(roomName);
      if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
      
      ws.username = targetUsername;
      ws.idtarget = targetUsername;
      ws.room = roomName;
      ws.roomname = roomName;
      
      this._safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
      this._broadcastToRoom(roomName, JSON.stringify(["userActiveChanged", targetUsername, seatNumber]));
    } catch(e) {}
  }

  // ==================== HANDLE UPDATE KURSI ====================
  _handleUpdateKursi(ws, args) {
    try {
      const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
      
      const roomMan = this.rooms.get(kursiRoom);
      if (!roomMan) return;
      
      const lockKey = `kursi_${kursiRoom}_${kursiSeat}`;
      if (this.kursiLocks.has(lockKey)) return;
      
      this.kursiLocks.set(lockKey, Date.now());
      
      try {
        const updated = roomMan.updateSeat(kursiSeat, {
          noimageUrl: kursiNoimg || "",
          namauser: kursiName || "",
          color: kursiColor || "",
          itembawah: kursiBawah || 0,
          itematas: kursiAtas || 0,
          vip: kursiVip || 0,
          viptanda: kursiVt || 0
        });
        
        if (updated) {
          const updatedSeat = roomMan.getSeat(kursiSeat);
          this._broadcastToRoom(kursiRoom, JSON.stringify(["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]));
        }
      } finally {
        this.kursiLocks.delete(lockKey);
      }
    } catch(e) {}
  }

  // ==================== HANDLE CHAT ====================
  _handleChat(ws, args) {
    try {
      const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
      
      if (!chatMsg || !chatRoom || !ROOMS_SET.has(chatRoom)) return;
      
      // 🔥 CEK APAKAH ADA USER DI ROOM
      const clients = this.roomClients.get(chatRoom);
      if (!clients || clients.size === 0) return;
      
      // 🔥 BROADCAST CHAT KE SEMUA USER DI ROOM
      this._broadcastToRoom(chatRoom, JSON.stringify([
        "chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor
      ]));
      
    } catch(e) {}
  }

  // ==================== HANDLE UPDATE POINT ====================
  _handleUpdatePoint(ws, args) {
    try {
      const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
      
      if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
        const roomMan = this.rooms.get(pointRoom);
        if (roomMan && roomMan.seats.has(pointSeat)) {
          if (roomMan.updatePoint(pointSeat, pointX, pointY, pointFast === 1)) {
            this._broadcastToRoom(pointRoom, JSON.stringify(["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]));
          }
        }
      }
    } catch(e) {}
  }

  // ==================== HANDLE GIFT ====================
  _handleGift(ws, args) {
    try {
      const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
      
      if (giftRoom && ROOMS_SET.has(giftRoom)) {
        const clients = this.roomClients.get(giftRoom);
        if (!clients || clients.size === 0) return;
        this._broadcastToRoom(giftRoom, JSON.stringify(["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]));
      }
    } catch(e) {}
  }

  // ==================== HANDLE ROLL ANGAK ====================
  _handleRollAngak(ws, args) {
    try {
      const [rollRoom, rollUser, rollAngka] = args;
      
      if (rollRoom && ROOMS_SET.has(rollRoom)) {
        const clients = this.roomClients.get(rollRoom);
        if (!clients || clients.size === 0) return;
        this._broadcastToRoom(rollRoom, JSON.stringify(["rollangakBroadcast", rollRoom, rollUser, rollAngka]));
      }
    } catch(e) {}
  }

  // ==================== HANDLE PRIVATE ====================
  _handlePrivate(ws, args) {
    try {
      const [privTarget, privNoimg, privMsg, privSender] = args;
      
      if (privTarget && privMsg) {
        const targetConns = this.userConnections.get(privTarget);
        if (targetConns && targetConns.ws) {
          if (targetConns.ws.readyState === 1) {
            this._safeSend(targetConns.ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
          }
        }
        this._safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
      }
    } catch(e) {}
  }

  // ==================== HANDLE SEND NOTIF ====================
  _handleSendNotif(ws, args) {
    try {
      const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
      
      if (notifTarget && notifMsg) {
        const targetConns = this.userConnections.get(notifTarget);
        if (targetConns && targetConns.ws && targetConns.ws.readyState === 1) {
          this._safeSend(targetConns.ws, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
        }
      }
    } catch(e) {}
  }

  // ==================== HANDLE REMOVE KURSI ====================
  _handleRemoveKursi(ws, args) {
    try {
      const [removeRoom, removeSeat] = args;
      
      const roomMan = this.rooms.get(removeRoom);
      if (!roomMan || !roomMan.seats.has(removeSeat)) return;
      
      for (const [username, info] of this.userSeat) {
        if (info.seat === removeSeat && info.room === removeRoom) {
          this.userSeat.delete(username);
          this.userRoom.delete(username);
          break;
        }
      }
      
      roomMan.removeSeat(removeSeat);
      this._broadcastToRoom(removeRoom, JSON.stringify(["removeKursi", removeRoom, removeSeat]));
      this._updateRoomCount(removeRoom);
      
      this._broadcastToRoom(removeRoom, JSON.stringify([
        "userLeftRoom", "Unknown", removeRoom
      ]));
    } catch(e) {}
  }

  // ==================== HANDLE SET MUTE ====================
  _handleSetMute(ws, args) {
    try {
      const [muteVal, muteRoom] = args;
      
      if (!muteRoom || !ROOMS_SET.has(muteRoom)) return;
      
      const rm = this.rooms.get(muteRoom);
      if (!rm) return;
      
      rm.setMuted(muteVal);
      this._broadcastToRoom(muteRoom, JSON.stringify(["muteStatusChanged", !!muteVal, muteRoom]));
      this._safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
    } catch(e) {}
  }

  // ==================== HANDLE GET MUTE ====================
  _handleGetMute(ws, muteRoom) {
    try {
      if (muteRoom && ROOMS_SET.has(muteRoom)) {
        const rm = this.rooms.get(muteRoom);
        this._safeSend(ws, ["muteTypeResponse", rm?.getMuted() || false, muteRoom]);
      }
    } catch(e) {}
  }

  // ==================== HANDLE MOD WARNING ====================
  _handleModWarning(ws, args) {
    try {
      const modRoom = args[0];
      if (modRoom && ROOMS_SET.has(modRoom)) {
        this._broadcastToRoom(modRoom, JSON.stringify(["modwarning", modRoom]));
      }
    } catch(e) {}
  }

  // ==================== HANDLE IS USER ONLINE ====================
  _handleIsUserOnline(ws, onlineTarget, onlineCallback) {
    try {
      let isOnline = false;
      const seatInfo = this.userSeat.get(onlineTarget);
      
      if (seatInfo?.seat) {
        if (seatInfo.isMulti) {
          isOnline = true;
        } else {
          const connections = this.userConnections.get(onlineTarget);
          if (connections && connections.ws && connections.ws.readyState === 1) {
            isOnline = true;
          }
        }
      }
      
      this._safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
    } catch(e) {}
  }

  // ==================== HANDLE GET ONLINE USERS ====================
  _handleGetOnlineUsers(ws) {
    try {
      const users = [];
      for (const [username, seatInfo] of this.userSeat) {
        if (seatInfo?.seat) {
          if (seatInfo.isMulti) {
            users.push(username);
          } else {
            const connections = this.userConnections.get(username);
            if (connections && connections.ws && connections.ws.readyState === 1) {
              users.push(username);
            }
          }
        }
      }
      this._safeSend(ws, ["allOnlineUsers", users]);
    } catch(e) {}
  }

  // ==================== HANDLE GET ALL ROOMS USER COUNT ====================
  _handleGetAllRoomsUserCount(ws) {
    try {
      const counts = {};
      for (const room of ROOMS) {
        const rm = this.rooms.get(room);
        counts[room] = rm?.getCount() || 0;
      }
      this._safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
    } catch(e) {}
  }

  // ==================== HANDLE GET ROOM USER COUNT ====================
  _handleGetRoomUserCount(ws, roomName) {
    try {
      if (roomName && ROOMS_SET.has(roomName)) {
        const rm = this.rooms.get(roomName);
        this._safeSend(ws, ["roomUserCount", roomName, rm?.getCount() || 0]);
      }
    } catch(e) {}
  }

  // ==================== CLEANUP ====================
  _cleanup(ws) {
    if (!ws || ws._cleaning || this._cleaningUp.has(ws)) return;
    
    ws._cleaning = true;
    this._cleaningUp.add(ws);
    
    try {
      const username = ws.username;
      const room = ws.room;
      
      if (room) {
        try {
          const clients = this.roomClients.get(room);
          if (clients) clients.delete(ws);
        } catch(e) {}
      }
      
      try {
        const activeData = this.wsActiveMulti.get(ws);
        if (activeData?.room) {
          const clients = this.roomClients.get(activeData.room);
          if (clients) clients.delete(ws);
        }
        this.wsActiveMulti.delete(ws);
      } catch(e) {}
      
      if (username) {
        try {
          const connections = this.userConnections.get(username);
          if (connections) {
            if (connections.ws === ws) {
              connections.ws = null;
            }
            
            const seatInfo = this.userSeat.get(username);
            const isMulti = seatInfo?.isMulti === true;
            
            if (!isMulti && (!connections.ws || connections.ws.readyState !== 1)) {
              this.userConnections.delete(username);
              if (seatInfo?.room) {
                const roomMan = this.rooms.get(seatInfo.room);
                if (roomMan) {
                  try {
                    const seatData = roomMan.getSeat(seatInfo.seat);
                    if (seatData?.namauser === username) {
                      roomMan.removeSeat(seatInfo.seat);
                      this._broadcastToRoom(seatInfo.room, JSON.stringify(["removeKursi", seatInfo.room, seatInfo.seat]));
                      this._updateRoomCount(seatInfo.room);
                      this._broadcastToRoom(seatInfo.room, JSON.stringify([
                        "userLeftRoom", username, seatInfo.room
                      ]));
                    }
                  } catch(e) {}
                }
              }
              this.userSeat.delete(username);
              this.userRoom.delete(username);
            }
          }
        } catch(e) {}
      }
      
      try {
        this.wsSet.delete(ws);
      } catch(e) {}
      
    } catch(e) {} finally {
      ws._cleaning = false;
      this._cleaningUp.delete(ws);
      
      try {
        if (ws && ws.readyState === 1) {
          ws.close(1000, "Cleanup");
        }
      } catch(e) {}
    }
  }

  // ==================== BROADCAST HELPERS ====================

  _broadcastToRoom(room, msgStr) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const clientArray = Array.from(clients);
    const toRemove = [];
    
    for (let i = 0; i < clientArray.length; i += C.BATCH_SIZE) {
      const batch = clientArray.slice(i, Math.min(i + C.BATCH_SIZE, clientArray.length));
      
      for (const ws of batch) {
        if (!ws) {
          toRemove.push(ws);
          continue;
        }
        
        try {
          if (ws.readyState === 1 && !ws._closing && !this._cleaningUp.has(ws)) {
            ws.send(msgStr);
          } else {
            toRemove.push(ws);
          }
        } catch(e) {
          toRemove.push(ws);
        }
      }
    }
    
    if (toRemove.length > 0) {
      for (const ws of toRemove) {
        try {
          clients.delete(ws);
          if (ws && !this._cleaningUp.has(ws)) {
            this._cleanup(ws);
          }
        } catch(e) {}
      }
    }
  }

  _safeSend(ws, msg) {
    if (!ws) return false;
    try {
      if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws)) {
        return false;
      }
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      this._cleanup(ws);
      return false;
    }
  }

  _updateRoomCount(room) {
    const roomMan = this.rooms.get(room);
    if (!roomMan) return 0;
    const count = roomMan.getCount();
    this._broadcastToRoom(room, JSON.stringify(["roomUserCount", room, count]));
    return count;
  }

  _sendAllState(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws)) return;
    
    const roomMan = this.rooms.get(room);
    if (!roomMan) return;
    
    try {
      const allSeats = roomMan.getAllSeats();
      const allPoints = roomMan.getAllPoints();
      const selfSeat = this.userSeat.get(ws.username)?.seat;
      
      this._safeSend(ws, ["roomUserCount", room, roomMan.getCount()]);
      
      if (allSeats && Object.keys(allSeats).length > 0) {
        if (excludeSelf && selfSeat && allSeats[selfSeat]) {
          const filtered = { ...allSeats };
          delete filtered[selfSeat];
          if (Object.keys(filtered).length > 0) {
            this._safeSend(ws, ["allUpdateKursiList", room, filtered]);
          }
        } else {
          this._safeSend(ws, ["allUpdateKursiList", room, allSeats]);
        }
      }
      
      if (allPoints?.length > 0) {
        let filteredPoints = allPoints;
        if (excludeSelf && selfSeat) {
          filteredPoints = allPoints.filter(p => p.seat !== selfSeat);
        }
        if (filteredPoints.length > 0) {
          this._safeSend(ws, ["allPointsList", room, filteredPoints]);
        }
      }
    } catch(e) {}
  }

  // ==================== DESTROY ====================
  destroy() {
    for (const interval of this._intervals) {
      clearInterval(interval);
    }
    this._intervals = [];
    this._started = false;
    
    for (const ws of this.wsSet) {
      try {
        if (ws.readyState === 1) {
          ws.close(1000, "Server shutting down");
        }
      } catch(e) {}
    }
    
    this.wsSet.clear();
    this.rooms.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this.userConnections.clear();
    this.roomClients.clear();
    this.wsActiveMulti.clear();
    this.joinLocks.clear();
    this.kursiLocks.clear();
    this._eventQueue = [];
    this._processingMessages.clear();
    this._cleaningUp.clear();
  }
}
