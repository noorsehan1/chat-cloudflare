// ==================== CHAT-SERVER.JS ====================
// VERSION: 10.3.0 - FULL FIX WEBSOCKET CONNECT

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_UPDATE_TIK: 6,
  MAX_NUMBER: 6,
  TIK_INTERVAL_MS: 900000,
  CLEANUP_INTERVAL: 600000,
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
    this.clients = new Set();
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

  getCount() {
    return this.seats.size;
  }

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

  getMuted() {
    return this.muted;
  }

  setNumber(n) {
    this.number = n || 1;
  }

  getNumber() {
    return this.number;
  }

  updatePoint(seat, x, y, fast) {
    if (!this.seats.has(seat)) return false;
    this.points.set(seat, { x: x || 0, y: y || 0, fast: !!fast });
    return true;
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

  broadcast(msg) {
    if (!msg) return;
    const toRemove = new Set();
    
    for (const ws of this.clients) {
      try {
        if (ws && ws.readyState === 1 && !ws._closing) {
          ws.send(msg);
        } else {
          toRemove.add(ws);
        }
      } catch (e) {
        toRemove.add(ws);
      }
    }
    
    for (const ws of toRemove) {
      this.clients.delete(ws);
    }
  }

  addClient(ws) {
    if (ws && !this.clients.has(ws)) {
      this.clients.add(ws);
    }
  }

  removeClient(ws) {
    if (ws) {
      this.clients.delete(ws);
    }
  }
}

// ==================== CHAT SERVER ====================
export class ChatServer {
  constructor() {
    // ========== INISIALISASI ==========
    this.wsSet = new Set();
    this.userConnections = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.roomClients = new Map();
    this.wsActiveMulti = new Map();
    this.rooms = new Map();
    this.currentNumber = 1;
    this._tikCounter = 0;
    this._intervalsStarted = false;
    this._mainInterval = null;
    this._cleanupInterval = null;
    this.closing = false;
    this.isDestroyed = false;
    
    // Init rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
  }

  _startIntervals() {
    if (this._intervalsStarted || this.closing || this.isDestroyed) return;
    this._intervalsStarted = true;
    
    this._mainInterval = setInterval(() => {
      if (this.closing || this.isDestroyed) {
        clearInterval(this._mainInterval);
        return;
      }
      this._tikCounter++;
      if (this._tikCounter >= C.NUMBER_UPDATE_TIK) {
        this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
        this._tikCounter = 0;
        const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
        for (const [, roomMan] of this.rooms) {
          roomMan.setNumber(this.currentNumber);
          roomMan.broadcast(numberMsg);
        }
      }
    }, C.TIK_INTERVAL_MS);
    
    this._cleanupInterval = setInterval(() => {
      if (this.closing || this.isDestroyed) {
        clearInterval(this._cleanupInterval);
        return;
      }
      this._performCleanup();
    }, C.CLEANUP_INTERVAL);
  }

  _performCleanup() {
    const toRemove = [];
    for (const ws of this.wsSet) {
      if (!ws || ws.readyState !== 1 || ws._closing) {
        toRemove.push(ws);
      }
    }
    for (const ws of toRemove) {
      this.cleanup(ws);
    }
  }

  // ==================== FETCH ====================
  async fetch(request) {
    // START INTERVALS
    this._startIntervals();

    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade");

      // ===== BUKAN WEBSOCKET =====
      if (upgrade !== "websocket") {
        return new Response(JSON.stringify({
          status: "chat-server",
          message: "WebSocket server running",
          path: "/chat/ws",
          rooms: ROOMS,
          timestamp: Date.now()
        }), {
          status: 200,
          headers: { 
            "Content-Type": "application/json",
            "Cache-Control": "no-cache" 
          }
        });
      }

      // ===== CEK KAPASITAS =====
      if (this.wsSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
        return new Response(JSON.stringify({
          error: "Server full",
          max: C.MAX_GLOBAL_CONNECTIONS,
          current: this.wsSet.size
        }), { 
          status: 503,
          headers: { "Content-Type": "application/json" }
        });
      }

      // ===== BUAT WEBSOCKET PAIR =====
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];

      // ===== ACCEPT WEBSOCKET =====
      try {
        server.accept();
      } catch(e) {
        console.error('Chat WS accept failed:', e);
        return new Response(JSON.stringify({
          error: "WebSocket acceptance failed",
          message: e.message
        }), { 
          status: 500,
          headers: { "Content-Type": "application/json" }
        });
      }

      // ===== INIT CONNECTION =====
      server.wsId = Date.now() + Math.random();
      server.username = null;
      server.room = null;
      server.seat = null;
      server.isMulti = false;
      server._closing = false;

      this.wsSet.add(server);

      // ===== SEND CONNECTION ACKNOWLEDGMENT =====
      try {
        server.send(JSON.stringify(["connected", "Chat server connected", Date.now()]));
      } catch(e) {
        console.error('Send connected failed:', e);
      }

      // ===== EVENT HANDLERS =====
      server.onmessage = async (event) => {
        if (server._closing) return;
        await this.handleMessage(server, event.data);
      };

      server.onclose = () => {
        this.cleanup(server);
      };

      server.onerror = () => {
        this.cleanup(server);
      };

      // ===== RETURN =====
      return new Response(null, {
        status: 101,
        webSocket: client
      });

    } catch (e) {
      console.error('Chat fetch error:', e);
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message || "Unknown error"
      }), { 
        status: 500,
        headers: { "Content-Type": "application/json" }
      });
    }
  }

  // ==================== HANDLE MESSAGE ====================
  async handleMessage(ws, raw) {
    if (!ws || ws._closing) return;
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MESSAGE_SIZE) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      // ===== ECHO FOR TESTING =====
      if (evt === "ping") {
        this._safeSend(ws, ["pong", Date.now()]);
        return;
      }

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
          
        case "removeKursiAndPoint":
          this._handleRemoveKursi(ws, args);
          break;
          
        case "private":
          this._handlePrivate(ws, args);
          break;
          
        case "gift":
          this._handleGift(ws, args);
          break;
          
        case "rollangak":
          this._handleRoll(ws, args);
          break;
          
        case "sendnotif":
          this._handleNotif(ws, args);
          break;
          
        case "getCurrentNumber":
          this._safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline":
          this._handleIsUserOnline(ws, args);
          break;
          
        case "getOnlineUsers":
          this._handleGetOnlineUsers(ws);
          break;
          
        case "getAllRoomsUserCount":
          this._handleAllRoomsCount(ws);
          break;
          
        case "getRoomUserCount":
          this._handleRoomCount(ws, args[0]);
          break;
          
        case "setMuteType":
          this._handleSetMute(ws, args);
          break;
          
        case "modwarning":
          if (args[0] && ROOMS_SET.has(args[0])) {
            const roomMan = this.rooms.get(args[0]);
            if (roomMan) {
              roomMan.broadcast(JSON.stringify(["modwarning", args[0]]));
            }
          }
          break;
          
        case "getMuteType":
          this._handleGetMute(ws, args[0]);
          break;
          
        case "onDestroy":
          this.cleanup(ws);
          break;
          
        default:
          this._safeSend(ws, ["error", `Unknown event: ${evt}`]);
          break;
      }
    } catch (e) {
      console.error('Message error:', e);
    }
  }

  // ==================== HANDLE SET ID ====================
  _handleSetId(ws, username, isNewUser) {
    if (!ws || !username) return;
    
    const seatInfo = this.userSeat.get(username);
    if (seatInfo && seatInfo.isMulti && isNewUser === false) {
      ws.username = username;
      ws.isMulti = true;
      ws.room = seatInfo.room;
      ws.seat = seatInfo.seat;
      this._safeSend(ws, ["multiUserActive", username]);
      return;
    }
    
    const oldConns = this.userConnections.get(username);
    if (oldConns) {
      const toRemove = [];
      for (const conn of oldConns) {
        if (!conn || conn.readyState !== 1) {
          toRemove.push(conn);
        }
      }
      for (const conn of toRemove) {
        oldConns.delete(conn);
        this.wsSet.delete(conn);
        this.wsActiveMulti.delete(conn);
      }
    }
    
    if (!seatInfo || !seatInfo.isMulti) {
      if (seatInfo) {
        const roomMan = this.rooms.get(seatInfo.room);
        if (roomMan && seatInfo.seat) {
          roomMan.removeSeat(seatInfo.seat);
          roomMan.broadcast(JSON.stringify(["removeKursi", seatInfo.room, seatInfo.seat]));
          roomMan.broadcast(JSON.stringify(["roomUserCount", seatInfo.room, roomMan.getCount()]));
        }
      }
      this.userSeat.delete(username);
      this.userRoom.delete(username);
    }
    
    ws.username = username;
    ws.isMulti = false;
    
    let conns = this.userConnections.get(username);
    if (!conns) {
      conns = new Set();
      this.userConnections.set(username, conns);
    }
    conns.add(ws);
    
    if (isNewUser) {
      this._safeSend(ws, ["joinroomawal"]);
    } else {
      this._safeSend(ws, ["needJoinRoom"]);
    }
  }

  // ==================== HANDLE JOIN ====================
  _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName)) {
      return false;
    }
    
    const username = ws.username;
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return false;
    
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data.namauser === username) {
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
    
    const oldRoom = ws.room;
    if (oldRoom && oldRoom !== roomName) {
      const oldRoomMan = this.rooms.get(oldRoom);
      if (oldRoomMan) {
        const oldSeat = this.userSeat.get(username)?.seat;
        if (oldSeat) {
          oldRoomMan.removeSeat(oldSeat);
          oldRoomMan.broadcast(JSON.stringify(["removeKursi", oldRoom, oldSeat]));
          oldRoomMan.broadcast(JSON.stringify(["roomUserCount", oldRoom, oldRoomMan.getCount()]));
        }
        oldRoomMan.removeClient(ws);
      }
      this.userSeat.delete(username);
      this.userRoom.delete(username);
    }
    
    ws.room = roomName;
    ws.seat = seat;
    ws.isMulti = false;
    
    this.userSeat.set(username, { room: roomName, seat, isMulti: false });
    this.userRoom.set(username, roomName);
    roomMan.addClient(ws);
    
    this._safeSend(ws, ["rooMasuk", seat, roomName]);
    this._safeSend(ws, ["numberKursiSaya", seat]);
    this._safeSend(ws, ["muteTypeResponse", roomMan.getMuted(), roomName]);
    this._safeSend(ws, ["currentNumber", this.currentNumber]);
    this._safeSend(ws, ["roomUserCount", roomName, roomMan.getCount()]);
    
    roomMan.broadcast(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
    
    setTimeout(() => {
      if (ws && ws.readyState === 1 && !ws._closing) {
        this._sendAllState(ws, roomName, true);
      }
    }, 1000);
    
    return true;
  }

  // ==================== HANDLE MULTI JOIN ====================
  _handleMultiJoin(ws, username, roomName) {
    if (!username || !roomName || !ROOMS_SET.has(roomName)) return;
    
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return;
    
    for (const [room, rm] of this.rooms) {
      for (const [seat, data] of rm.seats) {
        if (data.namauser === username) {
          rm.removeSeat(seat);
          rm.broadcast(JSON.stringify(["removeKursi", room, seat]));
          rm.broadcast(JSON.stringify(["roomUserCount", room, rm.getCount()]));
          break;
        }
      }
    }
    
    let seat = null;
    for (let s = 1; s <= C.MAX_SEATS; s++) {
      if (!roomMan.seats.has(s)) {
        seat = s;
        break;
      }
    }
    if (!seat) {
      this._safeSend(ws, ["roomFull", roomName]);
      return;
    }
    
    roomMan.addSeat(username, "", "", 0, 0, 0, 0);
    
    ws.username = username;
    ws.room = roomName;
    ws.seat = seat;
    ws.isMulti = true;
    
    this.userSeat.set(username, { room: roomName, seat, isMulti: true });
    this.userRoom.set(username, roomName);
    this.wsActiveMulti.set(ws, { username, room: roomName });
    roomMan.addClient(ws);
    
    this._safeSend(ws, ["rooMasukMulti", seat, roomName]);
    roomMan.broadcast(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
  }

  // ==================== HANDLE EXIT MULTI ====================
  _handleExitMulti(ws, username) {
    if (!username) return;
    
    const seatInfo = this.userSeat.get(username);
    if (!seatInfo) return;
    
    const roomName = seatInfo.room;
    const roomMan = this.rooms.get(roomName);
    if (roomMan && seatInfo.seat) {
      roomMan.removeSeat(seatInfo.seat);
      roomMan.broadcast(JSON.stringify(["removeKursi", roomName, seatInfo.seat]));
      roomMan.broadcast(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
      roomMan.removeClient(ws);
    }
    
    this.userSeat.delete(username);
    this.userRoom.delete(username);
    this.wsActiveMulti.delete(ws);
    ws.username = null;
    ws.room = null;
    ws.seat = null;
    ws.isMulti = false;
  }

  // ==================== HANDLE SET ACTIVE MULTI ====================
  _handleSetActiveMulti(ws, username) {
    const seatInfo = this.userSeat.get(username);
    if (!seatInfo) return;
    
    const roomName = seatInfo.room;
    const roomMan = this.rooms.get(roomName);
    
    if (ws.room && ws.room !== roomName) {
      const oldRoomMan = this.rooms.get(ws.room);
      if (oldRoomMan) {
        oldRoomMan.removeClient(ws);
      }
    }
    
    ws.username = username;
    ws.room = roomName;
    ws.seat = seatInfo.seat;
    ws.isMulti = true;
    
    this.wsActiveMulti.set(ws, { username, room: roomName });
    roomMan.addClient(ws);
    
    this._safeSend(ws, ["activeChangedMulti", username, seatInfo.seat, roomName]);
    roomMan.broadcast(JSON.stringify(["userActiveChanged", username, seatInfo.seat]));
  }

  // ==================== HANDLE UPDATE KURSI ====================
  _handleUpdateKursi(ws, args) {
    const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
    const roomMan = this.rooms.get(kursiRoom);
    if (!roomMan) return;
    
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
      roomMan.broadcast(JSON.stringify(["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]));
    }
  }

  // ==================== HANDLE CHAT ====================
  _handleChat(ws, args) {
    const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
    
    if (!chatMsg || !ROOMS_SET.has(chatRoom)) return;
    
    const roomMan = this.rooms.get(chatRoom);
    if (!roomMan) return;
    
    if (roomMan.getMuted()) {
      this._safeSend(ws, ["chatError", "Room is muted"]);
      return;
    }
    
    roomMan.broadcast(JSON.stringify([
      "chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor
    ]));
  }

  // ==================== HANDLE UPDATE POINT ====================
  _handleUpdatePoint(ws, args) {
    const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
    if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
      const roomMan = this.rooms.get(pointRoom);
      if (roomMan && roomMan.seats.has(pointSeat)) {
        if (roomMan.updatePoint(pointSeat, pointX, pointY, pointFast === 1)) {
          roomMan.broadcast(JSON.stringify([
            "pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast
          ]));
        }
      }
    }
  }

  // ==================== HANDLE REMOVE KURSI ====================
  _handleRemoveKursi(ws, args) {
    const [removeRoom, removeSeat] = args;
    const roomMan = this.rooms.get(removeRoom);
    if (roomMan && roomMan.seats.has(removeSeat)) {
      for (const [username, info] of this.userSeat) {
        if (info.seat === removeSeat && info.room === removeRoom) {
          this.userSeat.delete(username);
          this.userRoom.delete(username);
          break;
        }
      }
      roomMan.removeSeat(removeSeat);
      roomMan.broadcast(JSON.stringify(["removeKursi", removeRoom, removeSeat]));
      roomMan.broadcast(JSON.stringify(["roomUserCount", removeRoom, roomMan.getCount()]));
    }
  }

  // ==================== HANDLE PRIVATE ====================
  _handlePrivate(ws, args) {
    const [privTarget, privNoimg, privMsg, privSender] = args;
    if (privTarget && privMsg) {
      const targetConns = this.userConnections.get(privTarget);
      if (targetConns) {
        for (const targetWs of targetConns) {
          if (targetWs?.readyState === 1) {
            this._safeSend(targetWs, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
            break;
          }
        }
      }
      this._safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
    }
  }

  // ==================== HANDLE GIFT ====================
  _handleGift(ws, args) {
    const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
    if (giftRoom && ROOMS_SET.has(giftRoom)) {
      const roomMan = this.rooms.get(giftRoom);
      if (roomMan) {
        roomMan.broadcast(JSON.stringify([
          "gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()
        ]));
      }
    }
  }

  // ==================== HANDLE ROLL ====================
  _handleRoll(ws, args) {
    const [rollRoom, rollUser, rollAngka] = args;
    if (rollRoom && ROOMS_SET.has(rollRoom)) {
      const roomMan = this.rooms.get(rollRoom);
      if (roomMan) {
        roomMan.broadcast(JSON.stringify([
          "rollangakBroadcast", rollRoom, rollUser, rollAngka
        ]));
      }
    }
  }

  // ==================== HANDLE NOTIF ====================
  _handleNotif(ws, args) {
    const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
    if (notifTarget && notifMsg) {
      const targetConns = this.userConnections.get(notifTarget);
      if (targetConns) {
        for (const c of targetConns) {
          if (c?.readyState === 1) {
            this._safeSend(c, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
            break;
          }
        }
      }
    }
  }

  // ==================== HANDLE IS USER ONLINE ====================
  _handleIsUserOnline(ws, args) {
    const [onlineTarget, onlineCallback] = args;
    let isOnline = false;
    const seatInfo = this.userSeat.get(onlineTarget);
    if (seatInfo?.seat) {
      if (seatInfo.isMulti) {
        isOnline = true;
      } else {
        const connections = this.userConnections.get(onlineTarget);
        if (connections) {
          for (const conn of connections) {
            if (conn?.readyState === 1) { isOnline = true; break; }
          }
        }
      }
    }
    this._safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
  }

  // ==================== HANDLE GET ONLINE USERS ====================
  _handleGetOnlineUsers(ws) {
    const users = [];
    for (const [username, seatInfo] of this.userSeat) {
      if (seatInfo?.seat) {
        if (seatInfo.isMulti) {
          users.push(username);
        } else {
          const connections = this.userConnections.get(username);
          if (connections) {
            for (const conn of connections) {
              if (conn?.readyState === 1) { users.push(username); break; }
            }
          }
        }
      }
    }
    this._safeSend(ws, ["allOnlineUsers", users]);
  }

  // ==================== HANDLE ALL ROOMS COUNT ====================
  _handleAllRoomsCount(ws) {
    const counts = {};
    for (const [room, roomMan] of this.rooms) {
      counts[room] = roomMan.getCount();
    }
    this._safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
  }

  // ==================== HANDLE ROOM COUNT ====================
  _handleRoomCount(ws, roomName) {
    if (roomName && ROOMS_SET.has(roomName)) {
      const roomMan = this.rooms.get(roomName);
      this._safeSend(ws, ["roomUserCount", roomName, roomMan?.getCount() || 0]);
    }
  }

  // ==================== HANDLE SET MUTE ====================
  _handleSetMute(ws, args) {
    const [muteVal, muteRoom] = args;
    if (!muteRoom || !ROOMS_SET.has(muteRoom)) return;
    
    const roomMan = this.rooms.get(muteRoom);
    if (!roomMan) return;
    
    roomMan.setMuted(muteVal);
    roomMan.broadcast(JSON.stringify(["muteStatusChanged", !!muteVal, muteRoom]));
    this._safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
  }

  // ==================== HANDLE GET MUTE ====================
  _handleGetMute(ws, muteRoom) {
    if (muteRoom && ROOMS_SET.has(muteRoom)) {
      const roomMan = this.rooms.get(muteRoom);
      this._safeSend(ws, ["muteTypeResponse", roomMan?.getMuted() || false, muteRoom]);
    }
  }

  // ==================== SEND ALL STATE ====================
  _sendAllState(ws, room, excludeSelf = false) {
    if (!ws || !ws.username || ws.readyState !== 1) return;
    
    const roomMan = this.rooms.get(room);
    if (!roomMan) return;
    
    const allSeats = roomMan.getAllSeats();
    const allPoints = roomMan.getAllPoints();
    const selfSeat = ws.seat;
    
    this._safeSend(ws, ["roomUserCount", room, roomMan.getCount()]);
    this._safeSend(ws, ["currentNumber", this.currentNumber]);
    
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
  }

  // ==================== SAFE SEND ====================
  _safeSend(ws, msg) {
    if (!ws) return false;
    try {
      if (ws.readyState === 1 && !ws._closing) {
        ws.send(JSON.stringify(msg));
        return true;
      }
    } catch (e) {}
    return false;
  }

  // ==================== CLEANUP ====================
  cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    try {
      const username = ws.username;
      const room = ws.room;
      const isMulti = ws.isMulti || false;
      
      if (room) {
        const roomMan = this.rooms.get(room);
        if (roomMan) {
          roomMan.removeClient(ws);
          if (!isMulti && ws.seat) {
            roomMan.removeSeat(ws.seat);
            roomMan.broadcast(JSON.stringify(["removeKursi", room, ws.seat]));
            roomMan.broadcast(JSON.stringify(["roomUserCount", room, roomMan.getCount()]));
          }
        }
      }
      
      this.wsSet.delete(ws);
      
      if (username) {
        const conns = this.userConnections.get(username);
        if (conns) {
          conns.delete(ws);
          if (conns.size === 0) {
            this.userConnections.delete(username);
            if (!isMulti) {
              const seatInfo = this.userSeat.get(username);
              if (seatInfo) {
                const roomMan = this.rooms.get(seatInfo.room);
                if (roomMan && seatInfo.seat) {
                  roomMan.removeSeat(seatInfo.seat);
                  roomMan.broadcast(JSON.stringify(["removeKursi", seatInfo.room, seatInfo.seat]));
                  roomMan.broadcast(JSON.stringify(["roomUserCount", seatInfo.room, roomMan.getCount()]));
                }
              }
              this.userSeat.delete(username);
              this.userRoom.delete(username);
            }
          }
        }
      }
      
      this.wsActiveMulti.delete(ws);
      
    } catch (e) {}
    
    try {
      if (ws.readyState === 1) {
        ws.close(1000, "Cleanup");
      }
    } catch (e) {}
  }

  // ==================== DESTROY ====================
  destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    if (this._mainInterval) {
      clearInterval(this._mainInterval);
      this._mainInterval = null;
    }
    
    const wsCopy = Array.from(this.wsSet);
    for (const ws of wsCopy) {
      try {
        if (ws.readyState === 1) {
          ws.close(1000, "Shutdown");
        }
      } catch (e) {}
      this.cleanup(ws);
    }
    
    this.wsSet.clear();
    this.userConnections.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this.wsActiveMulti.clear();
    this.roomClients.clear();
    this.rooms.clear();
  }
}
