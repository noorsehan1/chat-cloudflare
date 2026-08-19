// ==================== CHAT-SERVER.JS - D1 ====================
// VERSION: 5.0.0

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);
const MAX_SEATS = 45;
const BATCH_SIZE = 20;

export class ChatServer {
  constructor(env) {
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    this._wsIdCounter = 0;
    
    // WebSocket connections
    this.wsSet = new Set();
    this.roomClients = new Map();
    this.userConnections = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.wsActiveMulti = new Map();
    
    // Room data (cache)
    this.rooms = new Map();
    for (const room of ROOMS) {
      this.rooms.set(room, {
        seats: new Map(),
        points: new Map(),
        muted: false,
        number: 1
      });
      this.roomClients.set(room, new Set());
    }
    
    this.currentNumber = 1;
    this._tikCounter = 0;
    this._eventQueue = [];
    this._isProcessingQueue = false;
    this._pendingTimeouts = new Set();
    
    // Cleanup interval
    this._cleanupInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._cleanupDeadConnections();
        this._processEventQueue();
      }
    }, 10000);
    this._pendingTimeouts.add(this._cleanupInterval);
  }

  // ========== LOAD STATE DARI D1 ==========
  async loadState() {
    try {
      if (!this.env?.DB) return;
      const db = this.env.DB;
      
      console.log('[ChatServer] Loading state from D1...');
      
      // Load room settings
      const settings = await db.prepare(`SELECT * FROM chat_rooms`).all();
      for (const row of settings.results || []) {
        const room = this.rooms.get(row.room_name);
        if (room) {
          room.muted = row.muted === 1;
          room.number = row.number || 1;
        }
      }
      
      // Load seats
      const seats = await db.prepare(`SELECT * FROM chat_seats`).all();
      for (const row of seats.results || []) {
        const room = this.rooms.get(row.room_name);
        if (room) {
          room.seats.set(row.seat_number, {
            namauser: row.namauser || '',
            noimageUrl: row.noimageUrl || '',
            color: row.color || '',
            itembawah: row.itembawah || 0,
            itematas: row.itematas || 0,
            vip: row.vip || 0,
            viptanda: row.viptanda || 0
          });
        }
      }
      
      // Load points
      const points = await db.prepare(`SELECT * FROM chat_points`).all();
      for (const row of points.results || []) {
        const room = this.rooms.get(row.room_name);
        if (room) {
          room.points.set(row.seat_number, {
            x: row.x || 0,
            y: row.y || 0,
            fast: row.fast || 0
          });
        }
      }
      
      // Load users
      const users = await db.prepare(`SELECT * FROM chat_users`).all();
      for (const row of users.results || []) {
        this.userSeat.set(row.username, {
          room: row.room_name,
          seat: row.seat_number,
          isMulti: row.is_multi === 1
        });
        this.userRoom.set(row.username, row.room_name);
      }
      
      console.log(`[ChatServer] Loaded: ${seats.results?.length || 0} seats, ${users.results?.length || 0} users`);
    } catch(e) {
      console.error('[ChatServer] Load error:', e);
    }
  }

  // ========== SAVE STATE KE D1 ==========
  async saveState() {
    try {
      if (!this.env?.DB) return;
      const db = this.env.DB;
      
      // Save room settings
      for (const [roomName, room] of this.rooms) {
        await db.prepare(
          `INSERT OR REPLACE INTO chat_rooms (room_name, muted, number, updated_at)
           VALUES (?, ?, ?, unixepoch())`
        ).bind(roomName, room.muted ? 1 : 0, room.number || 1).run();
      }
      
      // Save seats
      await db.prepare(`DELETE FROM chat_seats`).run();
      for (const [roomName, room] of this.rooms) {
        for (const [seat, data] of room.seats) {
          await db.prepare(
            `INSERT INTO chat_seats 
             (room_name, seat_number, namauser, noimageUrl, color, itembawah, itematas, vip, viptanda, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch())`
          ).bind(
            roomName, seat, data.namauser || '', data.noimageUrl || '',
            data.color || '', data.itembawah || 0, data.itematas || 0,
            data.vip || 0, data.viptanda || 0
          ).run();
        }
      }
      
      // Save points
      await db.prepare(`DELETE FROM chat_points`).run();
      for (const [roomName, room] of this.rooms) {
        for (const [seat, point] of room.points) {
          await db.prepare(
            `INSERT INTO chat_points (room_name, seat_number, x, y, fast, updated_at)
             VALUES (?, ?, ?, ?, ?, unixepoch())`
          ).bind(roomName, seat, point.x || 0, point.y || 0, point.fast || 0).run();
        }
      }
      
      // Save users
      await db.prepare(`DELETE FROM chat_users`).run();
      for (const [username, info] of this.userSeat) {
        await db.prepare(
          `INSERT INTO chat_users (username, room_name, seat_number, is_multi, last_seen, updated_at)
           VALUES (?, ?, ?, ?, unixepoch(), unixepoch())`
        ).bind(username, info.room, info.seat, info.isMulti ? 1 : 0).run();
      }
      
      console.log('[ChatServer] Saved state to D1');
    } catch(e) {
      console.error('[ChatServer] Save error:', e);
    }
  }

  // ========== BROADCAST ==========
  _broadcastToRoom(room, msgStr) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const clientArray = Array.from(clients);
    for (let i = 0; i < clientArray.length; i += BATCH_SIZE) {
      const batch = clientArray.slice(i, i + BATCH_SIZE);
      for (const ws of batch) {
        if (ws && ws.readyState === 1 && !ws._closing) {
          try { ws.send(msgStr); } catch(e) {}
        }
      }
    }
  }

  broadcast(room, msg) {
    if (!room || !msg || this.closing || this.isDestroyed) return;
    try { this._broadcastToRoom(room, JSON.stringify(msg)); } catch(e) {}
  }

  safeSend(ws, msg) {
    if (!ws) return false;
    try {
      if (ws.readyState === 1 && !ws._closing) {
        ws.send(JSON.stringify(msg));
        return true;
      }
    } catch(e) {}
    return false;
  }

  // ========== FETCH ==========
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    try {
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server", { status: 200 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      
      try { server.accept(); } catch(e) {
        return new Response("WebSocket failed", { status: 500 });
      }
      
      const wsId = ++this._wsIdCounter;
      server._wsId = wsId;
      server.username = null;
      server.room = null;
      server._closing = false;
      
      // Event handlers
      server.addEventListener("message", async (event) => {
        try {
          if (server._closing || this.closing || this.isDestroyed) return;
          let data;
          try { data = JSON.parse(event.data); } catch(e) { return; }
          if (Array.isArray(data) && data.length > 0) {
            await this._handleEventInternal(server, data);
          }
        } catch(e) {}
      });
      
      server.addEventListener("close", () => { this.cleanup(server); });
      server.addEventListener("error", () => { this.cleanup(server); });
      
      this.wsSet.add(server);
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      return new Response("Error", { status: 500 });
    }
  }

  // ========== HANDLE EVENT ==========
  async _handleEventInternal(ws, data) {
    if (!ws || !data || !data[0]) return;
    const [evt, ...args] = data;
    
    switch(evt) {
      case "setIdTarget2":
        this._handleSetId(ws, args[0], args[1]);
        break;
        
      case "joinRoom":
        this._handleJoin(ws, args[0]);
        break;
        
      case "chat": {
        const [room, noimg, user, msg, color, textColor] = args;
        if (room && ROOMS_SET.has(room) && msg) {
          this.broadcast(room, ["chat", room, noimg, user, msg, color, textColor]);
        }
        break;
      }
      
      case "updateKursi": {
        const [room, seat, noimg, name, color, bawah, atas, vip, vt] = args;
        const roomData = this.rooms.get(room);
        if (roomData && roomData.seats.has(seat)) {
          const data = roomData.seats.get(seat);
          data.noimageUrl = noimg || data.noimageUrl;
          data.namauser = name || data.namauser;
          data.color = color || data.color;
          data.itembawah = bawah || data.itembawah;
          data.itematas = atas || data.itematas;
          data.vip = vip || data.vip;
          data.viptanda = vt || data.viptanda;
          this.broadcast(room, ["kursiBatchUpdate", room, [[seat, data]]]);
        }
        break;
      }
      
      case "updatePoint": {
        const [room, seat, x, y, fast] = args;
        const roomData = this.rooms.get(room);
        if (roomData && roomData.seats.has(seat)) {
          roomData.points.set(seat, { x: x || 0, y: y || 0, fast: fast || 0 });
          this.broadcast(room, ["pointUpdated", room, seat, x, y, fast]);
        }
        break;
      }
      
      case "removeKursiAndPoint": {
        const [room, seat] = args;
        const roomData = this.rooms.get(room);
        if (roomData) {
          // Remove from userSeat
          for (const [username, info] of this.userSeat) {
            if (info.seat === seat && info.room === room) {
              this.userSeat.delete(username);
              this.userRoom.delete(username);
              break;
            }
          }
          roomData.seats.delete(seat);
          roomData.points.delete(seat);
          this.broadcast(room, ["removeKursi", room, seat]);
          this.broadcast(room, ["roomUserCount", room, roomData.seats.size]);
        }
        break;
      }
      
      case "private": {
        const [target, noimg, msg, sender] = args;
        if (target && msg) {
          const targetConns = this.userConnections.get(target);
          if (targetConns) {
            for (const targetWs of targetConns) {
              if (targetWs?.readyState === 1) {
                this.safeSend(targetWs, ["private", target, noimg, msg, Date.now(), sender]);
                break;
              }
            }
          }
          this.safeSend(ws, ["private", target, noimg, msg, Date.now(), sender]);
        }
        break;
      }
      
      case "gift": {
        const [room, sender, receiver, giftName] = args;
        if (room && ROOMS_SET.has(room)) {
          this.broadcast(room, ["gift", room, sender, receiver, giftName, Date.now()]);
        }
        break;
      }
      
      case "rollangak": {
        const [room, user, angka] = args;
        if (room && ROOMS_SET.has(room)) {
          this.broadcast(room, ["rollangakBroadcast", room, user, angka]);
        }
        break;
      }
      
      case "getRoomUserCount": {
        const room = args[0];
        if (room && ROOMS_SET.has(room)) {
          const roomData = this.rooms.get(room);
          this.safeSend(ws, ["roomUserCount", room, roomData?.seats?.size || 0]);
        }
        break;
      }
      
      case "getAllRoomsUserCount": {
        const counts = {};
        for (const room of ROOMS) {
          const roomData = this.rooms.get(room);
          counts[room] = roomData?.seats?.size || 0;
        }
        this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
        break;
      }
      
      case "getOnlineUsers": {
        const users = [];
        for (const [username, info] of this.userSeat) {
          if (info.seat) {
            const conns = this.userConnections.get(username);
            if (conns) {
              for (const conn of conns) {
                if (conn?.readyState === 1) { users.push(username); break; }
              }
            }
          }
        }
        this.safeSend(ws, ["allOnlineUsers", users]);
        break;
      }
      
      case "isUserOnline": {
        const [target, callback] = args;
        let isOnline = false;
        const info = this.userSeat.get(target);
        if (info?.seat) {
          const conns = this.userConnections.get(target);
          if (conns) {
            for (const conn of conns) {
              if (conn?.readyState === 1) { isOnline = true; break; }
            }
          }
        }
        this.safeSend(ws, ["userOnlineStatus", target, isOnline, callback || ""]);
        break;
      }
      
      case "setMuteType": {
        const [muteVal, room] = args;
        const roomData = this.rooms.get(room);
        if (roomData) {
          roomData.muted = !!muteVal;
          this.broadcast(room, ["muteStatusChanged", !!muteVal, room]);
          this.safeSend(ws, ["muteTypeSet", !!muteVal, true, room]);
        }
        break;
      }
      
      case "getMuteType": {
        const room = args[0];
        if (room && ROOMS_SET.has(room)) {
          const roomData = this.rooms.get(room);
          this.safeSend(ws, ["muteTypeResponse", roomData?.muted || false, room]);
        }
        break;
      }
      
      case "modwarning": {
        const room = args[0];
        if (room && ROOMS_SET.has(room)) {
          this.broadcast(room, ["modwarning", room]);
        }
        break;
      }
      
      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;
      
      case "onDestroy":
        this.cleanup(ws);
        break;
        
      default:
        break;
    }
  }

  // ========== HANDLE SET ID ==========
  _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || this.closing || this.isDestroyed) return;
    if (ws.readyState !== 1) { this.cleanup(ws); return; }
    
    // Clean existing connections
    const oldConns = this.userConnections.get(username);
    if (oldConns) {
      const toRemove = [];
      for (const conn of oldConns) {
        if (!conn || conn.readyState !== 1 || conn._closing) {
          toRemove.push(conn);
        }
      }
      for (const conn of toRemove) {
        oldConns.delete(conn);
        this.wsSet.delete(conn);
        this.wsActiveMulti.delete(conn);
      }
      if (oldConns.size === 0) this.userConnections.delete(username);
    }
    
    // Set user
    ws.username = username;
    ws.idtarget = username;
    ws._closing = false;
    
    let connections = this.userConnections.get(username);
    if (!connections) {
      connections = new Set();
      this.userConnections.set(username, connections);
    }
    connections.add(ws);
    
    if (!this.wsSet.has(ws)) this.wsSet.add(ws);
    
    // Send response
    if (isNewUser) {
      this.safeSend(ws, ["joinroomawal"]);
    } else {
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }

  // ========== HANDLE JOIN ==========
  _handleJoin(ws, roomName) {
    if (!ws || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) return;
    
    const username = ws.username;
    if (!username) return;
    
    const roomData = this.rooms.get(roomName);
    if (!roomData) return;
    
    // Remove from old room
    const oldRoom = ws.room;
    if (oldRoom && oldRoom !== roomName) {
      const oldRoomData = this.rooms.get(oldRoom);
      if (oldRoomData) {
        let oldSeat = null;
        for (const [s, data] of oldRoomData.seats) {
          if (data.namauser === username) { oldSeat = s; break; }
        }
        if (oldSeat) {
          oldRoomData.seats.delete(oldSeat);
          oldRoomData.points.delete(oldSeat);
          this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          this.broadcast(oldRoom, ["roomUserCount", oldRoom, oldRoomData.seats.size]);
        }
      }
      const oldClients = this.roomClients.get(oldRoom);
      if (oldClients) oldClients.delete(ws);
      this.userSeat.delete(username);
      this.userRoom.delete(username);
      ws.room = null;
    }
    
    // Check existing seat
    let seat = null;
    for (const [s, data] of roomData.seats) {
      if (data.namauser === username) { seat = s; break; }
    }
    
    // Find available seat
    if (!seat) {
      if (roomData.seats.size >= MAX_SEATS) {
        this.safeSend(ws, ["roomFull", roomName]);
        return;
      }
      for (let s = 1; s <= MAX_SEATS; s++) {
        if (!roomData.seats.has(s)) { seat = s; break; }
      }
    }
    
    if (!seat) {
      this.safeSend(ws, ["roomFull", roomName]);
      return;
    }
    
    // Add seat
    roomData.seats.set(seat, {
      namauser: username,
      noimageUrl: '',
      color: '',
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0
    });
    
    // Update state
    this.userSeat.set(username, { room: roomName, seat, isMulti: false });
    this.userRoom.set(username, roomName);
    ws.room = roomName;
    ws.idtarget = username;
    
    const clients = this.roomClients.get(roomName);
    if (clients && !clients.has(ws)) clients.add(ws);
    
    // Send responses
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomData.muted, roomName]);
    this.broadcast(roomName, ["roomUserCount", roomName, roomData.seats.size]);
    
    // Send all seats
    const allSeats = {};
    for (const [s, data] of roomData.seats) {
      allSeats[s] = data;
    }
    this.safeSend(ws, ["allUpdateKursiList", roomName, allSeats]);
    
    // Send all points
    const allPoints = [];
    for (const [s, point] of roomData.points) {
      allPoints.push({ seat: s, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    if (allPoints.length > 0) {
      this.safeSend(ws, ["allPointsList", roomName, allPoints]);
    }
  }

  // ========== CLEANUP ==========
  cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    ws._closing = true;
    
    const username = ws.username;
    const room = ws.room;
    
    if (room) {
      const clients = this.roomClients.get(room);
      if (clients) clients.delete(ws);
      
      const roomData = this.rooms.get(room);
      if (roomData && username) {
        let seatToRemove = null;
        for (const [s, data] of roomData.seats) {
          if (data.namauser === username) { seatToRemove = s; break; }
        }
        if (seatToRemove) {
          roomData.seats.delete(seatToRemove);
          roomData.points.delete(seatToRemove);
          this.broadcast(room, ["removeKursi", room, seatToRemove]);
          this.broadcast(room, ["roomUserCount", room, roomData.seats.size]);
        }
      }
    }
    
    if (username) {
      const conns = this.userConnections.get(username);
      if (conns) {
        conns.delete(ws);
        if (conns.size === 0) {
          this.userConnections.delete(username);
          this.userSeat.delete(username);
          this.userRoom.delete(username);
        }
      }
      this.wsActiveMulti.delete(ws);
    }
    
    this.wsSet.delete(ws);
    ws._cleaning = false;
    
    try { ws.close(1000); } catch(e) {}
  }

  _cleanupDeadConnections() {
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

  _processEventQueue() {
    // Simple queue processing
  }

  // ========== DESTROY ==========
  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    for (const timer of this._pendingTimeouts) {
      clearInterval(timer);
      clearTimeout(timer);
    }
    this._pendingTimeouts.clear();
    
    for (const ws of this.wsSet) {
      try { ws.close(1000); } catch(e) {}
    }
    this.wsSet.clear();
    this.roomClients.clear();
    this.userConnections.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    
    await this.saveState();
  }
}
