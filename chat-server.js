// ==================== CHAT-SERVER.JS ====================
// VERSION: 8.0.0 - WITH CACHE PERSISTENCE

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);
const MAX_SEATS = 45;

export class ChatServer {
  constructor(options) {
    const { env, cache } = options || {};
    
    this.env = env;
    this.cache = cache;
    this.closing = false;
    this.isDestroyed = false;
    
    // ========== WEBSOCKET ==========
    this.wsSet = new Set();
    this.userConnections = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.roomClients = new Map();
    this.wsActiveMulti = new Map();
    
    // ========== ROOM STATE ==========
    this.rooms = new Map();
    
    // Init rooms dengan Map biasa
    for (const room of ROOMS) {
      this.rooms.set(room, {
        seats: new Map(),
        points: new Map(),
        users: new Set(),
        muted: false,
        number: 1
      });
      this.roomClients.set(room, new Set());
    }
    
    // Load dari cache jika ada
    this._loadFromCache().catch(() => {});
    
    // Cleanup setiap 5 detik
    this._cleanupInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._cleanup();
      }
    }, 5000);
  }

  // ========== LOAD FROM CACHE ==========
  async _loadFromCache() {
    try {
      if (!this.cache) return;
      
      const response = await this.cache.match('room_state');
      if (!response) return;
      
      const data = await response.json();
      if (!data) return;
      
      for (const [roomName, roomData] of Object.entries(data)) {
        const room = this.rooms.get(roomName);
        if (room) {
          room.seats = new Map(roomData.seats || []);
          room.points = new Map(roomData.points || []);
          room.users = new Set(roomData.users || []);
          room.muted = roomData.muted || false;
          room.number = roomData.number || 1;
        }
      }
    } catch(e) {
      // Silent fail
    }
  }

  // ========== SAVE TO CACHE ==========
  async saveToCache() {
    try {
      if (!this.cache || this.closing || this.isDestroyed) return;
      
      const data = {};
      for (const [roomName, room] of this.rooms) {
        data[roomName] = {
          seats: Array.from(room.seats.entries()),
          points: Array.from(room.points.entries()),
          users: Array.from(room.users),
          muted: room.muted,
          number: room.number
        };
      }
      
      const response = new Response(JSON.stringify(data), {
        headers: { 'Content-Type': 'application/json' }
      });
      
      await this.cache.put('room_state', response);
      
    } catch(e) {
      // Silent fail
    }
  }

  // ========== BROADCAST ==========
  _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed || !room) return;
    
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const toRemove = new Set();
    
    for (const ws of clients) {
      if (!ws) {
        toRemove.add(ws);
        continue;
      }
      
      try {
        if (ws.readyState === 1 && !ws._closing) {
          ws.send(msgStr);
        } else {
          toRemove.add(ws);
        }
      } catch(e) {
        toRemove.add(ws);
      }
    }
    
    for (const ws of toRemove) {
      clients.delete(ws);
      if (ws) this.cleanup(ws);
    }
  }

  broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try {
      this._broadcastToRoom(room, JSON.stringify(msg));
    } catch(e) {}
  }

  safeSend(ws, msg) {
    if (!ws) return false;
    
    try {
      if (ws.readyState !== 1 || ws._closing || this.closing || this.isDestroyed) {
        return false;
      }
      
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      this.cleanup(ws);
      return false;
    }
  }

  // ========== CLEANUP ==========
  cleanup(ws) {
    if (!ws || ws._cleaning) return;
    
    ws._cleaning = true;
    
    try {
      const username = ws.username;
      const room = ws.room;
      
      if (room) {
        const clients = this.roomClients.get(room);
        if (clients) clients.delete(ws);
      }
      
      const activeData = this.wsActiveMulti.get(ws);
      if (activeData?.room) {
        const clients = this.roomClients.get(activeData.room);
        if (clients) clients.delete(ws);
      }
      this.wsActiveMulti.delete(ws);
      
      if (username) {
        const connections = this.userConnections.get(username);
        if (connections) {
          connections.delete(ws);
          
          const seatInfo = this.userSeat.get(username);
          const isMulti = seatInfo?.isMulti === true;
          
          if (!isMulti && connections.size === 0) {
            this.userConnections.delete(username);
            
            if (seatInfo?.room) {
              const roomData = this.rooms.get(seatInfo.room);
              if (roomData) {
                roomData.seats.delete(seatInfo.seat);
                roomData.users.delete(username);
                roomData.points.delete(seatInfo.seat);
                this.broadcast(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
              }
            }
            
            this.userSeat.delete(username);
            this.userRoom.delete(username);
          }
        }
      }
      
      this.wsSet.delete(ws);
      
    } catch(e) {} finally {
      ws._cleaning = false;
      try { if (ws && ws.readyState === 1) ws.close(1000, "Cleanup"); } catch(e) {}
    }
  }

  _cleanup() {
    try {
      // Dead connections
      const toRemove = [];
      for (const ws of this.wsSet) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(ws);
        }
      }
      for (const ws of toRemove) {
        this.cleanup(ws);
      }
      
      // User connections
      for (const [username, connections] of this.userConnections) {
        const toRemove2 = [];
        for (const conn of connections) {
          if (!conn || conn.readyState !== 1 || conn._closing) {
            toRemove2.push(conn);
          }
        }
        for (const conn of toRemove2) {
          connections.delete(conn);
        }
        if (connections.size === 0) {
          this.userConnections.delete(username);
        }
      }
    } catch(e) {}
  }

  // ========== HANDLE MESSAGE ==========
  async handleMessage(ws, raw) {
    if (!ws) return;
    
    try {
      if (ws.readyState !== 1 || ws._closing || this.closing || this.isDestroyed) {
        return;
      }
      
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      if (evt === "chat" || evt === "updatePoint" || evt === "gift" || evt === "rollangak") {
        const room = args[0];
        if (room && !ROOMS_SET.has(room)) return;
      }
      
      this._handleEvent(ws, evt, args);
      
    } catch(e) {}
  }

  // ========== EVENT HANDLER ==========
  _handleEvent(ws, evt, args) {
    try {
      switch(evt) {
        case "setIdTarget2": {
          const [username] = args;
          this._handleSetId(ws, username);
          break;
        }
        
        case "joinRoom": {
          const [room] = args;
          this._handleJoin(ws, room);
          break;
        }
        
        case "chat": {
          const [room, noimg, user, msg, color, textColor] = args;
          if (!msg || !ROOMS_SET.has(room)) break;
          this._broadcastToRoom(room, JSON.stringify(["chat", room, noimg, user, msg, color, textColor]));
          break;
        }
        
        case "updatePoint": {
          const [room, seat, x, y, fast] = args;
          if (room && typeof seat === 'number' && seat >= 1 && seat <= MAX_SEATS) {
            const roomData = this.rooms.get(room);
            if (roomData && roomData.seats.has(seat)) {
              roomData.points.set(seat, { x: x || 0, y: y || 0, fast: !!fast });
              this._broadcastToRoom(room, JSON.stringify(["pointUpdated", room, seat, x, y, fast]));
            }
          }
          break;
        }
        
        case "updateKursi": {
          const [room, seat, noimg, name, color, bawah, atas, vip, vt] = args;
          const roomData = this.rooms.get(room);
          if (!roomData || !roomData.seats.has(seat)) break;
          
          roomData.seats.set(seat, {
            noimageUrl: noimg || "",
            namauser: name || "",
            color: color || "",
            itembawah: bawah || 0,
            itematas: atas || 0,
            vip: vip || 0,
            viptanda: vt || 0
          });
          
          const updated = roomData.seats.get(seat);
          this.broadcast(room, ["kursiBatchUpdate", room, [[seat, updated]]]);
          break;
        }
        
        case "removeKursiAndPoint": {
          const [room, seat] = args;
          const roomData = this.rooms.get(room);
          if (roomData && roomData.seats.has(seat)) {
            const data = roomData.seats.get(seat);
            if (data?.namauser) {
              roomData.users.delete(data.namauser);
              this.userSeat.delete(data.namauser);
              this.userRoom.delete(data.namauser);
            }
            roomData.seats.delete(seat);
            roomData.points.delete(seat);
            this.broadcast(room, ["removeKursi", room, seat]);
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
            this._broadcastToRoom(room, JSON.stringify(["gift", room, sender, receiver, giftName, Date.now()]));
          }
          break;
        }
        
        case "rollangak": {
          const [room, user, angka] = args;
          if (room && ROOMS_SET.has(room)) {
            this._broadcastToRoom(room, JSON.stringify(["rollangakBroadcast", room, user, angka]));
          }
          break;
        }
        
        case "sendnotif": {
          const [target, noimg, user, msg] = args;
          if (target && msg) {
            const targetConns = this.userConnections.get(target);
            if (targetConns) {
              for (const c of targetConns) {
                if (c?.readyState === 1) {
                  this.safeSend(c, ["notif", noimg, user, msg, Date.now()]);
                  break;
                }
              }
            }
          }
          break;
        }
        
        case "isUserOnline": {
          const [target, callback] = args;
          let isOnline = false;
          const connections = this.userConnections.get(target);
          if (connections) {
            for (const conn of connections) {
              if (conn?.readyState === 1) { isOnline = true; break; }
            }
          }
          this.safeSend(ws, ["userOnlineStatus", target, isOnline, callback || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const [username] of this.userSeat) {
            const connections = this.userConnections.get(username);
            if (connections) {
              for (const conn of connections) {
                if (conn?.readyState === 1) { 
                  users.push(username); 
                  break; 
                }
              }
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) {
            const roomData = this.rooms.get(room);
            counts[room] = roomData?.seats.size || 0;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const [room] = args;
          if (room && ROOMS_SET.has(room)) {
            const roomData = this.rooms.get(room);
            this.safeSend(ws, ["roomUserCount", room, roomData?.seats.size || 0]);
          }
          break;
        }
        
        case "getMuteType": {
          const [room] = args;
          if (room && ROOMS_SET.has(room)) {
            const roomData = this.rooms.get(room);
            this.safeSend(ws, ["muteTypeResponse", roomData?.muted || false, room]);
          }
          break;
        }
        
        case "setMuteType": {
          const [val, room] = args;
          if (room && ROOMS_SET.has(room)) {
            const roomData = this.rooms.get(room);
            if (roomData) {
              roomData.muted = !!val;
              this.broadcast(room, ["muteStatusChanged", !!val, room]);
              this.safeSend(ws, ["muteTypeSet", !!val, true, room]);
            }
          }
          break;
        }
        
        case "onDestroy":
          this.cleanup(ws);
          break;
        
        default:
          this.safeSend(ws, ["error", `Unknown: ${evt}`]);
          break;
      }
    } catch(e) {}
  }

  // ========== HANDLE SET ID ==========
  _handleSetId(ws, username) {
    if (!ws || !username || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid"); } catch(e) {}
      return;
    }
    
    if (ws.readyState !== 1) {
      this.cleanup(ws);
      return;
    }
    
    try {
      // Hapus koneksi lama yang mati
      const oldConnections = this.userConnections.get(username);
      if (oldConnections) {
        const toRemove = [];
        for (const conn of oldConnections) {
          if (!conn || conn.readyState !== 1 || conn._closing) {
            toRemove.push(conn);
          }
        }
        for (const conn of toRemove) {
          oldConnections.delete(conn);
          this.wsSet.delete(conn);
        }
        if (oldConnections.size === 0) {
          this.userConnections.delete(username);
        }
      }
      
      // Tambah koneksi baru
      let connections = this.userConnections.get(username);
      if (!connections) {
        connections = new Set();
        this.userConnections.set(username, connections);
      }
      if (!connections.has(ws)) {
        connections.add(ws);
      }
      
      if (!this.wsSet.has(ws)) {
        this.wsSet.add(ws);
      }
      
      ws.username = username;
      ws.idtarget = username;
      ws._closing = false;
      
      this.safeSend(ws, ["needJoinRoom"]);
      
    } catch(e) {}
  }

  // ========== HANDLE JOIN ==========
  _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
      return;
    }
    
    const username = ws.username;
    const roomData = this.rooms.get(roomName);
    if (!roomData) return;
    
    try {
      // Cek apakah user sudah punya seat
      let seat = null;
      for (const [s, data] of roomData.seats) {
        if (data?.namauser === username) {
          seat = s;
          break;
        }
      }
      
      // Cari seat baru jika belum punya
      if (!seat) {
        if (roomData.seats.size >= MAX_SEATS) {
          this.safeSend(ws, ["roomFull", roomName]);
          return;
        }
        
        for (let s = 1; s <= MAX_SEATS; s++) {
          if (!roomData.seats.has(s)) {
            seat = s;
            break;
          }
        }
        
        if (!seat) {
          this.safeSend(ws, ["roomFull", roomName]);
          return;
        }
        
        roomData.seats.set(seat, {
          noimageUrl: "",
          namauser: username,
          color: "",
          itembawah: 0,
          itematas: 0,
          vip: 0,
          viptanda: 0
        });
        roomData.users.add(username);
      }
      
      // Update user seat info
      this.userSeat.set(username, { room: roomName, seat, isMulti: false });
      this.userRoom.set(username, roomName);
      ws.room = roomName;
      ws.roomname = roomName;
      
      // Add to room clients
      const roomClients = this.roomClients.get(roomName);
      if (roomClients && !roomClients.has(ws)) {
        roomClients.add(ws);
      }
      
      // Send response
      this.safeSend(ws, ["rooMasuk", seat, roomName]);
      this.safeSend(ws, ["numberKursiSaya", seat]);
      this.safeSend(ws, ["muteTypeResponse", roomData.muted, roomName]);
      this.safeSend(ws, ["roomUserCount", roomName, roomData.seats.size]);
      
      // Broadcast to room
      this.broadcast(roomName, ["roomUserCount", roomName, roomData.seats.size]);
      
      // Kirim semua state ke client
      setTimeout(() => {
        if (ws && ws.readyState === 1 && !this.closing && !this.isDestroyed) {
          const allSeats = {};
          for (const [s, d] of roomData.seats) {
            if (d) allSeats[s] = { ...d };
          }
          
          const allPoints = [];
          for (const [s, p] of roomData.points) {
            if (roomData.seats.has(s) && p) {
              allPoints.push({ seat: s, x: p.x, y: p.y, fast: p.fast ? 1 : 0 });
            }
          }
          
          this.safeSend(ws, ["allUpdateKursiList", roomName, allSeats]);
          if (allPoints.length > 0) {
            this.safeSend(ws, ["allPointsList", roomName, allPoints]);
          }
        }
      }, 500);
      
    } catch(e) {}
  }

  // ========== FETCH ==========
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    try {
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server", { 
          status: 200,
          headers: { "Cache-Control": "no-cache" }
        });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      
      try { server.accept(); } catch(e) {
        return new Response("WebSocket failed", { status: 500 });
      }
      
      server.username = null;
      server.room = null;
      server.roomname = null;
      server.idtarget = null;
      server._closing = false;
      server._wsId = Date.now() + Math.random();
      
      server.addEventListener("message", async (event) => {
        if (!server._closing && !this.closing && !this.isDestroyed) {
          await this.handleMessage(server, event.data);
        }
      });
      
      server.addEventListener("close", () => { this.cleanup(server); });
      server.addEventListener("error", () => { this.cleanup(server); });
      
      this.wsSet.add(server);
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      return new Response("Error", { status: 500 });
    }
  }

  // ========== DESTROY ==========
  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    await this.saveToCache();
    
    for (const ws of this.wsSet) {
      try {
        if (ws && ws.readyState === 1) {
          ws.close(1000, "Shutdown");
        }
      } catch(e) {}
    }
    
    this.wsSet.clear();
    this.userConnections.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this.roomClients.clear();
    this.wsActiveMulti.clear();
  }
}
