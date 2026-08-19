// ==================== CHAT-SERVER.JS ====================
// FULL COMPREHENSIVE CODE - MAINTAINING ORIGINAL ARCHITECTURE & HIBERNATION SAFE

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  ALARM_INTERVAL_MS: 900000,    // 15 MENIT
  NUMBER_UPDATE_TIK: 6,         // 6 × 15 menit = 90 menit
  MAX_NUMBER: 6,
  BATCH_SIZE: 20,
  LOCK_TIMEOUT: 10000,
  CLEANUP_INTERVAL: 600000,     // 10 MENIT
  MAX_EVENT_QUEUE: 100,
  MAX_PROCESS_TIME_MS: 500,
  CPU_YIELD_MS: 1,
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

// ==================== CHAT SERVER ====================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.ctx = state;
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    this._initialized = false;
    this._startTime = Date.now();
    
    // ========== WEBSOCKET & STATE TRACKING ==========
    this.wsSet = new Set();
    this.userConnections = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.roomClients = new Map();
    this.rooms = new Map();
    this.wsActiveMulti = new Map();
    
    // ========== PROCESSING & CLEANUP ==========
    this._processingMessages = new Set();
    this._cleaningUp = new Set();
    this._cleanupInProgress = false;
    this._eventQueue = [];
    this._isProcessingQueue = false;
    
    // ========== LOCKS ==========
    this._joinLocks = new Map();
    this._kursiLocks = new Map();
    
    // ========== NUMBER ==========
    this.currentNumber = 1;
    this._tikCounter = 0;
    
    // ========== INIT ROOMS ==========
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    // Inisialisasi Storage & Set Alarm Pertama
    this.ctx.blockConcurrencyWhile(async () => {
      const storedNumber = await this.ctx.storage.get("currentNumber");
      const storedTik = await this.ctx.storage.get("tikCounter");
      if (storedNumber !== undefined) this.currentNumber = storedNumber;
      if (storedTik !== undefined) this._tikCounter = storedTik;

      const currentAlarm = await this.ctx.storage.getAlarm();
      if (!currentAlarm) {
        await this.ctx.storage.setAlarm(Date.now() + C.ALARM_INTERVAL_MS);
      }
    });
  }

  // ==================== HTTP / WS FETCH HANDLER ====================
  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    // Menerima koneksi menggunakan Hibernation API Cloudflare
    this.ctx.acceptWebSocket(server);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ==================== WEBSOCKET HIBERNATION EVENT HANDLERS ====================
  async webSocketMessage(ws, message) {
    this.wsSet.add(ws);
    await this.handleMessage(ws, message);
  }

  async webSocketClose(ws, code, reason, wasClean) {
    this.cleanup(ws);
  }

  async webSocketError(ws, error) {
    this.cleanup(ws);
  }

  // ==================== ALARM HANDLER (Per 15 Menit) ====================
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    try {
      this._tikCounter++;
      
      // Update nomor setiap 6 tik (90 menit)
      if (this._tikCounter >= C.NUMBER_UPDATE_TIK) {
        this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
        
        for (const room of this.rooms.values()) {
          if (room) {
            room.setNumber(this.currentNumber);
          }
        }

        await this.ctx.storage.put("currentNumber", this.currentNumber);
        await this.ctx.storage.put("tikCounter", 0);
        
        const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
        
        for (const [room, clients] of this.roomClients) {
          if (clients && clients.size > 0) {
            this._broadcastToRoom(room, numberMsg);
          }
        }
        
        this._tikCounter = 0;
      } else {
        await this.ctx.storage.put("tikCounter", this._tikCounter);
      }
      
      // Cleanup rutin dilakukan saat alarm terpicu
      this._performCleanup();
      
    } catch(e) {}
    
    // Set alarm berikutnya untuk 15 menit mendatang
    try {
      await this.state.storage.setAlarm(Date.now() + C.ALARM_INTERVAL_MS);
    } catch(e) {}
  }

  // ==================== PERFORM CLEANUP ====================
  _performCleanup() {
    if (this._cleanupInProgress || this.closing || this.isDestroyed) return;
    this._cleanupInProgress = true;
    
    try {
      this._cleanupDeadConnections();
      this._cleanupStaleLocks();
      this._cleanupMemory();
      this._cleanupEventQueue();
    } catch(e) {} finally {
      this._cleanupInProgress = false;
    }
  }

  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      for (const ws of this.wsSet) {
        if (!ws || ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws)) {
          toRemove.push(ws);
        }
      }
      for (const ws of toRemove) {
        this.cleanup(ws);
      }
    } catch(e) {}
  }

  _cleanupStaleLocks() {
    try {
      const now = Date.now();
      for (const [key, time] of this._joinLocks) {
        if (now - time > C.LOCK_TIMEOUT) this._joinLocks.delete(key);
      }
      for (const [key, time] of this._kursiLocks) {
        if (now - time > C.LOCK_TIMEOUT) this._kursiLocks.delete(key);
      }
    } catch(e) {}
  }

  _cleanupMemory() {
    try {
      for (const [username, connections] of this.userConnections) {
        const toRemove = [];
        for (const conn of connections) {
          if (!conn || conn.readyState !== 1 || conn._closing || this._cleaningUp.has(conn)) {
            toRemove.push(conn);
          }
        }
        for (const conn of toRemove) {
          connections.delete(conn);
        }
        if (connections.size === 0) {
          this.userConnections.delete(username);
        }
      }
      
      for (const [roomName, roomMan] of this.rooms) {
        if (roomMan) {
          const pointsToRemove = [];
          for (const [seat] of roomMan.points) {
            if (!roomMan.seats.has(seat)) {
              pointsToRemove.push(seat);
            }
          }
          for (const seat of pointsToRemove) {
            roomMan.points.delete(seat);
          }
        }
      }
    } catch(e) {}
  }

  _cleanupEventQueue() {
    try {
      if (this._eventQueue && this._eventQueue.length > C.MAX_EVENT_QUEUE) {
        this._eventQueue.splice(0, this._eventQueue.length - C.MAX_EVENT_QUEUE);
      }
    } catch(e) {}
  }

  _processEventQueue() {
    try {
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
    } catch(e) {
      this._isProcessingQueue = false;
    }
  }

  _removeUserFromRooms(username) {
    try {
      const seatInfo = this.userSeat.get(username);
      if (seatInfo && !seatInfo.isMulti) {
        const roomMan = this.rooms.get(seatInfo.room);
        if (roomMan) {
          roomMan.removeSeat(seatInfo.seat);
          this.broadcast(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
          this.updateRoomCount(seatInfo.room);
        }
        this.userSeat.delete(username);
        this.userRoom.delete(username);
      }
    } catch(e) {}
  }

  _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed || !room) return;
    
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const clientArray = Array.from(clients);
    const toRemove = new Set();
    
    for (let i = 0; i < clientArray.length; i += C.BATCH_SIZE) {
      const batch = clientArray.slice(i, Math.min(i + C.BATCH_SIZE, clientArray.length));
      
      for (const ws of batch) {
        if (!ws) {
          toRemove.add(ws);
          continue;
        }
        
        try {
          if (ws.readyState === 1 && !ws._closing && !this._cleaningUp.has(ws)) {
            ws.send(msgStr);
          } else {
            toRemove.add(ws);
          }
        } catch(e) {
          toRemove.add(ws);
        }
      }
    }
    
    if (toRemove.size > 0) {
      for (const ws of toRemove) {
        try {
          clients.delete(ws);
          if (ws && !this._cleaningUp.has(ws)) {
            this.cleanup(ws);
          }
        } catch(e) {}
      }
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
      if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
        return false;
      }
      
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      this.cleanup(ws);
      return false;
    }
  }

  updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      const roomMan = this.rooms.get(room);
      if (!roomMan) return 0;
      const count = roomMan.getCount();
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) {
      return 0;
    }
  }

  sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    
    try {
      if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
        return;
      }
    } catch(e) {
      return;
    }
    
    const roomMan = this.rooms.get(room);
    if (!roomMan) return;
    
    try {
      const allSeats = roomMan.getAllSeats();
      const allPoints = roomMan.getAllPoints();
      const selfSeat = this.userSeat.get(ws.username)?.seat;
      
      this.safeSend(ws, ["roomUserCount", room, roomMan.getCount()]);
      
      if (allSeats && Object.keys(allSeats).length > 0) {
        if (excludeSelf && selfSeat && allSeats[selfSeat]) {
          const filtered = { ...allSeats };
          delete filtered[selfSeat];
          if (Object.keys(filtered).length > 0) {
            this.safeSend(ws, ["allUpdateKursiList", room, filtered]);
          }
        } else {
          this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
        }
      }
      
      if (allPoints?.length > 0) {
        let filteredPoints = allPoints;
        if (excludeSelf && selfSeat) {
          filteredPoints = allPoints.filter(p => p.seat !== selfSeat);
        }
        if (filteredPoints.length > 0) {
          this.safeSend(ws, ["allPointsList", room, filteredPoints]);
        }
      }
    } catch(e) {}
  }

  cleanup(ws) {
    if (!ws || ws._cleaning || this._cleaningUp.has(ws)) {
      return;
    }
    
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
            connections.delete(ws);
            
            const seatInfo = this.userSeat.get(username);
            const isMulti = seatInfo?.isMulti === true;
            
            if (!isMulti && connections.size === 0) {
              this.userConnections.delete(username);
              
              if (seatInfo?.room) {
                const roomMan = this.rooms.get(seatInfo.room);
                if (roomMan) {
                  try {
                    const seatData = roomMan.getSeat(seatInfo.seat);
                    if (seatData?.namauser === username) {
                      roomMan.removeSeat(seatInfo.seat);
                      this.broadcast(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
                      this.updateRoomCount(seatInfo.room);
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

  async handleMessage(ws, raw) {
    if (!ws) return;
    
    try {
      if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
        return;
      }
    } catch(e) {
      return;
    }
    
    if (this._processingMessages.has(ws)) return;
    this._processingMessages.add(ws);
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MESSAGE_SIZE) return;
      
      let data;
      try { 
        data = JSON.parse(str); 
      } catch(e) { 
        return; 
      }
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
      try {
        this._processingMessages.delete(ws);
      } catch(e) {}
    }
  }

  _handleSetId(ws, username, idtarget) {
    if (!username || !idtarget) return;
    ws.username = username;
    ws.idtarget = idtarget;

    let connections = this.userConnections.get(username);
    if (!connections) connections = new Set();
    connections.add(ws);
    this.userConnections.set(username, connections);
    this.wsSet.add(ws);
  }

  _handleJoin(ws, roomName) {
    if (!roomName || !ROOMS_SET.has(roomName) || !ws.username) return;

    const lockKey = `join_${ws.username}_${roomName}`;
    if (this._joinLocks.has(lockKey)) return;
    this._joinLocks.set(lockKey, Date.now());

    try {
      this._removeUserFromRooms(ws.username);

      const roomMan = this.rooms.get(roomName);
      if (!roomMan || roomMan.getCount() >= C.MAX_SEATS) return;

      const seat = roomMan.addSeat(ws.username, "", "", 0, 0, 0, 0);
      if (!seat) return;

      ws.room = roomName;
      ws.roomname = roomName;

      this.userSeat.set(ws.username, { room: roomName, seat, isMulti: false });
      this.userRoom.set(ws.username, roomName);

      const roomClients = this.roomClients.get(roomName);
      if (roomClients) roomClients.add(ws);

      this.safeSend(ws, ["rooMasuk", seat, roomName]);
      this.sendAllStateTo(ws, roomName, true);
      this.updateRoomCount(roomName);
    } finally {
      this._joinLocks.delete(lockKey);
    }
  }

  _handleEventInternal(ws, data) {
    try {
      if (!ws || !data || !data[0]) return;
      const [evt, ...args] = data;
      
      switch(evt) {
        case "setIdTarget2":
          this._handleSetId(ws, args[0], args[1]);
          break;
        
        case "joinRoom":
          this._handleJoin(ws, args[0]);
          break;
        
        case "multiJoin": {
          const multiUsername = args[0];
          const multiRoomname = args[1];
          if (!multiUsername || !multiRoomname || this.closing || this.isDestroyed) break;
          
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
                this.broadcast(existingRoom, ["removeKursi", existingRoom, existingSeat]);
                this.updateRoomCount(existingRoom);
              }
              this.userSeat.delete(multiUsername);
              this.userRoom.delete(multiUsername);
            }
          } catch(e) {}
          
          const roomMan = this.rooms.get(multiRoomname);
          if (!roomMan || roomMan.getCount() >= C.MAX_SEATS) break;
          
          const seat = roomMan.addSeat(multiUsername, "", "", 0, 0, 0, 0);
          if (!seat) break;
          
          try {
            this.userSeat.set(multiUsername, { room: multiRoomname, seat, isMulti: true });
            this.userRoom.set(multiUsername, multiRoomname);
            
            let connections = this.userConnections.get(multiUsername);
            if (!connections) connections = new Set();
            if (!connections.has(ws)) connections.add(ws);
            this.userConnections.set(multiUsername, connections);
            
            this.wsActiveMulti.set(ws, { username: multiUsername, room: multiRoomname });
            const roomClients = this.roomClients.get(multiRoomname);
            if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
            
            this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
            this.broadcast(multiRoomname, ["roomUserCount", multiRoomname, roomMan.getCount()]);
          } catch(e) {}
          break;
        }
        
        case "exitMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          
          try {
            const seatInfo = this.userSeat.get(targetUsername);
            if (!seatInfo) break;
            
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
              this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
              this.broadcast(roomName, ["roomUserCount", roomName, roomMan.getCount()]);
            }
            
            this.userSeat.delete(targetUsername);
            this.userRoom.delete(targetUsername);
            
            const connections = this.userConnections.get(targetUsername);
            if (connections) {
              connections.delete(ws);
              if (connections.size === 0) {
                this.userConnections.delete(targetUsername);
              }
            }
            
            if (ws.username === targetUsername) {
              ws.username = null;
              ws.idtarget = null;
            }
          } catch(e) {}
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
          try {
            const seatInfo = this.userSeat.get(targetUsername);
            if (!seatInfo) break;
            
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
            
            this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
            this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
          } catch(e) {}
          break;
        }
        
        case "updateKursi": {
          try {
            const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
            const roomMan = this.rooms.get(kursiRoom);
            if (!roomMan) break;
            
            const lockKey = `kursi_${kursiRoom}_${kursiSeat}`;
            if (this._kursiLocks.has(lockKey)) break;
            
            this._kursiLocks.set(lockKey, Date.now());
            
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
                this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
              }
            } finally {
              this._kursiLocks.delete(lockKey);
            }
          } catch(e) {}
          break;
        }
        
        case "chat": {
          try {
            const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
            if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;
            
            const clients = this.roomClients.get(chatRoom);
            if (!clients || clients.size === 0) break;
            
            this._broadcastToRoom(chatRoom, JSON.stringify(["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]));
          } catch(e) {}
          break;
        }
        
        case "updatePoint": {
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
          break;
        }
        
        case "removeKursiAndPoint": {
          try {
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
              this.broadcast(removeRoom, ["removeKursi", removeRoom, removeSeat]);
              this.updateRoomCount(removeRoom);
            }
          } catch(e) {}
          break;
        }
        
        case "private": {
          try {
            const [privTarget, privNoimg, privMsg, privSender] = args;
            if (privTarget && privMsg) {
              const targetConns = this.userConnections.get(privTarget);
              if (targetConns) {
                for (const targetWs of targetConns) {
                  if (targetWs?.readyState === 1) {
                    this.safeSend(targetWs, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
                    break;
                  }
                }
              }
              this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
            }
          } catch(e) {}
          break;
        }
        
        case "gift": {
          try {
            const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
            if (giftRoom && ROOMS_SET.has(giftRoom)) {
              const clients = this.roomClients.get(giftRoom);
              if (!clients || clients.size === 0) break;
              this._broadcastToRoom(giftRoom, JSON.stringify(["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]));
            }
          } catch(e) {}
          break;
        }
        
        case "rollangak": {
          try {
            const [rollRoom, rollUser, rollAngka] = args;
            if (rollRoom && ROOMS_SET.has(rollRoom)) {
              const clients = this.roomClients.get(rollRoom);
              if (!clients || clients.size === 0) break;
              this._broadcastToRoom(rollRoom, JSON.stringify(["rollangakBroadcast", rollRoom, rollUser, rollAngka]));
            }
          } catch(e) {}
          break;
        }
        
        case "sendnotif": {
          try {
            const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
            if (notifTarget && notifMsg) {
              const targetConns = this.userConnections.get(notifTarget);
              if (targetConns) {
                for (const c of targetConns) {
                  if (c?.readyState === 1) {
                    this.safeSend(c, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
                    break;
                  }
                }
              }
            }
          } catch(e) {}
          break;
        }
        
        case "getCurrentNumber":
          try { this.safeSend(ws, ["currentNumber", this.currentNumber]); } catch(e) {}
          break;
        
        case "isUserOnline": {
          try {
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
            this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          } catch(e) {}
          break;
        }
        
        case "getOnlineUsers": {
          try {
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
            this.safeSend(ws, ["allOnlineUsers", users]);
          } catch(e) {}
          break;
        }
        
        case "getAllRoomsUserCount": {
          try {
            const counts = {};
            for (const room of ROOMS) {
              const rm = this.rooms.get(room);
              counts[room] = rm?.getCount() || 0;
            }
            this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          } catch(e) {}
          break;
        }
        
        case "getRoomUserCount": {
          try {
            const roomName = args[0];
            if (roomName && ROOMS_SET.has(roomName)) {
              const rm = this.rooms.get(roomName);
              this.safeSend(ws, ["roomUserCount", roomName, rm?.getCount() || 0]);
            }
          } catch(e) {}
          break;
        }
        
        case "setMuteType": {
          try {
            const [muteVal, muteRoom] = args;
            if (!muteRoom || !ROOMS_SET.has(muteRoom)) break;
            
            const rm = this.rooms.get(muteRoom);
            if (!rm) break;
            
            rm.setMuted(muteVal);
            this.broadcast(muteRoom, ["muteStatusChanged", muteRoom, rm.getMuted()]);
          } catch(e) {}
          break;
        }
      }
    } catch(e) {}
  }
}
