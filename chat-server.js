 // ==================== CHAT-SERVER.JS ====================
// VERSION: 6.0.0 - MEMORY ONLY, NO LOOP, NO LOGS
// ISOLATED ROOMS - CHAT TIDAK BOCOR KE ROOM LAIN

 const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_INTERVAL_MS: 900000,
  MAX_NUMBER: 6,
  BATCH_SIZE: 20,
  LOCK_TIMEOUT: 5000,
  MAX_EVENT_QUEUE: 100,
  MAX_PROCESS_TIME_MS: 500,
  USER_JOIN_LOCK_TIMEOUT: 10000,
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

  toJSON() {
    const seats = {};
    for (const [key, val] of this.seats) {
      seats[key] = val;
    }
    const points = {};
    for (const [key, val] of this.points) {
      points[key] = val;
    }
    return {
      seats: seats,
      points: points,
      muted: this.muted,
      number: this.number
    };
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
    this.env = env;
    this.ctx = state;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    this._restored = true;
    
    // ========== WEBSOCKET ==========
    this.wsSet = new Set();
    this.userConnections = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.roomClients = new Map();
    this.rooms = new Map();
    this.wsActiveMulti = new Map();
    
    // ========== PROCESSING ==========
    this._processingMessages = new Set();
    this._cleaningUp = new Set();
    this._eventQueue = [];
    this._isProcessingQueue = false;
    this._pendingTimeouts = new Set();
    
    // ========== LOCKS ==========
    this._joinLocks = new Map();
    this._kursiLocks = new Map();
    this._userJoinLock = new Map();
    
    // ========== USER TRACKING ==========
    this._userRoomMapping = new Map();
    
    // ========== NUMBER ==========
    this.currentNumber = 1;
    this._isNumberUpdating = false;
    
    // ========== INIT ROOMS ==========
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // ========== SCHEDULE ALARM PERTAMA ==========
    if (!this.closing && !this.isDestroyed) {
      this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
    }
  }

  // ========== CECK USER DI SEMUA ROOM ==========
  _isUserInAnyRoom(username) {
    if (!username) return null;
    
    const roomFromMapping = this._userRoomMapping.get(username);
    if (roomFromMapping) {
      const roomMan = this.rooms.get(roomFromMapping);
      if (roomMan) {
        for (const [seat, seatData] of roomMan.seats) {
          if (seatData && seatData.namauser === username) {
            return { room: roomFromMapping, seat: seat };
          }
        }
      }
      this._userRoomMapping.delete(username);
    }
    
    for (const [roomName, roomMan] of this.rooms) {
      if (!roomMan) continue;
      for (const [seat, seatData] of roomMan.seats) {
        if (seatData && seatData.namauser === username) {
          this._userRoomMapping.set(username, roomName);
          return { room: roomName, seat: seat };
        }
      }
    }
    
    return null;
  }

  // ========== FORCE REMOVE USER DARI ROOM ==========
  _forceRemoveUserFromRoom(username, roomName) {
    if (!username || !roomName) return false;
    
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return false;
    
    let removed = false;
    for (const [seat, seatData] of roomMan.seats) {
      if (seatData && seatData.namauser === username) {
        roomMan.removeSeat(seat);
        this.broadcast(roomName, ["removeKursi", roomName, seat]);
        this.updateRoomCount(roomName);
        removed = true;
        break;
      }
    }
    
    if (this._userRoomMapping.get(username) === roomName) {
      this._userRoomMapping.delete(username);
    }
    
    const seatInfo = this.userSeat.get(username);
    if (seatInfo && seatInfo.room === roomName) {
      this.userSeat.delete(username);
      this.userRoom.delete(username);
    }
    
    return removed;
  }

  // ========== REBUILD USER ROOM MAPPING ==========
  _rebuildUserRoomMapping() {
    this._userRoomMapping.clear();
    for (const [username, seatInfo] of this.userSeat) {
      if (seatInfo && seatInfo.room) {
        this._userRoomMapping.set(username, seatInfo.room);
      }
    }
  }

  // ========== VALIDASI USER HANYA DI 1 ROOM ==========
  _validateUserInOneRoomOnly(username) {
    if (!username) return true;
    
    const rooms = [];
    for (const [roomName, roomMan] of this.rooms) {
      if (!roomMan) continue;
      for (const [seat, seatData] of roomMan.seats) {
        if (seatData && seatData.namauser === username) {
          rooms.push({ room: roomName, seat: seat });
        }
      }
    }
    
    if (rooms.length > 1) {
      for (let i = 1; i < rooms.length; i++) {
        const { room, seat } = rooms[i];
        const roomMan = this.rooms.get(room);
        if (roomMan) {
          roomMan.removeSeat(seat);
          this.broadcast(room, ["removeKursi", room, seat]);
          this.updateRoomCount(room);
        }
      }
      
      if (rooms.length > 0) {
        this._userRoomMapping.set(username, rooms[0].room);
      }
      
      return false;
    }
    
    return true;
  }

  // ========== CLEANUP ON ALARM ==========
  _cleanupOnAlarm() {
    const allUsers = new Set();
    for (const [roomName, roomMan] of this.rooms) {
      if (!roomMan) continue;
      for (const [seat, seatData] of roomMan.seats) {
        if (seatData && seatData.namauser) {
          allUsers.add(seatData.namauser);
        }
      }
    }
    
    for (const username of allUsers) {
      this._validateUserInOneRoomOnly(username);
    }
    
    this._rebuildUserRoomMapping();
    this._cleanupDeadConnections();
  }

  // ========== ALARM ==========
  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    this._updateNumber();
    this._cleanupDeadConnections();
    this._cleanupStaleLocks();
    this._cleanupMemory();
    this._cleanupOnAlarm();
    
    this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
  }

  // ========== UPDATE NUMBER ==========
  _updateNumber() {
    if (this._isNumberUpdating || this.closing || this.isDestroyed) return;
    this._isNumberUpdating = true;
    try {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      for (const room of this.rooms.values()) {
        if (room) room.setNumber(this.currentNumber);
      }
      
      const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const [room, clients] of this.roomClients) {
        if (clients && clients.size > 0) {
          this._broadcastToRoom(room, numberMsg);
        }
      }
    } catch(e) {} finally {
      this._isNumberUpdating = false;
    }
  }

  // ========== CLEANUP DEAD CONNECTIONS ==========
  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      for (const ws of this.wsSet) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(ws);
        }
      }
      for (const ws of toRemove) {
        this.cleanup(ws);
      }
    } catch(e) {}
  }

  // ========== CLEANUP STALE LOCKS ==========
  _cleanupStaleLocks() {
    try {
      const now = Date.now();
      for (const [key, time] of this._joinLocks) {
        if (now - time > C.LOCK_TIMEOUT) this._joinLocks.delete(key);
      }
      for (const [key, time] of this._kursiLocks) {
        if (now - time > C.LOCK_TIMEOUT) this._kursiLocks.delete(key);
      }
      for (const [key, time] of this._userJoinLock) {
        if (now - time > C.USER_JOIN_LOCK_TIMEOUT) this._userJoinLock.delete(key);
      }
    } catch(e) {}
  }

  // ========== CLEANUP MEMORY ==========
  _cleanupMemory() {
    try {
      for (const [username, connections] of this.userConnections) {
        const toRemove = [];
        for (const conn of connections) {
          if (!conn || conn.readyState !== 1 || conn._closing) {
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
    } catch(e) {}
  }

  // ========== PROCESS EVENT QUEUE ==========
  _processEventQueue() {
    try {
      if (this._isProcessingQueue || this._eventQueue.length === 0) return;
      this._isProcessingQueue = true;
      const startTime = Date.now();
      let processed = 0;
      
      while (this._eventQueue.length > 0 && processed < 50) {
        if (Date.now() - startTime > C.MAX_PROCESS_TIME_MS) break;
        const item = this._eventQueue.shift();
        try { this._handleEventInternal(item.ws, item.data); } catch(e) {}
        processed++;
      }
      this._isProcessingQueue = false;
    } catch(e) {
      this._isProcessingQueue = false;
    }
  }

  // ========== BROADCAST TO ROOM ==========
  _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed || !room) return;
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const clientArray = Array.from(clients);
    const toRemove = new Set();
    
    for (let i = 0; i < clientArray.length; i += C.BATCH_SIZE) {
      const batch = clientArray.slice(i, Math.min(i + C.BATCH_SIZE, clientArray.length));
      for (const ws of batch) {
        if (!ws) { toRemove.add(ws); continue; }
        
        const wsRoom = ws.room || ws.roomname;
        if (wsRoom !== room) {
          toRemove.add(ws);
          continue;
        }
        
        try {
          if (ws.readyState === 1 && !ws._closing && !this._cleaningUp.has(ws)) {
            ws.send(msgStr);
          } else {
            toRemove.add(ws);
          }
        } catch(e) { toRemove.add(ws); }
      }
    }
    
    if (toRemove.size > 0) {
      for (const ws of toRemove) {
        try {
          clients.delete(ws);
          if (ws && !this._cleaningUp.has(ws)) this.cleanup(ws);
        } catch(e) {}
      }
    }
  }

  // ========== BROADCAST ==========
  broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try { this._broadcastToRoom(room, JSON.stringify(msg)); } catch(e) {}
  }

  // ========== SAFE SEND ==========
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

  // ========== UPDATE ROOM COUNT ==========
  updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      const roomMan = this.rooms.get(room);
      if (!roomMan) return 0;
      const count = roomMan.getCount();
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { return 0; }
  }

  // ========== SEND ALL STATE ==========
  sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    try {
      if (ws.readyState !== 1 || ws._closing) return;
    } catch(e) { return; }
    
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

  // ========== CLEANUP ==========
  cleanup(ws) {
    if (!ws || ws._cleaning || this._cleaningUp.has(ws)) return;
    ws._cleaning = true;
    this._cleaningUp.add(ws);
    
    try {
      const username = ws.username;
      const room = ws.room;
      
      if (room) {
        try { this.roomClients.get(room)?.delete(ws); } catch(e) {}
      }
      
      try {
        const activeData = this.wsActiveMulti.get(ws);
        if (activeData?.room) {
          this.roomClients.get(activeData.room)?.delete(ws);
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
              this._userRoomMapping.delete(username);
            }
          }
        } catch(e) {}
      }
      
      try { this.wsSet.delete(ws); } catch(e) {}
    } catch(e) {} finally {
      ws._cleaning = false;
      this._cleaningUp.delete(ws);
      try { if (ws && ws.readyState === 1) ws.close(1000, "Cleanup"); } catch(e) {}
    }
  }

  // ========== HANDLE MESSAGE ==========
  async handleMessage(ws, raw) {
    if (!ws) return;
    try {
      if (ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
        return;
      }
    } catch(e) { return; }
    
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
        if (!this._isProcessingQueue) this._processEventQueue();
      }
    } catch(e) {} finally {
      try { this._processingMessages.delete(ws); } catch(e) {}
    }
  }

  // ========== HANDLE EVENT INTERNAL ==========
  _handleEventInternal(ws, data) {
    try {
      if (!ws || !data || !data[0]) return;
      const [evt, ...args] = data;
      
      switch(evt) {
        case "getCurrentNumber":
          try { this.safeSend(ws, ["currentNumber", this.currentNumber]); } catch(e) {}
          break;
        
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
          
          const existing = this._isUserInAnyRoom(multiUsername);
          if (existing) {
            this._forceRemoveUserFromRoom(multiUsername, existing.room);
            this._userRoomMapping.delete(multiUsername);
            this.userSeat.delete(multiUsername);
            this.userRoom.delete(multiUsername);
          }
          
          const roomMan = this.rooms.get(multiRoomname);
          if (!roomMan || roomMan.getCount() >= C.MAX_SEATS) break;
          
          let seat = null;
          for (const [s, seatData] of roomMan.seats) {
            if (seatData && seatData.namauser === multiUsername) {
              seat = s;
              break;
            }
          }
          
          if (!seat) {
            seat = roomMan.addSeat(multiUsername, "", "", 0, 0, 0, 0);
            if (!seat) break;
          }
          
          try {
            const seatInfo = { room: multiRoomname, seat, isMulti: true };
            this._userRoomMapping.set(multiUsername, multiRoomname);
            this.userSeat.set(multiUsername, seatInfo);
            this.userRoom.set(multiUsername, multiRoomname);
            
            let connections = this.userConnections.get(multiUsername);
            if (!connections) connections = new Set();
            if (!connections.has(ws)) connections.add(ws);
            this.userConnections.set(multiUsername, connections);
            
            ws.serializeAttachment({
              username: multiUsername,
              seatInfo: seatInfo
            });
            
            this.wsActiveMulti.set(ws, { username: multiUsername, room: multiRoomname });
            
            for (const [otherRoom, clients] of this.roomClients) {
              if (otherRoom !== multiRoomname && clients) {
                clients.delete(ws);
              }
            }
            const roomClients = this.roomClients.get(multiRoomname);
            if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
            
            this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
            this.broadcast(multiRoomname, ["roomUserCount", multiRoomname, roomMan.getCount()]);
            
            this._validateUserInOneRoomOnly(multiUsername);
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
            this._userRoomMapping.delete(targetUsername);
            
            const connections = this.userConnections.get(targetUsername);
            if (connections) {
              connections.delete(ws);
              if (connections.size === 0) this.userConnections.delete(targetUsername);
            }
            
            if (ws.username === targetUsername) {
              ws.username = null;
              ws.idtarget = null;
            }
            
            ws.serializeAttachment({});
          } catch(e) {}
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
          try {
            const seatInfo = this.userSeat.get(targetUsername);
            if (!seatInfo) break;
            
            const existing = this._isUserInAnyRoom(targetUsername);
            if (existing && existing.room !== seatInfo.room) {
              seatInfo.room = existing.room;
              seatInfo.seat = existing.seat;
              this.userSeat.set(targetUsername, seatInfo);
              this.userRoom.set(targetUsername, existing.room);
              this._userRoomMapping.set(targetUsername, existing.room);
              ws.serializeAttachment({
                username: targetUsername,
                seatInfo: seatInfo
              });
            }
            
            const roomName = seatInfo.room;
            const seatNumber = seatInfo.seat;
            
            const oldActive = this.wsActiveMulti.get(ws);
            if (oldActive?.room) {
              const oldClients = this.roomClients.get(oldActive.room);
              if (oldClients) oldClients.delete(ws);
            }
            
            this.wsActiveMulti.set(ws, { username: targetUsername, room: roomName });
            
            for (const [otherRoom, clients] of this.roomClients) {
              if (otherRoom !== roomName && clients) {
                clients.delete(ws);
              }
            }
            const roomClients = this.roomClients.get(roomName);
            if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
            
            ws.username = targetUsername;
            ws.idtarget = targetUsername;
            ws.room = roomName;
            ws.roomname = roomName;
            
            ws.serializeAttachment({
              username: targetUsername,
              seatInfo: seatInfo
            });
            
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
            
            const seatInfo = this.userSeat.get(chatUser);
            if (!seatInfo || seatInfo.room !== chatRoom) break;
            
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
                  this._userRoomMapping.delete(username);
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
              const receiverSeat = this.userSeat.get(giftReceiver);
              if (!receiverSeat || receiverSeat.room !== giftRoom) break;
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
              const userSeat = this.userSeat.get(rollUser);
              if (!userSeat || userSeat.room !== rollRoom) break;
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
            this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
            this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
          } catch(e) {}
          break;
        }

        case "modwarning": {
          try {
            const modRoom = args[0];
            if (modRoom && ROOMS_SET.has(modRoom)) {
              this.broadcast(modRoom, ["modwarning", modRoom]);
            }
          } catch(e) {}
          break;
        }

        case "getMuteType": {
          try {
            const getMuteRoom = args[0];
            if (getMuteRoom && ROOMS_SET.has(getMuteRoom)) {
              const rm = this.rooms.get(getMuteRoom);
              this.safeSend(ws, ["muteTypeResponse", rm?.getMuted() || false, getMuteRoom]);
            }
          } catch(e) {}
          break;
        }
        
        case "onDestroy":
          this.cleanup(ws);
          break;
        
        default:
          try { this.safeSend(ws, ["error", `Unknown event: ${evt}`]); } catch(e) {}
          break;
      }
    } catch(e) {}
  }

  // ========== HANDLE SET ID ==========
  _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    
    if (ws.readyState !== 1) {
      try { this.cleanup(ws); } catch(e) {}
      return;
    }
    
    const existingSeatInfo = this.userSeat.get(username);
    if (existingSeatInfo?.isMulti === true) {
      const roomName = existingSeatInfo.room;
      const seat = existingSeatInfo.seat;
      const roomMan = this.rooms.get(roomName);
      if (roomMan) {
        roomMan.removeSeat(seat);
        this.broadcast(roomName, ["removeKursi", roomName, seat]);
        this.updateRoomCount(roomName);
      }
      
      this.userSeat.delete(username);
      this.userRoom.delete(username);
      this._userRoomMapping.delete(username);
      
      for (const [wsKey, data] of this.wsActiveMulti) {
        if (data && data.username === username) {
          this.wsActiveMulti.delete(wsKey);
          if (data.room) {
            const clients = this.roomClients.get(data.room);
            if (clients) clients.delete(wsKey);
          }
        }
      }
      
      const connections = this.userConnections.get(username);
      if (connections) {
        for (const conn of connections) {
          this.wsSet.delete(conn);
        }
        this.userConnections.delete(username);
      }
    }
    
    const existing = this._isUserInAnyRoom(username);
    if (existing) {
      const roomMan = this.rooms.get(existing.room);
      if (roomMan) {
        roomMan.removeSeat(existing.seat);
        this.broadcast(existing.room, ["removeKursi", existing.room, existing.seat]);
        this.updateRoomCount(existing.room);
      }
      this.userSeat.delete(username);
      this.userRoom.delete(username);
      this._userRoomMapping.delete(username);
    }
    
    ws.username = username;
    ws.idtarget = username;
    ws.room = null;
    ws.roomname = null;
    ws._closing = false;
    
    ws.serializeAttachment({ username: username });
    
    let connections = this.userConnections.get(username);
    if (!connections) { 
      connections = new Set(); 
      this.userConnections.set(username, connections); 
    }
    if (!connections.has(ws)) connections.add(ws);
    if (!this.wsSet.has(ws)) this.wsSet.add(ws);
    
    if (isNewUser) { 
      this.safeSend(ws, ["joinroomawal"]); 
    } else { 
      this.safeSend(ws, ["needJoinRoom"]); 
    }
  }

  // ========== HANDLE JOIN ==========
  _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
      return false;
    }
    
    const username = ws.username;
    const lockKey = `join_user_${username}`;
    
    if (this._userJoinLock.has(lockKey)) {
      const lockTime = this._userJoinLock.get(lockKey);
      if (Date.now() - lockTime < C.USER_JOIN_LOCK_TIMEOUT) {
        this.safeSend(ws, ["joinInProgress", "Please wait..."]);
        return false;
      } else {
        this._userJoinLock.delete(lockKey);
      }
    }
    
    this._userJoinLock.set(lockKey, Date.now());
    try { 
      return this._joinInternal(ws, roomName, username); 
    } finally { 
      this._userJoinLock.delete(lockKey); 
    }
  }

  // ========== JOIN INTERNAL ==========
  async _joinInternal(ws, roomName, username) {
    const existing = this._isUserInAnyRoom(username);
    
    if (existing && existing.room !== roomName) {
      this._forceRemoveUserFromRoom(username, existing.room);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) {
      this.safeSend(ws, ["error", "Room not found"]);
      return false;
    }
    
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data && data.namauser === username) {
        seat = s;
        break;
      }
    }
    
    if (!seat) {
      if (roomMan.getCount() >= C.MAX_SEATS) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      seat = roomMan.getAvailableSeat();
      if (!seat) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      roomMan.addSeat(username, "", "", 0, 0, 0, 0);
    }
    
    try {
      const seatInfo = { room: roomName, seat, isMulti: false };
      
      this._userRoomMapping.set(username, roomName);
      this.userSeat.set(username, seatInfo);
      this.userRoom.set(username, roomName);
      
      ws.room = roomName;
      ws.roomname = roomName;
      ws.idtarget = username;
      
      ws.serializeAttachment({
        username: username,
        seatInfo: seatInfo
      });
      
      for (const [otherRoom, clients] of this.roomClients) {
        if (otherRoom !== roomName && clients) {
          clients.delete(ws);
        }
      }
      const roomClients = this.roomClients.get(roomName);
      if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
      
      this.wsActiveMulti.delete(ws);
      
      this.safeSend(ws, ["rooMasuk", seat, roomName]);
      this.safeSend(ws, ["numberKursiSaya", seat]);
      this.safeSend(ws, ["muteTypeResponse", roomMan.getMuted(), roomName]);
      this.safeSend(ws, ["roomUserCount", roomName, roomMan.getCount()]);
      
      this.updateRoomCount(roomName);
      
      this._validateUserInOneRoomOnly(username);
      
      setTimeout(() => {
        try {
          if (ws && ws.readyState === 1 && !this.closing && !this.isDestroyed) {
            this.sendAllStateTo(ws, roomName, true);
          }
        } catch(e) {}
      }, 1000);
      
      return true;
    } catch(e) { 
      return false; 
    }
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
      
      if (this.wsSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server full", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      
      try { this.ctx.acceptWebSocket(server); } 
      catch(e) { return new Response("WebSocket acceptance failed", { status: 500 }); }
      
      server.username = null;
      server.room = null;
      server.roomname = null;
      server.idtarget = null;
      server._closing = false;
      server._wsId = Date.now() + Math.random();
      
      server.serializeAttachment({});
      
      if (!this.wsSet.has(server)) this.wsSet.add(server);
      
      return new Response(null, { status: 101, webSocket: client });
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ==================== WEBSOCKET HANDLERS ====================
  
  async webSocketMessage(ws, msg) {
    if (!ws || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) return;
    
    try { await this.handleMessage(ws, msg); } catch(e) {}
  }

  async webSocketClose(ws) { 
    if (!ws) return;
    try { this.cleanup(ws); } catch(e) {}
  }

  async webSocketError(ws) { 
    if (!ws) return;
    try { this.cleanup(ws); } catch(e) {}
  }

  // ==================== DESTROY ====================
  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    this._joinLocks.clear();
    this._kursiLocks.clear();
    this._userJoinLock.clear();
    
    for (const timeout of this._pendingTimeouts) {
      clearTimeout(timeout);
    }
    this._pendingTimeouts.clear();
    
    const wsCopy = Array.from(this.wsSet);
    for (const ws of wsCopy) {
      if (ws?.readyState === 1) {
        try { ws.send(JSON.stringify(["serverShutdown", "Server shutting down"])); } catch(e) {}
        try { ws.close(1000, "Shutdown"); } catch(e) {}
      }
      try { this.cleanup(ws); } catch(e) {}
    }
    
    this.wsSet.clear();
    this.userConnections.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this._userRoomMapping.clear();
    this.wsActiveMulti.clear();
    this.roomClients.clear();
    this.rooms.clear();
    this._processingMessages.clear();
    this._cleaningUp.clear();
    this._eventQueue.clear();
  }
}

// ==================== EXPORT ====================
export default ChatServer;
