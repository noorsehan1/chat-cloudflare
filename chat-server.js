// ==================== CHAT-SERVER-HIBERNATION-NO-PING.JS ====================
// VERSION: 9.3.3 - DIRECT STORAGE SAVE NO DELAY

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_INTERVAL_MS: 900000,
  MAX_NUMBER: 6,
};

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.ctx = state;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    
    // ===== ROOM CLIENTS =====
    this.roomClients = new Map();
    for (const room of ROOMS) {
      this.roomClients.set(room, new Set());
    }
    
    // ===== CACHE DATA =====
    this._roomsDataCache = {};
    this._userSeatDataCache = {};
    this.currentNumber = 1;
    
    // ===== USER COUNTER =====
    this._onlineUsers = new Set();
    this._userCounts = {};
    for (const room of ROOMS) {
      this._userCounts[room] = 0;
    }
    
    this._isNumberUpdating = false;
    this._isRestoring = false;
    this._lastRefreshTime = 0;
    
    this._restoreAllState().then(() => {});
  }

  // ============ WEBSOCKET MANAGEMENT ============

  _getActiveWebSockets() {
    try {
      return this.ctx.getWebSockets();
    } catch(e) {
      return [];
    }
  }

  _refreshRoomClients(force = false) {
    const now = Date.now();
    if (!force && (now - this._lastRefreshTime) < 100) {
      return;
    }
    this._lastRefreshTime = now;
    
    for (const room of ROOMS) {
      this.roomClients.set(room, new Set());
    }
    
    const webSockets = this._getActiveWebSockets();
    for (const ws of webSockets) {
      try {
        let room = ws._cachedRoom;
        let username = ws._cachedUsername;
        
        if (!room || !username) {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username && attachment.room) {
            room = attachment.room;
            username = attachment.username;
            ws._cachedRoom = room;
            ws._cachedUsername = username;
          }
        }
        
        if (room && username) {
          const roomClients = this.roomClients.get(room);
          if (roomClients) {
            roomClients.add(ws);
          }
        }
      } catch(e) {}
    }
  }

  // ============ CORE: DIRECT STORAGE SAVE ============

  async _saveToStorageDirect() {
    try {
      await this.ctx.storage.put({
        roomsData: this._roomsDataCache,
        userSeatData: this._userSeatDataCache,
        currentNumber: this.currentNumber,
        userCounts: this._userCounts,
        onlineUsers: Array.from(this._onlineUsers)
      });
    } catch(e) {
      console.error("Direct storage save error:", e);
      throw e;
    }
  }

  // ============ USER COUNT MANAGEMENT ============

  async _updateUserCounts() {
    try {
      const newCounts = {};
      let totalUsers = 0;
      
      for (const room of ROOMS) {
        const roomData = this._roomsDataCache[room];
        const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
        newCounts[room] = count;
        totalUsers += count;
      }
      
      this._userCounts = newCounts;
      this._onlineUsers.clear();
      for (const [username] of Object.entries(this._userSeatDataCache)) {
        this._onlineUsers.add(username);
      }
      
      // LANGSUNG SAVE
      await this._saveToStorageDirect();
      
      return { counts: newCounts, total: totalUsers };
    } catch(e) {
      return { counts: this._userCounts, total: this._onlineUsers.size };
    }
  }

  // ============ STORAGE OPERATIONS ============

  async _loadFromStorage() {
    try {
      if (Object.keys(this._roomsDataCache).length === 0 && 
          Object.keys(this._userSeatDataCache).length === 0) {
        const storage = await this.ctx.storage.get(["roomsData", "userSeatData", "currentNumber", "userCounts", "onlineUsers"]);
        this._roomsDataCache = storage.roomsData || {};
        this._userSeatDataCache = storage.userSeatData || {};
        this.currentNumber = storage.currentNumber || 1;
        this._userCounts = storage.userCounts || {};
        this._onlineUsers = new Set(storage.onlineUsers || []);
      }
      
      return {
        roomsData: this._roomsDataCache,
        userSeatData: this._userSeatDataCache,
        currentNumber: this.currentNumber,
        userCounts: this._userCounts,
        onlineUsers: Array.from(this._onlineUsers)
      };
    } catch(e) {
      return { 
        roomsData: {}, 
        userSeatData: {}, 
        currentNumber: 1,
        userCounts: {},
        onlineUsers: []
      };
    }
  }

  // ============ USER MANAGEMENT ============

  _isUsernameExists(username) {
    if (!username) return false;
    return this._onlineUsers.has(username) || 
           this._userSeatDataCache.hasOwnProperty(username);
  }

  // Hapus user dari semua room - LANGSUNG SAVE
  async _removeUserFromAllRooms(username) {
    if (!username) return false;
    
    let removed = false;
    
    for (const [roomName, roomData] of Object.entries(this._roomsDataCache)) {
      if (!roomData || !roomData.seats) continue;
      
      let seatToRemove = null;
      for (const [seat, data] of Object.entries(roomData.seats)) {
        if (data && data.namauser === username) {
          seatToRemove = parseInt(seat);
          break;
        }
      }
      
      if (seatToRemove !== null) {
        delete roomData.seats[seatToRemove];
        if (roomData.points) {
          delete roomData.points[seatToRemove];
        }
        removed = true;
        
        this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
        await this.updateRoomCount(roomName);
        await this._deleteRoomIfEmpty(roomName);
      }
    }
    
    if (this._userSeatDataCache[username]) {
      delete this._userSeatDataCache[username];
      removed = true;
    }
    
    if (this._onlineUsers.has(username)) {
      this._onlineUsers.delete(username);
      removed = true;
    }
    
    if (removed) {
      // LANGSUNG SAVE
      await this._saveToStorageDirect();
      await this._updateUserCounts();
    }
    
    return removed;
  }

  async _isUserInAnyRoom(username) {
    if (!username) return null;
    
    const seatInfo = this._userSeatDataCache[username];
    if (seatInfo && seatInfo.room) {
      const roomData = this._roomsDataCache[seatInfo.room];
      if (roomData && roomData.seats) {
        for (const [seat, data] of Object.entries(roomData.seats)) {
          if (data && data.namauser === username) {
            return { 
              room: seatInfo.room, 
              seat: parseInt(seat), 
              isMulti: seatInfo.isMulti || false 
            };
          }
        }
      }
      delete this._userSeatDataCache[username];
      await this._saveToStorageDirect();
      this._onlineUsers.delete(username);
      await this._saveToStorageDirect();
    }
    
    for (const [roomName, roomData] of Object.entries(this._roomsDataCache)) {
      if (!roomData || !roomData.seats) continue;
      for (const [seat, data] of Object.entries(roomData.seats)) {
        if (data && data.namauser === username) {
          this._userSeatDataCache[username] = { 
            room: roomName, 
            seat: parseInt(seat), 
            isMulti: false 
          };
          await this._saveToStorageDirect();
          this._onlineUsers.add(username);
          await this._saveToStorageDirect();
          return { room: roomName, seat: parseInt(seat), isMulti: false };
        }
      }
    }
    
    return null;
  }

  _isUserMulti(username) {
    if (!username) return false;
    const seatInfo = this._userSeatDataCache[username];
    return seatInfo ? (seatInfo.isMulti || false) : false;
  }

  async _removeUserFromRoom(username, roomName) {
    if (!username || !roomName) return false;
    
    const roomData = this._roomsDataCache[roomName];
    if (!roomData || !roomData.seats) return false;
    
    let seat = null;
    for (const [s, data] of Object.entries(roomData.seats)) {
      if (data && data.namauser === username) {
        seat = parseInt(s);
        break;
      }
    }
    
    if (!seat) return false;
    
    delete roomData.seats[seat];
    if (roomData.points) {
      delete roomData.points[seat];
    }
    delete this._userSeatDataCache[username];
    this._onlineUsers.delete(username);
    
    await this._saveToStorageDirect();
    await this._updateUserCounts();
    
    this.broadcast(roomName, ["removeKursi", roomName, seat]);
    await this.updateRoomCount(roomName);
    await this._deleteRoomIfEmpty(roomName);
    
    return true;
  }

  async _updateKursi(roomName, seat, data) {
    const roomData = this._roomsDataCache[roomName];
    if (!roomData || !roomData.seats || !roomData.seats[seat]) return false;
    
    roomData.seats[seat] = {
      noimageUrl: data.noimageUrl || "",
      namauser: data.namauser || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0
    };
    
    // LANGSUNG SAVE
    await this._saveToStorageDirect();
    return true;
  }

  async _updatePoint(roomName, seat, x, y, fast) {
    const roomData = this._roomsDataCache[roomName];
    if (!roomData || !roomData.seats || !roomData.seats[seat]) return false;
    
    if (!roomData.points) roomData.points = {};
    roomData.points[seat] = { x: x || 0, y: y || 0, fast: !!fast };
    
    // LANGSUNG SAVE
    await this._saveToStorageDirect();
    return true;
  }

  async _deleteUserSeat(username) {
    delete this._userSeatDataCache[username];
    await this._saveToStorageDirect();
    this._onlineUsers.delete(username);
    await this._saveToStorageDirect();
    await this._updateUserCounts();
  }

  async _deleteRoomIfEmpty(roomName) {
    const roomData = this._roomsDataCache[roomName];
    if (!roomData) return;
    
    const hasSeats = roomData.seats && Object.keys(roomData.seats).length > 0;
    const hasPoints = roomData.points && Object.keys(roomData.points).length > 0;
    
    if (!hasSeats && !hasPoints) {
      delete this._roomsDataCache[roomName];
      await this._saveToStorageDirect();
      await this._updateUserCounts();
    }
  }

  // ============ JOIN HANDLING ============

  async _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
      return false;
    }
    
    const username = ws.username;
    
    await this._removeUserFromAllRooms(username);
    
    let roomData = this._roomsDataCache[roomName];
    if (!roomData) {
      roomData = { seats: {}, points: {}, muted: false, number: 1 };
      this._roomsDataCache[roomName] = roomData;
    }
    
    let seat = null;
    
    const seatCount = Object.keys(roomData.seats).length;
    if (seatCount >= C.MAX_SEATS) {
      this.safeSend(ws, ["roomFull", roomName]);
      return false;
    }
    
    for (let s = 1; s <= C.MAX_SEATS; s++) {
      if (!roomData.seats[s]) {
        seat = s;
        break;
      }
    }
    
    if (!seat) {
      this.safeSend(ws, ["roomFull", roomName]);
      return false;
    }
    
    roomData.seats[seat] = {
      noimageUrl: "",
      namauser: username,
      color: "",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0
    };
    
    const seatInfo = { room: roomName, seat, isMulti: false };
    this._userSeatDataCache[username] = seatInfo;
    this._onlineUsers.add(username);
    
    // LANGSUNG SAVE SEKALIGUS
    await this._saveToStorageDirect();
    await this._updateUserCounts();
    
    ws.serializeAttachment({
      username: username,
      room: roomName,
      seat: seat,
      isMulti: false,
      seatInfo: seatInfo
    });
    
    ws._cachedRoom = roomName;
    ws._cachedUsername = username;
    ws.username = username;
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = username;
    ws._isMulti = false;
    ws._multiRoom = null;
    ws._multiSeat = null;
    
    this._refreshRoomClients(true);
    
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomData.muted || false, roomName]);
    
    const count = Object.keys(roomData.seats).length;
    this.safeSend(ws, ["roomUserCount", roomName, count]);
    this.broadcast(roomName, ["roomUserCount", roomName, count]);
    
    setTimeout(() => {
      try {
        if (ws && ws.readyState === 1) {
          this.sendAllStateTo(ws, roomName, true);
        }
      } catch(e) {}
    }, 1000);
    
    return true;
  }

  // ============ CLEANUP ============

  async _cleanupUserOnDisconnect(ws) {
    try {
      const username = ws.username || ws._cachedUsername;
      if (!username) return;
      
      await this._removeUserFromAllRooms(username);
      this._refreshRoomClients(true);
    } catch(e) {}
  }

  // ============ BROADCAST ============

  _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed || !room) return;
    
    this._refreshRoomClients(false);
    
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const clientArray = Array.from(clients);
    const toRemove = new Set();
    
    for (const ws of clientArray) {
      if (!ws) { toRemove.add(ws); continue; }
      
      try {
        const wsRoom = ws._cachedRoom || ws.room || ws.roomname;
        if (wsRoom !== room) {
          toRemove.add(ws);
          continue;
        }
        
        if (ws.readyState === 1 && !ws._closing) {
          ws.send(msgStr);
        } else {
          toRemove.add(ws);
        }
      } catch(e) { toRemove.add(ws); }
    }
    
    if (toRemove.size > 0) {
      for (const ws of toRemove) {
        try { clients.delete(ws); } catch(e) {}
      }
    }
  }

  broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try { this._broadcastToRoom(room, JSON.stringify(msg)); } catch(e) {}
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
      return false;
    }
  }

  // ============ STATE MANAGEMENT ============

  async updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      const roomData = this._roomsDataCache[room];
      if (!roomData || !roomData.seats) return 0;
      const count = Object.keys(roomData.seats).length;
      
      this._userCounts[room] = count;
      await this._saveToStorageDirect();
      
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { return 0; }
  }

  async sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    try {
      if (ws.readyState !== 1 || ws._closing) return;
    } catch(e) { return; }
    
    const roomData = this._roomsDataCache[room];
    if (!roomData) return;
    
    try {
      const allSeats = roomData.seats || {};
      const allPoints = roomData.points || {};
      
      const userSeat = this._userSeatDataCache[ws.username];
      const selfSeat = userSeat?.seat;
      
      const count = Object.keys(allSeats).length;
      this.safeSend(ws, ["roomUserCount", room, count]);
      
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
      
      if (allPoints && Object.keys(allPoints).length > 0) {
        let filteredPoints = Object.entries(allPoints).map(([seat, point]) => ({
          seat: parseInt(seat),
          x: point.x,
          y: point.y,
          fast: point.fast ? 1 : 0
        }));
        
        if (excludeSelf && selfSeat) {
          filteredPoints = filteredPoints.filter(p => p.seat !== selfSeat);
        }
        
        if (filteredPoints.length > 0) {
          this.safeSend(ws, ["allPointsList", room, filteredPoints]);
        }
      }
    } catch(e) {}
  }

  // ============ ALARM / NUMBER UPDATER ============

  async alarm() {
    if (this.closing || this.isDestroyed) return;
    
    await this._updateNumber();
    await this._cleanupStorage();
    
    if (!this.closing && !this.isDestroyed) {
      this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
    }
  }

  async _updateNumber() {
    if (this._isNumberUpdating || this.closing || this.isDestroyed) return;
    this._isNumberUpdating = true;
    try {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      let changed = false;
      
      for (const [roomName, roomData] of Object.entries(this._roomsDataCache)) {
        if (roomData) {
          roomData.number = this.currentNumber;
          changed = true;
        }
      }
      
      if (changed) {
        // LANGSUNG SAVE
        await this._saveToStorageDirect();
      } else {
        await this.ctx.storage.put("currentNumber", this.currentNumber);
      }
      
      const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
      
      this._refreshRoomClients(true);
      for (const [room, clients] of this.roomClients) {
        if (clients && clients.size > 0) {
          this._broadcastToRoom(room, numberMsg);
        }
      }
      
    } catch(e) {
      const storage = await this.ctx.storage.get(["currentNumber", "roomsData"]);
      if (storage.currentNumber !== undefined) this.currentNumber = storage.currentNumber;
      if (storage.roomsData !== undefined) this._roomsDataCache = storage.roomsData;
    } finally {
      this._isNumberUpdating = false;
    }
  }

  async _cleanupStorage() {
    try {
      const storage = await this._loadFromStorage();
      const roomsData = storage.roomsData || {};
      const userSeatData = storage.userSeatData || {};
      
      let changed = false;
      
      const connectedUsers = new Set();
      const webSockets = this._getActiveWebSockets();
      for (const ws of webSockets) {
        try {
          const uname = ws._cachedUsername || ws.username || ws.deserializeAttachment()?.username;
          if (uname) {
            connectedUsers.add(uname);
          }
        } catch(e) {}
      }
      
      for (const [username, seatInfo] of Object.entries(userSeatData)) {
        if (!seatInfo || !seatInfo.room) {
          delete userSeatData[username];
          changed = true;
          continue;
        }
        
        if (!connectedUsers.has(username)) {
          delete userSeatData[username];
          changed = true;
        }
      }
      
      for (const [roomName, roomData] of Object.entries(roomsData)) {
        const hasSeats = roomData.seats && Object.keys(roomData.seats).length > 0;
        const hasPoints = roomData.points && Object.keys(roomData.points).length > 0;
        
        if (!hasSeats && !hasPoints) {
          delete roomsData[roomName];
          changed = true;
        }
      }
      
      if (changed) {
        this._roomsDataCache = roomsData;
        this._userSeatDataCache = userSeatData;
        await this._saveToStorageDirect();
      }
      
    } catch(e) {}
  }

  // ============ WEBSOCKET EVENT HANDLERS ============

  async webSocketMessage(ws, message) {
    if (!ws || ws._closing || this.closing || this.isDestroyed) return;
    
    try {
      let attachment = null;
      try {
        attachment = ws.deserializeAttachment();
      } catch(e) {}
      
      if (attachment && attachment.username) {
        ws.username = attachment.username;
        ws.room = attachment.room;
        ws.roomname = attachment.room;
        ws.idtarget = attachment.username;
        ws._isMulti = attachment.isMulti || false;
        ws._multiRoom = attachment.multiRoom || null;
        ws._multiSeat = attachment.multiSeat || null;
        ws._cachedUsername = attachment.username;
        ws._cachedRoom = attachment.room;
        
        if (attachment.seatInfo) {
          this._userSeatDataCache[attachment.username] = attachment.seatInfo;
        }
      }
      
      await this.handleMessage(ws, message);
    } catch(e) {
      console.error("WebSocket message error:", e);
    }
  }

  async webSocketClose(ws, code, reason, wasClean) {
    if (!ws) return;
    try {
      await this._cleanupUserOnDisconnect(ws);
    } catch(e) {}
  }

  async webSocketError(ws, error) {
    if (!ws) return;
    try {
      await this._cleanupUserOnDisconnect(ws);
    } catch(e) {}
  }

  // ============ HANDLE SET ID ============

  async _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    
    if (ws.readyState !== 1) return;
    
    await this._removeUserFromAllRooms(username);
    
    ws.username = username;
    ws.idtarget = username;
    ws.room = null;
    ws.roomname = null;
    ws._closing = false;
    ws._isMulti = false;
    ws._multiRoom = null;
    ws._multiSeat = null;
    ws._cachedUsername = username;
    ws._cachedRoom = null;
    
    ws.serializeAttachment({ username: username });
    
    if (isNewUser) { 
      this.safeSend(ws, ["joinroomawal"]); 
    } else { 
      this.safeSend(ws, ["needJoinRoom"]); 
    }
  }

  // ============ HANDLE MESSAGE ============

  async handleMessage(ws, raw) {
    if (!ws) return;
    try {
      if (ws.readyState !== 1 || ws._closing || this.closing || this.isDestroyed) {
        return;
      }
    } catch(e) { return; }
    
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
      
      await this._handleEventInternal(ws, [evt, ...args]);
    } catch(e) {}
  }

  // ============ EVENT HANDLER INTERNAL ============

  async _handleEventInternal(ws, data) {
    try {
      if (!ws || !data || !data[0]) return;
      const [evt, ...args] = data;
      
      switch(evt) {
        case "resetServer": {
          const result = await this.resetAllData();
          this.safeSend(ws, ["resetResult", result]);
          break;
        }
        
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        
        case "setIdTarget2": {
          const username = args[0];
          const isNewUser = args[1];
          
          if (username) {
            await this._removeUserFromAllRooms(username);
          }
          
          await this._handleSetId(ws, username, isNewUser);
          break;
        }
        
        case "joinRoom":
          await this._handleJoin(ws, args[0]);
          break;
        
        case "multiJoin": {
          const multiUsername = args[0];
          const multiRoomname = args[1];
          
          if (!multiUsername || !multiRoomname) {
            this.safeSend(ws, ["multiJoinError", "Username dan room harus diisi"]);
            break;
          }
          
          if (!ROOMS_SET.has(multiRoomname)) {
            this.safeSend(ws, ["multiJoinError", "Room tidak valid"]);
            break;
          }
          
          await this._removeUserFromAllRooms(multiUsername);
          
          let roomData = this._roomsDataCache[multiRoomname];
          if (!roomData) {
            roomData = { seats: {}, points: {}, muted: false, number: 1 };
            this._roomsDataCache[multiRoomname] = roomData;
          }
          
          let seat = null;
          
          const seatCount = Object.keys(roomData.seats).length;
          if (seatCount >= C.MAX_SEATS) {
            this.safeSend(ws, ["multiJoinError", "Room penuh"]);
            break;
          }
          
          for (let s = 1; s <= C.MAX_SEATS; s++) {
            if (!roomData.seats[s]) {
              seat = s;
              break;
            }
          }
          
          if (!seat) {
            this.safeSend(ws, ["multiJoinError", "Tidak ada kursi tersedia"]);
            break;
          }
          
          roomData.seats[seat] = {
            noimageUrl: "",
            namauser: multiUsername,
            color: "",
            itembawah: 0,
            itematas: 0,
            vip: 0,
            viptanda: 0
          };
          
          const seatInfo = { 
            room: multiRoomname, 
            seat, 
            isMulti: true,
            multiRoom: multiRoomname,
            multiSeat: seat
          };
          this._userSeatDataCache[multiUsername] = seatInfo;
          this._onlineUsers.add(multiUsername);
          
          // LANGSUNG SAVE
          await this._saveToStorageDirect();
          await this._updateUserCounts();
          
          ws.serializeAttachment({
            username: multiUsername,
            room: multiRoomname,
            seat: seat,
            isMulti: true,
            multiRoom: multiRoomname,
            multiSeat: seat,
            seatInfo: seatInfo
          });
          
          ws._cachedUsername = multiUsername;
          ws._cachedRoom = multiRoomname;
          ws.username = multiUsername;
          ws.idtarget = multiUsername;
          ws.room = multiRoomname;
          ws.roomname = multiRoomname;
          ws._isMulti = true;
          ws._multiRoom = multiRoomname;
          ws._multiSeat = seat;
          
          const webSockets = this._getActiveWebSockets();
          for (const wsKey of webSockets) {
            if (wsKey === ws) continue;
            try {
              const uname = wsKey._cachedUsername || 
                            wsKey.username || 
                            wsKey.deserializeAttachment()?.username;
              if (uname === multiUsername && wsKey.readyState === 1) {
                wsKey.serializeAttachment({
                  username: multiUsername,
                  room: multiRoomname,
                  seat: seat,
                  isMulti: true,
                  multiRoom: multiRoomname,
                  multiSeat: seat,
                  seatInfo: seatInfo
                });
                wsKey._cachedUsername = multiUsername;
                wsKey._cachedRoom = multiRoomname;
                wsKey.username = multiUsername;
                wsKey.idtarget = multiUsername;
                wsKey.room = multiRoomname;
                wsKey.roomname = multiRoomname;
                wsKey._isMulti = true;
                wsKey._multiRoom = multiRoomname;
                wsKey._multiSeat = seat;
                wsKey._closing = false;
              }
            } catch(e) {}
          }
          
          this._refreshRoomClients(true);
          
          this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
          this.broadcast(multiRoomname, ["roomUserCount", multiRoomname, Object.keys(roomData.seats).length]);
          
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
          
          if (!targetUsername) {
            this.safeSend(ws, ["activeChangedMultiError", "Username tidak boleh kosong"]);
            break;
          }
          
          let userSeat = this._userSeatDataCache[targetUsername];
          
          if (!userSeat) {
            for (const [roomName, roomData] of Object.entries(this._roomsDataCache)) {
              if (!roomData || !roomData.seats) continue;
              for (const [seat, data] of Object.entries(roomData.seats)) {
                if (data && data.namauser === targetUsername) {
                  userSeat = { 
                    room: roomName, 
                    seat: parseInt(seat), 
                    isMulti: true 
                  };
                  break;
                }
              }
              if (userSeat) break;
            }
          }
          
          if (!userSeat) {
            this.safeSend(ws, ["activeChangedMultiError", `User ${targetUsername} tidak ditemukan`]);
            break;
          }
          
          const roomName = userSeat.room;
          const seatNumber = userSeat.seat;
          
          this._userSeatDataCache[targetUsername] = {
            room: roomName,
            seat: seatNumber,
            isMulti: true,
            multiRoom: roomName,
            multiSeat: seatNumber
          };
          await this._saveToStorageDirect();
          
          const webSockets = this._getActiveWebSockets();
          let foundAny = false;
          
          for (const wsKey of webSockets) {
            try {
              const uname = wsKey._cachedUsername || 
                            wsKey.username || 
                            wsKey.deserializeAttachment()?.username;
              
              if (uname === targetUsername && wsKey.readyState === 1) {
                wsKey.serializeAttachment({
                  username: targetUsername,
                  room: roomName,
                  seat: seatNumber,
                  isMulti: true,
                  multiRoom: roomName,
                  multiSeat: seatNumber,
                  seatInfo: { 
                    room: roomName, 
                    seat: seatNumber, 
                    isMulti: true,
                    multiRoom: roomName,
                    multiSeat: seatNumber
                  }
                });
                
                wsKey._cachedUsername = targetUsername;
                wsKey._cachedRoom = roomName;
                wsKey.username = targetUsername;
                wsKey.idtarget = targetUsername;
                wsKey.room = roomName;
                wsKey.roomname = roomName;
                wsKey._isMulti = true;
                wsKey._multiRoom = roomName;
                wsKey._multiSeat = seatNumber;
                wsKey._closing = false;
                
                foundAny = true;
                
                this.safeSend(wsKey, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
                
                if (wsKey !== ws) {
                  this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
                }
              }
            } catch(e) {}
          }
          
          if (!foundAny) {
            this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
          }
          
          this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
          this._refreshRoomClients(true);
          
          break;
        }
        
        case "exitMulti": {
          const targetUsername = args[0];
          
          if (!targetUsername) {
            this.safeSend(ws, ["exitMultiError", "Username tidak boleh kosong"]);
            break;
          }
          
          try {
            await this._removeUserFromAllRooms(targetUsername);
            
            const webSockets = this._getActiveWebSockets();
            for (const wsKey of webSockets) {
              try {
                const uname = wsKey._cachedUsername || 
                              wsKey.username || 
                              wsKey.deserializeAttachment()?.username;
                if (uname === targetUsername) {
                  wsKey._isMulti = false;
                  wsKey._multiRoom = null;
                  wsKey._multiSeat = null;
                  wsKey._cachedRoom = null;
                  wsKey.room = null;
                  wsKey.roomname = null;
                  wsKey.idtarget = null;
                  wsKey.serializeAttachment({ 
                    username: targetUsername,
                    isMulti: false
                  });
                  
                  this.safeSend(wsKey, ["exitMultiSuccess", targetUsername, null, null]);
                }
              } catch(e) {}
            }
            
            this._refreshRoomClients(true);
            this.safeSend(ws, ["exitMultiSuccess", targetUsername, null, null]);
            
          } catch(e) {
            this.safeSend(ws, ["exitMultiError", e.message]);
          }
          break;
        }
        
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          
          const updated = await this._updateKursi(kursiRoom, kursiSeat, {
            noimageUrl: kursiNoimg || "",
            namauser: kursiName || "",
            color: kursiColor || "",
            itembawah: kursiBawah || 0,
            itematas: kursiAtas || 0,
            vip: kursiVip || 0,
            viptanda: kursiVt || 0
          });
          
          if (updated) {
            const roomData = this._roomsDataCache[kursiRoom];
            const updatedSeat = roomData?.seats?.[kursiSeat];
            if (updatedSeat) {
              this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
            }
          }
          break;
        }
        
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;
          
          const userSeat = this._userSeatDataCache[chatUser];
          if (!userSeat || userSeat.room !== chatRoom) {
            break;
          }
          
          this._broadcastToRoom(chatRoom, JSON.stringify(["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]));
          break;
        }
        
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (!pointRoom || typeof pointSeat !== 'number') break;
          
          const updated = await this._updatePoint(pointRoom, pointSeat, pointX, pointY, pointFast === 1);
          if (updated) {
            this._broadcastToRoom(pointRoom, JSON.stringify(["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]));
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          
          const roomData = this._roomsDataCache[removeRoom];
          let username = null;
          if (roomData && roomData.seats && roomData.seats[removeSeat]) {
            username = roomData.seats[removeSeat].namauser;
          }
          
          if (username) {
            await this._removeUserFromAllRooms(username);
          }
          break;
        }
        
        case "private": {
          const [privTarget, privNoimg, privMsg, privSender] = args;
          if (privTarget && privMsg) {
            const userSeat = this._userSeatDataCache[privTarget];
            if (userSeat) {
              const webSockets = this._getActiveWebSockets();
              for (const wsKey of webSockets) {
                try {
                  const uname = wsKey._cachedUsername || 
                                wsKey.username || 
                                wsKey.deserializeAttachment()?.username;
                  if (uname === privTarget && wsKey.readyState === 1) {
                    this.safeSend(wsKey, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
                  }
                } catch(e) {}
              }
            }
            this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
          }
          break;
        }
        
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (giftRoom && ROOMS_SET.has(giftRoom)) {
            const receiverSeat = this._userSeatDataCache[giftReceiver];
            if (!receiverSeat || receiverSeat.room !== giftRoom) break;
            this._broadcastToRoom(giftRoom, JSON.stringify(["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]));
          }
          break;
        }
        
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (rollRoom && ROOMS_SET.has(rollRoom)) {
            const userSeat = this._userSeatDataCache[rollUser];
            if (!userSeat || userSeat.room !== rollRoom) break;
            this._broadcastToRoom(rollRoom, JSON.stringify(["rollangakBroadcast", rollRoom, rollUser, rollAngka]));
          }
          break;
        }
        
        case "sendnotif": {
          try {
            const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
            if (notifTarget && notifMsg) {
              const webSockets = this._getActiveWebSockets();
              for (const wsKey of webSockets) {
                try {
                  const uname = wsKey._cachedUsername || 
                                wsKey.username || 
                                wsKey.deserializeAttachment()?.username;
                  if (uname === notifTarget && wsKey.readyState === 1) {
                    this.safeSend(wsKey, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
                    break;
                  }
                } catch(e) {}
              }
            }
          } catch(e) {}
          break;
        }
        
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          const userSeat = this._userSeatDataCache[onlineTarget];
          
          if (userSeat) {
            if (userSeat.isMulti) {
              isOnline = true;
            } else {
              const webSockets = this._getActiveWebSockets();
              for (const wsKey of webSockets) {
                try {
                  const uname = wsKey._cachedUsername || 
                                wsKey.username || 
                                wsKey.deserializeAttachment()?.username;
                  if (uname === onlineTarget && wsKey.readyState === 1 && !wsKey._isMulti) {
                    isOnline = true;
                    break;
                  }
                } catch(e) {}
              }
            }
          }
          
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const [username, seatInfo] of Object.entries(this._userSeatDataCache)) {
            if (seatInfo) {
              if (seatInfo.isMulti) {
                users.push(username);
              } else {
                const webSockets = this._getActiveWebSockets();
                for (const wsKey of webSockets) {
                  try {
                    const uname = wsKey._cachedUsername || 
                                  wsKey.username || 
                                  wsKey.deserializeAttachment()?.username;
                    if (uname === username && wsKey.readyState === 1 && !wsKey._isMulti) {
                      users.push(username);
                      break;
                    }
                  } catch(e) {}
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
            const roomData = this._roomsDataCache[room];
            counts[room] = roomData?.seats ? Object.keys(roomData.seats).length : 0;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = args[0];
          if (roomName && ROOMS_SET.has(roomName)) {
            const roomData = this._roomsDataCache[roomName];
            const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
            this.safeSend(ws, ["roomUserCount", roomName, count]);
          }
          break;
        }
        
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (!muteRoom || !ROOMS_SET.has(muteRoom)) break;
          
          const roomData = this._roomsDataCache[muteRoom];
          if (roomData) {
            roomData.muted = !!muteVal;
            await this._saveToStorageDirect();
          }
          
          this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
          break;
        }

        case "modwarning": {
          const modRoom = args[0];
          if (modRoom && ROOMS_SET.has(modRoom)) {
            this.broadcast(modRoom, ["modwarning", modRoom]);
          }
          break;
        }

        case "getMuteType": {
          const getMuteRoom = args[0];
          if (getMuteRoom && ROOMS_SET.has(getMuteRoom)) {
            const roomData = this._roomsDataCache[getMuteRoom];
            this.safeSend(ws, ["muteTypeResponse", roomData?.muted || false, getMuteRoom]);
          }
          break;
        }
        
        case "onDestroy":
          break;
        
        default:
          this.safeSend(ws, ["error", `Unknown event: ${evt}`]);
          break;
      }
    } catch(e) {}
  }

  // ============ RESTORE STATE ============

  async _restoreAllState() {
    if (this._isRestoring) return;
    this._isRestoring = true;
    
    try {
      const storage = await this.ctx.storage.get(["roomsData", "userSeatData", "currentNumber", "userCounts", "onlineUsers"]);
      this._roomsDataCache = storage.roomsData || {};
      this._userSeatDataCache = storage.userSeatData || {};
      this.currentNumber = storage.currentNumber || 1;
      this._userCounts = storage.userCounts || {};
      this._onlineUsers = new Set(storage.onlineUsers || []);
      
      // Validasi data
      for (const [username, seatInfo] of Object.entries(this._userSeatDataCache)) {
        if (!seatInfo || !seatInfo.room) {
          delete this._userSeatDataCache[username];
          this._onlineUsers.delete(username);
          continue;
        }
        const roomData = this._roomsDataCache[seatInfo.room];
        if (!roomData || !roomData.seats || !roomData.seats[seatInfo.seat]) {
          delete this._userSeatDataCache[username];
          this._onlineUsers.delete(username);
        }
      }
      
      // Restore WebSocket yang masih aktif
      const webSockets = this.ctx.getWebSockets();
      for (const ws of webSockets) {
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username) {
            const userSeat = this._userSeatDataCache[attachment.username];
            if (userSeat) {
              const isMulti = attachment.isMulti || userSeat.isMulti || false;
              const roomName = attachment.room || userSeat.room;
              const seatNumber = attachment.seat || userSeat.seat;
              
              ws.username = attachment.username;
              ws.room = roomName;
              ws.roomname = roomName;
              ws.idtarget = attachment.username;
              ws._closing = false;
              ws._isMulti = isMulti;
              ws._multiRoom = attachment.multiRoom || roomName;
              ws._multiSeat = attachment.multiSeat || seatNumber;
              ws._cachedUsername = attachment.username;
              ws._cachedRoom = roomName;
              
              ws.serializeAttachment({
                username: attachment.username,
                room: roomName,
                seat: seatNumber,
                isMulti: isMulti,
                multiRoom: attachment.multiRoom || roomName,
                multiSeat: attachment.multiSeat || seatNumber,
                seatInfo: userSeat
              });
              
              this._onlineUsers.add(attachment.username);
            }
          }
        } catch(e) {}
      }
      
      this._refreshRoomClients(true);
      await this._updateUserCounts();
      
      if (!this.closing && !this.isDestroyed) {
        const existingAlarm = await this.ctx.storage.getAlarm();
        if (!existingAlarm) {
          this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
        }
      }
      
    } catch(e) {
      console.error("Restore state error:", e);
    } finally {
      this._isRestoring = false;
    }
  }

  // ============ RESET ALL DATA ============

  async resetAllData() {
    const timestamp = Date.now();
    
    try {
      this._roomsDataCache = {};
      this._userSeatDataCache = {};
      this.currentNumber = 1;
      this._onlineUsers.clear();
      this._userCounts = {};
      for (const room of ROOMS) {
        this._userCounts[room] = 0;
      }
      
      // LANGSUNG SAVE
      await this._saveToStorageDirect();
      
      const resetMessage = JSON.stringify(["serverReset", "Server di-reset pada: " + new Date(timestamp).toLocaleString()]);
      
      const webSockets = this._getActiveWebSockets();
      for (const ws of webSockets) {
        try {
          if (ws.readyState === 1) {
            ws.send(resetMessage);
          }
        } catch(e) {}
      }
      
      for (const ws of webSockets) {
        try {
          if (ws.readyState === 1) {
            ws.close(1000, "Server reset - " + timestamp);
          }
        } catch(e) {}
      }
      
      this._refreshRoomClients(true);
      
      if (!this.closing && !this.isDestroyed) {
        await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      }
      
      await this.ctx.storage.put("lastReset", timestamp);
      
      return {
        success: true,
        message: "Semua data berhasil direset",
        timestamp: timestamp,
        resetTime: new Date(timestamp).toLocaleString()
      };
      
    } catch(e) {
      return {
        success: false,
        error: e.message,
        timestamp: timestamp
      };
    }
  }

  // ============ FETCH ============

  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    try {
      const url = new URL(req.url);
      
      if (url.pathname === "/reset" && req.method === "POST") {
        const result = await this.resetAllData();
        return new Response(JSON.stringify(result), {
          status: result.success ? 200 : 500,
          headers: { "Content-Type": "application/json" }
        });
      }
      
      if (url.pathname === "/status") {
        const webSockets = this._getActiveWebSockets();
        const status = {
          activeConnections: webSockets.length,
          rooms: this._userCounts,
          totalUsers: this._onlineUsers.size,
          currentNumber: this.currentNumber,
          isClosing: this.closing,
          isDestroyed: this.isDestroyed,
          uptime: Date.now() - this._startTime
        };
        return new Response(JSON.stringify(status), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });
      }
      
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server - No Ping/Pong", { 
          status: 200,
          headers: { "Cache-Control": "no-cache" }
        });
      }
      
      const currentConnections = this._getActiveWebSockets().length;
      if (currentConnections >= C.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server full", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      
      try {
        this.ctx.acceptWebSocket(server);
      } catch(e) {
        return new Response("WebSocket acceptance failed", { status: 500 });
      }
      
      server.username = null;
      server.room = null;
      server.roomname = null;
      server.idtarget = null;
      server._closing = false;
      server._wsId = Date.now() + Math.random();
      server._isMulti = false;
      server._multiRoom = null;
      server._multiSeat = null;
      server._cachedUsername = null;
      server._cachedRoom = null;
      
      server.serializeAttachment({});
      
      this._refreshRoomClients(true);
      
      return new Response(null, { 
        status: 101, 
        webSocket: client
      });
      
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ============ DESTROY ============

  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    await this._cleanupStorage();
    
    const webSockets = this._getActiveWebSockets();
    for (const ws of webSockets) {
      if (ws?.readyState === 1) {
        try { ws.send(JSON.stringify(["serverShutdown", "Server shutting down"])); } catch(e) {}
        try { ws.close(1000, "Shutdown"); } catch(e) {}
      }
    }
    
    this.roomClients.clear();
    this._onlineUsers.clear();
    
    try {
      await this.ctx.storage.deleteAlarm();
    } catch(e) {}
  }
}

export default ChatServer;
