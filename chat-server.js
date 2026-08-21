// ==================== CHAT-SERVER.JS ====================
// VERSION: 9.0.0 - PERSISTENT STATE WITH MINIMAL LOOPS

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_MESSAGE_SIZE: 5000,
  NUMBER_INTERVAL_MS: 900000,
  MAX_NUMBER: 6,
  SAVE_INTERVAL_MS: 300000, // 5 menit
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
    
    // In-memory state (akan direstore dari storage)
    this.wsSet = new Set();
    this.roomClients = new Map();
    for (const room of ROOMS) {
      this.roomClients.set(room, new Set());
    }
    
    this.currentNumber = 1;
    this._isNumberUpdating = false;
    this._lastSaveTime = Date.now();
    
    // Restore semua state dari storage
    this._restoreAllState().then(() => {});
  }

  // ============ STORAGE OPERATIONS ============
  
  async _loadFromStorage() {
    try {
      const roomsData = await this.ctx.storage.get("roomsData") || {};
      const userSeatData = await this.ctx.storage.get("userSeatData") || {};
      const currentNumber = await this.ctx.storage.get("currentNumber") || 1;
      const wsState = await this.ctx.storage.get("wsState") || {};
      const roomClientsState = await this.ctx.storage.get("roomClientsState") || {};
      const flags = await this.ctx.storage.get("flags") || {};
      return { roomsData, userSeatData, currentNumber, wsState, roomClientsState, flags };
    } catch(e) {
      return { roomsData: {}, userSeatData: {}, currentNumber: 1, wsState: {}, roomClientsState: {}, flags: {} };
    }
  }

  async _saveAllState() {
    try {
      // Save rooms data
      const roomsData = await this.ctx.storage.get("roomsData") || {};
      
      // Save user seat data
      const userSeatData = await this.ctx.storage.get("userSeatData") || {};
      
      // Save WebSocket state (tanpa loop, langsung dari storage)
      const wsState = await this._getWSState();
      
      // Save room clients state
      const roomClientsState = await this._getRoomClientsState();
      
      // Save flags
      const flags = {
        closing: this.closing,
        isDestroyed: this.isDestroyed,
        currentNumber: this.currentNumber,
        lastSaveTime: Date.now()
      };
      
      await this.ctx.storage.put({
        roomsData,
        userSeatData,
        currentNumber: this.currentNumber,
        wsState,
        roomClientsState,
        flags
      });
      
      this._lastSaveTime = Date.now();
    } catch(e) {}
  }

  async _getWSState() {
    // Ambil dari WebSocket attachments tanpa loop
    const wsState = {};
    try {
      const webSockets = this.ctx.getWebSockets();
      for (let i = 0; i < webSockets.length; i++) {
        const ws = webSockets[i];
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username) {
            wsState[attachment.username] = {
              username: attachment.username,
              room: attachment.room || attachment.seatInfo?.room,
              seat: attachment.seatInfo?.seat,
              isMulti: attachment.isMulti || false,
              multiRoom: attachment.multiRoom || attachment.seatInfo?.room,
              multiSeat: attachment.multiSeat || attachment.seatInfo?.seat,
              wsId: ws._wsId || Date.now() + Math.random()
            };
          }
        } catch(e) {}
      }
    } catch(e) {}
    return wsState;
  }

  async _getRoomClientsState() {
    // Ambil dari WebSocket attachments tanpa loop
    const roomClientsState = {};
    try {
      const webSockets = this.ctx.getWebSockets();
      for (let i = 0; i < webSockets.length; i++) {
        const ws = webSockets[i];
        try {
          const attachment = ws.deserializeAttachment();
          if (attachment && attachment.username && attachment.room) {
            const room = attachment.room;
            if (!roomClientsState[room]) {
              roomClientsState[room] = [];
            }
            roomClientsState[room].push(attachment.username);
          }
        } catch(e) {}
      }
    } catch(e) {}
    return roomClientsState;
  }

  // ============ USER MANAGEMENT ============

  async _isUserInAnyRoom(username) {
    if (!username) return null;
    
    const storage = await this._loadFromStorage();
    const { roomsData, userSeatData } = storage;
    
    // Cek dari userSeatData
    const seatInfo = userSeatData[username];
    if (seatInfo && seatInfo.room) {
      const roomData = roomsData[seatInfo.room];
      if (roomData && roomData.seats) {
        const seat = Object.keys(roomData.seats).find(s => 
          roomData.seats[s]?.namauser === username
        );
        if (seat) {
          return { 
            room: seatInfo.room, 
            seat: parseInt(seat), 
            isMulti: seatInfo.isMulti || false 
          };
        }
      }
      // Invalid, hapus dari userSeatData
      delete userSeatData[username];
      await this.ctx.storage.put("userSeatData", userSeatData);
    }
    
    // Cek dari roomsData
    for (const roomName of ROOMS) {
      const roomData = roomsData[roomName];
      if (!roomData || !roomData.seats) continue;
      const seat = Object.keys(roomData.seats).find(s => 
        roomData.seats[s]?.namauser === username
      );
      if (seat) {
        userSeatData[username] = { 
          room: roomName, 
          seat: parseInt(seat), 
          isMulti: false 
        };
        await this.ctx.storage.put("userSeatData", userSeatData);
        return { room: roomName, seat: parseInt(seat), isMulti: false };
      }
    }
    
    return null;
  }

  async _removeUserFromRoom(username, roomName) {
    if (!username || !roomName) return false;
    
    const storage = await this._loadFromStorage();
    const { roomsData, userSeatData } = storage;
    
    if (!roomsData[roomName] || !roomsData[roomName].seats) return false;
    
    let seat = null;
    for (const [s, data] of Object.entries(roomsData[roomName].seats)) {
      if (data && data.namauser === username) {
        seat = parseInt(s);
        break;
      }
    }
    
    if (!seat) return false;
    
    delete roomsData[roomName].seats[seat];
    if (roomsData[roomName].points) {
      delete roomsData[roomName].points[seat];
    }
    
    // Hapus room jika kosong
    const hasSeats = Object.keys(roomsData[roomName].seats).length > 0;
    const hasPoints = Object.keys(roomsData[roomName].points || {}).length > 0;
    if (!hasSeats && !hasPoints) {
      delete roomsData[roomName];
    }
    
    delete userSeatData[username];
    
    await this.ctx.storage.put({
      roomsData,
      userSeatData
    });
    
    // Broadcast
    this.broadcast(roomName, ["removeKursi", roomName, seat]);
    await this.updateRoomCount(roomName);
    
    return true;
  }

  // ============ JOIN HANDLING ============

  async _handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
      return false;
    }
    
    const username = ws.username;
    const existing = await this._isUserInAnyRoom(username);
    
    if (existing && existing.room !== roomName) {
      await this._removeUserFromRoom(username, existing.room);
    }
    
    const storage = await this._loadFromStorage();
    let { roomsData, userSeatData } = storage;
    
    if (!roomsData[roomName]) {
      roomsData[roomName] = { seats: {}, points: {}, muted: false, number: 1 };
    }
    
    const roomData = roomsData[roomName];
    let seat = null;
    
    // Cek apakah user sudah punya seat
    for (const [s, data] of Object.entries(roomData.seats)) {
      if (data && data.namauser === username) {
        seat = parseInt(s);
        break;
      }
    }
    
    if (!seat) {
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
    }
    
    const seatInfo = { room: roomName, seat, isMulti: false };
    userSeatData[username] = seatInfo;
    
    await this.ctx.storage.put({
      roomsData,
      userSeatData
    });
    
    // Update WebSocket state
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = username;
    ws._isMulti = false;
    ws._multiRoom = null;
    ws._multiSeat = null;
    
    ws.serializeAttachment({
      username: username,
      seatInfo: seatInfo,
      isMulti: false
    });
    
    // Tambahkan ke roomClients (in-memory)
    for (const [otherRoom, clients] of this.roomClients) {
      if (otherRoom !== roomName && clients) {
        clients.delete(ws);
      }
    }
    const roomClients = this.roomClients.get(roomName);
    if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
    
    if (!this.wsSet.has(ws)) this.wsSet.add(ws);
    
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomData.muted || false, roomName]);
    
    const count = Object.keys(roomData.seats).length;
    this.safeSend(ws, ["roomUserCount", roomName, count]);
    this.broadcast(roomName, ["roomUserCount", roomName, count]);
    
    // Save state ke storage
    await this._saveAllState();
    
    return true;
  }

  // ============ CLEANUP ============

  async _cleanupUserOnDisconnect(ws) {
    try {
      const username = ws.username;
      if (!username) return;
      
      const isMulti = ws._isMulti || false;
      
      if (isMulti) {
        const roomName = ws.room || ws.roomname;
        if (roomName) {
          const roomClients = this.roomClients.get(roomName);
          if (roomClients) roomClients.delete(ws);
        }
        await this._deleteUserSeat(username);
        this.wsSet.delete(ws);
        return;
      }
      
      const roomName = ws.room || ws.roomname;
      
      if (roomName) {
        await this._removeUserFromRoom(username, roomName);
      } else {
        const userSeat = await this._getUserSeat(username);
        if (userSeat && userSeat.room) {
          await this._removeUserFromRoom(username, userSeat.room);
        } else {
          await this._deleteUserSeat(username);
        }
      }
      
      const targetRoom = roomName || (await this._getUserSeat(username))?.room;
      if (targetRoom) {
        const roomClients = this.roomClients.get(targetRoom);
        if (roomClients) {
          roomClients.delete(ws);
        }
      }
      
      this.wsSet.delete(ws);
      
      // Save state setelah cleanup
      await this._saveAllState();
      
    } catch(e) {}
  }

  async _deleteUserSeat(username) {
    const storage = await this._loadFromStorage();
    const userSeatData = storage.userSeatData || {};
    delete userSeatData[username];
    await this.ctx.storage.put("userSeatData", userSeatData);
  }

  async _getUserSeat(username) {
    const storage = await this._loadFromStorage();
    return storage.userSeatData[username] || null;
  }

  cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    try {
      const room = ws.room;
      if (room) {
        try { this.roomClients.get(room)?.delete(ws); } catch(e) {}
      }
      try { this.wsSet.delete(ws); } catch(e) {}
    } catch(e) {} finally {
      ws._cleaning = false;
      try { if (ws && ws.readyState === 1) ws.close(1000, "Cleanup"); } catch(e) {}
    }
  }

  // ============ BROADCAST ============

  _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed || !room) return;
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const toRemove = new Set();
    for (const ws of clients) {
      if (!ws) { toRemove.add(ws); continue; }
      
      const wsRoom = ws.room || ws.roomname;
      if (wsRoom !== room) {
        toRemove.add(ws);
        continue;
      }
      
      try {
        if (ws.readyState === 1 && !ws._closing) {
          ws.send(msgStr);
        } else {
          toRemove.add(ws);
        }
      } catch(e) { toRemove.add(ws); }
    }
    
    if (toRemove.size > 0) {
      for (const ws of toRemove) {
        try {
          clients.delete(ws);
          if (ws) this.cleanup(ws);
        } catch(e) {}
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
      this.cleanup(ws);
      return false;
    }
  }

  // ============ STATE MANAGEMENT ============

  async updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    try {
      const roomData = await this._getRoomData(room);
      if (!roomData || !roomData.seats) return 0;
      const count = Object.keys(roomData.seats).length;
      this.broadcast(room, ["roomUserCount", room, count]);
      return count;
    } catch(e) { return 0; }
  }

  async _getRoomData(roomName) {
    const storage = await this._loadFromStorage();
    return storage.roomsData[roomName] || null;
  }

  async sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username) return;
    try {
      if (ws.readyState !== 1 || ws._closing) return;
    } catch(e) { return; }
    
    const roomData = await this._getRoomData(room);
    if (!roomData) return;
    
    try {
      const allSeats = roomData.seats || {};
      const allPoints = roomData.points || {};
      
      const userSeat = await this._getUserSeat(ws.username);
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
    
    // Update number
    await this._updateNumber();
    
    // Cleanup storage (tanpa loop besar)
    await this._cleanupStorage();
    
    // Save all state
    await this._saveAllState();
    
    // Schedule next alarm
    this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
  }

  async _updateNumber() {
    if (this._isNumberUpdating || this.closing || this.isDestroyed) return;
    this._isNumberUpdating = true;
    try {
      // Atomic update dengan transaction
      await this.ctx.storage.transaction(async (txn) => {
        const current = await txn.get("currentNumber") || 1;
        const next = current < C.MAX_NUMBER ? current + 1 : 1;
        await txn.put("currentNumber", next);
        this.currentNumber = next;
        
        // Update rooms data
        const roomsData = await txn.get("roomsData") || {};
        for (const roomName of ROOMS) {
          if (roomsData[roomName]) {
            roomsData[roomName].number = next;
          }
        }
        await txn.put("roomsData", roomsData);
      });
      
      // Broadcast number update
      const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const room of ROOMS) {
        const clients = this.roomClients.get(room);
        if (clients && clients.size > 0) {
          this._broadcastToRoom(room, numberMsg);
        }
      }
      
    } catch(e) {} finally {
      this._isNumberUpdating = false;
    }
  }

  async _cleanupStorage() {
    try {
      const storage = await this._loadFromStorage();
      const { roomsData, userSeatData } = storage;
      let changed = false;
      
      // Cleanup userSeatData - cek koneksi dari WebSocket attachments
      const activeUsers = new Set();
      try {
        const webSockets = this.ctx.getWebSockets();
        for (let i = 0; i < webSockets.length; i++) {
          const ws = webSockets[i];
          try {
            const attachment = ws.deserializeAttachment();
            if (attachment && attachment.username && ws.readyState === 1) {
              activeUsers.add(attachment.username);
            }
          } catch(e) {}
        }
      } catch(e) {}
      
      // Hapus user yang tidak aktif
      for (const [username, seatInfo] of Object.entries(userSeatData)) {
        if (!seatInfo || !seatInfo.room) {
          delete userSeatData[username];
          changed = true;
          continue;
        }
        
        if (seatInfo.isMulti) {
          // Cek multi-user
          const roomData = roomsData[seatInfo.room];
          if (!roomData || !roomData.seats || !roomData.seats[seatInfo.seat]) {
            delete userSeatData[username];
            changed = true;
          }
          continue;
        }
        
        // Cek apakah user masih aktif
        if (!activeUsers.has(username)) {
          delete userSeatData[username];
          changed = true;
        }
      }
      
      // Hapus room kosong
      for (const roomName of ROOMS) {
        const roomData = roomsData[roomName];
        if (!roomData) continue;
        
        const hasSeats = roomData.seats && Object.keys(roomData.seats).length > 0;
        const hasPoints = roomData.points && Object.keys(roomData.points).length > 0;
        
        if (!hasSeats && !hasPoints) {
          delete roomsData[roomName];
          changed = true;
        }
      }
      
      if (changed) {
        await this.ctx.storage.put({
          roomsData,
          userSeatData
        });
      }
      
    } catch(e) {}
  }

  // ============ RESET ALL DATA ============

  async resetAllData() {
    const timestamp = Date.now();
    
    try {
      // Hapus semua data dari storage
      await this.ctx.storage.delete("roomsData");
      await this.ctx.storage.delete("userSeatData");
      await this.ctx.storage.delete("currentNumber");
      await this.ctx.storage.delete("wsState");
      await this.ctx.storage.delete("roomClientsState");
      await this.ctx.storage.delete("flags");
      
      // Broadcast reset ke semua client
      const resetMessage = JSON.stringify(["serverReset", "Server di-reset pada: " + new Date(timestamp).toLocaleString()]);
      for (const ws of this.wsSet) {
        try {
          if (ws.readyState === 1) {
            ws.send(resetMessage);
          }
        } catch(e) {}
      }
      
      // Tutup semua koneksi
      const wsCopy = Array.from(this.wsSet);
      for (const ws of wsCopy) {
        try {
          if (ws.readyState === 1) {
            ws.close(1000, "Server reset - " + timestamp);
          }
        } catch(e) {}
        try {
          this.cleanup(ws);
        } catch(e) {}
      }
      
      // Reset state
      this.wsSet.clear();
      this.roomClients.clear();
      for (const room of ROOMS) {
        this.roomClients.set(room, new Set());
      }
      this.currentNumber = 1;
      
      // Set alarm
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

  // ============ WEBSOCKET EVENT HANDLERS ============

  async webSocketMessage(ws, msg) {
    if (!ws || ws._closing || this.closing || this.isDestroyed) return;
    try { 
      await this.handleMessage(ws, msg); 
    } catch(e) {}
  }

  async webSocketClose(ws) { 
    if (!ws) return;
    try {
      await this._cleanupUserOnDisconnect(ws);
      this.cleanup(ws);
      await this._saveAllState();
    } catch(e) {}
  }

  async webSocketError(ws) { 
    if (!ws) return;
    try {
      await this._cleanupUserOnDisconnect(ws);
      this.cleanup(ws);
      await this._saveAllState();
    } catch(e) {}
  }

  // ============ HANDLE SET ID ============

  async _handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { if (ws?.readyState === 1) ws.close(1000, "Invalid username"); } catch(e) {}
      return;
    }
    
    if (ws.readyState !== 1) {
      try { this.cleanup(ws); } catch(e) {}
      return;
    }
    
    // Skip multi-user dengan isNewUser = false
    if (!isNewUser) {
      const existing = await this._isUserInAnyRoom(username);
      if (existing && existing.isMulti) {
        return;
      }
    }
    
    // Proses normal
    const existing = await this._isUserInAnyRoom(username);
    if (existing) {
      await this._removeUserFromRoom(username, existing.room);
    }
    
    ws.username = username;
    ws.idtarget = username;
    ws.room = null;
    ws.roomname = null;
    ws._closing = false;
    ws._isMulti = false;
    ws._multiRoom = null;
    ws._multiSeat = null;
    
    ws.serializeAttachment({ username: username });
    
    if (!this.wsSet.has(ws)) this.wsSet.add(ws);
    
    if (isNewUser) { 
      this.safeSend(ws, ["joinroomawal"]); 
    } else { 
      this.safeSend(ws, ["needJoinRoom"]); 
    }
    
    await this._saveAllState();
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
          
          if (isNewUser === false) {
            const existing = await this._isUserInAnyRoom(username);
            if (existing && existing.isMulti) {
              break;
            }
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
          if (!multiUsername || !multiRoomname) break;
          
          const existing = await this._isUserInAnyRoom(multiUsername);
          if (existing) {
            await this._removeUserFromRoom(multiUsername, existing.room);
          }
          
          const storage = await this._loadFromStorage();
          let { roomsData, userSeatData } = storage;
          
          if (!roomsData[multiRoomname]) {
            roomsData[multiRoomname] = { seats: {}, points: {}, muted: false, number: 1 };
          }
          
          const roomData = roomsData[multiRoomname];
          const seatCount = Object.keys(roomData.seats).length;
          if (seatCount >= C.MAX_SEATS) break;
          
          let seat = null;
          for (let s = 1; s <= C.MAX_SEATS; s++) {
            if (!roomData.seats[s]) {
              seat = s;
              break;
            }
          }
          
          if (!seat) break;
          
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
            isMulti: true
          };
          userSeatData[multiUsername] = seatInfo;
          
          await this.ctx.storage.put({
            roomsData,
            userSeatData
          });
          
          ws.serializeAttachment({
            username: multiUsername,
            seatInfo: seatInfo,
            isMulti: true,
            multiRoom: multiRoomname,
            multiSeat: seat
          });
          
          ws.username = multiUsername;
          ws.idtarget = multiUsername;
          ws.room = multiRoomname;
          ws.roomname = multiRoomname;
          ws._isMulti = true;
          ws._multiRoom = multiRoomname;
          ws._multiSeat = seat;
          
          // Update roomClients
          for (const [otherRoom, clients] of this.roomClients) {
            if (otherRoom !== multiRoomname && clients) {
              clients.delete(ws);
            }
          }
          const roomClients = this.roomClients.get(multiRoomname);
          if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
          
          if (!this.wsSet.has(ws)) this.wsSet.add(ws);
          
          this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
          this.broadcast(multiRoomname, ["roomUserCount", multiRoomname, Object.keys(roomData.seats).length]);
          
          await this._saveAllState();
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          
          let userSeat = await this._getUserSeat(targetUsername);
          if (!userSeat) {
            this.safeSend(ws, ["activeChangedMultiError", "User not found"]);
            break;
          }
          
          const roomName = userSeat.room;
          const seatNumber = userSeat.seat;
          
          await this._updateUserSeat(targetUsername, (data) => {
            data.room = roomName;
            data.seat = seatNumber;
            data.isMulti = true;
          });
          
          // Update WebSocket state
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
          ws._isMulti = true;
          ws._multiRoom = roomName;
          ws._multiSeat = seatNumber;
          
          ws.serializeAttachment({
            username: targetUsername,
            seatInfo: { room: roomName, seat: seatNumber, isMulti: true },
            isMulti: true,
            multiRoom: roomName,
            multiSeat: seatNumber
          });
          
          if (!this.wsSet.has(ws)) this.wsSet.add(ws);
          
          this.safeSend(ws, ["activeChangedMulti", targetUsername, seatNumber, roomName]);
          this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
          
          await this._saveAllState();
          break;
        }
        
        case "exitMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          
          try {
            let userSeat = await this._getUserSeat(targetUsername);
            
            if (!userSeat) {
              const storage = await this._loadFromStorage();
              const roomsData = storage.roomsData || {};
              for (const roomName of ROOMS) {
                const roomData = roomsData[roomName];
                if (!roomData || !roomData.seats) continue;
                for (const [seat, data] of Object.entries(roomData.seats)) {
                  if (data && data.namauser === targetUsername) {
                    userSeat = { room: roomName, seat: parseInt(seat), isMulti: true };
                    break;
                  }
                }
                if (userSeat) break;
              }
            }
            
            if (!userSeat) {
              await this._deleteUserSeat(targetUsername);
              this.safeSend(ws, ["exitMultiSuccess", targetUsername, null, null]);
              break;
            }
            
            const roomName = userSeat.room;
            const seatNumber = userSeat.seat;
            
            await this._removeUserFromRoom(targetUsername, roomName);
            await this._deleteUserSeat(targetUsername);
            
            // Reset semua WebSocket dengan username ini
            for (const wsKey of this.wsSet) {
              if (wsKey.username === targetUsername) {
                if (wsKey.room) {
                  const rc = this.roomClients.get(wsKey.room);
                  if (rc) rc.delete(wsKey);
                }
                wsKey._isMulti = false;
                wsKey._multiRoom = null;
                wsKey._multiSeat = null;
                try {
                  wsKey.serializeAttachment({});
                  wsKey.username = null;
                  wsKey.room = null;
                  wsKey.roomname = null;
                  wsKey.idtarget = null;
                } catch(e) {}
              }
            }
            
            this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
            await this.updateRoomCount(roomName);
            
            this.safeSend(ws, ["exitMultiSuccess", targetUsername, roomName, seatNumber]);
            
            await this._saveAllState();
            
          } catch(e) {
            this.safeSend(ws, ["exitMultiError", e.message]);
          }
          break;
        }
        
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          
          const storage = await this._loadFromStorage();
          const roomsData = storage.roomsData || {};
          
          if (!roomsData[kursiRoom] || !roomsData[kursiRoom].seats || !roomsData[kursiRoom].seats[kursiSeat]) break;
          
          roomsData[kursiRoom].seats[kursiSeat] = {
            noimageUrl: kursiNoimg || "",
            namauser: kursiName || "",
            color: kursiColor || "",
            itembawah: kursiBawah || 0,
            itematas: kursiAtas || 0,
            vip: kursiVip || 0,
            viptanda: kursiVt || 0
          };
          
          await this.ctx.storage.put("roomsData", roomsData);
          
          this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, roomsData[kursiRoom].seats[kursiSeat]]]]);
          break;
        }
        
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (!chatMsg || !ROOMS_SET.has(chatRoom)) break;
          
          const userSeat = await this._getUserSeat(chatUser);
          if (!userSeat || userSeat.room !== chatRoom) break;
          
          this._broadcastToRoom(chatRoom, JSON.stringify(["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]));
          break;
        }
        
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (!pointRoom || typeof pointSeat !== 'number') break;
          
          const storage = await this._loadFromStorage();
          const roomsData = storage.roomsData || {};
          
          if (!roomsData[pointRoom] || !roomsData[pointRoom].seats || !roomsData[pointRoom].seats[pointSeat]) break;
          
          if (!roomsData[pointRoom].points) roomsData[pointRoom].points = {};
          roomsData[pointRoom].points[pointSeat] = { x: pointX || 0, y: pointY || 0, fast: !!pointFast };
          
          await this.ctx.storage.put("roomsData", roomsData);
          
          this._broadcastToRoom(pointRoom, JSON.stringify(["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]));
          break;
        }
        
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          
          const storage = await this._loadFromStorage();
          const roomsData = storage.roomsData || {};
          
          if (!roomsData[removeRoom] || !roomsData[removeRoom].seats || !roomsData[removeRoom].seats[removeSeat]) break;
          
          const username = roomsData[removeRoom].seats[removeSeat].namauser;
          if (username) {
            await this._removeUserFromRoom(username, removeRoom);
          }
          break;
        }
        
        case "private": {
          const [privTarget, privNoimg, privMsg, privSender] = args;
          if (privTarget && privMsg) {
            const userSeat = await this._getUserSeat(privTarget);
            if (userSeat) {
              // Cari WebSocket target
              for (const wsKey of this.wsSet) {
                if (wsKey.username === privTarget && wsKey.readyState === 1) {
                  this.safeSend(wsKey, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
                }
              }
            }
            this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
          }
          break;
        }
        
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (giftRoom && ROOMS_SET.has(giftRoom)) {
            const receiverSeat = await this._getUserSeat(giftReceiver);
            if (!receiverSeat || receiverSeat.room !== giftRoom) break;
            this._broadcastToRoom(giftRoom, JSON.stringify(["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]));
          }
          break;
        }
        
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (rollRoom && ROOMS_SET.has(rollRoom)) {
            const userSeat = await this._getUserSeat(rollUser);
            if (!userSeat || userSeat.room !== rollRoom) break;
            this._broadcastToRoom(rollRoom, JSON.stringify(["rollangakBroadcast", rollRoom, rollUser, rollAngka]));
          }
          break;
        }
        
        case "sendnotif": {
          try {
            const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
            if (notifTarget && notifMsg) {
              for (const wsKey of this.wsSet) {
                if (wsKey.username === notifTarget && wsKey.readyState === 1) {
                  this.safeSend(wsKey, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
                  break;
                }
              }
            }
          } catch(e) {}
          break;
        }
        
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          const userSeat = await this._getUserSeat(onlineTarget);
          
          if (userSeat) {
            if (userSeat.isMulti) {
              isOnline = true;
            } else {
              for (const wsKey of this.wsSet) {
                if (wsKey.username === onlineTarget && wsKey.readyState === 1 && !wsKey._isMulti) {
                  isOnline = true;
                  break;
                }
              }
            }
          }
          
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          const storage = await this._loadFromStorage();
          const userSeatData = storage.userSeatData || {};
          
          for (const [username, seatInfo] of Object.entries(userSeatData)) {
            if (seatInfo) {
              if (seatInfo.isMulti) {
                users.push(username);
              } else {
                for (const wsKey of this.wsSet) {
                  if (wsKey.username === username && wsKey.readyState === 1 && !wsKey._isMulti) {
                    users.push(username);
                    break;
                  }
                }
              }
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "getAllRoomsUserCount": {
          const storage = await this._loadFromStorage();
          const roomsData = storage.roomsData || {};
          const counts = {};
          for (const room of ROOMS) {
            counts[room] = roomsData[room]?.seats ? Object.keys(roomsData[room].seats).length : 0;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = args[0];
          if (roomName && ROOMS_SET.has(roomName)) {
            const roomData = await this._getRoomData(roomName);
            const count = roomData?.seats ? Object.keys(roomData.seats).length : 0;
            this.safeSend(ws, ["roomUserCount", roomName, count]);
          }
          break;
        }
        
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (!muteRoom || !ROOMS_SET.has(muteRoom)) break;
          
          const storage = await this._loadFromStorage();
          const roomsData = storage.roomsData || {};
          
          if (!roomsData[muteRoom]) {
            roomsData[muteRoom] = { seats: {}, points: {}, muted: false, number: 1 };
          }
          roomsData[muteRoom].muted = !!muteVal;
          
          await this.ctx.storage.put("roomsData", roomsData);
          
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
            const roomData = await this._getRoomData(getMuteRoom);
            this.safeSend(ws, ["muteTypeResponse", roomData?.muted || false, getMuteRoom]);
          }
          break;
        }
        
        case "onDestroy":
          this.cleanup(ws);
          break;
        
        default:
          this.safeSend(ws, ["error", `Unknown event: ${evt}`]);
          break;
      }
    } catch(e) {}
  }

  // ============ HELPER ============

  async _updateUserSeat(username, updater) {
    const storage = await this._loadFromStorage();
    const userSeatData = storage.userSeatData || {};
    
    if (!userSeatData[username]) {
      userSeatData[username] = {};
    }
    
    updater(userSeatData[username]);
    
    if (Object.keys(userSeatData[username]).length === 0) {
      delete userSeatData[username];
    }
    
    await this.ctx.storage.put("userSeatData", userSeatData);
    return userSeatData[username];
  }

  // ============ RESTORE STATE ============

  async _restoreAllState() {
    try {
      const storage = await this._loadFromStorage();
      const { roomsData, userSeatData, currentNumber, flags } = storage;
      
      // Restore current number
      if (currentNumber !== undefined) {
        this.currentNumber = currentNumber;
      }
      
      // Restore flags
      if (flags) {
        this.closing = flags.closing || false;
        this.isDestroyed = flags.isDestroyed || false;
      }
      
      // Restore WebSocket state dari attachments
      try {
        const webSockets = this.ctx.getWebSockets();
        for (let i = 0; i < webSockets.length; i++) {
          const ws = webSockets[i];
          try {
            const attachment = ws.deserializeAttachment();
            if (attachment && attachment.username) {
              const userSeat = userSeatData[attachment.username];
              if (userSeat) {
                const isMulti = attachment.isMulti || userSeat.isMulti || false;
                const roomName = userSeat.room;
                const seatNumber = userSeat.seat;
                
                // Validasi multi-user state
                if (isMulti) {
                  const roomData = roomsData[roomName];
                  if (!roomData || !roomData.seats || !roomData.seats[seatNumber]) {
                    // Invalid, reset ke non-multi
                    ws._isMulti = false;
                    ws._multiRoom = null;
                    ws._multiSeat = null;
                    ws.serializeAttachment({
                      username: attachment.username,
                      seatInfo: { room: roomName, seat: seatNumber, isMulti: false },
                      isMulti: false
                    });
                    continue;
                  }
                }
                
                ws.username = attachment.username;
                ws.room = roomName;
                ws.roomname = roomName;
                ws.idtarget = attachment.username;
                ws._closing = false;
                ws._isMulti = isMulti;
                ws._multiRoom = roomName;
                ws._multiSeat = seatNumber;
                
                ws.serializeAttachment({
                  username: attachment.username,
                  seatInfo: userSeat,
                  isMulti: isMulti,
                  multiRoom: roomName,
                  multiSeat: seatNumber
                });
                
                // Add to roomClients
                const roomClients = this.roomClients.get(roomName);
                if (roomClients) roomClients.add(ws);
                
                this.wsSet.add(ws);
              }
            }
          } catch(e) {}
        }
      } catch(e) {}
      
      // Set alarm
      if (!this.closing && !this.isDestroyed) {
        await this.ctx.storage.setAlarm(Date.now() + C.NUMBER_INTERVAL_MS);
      }
      
    } catch(e) {}
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
      
      try { 
        this.ctx.acceptWebSocket(server); 
      } catch(e) { 
        try { server.close(1011, "Accept failed"); } catch(ex) {}
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
      
      server.serializeAttachment({});
      
      if (!this.wsSet.has(server)) this.wsSet.add(server);
      
      return new Response(null, { 
        status: 101, 
        webSocket: client
      });
      
    } catch(e) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ============ DESTROY ============

  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    // Save state sebelum destroy
    await this._saveAllState();
    
    const wsCopy = Array.from(this.wsSet);
    for (const ws of wsCopy) {
      if (ws?.readyState === 1) {
        try { ws.send(JSON.stringify(["serverShutdown", "Server shutting down"])); } catch(e) {}
        try { ws.close(1000, "Shutdown"); } catch(e) {}
      }
      try { this.cleanup(ws); } catch(e) {}
    }
    
    this.wsSet.clear();
    this.roomClients.clear();
  }
}

export default ChatServer;
