// ==================== CHAT SERVER - STABLE WITH ALARM (NO IDLE KICK) ====================

 const C = {
  NUMBER_CHANGE_TICKS: 90,
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 500,
  ALARM_INTERVAL: 10000,
  MAX_MESSAGE_SIZE: 5000,
};

const ROOMS = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);

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
    const old = this.seats.get(seat);
    if (!old) return false;
    
    this.seats.set(seat, {
      noimageUrl: data.noimageUrl !== undefined ? data.noimageUrl : old.noimageUrl,
      namauser: data.namauser !== undefined ? data.namauser : old.namauser,
      color: data.color !== undefined ? data.color : old.color,
      itembawah: data.itembawah !== undefined ? data.itembawah : old.itembawah,
      itematas: data.itematas !== undefined ? data.itematas : old.itematas,
      vip: data.vip !== undefined ? data.vip : old.vip,
      viptanda: data.viptanda !== undefined ? data.viptanda : old.viptanda,
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
  
  setNumber(n) { this.number = n || 1; }
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

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    
    // WebSocket management
    this.wsSet = new Set();
    this.userConnections = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.userCountry = new Map();
    this.roomClients = new Map();
    this.rooms = new Map();
    this.wsActiveMulti = new Map();
    
    // Processing & cleanup
    this._processingMessages = new Set();
    this._cleaningUp = new Set();
    this._pendingTimeouts = new Set();
    this._isCleaningUp = false;
    this._cleanupInProgress = false;
    
    // Alarm system
    this._alarmProcessing = false;
    this._tickCount = 0;
    this.currentNumber = 1;
    this._lastAlarmTime = Date.now();
    this._alarmFailCount = 0;
    this._alarmStartTime = 0;
    this._alarmScheduled = false;
    this._alarmTimeout = null;
    this._alarmRescheduleAttempts = 0;
    this._maxAlarmRescheduleAttempts = 5;
    
    // PERBAIKAN: Tambahkan heartbeat interval
    this._heartbeatInterval = null;
    this._lastHeartbeatTime = Date.now();
    
    // Initialize rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // Initialize state from storage
    this._initState();
  }
  
  // ==================== INIT STATE ====================
  
  async _initState() {
    try {
      const savedNumber = await this.state.storage.get("currentNumber");
      if (savedNumber !== undefined) this.currentNumber = savedNumber;
      
      const savedTick = await this.state.storage.get("tickCount");
      if (savedTick !== undefined) this._tickCount = savedTick;
      
      // PERBAIKAN: Cek kapan terakhir server hidup
      const lastAlive = await this.state.storage.get("lastAlive");
      if (lastAlive && (Date.now() - lastAlive > 120000)) {
        console.log("Server was down for more than 2 minutes, resetting some states");
        // Reset jika perlu
      }
      
      // PERBAIKAN: Simpan timestamp alive
      await this.state.storage.put("lastAlive", Date.now());
    } catch(e) {
      // Use defaults
    }
    
    this._scheduleAlarm(100);
    
    // PERBAIKAN: Start heartbeat
    this._startHeartbeat();
  }
  
  // PERBAIKAN: Tambahkan method heartbeat
  _startHeartbeat() {
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
    }
    
    this._heartbeatInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._doHeartbeat();
      }
    }, 15000); // Setiap 15 detik
  }
  
  // PERBAIKAN: Method heartbeat
  async _doHeartbeat() {
    try {
      this._lastHeartbeatTime = Date.now();
      
      // Simpan state secara periodik
      await this.state.storage.put("lastAlive", Date.now());
      
      // Jika alarm tidak berjalan, restart
      if (!this._alarmScheduled && !this._alarmProcessing) {
        console.log("Heartbeat: Alarm not running, restarting...");
        await this._scheduleAlarm(100);
      }
      
      // Cleanup jika perlu
      if (this.wsSet.size === 0 && !this._cleanupInProgress) {
        // Server idle, tetap hidup
        if (this._tickCount % 5 === 0) {
          await this._saveState();
        }
      }
    } catch(e) {
      // Ignore
    }
  }
  
  // ==================== ALARM SYSTEM ====================
  
  async _scheduleAlarm(delayMs = C.ALARM_INTERVAL) {
    if (this.closing || this.isDestroyed) {
      this._alarmScheduled = false;
      return;
    }
    
    // Clean up any existing timeout
    if (this._alarmTimeout) {
      clearTimeout(this._alarmTimeout);
      this._alarmTimeout = null;
    }
    
    try {
      await this.state.storage.setAlarm(Date.now() + delayMs);
      this._alarmScheduled = true;
      this._lastAlarmTime = Date.now();
      this._alarmRescheduleAttempts = 0;
      
      // PERBAIKAN: Simpan status alarm
      await this.state.storage.put("alarmScheduled", true);
      await this.state.storage.put("lastAlarmSchedule", Date.now());
    } catch(e) {
      this._alarmScheduled = false;
      this._alarmRescheduleAttempts++;
      
      // PERBAIKAN: Simpan status gagal
      try {
        await this.state.storage.put("alarmScheduled", false);
        await this.state.storage.put("lastAlarmError", Date.now());
      } catch(e2) {}
      
      // Fallback with exponential backoff
      const backoffDelay = Math.min(5000 * Math.pow(2, this._alarmRescheduleAttempts), 30000);
      
      this._alarmTimeout = setTimeout(() => {
        if (!this.closing && !this.isDestroyed && this._alarmRescheduleAttempts < this._maxAlarmRescheduleAttempts) {
          this._scheduleAlarm(backoffDelay);
        } else if (this._alarmRescheduleAttempts >= this._maxAlarmRescheduleAttempts) {
          // Force reset alarm
          this._alarmScheduled = false;
          this._alarmRescheduleAttempts = 0;
          this._alarmProcessing = false;
          
          // PERBAIKAN: Coba lagi dengan delay lebih panjang
          setTimeout(() => {
            if (!this.closing && !this.isDestroyed) {
              this._scheduleAlarm(5000);
            }
          }, 10000);
        }
      }, 5000);
    }
  }
  
  async _cleanupDeadConnections() {
    if (this._cleanupInProgress) return;
    this._cleanupInProgress = true;
    
    try {
      const toRemove = [];
      
      for (const ws of this.wsSet) {
        // Hanya membersihkan koneksi yang sudah mati/terputus
        // TIDAK ADA auto-kick untuk user diam
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(ws);
        }
      }
      
      for (const ws of toRemove) {
        try {
          await this.cleanup(ws);
        } catch(e) {
          // Ignore cleanup errors
        }
      }
    } catch(e) {
      // Ignore
    } finally {
      this._cleanupInProgress = false;
    }
  }
  
  async _saveState() {
    try {
      await this.state.storage.put("currentNumber", this.currentNumber);
      await this.state.storage.put("tickCount", this._tickCount);
      await this.state.storage.put("lastAlive", Date.now()); // PERBAIKAN
    } catch(e) {
      // Log but don't crash
      console.error("Failed to save state:", e.message);
    }
  }
  
  // PERBAIKAN: Tambahkan method untuk recover
  async _recoverFromStaleState() {
    try {
      if (!this._alarmScheduled && !this._alarmProcessing && !this.closing && !this.isDestroyed) {
        console.log("Recovering stale server state...");
        
        // Cek storage
        const lastAlive = await this.state.storage.get("lastAlive");
        const alarmScheduled = await this.state.storage.get("alarmScheduled");
        
        if (!alarmScheduled || (lastAlive && Date.now() - lastAlive > 60000)) {
          // Reset alarm
          await this._scheduleAlarm(100);
          await this._saveState();
          console.log("Server recovered successfully");
        }
      }
    } catch(e) {
      console.error("Recovery failed:", e.message);
    }
  }
  
  async alarm() {
    if (this.closing || this.isDestroyed) {
      this._alarmScheduled = false;
      return;
    }
    
    // PERBAIKAN: Update heartbeat time
    this._lastHeartbeatTime = Date.now();
    
    // Handle stuck alarm
    if (this._alarmProcessing) {
      if (Date.now() - this._alarmStartTime > 30000) {
        // Force reset stuck alarm
        this._alarmProcessing = false;
        this._alarmFailCount++;
        
        // Reschedule
        if (!this.closing && !this.isDestroyed) {
          await this._scheduleAlarm(C.ALARM_INTERVAL);
        }
      }
      return;
    }
    
    this._alarmProcessing = true;
    this._alarmStartTime = Date.now();
    this._lastAlarmTime = Date.now();
    
    try {
      // Cleanup dead connections
      await this._cleanupDeadConnections();
      
      // Increment tick
      this._tickCount = (this._tickCount || 0) + 1;
      
      // Change number every N ticks
      if (this._tickCount % C.NUMBER_CHANGE_TICKS === 0) {
        this.currentNumber = this.currentNumber < 6 ? this.currentNumber + 1 : 1;
        
        for (const room of this.rooms.values()) {
          if (room) {
            room.setNumber(this.currentNumber);
          }
        }
        
        const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
        for (const [room, clients] of this.roomClients) {
          if (clients && clients.size > 0) {
            await this._broadcastToRoom(room, numberMsg);
          }
        }
      }
      
      // Save state
      await this._saveState();
      this._alarmFailCount = 0;
      
      // PERBAIKAN: Simpan status alarm sukses
      await this.state.storage.put("alarmScheduled", true);
      await this.state.storage.put("lastAlive", Date.now());
      
    } catch(e) {
      this._alarmFailCount++;
      console.error("Alarm error:", e.message);
      
      // PERBAIKAN: Simpan error state
      try {
        await this.state.storage.put("lastAlarmError", Date.now());
      } catch(e2) {}
    } finally {
      this._alarmProcessing = false;
      
      if (!this.closing && !this.isDestroyed) {
        await this._scheduleAlarm(C.ALARM_INTERVAL);
      }
    }
  }
  
  // ==================== BROADCAST ====================
  
  async _broadcastToRoom(room, msgStr) {
    if (this.closing || this.isDestroyed) return 0;
    const clients = this.roomClients.get(room);
    if (!clients?.size) return 0;
    
    let count = 0;
    const toRemove = [];
    
    for (const ws of clients) {
      // Skip if ws is invalid
      if (!ws || ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws)) {
        toRemove.push(ws);
        continue;
      }
      
      try { 
        ws.send(msgStr); 
        count++; 
      } catch(e) { 
        toRemove.push(ws); 
      }
    }
    
    // Cleanup failed connections
    for (const ws of toRemove) {
      clients.delete(ws);
      try {
        await this.cleanup(ws);
      } catch(e) {
        // Ignore
      }
    }
    
    return count;
  }
  
  async broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try {
      await this._broadcastToRoom(room, JSON.stringify(msg));
    } catch(e) {
      // Ignore broadcast errors
    }
  }
  
  safeSend(ws, msg) {
    if (!ws || ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
      return false;
    }
    
    try {
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      this.cleanup(ws).catch(() => {});
      return false;
    }
  }
  
  updateRoomCount(room) {
    if (this.closing || this.isDestroyed || !room) return 0;
    const roomMan = this.rooms.get(room);
    if (!roomMan) return 0;
    const count = roomMan.getCount();
    this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }
  
  sendAllStateTo(ws, room, excludeSelf = false) {
    if (!ws || !ws.username || ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
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
        this.safeSend(ws, ["allPointsList", room, allPoints]);
      }
    } catch(e) {
      // Ignore
    }
  }
  
  // ==================== CLEANUP ====================
  
  async cleanup(ws) {
    // Prevent multiple cleanup
    if (!ws || ws._cleaning || this._cleaningUp.has(ws) || this._isCleaningUp) {
      return;
    }
    
    ws._cleaning = true;
    this._cleaningUp.add(ws);
    this._isCleaningUp = true;
    
    try {
      const username = ws.username;
      const room = ws.room;
      
      // Remove from room clients
      if (room) {
        const clients = this.roomClients.get(room);
        if (clients) clients.delete(ws);
      }
      
      // Remove from multi room
      const activeData = this.wsActiveMulti.get(ws);
      if (activeData?.room) {
        const clients = this.roomClients.get(activeData.room);
        if (clients) clients.delete(ws);
      }
      this.wsActiveMulti.delete(ws);
      
      // Remove user connections
      if (username) {
        const connections = this.userConnections.get(username);
        if (connections) {
          connections.delete(ws);
          
          const seatInfo = this.userSeat.get(username);
          const isMulti = seatInfo?.isMulti === true;
          
          // Only remove seat if not multi and no connections left
          if (!isMulti && connections.size === 0) {
            this.userConnections.delete(username);
            this.userCountry.delete(username);
            
            if (seatInfo?.room) {
              const roomMan = this.rooms.get(seatInfo.room);
              if (roomMan) {
                const seatData = roomMan.getSeat(seatInfo.seat);
                if (seatData?.namauser === username) {
                  roomMan.removeSeat(seatInfo.seat);
                  await this.broadcast(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
                  this.updateRoomCount(seatInfo.room);
                }
              }
            }
            
            this.userSeat.delete(username);
            this.userRoom.delete(username);
          }
        }
      }
      
      // Remove from wsSet
      this.wsSet.delete(ws);
      
    } catch(e) {
      // Silent catch
    } finally {
      ws._cleaning = false;
      this._cleaningUp.delete(ws);
      this._isCleaningUp = false;
      
      // Close connection if still open
      try {
        if (ws && ws.readyState === 1) {
          ws.close(1000, "Cleanup");
        }
      } catch(e) {
        // Ignore
      }
    }
  }
  
  // ==================== HANDLE MESSAGE ====================
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) {
      return;
    }
    
    if (this._processingMessages.has(ws)) return;
    this._processingMessages.add(ws);
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MESSAGE_SIZE) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      switch(evt) {
        case "setIdTarget2":
          await this.handleSetId(ws, args[0], args[1]);
          break;
        
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          break;
        
        case "multiJoin": {
          const multiUsername = args[0];
          const multiRoomname = args[1];
          if (!multiUsername || !multiRoomname || this.closing || this.isDestroyed) break;
          
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
              oldRoomMan.points.delete(existingSeat);
              await this.broadcast(existingRoom, ["removeKursi", existingRoom, existingSeat]);
              this.updateRoomCount(existingRoom);
            }
            this.userSeat.delete(multiUsername);
            this.userRoom.delete(multiUsername);
          }
          
          const roomMan = this.rooms.get(multiRoomname);
          if (!roomMan || roomMan.getCount() >= C.MAX_SEATS) break;
          
          const seat = roomMan.addSeat(multiUsername, "", "", 0, 0, 0, 0);
          if (!seat) break;
          
          this.userSeat.set(multiUsername, { room: multiRoomname, seat, isMulti: true });
          this.userRoom.set(multiUsername, multiRoomname);
          if (!this.userCountry.has(multiUsername)) {
            this.userCountry.set(multiUsername, ws.clientCountry || "Unknown");
          }
          
          let connections = this.userConnections.get(multiUsername);
          if (!connections) connections = new Set();
          if (!connections.has(ws)) connections.add(ws);
          this.userConnections.set(multiUsername, connections);
          
          this.wsActiveMulti.set(ws, { username: multiUsername, room: multiRoomname });
          const roomClients = this.roomClients.get(multiRoomname);
          if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
          
          this.safeSend(ws, ["rooMasukMulti", seat, multiRoomname]);
          await this.broadcast(multiRoomname, ["roomUserCount", multiRoomname, roomMan.getCount()]);
          break;
        }
        
        case "exitMulti": {
          const targetUsername = args[0];
          if (!targetUsername) break;
          
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
            roomMan.points.delete(seatNumber);
            await this.broadcast(roomName, ["removeKursi", roomName, seatNumber]);
            await this.broadcast(roomName, ["roomUserCount", roomName, roomMan.getCount()]);
          }
          
          this.userSeat.delete(targetUsername);
          this.userRoom.delete(targetUsername);
          
          const connections = this.userConnections.get(targetUsername);
          if (connections) {
            connections.delete(ws);
            if (connections.size === 0) {
              this.userConnections.delete(targetUsername);
              this.userCountry.delete(targetUsername);
            }
          }
          
          if (ws.username === targetUsername) {
            ws.username = null;
            ws.idtarget = null;
          }
          
          break;
        }
        
        case "setActiveMulti": {
          const targetUsername = args[0];
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
          if (roomName) await this.broadcast(roomName, ["userActiveChanged", targetUsername, seatNumber]);
          break;
        }
        
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          const roomMan = this.rooms.get(kursiRoom);
          if (!roomMan) break;
          
          const updated = roomMan.updateSeat(kursiSeat, {
            noimageUrl: kursiNoimg, namauser: kursiName, color: kursiColor,
            itembawah: kursiBawah, itematas: kursiAtas, vip: kursiVip, viptanda: kursiVt
          });
          
          if (updated) {
            const updatedSeat = roomMan.getSeat(kursiSeat);
            await this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
          }
          break;
        }
        
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (chatMsg && ROOMS_SET.has(chatRoom)) {
            await this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]);
          }
          break;
        }
        
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (pointRoom && typeof pointSeat === 'number' && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
            const roomMan = this.rooms.get(pointRoom);
            if (roomMan?.updatePoint(pointSeat, pointX, pointY, pointFast === 1)) {
              await this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]);
            }
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          const roomMan = this.rooms.get(removeRoom);
          if (roomMan?.getSeat(removeSeat)) {
            roomMan.removeSeat(removeSeat);
            roomMan.points.delete(removeSeat);
            await this.broadcast(removeRoom, ["removeKursi", removeRoom, removeSeat]);
            this.updateRoomCount(removeRoom);
          }
          break;
        }
        
        case "private": {
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
          break;
        }
        
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (giftRoom && ROOMS_SET.has(giftRoom)) {
            await this.broadcast(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, giftGiftName, Date.now()]);
          }
          break;
        }
        
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (rollRoom && ROOMS_SET.has(rollRoom)) {
            await this.broadcast(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, rollAngka]);
          }
          break;
        }
        
        case "sendnotif": {
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
          break;
        }
        
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        
        case "isUserOnline": {
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
          break;
        }
        
        case "getOnlineUsers": {
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
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) {
            const rm = this.rooms.get(room);
            counts[room] = rm?.getCount() || 0;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = args[0];
          if (roomName && ROOMS_SET.has(roomName)) {
            const rm = this.rooms.get(roomName);
            this.safeSend(ws, ["roomUserCount", roomName, rm?.getCount() || 0]);
          }
          break;
        }
        
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (!muteRoom || !ROOMS_SET.has(muteRoom)) break;
          
          const rm = this.rooms.get(muteRoom);
          if (!rm) break;
          
          rm.setMuted(muteVal);
          await this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
          break;
        }
        
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (getMuteRoom && ROOMS_SET.has(getMuteRoom)) {
            const rm = this.rooms.get(getMuteRoom);
            this.safeSend(ws, ["muteTypeResponse", rm?.getMuted() || false, getMuteRoom]);
          }
          break;
        }
        
        case "onDestroy":
          await this.cleanup(ws);
          break;
      }
    } catch(e) {
      // Silent catch
    } finally {
      this._processingMessages.delete(ws);
    }
  }
  
  // ==================== HANDLE SET ID ====================
  
  async handleSetId(ws, username, isNewUser) {
    if (!ws || !username || typeof username !== 'string' || username.length === 0 || this.closing || this.isDestroyed) {
      try { 
        if (ws?.readyState === 1) ws.close(1000, "Invalid username"); 
      } catch(e) {}
      return;
    }
    
    const userCountry = ws.clientCountry || "Unknown";
    
    const existingSeatInfo = this.userSeat.get(username);
    const isMultiUser = existingSeatInfo?.isMulti === true;
    
    if (isMultiUser && isNewUser === false) {
      ws.username = username;
      ws.idtarget = username;
      ws.room = existingSeatInfo.room;
      ws.roomname = existingSeatInfo.room;
      
      if (!this.userCountry.has(username)) {
        this.userCountry.set(username, userCountry);
      }
      
      let connections = this.userConnections.get(username);
      if (!connections) connections = new Set();
      if (!connections.has(ws)) connections.add(ws);
      this.userConnections.set(username, connections);
      
      if (!this.wsSet.has(ws)) this.wsSet.add(ws);
      
      const roomClients = this.roomClients.get(existingSeatInfo.room);
      if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
      
      const roomMan = this.rooms.get(existingSeatInfo.room);
      if (roomMan && !this.isDestroyed) {
        try {
          const seatData = roomMan.getSeat(existingSeatInfo.seat);
          const pointData = roomMan.getPoint(existingSeatInfo.seat);
          
          this.safeSend(ws, ["numberKursiSaya", existingSeatInfo.seat]);
          if (seatData) this.safeSend(ws, ["kursiData", existingSeatInfo.room, existingSeatInfo.seat, seatData]);
          if (pointData) this.safeSend(ws, ["pointData", existingSeatInfo.room, existingSeatInfo.seat, pointData.x, pointData.y, pointData.fast ? 1 : 0]);
          this.safeSend(ws, ["muteTypeResponse", roomMan.getMuted(), existingSeatInfo.room]);
          this.sendAllStateTo(ws, existingSeatInfo.room, true);
        } catch(e) {
          // Ignore
        }
      }
      return;
    }
    
    // Clean up existing connections for this user
    const existingConns = this.userConnections.get(username);
    if (existingConns?.size > 0) {
      for (const oldWs of Array.from(existingConns)) {
        if (oldWs && oldWs !== ws && oldWs.readyState === 1) {
          await this.cleanup(oldWs);
        }
      }
    }
    
    ws.username = username;
    ws.idtarget = username;
    if (!this.userCountry.has(username)) {
      this.userCountry.set(username, userCountry);
    }
    
    let connections = this.userConnections.get(username);
    if (!connections) connections = new Set();
    if (!connections.has(ws)) connections.add(ws);
    this.userConnections.set(username, connections);
    
    if (!this.wsSet.has(ws)) this.wsSet.add(ws);
    
    this.safeSend(ws, isNewUser ? ["joinroomawal"] : ["needJoinRoom"]);
  }
  
  // ==================== HANDLE JOIN ====================
  
  async handleJoin(ws, roomName) {
    if (!ws || !ws.username || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) {
      return false;
    }
    
    const username = ws.username;
    const oldRoom = ws.room;
    
    if (oldRoom && oldRoom !== roomName) {
      try {
        const oldMan = this.rooms.get(oldRoom);
        if (oldMan) {
          const oldSeat = this.userSeat.get(username)?.seat;
          if (oldSeat) {
            oldMan.removeSeat(oldSeat);
            await this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
            this.updateRoomCount(oldRoom);
          }
        }
        const oldClients = this.roomClients.get(oldRoom);
        if (oldClients) oldClients.delete(ws);
        this.userSeat.delete(username);
        this.userRoom.delete(username);
      } catch(e) {
        // Ignore
      }
      ws.room = null;
      ws.roomname = null;
    }
    
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return false;
    
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data?.namauser === username) { seat = s; break; }
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
    
    this.userSeat.set(username, { room: roomName, seat, isMulti: false });
    this.userRoom.set(username, roomName);
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = username;
    
    const roomClients = this.roomClients.get(roomName);
    if (roomClients && !roomClients.has(ws)) roomClients.add(ws);
    
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomMan.getMuted(), roomName]);
    this.safeSend(ws, ["roomUserCount", roomName, roomMan.getCount()]);
    
    this.updateRoomCount(roomName);
    
    // Schedule delayed state send with cleanup
    const timeout = setTimeout(() => {
      this._pendingTimeouts.delete(timeout);
      try {
        if (ws && ws.readyState === 1 && !this.closing && !this.isDestroyed) {
          this.sendAllStateTo(ws, roomName, true);
        }
      } catch(e) {
        // Ignore
      }
    }, 1000);
    
    this._pendingTimeouts.add(timeout);
    
    return true;
  }
  
  // ==================== FETCH ====================
  
  async fetch(req) {
    // PERBAIKAN: Recover dari stale state
    if (!this._alarmScheduled && !this._alarmProcessing && !this.closing && !this.isDestroyed) {
      await this._recoverFromStaleState();
    }
    
    // Ensure alarm is running
    if (!this._alarmProcessing && !this.closing && !this.isDestroyed) {
      try {
        await this.alarm();
        await this._scheduleAlarm(C.ALARM_INTERVAL);
      } catch(e) {
        // Ignore
      }
    }
    
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    try {
      const url = new URL(req.url);
      
      // Health check
      if (url.pathname === "/health") {
        const roomCounts = {};
        for (const [room, rm] of this.rooms) {
          roomCounts[room] = rm?.getCount() || 0;
        }
        
        return new Response(JSON.stringify({
          status: "alive",
          alarmScheduled: this._alarmScheduled,
          alarmProcessing: this._alarmProcessing,
          lastAlarm: this._lastAlarmTime,
          timeSinceLastAlarm: Date.now() - this._lastAlarmTime,
          tickCount: this._tickCount,
          currentNumber: this.currentNumber,
          wsConnections: this.wsSet.size,
          userCount: this.userConnections.size,
          alarmFailCount: this._alarmFailCount,
          roomCounts: roomCounts,
          pendingTimeouts: this._pendingTimeouts.size,
          cleaningUp: this._cleaningUp.size,
          heartbeatActive: this._heartbeatInterval !== null,
          lastHeartbeat: this._lastHeartbeatTime
        }), { 
          headers: { 
            "Content-Type": "application/json",
            "Cache-Control": "no-cache" // PERBAIKAN
          } 
        });
      }
      
      // PERBAIKAN: Endpoint ping untuk keep-alive
      if (url.pathname === "/ping") {
        return new Response("pong", { 
          headers: { 
            "Content-Type": "text/plain",
            "Cache-Control": "no-cache"
          } 
        });
      }
      
      // PERBAIKAN: Endpoint keep-alive
      if (url.pathname === "/keep-alive" && req.method === "POST") {
        if (!this._alarmProcessing) {
          await this.alarm();
          await this._scheduleAlarm(C.ALARM_INTERVAL);
        }
        await this._saveState();
        return new Response(JSON.stringify({
          success: true,
          alarmScheduled: this._alarmScheduled,
          timestamp: Date.now()
        }), { 
          headers: { 
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
          } 
        });
      }
      
      // PERBAIKAN: Endpoint status lengkap
      if (url.pathname === "/status") {
        return new Response(JSON.stringify({
          alive: true,
          alarmRunning: this._alarmScheduled || this._alarmProcessing,
          wsCount: this.wsSet.size,
          userCount: this.userConnections.size,
          uptime: Date.now() - this._lastAlarmTime,
          timestamp: Date.now()
        }), { 
          headers: { 
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
          } 
        });
      }
      
      // Trigger alarm manually
      if (url.pathname === "/trigger-alarm" && req.method === "POST") {
        await this.alarm();
        return new Response(JSON.stringify({ 
          success: true, 
          message: "Alarm triggered manually" 
        }), { 
          headers: { "Content-Type": "application/json" } 
        });
      }
      
      // Reset server
      if (url.pathname === "/reset" && req.method === "POST") {
        await this.reset();
        return new Response(JSON.stringify({ 
          success: true, 
          message: "Server reset successfully" 
        }), { 
          headers: { "Content-Type": "application/json" } 
        });
      }
      
      // Force cleanup
      if (url.pathname === "/cleanup" && req.method === "POST") {
        await this._cleanupDeadConnections();
        return new Response(JSON.stringify({ 
          success: true, 
          message: "Cleanup triggered" 
        }), { 
          headers: { "Content-Type": "application/json" } 
        });
      }
      
      // WebSocket upgrade
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server - ALARM ACTIVE", { 
          status: 200,
          headers: {
            "Cache-Control": "no-cache"
          }
        });
      }
      
      // Check max connections
      if (this.wsSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server full", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      const clientCountry = this._getClientCountry(req);
      
      try { 
        this.state.acceptWebSocket(server); 
      } catch(e) { 
        return new Response("WebSocket acceptance failed", { status: 500 }); 
      }
      
      // Initialize server WebSocket
      server.username = null;
      server.room = null;
      server.roomname = null;
      server.idtarget = null;
      server._closing = false;
      server.clientCountry = clientCountry;
      server._wsId = Date.now() + Math.random(); // Unique ID
      
      // Add to wsSet
      if (!this.wsSet.has(server)) {
        this.wsSet.add(server);
      }
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      console.error("Fetch error:", e.message);
      return new Response("Internal Server Error", { status: 500 });
    }
  }
  
  // ==================== RESET ====================
  
  async reset() {
    this.closing = true;
    
    // Clear all pending timeouts
    for (const timeout of this._pendingTimeouts) {
      clearTimeout(timeout);
    }
    this._pendingTimeouts.clear();
    
    // PERBAIKAN: Clear heartbeat
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
      this._heartbeatInterval = null;
    }
    
    // Notify and close all connections
    const wsCopy = Array.from(this.wsSet);
    for (const ws of wsCopy) {
      if (ws?.readyState === 1) {
        try { 
          ws.send(JSON.stringify(["serverRestart", "Restarting..."])); 
        } catch(e) {}
        try { 
          ws.close(1000, "Restart"); 
        } catch(e) {}
      }
      try {
        await this.cleanup(ws);
      } catch(e) {
        // Ignore
      }
    }
    
    // Clear all data
    this.wsSet.clear();
    this.userConnections.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this.userCountry.clear();
    this.wsActiveMulti.clear();
    this._processingMessages.clear();
    this._cleaningUp.clear();
    
    // Reinitialize rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // Reset state
    this.currentNumber = 1;
    this._tickCount = 0;
    this._alarmProcessing = false;
    this._alarmScheduled = false;
    this._alarmFailCount = 0;
    this._alarmRescheduleAttempts = 0;
    this._lastHeartbeatTime = Date.now(); // PERBAIKAN
    
    if (this._alarmTimeout) {
      clearTimeout(this._alarmTimeout);
      this._alarmTimeout = null;
    }
    
    this.closing = false;
    this.isDestroyed = false;
    
    // Restart alarm
    try {
      await this.alarm();
      await this._scheduleAlarm(C.ALARM_INTERVAL);
      // PERBAIKAN: Restart heartbeat
      this._startHeartbeat();
    } catch(e) {
      // Ignore
    }
  }
  
  // ==================== WEB SOCKET EVENTS ====================
  
  async webSocketMessage(ws, msg) { 
    if (!ws || ws._closing || this._cleaningUp.has(ws) || this.closing || this.isDestroyed) return;
    await this.handleMessage(ws, msg); 
  }
  
  async webSocketClose(ws) { 
    if (!ws) return;
    await this.cleanup(ws); 
  }
  
  async webSocketError(ws) { 
    if (!ws) return;
    await this.cleanup(ws); 
  }
  
  // ==================== DESTROY ====================
  
  async destroy() {
    if (this.isDestroyed) return;
    this.closing = true;
    this.isDestroyed = true;
    
    // PERBAIKAN: Clear heartbeat
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
      this._heartbeatInterval = null;
    }
    
    // Clear all pending timeouts
    for (const timeout of this._pendingTimeouts) {
      clearTimeout(timeout);
    }
    this._pendingTimeouts.clear();
    
    // Clear alarm timeout
    if (this._alarmTimeout) {
      clearTimeout(this._alarmTimeout);
      this._alarmTimeout = null;
    }
    
    // Close all connections
    const wsCopy = Array.from(this.wsSet);
    for (const ws of wsCopy) {
      if (ws?.readyState === 1) {
        try { 
          ws.send(JSON.stringify(["serverShutdown", "Server shutting down"])); 
        } catch(e) {}
        try { 
          ws.close(1000, "Shutdown"); 
        } catch(e) {}
      }
      try {
        await this.cleanup(ws);
      } catch(e) {
        // Ignore
      }
    }
    
    // Clear all data
    this.wsSet.clear();
    this.userConnections.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this.userCountry.clear();
    this.wsActiveMulti.clear();
    this.roomClients.clear();
    this.rooms.clear();
    this._processingMessages.clear();
    this._cleaningUp.clear();
  }
  
  // ==================== HELPER ====================
  
  _getClientCountry(req) {
    try {
      const country = req.headers.get("CF-IPCountry") || 
                      req.headers.get("X-Country-Code") ||
                      "Unknown";
      return country;
    } catch(e) { 
      return "Unknown"; 
    }
  }
}
