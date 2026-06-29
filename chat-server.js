// ==================== CHAT SERVER - NO MONITORING ====================

const C = {
  MAX_SEATS: 45,
  MAX_GLOBAL_CONNECTIONS: 500,
  MAX_MESSAGE_SIZE: 5000,
  INTERVAL_15_MENIT: 900000,   // 15 MENIT
  MAX_NUMBER: 6,
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
    
    // Number system
    this.currentNumber = 1;
    this._lastNumberChange = Date.now();
    
    // HANYA 1 INTERVAL = 15 MENIT
    this._mainInterval = null;
    this._lastActivityTime = Date.now();
    
    // Initialize rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // Start ONLY 1 interval (15 menit)
    this._startMainInterval();
  }
  
  // ==================== MAIN INTERVAL (15 MENIT) ====================
  
  _startMainInterval() {
    if (this._mainInterval) {
      clearInterval(this._mainInterval);
    }
    
    this._mainInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._doMainTask();
      }
    }, C.INTERVAL_15_MENIT);
  }
  
  _doMainTask() {
    try {
      this._lastActivityTime = Date.now();
      
      // ===== UPDATE NUMBER - CYCLE 1-6 =====
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      for (const room of this.rooms.values()) {
        if (room) {
          room.setNumber(this.currentNumber);
        }
      }
      
      const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const [room, clients] of this.roomClients) {
        if (clients && clients.size > 0) {
          this._broadcastToRoom(room, numberMsg).catch(() => {});
        }
      }
      
      // ===== CLEANUP =====
      this._doCleanup().catch(() => {});
      
    } catch(e) {}
  }
  
  // ==================== CLEANUP ====================
  
  async _doCleanup() {
    if (this._cleanupInProgress) return;
    this._cleanupInProgress = true;
    
    try {
      const toRemove = [];
      for (const ws of this.wsSet) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(ws);
        }
      }
      
      for (const ws of toRemove) {
        try {
          await this.cleanup(ws);
        } catch(e) {}
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
    finally {
      this._cleanupInProgress = false;
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
    
    for (const ws of toRemove) {
      clients.delete(ws);
      try {
        await this.cleanup(ws);
      } catch(e) {}
    }
    
    return count;
  }
  
  async broadcast(room, msg) {
    if (this.closing || this.isDestroyed || !room || !msg) return;
    try {
      await this._broadcastToRoom(room, JSON.stringify(msg));
    } catch(e) {}
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
    } catch(e) {}
  }
  
  // ==================== CLEANUP ====================
  
  async cleanup(ws) {
    if (!ws || ws._cleaning || this._cleaningUp.has(ws) || this._isCleaningUp) {
      return;
    }
    
    ws._cleaning = true;
    this._cleaningUp.add(ws);
    this._isCleaningUp = true;
    
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
      
      this.wsSet.delete(ws);
      
    } catch(e) {}
    finally {
      ws._cleaning = false;
      this._cleaningUp.delete(ws);
      this._isCleaningUp = false;
      
      try {
        if (ws && ws.readyState === 1) {
          ws.close(1000, "Cleanup");
        }
      } catch(e) {}
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
            if (roomMan && roomMan.seats.has(pointSeat)) {
              if (roomMan.updatePoint(pointSeat, pointX, pointY, pointFast === 1)) {
                await this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]);
              }
            }
          }
          break;
        }
        
        case "removeKursiAndPoint": {
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

        case "modwarning": {
          const modRoom = args[0];
          if (modRoom && ROOMS_SET.has(modRoom)) {
            await this.broadcast(modRoom, ["modwarning", modRoom]);
          }
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
    } catch(e) {}
    finally {
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
        } catch(e) {}
      }
      return;
    }
    
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
      } catch(e) {}
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
    
    setTimeout(() => {
      try {
        if (ws && ws.readyState === 1 && !this.closing && !this.isDestroyed) {
          this.sendAllStateTo(ws, roomName, true);
        }
      } catch(e) {}
    }, 1000);
    
    return true;
  }
  
  // ==================== FETCH ====================
  
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    try {
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server", { 
          status: 200,
          headers: {
            "Cache-Control": "no-cache"
          }
        });
      }
      
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
      
      server.username = null;
      server.room = null;
      server.roomname = null;
      server.idtarget = null;
      server._closing = false;
      server.clientCountry = clientCountry;
      server._wsId = Date.now() + Math.random();
      
      if (!this.wsSet.has(server)) {
        this.wsSet.add(server);
      }
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      console.error("Fetch error:", e.message);
      return new Response("Internal Server Error", { status: 500 });
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
    
    if (this._mainInterval) {
      clearInterval(this._mainInterval);
      this._mainInterval = null;
    }
    
    for (const timeout of this._pendingTimeouts) {
      clearTimeout(timeout);
    }
    this._pendingTimeouts.clear();
    
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
      } catch(e) {}
    }
    
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
