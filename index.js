// index.js - ChatServer FINAL - FIXED VERSION
import { LowCardGameManager } from "./lowcard.js";

const CONSTANTS = Object.freeze({
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 4000,
  MAX_JSON_DEPTH: 30,
  POINTS_CACHE_MS: 30,
  MESSAGE_TTL_MS: 3000,
  MAX_TOTAL_BUFFER_MESSAGES: 20,
  MAX_SEND_PER_TICK: 20,
  MASTER_TIMER_INTERVAL: 1000,
  MAX_USER_CONNECTIONS: 500,
  MAX_ROOM_CLIENTS: 300,
  MAX_CHAT_BUFFER_QUEUE: 100,
});

const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", 
  "India", "Indonesia", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love", 
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa", 
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"
]);

function safeStringify(obj) {
  try {
    const seen = new WeakSet();
    return JSON.stringify(obj, (key, value) => {
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return '[Circular]';
        seen.add(value);
      }
      return value;
    });
  } catch (e) {
    return JSON.stringify({ error: "Stringify failed" });
  }
}

function safeParseJSON(str) {
  if (!str || str.length > 4000) return null;
  try { return JSON.parse(str); } catch { return null; }
}

class SimpleChatBuffer {
  constructor() {
    this.queue = [];
    this.callback = null;
    this._flushing = false;
  }
  
  setCallback(cb) { this.callback = cb; }
  
  add(room, msg) {
    if (!this.callback) return;
    if (this.queue.length > CONSTANTS.MAX_CHAT_BUFFER_QUEUE) {
      this.queue = [];
      return;
    }
    this.queue.push({ room, msg, time: Date.now() });
    if (this.queue.length >= CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES) this.flush();
  }
  
  flush() {
    if (this._flushing || this.queue.length === 0) return;
    this._flushing = true;
    
    try {
      const now = Date.now();
      const toSend = this.queue.filter(item => now - item.time < CONSTANTS.MESSAGE_TTL_MS);
      this.queue = [];
      
      for (const item of toSend) {
        try { this.callback(item.room, item.msg); } catch(e) {}
      }
    } finally {
      this._flushing = false;
    }
  }
  
  destroy() { 
    this.flush(); 
    this.callback = null; 
    this.queue = null; 
  }
}

class SeatData {
  constructor() {
    this.noimageUrl = "";
    this.namauser = "";
    this.color = "";
    this.itembawah = 0;
    this.itematas = 0;
    this.vip = 0;
    this.viptanda = 0;
    this.lastPoint = null;
  }
  
  isEmpty() { return !this.namauser; }
  
  clear() {
    this.noimageUrl = "";
    this.namauser = "";
    this.color = "";
    this.itembawah = 0;
    this.itematas = 0;
    this.vip = 0;
    this.viptanda = 0;
    this.lastPoint = null;
  }
  
  copyFrom(other) {
    if (other) {
      this.noimageUrl = other.noimageUrl || "";
      this.namauser = other.namauser || "";
      this.color = other.color || "";
      this.itembawah = other.itembawah || 0;
      this.itematas = other.itematas || 0;
      this.vip = other.vip || 0;
      this.viptanda = other.viptanda || 0;
      this.lastPoint = other.lastPoint ? { ...other.lastPoint } : null;
    }
    return this;
  }
  
  toJSON() {
    return {
      noimageUrl: this.noimageUrl,
      namauser: this.namauser,
      color: this.color,
      itembawah: this.itembawah,
      itematas: this.itematas,
      vip: this.vip,
      viptanda: this.viptanda,
      lastPoint: this.lastPoint
    };
  }
}

class RoomManager {
  constructor() {
    this.seats = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
    this.pointsCache = null;
    this.pointsCacheTime = 0;
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) this.seats.set(i, new SeatData());
  }
  
  getOccupiedCount() {
    let count = 0;
    for (const seat of this.seats.values()) if (!seat.isEmpty()) count++;
    return count;
  }
  
  getSeat(num) { 
    const seat = this.seats.get(num); 
    return seat ? seat.toJSON() : null; 
  }
  
  replaceSeat(num, data) {
    const seat = this.seats.get(num);
    if (seat) { 
      seat.copyFrom(data); 
      this.pointsCache = null; 
      return true; 
    }
    return false;
  }
  
  replacePoint(num, point) {
    const seat = this.seats.get(num);
    if (seat && !seat.isEmpty()) {
      const x = parseFloat(point.x);
      const y = parseFloat(point.y);
      if (isNaN(x) || isNaN(y)) return false;
      
      seat.lastPoint = { x, y, fast: point.fast || false };
      this.pointsCache = null;
      return true;
    }
    return false;
  }
  
  removeSeat(num) {
    const seat = this.seats.get(num);
    if (seat) { 
      seat.clear(); 
      this.pointsCache = null; 
      return true; 
    }
    return false;
  }
  
  getAllSeatsMeta() {
    const meta = {};
    for (const [num, seat] of this.seats) {
      if (!seat.isEmpty()) {
        meta[num] = {
          noimageUrl: seat.noimageUrl,
          namauser: seat.namauser,
          color: seat.color,
          itembawah: seat.itembawah,
          itematas: seat.itematas,
          vip: seat.vip,
          viptanda: seat.viptanda
        };
      }
    }
    return meta;
  }
  
  getAllPoints() {
    if (this.pointsCache && Date.now() - this.pointsCacheTime < CONSTANTS.POINTS_CACHE_MS) return this.pointsCache;
    const points = [];
    for (const [num, seat] of this.seats) {
      if (seat.lastPoint && !seat.isEmpty()) {
        points.push({ seat: num, x: seat.lastPoint.x, y: seat.lastPoint.y, fast: seat.lastPoint.fast ? 1 : 0 });
      }
    }
    this.pointsCache = points;
    this.pointsCacheTime = Date.now();
    return points;
  }
  
  setMute(val) { this.muteStatus = !!val; return this.muteStatus; }
  getMute() { return this.muteStatus; }
  setCurrentNumber(n) { this.currentNumber = n; }
  getCurrentNumber() { return this.currentNumber; }
  
  destroy() {
    for (const seat of this.seats.values()) seat.clear();
    this.seats.clear();
    this.pointsCache = null;
  }
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._lastCleanup = 0;
    this._lastLog = 0;
    this._errorLogs = [];
    
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.activeClients = new Set();
    this._activeListeners = new Map();
    
    this.chatBuffer = new SimpleChatBuffer();
    this.chatBuffer.setCallback((room, msg) => this._sendToRoom(room, msg));
    
    try { 
      this.lowcard = new LowCardGameManager(this); 
    } catch(e) { 
      console.error("LowCard init failed:", e);
      this.lowcard = null; 
    }
    
    this.currentNumber = 1;
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager());
      this.roomClients.set(room, []);
    }
    
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TIMER_INTERVAL);
    this._tickCounter = 0;
  }
  
  _masterTick() {
    if (this._isClosing) return;
    this._tickCounter++;
    if (this._tickCounter % 900 === 0) this._doNumberTick();
    if (this.lowcard && this.lowcard.masterTick) this.lowcard.masterTick();
    if (this._tickCounter % 30 === 0) this._quickCleanup();
    this.chatBuffer.flush();
  }
  
  _doNumberTick() {
    this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
    for (const rm of this.roomManagers.values()) rm.setCurrentNumber(this.currentNumber);
    const msg = safeStringify(["currentNumber", this.currentNumber]);
    const notified = new Set();
    for (const client of this.activeClients) {
      if (client?.readyState === 1 && client.roomname && !notified.has(client.idtarget)) {
        try { client.send(msg); notified.add(client.idtarget); } catch(e) {}
      }
    }
  }
  
  _quickCleanup() {
    const now = Date.now();
    if (now - this._lastCleanup < 30000) return;
    this._lastCleanup = now;
    
    if (this.userConnections.size > CONSTANTS.MAX_USER_CONNECTIONS) {
      const entries = Array.from(this.userConnections.entries());
      entries.sort((a, b) => (a[1]?.size || 0) - (b[1]?.size || 0));
      const toDelete = entries.slice(0, this.userConnections.size - 400);
      for (const [userId] of toDelete) {
        this.userConnections.delete(userId);
        this._removeUserFromSeat(userId);
      }
    }
    
    for (const [userId, seatInfo] of this.userToSeat) {
      if (!this.userConnections.has(userId)) {
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
    }
    
    for (const [room, clients] of this.roomClients) {
      const alive = clients.filter(c => c && c.readyState === 1 && c.roomname === room);
      if (alive.length !== clients.length) {
        this.roomClients.set(room, alive);
      }
    }
    
    if (this.chatBuffer.queue && this.chatBuffer.queue.length > CONSTANTS.MAX_CHAT_BUFFER_QUEUE) {
      this.chatBuffer.queue = [];
    }
    
    if (this.lowcard) this.lowcard.cleanupStaleGames();
    
    if (!this._lastLog || now - this._lastLog > 300000) {
      console.log(`[STATS] Users: ${this.userConnections.size}, Connections: ${this.activeClients.size}, Games: ${this.lowcard?.activeGames?.size || 0}`);
      this._lastLog = now;
    }
  }
  
  _cleanupWebSocket(ws) {
    if (!ws) return;
    
    const listeners = this._activeListeners.get(ws);
    if (listeners) {
      for (const { event, handler } of listeners) {
        try { ws.removeEventListener(event, handler); } catch(e) {}
      }
      this._activeListeners.delete(ws);
    }
    
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    if (userId) {
      const conns = this.userConnections.get(userId);
      if (conns) {
        conns.delete(ws);
        if (conns.size === 0) {
          this.userConnections.delete(userId);
          this._removeUserFromSeat(userId, room);
        }
      }
    }
    
    if (room) {
      const clients = this.roomClients.get(room);
      if (clients) {
        const idx = clients.indexOf(ws);
        if (idx !== -1) clients.splice(idx, 1);
      }
    }
    
    this.activeClients.delete(ws);
    
    try {
      delete ws.idtarget;
      delete ws.roomname;
      delete ws._isClosing;
      delete ws._connectionTime;
    } catch(e) {}
  }
  
  _removeUserFromSeat(userId, room) {
    const seatInfo = this.userToSeat.get(userId);
    if (seatInfo) {
      const targetRoom = room || seatInfo.room;
      const rm = this.roomManagers.get(targetRoom);
      if (rm && seatInfo.seat) {
        rm.removeSeat(seatInfo.seat);
        this._sendToRoom(targetRoom, ["removeKursi", targetRoom, seatInfo.seat]);
        this._sendToRoom(targetRoom, ["roomUserCount", targetRoom, rm.getOccupiedCount()]);
      }
    }
    this.userToSeat.delete(userId);
    this.userCurrentRoom.delete(userId);
  }
  
  _sendToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.length === 0) return 0;
    
    const str = safeStringify(msg);
    let sent = 0;
    const batch = clients.slice(0, CONSTANTS.MAX_SEND_PER_TICK);
    
    for (let i = 0; i < batch.length; i++) {
      const client = batch[i];
      if (client && client.readyState === 1 && client.roomname === room) {
        try { client.send(str); sent++; } catch(e) {}
      }
    }
    
    if (clients.length > CONSTANTS.MAX_SEND_PER_TICK) {
      const remaining = clients.slice(CONSTANTS.MAX_SEND_PER_TICK);
      setTimeout(() => {
        for (const client of remaining) {
          if (client && client.readyState === 1 && client.roomname === room) {
            try { client.send(str); } catch(e) {}
          }
        }
      }, 10);
    }
    
    return sent;
  }
  
  broadcastToRoom(room, msg) {
    if (!roomList.includes(room)) return 0;
    if (msg[0] === "chat") {
      const rm = this.roomManagers.get(room);
      if (rm && rm.getMute()) return 0;
      this.chatBuffer.add(room, msg);
      return this.roomManagers.get(room)?.getOccupiedCount() || 0;
    }
    return this._sendToRoom(room, msg);
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
    try {
      const str = typeof msg === "string" ? msg : safeStringify(msg);
      if (str.length > CONSTANTS.MAX_MESSAGE_SIZE) return false;
      ws.send(str);
      return true;
    } catch(e) { 
      this._cleanupWebSocket(ws); 
      return false; 
    }
  }
  
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) return false;
    if (!roomList.includes(room)) return false;
    
    try {
      const existing = this.userToSeat.get(ws.idtarget);
      if (existing && existing.room === room) {
        const rm = this.roomManagers.get(room);
        const seat = rm.getSeat(existing.seat);
        if (seat && seat.namauser === ws.idtarget) {
          ws.roomname = room;
          this._addToRoom(ws, room);
          await this._sendRoomState(ws, room);
          await this.safeSend(ws, ["rooMasuk", existing.seat, room]);
          await this.safeSend(ws, ["numberKursiSaya", existing.seat]);
          return true;
        }
        this.userToSeat.delete(ws.idtarget);
      }
      
      if (ws.roomname && ws.roomname !== room) await this._leaveRoom(ws, ws.roomname);
      
      const rm = this.roomManagers.get(room);
      if (rm.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      let assignedSeat = null;
      for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
        const seat = rm.getSeat(i);
        if (!seat || !seat.namauser) {
          assignedSeat = i;
          break;
        }
      }
      
      if (!assignedSeat) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      const emptySeat = new SeatData();
      emptySeat.namauser = ws.idtarget;
      rm.replaceSeat(assignedSeat, emptySeat.toJSON());
      
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      
      this._addToRoom(ws, room);
      this._addUserConnection(ws.idtarget, ws);
      
      await this._sendRoomState(ws, room);
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["roomUserCount", room, rm.getOccupiedCount()]);
      this._sendToRoom(room, ["userOccupiedSeat", room, assignedSeat, ws.idtarget]);
      
      return true;
    } catch(e) {
      console.error("handleJoinRoom error:", e);
      return false;
    }
  }
  
  async _sendRoomState(ws, room) {
    const rm = this.roomManagers.get(room);
    if (!rm) return;
    
    const seatInfo = this.userToSeat.get(ws.idtarget);
    const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
    
    const allMeta = rm.getAllSeatsMeta();
    const allPoints = rm.getAllPoints();
    
    if (selfSeat) {
      delete allMeta[selfSeat];
      const filteredPoints = allPoints.filter(p => p.seat !== selfSeat);
      if (Object.keys(allMeta).length) await this.safeSend(ws, ["allUpdateKursiList", room, allMeta]);
      if (filteredPoints.length) await this.safeSend(ws, ["allPointsList", room, filteredPoints]);
    } else {
      if (Object.keys(allMeta).length) await this.safeSend(ws, ["allUpdateKursiList", room, allMeta]);
      if (allPoints.length) await this.safeSend(ws, ["allPointsList", room, allPoints]);
    }
  }
  
  async _leaveRoom(ws, room) {
    if (!ws?.idtarget) return;
    
    const seatInfo = this.userToSeat.get(ws.idtarget);
    if (seatInfo && seatInfo.room === room) {
      const rm = this.roomManagers.get(room);
      if (rm) {
        rm.removeSeat(seatInfo.seat);
        this._sendToRoom(room, ["removeKursi", room, seatInfo.seat]);
        this._sendToRoom(room, ["roomUserCount", room, rm.getOccupiedCount()]);
      }
    }
    
    this.userToSeat.delete(ws.idtarget);
    this.userCurrentRoom.delete(ws.idtarget);
    
    const clients = this.roomClients.get(room);
    if (clients) {
      const idx = clients.indexOf(ws);
      if (idx !== -1) clients.splice(idx, 1);
    }
    
    ws.roomname = undefined;
  }
  
  _addToRoom(ws, room) {
    let clients = this.roomClients.get(room);
    if (!clients) { clients = []; this.roomClients.set(room, clients); }
    if (!clients.includes(ws)) clients.push(ws);
  }
  
  _addUserConnection(userId, ws) {
    let conns = this.userConnections.get(userId);
    if (!conns) { conns = new Set(); this.userConnections.set(userId, conns); }
    conns.add(ws);
    this.activeClients.add(ws);
  }
  
  async safeWebSocketCleanup(ws) { 
    if (ws) this._cleanupWebSocket(ws); 
  }
  
  async handleSetIdTarget2(ws, id, isNew) {
    if (!id || !ws) return;
    
    const oldConns = this.userConnections.get(id);
    if (oldConns) {
      for (const old of oldConns) {
        if (old !== ws && old.readyState === 1) {
          try { old.close(1000, "Replaced"); } catch(e) {}
          this._cleanupWebSocket(old);
        }
      }
    }
    
    ws.idtarget = id;
    ws._isClosing = false;
    ws._connectionTime = Date.now();
    this.activeClients.add(ws);
    this._addUserConnection(id, ws);
    
    const seatInfo = this.userToSeat.get(id);
    if (seatInfo && !isNew) {
      const { room, seat } = seatInfo;
      const rm = this.roomManagers.get(room);
      if (rm) {
        const seatData = rm.getSeat(seat);
        if (seatData && seatData.namauser === id) {
          ws.roomname = room;
          this._addToRoom(ws, room);
          await this._sendRoomState(ws, room);
          await this.safeSend(ws, ["numberKursiSaya", seat]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          return;
        }
      }
      this.userToSeat.delete(id);
      this.userCurrentRoom.delete(id);
    }
    
    await this.safeSend(ws, isNew ? ["joinroomawal"] : ["needJoinRoom"]);
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1) return;
    
    let str = raw;
    if (raw instanceof ArrayBuffer) {
      try { str = new TextDecoder().decode(raw); } catch(e) { return; }
    }
    
    if (str.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    
    const data = safeParseJSON(str);
    if (!data || !Array.isArray(data)) return;
    
    const evt = data[0];
    
    try {
      switch (evt) {
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
        case "leaveRoom":
          if (ws.roomname) {
            await this._leaveRoom(ws, ws.roomname);
            await this.safeSend(ws, ["roomLeft", ws.roomname]);
          }
          break;
        case "chat": {
          const [, room, noimg, user, msg, userColor, textColor] = data;
          if (ws.roomname !== room || ws.idtarget !== user) return;
          if (!roomList.includes(room)) return;
          const rm = this.roomManagers.get(room);
          if (rm && rm.getMute()) return;
          const sanitizedMsg = msg ? String(msg).substring(0, 500) : "";
          this.broadcastToRoom(room, ["chat", room, noimg, user, sanitizedMsg, userColor, textColor]);
          break;
        }
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room) return;
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat) return;
          const rm = this.roomManagers.get(room);
          if (rm && rm.replacePoint(seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 })) {
            this._sendToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimg, user, color, bawah, atas, vip, vipTanda] = data;
          if (ws.roomname !== room || ws.idtarget !== user) return;
          const rm = this.roomManagers.get(room);
          const existing = rm.getSeat(seat);
          const updated = {
            noimageUrl: noimg, namauser: user, color: color,
            itembawah: bawah, itematas: atas, vip: vip, viptanda: vipTanda,
            lastPoint: existing?.lastPoint || null
          };
          if (rm.replaceSeat(seat, updated)) {
            this.userToSeat.set(user, { room, seat });
            this.userCurrentRoom.set(user, room);
            this._sendToRoom(room, ["kursiBatchUpdate", room, [[seat, {
              noimageUrl: noimg, namauser: user, color: color,
              itembawah: bawah, itematas: atas, vip: vip, viptanda: vipTanda
            }]]]);
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat || ws.roomname !== room) return;
          const rm = this.roomManagers.get(room);
          if (rm && rm.removeSeat(seat)) {
            this.userToSeat.delete(ws.idtarget);
            this.userCurrentRoom.delete(ws.idtarget);
            this._sendToRoom(room, ["removeKursi", room, seat]);
          }
          break;
        }
        case "getRoomUserCount": {
          const room = data[1];
          if (roomList.includes(room)) {
            const rm = this.roomManagers.get(room);
            await this.safeSend(ws, ["roomUserCount", room, rm?.getOccupiedCount() || 0]);
          }
          break;
        }
        case "getAllRoomsUserCount": {
          const counts = roomList.map(r => [r, this.roomManagers.get(r)?.getOccupiedCount() || 0]);
          await this.safeSend(ws, ["allRoomsUserCount", counts]);
          break;
        }
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        case "setMuteType": {
          const muted = data[1], room = data[2];
          if (roomList.includes(room)) {
            const rm = this.roomManagers.get(room);
            const success = rm.setMute(muted);
            await this.safeSend(ws, ["muteTypeSet", !!muted, success, room]);
            this._sendToRoom(room, ["muteStatusChanged", rm.getMute(), room]);
          }
          break;
        }
        case "getMuteType": {
          const room = data[1];
          if (roomList.includes(room)) {
            const rm = this.roomManagers.get(room);
            await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), room]);
          }
          break;
        }
        case "sendnotif": {
          const [, target, img, name, desc] = data;
          const conns = this.userConnections.get(target);
          if (conns) {
            const notif = ["notif", img, name, desc, Date.now()];
            for (const c of conns) {
              if (c.readyState === 1) {
                await this.safeSend(c, notif);
                break;
              }
            }
          }
          break;
        }
        case "private": {
          const [, target, url, msg, sender] = data;
          const out = ["private", target, url, msg, Date.now(), sender];
          await this.safeSend(ws, out);
          const conns = this.userConnections.get(target);
          if (conns) {
            for (const c of conns) {
              if (c.readyState === 1) {
                await this.safeSend(c, out);
                break;
              }
            }
          }
          break;
        }
        case "isUserOnline": {
          const user = data[1];
          const conns = this.userConnections.get(user);
          const online = conns && conns.size > 0;
          await this.safeSend(ws, ["userOnlineStatus", user, online, data[2] || ""]);
          break;
        }
        case "getOnlineUsers": {
          const users = [];
          const seen = new Set();
          for (const c of this.activeClients) {
            if (c?.idtarget && !seen.has(c.idtarget)) {
              users.push(c.idtarget);
              seen.add(c.idtarget);
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        case "getRoomOnlineUsers": {
          const room = data[1];
          if (!roomList.includes(room)) return;
          const users = [];
          const seen = new Set();
          const clients = this.roomClients.get(room) || [];
          for (const c of clients) {
            if (c?.idtarget && !seen.has(c.idtarget)) {
              users.push(c.idtarget);
              seen.add(c.idtarget);
            }
          }
          await this.safeSend(ws, ["roomOnlineUsers", room, users]);
          break;
        }
        case "gift": {
          const [, room, sender, receiver, gift] = data;
          if (ws.roomname !== room || ws.idtarget !== sender) return;
          this.broadcastToRoom(room, ["gift", room, sender, receiver, gift, Date.now()]);
          break;
        }
        case "rollangak": {
          const room = data[1];
          if (room && roomList.includes(room)) {
            this.broadcastToRoom(room, ["rollangakBroadcast", room, data[2], data[3]]);
          }
          break;
        }
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            await this.lowcard.handleEvent(ws, data);
          }
          break;
        case "onDestroy":
          await this.safeWebSocketCleanup(ws);
          break;
      }
    } catch(e) {
      console.error(`handleMessage error (${evt}):`, e);
    }
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeReal = 0;
          for (const c of this.activeClients) if (c?.readyState === 1) activeReal++;
          return new Response(JSON.stringify({
            status: "healthy",
            connections: activeReal,
            users: this.userConnections.size,
            games: this.lowcard?.activeGames?.size || 0,
            uptime: Date.now() - this._startTime,
            buffer: this.chatBuffer.queue?.length || 0
          }), { headers: { "content-type": "application/json" } });
        }
        
        if (url.pathname === "/debug/memory") {
          let totalRoomClients = 0;
          for (const clients of this.roomClients.values()) totalRoomClients += clients.length;
          return new Response(JSON.stringify({
            userConnections: this.userConnections.size,
            userToSeat: this.userToSeat.size,
            userCurrentRoom: this.userCurrentRoom.size,
            roomClients: totalRoomClients,
            activeClients: this.activeClients.size,
            chatBuffer: this.chatBuffer.queue?.length || 0,
            activeGames: this.lowcard?.activeGames?.size || 0
          }), { headers: { "content-type": "application/json" } });
        }
        
        return new Response("ChatServer Running", { status: 200 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      await server.accept();
      
      server.idtarget = undefined;
      server.roomname = undefined;
      server._isClosing = false;
      server._connectionTime = Date.now();
      
      const messageHandler = (ev) => this.handleMessage(server, ev.data);
      const closeHandler = () => this.safeWebSocketCleanup(server);
      const errorHandler = () => this.safeWebSocketCleanup(server);
      
      server.addEventListener("message", messageHandler);
      server.addEventListener("close", closeHandler);
      server.addEventListener("error", errorHandler);
      
      this._activeListeners.set(server, [
        { event: "message", handler: messageHandler },
        { event: "close", handler: closeHandler },
        { event: "error", handler: errorHandler }
      ]);
      
      this.activeClients.add(server);
      
      return new Response(null, { status: 101, webSocket: client });
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response("Error", { status: 500 });
    }
  }
  
  async shutdown() {
    this._isClosing = true;
    if (this._masterTimer) clearInterval(this._masterTimer);
    if (this.lowcard) this.lowcard.destroy();
    if (this.chatBuffer) this.chatBuffer.destroy();
    
    for (const ws of this.activeClients) {
      try { ws.close(1000, "Shutdown"); } catch(e) {}
    }
    
    for (const rm of this.roomManagers.values()) rm.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this.activeClients.clear();
    this._activeListeners.clear();
  }
}

export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER.idFromName("default");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    } catch(e) {
      console.error("Worker error:", e);
      return new Response("Server Error", { status: 500 });
    }
  }
};
