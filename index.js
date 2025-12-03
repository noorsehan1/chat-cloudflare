import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

function createEmptySeat() {
  return {
    noimageUrl: "",
    namauser: "",
    color: "",
    itembawah: 0,
    itematas: 0,
    vip: 0,
    viptanda: 0,
    lastPoint: null
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();
    this.userToSeat = new Map();
    this.userConnections = new Map();
    
    this.MAX_SEATS = 35;
    
    this.roomSeats = new Map();
    this.roomOccupancy = new Map();
    this.roomUserCount = new Map();
    
    for (const room of roomList) {
      const seats = new Array(this.MAX_SEATS + 1);
      const occupancy = new Array(this.MAX_SEATS + 1);
      
      for (let i = 0; i <= this.MAX_SEATS; i++) {
        seats[i] = i === 0 ? null : createEmptySeat();
        occupancy[i] = null;
      }
      
      this.roomSeats.set(room, seats);
      this.roomOccupancy.set(room, occupancy);
      this.roomUserCount.set(room, 0);
    }

    this.vipManager = new VipBadgeManager(this);

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.MAX_BUFFER_SIZE = 300;
    
    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }

    this._nextConnId = 1;
    this._timers = [];
    
    this.intervalMillis = 15 * 60 * 1000;
    this.currentNumber = 1;
    this.maxNumber = 6;
    
    this._tickTimer = setInterval(() => {
      if (this.clients.size > 0) this.tick();
    }, this.intervalMillis);
    this._timers.push(this._tickTimer);

    this._flushInterval = 100;
    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) this.periodicFlush();
    }, this._flushInterval);
    this._timers.push(this._flushTimer);

    this.lowcard = new LowCardGameManager(this);

    this.messageCounts = new Map();
    this.TOKEN_BUCKET = {
      capacity: 100,
      refillRate: 50,
      minInterval: 10,
      maxConnections: 3
    };

    this.gracePeriod = 5000;
    this.disconnectedTimers = new Map();

    this.roomClients = new Map();
    for (const room of roomList) {
      this.roomClients.set(room, new Set());
    }

    this._metrics = {
      totalMessages: 0,
      connections: 0,
      lastCleanup: Date.now()
    };

    this._cleanupTimer = setInterval(() => {
      this._performCleanup();
    }, 30000);
    this._timers.push(this._cleanupTimer);
  }

  _performCleanup() {
    const now = Date.now();
    
    for (const [room, clientSet] of this.roomClients) {
      const deadClients = [];
      for (const client of clientSet) {
        if (client.readyState !== 1) {
          deadClients.push(client);
        }
      }
      for (const client of deadClients) {
        clientSet.delete(client);
      }
    }
    
    if (now - this._metrics.lastCleanup > 60000) {
      for (const [key, bucket] of this.messageCounts.entries()) {
        if (now - bucket.lastRefill > 120000) {
          this.messageCounts.delete(key);
        }
      }
      this._metrics.lastCleanup = now;
    }
    
    for (const [userId, timer] of this.disconnectedTimers.entries()) {
      const connections = this.userConnections.get(userId);
      if (connections && connections.size > 0) {
        clearTimeout(timer);
        this.disconnectedTimers.delete(userId);
      }
    }
    
    for (const [room, messages] of this.chatMessageBuffer.entries()) {
      if (messages.length > this.MAX_BUFFER_SIZE) {
        this.chatMessageBuffer.set(room, messages.slice(-150));
      }
    }
    
    this._metrics.connections = this.clients.size;
  }

  scheduleCleanup(userId) {
    if (!userId) return;
    
    const oldTimer = this.disconnectedTimers.get(userId);
    if (oldTimer) {
      clearTimeout(oldTimer);
    }

    const connections = this.userConnections.get(userId);
    if (connections && connections.size > 0) {
      return;
    }

    const timer = setTimeout(() => {
      this.executeGracePeriodCleanup(userId);
    }, this.gracePeriod);

    this.disconnectedTimers.set(userId, timer);
  }

  executeGracePeriodCleanup(userId) {
    this.disconnectedTimers.delete(userId);
    
    const connections = this.userConnections.get(userId);
    if (connections && connections.size > 0) {
      return;
    }
    
    this.forceUserCleanup(userId);
  }

  cancelCleanup(userId) {
    const timer = this.disconnectedTimers.get(userId);
    if (timer) {
      clearTimeout(timer);
      this.disconnectedTimers.delete(userId);
    }
  }

  cleanupUserFromSeat(room, seatNumber, userId, immediate = true) {
    const seats = this.roomSeats.get(room);
    if (!seats) return;

    const seatInfo = seats[seatNumber];
    if (seatInfo && seatInfo.namauser === userId) {
      if (seatInfo.viptanda > 0) {
        this.vipManager.removeVipBadge(room, seatNumber);
      }
      
      if (immediate) {
        Object.assign(seatInfo, createEmptySeat());
        this.clearSeatBuffer(room, seatNumber);
        
        const currentCount = this.roomUserCount.get(room) || 0;
        if (currentCount > 0) {
          this.roomUserCount.set(room, currentCount - 1);
        }
        
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.broadcastRoomUserCount(room);
        
        // Hapus user connections dari roomClients
        const connections = this.userConnections.get(userId);
        if (connections) {
          const clientSet = this.roomClients.get(room);
          if (clientSet) {
            for (const conn of connections) {
              clientSet.delete(conn);
            }
          }
        }
      }
    }

    const occupancy = this.roomOccupancy.get(room);
    if (occupancy) {
      occupancy[seatNumber] = immediate ? null : userId;
    }

    if (immediate) {
      this.userToSeat.delete(userId);
    }
  }

  cleanupFromRoom(ws, room) {
    if (!ws.idtarget || !ws.roomname) return;
    
    const seatInfo = this.userToSeat.get(ws.idtarget);
    if (!seatInfo || seatInfo.room !== room) return;
    
    const { seat } = seatInfo;
    
    const seats = this.roomSeats.get(room);
    if (seats) {
      Object.assign(seats[seat], createEmptySeat());
      this.clearSeatBuffer(room, seat);
      
      const currentCount = this.roomUserCount.get(room) || 0;
      if (currentCount > 0) {
        this.roomUserCount.set(room, currentCount - 1);
      }
      
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
    }
    
    const occupancy = this.roomOccupancy.get(room);
    if (occupancy) {
      occupancy[seat] = null;
    }
    
    const clientSet = this.roomClients.get(room);
    if (clientSet) {
      clientSet.delete(ws);
    }
    
    this.vipManager.cleanupUserVipBadges(ws.idtarget);
    
    ws.roomname = undefined;
    ws.numkursi = new Set();
    this.userToSeat.delete(ws.idtarget);
  }

  clearSeatBuffer(room, seatNumber) {
    if (!room || typeof seatNumber !== "number") return;
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) roomMap.delete(seatNumber);
  }

  forceUserCleanup(userId) {
    if (!userId) return;

    this.cancelCleanup(userId);
    
    const seatInfo = this.userToSeat.get(userId);
    if (seatInfo) {
      const { room, seat } = seatInfo;
      this.cleanupUserFromSeat(room, seat, userId, true);
    }

    this.userToSeat.delete(userId);
    this.messageCounts.delete(userId);
    
    const connections = this.userConnections.get(userId);
    if (connections) {
      const connArray = Array.from(connections);
      for (const conn of connArray) {
        if (conn.readyState === 1) {
          try {
            conn.close(1000, "Force cleanup");
          } catch (e) {}
        }
        this.clients.delete(conn);
        
        for (const clientSet of this.roomClients.values()) {
          clientSet.delete(conn);
        }
      }
      this.userConnections.delete(userId);
    }
  }

  fullRemoveById(userId) {
    if (!userId) return;

    this.cancelCleanup(userId);
    
    for (const room of roomList) {
      const seats = this.roomSeats.get(room);
      if (!seats) continue;
      
      let removedCount = 0;
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        if (seats[i].namauser === userId) {
          Object.assign(seats[i], createEmptySeat());
          this.clearSeatBuffer(room, i);
          this.broadcastToRoom(room, ["removeKursi", room, i]);
          removedCount++;
        }
      }
      
      if (removedCount > 0) {
        const currentCount = this.roomUserCount.get(room) || 0;
        this.roomUserCount.set(room, Math.max(0, currentCount - removedCount));
        this.broadcastRoomUserCount(room);
      }
    }

    for (const [room, messages] of this.chatMessageBuffer.entries()) {
      const newMessages = [];
      for (const msg of messages) {
        if (msg[0] === "chat" && msg[3] !== userId) {
          newMessages.push(msg);
        } else if (msg[0] === "gift" && msg[2] !== userId && msg[3] !== userId) {
          newMessages.push(msg);
        }
      }
      if (newMessages.length !== messages.length) {
        this.chatMessageBuffer.set(room, newMessages);
      }
    }

    this.userToSeat.delete(userId);
    this.messageCounts.delete(userId);
    
    const connections = this.userConnections.get(userId);
    if (connections) {
      const connArray = Array.from(connections);
      for (const ws of connArray) {
        if (ws.readyState === 1) {
          try {
            ws.close(1000, "Session removed");
          } catch (e) {}
        }
        this.clients.delete(ws);
        
        for (const clientSet of this.roomClients.values()) {
          clientSet.delete(ws);
        }
      }
      this.userConnections.delete(userId);
    }
  }

  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const userId = ws.idtarget;
    const key = userId || ws._connId || 'anonymous';
    
    if (!this.messageCounts.has(key)) {
      this.messageCounts.set(key, {
        tokens: this.TOKEN_BUCKET.capacity,
        lastRefill: now,
        lastRequest: 0,
        burstCount: 0,
        violations: 0
      });
    }

    const bucket = this.messageCounts.get(key);
    
    const timePassed = now - bucket.lastRefill;
    const tokensToAdd = Math.floor(timePassed * this.TOKEN_BUCKET.refillRate / 1000);
    if (tokensToAdd > 0) {
      bucket.tokens = Math.min(this.TOKEN_BUCKET.capacity, bucket.tokens + tokensToAdd);
      bucket.lastRefill = now;
    }
    
    if (now - bucket.lastRequest < this.TOKEN_BUCKET.minInterval) {
      bucket.burstCount++;
      if (bucket.burstCount > 20) {
        this.safeSend(ws, ['error', 'Request too fast']);
        bucket.violations++;
        
        if (bucket.violations > 10 && userId) {
          setTimeout(() => this.fullRemoveById(userId), 1000);
        }
        return false;
      }
    } else {
      bucket.burstCount = Math.max(0, bucket.burstCount - 2);
    }
    
    bucket.lastRequest = now;
    
    let cost = {
      chat: 2,
      updatePoint: 1,
      updateKursi: 3,
      gift: 2,
      default: 1
    }[messageType] || 1;
    
    if (userId) {
      const connections = this.userConnections.get(userId);
      if (connections && connections.size > 1) {
        cost *= Math.min(connections.size, this.TOKEN_BUCKET.maxConnections);
      }
    }
    
    if (bucket.tokens < cost) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      bucket.violations++;
      
      if (bucket.violations > 5 && userId) {
        setTimeout(() => {
          if (this.messageCounts.get(key)?.violations > 5) {
            this.fullRemoveById(userId);
          }
        }, 2000);
      }
      return false;
    }
    
    bucket.tokens -= cost;
    bucket.violations = Math.max(0, bucket.violations - 0.5);
    
    return true;
  }

  safeSend(ws, arr) {
    if (!ws || ws.readyState !== 1) return false;
    
    const isHighPriority = [
      'currentNumber', 'rooMasuk', 'removeKursi', 
      'pointUpdated', 'kursiBatchUpdate', 'roomUserCount'
    ].includes(arr[0]);
    
    const bufferLimit = isHighPriority ? 1000000 : 5000000;
    if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > bufferLimit) {
      return false;
    }
    
    try {
      ws.send(JSON.stringify(arr));
      return true;
    } catch (error) {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    if (!roomList.includes(room)) return 0;
    
    let sentCount = 0;
    const clientSet = this.roomClients.get(room);
    if (!clientSet || clientSet.size === 0) return 0;
    
    const msgStr = JSON.stringify(msg);
    const deadClients = [];
    
    for (const client of clientSet) {
      if (client.readyState === 1) {
        try {
          client.send(msgStr);
          sentCount++;
        } catch (e) {
          deadClients.push(client);
        }
      } else {
        deadClients.push(client);
      }
    }
    
    for (const client of deadClients) {
      clientSet.delete(client);
    }
    
    this._metrics.totalMessages += sentCount;
    return sentCount;
  }

  getJumlahRoom() {
    const result = {};
    for (const room of roomList) {
      result[room] = this.roomUserCount.get(room) || 0;
    }
    return result;
  }

  broadcastRoomUserCount(room) {
    if (!roomList.includes(room)) return;
    const count = this.roomUserCount.get(room) || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length === 0 || !roomList.includes(room)) continue;
      
      const batchSize = Math.min(100, messages.length);
      const toSend = messages.splice(0, batchSize);
      
      for (const msg of toSend) {
        this.broadcastToRoom(room, msg);
      }
      
      if (messages.length > this.MAX_BUFFER_SIZE) {
        this.chatMessageBuffer.set(room, messages.slice(-150));
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      if (!roomList.includes(room) || seatMapUpdates.size === 0) continue;
      
      const updates = [];
      let count = 0;
      for (const [seat, info] of seatMapUpdates.entries()) {
        if (count >= 100) break;
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
        count++;
      }
      
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
    this.flushChatBuffer();
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    
    const msg = ["currentNumber", this.currentNumber];
    const msgStr = JSON.stringify(msg);
    
    for (const c of this.clients) {
      if (c.readyState === 1 && c.roomname) {
        try {
          c.send(msgStr);
        } catch (e) {}
      }
    }
  }

  isUserInAnyRoom(userId) {
    const seatInfo = this.userToSeat.get(userId);
    if (!seatInfo) return false;
    
    const { room, seat } = seatInfo;
    const seats = this.roomSeats.get(room);
    return seats && seats[seat] && seats[seat].namauser === userId;
  }

  handleSetIdTarget2(ws, id, baru) {
    if (!id) return;
    
    ws.idtarget = id;
    
    if (!this.userConnections.has(id)) {
      this.userConnections.set(id, new Set());
    }
    this.userConnections.get(id).add(ws);
    
    if (baru === true) {
      this.forceUserCleanup(id);
      ws.roomname = undefined;
      ws.numkursi = new Set();
      this.safeSend(ws, ["joinroomawal"]);
      return;
    }
    
    const seatInfo = this.userToSeat.get(id);
    
    if (!seatInfo) {
      this.forceUserCleanup(id);
      setTimeout(() => {
        if (ws.readyState === 1) this.safeSend(ws, ["needJoinRoom"]);
      }, 500);
      return;
    }
    
    const { room, seat } = seatInfo;
    const seats = this.roomSeats.get(room);
    
    if (!seats || !seats[seat] || seats[seat].namauser !== id) {
      this.forceUserCleanup(id);
      setTimeout(() => {
        if (ws.readyState === 1) this.safeSend(ws, ["needJoinRoom"]);
      }, 500);
      return;
    }
    
    ws.roomname = room;
    ws.numkursi = new Set([seat]);
    
    // ✅ PERBAIKAN: TAMBAH KE roomClients
    const clientSet = this.roomClients.get(room);
    if (clientSet) {
      clientSet.add(ws);
    }
    
    this.safeSend(ws, ["rooMasuk", seat, room]);
    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    const allKursiMeta = {};
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatData = seats[i];
      if (seatData.namauser) {
        allKursiMeta[i] = {
          noimageUrl: seatData.noimageUrl,
          namauser: seatData.namauser,
          color: seatData.color,
          itembawah: seatData.itembawah,
          itematas: seatData.itematas,
          vip: seatData.vip,
          viptanda: seatData.viptanda
        };
      }
    }
    
    if (Object.keys(allKursiMeta).length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    }
    
    const count = this.roomUserCount.get(room) || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
    
    this.vipManager.getAllVipBadges(ws, room);
  }

  handleJoinRoom(ws, room) {
    if (!ws.idtarget) {
      this.safeSend(ws, ["error", "User ID not set"]);
      return false;
    }
    
    if (!roomList.includes(room)) {
      this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    const userId = ws.idtarget;
    this.cancelCleanup(userId);
    
    if (ws.roomname && ws.roomname !== room) {
      this.cleanupFromRoom(ws, ws.roomname);
    }
    
    const seat = this.findEmptySeat(room, ws);
    if (!seat) {
      this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    const seats = this.roomSeats.get(room);
    
    Object.assign(seats[seat], {
      noimageUrl: "",
      namauser: userId,
      color: "#000000",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0
    });
    
    const occupancy = this.roomOccupancy.get(room);
    occupancy[seat] = userId;
    
    const currentCount = this.roomUserCount.get(room) || 0;
    this.roomUserCount.set(room, currentCount + 1);
    
    this.userToSeat.set(userId, { room, seat });
    ws.roomname = room;
    ws.numkursi = new Set([seat]);
    
    // ✅ PERBAIKAN: TAMBAH KE roomClients
    const clientSet = this.roomClients.get(room);
    if (clientSet) {
      clientSet.add(ws);
    }
    
    this.safeSend(ws, ["rooMasuk", seat, room]);
    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    const bufferMap = this.updateKursiBuffer.get(room) || new Map();
    bufferMap.set(seat, { ...seats[seat] });
    this.updateKursiBuffer.set(room, bufferMap);
    
    this.broadcastRoomUserCount(room);
    
    return true;
  }

  findEmptySeat(room, ws) {
    const occupancy = this.roomOccupancy.get(room);
    if (!occupancy) return null;
    
    const userId = ws.idtarget;
    
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancy[i] === userId) {
        return i;
      }
    }
    
    const seats = this.roomSeats.get(room);
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancy[i] === null) {
        if (!seats[i].namauser) {
          occupancy[i] = userId;
          return i;
        }
      }
    }
    
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const occupiedBy = occupancy[i];
      if (occupiedBy && occupiedBy !== userId) {
        const connections = this.userConnections.get(occupiedBy);
        if (!connections || connections.size === 0) {
          if (seats[i].namauser === occupiedBy) {
            this.cleanupUserFromSeat(room, i, occupiedBy, true);
            occupancy[i] = userId;
            return i;
          }
        }
      }
    }
    
    return null;
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1 || !room) return;
    if (ws.roomname !== room) return;
    
    const seats = this.roomSeats.get(room);
    if (!seats) return;

    const allKursiMeta = {};
    const lastPointsData = [];
    
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seats[seat];
      if (info.namauser) {
        allKursiMeta[seat] = {
          noimageUrl: info.noimageUrl,
          namauser: info.namauser,
          color: info.color,
          itembawah: info.itembawah,
          itematas: info.itematas,
          vip: info.vip,
          viptanda: info.viptanda
        };

        if (info.lastPoint) {
          lastPointsData.push({
            seat: seat,
            x: info.lastPoint.x,
            y: info.lastPoint.y,
            fast: info.lastPoint.fast
          });
        }
      }
    }

    if (Object.keys(allKursiMeta).length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    }

    if (lastPointsData.length > 0) {
      this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    }

    const count = this.roomUserCount.get(room) || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
  }

  removeAllSeatsById(userId) {
    if (!userId) return;

    const seatInfo = this.userToSeat.get(userId);
    if (!seatInfo) return;

    const { room, seat } = seatInfo;
    const seats = this.roomSeats.get(room);
    if (!seats) {
      this.userToSeat.delete(userId);
      return;
    }

    const currentSeat = seats[seat];
    if (currentSeat.namauser === userId) {
      if (currentSeat.viptanda > 0) {
        this.vipManager.removeVipBadge(room, seat);
      }

      Object.assign(currentSeat, createEmptySeat());
      this.clearSeatBuffer(room, seat);
      
      const currentCount = this.roomUserCount.get(room) || 0;
      if (currentCount > 0) {
        this.roomUserCount.set(room, currentCount - 1);
      }
      
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
      this.broadcastRoomUserCount(room);
    }

    const occupancy = this.roomOccupancy.get(room);
    if (occupancy && occupancy[seat] === userId) {
      occupancy[seat] = null;
    }

    this.userToSeat.delete(userId);
  }

  handleOnDestroy(ws, userId) {
    if (!userId) return;
    
    if (ws._destroyed) return;
    ws._destroyed = true;
    
    if (ws.isManualDestroy) {
      this.fullRemoveById(userId);
    } else {
      const seatInfo = this.userToSeat.get(userId);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        this.cleanupUserFromSeat(room, seat, userId, true);
      }
      this.userToSeat.delete(userId);
      this.messageCounts.delete(userId);
    }
    
    this.cancelCleanup(userId);
    
    const connections = this.userConnections.get(userId);
    if (connections) {
      connections.delete(ws);
      if (connections.size === 0) {
        this.userConnections.delete(userId);
      }
    }
    
    for (const clientSet of this.roomClients.values()) {
      clientSet.delete(ws);
    }
    
    this.clients.delete(ws);
    
    if (ws.readyState === 1) {
      try {
        ws.close(1000, "Manual destroy");
      } catch (error) {}
    }
  }

  getAllOnlineUsers() {
    const users = [];
    for (const [userId, connections] of this.userConnections) {
      if (connections.size > 0) {
        users.push(userId);
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    const clientSet = this.roomClients.get(roomName);
    
    if (clientSet) {
      for (const client of clientSet) {
        if (client.idtarget && client.readyState === 1) {
          users.push(client.idtarget);
        }
      }
    }
    return users;
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    
    const result = [];
    for (const room of roomList) {
      result.push([room, this.roomUserCount.get(room) || 0]);
    }
    
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;

    if (raw.length > 100000) {
      ws.close(1009, "Message too large");
      return;
    }

    let data;
    try { 
      data = JSON.parse(raw); 
      ws.errorCount = 0;
    } catch (e) { 
      ws.errorCount = (ws.errorCount || 0) + 1;
      if (ws.errorCount > 5) {
        try {
          ws.close(1008, "Protocol error");
        } catch (e2) {}
      }
      return; 
    }
    
    if (!Array.isArray(data) || data.length === 0) return;

    const evt = data[0];
    if (!this.checkRateLimit(ws, evt)) return;

    switch (evt) {
      case "vipbadge":
      case "removeVipBadge":
      case "getAllVipBadges":
        this.vipManager.handleEvent(ws, data);
        break;

      case "isInRoom": {
        const userId = ws.idtarget;
        if (!userId) {
          this.safeSend(ws, ["inRoomStatus", false]);
          return;
        }
        const seatInfo = this.userToSeat.get(userId);
        const isInRoom = seatInfo && 
                       this.roomSeats.get(seatInfo.room)?.[seatInfo.seat]?.namauser === userId;
        this.safeSend(ws, ["inRoomStatus", !!isInRoom]);
        break;
      }

      case "onDestroy": {
        const userId = ws.idtarget;
        this.handleOnDestroy(ws, userId);
        break;
      }
        
      case "setIdTarget2": 
        this.handleSetIdTarget2(ws, data[1], data[2]); 
        break;

      case "sendnotif": {
        const [, userId, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        
        const connections = this.userConnections.get(userId);
        if (connections) {
          for (const conn of connections) {
            if (conn.readyState === 1) {
              this.safeSend(conn, notif);
              break;
            }
          }
        }
        break;
      }

      case "private": {
        const [, targetId, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", targetId, url, msg, ts, sender];
        
        this.safeSend(ws, out);
        
        const connections = this.userConnections.get(targetId);
        if (connections) {
          for (const conn of connections) {
            if (conn.readyState === 1 && conn !== ws) {
              this.safeSend(conn, out);
              break;
            }
          }
        }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";
        
        const connections = this.userConnections.get(username);
        const online = !!(connections && connections.size > 0);
        
        this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);
        break;
      }

      case "getAllRoomsUserCount": 
        this.handleGetAllRoomsUserCount(ws); 
        break;

      case "getCurrentNumber": 
        this.safeSend(ws, ["currentNumber", this.currentNumber]); 
        break;

      case "getOnlineUsers": 
        this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]); 
        break;

      case "getRoomOnlineUsers": {
        const roomName = data[1];
        if (!roomList.includes(roomName)) return;
        this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
        break;
      }

      case "joinRoom": 
        this.handleJoinRoom(ws, data[1]); 
        break;

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        
        if (ws.roomname !== roomname) return;
        if (!roomList.includes(roomname)) return;

        const buffer = this.chatMessageBuffer.get(roomname) || [];
        if (buffer.length > this.MAX_BUFFER_SIZE) {
          buffer.shift();
        }
        buffer.push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
        this.chatMessageBuffer.set(roomname, buffer);
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        
        if (ws.roomname !== room) return;
        if (!roomList.includes(room)) return;
        
        const seats = this.roomSeats.get(room);
        if (!seats || !seats[seat]) return;
        
        seats[seat].lastPoint = { x, y, fast, timestamp: Date.now() };
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        
        if (ws.roomname !== room) return;
        if (!roomList.includes(room)) return;
        
        const seats = this.roomSeats.get(room);
        if (seats && seats[seat]) {
          Object.assign(seats[seat], createEmptySeat());
          
          const currentCount = this.roomUserCount.get(room) || 0;
          if (currentCount > 0) {
            this.roomUserCount.set(room, currentCount - 1);
          }
          
          this.clearSeatBuffer(room, seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        
        if (ws.roomname !== room) return;
        if (!roomList.includes(room)) return;

        const seats = this.roomSeats.get(room);
        const currentInfo = seats[seat] ? { ...seats[seat] } : createEmptySeat();
        
        const wasEmpty = !currentInfo.namauser;
        const nowHasUser = !!namauser;
        
        Object.assign(currentInfo, {
          noimageUrl: noimageUrl || "",
          namauser: namauser || "",
          color: color || "",
          itembawah: itembawah || 0,
          itematas: itematas || 0,
          vip: vip || 0,
          viptanda: viptanda || 0
        });

        seats[seat] = currentInfo;
        
        if (wasEmpty && nowHasUser) {
          const currentCount = this.roomUserCount.get(room) || 0;
          this.roomUserCount.set(room, currentCount + 1);
        } else if (!wasEmpty && !nowHasUser) {
          const currentCount = this.roomUserCount.get(room) || 0;
          if (currentCount > 0) {
            this.roomUserCount.set(room, currentCount - 1);
          }
        }
        
        const bufferMap = this.updateKursiBuffer.get(room) || new Map();
        bufferMap.set(seat, { ...currentInfo });
        this.updateKursiBuffer.set(room, bufferMap);
        
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        
        if (ws.roomname !== roomname) return;
        if (!roomList.includes(roomname)) return;
        
        const buffer = this.chatMessageBuffer.get(roomname) || [];
        if (buffer.length > this.MAX_BUFFER_SIZE) {
          buffer.shift();
        }
        buffer.push(["gift", roomname, sender, receiver, giftName, Date.now()]);
        this.chatMessageBuffer.set(roomname, buffer);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (ws.roomname === "LowCard") {
          this.lowcard.handleEvent(ws, data);
        }
        break;
        
      default: 
        break;
    }
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    await server.accept();

    const ws = server;
    ws._connId = `conn#${this._nextConnId++}`;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    ws.isManualDestroy = false;
    ws._destroyed = false;
    ws.errorCount = 0;

    this.clients.add(ws);

    ws.addEventListener("message", (ev) => {
      try {
        this.handleMessage(ws, ev.data);
      } catch (error) {
        ws.errorCount = (ws.errorCount || 0) + 1;
        if (ws.errorCount > 10) {
          try {
            ws.close(1008, "Too many errors");
          } catch (e) {}
        }
      }
    });

    ws.addEventListener("error", (event) => {
      // Ignore
    });

    ws.addEventListener("close", (event) => {
      const userId = ws.idtarget;
      if (userId && !ws.isManualDestroy) {
        this.scheduleCleanup(userId);
      }
      
      if (userId) {
        const connections = this.userConnections.get(userId);
        if (connections) {
          connections.delete(ws);
          if (connections.size === 0) {
            this.userConnections.delete(userId);
          }
        }
      }
      
      for (const clientSet of this.roomClients.values()) {
        clientSet.delete(ws);
      }
      
      this.clients.delete(ws);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  async alarm() {
    this._performCleanup();
    
    if (this.clients.size === 0) {
      for (const timer of this._timers) {
        clearInterval(timer);
      }
      this._timers = [];
    }
  }
}

export default {
  async fetch(req, env) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    }
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
