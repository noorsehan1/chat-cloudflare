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
    this.hasEverSetId = false;

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();

    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    this.vipManager = new VipBadgeManager(this);

    this.updateKursiBuffer = new Map();
    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
    }

    this._nextConnId = 1;

    this._timers = [];

    this.intervalMillis = 15 * 60 * 1000;
    this.currentNumber = 1;
    this.maxNumber = 6;
    this._tickTimer = setInterval(() => {
      this.tick();
    }, this.intervalMillis);
    this._timers.push(this._tickTimer);

    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) this.periodicFlush();
    }, 50);
    this._timers.push(this._flushTimer);

    this.lowcard = new LowCardGameManager(this);

    this.seatOccupancy = new Map();
    for (const room of roomList) {
      const occupancyMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        occupancyMap.set(i, null);
      }
      this.seatOccupancy.set(room, occupancyMap);
    }

    this.gracePeriod = 5000;
    this.disconnectedTimers = new Map();

    this.roomClients = new Map();
    for (const room of roomList) {
      this.roomClients.set(room, new Set());
    }
  }

  _cleanupTimers() {
    for (const timer of this._timers) {
      clearInterval(timer);
      clearTimeout(timer);
    }
    this._timers = [];

    for (const timer of this.disconnectedTimers.values()) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
  }

  scheduleCleanup(userId) {
    const oldTimer = this.disconnectedTimers.get(userId);
    if (oldTimer) {
      clearTimeout(oldTimer);
    }

    const timer = setTimeout(() => {
      this.executeGracePeriodCleanup(userId);
    }, this.gracePeriod);

    this.disconnectedTimers.set(userId, timer);
  }

  executeGracePeriodCleanup(userId) {
    this.disconnectedTimers.delete(userId);

    let isStillConnected = false;
    for (const c of this.clients) {
      if (c.idtarget === userId && c.readyState === 1) {
        isStillConnected = true;
        break;
      }
    }

    if (!isStillConnected) {
      this.forceUserCleanup(userId);
    }
  }

  cancelCleanup(userId) {
    const timer = this.disconnectedTimers.get(userId);
    if (timer) {
      clearTimeout(timer);
      this.disconnectedTimers.delete(userId);
    }
  }

  cleanupUserFromSeat(room, seatNumber, userId, immediate = true) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const seatInfo = seatMap.get(seatNumber);
    if (seatInfo && seatInfo.namauser === userId) {
      if (seatInfo.viptanda > 0) {
        this.vipManager.removeVipBadge(room, seatNumber);
      }
      
      if (immediate) {
        Object.assign(seatInfo, createEmptySeat());
        this.clearSeatBuffer(room, seatNumber);
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.broadcastRoomUserCount(room);
        
        const clientSet = this.roomClients.get(room);
        if (clientSet) {
          for (const client of clientSet) {
            if (client.idtarget === userId) {
              clientSet.delete(client);
              break;
            }
          }
        }
      }
    }

    const occupancyMap = this.seatOccupancy.get(room);
    if (occupancyMap) {
      occupancyMap.set(seatNumber, immediate ? null : userId);
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
    
    const seatMap = this.roomSeats.get(room);
    if (seatMap) {
      Object.assign(seatMap.get(seat), createEmptySeat());
      this.clearSeatBuffer(room, seat);
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
    }
    
    const occupancyMap = this.seatOccupancy.get(room);
    if (occupancyMap) {
      occupancyMap.set(seat, null);
    }
    
    const clientSet = this.roomClients.get(room);
    if (clientSet) {
      clientSet.delete(ws);
    }
    
    this.vipManager.cleanupUserVipBadges(ws.idtarget);
    
    ws.roomname = undefined;
    ws.numkursi = new Set();
    this.userToSeat.delete(ws.idtarget);
    
    // ✅ BROADCAST USER COUNT SETELAH CLEANUP
    this.broadcastRoomUserCount(room);
  }

  clearSeatBuffer(room, seatNumber) {
    if (!room || typeof seatNumber !== "number") return;
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) roomMap.delete(seatNumber);
  }

  forceUserCleanup(idtarget) {
    if (!idtarget) return;

    this.cancelCleanup(idtarget);

    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      
      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget) {
          this.cleanupUserFromSeat(room, seatNumber, idtarget, true);
        }
      }
    }

    this.userToSeat.delete(idtarget);
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;

    this.cancelCleanup(idtarget);

    this.vipManager.cleanupUserVipBadges(idtarget);

    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, info] of seatMap) {
        if (info.namauser === idtarget) {
          Object.assign(info, createEmptySeat());
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);

    for (const c of Array.from(this.clients)) {
      if (c && c.idtarget === idtarget) {
        if (c.readyState === 1) {
          c.close(1000, "Session removed");
        }
        this.clients.delete(c);
        
        for (const clientSet of this.roomClients.values()) {
          clientSet.delete(c);
        }
      }
    }
  }

  safeSend(ws, arr) {
    if (ws && ws.readyState === 1) {
      if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 5000000) return false;
      try {
        ws.send(JSON.stringify(arr));
        return true;
      } catch (error) {
        return false;
      }
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) {
      return 0;
    }
    
    const clientSet = this.roomClients.get(room);
    if (!clientSet) return 0;
    
    let sentCount = 0;
    for (const c of clientSet) {
      if (c.readyState === 1 && c.roomname === room) {
        if (this.safeSend(c, msg)) {
          sentCount++;
        }
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const info of seatMap.values()) {
        if (info.namauser) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    if (!room || !roomList.includes(room)) return;
    const counts = this.getJumlahRoom();
    const count = counts[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      if (!roomList.includes(room)) continue;
      
      const updates = [];
      for (const [seat, info] of seatMapUpdates.entries()) {
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of this.clients) {
      if (c.readyState === 1 && c.roomname) {
        this.safeSend(c, ["currentNumber", this.currentNumber]);
      }
    }
  }

  isUserInAnyRoom(idtarget) {
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      
      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget) {
          return true;
        }
      }
    }
    return false;
  }

  handleSetIdTarget2(ws, id, baru) {
    if (!id) return;

    if (baru === true) {
      this.forceUserCleanup(id);
      ws.idtarget = id;
      ws.roomname = undefined;
      ws.numkursi = new Set();
      this.safeSend(ws, ["joinroomawal"]);
      return;
    }
    
    ws.idtarget = id;
    
    const seatInfo = this.userToSeat.get(id);
    const canReconnect = seatInfo && 
                        this.roomSeats.get(seatInfo.room)?.get(seatInfo.seat)?.namauser === id;
    
    if (canReconnect) {
      const { room, seat } = seatInfo;
      ws.roomname = room;
      ws.numkursi = new Set([seat]);
      
      const clientSet = this.roomClients.get(room);
      if (clientSet) {
        clientSet.add(ws);
      }
      
      this.sendAllStateTo(ws, room);
      this.broadcastRoomUserCount(room);
      this.vipManager.getAllVipBadges(ws, room);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    } else {
      this.forceUserCleanup(id);
      
      setTimeout(() => {
        if (ws.readyState === 1) {
          this.safeSend(ws, ["needJoinRoom"]);
        }
      }, 2000);
    }
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
    
    this.cancelCleanup(ws.idtarget);
    
    // ✅ CLEANUP DARI ROOM LAMA SEBELUM JOIN ROOM BARU
    if (ws.roomname && ws.roomname !== room) {
      this.cleanupFromRoom(ws, ws.roomname);
    }
    
    const seat = this.findEmptySeat(room, ws);
    if (!seat) {
      this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    const seatMap = this.roomSeats.get(room);
    const seatInfo = seatMap.get(seat);
    
    const occupancyMap = this.seatOccupancy.get(room);
    occupancyMap.set(seat, ws.idtarget);
    
    this.userToSeat.set(ws.idtarget, { room, seat });
    ws.roomname = room;
    ws.numkursi = new Set([seat]);
    
    const clientSet = this.roomClients.get(room);
    if (clientSet) {
      clientSet.add(ws);
    }
    
    this.sendAllStateTo(ws, room);
    this.broadcastRoomUserCount(room);
    this.safeSend(ws, ["rooMasuk", seat, room]);
    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    return true;
  }

  findEmptySeat(room, ws) {
    const occupancyMap = this.seatOccupancy.get(room);
    if (!occupancyMap) return null;
    
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancyMap.get(i) === ws.idtarget) {
        return i;
      }
    }
    
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancyMap.get(i) === null) {
        const seatMap = this.roomSeats.get(room);
        const seatData = seatMap?.get(i);
        
        if (!seatData || seatData.namauser === "") {
          return i;
        } else {
          occupancyMap.set(i, seatData.namauser);
        }
      }
    }
    
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const occupiedBy = occupancyMap.get(i);
      if (occupiedBy && occupiedBy !== ws.idtarget) {
        let isOccupantOnline = false;
        for (const c of this.clients) {
          if (c.idtarget === occupiedBy && c.readyState === 1) {
            isOccupantOnline = true;
            break;
          }
        }
        
        if (!isOccupantOnline) {
          const seatMap = this.roomSeats.get(room);
          const seatData = seatMap?.get(i);
          if (seatData && seatData.namauser === occupiedBy) {
            this.cleanupUserFromSeat(room, i, occupiedBy, true);
            return i;
          }
        }
      }
    }
    
    return null;
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1 || !room) return;
    
    if (ws.roomname !== room) {
      return;
    }
    
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const allKursiMeta = {};
    const lastPointsData = [];

    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      
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

    const counts = this.getJumlahRoom();
    const count = counts[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
  }

  removeAllSeatsById(idtarget) {
    if (!idtarget) return;

    const seatInfo = this.userToSeat.get(idtarget);
    if (!seatInfo) return;

    const { room, seat } = seatInfo;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap || !seatMap.has(seat)) {
      this.userToSeat.delete(idtarget);
      return;
    }

    const currentSeat = seatMap.get(seat);
    if (currentSeat.namauser === idtarget) {
      if (currentSeat.viptanda > 0) {
        this.vipManager.removeVipBadge(room, seat);
      }

      Object.assign(currentSeat, createEmptySeat());
      this.clearSeatBuffer(room, seat);
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
      this.broadcastRoomUserCount(room);
    }

    const occupancyMap = this.seatOccupancy.get(room);
    if (occupancyMap && occupancyMap.get(seat) === idtarget) {
      occupancyMap.set(seat, null);
    }

    this.userToSeat.delete(idtarget);
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;
    
    if (ws.isManualDestroy) {
      this.fullRemoveById(idtarget);
    } else {
      const seatInfo = this.userToSeat.get(idtarget);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        this.cleanupUserFromSeat(room, seat, idtarget, true);
      }
      this.userToSeat.delete(idtarget);
    }
    
    this.cancelCleanup(idtarget);
    
    for (const clientSet of this.roomClients.values()) {
      clientSet.delete(ws);
    }
    
    this.clients.delete(ws);
    
    if (ws.readyState === 1) {
      try {
        ws.close(1000, "Manual destroy");
      } catch (error) {
        // Ignore
      }
    }
  }

  getAllOnlineUsers() {
    const users = [];
    for (const c of this.clients) {
      if (c.idtarget && c.readyState === 1) {
        users.push(c.idtarget);
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    const clientSet = this.roomClients.get(roomName);
    if (clientSet) {
      for (const c of clientSet) {
        if (c.idtarget && c.readyState === 1) {
          users.push(c.idtarget);
        }
      }
    }
    return users;
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
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

    switch (evt) {
      case "vipbadge":
      case "removeVipBadge":
      case "getAllVipBadges":
        this.vipManager.handleEvent(ws, data);
        break;

      case "isInRoom": {
        const idtarget = ws.idtarget;
        if (!idtarget) {
          this.safeSend(ws, ["inRoomStatus", false]);
          return;
        }
        const seatInfo = this.userToSeat.get(idtarget);
        if (!seatInfo) {
          this.safeSend(ws, ["inRoomStatus", false]);
          return;
        }
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        const seatData = seatMap?.get(seat);
        const isInRoom = seatData?.namauser === idtarget;
        this.safeSend(ws, ["inRoomStatus", isInRoom]);
        break;
      }

      case "onDestroy": {
        const idtarget = ws.idtarget;
        this.handleOnDestroy(ws, idtarget);
        break;
      }
        
      case "setIdTarget2": 
        this.handleSetIdTarget2(ws, data[1], data[2]); 
        break;

      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        for (const c of this.clients) {
          if (c.idtarget === idtarget && c.readyState === 1) {
            this.safeSend(c, notif);
            break;
          }
        }
        break;
      }

      case "private": {
        const [, idt, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", idt, url, msg, ts, sender];
        this.safeSend(ws, out);
        for (const c of this.clients) {
          if (c.idtarget === idt && c.readyState === 1) {
            this.safeSend(c, out);
            break;
          }
        }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";
        let online = false;
        for (const c of this.clients) {
          if (c.idtarget === username && c.readyState === 1) {
            online = true;
            break;
          }
        }
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
        
        if (ws.roomname !== roomname) {
          return;
        }
        
        if (ws.idtarget !== username) {
          return;
        }
        
        if (!roomList.includes(roomname)) return;

        const clientSet = this.roomClients.get(roomname);
        if (!clientSet) return;
        
        for (const c of clientSet) {
          if (c.readyState === 1 && c.roomname === roomname) {
            this.safeSend(c, [
              "chat", 
              roomname, 
              noImageURL, 
              username, 
              message, 
              usernameColor, 
              chatTextColor
            ]);
          }
        }
        
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        
        if (ws.roomname !== room) {
          return;
        }
        
        if (!roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        si.lastPoint = { x, y, fast, timestamp: Date.now() };
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        
        if (ws.roomname !== room) {
          return;
        }
        
        if (!roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        Object.assign(seatMap.get(seat), createEmptySeat());
        this.clearSeatBuffer(room, seat);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        
        if (ws.roomname !== room) {
          return;
        }
        
        if (!roomList.includes(room)) return;

        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        
        Object.assign(currentInfo, {
          noimageUrl, namauser, color, itembawah, itematas,
          vip: vip || 0,
          viptanda: viptanda || 0
        });

        seatMap.set(seat, currentInfo);
        if (!this.updateKursiBuffer.has(room))
          this.updateKursiBuffer.set(room, new Map());
        this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        
        if (ws.roomname !== roomname) {
          return;
        }
        
        if (ws.idtarget !== sender) {
          return;
        }
        
        if (!roomList.includes(roomname)) return;

        const giftData = ["gift", roomname, sender, receiver, giftName];
        
        const clientSet = this.roomClients.get(roomname);
        if (clientSet) {
          for (const c of clientSet) {
            if (c.readyState === 1 && c.roomname === roomname) {
              this.safeSend(c, giftData);
            }
          }
        }
        
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
    ws.errorCount = 0;

    this.clients.add(ws);

    ws.addEventListener("message", (ev) => {
      try {
        this.handleMessage(ws, ev.data);
      } catch (error) {
        // Ignore
      }
    });

    ws.addEventListener("error", (event) => {
      // Ignore
    });

    ws.addEventListener("close", (event) => {
      if (ws.idtarget && !ws.isManualDestroy) {
        this.scheduleCleanup(ws.idtarget);
      }
      
      for (const clientSet of this.roomClients.values()) {
        clientSet.delete(ws);
      }
      
      this.clients.delete(ws);
    });

    return new Response(null, { status: 101, webSocket: client });
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
