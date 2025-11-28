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
    lastPoint: null,
    lastActivity: Date.now(),
    isUpdating: false // Flag untuk mencegah race condition
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
    this.chatMessageBuffer = new Map();

    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }

    this._nextConnId = 1;

    this.intervalMillis = 15 * 60 * 1000;
    this.currentNumber = 1;
    this.maxNumber = 6;

    this._tickTimer = setInterval(() => {
      this.tick();
    }, this.intervalMillis);

    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) this.periodicFlush();
    }, 16);

    this.lowcard = new LowCardGameManager(this);

    this.messageCounts = new Map();
    this.MAX_MESSAGES_PER_SECOND = 20;

    this._cleanupTimer = setInterval(() => {
      this.aggressiveCleanup();
    }, 10 * 1000);

    // Cache untuk user data yang konsisten
    this.userDataCache = new Map(); // Map<idtarget, {noimageUrl, color, etc}>
  }

  aggressiveCleanup() {
    const now = Date.now();
    
    for (const [key, stats] of this.messageCounts.entries()) {
      const currentWindow = Math.floor(now / 1000);
      if (stats.window < currentWindow - 10) {
        this.messageCounts.delete(key);
      }
    }

    for (const [room, buffer] of this.chatMessageBuffer.entries()) {
      if (buffer.length === 0) {
        this.chatMessageBuffer.delete(room);
      }
    }

    this.cleanupInactiveUsers();
  }

  cleanupInactiveUsers() {
    const now = Date.now();
    const MAX_INACTIVE_TIME = 30 * 1000;
    
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser && (now - seatInfo.lastActivity > MAX_INACTIVE_TIME)) {
          let isUserConnected = false;
          for (const client of this.clients) {
            if (client.idtarget === seatInfo.namauser && client.readyState === 1) {
              isUserConnected = true;
              break;
            }
          }
          
          if (!isUserConnected) {
            console.log(`Cleaning up inactive user: ${seatInfo.namauser} from ${room} seat ${seatNumber}`);
            this.cleanupUserFromSeat(room, seatNumber, seatInfo.namauser);
          }
        }
      }
    }
  }

  cleanupUserFromSeat(room, seatNumber, userId) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const seatInfo = seatMap.get(seatNumber);
    if (seatInfo && seatInfo.namauser === userId) {
      console.log(`Cleaning up user ${userId} from ${room} seat ${seatNumber}`);
      
      if (seatInfo.viptanda > 0) {
        this.vipManager.removeVipBadge(room, seatNumber);
      }
      
      Object.assign(seatInfo, createEmptySeat());
      this.clearSeatBuffer(room, seatNumber);
      this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(userId);
  }

  clearSeatBuffer(room, seatNumber) {
    if (!room || typeof seatNumber !== "number") return;
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) roomMap.delete(seatNumber);
  }

  forceUserCleanup(idtarget) {
    if (!idtarget) return;

    console.log(`Force cleaning up user: ${idtarget}`);
    
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      
      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget) {
          this.cleanupUserFromSeat(room, seatNumber, idtarget);
        }
      }
    }

    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.userDataCache.delete(idtarget);
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;

    console.log(`Full removal of user: ${idtarget}`);
    
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

    for (const [room, messages] of this.chatMessageBuffer.entries()) {
      this.chatMessageBuffer.set(room, messages.filter(msg => {
        if (msg[0] === "chat") return msg[3] !== idtarget;
        if (msg[0] === "gift") return msg[2] !== idtarget && msg[3] !== idtarget;
        return true;
      }));
    }

    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.userDataCache.delete(idtarget);

    for (const c of Array.from(this.clients)) {
      if (c && c.idtarget === idtarget) {
        if (c.readyState === 1) {
          c.close(1000, "Session removed");
        }
        this.clients.delete(c);
      }
    }
  }

  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const key = ws.idtarget || ws._connId || 'anonymous';
    const windowStart = Math.floor(now / 1000);

    if (!this.messageCounts.has(key)) {
      this.messageCounts.set(key, { count: 0, window: windowStart });
    }

    const stats = this.messageCounts.get(key);
    if (stats.window !== windowStart) {
      stats.count = 0;
      stats.window = windowStart;
    }

    let limit = this.MAX_MESSAGES_PER_SECOND;
    if (messageType === "chat") limit = 50;
    if (messageType === "updatePoint") limit = 100;

    stats.count += 1;
    if (stats.count > limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }
    return true;
  }

  safeSend(ws, arr) {
    if (ws && ws.readyState === 1) {
      if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 1000000) return false;
      try {
        ws.send(JSON.stringify(arr));
        return true;
      } catch (e) {
        return false;
      }
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) {
      return 0;
    }
    
    let sentCount = 0;
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        if (this.safeSend(c, msg)) sentCount++;
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
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0 && roomList.includes(room)) {
        for (let i = 0; i < messages.length; i++) {
          this.broadcastToRoom(room, messages[i]);
        }
        this.chatMessageBuffer.set(room, []);
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      if (!roomList.includes(room)) continue;
      
      const updates = [];
      for (const [seat, info] of seatMapUpdates.entries()) {
        const { lastPoint, lastActivity, isUpdating, ...rest } = info;
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
    this.flushChatBuffer();
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

    console.log(`handleSetIdTarget2: ${id}, baru: ${baru}, current clients: ${this.clients.size}`);

    if (baru === true) {
        this.forceUserCleanup(id);
        ws.idtarget = id;
        ws.roomname = undefined;
        ws.numkursi = new Set();
        this.safeSend(ws, ["joinroomawal"]);
    } else {
        const existingSeatInfo = this.userToSeat.get(id);
        
        if (existingSeatInfo && this.isUserStillInSeat(id, existingSeatInfo.room, existingSeatInfo.seat)) {
            const { room, seat } = existingSeatInfo;
            ws.idtarget = id;
            ws.roomname = room;
            ws.numkursi = new Set([seat]);
            
            const seatMap = this.roomSeats.get(room);
            if (seatMap && seatMap.get(seat)) {
                const seatInfo = seatMap.get(seat);
                seatInfo.lastActivity = Date.now();
                
                // Restore dari cache jika ada data yang hilang
                const cachedData = this.userDataCache.get(id);
                if (cachedData && (!seatInfo.noimageUrl || seatInfo.noimageUrl === "0" || !seatInfo.noimageUrl)) {
                  console.log(`Restoring cached data for ${id}`);
                  Object.assign(seatInfo, {
                    noimageUrl: cachedData.noimageUrl || seatInfo.noimageUrl,
                    color: cachedData.color || seatInfo.color,
                    itembawah: cachedData.itembawah || seatInfo.itembawah,
                    itematas: cachedData.itematas || seatInfo.itematas
                  });
                }
            }
            
            this.safeSend(ws, ["currentNumber", this.currentNumber]);
            this.sendAllStateTo(ws, room);
            this.broadcastRoomUserCount(room);
            
            console.log(`User ${id} reconnected to ${room} seat ${seat}`);
        } else {
            if (existingSeatInfo) {
                this.userToSeat.delete(id);
            }
            ws.idtarget = id;
            this.safeSend(ws, ["needJoinRoom"]);
        }
    }
  }

  isUserStillInSeat(idtarget, room, seat) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return false;
    
    const seatData = seatMap.get(seat);
    return seatData?.namauser === idtarget;
  }
  
  handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom) || !ws.idtarget) {
      console.log(`Invalid join room request: room=${newRoom}, idtarget=${ws.idtarget}`);
      return false;
    }

    console.log(`User ${ws.idtarget} attempting to join ${newRoom}`);

    const currentSeatInfo = this.userToSeat.get(ws.idtarget);
    if (currentSeatInfo && currentSeatInfo.room === newRoom) {
      console.log(`User ${ws.idtarget} is already in ${newRoom}`);
      
      if (this.isUserStillInSeat(ws.idtarget, newRoom, currentSeatInfo.seat)) {
        ws.roomname = newRoom;
        ws.numkursi = new Set([currentSeatInfo.seat]);
        
        const seatMap = this.roomSeats.get(newRoom);
        if (seatMap && seatMap.get(currentSeatInfo.seat)) {
            const seatInfo = seatMap.get(currentSeatInfo.seat);
            seatInfo.lastActivity = Date.now();
        }
        
        this.safeSend(ws, ["numberKursiSaya", currentSeatInfo.seat]);
        this.safeSend(ws, ["rooMasuk", currentSeatInfo.seat, newRoom]);
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        this.sendAllStateTo(ws, newRoom);
        this.vipManager.getAllVipBadges(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);
        return true;
      }
    }

    if (currentSeatInfo && currentSeatInfo.room !== newRoom) {
      console.log(`Cleaning up user ${ws.idtarget} from previous room ${currentSeatInfo.room}`);
      this.cleanupUserFromSeat(currentSeatInfo.room, currentSeatInfo.seat, ws.idtarget);
    }

    const foundSeat = this.findAndReserveSeat(newRoom, ws.idtarget);
    if (!foundSeat) {
      console.log(`No available seat found for ${ws.idtarget} in ${newRoom}`);
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    ws.roomname = newRoom;
    ws.numkursi = new Set([foundSeat]);
    this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

    const seatMap = this.roomSeats.get(newRoom);
    if (seatMap && seatMap.get(foundSeat)) {
      const seatInfo = seatMap.get(foundSeat);
      
      // Preserve existing user data jika ada di cache
      const cachedData = this.userDataCache.get(ws.idtarget);
      if (cachedData) {
        console.log(`Using cached data for ${ws.idtarget} when joining room`);
        Object.assign(seatInfo, {
          namauser: ws.idtarget,
          noimageUrl: cachedData.noimageUrl || seatInfo.noimageUrl,
          color: cachedData.color || seatInfo.color,
          itembawah: cachedData.itembawah || seatInfo.itembawah,
          itematas: cachedData.itematas || seatInfo.itematas,
          lastActivity: Date.now()
        });
      } else {
        seatInfo.namauser = ws.idtarget;
        seatInfo.lastActivity = Date.now();
      }
    }

    console.log(`User ${ws.idtarget} joined ${newRoom} seat ${foundSeat}`);

    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
    this.safeSend(ws, ["currentNumber", this.currentNumber]);

    this.sendAllStateTo(ws, newRoom);
    this.vipManager.getAllVipBadges(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);

    return true;
  }

  findAndReserveSeat(room, idtarget) {
    if (!idtarget) return null;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (seatInfo && seatInfo.namauser === idtarget) {
        return i;
      }
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (seatInfo && !seatInfo.namauser && !seatInfo.isUpdating) {
        seatInfo.isUpdating = true; // Lock seat untuk mencegah race condition
        seatInfo.namauser = idtarget;
        seatInfo.lastActivity = Date.now();
        seatInfo.isUpdating = false;
        return i;
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
          noimageUrl: info.noimageUrl || "", // Pastikan tidak undefined
          namauser: info.namauser,
          color: info.color || "",
          itembawah: info.itembawah || 0,
          itematas: info.itematas || 0,
          vip: info.vip || 0,
          viptanda: info.viptanda || 0
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

    const count = this.getJumlahRoom()[room] || 0;
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

    this.userToSeat.delete(idtarget);
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;
    
    this.fullRemoveById(idtarget);
    this.forceUserCleanup(idtarget);
    this.clients.delete(ws);
    
    if (ws.readyState === 1) {
        ws.close(1000, "Manual destroy");
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
    for (const c of this.clients) {
      if (c.roomname === roomName && c.idtarget && c.readyState === 1) {
        users.push(c.idtarget);
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

  handleUpdateKursi(ws, data) {
    const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
    
    if (ws.roomname !== room) {
      return;
    }
    
    if (!roomList.includes(room)) return;

    const seatMap = this.roomSeats.get(room);
    const currentInfo = seatMap.get(seat) || createEmptySeat();
    
    if (currentInfo.namauser && currentInfo.namauser !== ws.idtarget) {
      console.log(`Security violation: User ${ws.idtarget} tried to update seat ${seat} owned by ${currentInfo.namauser}`);
      return;
    }

    // Cache user data untuk konsistensi
    if (ws.idtarget && noimageUrl && noimageUrl !== "0" && noimageUrl !== "") {
      this.userDataCache.set(ws.idtarget, {
        noimageUrl: noimageUrl,
        color: color || currentInfo.color,
        itembawah: itembawah || currentInfo.itembawah,
        itematas: itematas || currentInfo.itematas
      });
    }
    
    Object.assign(currentInfo, {
      noimageUrl: noimageUrl || currentInfo.noimageUrl, // Jangan overwrite dengan empty value
      namauser: namauser || currentInfo.namauser,
      color: color || currentInfo.color,
      itembawah: itembawah || currentInfo.itembawah,
      itematas: itematas || currentInfo.itematas,
      vip: vip || currentInfo.vip || 0,
      viptanda: viptanda || currentInfo.viptanda || 0,
      lastActivity: Date.now()
    });

    console.log(`Updated seat ${seat} in ${room} for user ${ws.idtarget}:`, {
      noimageUrl: currentInfo.noimageUrl,
      color: currentInfo.color
    });

    seatMap.set(seat, currentInfo);
    if (!this.updateKursiBuffer.has(room))
      this.updateKursiBuffer.set(room, new Map());
    this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
    this.broadcastRoomUserCount(room);
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;

    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      console.error("Error parsing message:", e);
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

      case "onDestroy": 
        break;
        
      case "setIdTarget2": 
        this.handleSetIdTarget2(ws, data[1], data[2]); 
        break;

      case "setIdTarget": {
        const newId = data[1];
        if (ws.idtarget && ws.idtarget !== newId) {
          this.forceUserCleanup(ws.idtarget);
        }
        ws.idtarget = newId;

        const prevSeat = this.userToSeat.get(newId);
        if (prevSeat) {
          ws.roomname = prevSeat.room;
          ws.numkursi = new Set([prevSeat.seat]);
          this.sendAllStateTo(ws, prevSeat.room);
        } else {
          if (this.hasEverSetId) {
            this.safeSend(ws, ["needJoinRoom"]);
          }
        }

        this.hasEverSetId = true;
        if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
        break;
      }

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
        
        if (!roomList.includes(roomname)) return;

        if (!this.chatMessageBuffer.has(roomname))
          this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname)
          .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
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
        si.lastActivity = Date.now();
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

      case "updateKursi": 
        this.handleUpdateKursi(ws, data);
        break;

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        
        if (ws.roomname !== roomname) {
          return;
        }
        
        if (!roomList.includes(roomname)) return;
        if (!this.chatMessageBuffer.has(roomname))
          this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname)
          .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (ws.roomname === "LowCard") {
          setTimeout(() => this.lowcard.handleEvent(ws, data), 0);
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

    this.clients.add(ws);

    ws.addEventListener("message", (ev) => {
        try {
            this.handleMessage(ws, ev.data);
        } catch (error) {
            console.error("Error handling message:", error);
        }
    });

    ws.addEventListener("error", (event) => {
        console.log(`WebSocket error for ${ws.idtarget}:`, event);
        if (ws.idtarget) {
            ws.isManualDestroy = true;
            this.handleOnDestroy(ws, ws.idtarget);
        }
    });

    ws.addEventListener("close", (event) => {
        console.log(`WebSocket closed for ${ws.idtarget}:`, event.code, event.reason);
        if (ws.idtarget && !ws.isManualDestroy) {
            this.handleOnDestroy(ws, ws.idtarget);
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
