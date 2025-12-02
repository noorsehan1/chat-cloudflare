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

    // FLUSH TIMER 100ms
    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) this.periodicFlush();
    }, 100);

    this.lowcard = new LowCardGameManager(this);

    this.messageCounts = new Map();
    this.MAX_MESSAGES_PER_SECOND = 20;

    // Map terpisah untuk menandai kursi yang sedang ditempati (HANYA untuk tracking)
    this.seatOccupancy = new Map();
    for (const room of roomList) {
      const occupancyMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        occupancyMap.set(i, null); // null berarti kursi kosong
      }
      this.seatOccupancy.set(room, occupancyMap);
    }
  }

  cleanupUserFromSeat(room, seatNumber, userId) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const seatInfo = seatMap.get(seatNumber);
    if (seatInfo && seatInfo.namauser === userId) {
      if (seatInfo.viptanda > 0) {
        this.vipManager.removeVipBadge(room, seatNumber);
      }
      
      Object.assign(seatInfo, createEmptySeat());
      this.clearSeatBuffer(room, seatNumber);
      this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      this.broadcastRoomUserCount(room);
    }

    // Juga bersihkan dari occupancy map
    const occupancyMap = this.seatOccupancy.get(room);
    if (occupancyMap && occupancyMap.get(seatNumber) === userId) {
      occupancyMap.set(seatNumber, null);
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
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;

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

    if (baru === true) {
      this.forceUserCleanup(id);
      ws.idtarget = id;
      ws.roomname = undefined;
      ws.numkursi = new Set();
      this.safeSend(ws, ["joinroomawal"]);
    } else {
      ws.idtarget = id;
      
      const seatInfo = this.userToSeat.get(id);
      
      if (seatInfo) {
        const { room, seat } = seatInfo;
        
        // Cek apakah user masih di seat yang lama
        const seatMap = this.roomSeats.get(room);
        const seatData = seatMap?.get(seat);
        const isStillInSeat = seatData?.namauser === id;
        
        if (isStillInSeat) {
          // User masih di seat yang sama - reconnect normal
          ws.roomname = room;
          ws.numkursi = new Set([seat]);
          
          // Update occupancy map juga
          const occupancyMap = this.seatOccupancy.get(room);
          if (occupancyMap) {
            occupancyMap.set(seat, id);
          }
          
          this.sendAllStateTo(ws, room);
          this.broadcastRoomUserCount(room);
          this.vipManager.getAllVipBadges(ws, room);
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          
          // Kirim event rooMasuk juga untuk konsistensi
          setTimeout(() => {
            if (ws.readyState === 1 && ws.roomname === room && ws.idtarget) {
              this.safeSend(ws, ["rooMasuk", seat, room]);
            }
          }, 100);
        } else {
          // User sudah tidak di seat lama
          this.forceUserCleanup(id);
          this.safeSend(ws, ["needJoinRoom"]);
        }
      } else {
        // Tidak ada seatInfo sama sekali - user baru atau sudah lama dibersihkan
        this.forceUserCleanup(id); // Untuk pastikan clean state
        this.safeSend(ws, ["needJoinRoom"]);
      }
    }
  }

  isUserStillInSeat(idtarget, room, seat) {
    // Cek di occupancy map terlebih dahulu untuk tracking
    const occupancyMap = this.seatOccupancy.get(room);
    if (occupancyMap && occupancyMap.get(seat) === idtarget) {
      return true;
    }
    
    // Juga cek di seat map biasa
    const seatMap = this.roomSeats.get(room);
    const seatData = seatMap?.get(seat);
    return seatData?.namauser === idtarget;
  }
  
  async handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom) || !ws.idtarget) {
      return false;
    }

    // Skip jika sudah di room yang sama
    const currentSeatInfo = this.userToSeat.get(ws.idtarget);
    if (currentSeatInfo && currentSeatInfo.room === newRoom) {
      // Sudah di room yang sama, cukup kirim state
      ws.roomname = newRoom;
      ws.numkursi = new Set([currentSeatInfo.seat]);
      this.sendAllStateTo(ws, newRoom);
      this.vipManager.getAllVipBadges(ws, newRoom);
      this.broadcastRoomUserCount(newRoom);
      this.safeSend(ws, ["numberKursiSaya", currentSeatInfo.seat]);
      this.safeSend(ws, ["rooMasuk", currentSeatInfo.seat, newRoom]);
      return true;
    }

    // Proses tanpa lock
    if (currentSeatInfo) {
      const { room: currentRoom, seat: currentSeat } = currentSeatInfo;
      
      // JANGAN kirim removeKursi jika pindah ke room yang sama
      if (currentRoom !== newRoom) {
        // Bersihkan kursi sebelumnya TANPA menghapus ID dari userToSeat
        const seatMap = this.roomSeats.get(currentRoom);
        if (seatMap) {
          const seatInfo = seatMap.get(currentSeat);
          if (seatInfo && seatInfo.namauser === ws.idtarget) {
            // Simpan data VIP dulu sebelum dibersihkan
            const hadVip = seatInfo.viptanda > 0;
            
            // Reset kursi
            Object.assign(seatInfo, createEmptySeat());
            this.clearSeatBuffer(currentRoom, currentSeat);
            
            // KIRIM removeKursi hanya jika pindah ke room BERBEDA
            this.broadcastToRoom(currentRoom, ["removeKursi", currentRoom, currentSeat]);
            
            // Update user count untuk room lama
            this.broadcastRoomUserCount(currentRoom);
            
            // Jika ada VIP, hapus juga
            if (hadVip) {
              this.vipManager.removeVipBadge(currentRoom, currentSeat);
            }
          }
        }
        
        // Bersihkan dari occupancy map hanya jika pindah room
        const occupancyMap = this.seatOccupancy.get(currentRoom);
        if (occupancyMap) {
          occupancyMap.set(currentSeat, null);
        }
      }
    }

    const foundSeat = this.findEmptySeat(newRoom, ws);
    if (!foundSeat) {
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    ws.roomname = newRoom;
    ws.numkursi = new Set([foundSeat]);
    
    // Update userToSeat dengan room dan seat baru
    this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

    // Update occupancy map
    const occupancyMap = this.seatOccupancy.get(newRoom);
    occupancyMap.set(foundSeat, ws.idtarget);

       this.sendAllStateTo(ws, newRoom);

    this.vipManager.getAllVipBadges(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);
    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
     this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
     
 
    
    return true;
  }

  findEmptySeat(room, ws) {
    if (!ws.idtarget) return null;
    
    // Gunakan occupancy map untuk mencari kursi kosong
    const occupancyMap = this.seatOccupancy.get(room);
    if (!occupancyMap) return null;

    // Cek apakah user sudah punya kursi di room ini (di occupancy map)
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancyMap.get(i) === ws.idtarget) {
        return i;
      }
    }

    // Cari kursi kosong di occupancy map
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (occupancyMap.get(i) === null) {
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

    // AMBIL DATA NORMAL DARI roomSeats MAP
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      
      // Jika kursi ada namauser di roomSeats, kirim data
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

    // Juga bersihkan dari occupancy map
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
      // Langsung cleanup tanpa grace period
      this.forceUserCleanup(idtarget);
    }
    
    this.clients.delete(ws);
    
    if (ws.readyState === 1) {
      try {
        ws.close(1000, "Manual destroy");
      } catch (error) {
        // Kosong
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

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;

    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
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

      case "onDestroy": {
        const idtarget = ws.idtarget;
        this.handleOnDestroy(ws, idtarget);
        break;
      }
        
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
        // Kosong
      }
    });

    ws.addEventListener("error", (event) => {
      // Kosong
    });

    ws.addEventListener("close", (event) => {
      if (ws.idtarget && !ws.isManualDestroy) {
        // Langsung cleanup tanpa grace period
        this.forceUserCleanup(ws.idtarget);
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


