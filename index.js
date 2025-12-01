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
    this.userProfiles = new Map();
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

    this.gracePeriod = 30 * 1000;
    this.graceTimers = new Map();
    this.joinLocks = new Map();

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
  }

  scheduleGraceCleanup(idtarget) {
    if (!idtarget) return;
    
    if (this.graceTimers.has(idtarget)) {
      clearTimeout(this.graceTimers.get(idtarget));
    }
    
    const timer = setTimeout(() => {
      this.forceUserCleanup(idtarget);
      this.graceTimers.delete(idtarget);
    }, this.gracePeriod);
    
    this.graceTimers.set(idtarget, timer);
  }

  cancelGraceCleanup(idtarget) {
    if (!idtarget) return;
    
    if (this.graceTimers.has(idtarget)) {
      clearTimeout(this.graceTimers.get(idtarget));
      this.graceTimers.delete(idtarget);
    }
  }

  shouldApplyGracePeriod(closeCode, reason) {
    const normalClosureCodes = [1000, 1001, 1005];
    
    if (reason && (
      reason.toLowerCase().includes('reconnect') ||
      reason.toLowerCase().includes('refresh') ||
      reason.toLowerCase().includes('reload')
    )) {
      return false;
    }
    
    if (normalClosureCodes.includes(closeCode)) {
      return false;
    }
    
    const gracePeriodCodes = [
      1006, 1011, 1012, 1013, 1014, 4000, 4001
    ];
    
    return gracePeriodCodes.includes(closeCode) || closeCode >= 4000;
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
    this.graceTimers.delete(idtarget);
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
    this.graceTimers.delete(idtarget);

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
      // LOGIN BARU / FIRST TIME
      this.forceUserCleanup(id);
      ws.idtarget = id;
      ws.roomname = undefined;
      ws.numkursi = new Set();
      
      // Reset profile
      ws.noimageUrl = "";
      ws.color = "";
      ws.itembawah = 0;
      ws.itematas = 0;
      ws.vip = 0;
      ws.viptanda = 0;
      
      // Hapus profile cache untuk fresh start
      this.userProfiles.delete(id);
      
      this.safeSend(ws, ["joinroomawal"]);
      
    } else {
      // RECONNECT
      ws.idtarget = id;
      
      this.cancelGraceCleanup(id);
      
      const seatInfo = this.userToSeat.get(id);
      
      if (seatInfo && this.isUserStillInSeat(id, seatInfo.room, seatInfo.seat)) {
        const { room, seat } = seatInfo;
        ws.roomname = room;
        ws.numkursi = new Set([seat]);
        
        // Load profile dari cache
        const userProfile = this.userProfiles.get(id);
        if (userProfile) {
          ws.noimageUrl = userProfile.noimageUrl;
          ws.color = userProfile.color;
          ws.itembawah = userProfile.itembawah;
          ws.itematas = userProfile.itematas;
          ws.vip = userProfile.vip;
          ws.viptanda = userProfile.viptanda;
        }
        
        this.sendAllStateTo(ws, room);
        this.broadcastRoomUserCount(room);
        this.vipManager.getAllVipBadges(ws, room);
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        
        // Kirim rooMasuk untuk trigger updateKursi di client
        this.safeSend(ws, ["rooMasuk", seat, room]);
        
      } else {
        this.safeSend(ws, ["needJoinRoom"]);
      }
    }
  }

  handleUpdateProfile(ws, data) {
    if (!ws.idtarget) return;
    
    const [, noimageUrl, color, itembawah, itematas, vip, viptanda] = data;
    
    ws.noimageUrl = noimageUrl || "";
    ws.color = color || "";
    ws.itembawah = itembawah || 0;
    ws.itematas = itematas || 0;
    ws.vip = vip || 0;
    ws.viptanda = viptanda || 0;
    
    this.userProfiles.set(ws.idtarget, {
      noimageUrl: ws.noimageUrl,
      color: ws.color,
      itembawah: ws.itembawah,
      itematas: ws.itematas,
      vip: ws.vip,
      viptanda: ws.viptanda
    });
    
    if (ws.roomname) {
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        this.updateKursiWithProfile(room, seat, ws.idtarget, {
          noimageUrl: ws.noimageUrl,
          color: ws.color,
          itembawah: ws.itembawah,
          itematas: ws.itematas,
          vip: ws.vip,
          viptanda: ws.viptanda
        });
      }
    }
  }

  updateKursiWithProfile(room, seat, userId, profile) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;
    
    const seatInfo = seatMap.get(seat);
    if (seatInfo && seatInfo.namauser === userId) {
      Object.assign(seatInfo, {
        noimageUrl: profile.noimageUrl,
        color: profile.color,
        itembawah: profile.itembawah,
        itematas: profile.itematas,
        vip: profile.vip,
        viptanda: profile.viptanda
      });
      
      if (!this.updateKursiBuffer.has(room))
        this.updateKursiBuffer.set(room, new Map());
      this.updateKursiBuffer.get(room).set(seat, { ...seatInfo });
    }
  }

  isUserStillInSeat(idtarget, room, seat) {
    const seatMap = this.roomSeats.get(room);
    const seatData = seatMap?.get(seat);
    return seatData?.namauser === idtarget;
  }
  
  async handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom) || !ws.idtarget) {
      return false;
    }

    const lockKey = `join-${ws.idtarget}`;
    
    if (this.joinLocks.has(lockKey)) {
      this.safeSend(ws, ["error", "Already processing join request"]);
      return false;
    }

    this.joinLocks.set(lockKey, true);
    
    try {
      // Cek apakah user sudah punya kursi
      const currentSeatInfo = this.userToSeat.get(ws.idtarget);
      
      if (currentSeatInfo) {
        // User sudah punya kursi (bisa di room lain atau sama)
        const { room: currentRoom, seat: currentSeat } = currentSeatInfo;
        
        // Jika sudah di room yang sama, tidak perlu join lagi
        if (currentRoom === newRoom) {
          this.safeSend(ws, ["alreadyInRoom", newRoom]);
          return false;
        }
        
        // Jika pindah room, cleanup dulu dari room lama
        this.cleanupUserFromSeat(currentRoom, currentSeat, ws.idtarget);
      }
      // Jika currentSeatInfo undefined, berarti FIRST TIME JOIN
      // Tidak perlu cleanup apa-apa

      // Cari kursi available
      let foundSeat = await this.findAvailableSeat(newRoom, ws.idtarget);
      if (!foundSeat) {
        this.safeSend(ws, ["roomFull", newRoom]);
        return false;
      }

      // Update state
      ws.roomname = newRoom;
      ws.numkursi = new Set([foundSeat]);
      
      this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

      this.cancelGraceCleanup(ws.idtarget);

      // Ambil profile user
      let userProfile = this.userProfiles.get(ws.idtarget);
      
      if (!userProfile) {
        // FIRST TIME: profile belum ada, gunakan dari ws object
        userProfile = {
          noimageUrl: ws.noimageUrl || "",
          color: ws.color || "",
          itembawah: ws.itembawah || 0,
          itematas: ws.itematas || 0,
          vip: ws.vip || 0,
          viptanda: ws.viptanda || 0
        };
        
        // Simpan untuk next time
        this.userProfiles.set(ws.idtarget, userProfile);
      }

      // Update kursi di server
      const seatMap = this.roomSeats.get(newRoom);
      const currentSeat = seatMap.get(foundSeat);
      
      // Safety check: kursi harus kosong untuk first time join
      if (currentSeat.namauser && currentSeat.namauser !== ws.idtarget) {
        // Kursi sudah ditempati orang lain - cari kursi lain
        const alternativeSeat = await this.findAvailableSeat(newRoom, ws.idtarget, foundSeat + 1);
        if (!alternativeSeat) {
          this.safeSend(ws, ["roomFull", newRoom]);
          return false;
        }
        foundSeat = alternativeSeat;
      }

      // Set data kursi
      Object.assign(currentSeat, {
        noimageUrl: userProfile.noimageUrl,
        namauser: ws.idtarget,
        color: userProfile.color,
        itembawah: userProfile.itembawah,
        itematas: userProfile.itematas,
        vip: userProfile.vip,
        viptanda: userProfile.viptanda,
        lastPoint: null
      });

      // Buffer update
      if (!this.updateKursiBuffer.has(newRoom))
        this.updateKursiBuffer.set(newRoom, new Map());
      this.updateKursiBuffer.get(newRoom).set(foundSeat, { ...currentSeat });

      // Kirim response
      this.sendAllStateTo(ws, newRoom);
      this.vipManager.getAllVipBadges(ws, newRoom);
      this.broadcastRoomUserCount(newRoom);
      
      // PENTING: Kirim rooMasuk untuk trigger updateKursi di client
      this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
      this.safeSend(ws, ["numberKursiSaya", foundSeat]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);

      return true;
    } finally {
      this.joinLocks.delete(lockKey);
    }
  }

  async findAvailableSeat(room, userId, startFrom = 1) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    // Priority 1: Cek apakah user sudah punya kursi di room ini
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (seatInfo && seatInfo.namauser === userId) {
        return i;
      }
    }

    // Priority 2: Cari kursi kosong
    for (let i = startFrom; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (seatInfo && !seatInfo.namauser) {
        // Double check untuk race condition
        if (!seatInfo.namauser) {
          return i;
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
    
    if (ws.isManualDestroy) {
      this.fullRemoveById(idtarget);
    } else {
      this.scheduleGraceCleanup(idtarget);
    }
    
    this.clients.delete(ws);
    
    if (ws.readyState === 1) {
      try {
        ws.close(1000, "Manual destroy");
      } catch (error) {
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

      case "updateProfile": {
        this.handleUpdateProfile(ws, data);
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

        this.cancelGraceCleanup(newId);

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
        this.handleJoinRoom(ws, data[1]).catch(() => {});
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
        
        if (currentInfo.namauser && currentInfo.namauser !== namauser) {
          this.safeSend(ws, ["error", "Seat already occupied"]);
          return;
        }
        
        Object.assign(currentInfo, {
          noimageUrl, namauser, color, itembawah, itematas,
          vip: vip || 0,
          viptanda: viptanda || 0
        });

        seatMap.set(seat, currentInfo);
        
        // Simpan profile
        this.userProfiles.set(namauser, {
          noimageUrl: noimageUrl,
          color: color,
          itembawah: itembawah,
          itematas: itematas,
          vip: vip,
          viptanda: viptanda
        });
        
        // Update ws object jika ini user yang sedang aktif
        if (ws.idtarget === namauser) {
          ws.noimageUrl = noimageUrl;
          ws.color = color;
          ws.itembawah = itembawah;
          ws.itematas = itematas;
          ws.vip = vip;
          ws.viptanda = viptanda;
        }
        
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

    // Initialize profile fields
    ws.noimageUrl = "";
    ws.color = "";
    ws.itembawah = 0;
    ws.itematas = 0;
    ws.vip = 0;
    ws.viptanda = 0;

    this.clients.add(ws);

    ws.addEventListener("message", (ev) => {
      try {
        this.handleMessage(ws, ev.data);
      } catch (error) {
      }
    });

    ws.addEventListener("error", (event) => {
    });

    ws.addEventListener("close", (event) => {
      if (ws.idtarget && !ws.isManualDestroy) {
        const shouldGracePeriod = this.shouldApplyGracePeriod(event.code, event.reason);
        
        if (shouldGracePeriod) {
          this.scheduleGraceCleanup(ws.idtarget);
        } else {
          this.forceUserCleanup(ws.idtarget);
        }
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
