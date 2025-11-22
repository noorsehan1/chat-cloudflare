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
    lockTime: undefined
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
    this.seatLocks = new Map();

    this.roomChatHistory = new Map();
    this.userDisconnectTime = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }

    this._tickTimer = setInterval(() => {
      this.tick().catch(() => {});
    }, this.intervalMillis);

    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) {
        this.periodicFlush().catch(() => {});
      }
    }, 100); // ✅ DARI 500ms JADI 100ms

    this.lowcard = new LowCardGameManager(this);

    this.pingTimeouts = new Map();
    this.RECONNECT_TIMEOUT = 20000; // 20 detik
    this.cleanupInProgress = new Set();
    this.usersToRemove = new Map();

    this.messageCounts = new Map();
    this.MAX_MESSAGES_PER_SECOND = 20;
  }

  clearSeatBuffer(room, seatNumber) {
    try {
      if (!room || typeof seatNumber !== "number") return;
      if (this.updateKursiBuffer.has(room)) {
        this.updateKursiBuffer.get(room).delete(seatNumber);
      }
    } catch (e) {
      // Ignore error
    }
  }

  scheduleCleanupTimeout(idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    
    const timeout = setTimeout(() => {
      if (this.pingTimeouts.has(idtarget)) {
        this.pingTimeouts.delete(idtarget);
      }
      this.usersToRemove.set(idtarget, Date.now());
    }, this.RECONNECT_TIMEOUT);
    
    this.pingTimeouts.set(idtarget, timeout);
  }

  async destroy() {
    const timers = [this._tickTimer, this._flushTimer];
    for (const timer of timers) {
      if (timer) clearInterval(timer);
    }

    for (const timeout of this.pingTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.pingTimeouts.clear();

    const buffersToClear = [
      this.chatMessageBuffer,
      this.updateKursiBuffer, 
      this.roomSeats,
      this.userToSeat,
      this.seatLocks,
      this.messageCounts,
      this.usersToRemove,
      this.cleanupInProgress,
      this.roomChatHistory,
      this.userDisconnectTime
    ];
    
    for (const buffer of buffersToClear) {
      if (buffer && typeof buffer.clear === 'function') {
        buffer.clear();
      }
    }

    for (const client of this.clients) {
      try {
        if (client.readyState === 1) {
          client.close(1000, "Server shutdown");
        }
      } catch (e) {}
    }
    this.clients.clear();

    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      this.lowcard.destroy();
    }
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;

    this.vipManager.cleanupUserVipBadges(idtarget);

    this.usersToRemove.delete(idtarget);
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }

    this.userDisconnectTime.delete(idtarget);

    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, info] of seatMap) {
        const n = info.namauser;
        if (!n) continue;

        if (n === idtarget || n === `__LOCK__${idtarget}`) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastRoomUserCount(room);
        }
      }
    }

    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.cleanupInProgress.delete(idtarget);

    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      for (const [seat, info] of seatMapUpdates) {
        if (info && (info.namauser === idtarget || info.namauser === `__LOCK__${idtarget}`)) {
          seatMapUpdates.delete(seat);
        }
      }
    }

    for (const [room, chatList] of this.chatMessageBuffer) {
      if (!Array.isArray(chatList) || chatList.length === 0) continue;
      let changed = false;
      const filtered = [];
      for (let i = 0; i < chatList.length; i++) {
        const msg = chatList[i];
        if (!msg) continue;
        if (msg[3] === idtarget) {
          changed = true;
          continue;
        }
        filtered.push(msg);
      }
      if (changed) {
        this.chatMessageBuffer.set(room, filtered);
      }
    }

    for (const [lockKey] of Array.from(this.seatLocks)) {
      const [room, seatStr] = lockKey.split("-");
      const seatNum = parseInt(seatStr, 10);
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) {
        this.seatLocks.delete(lockKey);
        continue;
      }
      const seatInfo = seatMap.get(seatNum);
      if (!seatInfo || !seatInfo.namauser) {
        this.seatLocks.delete(lockKey);
      } else {
        if (seatInfo.namauser === idtarget || seatInfo.namauser === `__LOCK__${idtarget}`) {
          this.seatLocks.delete(lockKey);
        }
      }
    }

    for (const c of Array.from(this.clients)) {
      try {
        if (c && c.idtarget === idtarget) {
          try {
            if (c.readyState === 1) {
              c.close(1000, "Session removed");
            }
          } catch (e) {}
          this.clients.delete(c);
        }
      } catch (e) {}
    }
  }

  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const key = ws.idtarget || ws._id;
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
    if (messageType === "chat") {
      limit = 50;
    } else if (messageType === "updatePoint") {
      limit = 100;
    }

    if (stats.count++ > limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }

    return true;
  }

  async batchAutoRemove() {
    try {
      const now = Date.now();
      const removalThreshold = 25000;

      this.cleanExpiredLocks();

      const usersToRemoveNow = [];
      let processed = 0;
      const maxBatchSize = 30;

      for (const [idtarget, removalTime] of this.usersToRemove) {
        if (processed >= maxBatchSize) break;

        if (now - removalTime >= removalThreshold) {
          usersToRemoveNow.push(idtarget);
          processed++;
        }
      }

      for (const idtarget of usersToRemoveNow) {
        if (this.cleanupInProgress.has(idtarget)) continue;

        this.cleanupInProgress.add(idtarget);

        try {
          const stillActive = Array.from(this.clients).some(
            c => c.idtarget === idtarget && c.readyState === 1
          );

          if (!stillActive) {
            this.fullRemoveById(idtarget);
          }

          this.usersToRemove.delete(idtarget);
        } catch (error) {
          // Ignore error
        } finally {
          this.cleanupInProgress.delete(idtarget);
        }
      }

      let consistencyChecks = 0;
      for (const [idtarget, seatInfo] of this.userToSeat) {
        if (consistencyChecks >= 50) break;

        if (this.usersToRemove.has(idtarget)) continue;

        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        if (!seatMap) continue;

        const seatData = seatMap.get(seat);
        if (!seatData || seatData.namauser !== idtarget) continue;

        const hasActiveConnection = Array.from(this.clients).some(
          c => c.idtarget === idtarget && c.readyState === 1
        );

        if (!hasActiveConnection) {
          this.usersToRemove.set(idtarget, now);
        }

        consistencyChecks++;
      }

    } catch (error) {
      // Ignore error
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {
      // Ignore error
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    let sentCount = 0;
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        try {
          if (this.safeSend(c, msg)) {
            sentCount++;
          }
        } catch (error) {
          // Ignore error
        }
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const info of seatMap.values()) {
          if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
            cnt[room]++;
          }
        }
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    try {
      const count = this.getJumlahRoom()[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {
      // Ignore error
    }
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        try {
          for (let i = 0; i < messages.length; i++) {
            const msg = messages[i];
            this.broadcastToRoom(room, msg);
          }
          this.chatMessageBuffer.set(room, []);
        } catch (error) {
          this.chatMessageBuffer.set(room, []);
        }
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      try {
        const updates = [];
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
          if (!seatMapUpdates.has(seat)) continue;
          const info = seatMapUpdates.get(seat);
          const { lastPoint, ...rest } = info;
          updates.push([seat, rest]);
        }
        if (updates.length > 0) {
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
        }
        this.updateKursiBuffer.set(room, new Map());
      } catch (error) {
        this.updateKursiBuffer.set(room, new Map());
      }
    }
  }

  async tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;

      for (const c of this.clients) {
        if (c.readyState === 1 && c.roomname) {
          this.safeSend(c, ["currentNumber", this.currentNumber]);
        }
      }
    } catch (error) {
      // Ignore error
    }
  }

  cleanExpiredLocks() {
    try {
      const now = Date.now();
      let cleanedLocks = 0;
      const maxLocksToClean = 20;

      for (const room of roomList) {
        if (cleanedLocks >= maxLocksToClean) break;

        const seatMap = this.roomSeats.get(room);
        if (seatMap) {
          for (const [seat, info] of seatMap) {
            if (cleanedLocks >= maxLocksToClean) break;

            if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
              Object.assign(info, createEmptySeat());
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.clearSeatBuffer(room, seat);
              this.broadcastRoomUserCount(room);
              cleanedLocks++;
            }
          }
        }
      }
    } catch (error) {
      // Ignore error
    }
  }

  async periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.cleanExpiredLocks();
    } catch (error) {
      // Ignore error
    }
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    try {
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {
      // Ignore error
    }
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return null;

      const now = Date.now();

      let locksCleaned = 0;
      for (const [seat, info] of seatMap) {
        if (locksCleaned >= 5) break;

        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          this.clearSeatBuffer(room, seat);
          locksCleaned++;
        }
      }

      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const k = seatMap.get(i);
        if (k && k.namauser === "") {
          k.namauser = "__LOCK__" + ws.idtarget;
          k.lockTime = now;
          return i;
        }
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  // ✅ METHOD BARU UNTUK KIRIM STATE CEPAT
  sendImmediateState(ws, room) {
    if (ws.readyState !== 1) return;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      // 1. KIRIM CURRENT NUMBER LANGSUNG
      this.safeSend(ws, ["currentNumber", this.currentNumber]);

      // 2. KIRIM ROOM COUNT LANGSUNG
      const count = this.getJumlahRoom()[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);

      // 3. KIRIM KURSI DATA YANG ADA ISI SAJA (OPTIMIZED)
      const kursiUpdates = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;

        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
          kursiUpdates.push([
            seat,
            {
              noimageUrl: info.noimageUrl,
              namauser: info.namauser,
              color: info.color,
              itembawah: info.itembawah,
              itematas: info.itematas,
              vip: info.vip,
              viptanda: info.viptanda
            }
          ]);
        }
      }

      if (kursiUpdates.length > 0) {
        this.safeSend(ws, ["kursiBatchUpdate", room, kursiUpdates]);
      }

      // 4. KIRIM LAST POINTS YANG ADA SAJA
      const lastPointsData = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info || !info.lastPoint) continue;
        
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
          lastPointsData.push({
            seat: seat,
            x: info.lastPoint.x,
            y: info.lastPoint.y,
            fast: info.lastPoint.fast
          });
        }
      }

      if (lastPointsData.length > 0) {
        this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }

    } catch (error) {
      console.error("Error sending immediate state:", error);
    }
  }

  handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) {
      return false;
    }

    if (ws.idtarget && ws.roomname && ws.roomname !== newRoom) {
        this.removeAllSeatsById(ws.idtarget);
    }

    ws.roomname = newRoom;
    const foundSeat = this.lockSeat(newRoom, ws);

    if (foundSeat === null) {
        this.safeSend(ws, ["roomFull", newRoom]);
        return false;
    }

    ws.numkursi = new Set([foundSeat]);
    
    // ✅ KIRIM LANGSUNG TANPA BUFFER
    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);

    if (ws.idtarget) {
        this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
    }
    
    // ✅ KIRIM STATE LANGSUNG, JANGAN PAKAI sendAllStateTo YANG COMPLEX
    this.sendImmediateState(ws, newRoom);
    this.vipManager.getAllVipBadges(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);
    
    return true;
  }

  cleanupClientSafely(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    if (this.cleanupInProgress.has(id)) return;

    this.cleanupInProgress.add(id);

    try {
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      const activeConnections = Array.from(this.clients).filter(
        c => c.idtarget === id && c !== ws && c.readyState === 1
      );

      if (activeConnections.length === 0) {
        this.usersToRemove.set(id, Date.now());
        this.userDisconnectTime.set(id, Date.now());
      }

      this.clients.delete(ws);

      if (activeConnections.length === 0) {
        this.fullRemoveById(id);
      } else {
        this.messageCounts.delete(id);
      }

    } catch (error) {
      // Ignore error
    } finally {
      this.cleanupInProgress.delete(id);
    }
  }

  async removeAllSeatsById(idtarget) {
    try {
      const seatInfo = this.userToSeat.get(idtarget);
      if (!seatInfo) return;

      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap || !seatMap.has(seat)) {
        this.userToSeat.delete(idtarget);
        return;
      }

      const currentSeat = seatMap.get(seat);
      if (currentSeat.namauser === idtarget || currentSeat.namauser === `__LOCK__${idtarget}`) {
        if (currentSeat.viptanda > 0) {
          this.vipManager.removeVipBadge(room, seat);
        }
        
        Object.assign(currentSeat, createEmptySeat());
        this.clearSeatBuffer(room, seat);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }

      this.userToSeat.delete(idtarget);
      this.usersToRemove.delete(idtarget);
    } catch (error) {
      // Ignore error
    }
  }

  getAllOnlineUsers() {
    const users = [];
    let count = 0;
    for (const ws of this.clients) {
      if (count >= 1000) break;
      if (ws.idtarget && ws.readyState === 1) {
        users.push(ws.idtarget);
        count++;
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    let count = 0;
    for (const ws of this.clients) {
      if (count >= 500) break;
      if (ws.roomname === roomName && ws.idtarget && ws.readyState === 1) {
        users.push(ws.idtarget);
        count++;
      }
    }
    return users;
  }

  handleSetIdTarget2(ws, id, baru) {
    ws.idtarget = id;

    // ✅ CLEANUP TIMEOUT & STATE
    if (this.pingTimeouts.has(id)) {
      clearTimeout(this.pingTimeouts.get(id));
      this.pingTimeouts.delete(id);
    }
    this.usersToRemove.delete(id);
    this.messageCounts.delete(id);

    if (baru === true) {
      // ✅ USER BARU - RESET SEMUA
      this.userDisconnectTime.delete(id);
      this.userToSeat.delete(id);
      ws.roomname = undefined;
      ws.numkursi = new Set();
      
    } else if (baru === false) {
      // ✅ USER EXISTING - CEK QUICK & REJOIN
      const seatInfo = this.userToSeat.get(id);
      
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        const seatData = seatMap?.get(seat);

        if (seatData?.namauser === id) {
          // ✅ MASIH ADA DI KURSI - REJOIN CEPAT
          ws.roomname = room;
          ws.numkursi = new Set([seat]);
          this.sendImmediateState(ws, room); // ✅ PAKAI YANG CEPAT
          this.broadcastRoomUserCount(room);
        } else {
          // ✅ KURSI SUDAH KOSONG - RESET
          this.userToSeat.delete(id);
          this.safeSend(ws, ["needJoinRoom"]);
        }
      } else {
        this.safeSend(ws, ["needJoinRoom"]);
      }
    }
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;

    try {
      this.fullRemoveById(idtarget);
      this.clients.delete(ws);
    } catch (error) {
      // Ignore error
    }
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

    try {
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
          if (idtarget) {
            // ✅ USER SENGAJA KELUAR APP - LANGSUNG BERSIHKAN SEMUA
            this.fullRemoveById(idtarget);
            this.clients.delete(ws);
          }
          break;
        }

        case "setIdTarget2": {
          const id = data[1];
          const baru = data[2];
          this.handleSetIdTarget2(ws, id, baru);
          break;
        }

        case "setIdTarget": {
          const newId = data[1];
          ws.idtarget = newId;

          if (this.pingTimeouts.has(newId)) {
            clearTimeout(this.pingTimeouts.get(newId));
            this.pingTimeouts.delete(newId);
          }

          if (this.usersToRemove.has(newId)) {
            this.usersToRemove.delete(newId);
          }

          const prevSeat = this.userToSeat.get(newId);

          if (prevSeat) {
            ws.roomname = prevSeat.room;
            ws.numkursi = new Set([prevSeat.seat]);
            this.sendImmediateState(ws, prevSeat.room); // ✅ PAKAI YANG CEPAT

            const seatMap = this.roomSeats.get(prevSeat.room);
            if (seatMap) {
              const seatInfo = seatMap.get(prevSeat.seat);
              if (seatInfo.namauser === `__LOCK__${newId}` || !seatInfo.namauser) {
                seatInfo.namauser = newId;
              }
            }
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

        case "joinRoom": {
          const newRoom = data[1];
          this.handleJoinRoom(ws, newRoom);
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return;
          
          if (!this.chatMessageBuffer.has(roomname))
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          
          if (!this.roomChatHistory.has(roomname)) {
            this.roomChatHistory.set(roomname, []);
          }
          const history = this.roomChatHistory.get(roomname);
          
          const chatData = {
            timestamp: Date.now(),
            noImageURL,
            username, 
            message,
            usernameColor,
            chatTextColor
          };
          
          history.push(chatData);
          
          if (history.length > 10) {
            this.roomChatHistory.set(roomname, history.slice(-10));
          }
          
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
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
          if (!roomList.includes(room)) return;

          const lockKey = `${room}-${seat}`;
          if (this.seatLocks.has(lockKey)) return;
          this.seatLocks.set(lockKey, true);

          try {
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
            this.updateKursiBuffer.get(room).set(seat, { ...currentInfo, lastPoint: currentInfo.lastPoint });
            this.broadcastRoomUserCount(room);
          } finally {
            this.seatLocks.delete(lockKey);
          }
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
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
        case "gameLowCardEnd": {
          const room = ws.roomname;
          if (room !== "LowCard") return;
          this.lowcard.handleEvent(ws, data);
          break;
        }
      }
    } catch (error) {
      // Ignore error
    }
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      await server.accept();

      const ws = server;

      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          this.cleanupClientSafely(ws);
        }
      });

      ws.addEventListener("close", (event) => {
        const id = ws.idtarget;
        // ✅ HANYA SCHEDULE TIMEOUT JIKA BUKAN NORMAL CLOSURE (1000)
        if (id && event.code !== 1000) {
          this.userDisconnectTime.set(id, Date.now());
          this.scheduleCleanupTimeout(id);
        }
      });

      ws.addEventListener("error", (event) => {
        const id = ws.idtarget;
        if (id) {
          this.userDisconnectTime.set(id, Date.now());
          this.scheduleCleanupTimeout(id);
        }
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      if (new URL(req.url).pathname === "/health")
        return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
}
