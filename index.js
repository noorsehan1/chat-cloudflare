// ChatServer Durable Object - FINAL FIX dengan VIP Badge System
import { LowCardGameManager } from "./lowcard.js";

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
    points: [],
    lockTime: undefined,
    lastActivity: Date.now()
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

    // Initialize rooms and seats
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    // VIP Badge System - Map terpisah
    this.vipBadgeMap = new Map(); // Format: { room -> { seat -> {numbadge, colorvip} } }

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    // Fix: Seat locks untuk prevent race condition
    this.seatLocks = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }

    // Optimized timers with cleanup
    this._tickTimer = setInterval(() => {
      this.tick().catch(() => {});
    }, this.intervalMillis);

    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) {
        this.periodicFlush().catch(() => {});
      }
    }, 500);

    this._autoRemoveTimer = setInterval(() => {
      if (this.usersToRemove.size > 0 || this.userToSeat.size > 0) {
        this.batchAutoRemove().catch(() => {});
      }
    }, 30000);

    this.lowcard = new LowCardGameManager(this);

    this.pingTimeouts = new Map();
    this.RECONNECT_TIMEOUT = 20000;
    this.cleanupInProgress = new Set();
    this.usersToRemove = new Map();

    // Rate limiting dengan adjustable limits
    this.messageCounts = new Map();
    this.MAX_MESSAGES_PER_SECOND = 20;
  }

  // ==================== VIP BADGE SYSTEM ====================

  // Function untuk VIP badge dengan Map terpisah
  handleVipBadge(room, seat, numbadge, colorvip) {
    if (!roomList.includes(room)) return false;
    
    // Initialize Map untuk room jika belum ada
    if (!this.vipBadgeMap.has(room)) {
      this.vipBadgeMap.set(room, new Map());
    }
    
    const roomVipMap = this.vipBadgeMap.get(room);
    
    if (numbadge > 0) {
      // Add/update VIP badge
      roomVipMap.set(seat, {
        numbadge: numbadge,
        colorvip: colorvip,
        timestamp: Date.now()
      });
    } else {
      // Remove VIP badge
      roomVipMap.delete(seat);
    }
    
    // Broadcast ke room tersebut
    this.broadcastToRoom(room, [
      "vipbadge", 
      room,
      seat,
      numbadge, 
      colorvip
    ]);
    
    return true;
  }

  // Function untuk get semua VIP badge di room (untuk new user join)
  getAllVipBadges(room) {
    if (!this.vipBadgeMap.has(room)) return [];
    
    const roomVipMap = this.vipBadgeMap.get(room);
    const vipBadges = [];
    
    for (const [seat, vipData] of roomVipMap) {
      vipBadges.push([
        seat,
        vipData.numbadge,
        vipData.colorvip
      ]);
    }
    
    return vipBadges;
  }

  // Hapus VIP badge ketika kursi dihapus
  removeVipBadgeFromSeat(room, seat) {
    if (this.vipBadgeMap.has(room)) {
      const roomVipMap = this.vipBadgeMap.get(room);
      if (roomVipMap.has(seat)) {
        roomVipMap.delete(seat);
        // Broadcast removal
        this.broadcastToRoom(room, [
          "vipbadgeremove", 
          room,
          seat
        ]);
      }
    }
  }

  // ==================== CORE FUNCTIONS ====================

  async destroy() {
    console.log('ChatServer Durable Object destroying...');
    
    // 1. Clear semua timers
    const timers = [this._tickTimer, this._flushTimer, this._autoRemoveTimer];
    for (const timer of timers) {
      if (timer) clearInterval(timer);
    }

    // 2. Clear semua timeouts
    for (const timeout of this.pingTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.pingTimeouts.clear();

    // 3. Clear semua buffers dan collections
    const buffersToClear = [
      this.chatMessageBuffer,
      this.updateKursiBuffer, 
      this.privateMessageBuffer,
      this.roomSeats,
      this.userToSeat,
      this.seatLocks,
      this.messageCounts,
      this.usersToRemove,
      this.cleanupInProgress,
      this.vipBadgeMap // Clear VIP map juga
    ];
    
    for (const buffer of buffersToClear) {
      if (buffer && typeof buffer.clear === 'function') {
        buffer.clear();
      }
    }

    // 4. Close semua websocket connections
    for (const client of this.clients) {
      try {
        if (client.readyState === 1) {
          client.close(1000, "Server shutdown");
        }
      } catch (e) {
        // suppress
      }
    }
    this.clients.clear();

    // 5. Cleanup game manager
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      this.lowcard.destroy();
    }
    
    console.log('ChatServer Durable Object destroyed successfully');
  }

  // Comprehensive remover that clears all references for an idtarget
  fullRemoveById(idtarget) {
    if (!idtarget) return;

    // 1) Remove scheduled removal and ping timeout
    this.usersToRemove.delete(idtarget);
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }

    // 2) Remove seat(s) across all rooms that reference the idtarget (or lock by id)
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, info] of seatMap) {
        const n = info.namauser;
        if (!n) continue;

        // Matches either exact id or lock pattern
        if (n === idtarget || n === `__LOCK__${idtarget}`) {
          Object.assign(info, createEmptySeat());
          
          // Hapus VIP badge dari kursi ini
          this.removeVipBadgeFromSeat(room, seatNumber);
          
          // Broadcast immediate removal untuk that seat
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.broadcastRoomUserCount(room);
        }
      }
    }

    // 3) Remove userToSeat mapping
    this.userToSeat.delete(idtarget);

    // 4) Remove private messages buffer and rate-limit counters and cleanup flags
    this.privateMessageBuffer.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.cleanupInProgress.delete(idtarget);

    // 5) Remove any pending seat updates from updateKursiBuffer referencing idtarget
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      for (const [seat, info] of seatMapUpdates) {
        if (info && (info.namauser === idtarget || info.namauser === `__LOCK__${idtarget}`)) {
          seatMapUpdates.delete(seat);
        }
      }
    }

    // 6) Remove chat messages coming from this idtarget in buffers
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

    // 7) Remove any seatLocks that point to empty seats (conservative cleanup)
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
        // If seatInfo belongs to idtarget, ensure lock removed
        if (seatInfo.namauser === idtarget || seatInfo.namauser === `__LOCK__${idtarget}`) {
          this.seatLocks.delete(lockKey);
        }
      }
    }

    // 8) Remove any client websockets with this idtarget from clients set
    for (const c of Array.from(this.clients)) {
      try {
        if (c && c.idtarget === idtarget) {
          try {
            if (c.readyState === 1) {
              c.close(1000, "Session removed");
            }
          } catch (e) {
            // suppress
          }
          this.clients.delete(c);
        }
      } catch (e) {
        // suppress
      }
    }
  }

  // Enhanced rate limiting dengan type-based limits
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

    // Adjust limits based on message type
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

            for (const client of this.clients) {
              if (client.idtarget === idtarget && client.readyState === 1) {
                this.safeSend(client, ["needJoinRoom"]);
              }
            }
          }

          this.usersToRemove.delete(idtarget);
        } catch (error) {
          // suppress
        } finally {
          this.cleanupInProgress.delete(idtarget);
        }
      }

      // Consistency check - limited to 50 users per run
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
      // suppress
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {
      // suppress
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
          // suppress
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
      // suppress
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
          const { points, ...rest } = info;
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
      // suppress
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
              this.broadcastRoomUserCount(room);
              cleanedLocks++;
            }
          }
        }
      }
    } catch (error) {
      // suppress
    }
  }

  async periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.cleanExpiredLocks();

      const now = Date.now();

      // Update seat activity untuk active clients
      for (const client of this.clients) {
        if (client.idtarget && client.readyState === 1) {
          const seatInfo = this.userToSeat.get(client.idtarget);
          if (seatInfo) {
            const { room, seat } = seatInfo;
            const seatMap = this.roomSeats.get(room);
            if (seatMap && seatMap.has(seat)) {
              const seatData = seatMap.get(seat);
              if (seatData.namauser === client.idtarget) {
                seatData.lastActivity = now;
                this.usersToRemove.delete(client.idtarget);
              }
            }
          }
        }
      }

      // Deliver private messages dengan batching
      let messagesDelivered = 0;
      const maxMessagesPerFlush = 50;

      for (const [id, msgs] of this.privateMessageBuffer) {
        if (messagesDelivered >= maxMessagesPerFlush) break;

        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === id && c.readyState === 1) {
            const batch = msgs.slice(0, 10);
            for (let i = 0; i < batch.length; i++) {
              const m = batch[i];
              this.safeSend(c, m);
              messagesDelivered++;
              if (messagesDelivered >= maxMessagesPerFlush) break;
            }
            delivered = true;
            break;
          }
        }
        if (delivered) {
          this.privateMessageBuffer.delete(id);
        }
      }
    } catch (error) {
      // suppress
    }
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    try {
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {
      // suppress
    }
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return null;

      const now = Date.now();

      // Clean expired locks first (limited)
      let locksCleaned = 0;
      for (const [seat, info] of seatMap) {
        if (locksCleaned >= 5) break;

        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          locksCleaned++;
        }
      }

      // Find available seat
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const k = seatMap.get(i);
        if (k && k.namauser === "") {
          k.namauser = "__LOCK__" + ws.idtarget;
          k.lockTime = now;
          k.lastActivity = now;
          this.userToSeat.set(ws.idtarget, { room, seat: i });
          return i;
        }
      }
      return null;
    } catch (error) {
      return null;
    }
  }

 senderrorstate(ws, room) {
    if (ws.readyState !== 1) return;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      this.safeSend(ws, ["currentNumber", this.currentNumber]);

      const count = this.getJumlahRoom()[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);

      // Kirim semua VIP badges untuk room ini ke user baru
      const vipBadges = this.getAllVipBadges(room);
      if (vipBadges.length > 0) {
        for (let i = 0; i < vipBadges.length; i++) {
          const vipData = vipBadges[i];
          
          // Delay 50ms untuk setiap badge
          setTimeout(() => {
            if (ws.readyState === 1) { // Cek lagi sebelum kirim
              this.safeSend(ws, [
                "vipbadge", 
                room,
                vipData[0], // seat
                vipData[1], // numbadge
                vipData[2]  // colorvip
              ]);
            }
          }, 50 * i);
        }
      }
    } catch (error) {
      // suppress
    }
}
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

    } catch (error) {
      // suppress
    }
  }

 sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      const allPoints = [];
      const meta = {};

      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;

        if (info.points.length > 0) {
          const recentPoints = info.points.slice(-5);
          for (let i = 0; i < recentPoints.length; i++) {
            const point = recentPoints[i];
            allPoints.push({ seat, ...point });
          }
        }

        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
          meta[seat] = {
            noimageUrl: info.noimageUrl,
            namauser: info.namauser,
            color: info.color,
            itembawah: info.itembawah,
            itematas: info.itematas,
            vip: info.vip,
            viptanda: info.viptanda
          };
        }
      }

      this.safeSend(ws, ["allPointsList", room, allPoints]);
      this.safeSend(ws, ["allUpdateKursiList", room, meta]);

      // Kirim semua VIP badges untuk room ini
      const vipBadges = this.getAllVipBadges(room);
      if (vipBadges.length > 0) {
        for (let i = 0; i < vipBadges.length; i++) {
          const vipData = vipBadges[i];
          
          setTimeout(() => {
            try {
              if (ws.readyState === 1) {
                this.safeSend(ws, [
                  "vipbadge", 
                  room,
                  vipData[0], // seat
                  vipData[1], // numbadge
                  vipData[2]  // colorvip
                ]);
              }
            } catch (error) {
              console.error(`Error sending VIP badge for seat ${vipData[0]}:`, error);
            }
          }, 50 * i);
        }
      }
    } catch (error) {
      // suppress
    }
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
      }

      this.clients.delete(ws);

      if (activeConnections.length === 0) {
        this.fullRemoveById(id);
      } else {
        this.privateMessageBuffer.delete(id);
        this.messageCounts.delete(id);
      }

    } catch (error) {
      // suppress
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
        Object.assign(currentSeat, createEmptySeat());
        
        // Hapus VIP badge dari kursi ini
        this.removeVipBadgeFromSeat(room, seat);
        
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }

      this.userToSeat.delete(idtarget);
      this.usersToRemove.delete(idtarget);
    } catch (error) {
      // suppress
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

  // ✅ USER BARU (baru === true) - BUTUH CLEANUP
  if (baru === true) {
    // === CLEANUP DULU UNTUK USER BARU ===
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === id) {
          Object.assign(seatInfo, createEmptySeat());
          
          // Hapus VIP badge dari kursi ini
          this.removeVipBadgeFromSeat(room, seatNumber);
          
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(id);
    this.usersToRemove.delete(id);
    this.privateMessageBuffer.delete(id);
    this.messageCounts.delete(id);
    
    if (this.pingTimeouts.has(id)) {
      clearTimeout(this.pingTimeouts.get(id));
      this.pingTimeouts.delete(id);
    }

    // PROSES USER BARU
    if (ws.roomname && ws.numkursi && ws.numkursi.size > 0) {
      // User sudah join room manual sebelumnya
      const room = ws.roomname;
      const seat = Array.from(ws.numkursi)[0];
      
      const seatMap = this.roomSeats.get(room);
      if (seatMap?.has(seat)) {
        const seatData = seatMap.get(seat);
        
        // Assign seat ke user baru
        seatData.namauser = id;
        seatData.lastActivity = Date.now();
        
        this.userToSeat.set(id, { room, seat });
        
        // Broadcast update kursi
        const updateData = {
          noimageUrl: seatData.noimageUrl,
          namauser: seatData.namauser,
          color: seatData.color,
          itembawah: seatData.itembawah,
          itematas: seatData.itematas,
          vip: seatData.vip,
          viptanda: seatData.viptanda
        };
        
        if (!this.updateKursiBuffer.has(room))
          this.updateKursiBuffer.set(room, new Map());
        this.updateKursiBuffer.get(room).set(seat, updateData);
        
        this.broadcastRoomUserCount(room);
      }
    }
  }
  // ✅ USER LAMA (baru === false) - HANYA RESTORE, TANPA CLEANUP
  else if (baru === false) {
    // Hanya hapus ping timeout (selalu perlu)
    if (this.pingTimeouts.has(id)) {
      clearTimeout(this.pingTimeouts.get(id));
      this.pingTimeouts.delete(id);
    }

    this.usersToRemove.delete(id);

    // LANGSUNG RESTORE DARI SEAT INFO YANG ADA
    const seatInfo = this.userToSeat.get(id);

    if (seatInfo) {
      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);

      if (seatMap?.has(seat)) {
        const seatData = seatMap.get(seat);

        if (seatData.namauser === id) {
          // ✅ User masih di seat yang sama - RESTORE BERHASIL
          ws.roomname = room;
          ws.numkursi = new Set([seat]);
          this.broadcastRoomUserCount(room);
        } else {
          // ❌ Seat sudah diduduki orang lain
          this.safeSend(ws, ["needJoinRoom"]);
        }
      } else {
        // ❌ Seat tidak ada
        this.safeSend(ws, ["needJoinRoom"]);
      }
    } else {
      // ❌ Tidak ada seat info
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }

  // Kirim private messages buffer (untuk kedua case)
  if (this.privateMessageBuffer.has(id)) {
    for (const msg of this.privateMessageBuffer.get(id)) {
      this.safeSend(ws, msg);
    }
    this.privateMessageBuffer.delete(id);
  }
}

  
  scheduleCleanupTimeout(idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
    }

    const timeout = setTimeout(() => {
      if (this.cleanupInProgress.has(idtarget)) return;
      this.cleanupInProgress.add(idtarget);

      try {
        const stillActive = Array.from(this.clients).some(
          c => c.idtarget === idtarget && c.readyState === 1
        );

        if (!stillActive) {
          this.usersToRemove.set(idtarget, Date.now());
        }

      } catch (error) {
        // suppress
      } finally {
        this.pingTimeouts.delete(idtarget);
        this.cleanupInProgress.delete(idtarget);
      }
    }, this.RECONNECT_TIMEOUT);

    this.pingTimeouts.set(idtarget, timeout);
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;

    try {
      this.fullRemoveById(idtarget);
      this.clients.delete(ws);
    } catch (error) {
      // suppress
    }
  }

handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) return false;

    if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);

    ws.roomname = newRoom;
    const foundSeat = this.lockSeat(newRoom, ws);

    if (foundSeat === null) {
        // ✅ KIRIM EVENT ROOM FULL KE CLIENT
        this.safeSend(ws, ["roomFull", newRoom]);
        return false;
    }

    ws.numkursi = new Set([foundSeat]);
    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);

    if (ws.idtarget) {
        this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
    }
    
    this.sendAllStateTo(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);
    
    return true;
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
            this.senderrorstate(ws, prevSeat.room);

            const seatMap = this.roomSeats.get(prevSeat.room);
            if (seatMap) {
              const seatInfo = seatMap.get(prevSeat.seat);
              if (seatInfo.namauser === `__LOCK__${newId}` || !seatInfo.namauser) {
                seatInfo.namauser = newId;
                seatInfo.lastActivity = Date.now();
              }
            }
          } else {
            if (this.hasEverSetId) {
              this.safeSend(ws, ["needJoinRoom"]);
            }
          }

          this.hasEverSetId = true;

          if (this.privateMessageBuffer.has(newId)) {
            for (const msg of this.privateMessageBuffer.get(newId))
              this.safeSend(ws, msg);
            this.privateMessageBuffer.delete(newId);
          }

          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
          break;
        }

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          let delivered = false;
          for (const c of this.clients) {
            if (c.idtarget === idtarget && c.readyState === 1) {
              this.safeSend(c, notif);
              delivered = true;
            }
          }
          if (!delivered) {
            if (!this.privateMessageBuffer.has(idtarget))
              this.privateMessageBuffer.set(idtarget, []);
            this.privateMessageBuffer.get(idtarget).push(notif);
          }
          break;
        }

        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const out = ["private", idt, url, msg, ts, sender];
          this.safeSend(ws, out);
          let delivered = false;
          for (const c of this.clients) {
            if (c.idtarget === idt && c.readyState === 1) {
              this.safeSend(c, out);
              delivered = true;
            }
          }
          if (!delivered) {
            if (!this.privateMessageBuffer.has(idt))
              this.privateMessageBuffer.set(idt, []);
            this.privateMessageBuffer.get(idt).push(out);
          }
          break;
        }

        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";

          // ✅ SEDERHANA: Cek apakah user ada yang online
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
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;

          si.points.push({ x, y, fast, timestamp: Date.now() });

          const now = Date.now();
          si.points = si.points.filter(point => now - point.timestamp < 3000);

          si.lastActivity = now;
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (!roomList.includes(room)) return;
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), createEmptySeat());
          
          // Hapus VIP badge dari kursi ini
          this.removeVipBadgeFromSeat(room, seat);
          
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
              noimageUrl, namauser, color, itembawah, itematas, vip, viptanda,
              lastActivity: Date.now()
            });

            seatMap.set(seat, currentInfo);
            if (!this.updateKursiBuffer.has(room))
              this.updateKursiBuffer.set(room, new Map());
            this.updateKursiBuffer.get(room).set(seat, { ...currentInfo, points: [] });
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

        // ==================== VIP BADGE CASES ====================
        case "vipbadge": {
          const [, room, seat, numbadge, colorvip] = data;
          this.handleVipBadge(room, seat, numbadge, colorvip);
          break;
        }

        case "vipbadgeremove": {
          const [, room, seat] = data;
          this.removeVipBadgeFromSeat(room, seat);
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
      // suppress
    }
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      // ✅ FIX: Tambahkan await di sini
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
        if (id) {
          this.scheduleCleanupTimeout(id);
        }
        this.cleanupClientSafely(ws);
      });

      // ✅ FIX: Perbaiki typo "reeor" menjadi "error"
      ws.addEventListener("error", (event) => {
        const id = ws.idtarget;
        if (id) {
          this.scheduleCleanupTimeout(id);
        }
        this.cleanupClientSafely(ws);
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
};


