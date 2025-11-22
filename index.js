import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// ✅ CONSTANTS
const CONSTANTS = {
  MAX_SEATS: 35,
  MAX_MESSAGES_PER_SECOND: 20,
  MAX_MESSAGES_CHAT: 50,
  MAX_MESSAGES_POINT: 100,
  SEAT_LOCK_TIMEOUT: 10000,
  RECONNECT_TIMEOUT: 20000,
  POINT_RETENTION: 3000,
  BUFFER_FLUSH_INTERVAL: 500,
  TICK_INTERVAL: 15 * 60 * 1000,
  MAX_BUFFER_SIZE: 1000,
  MAX_BATCH_SIZE: 30
};

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

    this.MAX_SEATS = CONSTANTS.MAX_SEATS;
    this.roomSeats = new Map();

    // Initialize rooms and seats
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    // ✅ INIT MANAGERS
    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);

    // ✅ BUFFERS
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();
    this.seatLocks = new Map();

    // ✅ NEW: JOIN LOCKS UNTUK CEK DOBEL JOIN
    this.joinLocks = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;

    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }

    // ✅ TIMERS
    this._timers = [];
    this.initTimers();

    // ✅ CONNECTION TRACKING
    this.pingTimeouts = new Map();
    this.cleanupInProgress = new Set();
    this.messageCounts = new Map();
    this.userConnections = new Map();
  }

  // ✅ INIT TIMERS
  initTimers() {
    try {
      this._timers.push(setInterval(() => {
        this.tick().catch(() => {});
      }, CONSTANTS.TICK_INTERVAL));

      this._timers.push(setInterval(() => {
        if (this.clients.size > 0) {
          this.periodicFlush().catch(() => {});
        }
      }, CONSTANTS.BUFFER_FLUSH_INTERVAL));
    } catch (error) {
      // Ignore timer errors
    }
  }

  // ✅ CLEAR SEAT BUFFER
  clearSeatBuffer(room, seatNumber) {
    try {
      if (!room || typeof seatNumber !== 'number') return;
      
      const roomBuffer = this.updateKursiBuffer.get(room);
      if (roomBuffer) {
        roomBuffer.delete(seatNumber);
      }
    } catch (error) {
      // Ignore buffer clear errors
    }
  }

  // ✅ SCHEDULE CLEANUP
  scheduleCleanupTimeout(idtarget) {
    if (!idtarget) return;

    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    
    const timeout = setTimeout(() => {
      if (this.pingTimeouts.has(idtarget)) {
        this.pingTimeouts.delete(idtarget);
      }
      
      const hasActiveConnections = this.getUserConnectionCount(idtarget) > 0;
      if (!hasActiveConnections) {
        this.fullRemoveById(idtarget);
      }
    }, CONSTANTS.RECONNECT_TIMEOUT);
    
    this.pingTimeouts.set(idtarget, timeout);
  }

  // ✅ DESTROY
  async destroy() {
    this._timers.forEach(timer => {
      if (timer) clearInterval(timer);
    });
    this._timers = [];

    for (const timeout of this.pingTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.pingTimeouts.clear();

    const buffersToClear = [
      this.chatMessageBuffer,
      this.updateKursiBuffer, 
      this.privateMessageBuffer,
      this.roomSeats,
      this.userToSeat,
      this.seatLocks,
      this.messageCounts,
      this.cleanupInProgress,
      this.userConnections,
      this.joinLocks
    ];
    
    buffersToClear.forEach(buffer => {
      if (buffer && typeof buffer.clear === 'function') {
        buffer.clear();
      }
    });

    for (const client of this.clients) {
      try {
        if (client.readyState === 1) {
          client.close(1000, "Server shutdown");
        }
      } catch (error) {
        // Ignore close errors
      }
    }
    this.clients.clear();

    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      this.lowcard.destroy();
    }
    
    if (this.vipManager && typeof this.vipManager.destroy === 'function') {
      this.vipManager.destroy();
    }
  }

  // ✅ FULL REMOVE BY ID - DENGAN JOIN LOCK CLEANUP
  fullRemoveById(idtarget) {
    if (!idtarget) return;

    if (this.cleanupInProgress.has(idtarget)) return;
    this.cleanupInProgress.add(idtarget);

    try {
      this.vipManager.cleanupUserVipBadges(idtarget);

      if (this.pingTimeouts.has(idtarget)) {
        clearTimeout(this.pingTimeouts.get(idtarget));
        this.pingTimeouts.delete(idtarget);
      }

      // ✅ CLEANUP JOIN LOCKS
      this.joinLocks.delete(idtarget);

      this.removeUserFromAllSeats(idtarget);

      this.userToSeat.delete(idtarget);
      this.privateMessageBuffer.delete(idtarget);
      this.messageCounts.delete(idtarget);
      this.userConnections.delete(idtarget);

      this.cleanUserFromBuffers(idtarget);
      this.closeUserConnections(idtarget);

    } catch (error) {
      // Ignore removal errors
    } finally {
      this.cleanupInProgress.delete(idtarget);
    }
  }

  // ✅ REMOVE USER FROM ALL SEATS
  removeUserFromAllSeats(idtarget) {
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, info] of seatMap) {
        const userName = info.namauser;
        if (!userName) continue;

        if (userName === idtarget || userName === `__LOCK__${idtarget}`) {
          if (info.viptanda > 0) {
            this.vipManager.removeVipBadge(room, seatNumber);
          }
          
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.clearSeatBuffer(room, seatNumber);
        }
      }
      this.broadcastRoomUserCount(room);
    }
  }

  // ✅ CLEAN USER FROM BUFFERS
  cleanUserFromBuffers(idtarget) {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      for (const [seat, info] of seatMapUpdates) {
        if (info && (info.namauser === idtarget || info.namauser === `__LOCK__${idtarget}`)) {
          seatMapUpdates.delete(seat);
        }
      }
    }

    for (const [room, chatList] of this.chatMessageBuffer) {
      if (!Array.isArray(chatList)) continue;
      
      const filtered = chatList.filter(msg => msg && msg[3] !== idtarget);
      if (filtered.length !== chatList.length) {
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
      if (!seatInfo || !seatInfo.namauser || 
          seatInfo.namauser === idtarget || 
          seatInfo.namauser === `__LOCK__${idtarget}`) {
        this.seatLocks.delete(lockKey);
      }
    }
  }

  // ✅ CLOSE USER CONNECTIONS
  closeUserConnections(idtarget) {
    const userConnections = this.userConnections.get(idtarget);
    if (userConnections) {
      for (const ws of userConnections) {
        try {
          if (ws.readyState === 1) {
            ws.close(1000, "Session removed");
          }
          this.clients.delete(ws);
        } catch (error) {
          // Ignore close errors
        }
      }
      this.userConnections.delete(idtarget);
    }
  }

  // ✅ RATE LIMITING
  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const key = ws.idtarget || ws._id;
    if (!key) return false;

    const windowStart = Math.floor(now / 1000);

    if (!this.messageCounts.has(key)) {
      this.messageCounts.set(key, { count: 0, window: windowStart });
    }

    const stats = this.messageCounts.get(key);
    if (stats.window !== windowStart) {
      stats.count = 0;
      stats.window = windowStart;
    }

    let limit = CONSTANTS.MAX_MESSAGES_PER_SECOND;
    if (messageType === "chat") {
      limit = CONSTANTS.MAX_MESSAGES_CHAT;
    } else if (messageType === "updatePoint") {
      limit = CONSTANTS.MAX_MESSAGES_POINT;
    }

    if (stats.count >= limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }

    stats.count++;
    return true;
  }

  // ✅ SAFE SEND
  safeSend(ws, arr) {
    try {
      if (ws && ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (error) {
      // Ignore send errors
    }
    return false;
  }

  // ✅ BROADCAST TO ROOM
  broadcastToRoom(room, msg) {
    let sentCount = 0;
    
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        try {
          if (this.safeSend(c, msg)) {
            sentCount++;
          }
        } catch (error) {
          // Skip failed sends
        }
      }
    }
    
    return sentCount;
  }

  // ✅ GET ROOM USER COUNTS
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

  // ✅ BROADCAST ROOM USER COUNT
  broadcastRoomUserCount(room) {
    try {
      const count = this.getJumlahRoom()[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {
      // Ignore broadcast errors
    }
  }

  // ✅ PERIODIC FLUSH
  async periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.cleanExpiredLocks();
      this.deliverBufferedPrivateMessages();
    } catch (error) {
      // Ignore flush errors
    }
  }

  // ✅ DELIVER BUFFERED PRIVATE MESSAGES
  deliverBufferedPrivateMessages() {
    let messagesDelivered = 0;

    for (const [id, msgs] of this.privateMessageBuffer) {
      if (messagesDelivered >= CONSTANTS.MAX_BATCH_SIZE) break;

      const userConnections = this.userConnections.get(id);
      if (userConnections && userConnections.size > 0) {
        const batch = msgs.slice(0, 10);
        for (let i = 0; i < batch.length; i++) {
          const m = batch[i];
          for (const c of userConnections) {
            if (c.readyState === 1) {
              this.safeSend(c, m);
              messagesDelivered++;
              if (messagesDelivered >= CONSTANTS.MAX_BATCH_SIZE) break;
            }
          }
          if (messagesDelivered >= CONSTANTS.MAX_BATCH_SIZE) break;
        }
        this.privateMessageBuffer.delete(id);
      }
    }
  }

  // ✅ CLEAN EXPIRED LOCKS
  cleanExpiredLocks() {
    try {
      const now = Date.now();
      let cleanedLocks = 0;

      for (const room of roomList) {
        if (cleanedLocks >= 10) break;

        const seatMap = this.roomSeats.get(room);
        if (seatMap) {
          for (const [seat, info] of seatMap) {
            if (cleanedLocks >= 10) break;

            if (String(info.namauser).startsWith("__LOCK__") && 
                info.lockTime && 
                now - info.lockTime > CONSTANTS.SEAT_LOCK_TIMEOUT) {
              
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
      // Ignore lock cleanup errors
    }
  }

  // ✅ FLUSH CHAT BUFFER
  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        try {
          let sentCount = 0;
          for (let i = 0; i < messages.length; i++) {
            const msg = messages[i];
            if (this.broadcastToRoom(room, msg) > 0) {
              sentCount++;
            }
            if (sentCount >= 50) break;
          }
          
          if (sentCount === messages.length) {
            this.chatMessageBuffer.set(room, []);
          } else {
            this.chatMessageBuffer.set(room, messages.slice(sentCount));
          }
        } catch (error) {
          this.chatMessageBuffer.set(room, []);
        }
      }
    }
  }

  // ✅ FLUSH KURSI UPDATES
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

  // ✅ TICK FUNCTION
  async tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;

      for (const c of this.clients) {
        if (c.readyState === 1 && c.roomname) {
          this.safeSend(c, ["currentNumber", this.currentNumber]);
        }
      }
    } catch (error) {
      // Ignore tick errors
    }
  }

  // ✅ LOCK SEAT - DIPERBAIKI UNTUK CEK DOBEL JOIN
  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return null;

      const now = Date.now();

      // ✅ CEK APAKAH USER SUDAH PUNYA SEAT DI ROOM INI
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const existingSeat = seatMap.get(existingSeatInfo.seat);
        if (existingSeat && (existingSeat.namauser === ws.idtarget || existingSeat.namauser === `__LOCK__${ws.idtarget}`)) {
          // ✅ USER SUDAH PUNYA SEAT, KEMBALIKAN SEAT YANG SAMA
          existingSeat.lastActivity = now;
          return existingSeatInfo.seat;
        } else {
          // ✅ SEAT TIDAK VALID LAGI, HAPUS DARI userToSeat
          this.userToSeat.delete(ws.idtarget);
        }
      }

      let locksCleaned = 0;
      for (const [seat, info] of seatMap) {
        if (locksCleaned >= 5) break;

        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > CONSTANTS.SEAT_LOCK_TIMEOUT) {
          Object.assign(info, createEmptySeat());
          this.clearSeatBuffer(room, seat);
          locksCleaned++;
        }
      }

      // ✅ CARI SEAT KOSONG
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

  // ✅ SEND ERROR STATE
  senderrorstate(ws, room) {
    if (ws.readyState !== 1) return;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      this.safeSend(ws, ["currentNumber", this.currentNumber]);

      const count = this.getJumlahRoom()[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);

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
      // Ignore state send errors
    }
  }

  // ✅ SEND ALL STATE TO CLIENT
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

    } catch (error) {
      // Ignore state send errors
    }
  }

  // ✅ CLEANUP CLIENT SAFELY
  cleanupClientSafely(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.removeClientConnection(ws);
      return;
    }

    if (this.cleanupInProgress.has(id)) return;
    this.cleanupInProgress.add(id);

    try {
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      const activeConnectionCount = this.getUserConnectionCount(id);

      this.removeClientConnection(ws);

      if (activeConnectionCount <= 1) {
        this.fullRemoveById(id);
      } else {
        this.privateMessageBuffer.delete(id);
        this.messageCounts.delete(id);
      }

    } catch (error) {
      // Ignore cleanup errors
    } finally {
      this.cleanupInProgress.delete(id);
    }
  }

  // ✅ REMOVE ALL SEATS BY ID
  async removeAllSeatsById(idtarget) {
    if (this.cleanupInProgress.has(idtarget)) return;
    this.cleanupInProgress.add(idtarget);

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
    } catch (error) {
      // Ignore removal errors
    } finally {
      this.cleanupInProgress.delete(idtarget);
    }
  }

  // ✅ GET ALL ONLINE USERS
  getAllOnlineUsers() {
    const users = [];
    let count = 0;
    for (const [idtarget, connections] of this.userConnections) {
      if (count >= 1000) break;
      if (connections.size > 0) {
        users.push(idtarget);
        count++;
      }
    }
    return users;
  }

  // ✅ GET ONLINE USERS BY ROOM
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

  // ✅ CONNECTION MANAGEMENT
  addClientConnection(ws) {
    this.clients.add(ws);
    
    if (ws.idtarget) {
      if (!this.userConnections.has(ws.idtarget)) {
        this.userConnections.set(ws.idtarget, new Set());
      }
      this.userConnections.get(ws.idtarget).add(ws);
    }
  }

  removeClientConnection(ws) {
    this.clients.delete(ws);
    
    if (ws.idtarget && this.userConnections.has(ws.idtarget)) {
      const connections = this.userConnections.get(ws.idtarget);
      connections.delete(ws);
      if (connections.size === 0) {
        this.userConnections.delete(ws.idtarget);
      }
    }
  }

  getUserConnectionCount(idtarget) {
    return this.userConnections.get(idtarget)?.size || 0;
  }

  // ✅ HANDLE SET ID TARGET 2 - DIPERBAIKI UNTUK CEK DOBEL JOIN
  handleSetIdTarget2(ws, id, baru) {
    if (!id) return;

    // ✅ CEK APAKAH USER SUDAH SEDANG PROSES JOIN
    if (this.joinLocks.has(id)) {
      this.safeSend(ws, ["error", "Join process already in progress"]);
      return;
    }

    // ✅ SET JOIN LOCK
    this.joinLocks.set(id, true);

    try {
      ws.idtarget = id;
      this.addClientConnection(ws);

      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      this.privateMessageBuffer.delete(id);
      this.messageCounts.delete(id);

      if (baru === true) {
        // ✅ NEW USER - PASTIKAN BERSIH DARI SEBELUMNYA
        this.removeUserFromAllSeats(id);
        this.userToSeat.delete(id);
        ws.roomname = undefined;
        ws.numkursi = new Set();
        
      } else if (baru === false) {
        // ✅ RETURNING USER - CEK SEAT MASIH VALID
        const seatInfo = this.userToSeat.get(id);

        if (seatInfo) {
          const { room, seat } = seatInfo;
          const seatMap = this.roomSeats.get(room);

          if (seatMap?.has(seat)) {
            const seatData = seatMap.get(seat);

            if (seatData.namauser === id) {
              // ✅ SEAT MASIH VALID
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              seatData.lastActivity = Date.now();
              this.sendAllStateTo(ws, room);
              this.broadcastRoomUserCount(room);
            } else {
              // ✅ SEAT SUDAH TIDAK VALID
              this.userToSeat.delete(id);
              this.safeSend(ws, ["needJoinRoom"]);
            }
          } else {
            // ✅ SEAT TIDAK ADA
            this.userToSeat.delete(id);
            this.safeSend(ws, ["needJoinRoom"]);
          }
        } else {
          // ✅ TIDAK ADA SEAT INFO
          this.safeSend(ws, ["needJoinRoom"]);
        }
      }

      // ✅ DELIVER BUFFERED MESSAGES
      if (this.privateMessageBuffer.has(id)) {
        for (const msg of this.privateMessageBuffer.get(id)) {
          this.safeSend(ws, msg);
        }
        this.privateMessageBuffer.delete(id);
      }
    } finally {
      // ✅ CLEAR JOIN LOCK
      this.joinLocks.delete(id);
    }
  }

  // ✅ HANDLE ON DESTROY
  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;

    try {
      this.fullRemoveById(idtarget);
      this.removeClientConnection(ws);
    } catch (error) {
      // Ignore destroy errors
    }
  }

  // ✅ HANDLE JOIN ROOM - DIPERBAIKI UNTUK CEK DOBEL JOIN
  handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) return false;

    const idtarget = ws.idtarget;
    if (!idtarget) return false;

    // ✅ CEK APAKAH USER SUDAH SEDANG PROSES JOIN
    if (this.joinLocks.has(idtarget)) {
      this.safeSend(ws, ["error", "Join process already in progress"]);
      return false;
    }

    // ✅ SET JOIN LOCK
    this.joinLocks.set(idtarget, true);

    try {
      // ✅ CEK APAKAH SUDAH DI ROOM YANG SAMA
      if (ws.roomname === newRoom) {
        const seatInfo = this.userToSeat.get(idtarget);
        if (seatInfo && seatInfo.room === newRoom) {
          this.safeSend(ws, ["numberKursiSaya", seatInfo.seat]);
          this.safeSend(ws, ["rooMasuk", seatInfo.seat, newRoom]);
          this.sendAllStateTo(ws, newRoom);
          this.vipManager.getAllVipBadges(ws, newRoom);
          return true;
        }
      }

      // ✅ HAPUS DARI SEAT SEBELUMNYA
      this.removeAllSeatsById(idtarget);

      ws.roomname = newRoom;
      const foundSeat = this.lockSeat(newRoom, ws);

      if (foundSeat === null) {
        this.safeSend(ws, ["roomFull", newRoom]);
        return false;
      }

      ws.numkursi = new Set([foundSeat]);
      this.safeSend(ws, ["numberKursiSaya", foundSeat]);
      this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);

      if (idtarget) {
        this.userToSeat.set(idtarget, { room: newRoom, seat: foundSeat });
      }
      
      this.sendAllStateTo(ws, newRoom);
      this.vipManager.getAllVipBadges(ws, newRoom);
      this.broadcastRoomUserCount(newRoom);
      
      return true;
    } finally {
      // ✅ CLEAR JOIN LOCK
      this.joinLocks.delete(idtarget);
    }
  }

  // ✅ HANDLE GET ALL ROOMS USER COUNT
  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    
    try {
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {
      // Ignore count send errors
    }
  }

  // ✅ MAIN MESSAGE HANDLER
  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;

    let data;
    try {
      data = JSON.parse(raw);
    } catch (error) {
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
          if (!newId) return;

          // ✅ CEK JOIN LOCK
          if (this.joinLocks.has(newId)) {
            this.safeSend(ws, ["error", "Join process already in progress"]);
            return;
          }

          this.joinLocks.set(newId, true);

          try {
            const oldId = ws.idtarget;
            if (oldId && oldId !== newId) {
              this.removeClientConnection(ws);
            }

            ws.idtarget = newId;
            this.addClientConnection(ws);

            if (this.pingTimeouts.has(newId)) {
              clearTimeout(this.pingTimeouts.get(newId));
              this.pingTimeouts.delete(newId);
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
          } finally {
            this.joinLocks.delete(newId);
          }
          break;
        }

        // ... (case lainnya tetap sama)
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          
          const userConnections = this.userConnections.get(idtarget);
          if (userConnections) {
            let delivered = false;
            for (const c of userConnections) {
              if (c.readyState === 1) {
                this.safeSend(c, notif);
                delivered = true;
              }
            }
            if (delivered) return;
          }
          
          if (!this.privateMessageBuffer.has(idtarget))
            this.privateMessageBuffer.set(idtarget, []);
          this.privateMessageBuffer.get(idtarget).push(notif);
          break;
        }

        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const out = ["private", idt, url, msg, ts, sender];
          this.safeSend(ws, out);
          
          const userConnections = this.userConnections.get(idt);
          if (userConnections) {
            let delivered = false;
            for (const c of userConnections) {
              if (c.readyState === 1) {
                this.safeSend(c, out);
                delivered = true;
              }
            }
            if (delivered) return;
          }
          
          if (!this.privateMessageBuffer.has(idt))
            this.privateMessageBuffer.set(idt, []);
          this.privateMessageBuffer.get(idt).push(out);
          break;
        }

        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";

          const online = this.getUserConnectionCount(username) > 0;
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
          
          const roomBuffer = this.chatMessageBuffer.get(roomname);
          if (roomBuffer.length < CONSTANTS.MAX_BUFFER_SIZE) {
            roomBuffer.push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          }
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
          si.points = si.points.filter(point => now - point.timestamp < CONSTANTS.POINT_RETENTION);

          si.lastActivity = now;
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
              viptanda: viptanda || 0,
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
          
          const roomBuffer = this.chatMessageBuffer.get(roomname);
          if (roomBuffer.length < CONSTANTS.MAX_BUFFER_SIZE) {
            roomBuffer.push(["gift", roomname, sender, receiver, giftName, Date.now()]);
          }
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
      // Ignore message handling errors
    }
  }

  // ✅ FETCH METHOD
  async fetch(request) {
    if (new URL(request.url).pathname === "/health") {
      return new Response(JSON.stringify({
        status: "healthy",
        clients: this.clients.size,
        rooms: roomList.length,
        timestamp: Date.now()
      }), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }

    const upgrade = request.headers.get("Upgrade");
    if (!upgrade || upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    try {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      await server.accept();
      this.setupWebSocketHandlers(server);
      
      return new Response(null, { 
        status: 101,
        webSocket: client
      });
    } catch (error) {
      return new Response("WebSocket upgrade failed", { status: 500 });
    }
  }

  // ✅ WEBSOCKET HANDLER SETUP
  setupWebSocketHandlers(ws) {
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();

    this.addClientConnection(ws);

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

    ws.addEventListener("error", (event) => {
      const id = ws.idtarget;
      if (id) {
        this.scheduleCleanupTimeout(id);
      }
      this.cleanupClientSafely(ws);
    });
  }
}

// ✅ DEFAULT EXPORT
export default {
  async fetch(req, env) {
    try {
      if (new URL(req.url).pathname === "/health") {
        return new Response("OK", { 
          status: 200, 
          headers: { "content-type": "text/plain" } 
        });
      }

      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        if (!env.CHAT_SERVER) {
          return new Response("Durable Object not configured", { status: 500 });
        }

        try {
          const id = env.CHAT_SERVER.idFromName("global-chat");
          const obj = env.CHAT_SERVER.get(id);
          return await obj.fetch(req);
        } catch (error) {
          return new Response("Service unavailable", { status: 503 });
        }
      }

      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
}
