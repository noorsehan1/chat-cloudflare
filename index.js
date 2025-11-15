// ChatServer Durable Object - OPTIMIZED NO MISS VERSION (FULL CLASS UPDATED)
// Perubahan: optimisasi memory, buffer limits, seatLocks dengan TTL, connId untuk rate-limit,
// index roomClients untuk broadcast efisien, validasi seat, safer rate-count increment,
// optional debug logging via env.DEBUG (keadaan default false).
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
    
    // Optional debug flag (set via Worker env if desired)
    this.DEBUG = !!(env && env.DEBUG);

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

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();
    
    // Seat locks sekarang menyimpan timestamp (ms) untuk TTL-based expiry
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
      this.tick().catch((e) => { if (this.DEBUG) console.error("tick err", e); });
    }, this.intervalMillis);
    
    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) {
        this.periodicFlush().catch((e) => { if (this.DEBUG) console.error("periodicFlush err", e); });
      }
    }, 500);

    this._autoRemoveTimer = setInterval(() => {
      if (this.usersToRemove.size > 0 || this.userToSeat.size > 0) {
        this.batchAutoRemove().catch((e) => { if (this.DEBUG) console.error("batchAutoRemove err", e); });
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

    // Memory & safety limits (configurable constants)
    this.LOCK_TTL_MS = 5000; // seatLocks TTL
    this.MAX_PRIVATE_PER_USER = 200;
    this.MAX_CHAT_BUFFER_PER_ROOM = 1000;
    this.MAX_PRIVATE_DELIVER_PER_FLUSH = 10;
    this.MAX_PRIVATE_MESSAGES_PER_FLUSH_TOTAL = 50;

    // room -> Set(ws) index untuk broadcast efisien
    this.roomClients = new Map();
    for (const room of roomList) this.roomClients.set(room, new Set());
  }

  async destroy() {
    const timers = [this._tickTimer, this._flushTimer, this._autoRemoveTimer];
    for (const timer of timers) {
      try { if (timer) clearInterval(timer); } catch (e) {}
    }
    
    for (const timeout of this.pingTimeouts.values()) {
      try { clearTimeout(timeout); } catch (e) {}
    }
    this.pingTimeouts.clear();
  }

  // Enhanced rate limiting dengan type-based limits
  checkRateLimit(ws, messageType) {
    const now = Date.now();
    // Perbaikan kunci: gunakan idtarget jika ada, else gunakan koneksi unik
    const key = ws.idtarget ? `user:${ws.idtarget}` : `conn:${ws._connId || "unknown"}`;
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
      limit = 50; // Higher limit for chat
    } else if (messageType === "updatePoint") {
      limit = 100; // Highest limit for drawing
    }
    
    // Perbaikan off-by-one: hitung sebelum increment
    if (stats.count + 1 > limit) {
      try { this.safeSend(ws, ['error', 'Rate limit exceeded']); } catch (e) {}
      return false;
    }
    stats.count++;
    
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
            await this.removeAllSeatsById(idtarget);
            
            for (const client of this.clients) {
              if (client.idtarget === idtarget && client.readyState === 1) {
                this.safeSend(client, ["needJoinRoom"]);
              }
            }
          }
          
          this.usersToRemove.delete(idtarget);
        } catch (error) {
          if (this.DEBUG) console.error("batchAutoRemove inner err", error);
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
      if (this.DEBUG) console.error("batchAutoRemove err", error);
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {
      if (this.DEBUG) console.error("safeSend err", e);
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    let sentCount = 0;
    try {
      const set = this.roomClients.get(room);
      if (!set) return 0;
      for (const c of set) {
        if (c.readyState === 1) {
          try {
            if (this.safeSend(c, msg)) {
              sentCount++;
            }
          } catch (error) {
            if (this.DEBUG) console.error("broadcastToRoom send err", error);
          }
        }
      }
    } catch (error) {
      if (this.DEBUG) console.error("broadcastToRoom err", error);
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
      if (this.DEBUG) console.error("broadcastRoomUserCount err", error);
    }
  }

  // FIXED: No batch limit untuk chat (tapi dengan buffer cap)
  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        try {
          // Process ALL messages namun buffer diawasi saat push (eviction)
          for (const msg of messages) {
            this.broadcastToRoom(room, msg);
          }
          this.chatMessageBuffer.set(room, []);
        } catch (error) {
          if (this.DEBUG) console.error("flushChatBuffer err", error);
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
        if (this.DEBUG) console.error("flushKursiUpdates err", error);
        this.updateKursiBuffer.set(room, new Map());
      }
    }
  }

  async tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      // Only broadcast to active clients in relevant rooms
      for (const c of this.clients) {
        if (c.readyState === 1 && c.roomname) {
          this.safeSend(c, ["currentNumber", this.currentNumber]);
        }
      }
    } catch (error) {
      if (this.DEBUG) console.error("tick err", error);
    }
  }

  cleanExpiredLocks() {
    try {
      const now = Date.now();
      let cleanedLocks = 0;
      const maxLocksToClean = 50; // sedikit dinaikkan untuk efisiensi
      
      // Hapus seatLocks yang melewati TTL
      for (const [k, ts] of this.seatLocks) {
        if (now - ts > this.LOCK_TTL_MS) this.seatLocks.delete(k);
      }

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
      if (this.DEBUG) console.error("cleanExpiredLocks err", error);
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
      const deliveredIds = [];
      let messagesDelivered = 0;
      const maxMessagesPerFlush = this.MAX_PRIVATE_MESSAGES_PER_FLUSH_TOTAL;
      
      for (const [id, msgs] of this.privateMessageBuffer) {
        if (messagesDelivered >= maxMessagesPerFlush) break;
        
        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === id && c.readyState === 1) {
            // Send up to MAX_PRIVATE_DELIVER_PER_FLUSH messages at once
            const batch = msgs.slice(0, this.MAX_PRIVATE_DELIVER_PER_FLUSH);
            for (const m of batch) {
              this.safeSend(c, m);
              messagesDelivered++;
              if (messagesDelivered >= maxMessagesPerFlush) break;
            }
            delivered = true;
            break;
          }
        }
        if (delivered) {
          deliveredIds.push(id);
        }
      }
      
      for (const id of deliveredIds) {
        this.privateMessageBuffer.delete(id);
      }
    } catch (error) {
      if (this.DEBUG) console.error("periodicFlush err", error);
    }
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    try {
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {
      if (this.DEBUG) console.error("handleGetAllRoomsUserCount err", error);
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
          // set seatLocks entry with timestamp
          this.seatLocks.set(`${room}-${i}`, Date.now());
          this.userToSeat.set(ws.idtarget, { room, seat: i });
          return i;
        }
      }
      return null;
    } catch (error) {
      if (this.DEBUG) console.error("lockSeat err", error);
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
      if (this.DEBUG) console.error("senderrorstate err", error);
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
        
        // Limit points to prevent overload - show recent points only
        if (info.points.length > 0) {
          const recentPoints = info.points.slice(-5); // Show last 5 points
          for (const point of recentPoints) {
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
      if (this.DEBUG) console.error("sendAllStateTo err", error);
    }
  }

  cleanupClientSafely(ws) {
    const id = ws.idtarget;
    if (!id) {
      // Remove from clients set
      try { this.clients.delete(ws); } catch (e) {}
      // Remove from roomClients index if present
      try {
        if (ws.roomname && this.roomClients.has(ws.roomname)) this.roomClients.get(ws.roomname).delete(ws);
      } catch (e) {}
      return;
    }

    if (this.cleanupInProgress.has(id)) return;
    
    this.cleanupInProgress.add(id);

    try {
      if (this.pingTimeouts.has(id)) {
        try { clearTimeout(this.pingTimeouts.get(id)); } catch (e) {}
        this.pingTimeouts.delete(id);
      }

      const activeConnections = Array.from(this.clients).filter(
        c => c.idtarget === id && c !== ws && c.readyState === 1
      );

      if (activeConnections.length === 0) {
        this.usersToRemove.set(id, Date.now());
      }

      // Remove ws from roomClients index
      try {
        if (ws.roomname && this.roomClients.has(ws.roomname)) {
          this.roomClients.get(ws.roomname).delete(ws);
        }
      } catch (e) {
        if (this.DEBUG) console.error("cleanupClientSafely remove roomClients err", e);
      }

      this.clients.delete(ws);
      this.privateMessageBuffer.delete(id);
      this.messageCounts.delete(`user:${id}`);
      
    } catch (error) {
      if (this.DEBUG) console.error("cleanupClientSafely err", error);
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
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }

      this.userToSeat.delete(idtarget);
      this.usersToRemove.delete(idtarget);
      // Remove any seatLocks for this seat
      try { this.seatLocks.delete(`${room}-${seat}`); } catch (e) {}
    } catch (error) {
      if (this.DEBUG) console.error("removeAllSeatsById err", error);
    }
  }

  getAllOnlineUsers() {
    const users = [];
    // Limit to 1000 users for performance
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

    this.usersToRemove.delete(id);
    if (this.pingTimeouts.has(id)) {
      try { clearTimeout(this.pingTimeouts.get(id)); } catch (e) {}
      this.pingTimeouts.delete(id);
    }

    if (baru === false) {
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        
        if (seatMap?.has(seat)) {
          const seatData = seatMap.get(seat);
          
          if (seatData.namauser === id) {
            ws.roomname = room;
            ws.numkursi = new Set([seat]);
            // Update roomClients index
            try { if (this.roomClients.has(room)) this.roomClients.get(room).add(ws); } catch (e) {}
            this.safeSend(ws, ["currentNumber", this.currentNumber]); 
            this.sendAllStateTo(ws, room);
            this.broadcastRoomUserCount(room);
          }
        }
      }
    }

    if (this.privateMessageBuffer.has(id)) {
      for (const msg of this.privateMessageBuffer.get(id)) {
        this.safeSend(ws, msg);
      }
      this.privateMessageBuffer.delete(id);
    }
  }

  scheduleCleanupTimeout(idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      try { clearTimeout(this.pingTimeouts.get(idtarget)); } catch (e) {}
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
        if (this.DEBUG) console.error("scheduleCleanupTimeout inner err", error);
      } finally {
        try { this.pingTimeouts.delete(idtarget); } catch (e) {}
        this.cleanupInProgress.delete(idtarget);
      }
    }, this.RECONNECT_TIMEOUT);
    
    this.pingTimeouts.set(idtarget, timeout);
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;
    
    try {
      // Immediate cleanup tanpa waiting
      this.usersToRemove.delete(idtarget);
      
      if (this.pingTimeouts.has(idtarget)) {
        try { clearTimeout(this.pingTimeouts.get(idtarget)); } catch (e) {}
        this.pingTimeouts.delete(idtarget);
      }
      
      this.cleanupInProgress.delete(idtarget);
      
      // Immediate seat removal - FORCE BROADCAST
      const seatInfo = this.userToSeat.get(idtarget);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        
        const seatMap = this.roomSeats.get(room);
        if (seatMap && seatMap.has(seat)) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
      
      // Immediate client removal
      try {
        if (ws.roomname && this.roomClients.has(ws.roomname)) this.roomClients.get(ws.roomname).delete(ws);
      } catch (e) {}
      this.clients.delete(ws);
      
      // Immediate buffer cleanup
      this.privateMessageBuffer.delete(idtarget);
      this.messageCounts.delete(`user:${idtarget}`);
      
      // Remove from userToSeat mapping
      this.userToSeat.delete(idtarget);
      
    } catch (error) {
      if (this.DEBUG) console.error("handleOnDestroy err", error);
    }
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;
    
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      if (this.DEBUG) console.error("handleMessage JSON parse err", e);
      return;
    }
    
    if (!Array.isArray(data) || data.length === 0) return;
    
    const evt = data[0];

    // Enhanced rate limiting dengan message type
    if (!this.checkRateLimit(ws, evt)) return;

    try {
      switch (evt) {
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
            try { clearTimeout(this.pingTimeouts.get(newId)); } catch (e) {}
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
            // Update roomClients index
            try { if (this.roomClients.has(ws.roomname)) this.roomClients.get(ws.roomname).add(ws); } catch (e) {}
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
            const buf = this.privateMessageBuffer.get(idtarget);
            if (buf.length >= this.MAX_PRIVATE_PER_USER) buf.shift(); // evict oldest
            buf.push(notif);
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
            const buf = this.privateMessageBuffer.get(idt);
            if (buf.length >= this.MAX_PRIVATE_PER_USER) buf.shift(); // evict oldest
            buf.push(out);
          }
          break;
        }

        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";

          const activeSockets = Array.from(this.clients)
            .filter(c => c.idtarget === username && c.readyState === 1);
          const online = activeSockets.length > 0;

          this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);

          if (activeSockets.length > 1) {
            const newest = activeSockets[activeSockets.length - 1];
            const oldSockets = activeSockets.slice(0, -1);

            const userSeatInfo = this.userToSeat.get(username);
            if (userSeatInfo) {
              const { room, seat } = userSeatInfo;
              const seatMap = this.roomSeats.get(room);
              if (seatMap && seatMap.has(seat)) {
                Object.assign(seatMap.get(seat), createEmptySeat());
                this.broadcastToRoom(room, ["removeKursi", room, seat]);
                this.broadcastRoomUserCount(room);
              }
              this.userToSeat.delete(username);
            }

            for (const old of oldSockets) {
              try { 
                if (old.readyState === 1) {
                  old.close(4000, "Duplicate login"); 
                }
                this.clients.delete(old); 
              } catch (e) {
                if (this.DEBUG) console.error("isUserOnline close old err", e);
              }
            }
          }
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
          if (!roomList.includes(newRoom)) return;
          
          if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);
          
          ws.roomname = newRoom;
          const foundSeat = this.lockSeat(newRoom, ws);
          
          if (foundSeat === null) return;
          
          ws.numkursi = new Set([foundSeat]);
          this.safeSend(ws, ["numberKursiSaya", foundSeat]);
          
          if (ws.idtarget) 
            this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
          this.safeSend(ws, ["currentNumber", this.currentNumber]); 
          this.sendAllStateTo(ws, newRoom);
          this.broadcastRoomUserCount(newRoom);

          // Update roomClients index
          try { if (this.roomClients.has(newRoom)) this.roomClients.get(newRoom).add(ws); } catch (e) {}
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return;
          if (!this.chatMessageBuffer.has(roomname)) 
            this.chatMessageBuffer.set(roomname, []);
          const buf = this.chatMessageBuffer.get(roomname);
          // enforce per-room chat buffer limit
          if (buf.length >= this.MAX_CHAT_BUFFER_PER_ROOM) buf.shift();
          buf.push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) return;
          // validate seat range
          if (typeof seat !== "number" || seat < 1 || seat > this.MAX_SEATS) return;
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;
          
          // FIXED: Accumulate points instead of overwriting
          si.points.push({ x, y, fast, timestamp: Date.now() });
          
          // Clean old points (keep last 3 seconds)
          const now = Date.now();
          si.points = si.points.filter(point => now - point.timestamp < 3000);
          
          si.lastActivity = Date.now();
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (!roomList.includes(room)) return;
          if (typeof seat !== "number" || seat < 1 || seat > this.MAX_SEATS) return;
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) return;
          if (typeof seat !== "number" || seat < 1 || seat > this.MAX_SEATS) return;
          
          // Simple lock mechanism untuk prevent race condition (timestamp-based)
          const lockKey = `${room}-${seat}`;
          const lockTs = this.seatLocks.get(lockKey);
          if (lockTs && (Date.now() - lockTs) < this.LOCK_TTL_MS) return;
          this.seatLocks.set(lockKey, Date.now());
          
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
            // avoid copying heavy points
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
          const buf = this.chatMessageBuffer.get(roomname);
          if (buf.length >= this.MAX_CHAT_BUFFER_PER_ROOM) buf.shift();
          buf.push(["gift", roomname, sender, receiver, giftName, Date.now()]);
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
      if (this.DEBUG) console.error("handleMessage switch err", error);
    }
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();

      const ws = server;

      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();
      // generate per-connection ID untuk rate-limiting fallback
      try {
        ws._connId = (typeof crypto !== "undefined" && crypto.randomUUID) ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
      } catch (e) {
        ws._connId = `${Date.now()}-${Math.random()}`;
      }

      this.clients.add(ws);

      // maintain roomClients index (will be added when joinRoom or setIdTarget)
      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          if (this.DEBUG) console.error("ws.message handler err", error);
          this.cleanupClientSafely(ws);
        }
      });
      
      ws.addEventListener("close", (event) => {
        const id = ws.idtarget;
        if (id) {
          this.scheduleCleanupTimeout(id);
        }
        // remove from roomClients if present
        try { if (ws.roomname && this.roomClients.has(ws.roomname)) this.roomClients.get(ws.roomname).delete(ws); } catch (e) {}
        this.cleanupClientSafely(ws);
      });

      ws.addEventListener("error", (error) => {
        const id = ws.idtarget;
        if (id) {
          this.scheduleCleanupTimeout(id);
        }
        try { if (ws.roomname && this.roomClients.has(ws.roomname)) this.roomClients.get(ws.roomname).delete(ws); } catch (e) {}
        this.cleanupClientSafely(ws);
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      if (this.DEBUG) console.error("fetch err", error);
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
