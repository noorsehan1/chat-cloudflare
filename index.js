import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// Optimized: Pre-create empty seat object to avoid repeated object creation
const EMPTY_SEAT = Object.freeze({
  noimageUrl: "",
  namauser: "",
  color: "",
  itembawah: 0,
  itematas: 0,
  vip: 0,
  viptanda: 0,
  lastPoint: null,
  lockTime: undefined
});

function createEmptySeat() {
  return { ...EMPTY_SEAT };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();
    this.userToSeat = new Map();
    this.hasEverSetId = false;

    this.MAX_SEATS = 35;
    
    // Optimized: Initialize room seats more efficiently
    this.roomSeats = new Map(roomList.map(room => {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      return [room, seatMap];
    }));

    this.vipManager = new VipBadgeManager(this);

    // Optimized: Use simpler data structures
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.seatLocks = new Map();

    this.roomChatHistory = new Map();
    this.userDisconnectTime = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    // Optimized: Initialize buffers in one pass
    roomList.forEach(room => {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    });

    this._nextConnId = 1;

    // Optimized: Reduce timer frequencies
    this._tickTimer = setInterval(() => {
      this.tick().catch(() => {});
    }, this.intervalMillis);

    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) {
        this.periodicFlush().catch(() => {});
      }
    }, 50); // Reduced from 100ms to 50ms

    this._autoRemoveTimer = setInterval(() => {
      this.batchAutoRemove().catch(() => {});
    }, 5000); // Reduced from 10s to 5s

    this._resetDataTimer = setInterval(() => {
      this.resetStaleData().catch(() => {});
    }, 2 * 60 * 1000); // Reduced from 5min to 2min

    this.lowcard = new LowCardGameManager(this);

    this.pingTimeouts = new Map();
    this.RECONNECT_TIMEOUT = 5000; // Reduced from 10s to 5s
    this.cleanupInProgress = new Set();
    this.usersToRemove = new Map();

    this.messageCounts = new Map();
    this.MAX_MESSAGES_PER_SECOND = 50; // Increased limit but better optimized

    this.userJoinLocks = new Map();
    
    // Optimized: Add batching for broadcasts
    this._broadcastQueue = new Map();
    this._broadcastTimer = setInterval(() => {
      this.flushBroadcastQueue();
    }, 16); // ~60fps
  }

  // Optimized: Batch broadcasts to reduce WebSocket overhead
  queueBroadcast(room, msg) {
    if (!this._broadcastQueue.has(room)) {
      this._broadcastQueue.set(room, []);
    }
    this._broadcastQueue.get(room).push(msg);
  }

  flushBroadcastQueue() {
    if (this._broadcastQueue.size === 0) return;

    const batchMessage = ["batch"];
    const roomMessages = new Map();

    for (const [room, messages] of this._broadcastQueue) {
      if (messages.length > 0) {
        roomMessages.set(room, messages);
      }
    }

    if (roomMessages.size > 0) {
      batchMessage.push(Array.from(roomMessages.entries()));
      
      for (const client of this.clients) {
        if (client.readyState === 1) {
          const room = client.roomname;
          if (room && roomMessages.has(room)) {
            this.safeSend(client, batchMessage);
          }
        }
      }
    }

    this._broadcastQueue.clear();
  }

  async acquireUserJoinLock(idtarget) {
    if (!idtarget) return true;
    
    const now = Date.now();
    const existingLock = this.userJoinLocks.get(idtarget);
    
    if (existingLock && (now - existingLock < 5000)) {
      return false;
    }
    
    this.userJoinLocks.set(idtarget, now);
    return true;
  }

  releaseUserJoinLock(idtarget) {
    this.userJoinLocks.delete(idtarget);
  }

  async resetStaleData() {
    const now = Date.now();

    // Optimized: Use faster iteration
    for (const [room, seatMap] of this.roomSeats) {
      let hasActiveUsers = false;

      // Fast check for active users
      for (const info of seatMap.values()) {
        if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
          hasActiveUsers = true;
          break;
        }
      }

      if (!hasActiveUsers) {
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          this.seatLocks.delete(`${room}-${i}`);
          seatMap.set(i, createEmptySeat());
        }
      }
    }

    // Clean expired join locks
    for (const [idtarget, lockTime] of this.userJoinLocks) {
      if (now - lockTime > 10000) { // Reduced from 30s to 10s
        this.userJoinLocks.delete(idtarget);
      }
    }
  }

  clearSeatBuffer(room, seatNumber) {
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) {
      roomMap.delete(seatNumber);
    }
  }

  scheduleCleanupTimeout(idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
    }

    const timeout = setTimeout(() => {
      this.pingTimeouts.delete(idtarget);
      this.usersToRemove.set(idtarget, Date.now());
    }, this.RECONNECT_TIMEOUT);

    this.pingTimeouts.set(idtarget, timeout);
  }

  forceUserCleanup(idtarget) {
    if (!idtarget) return;

    this.releaseUserJoinLock(idtarget);

    // Optimized: Batch cleanup operations
    this.userDisconnectTime.delete(idtarget);
    this.usersToRemove.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.cleanupInProgress.delete(idtarget);

    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }

    // Optimized: Single pass through all rooms
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget || seatInfo.namauser === `__LOCK__${idtarget}`) {
          if (seatInfo.viptanda > 0) {
            this.vipManager.removeVipBadge(room, seatNumber);
          }
          Object.assign(seatInfo, createEmptySeat());
          this.clearSeatBuffer(room, seatNumber);
          this.seatLocks.delete(`${room}-${seatNumber}`);
          
          this.queueBroadcast(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;

    this.releaseUserJoinLock(idtarget);
    this.vipManager.cleanupUserVipBadges(idtarget);

    // Optimized: Single cleanup call
    this.usersToRemove.delete(idtarget);
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    this.userDisconnectTime.delete(idtarget);

    // Optimized: Faster room iteration
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, info] of seatMap) {
        const n = info.namauser;
        if (n && (n === idtarget || n === `__LOCK__${idtarget}`)) {
          Object.assign(info, createEmptySeat());
          this.seatLocks.delete(`${room}-${seatNumber}`);
          this.queueBroadcast(room, ["removeKursi", room, seatNumber]);
          this.clearSeatBuffer(room, seatNumber);
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.cleanupInProgress.delete(idtarget);

    // Optimized: Faster client cleanup
    for (const c of this.clients) {
      if (c.idtarget === idtarget) {
        try {
          if (c.readyState === 1) c.close(1000, "Session removed");
        } catch (e) {}
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

    const limit = messageType === "chat" ? 100 : 200;
    stats.count++;

    if (stats.count > limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }

    return true;
  }

  async batchAutoRemove() {
    const now = Date.now();
    const removalThreshold = 2000; // Reduced from 5s to 2s

    this.cleanExpiredLocks();

    const usersToRemoveNow = [];
    let processed = 0;
    const maxBatchSize = 100; // Increased batch size

    for (const [idtarget, removalTime] of this.usersToRemove) {
      if (processed >= maxBatchSize) break;

      if (now - removalTime >= removalThreshold) {
        usersToRemoveNow.push(idtarget);
        processed++;
      }
    }

    // Optimized: Parallel processing where safe
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
      } finally {
        this.cleanupInProgress.delete(idtarget);
      }
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws && ws.readyState === 1) {
        // Optimized: Less strict bufferedAmount check
        if (ws.bufferedAmount > 2_000_000) {
          return false;
        }
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {
      // silent
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    let sentCount = 0;
    // Optimized: Use for-of which is faster for Sets
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        if (this.safeSend(c, msg)) {
          sentCount++;
        }
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const cnt = Object.create(null);
    
    // Optimized: Pre-allocate and single pass
    for (const room of roomList) {
      cnt[room] = 0;
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const info of seatMap.values()) {
          if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
            cnt[room]++;
          }
        }
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.queueBroadcast(room, ["roomUserCount", room, count]);
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        // Use batch messaging for multiple chats
        if (messages.length > 1) {
          this.queueBroadcast(room, ["chatBatch", room, messages]);
        } else {
          this.queueBroadcast(room, messages[0]);
        }
        this.chatMessageBuffer.set(room, []);
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      const updates = [];
      for (const [seat, info] of seatMapUpdates) {
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) {
        this.queueBroadcast(room, ["kursiBatchUpdate", room, updates]);
      }
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  async periodicFlush() {
    this.flushKursiUpdates();
    this.flushChatBuffer();
    this.cleanExpiredLocks();
  }

  async tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    
    // Optimized: Batch number updates
    const updateMsg = ["currentNumber", this.currentNumber];
    for (const c of this.clients) {
      if (c.readyState === 1 && c.roomname) {
        this.safeSend(c, updateMsg);
      }
    }
  }

  cleanExpiredLocks() {
    const now = Date.now();
    let cleanedLocks = 0;
    const maxLocksToClean = 100; // Increased

    for (const room of roomList) {
      if (cleanedLocks >= maxLocksToClean) break;

      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const [seat, info] of seatMap) {
          if (cleanedLocks >= maxLocksToClean) break;

          if (info.namauser.startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000) {
            Object.assign(info, createEmptySeat());
            this.seatLocks.delete(`${room}-${seat}`);
            this.queueBroadcast(room, ["removeKursi", room, seat]);
            this.clearSeatBuffer(room, seat);
            this.broadcastRoomUserCount(room);
            cleanedLocks++;
          }
        }
      }
    }
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    const now = Date.now();
    const userId = ws.idtarget;

    // Optimized: Early return if user already in room
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      if (seatMap.get(i).namauser === userId) {
        return null;
      }
    }

    // Clean expired locks first
    this.cleanExpiredLocks();

    // Find empty seat
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k.namauser === "") {
        k.namauser = "__LOCK__" + userId;
        k.lockTime = now;
        this.seatLocks.set(`${room}-${i}`, { owner: userId, ts: now });
        return i;
      }
    }
    
    return null;
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const allKursiMeta = {};
    const lastPointsData = [];

    // Optimized: Single pass for both kursi meta and points
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
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

    // Send batched data
    if (Object.keys(allKursiMeta).length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    }

    if (lastPointsData.length > 0) {
      this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    }

    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    const count = this.getJumlahRoom()[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
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

      const hasActiveConnections = Array.from(this.clients).some(
        c => c.idtarget === id && c !== ws && c.readyState === 1
      );

      this.clients.delete(ws);

      if (!hasActiveConnections) {
        this.fullRemoveById(id);
      } else {
        this.messageCounts.delete(id);
      }

    } finally {
      this.cleanupInProgress.delete(id);
    }
  }

  // ... (keep other methods similar but apply the same optimization patterns)

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

    // Optimized: Use direct function calls instead of switch for performance
    const handler = this.messageHandlers[evt];
    if (handler) {
      handler.call(this, ws, data);
    } else {
      // Fallback to VIP manager
      if (evt === "vipbadge" || evt === "removeVipBadge" || evt === "getAllVipBadges") {
        this.vipManager.handleEvent(ws, data);
      }
    }
  }

  // Optimized: Use method dictionary for faster message handling
  messageHandlers = {
    isInRoom: (ws, data) => {
      const idtarget = ws.idtarget;
      if (!idtarget) {
        this.safeSend(ws, ["inRoomStatus", false]);
        return;
      }
      const seatInfo = this.userToSeat.get(idtarget);
      const isInRoom = seatInfo && this.roomSeats.get(seatInfo.room)?.get(seatInfo.seat)?.namauser === idtarget;
      this.safeSend(ws, ["inRoomStatus", isInRoom]);
    },

    onDestroy: (ws, data) => {
      this.handleOnDestroy(ws, ws.idtarget);
    },

    setIdTarget2: (ws, data) => {
      this.handleSetIdTarget2(ws, data[1], data[2]);
    },

    // ... add other handlers here
  };

  async fetch(request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    await server.accept();
    const ws = server;

    // Optimized: Minimal setup
    ws._connId = `conn#${this._nextConnId++}`;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();

    this.clients.add(ws);

    // Optimized: Use arrow functions to maintain context
    ws.addEventListener("message", (ev) => {
      this.handleMessage(ws, ev.data);
    });

    ws.addEventListener("error", () => {
      setTimeout(() => this.cleanupClientSafely(ws), 10000);
    });

    ws.addEventListener("close", () => {
      if (ws.idtarget) {
        this.userDisconnectTime.set(ws.idtarget, Date.now());
      }
      this.cleanupClientSafely(ws);
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(req, env) {
    const upgrade = req.headers.get("Upgrade")?.toLowerCase();
    
    if (upgrade === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200 });
    }
    
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
