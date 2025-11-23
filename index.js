import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

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
    this.roomSeats = new Map();

    // Initialize all rooms with empty seats
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    this.vipManager = new VipBadgeManager(this);

    // Buffers for optimization
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.seatLocks = new Map();

    this.roomChatHistory = new Map();
    this.userDisconnectTime = new Map();

    // Game state
    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    // Initialize buffers for each room
    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }

    this._nextConnId = 1;

    // Essential timers only
    this._tickTimer = setInterval(() => {
      this.tick().catch(() => {});
    }, this.intervalMillis);

    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) {
        this.periodicFlush().catch(() => {});
      }
    }, 50); // Fast flush for real-time performance

    this.lowcard = new LowCardGameManager(this);

    // User management
    this.pingTimeouts = new Map();
    this.RECONNECT_TIMEOUT = 5000;
    this.cleanupInProgress = new Set();
    this.usersToRemove = new Map();

    // Rate limiting
    this.messageCounts = new Map();
    this.MAX_MESSAGES_PER_SECOND = 30;

    // Concurrency control
    this.userJoinLocks = new Map();
  }

  // COMPREHENSIVE USER DESTROY - CLEAN ALL USER DATA
  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;

    console.log(`[DESTROY] Comprehensive cleanup for: ${idtarget}`);
    
    // 1. RELEASE ALL LOCKS
    this.releaseUserJoinLock(idtarget);
    
    // 2. CLEANUP VIP BADGES
    this.vipManager.cleanupUserVipBadges(idtarget);
    
    // 3. CLEAR ALL TIMEOUTS
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    
    // 4. REMOVE FROM ALL TRACKING MAPS
    this.usersToRemove.delete(idtarget);
    this.userDisconnectTime.delete(idtarget);
    this.messageCounts.delete(idtarget);
    this.cleanupInProgress.delete(idtarget);
    
    // 5. REMOVE USER-TO-SEAT MAPPING
    this.userToSeat.delete(idtarget);
    
    // 6. CLEAN ALL SEATS IN ALL ROOMS
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget || seatInfo.namauser === `__LOCK__${idtarget}`) {
          // Remove VIP badge if exists
          if (seatInfo.viptanda > 0) {
            this.vipManager.removeVipBadge(room, seatNumber);
          }
          
          // Reset seat to empty
          Object.assign(seatInfo, createEmptySeat());
          
          // Remove seat locks
          this.seatLocks.delete(`${room}-${seatNumber}`);
          
          // Clear buffer
          this.clearSeatBuffer(room, seatNumber);
          
          // Broadcast to room
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      
      // Update user count
      this.broadcastRoomUserCount(room);
    }
    
    // 7. REMOVE FROM CLIENT SET
    this.clients.delete(ws);
    
    // 8. CLOSE WEBSOCKET IF STILL OPEN
    try {
      if (ws.readyState === 1) {
        ws.close(1000, "User destroyed");
      }
    } catch (e) {
      // silent
    }
    
    console.log(`[DESTROY] Complete cleanup done for: ${idtarget}`);
  }

  async acquireUserJoinLock(idtarget) {
    if (!idtarget) return true;
    
    if (this.userJoinLocks.has(idtarget)) {
      return false;
    }
    
    this.userJoinLocks.set(idtarget, Date.now());
    return true;
  }

  releaseUserJoinLock(idtarget) {
    if (!idtarget) return;
    this.userJoinLocks.delete(idtarget);
  }

  clearSeatBuffer(room, seatNumber) {
    try {
      if (!room || typeof seatNumber !== "number") return;
      const roomMap = this.updateKursiBuffer.get(room);
      if (roomMap) {
        roomMap.delete(seatNumber);
      }
    } catch (e) {
      // silent
    }
  }

  safeSend(ws, arr) {
    try {
      if (ws && ws.readyState === 1) {
        if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 1_000_000) {
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
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        try {
          if (this.safeSend(c, msg)) {
            sentCount++;
          }
        } catch (error) {
          // ignore individual client errors
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
      // silent
    }
  }

  // SIMPLE RECONNECT SYNC METHOD
  async handleSetIdTarget2(ws, id, baru) {
    if (!id) return;

    const lockAcquired = await this.acquireUserJoinLock(id);
    if (!lockAcquired) {
      this.safeSend(ws, ["error", "Another operation in progress"]);
      return;
    }

    try {
      // Cleanup existing user completely
      this.handleOnDestroy(ws, id);

      ws.idtarget = id;

      if (baru === true) {
        // New user - fresh start
        this.userDisconnectTime.delete(id);
        ws.roomname = undefined;
        ws.numkursi = new Set();
        this.safeSend(ws, ["needJoinRoom"]);
      } 
      else if (baru === false) {
        // Reconnecting user - try to restore position
        const seatInfo = this.userToSeat.get(id);

        if (seatInfo) {
          const { room, seat } = seatInfo;
          const seatMap = this.roomSeats.get(room);

          if (seatMap?.has(seat)) {
            const seatData = seatMap.get(seat);
            
            if (seatData.namauser === id) {
              // Successful reconnect - restore position
              ws.roomname = room;
              ws.numkursi = new Set([seat]);
              this.sendAllStateTo(ws, room);
              this.broadcastRoomUserCount(room);
            } else {
              // Seat taken by someone else
              this.userToSeat.delete(id);
              this.safeSend(ws, ["needJoinRoom"]);
            }
          } else {
            // Seat doesn't exist
            this.userToSeat.delete(id);
            this.safeSend(ws, ["needJoinRoom"]);
          }
        } else {
          // No previous seat found
          this.safeSend(ws, ["needJoinRoom"]);
        }
      }
    } finally {
      this.releaseUserJoinLock(id);
    }
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;

      // Send current game number
      this.safeSend(ws, ["currentNumber", this.currentNumber]);

      // Send room user count
      const count = this.getJumlahRoom()[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);

      // Send all seats data
      const allKursiMeta = {};
      const lastPointsData = [];

      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;

        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
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

    } catch (error) {
      // silent
    }
  }

  // JOIN ROOM HANDLER
  async handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) {
      return false;
    }

    if (!ws.idtarget) {
      this.safeSend(ws, ["error", "No user ID set"]);
      return false;
    }

    const lockAcquired = await this.acquireUserJoinLock(ws.idtarget);
    if (!lockAcquired) {
      this.safeSend(ws, ["error", "Another join operation in progress"]);
      return false;
    }

    try {
      // Leave previous room if any
      if (ws.roomname && ws.roomname !== newRoom) {
        this.handleOnDestroy(ws, ws.idtarget);
      }

      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      if (existingSeatInfo && existingSeatInfo.room === newRoom) {
        this.safeSend(ws, ["error", "Already in this room"]);
        return false;
      }

      ws.roomname = newRoom;
      
      // Find available seat
      const foundSeat = this.findAvailableSeat(newRoom, ws);
      if (foundSeat === null) {
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
      this.vipManager.getAllVipBadges(ws, newRoom);
      this.broadcastRoomUserCount(newRoom);

      return true;
    } finally {
      this.releaseUserJoinLock(ws.idtarget);
    }
  }

  findAvailableSeat(room, ws) {
    if (!ws.idtarget) return null;

    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return null;

      const now = Date.now();
      const userId = ws.idtarget;

      // Check if user already has a seat
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const seatInfo = seatMap.get(i);
        if (seatInfo && seatInfo.namauser === userId) {
          return null;
        }
      }

      // Find first available seat
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const seatInfo = seatMap.get(i);
        if (seatInfo && seatInfo.namauser === "") {
          seatInfo.namauser = userId;
          return i;
        }
      }

      return null;
    } catch (error) {
      return null;
    }
  }

  // BUFFER FLUSHING METHODS
  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        try {
          for (let i = 0, len = messages.length; i < len; i++) {
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
        for (const [seat, info] of seatMapUpdates.entries()) {
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

  async periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
    } catch (error) {
      // silent
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
      // silent
    }
  }

  // RATE LIMITING
  checkRateLimit(ws, messageType) {
    const now = Date.now();
    const key = ws.idtarget || ws._connId || ws._id || 'anonymous';
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

    stats.count += 1;
    if (stats.count > limit) {
      this.safeSend(ws, ['error', 'Rate limit exceeded']);
      return false;
    }

    return true;
  }

  // MESSAGE HANDLER
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
          
          if (ws.idtarget && ws.idtarget !== newId) {
            this.handleOnDestroy(ws, ws.idtarget);
          }

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

        case "getAllRoomsUserCount": {
          const allCounts = this.getJumlahRoom();
          const result = roomList.map(room => [room, allCounts[room]]);
          this.safeSend(ws, ["allRoomsUserCount", result]);
          break;
        }

        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;

        case "getOnlineUsers": {
          const users = [];
          for (const c of this.clients) {
            if (c.idtarget && c.readyState === 1) {
              users.push(c.idtarget);
              if (users.length >= 1000) break;
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }

        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) return;
          const users = [];
          for (const c of this.clients) {
            if (c.roomname === roomName && c.idtarget && c.readyState === 1) {
              users.push(c.idtarget);
              if (users.length >= 500) break;
            }
          }
          this.safeSend(ws, ["roomOnlineUsers", roomName, users]);
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

          // Use buffer for chat
          if (!this.chatMessageBuffer.has(roomname))
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);

          // Store in history
          if (!this.roomChatHistory.has(roomname)) {
            this.roomChatHistory.set(roomname, []);
          }
          const history = this.roomChatHistory.get(roomname);

          history.push({
            timestamp: Date.now(),
            noImageURL,
            username,
            message,
            usernameColor,
            chatTextColor
          });

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
          this.seatLocks.delete(`${room}-${seat}`);

          this.clearSeatBuffer(room, seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) return;

          const seatMap = this.roomSeats.get(room);
          const currentInfo = seatMap.get(seat) || createEmptySeat();

          try {
            Object.assign(currentInfo, {
              noimageUrl, namauser, color, itembawah, itematas,
              vip: vip || 0,
              viptanda: viptanda || 0
            });

            seatMap.set(seat, currentInfo);
            // Use buffer for seat updates
            if (!this.updateKursiBuffer.has(room))
              this.updateKursiBuffer.set(room, new Map());
            this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
            this.broadcastRoomUserCount(room);
          } catch (error) {
            // silent
          }
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          // Use buffer for gifts
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
          setTimeout(() => this.lowcard.handleEvent(ws, data), 0);
          break;
        }
      }
    } catch (error) {
      console.error('[HANDLE MESSAGE] Error:', error);
    }
  }

  // WEBSOCKET SERVER
  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      await server.accept();

      const ws = server;
      ws._connId = `conn#${this._nextConnId++}`;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          this.handleOnDestroy(ws, ws.idtarget);
        }
      });

      ws.addEventListener("error", (event) => {
        this.handleOnDestroy(ws, ws.idtarget);
      });

      ws.addEventListener("close", (event) => {
        this.handleOnDestroy(ws, ws.idtarget);
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
