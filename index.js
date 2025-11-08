import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
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
    lockTime: undefined
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // core client state
    this.clients = new Set();
    this.userToSeat = new Map();

    // seats and rooms
    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    // rooms map to keep active connections per room
    // { roomName => { clients: [ws,...] } }
    this.rooms = new Map();

    // buffers and other state
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    // counters / timers
    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000; // tick interval example
    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), 100);

    // game manager
    this.lowcard = new LowCardGameManager(this);

    // reconnect system
    this.reconnectSessions = new Map();
    this.reconnectTimeouts = new Map();
    this.lastActivity = new Map();
    this.pingTimeouts = new Map();

    // timing config (tweak)
    this.RECONNECT_TIMEOUT_MS = 30 * 1000; // 30s reconnect window
    this.PING_TIMEOUT_MS = 25 * 1000;
    this.HEARTBEAT_INTERVAL = 10 * 1000;
    this.INACTIVE_TIMEOUT_MS = 60 * 1000;
    this.INITIAL_CONNECTION_GRACE_MS = 5000;

    // periodic reconnect cleanup to avoid buildup
    this._reconnectCleanupInterval = setInterval(() => this.cleanupExpiredReconnectSessions(), 60 * 1000);

    // background tasks: heartbeat pings, zombie cleanup
    this.startBackgroundTasks();
  }

  // ---------------------------
  // Background tasks
  // ---------------------------
  startBackgroundTasks() {
    // Ping loop (keeps connection liveness)
    this.heartbeatInterval = setInterval(() => {
      const now = Date.now();
      for (const ws of Array.from(this.clients)) {
        try {
          // only ping established connections with idtarget
          if (!ws || ws.readyState !== 1) continue;
          if (!ws.idtarget) continue;
          // Send ping message and set ping timeout
          this.sendPingToClient(ws);
          ws._lastPingSent = now;
        } catch (err) {
          console.warn("Failed to send ping:", err);
        }
      }
    }, Math.max(5000, this.HEARTBEAT_INTERVAL)); // at least heartbeat interval

    // Zombie/reconnect cleanup loop
    this.sessionCleanupInterval = setInterval(() => {
      const now = Date.now();

      // expire reconnectSessions older than RECONNECT_TIMEOUT_MS
      for (const [id, session] of Array.from(this.reconnectSessions.entries())) {
        if (now - session.timestamp > this.RECONNECT_TIMEOUT_MS) {
          console.log(`🧹 Reconnect session auto-expire: ${id}`);
          this.cleanupReconnectSession(id);
        }
      }

      // Close clients that didn't respond to ping (no pong/ping timeout)
      for (const ws of Array.from(this.clients)) {
        try {
          if (!ws || ws.readyState !== 1) continue;
          if (!ws.idtarget) continue;
          const lastAct = this.lastActivity.get(ws.idtarget) || 0;
          // if no activity for INACTIVE_TIMEOUT_MS, force offline
          if (now - lastAct > this.INACTIVE_TIMEOUT_MS) {
            console.log(`💀 Inactive for too long -> force offline: ${ws.idtarget}`);
            this.cleanupClient(ws, "Inactive timeout (background)");
            continue;
          }
        } catch (err) {
          console.warn("Error during sessionCleanupInterval:", err);
        }
      }
    }, 10000);
  }

  // ---------------------------
  // Safe send helper
  // ---------------------------
  safeSend(ws, data) {
    try {
      if (ws && ws.readyState === 1) {
        ws.send(JSON.stringify(data));
        if (ws.idtarget) this.lastActivity.set(ws.idtarget, Date.now());
        return true;
      }
    } catch (e) {
      console.error("safeSend error:", e);
    }
    return false;
  }

  // convenience broadcast using rooms map
  broadcast(roomname, data) {
    const room = this.rooms.get(roomname);
    if (!room || !room.clients) return;
    for (const c of room.clients) {
      this.safeSend(c, data);
    }
  }

  // ---------------------------
  // Ping / heartbeat utils
  // ---------------------------
  sendPingToClient(ws) {
    if (ws && ws.idtarget && ws.readyState === 1) {
      const sent = this.safeSend(ws, ["ping", Date.now()]);
      if (sent) this.setPingTimeout(ws.idtarget);
      return sent;
    }
    return false;
  }

  setPingTimeout(userId) {
    if (this.pingTimeouts.has(userId)) {
      clearTimeout(this.pingTimeouts.get(userId));
    }
    const timeoutId = setTimeout(() => {
      console.log(`Ping timeout for user: ${userId}`);
      const userWs = Array.from(this.clients).find(c => c.idtarget === userId);
      if (userWs) this.handlePingTimeout(userWs);
      this.pingTimeouts.delete(userId);
    }, this.PING_TIMEOUT_MS);
    this.pingTimeouts.set(userId, timeoutId);
  }

  handlePingTimeout(ws) {
    if (!ws || !ws.idtarget) return;
    console.log(`Closing connection due to ping timeout: ${ws.idtarget}`);
    try { ws.close(4000, "Ping timeout"); } catch (e) {}
    this.cleanupClient(ws, "Ping timeout");
  }

  heartbeat() {
    const now = Date.now();
    for (const ws of Array.from(this.clients)) {
      if (ws.readyState !== 1 || !ws.idtarget) continue;
      const lastActive = this.lastActivity.get(ws.idtarget) || now;
      const timeSinceLastActivity = now - lastActive;
      const connectionAge = now - (ws.connectionTime || now);
      const gracePeriod = connectionAge < this.INITIAL_CONNECTION_GRACE_MS;
      if (!gracePeriod && timeSinceLastActivity >= this.HEARTBEAT_INTERVAL) {
        this.sendPingToClient(ws);
      }
    }
  }

  // ---------------------------
  // Broadcast helpers
  // ---------------------------
  broadcastToRoom(room, msg) {
    // backward-compatible broadcast that scans clients (used in many places)
    for (const c of Array.from(this.clients)) {
      if (c.roomname === room) this.safeSend(c, msg);
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const info of seatMap.values()) {
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcast(room, ["roomUserCount", room, count]);
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  // ---------------------------
  // Buffers flush & housekeeping
  // ---------------------------
  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      for (const msg of messages) {
        this.broadcast(room, msg);
        this.broadcastToRoom(room, msg);
      }
      messages.length = 0;
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      const updates = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatMapUpdates.has(seat)) continue;
        const info = seatMapUpdates.get(seat);
        const { points, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) {
        this.broadcast(room, ["kursiBatchUpdate", room, updates]);
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      seatMapUpdates.clear();
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of Array.from(this.clients)) this.safeSend(c, ["currentNumber", this.currentNumber]);
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          this.broadcast(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.cleanExpiredLocks();

      // Heartbeat and cleanup throttling to reduce load
      if (Date.now() % 3000 < 100) this.heartbeat();
      if (Date.now() % 5000 < 100) {
        this.checkInactiveUsers();
        this.cleanupExpiredReconnectSessions();
      }

      // deliver private buffers
      for (const [id, msgs] of Array.from(this.privateMessageBuffer)) {
        const targetClient = Array.from(this.clients).find(c => c.idtarget === id);
        if (targetClient && targetClient.readyState === 1) {
          for (const m of msgs) this.safeSend(targetClient, m);
          this.privateMessageBuffer.delete(id);
          if (targetClient.roomname) this.broadcastRoomUserCount(targetClient.roomname);
        }
      }
    } catch (error) {
      console.error("Error in periodicFlush:", error);
    }
  }

  checkInactiveUsers() {
    const now = Date.now();
    const toRemove = [];
    for (const [id, lastActive] of this.lastActivity.entries()) {
      if (now - lastActive >= this.INACTIVE_TIMEOUT_MS) toRemove.push(id);
    }
    for (const id of toRemove) {
      console.log(`Removing inactive user: ${id}`);
      this.forceUserOffline(id);
    }
  }

  // ---------------------------
  // Reconnect helpers
  // ---------------------------
  saveReconnectSession(userId, sessionData) {
    this.reconnectSessions.set(userId, { ...sessionData, timestamp: Date.now() });
    if (this.reconnectTimeouts.has(userId)) {
      clearTimeout(this.reconnectTimeouts.get(userId));
    }
    const timeoutId = setTimeout(() => {
      console.log(`Reconnect session expired for: ${userId}`);
      this.cleanupReconnectSession(userId);
    }, this.RECONNECT_TIMEOUT_MS);
    this.reconnectTimeouts.set(userId, timeoutId);
  }

  cleanupExpiredReconnectSessions() {
    const now = Date.now();
    const toRemove = [];
    for (const [userId, session] of this.reconnectSessions.entries()) {
      if (now - session.timestamp >= this.RECONNECT_TIMEOUT_MS) toRemove.push(userId);
    }
    for (const userId of toRemove) this.cleanupReconnectSession(userId);
  }

  cleanupReconnectSession(userId) {
    console.log(`Cleaning up reconnect session for: ${userId}`);
    this.removeAllSeatsById(userId);
    this.reconnectSessions.delete(userId);
    if (this.reconnectTimeouts.has(userId)) {
      clearTimeout(this.reconnectTimeouts.get(userId));
      this.reconnectTimeouts.delete(userId);
    }
    this.lastActivity.delete(userId);
    if (this.pingTimeouts.has(userId)) {
      clearTimeout(this.pingTimeouts.get(userId));
      this.pingTimeouts.delete(userId);
    }
    this.userToSeat.delete(userId);
  }

  forceUserOffline(userId) {
    const userWs = Array.from(this.clients).find(c => c.idtarget === userId);
    if (userWs) {
      this.cleanupClient(userWs, "Inactive timeout");
    } else {
      this.cleanupReconnectSession(userId);
    }
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  // ---------------------------
  // Seat & room helpers
  // ---------------------------
  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;
    const now = Date.now();

    for (const [seat, info] of seatMap) {
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000) {
        Object.assign(info, createEmptySeat());
      }
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (!k) continue;
      if (k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = now;
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;
    const allPoints = [];
    const meta = {};
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      for (const p of info.points) allPoints.push({ seat, ...p });
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
  }

  cleanupClientById(idtarget) {
    for (const c of Array.from(this.clients)) {
      if (c.idtarget === idtarget) {
        try { c.close(4000, "Duplicate connection cleanup"); } catch {}
        this.cleanupClient(c, "Duplicate connection");
      }
    }
  }

  removeAllSeatsById(idtarget) {
    let removedAny = false;
    for (const [room, seatMap] of this.roomSeats) {
      let removedInRoom = false;
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcast(room, ["removeKursi", room, seat]);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          removedInRoom = true;
          removedAny = true;
        }
      }
      if (removedInRoom) this.broadcastRoomUserCount(room);
    }
    this.userToSeat.delete(idtarget);
    return removedAny;
  }

  getAllOnlineUsers() {
    const users = [];
    for (const ws of this.clients) if (ws.idtarget) users.push(ws.idtarget);
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    for (const ws of this.clients) if (ws.roomname === roomName && ws.idtarget) users.push(ws.idtarget);
    return users;
  }

  // ---------------------------
  // Cleanup client (central)
  // ---------------------------
  cleanupClient(ws, reason = "unknown") {
    try {
      if (!ws) return;
      // If ws has no idtarget yet just remove it
      if (!ws.idtarget) {
        this.clients.delete(ws);
        return;
      }
      const id = ws.idtarget;
      console.log(`Cleaning up client ${id} — reason: ${reason}`);

      // remove trackers
      this.lastActivity.delete(id);
      this.clients.delete(ws);

      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      // remove ws from rooms map if present
      if (ws.roomname && this.rooms.has(ws.roomname)) {
        const room = this.rooms.get(ws.roomname);
        room.clients = room.clients ? room.clients.filter(c => c !== ws) : [];
        if (!room.clients.length) {
          this.rooms.delete(ws.roomname);
          console.log(`Room ${ws.roomname} deleted (empty)`);
        }
      }

      // reconnect logic:
      // - if normal close (1000) -> remove reconnect session
      // - otherwise save reconnect session if ws was in a room
      if (ws._lastCloseCode === 1000) {
        // normal close -> delete saved session
        this.reconnectSessions.delete(id);
        if (this.reconnectTimeouts.has(id)) {
          clearTimeout(this.reconnectTimeouts.get(id));
          this.reconnectTimeouts.delete(id);
        }
        console.log(`Normal close — reconnect session removed for ${id}`);
      } else {
        // abnormal close -> save reconnect session only if user was in a room
        if (!this.reconnectSessions.has(id) && ws.roomname) {
          const sessionData = {
            roomname: ws.roomname,
            seats: ws.numkursi ? Array.from(ws.numkursi) : [],
            userToSeat: this.userToSeat.get(id)
          };
          this.saveReconnectSession(id, sessionData);
          console.log(`Saved reconnect session for: ${id}`);
        }
      }

      // clean userToSeat mapping
      this.userToSeat.delete(id);
    } catch (e) {
      console.error("Error during cleanupClient:", e);
    }
  }

  cleanupondestroy(ws) {
    if (!ws) return;
    const id = ws.idtarget;
    if (id) {
      console.log(`Permanent cleanup for: ${id}`);
      this.cleanupReconnectSession(id);
      this.removeAllSeatsById(id);
    }
    if (ws.numkursi) ws.numkursi.clear();
    const previousRoom = ws.roomname;
    this.clients.delete(ws);
    ws.roomname = undefined;
    ws.idtarget = undefined;
    try { if (ws.readyState === 1) ws.close(1000, "Cleanup on destroy"); } catch (e) {}
    if (previousRoom && roomList.includes(previousRoom)) this.broadcastRoomUserCount(previousRoom);
  }

  // ---------------------------
  // Request handlers
  // ---------------------------
  handleGetRoomUserCount(ws, roomName) {
    const count = this.getJumlahRoom()[roomName] || 0;
    this.safeSend(ws, ["roomUserCount", roomName, count]);
  }

  handleRemoveUserById(ws, targetId, reason = "Removed by system") {
    this.removeAllSeatsById(targetId);
    for (const client of this.clients) {
      if (client.idtarget === targetId) {
        this.safeSend(client, ["forceDisconnect", reason]);
        this.cleanupondestroy(client);
      }
    }
    this.cleanupReconnectSession(targetId);
    this.safeSend(ws, ["removeUserResult", targetId, "success", reason]);
  }

  handleNewConnection(ws) {
    console.log("New WebSocket connection established");
    // do not auto-send needJoinRoom; client must setIdTarget & joinRoom manually
  }

  // ---------------------------
  // handleSetIdTarget - improved & safe
  // ---------------------------
  handleSetIdTarget(ws, data) {
    const newId = data[1];
    if (!newId || typeof newId !== "string") return this.safeSend(ws, ["error", "Invalid user ID"]);

    console.log(`Setting ID target for new connection: ${newId}`);

    // close duplicate existing connections
    const existingConnections = Array.from(this.clients).filter(c =>
      c.idtarget === newId && c !== ws && c.readyState === 1
    );
    for (const oldWs of existingConnections) {
      try {
        console.log(`Closing duplicate connection for: ${newId}`);
        this.safeSend(oldWs, ["forceDisconnect", "New login detected"]);
        oldWs.close(4000, "Duplicate login");
        this.cleanupClient(oldWs, "Duplicate connection closed");
      } catch (e) {
        console.error("Error closing duplicate connection:", e);
      }
    }

    // set id, activity
    ws.idtarget = newId;
    ws.connectionTime = Date.now();
    this.lastActivity.set(newId, Date.now());

    // reset ping timeout
    if (this.pingTimeouts.has(newId)) {
      clearTimeout(this.pingTimeouts.get(newId));
      this.pingTimeouts.delete(newId);
    }

    // send buffered messages
    if (this.privateMessageBuffer.has(newId)) {
      const bufferedMessages = this.privateMessageBuffer.get(newId);
      console.log(`Sending ${bufferedMessages.length} buffered messages to: ${newId}`);
      for (const msg of bufferedMessages) this.safeSend(ws, msg);
      this.privateMessageBuffer.delete(newId);
    }

    // reconnect logic
    const reconnectSession = this.reconnectSessions.get(newId);
    if (reconnectSession) {
      console.log(`Processing reconnect session for: ${newId}`);
      const now = Date.now();
      const sessionAge = now - reconnectSession.timestamp;
      if (sessionAge < this.RECONNECT_TIMEOUT_MS) {
        this.restoreReconnectSession(ws, newId, reconnectSession);
      } else {
        console.log(`Reconnect session expired for: ${newId}`);
        this.cleanupReconnectSession(newId);
        this.safeSend(ws, ["sessionExpired"]);
      }
    } else {
      console.log(`New user ${newId} connected — waiting for manual joinRoom`);
      this.safeSend(ws, ["setIdTargetOK", newId]);
    }
  }

  // ---------------------------
  // Restore reconnect session (full)
  // ---------------------------
  restoreReconnectSession(ws, userId, session) {
    const { roomname, seats, userToSeat } = session;
    console.log(`Restoring reconnect session for: ${userId} in room: ${roomname}`);

    // Clear any scheduled timeout for this session
    if (this.reconnectTimeouts.has(userId)) {
      clearTimeout(this.reconnectTimeouts.get(userId));
      this.reconnectTimeouts.delete(userId);
    }
    this.reconnectSessions.delete(userId);

    // Validate room
    if (!roomList.includes(roomname)) {
      console.log(`Invalid room in reconnect session: ${roomname}`);
      return this.safeSend(ws, ["needJoinRoom", "Room no longer available"]);
    }

    // Ensure rooms map and add ws to active room clients
    if (!this.rooms.has(roomname)) this.rooms.set(roomname, { clients: [] });
    const room = this.rooms.get(roomname);
    if (!room.clients.includes(ws)) room.clients.push(ws);

    // Assign connection properties
    ws.roomname = roomname;
    ws.numkursi = new Set(seats || []);
    if (userToSeat) this.userToSeat.set(userId, userToSeat);

    // Restore seat state if locked for this user or empty
    const seatMap = this.roomSeats.get(roomname);
    let seatsRestored = 0;
    if (seatMap && seats && seats.length) {
      for (const seat of seats) {
        const info = seatMap.get(seat);
        if (info && (String(info.namauser).startsWith("__LOCK__") || info.namauser === userId)) {
          info.namauser = userId;
          info.lockTime = undefined;
          seatsRestored++;
        }
      }
    }

    console.log(`Restored ${seatsRestored} seats for: ${userId}`);

    // Send current state and notify room
    this.sendAllStateTo(ws, roomname);
    this.broadcast(roomname, ["userRejoined", userId]);
    this.broadcastToRoom(roomname, ["userRejoined", userId]);
    this.broadcastRoomUserCount(roomname);

    // Confirm to client, and give active notice
    this.safeSend(ws, ["reconnectSuccess", roomname]);
    this.safeSend(ws, ["roomRestored", roomname, { seats }]);
    this.safeSend(ws, ["system", `You are now active again in room ${roomname}`]);

    // Force flush for the room
    this.forceFlushRoomBuffers(roomname);

    console.log(`✅ ${userId} fully reconnected to ${roomname}`);
  }

  forceFlushRoomBuffers(roomName) {
    if (this.chatMessageBuffer.has(roomName)) {
      const messages = this.chatMessageBuffer.get(roomName);
      for (const msg of messages) {
        this.broadcast(roomName, msg);
        this.broadcastToRoom(roomName, msg);
      }
      messages.length = 0;
    }
    if (this.updateKursiBuffer.has(roomName)) {
      const seatUpdates = this.updateKursiBuffer.get(roomName);
      const updates = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        if (!seatUpdates.has(seat)) continue;
        const info = seatUpdates.get(seat);
        const { points, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) {
        this.broadcast(roomName, ["kursiBatchUpdate", roomName, updates]);
        this.broadcastToRoom(roomName, ["kursiBatchUpdate", roomName, updates]);
      }
      seatUpdates.clear();
    }
  }

  // ---------------------------
  // Message router
  // ---------------------------
  handleMessage(ws, raw) {
    if (ws.idtarget) this.lastActivity.set(ws.idtarget, Date.now());
    let data;
    try { data = JSON.parse(raw); } catch { return this.safeSend(ws, ["error", "Invalid JSON"]); }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    switch (evt) {
      case "onDestroy":
        this.cleanupondestroy(ws);
        break;

      case "getRoomUserCount": {
        const roomName = data[1];
        if (!roomList.includes(roomName)) return this.safeSend(ws, ["error", "Unknown room"]);
        this.handleGetRoomUserCount(ws, roomName);
        break;
      }

      case "removeUserById": {
        const targetId = data[1];
        const reason = data[2] || "Removed by admin";
        this.handleRemoveUserById(ws, targetId, reason);
        break;
      }

      case "getOnlineUsers":
        this.safeSend(ws, ["onlineUsersList", this.getAllOnlineUsers()]);
        break;

      case "setIdTarget":
        this.handleSetIdTarget(ws, data);
        break;

      case "pong": {
        const pingTime = data[1];
        if (ws.idtarget) {
          this.lastActivity.set(ws.idtarget, Date.now());
          if (this.pingTimeouts.has(ws.idtarget)) {
            clearTimeout(this.pingTimeouts.get(ws.idtarget));
            this.pingTimeouts.delete(ws.idtarget);
          }
          const latency = Date.now() - pingTime;
          this.safeSend(ws, ["pong", latency]);
        }
        break;
      }

      case "ping": {
        const pingTime = data[1];
        if (ws.idtarget) this.lastActivity.set(ws.idtarget, Date.now());
        this.safeSend(ws, ["pong", pingTime]);
        break;
      }

      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        let delivered = false;
        for (const c of this.clients) if (c.idtarget === idtarget) { this.safeSend(c, notif); delivered = true; }
        if (!delivered) {
          if (!this.privateMessageBuffer.has(idtarget)) this.privateMessageBuffer.set(idtarget, []);
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
        for (const c of this.clients) if (c.idtarget === idt) { this.safeSend(c, out); delivered = true; }
        if (!delivered) {
          if (!this.privateMessageBuffer.has(idt)) this.privateMessageBuffer.set(idt, []);
          this.privateMessageBuffer.get(idt).push(out);
          this.safeSend(ws, ["privateFailed", idt, "User offline"]);
        }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";
        const activeSockets = Array.from(this.clients).filter(c => c.idtarget === username);
        const hasReconnectSession = this.reconnectSessions.has(username);
        const online = activeSockets.length > 0 || hasReconnectSession;
        this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);

        if (activeSockets.length > 1) {
          const newest = activeSockets[activeSockets.length - 1];
          const oldSockets = activeSockets.slice(0, -1);
          for (const old of oldSockets) {
            try {
              old.close(4000, "Duplicate login — old session closed");
              this.clients.delete(old);
            } catch {}
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

      case "getAllOnlineUsers":
        this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
        break;

      case "getRoomOnlineUsers": {
        const roomName = data[1];
        if (!roomList.includes(roomName)) return this.safeSend(ws, ["error", "Unknown room"]);
        this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
        break;
      }

      case "joinRoom": {
        const newRoom = data[1];
        if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);

        if (ws.idtarget) {
          // explicit join => treat as fresh: remove any stored reconnect session
          this.cleanupReconnectSession(ws.idtarget);
          this.removeAllSeatsById(ws.idtarget);
        }

        ws.roomname = newRoom;

        // ensure rooms map and register client
        if (!this.rooms.has(newRoom)) this.rooms.set(newRoom, { clients: [] });
        const room = this.rooms.get(newRoom);
        if (!room.clients.includes(ws)) room.clients.push(ws);

        const seatMap = this.roomSeats.get(newRoom);
        const foundSeat = this.lockSeat(newRoom, ws);
        if (foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);
        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
        this.sendAllStateTo(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);

        if (ws.idtarget) this.lastActivity.set(ws.idtarget, Date.now());
        break;
      }

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname).push([
          "chat", roomname, noImageURL, username, message, usernameColor, chatTextColor
        ]);
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        si.points.push({ x, y, fast });
        if (si.points.length > 200) si.points.shift();
        this.broadcast(room, ["pointUpdated", room, seat, x, y, fast]);
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        Object.assign(seatMap.get(seat), createEmptySeat());
        for (const c of this.clients) c.numkursi?.delete(seat);
        this.broadcast(room, ["removeKursi", room, seat]);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        Object.assign(currentInfo, { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda });
        seatMap.set(seat, currentInfo);
        if (!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room, new Map());
        this.updateKursiBuffer.get(room).set(seat, { ...currentInfo, points: [] });
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for gift"]);
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname).push(["gift", roomname, sender, receiver, giftName, Date.now()]);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
        // delegate to LowCardGameManager
        try {
          this.lowcard.handleEvent(ws, data);
        } catch (e) {
          console.error("LowCard handler error:", e);
        }
        break;

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
    }
  }

  // ---------------------------
  // WebSocket entrypoint (Durable Object fetch)
  // ---------------------------
  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const ws = server;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    ws.connectionTime = Date.now();

    this.clients.add(ws);

    // new connection hook (no auto-needJoinRoom)
    this.handleNewConnection(ws);

    ws.addEventListener("message", (ev) => {
      try {
        this.handleMessage(ws, ev.data);
      } catch (error) {
        console.error("Error handling message:", error);
        this.safeSend(ws, ["error", "Internal server error"]);
      }
    });

    ws.addEventListener("close", (ev) => {
      console.log(`WebSocket closed: code=${ev.code}, reason=${ev.reason}`);
      ws._lastCloseCode = ev.code;
      this.cleanupClient(ws, `Connection closed: ${ev.reason}`);
    });

    ws.addEventListener("error", (e) => {
      console.log("WebSocket error:", e);
      ws._lastCloseCode = 1006;
      this.cleanupClient(ws, "WebSocket error");
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}

// Default export for Cloudflare Worker routing
export default {
  async fetch(req, env) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if (new URL(req.url).pathname === "/health")
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
