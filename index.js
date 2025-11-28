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

    // Keep buffers present for all rooms to avoid edge cases where some rooms missing keys.
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();

    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []); // always keep key present
    }

    this._nextConnId = 1;

    this.intervalMillis = 15 * 60 * 1000;
    this.currentNumber = 1;
    this.maxNumber = 6;

    // bind tick to `this`
    this._tickTimer = setInterval(() => {
      try { this.tick(); } catch (e) { /* swallow to keep timers alive */ }
    }, this.intervalMillis);

    this._flushTimer = setInterval(() => {
      try {
        if (this.clients.size > 0) this.periodicFlush();
      } catch (e) {}
    }, 16);

    this.lowcard = new LowCardGameManager(this);

    this.messageCounts = new Map();
    this.MAX_MESSAGES_PER_SECOND = 20;

    this._cleanupTimer = setInterval(() => {
      try { this.aggressiveCleanup(); } catch (e) {}
    }, 10 * 1000);
  }

  // optional helper to fully stop & cleanup (call if you want to destroy server)
  destroyServer() {
    try { clearInterval(this._tickTimer); } catch (e) {}
    try { clearInterval(this._flushTimer); } catch (e) {}
    try { clearInterval(this._cleanupTimer); } catch (e) {}

    // Close and clear clients safely
    for (const c of Array.from(this.clients)) {
      try {
        if (c && c.readyState === 1) {
          try { c.close(1000, "Server shutting down"); } catch (e) {}
        }
      } catch (e) {}
      this.clients.delete(c);
    }

    // clear maps
    this.userToSeat.clear();
    this.messageCounts.clear();
    for (const r of roomList) {
      this.updateKursiBuffer.set(r, new Map());
      this.chatMessageBuffer.set(r, []);
    }
  }

  aggressiveCleanup() {
    const now = Date.now();

    // prune messageCounts entries if window is stale
    for (const [key, stats] of Array.from(this.messageCounts.entries())) {
      const currentWindow = Math.floor(now / 1000);
      if (stats && typeof stats.window === "number" && stats.window < currentWindow - 10) {
        this.messageCounts.delete(key);
      }
    }

    // keep room keys, but clear empty arrays to avoid missing keys
    for (const [room, buffer] of Array.from(this.chatMessageBuffer.entries())) {
      if (!Array.isArray(buffer) || buffer.length === 0) {
        // normalize to empty array instead of deleting the key
        this.chatMessageBuffer.set(room, []);
      }
    }

    this.cleanupGhostUsers();
  }

  cleanupGhostUsers() {
    const activeUsers = new Set();
    for (const client of this.clients) {
      try {
        if (client && client.idtarget && client.readyState === 1) {
          activeUsers.add(client.idtarget);
        }
      } catch (e) {}
    }

    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, seatInfo] of seatMap.entries()) {
        try {
          if (seatInfo && seatInfo.namauser && !activeUsers.has(seatInfo.namauser)) {
            this.cleanupUserFromSeat(room, seatNumber, seatInfo.namauser);
          }
        } catch (e) {}
      }
    }
  }

  cleanupUserFromSeat(room, seatNumber, userId) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const seatInfo = seatMap.get(seatNumber);
    if (seatInfo && seatInfo.namauser === userId) {
      try {
        if (seatInfo.viptanda > 0) {
          this.vipManager.removeVipBadge(room, seatNumber);
        }
      } catch (e) {}

      // reset seat
      seatMap.set(seatNumber, createEmptySeat());
      this.clearSeatBuffer(room, seatNumber);
      this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      this.broadcastRoomUserCount(room);
    }

    try { this.userToSeat.delete(userId); } catch (e) {}
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
      for (const [seatNumber, seatInfo] of seatMap.entries()) {
        try {
          if (seatInfo && seatInfo.namauser === idtarget) {
            this.cleanupUserFromSeat(room, seatNumber, idtarget);
          }
        } catch (e) {}
      }
    }

    try { this.userToSeat.delete(idtarget); } catch (e) {}
    try { this.messageCounts.delete(idtarget); } catch (e) {}
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;

    try { this.vipManager.cleanupUserVipBadges(idtarget); } catch (e) {}

    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, info] of seatMap.entries()) {
        try {
          if (info && info.namauser === idtarget) {
            seatMap.set(seatNumber, createEmptySeat());
            this.clearSeatBuffer(room, seatNumber);
            this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          }
        } catch (e) {}
      }
      this.broadcastRoomUserCount(room);
    }

    // filter chatMessageBuffer entries to remove messages from user
    for (const [room, messages] of Array.from(this.chatMessageBuffer.entries())) {
      if (!Array.isArray(messages)) continue;
      const filtered = messages.filter(msg => {
        if (!Array.isArray(msg)) return true;
        if (msg[0] === "chat") return msg[3] !== idtarget;
        if (msg[0] === "gift") return msg[2] !== idtarget && msg[3] !== idtarget;
        return true;
      });
      this.chatMessageBuffer.set(room, filtered);
    }

    try { this.userToSeat.delete(idtarget); } catch (e) {}
    try { this.messageCounts.delete(idtarget); } catch (e) {}

    for (const c of Array.from(this.clients)) {
      try {
        if (c && c.idtarget === idtarget) {
          if (c.readyState === 1) {
            try { c.close(1000, "Session removed"); } catch (e) {}
          }
          this.clients.delete(c);
        }
      } catch (e) {}
    }
  }

  checkRateLimit(ws, messageType) {
    const now = Date.now();
    // choose key: prefer idtarget, fallback to connection id
    const key = (ws && ws.idtarget) ? ws.idtarget : (ws && ws._connId) ? ws._connId : 'anonymous';
    const windowStart = Math.floor(now / 1000);

    if (!this.messageCounts.has(key)) {
      this.messageCounts.set(key, { count: 0, window: windowStart });
    }

    const stats = this.messageCounts.get(key);
    if (!stats || typeof stats.window !== "number") {
      this.messageCounts.set(key, { count: 0, window: windowStart });
    }

    // roll window if changed
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
    if (!ws) return false;
    try {
      if (ws.readyState !== 1) return false;
      if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 1000000) return false;
      const payload = JSON.stringify(arr);
      ws.send(payload);
      return true;
    } catch (e) {
      // send failed; consider closing socket if fatal
      try {
        if (ws && ws.readyState === 1) {
          // don't force-close; instead mark for cleanup by handlers
        }
      } catch (ee) {}
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) {
      return 0;
    }

    let sentCount = 0;
    for (const c of Array.from(this.clients)) {
      try {
        if (c && c.roomname === room && c.readyState === 1) {
          if (this.safeSend(c, msg)) sentCount++;
        }
      } catch (e) {}
    }
    return sentCount;
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const info of seatMap.values()) {
        if (info && info.namauser) cnt[room]++;
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
    // iterate snapshot to avoid mutation issues
    for (const room of roomList) {
      const messages = this.chatMessageBuffer.get(room) || [];
      if (!Array.isArray(messages) || messages.length === 0) {
        this.chatMessageBuffer.set(room, []);
        continue;
      }
      // send each message (safeSend already checks ws)
      for (let i = 0; i < messages.length; i++) {
        this.broadcastToRoom(room, messages[i]);
      }
      // reset to empty array
      this.chatMessageBuffer.set(room, []);
    }
  }

  flushKursiUpdates() {
    for (const room of roomList) {
      const seatMapUpdates = this.updateKursiBuffer.get(room) || new Map();
      const updates = [];
      for (const [seat, info] of seatMapUpdates.entries()) {
        try {
          const { lastPoint, ...rest } = info || {};
          updates.push([seat, rest]);
        } catch (e) {}
      }
      if (updates.length > 0) {
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      // replace with fresh empty map for consistency
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
    this.flushChatBuffer();
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of Array.from(this.clients)) {
      try {
        if (c && c.readyState === 1 && c.roomname) {
          this.safeSend(c, ["currentNumber", this.currentNumber]);
        }
      } catch (e) {}
    }
  }

  isUserInAnyRoom(idtarget) {
    if (!idtarget) return false;
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, seatInfo] of seatMap.entries()) {
        if (seatInfo && seatInfo.namauser === idtarget) {
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

        if (seatInfo && this.isUserStillInSeat(id, seatInfo.room, seatInfo.seat)) {
            const { room, seat } = seatInfo;
            ws.roomname = room;
            ws.numkursi = new Set([seat]);
            this.safeSend(ws, ["currentNumber", this.currentNumber]);
            this.sendAllStateTo(ws, room);
            this.broadcastRoomUserCount(room);
        } else {
            this.safeSend(ws, ["needJoinRoom"]);
        }
    }
  }

  isUserStillInSeat(idtarget, room, seat) {
    if (!idtarget || !room || typeof seat !== "number") return false;
    const seatMap = this.roomSeats.get(room);
    const seatData = seatMap?.get(seat);
    return seatData?.namauser === idtarget;
  }

  handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom) || !ws.idtarget) {
      return false;
    }

    const currentSeatInfo = this.userToSeat.get(ws.idtarget);
    if (currentSeatInfo) {
      const { room: currentRoom, seat: currentSeat } = currentSeatInfo;
      this.cleanupUserFromSeat(currentRoom, currentSeat, ws.idtarget);
    }

    const foundSeat = this.findEmptySeat(newRoom, ws);
    if (!foundSeat) {
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    ws.roomname = newRoom;
    ws.numkursi = new Set([foundSeat]);

    this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
    this.safeSend(ws, ["currentNumber", this.currentNumber]);

    this.sendAllStateTo(ws, newRoom);
    try { this.vipManager.getAllVipBadges(ws, newRoom); } catch (e) {}
    this.broadcastRoomUserCount(newRoom);

    return true;
  }

  findEmptySeat(room, ws) {
    if (!ws || !ws.idtarget) return null;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    // If user already occupies any seat in this room, return that seat (preserve)
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (seatInfo && seatInfo.namauser === ws.idtarget) {
        return i;
      }
    }

    // find truly empty seat
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k && !k.namauser) {
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    if (!ws || ws.readyState !== 1 || !room) return;

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
          itembawah: infoitembawah in info ? info.itembawah : info.itembawah,
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
    if (currentSeat && currentSeat.namauser === idtarget) {
      try {
        if (currentSeat.viptanda > 0) {
          this.vipManager.removeVipBadge(room, seat);
        }
      } catch (e) {}

      seatMap.set(seat, createEmptySeat());
      this.clearSeatBuffer(room, seat);
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
      this.broadcastRoomUserCount(room);
    }

    try { this.userToSeat.delete(idtarget); } catch (e) {}
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;

    // make sure this only runs once per idtarget
    try { this.fullRemoveById(idtarget); } catch (e) {}
    try { this.forceUserCleanup(idtarget); } catch (e) {}

    // attempt to close and remove ws
    if (ws) {
      try {
        this.clients.delete(ws);
      } catch (e) {}
      try {
        if (ws.readyState === 1) {
          try { ws.close(1000, "Manual destroy"); } catch (e) {}
        }
      } catch (e) {}
    }
  }

  getAllOnlineUsers() {
    const users = [];
    for (const c of this.clients) {
      try {
        if (c && c.idtarget && c.readyState === 1) {
          users.push(c.idtarget);
        }
      } catch (e) {}
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    for (const c of this.clients) {
      try {
        if (c && c.roomname === roomName && c.idtarget && c.readyState === 1) {
          users.push(c.idtarget);
        }
      } catch (e) {}
    }
    return users;
  }

  handleGetAllRoomsUserCount(ws) {
    if (!ws || ws.readyState !== 1) return;
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1) return;

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

      case "onDestroy":
        // call cleanup properly
        try { this.handleOnDestroy(ws, ws.idtarget); } catch (e) {}
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
          try {
            if (c.idtarget === idtarget && c.readyState === 1) {
              this.safeSend(c, notif);
              break;
            }
          } catch (e) {}
        }
        break;
      }

      case "private": {
        const [, idt, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", idt, url, msg, ts, sender];
        this.safeSend(ws, out);
        for (const c of this.clients) {
          try {
            if (c.idtarget === idt && c.readyState === 1) {
              this.safeSend(c, out);
              break;
            }
          } catch (e) {}
        }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";
        let online = false;
        for (const c of this.clients) {
          try {
            if (c.idtarget === username && c.readyState === 1) {
              online = true;
              break;
            }
          } catch (e) {}
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
        const si = seatMap?.get(seat);
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
        if (!seatMap) return;
        const target = seatMap.get(seat);
        if (!target) return;
        seatMap.set(seat, createEmptySeat());
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
          // schedule async to avoid blocking message handler
          setTimeout(() => {
            try { this.lowcard.handleEvent(ws, data); } catch (e) {}
          }, 0);
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

    // Accept the server side socket
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
            // swallow handling errors to keep socket alive
        }
    });

    ws.addEventListener("error", (event) => {
        try {
          if (ws.idtarget) {
              ws.isManualDestroy = true;
              this.handleOnDestroy(ws, ws.idtarget);
          } else {
              // best-effort: remove ws
              this.clients.delete(ws);
              try { ws.close(1011, "Socket error"); } catch (e) {}
          }
        } catch (e) {}
    });

    ws.addEventListener("close", (event) => {
        try {
          if (ws.idtarget) {
              ws.isManualDestroy = true;
              this.handleOnDestroy(ws, ws.idtarget);
          }
        } catch (e) {}
        // ensure client removed
        try { this.clients.delete(ws); } catch (e) {}
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
