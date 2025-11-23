// ============================
// Cloudflare Workers + DO Chat
// ============================

// ---- Konstanta Room ----
const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// ---- Util seat ----
function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "",
    itembawah: 0, itematas: 0, vip: 0, viptanda: 0,
    lastPoint: null, lockTime: undefined
  };
}

// =====================
// Durable Object Server
// =====================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set(); // ws augmented: {roomname, idtarget, numkursi:Set<number>}
    this.userToSeat = new Map(); // idtarget -> { room, seat }

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    this.updateKursiBuffer = new Map(); // room -> Map(seat -> seatInfo)
    this.chatMessageBuffer = new Map(); // room -> [msg...]

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), 100);

    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);
  }

  // ---------- Helpers ----------
  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) ws.send(JSON.stringify(arr));
      else this.cleanupClient(ws);
    } catch (err) {
      this.cleanupClient(ws);
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of Array.from(this.clients)) {
      if (c.roomname === room) {
        try { this.safeSend(c, msg); } catch (e) { }
      }
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
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      for (const msg of messages) {
        try { this.broadcastToRoom(room, msg); } catch (e) { }
      }
      messages.length = 0;
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMap] of this.updateKursiBuffer) {
      const updates = [];
      for (const [seat, info] of seatMap) {
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) {
        try { this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]); } catch (e) { }
      }
      seatMap.clear();
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of Array.from(this.clients)) {
      try { this.safeSend(c, ["currentNumber", this.currentNumber]); } catch (e) { }
    }
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 30000) {
          Object.assign(info, createEmptySeat());
          try { this.broadcastToRoom(room, ["removeKursi", room, seat]); } catch (e) { }
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
    } catch (err) { }
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;

    // Cek jika user sudah punya seat di room ini
    if (this.userToSeat.has(ws.idtarget)) {
      const prev = this.userToSeat.get(ws.idtarget);
      if (prev.room === room) {
        return prev.seat;
      }
    }

    // Cari seat kosong
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k && k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = Date.now();
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const meta = {};

    for (const [seat, info] of seatMap) {
      if (info.lastPoint) {
        allPoints.push({
          seat: seat,
          x: info.lastPoint.x,
          y: info.lastPoint.y,
          fast: info.lastPoint.fast
        });
      }
      if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
        const { lastPoint, ...rest } = info;
        meta[seat] = rest;
      }
    }

    if (Object.keys(meta).length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, meta]);
    }
    if (allPoints.length > 0) {
      this.safeSend(ws, ["allPointsList", room, allPoints]);
    }

    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    const count = this.getJumlahRoom()[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
  }

  removeAllSeatsById(idtarget) {
    for (const [room, seatMap] of this.roomSeats) {
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          try {
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
          } catch (e) { }
        }
      }
    }
  }

  cleanupClientById(idtarget) {
    for (const c of Array.from(this.clients)) {
      if (c.idtarget === idtarget) {
        this.cleanupClient(c);
      }
    }
  }

  handleMessage(ws, raw) {
    let data;
    try {
      data = JSON.parse(raw);
    } catch (e) {
      return this.safeSend(ws, ["error", "Invalid JSON"]);
    }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);

    const evt = data[0];

    try {
      switch (evt) {
        case "setIdTarget": {
          const newId = data[1];
          this.cleanupClientById(newId);
          ws.idtarget = newId;
          this.safeSend(ws, ["setIdTargetAck", ws.idtarget]);
          break;
        }

        case "isInRoom": {
          const idtarget = ws.idtarget;
          if (!idtarget) {
            this.safeSend(ws, ["inRoomStatus", false]);
            return;
          }
          const seatInfo = this.userToSeat.get(idtarget);
          const isInRoom = seatInfo ? true : false;
          this.safeSend(ws, ["inRoomStatus", isInRoom]);
          break;
        }

        case "onDestroy": {
          const idtarget = ws.idtarget;
          this.removeAllSeatsById(idtarget);
          this.userToSeat.delete(idtarget);
          break;
        }

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          for (const c of this.clients) {
            if (c.idtarget === idtarget) {
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
            if (c.idtarget === idt) {
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
            if (c.idtarget === username) {
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

        // ----------------------
        // Join Room
        // ----------------------
        case "joinRoom": {
          const newRoom = data[1];
          if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);

          if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);

          ws.roomname = newRoom;
          const foundSeat = this.lockSeat(newRoom, ws);
          if (foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);

          ws.numkursi = new Set([foundSeat]);
          this.safeSend(ws, ["numberKursiSaya", foundSeat]);
          if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

          this.sendAllStateTo(ws, newRoom);
          this.vipManager.getAllVipBadges(ws, newRoom);
          this.broadcastRoomUserCount(newRoom);
          break;
        }

        // ----------------------
        // Chat
        // ----------------------
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return;
          if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        // ----------------------
        // Update Point (real-time)
        // ----------------------
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
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) return;

          const seatInfo = {
            noimageUrl, namauser, color, itembawah, itematas,
            vip: vip || 0, viptanda: viptanda || 0,
            lastPoint: null, lockTime: undefined
          };

          if (!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room, new Map());
          this.updateKursiBuffer.get(room).set(seat, seatInfo);
          this.roomSeats.get(room).set(seat, seatInfo);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
          break;
        }

        // VIP Badge Events
        case "vipbadge":
        case "removeVipBadge":
        case "getAllVipBadges":
          this.vipManager.handleEvent(ws, data);
          break;

        // LowCard Game Events
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          const room = ws.roomname;
          if (room !== "LowCard") return;
          setTimeout(() => this.lowcard.handleEvent(ws, data), 0);
          break;
        }

        default:
          this.safeSend(ws, ["error", "Unknown event"]);
      }
    } catch (err) {
      this.safeSend(ws, ["error", "Internal error"]);
    }
  }

  cleanupClient(ws) {
    try {
      const id = ws.idtarget;
      if (id) {
        for (const [room, seatMap] of this.roomSeats) {
          for (const [seat, info] of seatMap) {
            if (info.namauser === "__LOCK__" + id || info.namauser === id) {
              Object.assign(seatMap.get(seat), createEmptySeat());
              try { this.broadcastToRoom(room, ["removeKursi", room, seat]); } catch (e) { }
            }
          }
        }
        this.userToSeat.delete(id);
      }

      const room = ws.roomname;
      if (room) {
        this.broadcastRoomUserCount(room);
      }
    } catch (e) {
    } finally {
      this.clients.delete(ws);
      ws.numkursi?.clear?.();
      ws.roomname = undefined;
      ws.idtarget = undefined;
    }
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || request.headers.get("upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") return new Response("Expected WebSocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const ws = server;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    this.clients.add(ws);

    ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));
    ws.addEventListener("close", () => this.cleanupClient(ws));
    ws.addEventListener("error", () => this.cleanupClient(ws));

    return new Response(null, { status: 101, webSocket: client });
  }
}

// ======================
// Worker Entry (Router)
// ======================
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
