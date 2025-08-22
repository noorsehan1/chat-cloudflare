// ============================
// Cloudflare Workers + DO Chat
// ============================

// ---- Konstanta Room ----
const roomList = [
  "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout",
  "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// ---- Util seat ----
function createEmptySeat() {
  return {
    noimageUrl: "",
    namauser: "",
    color: "",
    itembawah: 0,
    itematas: 0,
    vip: false,
    viptanda: 0,
    points: [],
    lockTime: undefined,
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

    this.pointUpdateBuffer = new Map();   // room -> Map(seat -> [{x,y,fast}])
    this.updateKursiBuffer = new Map();   // room -> Map(seat -> seatInfo)
    this.chatMessageBuffer = new Map();   // room -> [msg...]
    this.privateMessageBuffer = new Map();// idtarget -> [msg...]

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), 100);
  }

  // ---------- Helpers ----------
  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) ws.send(JSON.stringify(arr));
      else this.clients.delete(ws);
    } catch {
      this.clients.delete(ws);
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of this.clients) if (c.roomname === room) this.safeSend(c, msg);
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

  // ---------- Flushers ----------
  flushPrivateMessageBuffer() {
    for (const [idtarget, messages] of this.privateMessageBuffer) {
      for (const c of this.clients) {
        if (c.idtarget === idtarget) {
          for (const msg of messages) this.safeSend(c, msg);
        }
      }
      messages.length = 0;
    }
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      for (const msg of messages) this.broadcastToRoom(room, msg);
      messages.length = 0;
    }
  }

  flushPointUpdates() {
    for (const [room, seatMap] of this.pointUpdateBuffer) {
      for (const [seat, points] of seatMap) {
        for (const p of points) {
          this.broadcastToRoom(room, ["pointUpdated", room, seat, p.x, p.y, p.fast]);
        }
        points.length = 0;
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMap] of this.updateKursiBuffer) {
      const updates = [];
      for (const [seat, info] of seatMap) {
        const { points, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      seatMap.clear();
    }
  }

  // ---------- Housekeeping ----------
  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of this.clients) this.safeSend(c, ["currentNumber", this.currentNumber]);
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  periodicFlush() {
    try {
      this.flushPointUpdates();
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.flushPrivateMessageBuffer();
      this.cleanExpiredLocks();
    } catch (err) {
      console.error("Periodic flush error:", err);
    }
  }

  // ---------- Event Handlers ----------
  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;

    if (this.userToSeat.has(ws.idtarget)) {
      const prev = this.userToSeat.get(ws.idtarget);
      if (prev.room === room) {
        const seatInfo = seatMap.get(prev.seat);
        if (seatInfo && seatInfo.namauser === "") return prev.seat;
      }
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k && k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = Date.now();
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
      for (const p of info.points) allPoints.push({ seat, ...p });
      if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
        const { points, ...rest } = info;
        meta[seat] = rest;
      }
    }
    this.safeSend(ws, ["allPointsList", room, allPoints]);
    this.safeSend(ws, ["allUpdateKursiList", room, meta]);
  }

  // ---------- Message Router ----------
  handleMessage(ws, raw) {
    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      return this.safeSend(ws, ["error", "Invalid JSON"]);
    }
    if (!Array.isArray(data) || data.length === 0) {
      return this.safeSend(ws, ["error", "Invalid message format"]);
    }
    const evt = data[0];

    try {
      switch (evt) {
        // ---------------------
        // SET IDTARGET
        // ---------------------
        case "setIdTarget": {
          ws.idtarget = data[1];

          // ---- Bersihkan client lain yang pakai idtarget sama ----
          for (const c of Array.from(this.clients)) {
            if (c !== ws && c.idtarget === ws.idtarget) {
              this.cleanupClient(c);
            }
          }

          // Hapus mapping lama di userToSeat jika ada
          if (this.userToSeat.has(ws.idtarget)) this.userToSeat.delete(ws.idtarget);

          this.safeSend(ws, ["setIdTargetAck", ws.idtarget]);
          break;
        }

        // ---------------------
        // JOIN ROOM
        // ---------------------
        case "joinRoom": {
          const newRoom = data[1];
          if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);

          // ---- Bersihkan kursi lama user di room manapun sebelum join room baru ----
          if (ws.idtarget) {
            // Bersihkan kursi user di room lama
            if (ws.roomname && ws.numkursi) {
              const oldRoom = ws.roomname;
              const oldSeatMap = this.roomSeats.get(oldRoom);
              for (const s of ws.numkursi) {
                Object.assign(oldSeatMap.get(s), createEmptySeat());
                this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, s]);
              }
              this.broadcastRoomUserCount(oldRoom);
              ws.numkursi.clear();
            }

            // Hapus mapping lama userToSeat jika ada
            if (this.userToSeat.has(ws.idtarget)) this.userToSeat.delete(ws.idtarget);

            // Bersihkan client lain yang pakai idtarget sama
            for (const c of Array.from(this.clients)) {
              if (c !== ws && c.idtarget === ws.idtarget) {
                this.cleanupClient(c);
              }
            }
          }

          ws.roomname = newRoom;

          // ---- Lock seat baru di room baru ----
          const seatMap = this.roomSeats.get(newRoom);
          let foundSeat = this.lockSeat(newRoom, ws);
          if (foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);

          ws.numkursi = new Set([foundSeat]);
          this.safeSend(ws, ["numberKursiSaya", foundSeat]);

          // Simpan mapping userToSeat baru
          if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

          // Kirim semua state ke client
          this.sendAllStateTo(ws, newRoom);

          // Update user count room
          this.broadcastRoomUserCount(newRoom);
          break;
        }

        // ---------------------
        // CHAT
        // ---------------------
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
          if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname).push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        // ---------------------
        // UPDATE POINT
        // ---------------------
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if (!si) return;
          si.points.push({ x, y, fast });
          if (si.points.length > 200) si.points.shift(); // batas buffer

          if (!this.pointUpdateBuffer.has(room)) this.pointUpdateBuffer.set(room, new Map());
          const roomBuf = this.pointUpdateBuffer.get(room);
          if (!roomBuf.has(seat)) roomBuf.set(seat, []);
          roomBuf.get(seat).push({ x, y, fast });
          break;
        }

        default:
          this.safeSend(ws, ["error", "Unknown event"]);
      }
    } catch (err) {
      console.error("handleMessage error:", err);
      this.safeSend(ws, ["error", "Internal error"]);
    }
  }

  // ---------- Lifecycle ----------
  cleanupClient(ws) {
    try {
      const room = ws.roomname;
      const kursis = ws.numkursi;
      if (room && kursis && this.roomSeats.has(room)) {
        const seatMap = this.roomSeats.get(room);
        for (const seat of kursis) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
        }
        if (ws.idtarget && this.userToSeat.has(ws.idtarget)) {
          const prev = this.userToSeat.get(ws.idtarget);
          if (prev.room === room && kursis.has(prev.seat)) this.userToSeat.delete(ws.idtarget);
        }
        this.broadcastRoomUserCount(room);
      }
    } catch (e) {
      console.error("cleanup error:", e);
    } finally {
      this.clients.delete(ws);
      ws.numkursi?.clear?.();
      ws.roomname = undefined;
    }
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || request.headers.get("upgrade") || "";
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
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    }
    return new Response("WebSocket endpoint at wss://<your-subdomain>.workers.dev", { status: 200 });
  }
};
