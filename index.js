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

    // Koneksi aktif (WebSocket)
    this.clients = new Set(); // ws augmented: {roomname, idtarget, numkursi:Set<number>}

    // Pemetaan user -> kursi
    this.userToSeat = new Map(); // idtarget -> { room, seat }

    // Data kursi per room
    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    // Buffer broadcast (di-flush per 100ms)
    this.pointUpdateBuffer = new Map();  // room -> Map(seat -> [{x,y,fast}])
    this.updateKursiBuffer = new Map();  // room -> Map(seat -> seatInfo)
    this.chatMessageBuffer = new Map();  // room -> [msg...]
    this.privateMessageBuffer = new Map(); // idtarget -> [msg...]

    // Angka bergilir 1..6 tiap 15 menit
    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), 100);
  }

  // ---------- Helpers ----------
  safeSend(ws, arr) {
    try {
      // di Workers, readyState OPEN = 1
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

    // Jika sudah pernah punya seat di room yang sama
    if (this.userToSeat.has(ws.idtarget)) {
      const prev = this.userToSeat.get(ws.idtarget);
      if (prev.room === room) {
        const seatInfo = seatMap.get(prev.seat);
        if (seatInfo.namauser === "") return prev.seat;
      }
    }
    // Cari kursi kosong & lock
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k.namauser === "") {
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

    switch (evt) {
      case "setIdTarget": {
        ws.idtarget = data[1];
        this.safeSend(ws, ["setIdTargetAck", ws.idtarget]);
        break;
      }
      case "ping": {
        const pingId = data[1];
        if (pingId && ws.idtarget === pingId) this.safeSend(ws, ["pong"]);
        break;
      }
      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === idtarget) {
            this.safeSend(c, notif);
            delivered = true;
          }
        }
        // simpan ke buffer supaya terkirim saat user online
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

        // echo ke pengirim juga (biar history tampil)
        this.safeSend(ws, out);

        // kirim ke target kalau online; jika tidak, buffer + beri info gagal ke pengirim
        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === idt) {
            this.safeSend(c, out);
            delivered = true;
          }
        }
        if (!delivered) {
          if (!this.privateMessageBuffer.has(idt)) this.privateMessageBuffer.set(idt, []);
          this.privateMessageBuffer.get(idt).push(out);
          this.safeSend(ws, ["privateFailed", idt, "User offline"]);
        }
        break;
      }
      case "isUserOnline": {
        const target = data[1];
        const tanda = data[2] ?? "";
        const online = Array.from(this.clients).some(c => c.idtarget === target);
        this.safeSend(ws, ["userOnlineStatus", target, online, tanda]);
        break;
      }
      case "getAllRoomsUserCount": {
        this.handleGetAllRoomsUserCount(ws);
        break;
      }
      case "getCurrentNumber": {
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;
      }
      case "joinRoom": {
        const newRoom = data[1];
        if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);

        const seatMap = this.roomSeats.get(newRoom);
        let foundSeat = null;

        if (ws.idtarget && this.userToSeat.has(ws.idtarget)) {
          const prev = this.userToSeat.get(ws.idtarget);
          if (prev.room === newRoom) {
            const si = seatMap.get(prev.seat);
            if (si.namauser === "") foundSeat = prev.seat;
          }
        }
        if (foundSeat === null) foundSeat = this.lockSeat(newRoom, ws);

        if (foundSeat === null) {
          return this.safeSend(ws, ["roomFull", newRoom]);
        }
        const kursiFinal = seatMap.get(foundSeat);
        if (!String(kursiFinal.namauser).startsWith("__LOCK__")) {
          return this.safeSend(ws, ["roomFull", newRoom]);
        }

        // bersihkan kursi lama bila pindah room
        if (ws.roomname && ws.numkursi) {
          const oldRoom = ws.roomname;
          const oldSeatMap = this.roomSeats.get(oldRoom);
          for (const s of ws.numkursi) {
            Object.assign(oldSeatMap.get(s), createEmptySeat());
            this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, s]);
          }
          this.broadcastRoomUserCount(oldRoom);
        }

        ws.roomname = newRoom;
        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);

        if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

        // kirim snapshot awal
        this.sendAllStateTo(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);
        break;
      }
      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname).push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
        break;
      }
      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        si.points.push({ x, y, fast });

        if (!this.pointUpdateBuffer.has(room)) this.pointUpdateBuffer.set(room, new Map());
        const roomBuf = this.pointUpdateBuffer.get(room);
        if (!roomBuf.has(seat)) roomBuf.set(seat, []);
        roomBuf.get(seat).push({ x, y, fast });
        break;
      }
      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);

        const seatMap = this.roomSeats.get(room);
        Object.assign(seatMap.get(seat), createEmptySeat());

        for (const c of this.clients) c.numkursi?.delete(seat);

        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }
      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);

        const seatInfo = { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda, points: [] };

        if (!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room, new Map());
        this.updateKursiBuffer.get(room).set(seat, seatInfo);
        this.roomSeats.get(room).set(seat, seatInfo);

        this.broadcastRoomUserCount(room);
        break;
      }
      case "resetRoom": {
        // kosongkan semua kursi di room aktif klien ini (kalau ada)
        const room = ws.roomname;
        if (!room) return;
        const seatMap = this.roomSeats.get(room);
        for (let i = 1; i <= this.MAX_SEATS; i++) {
          const si = seatMap.get(i);
          Object.assign(si, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, i]);
        }
        // info ke klien: resetRoom
        this.broadcastToRoom(room, ["resetRoom", room]);
        this.broadcastRoomUserCount(room);
        break;
      }
      default:
        this.safeSend(ws, ["error", "Unknown event"]);
    }
  }

  // ---------- Lifecycle ----------
  cleanupClient(ws) {
    try {
      // bersihkan kursi user ini
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

    // augment ws
    const ws = server;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();

    this.clients.add(ws);

    ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));
    ws.addEventListener("close", () => this.cleanupClient(ws));
    ws.addEventListener("error", () => this.cleanupClient(ws));

    // Cloudflare akan menangani 101 switch saat Response dikembalikan
    return new Response(null, { status: 101, webSocket: client });
  }
}

// ======================
// Worker Entry (Router)
// ======================
export default {
  async fetch(req, env) {
    // Semua koneksi WS diarahkan ke DO singleton "global-chat"
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    // Endpoint HTTP sederhana (cek cepat)
    if (new URL(req.url).pathname === "/health") {
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    }
    return new Response("WebSocket endpoint at wss://<your-subdomain>.workers.dev", { status: 200 });
  }
};
