// ===== Constants & Types =====
const roomList = [
  "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout",
  "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// ===== Durable Object =====
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();
    this.userToSeat = new Map();
    this.roomSeats = new Map();

    this.pointUpdateBuffer = new Map();
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= 35; i++) seatMap.set(i, this.createEmptySeat());
      this.roomSeats.set(room, seatMap);
    }

    setInterval(() => this.tick(), this.intervalMillis);
    setInterval(() => this.periodicFlush(), 100);
  }

  createEmptySeat() {
    return {
      noimageUrl: "",
      namauser: "",
      color: "",
      itembawah: 0,
      itematas: 0,
      vip: false,
      viptanda: 0,
      points: [],
    };
  }

  safeSend(ws, msg) {
    try {
      if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
      else this.clients.delete(ws);
    } catch {
      this.clients.delete(ws);
    }
  }

  assertValidRoom(room) {
    if (!roomList.includes(room)) throw new Error("Unknown room: " + room);
    return true;
  }

  broadcastToRoom(room, msg) {
    for (const c of [...this.clients]) if (c.roomname === room) this.safeSend(c, msg);
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map((r) => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const info of seatMap.values())
        if (info.namauser && !info.namauser.startsWith("__LOCK__")) cnt[room]++;
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    const result = roomList.map((room) => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  flushPrivateMessageBuffer() {
    for (const [idtarget, messages] of this.privateMessageBuffer) {
      for (const c of this.clients)
        if (c.idtarget === idtarget) messages.forEach((msg) => this.safeSend(c, msg));
      messages.length = 0;
    }
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      messages.forEach((msg) => this.broadcastToRoom(room, msg));
      messages.length = 0;
    }
  }

  flushPointUpdates() {
    for (const [room, seatMap] of this.pointUpdateBuffer) {
      for (const [seat, points] of seatMap) {
        points.forEach((p) =>
          this.broadcastToRoom(room, ["pointUpdated", room, seat, p.x, p.y, p.fast])
        );
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
      if (updates.length > 0)
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      seatMap.clear();
    }
  }

  tick() {
    this.currentNumber =
      this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of [...this.clients])
      this.safeSend(c, ["currentNumber", this.currentNumber]);
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (
          info.namauser.startsWith("__LOCK__") &&
          info.lockTime &&
          now - info.lockTime > 10000
        ) {
          Object.assign(info, this.createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;

    if (this.userToSeat.has(ws.idtarget)) {
      const prev = this.userToSeat.get(ws.idtarget);
      if (prev.room === room && seatMap.get(prev.seat).namauser === "")
        return prev.seat;
    }

    for (let i = 1; i <= 35; i++) {
      const kursi = seatMap.get(i);
      if (kursi.namauser === "") {
        kursi.namauser = "__LOCK__" + ws.idtarget;
        kursi.lockTime = Date.now();
        return i;
      }
    }
    return null;
  }

  cleanupBuffers(ws) {
    if (ws.idtarget) {
      this.privateMessageBuffer.delete(ws.idtarget);
      this.userToSeat.delete(ws.idtarget);
    }
  }

  cleanupClient(ws) {
    this.cleanupBuffers(ws);
    this.clients.delete(ws);
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

  handleMessage(ws, dataStr) {
    try {
      const data = JSON.parse(dataStr);
      if (!Array.isArray(data) || data.length === 0)
        return this.safeSend(ws, ["error", "Invalid message format"]);
      const [evt, ...args] = data;
      switch (evt) {
        case "setIdTarget":
          ws.idtarget = args[0];
          break;
        case "ping":
          this.safeSend(ws, ["pong"]);
          break;
        case "getAllRoomsUserCount":
          this.handleGetAllRoomsUserCount(ws);
          break;
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        case "joinRoom":
          ws.roomname = args[0];
          this.broadcastRoomUserCount(ws.roomname);
          break;
        case "chat":
          this.chatMessageBuffer.set(ws.roomname, [
            ...(this.chatMessageBuffer.get(ws.roomname) || []),
            ["chat", ...args],
          ]);
          break;
        case "updatePoint": {
          const [room, seat, x, y, fast] = args;
          if (!this.pointUpdateBuffer.has(room))
            this.pointUpdateBuffer.set(room, new Map());
          const seatMap = this.pointUpdateBuffer.get(room);
          if (!seatMap.has(seat)) seatMap.set(seat, []);
          seatMap.get(seat).push({ x, y, fast });
          break;
        }
        case "removeKursiAndPoint":
          this.broadcastToRoom(ws.roomname, ["removeKursi", ...args]);
          break;
        case "updateKursi": {
          const [room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] =
            args;
          if (!this.updateKursiBuffer.has(room))
            this.updateKursiBuffer.set(room, new Map());
          this.updateKursiBuffer.get(room).set(seat, {
            noimageUrl,
            namauser,
            color,
            itembawah,
            itematas,
            vip,
            viptanda,
          });
          break;
        }
        case "sendnotif":
          this.safeSend(ws, ["notif", ...args, Date.now()]);
          break;
        case "private": {
          const [idtarget, noimageUrl, message, sender] = args;
          if (!this.privateMessageBuffer.has(idtarget))
            this.privateMessageBuffer.set(idtarget, []);
          this.privateMessageBuffer.get(idtarget).push([
            "private",
            ws.idtarget,
            noimageUrl,
            message,
            Date.now(),
            sender,
          ]);
          break;
        }
        case "isUserOnline": {
          const [userId, tanda] = args;
          const online = [...this.clients].some((c) => c.idtarget === userId);
          this.safeSend(ws, ["userOnlineStatus", userId, online, tanda]);
          break;
        }
        default:
          this.safeSend(ws, ["error", "Unknown event"]);
          break;
      }
    } catch (err) {
      console.error("Error handling message:", err, "raw:", dataStr);
    }
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") === "websocket") {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("message", (ev) => this.handleMessage(server, ev.data));
      server.addEventListener("close", () => this.cleanupClient(server));
      this.clients.add(server);
      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response("ChatServer active");
  }
}

// ===== Worker Entry =====
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER.idFromName("global-chat");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(req);
  },
};
