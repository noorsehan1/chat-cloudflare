// ===== Constants =====
const roomList = [
  "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout",
  "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// ===== Durable Object =====
export class ChatServer {
  constructor(state) {
    this.state = state;
    this.clients = new Set();
    this.userToSeat = new Map();
    this.roomSeats = new Map();

    // buffers
    this.pointUpdateBuffer = new Map();
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    // seat maps
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= 35; i++) seatMap.set(i, this.createEmptySeat());
      this.roomSeats.set(room, seatMap);
    }

    // background numbers
    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

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
      vip: 0,
      viptanda: 0,
      points: [],
    };
  }

  safeSend(ws, msg) {
    try {
      if (ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify(msg));
      } else {
        this.clients.delete(ws);
      }
    } catch {
      this.clients.delete(ws);
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of this.clients) {
      if (c.roomname === room) this.safeSend(c, msg);
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map((r) => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const info of seatMap.values()) {
        if (info.namauser && !info.namauser.startsWith("__LOCK__")) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  // ===== Buffer Flushers =====
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
          this.broadcastToRoom(room, [
            "pointUpdated",
            room,
            seat,
            p.x,
            p.y,
            p.fast,
          ])
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

  flushPrivateMessageBuffer() {
    for (const [idtarget, messages] of this.privateMessageBuffer) {
      for (const c of this.clients) {
        if (c.idtarget === idtarget)
          messages.forEach((msg) => this.safeSend(c, msg));
      }
      messages.length = 0;
    }
  }

  periodicFlush() {
    try {
      this.flushPointUpdates();
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.flushPrivateMessageBuffer();
    } catch (err) {
      console.error("Periodic flush error:", err);
    }
  }

  // ===== Tick =====
  tick() {
    this.currentNumber =
      this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of this.clients) {
      this.safeSend(c, ["currentNumber", this.currentNumber]);
    }
  }

  // ===== Event Handlers =====
  handleMessage(ws, dataStr) {
    let data;
    try {
      data = JSON.parse(dataStr);
    } catch {
      return this.safeSend(ws, ["error", "Invalid JSON"]);
    }
    if (!Array.isArray(data)) return;

    const [evt, ...args] = data;
    switch (evt) {
      case "ping":
        this.safeSend(ws, ["pong"]);
        break;

      case "setIdTarget":
        ws.idtarget = args[0];
        break;

      case "getAllRoomsUserCount": {
        const allCounts = this.getJumlahRoom();
        const result = roomList.map((room) => [room, allCounts[room]]);
        this.safeSend(ws, ["allRoomsUserCount", result]);
        break;
      }

      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;

      case "joinRoom":
        ws.roomname = args[0];
        // kasih kursi random
        this.safeSend(ws, ["numberKursiSaya", Math.floor(Math.random() * 35) + 1]);
        this.broadcastRoomUserCount(ws.roomname);
        break;

      case "chat": {
        const [room, noImageURL, username, message, usernameColor, chatTextColor] = args;
        if (!this.chatMessageBuffer.has(room))
          this.chatMessageBuffer.set(room, []);
        this.chatMessageBuffer.get(room).push([
          "chat",
          room,
          noImageURL,
          username,
          message,
          usernameColor,
          chatTextColor,
        ]);
        break;
      }

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

      case "sendnotif": {
        const [idtarget, noimageUrl, username, deskripsi] = args;
        this.safeSend(ws, ["notif", noimageUrl, username, deskripsi, Date.now()]);
        break;
      }

      case "removeKursiAndPoint": {
        const [room, seat] = args;
        const seatMap = this.roomSeats.get(room);
        if (seatMap) {
          seatMap.set(seat, this.createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
        break;
      }

      case "updatePoint": {
        const [room, seat, x, y, fast] = args;
        if (!this.pointUpdateBuffer.has(room))
          this.pointUpdateBuffer.set(room, new Map());
        if (!this.pointUpdateBuffer.get(room).has(seat))
          this.pointUpdateBuffer.get(room).set(seat, []);
        this.pointUpdateBuffer.get(room).get(seat).push({ x, y, fast });
        break;
      }

      case "updateKursi": {
        const [room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = args;
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

      case "resetRoom": {
        for (const room of roomList) {
          const seatMap = this.roomSeats.get(room);
          for (let i = 1; i <= 35; i++) seatMap.set(i, this.createEmptySeat());
          this.broadcastToRoom(room, ["resetRoom", room]);
        }
        break;
      }

      case "isUserOnline": {
        const [userId, tanda] = args;
        let online = false;
        for (const c of this.clients) {
          if (c.idtarget === userId) {
            online = true;
            break;
          }
        }
        this.safeSend(ws, ["userOnlineStatus", userId, online, tanda]);
        break;
      }

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
    }
  }

  // ===== fetch() untuk DO =====
  async fetch(request) {
    if (request.headers.get("upgrade") !== "websocket") {
      return new Response("Expected websocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.accept();
    this.clients.add(server);

    server.addEventListener("message", (ev) =>
      this.handleMessage(server, ev.data)
    );
    server.addEventListener("close", () => this.clients.delete(server));

    return new Response(null, { status: 101, webSocket: client });
  }
}

// ===== Worker Entry =====
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER.idFromName("global");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(req);
  },
};
