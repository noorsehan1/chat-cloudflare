// ===== Constants =====
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

    this.nextTickAt = Date.now() + this.intervalMillis;
    this._scheduleAt(this.nextTickAt);
  }

  // ===== Alarm scheduling =====
  async _scheduleAt(ts) {
    try {
      const cur = await this.state.storage.getAlarm();
      if (!cur || ts < cur) await this.state.storage.setAlarm(ts);
    } catch {}
  }
  _scheduleSoon(ms) {
    return this._scheduleAt(Date.now() + ms);
  }

  // ===== Helpers =====
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
      lockTime: 0
    };
  }

  safeSend(ws, msg) {
    try {
      ws.send(JSON.stringify(msg));
    } catch {
      this.clients.delete(ws);
    }
  }

  assertValidRoom(room) {
    if (!roomList.includes(room)) throw new Error("Unknown room: " + room);
    return true;
  }

  broadcastToRoom(room, msg) {
    for (const c of this.clients) if (c.roomname === room) this.safeSend(c, msg);
  }

  _ensureMap(map, key, ctor) {
    if (!map.has(key)) map.set(key, ctor());
    return map.get(key);
  }

  // ===== Flush Buffers =====
  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length) {
        messages.forEach(msg => this.broadcastToRoom(room, msg));
        messages.length = 0;
      }
    }
  }
  flushPrivateMessageBuffer() {
    for (const [idtarget, messages] of this.privateMessageBuffer) {
      for (const c of this.clients) if (c.idtarget === idtarget) {
        messages.forEach(msg => this.safeSend(c, msg));
      }
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
        updates.push([seat, { ...info }]);
      }
      if (updates.length > 0) this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      seatMap.clear();
    }
  }

  periodicFlush() {
    this.flushChatBuffer();
    this.flushPrivateMessageBuffer();
    this.flushPointUpdates();
    this.flushKursiUpdates();
  }

  // ===== Tick number =====
  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of this.clients) this.safeSend(c, ["currentNumber", this.currentNumber]);
    this.nextTickAt = Date.now() + this.intervalMillis;
  }

  // ===== Message Handlers (match Java client format) =====
  handleSetIdTarget(ws, id) {
    ws.idtarget = String(id || "");
    this.safeSend(ws, ["ack", "setIdTarget"]);
  }

  handlePing(ws, payload) {
    this.safeSend(ws, ["pong", payload ?? null, Date.now()]);
  }

  handleJoinRoom(ws, room) {
    this.assertValidRoom(room);
    ws.roomname = room;
    this.safeSend(ws, ["joined", room]);
  }

  handleChat(ws, room, noImageURL, username, message, usernameColor, chatTextColor) {
    if (!roomList.includes(room)) return;
    const arr = this._ensureMap(this.chatMessageBuffer, room, () => []);
    arr.push(["chat", room, noImageURL, username, message, usernameColor, chatTextColor]);
    this._scheduleSoon(200);
  }

  handlePrivate(ws, idtarget, noimageUrl, message, sender) {
    idtarget = String(idtarget || "");
    const arr = this._ensureMap(this.privateMessageBuffer, idtarget, () => []);
    arr.push(["private", ws.idtarget || "anon", noimageUrl, message, Date.now(), sender]);
    this._scheduleSoon(200);
  }

  handleSendNotif(ws, idtarget, noimageUrl, username, deskripsi) {
    const room = ws.roomname;
    if (!room) return;
    this.broadcastToRoom(room, ["notif", noimageUrl, username, deskripsi, Date.now()]);
  }

  handleUpdatePoint(ws, room, seat, x, y, fast) {
    if (!roomList.includes(room)) return;
    seat = Number(seat);
    const seatMap = this._ensureMap(this.pointUpdateBuffer, room, () => new Map());
    const arr = this._ensureMap(seatMap, seat, () => []);
    arr.push({ x: Number(x), y: Number(y), fast: Number(fast) });
    this._scheduleSoon(200);
  }

  handleUpdateKursi(ws, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda) {
    if (!roomList.includes(room)) return;
    seat = Number(seat);
    const seatMap = this.roomSeats.get(room);
    const info = {
      noimageUrl,
      namauser,
      color,
      itembawah: Number(itembawah),
      itematas: Number(itematas),
      vip: Number(vip),
      viptanda: Number(viptanda),
      points: []
    };
    seatMap.set(seat, info);

    const buf = this._ensureMap(this.updateKursiBuffer, room, () => new Map());
    buf.set(seat, { ...info });
    this._scheduleSoon(200);
  }

  handleRemoveKursi(ws, room, seat) {
    if (!roomList.includes(room)) return;
    seat = Number(seat);
    const seatMap = this.roomSeats.get(room);
    const info = seatMap.get(seat);
    if (info) {
      seatMap.set(seat, this.createEmptySeat());
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
    }
  }

  handleIsUserOnline(ws, userId, tanda) {
    const online = [...this.clients].some(c => c.idtarget === userId);
    this.safeSend(ws, ["userOnlineStatus", userId, online, tanda]);
  }

  // ===== Dispatcher =====
  handleMessage(ws, dataStr) {
    try {
      const data = JSON.parse(dataStr);
      if (!Array.isArray(data) || data.length === 0) return;
      const [evt, ...args] = data;
      switch (evt) {
        case "setIdTarget": this.handleSetIdTarget(ws, ...args); break;
        case "ping": this.handlePing(ws, ...args); break;
        case "joinRoom": this.handleJoinRoom(ws, ...args); break;
        case "chat": this.handleChat(ws, ...args); break;
        case "private": this.handlePrivate(ws, ...args); break;
        case "sendnotif": this.handleSendNotif(ws, ...args); break;
        case "updatePoint": this.handleUpdatePoint(ws, ...args); break;
        case "updateKursi": this.handleUpdateKursi(ws, ...args); break;
        case "removeKursiAndPoint": this.handleRemoveKursi(ws, ...args); break;
        case "isUserOnline": this.handleIsUserOnline(ws, ...args); break;
        case "getCurrentNumber": this.safeSend(ws, ["currentNumber", this.currentNumber]); break;
        default: this.safeSend(ws, ["error", "Unknown event: " + evt]); break;
      }
      this._scheduleSoon(250);
    } catch (err) {
      console.error("Error handling message:", err, "raw:", dataStr);
    }
  }

  // ===== WebSocket entry =====
  async fetch(request) {
    const upgrade = request.headers.get("upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected websocket", { status: 400 });
    }
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    server.accept();
    this.clients.add(server);

    server.addEventListener("message", ev => this.handleMessage(server, ev.data));
    server.addEventListener("close", () => this.clients.delete(server));
    server.addEventListener("error", () => this.clients.delete(server));

    this.safeSend(server, ["currentNumber", this.currentNumber]);
    return new Response(null, { status: 101, webSocket: client });
  }

  // ===== Alarm =====
  async alarm() {
    this.periodicFlush();
    const now = Date.now();
    if (now >= this.nextTickAt) this.tick();
    await this._scheduleAt(Math.min(this.nextTickAt, now + 1000));
  }
}

// ===== Worker Entry =====
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER.idFromName("global-chat");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(req);
  }
};
