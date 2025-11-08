// ChatServer Durable Object (Bahasa Indonesia)
// Versi lengkap: aman untuk reconnect cepat, kursi hanya dihapus jika user benar-benar disconnect

import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "General","Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
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
    this.clients = new Set();
    this.userToSeat = new Map();

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;
    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), 100);

    this.lowcard = new LowCardGameManager(this);

    this.gracePeriod = 15000; // 15 detik grace period untuk reconnect
    this.pendingRemove = new Map(); // Map<idtarget, timeout>
    this.pingTimeouts = new Map(); // Map<idtarget, timeout>
    this.PING_TIMEOUT = 30000; // 30 detik
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) ws.send(JSON.stringify(arr));
      else if (ws.readyState === 0) {
        setTimeout(() => {
          if (ws.readyState === 1) {
            try { ws.send(JSON.stringify(arr)); } catch {}
          }
        }, 300);
      }
    } catch {}
  }

  broadcastToRoom(room, msg) {
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
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      for (const msg of messages) this.broadcastToRoom(room, msg);
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
      if (updates.length > 0)
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
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
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
    this.flushChatBuffer();
    this.cleanExpiredLocks();

    for (const [id, msgs] of Array.from(this.privateMessageBuffer)) {
      for (const c of this.clients) {
        if (c.idtarget === id) {
          for (const m of msgs) this.safeSend(c, m);
          this.privateMessageBuffer.delete(id);
          if (c.roomname) this.broadcastRoomUserCount(c.roomname);
        }
      }
    }
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;
    const now = Date.now();

    for (const [seat, info] of seatMap) {
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000)
        Object.assign(info, createEmptySeat());
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
      if (c.idtarget === idtarget) this.cleanupClient(c);
    }
  }

  removeAllSeatsById(idtarget) {
    const seatInfo = this.userToSeat.get(idtarget);
    if (!seatInfo) return;

    const { room, seat } = seatInfo;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    if (seatMap.has(seat)) {
      Object.assign(seatMap.get(seat), createEmptySeat());
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);
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

  handlePing(ws, idtarget) {
    if (this.pingTimeouts.has(idtarget)) clearTimeout(this.pingTimeouts.get(idtarget));
    this.safeSend(ws, ["ping", idtarget]);
    const timeout = setTimeout(() => {
      if (this.clients.has(ws)) this.cleanupClient(ws);
      this.pingTimeouts.delete(idtarget);
    }, this.PING_TIMEOUT);
    this.pingTimeouts.set(idtarget, timeout);
  }

  handlePong(ws, idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
  }

  handleOnDestroy(ws, idtarget) {
    ws.isDestroyed = true;
    this.removeAllSeatsById(idtarget);
    this.clients.delete(ws);
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  handleMessage(ws, raw) {
    let data;
    try { data = JSON.parse(raw); } catch { return this.safeSend(ws, ["error", "Invalid JSON"]); }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    switch (evt) {
      case "ping": this.handlePing(ws, data[1]); break;
      case "pong": this.handlePong(ws, data[1]); break;
      case "onDestroy": this.handleOnDestroy(ws, ws.idtarget); break;
      case "setIdTarget": this.handleSetIdTarget(ws, data[1]); break;
      case "sendnotif": this.handleSendNotif(ws, data); break;
      case "private": this.handlePrivateMessage(ws, data); break;
      case "isUserOnline": this.handleIsUserOnline(ws, data); break;
      case "getAllRoomsUserCount": this.handleGetAllRoomsUserCount(ws); break;
      case "getCurrentNumber": this.safeSend(ws, ["currentNumber", this.currentNumber]); break;
      case "getOnlineUsers": this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]); break;
      case "getRoomOnlineUsers": this.handleGetRoomOnlineUsers(ws, data); break;
      case "joinRoom": this.handleJoinRoom(ws, data); break;
      case "chat": this.handleChat(ws, data); break;
      case "updatePoint": this.handleUpdatePoint(ws, data); break;
      case "removeKursiAndPoint": this.handleRemoveKursiAndPoint(ws, data); break;
      case "updateKursi": this.handleUpdateKursi(ws, data); break;
      case "gift": this.handleGift(ws, data); break;
      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        this.lowcard.handleEvent(ws, data);
        break;
      default: this.safeSend(ws, ["error", "Unknown event"]);
    }
  }

  cleanupClient(ws) {
    const id = ws.idtarget;
    if (id) {
      ws.isDestroyed = false;
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      const stillActive = Array.from(this.clients).some(c => c !== ws && c.idtarget === id);
      if (stillActive) {
        this.clients.delete(ws);
        return;
      }

      if (this.pendingRemove.has(id)) clearTimeout(this.pendingRemove.get(id));

      const timeout = setTimeout(() => {
        this.removeAllSeatsById(id);
        this.pendingRemove.delete(id);
      }, this.gracePeriod);

      this.pendingRemove.set(id, timeout);
    }

    ws.numkursi?.clear?.();
    this.clients.delete(ws);
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket")
      return new Response("Expected WebSocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const ws = server;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    ws.isDestroyed = false;

    this.clients.add(ws);
    ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));
    ws.addEventListener("close", () => {
      if (ws.isDestroyed) {
        this.clients.delete(ws);
        return;
      }
      this.cleanupClient(ws);
    });
    ws.addEventListener("error", () => this.cleanupClient(ws));

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
    if (new URL(req.url).pathname === "/health")
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    return new Response("WebSocket endpoint", { status: 200 });
  }
};
