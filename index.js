import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

// Simple seat structure
function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "",
    itembawah: 0, itematas: 0, vip: 0, viptanda: 0,
    lastPoint: null
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

    // Simple buffers like the lightweight version
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    // Minimal timers
    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), 50); // Faster flush

    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);
  }

  // Simplified safeSend
  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (err) {
      this.cleanupClient(ws);
    }
    return false;
  }

  // Direct broadcast - no complex queue
  broadcastToRoom(room, msg) {
    let sent = 0;
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        if (this.safeSend(c, msg)) sent++;
      }
    }
    return sent;
  }

  getJumlahRoom() {
    const cnt = {};
    for (const room of roomList) {
      cnt[room] = 0;
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

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      for (const msg of messages) {
        this.broadcastToRoom(room, msg);
      }
      this.chatMessageBuffer.set(room, []);
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
        this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      }
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
    this.flushChatBuffer();
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    const msg = ["currentNumber", this.currentNumber];
    for (const c of this.clients) {
      this.safeSend(c, msg);
    }
  }

  // Simplified seat locking
  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    const seatMap = this.roomSeats.get(room);
    
    // Check if already in room
    const existing = this.userToSeat.get(ws.idtarget);
    if (existing && existing.room === room) {
      return existing.seat;
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    const seatMap = this.roomSeats.get(room);
    const allKursiMeta = {};
    const lastPointsData = [];

    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
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

    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    const count = this.getJumlahRoom()[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
  }

  removeAllSeatsById(idtarget) {
    for (const [room, seatMap] of this.roomSeats) {
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
        }
      }
    }
    this.userToSeat.delete(idtarget);
  }

  // Fast message handler
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

    // Fast event routing
    switch (evt) {
      case "vipbadge":
      case "removeVipBadge":
      case "getAllVipBadges":
        this.vipManager.handleEvent(ws, data);
        break;

      case "setIdTarget": {
        const newId = data[1];
        if (ws.idtarget && ws.idtarget !== newId) {
          this.removeAllSeatsById(ws.idtarget);
        }
        ws.idtarget = newId;
        this.safeSend(ws, ["setIdTargetAck", newId]);
        break;
      }

      case "isInRoom": {
        const idtarget = ws.idtarget;
        if (!idtarget) {
          this.safeSend(ws, ["inRoomStatus", false]);
          return;
        }
        const seatInfo = this.userToSeat.get(idtarget);
        const isInRoom = seatInfo && this.roomSeats.get(seatInfo.room)?.get(seatInfo.seat)?.namauser === idtarget;
        this.safeSend(ws, ["inRoomStatus", isInRoom]);
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

      case "getAllRoomsUserCount": {
        const allCounts = this.getJumlahRoom();
        const result = roomList.map(room => [room, allCounts[room]]);
        this.safeSend(ws, ["allRoomsUserCount", result]);
        break;
      }

      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;

      case "joinRoom": {
        const newRoom = data[1];
        if (!roomList.includes(newRoom)) {
          this.safeSend(ws, ["error", "Unknown room"]);
          return;
        }

        if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);

        ws.roomname = newRoom;
        const foundSeat = this.lockSeat(newRoom, ws);

        if (foundSeat === null) {
          this.safeSend(ws, ["roomFull", newRoom]);
          return;
        }

        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);

        this.sendAllStateTo(ws, newRoom);
        this.vipManager.getAllVipBadges(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);
        break;
      }

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (!roomList.includes(roomname)) return;

        if (!this.chatMessageBuffer.has(roomname)) {
          this.chatMessageBuffer.set(roomname, []);
        }
        this.chatMessageBuffer.get(roomname)
          .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
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
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (!roomList.includes(room)) return;

        const seatMap = this.roomSeats.get(room);
        Object.assign(seatMap.get(seat), {
          noimageUrl, namauser, color, itembawah, itematas,
          vip: vip || 0,
          viptanda: viptanda || 0,
          lastPoint: null
        });

        if (!this.updateKursiBuffer.has(room)) {
          this.updateKursiBuffer.set(room, new Map());
        }
        this.updateKursiBuffer.get(room).set(seat, { ...seatMap.get(seat) });
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (!roomList.includes(roomname)) return;
        if (!this.chatMessageBuffer.has(roomname)) {
          this.chatMessageBuffer.set(roomname, []);
        }
        this.chatMessageBuffer.get(roomname)
          .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd": {
        const room = ws.roomname;
        if (room === "LowCard") {
          setTimeout(() => this.lowcard.handleEvent(ws, data), 0);
        }
        break;
      }
    }
  }

  // Simple cleanup like lightweight version
  cleanupClient(ws) {
    const id = ws.idtarget;
    if (id) {
      this.removeAllSeatsById(id);
    }

    // Clean room seats
    const room = ws.roomname;
    const kursis = ws.numkursi;
    if (room && kursis && this.roomSeats.has(room)) {
      const seatMap = this.roomSeats.get(room);
      for (const seat of kursis) {
        Object.assign(seatMap.get(seat), createEmptySeat());
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
      }
      this.broadcastRoomUserCount(room);
    }

    this.clients.delete(ws);
    ws.numkursi?.clear?.();
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    await server.accept();

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

export default {
  async fetch(req, env) {
    try {
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      if (new URL(req.url).pathname === "/health") {
        return new Response("ok", { status: 200 });
      }
      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
