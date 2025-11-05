// index.js – ChatServer Durable Object (Lengkap, siap deploy)
// Versi lengkap: lock kursi aman (tidak menimpa kursi yang sudah terisi)
// Filter kata dilarang dihapus sesuai permintaan

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

    this.lowcard = new LowCardGameManager(this);

    this.offlineUsers = new Map();
    this.offlineTimers = new Map();
    this.OFFLINE_TIMEOUT_MS = 5 * 60 * 1000;
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
    } catch (e) {}
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
    for (const [room, seatMap] of this.roomSeats) {
      let removed = false;
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          removed = true;
        }
      }
      if (removed) this.broadcastRoomUserCount(room);
    }
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

  scheduleOfflineRemoval(idtarget) {
    if (this.offlineTimers.has(idtarget)) return;
    const timeoutId = setTimeout(() => {
      const saved = this.offlineUsers.get(idtarget);
      if (saved && Date.now() - saved.timestamp >= this.OFFLINE_TIMEOUT_MS) {
        this.offlineUsers.delete(idtarget);
        this.offlineTimers.delete(idtarget);
        this.removeAllSeatsById(idtarget);
      } else {
        this.offlineTimers.delete(idtarget);
      }
    }, this.OFFLINE_TIMEOUT_MS + 10000);
    this.offlineTimers.set(idtarget, timeoutId);
  }

  cancelOfflineRemoval(idtarget) {
    if (this.offlineTimers.has(idtarget)) {
      clearTimeout(this.offlineTimers.get(idtarget));
      this.offlineTimers.delete(idtarget);
    }
    if (this.offlineUsers.has(idtarget)) this.offlineUsers.delete(idtarget);
  }

  handleMessage(ws, raw) {
    let data;
    try { data = JSON.parse(raw); } catch { return this.safeSend(ws, ["error", "Invalid JSON"]); }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    switch (evt) {
      case "setIdTarget": {
        const newId = data[1];
        this.cleanupClientById(newId);
        if (this.offlineUsers.has(newId)) this.cancelOfflineRemoval(newId);
        ws.idtarget = newId;
        this.safeSend(ws, ["setIdTargetAck", ws.idtarget]);
        if (this.privateMessageBuffer.has(ws.idtarget)) {
          for (const msg of this.privateMessageBuffer.get(ws.idtarget)) this.safeSend(ws, msg);
          this.privateMessageBuffer.delete(ws.idtarget);
        }
        if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
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
          if (c.idtarget === idtarget) { this.safeSend(c, notif); delivered = true; }
        }
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
        for (const c of this.clients) {
          if (c.idtarget === idt) { this.safeSend(c, out); delivered = true; }
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
  if (!roomList.includes(newRoom))
    return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);

  ws.roomname = newRoom;
  const seatMap = this.roomSeats.get(newRoom);

  // Cek kursi lama jika reconnect
  let foundSeat = null;
  if (ws.idtarget && this.userToSeat.has(ws.idtarget)) {
    const seatInfo = this.userToSeat.get(ws.idtarget);
    if (seatInfo.room === newRoom) foundSeat = seatInfo.seat;
  }

  // Jika belum ada kursi (user baru), lock kursi baru
  if (!foundSeat) {
    foundSeat = this.lockSeat(newRoom, ws);
    if (!foundSeat) return this.safeSend(ws, ["roomFull", newRoom]);
  }

  // Update ws.numkursi
  ws.numkursi = new Set([foundSeat]);

  // Simpan di userToSeat jika belum ada
  if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

  // **Langsung update kursi di state server agar muncul**
  const currentInfo = seatMap.get(foundSeat) || createEmptySeat();
  Object.assign(currentInfo, {
    namauser: ws.idtarget,
    noimageUrl: "",   // bisa diubah sesuai data client
    color: "#000000", // default warna
    itembawah: 0,
    itematas: 0,
    vip: 0,
    viptanda: 0
  });
  seatMap.set(foundSeat, currentInfo);

  // Tambahkan ke buffer untuk broadcast
  if (!this.updateKursiBuffer.has(newRoom)) this.updateKursiBuffer.set(newRoom, new Map());
  this.updateKursiBuffer.get(newRoom).set(foundSeat, { ...currentInfo, points: [] });

  // Kirim feedback ke client
  this.safeSend(ws, ["numberKursiSaya", foundSeat]);
  this.sendAllStateTo(ws, newRoom);
  this.broadcastRoomUserCount(newRoom);

  console.log(`✅ User ${ws.idtarget || "new"} joinRoom ${newRoom}, assigned seat: ${foundSeat}`);
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
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
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
        this.chatMessageBuffer.get(roomname).push([
          "gift", roomname, sender, receiver, giftName, Date.now()
        ]);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
        this.lowcard.handleEvent(ws, data);
        break;

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
    }
  }

  cleanupClient(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    const stillActive = Array.from(this.clients).some(c => c !== ws && c.idtarget === id);
    if (stillActive) {
      this.clients.delete(ws);
      return;
    }

    this.offlineUsers.set(id, { roomname: ws.roomname, timestamp: Date.now() });
    this.scheduleOfflineRemoval(id);

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
    this.clients.add(ws);

    ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));

    ws.addEventListener("close", () => {
      console.log(`❌ WebSocket closed: ${ws.idtarget || "unknown user"} — cleanup langsung`);
      this.cleanupClient(ws);
    });

    ws.addEventListener("error", (ev) => {
      console.log(`⚠️ WebSocket error (tidak cleanup langsung): ${ws.idtarget || "unknown user"}`, ev);

      if (ws.readyState === 1) this.safeSend(ws, ["reconnecting", "WebSocket error detected. Attempting reconnect..."]);
      if (!ws.idtarget) return;

      const now = Date.now();
      this.offlineUsers.set(ws.idtarget, { roomname: ws.roomname, timestamp: now });

      if (!this.offlineTimers.has(ws.idtarget)) {
        const timeoutMs = 10 * 1000;
        const timeoutId = setTimeout(() => {
          const offline = this.offlineUsers.get(ws.idtarget);
          if (offline && Date.now() - offline.timestamp >= timeoutMs) {
            console.log(`🗑️ Offline > 10 detik, cleanup WS ${ws.idtarget}`);
            this.removeAllSeatsById(ws.idtarget);
            this.offlineUsers.delete(ws.idtarget);
            this.offlineTimers.delete(ws.idtarget);
          }
        }, timeoutMs);
        this.offlineTimers.set(ws.idtarget, timeoutId);
      }
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
    if (new URL(req.url).pathname === "/health")
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    return new Response("WebSocket endpoint", { status: 200 });
  }
};

