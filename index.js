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

    // ✅ Grace period untuk reconnect
    this.gracePeriod = 15000; // 10 detik
    this.pendingRemove = new Map(); // Map<idtarget, timeout>

    // ✅ Ping/Pong mechanism untuk Android client
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

  // ✅ Handle ping/pong untuk Android client
  handlePing(ws, idtarget) {
    // Clear existing timeout
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
    }
    
    // Send ping response
    this.safeSend(ws, ["ping", idtarget]);
    
    // Set new timeout
    const timeout = setTimeout(() => {
      // Jika tidak ada pong dalam waktu yang ditentukan, anggap client disconnect
      if (this.clients.has(ws)) {
        this.cleanupClient(ws);
      }
      this.pingTimeouts.delete(idtarget);
    }, this.PING_TIMEOUT);
    
    this.pingTimeouts.set(idtarget, timeout);
  }

  handlePong(ws, idtarget) {
    // Clear ping timeout ketika menerima pong
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
  }

  handleOnDestroy(ws, idtarget) {
  // ✅ Tandai bahwa user keluar manual (bukan disconnect mendadak)
  ws.isDestroyed = true;

  // ✅ Hapus semua data user langsung (tidak pakai grace period)
  this.removeAllSeatsById(idtarget);

  // ✅ Hapus dari daftar client aktif
  this.clients.delete(ws);

  // Bersihkan timeout ping jika ada
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
      // ✅ Tambahkan handler untuk ping/pong/onDestroy
      case "ping": {
        const idtarget = data[1];
        this.handlePing(ws, idtarget);
        break;
      }

      case "pong": {
        const idtarget = data[1];
        this.handlePong(ws, idtarget);
        break;
      }

      case "onDestroy": {
        const idtarget = ws.idtarget;
        this.handleOnDestroy(ws, idtarget);
        break;
      }

      case "setIdTarget": {
  const newId = data[1];
  
  // Cleanup duplicate connections
  this.cleanupClientById(newId);

  // Set ID baru
  ws.idtarget = newId;

  // ✅ CEK APAKAH PERLU JOIN ROOM ATAU TIDAK
  const previousSeatInfo = this.userToSeat.get(newId);
  const isInGracePeriod = this.pendingRemove.has(newId);

  if (isInGracePeriod) {
    // 🔹 CANCEL GRACE PERIOD - Reconnect cepat
    clearTimeout(this.pendingRemove.get(newId));
    this.pendingRemove.delete(newId);
  }

  if (previousSeatInfo && isInGracePeriod) {
    // 🔹 RECONNECT CEPAT: Kembalikan state sebelumnya
    const { room, seat } = previousSeatInfo;
    ws.roomname = room;
    ws.numkursi = new Set([seat]);
    
    // ✅ HANYA kirim state room, TANPA numberKursiSaya
    this.sendAllStateTo(ws, room);
    
    // Update ownership kursi
    const seatMap = this.roomSeats.get(room);
    if (seatMap && seatMap.has(seat)) {
      const seatInfo = seatMap.get(seat);
      seatInfo.namauser = newId; // Kembalikan ownership
    }
    
    // ✅ Client akan tahu dia dapat kursi dari allUpdateKursiList
    
  } else {
    // 🔹 PERLU JOIN ROOM: User baru ATAU reconnect terlambat
    if (previousSeatInfo) {
      // Reconnect terlambat - hapus mapping lama
      this.userToSeat.delete(newId);
    }
    this.safeSend(ws, ["needJoinRoom", "Silakan join room"]);
  }

  // Kirim buffered messages
  if (this.privateMessageBuffer.has(newId)) {
    for (const msg of this.privateMessageBuffer.get(newId)) this.safeSend(ws, msg);
    this.privateMessageBuffer.delete(newId);
  }

  if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
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
        const username = data[1];
        const tanda = data[2] ?? "";

        const activeSockets = Array.from(this.clients).filter(c => c.idtarget === username);
        const online = activeSockets.length > 0;

        this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);

        if (activeSockets.length > 1) {
          const newest = activeSockets[activeSockets.length - 1];
          const oldSockets = activeSockets.slice(0, -1);

          const userSeatInfo = this.userToSeat.get(username);

          if (userSeatInfo) {
            const { room, seat } = userSeatInfo;
            const seatMap = this.roomSeats.get(room);
            if (seatMap && seatMap.has(seat)) {
              Object.assign(seatMap.get(seat), createEmptySeat());
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.broadcastRoomUserCount(room);
            }
            this.userToSeat.delete(username);
          }

          for (const old of oldSockets) {
            try { old.close(4000, "Duplicate login — old session closed"); this.clients.delete(old); } catch {}
          }
        }
        break;
      }

      case "getAllRoomsUserCount": this.handleGetAllRoomsUserCount(ws); break;
      case "getCurrentNumber": this.safeSend(ws, ["currentNumber", this.currentNumber]); break;
      case "getOnlineUsers": this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]); break;
      case "getRoomOnlineUsers": {
        const roomName = data[1];
        if (!roomList.includes(roomName)) return this.safeSend(ws, ["error", "Unknown room"]);
        this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
        break;
      }

      case "joinRoom": {
        const newRoom = data[1];
        if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
        if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);
        ws.roomname = newRoom;
        const seatMap = this.roomSeats.get(newRoom);
        const foundSeat = this.lockSeat(newRoom, ws);
        if (foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);
        ws.numkursi = new Set([foundSeat]);
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        if (ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
        this.sendAllStateTo(ws, newRoom);
        this.broadcastRoomUserCount(newRoom);

        // Jika user join room after pendingRemove existed (reconnect success), cancel pending removal
        if (ws.idtarget && this.pendingRemove.has(ws.idtarget)) {
          clearTimeout(this.pendingRemove.get(ws.idtarget));
          this.pendingRemove.delete(ws.idtarget);
        }
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
        this.chatMessageBuffer.get(roomname).push(["gift", roomname, sender, receiver, giftName, Date.now()]);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        this.lowcard.handleEvent(ws, data);
        break;

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
    }
  }

  cleanupClient(ws) {
    const id = ws.idtarget;
    if (id) {
        ws.isDestroyed = false;
      // ✅ Clear ping timeout
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      // ✅ Aman untuk reconnect cepat: hanya hapus kursi jika user tidak punya koneksi lain
      const stillActive = Array.from(this.clients).some(c => c !== ws && c.idtarget === id);
      if (stillActive) {
        this.clients.delete(ws);
        return;
      }

      // ❗ Jangan langsung hapus kursi di sini
      // Mulai gracePeriod timer (jika user reconnect dalam periode ini, kita batalkan)
      if (this.pendingRemove.has(id)) clearTimeout(this.pendingRemove.get(id));

      const timeout = setTimeout(() => {
        // kalau belum reconnect dalam gracePeriod
        this.removeAllSeatsById(id);
        this.pendingRemove.delete(id);
      }, this.gracePeriod);

      this.pendingRemove.set(id, timeout);
    }

    // Hapus koneksi dari list
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

  // ✅ Set properti default untuk setiap koneksi baru
  ws.roomname = undefined;
  ws.idtarget = undefined;
  ws.numkursi = new Set();
  ws.isDestroyed = false; // 🟢 default: belum keluar manual

  // Tambahkan koneksi ke daftar aktif
  this.clients.add(ws);

  // ✅ Handler pesan masuk
  ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));

  // ✅ Handler koneksi ditutup
  ws.addEventListener("close", () => {
    if (ws.isDestroyed) {
      // 🟢 User keluar manual — sudah dibersihkan di handleOnDestroy
      this.clients.delete(ws);
      return;
    }

    // 🔵 User disconnect mendadak — aktifkan grace period reconnect
    this.cleanupClient(ws);
  });

  // ✅ Handler error (anggap sama seperti disconnect)
  ws.addEventListener("error", () => this.cleanupClient(ws));

  return new Response(null, { status: 101, webSocket: client });
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


