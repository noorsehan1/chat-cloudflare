// ChatServer Durable Object (Cloudflare Workers) - FINAL COMPLETE (dengan variabel waktu)
import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "General","Indonesia","Chill Zone","Catch Up","Casual Vibes","Lounge Talk",
  "Easy Talk","Friendly Corner","The Hangout","Relax & Chat","Just Chillin","The Chatter Room"
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

// -----------------------------
// Waktu / interval (ubah di sini kalau mau tweak semua behavior waktu)
// -----------------------------
const LOCK_DURATION_MS = 60_000;            // waktu lock kursi untuk memungkinkan reconnect (default 60s)
const CLIENT_DISCONNECT_GRACE_MS = LOCK_DURATION_MS; // waktu tunggu sebelum kursi dihapus setelah disconnect (default sama dengan LOCK_DURATION_MS)
const TICK_INTERVAL_MS = 15 * 60 * 1000;    // interval tick untuk currentNumber
const FLUSH_INTERVAL_MS = 100;              // interval flush buffer
// -----------------------------

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();       // semua WebSocket aktif
    this.userToSeat = new Map();    // mapping id -> {room, seat} untuk user aktif (tidak termasuk yang di-lock)
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
    this.intervalMillis = TICK_INTERVAL_MS;
    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), FLUSH_INTERVAL_MS);

    // LowCard manager (pastikan file lowcard.js ada dan diekspor)
    this.lowcard = new LowCardGameManager(this);
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

  // Hapus lock yang sudah melebihi LOCK_DURATION_MS
  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > LOCK_DURATION_MS) {
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

  // Lock seat: cari kursi kosong, set namauser = __LOCK__id dan return seat number
  lockSeat(room, ws) {
    const seatMap = this.roomSeats.get(room);
    if (!ws.idtarget) return null;
    const now = Date.now();

    // cleanup lock yang sudah lebih dari LOCK_DURATION_MS
    for (const [seat, info] of seatMap) {
      if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > LOCK_DURATION_MS)
        Object.assign(info, createEmptySeat());
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (!k) continue;
      if (k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = now;
        // pastikan kita simpan mapping temporer (bisa dihapus saat lock)
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        return i;
      }
    }
    return null;
  }

  // Kirim semua point & kursi meta ke satu ws
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

  broadcastFullState(room) {
    const seatMap = this.roomSeats.get(room);
    const updates = [];
    const allPoints = [];
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
        updates.push([seat, {
          noimageUrl: info.noimageUrl,
          namauser: info.namauser,
          color: info.color,
          itembawah: info.itembawah,
          itematas: info.itematas,
          vip: info.vip,
          viptanda: info.viptanda
        }]);
      }
      for (const p of info.points) allPoints.push({ seat, ...p });
    }
    if (updates.length > 0) this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
    if (allPoints.length > 0) this.broadcastToRoom(room, ["allPointsList", room, allPoints]);
  }

  // Cleanup koneksi lama tanpa menghancurkan kursi: netralkan idtarget lalu close
  cleanupClientById(idtarget) {
    const oldSockets = Array.from(this.clients).filter(c => c.idtarget === idtarget);
    if (oldSockets.length === 0) return;

    for (const old of oldSockets) {
      try {
        this.safeSend(old, ["duplicateLogin", "Session baru menggantikan koneksi lama"]);
        // netralkan idtarget dulu supaya cleanupClient tidak mengunci/hapus kursi
        old.idtarget = undefined;
        old.close(4000, "Duplicate login — old session closed");
      } catch {}
      this.clients.delete(old);
    }
  }

  // Hapus semua kursi (dipanggil setelah lock expired) — tetap broadcast removeKursi
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
    // pastikan mapping juga dihapus
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

  // Main message handler
  handleMessage(ws, raw) {
    let data;
    try { data = JSON.parse(raw); } catch { return this.safeSend(ws, ["error", "Invalid JSON"]); }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    switch (evt) {

      // setIdTarget: server akan coba restore kursi berdasarkan __LOCK__<id>
      // client boleh kirim optional room: ["setIdTarget", id, roomname]
    case "setIdTarget": {
  const newId = data[1];
  const maybeRoom = (data.length > 2 && data[2] !== null) ? data[2] : null;

  // Jangan cleanup dulu — kita coba restore dulu
  ws.idtarget = newId;

  let restored = false;
  const now = Date.now();

  // Jika client kirim preferensi room, cek room itu dulu
  if (maybeRoom && this.roomSeats.has(maybeRoom)) {
    const seatMap = this.roomSeats.get(maybeRoom);
    for (const [seat, info] of seatMap) {
      if (info.namauser === "__LOCK__" + newId) {
        // Cek apakah masih dalam waktu lock
        if (info.lockTime && now - info.lockTime < LOCK_DURATION_MS) {
          // Restore kursi
          info.namauser = newId;
          info.lockTime = undefined;

          ws.roomname = maybeRoom;
          ws.numkursi = new Set([seat]);

          this.userToSeat.set(newId, { room: maybeRoom, seat });

          this.safeSend(ws, ["numberKursiSaya", seat]);
          this.sendAllStateTo(ws, maybeRoom);
          this.broadcastRoomUserCount(maybeRoom);

          restored = true;
        } else {
          // Lock sudah kadaluarsa — reset kursi
          Object.assign(info, createEmptySeat());
        }
        break;
      }
    }
  }

  // Jika belum restore, scan semua room
  if (!restored) {
    for (const [room, seatMap] of this.roomSeats) {
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + newId) {
          // Cek apakah masih dalam waktu lock
          if (info.lockTime && now - info.lockTime < LOCK_DURATION_MS) {
            // Restore kursi
            info.namauser = newId;
            info.lockTime = undefined;

            ws.roomname = room;
            ws.numkursi = new Set([seat]);

            this.userToSeat.set(newId, { room, seat });

            this.safeSend(ws, ["numberKursiSaya", seat]);
            this.sendAllStateTo(ws, room);
            this.broadcastRoomUserCount(room);

            restored = true;
          } else {
            // Lock kadaluarsa — reset kursi
            Object.assign(info, createEmptySeat());
          }
          break;
        }
      }
      if (restored) break;
    }
  }

  // Setelah restore sukses, baru bersihkan koneksi lama jika ada
  if (restored) {
    this.cleanupClientById(newId);
  }

  // Jika gagal restore => minta client joinRoom lagi
  if (!restored) {
    this.safeSend(ws, ["needJoinRoomAgain"]);
  }

  // Kirim private message buffer jika ada
  if (this.privateMessageBuffer.has(newId)) {
    for (const msg of this.privateMessageBuffer.get(newId)) this.safeSend(ws, msg);
    this.privateMessageBuffer.delete(newId);
  }

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
            try {
              old.idtarget = undefined;
              old.close(4000, "Duplicate login — old session closed");
              this.clients.delete(old);
            } catch {}
          }
        }
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
        if (!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);

        // jika ws.idtarget punya seat di tempat lain, hapus seat lama
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

      // Semua event LowCard diteruskan
      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        try {
          this.lowcard.handleEvent(ws, data);
        } catch (e) {
          this.safeSend(ws, ["error", "LowCard error"]);
        }
        break;

      default:
        this.safeSend(ws, ["error", "Unknown event: " + evt]);
    }
  }

  // Cleanup client saat socket close/error
  cleanupClient(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    // Jika masih punya koneksi lain, jangan hapus kursi
    const stillActive = Array.from(this.clients).some(c => c !== ws && c.idtarget === id);
    if (stillActive) {
      this.clients.delete(ws);
      return;
    }

    // Jika user punya seat (ws.numkursi), lock kursi dan hapus mapping userToSeat
    const seatInfo = this.userToSeat.get(id);
    if (seatInfo) {
      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);
      if (seatMap && seatMap.has(seat) && ws.readyState !== 1) {
        const info = seatMap.get(seat);
        // set lock
        info.namauser = "__LOCK__" + id;
        info.lockTime = Date.now();
      }

      // hapus mapping sementara; saat restore setIdTarget akan menulis mapping lagi
      this.userToSeat.delete(id);

      // setelah CLIENT_DISCONNECT_GRACE_MS, jika belum reconnect, hapus kursi
      setTimeout(() => {
        const stillActiveNow = Array.from(this.clients).some(c => c.idtarget === id);
        if (!stillActiveNow) {
          this.removeAllSeatsById(id);
          try { ws.close(4000, `User disconnected >${Math.floor(CLIENT_DISCONNECT_GRACE_MS/1000)}s`); } catch {}
          this.clients.delete(ws);
        }
      }, CLIENT_DISCONNECT_GRACE_MS);
    }

    ws.numkursi?.clear?.();
    ws.roomname = undefined;
    ws.idtarget = undefined;
    this.clients.delete(ws);
  }

  // fetch untuk upgrade websocket
  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
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
