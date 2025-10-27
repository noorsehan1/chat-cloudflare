// ChatServer Durable Object (Bahasa Indonesia)
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

    // Klien WebSocket aktif
    this.clients = new Set();

    // Pemetaan idtarget => { room, seat }
    this.userToSeat = new Map();

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    // Buffer untuk mengumpulkan update yang dikirim periodik
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    // Timer / tick
    this.currentNumber = 1;
    this.maxNumber = 6;
    // default tick interval lama (dibiarkan sama): 15 menit
    this.intervalMillis = 15 * 60 * 1000;

    // lastTickTime untuk mengontrol tick() berdasarkan intervalMillis
    this._lastTickTime = Date.now();

    // main loop: gabung flush + tick (200ms). 
    // Simpan ke dua variable supaya nama tetap ada (_tickTimer, _flushTimer)
    this._tickTimer = this._flushTimer = setInterval(() => {
      try {
        // flush dan housekeeping
        this._mainLoopIteration();
      } catch (e) {
        // jangan lempar error sehingga mematikan interval
        // opsional: console.error("mainLoop error", e);
      }
    }, 200);

    // LowCard game manager (diberikan dari file lain)
    this.lowcard = new LowCardGameManager(this);
  }

  // ---------------- Helpers ----------------

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        // kirim langsung jika open
        ws.send(JSON.stringify(arr));
      } else if (ws.readyState === 0) {
        // CONNECTING — coba sekali lagi setelah singkat
        setTimeout(() => {
          try {
            if (ws.readyState === 1) ws.send(JSON.stringify(arr));
          } catch (e) {
            // abaikan
          }
        }, 300);
      } else {
        // CLOSING / CLOSED -> abaikan, event close/error akan handle cleanup
      }
    } catch (e) {
      // Jangan cleanup di sini untuk menghindari disconnect agresif
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of Array.from(this.clients)) {
      try {
        if (c.roomname === room && c.readyState === 1) {
          // throttle ringan per client: minimal 15ms antar send
          const now = Date.now();
          if (!c._lastSend || now - c._lastSend > 15) {
            this.safeSend(c, msg);
            c._lastSend = now;
          }
        }
      } catch (e) {
        // Abaikan error per-client agar tidak menghentikan broadcast
      }
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

  // Batasi chat buffer untuk menghindari flood (max 100 pesan per room)
  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      try {
        if (!Array.isArray(messages)) continue;
        if (messages.length > 100) {
          // simpan hanya 100 pesan terakhir
          messages.splice(0, messages.length - 100);
        }
        for (const msg of messages) {
          try { this.broadcastToRoom(room, msg); } catch (e) {}
        }
        messages.length = 0;
      } catch (e) {
        // abaikan dan lanjut
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      try {
        const updates = [];
        for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
          if (!seatMapUpdates.has(seat)) continue;
          const info = seatMapUpdates.get(seat);
          const { points, ...rest } = info;
          updates.push([seat, rest]);
        }
        if (updates.length > 0) {
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
        }
        seatMapUpdates.clear();
      } catch (e) {
        // abaikan
      }
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of Array.from(this.clients)) {
      try { this.safeSend(c, ["currentNumber", this.currentNumber]); } catch (e) {}
    }
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seat, info] of seatMap) {
        try {
          if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
            Object.assign(info, createEmptySeat());
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.broadcastRoomUserCount(room);
          }
        } catch (e) {}
      }
    }
  }

  // main loop iteration (dipanggil tiap 200ms)
  _mainLoopIteration() {
    // flush & housekeeping
    this.flushKursiUpdates();
    this.flushChatBuffer();
    this.cleanExpiredLocks();

    // private message delivery (satu per klien jika ada)
    for (const [id, msgs] of Array.from(this.privateMessageBuffer)) {
      try {
        for (const c of this.clients) {
          if (c.idtarget === id) {
            for (const m of msgs) {
              try { this.safeSend(c, m); } catch (e) {}
            }
            this.privateMessageBuffer.delete(id);
            if (c.roomname) this.broadcastRoomUserCount(c.roomname);
            break;
          }
        }
      } catch (e) {}
    }

    // panggil tick() berdasarkan intervalMillis (awalnya 15 menit)
    const now = Date.now();
    if (now - this._lastTickTime >= this.intervalMillis) {
      this._lastTickTime = now;
      try { this.tick(); } catch (e) {}
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
      try {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000)
          Object.assign(info, createEmptySeat());
      } catch (e) {}
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

  // cleanupClientById sekarang mendukung exclude ws agar setIdTarget tidak menghapus kursi saat reconnect cepat
  cleanupClientById(idtarget, excludeWs) {
    for (const c of Array.from(this.clients)) {
      if (c.idtarget === idtarget && c !== excludeWs) this.cleanupClient(c);
    }
  }

  removeAllSeatsById(idtarget) {
    for (const [room, seatMap] of this.roomSeats) {
      let removed = false;
      for (const [seat, info] of seatMap) {
        try {
          if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
            Object.assign(seatMap.get(seat), createEmptySeat());
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            removed = true;
          }
        } catch (e) {}
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

  handleMessage(ws, raw) {
    let data;
    try { data = JSON.parse(raw); } catch { return this.safeSend(ws, ["error", "Invalid JSON"]); }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    // tangani ping singkat di awal untuk menjaga heartbeat klien jika diperlukan
    if (evt === "ping") {
      try {
        ws._lastPing = Date.now();
        const pingId = data[1];
        if (pingId && ws.idtarget === pingId) this.safeSend(ws, ["pong"]);
      } catch (e) {}
      return;
    }

    try {
      switch (evt) {
        case "setIdTarget": {
          const newId = data[1];
          // pertama set di ws (agar reconnect cepat tidak terhapus)
          ws.idtarget = newId;
          // hapus client lain yang masih memakai id ini, kecuali ws saat ini
          this.cleanupClientById(newId, ws);
          this.safeSend(ws, ["setIdTargetAck", ws.idtarget]);
          if (this.privateMessageBuffer.has(ws.idtarget)) {
            for (const msg of this.privateMessageBuffer.get(ws.idtarget)) this.safeSend(ws, msg);
            this.privateMessageBuffer.delete(ws.idtarget);
          }
          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
          break;
        }

        case "pongClient": {
          // jika klien mengirim pong; perbarui last ping (opsional)
          ws._lastPing = Date.now();
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
    } catch (e) {
      // jika ada error di handler, laporkan tapi jangan crash server
      // console.error("handleMessage error:", e);
      try { this.safeSend(ws, ["error", "Internal handler error"]); } catch (e2) {}
    }
  }

  cleanupClient(ws) {
    // anti-double-cleanup
    if (ws.isCleaning) return;
    ws.isCleaning = true;

    try {
      const id = ws.idtarget;
      if (id) {
        // ✅ FIX: pastikan tidak hapus kursi jika user masih punya koneksi aktif lain (reconnect cepat)
        const stillActive = Array.from(this.clients).some(c => c !== ws && c.idtarget === id);
        if (stillActive) {
          // hanya hapus koneksi ini dari set klien, jangan hapus kursi
          this.clients.delete(ws);
          return;
        }
        this.removeAllSeatsById(id);
      }
      ws.numkursi?.clear?.();
      this.clients.delete(ws);
      ws.roomname = undefined;
      ws.idtarget = undefined;
    } catch (e) {
      // console.error("cleanupClient error:", e);
    } finally {
      ws.isCleaning = false;
    }
  }

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

    ws.addEventListener("message", (ev) => {
      try {
        this.handleMessage(ws, ev.data);
      } catch (e) {
        // jangan crash DO karena pesan bermasalah
        // console.error("ws message handler error:", e);
      }
    });
    ws.addEventListener("close", () => {
      try { this.cleanupClient(ws); } catch (e) {}
    });
    ws.addEventListener("error", () => {
      try { this.cleanupClient(ws); } catch (e) {}
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
