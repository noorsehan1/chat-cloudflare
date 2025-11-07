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

    // Low card game manager terintegrasi
    this.lowcard = new LowCardGameManager(this);

    // Penyimpanan untuk user yang sementara disconnect (kursi tidak dihapus langsung)
    this.offlineUsers = new Map();
    this.offlineTimers = new Map();
    this.OFFLINE_TIMEOUT_MS = 30 * 1000;

    console.log("[ChatServer] initialized");
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
      } else if (ws.readyState === 0) {
        // Jika belum OPEN, beri sedikit waktu
        setTimeout(() => {
          try {
            if (ws.readyState === 1) {
              ws.send(JSON.stringify(arr));
            } else {
              console.log("[safeSend] ws not ready after timeout", ws.idtarget);
              // ⚠️ PERBAIKAN: Jangan panggil cleanupondestroy, biarkan reconnect process handle
            }
          } catch (e) {
            // ⚠️ PERBAIKAN: Jangan panggil cleanupondestroy untuk error sementara
          }
        }, 300);
      }
      // ⚠️ PERBAIKAN: Jangan panggil cleanupondestroy untuk state closed/closing
    } catch (e) {
      console.log("[safeSend] error sending:", e);
      // ⚠️ PERBAIKAN: Jangan panggil cleanupondestroy untuk error send
    }
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
          console.log(`[cleanExpiredLocks] expired lock removed: ${room}#${seat}`);
        }
      }
    }
  }

  periodicFlush() {
    // ⚠️ PERBAIKAN PENTING: Gunakan cleanupClient, BUKAN cleanupondestroy
    for (const c of Array.from(this.clients)) {
      if (c.readyState !== 1) {
        console.log("[periodicFlush] found non-open ws, gentle cleanup", c.idtarget);
        this.cleanupClient(c); // ✅ GUNAKAN cleanupClient untuk simpan state offline
      }
    }

    this.flushKursiUpdates();
    this.flushChatBuffer();
    this.cleanExpiredLocks();
    this.checkOfflineUsers();

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

  checkOfflineUsers() {
    const now = Date.now();
    for (const [id, saved] of this.offlineUsers.entries()) {
      if (now - saved.timestamp >= this.OFFLINE_TIMEOUT_MS) {
        console.log(`[checkOfflineUsers] offline timeout, removing seats for ${id}`);
        this.offlineUsers.delete(id);
        this.offlineTimers.delete(id);
        this.removeAllSeatsById(id);
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
        console.log(`[lockSeat] ${ws.idtarget} locked ${room}#${i}`);
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
      if (c.idtarget === idtarget) {
        try { c.close(4000, "Duplicate connection cleanup"); } catch {}
        console.log(`[cleanupClientById] cleaning existing connection for ${idtarget}`);
        this.cleanupondestroy(c); // langsung hapus total untuk duplicate
      }
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
          console.log(`[removeAllSeatsById] removed seat ${room}#${seat} for ${idtarget}`);
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
        console.log(`[scheduleOfflineRemoval] final timeout, removing seats for ${idtarget}`);
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
      console.log(`[setIdTarget] ${newId} setting id on ws`);
      this.cleanupClientById(newId);
      ws.idtarget = newId;

      // Kirim private messages yang tertunda
      if (this.privateMessageBuffer.has(ws.idtarget)) {
        for (const msg of this.privateMessageBuffer.get(ws.idtarget)) this.safeSend(ws, msg);
        this.privateMessageBuffer.delete(ws.idtarget);
      }

      // Cek apakah user masih punya data offline
      const offline = this.offlineUsers.get(newId);
      if (offline) {
        // === Restore kursi lama ===
        const { roomname, seats } = offline;
        ws.roomname = roomname;
        ws.numkursi = new Set(seats);

        const seatMap = this.roomSeats.get(roomname);
        for (const s of seats) {
          const info = seatMap.get(s);
          if (info.namauser === "" || info.namauser.startsWith("__LOCK__")) {
            info.namauser = newId;
          }
        }

        // Kirim semua points & state kursi
        this.sendAllStateTo(ws, roomname);
        this.broadcastRoomUserCount(roomname);

        // Hapus dari daftar offline
        this.offlineUsers.delete(newId);
        this.cancelOfflineRemoval(newId);
        console.log(`[setIdTarget] restored seats for ${newId} in ${roomname}`);
      } else {
        // === Tidak ada data offline, minta client joinRoom lagi ===
        this.safeSend(ws, ["needJoinRoom"]);
      }

      break;
    }

    case "sendnotif": {
      const [, idtarget, noimageUrl, username, deskripsi] = data;
      const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
      let delivered = false;
      for (const c of this.clients) if (c.idtarget === idtarget) { this.safeSend(c, notif); delivered = true; }
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
      for (const c of this.clients) if (c.idtarget === idt) { this.safeSend(c, out); delivered = true; }
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
            console.log(`[isUserOnline] removed duplicate seat ${room}#${seat} for ${username}`);
          }
          this.userToSeat.delete(username);
        }

        for (const old of oldSockets) {
          try {
            old.close(4000, "Duplicate login — old session closed");
            this.clients.delete(old);
            console.log(`[isUserOnline] closed old socket for ${username}`);
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
  }

  cleanupClient(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      console.log("[cleanupClient] client without id removed");
      return;
    }

    // Simpan kursi sementara untuk reconnect
    if (ws.roomname && ws.numkursi && ws.numkursi.size > 0) {
      this.offlineUsers.set(id, {
        roomname: ws.roomname,
        seats: Array.from(ws.numkursi),
        timestamp: Date.now()
      });
      this.scheduleOfflineRemoval(id);
      console.log(`[cleanupClient] saved offline data for ${id} seats=${Array.from(ws.numkursi).join(",")}`);
    } else {
      console.log(`[cleanupClient] ${id} had no seats to save`);
    }

    ws.numkursi?.clear?.();
    this.clients.delete(ws);
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  cleanupondestroy(ws) {
    if (!ws) return;
    const id = ws.idtarget;
    if (id) {
      console.log(`[cleanupondestroy] destroying ws and removing seats for ${id}`);
      this.removeAllSeatsById(id);
      this.cancelOfflineRemoval(id);
    } else {
      console.log("[cleanupondestroy] destroying ws without id");
    }
    ws.numkursi?.clear?.();
    this.clients.delete(ws);
    ws.roomname = undefined;
    ws.idtarget = undefined;
    try { ws.close(); } catch {}
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

    ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));
    ws.addEventListener("close", () => {
      console.log("[fetch] ws close event for", ws.idtarget);
      this.cleanupondestroy(ws);
    });
    ws.addEventListener("error", (e) => {
      // ⚠️ PERBAIKAN: Untuk error, gunakan cleanupClient (simpan offline) bukan cleanupondestroy
      console.log("[fetch] ws error event for", ws.idtarget, e && e.message);
      this.cleanupClient(ws);
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
