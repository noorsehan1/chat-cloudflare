// ChatServer Durable Object (Bahasa Indonesia)
import { LowCardGameManager } from "./lowcard.js";

const roomList = [
  "LowCard","General","Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
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
   this.hasEverSetId = false;
   this.rejoinHandledMap = new Map(); // NEW: simpan status user yang sudah dihandle reconnect-nya


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

    this.gracePeriod = 5 * 60 * 1000; // 5 menit

    this.pendingRemove = new Map();
    this.pingTimeouts = new Map();
    this.PING_TIMEOUT = 30000;
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

  handlePing(ws, idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
    }
    
    this.safeSend(ws, ["ping", idtarget]);
    
    const timeout = setTimeout(() => {
      if (this.clients.has(ws)) {
        this.cleanupClient(ws);
      }
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
  // 🔹 Tandai koneksi sudah dihancurkan

  // 🔹 Hapus kursi dan mapping user di semua room
  this.removeAllSeatsById(idtarget);

  // 🔹 Hapus dari daftar client aktif
  this.clients.delete(ws);

  // 🔹 Bersihkan timeout ping, kalau masih ada
  if (this.pingTimeouts.has(idtarget)) {
    clearTimeout(this.pingTimeouts.get(idtarget));
    this.pingTimeouts.delete(idtarget);
  }

  // 🔹 Bersihkan timeout grace period, kalau masih ada
  if (this.pendingRemove.has(idtarget)) {
    clearTimeout(this.pendingRemove.get(idtarget));
    this.pendingRemove.delete(idtarget);
  }

  // 🔹 Hapus dari userToSeat agar tidak dianggap reconnect cepat
  if (this.userToSeat.has(idtarget)) {
    this.userToSeat.delete(idtarget);
  }

  // 🔹 Hapus semua data terkait client
  ws.roomname = undefined;
  ws.idtarget = undefined;
}


  handleMessage(ws, raw) {
    let data;
    try { data = JSON.parse(raw); } catch { return this.safeSend(ws, ["error", "Invalid JSON"]); }
    if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
    const evt = data[0];

    switch (evt) {
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

  if (idtarget) {
    this.rejoinHandledMap.delete(idtarget); // reset untuk koneksi berikutnya
  }

  ws.isDestroyed = true;
  this.handleOnDestroy(ws, idtarget);
  break;
}




        

case "setIdTarget": {
    const newId = data[1];

    // 🔹 Bersihkan koneksi lama dengan ID sama
    this.cleanupClientById(newId);
    ws.idtarget = newId;

    const previousSeatInfo = this.userToSeat.get(newId);
    const isInGracePeriod = this.pendingRemove.has(newId);

    // 🔹 Jika masih dalam grace period → auto reconnect cepat
    if (previousSeatInfo && isInGracePeriod) {
        clearTimeout(this.pendingRemove.get(newId));
        this.pendingRemove.delete(newId);

        const { room, seat } = previousSeatInfo;

        // 🔹 Assign ws ke room dan kursi
        ws.roomname = room;
        ws.numkursi = new Set([seat]);

        // 🔹 Assign kembali mapping seat
        this.userToSeat.set(newId, { room, seat });

        // 🔹 Update seat info di roomSeats
        const seatMap = this.roomSeats.get(room);
        if (seatMap && seatMap.has(seat)) {
            seatMap.get(seat).namauser = newId;
        }

        // 🔹 Kirim semua state room ke client
        this.sendAllStateTo(ws, room);

        // 🔹 Kirim pesan pribadi pending
        if (this.privateMessageBuffer.has(newId)) {
            for (const msg of this.privateMessageBuffer.get(newId)) {
                this.safeSend(ws, msg);
            }
            this.privateMessageBuffer.delete(newId);
        }

        this.safeSend(ws, ["autoRejoinSuccess", room]);
    }

    // 🔹 Jika user pernah join tapi grace period sudah habis
    else if (previousSeatInfo && !isInGracePeriod) {
        this.userToSeat.delete(newId);

        // Hanya kirim needJoinRoom jika bukan setIdTarget pertama
        if (!this.firstSetIdTarget) {
            this.safeSend(ws, ["needJoinRoom", previousSeatInfo.room]);
        }
    }

    // 🔹 Jika user baru total
    else if (!previousSeatInfo && !isInGracePeriod) {
        // Tidak kirim apa pun, tunggu joinRoom
    }

    // 🔹 Setelah selesai pertama kali set ID
    this.firstSetIdTarget = false;

    // 🔹 Kirim private messages tertunda (jika ada)
    if (this.privateMessageBuffer.has(newId)) {
        for (const msg of this.privateMessageBuffer.get(newId)) {
            this.safeSend(ws, msg);
        }
        this.privateMessageBuffer.delete(newId);
    }

    // 🔹 Update jumlah user room
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

        if (ws.idtarget && this.pendingRemove.has(ws.idtarget)) {
          clearTimeout(this.pendingRemove.get(ws.idtarget));
          this.pendingRemove.delete(ws.idtarget);
        }
          this.firstSetIdTarget = false;
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
      case "gameLowCardEnd": {
        // Pastikan hanya room "Lowcard" yang bisa menjalankan game
        const room = ws.roomname;
        if (room !== "LowCard") {
          this.safeSend(ws, ["error", "Game LowCard hanya bisa dimainkan di room 'Lowcard'"]);
          break;
        }

        // Jalankan event hanya jika benar di room Lowcard
        this.lowcard.handleEvent(ws, data);
        break;
      }

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
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

    
  ws.addEventListener("error", () => {
    if (ws.isDestroyed) {
        this.clients.delete(ws);
        return;
      }
    // Bersihkan client dulu
    this.cleanupClient(ws);

    // Jika ws masih punya idtarget, kirim needReconnect
    if (ws.idtarget) {
        this.safeSend(ws, ["needReconnect", "Connection error, please reconnect"]);
    }
});


    return new Response(null, { status: 101, webSocket: client });
  }
}

// Handler utama
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






















