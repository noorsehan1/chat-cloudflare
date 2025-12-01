import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
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
    lastPoint: null,
    locked: false,
    lockedBy: null
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();
    this.userToSeat = new Map();
    this.hasEverSetId = false;
    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    this.roomClients = new Map();
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();

    this.messageCounts = new Map(); // bisa tetap untuk statistik jika perlu

    this._nextConnId = 1;
    this.currentNumber = 1;
    this.maxNumber = 6;

    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) seatMap.set(i, createEmptySeat());
      this.roomSeats.set(room, seatMap);
      this.roomClients.set(room, new Set());
    }

    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);

    // Interval utama
    this._tickTimer = setInterval(() => this.tick(), 15 * 60 * 1000);
    this._flushTimer = setInterval(() => { if (this.clients.size > 0) this.periodicFlush(); }, 100);
    this._cleanupTimer = setInterval(() => { if (this.clients.size > 0) this.aggressiveCleanup(); }, 30_000);
  }

  // ===== Kursi Lock / Unlock =====
  lockSeat(room, seatNumber, userId) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap || !seatMap.has(seatNumber)) return false;
    const seatInfo = seatMap.get(seatNumber);
    seatInfo.locked = true;
    seatInfo.lockedBy = userId;
    return true;
  }

  unlockSeat(room, seatNumber) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap || !seatMap.has(seatNumber)) return false;
    const seatInfo = seatMap.get(seatNumber);
    seatInfo.locked = false;
    seatInfo.lockedBy = null;
    return true;
  }

  isSeatLocked(room, seatNumber) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap || !seatMap.has(seatNumber)) return false;
    const seatInfo = seatMap.get(seatNumber);
    return seatInfo.locked && seatInfo.lockedBy !== null;
  }

  // ===== Cleanup =====
  aggressiveCleanup() {
    const now = Date.now();
    for (const [key, stats] of this.messageCounts.entries()) {
      const currentWindow = Math.floor(now / 1000);
      if (stats.window < currentWindow - 10) this.messageCounts.delete(key);
    }
    for (const [room, buffer] of this.chatMessageBuffer.entries()) {
      if (!buffer || buffer.length === 0) this.chatMessageBuffer.delete(room);
    }
  }

  cleanupUserFromSeat(room, seatNumber, userId) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;
    const seatInfo = seatMap.get(seatNumber);
    if (seatInfo && seatInfo.namauser === userId) {
      if (seatInfo.viptanda > 0) this.vipManager.removeVipBadge(room, seatNumber);
      this.unlockSeat(room, seatNumber);
      Object.assign(seatInfo, createEmptySeat());
      this.clearSeatBuffer(room, seatNumber);
      this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      this.broadcastRoomUserCount(room);
    }
    this.userToSeat.delete(userId);
    this.roomClients.get(room)?.forEach(ws => { if (ws.idtarget === userId) this.roomClients.get(room)?.delete(ws); });
  }

  forceUserCleanup(idtarget) {
    if (!idtarget) return;
    const seatInfo = this.userToSeat.get(idtarget);
    if (seatInfo) this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, idtarget);
    this.userToSeat.delete(idtarget);
    this.messageCounts.delete(idtarget);
  }

  fullRemoveById(idtarget) {
    if (!idtarget) return;
    this.vipManager.cleanupUserVipBadges(idtarget);
    this.forceUserCleanup(idtarget);
    for (const c of Array.from(this.clients)) {
      if (c.idtarget === idtarget) {
        if (c.readyState === 1) c.close(1000, "Session removed");
        this.clients.delete(c);
      }
    }
  }

  clearSeatBuffer(room, seatNumber) {
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) roomMap.delete(seatNumber);
  }

  // ===== Send / Broadcast =====
  safeSend(ws, arr) {
    if (ws && ws.readyState === 1) {
      if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > 1_000_000) return false;
      ws.send(JSON.stringify(arr));
      return true;
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    if (!roomList.includes(room)) return 0;
    let sentCount = 0;
    for (const c of this.roomClients.get(room) || []) {
      if (c.readyState === 1) {
        if (this.safeSend(c, msg)) sentCount++;
      }
    }
    return sentCount;
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const info of seatMap.values()) if (info.namauser) cnt[room]++;
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    if (!roomList.includes(room)) return;
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0 && roomList.includes(room)) {
        for (const msg of messages) this.broadcastToRoom(room, msg);
        this.chatMessageBuffer.set(room, []);
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMapUpdates] of this.updateKursiBuffer) {
      if (!roomList.includes(room)) continue;
      const updates = [];
      for (const [seat, info] of seatMapUpdates.entries()) {
        const { lastPoint, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      this.updateKursiBuffer.set(room, new Map());
    }
  }

  periodicFlush() {
    this.flushKursiUpdates();
    this.flushChatBuffer();
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of this.clients) {
      if (c.readyState === 1 && c.roomname) this.safeSend(c, ["currentNumber", this.currentNumber]);
    }
  }

  // ===== Room / Seat Handling =====
  findEmptySeat(room, ws) {
    if (!ws.idtarget) return null;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (seatInfo && seatInfo.namauser === ws.idtarget) return i;
    }

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k && !k.namauser && (!k.locked || k.lockedBy === ws.idtarget)) return i;
    }
    return null;
  }

  handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom) || !ws.idtarget || ws.readyState !== 1) return false;

    const currentSeatInfo = this.userToSeat.get(ws.idtarget);
    if (currentSeatInfo) this.cleanupUserFromSeat(currentSeatInfo.room, currentSeatInfo.seat, ws.idtarget);

    const foundSeat = this.findEmptySeat(newRoom, ws);
    if (!foundSeat) {
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    this.lockSeat(newRoom, foundSeat, ws.idtarget);
    ws.roomname = newRoom;
    this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
    this.roomClients.get(newRoom)?.add(ws);

    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
    this.safeSend(ws, ["currentNumber", this.currentNumber]);

    setTimeout(() => {
      if (ws.readyState === 1 && ws.roomname === newRoom) this.sendAllStateTo(ws, newRoom);
    }, 100);

    this.vipManager.getAllVipBadges(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);

    return true;
  }

handleSetIdTarget2(ws, id, baru) {
    if (!id) return;

    ws.idtarget = id;
    const prevSeatInfo = this.userToSeat.get(id);

    if (prevSeatInfo) {
        const { room, seat } = prevSeatInfo;
        ws.roomname = room;

        // Lock seat kembali jika kosong atau user lama
        const seatMap = this.roomSeats.get(room);
        const seatInfo = seatMap.get(seat);
        if (seatInfo && (!seatInfo.namauser || seatInfo.namauser === id)) {
            Object.assign(seatInfo, { namauser: id, locked: true, lockedBy: id });
            seatMap.set(seat, seatInfo);
        }

        // Tambahkan ws ke roomClients
        this.roomClients.get(room)?.add(ws);

        // Kirim kursi dan state ke client
      
        this.sendAllStateTo(ws, room);

        // Update jumlah user di room
        this.broadcastRoomUserCount(room);

        // Restore VIP badge
        this.vipManager.getAllVipBadges(ws, room);
    } else {
        ws.roomname = undefined;
        if (baru === true) {
            this.safeSend(ws, ["joinroomawal"]);
        } else {
            this.safeSend(ws, ["needJoinRoom"]);
        }
    }
}



  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;
    this.fullRemoveById(idtarget);
    this.clients.delete(ws);
    if (ws.readyState === 1) ws.close(1000, "Manual destroy");
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1 || ws.roomname !== room) return;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const allKursiMeta = {};
    const lastPointsData = [];

    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      if (info.namauser) {
        allKursiMeta[seat] = { ...info };
        delete allKursiMeta[seat].lastPoint;
        if (info.lastPoint) lastPointsData.push({ seat, ...info.lastPoint });
      }
    }

    if (Object.keys(allKursiMeta).length > 0) this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    if (lastPointsData.length > 0) this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    this.safeSend(ws, ["roomUserCount", room, this.getJumlahRoom()[room] || 0]);
  }

  removeAllSeatsById(idtarget) {
    if (!idtarget) return;
    const seatInfo = this.userToSeat.get(idtarget);
    if (!seatInfo) return;
    const { room, seat } = seatInfo;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap || !seatMap.has(seat)) {
      this.userToSeat.delete(idtarget);
      return;
    }
    const currentSeat = seatMap.get(seat);
    if (currentSeat.namauser === idtarget) {
      if (currentSeat.viptanda > 0) this.vipManager.removeVipBadge(room, seat);
      this.unlockSeat(room, seat);
      Object.assign(currentSeat, createEmptySeat());
      this.clearSeatBuffer(room, seat);
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
      this.broadcastRoomUserCount(room);
    }
    this.userToSeat.delete(idtarget);
  }

  // ===== WebSocket fetch =====
  async fetch(request) {
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") return new Response("Expected WebSocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    await server.accept();

    const ws = server;
    ws._connId = `conn#${this._nextConnId++}`;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.isManualDestroy = false;
    this.clients.add(ws);

    ws.addEventListener("message", ev => { try { this.handleMessage(ws, ev.data); } catch (e) {} });
    ws.addEventListener("error", () => { if (ws.idtarget && !ws.isManualDestroy) this.handleOnDestroy(ws, ws.idtarget); });
    ws.addEventListener("close", () => { if (ws.idtarget && !ws.isManualDestroy) this.handleOnDestroy(ws, ws.idtarget); this.clients.delete(ws); });

    return new Response(null, { status: 101, webSocket: client });
  }

  // ===== Semua handleMessage cases lengkap =====
  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;
    let data; try { data = JSON.parse(raw); } catch (e) { return; }
    if (!Array.isArray(data) || data.length === 0) return;
    const evt = data[0];

    switch (evt) {
      case "vipbadge":
      case "removeVipBadge":
      case "getAllVipBadges":
        this.vipManager.handleEvent(ws, data); break;

      case "isInRoom": {
        const seatInfo = this.userToSeat.get(ws.idtarget);
        this.safeSend(ws, ["inRoomStatus", !!seatInfo]); break;
      }

      case "lockSeat": {
        const [, room, seat] = data;
        if (ws.roomname !== room || !ws.idtarget) return;
        const seatMap = this.roomSeats.get(room);
        const seatInfo = seatMap.get(seat);
        if (seatInfo && seatInfo.namauser === ws.idtarget) {
          this.lockSeat(room, seat, ws.idtarget);
          this.safeSend(ws, ['seatLocked', room, seat]);
        }
        break;
      }

      case "unlockSeat": {
        const [, room, seat] = data;
        if (ws.roomname !== room || !ws.idtarget) return;
        const seatMap = this.roomSeats.get(room);
        const seatInfo = seatMap.get(seat);
        if (seatInfo && seatInfo.namauser === ws.idtarget) {
          this.unlockSeat(room, seat);
          this.safeSend(ws, ['seatUnlocked', room, seat]);
        }
        break;
      }

      case "onDestroy": break;
      case "setIdTarget2": this.handleSetIdTarget2(ws, data[1], data[2]); break;

      case "setIdTarget": {
        const newId = data[1];
        if (ws.idtarget && ws.idtarget !== newId) this.forceUserCleanup(ws.idtarget);
        ws.idtarget = newId;
        const prevSeat = this.userToSeat.get(newId);
        if (prevSeat) { ws.roomname = prevSeat.room; this.sendAllStateTo(ws, prevSeat.room); }
        else if (this.hasEverSetId) this.safeSend(ws, ["needJoinRoom"]);
        this.hasEverSetId = true;
        if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
        break;
      }

      case "sendnotif": {
        const [, idtarget, noimageUrl, username, deskripsi] = data;
        const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
        for (const c of this.clients) if (c.idtarget === idtarget && c.readyState === 1) { this.safeSend(c, notif); break; }
        break;
      }

      case "private": {
        const [, idt, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", idt, url, msg, ts, sender];
        this.safeSend(ws, out);
        for (const c of this.clients) if (c.idtarget === idt && c.readyState === 1) { this.safeSend(c, out); break; }
        break;
      }

      case "isUserOnline": {
        const username = data[1];
        const tanda = data[2] ?? "";
        let online = false;
        for (const c of this.clients) if (c.idtarget === username && c.readyState === 1) { online = true; break; }
        this.safeSend(ws, ["userOnlineStatus", username, online, tanda]); break;
      }

      case "getAllRoomsUserCount": this.safeSend(ws, ["allRoomsUserCount", roomList.map(r => [r, this.getJumlahRoom()[r]])]); break;
      case "getCurrentNumber": this.safeSend(ws, ["currentNumber", this.currentNumber]); break;
      case "getOnlineUsers": this.safeSend(ws, ["allOnlineUsers", Array.from(this.clients).filter(c => c.idtarget && c.readyState === 1).map(c => c.idtarget)]); break;
      case "getRoomOnlineUsers": {
        const roomName = data[1];
        if (!roomList.includes(roomName)) return;
        this.safeSend(ws, ["roomOnlineUsers", roomName, Array.from(this.roomClients.get(roomName) || []).filter(c => c.readyState === 1).map(c => c.idtarget)]);
        break;
      }

      case "joinRoom": this.handleJoinRoom(ws, data[1]); break;

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (ws.roomname !== roomname || !roomList.includes(roomname)) return;
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname).push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (ws.roomname !== room || !roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        si.lastPoint = { x, y, fast, timestamp: Date.now() };
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (ws.roomname !== room || !roomList.includes(room)) return;
        Object.assign(this.roomSeats.get(room).get(seat), createEmptySeat());
        this.clearSeatBuffer(room, seat);
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (ws.roomname !== room || !roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        if (currentInfo.locked && currentInfo.lockedBy !== ws.idtarget) { this.safeSend(ws, ['error', 'Kursi sedang digunakan oleh user lain']); return; }
        Object.assign(currentInfo, { noimageUrl, namauser, color, itembawah, itematas, vip: vip || 0, viptanda: viptanda || 0 });
        if (namauser && namauser !== "") this.lockSeat(room, seat, namauser);
        seatMap.set(seat, currentInfo);
        if (!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room, new Map());
        this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (ws.roomname !== roomname || !roomList.includes(roomname)) return;
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname).push(["gift", roomname, sender, receiver, giftName, Date.now()]);
        break;
      }

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (ws.roomname === "LowCard") setTimeout(() => this.lowcard.handleEvent(ws, data), 0); break;

      default: break;
    }
  }
}

export default {
  async fetch(req, env) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if (new URL(req.url).pathname === "/health") return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    return new Response("WebSocket endpoint", { status: 200 });
  }
};

