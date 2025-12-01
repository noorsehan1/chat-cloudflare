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

    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
      this.roomClients.set(room, new Set());
    }

    this.vipManager = new VipBadgeManager(this);
    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this._nextConnId = 1;

    this.intervalMillis = 15 * 60 * 1000;
    this.currentNumber = 1;
    this.maxNumber = 6;

    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => {
      if (this.clients.size > 0) this.periodicFlush();
    }, 100);

    this.lowcard = new LowCardGameManager(this);
  }

  // =================== Seat Lock/Unlock ===================
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

  // =================== Seat Management ===================
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
  }

  forceUserCleanup(idtarget) {
    if (!idtarget) return;
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget) {
          this.cleanupUserFromSeat(room, seatNumber, idtarget);
        }
      }
    }
    this.userToSeat.delete(idtarget);
    this.chatMessageBuffer.forEach((msgs) => msgs.filter(msg => msg[3] !== idtarget));
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

  clearSeatBuffer(room, seatNumber) {
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) roomMap.delete(seatNumber);
  }

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
    if (currentSeatInfo) {
      this.cleanupUserFromSeat(currentSeatInfo.room, currentSeatInfo.seat, ws.idtarget);
    }

    const foundSeat = this.findEmptySeat(newRoom, ws);
    if (!foundSeat) {
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    this.lockSeat(newRoom, foundSeat, ws.idtarget);
    ws.roomname = newRoom;
    this.roomClients.get(newRoom).add(ws);

    this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });

    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
    this.safeSend(ws, ["currentNumber", this.currentNumber]);

    setTimeout(() => {
      if (ws.readyState === 1 && ws.roomname === newRoom) {
        this.sendAllStateTo(ws, newRoom);
      }
    }, 50);

    this.vipManager.getAllVipBadges(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);

    return true;
  }

  handleSetIdTarget2(ws, id, baru) {
    if (!id) return;

    ws.idtarget = id;

    if (baru === true) {
        this.forceUserCleanup(id);
        ws.roomname = undefined;
        this.safeSend(ws, ["joinroomawal"]);
    } else {
        const prevSeatInfo = this.userToSeat.get(id);
        if (prevSeatInfo) {
            ws.roomname = prevSeatInfo.room;
            this.clients.add(ws);
            this.roomClients.get(prevSeatInfo.room).add(ws);

            // Kirim semua state kursi & poin
            this.sendAllStateTo(ws, prevSeatInfo.room);

            // Sekarang client bisa langsung chat, gift, updatePoint
            this.broadcastRoomUserCount(prevSeatInfo.room);
        } else {
            ws.roomname = undefined;
            this.safeSend(ws, ["needJoinRoom"]);
        }
    }
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;
    this.removeAllSeatsById(idtarget);
    this.clients.delete(ws);
    if (ws.roomname) this.roomClients.get(ws.roomname)?.delete(ws);
    if (ws.readyState === 1) ws.close(1000, "Manual destroy");
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1 || !room) return;
    if (ws.roomname !== room) return;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const allKursiMeta = {};
    const lastPointsData = [];

    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;
      if (info.namauser) {
        allKursiMeta[seat] = {
          noimageUrl: info.noimageUrl,
          namauser: info.namauser,
          color: info.color,
          itembawah: info.itembawah,
          itematas: info.itematas,
          vip: info.vip,
          viptanda: info.viptanda,
          locked: info.locked,
          lockedBy: info.lockedBy
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

    if (Object.keys(allKursiMeta).length > 0)
      this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);

    if (lastPointsData.length > 0)
      this.safeSend(ws, ["allPointsList", room, lastPointsData]);

    const count = this.getJumlahRoom()[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);
  }

  // =================== Broadcast & SafeSend ===================
  safeSend(ws, arr) {
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify(arr));
      return true;
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    if (!roomList.includes(room)) return 0;
    let sentCount = 0;
    for (const c of this.roomClients.get(room)) {
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
      for (const info of seatMap.values()) {
        if (info.namauser) cnt[room]++;
      }
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
        for (let i = 0; i < messages.length; i++) this.broadcastToRoom(room, messages[i]);
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

  isUserInAnyRoom(idtarget) {
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;
      for (const seatInfo of seatMap.values()) {
        if (seatInfo.namauser === idtarget) return true;
      }
    }
    return false;
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;
    let data;
    try { data = JSON.parse(raw); } catch (e) { return; }
    if (!Array.isArray(data) || data.length === 0) return;

    const evt = data[0];

    switch (evt) {
      case "vipbadge":
      case "removeVipBadge":
      case "getAllVipBadges":
        this.vipManager.handleEvent(ws, data);
        break;

      case "joinRoom":
        this.handleJoinRoom(ws, data[1]);
        break;

      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (ws.roomname !== roomname) return;
        if (!roomList.includes(roomname)) return;
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname)
          .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
        break;
      }

      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (ws.roomname !== room) return;
        if (!roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        const si = seatMap.get(seat);
        if (!si) return;
        si.lastPoint = { x, y, fast, timestamp: Date.now() };
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (ws.roomname !== room) return;
        if (!roomList.includes(room)) return;
        const seatMap = this.roomSeats.get(room);
        const currentInfo = seatMap.get(seat) || createEmptySeat();
        if (currentInfo.locked && currentInfo.lockedBy !== ws.idtarget) {
          this.safeSend(ws, ['error', 'Kursi sedang digunakan oleh user lain']);
          return;
        }
        Object.assign(currentInfo, { noimageUrl, namauser, color, itembawah, itematas, vip: vip||0, viptanda: viptanda||0 });
        if (namauser && namauser !== "") this.lockSeat(room, seat, namauser);
        seatMap.set(seat, currentInfo);
        if (!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room, new Map());
        this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
        this.broadcastRoomUserCount(room);
        break;
      }

      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        if (ws.roomname !== roomname) return;
        if (!roomList.includes(roomname)) return;
        if (!this.chatMessageBuffer.has(roomname)) this.chatMessageBuffer.set(roomname, []);
        this.chatMessageBuffer.get(roomname)
          .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
        break;
      }

      case "private": {
        const [, idt, url, msg, sender] = data;
        const ts = Date.now();
        const out = ["private", idt, url, msg, ts, sender];
        this.safeSend(ws, out);
        for (const c of this.clients) {
          if (c.idtarget === idt && c.readyState === 1) this.safeSend(c, out);
        }
        break;
      }

      case "setIdTarget2": 
        this.handleSetIdTarget2(ws, data[1], data[2]); 
        break;

      case "setIdTarget": {
        const newId = data[1];
        if (ws.idtarget && ws.idtarget !== newId) this.forceUserCleanup(ws.idtarget);
        ws.idtarget = newId;

        const prevSeat = this.userToSeat.get(newId);
        if (prevSeat) {
          ws.roomname = prevSeat.room;
          this.sendAllStateTo(ws, prevSeat.room);
        } else {
          if (this.hasEverSetId) this.safeSend(ws, ["needJoinRoom"]);
        }

        this.hasEverSetId = true;
        if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
        break;
      }

      case "getAllRoomsUserCount":
        if (ws.readyState === 1) {
          const allCounts = this.getJumlahRoom();
          const result = roomList.map(room => [room, allCounts[room]]);
          this.safeSend(ws, ["allRoomsUserCount", result]);
        }
        break;

      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;
    }
  }
}
