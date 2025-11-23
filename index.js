import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

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

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    this._tickTimer = setInterval(() => this.tick(), this.intervalMillis);
    this._flushTimer = setInterval(() => this.periodicFlush(), 100);

    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);
  }

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

  // FIXED: Improved seat locking logic
  lockSeat(room, ws) {
    if (!ws.idtarget) {
      console.log("lockSeat: No idtarget");
      return null;
    }

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) {
      console.log("lockSeat: Room not found", room);
      return null;
    }

    // Check if user already has a seat in this room
    const existingSeat = this.userToSeat.get(ws.idtarget);
    if (existingSeat && existingSeat.room === room) {
      console.log("lockSeat: User already in room", ws.idtarget, room);
      return existingSeat.seat;
    }

    // Find empty seat
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seatInfo = seatMap.get(i);
      if (seatInfo && seatInfo.namauser === "") {
        console.log("lockSeat: Found empty seat", i, "for user", ws.idtarget);
        
        // Lock the seat
        seatInfo.namauser = "__LOCK__" + ws.idtarget;
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        
        return i;
      }
    }

    console.log("lockSeat: No empty seats in room", room);
    return null;
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    const allKursiMeta = {};
    const lastPointsData = [];

    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info && info.namauser && !info.namauser.startsWith("__LOCK__")) {
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
    if (!idtarget) return;

    console.log("removeAllSeatsById:", idtarget);
    
    for (const [room, seatMap] of this.roomSeats) {
      for (const [seat, info] of seatMap) {
        if (info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget) {
          console.log("Removing seat", seat, "in room", room, "for user", idtarget);
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
        }
      }
    }
    
    this.userToSeat.delete(idtarget);
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;

    let data;
    try {
      data = JSON.parse(raw);
    } catch (e) {
      console.log("Invalid JSON:", raw);
      return;
    }

    if (!Array.isArray(data) || data.length === 0) return;

    const evt = data[0];
    console.log("Received event:", evt, "from:", ws.idtarget);

    switch (evt) {
      case "vipbadge":
      case "removeVipBadge":
      case "getAllVipBadges":
        this.vipManager.handleEvent(ws, data);
        break;

      case "setIdTarget": {
        const newId = data[1];
        console.log("setIdTarget:", newId);
        
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

      case "getAllRoomsUserCount": {
        const allCounts = this.getJumlahRoom();
        const result = roomList.map(room => [room, allCounts[room]]);
        this.safeSend(ws, ["allRoomsUserCount", result]);
        break;
      }

      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;

      // FIXED: Improved joinRoom handler
      case "joinRoom": {
        const newRoom = data[1];
        console.log("joinRoom attempt:", newRoom, "by user:", ws.idtarget);
        
        if (!roomList.includes(newRoom)) {
          console.log("joinRoom: Unknown room", newRoom);
          this.safeSend(ws, ["error", "Unknown room: " + newRoom]);
          return;
        }

        if (!ws.idtarget) {
          console.log("joinRoom: No idtarget set");
          this.safeSend(ws, ["error", "Please set ID first"]);
          return;
        }

        // Remove from previous room if any
        if (ws.roomname && ws.roomname !== newRoom) {
          this.removeAllSeatsById(ws.idtarget);
        }

        // Try to lock a seat
        const foundSeat = this.lockSeat(newRoom, ws);
        
        if (foundSeat === null) {
          console.log("joinRoom: Room full", newRoom);
          this.safeSend(ws, ["roomFull", newRoom]);
          return;
        }

        console.log("joinRoom: Success - room:", newRoom, "seat:", foundSeat, "user:", ws.idtarget);
        
        // Update client state
        ws.roomname = newRoom;
        ws.numkursi = new Set([foundSeat]);
        
        // Send success messages
        this.safeSend(ws, ["numberKursiSaya", foundSeat]);
        this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
        
        // Finalize seat assignment (remove lock)
        const seatMap = this.roomSeats.get(newRoom);
        const seatInfo = seatMap.get(foundSeat);
        if (seatInfo && seatInfo.namauser === "__LOCK__" + ws.idtarget) {
          seatInfo.namauser = ws.idtarget;
        }

        // Send room state
        this.sendAllStateTo(ws, newRoom);
        
        // Notify others
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

      // Add ping handler for connection health
      case "ping":
        this.safeSend(ws, ["pong"]);
        break;
    }
  }

  cleanupClient(ws) {
    const id = ws.idtarget;
    console.log("cleanupClient:", id);
    
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
    if (ws.numkursi) ws.numkursi.clear();
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
    console.log("New client connected, total:", this.clients.size);

    ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));
    ws.addEventListener("close", () => {
      console.log("Client disconnected:", ws.idtarget);
      this.cleanupClient(ws);
    });
    ws.addEventListener("error", (err) => {
      console.log("Client error:", ws.idtarget, err);
      this.cleanupClient(ws);
    });

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
      console.error("Worker error:", error);
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
