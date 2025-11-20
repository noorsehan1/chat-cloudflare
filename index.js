// ChatServer Durable Object - LENGKAP & RINGAN
import { LowCardGameManager } from "./lowcard.js";

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
    points: []
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

    // Initialize semua 13 rooms
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    // Buffers minimal
    this.chatMessages = new Map();
    this.privateMessages = new Map();

    // Current number system
    this.currentNumber = 1;
    this.maxNumber = 6;

    // Timers
    this._flushTimer = setInterval(() => {
      this.flushAllMessages();
    }, 200);

    this._numberTimer = setInterval(() => {
      this.tickNumber();
    }, 15 * 60 * 1000);

    this.lowcard = new LowCardGameManager(this);
  }

  async destroy() {
    clearInterval(this._flushTimer);
    clearInterval(this._numberTimer);
    
    for (const client of this.clients) {
      try {
        if (client.readyState === 1) {
          client.close(1000, "Server shutdown");
        }
      } catch (e) {}
    }
    this.clients.clear();
  }

  // ✅ SEMUA CASE LENGKAP DARI AWAL

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {}
    return false;
  }

  broadcastToRoom(room, msg) {
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        this.safeSend(c, msg);
      }
    }
  }

  getRoomUserCount(room) {
    const seatMap = this.roomSeats.get(room);
    let count = 0;
    for (const info of seatMap.values()) {
      if (info.namauser) count++;
    }
    return count;
  }

  getAllRoomsUserCount() {
    const counts = {};
    for (const room of roomList) {
      counts[room] = this.getRoomUserCount(room);
    }
    return counts;
  }

  broadcastRoomUserCount(room) {
    const count = this.getRoomUserCount(room);
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  flushAllMessages() {
    // Flush chat messages
    for (const [room, messages] of this.chatMessages) {
      if (messages.length > 0) {
        for (const msg of messages) {
          this.broadcastToRoom(room, msg);
        }
        this.chatMessages.set(room, []);
      }
    }

    // Flush private messages
    for (const [userId, messages] of this.privateMessages) {
      if (messages.length > 0) {
        for (const client of this.clients) {
          if (client.idtarget === userId && client.readyState === 1) {
            for (const msg of messages) {
              this.safeSend(client, msg);
            }
            break;
          }
        }
        this.privateMessages.delete(userId);
      }
    }
  }

  tickNumber() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const client of this.clients) {
      if (client.readyState === 1 && client.roomname) {
        this.safeSend(client, ["currentNumber", this.currentNumber]);
      }
    }
  }

  removeUserById(idtarget) {
    if (!idtarget) return;

    // Remove dari semua 13 rooms
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const [seatNumber, info] of seatMap) {
        if (info.namauser === idtarget) {
          Object.assign(info, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);
    this.privateMessages.delete(idtarget);
  }

  findAvailableSeat(room, idtarget) {
    const seatMap = this.roomSeats.get(room);
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seat = seatMap.get(i);
      if (seat.namauser === "") {
        seat.namauser = idtarget;
        return i;
      }
    }
    return null;
  }

  sendRoomState(ws, room) {
    if (ws.readyState !== 1) return;

    const seatMap = this.roomSeats.get(room);
    
    // Kirim current number
    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    // Kirim room user count
    const count = this.getRoomUserCount(room);
    this.safeSend(ws, ["roomUserCount", room, count]);

    // Kirim semua kursi data
    const kursiUpdates = [];
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info.namauser) {
        kursiUpdates.push([
          seat,
          {
            noimageUrl: info.noimageUrl,
            namauser: info.namauser,
            color: info.color,
            itembawah: info.itembawah,
            itematas: info.itematas,
            vip: info.vip,
            viptanda: info.viptanda
          }
        ]);
      }
    }

    if (kursiUpdates.length > 0) {
      this.safeSend(ws, ["kursiBatchUpdate", room, kursiUpdates]);
    }

    // Kirim semua points
    const allPoints = [];
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info.points.length > 0) {
        for (const point of info.points.slice(-5)) {
          allPoints.push({ seat, ...point });
        }
      }
    }

    if (allPoints.length > 0) {
      this.safeSend(ws, ["allPointsList", room, allPoints]);
    }
  }

  handleSetIdTarget2(ws, id, baru) {
    ws.idtarget = id;

    // ✅ USER BARU - CLEANUP DI SEMUA ROOM
    if (baru === true) {
      this.removeUserById(id);
    }
    // ✅ USER LAMA - COBA RESTORE SEAT
    else if (baru === false) {
      const seatInfo = this.userToSeat.get(id);
      
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        const seatData = seatMap.get(seat);
        
        if (seatData.namauser === id) {
          // ✅ RESTORE BERHASIL
          ws.roomname = room;
          this.sendRoomState(ws, room);
        } else {
          // ❌ SEAT SUDAH DIDUDUKI ORANG LAIN
          this.userToSeat.delete(id);
          this.safeSend(ws, ["needJoinRoom"]);
        }
      } else {
        // ❌ TIDAK ADA SEAT INFO
        this.safeSend(ws, ["needJoinRoom"]);
      }
    }

    // KIRIM PRIVATE MESSAGES YANG PENDING
    if (this.privateMessages.has(id)) {
      for (const msg of this.privateMessages.get(id)) {
        this.safeSend(ws, msg);
      }
      this.privateMessages.delete(id);
    }
  }

  handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) return false;

    if (ws.idtarget) {
      this.removeUserById(ws.idtarget);
    }

    ws.roomname = newRoom;
    const foundSeat = this.findAvailableSeat(newRoom, ws.idtarget);

    if (foundSeat === null) {
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);
    
    this.sendRoomState(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);
    
    return true;
  }

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

    try {
      switch (evt) {
        // ✅ SEMUA CASE DARI AWAL
        case "isInRoom": {
          const idtarget = ws.idtarget;
          if (!idtarget) {
            this.safeSend(ws, ["inRoomStatus", false]);
            return;
          }
          const seatInfo = this.userToSeat.get(idtarget);
          const isInRoom = seatInfo ? true : false;
          this.safeSend(ws, ["inRoomStatus", isInRoom]);
          break;
        }

        case "onDestroy": {
          const idtarget = ws.idtarget;
          if (idtarget) this.removeUserById(idtarget);
          this.clients.delete(ws);
          break;
        }

        case "setIdTarget2":
          this.handleSetIdTarget2(ws, data[1], data[2]);
          break;

        case "setIdTarget": {
          const newId = data[1];
          ws.idtarget = newId;
          this.safeSend(ws, ["needJoinRoom"]);
          break;
        }

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          
          let delivered = false;
          for (const c of this.clients) {
            if (c.idtarget === idtarget && c.readyState === 1) {
              this.safeSend(c, notif);
              delivered = true;
            }
          }
          if (!delivered) {
            if (!this.privateMessages.has(idtarget)) {
              this.privateMessages.set(idtarget, []);
            }
            this.privateMessages.get(idtarget).push(notif);
          }
          break;
        }

        case "private": {
          const [, idt, url, msg, sender] = data;
          const out = ["private", idt, url, msg, Date.now(), sender];
          
          this.safeSend(ws, out);
          let delivered = false;
          for (const c of this.clients) {
            if (c.idtarget === idt && c.readyState === 1) {
              this.safeSend(c, out);
              delivered = true;
            }
          }
          if (!delivered) {
            if (!this.privateMessages.has(idt)) {
              this.privateMessages.set(idt, []);
            }
            this.privateMessages.get(idt).push(out);
          }
          break;
        }

        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";
          let online = false;
          for (const c of this.clients) {
            if (c.idtarget === username && c.readyState === 1) {
              online = true;
              break;
            }
          }
          this.safeSend(ws, ["userOnlineStatus", username, online, tanda]);
          break;
        }

        case "getAllRoomsUserCount": {
          const allCounts = this.getAllRoomsUserCount();
          const result = roomList.map(room => [room, allCounts[room]]);
          this.safeSend(ws, ["allRoomsUserCount", result]);
          break;
        }

        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;

        case "getOnlineUsers": {
          const users = [];
          for (const client of this.clients) {
            if (client.idtarget && client.readyState === 1) {
              users.push(client.idtarget);
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }

        case "getRoomOnlineUsers": {
          const roomName = data[1];
          const users = [];
          for (const client of this.clients) {
            if (client.roomname === roomName && client.idtarget && client.readyState === 1) {
              users.push(client.idtarget);
            }
          }
          this.safeSend(ws, ["roomOnlineUsers", roomName, users]);
          break;
        }

        case "joinRoom":
          this.handleJoinRoom(ws, data[1]);
          break;

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!this.chatMessages.has(roomname)) {
            this.chatMessages.set(roomname, []);
          }
          this.chatMessages.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          si.points.push({ x, y, fast });
          // Keep only last 10 points
          if (si.points.length > 10) si.points = si.points.slice(-10);
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          const seatMap = this.roomSeats.get(room);
          const currentInfo = seatMap.get(seat);
          Object.assign(currentInfo, {
            noimageUrl, namauser, color, itembawah, itematas, vip, viptanda
          });
          this.broadcastToRoom(room, ["updateKursi", room, seat, currentInfo]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!this.chatMessages.has(roomname)) {
            this.chatMessages.set(roomname, []);
          }
          this.chatMessages.get(roomname)
            .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
          break;
        }

        // Game events
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          if (ws.roomname === "LowCard") {
            this.lowcard.handleEvent(ws, data);
          }
          break;
        }
      }
    } catch (error) {
      // Suppress errors
    }
  }

  async fetch(request) {
    try {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      await server.accept();
      const ws = server;

      ws.roomname = undefined;
      ws.idtarget = undefined;

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        this.handleMessage(ws, ev.data);
      });

      ws.addEventListener("close", () => {
        this.clients.delete(ws);
      });

      ws.addEventListener("error", () => {
        this.clients.delete(ws);
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      if (req.headers.get("Upgrade") === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
