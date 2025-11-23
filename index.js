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
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    this.vipManager = new VipBadgeManager(this);
    this.lowcard = new LowCardGameManager(this);

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.roomChatHistory = new Map();

    for (const room of roomList) {
      this.updateKursiBuffer.set(room, new Map());
      this.chatMessageBuffer.set(room, []);
    }

    this.currentNumber = 1;
    this.maxNumber = 6;
    this._nextConnId = 1;
  }

  safeSend(ws, arr) {
    try {
      if (ws && ws.readyState === 1) {
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

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const info of seatMap.values()) {
          if (info.namauser) {
            cnt[room]++;
          }
        }
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  clearSeatBuffer(room, seatNumber) {
    const roomMap = this.updateKursiBuffer.get(room);
    if (roomMap) {
      roomMap.delete(seatNumber);
    }
  }

  forceUserCleanup(idtarget) {
    if (!idtarget) return;

    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) continue;

      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget) {
          if (seatInfo.viptanda > 0) {
            this.vipManager.removeVipBadge(room, seatNumber);
          }
          Object.assign(seatInfo, createEmptySeat());
          this.clearSeatBuffer(room, seatNumber);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const k = seatMap.get(i);
      if (k && k.namauser === "") {
        k.namauser = "__LOCK__" + ws.idtarget;
        return i;
      }
    }
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
      if (!info) continue;

      if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
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

    if (this.roomChatHistory.has(room)) {
      const history = this.roomChatHistory.get(room);
      const recentChats = history.slice(-10);
      for (const chat of recentChats) {
        this.safeSend(ws, [
          "chat",
          room,
          chat.noImageURL,
          chat.username,
          chat.message,
          chat.usernameColor,
          chat.chatTextColor
        ]);
      }
    }
  }

  cleanupClient(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    const activeConnections = Array.from(this.clients).filter(
      c => c.idtarget === id && c !== ws && c.readyState === 1
    );

    this.clients.delete(ws);

    if (activeConnections.length === 0) {
      this.forceUserCleanup(id);
    }
  }

  handleSetIdTarget2(ws, id, baru) {
    this.forceUserCleanup(id);
    ws.idtarget = id;

    if (baru === true) {
      ws.roomname = undefined;
      ws.numkursi = new Set();
      this.safeSend(ws, ["needJoinRoom"]);
    } else if (baru === false) {
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        if (seatMap?.has(seat)) {
          const seatData = seatMap.get(seat);
          if (seatData.namauser === id) {
            ws.roomname = room;
            ws.numkursi = new Set([seat]);
            this.sendAllStateTo(ws, room);
            this.broadcastRoomUserCount(room);
            return;
          }
        }
      }
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }

  handleOnDestroy(ws, idtarget) {
    if (!idtarget) return;
    this.forceUserCleanup(idtarget);
    this.clients.delete(ws);
  }

  handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom)) return false;

    if (ws.idtarget && ws.roomname && ws.roomname !== newRoom) {
      this.forceUserCleanup(ws.idtarget);
    }

    ws.roomname = newRoom;
    const foundSeat = this.lockSeat(newRoom, ws);

    if (foundSeat === null) {
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    ws.numkursi = new Set([foundSeat]);
    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);

    if (ws.idtarget) {
      this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
    }

    this.sendAllStateTo(ws, newRoom);
    this.vipManager.getAllVipBadges(ws, newRoom);
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
        case "vipbadge":
        case "removeVipBadge":
        case "getAllVipBadges":
          this.vipManager.handleEvent(ws, data);
          break;

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
          this.handleOnDestroy(ws, idtarget);
          break;
        }

        case "setIdTarget2": {
          const id = data[1];
          const baru = data[2];
          this.handleSetIdTarget2(ws, id, baru);
          break;
        }

        case "setIdTarget": {
          const newId = data[1];
          if (ws.idtarget && ws.idtarget !== newId) {
            this.forceUserCleanup(ws.idtarget);
          }
          ws.idtarget = newId;

          const prevSeat = this.userToSeat.get(newId);
          if (prevSeat) {
            ws.roomname = prevSeat.room;
            ws.numkursi = new Set([prevSeat.seat]);
            
            const seatMap = this.roomSeats.get(prevSeat.room);
            if (seatMap) {
              const seatInfo = seatMap.get(prevSeat.seat);
              if (seatInfo.namauser === `__LOCK__${newId}` || !seatInfo.namauser) {
                seatInfo.namauser = newId;
              }
            }
          } else {
            this.safeSend(ws, ["needJoinRoom"]);
          }
          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
          break;
        }

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          for (const c of this.clients) {
            if (c.idtarget === idtarget && c.readyState === 1) {
              this.safeSend(c, notif);
              break;
            }
          }
          break;
        }

        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const out = ["private", idt, url, msg, ts, sender];
          this.safeSend(ws, out);
          for (const c of this.clients) {
            if (c.idtarget === idt && c.readyState === 1) {
              this.safeSend(c, out);
              break;
            }
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
          const allCounts = this.getJumlahRoom();
          const result = roomList.map(room => [room, allCounts[room]]);
          this.safeSend(ws, ["allRoomsUserCount", result]);
          break;
        }

        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;

        case "getOnlineUsers": {
          const users = [];
          for (const ws of this.clients) {
            if (ws.idtarget && ws.readyState === 1) {
              users.push(ws.idtarget);
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }

        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) return;
          const users = [];
          for (const ws of this.clients) {
            if (ws.roomname === roomName && ws.idtarget && ws.readyState === 1) {
              users.push(ws.idtarget);
            }
          }
          this.safeSend(ws, ["roomOnlineUsers", roomName, users]);
          break;
        }

        case "joinRoom": {
          const newRoom = data[1];
          this.handleJoinRoom(ws, newRoom);
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) return;

          if (!this.chatMessageBuffer.has(roomname))
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);

          if (!this.roomChatHistory.has(roomname)) {
            this.roomChatHistory.set(roomname, []);
          }
          const history = this.roomChatHistory.get(roomname);

          const chatData = {
            timestamp: Date.now(),
            noImageURL,
            username,
            message,
            usernameColor,
            chatTextColor
          };

          history.push(chatData);
          if (history.length > 10) {
            this.roomChatHistory.set(roomname, history.slice(-10));
          }
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
          const seatInfo = seatMap.get(seat);
          if (seatInfo) {
            Object.assign(seatInfo, createEmptySeat());
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.broadcastRoomUserCount(room);
          }
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) return;

          const seatMap = this.roomSeats.get(room);
          const currentInfo = seatMap.get(seat) || createEmptySeat();

          Object.assign(currentInfo, {
            noimageUrl, namauser, color, itembawah, itematas,
            vip: vip || 0,
            viptanda: viptanda || 0
          });

          seatMap.set(seat, currentInfo);
          if (!this.updateKursiBuffer.has(room))
            this.updateKursiBuffer.set(room, new Map());
          this.updateKursiBuffer.get(room).set(seat, { ...currentInfo });
          this.broadcastRoomUserCount(room);
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          if (!this.chatMessageBuffer.has(roomname))
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["gift", roomname, sender, receiver, giftName, Date.now()]);
          break;
        }

        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          const room = ws.roomname;
          if (room !== "LowCard") return;
          this.lowcard.handleEvent(ws, data);
          break;
        }

        // Default case untuk handle unknown events
        default:
          console.log("Unknown event:", evt);
          break;
      }
    } catch (error) {
      console.error("Error handling message:", error);
    }
  }

  async fetch(request) {
    try {
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      await server.accept();

      const ws = server;
      ws._connId = `conn#${this._nextConnId++}`;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.numkursi = new Set();

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        this.handleMessage(ws, ev.data);
      });

      ws.addEventListener("close", (event) => {
        this.cleanupClient(ws);
      });

      ws.addEventListener("error", (event) => {
        this.cleanupClient(ws);
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
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      if (new URL(req.url).pathname === "/health")
        return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
      return new Response("WebSocket endpoint", { status: 200 });
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
