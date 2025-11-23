import { LowCardGameManager } from "./lowcard.js";
import { VipBadgeManager } from "./vipbadge.js";

const roomList = [
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
];

function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "", itembawah: 0, itematas: 0, vip: 0, viptanda: 0, lastPoint: null
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.clients = new Set();
    this.userToSeat = new Map();
    this.roomSeats = new Map();
    this.roomChatHistory = new Map();
    this.userDisconnectTime = new Map();
    this.pendingCleanups = new Map();

    this.lowcard = new LowCardGameManager(this);
    this.vipManager = new VipBadgeManager(this);

    this.MAX_SEATS = 35;
    this.RECONNECT_GRACE_PERIOD = 20000;
    this.currentNumber = 1;
    this.maxNumber = 6;

    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
      this.roomChatHistory.set(room, []);
    }

    this._numberTimer = setInterval(() => this.rotateNumber(), 15 * 60 * 1000);
    this._nextConnId = 1;
  }

  rotateNumber() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of this.clients) {
      if (c.readyState === 1 && c.roomname) {
        this.safeSend(c, ["currentNumber", this.currentNumber]);
      }
    }
  }

  processPendingCleanups() {
    const now = Date.now();
    for (const [idtarget, disconnectTime] of this.pendingCleanups) {
      if (now - disconnectTime >= this.RECONNECT_GRACE_PERIOD) {
        if (!this.isUserConnected(idtarget)) {
          this.removeUserData(idtarget);
        }
        this.pendingCleanups.delete(idtarget);
      }
    }
  }

  removeUserData(idtarget) {
    if (!idtarget) return;

    this.vipManager.cleanupUserVipBadges(idtarget);

    for (const [room, seatMap] of this.roomSeats) {
      for (const [seatNumber, seatInfo] of seatMap) {
        if (seatInfo.namauser === idtarget) {
          Object.assign(seatInfo, createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        }
      }
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(idtarget);
    this.userDisconnectTime.delete(idtarget);
  }

  isUserConnected(idtarget) {
    return Array.from(this.clients).some(c => c.idtarget === idtarget && c.readyState === 1);
  }

  safeSend(ws, arr) {
    try {
      if (ws?.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      }
    } catch (e) {}
    return false;
  }

  broadcastToRoom(room, msg) {
    for (const client of this.clients) {
      if (client.roomname === room && client.readyState === 1) {
        this.safeSend(client, msg);
      }
    }
  }

  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) {
      counts[room] = 0;
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const info of seatMap.values()) {
          if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
            counts[room]++;
          }
        }
      }
    }
    return counts;
  }

  broadcastRoomUserCount(room) {
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;

    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seat = seatMap.get(i);
      if (seat && seat.namauser === "") {
        seat.namauser = "__LOCK__" + ws.idtarget;
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;

    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;

    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    const count = this.getJumlahRoom()[room] || 0;
    this.safeSend(ws, ["roomUserCount", room, count]);

    // Kirim allUpdateKursiList untuk client Java
    const allKursiMeta = {};
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info?.namauser && !info.namauser.startsWith("__LOCK__")) {
        allKursiMeta[seat] = {
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
    this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);

    // Kirim allPointsList untuk client Java
    const pointsData = [];
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info?.lastPoint) {
        pointsData.push({
          seat: seat,
          x: info.lastPoint.x,
          y: info.lastPoint.y,
          fast: info.lastPoint.fast
        });
      }
    }
    if (pointsData.length > 0) {
      this.safeSend(ws, ["allPointsList", room, pointsData]);
    }

   
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id) return;

    this.pendingCleanups.delete(id);
    ws.idtarget = id;

    if (baru === true) {
      ws.roomname = undefined;
      this.userDisconnectTime.delete(id);
      
    } else if (baru === false) {
      const seatInfo = this.userToSeat.get(id);

      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);

        if (seatMap?.get(seat)?.namauser === id) {
          ws.roomname = room;
          
          const disconnectTime = this.userDisconnectTime.get(id) || 0;
          if (disconnectTime > 0 && this.roomChatHistory.has(room)) {
            const history = this.roomChatHistory.get(room);
            const missedChats = history.filter(chat => chat.timestamp > disconnectTime);
            for (const chat of missedChats) {
              this.safeSend(ws, ["restoreChatHistory", room, chat.noImageURL, chat.username, chat.message, chat.usernameColor, chat.chatTextColor]);
            }
          }
          this.userDisconnectTime.delete(id);

          this.sendAllStateTo(ws, room);
          this.broadcastRoomUserCount(room);
          return;
        }
      }
      
      this.userToSeat.delete(id);
      this.userDisconnectTime.delete(id);
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }

  async handleJoinRoom(ws, newRoom) {
    if (!roomList.includes(newRoom) || !ws.idtarget) {
      this.safeSend(ws, ["error", "Invalid room or user ID"]);
      return false;
    }

    if (ws.roomname && ws.roomname !== newRoom) {
      this.removeUserData(ws.idtarget);
    }

    ws.roomname = newRoom;
    const foundSeat = this.lockSeat(newRoom, ws);

    if (foundSeat === null) {
      this.safeSend(ws, ["roomFull", newRoom]);
      return false;
    }

    this.safeSend(ws, ["numberKursiSaya", foundSeat]);
    this.safeSend(ws, ["roomMasuk", foundSeat, newRoom]);

    this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
    this.sendAllStateTo(ws, newRoom);
    this.vipManager.getAllVipBadges(ws, newRoom);
    this.broadcastRoomUserCount(newRoom);

    return true;
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;

    let data;
    try { data = JSON.parse(raw); } catch (e) { return; }
    if (!Array.isArray(data) || data.length === 0) return;

    const evt = data[0];

    try {
      switch (evt) {
        case "resetRoom": {
          const roomName = data[1];
          const seatMap = this.roomSeats.get(roomName);
          if (seatMap) {
            for (let i = 1; i <= this.MAX_SEATS; i++) {
              Object.assign(seatMap.get(i), createEmptySeat());
              this.broadcastToRoom(roomName, ["removeKursi", roomName, i]);
            }
          }
          this.broadcastRoomUserCount(roomName);
          break;
        }

        case "isInRoom": {
          const idtarget = ws.idtarget;
          if (!idtarget) {
            this.safeSend(ws, ["inRoomStatus", false]);
            return;
          }
          const seatInfo = this.userToSeat.get(idtarget);
          if (!seatInfo) {
            this.safeSend(ws, ["inRoomStatus", false]);
            return;
          }
          const { room, seat } = seatInfo;
          const seatMap = this.roomSeats.get(room);
          const seatData = seatMap?.get(seat);
          const isInRoom = seatData?.namauser === idtarget;
          this.safeSend(ws, ["inRoomStatus", isInRoom]);
          break;
        }

        case "onDestroy":
          ws._manualDisconnect = true;
          this.removeUserData(ws.idtarget);
          this.clients.delete(ws);
          break;

        case "setIdTarget2":
          this.handleSetIdTarget2(ws, data[1], data[2]);
          break;

        case "setIdTarget":
          const newId = data[1];
          if (ws.idtarget && ws.idtarget !== newId) {
            this.removeUserData(ws.idtarget);
          }
          ws.idtarget = newId;
          this.pendingCleanups.delete(newId);
          
          const prevSeat = this.userToSeat.get(newId);
          if (prevSeat) {
            ws.roomname = prevSeat.room;
            this.userDisconnectTime.delete(newId);
            this.sendAllStateTo(ws, prevSeat.room);
          } else {
            this.safeSend(ws, ["needJoinRoom"]);
          }
          break;

        case "joinRoom":
          this.handleJoinRoom(ws, data[1]);
          this.processPendingCleanups();
          break;

        case "chat":
          const [_, roomChat, noImageURLChat, usernameChat, messageChat, usernameColorChat, chatTextColorChat] = data;
          if (roomList.includes(roomChat)) {
            if (!this.roomChatHistory.has(roomChat)) this.roomChatHistory.set(roomChat, []);
            const history = this.roomChatHistory.get(roomChat);
            history.push({ 
              timestamp: Date.now(), 
              noImageURL: noImageURLChat, 
              username: usernameChat, 
              message: messageChat, 
              usernameColor: usernameColorChat, 
              chatTextColor: chatTextColorChat 
            });
            if (history.length > 15) this.roomChatHistory.set(roomChat, history.slice(-15));
            
            this.broadcastToRoom(roomChat, ["chat", roomChat, noImageURLChat, usernameChat, messageChat, usernameColorChat, chatTextColorChat]);
          }
          break;

        case "updatePoint":
          const [__, roomPoint, seatPoint, xPoint, yPoint, fastPoint] = data;
          if (roomList.includes(roomPoint)) {
            const seatMap = this.roomSeats.get(roomPoint);
            const si = seatMap?.get(seatPoint);
            if (si) {
              si.lastPoint = { x: xPoint, y: yPoint, fast: fastPoint };
              this.broadcastToRoom(roomPoint, ["pointUpdated", roomPoint, seatPoint, xPoint, yPoint, fastPoint]);
            }
          }
          break;

        case "updateKursi":
          const [___, roomKursi, seatKursi, noimageUrlKursi, namauserKursi, colorKursi, itembawahKursi, itematasKursi, vipKursi, viptandaKursi] = data;
          if (roomList.includes(roomKursi)) {
            const seatMap = this.roomSeats.get(roomKursi);
            const currentInfo = seatMap?.get(seatKursi) || createEmptySeat();
            Object.assign(currentInfo, { 
              noimageUrl: noimageUrlKursi, 
              namauser: namauserKursi, 
              color: colorKursi, 
              itembawah: itembawahKursi, 
              itematas: itematasKursi, 
              vip: vipKursi || 0, 
              viptanda: viptandaKursi || 0 
            });
            this.broadcastToRoom(roomKursi, ["updateKursi", roomKursi, seatKursi, noimageUrlKursi, namauserKursi, colorKursi, itembawahKursi, itematasKursi, vipKursi, viptandaKursi]);
            this.broadcastRoomUserCount(roomKursi);
          }
          break;

        case "removeKursiAndPoint":
          const [____, roomRemove, seatRemove] = data;
          if (roomList.includes(roomRemove)) {
            const seatMap = this.roomSeats.get(roomRemove);
            const seatInfo = seatMap?.get(seatRemove);
            if (seatInfo) {
              Object.assign(seatInfo, createEmptySeat());
              this.broadcastToRoom(roomRemove, ["removeKursi", roomRemove, seatRemove]);
              this.broadcastRoomUserCount(roomRemove);
            }
          }
          break;

        case "gift":
          const [_____, roomGift, senderGift, receiverGift, giftNameGift] = data;
          if (roomList.includes(roomGift)) {
            this.broadcastToRoom(roomGift, ["gift", roomGift, senderGift, receiverGift, giftNameGift, Date.now()]);
          }
          break;

        case "sendnotif":
          const [______, idtargetNotif, noimageUrlNotif, usernameNotif, deskripsiNotif] = data;
          const notif = ["notif", noimageUrlNotif, usernameNotif, deskripsiNotif, Date.now()];
          for (const c of this.clients) {
            if (c.idtarget === idtargetNotif && c.readyState === 1) {
              this.safeSend(c, notif);
              break;
            }
          }
          break;

        case "private":
          const [_______, idtPrivate, urlPrivate, msgPrivate, senderPrivate] = data;
          const ts = Date.now();
          const out = ["private", idtPrivate, urlPrivate, msgPrivate, ts, senderPrivate];
          this.safeSend(ws, out);
          for (const c of this.clients) {
            if (c.idtarget === idtPrivate && c.readyState === 1) {
              this.safeSend(c, out);
              break;
            }
          }
          break;

        case "isUserOnline":
          const usernameOnline = data[1];
          const tanda = data[2] ?? "";
          let online = false;
          for (const c of this.clients) {
            if (c.idtarget === usernameOnline && c.readyState === 1) {
              online = true;
              break;
            }
          }
          this.safeSend(ws, ["userOnlineStatus", usernameOnline, online, tanda]);
          break;

        case "getAllRoomsUserCount":
          const allCounts = this.getJumlahRoom();
          // Sesuai dengan format yang di-expect client Java
          const result = Object.entries(allCounts).map(([roomName, userCount]) => ({
            roomName,
            userCount
          }));
          this.safeSend(ws, ["allRoomsUserCount", result]);
          break;

        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;

        case "getOnlineUsers":
          const users = [];
          for (const client of this.clients) {
            if (client.idtarget && client.readyState === 1) {
              users.push(client.idtarget);
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;

        case "getRoomOnlineUsers":
          const roomName = data[1];
          if (!roomList.includes(roomName)) return;
          const roomUsers = [];
          for (const client of this.clients) {
            if (client.roomname === roomName && client.idtarget && client.readyState === 1) {
              roomUsers.push(client.idtarget);
            }
          }
          this.safeSend(ws, ["roomOnlineUsers", roomName, roomUsers]);
          break;

        case "vipbadge": {
          const [, room, seat, numbadge, colortext] = data;
          // Tambahkan timestamp untuk client Java
          this.broadcastToRoom(room, ["vipbadge", room, seat, numbadge, colortext, Date.now()]);
          break;
        }

        case "removeVipBadge": {
          const [, room, seat] = data;
          this.broadcastToRoom(room, ["removeVipBadge", room, seat]);
          break;
        }

        case "getAllVipBadges": {
          const room = data[1];
          // Delegate ke vipManager
          this.vipManager.handleEvent(ws, data);
          break;
        }

        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (ws.roomname === "LowCard") this.lowcard.handleEvent(ws, data);
          break;

        default:
          console.log(`Unknown event: ${evt}`);
      }
    } catch (error) {
      console.error(`[HANDLE MESSAGE] Error in ${evt}:`, error);
    }
  }

  async fetch(request) {
    try {
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      await server.accept();

      const ws = server;
      ws._connId = `conn#${this._nextConnId++}`;
      ws._manualDisconnect = false;
      ws.roomname = undefined;
      ws.idtarget = undefined;

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));
      ws.addEventListener("error", (event) => {
        const id = ws.idtarget;
        if (id && !ws._manualDisconnect) this.pendingCleanups.set(id, Date.now());
      });
      ws.addEventListener("close", (event) => {
        const id = ws.idtarget;
        if (ws._manualDisconnect) {
          this.removeUserData(id);
          this.clients.delete(ws);
        } else if (id) {
          this.userDisconnectTime.set(id, Date.now());
          this.pendingCleanups.set(id, Date.now());
        } else {
          this.clients.delete(ws);
        }
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
      if (req.headers.get("Upgrade")?.toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      if (new URL(req.url).pathname === "/health") return new Response("ok");
      return new Response("WebSocket endpoint");
    } catch (error) {
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
