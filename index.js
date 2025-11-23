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

    // Kirim kursi data
    const kursiUpdates = [];
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info?.namauser && !info.namauser.startsWith("__LOCK__")) {
        kursiUpdates.push([seat, {
          noimageUrl: info.noimageUrl, namauser: info.namauser, color: info.color,
          itembawah: info.itembawah, itematas: info.itematas, vip: info.vip, viptanda: info.viptanda
        }]);
      }
    }

    if (kursiUpdates.length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, kursiUpdates]);
    }

    // ✅ HISTORY CHAT MAX 15
    if (this.roomChatHistory.has(room)) {
      const history = this.roomChatHistory.get(room);
      for (const chat of history.slice(-15)) { // ✅ MAX 15 PESAN
        this.safeSend(ws, ["chat", room, chat.noImageURL, chat.username, chat.message, chat.usernameColor, chat.chatTextColor]);
      }
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
          
          // Kirim missed chats
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
    this.safeSend(ws, ["rooMasuk", foundSeat, newRoom]);

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
            
            // ✅ KIRIM MISSED CHATS untuk setIdTarget juga
            const disconnectTime = this.userDisconnectTime.get(newId) || 0;
            if (disconnectTime > 0 && this.roomChatHistory.has(prevSeat.room)) {
              const history = this.roomChatHistory.get(prevSeat.room);
              const missedChats = history.filter(chat => chat.timestamp > disconnectTime);
              for (const chat of missedChats) {
                this.safeSend(ws, ["restoreChatHistory", prevSeat.room, chat.noImageURL, chat.username, chat.message, chat.usernameColor, chat.chatTextColor]);
              }
            }
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
          const [, room, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (roomList.includes(room)) {
            // ✅ SIMPAN HISTORY MAX 15
            if (!this.roomChatHistory.has(room)) this.roomChatHistory.set(room, []);
            const history = this.roomChatHistory.get(room);
            history.push({ timestamp: Date.now(), noImageURL, username, message, usernameColor, chatTextColor });
            if (history.length > 15) this.roomChatHistory.set(room, history.slice(-15)); // ✅ MAX 15
            
            this.broadcastToRoom(room, ["chat", room, noImageURL, username, message, usernameColor, chatTextColor]);
          }
          break;

        case "updatePoint":
          const [, roomP, seat, x, y, fast] = data;
          if (roomList.includes(roomP)) {
            const seatMap = this.roomSeats.get(roomP);
            const si = seatMap?.get(seat);
            if (si) {
              si.lastPoint = { x, y, fast };
              this.broadcastToRoom(roomP, ["pointUpdated", roomP, seat, x, y, fast]);
            }
          }
          break;

        case "updateKursi":
          const [, roomK, seatK, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (roomList.includes(roomK)) {
            const seatMap = this.roomSeats.get(roomK);
            const currentInfo = seatMap?.get(seatK) || createEmptySeat();
            Object.assign(currentInfo, { noimageUrl, namauser, color, itembawah, itematas, vip: vip || 0, viptanda: viptanda || 0 });
            this.broadcastToRoom(roomK, ["updateKursi", roomK, seatK, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda]);
            this.broadcastRoomUserCount(roomK);
          }
          break;

        // ✅ CASE-CASE YANG TERTINGGAL
        case "removeKursiAndPoint":
          const [, roomR, seatR] = data;
          if (roomList.includes(roomR)) {
            const seatMap = this.roomSeats.get(roomR);
            const seatInfo = seatMap?.get(seatR);
            if (seatInfo) {
              Object.assign(seatInfo, createEmptySeat());
              this.broadcastToRoom(roomR, ["removeKursi", roomR, seatR]);
              this.broadcastRoomUserCount(roomR);
            }
          }
          break;

        case "gift":
          const [, roomG, sender, receiver, giftName] = data;
          if (roomList.includes(roomG)) {
            this.broadcastToRoom(roomG, ["gift", roomG, sender, receiver, giftName, Date.now()]);
          }
          break;

        case "sendnotif":
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          for (const c of this.clients) {
            if (c.idtarget === idtarget && c.readyState === 1) {
              this.safeSend(c, notif);
              break;
            }
          }
          break;

        case "private":
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
          const result = roomList.map(room => [room, allCounts[room]]);
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

        case "vipbadge":
        case "removeVipBadge":
        case "getAllVipBadges":
          this.vipManager.handleEvent(ws, data);
          break;

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
