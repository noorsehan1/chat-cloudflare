// ChatServer Durable Object (Bahasa Indonesia) - FULL FIXED VERSION
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
    
    // Protected intervals dengan error handling
    this._tickTimer = setInterval(() => {
      try {
        this.tick();
      } catch (error) {
        console.error('Error in tick timer:', error);
      }
    }, this.intervalMillis);
    
    this._flushTimer = setInterval(() => {
      try {
        this.periodicFlush();
      } catch (error) {
        console.error('Error in flush timer:', error);
      }
    }, 100);

    this.lowcard = new LowCardGameManager(this);

    this.pingTimeouts = new Map();
    this.RECONNECT_TIMEOUT = 45000;
    this.cleanupInProgress = new Set();
  }

  safeSend(ws, arr) {
    try {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(arr));
        return true;
      } else if (ws.readyState === 0) {
        setTimeout(() => {
          if (ws.readyState === 1) {
            try { 
              ws.send(JSON.stringify(arr)); 
            } catch (e) {
              console.log("safeSend timeout error:", e);
            }
          }
        }, 300);
      }
    } catch (e) {
      console.log("safeSend general error:", e);
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    const clientsToRemove = [];
    let successCount = 0;
    let errorCount = 0;
    
    for (const c of Array.from(this.clients)) {
      if (c.roomname === room) {
        if (c.readyState === 3) {
          clientsToRemove.push(c);
        } else {
          try {
            if (this.safeSend(c, msg)) {
              successCount++;
            } else {
              errorCount++;
            }
          } catch (error) {
            console.log(`Broadcast error to client:`, error);
            errorCount++;
          }
        }
      }
    }
    
    for (const closedClient of clientsToRemove) {
      this.cleanupClientSafely(closedClient);
    }
    
    return { successCount, errorCount };
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
    try {
      const count = this.getJumlahRoom()[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {
      console.error(`Error broadcasting room count for ${room}:`, error);
    }
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        try {
          for (const msg of messages) {
            this.broadcastToRoom(room, msg);
          }
          messages.length = 0;
        } catch (error) {
          console.error(`Error flushing chat buffer for ${room}:`, error);
        }
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
      } catch (error) {
        console.error(`Error flushing kursi updates for ${room}:`, error);
      }
    }
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const clientsToRemove = [];
      for (const c of Array.from(this.clients)) {
        if (c.readyState === 3) {
          clientsToRemove.push(c);
        } else {
          try {
            this.safeSend(c, ["currentNumber", this.currentNumber]);
          } catch (error) {
            console.log(`Tick send error to client:`, error);
          }
        }
      }
      
      for (const closedClient of clientsToRemove) {
        this.cleanupClientSafely(closedClient);
      }
    } catch (error) {
      console.error('Error in tick:', error);
    }
  }

  cleanExpiredLocks() {
    try {
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
    } catch (error) {
      console.error('Error cleaning expired locks:', error);
    }
  }

  periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.cleanExpiredLocks();

      for (const [id, msgs] of Array.from(this.privateMessageBuffer)) {
        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === id && c.readyState === 1) {
            try {
              for (const m of msgs) this.safeSend(c, m);
              delivered = true;
            } catch (error) {
              console.log(`Private message delivery error for ${id}:`, error);
            }
            break;
          }
        }
        if (delivered) {
          this.privateMessageBuffer.delete(id);
          for (const c of this.clients) {
            if (c.idtarget === id && c.roomname) {
              this.broadcastRoomUserCount(c.roomname);
              break;
            }
          }
        }
      }
    } catch (error) {
      console.error('Error in periodicFlush:', error);
    }
  }

  handleGetAllRoomsUserCount(ws) {
    try {
      if (ws.readyState !== 1) return;
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {
      console.error('Error handling getAllRoomsUserCount:', error);
    }
  }

  lockSeat(room, ws) {
    try {
      const seatMap = this.roomSeats.get(room);
      if (!ws.idtarget) return null;
      const now = Date.now();

      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 5000) {
          Object.assign(info, createEmptySeat());
        }
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
    } catch (error) {
      console.error(`Error locking seat in ${room}:`, error);
      return null;
    }
  }

  sendAllStateTo(ws, room) {
    try {
      if (ws.readyState !== 1) return;
      
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
    } catch (error) {
      console.error(`Error sending all state to client in ${room}:`, error);
    }
  }

  cleanupClientSafely(ws) {
    const id = ws.idtarget;
    if (!id) {
      this.clients.delete(ws);
      return;
    }

    if (this.cleanupInProgress.has(id)) {
      return;
    }
    
    this.cleanupInProgress.add(id);

    try {
      if (this.pingTimeouts.has(id)) {
        clearTimeout(this.pingTimeouts.get(id));
        this.pingTimeouts.delete(id);
      }

      const activeConnections = Array.from(this.clients).filter(
        c => c.idtarget === id && c !== ws && c.readyState !== 3
      );

      if (activeConnections.length === 0) {
        this.removeAllSeatsById(id);
      }

      this.clients.delete(ws);
      
    } catch (error) {
      console.error(`Error cleaning up client ${id}:`, error);
    } finally {
      this.cleanupInProgress.delete(id);
    }
  }

  removeAllSeatsById(idtarget) {
    try {
      const seatInfo = this.userToSeat.get(idtarget);
      if (!seatInfo) return;

      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);
      if (!seatMap || !seatMap.has(seat)) return;

      const currentSeat = seatMap.get(seat);
      if (currentSeat.namauser === idtarget) {
        Object.assign(currentSeat, createEmptySeat());
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }

      this.userToSeat.delete(idtarget);
    } catch (error) {
      console.error(`Error removing seats for ${idtarget}:`, error);
    }
  }

  getAllOnlineUsers() {
    const users = [];
    for (const ws of this.clients) {
      if (ws.idtarget && ws.readyState === 1) users.push(ws.idtarget);
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    for (const ws of this.clients) {
      if (ws.roomname === roomName && ws.idtarget && ws.readyState === 1) 
        users.push(ws.idtarget);
    }
    return users;
  }

  handleOnDestroy(ws, idtarget) {
    if (ws.isDestroyed) return;
    
    ws.isDestroyed = true;
    this.cleanupClientSafely(ws);
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;
    
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
      console.log("JSON parse error:", e);
      return this.safeSend(ws, ["error", "Invalid JSON"]); 
    }
    
    if (!Array.isArray(data) || data.length === 0) 
      return this.safeSend(ws, ["error", "Invalid message format"]);
    
    const evt = data[0];

    try {
      switch (evt) {
        case "onDestroy": {
          const idtarget = ws.idtarget;
          this.handleOnDestroy(ws, idtarget);
          break;
        }

        case "setIdTarget": {
          const newId = data[1];
          ws.idtarget = newId;

          if (this.pingTimeouts.has(newId)) {
            console.log(`User ${newId} reconnected, cancel timeout`);
            clearTimeout(this.pingTimeouts.get(newId));
            this.pingTimeouts.delete(newId);
          }

          const prevSeat = this.userToSeat.get(newId);

          if (prevSeat) {
            ws.roomname = prevSeat.room;
            ws.numkursi = new Set([prevSeat.seat]);

            this.sendAllStateTo(ws, prevSeat.room);

            const seatMap = this.roomSeats.get(prevSeat.room);
            if (seatMap) {
              const seatInfo = seatMap.get(prevSeat.seat);
              if (seatInfo.namauser === `__LOCK__${newId}` || !seatInfo.namauser) {
                seatInfo.namauser = newId;
              }
            }
          } else {
            if (!this.hasEverSetId) {
              console.log(`User ${newId} first time, no needJoinRoom`);
            } else {
              this.safeSend(ws, ["needJoinRoom"]);
            }
          }

          this.hasEverSetId = true;

          if (this.privateMessageBuffer.has(newId)) {
            for (const msg of this.privateMessageBuffer.get(newId)) 
              this.safeSend(ws, msg);
            this.privateMessageBuffer.delete(newId);
          }

          if (ws.roomname) this.broadcastRoomUserCount(ws.roomname);
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
            if (!this.privateMessageBuffer.has(idtarget)) 
              this.privateMessageBuffer.set(idtarget, []);
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
            if (c.idtarget === idt && c.readyState === 1) { 
              this.safeSend(c, out); 
              delivered = true; 
            }
          }
          if (!delivered) {
            if (!this.privateMessageBuffer.has(idt)) 
              this.privateMessageBuffer.set(idt, []);
            this.privateMessageBuffer.get(idt).push(out);
            this.safeSend(ws, ["privateFailed", idt, "User offline"]);
          }
          break;
        }

        case "isUserOnline": {
          const username = data[1];
          const tanda = data[2] ?? "";

          const activeSockets = Array.from(this.clients)
            .filter(c => c.idtarget === username && c.readyState === 1);
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
              try { 
                if (old.readyState === 1) {
                  old.close(4000, "Duplicate login"); 
                }
                this.clients.delete(old); 
              } catch (e) {
                console.log("Error closing duplicate socket:", e);
              }
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

        case "getOnlineUsers": 
          this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]); 
          break;

        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) 
            return this.safeSend(ws, ["error", "Unknown room"]);
          this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
          break;
        }

        case "joinRoom": {
          const newRoom = data[1];
          if (!roomList.includes(newRoom)) 
            return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
          
          if (ws.idtarget) this.removeAllSeatsById(ws.idtarget);
          
          ws.roomname = newRoom;
          const foundSeat = this.lockSeat(newRoom, ws);
          
          if (foundSeat === null) 
            return this.safeSend(ws, ["roomFull", newRoom]);
          
          ws.numkursi = new Set([foundSeat]);
          this.safeSend(ws, ["numberKursiSaya", foundSeat]);
          
          if (ws.idtarget) 
            this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
          
          this.sendAllStateTo(ws, newRoom);
          this.broadcastRoomUserCount(newRoom);
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!roomList.includes(roomname)) 
            return this.safeSend(ws, ["error", "Invalid room for chat"]);
          if (!this.chatMessageBuffer.has(roomname)) 
            this.chatMessageBuffer.set(roomname, []);
          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (!roomList.includes(room)) 
            return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
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
          if (!roomList.includes(room)) 
            return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), createEmptySeat());
          for (const c of this.clients) c.numkursi?.delete(seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (!roomList.includes(room)) 
            return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          const currentInfo = seatMap.get(seat) || createEmptySeat();
          Object.assign(currentInfo, { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda });
          seatMap.set(seat, currentInfo);
          if (!this.updateKursiBuffer.has(room)) 
            this.updateKursiBuffer.set(room, new Map());
          this.updateKursiBuffer.get(room).set(seat, { ...currentInfo, points: [] });
          this.broadcastRoomUserCount(room);
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) 
            return this.safeSend(ws, ["error", "Invalid room for gift"]);
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
          if (room !== "LowCard") {
            this.safeSend(ws, ["error", "Game LowCard hanya bisa dimainkan di room 'Lowcard'"]);
            break;
          }
          this.lowcard.handleEvent(ws, data);
          break;
        }

        default:
          this.safeSend(ws, ["error", "Unknown event"]);
      }
    } catch (error) {
      console.error(`Error handling message ${evt}:`, error);
      try {
        this.safeSend(ws, ["error", "Internal server error"]);
      } catch (sendError) {
        console.error("Even error sending failed:", sendError);
      }
    }
  }

  async fetch(request) {
    try {
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

      // Message handler dengan error isolation
      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          console.error("Fatal error in message handler:", error);
          try {
            ws.close(1011, "Internal server error");
          } catch (closeError) {
            console.error("Even close failed:", closeError);
          } finally {
            this.cleanupClientSafely(ws);
          }
        }
      });
      
      // Close handler
      ws.addEventListener("close", () => {
        if (!ws.isDestroyed) {
          this.cleanupClientSafely(ws);
        }
      });

      // Error handler - isolated
      ws.addEventListener("error", (error) => {
        console.log("WebSocket connection error:", error);
        const id = ws.idtarget;
        if (id && !this.pingTimeouts.has(id)) {
          console.log(`Setting reconnect timeout for ${id}`);
          
          const timeout = setTimeout(() => {
            console.log(`Reconnect timeout reached for ${id}`);
            if (this.clients.has(ws) && ws.readyState !== 1) {
              this.cleanupClientSafely(ws);
            }
            this.pingTimeouts.delete(id);
          }, this.RECONNECT_TIMEOUT);
          
          this.pingTimeouts.set(id, timeout);
        }
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error("Fatal error in fetch:", error);
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
      console.error("Fatal error in default handler:", error);
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
