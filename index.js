// ChatServer Durable Object - CLEAN FIXED VERSION
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
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    this.updateKursiBuffer = new Map();
    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();

    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;
    
    this._tickTimer = setInterval(() => {
      try {
        this.tick();
      } catch (error) {}
    }, this.intervalMillis);
    
    this._flushTimer = setInterval(() => {
      try {
        this.periodicFlush();
      } catch (error) {}
    }, 100);

    this.lowcard = new LowCardGameManager(this);

    this.pingTimeouts = new Map();
    this.RECONNECT_TIMEOUT = 40000;
  }

  async destroy() {
    if (this._tickTimer) clearInterval(this._tickTimer);
    if (this._flushTimer) clearInterval(this._flushTimer);
    
    for (const timeout of this.pingTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.pingTimeouts.clear();
  }

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
    const clientsToRemove = [];
    
    for (const c of this.clients) {
      if (c.roomname === room) {
        if (c.readyState === 3) {
          clientsToRemove.push(c);
        } else if (c.readyState === 1) {
          try {
            this.safeSend(c, msg);
          } catch (error) {
            clientsToRemove.push(c);
          }
        }
      }
    }
    
    for (const closedClient of clientsToRemove) {
      this.clients.delete(closedClient);
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (seatMap) {
        for (const info of seatMap.values()) {
          if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
            cnt[room]++;
          }
        }
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room) {
    try {
      const count = this.getJumlahRoom()[room] || 0;
      this.broadcastToRoom(room, ["roomUserCount", room, count]);
    } catch (error) {}
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        try {
          for (const msg of messages) {
            this.broadcastToRoom(room, msg);
          }
          this.chatMessageBuffer.set(room, []);
        } catch (error) {
          this.chatMessageBuffer.set(room, []);
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
        this.updateKursiBuffer.set(room, new Map());
      } catch (error) {
        this.updateKursiBuffer.set(room, new Map());
      }
    }
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const clientsToRemove = [];
      for (const c of this.clients) {
        if (c.readyState === 3) {
          clientsToRemove.push(c);
        } else if (c.readyState === 1) {
          this.safeSend(c, ["currentNumber", this.currentNumber]);
        }
      }
      
      for (const closedClient of clientsToRemove) {
        this.clients.delete(closedClient);
      }
    } catch (error) {}
  }

  cleanExpiredLocks() {
    try {
      const now = Date.now();
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        if (seatMap) {
          for (const [seat, info] of seatMap) {
            if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
              Object.assign(info, createEmptySeat());
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.broadcastRoomUserCount(room);
            }
          }
        }
      }
    } catch (error) {}
  }

  periodicFlush() {
    try {
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.cleanExpiredLocks();

      const deliveredIds = [];
      for (const [id, msgs] of this.privateMessageBuffer) {
        let delivered = false;
        for (const c of this.clients) {
          if (c.idtarget === id && c.readyState === 1) {
            try {
              for (const m of msgs) {
                this.safeSend(c, m);
              }
              delivered = true;
              break;
            } catch (error) {}
          }
        }
        if (delivered) {
          deliveredIds.push(id);
        }
      }
      
      for (const id of deliveredIds) {
        this.privateMessageBuffer.delete(id);
      }
    } catch (error) {}
  }

  handleGetAllRoomsUserCount(ws) {
    if (ws.readyState !== 1) return;
    try {
      const allCounts = this.getJumlahRoom();
      const result = roomList.map(room => [room, allCounts[room]]);
      this.safeSend(ws, ["allRoomsUserCount", result]);
    } catch (error) {}
  }

  lockSeat(room, ws) {
    if (!ws.idtarget) return null;
    
    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return null;
      
      const now = Date.now();

      for (const [seat, info] of seatMap) {
        if (String(info.namauser).startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, createEmptySeat());
        }
      }

      for (let i = 1; i <= this.MAX_SEATS; i++) {
        const k = seatMap.get(i);
        if (k && k.namauser === "") {
          k.namauser = "__LOCK__" + ws.idtarget;
          k.lockTime = now;
          this.userToSeat.set(ws.idtarget, { room, seat: i });
          return i;
        }
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  senderrorstate(ws, room) {
    if (ws.readyState !== 1) return;
    
    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
      const count = this.getJumlahRoom()[room] || 0;
      this.safeSend(ws, ["roomUserCount", room, count]);
      
      const activeSeats = [];
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;
        
        const hasUser = info.namauser && !String(info.namauser).startsWith("__LOCK__");
        const hasPoints = info.points.length > 0;
        
        if (hasUser || hasPoints) {
          activeSeats.push({ seat, info });
        }
      }
      
      const kursiUpdates = [];
      for (const { seat, info } of activeSeats) {
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) {
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
      
      activeSeats.forEach(({ seat, info }, activeIndex) => {
        if (info.points.length > 0) {
          setTimeout(() => {
            if (ws.readyState !== 1) return;
            
            for (const point of info.points) {
              this.safeSend(ws, ["updatePoint", room, seat, point.x, point.y, point.fast]);
            }
            
          }, activeIndex * 100);
        }
      });
      
    } catch (error) {}
  }

  sendAllStateTo(ws, room) {
    if (ws.readyState !== 1) return;
    
    try {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      
      const allPoints = [];
      const meta = {};
      
      for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (!info) continue;
        
        for (const p of info.points) {
          allPoints.push({ seat, ...p });
        }
        
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
    } catch (error) {}
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
    } catch (error) {}
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
      if (ws.roomname === roomName && ws.idtarget && ws.readyState === 1) {
        users.push(ws.idtarget);
      }
    }
    return users;
  }

  handleOnDestroy(ws, idtarget) {
    if (idtarget) {
      const seatInfo = this.userToSeat.get(idtarget);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        if (seatMap && seatMap.has(seat)) {
          const currentSeat = seatMap.get(seat);
          if (currentSeat.namauser === idtarget) {
            Object.assign(currentSeat, createEmptySeat());
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.broadcastRoomUserCount(room);
          }
        }
        this.userToSeat.delete(idtarget);
      }
      
      if (this.pingTimeouts.has(idtarget)) {
        clearTimeout(this.pingTimeouts.get(idtarget));
        this.pingTimeouts.delete(idtarget);
      }
    }
    
    this.clients.delete(ws);
  }

  scheduleCleanupTimeout(idtarget) {
    if (this.pingTimeouts.has(idtarget)) {
      clearTimeout(this.pingTimeouts.get(idtarget));
      this.pingTimeouts.delete(idtarget);
    }
    
    const timeout = setTimeout(() => {
      const stillActive = Array.from(this.clients).some(
        c => c.idtarget === idtarget && c.readyState === 1
      );
      
      if (!stillActive) {
        const seatInfo = this.userToSeat.get(idtarget);
        if (seatInfo) {
          const { room, seat } = seatInfo;
          const seatMap = this.roomSeats.get(room);
          if (seatMap && seatMap.has(seat)) {
            const currentSeat = seatMap.get(seat);
            if (currentSeat.namauser === idtarget) {
              Object.assign(currentSeat, createEmptySeat());
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.broadcastRoomUserCount(room);
            }
          }
          this.userToSeat.delete(idtarget);
        }
        
        if (this.privateMessageBuffer.has(idtarget)) {
          this.privateMessageBuffer.delete(idtarget);
        }
        
        const stuckClients = Array.from(this.clients).filter(
          client => client.idtarget === idtarget && client.readyState !== 1
        );
        
        for (const client of stuckClients) {
          this.clients.delete(client);
        }
      }
      
      this.pingTimeouts.delete(idtarget);
    }, this.RECONNECT_TIMEOUT);
    
    this.pingTimeouts.set(idtarget, timeout);
  }

  handleSetIdTarget(ws, newId) {
    ws.idtarget = newId;

    if (this.pingTimeouts.has(newId)) {
      clearTimeout(this.pingTimeouts.get(newId));
      this.pingTimeouts.delete(newId);
    }

    const prevSeat = this.userToSeat.get(newId);

    if (prevSeat) {
      ws.roomname = prevSeat.room;
      ws.numkursi = new Set([prevSeat.seat]);

      this.senderrorstate(ws, prevSeat.room);

      const seatMap = this.roomSeats.get(prevSeat.room);
      if (seatMap) {
        const seatInfo = seatMap.get(prevSeat.seat);
        if (seatInfo.namauser === `__LOCK__${newId}` || !seatInfo.namauser) {
          seatInfo.namauser = newId;
        }
      }
    } else {
      if (!this.hasEverSetId) {
        // First time setup
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
  }

  handleMessage(ws, raw) {
    if (ws.readyState !== 1) return;
    
    let data;
    try { 
      data = JSON.parse(raw); 
    } catch (e) { 
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
          this.handleSetIdTarget(ws, newId);
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
              } catch (e) {}
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
          
          si.points = [{ x, y, fast }];
          
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
      try {
        this.safeSend(ws, ["error", "Internal server error"]);
      } catch (sendError) {}
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

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        try {
          this.handleMessage(ws, ev.data);
        } catch (error) {
          try {
            if (ws.readyState === 1) {
              ws.close(1011, "Internal server error");
            }
          } catch (closeError) {
          } finally {
            this.clients.delete(ws);
          }
        }
      });
      
 

      ws.addEventListener("error", (error) => {
        if (ws.idtarget) {
          this.scheduleCleanupTimeout(ws.idtarget);
        }
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
