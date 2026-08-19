// ==================== CHAT-SERVER.JS ====================
// VERSION: 5.0.0 - MEMORY ONLY - REALTIME

const CONSTANTS = {
  MAX_CLIENTS: 200,
  MAX_ROOMS: 100,
  MAX_PLAYERS_PER_ROOM: 50,
  MAX_SEAT: 45,
};

// ============================================================
// ✅ CHAT SERVER
// ============================================================
export class ChatServer {
  constructor() {
    this.wsSet = new Map();        // wsId → WebSocket
    this.rooms = new Map();        // room → Set(wsId)
    this.wsRoom = new Map();       // wsId → room
    
    this.userSeat = new Map();     // username → {room, seat, wsId, data}
    this.roomSeats = new Map();    // room → Map(seat → username)
    this.roomUsers = new Map();    // room → Map(username → userData)
    this.roomPoints = new Map();   // room → Map(seat → {x, y, fast})
    this.roomMutes = new Map();    // room → boolean
    
    this.wsIdCounter = 0;
    this._started = false;
    this._startTime = Date.now();
    this._cleanupInterval = null;
    this._allTimers = new Set();
  }

  start() {
    if (this._started) return;
    this._started = true;
    
    this._cleanupInterval = setInterval(() => {
      this._cleanupDeadConnections();
      this._cleanupMemory();
    }, 30000);
    this._allTimers.add(this._cleanupInterval);
    
    console.log('🟢 ChatServer started - Memory Only');
  }

  _cleanupDeadConnections() {
    try {
      const toRemove = [];
      for (const [wsId, ws] of this.wsSet) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toRemove.push(wsId);
        }
      }
      for (const wsId of toRemove) {
        const ws = this.wsSet.get(wsId);
        if (ws) {
          const room = this.wsRoom.get(wsId);
          if (room) {
            const clients = this.rooms.get(room);
            if (clients) {
              clients.delete(wsId);
              if (clients.size === 0) this.rooms.delete(room);
            }
          }
          this.wsRoom.delete(wsId);
          this.wsSet.delete(wsId);
        }
      }
    } catch(e) {}
  }

  _cleanupMemory() {
    try {
      for (const [room, clients] of this.rooms) {
        if (clients.size === 0) this.rooms.delete(room);
      }
      for (const [room, users] of this.roomUsers) {
        if (users.size === 0) this.roomUsers.delete(room);
      }
      for (const [room, seats] of this.roomSeats) {
        if (seats.size === 0) this.roomSeats.delete(room);
      }
      for (const [room, points] of this.roomPoints) {
        if (points.size === 0) this.roomPoints.delete(room);
      }
    } catch(e) {}
  }

  // ============================================================
  // ✅ FETCH - SUPPORT ROOT PATH
  // ============================================================
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;

      // 🔥 Health check
      if (pathname === "/health" || pathname === "/") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          rooms: this.rooms.size,
          users: this.userSeat.size,
          roomUsers: this.roomUsers.size,
          uptime: Math.floor((Date.now() - this._startTime) / 1000),
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }

      // 🔥 WebSocket di root path atau /ws
      if (pathname === "/" || pathname === "/ws" || pathname === "/chat" || pathname === "/chat/ws") {
        const upgrade = request.headers.get("Upgrade");
        if (upgrade !== "websocket") {
          return new Response("WebSocket only", { status: 400 });
        }

        if (this.wsSet.size >= CONSTANTS.MAX_CLIENTS) {
          return new Response("Server full", { 
            status: 503,
            headers: { 'Retry-After': '10' }
          });
        }

        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        const wsId = ++this.wsIdCounter;

        server._wsId = wsId;
        server._closing = false;
        server.room = null;
        server.username = null;
        server.idTarget = null;
        server.seatNumber = -1;
        server.noimageUrl = '';
        server.color = '#FFFFFF';
        server.itembawah = 0;
        server.itematas = 0;
        server.vip = 0;
        server.viptanda = 0;

        try {
          server.accept();
        } catch(e) {
          return new Response("WebSocket failed", { status: 500 });
        }

        this.wsSet.set(wsId, server);

        server.addEventListener("message", async (event) => {
          try {
            if (server._closing) return;
            const data = JSON.parse(event.data);
            if (Array.isArray(data) && data.length > 0) {
              await this._handleMessage(server, data);
            }
          } catch(e) {
            console.error('Message error:', e);
          }
        });

        server.addEventListener("close", () => {
          this._handleClose(server);
        }, { once: true });

        server.addEventListener("error", () => {
          this._handleClose(server);
        }, { once: true });

        return new Response(null, { status: 101, webSocket: client });
      }

      return new Response("Chat Server", { status: 200 });

    } catch(e) {
      console.error('Fetch error:', e);
      return new Response("Error", { status: 500 });
    }
  }

  // ============================================================
  // ✅ HANDLE MESSAGE (Lengkap)
  // ============================================================
  async _handleMessage(ws, data) {
    try {
      const evt = data[0];

      switch(evt) {
        case "joinRoom":
        case "setIdTarget": {
          const idTarget = data[1] || "";
          const room = data[2] || "default";
          ws.idTarget = idTarget;
          ws.username = idTarget || `user_${ws._wsId}`;
          await this._joinRoom(ws, room);
          break;
        }

        case "setIdTarget2": {
          ws.idTarget = data[1] || "";
          ws.username = ws.idTarget || `user_${ws._wsId}`;
          break;
        }

        case "isInRoom": {
          const room = ws.room || "default";
          const isInRoom = this.rooms.has(room) && this.rooms.get(room).has(ws._wsId);
          this._send(ws, ["inRoomStatus", isInRoom]);
          break;
        }

        case "chat": {
          const room = data[1] || ws.room || "default";
          const noImage = data[2] || ws.noimageUrl || "";
          const username = data[3] || ws.username || "Unknown";
          const message = data[4] || "";
          const userColor = data[5] || ws.color || "#FFFFFF";
          const textColor = data[6] || "#000000";
          
          const cleanMsg = message.replace(/"/g, '\\"').replace(/\n/g, ' ');
          
          this._broadcast(room, ["chat", room, noImage, username, cleanMsg, userColor, textColor]);
          break;
        }

        case "private": {
          const targetId = data[1] || "";
          const noImage = data[2] || ws.noimageUrl || "";
          const message = data[3] || "";
          const sender = data[4] || ws.username || "Unknown";
          
          const cleanMsg = message.replace(/"/g, '\\"').replace(/\n/g, ' ');
          
          for (const [id, client] of this.wsSet) {
            if ((client.idTarget === targetId || client.username === targetId) && client.readyState === 1) {
              this._send(client, ["private", sender, noImage, cleanMsg, Date.now(), sender]);
              break;
            }
          }
          break;
        }

        case "sendnotif": {
          const targetId = data[1] || "";
          const noImage = data[2] || ws.noimageUrl || "";
          const username = data[3] || ws.username || "Unknown";
          const deskripsi = data[4] || "";
          
          for (const [id, client] of this.wsSet) {
            if ((client.idTarget === targetId || client.username === targetId) && client.readyState === 1) {
              this._send(client, ["notif", noImage, username, deskripsi, Date.now()]);
              break;
            }
          }
          break;
        }

        case "updateKursi": {
          const room = data[1] || ws.room || "default";
          const seat = data[2] || 0;
          const noImage = data[3] || "";
          const namauser = data[4] || "";
          const color = data[5] || "#FFFFFF";
          const itembawah = data[6] || 0;
          const itematas = data[7] || 0;
          const vip = data[8] || 0;
          const viptanda = data[9] || 0;

          ws.noimageUrl = noImage;
          ws.username = namauser || ws.idTarget || `user_${ws._wsId}`;
          ws.color = color;
          ws.itembawah = itembawah;
          ws.itematas = itematas;
          ws.vip = vip;
          ws.viptanda = viptanda;
          ws.seatNumber = seat;

          if (!this.roomUsers.has(room)) this.roomUsers.set(room, new Map());
          this.roomUsers.get(room).set(ws.username, {
            wsId: ws._wsId,
            seat: seat,
            noimageUrl: noImage,
            color: color,
            itembawah: itembawah,
            itematas: itematas,
            vip: vip,
            viptanda: viptanda
          });

          if (!this.roomSeats.has(room)) this.roomSeats.set(room, new Map());
          this.roomSeats.get(room).set(seat, ws.username);

          this._broadcast(room, ["kursiUpdated", room, seat, noImage, namauser, color, itembawah, itematas, vip, viptanda]);
          this._broadcast(room, ["updateKursi", room, seat, noImage, namauser, color, itembawah, itematas, vip, viptanda]);
          break;
        }

        case "updatePoint": {
          const room = data[1] || ws.room || "default";
          const seat = data[2] || 0;
          const x = data[3] || 0;
          const y = data[4] || 0;
          const fast = data[5] || 0;

          if (!this.roomPoints.has(room)) this.roomPoints.set(room, new Map());
          this.roomPoints.get(room).set(seat, { x, y, fast });

          this._broadcast(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const room = data[1] || ws.room || "default";
          const seat = data[2] || 0;

          if (this.roomSeats.has(room)) {
            const username = this.roomSeats.get(room).get(seat);
            this.roomSeats.get(room).delete(seat);
            if (username && this.roomUsers.has(room)) {
              this.roomUsers.get(room).delete(username);
            }
          }
          if (this.roomPoints.has(room)) {
            this.roomPoints.get(room).delete(seat);
          }

          this._broadcast(room, ["removeKursi", room, seat]);
          break;
        }

        case "resetRoom": {
          const room = data[1] || ws.room || "default";
          this.roomUsers.delete(room);
          this.roomSeats.delete(room);
          this.roomPoints.delete(room);
          this._broadcast(room, ["resetRoom", room]);
          break;
        }

        case "getOnlineUsers": {
          const users = [];
          for (const [id, client] of this.wsSet) {
            if (client.username && client.readyState === 1) {
              users.push(client.username);
            }
          }
          this._send(ws, ["allOnlineUsers", users]);
          break;
        }

        case "isUserOnline": {
          const userId = data[1] || "";
          const tanda = data[2] || "";
          let online = false;

          for (const [id, client] of this.wsSet) {
            if ((client.idTarget === userId || client.username === userId) && client.readyState === 1) {
              online = true;
              break;
            }
          }

          this._send(ws, ["userOnlineStatus", userId, online, tanda]);
          break;
        }

        case "getAllRoomsUserCount": {
          const rooms = [];
          for (const [room, clients] of this.rooms) {
            rooms.push({ roomName: room, userCount: clients.size });
          }
          this._send(ws, ["allRoomsUserCount", rooms]);
          break;
        }

        case "getCurrentNumber": {
          this._send(ws, ["currentNumber", 0]);
          break;
        }

        case "gift": {
          const room = data[1] || ws.room || "default";
          const sender = data[2] || ws.username || "Unknown";
          const receiver = data[3] || "";
          const giftName = data[4] || "";

          this._broadcast(room, ["gift", room, sender, receiver, giftName, Date.now()]);
          break;
        }

        case "modwarning": {
          const room = data[1] || ws.room || "default";
          this._broadcast(room, ["modwarning", room]);
          break;
        }

        case "setMuteType": {
          const isMuted = data[1] || false;
          const room = data[2] || ws.room || "default";
          this.roomMutes.set(room, isMuted);
          this._send(ws, ["muteTypeResponse", isMuted, room]);
          break;
        }

        case "getMuteType": {
          const room = data[1] || ws.room || "default";
          const isMuted = this.roomMutes.get(room) || false;
          this._send(ws, ["muteTypeResponse", isMuted, room]);
          break;
        }

        case "rollangak": {
          const room = data[1] || ws.room || "default";
          const username = data[2] || ws.username || "Unknown";
          const angka = data[3] || 0;
          this._broadcast(room, ["rollangakBroadcast", room, username, angka]);
          break;
        }

        case "multiJoin": {
          const username = data[1] || "";
          const room = data[2] || ws.room || "default";
          
          if (!username) {
            this._send(ws, ["multiError", "Username tidak boleh kosong"]);
            break;
          }
          
          let seat = 1;
          if (this.roomSeats.has(room)) {
            const usedSeats = new Set(this.roomSeats.get(room).keys());
            while (usedSeats.has(seat) && seat <= CONSTANTS.MAX_SEAT) seat++;
          }
          
          if (!this.roomUsers.has(room)) this.roomUsers.set(room, new Map());
          this.roomUsers.get(room).set(username, {
            wsId: ws._wsId,
            seat: seat,
            noimageUrl: '',
            color: '#FFFFFF',
            itembawah: 0,
            itematas: 0,
            vip: 0,
            viptanda: 0
          });
          
          if (!this.roomSeats.has(room)) this.roomSeats.set(room, new Map());
          this.roomSeats.get(room).set(seat, username);
          
          this._send(ws, ["rooMasukMulti", seat, room]);
          break;
        }

        case "setActiveMulti": {
          const username = data[1] || "";
          if (!username) {
            this._send(ws, ["multiError", "Username tidak boleh kosong"]);
            break;
          }
          
          for (const [room, users] of this.roomUsers) {
            if (users.has(username)) {
              const userData = users.get(username);
              this._send(ws, ["activeChangedMulti", username, userData.seat]);
              break;
            }
          }
          break;
        }

        case "exitMulti": {
          const username = data[1] || "";
          if (!username) {
            this._send(ws, ["multiError", "Username tidak boleh kosong"]);
            break;
          }
          
          for (const [room, users] of this.roomUsers) {
            if (users.has(username)) {
              const userData = users.get(username);
              users.delete(username);
              if (this.roomSeats.has(room)) {
                this.roomSeats.get(room).delete(userData.seat);
              }
              this._broadcast(room, ["removeKursi", room, userData.seat]);
              this._send(ws, ["multiExitSuccess", username]);
              break;
            }
          }
          break;
        }

        case "onDestroy": {
          this._handleClose(ws);
          break;
        }

        default:
          break;
      }

    } catch(e) {
      console.error('Handle message error:', e);
    }
  }

  // ============================================================
  // ✅ JOIN ROOM
  // ============================================================
  async _joinRoom(ws, room) {
    try {
      const wsId = ws._wsId;
      if (!wsId) return;

      const currentRoom = ws.room || this.wsRoom.get(wsId);

      if (currentRoom && currentRoom !== room) {
        const clients = this.rooms.get(currentRoom);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) this.rooms.delete(currentRoom);
        }
        this._broadcast(currentRoom, ["userLeftRoom", ws.username || ws.idTarget, currentRoom]);
      }

      ws.room = room;
      this.wsRoom.set(wsId, room);

      let clients = this.rooms.get(room);
      if (!clients) {
        clients = new Set();
        this.rooms.set(room, clients);
      }
      clients.add(wsId);

      let seat = 1;
      if (this.roomSeats.has(room)) {
        const usedSeats = new Set(this.roomSeats.get(room).keys());
        while (usedSeats.has(seat) && seat <= CONSTANTS.MAX_SEAT) seat++;
      }
      ws.seatNumber = seat;

      if (!this.roomUsers.has(room)) this.roomUsers.set(room, new Map());
      this.roomUsers.get(room).set(ws.username || ws.idTarget || `user_${wsId}`, {
        wsId: wsId,
        seat: seat,
        noimageUrl: ws.noimageUrl || '',
        color: ws.color || '#FFFFFF',
        itembawah: ws.itembawah || 0,
        itematas: ws.itematas || 0,
        vip: ws.vip || 0,
        viptanda: ws.viptanda || 0
      });

      if (!this.roomSeats.has(room)) this.roomSeats.set(room, new Map());
      this.roomSeats.get(room).set(seat, ws.username || ws.idTarget);

      this._send(ws, ["rooMasuk", seat, room]);
      this._send(ws, ["numberKursiSaya", seat]);
      this._broadcast(room, ["userJoinedRoom", ws.username || ws.idTarget, room]);

      if (this.roomUsers.has(room)) {
        const allUsers = this.roomUsers.get(room);
        const kursiList = [];
        for (const [username, userData] of allUsers) {
          if (username !== (ws.username || ws.idTarget)) {
            kursiList.push([userData.seat, {
              noimageUrl: userData.noimageUrl || '',
              namauser: username,
              color: userData.color || '#FFFFFF',
              itembawah: userData.itembawah || 0,
              itematas: userData.itematas || 0,
              vip: userData.vip || 0,
              viptanda: userData.viptanda || 0
            }]);
          }
        }
        if (kursiList.length > 0) {
          this._send(ws, ["kursiBatchUpdate", room, kursiList]);
        }
      }

      if (this.roomPoints.has(room)) {
        const points = this.roomPoints.get(room);
        const pointList = [];
        for (const [seat, pointData] of points) {
          pointList.push({ seat, x: pointData.x, y: pointData.y, fast: pointData.fast });
        }
        if (pointList.length > 0) {
          this._send(ws, ["allPointsList", room, pointList]);
        }
      }

    } catch(e) {
      console.error('Join room error:', e);
    }
  }

  // ============================================================
  // ✅ CLOSE
  // ============================================================
  _handleClose(ws) {
    try {
      if (!ws) return;
      ws._closing = true;

      const wsId = ws._wsId;
      const room = ws.room || this.wsRoom.get(wsId);
      const username = ws.username || ws.idTarget;

      if (room) {
        if (this.roomUsers.has(room)) {
          this.roomUsers.get(room).delete(username);
          if (this.roomUsers.get(room).size === 0) this.roomUsers.delete(room);
        }

        if (this.roomSeats.has(room) && ws.seatNumber) {
          this.roomSeats.get(room).delete(ws.seatNumber);
          if (this.roomSeats.get(room).size === 0) this.roomSeats.delete(room);
        }

        if (this.roomPoints.has(room) && ws.seatNumber) {
          this.roomPoints.get(room).delete(ws.seatNumber);
          if (this.roomPoints.get(room).size === 0) this.roomPoints.delete(room);
        }

        const clients = this.rooms.get(room);
        if (clients) {
          clients.delete(wsId);
          if (clients.size === 0) this.rooms.delete(room);
        }

        this.wsRoom.delete(wsId);
        this.wsSet.delete(wsId);

        this._broadcast(room, ["userLeftRoom", username, room]);
        this._broadcast(room, ["removeKursi", room, ws.seatNumber || 0]);
      }

      ws.room = null;
      ws.username = null;
      ws.idTarget = null;
      ws._wsId = null;
      ws._closing = true;

      try { ws.close(1000, "Closed"); } catch(e) {}

    } catch(e) {
      console.error('Close error:', e);
    }
  }

  // ============================================================
  // ✅ UTILITY
  // ============================================================
  _send(ws, message) {
    try {
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }

  _broadcast(room, message) {
    try {
      if (!room || !message) return;
      const wsIds = this.rooms.get(room);
      if (!wsIds?.size) return;

      const msgStr = JSON.stringify(message);
      const wsIdArray = Array.from(wsIds);

      for (const wsId of wsIdArray) {
        const ws = this.wsSet.get(wsId);
        if (ws && ws.readyState === 1 && !ws._closing) {
          try { ws.send(msgStr); } catch(e) {}
        }
      }

    } catch(e) {
      console.error('Broadcast error:', e);
    }
  }

  // ============================================================
  // ✅ DESTROY
  // ============================================================
  async destroy() {
    try {
      if (this._cleanupInterval) {
        clearInterval(this._cleanupInterval);
        this._cleanupInterval = null;
      }

      for (const [wsId, ws] of this.wsSet) {
        try { 
          if (ws && ws.readyState === 1) {
            ws.close(1000, "Server shutting down"); 
          }
        } catch(e) {}
      }

      this.wsSet.clear();
      this.rooms.clear();
      this.wsRoom.clear();
      this.userSeat.clear();
      this.roomSeats.clear();
      this.roomUsers.clear();
      this.roomPoints.clear();
      this.roomMutes.clear();

      console.log('🛑 ChatServer destroyed');

    } catch(e) {
      console.error('Destroy error:', e);
    }
  }
}
