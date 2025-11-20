// ChatServer Durable Object - FULL VERSION
import { LowCardGameManager } from "./lowcard.js";

const roomList = ["LowCard", "General", "Indonesia", "Chill Zone"];

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
    lastActivity: Date.now()
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

    // Initialize rooms
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }

    this.chatMessageBuffer = new Map();
    this.privateMessageBuffer = new Map();
    this.cleanupTimeouts = new Map();

    // Current number system
    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15 * 60 * 1000;

    // Simple timers
    this._flushTimer = setInterval(() => {
      this.periodicFlush();
    }, 1000);

    this._numberTimer = setInterval(() => {
      this.tickNumber();
    }, this.intervalMillis);

    this.lowcard = new LowCardGameManager(this);
  }

  async destroy() {
    clearInterval(this._flushTimer);
    clearInterval(this._numberTimer);
    
    // Clear all cleanup timeouts
    for (const timeout of this.cleanupTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.cleanupTimeouts.clear();
    
    for (const client of this.clients) {
      try {
        if (client.readyState === 1) {
          client.close(1000, "Server shutdown");
        }
      } catch (e) {}
    }
    this.clients.clear();
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
    for (const c of this.clients) {
      if (c.roomname === room && c.readyState === 1) {
        this.safeSend(c, msg);
      }
    }
  }

  getRoomUserCount(room) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return 0;
    
    let count = 0;
    for (const info of seatMap.values()) {
      if (info.namauser) {
        count++;
      }
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

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      if (messages.length > 0) {
        for (const msg of messages) {
          this.broadcastToRoom(room, msg);
        }
        this.chatMessageBuffer.set(room, []);
      }
    }
  }

  removeUserById(idtarget) {
    if (!idtarget) return;

    console.log(`Removing user: ${idtarget}`);
    
    let removedFromRooms = [];

    // Remove dari semua room
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      let removedFromThisRoom = false;
      
      for (const [seatNumber, info] of seatMap) {
        if (info.namauser === idtarget) {
          // Kosongkan kursi
          Object.assign(info, createEmptySeat());
          // Broadcast penghapusan
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          removedFromThisRoom = true;
        }
      }
      
      if (removedFromThisRoom) {
        removedFromRooms.push(room);
        // Update user count
        this.broadcastRoomUserCount(room);
      }
    }

    // Hapus dari mapping
    this.userToSeat.delete(idtarget);
    this.privateMessageBuffer.delete(idtarget);
    
    // Hapus cleanup timeout jika ada
    if (this.cleanupTimeouts.has(idtarget)) {
      clearTimeout(this.cleanupTimeouts.get(idtarget));
      this.cleanupTimeouts.delete(idtarget);
    }
    
    console.log(`User ${idtarget} removed from rooms:`, removedFromRooms);
  }

  immediateCleanup(ws, idtarget) {
    if (!idtarget) return;
    
    console.log(`Immediate cleanup for: ${idtarget}`);
    
    // Cek apakah ada connection lain dengan idtarget yang sama
    const hasOtherConnection = Array.from(this.clients).some(client => 
      client !== ws && 
      client.idtarget === idtarget && 
      client.readyState === 1
    );
    
    if (!hasOtherConnection) {
      // Tidak ada connection lain, lakukan cleanup
      this.removeUserById(idtarget);
    }
  }

  scheduleDelayedCleanup(ws, idtarget) {
    if (!idtarget) return;
    
    console.log(`Scheduling delayed cleanup for: ${idtarget}`);
    
    // Hapus timeout sebelumnya jika ada
    if (this.cleanupTimeouts.has(idtarget)) {
      clearTimeout(this.cleanupTimeouts.get(idtarget));
    }
    
    // Tunggu 10 detik sebelum cleanup untuk memberi waktu reconnect
    const timeout = setTimeout(() => {
      // Cek lagi apakah masih tidak ada connection aktif
      const hasActiveConnection = Array.from(this.clients).some(client => 
        client.idtarget === idtarget && 
        client.readyState === 1
      );
      
      if (!hasActiveConnection) {
        console.log(`Executing delayed cleanup for: ${idtarget}`);
        this.removeUserById(idtarget);
      }
      
      this.cleanupTimeouts.delete(idtarget);
    }, 10000); // 10 detik
    
    this.cleanupTimeouts.set(idtarget, timeout);
  }

  periodicFlush() {
    this.flushChatBuffer();
    
    // Deliver private messages
    for (const [id, msgs] of this.privateMessageBuffer) {
      for (const client of this.clients) {
        if (client.idtarget === id && client.readyState === 1) {
          for (const msg of msgs) {
            this.safeSend(client, msg);
          }
          this.privateMessageBuffer.delete(id);
          break;
        }
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

  findAvailableSeat(room, idtarget) {
    const seatMap = this.roomSeats.get(room);
    for (let i = 1; i <= this.MAX_SEATS; i++) {
      const seat = seatMap.get(i);
      if (seat.namauser === "") {
        seat.namauser = idtarget;
        seat.lastActivity = Date.now();
        return i;
      }
    }
    return null;
  }

  sendRoomState(ws, room) {
    if (ws.readyState !== 1) return;

    const seatMap = this.roomSeats.get(room);
    
    // Send current number
    this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    // Send room user count
    const count = this.getRoomUserCount(room);
    this.safeSend(ws, ["roomUserCount", room, count]);

    // Send all seats data
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

    // Send all points
    const allPoints = [];
    for (let seat = 1; seat <= this.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info.points.length > 0) {
        const recentPoints = info.points.slice(-5);
        for (const point of recentPoints) {
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

    // ✅ USER BARU - CLEANUP DULU DI SEMUA ROOM
    if (baru === true) {
      // Cleanup semua data user sebelumnya di SEMUA room
      for (const room of roomList) {
        const seatMap = this.roomSeats.get(room);
        for (const [seatNumber, info] of seatMap) {
          if (info.namauser === id) {
            // Kosongkan kursi yang diduduki user ini
            Object.assign(info, createEmptySeat());
            // Broadcast penghapusan kursi ke room tersebut
            this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          }
        }
        // Update jumlah user di room tersebut
        this.broadcastRoomUserCount(room);
      }

      // Hapus dari mapping userToSeat
      this.userToSeat.delete(id);
      
      // Hapus private messages buffer
      this.privateMessageBuffer.delete(id);
      
      // Hapus cleanup timeout jika ada
      if (this.cleanupTimeouts.has(id)) {
        clearTimeout(this.cleanupTimeouts.get(id));
        this.cleanupTimeouts.delete(id);
      }
    }
    // ✅ USER LAMA - COBA RESTORE SEAT
    else if (baru === false) {
      const seatInfo = this.userToSeat.get(id);
      
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const seatMap = this.roomSeats.get(room);
        const seatData = seatMap.get(seat);
        
        if (seatData.namauser === id) {
          // ✅ RESTORE BERHASIL - user masih di seat yang sama
          ws.roomname = room;
          this.sendRoomState(ws, room);
          
          // Hapus cleanup timeout karena user aktif kembali
          if (this.cleanupTimeouts.has(id)) {
            clearTimeout(this.cleanupTimeouts.get(id));
            this.cleanupTimeouts.delete(id);
          }
        } else {
          // ❌ SEAT SUDAH DIDUDUKI ORANG LAIN
          // Hapus mapping yang sudah tidak valid
          this.userToSeat.delete(id);
          this.safeSend(ws, ["needJoinRoom"]);
        }
      } else {
        // ❌ TIDAK ADA SEAT INFO
        this.safeSend(ws, ["needJoinRoom"]);
      }
    }

    // KIRIM PRIVATE MESSAGES YANG PENDING
    if (this.privateMessageBuffer.has(id)) {
      for (const msg of this.privateMessageBuffer.get(id)) {
        this.safeSend(ws, msg);
      }
      this.privateMessageBuffer.delete(id);
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
          if (idtarget) {
            this.removeUserById(idtarget);
          }
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
            if (!this.privateMessageBuffer.has(idtarget)) {
              this.privateMessageBuffer.set(idtarget, []);
            }
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
            if (!this.privateMessageBuffer.has(idt)) {
              this.privateMessageBuffer.set(idt, []);
            }
            this.privateMessageBuffer.get(idt).push(out);
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
          if (!this.chatMessageBuffer.has(roomname)) {
            this.chatMessageBuffer.set(roomname, []);
          }
          this.chatMessageBuffer.get(roomname)
            .push(["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          
          si.points.push({ x, y, fast, timestamp: Date.now() });
          si.lastActivity = Date.now();
          
          // Keep only recent points (3 detik)
          const now = Date.now();
          si.points = si.points.filter(point => now - point.timestamp < 3000);
          
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
            noimageUrl, namauser, color, itembawah, itematas, vip, viptanda,
            lastActivity: Date.now()
          });
          
          this.broadcastToRoom(room, ["updateKursi", room, seat, currentInfo]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!this.chatMessageBuffer.has(roomname)) {
            this.chatMessageBuffer.set(roomname, []);
          }
          this.chatMessageBuffer.get(roomname)
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
      console.error('Error in handleMessage:', error);
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
      ws.connectedAt = Date.now();

      this.clients.add(ws);

      ws.addEventListener("message", (ev) => {
        this.handleMessage(ws, ev.data);
      });

      ws.addEventListener("close", (event) => {
        console.log(`WebSocket closed: code=${event.code}, reason=${event.reason}`);
        
        const idtarget = ws.idtarget;
        
        // Jika ini graceful close (code 1000), langsung cleanup
        if (event.code === 1000) {
          this.immediateCleanup(ws, idtarget);
        } else {
          // Untuk unexpected close, beri waktu untuk reconnect
          this.scheduleDelayedCleanup(ws, idtarget);
        }
        
        this.clients.delete(ws);
      });

      ws.addEventListener("error", (error) => {
        console.log(`WebSocket error:`, error);
        
        const idtarget = ws.idtarget;
        
        // Untuk error, langsung cleanup
        this.immediateCleanup(ws, idtarget);
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
