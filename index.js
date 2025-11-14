// ChatServer Durable Object (Bahasa Indonesia)
// Versi stabil tanpa log dan error event

import { LowCardGameManager } from "./lowcard.js";

const ROOM_LIST = Object.freeze([
  "LowCard", "General", "Indonesia", "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout", "Relax & Chat", "Just Chillin", "The Chatter Room"
]);

const ROOM_SET = new Set(ROOM_LIST);
const MAX_SEATS = 35;
const NUMBER_INTERVAL = 15 * 60 * 1000; // 15 menit
const GRACE_PERIOD = 5000; // 5 detik
const LOCK_TIMEOUT = 5000; // 5 detik

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
    lockTime: 0
  };
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.clients = new Set();
    this.userToSeat = new Map();
    this.pendingRemove = new Map();
    this.connectionAttempts = new Map();

    this.roomSeats = new Map();
    const emptySeat = createEmptySeat();
    
    for (const room of ROOM_LIST) {
      const seats = new Map();
      for (let i = 1; i <= MAX_SEATS; i++) {
        seats.set(i, { ...emptySeat });
      }
      this.roomSeats.set(room, seats);
    }

    this.currentNumber = 1;
    this.maxNumber = 6;
    
    this._tickTimer = null;
    this.startTickTimer();
  }

  startTickTimer() {
    try {
      if (this._tickTimer) {
        clearInterval(this._tickTimer);
      }
      this._tickTimer = setInterval(() => this.tick(), NUMBER_INTERVAL);
    } catch (e) {
      // Timer error handled silently
    }
  }

  sendCurrentNumber(ws) {
    if (!this.isWebSocketReady(ws)) return false;
    
    try {
      ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
      return true;
    } catch (e) {
      return false;
    }
  }

  isWebSocketReady(ws) {
    return ws && ws.readyState === 1;
  }

  safeSend(ws, data) {
    if (!this.isWebSocketReady(ws)) return false;
    
    try {
      ws.send(JSON.stringify(data));
      return true;
    } catch (e) {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    if (!ROOM_SET.has(room)) return 0;
    
    let sentCount = 0;
    const msgStr = JSON.stringify(msg);
    
    for (const client of this.clients) {
      if (client.roomname === room && client.readyState === 1) {
        try {
          client.send(msgStr);
          sentCount++;
        } catch (e) {
          // Skip failed sends
        }
      }
    }
    return sentCount;
  }

  broadcastToAll(msg) {
    let sentCount = 0;
    const msgStr = JSON.stringify(msg);
    
    for (const client of this.clients) {
      if (client.readyState === 1) {
        try {
          client.send(msgStr);
          sentCount++;
        } catch (e) {
          // Skip failed sends
        }
      }
    }
    return sentCount;
  }

  getRoomUserCounts() {
    const counts = {};
    
    for (const room of ROOM_LIST) {
      const seatMap = this.roomSeats.get(room);
      let count = 0;
      
      for (const info of seatMap.values()) {
        if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
          count++;
        }
      }
      
      counts[room] = count;
    }
    return counts;
  }

  broadcastRoomUserCount(room) {
    if (!ROOM_SET.has(room)) return;
    
    const counts = this.getRoomUserCounts();
    this.broadcastToRoom(room, ["roomUserCount", room, counts[room]]);
  }

  tick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const currentNumberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
      
      for (const client of this.clients) {
        if (client.readyState === 1) {
          try {
            client.send(currentNumberMsg);
          } catch (e) {
            // Skip failed sends
          }
        }
      }
    } catch (e) {
      // Tick error handled silently
    }
  }

  cleanExpiredLocks() {
    const now = Date.now();
    const expiredLocks = [];
    
    for (const room of ROOM_LIST) {
      const seatMap = this.roomSeats.get(room);
      
      for (const [seat, info] of seatMap) {
        if (info.namauser.startsWith("__LOCK__") && 
            info.lockTime && 
            (now - info.lockTime) > LOCK_TIMEOUT) {
          expiredLocks.push({ room, seat });
        }
      }
    }
    
    for (const { room, seat } of expiredLocks) {
      const seatMap = this.roomSeats.get(room);
      Object.assign(seatMap.get(seat), createEmptySeat());
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
      this.broadcastRoomUserCount(room);
    }
  }

  cleanupClosedWebSockets() {
    const toRemove = [];
    
    for (const client of this.clients) {
      if (client.readyState === 2 || client.readyState === 3) {
        toRemove.push(client);
      }
    }
    
    for (const client of toRemove) {
      this.cleanupClient(client);
    }
  }

  periodicMaintenance() {
    this.cleanExpiredLocks();
    this.cleanupClosedWebSockets();
  }

  lockSeat(room, ws) {
    if (!ws.idtarget || !ROOM_SET.has(room)) return null;
    
    const seatMap = this.roomSeats.get(room);
    const now = Date.now();
    const emptySeat = createEmptySeat();

    for (let seat = 1; seat <= MAX_SEATS; seat++) {
      const seatInfo = seatMap.get(seat);
      
      const isExpiredLock = seatInfo.namauser.startsWith("__LOCK__") && 
                           seatInfo.lockTime && 
                           (now - seatInfo.lockTime) > LOCK_TIMEOUT;
      
      if (isExpiredLock) {
        Object.assign(seatInfo, emptySeat);
      }
      
      if (seatInfo.namauser === "") {
        seatInfo.namauser = `__LOCK__${ws.idtarget}`;
        seatInfo.lockTime = now;
        this.userToSeat.set(ws.idtarget, { room, seat });
        return seat;
      }
    }
    
    return null;
  }

  sendRoomState(ws, room) {
    if (!this.isWebSocketReady(ws) || !ROOM_SET.has(room)) return;
    
    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const seatMetadata = {};

    for (let seat = 1; seat <= MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (!info) continue;

      if (info.points.length > 0) {
        for (const point of info.points) {
          allPoints.push({ seat, ...point });
        }
      }

      if (info.namauser && !info.namauser.startsWith("__LOCK__")) {
        seatMetadata[seat] = {
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

    if (allPoints.length > 0) {
      this.safeSend(ws, ["allPointsList", room, allPoints]);
    }
    
    if (Object.keys(seatMetadata).length > 0) {
      this.safeSend(ws, ["allUpdateKursiList", room, seatMetadata]);
    }
    
    this.sendCurrentNumber(ws);
  }

  removeUserSeats(userId) {
    if (!userId) return 0;
    
    const seatsToRemove = [];
    
    for (const room of ROOM_LIST) {
      const seatMap = this.roomSeats.get(room);
      
      for (const [seat, info] of seatMap) {
        const isUserSeat = info.namauser === userId || 
                          info.namauser.startsWith(`__LOCK__${userId}`);
        
        if (isUserSeat) {
          seatsToRemove.push({ room, seat });
        }
      }
    }

    for (const { room, seat } of seatsToRemove) {
      const seatMap = this.roomSeats.get(room);
      Object.assign(seatMap.get(seat), createEmptySeat());
      this.broadcastToRoom(room, ["removeKursi", room, seat]);
      this.broadcastRoomUserCount(room);
    }

    this.userToSeat.delete(userId);
    return seatsToRemove.length;
  }

  batalkanPendingRemoval(userId) {
    if (userId && this.pendingRemove.has(userId)) {
      clearTimeout(this.pendingRemove.get(userId));
      this.pendingRemove.delete(userId);
    }
  }

  getAllOnlineUsers() {
    const users = [];
    for (const client of this.clients) {
      if (client.idtarget && client.readyState === 1) {
        users.push(client.idtarget);
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    if (!ROOM_SET.has(roomName)) return [];
    
    const users = [];
    for (const client of this.clients) {
      if (client.roomname === roomName && client.idtarget && client.readyState === 1) {
        users.push(client.idtarget);
      }
    }
    return users;
  }


cleanupforce(ws) {
  const userId = ws.idtarget;

  this.clients.delete(ws);

  if (userId) {
    if (this.connectionAttempts.has(userId)) {
      clearTimeout(this.connectionAttempts.get(userId));
      this.connectionAttempts.delete(userId);
    }

    let hasActiveConnection = false;
    for (const client of this.clients) {
      if (client.idtarget === userId && client.readyState === 1) {
        hasActiveConnection = true;
        break;
      }
    }

    if (!hasActiveConnection) {
      this.batalkanPendingRemoval(userId);

      // TANPA GRACE PERIOD → langsung hapus
      this.removeUserSeats(userId);
      this.pendingRemove.delete(userId);

    } else {
      this.batalkanPendingRemoval(userId);
    }
  }
}


  
  cleanupClient(ws) {
    const userId = ws.idtarget;
    
    this.clients.delete(ws);
    
    if (userId) {
      if (this.connectionAttempts.has(userId)) {
        clearTimeout(this.connectionAttempts.get(userId));
        this.connectionAttempts.delete(userId);
      }
      
      let hasActiveConnection = false;
      for (const client of this.clients) {
        if (client.idtarget === userId && client.readyState === 1) {
          hasActiveConnection = true;
          break;
        }
      }
      
      if (!hasActiveConnection) {
        this.batalkanPendingRemoval(userId);

        const timeout = setTimeout(() => {
          this.removeUserSeats(userId);
          this.pendingRemove.delete(userId);
        }, GRACE_PERIOD);

        this.pendingRemove.set(userId, timeout);
      } else {
        this.batalkanPendingRemoval(userId);
      }
    }

    if (ws.numkursi) {
      ws.numkursi.clear();
    }
    ws.roomname = undefined;
    ws.idtarget = undefined;
  }

  isInLowcardRoom(ws) {
    return ws.roomname === "LowCard";
  }

  handleMessage(ws, rawMessage) {
    let data;
    try { 
      data = JSON.parse(rawMessage); 
    } catch (e) { 
      return this.safeSend(ws, ["error", "Invalid JSON"]); 
    }
    
    if (!Array.isArray(data) || data.length === 0) {
      return this.safeSend(ws, ["error", "Invalid message format"]);
    }
    
    const [eventType, ...args] = data;
    
    if (eventType === "joinRoom" || eventType === "updateKursi" || eventType === "removeKursiAndPoint") {
      this.periodicMaintenance();
    }

    switch (eventType) {
      case "setIdTarget":
        this.handleSetIdTarget(ws, args[0]);
        break;
        
        case "setIdTarget2":
     this.handleSetIdTarget2(ws, args[0], args[1]);
     break;


      case "sendnotif":
        this.handleSendNotification(ws, args);
        break;

      case "private":
        this.handlePrivateMessage(ws, args);
        break;

      case "isUserOnline":
        this.handleUserOnlineCheck(ws, args[0], args[1]);
        break;

      case "getAllRoomsUserCount":
        this.handleGetAllRoomsUserCount(ws);
        break;
        
      case "getCurrentNumber":
        this.sendCurrentNumber(ws);
        break;
        
      case "getAllOnlineUsers":
        this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
        break;
        
      case "getRoomOnlineUsers":
        this.handleGetRoomOnlineUsers(ws, args[0]);
        break;

      case "joinRoom":
        this.handleJoinRoom(ws, args[0]);
        break;

      case "chat":
        this.handleChatMessage(ws, args);
        break;

      case "updatePoint":
        this.handleUpdatePoint(ws, args);
        break;

      case "removeKursiAndPoint":
        this.handleRemoveSeat(ws, args[0], args[1]);
        break;

      case "updateKursi":
        this.handleUpdateSeat(ws, args);
        break;

      case "gift":
        this.handleGift(ws, args);
        break;

      case "onDestroy":
        this.handleDestroy(ws);
        break;

      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (!this.isInLowcardRoom(ws)) {
          return this.safeSend(ws, ["error", "Game Lowcard hanya tersedia di room LowCard"]);
        }
        this.lowcard.handleEvent(ws, data);
        break;

      default:
        this.safeSend(ws, ["error", "Unknown event"]);
    }
  }


  

handleSetIdTarget2(ws, id, baru) {

  ws.idtarget = id;

  // =======================
  // USER BARU
  // =======================
  if (baru === true) {

    // bersihkan pending remove kalau ada
    if (this.pendingRemove.has(id)) {
      clearTimeout(this.pendingRemove.get(id));
      this.pendingRemove.delete(id);
    }

    ws.isNewUser = true;
    return;
  }

  // =======================
  // RECONNECT (baru = false)
  // =======================
  ws.isNewUser = false;

  // hapus pending remove + timeout
  if (this.pendingRemove.has(id)) {
    clearTimeout(this.pendingRemove.get(id));
    this.pendingRemove.delete(id);
  }

  const seatInfo = this.userToSeat.get(id);

  if (seatInfo && this.isWebSocketReady(ws)) {
    ws.roomname = seatInfo.room;

    // restore room state
    this.sendRoomState(ws, seatInfo.room);

    // broadcast update user count
    this.broadcastRoomUserCount(seatInfo.room);
  }

  console.log("SET ID 2:", id, "baru:", baru);
}


  console.log("SET ID 2:", id, "baru:", baru);
}

  

  handleSetIdTarget(ws, newId) {
    if (!newId) return;
    
    this.batalkanPendingRemoval(newId);
    
    if (this.connectionAttempts.has(newId)) {
      clearTimeout(this.connectionAttempts.get(newId));
      this.connectionAttempts.delete(newId);
    }
    
    ws.idtarget = newId;

    const duplicates = [];
    for (const client of this.clients) {
      if (client.idtarget === newId && client !== ws) {
        duplicates.push(client);
      }
    }

    if (duplicates.length > 0) {
      setTimeout(() => {
        for (const dup of duplicates) {
          try {
            if (dup.readyState === 1) {
              dup.close(4000, "Duplicate connection");
            }
            this.clients.delete(dup);
          } catch (e) {
            // Error closing duplicate handled silently
          }
        }
      }, 100);
    }

    // ✅ PERBAIKAN: Langsung restore state tanpa timeout
    const seatInfo = this.userToSeat.get(newId);
    if (seatInfo && this.isWebSocketReady(ws)) {
      ws.roomname = seatInfo.room;
      this.sendRoomState(ws, seatInfo.room);
      this.broadcastRoomUserCount(seatInfo.room);
    }
  }

  handleSendNotification(ws, [targetId, imageUrl, username, description]) {
    if (!targetId) return;
    
    const notification = ["notif", imageUrl, username, description, Date.now()];
    let delivered = false;
    
    for (const client of this.clients) {
      if (client.idtarget === targetId && client.readyState === 1) {
        this.safeSend(client, notification);
        delivered = true;
      }
    }
    
    if (!delivered) {
      this.safeSend(ws, ["notifFailed", targetId, "User offline"]);
    }
  }

  handlePrivateMessage(ws, [targetId, imageUrl, message, sender]) {
    if (!targetId) return;
    
    const privateMsg = ["private", targetId, imageUrl, message, Date.now(), sender];
    this.safeSend(ws, privateMsg);
    
    let delivered = false;
    for (const client of this.clients) {
      if (client.idtarget === targetId && client.readyState === 1) {
        this.safeSend(client, privateMsg);
        delivered = true;
      }
    }
    
    if (!delivered) {
      this.safeSend(ws, ["privateFailed", targetId, "User offline"]);
    }
  }

  handleUserOnlineCheck(ws, username, marker = "") {
    if (!username) return;
    
    const activeConnections = [];
    for (const client of this.clients) {
      if (client.idtarget === username && client.readyState === 1) {
        activeConnections.push(client);
      }
    }
    
    const isOnline = activeConnections.length > 0;
    this.safeSend(ws, ["userOnlineStatus", username, isOnline, marker]);

    if (activeConnections.length > 1) {
      this.handleDuplicateConnections(username, activeConnections);
    }
  }

  handleDuplicateConnections(username, connections) {
    if (connections.length <= 1) return;
    
    const newest = connections[connections.length - 1];
    const oldConnections = connections.slice(0, -1);

    const seatInfo = this.userToSeat.get(username);
    if (seatInfo) {
      const { room, seat } = seatInfo;
      const seatMap = this.roomSeats.get(room);
      
      if (seatMap && seatMap.has(seat)) {
        Object.assign(seatMap.get(seat), createEmptySeat());
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        this.broadcastRoomUserCount(room);
      }
      
      this.userToSeat.delete(username);
    }

    for (const oldConnection of oldConnections) {
      if (oldConnection.readyState === 1) {
        oldConnection.close(4000, "Duplicate login");
      }
      this.clients.delete(oldConnection);
    }
  }

  handleGetAllRoomsUserCount(ws) {
    const allCounts = this.getRoomUserCounts();
    const result = [];
    for (const room of ROOM_LIST) {
      result.push([room, allCounts[room]]);
    }
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  handleGetRoomOnlineUsers(ws, roomName) {
    if (!ROOM_SET.has(roomName)) {
      return this.safeSend(ws, ["error", "Unknown room"]);
    }
    
    this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
  }

  handleJoinRoom(ws, newRoom) {
    if (!ROOM_SET.has(newRoom)) {
      return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);
    }
    
    if (ws.idtarget) {
      this.batalkanPendingRemoval(ws.idtarget);
      this.removeUserSeats(ws.idtarget);
    }
    
    ws.roomname = newRoom;
    const assignedSeat = this.lockSeat(newRoom, ws);
    
    if (assignedSeat === null) {
      return this.safeSend(ws, ["roomFull", newRoom]);
    }
    
    if (!ws.numkursi) ws.numkursi = new Set();
    ws.numkursi.add(assignedSeat);
    
    this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
    
    if (ws.idtarget) {
      this.userToSeat.set(ws.idtarget, { room: newRoom, seat: assignedSeat });
    }
    
    if (this.isWebSocketReady(ws)) {
      this.sendRoomState(ws, newRoom);
      this.broadcastRoomUserCount(newRoom);
      this.sendCurrentNumber(ws);
    }
  }

  handleChatMessage(ws, [room, imageUrl, username, message, usernameColor, textColor]) {
    if (!ROOM_SET.has(room)) return;
    
    this.broadcastToRoom(room, ["chat", room, imageUrl, username, message, usernameColor, textColor]);
  }

  handleUpdatePoint(ws, [room, seat, x, y, fast]) {
    if (!ROOM_SET.has(room)) return;
    
    const seatMap = this.roomSeats.get(room);
    const seatInfo = seatMap.get(seat);
    
    if (seatInfo) {
      seatInfo.points = [{ x, y, fast }];
      this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
    }
  }

  handleRemoveSeat(ws, room, seat) {
    if (!ROOM_SET.has(room)) return;
    
    const seatMap = this.roomSeats.get(room);
    Object.assign(seatMap.get(seat), createEmptySeat());
    
    for (const client of this.clients) {
      if (client.numkursi) {
        client.numkursi.delete(seat);
      }
    }
    
    this.broadcastToRoom(room, ["removeKursi", room, seat]);
    this.broadcastRoomUserCount(room);
  }

  handleUpdateSeat(ws, [room, seat, imageUrl, username, color, bottomItem, topItem, vip, vipMark]) {
    if (!ROOM_SET.has(room)) return;
    
    const seatMap = this.roomSeats.get(room);
    const currentInfo = seatMap.get(seat);
    
    if (currentInfo) {
      Object.assign(currentInfo, { 
        noimageUrl: imageUrl, 
        namauser: username, 
        color, 
        itembawah: bottomItem, 
        itematas: topItem, 
        vip, 
        viptanda: vipMark
      });
      
      this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
        noimageUrl: imageUrl, 
        namauser: username, 
        color, 
        itembawah: bottomItem, 
        itematas: topItem, 
        vip, 
        viptanda: vipMark
      }]]]);
      
      this.broadcastRoomUserCount(room);
    }
  }

  handleGift(ws, [room, sender, receiver, giftName]) {
    if (!ROOM_SET.has(room)) return;
    
    this.broadcastToRoom(room, ["gift", room, sender, receiver, giftName, Date.now()]);
  }

  handleDestroy(ws) {
    if (ws.idtarget) {
      this.cleanupforce(ws);
    }
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    
    if (upgradeHeader?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    server.roomname = undefined;
    server.idtarget = undefined;
    server.numkursi = new Set();
    
    this.clients.add(server);

    const messageHandler = (event) => {
      this.handleMessage(server, event.data);
    };

    const closeHandler = () => {
      this.cleanupClient(server);
    };

    const errorHandler = () => {
      this.cleanupClient(server);
    };

    server.addEventListener("message", messageHandler);
    server.addEventListener("close", closeHandler);
    server.addEventListener("error", errorHandler);

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(req, env) {
    const upgradeHeader = req.headers.get("Upgrade");
    const url = new URL(req.url);
    
    if (upgradeHeader?.toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    
    if (url.pathname === "/health") {
      return new Response("ok", { 
        status: 200, 
        headers: { "content-type": "text/plain" } 
      });
    }
    
    return new Response("WebSocket endpoint", { status: 200 });
  }
}


