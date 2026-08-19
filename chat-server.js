// ==================== CHAT-SERVER.JS - PERBAIKAN ====================

const ROOMS = [
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
];

const ROOMS_SET = new Set(ROOMS);
const MAX_SEATS = 45;
const BATCH_SIZE = 20;

export class ChatServer {
  constructor(env) {
    this.env = env;
    this.closing = false;
    this.isDestroyed = false;
    this._startTime = Date.now();
    this._wsIdCounter = 0;
    
    // WebSocket connections
    this.wsSet = new Set();
    this.roomClients = new Map();
    this.userConnections = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.wsActiveMulti = new Map();
    
    // Room data (cache) - INIT DENGAN DATA DEFAULT
    this.rooms = new Map();
    for (const room of ROOMS) {
      this.rooms.set(room, {
        seats: new Map(),
        points: new Map(),
        muted: false,
        number: 1
      });
      this.roomClients.set(room, new Set());
    }
    
    this.currentNumber = 1;
    this._tikCounter = 0;
    this._eventQueue = [];
    this._isProcessingQueue = false;
    this._pendingTimeouts = new Set();
    this._loadAttempted = false;
    
    // ========== LOAD STATE DARI D1 ==========
    // Load segera
    this.loadState().then(() => {
      console.log('[ChatServer] Initial load complete');
      this._loadAttempted = true;
    }).catch(e => {
      console.error('[ChatServer] Initial load failed:', e);
    });
    
    // Cleanup interval
    this._cleanupInterval = setInterval(() => {
      if (!this.closing && !this.isDestroyed) {
        this._cleanupDeadConnections();
        this._processEventQueue();
        // Auto-save every 30 seconds
        if (this._loadAttempted) {
          this.saveState().catch(() => {});
        }
      }
    }, 10000);
    this._pendingTimeouts.add(this._cleanupInterval);
    
    // Force load after 2 seconds jika belum
    setTimeout(async () => {
      if (!this._loadAttempted) {
        console.log('[ChatServer] Force loading state...');
        await this.loadState();
        this._loadAttempted = true;
      }
    }, 2000);
  }

  // ========== LOAD STATE DARI D1 ==========
  async loadState() {
    try {
      if (!this.env?.DB) {
        console.log('[ChatServer] No DB binding, using default state');
        return;
      }
      const db = this.env.DB;
      
      console.log('[ChatServer] Loading state from D1...');
      
      // Load room settings
      const settings = await db.prepare(`SELECT * FROM chat_rooms`).all();
      console.log(`[ChatServer] Loaded ${settings.results?.length || 0} room settings`);
      
      for (const row of settings.results || []) {
        const room = this.rooms.get(row.room_name);
        if (room) {
          room.muted = row.muted === 1;
          room.number = row.number || 1;
        }
      }
      
      // Load seats
      const seats = await db.prepare(`SELECT * FROM chat_seats`).all();
      console.log(`[ChatServer] Loaded ${seats.results?.length || 0} seats`);
      
      for (const row of seats.results || []) {
        const room = this.rooms.get(row.room_name);
        if (room) {
          room.seats.set(row.seat_number, {
            namauser: row.namauser || '',
            noimageUrl: row.noimageUrl || '',
            color: row.color || '',
            itembawah: row.itembawah || 0,
            itematas: row.itematas || 0,
            vip: row.vip || 0,
            viptanda: row.viptanda || 0
          });
        }
      }
      
      // Log jumlah seats per room
      for (const [roomName, room] of this.rooms) {
        console.log(`[ChatServer] Room ${roomName}: ${room.seats.size} seats`);
      }
      
      return true;
    } catch(e) {
      console.error('[ChatServer] Load error:', e);
      return false;
    }
  }

  // ========== SAVE STATE KE D1 ==========
  async saveState() {
    try {
      if (!this.env?.DB) return;
      const db = this.env.DB;
      
      // Save room settings
      for (const [roomName, room] of this.rooms) {
        await db.prepare(
          `INSERT OR REPLACE INTO chat_rooms (room_name, muted, number, updated_at)
           VALUES (?, ?, ?, unixepoch())`
        ).bind(roomName, room.muted ? 1 : 0, room.number || 1).run();
      }
      
      // Save seats - DELETE dulu
      await db.prepare(`DELETE FROM chat_seats`).run();
      
      for (const [roomName, room] of this.rooms) {
        for (const [seat, data] of room.seats) {
          await db.prepare(
            `INSERT INTO chat_seats 
             (room_name, seat_number, namauser, noimageUrl, color, itembawah, itematas, vip, viptanda, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch())`
          ).bind(
            roomName, seat, data.namauser || '', data.noimageUrl || '',
            data.color || '', data.itembawah || 0, data.itematas || 0,
            data.vip || 0, data.viptanda || 0
          ).run();
        }
      }
      
      // Save points
      await db.prepare(`DELETE FROM chat_points`).run();
      for (const [roomName, room] of this.rooms) {
        for (const [seat, point] of room.points) {
          await db.prepare(
            `INSERT INTO chat_points (room_name, seat_number, x, y, fast, updated_at)
             VALUES (?, ?, ?, ?, ?, unixepoch())`
          ).bind(roomName, seat, point.x || 0, point.y || 0, point.fast || 0).run();
        }
      }
      
      console.log('[ChatServer] Saved state to D1');
      return true;
    } catch(e) {
      console.error('[ChatServer] Save error:', e);
      return false;
    }
  }

  // ========== HANDLE JOIN - PASTIKAN KURSI TERLIHAT ==========
  _handleJoin(ws, roomName) {
    if (!ws || !roomName || !ROOMS_SET.has(roomName) || this.closing || this.isDestroyed) return;
    
    const username = ws.username;
    if (!username) return;
    
    const roomData = this.rooms.get(roomName);
    if (!roomData) return;
    
    console.log(`[Chat] ${username} joining ${roomName}, current seats: ${roomData.seats.size}`);
    
    // Remove from old room
    const oldRoom = ws.room;
    if (oldRoom && oldRoom !== roomName) {
      const oldRoomData = this.rooms.get(oldRoom);
      if (oldRoomData) {
        let oldSeat = null;
        for (const [s, data] of oldRoomData.seats) {
          if (data.namauser === username) { oldSeat = s; break; }
        }
        if (oldSeat) {
          oldRoomData.seats.delete(oldSeat);
          oldRoomData.points.delete(oldSeat);
          this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          this.broadcast(oldRoom, ["roomUserCount", oldRoom, oldRoomData.seats.size]);
        }
      }
      const oldClients = this.roomClients.get(oldRoom);
      if (oldClients) oldClients.delete(ws);
      this.userSeat.delete(username);
      this.userRoom.delete(username);
      ws.room = null;
    }
    
    // Check existing seat
    let seat = null;
    for (const [s, data] of roomData.seats) {
      if (data.namauser === username) { seat = s; break; }
    }
    
    // Find available seat
    if (!seat) {
      if (roomData.seats.size >= MAX_SEATS) {
        this.safeSend(ws, ["roomFull", roomName]);
        return;
      }
      for (let s = 1; s <= MAX_SEATS; s++) {
        if (!roomData.seats.has(s)) { seat = s; break; }
      }
    }
    
    if (!seat) {
      this.safeSend(ws, ["roomFull", roomName]);
      return;
    }
    
    // Add seat
    roomData.seats.set(seat, {
      namauser: username,
      noimageUrl: '',
      color: '',
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0
    });
    
    // Update state
    this.userSeat.set(username, { room: roomName, seat, isMulti: false });
    this.userRoom.set(username, roomName);
    ws.room = roomName;
    ws.idtarget = username;
    
    const clients = this.roomClients.get(roomName);
    if (clients && !clients.has(ws)) clients.add(ws);
    
    // ========== SEND ALL SEATS ==========
    const allSeats = {};
    for (const [s, data] of roomData.seats) {
      allSeats[s] = data;
    }
    
    console.log(`[Chat] Sending ${Object.keys(allSeats).length} seats to ${username}`);
    
    // Send responses - URUTAN PENTING!
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomData.muted, roomName]);
    this.safeSend(ws, ["roomUserCount", roomName, roomData.seats.size]);
    
    // Kirim semua kursi
    this.safeSend(ws, ["allUpdateKursiList", roomName, allSeats]);
    
    // Kirim points
    const allPoints = [];
    for (const [s, point] of roomData.points) {
      allPoints.push({ seat: s, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    if (allPoints.length > 0) {
      this.safeSend(ws, ["allPointsList", roomName, allPoints]);
    }
    
    // Broadcast ke room
    this.broadcast(roomName, ["roomUserCount", roomName, roomData.seats.size]);
    
    // Save ke D1
    this.saveState().catch(() => {});
  }

  // ========== BROADCAST ==========
  _broadcastToRoom(room, msgStr) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) {
      console.log(`[Chat] No clients in ${room} to broadcast`);
      return;
    }
    
    const clientArray = Array.from(clients);
    console.log(`[Chat] Broadcasting to ${clientArray.length} clients in ${room}`);
    
    for (let i = 0; i < clientArray.length; i += BATCH_SIZE) {
      const batch = clientArray.slice(i, i + BATCH_SIZE);
      for (const ws of batch) {
        if (ws && ws.readyState === 1 && !ws._closing) {
          try { ws.send(msgStr); } catch(e) {}
        }
      }
    }
  }

  // ========== FETCH ==========
  async fetch(req) {
    if (this.closing || this.isDestroyed) {
      return new Response("Shutting down", { status: 503 });
    }
    
    try {
      const upgrade = req.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Chat Server", { status: 200 });
      }
      
      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      
      try { server.accept(); } catch(e) {
        return new Response("WebSocket failed", { status: 500 });
      }
      
      const wsId = ++this._wsIdCounter;
      server._wsId = wsId;
      server.username = null;
      server.room = null;
      server._closing = false;
      
      // Event handlers
      server.addEventListener("message", async (event) => {
        try {
          if (server._closing || this.closing || this.isDestroyed) return;
          let data;
          try { data = JSON.parse(event.data); } catch(e) { return; }
          if (Array.isArray(data) && data.length > 0) {
            await this._handleEventInternal(server, data);
          }
        } catch(e) {}
      });
      
      server.addEventListener("close", () => { 
        console.log(`[Chat] Connection closed: ${wsId}`);
        this.cleanup(server); 
      });
      
      server.addEventListener("error", () => { 
        console.log(`[Chat] Connection error: ${wsId}`);
        this.cleanup(server); 
      });
      
      this.wsSet.add(server);
      
      console.log(`[Chat] New connection: ${wsId}`);
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      console.error('[Chat] Fetch error:', e);
      return new Response("Error", { status: 500 });
    }
  }
}
