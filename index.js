// ============================
// Cloudflare Workers + DO Chat (Sempurna)
// ============================

// ---- Konstanta Room ----
const roomList = [
  "Chill Zone","Catch Up","Casual Vibes","Lounge Talk",
  "Easy Talk","Friendly Corner","The Hangout",
  "Relax & Chat","Just Chillin","The Chatter Room"
];

// ---- Util seat ----
function createEmptySeat() {
  return {
    noimageUrl: "", namauser: "", color: "",
    itembawah: 0, itematas: 0, vip: false, viptanda: 0,
    points: [], lockTime: undefined
  };
}

// =====================
// Durable Object Server (Sempurna)
// =====================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // tetap sama nama variabel
    this.clients = new Set(); // ws augmented: {roomname, idtarget, numkursi:Set<number>}
    this.userToSeat = new Map(); // idtarget -> { room, seat }

    this.MAX_SEATS = 35;
    this.roomSeats = new Map();
    for (const room of roomList) {
      const m = new Map();
      for (let i = 1; i <= this.MAX_SEATS; i++) m.set(i, createEmptySeat());
      this.roomSeats.set(room, m);
    }

    // Buffers (tetap ada tapi kita tidak flush periodik secara otomatis)
    this.pointUpdateBuffer = new Map();   // room -> Map(seat -> [{x,y,fast}])
    this.updateKursiBuffer = new Map();   // room -> Map(seat -> seatInfo)
    this.chatMessageBuffer = new Map();   // room -> [msg...]
    this.privateMessageBuffer = new Map();// idtarget -> [msg...]

    // optional tick (sama seperti sebelumnya), jarang sehingga ringan
    this.currentNumber = 1;
    this.maxNumber = 6;
    this.intervalMillis = 15*60*1000;
    this._tickTimer = setInterval(()=>this.tick(), this.intervalMillis);

    // lock cleanup interval ringan (cek hanya lock timestamp)
    this._lockCleanupTimer = setInterval(()=>this.cleanExpiredLocks(), 3000); // tiap 3 detik
  }

  // ---------- Helpers ----------
  safeSend(ws, arr) {
    try {
      if (ws && ws.readyState === 1) ws.send(JSON.stringify(arr));
      else this.cleanupClient(ws);
    } catch (err) {
      console.error("safeSend error:", ws?.idtarget, err);
      try { this.cleanupClient(ws); } catch(e){}
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of Array.from(this.clients)) {
      if (c.roomname === room) {
        try { this.safeSend(c, msg); } catch (e) { console.error("broadcastToRoom error:", e); }
      }
    }
  }

  getJumlahRoom() {
    const cnt = Object.fromEntries(roomList.map(r=>[r,0]));
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      for (const info of seatMap.values()) {
        if (info.namauser && !String(info.namauser).startsWith("__LOCK__")) cnt[room]++;
      }
    }
    return cnt;
  }

  broadcastRoomUserCount(room){
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  // ----------------------------
  // Buffer helpers (we keep buffers but do not auto-flush them often)
  // we still provide flush functions if you want to call them externally
  // ----------------------------
  flushPrivateMessageBufferFor(idtarget){
    if(!this.privateMessageBuffer.has(idtarget)) return;
    const messages = this.privateMessageBuffer.get(idtarget);
    if(messages.length===0) return;
    for(const c of Array.from(this.clients)){
      if(c.idtarget===idtarget){
        for(const msg of messages){
          this.safeSend(c,msg);
        }
      }
    }
    messages.length = 0;
  }

  flushChatBufferForRoom(room){
    if(!this.chatMessageBuffer.has(room)) return;
    const messages = this.chatMessageBuffer.get(room);
    if(messages.length===0) return;
    for(const msg of messages){
      this.broadcastToRoom(room,msg);
    }
    messages.length = 0;
  }

  // optional full flush if wanted (not automatic)
  flushPointUpdates(){
    for(const [room, seatMap] of this.pointUpdateBuffer){
      for(const [seat, points] of seatMap){
        if(!points || points.length===0) continue;
        // broadcast all points for seat in order
        for(const p of points){
          this.broadcastToRoom(room, ["pointUpdated", room, seat, p.x, p.y, p.fast]);
        }
        points.length = 0;
      }
    }
  }

  flushKursiUpdates(){
    for(const [room, seatMap] of this.updateKursiBuffer){
      if(!seatMap || seatMap.size===0) continue;
      const updates = [];
      for(const [seat, info] of seatMap){
        const {points, ...rest} = info;
        updates.push([seat, rest]);
      }
      if(updates.length>0) this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      seatMap.clear();
    }
  }

  // --------------------------------
  // Keep tick (rare) to preserve behavior if client expects currentNumber
  // --------------------------------
  tick(){
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for(const c of Array.from(this.clients)){
      try{ this.safeSend(c, ["currentNumber", this.currentNumber]); } catch(e){ console.error("tick error:", e); }
    }
  }

  // -------------------------
  // Lock expiry: hanya mencari lock yang expired (namauser startsWith "__LOCK__")
  // -------------------------
  cleanExpiredLocks(){
    try{
      const now = Date.now();
      const expired = []; // collect removed to update counts after loop
      for(const room of roomList){
        const seatMap = this.roomSeats.get(room);
        for(const [seat, info] of seatMap){
          if(String(info.namauser).startsWith("__LOCK__") && info.lockTime && (now - info.lockTime) > 10000){
            // kosongkan kursi
            Object.assign(seatMap.get(seat), createEmptySeat());
            try{ this.broadcastToRoom(room, ["removeKursi", room, seat]); } catch(e){ console.error("cleanExpiredLocks broadcast:", e); }
            expired.push(room);
          }
        }
      }
      // update counts for affected rooms (dedupe)
      const dedupe = [...new Set(expired)];
      for(const r of dedupe) this.broadcastRoomUserCount(r);
    } catch(e){
      console.error("cleanExpiredLocks error:", e);
    }
  }

  // -------------------------
  // Seat/lock helpers (names kept)
  // -------------------------
  handleGetAllRoomsUserCount(ws){
    const allCounts = this.getJumlahRoom();
    const result = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  lockSeat(room, ws){
    const seatMap = this.roomSeats.get(room);
    if(!ws.idtarget) return null;

    // if already has a seat in same room, return it
    if(this.userToSeat.has(ws.idtarget)){
      const prev = this.userToSeat.get(ws.idtarget);
      if(prev.room === room) return prev.seat;
    }

    for(let i=1;i<=this.MAX_SEATS;i++){
      const k = seatMap.get(i);
      if(k && k.namauser === ""){
        k.namauser = "__LOCK__" + ws.idtarget;
        k.lockTime = Date.now();
        this.userToSeat.set(ws.idtarget, { room, seat: i });
        return i;
      }
    }
    return null;
  }

  sendAllStateTo(ws, room){
    const seatMap = this.roomSeats.get(room);
    const allPoints = [];
    const meta = {};
    for(const [seat, info] of seatMap){
      for(const p of info.points) allPoints.push({seat, ...p});
      if(info.namauser && !String(info.namauser).startsWith("__LOCK__")){
        const {points, ...rest} = info;
        meta[seat] = rest;
      }
    }
    this.safeSend(ws, ["allPointsList", room, allPoints]);
    this.safeSend(ws, ["allUpdateKursiList", room, meta]);
  }

  // -----------------------------
  // Strong cleanup helpers used on disconnect / forced cleanup
  // -----------------------------
  cleanupClientById(idtarget){
    // remove seats, buffers, and remove any live ws with same id
    this.removeAllSeatsById(idtarget);

    for(const c of Array.from(this.clients)){
      if(c.idtarget === idtarget){
        c.numkursi?.clear?.();
        c.roomname = undefined;
        c.idtarget = undefined;
        this.clients.delete(c);
      }
    }
  }

  removeAllSeatsById(idtarget) {
    // Remove seat state and broadcast removeKursi immediately
    for(const [room, seatMap] of this.roomSeats){
      for(const [seat, info] of seatMap){
        if(info.namauser === "__LOCK__" + idtarget || info.namauser === idtarget){
          Object.assign(seatMap.get(seat), createEmptySeat());
          try{ this.broadcastToRoom(room, ["removeKursi", room, seat]); } catch(e){ console.error("removeAllSeatsById broadcast:", e); }
          // Also clear any buffered points for this seat if present
          const roomBuf = this.pointUpdateBuffer.get(room);
          if(roomBuf && roomBuf.has(seat)) roomBuf.delete(seat);
          // Also remove updateKursiBuffer entry
          const kuBuf = this.updateKursiBuffer.get(room);
          if(kuBuf && kuBuf.has(seat)) kuBuf.delete(seat);
        }
      }
    }
    // Remove mapping
    this.userToSeat.delete(idtarget);
  }

  // -----------------------------
  // Main message handler (keep all case names same)
  // -----------------------------
  handleMessage(ws, raw){
    let data;
    try{
      data = JSON.parse(raw);
    } catch(e){
      console.error("Invalid JSON:", raw, e);
      return this.safeSend(ws, ["error", "Invalid JSON"]);
    }
    if(!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);

    const evt = data[0];

    try{
      switch(evt){
        case "setIdTarget": {
          const newId = data[1];
          // if some other socket using same id, cleanup first
          this.cleanupClientById(newId);
          ws.idtarget = newId;
          this.safeSend(ws, ["setIdTargetAck", ws.idtarget]);
          break;
        }

        case "ping": {
          const pingId = data[1];
          if(pingId && ws.idtarget === pingId) this.safeSend(ws, ["pong"]);
          break;
        }

        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          let delivered = false;
          for(const c of this.clients){ if(c.idtarget === idtarget){ this.safeSend(c, notif); delivered = true; } }
          if(!delivered){
            if(!this.privateMessageBuffer.has(idtarget)) this.privateMessageBuffer.set(idtarget, []);
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
          for(const c of this.clients){ if(c.idtarget === idt){ this.safeSend(c, out); delivered = true; } }
          if(!delivered){
            if(!this.privateMessageBuffer.has(idt)) this.privateMessageBuffer.set(idt, []);
            this.privateMessageBuffer.get(idt).push(out);
            this.safeSend(ws, ["privateFailed", idt, "User offline"]);
          }
          break;
        }

        case "isUserOnline": {
          const target = data[1];
          const tanda = data[2] ?? "";
          const online = Array.from(this.clients).some(c => c.idtarget === target);
          this.safeSend(ws, ["userOnlineStatus", target, online, tanda]);
          break;
        }

        case "getAllRoomsUserCount": this.handleGetAllRoomsUserCount(ws); break;
        case "getCurrentNumber": this.safeSend(ws, ["currentNumber", this.currentNumber]); break;

        case "joinRoom": {
          const newRoom = data[1];
          if(!roomList.includes(newRoom)) return this.safeSend(ws, ["error", `Unknown room: ${newRoom}`]);

          // ensure previous seats/locks removed immediately
          if(ws.idtarget) this.removeAllSeatsById(ws.idtarget);

          ws.roomname = newRoom;
          const seatMap = this.roomSeats.get(newRoom);
          const foundSeat = this.lockSeat(newRoom, ws);
          if(foundSeat === null) return this.safeSend(ws, ["roomFull", newRoom]);

          ws.numkursi = new Set([foundSeat]);
          this.safeSend(ws, ["numberKursiSaya", foundSeat]);
          if(ws.idtarget) this.userToSeat.set(ws.idtarget, { room: newRoom, seat: foundSeat });
          this.sendAllStateTo(ws, newRoom);
          this.broadcastRoomUserCount(newRoom);
          break;
        }

        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if(!roomList.includes(roomname)) return this.safeSend(ws, ["error", "Invalid room for chat"]);
          // broadcast immediately (ultra-light)
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }

        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if(!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          const si = seatMap.get(seat);
          if(!si) return;
          // push to seat state (kept short)
          si.points.push({ x, y, fast });
          if(si.points.length > 200) si.points.shift();

          // immediate broadcast (ultra-light). If you want batching, replace with buffer + flush.
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }

        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if(!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatMap = this.roomSeats.get(room);
          Object.assign(seatMap.get(seat), createEmptySeat());
          // clear any buffered points for that seat
          const rBuf = this.pointUpdateBuffer.get(room);
          if(rBuf && rBuf.has(seat)) rBuf.delete(seat);
          // also update updateKursiBuffer
          const kuBuf = this.updateKursiBuffer.get(room);
          if(kuBuf && kuBuf.has(seat)) kuBuf.delete(seat);

          for(const c of this.clients) c.numkursi?.delete(seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
          break;
        }

        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if(!roomList.includes(room)) return this.safeSend(ws, ["error", `Unknown room: ${room}`]);
          const seatInfo = { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda, points: [] };
          // update immediate
          this.roomSeats.get(room).set(seat, seatInfo);
          // maintain updateKursiBuffer if needed later
          if(!this.updateKursiBuffer.has(room)) this.updateKursiBuffer.set(room, new Map());
          this.updateKursiBuffer.get(room).set(seat, seatInfo);
          this.broadcastRoomUserCount(room);
          break;
        }

        default:
          this.safeSend(ws, ["error", "Unknown event"]);
      }
    } catch(err){
      console.error("handleMessage error:", ws.idtarget, err);
      this.safeSend(ws, ["error", "Internal error"]);
    }
  }

  // -----------------------
  // Disconnect cleanup: immediate, complete, and safe
  // -----------------------
  cleanupClient(ws){
    try{
      const id = ws.idtarget;
      const room = ws.roomname;
      const kursis = ws.numkursi;

      if(id){
        // remove any seat owned/locked by id across all rooms
        this.removeAllSeatsById(id);

        // clear buffered points for this id (scan pointUpdateBuffer by seat ownership)
        for(const [r, seatMap] of this.pointUpdateBuffer){
          for(const [seat, points] of seatMap){
            // if seat in main roomSeats is now empty, clear buffer for it
            const mainSeat = this.roomSeats.get(r)?.get(seat);
            if(!mainSeat || !mainSeat.namauser) {
              seatMap.delete(seat);
            }
          }
          if(seatMap.size===0) this.pointUpdateBuffer.delete(r);
        }

        // remove updateKursiBuffer entries with that id
        for(const [r, seatMap] of this.updateKursiBuffer){
          for(const [seat, info] of seatMap){
            if(info.namauser === id || info.namauser === "__LOCK__"+id) seatMap.delete(seat);
          }
          if(seatMap.size===0) this.updateKursiBuffer.delete(r);
        }

        this.userToSeat.delete(id);
      }

      // remove seats recorded on this ws (fallback)
      if(room && kursis && this.roomSeats.has(room)){
        const seatMap = this.roomSeats.get(room);
        for(const seat of kursis){
          Object.assign(seatMap.get(seat), createEmptySeat());
          try{ this.broadcastToRoom(room, ["removeKursi", room, seat]); } catch(e){ console.error("cleanupClient broadcast error:", e); }
        }
        this.broadcastRoomUserCount(room);
      }

    } catch(e){
      console.error("cleanupClient error:", e);
    } finally {
      // final ws removal
      this.clients.delete(ws);
      try{ ws.numkursi?.clear?.(); } catch(e){}
      ws.roomname = undefined;
      ws.idtarget = undefined;
    }
  }

  // -----------------------
  // fetch: WebSocket upgrade handling (unchanged names)
  // -----------------------
  async fetch(request){
    const upgrade = request.headers.get("Upgrade") || request.headers.get("upgrade") || "";
    if(upgrade.toLowerCase() !== "websocket") return new Response("Expected WebSocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const ws = server;
    ws.roomname = undefined;
    ws.idtarget = undefined;
    ws.numkursi = new Set();
    this.clients.add(ws);

    ws.addEventListener("message", (ev) => this.handleMessage(ws, ev.data));
    ws.addEventListener("close", () => this.cleanupClient(ws));
    ws.addEventListener("error", () => this.cleanupClient(ws));

    return new Response(null, { status: 101, webSocket: client });
  }
}

// ======================
// Worker Entry (Router)
// ======================
export default {
  async fetch(req, env){
    if((req.headers.get("Upgrade") || "").toLowerCase() === "websocket"){
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if(new URL(req.url).pathname === "/health")
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" }});
    return new Response("WebSocket endpoint at wss://<your-subdomain>.workers.dev", { status: 200 });
  }
};
