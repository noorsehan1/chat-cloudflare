// ============================
// Cloudflare Worker - Kursi Chat
// ============================

// ---- Konstanta Room ----
const roomList = [
  "Chill Zone","Catch Up","Casual Vibes","Lounge Talk",
  "Easy Talk","Friendly Corner","The Hangout",
  "Relax & Talk","Late Night","Morning Coffee"
];

// ---- Helper kursi kosong ----
function createEmptySeat() {
  return {
    noimageUrl: "",
    namauser: "",
    color: "",
    itembawah: 0,
    itematas: 0,
    vip: 0,       // int biar sama dengan Java
    viptanda: 0,
    points: [],
    lockTime: undefined
  };
}

// ============================
// Durable Object
// ============================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.clients = new Map();  // connId -> WebSocket
    this.rooms = new Map();    // roomName -> Map<seatIndex,obj>
    this.updateQueue = new Map(); // roomName -> Map<seatIndex,obj>

    // flush kursi update batch tiap 200ms
    this.flushInterval = setInterval(() => this.flushKursiUpdates(), 200);
  }

  // ---- Kirim aman ke client ----
  safeSend(ws, msg) {
    try { ws.send(JSON.stringify(msg)); } catch(e) {}
  }

  // ---- Broadcast ke semua client di room ----
  broadcast(room, msg) {
    for (let [id, ws] of this.clients) {
      if (ws.room === room) {
        this.safeSend(ws, msg);
      }
    }
  }

  // ---- Broadcast jumlah user di room ----
  broadcastRoomUserCount(room) {
    let count = 0;
    for (let [, ws] of this.clients) {
      if (ws.room === room) count++;
    }
    this.broadcast(room, ["roomUserCount", room, count]);
  }

  // ---- Kirim state kursi + point saat user join ----
  sendAllStateTo(ws, room) {
    const seats = this.rooms.get(room) || new Map();

    // format sesuai Java → object { seatIndex: info }
    const meta = {};
    for (let [seat, info] of seats) {
      meta[seat] = info;
    }
    this.safeSend(ws, ["allUpdateKursiList", room, meta]);

    // points
    for (let [seat, info] of seats) {
      if (info.points && info.points.length > 0) {
        this.safeSend(ws, ["allUpdatePoints", room, seat, info.points]);
      }
    }
  }

  // ---- Flush batch kursi ----
  flushKursiUpdates() {
    for (let [room, seatMap] of this.updateQueue) {
      if (seatMap.size === 0) continue;
      const updates = [];
      for (let [seat, info] of seatMap) {
        updates.push([seat, info]);  // array [seatIndex, obj]
      }
      this.broadcast(room, ["kursiBatchUpdate", room, updates]);
      seatMap.clear();
    }
  }

  // ---- Handle pesan dari client ----
  handleMessage(ws, msg) {
    let data;
    try { data = JSON.parse(msg); } catch { return; }

    const type = data[0];
    switch (type) {
      case "join": {
        const room = data[1];
        ws.room = room;
        this.sendAllStateTo(ws, room);
        this.broadcastRoomUserCount(room);
        break;
      }

      case "leave": {
        const room = ws.room;
        ws.room = null;
        this.broadcastRoomUserCount(room);
        break;
      }

      case "updateKursi": {
        const room = data[1];
        const seat = data[2];
        const info = {
          noimageUrl: data[3],
          namauser:  data[4],
          color:     data[5],
          itembawah: data[6],
          itematas:  data[7],
          vip:       data[8],
          viptanda:  data[9]
        };

        if (!this.rooms.has(room)) this.rooms.set(room, new Map());
        this.rooms.get(room).set(seat, info);

        if (!this.updateQueue.has(room)) this.updateQueue.set(room, new Map());
        this.updateQueue.get(room).set(seat, info);
        break;
      }

      case "addPoint": {
        const room = data[1];
        const seat = data[2];
        const point = data[3];

        if (!this.rooms.has(room)) this.rooms.set(room, new Map());
        if (!this.rooms.get(room).has(seat)) {
          this.rooms.get(room).set(seat, createEmptySeat());
        }
        this.rooms.get(room).get(seat).points.push(point);

        this.broadcast(room, ["pointAdded", room, seat, point]);
        break;
      }

      case "clearPoints": {
        const room = data[1];
        const seat = data[2];

        if (this.rooms.has(room) && this.rooms.get(room).has(seat)) {
          this.rooms.get(room).get(seat).points = [];
        }
        this.broadcast(room, ["pointsCleared", room, seat]);
        break;
      }
    }
  }

  // ---- Connection handler ----
  async fetch(req) {
    if (req.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected websocket", { status: 400 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    const connId = Math.random().toString(36).slice(2);
    server.room = null;

    server.accept();
    this.clients.set(connId, server);

    server.addEventListener("message", e => this.handleMessage(server, e.data));
    server.addEventListener("close", () => {
      if (server.room) this.broadcastRoomUserCount(server.room);
      this.clients.delete(connId);
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}

// ============================
// Worker Entry (Router)
// ============================
export default {
  async fetch(req, env) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const id = env.CHAT_SERVER.idFromName("global-chat");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(req);
    }
    if (new URL(req.url).pathname === "/health")
      return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
    return new Response("WebSocket endpoint at wss://<your-subdomain>.workers.dev", { status: 200 });
  }
};
