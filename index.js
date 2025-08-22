const roomList = [
  "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk",
  "Easy Talk", "Friendly Corner", "The Hangout",
  "Relax & Chat", "Just Chillin", "The Chatter Room"
];

export class ChatServer {
  constructor(state) {
    this.state = state;
    this.clients = new Set();
    this.userToSeat = new Map();
    this.roomSeats = new Map();

    // Inisialisasi kursi untuk semua room
    for (const room of roomList) {
      const seatMap = new Map();
      for (let i = 1; i <= 35; i++) {
        seatMap.set(i, this.createEmptySeat());
      }
      this.roomSeats.set(room, seatMap);
    }
  }

  createEmptySeat() {
    return {
      noimageUrl: "",
      namauser: "",
      color: "",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0,
      points: []
    };
  }

  safeSend(ws, msg) {
    try {
      if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
      else this.clients.delete(ws);
    } catch {
      this.clients.delete(ws);
    }
  }

  broadcastToRoom(room, msg) {
    for (const c of this.clients) {
      if (c.roomname === room) this.safeSend(c, msg);
    }
  }

  handleMessage(ws, dataStr) {
    let data;
    try {
      data = JSON.parse(dataStr);
    } catch {
      return this.safeSend(ws, ["error", "Invalid JSON"]);
    }

    if (!Array.isArray(data)) return;
    const [evt, ...args] = data;

    switch (evt) {
      case "ping":
        this.safeSend(ws, ["pong", ...args]);
        break;

      case "setIdTarget":
        ws.idtarget = args[0];
        this.safeSend(ws, ["ack", "setIdTarget"]);
        break;

      case "joinRoom":
        ws.roomname = args[0];
        this.safeSend(ws, ["joined", ws.roomname]);
        break;

      case "chat": {
        const [room, noImg, username, message, nameColor, chatColor] = args;
        this.broadcastToRoom(room, ["chat", room, noImg, username, message, nameColor, chatColor]);
        break;
      }

      case "private": {
        const [targetId, noImg, message, sender] = args;
        for (const c of this.clients) {
          if (c.idtarget === targetId) {
            this.safeSend(c, ["private", ws.idtarget || "", noImg, message, Date.now(), sender]);
          }
        }
        break;
      }

      case "sendnotif": {
        const [targetId, noImg, username, deskripsi] = args;
        for (const c of this.clients) {
          if (c.idtarget === targetId) {
            this.safeSend(c, ["notif", noImg, username, deskripsi, Date.now()]);
          }
        }
        break;
      }

      case "updatePoint": {
        const [room, seat, x, y, fast] = args;
        this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        break;
      }

      case "updateKursi": {
        const [room, seat, noImg, nama, color, itembawah, itematas, vip, viptanda] = args;
        const seatMap = this.roomSeats.get(room);
        if (seatMap) {
          seatMap.set(seat, { noimageUrl: noImg, namauser: nama, color, itembawah, itematas, vip, viptanda, points: [] });
        }
        this.broadcastToRoom(room, [
          "kursiBatchUpdate",
          room,
          [[seat, { noimageUrl: noImg, namauser: nama, color, itembawah, itematas, vip, viptanda }]]
        ]);
        break;
      }

      case "removeKursiAndPoint": {
        const [room, seat] = args;
        const seatMap = this.roomSeats.get(room);
        if (seatMap) seatMap.set(seat, this.createEmptySeat());
        this.broadcastToRoom(room, ["removeKursi", room, seat]);
        break;
      }

      case "isUserOnline": {
        const [userId, tanda] = args;
        let online = false;
        for (const c of this.clients) {
          if (c.idtarget === userId) {
            online = true;
            break;
          }
        }
        this.safeSend(ws, ["userOnlineStatus", userId, online, tanda]);
        break;
      }

      case "getAllRoomsUserCount": {
        const result = [];
        for (const room of roomList) {
          let count = 0;
          const seatMap = this.roomSeats.get(room);
          for (const info of seatMap.values()) {
            if (info.namauser) count++;
          }
          result.push([room, count]);
        }
        this.safeSend(ws, ["allRoomsUserCount", result]);
        break;
      }

      case "getCurrentNumber":
        this.safeSend(ws, ["currentNumber", Date.now() % 6 + 1]);
        break;

      case "resetRoom": {
        for (const room of roomList) {
          const seatMap = this.roomSeats.get(room);
          for (let i = 1; i <= 35; i++) {
            seatMap.set(i, this.createEmptySeat());
          }
          this.broadcastToRoom(room, ["resetRoom", room]);
        }
        break;
      }

      default:
        this.safeSend(ws, ["error", "Unknown event " + evt]);
        break;
    }
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected websocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.accept();
    this.clients.add(server);

    server.addEventListener("message", (ev) => this.handleMessage(server, ev.data));
    server.addEventListener("close", () => this.clients.delete(server));

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER.idFromName("global");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(req);
  }
};
