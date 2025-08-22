// ===== Constants & Types =====
const roomList = [
  "Chill Zone", "Catch Up", "Casual Vibes", "Lounge Talk", 
  "Easy Talk", "Friendly Corner", "The Hangout", 
  "Relax & Chat", "Just Chillin", "The Chatter Room"
] as const;

type RoomName = typeof roomList[number];

interface SeatInfo {
  noimageUrl: string;
  namauser: string;
  color: string;
  itembawah: number;
  itematas: number;
  vip: boolean;
  viptanda: number;
  points: Array<{ x: number; y: number; fast: number }>;
  lockTime?: number;
}

interface WebSocketWithRoom extends WebSocket {
  roomname?: RoomName;
  idtarget?: string;
  numkursi?: Set<number>;
}

// ===== Durable Object =====
export class ChatServer {
  clients = new Set<WebSocketWithRoom>();
  userToSeat: Map<string, { room: RoomName; seat: number }> = new Map();
  roomSeats: Map<RoomName, Map<number, SeatInfo>> = new Map();

  pointUpdateBuffer: Map<RoomName, Map<number, Array<{ x: number; y: number; fast: number }>>> = new Map();
  updateKursiBuffer: Map<RoomName, Map<number, SeatInfo>> = new Map();
  chatMessageBuffer: Map<RoomName, Array<any>> = new Map();
  privateMessageBuffer: Map<string, Array<any>> = new Map();

  currentNumber = 1;
  maxNumber = 6;
  intervalMillis = 15 * 60 * 1000;

  constructor(private state: DurableObjectState) {
    for (const room of roomList) {
      const seatMap = new Map<number, SeatInfo>();
      for (let i = 1; i <= 35; i++) seatMap.set(i, this.createEmptySeat());
      this.roomSeats.set(room, seatMap);
    }
    setInterval(() => this.tick(), this.intervalMillis);
    setInterval(() => this.periodicFlush(), 100);
  }

  createEmptySeat(): SeatInfo {
    return { noimageUrl: "", namauser: "", color: "", itembawah: 0, itematas: 0, vip: false, viptanda: 0, points: [] };
  }

  safeSend(ws: WebSocketWithRoom, msg: any) {
    try {
      if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
      else this.clients.delete(ws);
    } catch { this.clients.delete(ws); }
  }

  assertValidRoom(room: any): room is RoomName {
    if (!roomList.includes(room)) throw new Error("Unknown room: " + room);
    return true;
  }

  broadcastToRoom(room: RoomName, msg: any) {
    for (const c of [...this.clients]) if (c.roomname === room) this.safeSend(c, msg);
  }

  getJumlahRoom(): Record<RoomName, number> {
    const cnt = Object.fromEntries(roomList.map(r => [r, 0])) as Record<RoomName, number>;
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room)!;
      for (const info of seatMap.values()) if (info.namauser && !info.namauser.startsWith("__LOCK__")) cnt[room]++;
    }
    return cnt;
  }

  broadcastRoomUserCount(room: RoomName) {
    const count = this.getJumlahRoom()[room] || 0;
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }

  handleGetAllRoomsUserCount(ws: WebSocketWithRoom) {
    const allCounts = this.getJumlahRoom();
    const result: Array<[RoomName, number]> = roomList.map(room => [room, allCounts[room]]);
    this.safeSend(ws, ["allRoomsUserCount", result]);
  }

  flushPrivateMessageBuffer() {
    for (const [idtarget, messages] of this.privateMessageBuffer) {
      for (const c of this.clients) if (c.idtarget === idtarget) messages.forEach(msg => this.safeSend(c, msg));
      messages.length = 0;
    }
  }

  flushChatBuffer() {
    for (const [room, messages] of this.chatMessageBuffer) {
      messages.forEach(msg => this.broadcastToRoom(room, msg));
      messages.length = 0;
    }
  }

  flushPointUpdates() {
    for (const [room, seatMap] of this.pointUpdateBuffer) {
      for (const [seat, points] of seatMap) {
        points.forEach(p => this.broadcastToRoom(room, ["pointUpdated", room, seat, p.x, p.y, p.fast]));
        points.length = 0;
      }
    }
  }

  flushKursiUpdates() {
    for (const [room, seatMap] of this.updateKursiBuffer) {
      const updates: Array<[number, Omit<SeatInfo, "points">]> = [];
      for (const [seat, info] of seatMap) {
        const { points, ...rest } = info;
        updates.push([seat, rest]);
      }
      if (updates.length > 0) this.broadcastToRoom(room, ["kursiBatchUpdate", room, updates]);
      seatMap.clear();
    }
  }

  tick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const c of [...this.clients]) this.safeSend(c, ["currentNumber", this.currentNumber]);
  }

  cleanExpiredLocks() {
    const now = Date.now();
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room)!;
      for (const [seat, info] of seatMap) {
        if (info.namauser.startsWith("__LOCK__") && info.lockTime && now - info.lockTime > 10000) {
          Object.assign(info, this.createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastRoomUserCount(room);
        }
      }
    }
  }

  lockSeat(room: RoomName, ws: WebSocketWithRoom): number | null {
    const seatMap = this.roomSeats.get(room)!;
    if (!ws.idtarget) return null;

    if (this.userToSeat.has(ws.idtarget)) {
      const prev = this.userToSeat.get(ws.idtarget)!;
      if (prev.room === room && seatMap.get(prev.seat)!.namauser === "") return prev.seat;
    }

    for (let i = 1; i <= 35; i++) {
      const kursi = seatMap.get(i)!;
      if (kursi.namauser === "") {
        kursi.namauser = "__LOCK__" + ws.idtarget;
        kursi.lockTime = Date.now();
        return i;
      }
    }
    return null;
  }

  cleanupBuffers(ws: WebSocketWithRoom) {
    if (ws.idtarget) {
      this.privateMessageBuffer.delete(ws.idtarget);
      this.userToSeat.delete(ws.idtarget);
    }
  }

  cleanupClient(ws: WebSocketWithRoom) {
    this.cleanupBuffers(ws);
    this.clients.delete(ws);
  }

  periodicFlush() {
    try {
      this.flushPointUpdates();
      this.flushKursiUpdates();
      this.flushChatBuffer();
      this.flushPrivateMessageBuffer();
      this.cleanExpiredLocks();
    } catch (err) { console.error("Periodic flush error:", err); }
  }

  handleMessage(ws: WebSocketWithRoom, dataStr: string) {
    try {
      const data = JSON.parse(dataStr);
      if (!Array.isArray(data) || data.length === 0) return this.safeSend(ws, ["error", "Invalid message format"]);
      const [evt, ...args] = data;
      switch(evt) {
        case "setIdTarget": this.handleSetIdTarget(ws, ...args); break;
        case "ping": this.handlePing(ws, ...args); break;
        case "getAllRoomsUserCount": this.handleGetAllRoomsUserCount(ws); break;
        case "getCurrentNumber": this.safeSend(ws, ["currentNumber", this.currentNumber]); break;
        case "joinRoom": this.handleJoinRoom(ws, ...args); break;
        case "chat": this.handleChat(ws, ...args); break;
        case "updatePoint": this.handleUpdatePoint(ws, ...args); break;
        case "removeKursiAndPoint": this.handleRemoveKursi(ws, ...args); break;
        case "updateKursi": this.handleUpdateKursi(ws, ...args); break;
        case "sendnotif": this.handleSendNotif(ws, ...args); break;
        case "private": this.handlePrivate(ws, ...args); break;
        case "isUserOnline": this.handleIsUserOnline(ws, ...args); break;
        default: this.safeSend(ws, ["error", "Unknown event"]); break;
      }
    } catch (err) { console.error("Error handling message:", err, "raw:", dataStr); }
  }

  async fetch(request: Request) {
    const upgrade = request.headers.get("upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") return new Response("Expected websocket", { status: 400 });

    const { socket, response } = new WebSocketPair();
    socket.accept();
    const ws = socket as unknown as WebSocketWithRoom;

    this.clients.add(ws);
    ws.numkursi = new Set<number>();
    ws.addEventListener("message", ev => this.handleMessage(ws, ev.data));
    ws.addEventListener("close", () => this.cleanupClient(ws));

    return response;
  }
}

// ===== Worker Entry =====
export default {
  async fetch(req: Request, env: { CHAT_SERVER: DurableObjectNamespace }) {
    const id = env.CHAT_SERVER.idFromName("global-chat");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(req);
  }
};
