// ==================== CHAT-SERVER.JS ====================
// OPTIMIZED FOR CLOUDFLARE DO FREE TIER WITH WEBSOCKET HIBERNATION

const C = {
  MAX_SEATS: 45,
  MAX_MESSAGE_SIZE: 5000,
  ALARM_INTERVAL_MS: 5, // 15 Minutes
  NUMBER_UPDATE_TIK: 6,         // 6 x 15m = 90m
  MAX_NUMBER: 6,
};

const ROOMS = new Set([
  "LowCard", "Quiz", "Gacor", "General", "LOVE BIRDS", "Birthday Party",
  "Sweet Memories", "Lounge Talk", "Noxxeliverothcifsa", "BESTIES",
  "Happy Vibes", "The Chatter Room"
]);

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.ctx = state;
    this.env = env;

    // Use state.storage / in-memory structures populated on load
    this.currentNumber = 1;
    this.tikCounter = 0;

    // Ensure Alarm is initialized once
    this.ctx.blockConcurrencyWhile(async () => {
      const currentAlarm = await this.ctx.storage.getAlarm();
      if (!currentAlarm) {
        await this.ctx.storage.setAlarm(Date.now() + C.ALARM_INTERVAL_MS);
      }
      this.currentNumber = (await this.ctx.storage.get("currentNumber")) || 1;
      this.tikCounter = (await this.ctx.storage.get("tikCounter")) || 0;
    });
  }

  // ==================== HTTP / WS FETCH HANDLER ====================
  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    // Accept using Hibernation API to drop duration billing when idle
    this.ctx.acceptWebSocket(server);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ==================== HIBERNATED WEBSOCKET EVENT HANDLERS ====================
  async webSocketMessage(ws, message) {
    try {
      const str = typeof message === "string" ? message : new TextDecoder().decode(message);
      if (str.length > C.MAX_MESSAGE_SIZE) return;

      const data = JSON.parse(str);
      if (!Array.isArray(data) || !data.length) return;

      const [evt, ...args] = data;

      switch (evt) {
        case "setIdTarget2": {
          const [username, idtarget] = args;
          // Attach custom tags to socket for target identification without holding memory references
          this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
          ws.serializeAttachment({ username, idtarget, room: ws.deserializeAttachment()?.room });
          break;
        }

        case "joinRoom": {
          const [roomName] = args;
          if (!ROOMS.has(roomName)) break;

          const attachment = ws.deserializeAttachment() || {};
          attachment.room = roomName;
          ws.serializeAttachment(attachment);

          // Get all connections in this room using Cloudflare Tags
          const roomSockets = this.ctx.getWebSockets(roomName);
          if (roomSockets.length >= C.MAX_SEATS) break;

          // Tag the websocket with the room name
          ws.serializeAttachment(attachment);
          
          this.broadcastRoom(roomName, ["roomUserCount", roomName, roomSockets.length + 1]);
          ws.send(JSON.stringify(["rooMasuk", roomSockets.length + 1, roomName]));
          break;
        }

        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (!chatMsg || !ROOMS.has(chatRoom)) break;

          this.broadcastRoom(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor]);
          break;
        }

        case "getCurrentNumber": {
          ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
          break;
        }
      }
    } catch (e) {}
  }

  async webSocketClose(ws, code, reason, wasClean) {
    const attachment = ws.deserializeAttachment();
    if (attachment?.room) {
      const remainingSockets = this.ctx.getWebSockets(attachment.room).length - 1;
      this.broadcastRoom(attachment.room, ["roomUserCount", attachment.room, Math.max(0, remainingSockets)]);
    }
    ws.close(code, "Closed");
  }

  async webSocketError(ws, error) {
    ws.close(1011, "WebSocket Error");
  }

  // ==================== ALARM HANDLER (Runs every 15 mins) ====================
  async alarm() {
    this.tikCounter++;

    if (this.tikCounter >= C.NUMBER_UPDATE_TIK) {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      this.tikCounter = 0;

      await this.ctx.storage.put("currentNumber", this.currentNumber);
      await this.ctx.storage.put("tikCounter", this.tikCounter);

      this.broadcastAll(["currentNumber", this.currentNumber]);
    }

    // Reschedule next Alarm
    await this.ctx.storage.setAlarm(Date.now() + C.ALARM_INTERVAL_MS);
  }

  // ==================== BROADCAST HELPER METHODS ====================
  broadcastRoom(room, data) {
    const payload = JSON.stringify(data);
    for (const ws of this.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment();
      if (attachment?.room === room) {
        ws.send(payload);
      }
    }
  }

  broadcastAll(data) {
    const payload = JSON.stringify(data);
    for (const ws of this.ctx.getWebSockets()) {
      ws.send(payload);
    }
  }
}
