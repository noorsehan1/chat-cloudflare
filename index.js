export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.sessions = [];
  }

  async fetch(request) {
    // WAJIB cek websocket
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // WAJIB
    server.accept();

    // simpan koneksi
    this.sessions.push(server);

    // handle message
    server.addEventListener("message", (event) => {
      const msg = event.data;

      // broadcast ke semua client
      for (let ws of this.sessions) {
        try {
          ws.send(msg);
        } catch (e) {}
      }
    });

    // handle close
    server.addEventListener("close", () => {
      this.sessions = this.sessions.filter(ws => ws !== server);
    });

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
}
