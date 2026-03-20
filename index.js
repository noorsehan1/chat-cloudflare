// Durable Object ChatServer
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Set();
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    try {
      const webSocketPair = new WebSocketPair();
      const [client, server] = Object.values(webSocketPair);
      
      this.handleSession(server);
      
      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    } catch (err) {
      console.error("Error:", err);
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  handleSession(webSocket) {
    webSocket.accept();
    this.sessions.add(webSocket);
    
    console.log("Client connected");
    
    webSocket.send(JSON.stringify({
      type: "connected",
      message: "Connected to server"
    }));
    
    webSocket.addEventListener("message", (event) => {
      console.log("Received:", event.data);
      
      // Echo back
      webSocket.send(JSON.stringify({
        type: "echo",
        data: event.data,
        timestamp: Date.now()
      }));
    });
    
    webSocket.addEventListener("close", () => {
      this.sessions.delete(webSocket);
      console.log("Client disconnected");
    });
    
    webSocket.addEventListener("error", (error) => {
      console.error("WebSocket error:", error);
      this.sessions.delete(webSocket);
    });
  }
}

// Main Worker
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // WebSocket endpoint
    if (url.pathname === "/") {
      try {
        const id = env.CHAT_SERVER.idFromName("default");
        const server = env.CHAT_SERVER.get(id);
        return await server.fetch(request);
      } catch (err) {
        console.error("Error:", err);
        return new Response("Connection failed", { status: 500 });
      }
    }
    
    return new Response("WebSocket Server", { status: 200 });
  }
};
