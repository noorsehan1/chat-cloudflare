// Durable Object ChatServer
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.sessions = new Set();
  }

  async fetch(request) {
    // Cek apakah request adalah WebSocket upgrade
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    try {
      // Buat WebSocket pair
      const webSocketPair = new WebSocketPair();
      const [client, server] = Object.values(webSocketPair);

      // Handle server-side WebSocket
      this.handleSession(server);

      // Return client WebSocket
      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    } catch (err) {
      console.error('Failed to create WebSocket:', err);
      return new Response('Internal Server Error', { status: 500 });
    }
  }

  handleSession(webSocket) {
    // KRITICAL: Harus accept WebSocket dulu
    webSocket.accept();
    
    console.log('New WebSocket connection accepted');
    
    // Kirim pesan selamat datang
    webSocket.send(JSON.stringify({
      type: 'welcome',
      message: 'Connected to ChatServer',
      timestamp: Date.now()
    }));

    // Handle incoming messages
    webSocket.addEventListener('message', async (event) => {
      console.log('Message received:', event.data);
      
      // Echo back the message
      try {
        const data = JSON.parse(event.data);
        webSocket.send(JSON.stringify({
          type: 'echo',
          original: data,
          timestamp: Date.now()
        }));
      } catch {
        // If not JSON, send as plain text
        webSocket.send(`Echo: ${event.data}`);
      }
    });

    // Handle close
    webSocket.addEventListener('close', (event) => {
      console.log(`WebSocket closed: ${event.code} - ${event.reason}`);
      this.sessions.delete(webSocket);
    });

    // Handle error
    webSocket.addEventListener('error', (error) => {
      console.error('WebSocket error:', error);
      this.sessions.delete(webSocket);
    });
  }
}

// Main Worker
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // Routing untuk WebSocket
    if (url.pathname === '/ws' || url.pathname === '/') {
      try {
        // Gunakan Durable Object
        const id = env.CHAT_SERVER.idFromName('default');
        const chatServer = env.CHAT_SERVER.get(id);
        return await chatServer.fetch(request);
      } catch (err) {
        console.error('Durable Object error:', err);
        return new Response('WebSocket connection failed', { status: 500 });
      }
    }
    
    // Untuk testing langsung (tanpa Durable Object)
    if (url.pathname === '/direct') {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        return new Response('Expected WebSocket', { status: 426 });
      }

      const webSocketPair = new WebSocketPair();
      const [client, server] = Object.values(webSocketPair);

      server.accept();
      server.send('Connected to direct WebSocket');
      
      server.addEventListener('message', (event) => {
        server.send(`Echo: ${event.data}`);
      });

      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    }
    
    return new Response('Not Found', { status: 404 });
  }
};
