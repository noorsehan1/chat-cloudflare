// Durable Object class for handling WebSocket connections
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.sessions = new Set();
    
    // Optional: Load persisted data from SQLite
    this.state.blockConcurrencyWhile(async () => {
      // Initialize any stored data
    });
  }

  async fetch(request) {
    // Check for WebSocket upgrade
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('WebSocket connections only', { status: 426 });
    }

    try {
      // Create WebSocket pair
      const webSocketPair = new WebSocketPair();
      const [client, server] = Object.values(webSocketPair);

      // Handle the WebSocket connection
      this.handleWebSocket(server);

      // Return the client WebSocket
      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    } catch (error) {
      console.error('Failed to establish WebSocket:', error);
      return new Response('WebSocket connection failed', { status: 500 });
    }
  }

  handleWebSocket(webSocket) {
    // CRITICAL: Must accept the WebSocket first
    webSocket.accept();
    
    // Add to sessions set
    this.sessions.add(webSocket);
    console.log(`New connection. Total sessions: ${this.sessions.size}`);

    // Send welcome message
    webSocket.send(JSON.stringify({
      type: 'connected',
      message: 'Connected to chat server',
      timestamp: Date.now()
    }));

    // Handle incoming messages
    webSocket.addEventListener('message', async (event) => {
      try {
        const data = JSON.parse(event.data);
        console.log('Received:', data);
        
        // Echo back for now (or broadcast to all sessions)
        const response = {
          type: 'message',
          data: data,
          timestamp: Date.now()
        };
        
        // Broadcast to all connected clients
        this.broadcast(JSON.stringify(response), webSocket);
        
      } catch (error) {
        console.error('Error processing message:', error);
        webSocket.send(JSON.stringify({
          type: 'error',
          message: 'Invalid message format'
        }));
      }
    });

    // Handle close
    webSocket.addEventListener('close', (event) => {
      console.log(`Connection closed: ${event.code} - ${event.reason}`);
      this.sessions.delete(webSocket);
      console.log(`Remaining sessions: ${this.sessions.size}`);
    });

    // Handle errors
    webSocket.addEventListener('error', (error) => {
      console.error('WebSocket error:', error);
      this.sessions.delete(webSocket);
    });
  }

  broadcast(message, sender = null) {
    for (const session of this.sessions) {
      try {
        if (session.readyState === WebSocket.OPEN) {
          session.send(message);
        } else {
          this.sessions.delete(session);
        }
      } catch (error) {
        console.error('Failed to send message:', error);
        this.sessions.delete(session);
      }
    }
  }
}

// Main Worker entry point
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // Route WebSocket connections to Durable Object
    if (url.pathname === '/ws') {
      // Get room name from query param or use 'default'
      const roomId = url.searchParams.get('room') || 'default';
      
      try {
        // Get Durable Object instance
        const id = env.CHAT_SERVER.idFromName(roomId);
        const chatServer = env.CHAT_SERVER.get(id);
        
        // Forward request to Durable Object
        return await chatServer.fetch(request);
      } catch (error) {
        console.error('Durable Object error:', error);
        return new Response('Failed to connect to chat server', { status: 500 });
      }
    }
    
    // Serve a simple HTML client for testing
    if (url.pathname === '/' || url.pathname === '/test') {
      return new Response(getHTMLClient(), {
        headers: { 'Content-Type': 'text/html' },
      });
    }
    
    return new Response('Not found', { status: 404 });
  }
};

// Simple test client HTML
function getHTMLClient() {
  return `
<!DOCTYPE html>
<html>
<head>
  <title>WebSocket Test</title>
  <style>
    body { font-family: monospace; padding: 20px; }
    #messages { border: 1px solid #ccc; height: 300px; overflow-y: auto; padding: 10px; margin-bottom: 10px; }
    #message { width: 80%; padding: 5px; }
    button { padding: 5px 10px; }
    .error { color: red; }
    .success { color: green; }
  </style>
</head>
<body>
  <h1>WebSocket Chat Test</h1>
  <div>
    <label>Room: </label>
    <input type="text" id="room" value="default" placeholder="room name">
    <button onclick="connect()">Connect</button>
    <button onclick="disconnect()">Disconnect</button>
  </div>
  <div id="status">Disconnected</div>
  <div id="messages"></div>
  <div>
    <input type="text" id="message" placeholder="Type a message">
    <button onclick="send()" disabled>Send</button>
  </div>
  
  <script>
    let ws = null;
    
    function addMessage(msg, isError = false) {
      const messages = document.getElementById('messages');
      const div = document.createElement('div');
      div.textContent = new Date().toLocaleTimeString() + ': ' + msg;
      div.style.color = isError ? 'red' : 'black';
      messages.appendChild(div);
      messages.scrollTop = messages.scrollHeight;
    }
    
    function connect() {
      if (ws && ws.readyState === WebSocket.OPEN) {
        addMessage('Already connected', true);
        return;
      }
      
      const room = document.getElementById('room').value;
      const wsUrl = \`ws://\${window.location.host}/ws?room=\${room}\`;
      addMessage('Connecting to: ' + wsUrl);
      
      ws = new WebSocket(wsUrl);
      
      ws.onopen = () => {
        addMessage('Connected!', false);
        document.getElementById('status').textContent = 'Connected';
        document.getElementById('status').style.color = 'green';
        document.querySelector('button[onclick="send()"]').disabled = false;
      };
      
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          addMessage('Received: ' + JSON.stringify(data, null, 2));
        } catch(e) {
          addMessage('Received: ' + event.data);
        }
      };
      
      ws.onclose = (event) => {
        addMessage(\`Closed: Code \${event.code} - \${event.reason || 'No reason'}\`, true);
        document.getElementById('status').textContent = 'Disconnected';
        document.getElementById('status').style.color = 'red';
        document.querySelector('button[onclick="send()"]').disabled = true;
        ws = null;
      };
      
      ws.onerror = (error) => {
        addMessage('Error occurred', true);
        console.error('WebSocket error:', error);
      };
    }
    
    function disconnect() {
      if (ws) {
        ws.close();
        ws = null;
      }
    }
    
    function send() {
      if (ws && ws.readyState === WebSocket.OPEN) {
        const msg = document.getElementById('message').value;
        if (msg) {
          ws.send(JSON.stringify({ text: msg }));
          addMessage('Sent: ' + msg);
          document.getElementById('message').value = '';
        }
      } else {
        addMessage('Not connected', true);
      }
    }
  </script>
</body>
</html>
  `;
}
