// ==================== INDEX.JS - FIXED ====================
import { ChatServer } from "./chat-server.js";
import { GameServer } from "./game-server.js";

let chatInstance = null;
let gameInstance = null;

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;

      // ============================================================
      // ROOT - TAMPILAN HTML
      // ============================================================
      if (pathname === "/") {
        return new Response(HTML, {
          headers: { "Content-Type": "text/html" }
        });
      }

      // ============================================================
      // CHAT SERVER
      // ============================================================
      if (pathname === "/ws" || pathname === "/chat") {
        if (!chatInstance) {
          chatInstance = new ChatServer(env);
        }
        return chatInstance.fetch(request);
      }

      // ============================================================
      // GAME SERVER
      // ============================================================
      if (pathname === "/game" || pathname === "/game/ws") {
        if (!gameInstance) {
          gameInstance = new GameServer(env);
        }
        return gameInstance.fetch(request);
      }

      // ============================================================
      // HEALTH CHECK
      // ============================================================
      if (pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          chat: chatInstance ? "active" : "inactive",
          game: gameInstance ? "active" : "inactive",
          timestamp: Date.now()
        }), {
          headers: { 
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
          }
        });
      }

      return new Response("Server running", { status: 200 });

    } catch(e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};

// ============================================================
// HTML FRONTEND - TEST KONEKSI
// ============================================================
const HTML = `
<!DOCTYPE html>
<html>
<head>
  <title>Chat Test</title>
  <style>
    body { font-family: Arial; background: #0d1117; color: #e6edf3; padding: 20px; }
    .status { padding: 10px; border-radius: 4px; margin: 10px 0; }
    .connected { background: #238636; }
    .disconnected { background: #da3633; }
    input, button { padding: 10px; margin: 5px; border-radius: 4px; border: none; }
    input { background: #161b22; color: #e6edf3; width: 300px; }
    button { background: #238636; color: white; cursor: pointer; }
    #messages { margin-top: 20px; }
    .msg { padding: 5px 10px; margin: 2px 0; background: #161b22; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>💬 Chat Test</h1>
  <div>
    <input id="username" placeholder="Username" value="TestUser" />
    <input id="room" placeholder="Room" value="General" />
    <button onclick="connect()">Connect</button>
    <button onclick="disconnect()">Disconnect</button>
  </div>
  <div id="status" class="status disconnected">Disconnected</div>
  <div>
    <input id="msg" placeholder="Type message..." />
    <button onclick="sendMessage()">Send</button>
  </div>
  <div id="messages"></div>

  <script>
    let ws = null;

    function connect() {
      const username = document.getElementById('username').value || 'TestUser';
      const room = document.getElementById('room').value || 'General';
      
      // ✅ URL YANG BENAR
      const wsUrl = \`wss://\${window.location.host}/ws?username=\${username}&room=\${room}\`;
      
      document.getElementById('status').textContent = 'Connecting...';
      document.getElementById('status').className = 'status disconnected';
      
      try {
        ws = new WebSocket(wsUrl);
        
        ws.onopen = () => {
          document.getElementById('status').textContent = '✅ Connected';
          document.getElementById('status').className = 'status connected';
          addMessage('System', 'Connected to server');
          
          // ✅ KIRIM SET ID
          ws.send(JSON.stringify(["setIdTarget2", username, true]));
        };
        
        ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            console.log('Received:', data);
            
            if (data[0] === 'chat') {
              addMessage(data[2], data[3]);
            }
            if (data[0] === 'joinroomawal') {
              // ✅ JOIN ROOM
              ws.send(JSON.stringify(["joinRoom", document.getElementById('room').value || 'General']));
            }
            if (data[0] === 'rooMasuk') {
              addMessage('System', 'Joined room, seat: ' + data[1]);
            }
            if (data[0] === 'roomUserCount') {
              addMessage('System', 'Room users: ' + data[2]);
            }
          } catch(e) {
            console.error('Parse error:', e);
          }
        };
        
        ws.onclose = () => {
          document.getElementById('status').textContent = '❌ Disconnected';
          document.getElementById('status').className = 'status disconnected';
          addMessage('System', 'Disconnected');
        };
        
        ws.onerror = (error) => {
          document.getElementById('status').textContent = '❌ Error: ' + error.message;
          document.getElementById('status').className = 'status disconnected';
          addMessage('System', 'Error: ' + error.message);
        };
        
      } catch(e) {
        document.getElementById('status').textContent = '❌ Error: ' + e.message;
        addMessage('System', 'Error: ' + e.message);
      }
    }

    function disconnect() {
      if (ws) {
        ws.close();
        ws = null;
      }
      document.getElementById('status').textContent = 'Disconnected';
      document.getElementById('status').className = 'status disconnected';
    }

    function sendMessage() {
      const msg = document.getElementById('msg').value.trim();
      if (!msg || !ws || ws.readyState !== 1) {
        alert('Not connected!');
        return;
      }
      
      const username = document.getElementById('username').value || 'TestUser';
      const room = document.getElementById('room').value || 'General';
      
      ws.send(JSON.stringify(["chat", room, "", username, msg, "", ""]));
      document.getElementById('msg').value = '';
    }

    function addMessage(username, message) {
      const container = document.getElementById('messages');
      const div = document.createElement('div');
      div.className = 'msg';
      div.innerHTML = \`<strong>\${username}</strong>: \${message}\`;
      container.appendChild(div);
      container.scrollTop = container.scrollHeight;
    }

    // ✅ ENTER TO SEND
    document.getElementById('msg').onkeydown = (e) => {
      if (e.key === 'Enter') sendMessage();
    };
  </script>
</body>
</html>
`;
