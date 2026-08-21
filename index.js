export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const upgrade = request.headers.get("Upgrade");
      
      console.log('📡 Index:', {
        pathname,
        upgrade,
        isWebSocket: upgrade === "websocket",
        url: request.url
      });
      
      // CHAT SERVER
      if (
        pathname === "/ws" || 
        pathname === "/chat" || 
        pathname === "/reset" || 
        pathname === "/health" ||
        (pathname === "/" && upgrade === "websocket")
      ) {
        console.log('✅ Routing ke ChatServer');
        const id = env.CHAT_SERVER.idFromName("global");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(request);
      }
      
      // GAME SERVER
      if (pathname.startsWith("/game")) {
        console.log('✅ Routing ke GameServer');
        const id = env.GAME_SERVER.idFromName("game");
        const obj = env.GAME_SERVER.get(id);
        return obj.fetch(request);
      }
      
      if (pathname === "/") {
        return new Response("Chat Server Running ✅\nWebSocket: wss://" + url.host + "/\n", { 
          status: 200,
          headers: { 'Content-Type': 'text/plain' }
        });
      }
      
      return new Response("Not Found", { status: 404 });
      
    } catch(e) {
      console.error('❌ Error:', e);
      return new Response("Error: " + e.message, { status: 500 });
    }
  }
};
