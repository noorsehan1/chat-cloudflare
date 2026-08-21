// ==================== INDEX.JS ====================
// VERSION: 3.4.2 - FIXED

import { ChatServer } from "./chat-server.js";

export default {
  async fetch(request, env) {
    try {
      // PASTIKAN ENV ADA
      if (!env || !env.CHAT_SERVER) {
        return new Response("CHAT_SERVER binding not found", { status: 500 });
      }
      
      const id = env.CHAT_SERVER.idFromName("global");
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(request);
      
    } catch(error) {
      return new Response("Error: " + error.message, { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
  }
};

export { ChatServer };
