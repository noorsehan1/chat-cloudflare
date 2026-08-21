// ==================== INDEX.JS ====================
// VERSION: 3.4.1 - SUPER SIMPLE

import { ChatServer } from "./chat-server.js";

export default {
  async fetch(request, env) {
    // SEMUA REQUEST LANGSUNG KE CHAT SERVER
    const id = env.CHAT_SERVER.idFromName("global");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(request);
  }
};

export { ChatServer };
