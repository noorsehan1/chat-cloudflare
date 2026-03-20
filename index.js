export default {
  async fetch(request) {

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("OK HTTP", { status: 200 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    server.accept();

    server.addEventListener("message", (e) => {
      server.send("Reply: " + e.data);
    });

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
};
