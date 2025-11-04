package valfsocket.websocketvalf;

import android.app.Activity;
import android.graphics.Color;
import android.graphics.Typeface;
import android.graphics.drawable.GradientDrawable;
import android.os.Handler;
import android.os.Looper;
import android.view.Gravity;
import android.view.View;
import android.view.ViewGroup;
import android.widget.FrameLayout;
import android.widget.TextView;
import android.widget.Toast;
import com.google.appinventor.components.annotations.SimpleEvent;
import com.google.appinventor.components.annotations.SimpleFunction;
import com.google.appinventor.components.runtime.AndroidNonvisibleComponent;
import com.google.appinventor.components.runtime.ComponentContainer;
import com.google.appinventor.components.runtime.EventDispatcher;
import com.google.appinventor.components.runtime.util.YailList;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Websocketvalf extends AndroidNonvisibleComponent {

    private final Activity activity;
    private final TextView reconnectButton;
    private WebSocket webSocket;
    private final OkHttpClient client;
    private final ComponentContainer container;
    private String serverUrl = "";
    private int mySeatNumber = -1;
    private final Handler handler = new Handler(Looper.getMainLooper());
    private String myIdTarget;
    private String roomnama;
    private android.animation.ValueAnimator pulseAnimator;
    private TextView text;
    private Toast toast;

    // Untuk cek ping

    private Runnable cekPingRunnable;


    private int connectFailCount = 0; // Tambahkan ini di deklarasi class

    private final int MAX_RECONNECT_ATTEMPTS = 10;
    private final int RECONNECT_DELAY_MS = 1000; // reconnect tiap 1 detik
    private final Map<String, Map<Integer, JSONObject>> roomSeatsMap = new HashMap<>();
    private final Handler handlerReconnect = new Handler(Looper.getMainLooper());

    public Websocketvalf(ComponentContainer container) {
        super(container.$form());
        this.container = container;
        this.activity = container.$context();

        this.client = new OkHttpClient.Builder()
                .pingInterval(15, TimeUnit.SECONDS)
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

        // Buat TextView sebagai tombol
        reconnectButton = new TextView(activity);
        reconnectButton.setText("Reconnect");
        reconnectButton.setVisibility(View.GONE);
        reconnectButton.setTextColor(Color.WHITE);
        reconnectButton.setTypeface(null, Typeface.BOLD);
        reconnectButton.setGravity(Gravity.CENTER);
        reconnectButton.setTextSize(14);
        reconnectButton.setPadding(dpToPx(16), dpToPx(6), dpToPx(16), dpToPx(6));
        GradientDrawable bgDrawable = new GradientDrawable();
        bgDrawable.setColor(Color.parseColor("#2E8B57"));
        bgDrawable.setCornerRadius(Math.round(dpToPx(4)));
        reconnectButton.setBackground(bgDrawable);

        FrameLayout.LayoutParams params =
                new FrameLayout.LayoutParams(
                        FrameLayout.LayoutParams.WRAP_CONTENT,
                        FrameLayout.LayoutParams.WRAP_CONTENT
                );
        params.gravity = Gravity.CENTER;

        View rootView = activity.findViewById(android.R.id.content);
        if (rootView instanceof ViewGroup) {
            ((ViewGroup) rootView).addView(reconnectButton, params);
        }

        // Saat tombol reconnect ditekan
        reconnectButton.setOnClickListener(v -> {
            stopPulseAnimation();
            reconnectButton.setVisibility(View.GONE);
            connectFailCount = 0; // reset counter supaya retry dimulai dari awal
            retryConnect();        // panggil metode retry
        });


        initSlowNetworkToast();
    }

    private int dpToPx(int dp) {
        return Math.round(dp * activity.getResources().getDisplayMetrics().density);
    }

    private void initSlowNetworkToast() {
        if (toast != null) return;
        text = new TextView(activity);
        text.setText("⚡️Slow Network...");
        text.setTextColor(Color.WHITE);
        text.setTypeface(null, Typeface.BOLD);
        text.setTextSize(14);
        text.setGravity(Gravity.CENTER);
        text.setPadding(dpToPx(16), dpToPx(6), dpToPx(16), dpToPx(6));
        text.setShadowLayer(0, 2, 2, Color.BLACK);
        GradientDrawable bg = new GradientDrawable();
        bg.setColor(Color.parseColor("#FFA726"));
        bg.setCornerRadius(Math.round(dpToPx(4)));
        text.setBackground(bg);
        toast = new Toast(activity);
        toast.setView(text);
        toast.setGravity(Gravity.BOTTOM | Gravity.CENTER_HORIZONTAL, 0, dpToPx(100));
        toast.setDuration(Toast.LENGTH_SHORT);
    }

    public void showSlowNetworkToast(String message) {
        if (toast == null) initSlowNetworkToast();
        text.setText(message);
        toast.show();
    }




    private void retryConnect() {
        activity.runOnUiThread(() -> {
            // 🔒 Hentikan semua percobaan reconnect lama
            if (handlerReconnect != null) {
                handlerReconnect.removeCallbacksAndMessages(null);
            }

            // 🧹 Pastikan websocket lama benar-benar dibersihkan
            if (webSocket != null) {
                try { webSocket.cancel(); } catch (Exception ignored) {}
                webSocket = null;
            }

            connectFailCount++; // Tambah counter gagal
            showSlowNetworkToast("⚡️Reconnecting... (" + connectFailCount + "/" + MAX_RECONNECT_ATTEMPTS + ")");

            if (connectFailCount <= MAX_RECONNECT_ATTEMPTS) {
                // 🔁 Coba reconnect otomatis setelah delay
                handlerReconnect.postDelayed(() -> {
                        Connect(serverUrl);

                }, RECONNECT_DELAY_MS);
            } else {
                // ❌ Sudah gagal 3x → tampilkan tombol reconnect manual
                reconnectButton.setVisibility(View.VISIBLE);
                startPulseAnimation();
                showSlowNetworkToast("⚠️Connection failed. Tap Reconnect!");
                connectFailCount = 0; // reset counter
            }
        });
    }


    public void stopCekping() {

        // 🔹 Hentikan semua callback reconnect yang mungkin masih berjalan
        if (handlerReconnect != null) {
            handlerReconnect.removeCallbacksAndMessages(null);
        }

        // 🔹 Reset counter reconnect supaya bersih
        connectFailCount = 0;


    }


    public void onConnected() {
        // Stop semua cek ping/reconnect karena sudah connect
        // 🔹 Hentikan semua callback reconnect yang mungkin masih berjalan
        if (handlerReconnect != null) {
            handlerReconnect.removeCallbacksAndMessages(null);
        }

        // 🔹 Reset counter reconnect supaya bersih
        connectFailCount = 0;

        // Sembunyikan tombol reconnect dan hentikan animasi
        activity.runOnUiThread(() -> {
            reconnectButton.setVisibility(View.GONE);
            stopPulseAnimation();
        });
    }

    @SimpleFunction(description = "Parse user input like '.start 0' or '.start >=100', returns '' if invalid")
    public String ParseStartCommand(String input) {
        if (input == null || input.trim().isEmpty()) return "";

        input = input.trim();
        String[] parts = input.split("\\s+");

        // Harus mulai dengan '.start' dan ada angka di belakang
        if (parts.length < 2) return "";
        if (!parts[0].equalsIgnoreCase(".start")) return "";

        try {
            int bet = Integer.parseInt(parts[1]);
            if (bet == 0 || bet >= 100) {
                return String.valueOf(bet); // valid
            } else {
                return "min"; // selain 0 atau ≥100 → set minimum
            }
        } catch (NumberFormatException e) {
            return ""; // bukan angka → empty
        }
    }




    private void startPulseAnimation() {
        if (pulseAnimator != null && pulseAnimator.isRunning()) return;
        pulseAnimator = android.animation.ValueAnimator.ofFloat(1f, 1.1f, 1f);
        pulseAnimator.setDuration(1000);
        pulseAnimator.setRepeatCount(android.animation.ValueAnimator.INFINITE);
        pulseAnimator.addUpdateListener(animation -> {
            float scale = (float) animation.getAnimatedValue();
            reconnectButton.setScaleX(scale);
            reconnectButton.setScaleY(scale);
        });
        pulseAnimator.start();
    }

    private void stopPulseAnimation() {
        if (pulseAnimator != null) {
            pulseAnimator.cancel();
            reconnectButton.setScaleX(1f);
            reconnectButton.setScaleY(1f);
            pulseAnimator = null;
        }
    }

    public void SendPing() {
        if ( myIdTarget == null || myIdTarget.isEmpty() || webSocket == null) return;
        Connect(serverUrl); // reconnect WebSocket
        JSONArray arr = new JSONArray();
        arr.put("ping");
        arr.put(myIdTarget);
        sendJson(arr.toString());
    }

    public void SendPong() {
        if (webSocket == null) return;
        JSONArray arr = new JSONArray();
        arr.put("pong");
        sendJson(arr.toString());
    }




    @SimpleFunction
    public void SetIdTarget(String id) {
        myIdTarget = id;
        sendJson(new JSONArray().put("setIdTarget").put(id).toString());
    }

    @SimpleFunction
    public void Connect(String url) {
        serverUrl = url;

        if (webSocket != null) {
            try { webSocket.close(1000, "Reconnecting"); } catch (Exception ignored) {}
            webSocket = null;
        }

        isConnecting = true; // set sebelum memulai koneksi baru

        try {
            Request request = new Request.Builder().url(url).build();
            client.newWebSocket(request, new WebSocketListenerImpl());
        } catch (IllegalArgumentException e) {
            OnError("Invalid URL: " + e.getMessage());
            isConnecting = false;
        }
    }



    @SimpleFunction
    public void Disconnect() {
        stopCekping();
        if (webSocket != null) {
            try { webSocket.close(1000, "Client disconnected"); } catch (Exception ignored) {}
            webSocket = null;
        }
    }

    @SimpleFunction
    public void SendChat(String roomname, String noImageURL, String username, String message, String usernameColor, String chatTextColor) {
        JSONArray arr = new JSONArray();
        arr.put("chat"); arr.put(roomname); arr.put(noImageURL); arr.put(username); arr.put(message);
        arr.put(usernameColor); arr.put(chatTextColor);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void SendPrivate(String idtarget, String noimageUrl, String message, String sender) {
        JSONArray arr = new JSONArray();
        arr.put("private"); arr.put(idtarget); arr.put(noimageUrl); arr.put(message); arr.put(sender);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void Sendnotif(String idtarget, String noimageUrl, String username, String deskripsi) {
        JSONArray arr = new JSONArray();
        arr.put("sendnotif"); arr.put(idtarget); arr.put(noimageUrl); arr.put(username); arr.put(deskripsi);
        sendJson(arr.toString());
    }
    @SimpleFunction
    public void SendNotifWithDelay(YailList onlineUsers, String noimageUrl, String username, String deskripsi) {
        if (onlineUsers == null || onlineUsers.size() == 0) return;

        final Object[] usersArray = onlineUsers.toArray();
        final Handler handler = new Handler(Looper.getMainLooper());

        for (int i = 0; i < usersArray.length; i++) {
            final int index = i;
            handler.postDelayed(() -> {
                String userId = usersArray[index] == null ? "" : usersArray[index].toString();
                if (!userId.isEmpty()) {
                    Sendnotif(userId, noimageUrl, username, deskripsi);
                }
            }, index * 50L); // delay 50ms per user
        }
    }

    @SimpleFunction
    public void RemoveKursiAndPoint(String roomName, int seatNumber) {
        JSONArray arr = new JSONArray();
        arr.put("removeKursiAndPoint"); arr.put(roomName); arr.put(seatNumber);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void resetlallrom() {
        JSONArray arr = new JSONArray();
        arr.put("resetRoom");
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void updatePoint(String roomname, int seat, double x, double y, int fast) {
        try {
            JSONArray arr = new JSONArray();
            arr.put("updatePoint"); arr.put(roomname); arr.put(seat); arr.put(x); arr.put(y); arr.put(fast);
            sendJson(arr.toString());
        } catch (Exception ignored) {}
    }

    @SimpleFunction
    public void updateKursi(
            String roomname, int seat, String noimageUrl, String namauser,
            String color, int itembawah, int itematas, int vip, int viptanda
    ) {
        try {
            JSONArray arr = new JSONArray();
            arr.put("updateKursi");
            arr.put(roomname);
            arr.put(seat);
            arr.put(noimageUrl);
            arr.put(namauser);
            arr.put(color);
            arr.put(itembawah);
            arr.put(itematas);
            arr.put(vip);
            arr.put(viptanda);

            sendJson(arr.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // ---------- Join Room yang lebih aman ----------
    @SimpleFunction
    public void JoinRoom(String roomname) {
        // Simpan nama room untuk dipakai setelah setIdTargetAck
        this.roomnama = roomname;

        // Jika WebSocket sudah connected dan ID sudah diset
        if (webSocket != null && myIdTarget != null && !myIdTarget.isEmpty()) {
            sendJoinRoom();
        }
        // Kalau belum siap, akan otomatis join saat menerima setIdTargetAck
    }

    private void sendJoinRoom() {
        if (webSocket != null && roomnama != null && !roomnama.isEmpty()) {
            try {
                JSONArray arr = new JSONArray();
                arr.put("joinRoom");
                arr.put(roomnama);
                sendJson(arr.toString());
            } catch (Exception e) {
                OnError("JoinRoom error: " + e.getMessage());
            }
        }
    }

    @SimpleFunction
    public void RequestOnlineALLUsersList() {
        try {
            // Kirim perintah ke server untuk minta daftar user online
            JSONArray arr = new JSONArray();
            arr.put("getOnlineUsers"); // nama event sesuai server
            sendJson(arr.toString());
        } catch (Exception e) {
            OnError("RequestOnlineUsersList error: " + e.getMessage());
        }
    }

    @SimpleEvent
    public void OnAllUserOnlineList(YailList users) {
        EventDispatcher.dispatchEvent(this, "OnAllUserOnlineList", users);

    }


    @SimpleFunction
    public void IsUserOnline(String userId, String tanda) {
        JSONArray arr = new JSONArray();
        arr.put("isUserOnline"); arr.put(userId); arr.put(tanda);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void GetAllRoomsUserCount() {
        sendJson(new JSONArray().put("getAllRoomsUserCount").toString());
    }

    private void sendJson(String jsonStr) {
        if (webSocket != null) {
            try {
                boolean sent = webSocket.send(jsonStr);
                if (!sent) activity.runOnUiThread(() -> OnError("Failed to send message"));
            } catch (Exception e) { activity.runOnUiThread(() -> OnError("SendJson error: " + e.getMessage())); }
        } else { activity.runOnUiThread(() -> OnError("Cannot send message: WebSocket not connected")); }
    }

    @SimpleFunction
    public void GetNumber() { sendJson(new JSONArray().put("getCurrentNumber").toString()); }




    @SimpleFunction
    public void SendGift(String sender, String receiver, String giftName) {
        if (webSocket != null && roomnama != null && !roomnama.isEmpty()) {
            try {
                JSONArray arr = new JSONArray();
                arr.put("gift");       // event type
                arr.put(roomnama);     // room tempat gift dikirim
                arr.put(sender);       // pengirim gift
                arr.put(receiver);     // penerima gift
                arr.put(giftName);     // nama gift
                sendJson(arr.toString());
            } catch (Exception e) {
                OnError("SendGift error: " + e.getMessage());
            }
        } else {
            OnError("Cannot send gift: WebSocket not connected or room not joined");
        }
    }

    @SimpleFunction(description = "Mulai game LowCard dengan jumlah taruhan (betAmount)")
    public void GameLowCardStart(int betAmount) {
        if (webSocket != null ) {
            JSONArray arr = new JSONArray();
            try {
                arr.put("gameLowCardStart");
                arr.put(betAmount);
                webSocket.send(arr.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @SimpleFunction(description = "Join game LowCard yang sedang open")
    public void GameLowCardJoin() {
        if (webSocket != null ) {
            JSONArray arr = new JSONArray();
            try {
                arr.put("gameLowCardJoin");
                webSocket.send(arr.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @SimpleFunction(description = "Submit angka (1-11) untuk ronde LowCard")
    public void GameLowCardNumber(int number, String tanda) {
        if (webSocket != null) {
            try {
                JSONArray arr = new JSONArray();
                arr.put("gameLowCardNumber"); // event
                arr.put(number);              // angka yang di-submit
                if (tanda != null && !tanda.isEmpty()) {
                    arr.put(tanda);           // optional, bisa dipakai untuk tracking user di client
                }
                webSocket.send(arr.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }





    // ---------- EVENTS ----------

    @SimpleEvent
    public void OnGiftReceived(String roomname, String sender, String receiver, String giftName, long timestamp) {
        EventDispatcher.dispatchEvent(this, "OnGiftReceived", roomname, sender, receiver, giftName, timestamp);
    }



    @SimpleEvent public void cekpingsucces() { EventDispatcher.dispatchEvent(this, "cekpingsucces"); }
    @SimpleEvent public void OnPointUpdated(String roomname, int seat, double x, double y, int fast) {
        EventDispatcher.dispatchEvent(this, "OnPointUpdated", roomname, seat, x, y, fast);
    }
    @SimpleEvent public void OnPrivateMessageReceived(String fromId, String imageUrl, String message, long timestamp, String sender) {
        EventDispatcher.dispatchEvent(this, "OnPrivateMessageReceived", fromId, imageUrl, message, timestamp, sender);
    }
    @SimpleEvent public void OnKursiUpdated(String roomname, int seat, String noimageUrl, String namauser,
                                            String color, int itembawah, int itematas, int vip, int viptanda) {
        EventDispatcher.dispatchEvent(this, "OnKursiUpdated", roomname, seat, noimageUrl, namauser,
                color, itembawah, itematas, vip, viptanda);
    }
    @SimpleEvent public void OnUpdateKursiHistory(String roomname, int seat, String noimageUrl, String namauser,
                                                  String color, int itembawah, int itematas, int vip, int viptanda) {
        EventDispatcher.dispatchEvent(this, "OnUpdateKursiHistory", roomname, seat, noimageUrl, namauser,
                color, itembawah, itematas, vip, viptanda);
    }
    @SimpleEvent public void OnNumberKursiSaya(int kursi) { EventDispatcher.dispatchEvent(this, "OnNumberKursiSaya", kursi); }
    @SimpleEvent public void OnRoomFull(String roomname) { EventDispatcher.dispatchEvent(this, "OnRoomFull", roomname); }
    @SimpleEvent public void OnPointHistory(String roomname, int seat, double x, double y, int fast) {
        EventDispatcher.dispatchEvent(this, "OnPointHistory", roomname, seat, x, y, fast);
    }
    @SimpleEvent public void OnRemoveKursi(String roomname, int seatNumber) {
        EventDispatcher.dispatchEvent(this, "OnRemoveKursi", roomname, seatNumber);
    }
    @SimpleEvent public void OnChaRoomReceived(String roomname, String noImageURL, String username, String message, String usernameColor, String chatTextColor) {
        EventDispatcher.dispatchEvent(this, "OnChaRoomReceived", roomname, noImageURL, username, message, usernameColor, chatTextColor);
    }
    @SimpleEvent public void OnRoomUserCount(String roomname, int count) {
        EventDispatcher.dispatchEvent(this, "OnRoomUserCount", roomname, count);
    }
    @SimpleEvent public void setAllRoomsFromJson(String json) {
        EventDispatcher.dispatchEvent(this, "setAllRoomsFromJson",json); }

    @SimpleEvent public void OnAllJumlahRoom(String roomName, int jumlah) { EventDispatcher.dispatchEvent(this, "OnAllJumlahRoom", roomName, jumlah); }
    @SimpleEvent public void OnUserOnlineStatus(String userName, boolean online, String tanda) {
        EventDispatcher.dispatchEvent(this, "OnUserOnlineStatus", userName, online, tanda);
    }
    @SimpleEvent public void OnBgNumberReceived(int number) { EventDispatcher.dispatchEvent(this, "OnBgNumberReceived", number); }
    @SimpleEvent public void OnReceiveNotif(String imageUrl, String username, String deskripsi, long timestamp) {
        EventDispatcher.dispatchEvent(this, "OnReceiveNotif", imageUrl, username, deskripsi, timestamp);
    }
    @SimpleEvent public void OnPrivateFailed(String username, String reason) { EventDispatcher.dispatchEvent(this, "OnPrivateFailed", username, reason); }
    @SimpleEvent public void OnResetRoom(String roomName) { EventDispatcher.dispatchEvent(this, "OnResetRoom", roomName); }
    @SimpleEvent public void OnError(String errorMsg) { EventDispatcher.dispatchEvent(this, "OnError", errorMsg); }
    @SimpleEvent public void OnOpen() { EventDispatcher.dispatchEvent(this, "OnOpen"); }

    // =============================
// SimpleEvent untuk LowCard dengan kata-kata dan emoji
// =============================

    // ======= Handler tunggal untuk semua event LowCard =======
    private final Handler mainHandler = new Handler(Looper.getMainLooper());

    private void postLowCardEvent(Runnable runnable) {
        mainHandler.post(runnable);
    }

// ======= LowCard Events =======

    @SimpleEvent
    public void OnGameLowCardStart(int bet) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardStart", bet)
        );
    }
    @SimpleEvent(description = "Dijalankan ketika start game LowCard berhasil, kirim hostName dan bet")
    public void OnGameLowCardStartSuccess(String hostName, int bet) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardStartSuccess", hostName, bet)
        );
    }

    @SimpleEvent
    public void OnGameLowCardJoin(String player, int bet) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardJoin", player,bet)
        );
    }

    @SimpleEvent
    public void OnGameLowCardNoJoin(String hostName, int bet) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardNoJoin", hostName, bet)
        );
    }

    @SimpleEvent
    public void OnGameLowCardClosedMessage(String message) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardClosedMessage", message)
        );
    }

    @SimpleEvent
    public void OnGameLowCardPlayerDraw(String playerId, int number, String tanda) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardPlayerDraw", playerId, number, tanda)
        );
    }

    @SimpleEvent
    public void OnGameLowCardRoundResult(String message) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardRoundResult", message)
        );
    }

    @SimpleEvent
    public void OnGameLowCardWinner(String winnerId, int totalCoin) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardWinner", winnerId, totalCoin)
        );
    }

    @SimpleEvent
    public void OnGameLowCardNextRound(String message) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardNextRound", message)
        );
    }

    @SimpleEvent
    public void OnGameLowCardTimeLeft(String message) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardTimeLeft", message)
        );
    }

    @SimpleEvent
    public void OnGameLowCardError(String message) {
        postLowCardEvent(() ->
                EventDispatcher.dispatchEvent(this, "OnGameLowCardError", message)
        );
    }


    private boolean isConnecting = false;


    // ---------- WebSocket Listener ----------
    private class WebSocketListenerImpl extends WebSocketListener {
        @Override
        public void onOpen(WebSocket ws, Response response) {
            Websocketvalf.this.webSocket = ws;
            connectFailCount = 0; // reset counter saat berhasil connect
            onConnected();

            activity.runOnUiThread(() -> {
                // Jika sudah ada ID target → set ID
                if (myIdTarget != null && !myIdTarget.isEmpty()) {
                    SetIdTarget(myIdTarget);
                } else {
                    OnOpen();
                }
            });
        }
        private boolean isJoiningRoom = false;


        @Override
        public void onMessage(WebSocket ws, String text) {
            activity.runOnUiThread(() -> {
                try {
                    JSONArray data = new JSONArray(text);
                    String evt = data.getString(0);

                    switch (evt) {
                        case "needJoinRoom": {
                            String roomName = data.optString(1, "");
                            if (roomName != null && !roomName.isEmpty() && !isJoiningRoom) {
                                isJoiningRoom = true;
                                showSlowNetworkToast("🔄 Reconnecting to room...");
                                new Handler(Looper.getMainLooper()).postDelayed(() -> {
                                    sendJoinRoom();
                                    isJoiningRoom = false;
                                    showSlowNetworkToast("✅ Rejoined room: " + roomnama);
                                }, 3000);
                            }
                            break;
                        }



                        case "setIdTargetAck":
                            if (roomnama != null && !roomnama.isEmpty()) sendJoinRoom();

                            break;


                        case "pong":

                            break;

                        case "cek":

                            break;

                        case "numberKursiSaya": {
                            int seat = data.getInt(1);
                            OnNumberKursiSaya(seat);
                            break;
                        }


                        case "roomFull":
                            OnRoomFull(data.getString(1));
                            break;

                        case "resetRoom": {
                            String roomName = data.getString(1);
                            roomSeatsMap.put(roomName, new HashMap<>());
                            OnResetRoom(roomName);
                            break;
                        }

                        case "removeKursi": {
                            String roomName = data.getString(1);
                            int seatNumber = data.getInt(2);
                            Map<Integer, JSONObject> seats = roomSeatsMap.get(roomName);
                            if (seats != null) seats.remove(seatNumber);
                            OnRemoveKursi(roomName, seatNumber);
                            break;
                        }


                        case "allUpdateKursiList": {
                            String room = data.getString(1);
                            JSONObject meta = data.getJSONObject(2);

                            new Handler(Looper.getMainLooper()).postDelayed(() -> {
                                updateKursiListWithDelay(room, meta);
                            }, 1000); // Delay 1000 ms sebelum mulai update
                            break;

                        }


                        case "kursiBatchUpdate": {
                            String roomName = data.getString(1);
                            JSONArray kursiList = data.getJSONArray(2);

                            for (int i = 0; i < kursiList.length(); i++) {
                                JSONArray kursi = kursiList.getJSONArray(i);
                                int seat = kursi.getInt(0);
                                JSONObject info = kursi.getJSONObject(1);

                                OnKursiUpdated(
                                        roomName,
                                        seat,
                                        info.optString("noimageUrl", ""),
                                        info.optString("namauser", ""),
                                        info.optString("color", ""),
                                        info.optInt("itembawah", 0),
                                        info.optInt("itematas", 0),
                                        info.optInt("vip", 0),
                                        info.optInt("viptanda", 0)

                                );
                            }
                            break;
                        }



                        case "allPointsList": {
                            String roomName = data.optString(1, "");
                            JSONArray points = data.getJSONArray(2);
                            for (int i = 0; i < points.length(); i++) {
                                JSONObject p = points.getJSONObject(i);
                                OnPointHistory(roomName,
                                        p.optInt("seat", 0),
                                        p.optDouble("x", 0),
                                        p.optDouble("y", 0),
                                        p.optInt("fast", 0));
                            }
                            break;
                        }

                        case "pointUpdated": {
                            String roomName = data.getString(1);
                            int seat = data.getInt(2);
                            double x = data.getDouble(3);
                            double y = data.getDouble(4);
                            int fast = data.getInt(5);
                            OnPointUpdated(roomName, seat, x, y, fast);
                            break;
                        }

                        case "chat": {
                            String room = data.getString(1);
                            String noImg = data.getString(2);
                            String name = data.getString(3);
                            String msg = data.getString(4);
                            String nameColor = data.getString(5);
                            String chatColor = data.getString(6);
                            OnChaRoomReceived(room, noImg, name, msg, nameColor, chatColor);
                            break;
                        }

                        case "private": {
                            String fromId = data.getString(1);
                            String imageUrl = data.getString(2);
                            String messageText = data.getString(3);
                            long timestamp = data.getLong(4);
                            String senderName = data.getString(5);
                            OnPrivateMessageReceived(fromId, imageUrl, messageText, timestamp, senderName);
                            break;
                        }

                        case "notif": {
                            String noimageUrl = data.getString(1);
                            String username = data.getString(2);
                            String deskripsi = data.getString(3);
                            long timestamp = data.getLong(4);
                            OnReceiveNotif(noimageUrl, username, deskripsi, timestamp);
                            break;
                        }

                        case "userOnlineStatus": {
                            String id = data.getString(1);
                            boolean online = data.getBoolean(2);
                            String tanda = data.length() > 3 ? data.getString(3) : "";
                            OnUserOnlineStatus(id, online, tanda);
                            break;
                        }

                        case "roomUserCount": {
                            String room = data.getString(1);
                            int count = data.getInt(2);
                            OnRoomUserCount(room, count);
                            break;
                        }


                        case "privateFailed": {
                            String userId = data.getString(1);
                            String reason = data.getString(2);
                            OnPrivateFailed(userId, reason);
                            break;
                        }

                        case "currentNumber": {
                            int number = data.getInt(1);
                            OnBgNumberReceived(number);
                            break;
                        }
                        case "allOnlineUsers": {
                            JSONArray onlineList = data.getJSONArray(1);
                            List<String> users = new ArrayList<>();
                            for (int i = 0; i < onlineList.length(); i++) {
                                users.add(onlineList.getString(i));
                            }
                            YailList userList = YailList.makeList(users);
                            OnAllUserOnlineList(userList); // callback ke UI atau handlermu
                            break;
                        }


                        case "allRoomsUserCount": {
                            // Server mengirim array JSON jumlah user di semua room
                            JSONArray counts = data.getJSONArray(1);

                            // Langsung lempar string JSON ke event setAllRoomsFromJson
                            setAllRoomsFromJson(counts.toString());
                            break;
                        }

                        case "gift": {
                            String room = data.getString(1);
                            String sender = data.getString(2);
                            String receiver = data.getString(3);
                            String giftName = data.getString(4);
                            long timestamp = data.getLong(5);
                            OnGiftReceived(room, sender, receiver, giftName, timestamp);
                            break;
                        }

                        case "gameLowCardStart": {
                            int betAmount = data.getInt(1);
                            OnGameLowCardStart(betAmount);
                            break;
                        }

                        case "gameLowCardStartSuccess": {
                            String hostName = data.getString(1);
                            int betAmount = data.getInt(2);
                            OnGameLowCardStartSuccess(hostName, betAmount);
                            break;
                        }


                        case "gameLowCardJoin": {
                            String hostName = data.optString(1, "");  // langsung nama host
                            int bet = data.optInt(2, 0);
                            OnGameLowCardJoin(hostName, bet);
                            break;
                        }



                        case "gameLowCardNoJoin": {
                            String hostName = data.optString(1, "");  // langsung nama host
                            int bet = data.optInt(2, 0);              // langsung angka bet
                            OnGameLowCardNoJoin(hostName, bet);
                            break;
                        }


                        case "gameLowCardClosed": {
                            JSONArray arr = data.getJSONArray(1);
                            List<String> playerList = new ArrayList<>();
                            for (int i = 0; i < arr.length(); i++) {
                                playerList.add(arr.getString(i));
                            }
                            String playersStr = String.join(", ", playerList);
                            String msg = "Players in the game:" + playersStr; // tambah line baru di awal
                            OnGameLowCardClosedMessage(msg);
                            break;
                        }


                        case "gameLowCardPlayerDraw": {
                            String playerId = data.optString(1, "");
                            int number = data.optInt(2, 0);
                            String tanda = data.optString(3, ""); // langsung ambil, default "" kalau kosong

                            OnGameLowCardPlayerDraw(playerId, number, tanda);
                            break;
                        }




                        case "gameLowCardRoundResult": {
                            int round = data.getInt(1);
                            JSONArray losersArr = data.getJSONArray(3);
                            JSONArray remainingArr = data.getJSONArray(4);

                            List<String> losersList = new ArrayList<>();
                            for (int i = 0; i < losersArr.length(); i++)
                                losersList.add(losersArr.getString(i));

                            List<String> remainingList = new ArrayList<>();
                            for (int i = 0; i < remainingArr.length(); i++)
                                remainingList.add(remainingArr.getString(i));

                            StringBuilder sb = new StringBuilder();

                            if (!losersList.isEmpty()) {
                                sb.append(String.join(", ", losersList))
                                        .append(" OUT with the lowest card!\n");
                            } else {
                                sb.append("No one eliminated this round! 🎉\n");
                            }

                            sb.append("Remaining: ")
                                    .append(String.join(", ", remainingList));

                            OnGameLowCardRoundResult(sb.toString());
                            break;
                        }



                        case "gameLowCardWinner": {
                            String winnerId = data.optString(1, ""); // index 1, default ""
                            int totalCoin = data.optInt(2, 0);      // index 2, default 0

                            // Tambahkan delay 2 detik
                            new android.os.Handler().postDelayed(() -> {
                                OnGameLowCardWinner(winnerId, totalCoin);
                            }, 2000);

                            break;
                        }


                        case "gameLowCardNextRound": {
                            int round = data.getInt(1);
                            String message = "ROUND #" + round + "\n"
                                    + "Get ready now! Type .id";
                            OnGameLowCardNextRound(message);
                            break;
                        }





                        case "gameLowCardTimeLeft": {
                            // Ambil sebagai string supaya cocok dengan server
                            String timeLeft = data.getString(1);
                            OnGameLowCardTimeLeft("Time left: " + timeLeft);
                            break;
                        }

                        case "gameLowCardError": {
                            String msg = data.getString(1);
                            OnGameLowCardError(msg);
                            break;
                        }

                        default:
                            break;
                    }

                } catch (JSONException e) {
                    OnError("Parse error: " + e.getMessage());
                }
            });
        }

        @Override
        public void onFailure(WebSocket ws, Throwable t, Response r) {
            retryConnect();
        }





        @Override
        public void onClosed(WebSocket ws, int code, String reason) {
            Websocketvalf.this.webSocket = null;
        }
    }


    public void updateKursiListWithDelay(String room, JSONObject meta) {
        List<String> keyList = new ArrayList<>();
        Iterator<String> keys = meta.keys();
        while (keys.hasNext()) {
            keyList.add(keys.next());
        }

        Handler handler = new Handler(Looper.getMainLooper());
        final int delayPerItem = 50; // base delay per item

        for (int i = 0; i < keyList.size(); i++) {
            final int index = i;
            final String key = keyList.get(index);

            handler.postDelayed(() -> {
                try {
                    int seat = Integer.parseInt(key);
                    JSONObject info = meta.getJSONObject(key);

                    OnUpdateKursiHistory(
                            room,
                            seat,
                            info.getString("noimageUrl"),
                            info.getString("namauser"),
                            info.getString("color"),
                            info.getInt("itembawah"),
                            info.getInt("itematas"),
                            info.getInt("vip"),
                            info.getInt("viptanda")

                    );
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, index * delayPerItem); // delay bertingkat: 0ms, 50ms, 100ms, dst
        }
    }

}
