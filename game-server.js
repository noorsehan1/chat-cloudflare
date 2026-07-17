package valfsocket.websocketvalf;

import android.app.Activity;
import android.graphics.Color;
import android.graphics.Typeface;
import android.graphics.drawable.GradientDrawable;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;
import android.os.SystemClock;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Websocketvalf extends AndroidNonvisibleComponent {

    // ==================== CHAT SERVER ====================
    private final Activity activity;
    private final TextView reconnectButton;
    private WebSocket webSocket;
    private final OkHttpClient client;
    private final ComponentContainer container;
    private String serverUrl = "";
    private int mySeatNumber = -1;
    private final Handler mainHandler = new Handler(Looper.getMainLooper());
    private Handler pointHandler;
    private Handler kursiHandler;
    private String myIdTarget = "";
    private String roomnama = "";
    private android.animation.ValueAnimator pulseAnimator;
    private TextView text;
    private Toast toast;

    private final Handler pingHandler = new Handler(Looper.getMainLooper());
    private Runnable pingRunnable;
    private int pingReconnectCount = 0;
    private final int MAX_PING_RECONNECT = 2;

    private final AtomicBoolean isReconnecting = new AtomicBoolean(false);
    private final AtomicBoolean isManualDisconnect = new AtomicBoolean(false);
    private final AtomicBoolean isConnecting = new AtomicBoolean(false);
    private final AtomicBoolean shouldReconnect = new AtomicBoolean(true);
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean isHandlingConnectionLoss = new AtomicBoolean(false);

    private boolean fastkursi = false;
    private boolean minimize = false;

    private final Handler statusTimeoutHandler = new Handler(Looper.getMainLooper());
    private Runnable statusTimeoutRunnable;
    private int hasReceivedStatusResponse = 0;

    private final Object webSocketLock = new Object();

    private final ConcurrentLinkedQueue<Runnable> messageQueue = new ConcurrentLinkedQueue<>();
    private final Handler messageHandler = new Handler(Looper.getMainLooper());

    private long lastConnectionAttempt = 0;
    private final long MIN_CONNECTION_INTERVAL = 3000;
    private final AtomicBoolean isCleaningUp = new AtomicBoolean(false);
    private String activeTargetId = "";

    // ==================== GAME SERVER ====================
    private WebSocket gameWebSocket;
    private final AtomicBoolean isGameConnected = new AtomicBoolean(false);
    private String gameRoom = "";
    private String gameUsername = "";
    private String gameId = "";
    private String gameServerUrl = "";
    private int reconnectAttempts = 0;
    private static final int MAX_RECONNECT_ATTEMPTS = 5;
    private final Runnable messageProcessor = new Runnable() {
        private int processedCount = 0;
        private static final int MAX_PER_BATCH = 20;

        @Override
        public void run() {
            Runnable task;
            long startTime = SystemClock.uptimeMillis();
            processedCount = 0;

            while ((task = messageQueue.poll()) != null
                    && processedCount < MAX_PER_BATCH
                    && (SystemClock.uptimeMillis() - startTime) < 200) {
                task.run();
                processedCount++;
            }

            if (!messageQueue.isEmpty()) {
                messageHandler.postDelayed(this, 16);
            }
        }
    };

    public Websocketvalf(ComponentContainer container) {
        super(container.$form());
        this.container = container;
        this.activity = container.$context();

        this.pointHandler = new Handler(Looper.getMainLooper());
        this.kursiHandler = new Handler(Looper.getMainLooper());

        this.client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .pingInterval(20, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .build();

        OnAndroidVersionDetected(Build.VERSION.SDK_INT);

        reconnectButton = new TextView(activity);
        reconnectButton.setText("Click to Reconnect");
        reconnectButton.setVisibility(View.GONE);
        reconnectButton.setTextColor(Color.WHITE);
        reconnectButton.setTypeface(null, Typeface.BOLD);
        reconnectButton.setGravity(Gravity.CENTER);
        reconnectButton.setTextSize(14);
        reconnectButton.setPadding(dpToPx(16), dpToPx(6), dpToPx(16), dpToPx(6));

        GradientDrawable bgDrawable = new GradientDrawable();
        bgDrawable.setColor(Color.parseColor("#ff0080"));
        bgDrawable.setCornerRadius(dpToPx(4));
        reconnectButton.setBackground(bgDrawable);

        FrameLayout.LayoutParams params = new FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.WRAP_CONTENT,
                FrameLayout.LayoutParams.WRAP_CONTENT
        );
        params.gravity = Gravity.CENTER;

        View rootView = activity.findViewById(android.R.id.content);
        if (rootView instanceof ViewGroup) {
            ((ViewGroup) rootView).addView(reconnectButton, params);
        }

        reconnectButton.setOnClickListener(v -> {
            stopPulseAnimation();
            reconnectButton.setVisibility(View.GONE);
            pingReconnectCount = 0;
            isManualDisconnect.set(false);
            shouldReconnect.set(true);
            startReconnectProcess();
        });

        isConnecting.set(false);
        isReconnecting.set(false);
        isManualDisconnect.set(false);
        shouldReconnect.set(true);
        isConnected.set(false);
        initSlowNetworkToast();
    }

    private int dpToPx(int dp) {
        return Math.round(dp * activity.getResources().getDisplayMetrics().density);
    }

    private void initSlowNetworkToast() {
        if (toast != null) return;
        text = new TextView(activity);
        text.setTextColor(Color.WHITE);
        text.setTypeface(null, Typeface.BOLD);
        text.setTextSize(14);
        text.setGravity(Gravity.CENTER);
        text.setPadding(dpToPx(16), dpToPx(6), dpToPx(16), dpToPx(6));
        text.setShadowLayer(0, 2, 2, Color.BLACK);
        GradientDrawable bg = new GradientDrawable();
        bg.setColor(Color.parseColor("#ff0080"));
        bg.setCornerRadius(dpToPx(4));
        text.setBackground(bg);
        toast = new Toast(activity);
        toast.setView(text);
        toast.setGravity(Gravity.BOTTOM | Gravity.CENTER_HORIZONTAL, 0, dpToPx(100));
        toast.setDuration(Toast.LENGTH_SHORT);
    }

    public void showSlowNetworkToast(String message) {
        if (toast == null) initSlowNetworkToast();
        if (message != null) {
            text.setText(message);
            toast.show();
        }
    }

    // ==================== CHAT METHODS ====================

    @SimpleFunction
    public void startReconnectProcess() {
        if (isManualDisconnect.get() || isConnected.get() || isCleaningUp.get()) {
            return;
        }

        if (!isReconnecting.compareAndSet(false, true)) {
            return;
        }

        if (pingRunnable != null) {
            pingHandler.removeCallbacks(pingRunnable);
            pingRunnable = null;
        }

        pingReconnectCount = 0;

        pingRunnable = new Runnable() {
            @Override
            public void run() {
                if (isManualDisconnect.get() || isConnected.get() || isCleaningUp.get()) {
                    stopReconnectProcess();
                    return;
                }

                pingReconnectCount++;
                if (pingReconnectCount <= MAX_PING_RECONNECT) {
                    mainHandler.postDelayed(() -> {
                        if (!isManualDisconnect.get() && !isConnected.get() && !isCleaningUp.get()) {
                            Connect(serverUrl);
                        }
                    }, 1000);

                    showSlowNetworkToast("Network error, reconnecting...");

                    pingHandler.postDelayed(this, 4000);
                } else {
                    stopReconnectProcess();
                    mainHandler.post(() -> {
                        if (!isCleaningUp.get()) {
                            reconnectButton.setVisibility(View.VISIBLE);
                            startPulseAnimation();
                            OnMaxReconnectAttemptsReached(pingReconnectCount);
                        }
                    });
                }
            }
        };

        pingHandler.post(pingRunnable);
        OnReconnectStarted();
    }

    public void stopReconnectProcess() {
        isReconnecting.set(false);
        if (pingRunnable != null) {
            pingHandler.removeCallbacks(pingRunnable);
            pingRunnable = null;
        }
        pingHandler.removeCallbacksAndMessages(null);
        pingReconnectCount = 0;
        OnReconnectStopped();
    }

    public void onConnected() {
        isHandlingConnectionLoss.set(false);
        isConnecting.set(false);
        isConnected.set(true);
        isReconnecting.set(false);

        stopReconnectProcess();
        cleanupStatusTimeout();

        pingHandler.removeCallbacksAndMessages(null);

        mainHandler.post(() -> {
            reconnectButton.setVisibility(View.GONE);
            stopPulseAnimation();
            pingReconnectCount = 0;
            OnReconnectSuccess(roomnama != null ? roomnama : "");
        });
    }

    public void onDisconnected() {
        isConnected.set(false);
        isConnecting.set(false);
    }

    @SimpleFunction
    public void checkRoomStatusWithTimeout() {
        this.minimize = false;
        SendIsInRoom();
        hasReceivedStatusResponse = 0;

        cleanupStatusTimeout();
        statusTimeoutRunnable = new Runnable() {
            @Override
            public void run() {
                int status = hasReceivedStatusResponse;
                if (status == 0) {
                    handleConnectionLoss("Connection failed");
                } else if (status == 2) {
                    sendJoinRoom();
                }
            }
        };

        statusTimeoutHandler.postDelayed(statusTimeoutRunnable, 3000);
    }

    @SimpleFunction
    public void ClearMessageQueue() {
        messageQueue.clear();
        messageHandler.removeCallbacks(messageProcessor);
        OnMessageQueueCleared();
    }

    @SimpleEvent
    public void OnMessageQueueCleared() {
        EventDispatcher.dispatchEvent(this, "OnMessageQueueCleared");
    }

    @SimpleEvent
    public void OnSlowNetworkDetected(String reason) {
        EventDispatcher.dispatchEvent(this, "OnSlowNetworkDetected", reason != null ? reason : "");
    }

    @SimpleFunction
    public void SendIsInRoom() {
        JSONArray arr = new JSONArray();
        arr.put("isInRoom");
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void WebsocketsetMinimize(boolean value) {
        this.minimize = value;
        OnMinimizeStateChanged(value);
    }

    @SimpleFunction
    public String ParseStartCommand(String input) {
        if (input == null || input.trim().isEmpty()) return "";

        input = input.trim();

        if (input.contains("'")) {
            return "";
        }

        String[] parts = input.split("\\s+");

        if (parts.length < 2) return "";
        if (!parts[0].equalsIgnoreCase(".start")) return "";

        try {
            int bet = Integer.parseInt(parts[1]);
            if (bet == 0 || bet >= 100) {
                return String.valueOf(bet);
            } else {
                return "min";
            }
        } catch (NumberFormatException e) {
            return "";
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
        OnPulseAnimationStarted();
    }

    private void stopPulseAnimation() {
        if (pulseAnimator != null) {
            pulseAnimator.cancel();
            pulseAnimator.removeAllUpdateListeners();
            reconnectButton.setScaleX(1f);
            reconnectButton.setScaleY(1f);
            pulseAnimator = null;
            OnPulseAnimationStopped();
        }
    }

    @SimpleFunction
    public void SetIdTarget(String id, String roomname) {
        if (id == null) id = "";
        if (roomname == null) roomname = "";
        myIdTarget = id;
        this.roomnama = roomname;

        JSONArray arr = new JSONArray();
        arr.put("setIdTarget");
        arr.put(id);
        arr.put(roomname);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void SetIdTarget2(String id, boolean baru) {
        if (id == null) id = "";
        myIdTarget = id;
        JSONArray arr = new JSONArray();
        arr.put("setIdTarget2");
        arr.put(id);
        arr.put(baru);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void Connect(String url) {
        cleanupStatusTimeout();
        if (url == null || url.isEmpty()) {
            isConnecting.set(false);
            return;
        }

        serverUrl = url;

        if (!isConnecting.compareAndSet(false, true)) {
            return;
        }

        try {
            Request request = new Request.Builder().url(url).build();
            client.newWebSocket(request, new WebSocketListenerImpl());
        } catch (IllegalArgumentException e) {
            isConnecting.set(false);
        } catch (Exception e) {
            isConnecting.set(false);
        }
    }

    @SimpleFunction
    public void SendOnDestroy() {
        if (!isConnected.get()) return;

        JSONArray arr = new JSONArray();
        arr.put("onDestroy");
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void Disconnect() {
        isManualDisconnect.set(true);
        shouldReconnect.set(false);
        isCleaningUp.set(true);

        SendOnDestroy();
        DisconnectGame();
        mainHandler.postDelayed(() -> {
            synchronized (webSocketLock) {
                if (webSocket != null) {
                    try {
                        webSocket.close(1000, "Permanent logout");
                    } catch (Exception ignored) {}
                    webSocket = null;
                }
            }

            cleanup();
            isCleaningUp.set(false);
        }, 300);
    }

    @SimpleFunction
    public void ForceReconnect() {
        isManualDisconnect.set(false);
        shouldReconnect.set(true);
        isCleaningUp.set(false);

        synchronized (webSocketLock) {
            if (webSocket != null) {
                try {
                    webSocket.close(1000, "Force reconnect");
                } catch (Exception ignored) {}
                webSocket = null;
            }
        }

        onDisconnected();
        stopReconnectProcess();
        cleanupStatusTimeout();
        startReconnectProcess();
    }

    @SimpleFunction
    public void SendChat(String roomname, String noImageURL, String username, String message, String usernameColor, String chatTextColor) {
        if (roomname == null) roomname = "";
        if (noImageURL == null) noImageURL = "";
        if (username == null) username = "";
        if (message == null) message = "";
        if (usernameColor == null) usernameColor = "";
        if (chatTextColor == null) chatTextColor = "";

        JSONArray arr = new JSONArray();
        arr.put("chat");
        arr.put(roomname);
        arr.put(noImageURL);
        arr.put(username);
        arr.put(message);
        arr.put(usernameColor);
        arr.put(chatTextColor);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void SendPrivate(String idtarget, String noimageUrl, String message, String sender) {
        if (idtarget == null) idtarget = "";
        if (noimageUrl == null) noimageUrl = "";
        if (message == null) message = "";
        if (sender == null) sender = "";

        JSONArray arr = new JSONArray();
        arr.put("private");
        arr.put(idtarget);
        arr.put(noimageUrl);
        arr.put(message);
        arr.put(sender);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void Sendnotif(String idtarget, String noimageUrl, String username, String deskripsi) {
        if (idtarget == null) idtarget = "";
        if (noimageUrl == null) noimageUrl = "";
        if (username == null) username = "";
        if (deskripsi == null) deskripsi = "";

        JSONArray arr = new JSONArray();
        arr.put("sendnotif");
        arr.put(idtarget);
        arr.put(noimageUrl);
        arr.put(username);
        arr.put(deskripsi);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void SendNotifWithDelay(YailList onlineUsers, String noimageUrl, String username, String deskripsi) {
        if (onlineUsers == null || onlineUsers.size() == 0) {
            OnNotificationBatchCompleted(0, 0);
            return;
        }

        if (noimageUrl == null) noimageUrl = "";
        if (username == null) username = "";
        if (deskripsi == null) deskripsi = "";

        final Object[] usersArray = onlineUsers.toArray();
        final int total = usersArray.length;
        final int[] sentCount = {0};

        for (int i = 0; i < usersArray.length; i++) {
            final int index = i;
            String finalNoimageUrl = noimageUrl;
            String finalUsername = username;
            String finalDeskripsi = deskripsi;
            mainHandler.postDelayed(() -> {
                String userId = usersArray[index] == null ? "" : usersArray[index].toString();
                if (!userId.isEmpty()) {
                    Sendnotif(userId, finalNoimageUrl, finalUsername, finalDeskripsi);
                    sentCount[0]++;
                }

                if (sentCount[0] == total) {
                    OnNotificationBatchCompleted(total, total);
                }
            }, index * 50L);
        }
    }

    @SimpleFunction
    public void RemoveKursiAndPoint(String roomName, int seatNumber) {
        if (roomName == null) roomName = "";
        JSONArray arr = new JSONArray();
        arr.put("removeKursiAndPoint");
        arr.put(roomName);
        arr.put(seatNumber);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void resetlallrom() {
        JSONArray arr = new JSONArray();
        arr.put("resetRoom");
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void updatePoint(String roomname, String seat, float x, float y, String fast) {
        if (roomname == null) roomname = "";
        if (seat == null) seat = "-1";
        if (fast == null) fast = "0";

        try {
            int seatInt = Integer.parseInt(seat);
            int fastInt = Integer.parseInt(fast);

            if (seatInt >= 0) {
                JSONArray arr = new JSONArray();
                arr.put("updatePoint");
                arr.put(roomname);
                arr.put(seatInt);
                arr.put(x);
                arr.put(y);
                arr.put(fastInt);
                sendJson(arr.toString());
            }
        } catch (NumberFormatException e) {
            // Silent fail
        } catch (JSONException e) {
            // Silent fail
        }
    }

    @SimpleFunction
    public void updateKursi(String roomname, String seat, String noimageUrl, String namauser,
                            String color, String itembawah, String itematas, String vip, String viptanda) {
        if (roomname == null) roomname = "";
        if (seat == null) seat = "-1";
        if (noimageUrl == null) noimageUrl = "";
        if (namauser == null) namauser = "";
        if (color == null) color = "";
        if (itembawah == null) itembawah = "0";
        if (itematas == null) itematas = "0";
        if (vip == null) vip = "0";
        if (viptanda == null) viptanda = "0";

        try {
            int seatInt = Integer.parseInt(seat);
            int itembawahInt = Integer.parseInt(itembawah);
            int itematasInt = Integer.parseInt(itematas);
            int vipInt = Integer.parseInt(vip);
            int viptandaInt = Integer.parseInt(viptanda);

            if (seatInt >= 0) {
                JSONArray arr = new JSONArray();
                arr.put("updateKursi");
                arr.put(roomname);
                arr.put(seatInt);
                arr.put(noimageUrl);
                arr.put(namauser);
                arr.put(color);
                arr.put(itembawahInt);
                arr.put(itematasInt);
                arr.put(vipInt);
                arr.put(viptandaInt);
                sendJson(arr.toString());
            }
        } catch (NumberFormatException e) {
            // Silent fail
        }
    }

    @SimpleFunction
    public void SendModwarning() {
        if (isConnected.get() && roomnama != null && !roomnama.isEmpty()) {
            JSONArray arr = new JSONArray();
            arr.put("modwarning");
            arr.put(roomnama);
            sendJson(arr.toString());
        }
    }

    @SimpleFunction
    public void JoinRoom(String roomname) {
        if (roomname == null) roomname = "";
        this.roomnama = roomname;

        if (isConnected.get() && myIdTarget != null && !myIdTarget.isEmpty()) {
            sendJoinRoom();
        }
    }

    private void sendJoinRoom() {
        if (isConnected.get() && roomnama != null && !roomnama.isEmpty()) {
            JSONArray arr = new JSONArray();
            arr.put("joinRoom");
            arr.put(roomnama);
            sendJson(arr.toString());
        }
    }

    @SimpleFunction
    public void RequestOnlineALLUsersList() {
        JSONArray arr = new JSONArray();
        arr.put("getOnlineUsers");
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void IsUserOnline(String userId, String tanda) {
        if (userId == null) userId = "";
        if (tanda == null) tanda = "";

        JSONArray arr = new JSONArray();
        arr.put("isUserOnline");
        arr.put(userId);
        arr.put(tanda);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void GetAllRoomsUserCount() {
        JSONArray arr = new JSONArray();
        arr.put("getAllRoomsUserCount");
        sendJson(arr.toString());
    }

    private boolean sendJson(String jsonStr) {
        if (jsonStr == null || jsonStr.isEmpty()) {
            OnSlowNetworkDetected("Cannot send empty message");
            return false;
        }

        if (!isConnected.get()) {
            OnSlowNetworkDetected("Not connected to server. Please check network.");
            return false;
        }

        if (isReconnecting.get() || isCleaningUp.get()) {
            OnSlowNetworkDetected("Reconnecting, message queued but not sent");
            return false;
        }

        synchronized (webSocketLock) {
            if (webSocket == null) {
                OnSlowNetworkDetected("WebSocket is null. Connection lost.");
                isConnected.set(false);
                return false;
            }

            try {
                boolean sent = webSocket.send(jsonStr);
                if (!sent) {
                    OnSlowNetworkDetected("Message failed to send. Network problem.");
                }
                return sent;
            } catch (IllegalStateException e) {
                OnSlowNetworkDetected("WebSocket closed. " + e.getMessage());
                synchronized (webSocketLock) {
                    webSocket = null;
                }
                isConnected.set(false);
                return false;
            } catch (Exception e) {
                OnSlowNetworkDetected("Send error: " + e.getMessage());
                return false;
            }
        }
    }

    @SimpleFunction
    public void GetNumber() {
        JSONArray arr = new JSONArray();
        arr.put("getCurrentNumber");
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void SendGift(String sender, String receiver, String giftName) {
        if (sender == null) sender = "";
        if (receiver == null) receiver = "";
        if (giftName == null) giftName = "";

        if (isConnected.get() && roomnama != null && !roomnama.isEmpty()) {
            JSONArray arr = new JSONArray();
            arr.put("gift");
            arr.put(roomnama);
            arr.put(sender);
            arr.put(receiver);
            arr.put(giftName);
            sendJson(arr.toString());
        }
    }

    // ==================== GAME METHODS ====================

    @SimpleFunction
    public void GameLowCardStart(int betAmount, String username) {
        if (username == null) username = "";

        if (isGameConnected.get() && gameWebSocket != null) {
            JSONArray arr = new JSONArray();
            arr.put("gameLowCardStart");
            arr.put(betAmount);
            arr.put(username);
            sendGameJson(arr.toString());
            return;
        }
        if (isConnected.get()) {
            JSONArray arr = new JSONArray();
            arr.put("gameLowCardStart");
            arr.put(betAmount);
            arr.put(username);
            sendJson(arr.toString());
        }
    }

    @SimpleFunction
    public void GameLowCardJoin(String username) {
        if (username == null) username = "";

        if (isGameConnected.get() && gameWebSocket != null) {
            JSONArray arr = new JSONArray();
            arr.put("gameLowCardJoin");
            arr.put(username);
            sendGameJson(arr.toString());
            return;
        }
        if (isConnected.get()) {
            JSONArray arr = new JSONArray();
            arr.put("gameLowCardJoin");
            arr.put(username);
            sendJson(arr.toString());
        }
    }

    @SimpleFunction
    public void GameLowCardNumber(int number, String tanda, String username) {
        if (tanda == null) tanda = "";
        if (username == null) username = "";

        if (isGameConnected.get() && gameWebSocket != null) {
            JSONArray arr = new JSONArray();
            arr.put("gameLowCardNumber");
            arr.put(number);
            arr.put(tanda);
            arr.put(username);
            sendGameJson(arr.toString());
            return;
        }
        if (isConnected.get()) {
            JSONArray arr = new JSONArray();
            arr.put("gameLowCardNumber");
            arr.put(number);
            arr.put(tanda);
            arr.put(username);
            sendJson(arr.toString());
        }
    }

    @SimpleFunction
    public void CheckGameRunning(String roomname) {
        if (roomname == null || roomname.isEmpty()) {
            return;
        }

        if (isGameConnected.get() && gameWebSocket != null) {
            JSONArray arr = new JSONArray();
            arr.put("checkGameRunning");
            arr.put(roomname);
            sendGameJson(arr.toString());
            return;
        }
        if (isConnected.get()) {
            JSONArray arr = new JSONArray();
            arr.put("checkGameRunning");
            arr.put(roomname);
            sendJson(arr.toString());
        }
    }

    @SimpleFunction
    public void ConnectGame(String baseUrl) {
        this.gameServerUrl = baseUrl;

        String url = baseUrl + "/game/ws";

        Request request = new Request.Builder().url(url).build();
        gameWebSocket = client.newWebSocket(request, new GameWebSocketListener());
    }

    @SimpleFunction
    public void GameSwitchRoom(String room) {
        if (gameWebSocket != null) {
            JSONArray message = new JSONArray();
            message.put("switchRoom");
            message.put(room);
            sendGameJson(message.toString());
        }
    }

    @SimpleFunction
    public void DisconnectGame() {
        isGameConnected.set(false);
        reconnectAttempts = 0;
        if (gameWebSocket != null) {
            try {
                gameWebSocket.close(1000, "Disconnect");
            } catch (Exception e) {}
            gameWebSocket = null;
        }
    }

    @SimpleFunction
    public void GameLowCardLeave() {
        if (!isGameConnected.get() || gameWebSocket == null) return;
        if (gameUsername == null || gameUsername.isEmpty()) return;

        try {
            JSONArray arr = new JSONArray();
            arr.put("gameLowCardLeave");
            arr.put(gameUsername);
            arr.put(roomnama);
            sendGameJson(arr.toString());
        } catch (Exception e) {
            OnGameError("Failed to send leave: " + e.getMessage());
        }
    }

    private boolean sendGameJson(String jsonStr) {
        if (!isGameConnected.get() || gameWebSocket == null) return false;
        try {
            gameWebSocket.send(jsonStr);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ==================== QUIZ METHODS ====================

    @SimpleFunction
    public void SubmitQuizAnswer(String username, String answer) {
        if (username == null || username.isEmpty()) {
            OnQuizError("Username is required");
            return;
        }
        if (answer == null || answer.isEmpty()) {
            OnQuizError("Answer is required");
            return;
        }

        String answerUpper = answer.toUpperCase().trim();
        
        if (!answerUpper.matches("[ABCD]")) {
            OnQuizError("Invalid answer! Use A, B, C, or D");
            return;
        }

        if (isGameConnected.get() && gameWebSocket != null) {
            try {
                JSONArray arr = new JSONArray();
                arr.put("submitQuizAnswer");
                arr.put(username);
                arr.put(answerUpper);
                sendGameJson(arr.toString());
                return;
            } catch (Exception e) {
                OnQuizError("Failed to send: " + e.getMessage());
            }
        }

        if (isConnected.get()) {
            try {
                JSONArray arr = new JSONArray();
                arr.put("submitQuizAnswer");
                arr.put(username);
                arr.put(answerUpper);
                sendJson(arr.toString());
                return;
            } catch (Exception e) {
                OnQuizError("Failed to send: " + e.getMessage());
            }
        }

        OnQuizError("Not connected");
    }

    // ==================== GETTER METHODS ====================

    @SimpleFunction
    public String GetConnectionState() {
        if (isConnected.get()) return "CONNECTED";
        if (isConnecting.get()) return "CONNECTING";
        if (isReconnecting.get()) return "RECONNECTING";
        if (isManualDisconnect.get()) return "MANUALLY_DISCONNECTED";
        return "DISCONNECTED";
    }

    @SimpleFunction
    public int GetAndroidSDKVersion() {
        return Build.VERSION.SDK_INT;
    }

    @SimpleFunction
    public int GetReconnectCount() {
        return pingReconnectCount;
    }

    @SimpleFunction
    public String GetCurrentRoom() {
        return roomnama != null ? roomnama : "";
    }

    @SimpleFunction
    public boolean IsMinimized() {
        return minimize;
    }

    @SimpleFunction
    public int GetMySeatNumber() {
        return mySeatNumber;
    }

    @SimpleFunction
    public String GetMyIdTarget() {
        return myIdTarget != null ? myIdTarget : "";
    }

    @SimpleFunction
    public void ClearAllRoomsData() {
        // Tidak ada data yang disimpan
    }

    @SimpleFunction
    public int GetRoomSeatCount(String roomName) {
        return 0;
    }

    @SimpleFunction
    public void onDestroy() {
        SendOnDestroy();
        GameLowCardLeave();
        Disconnect();
        DisconnectGame();
        cleanup();
    }

    // ==================== CLEANUP ====================

    private void cleanup() {
        isCleaningUp.set(true);
        stopReconnectProcess();
        cleanupStatusTimeout();
        cleanupPointHandler();
        cleanupKursiHandler();

        synchronized (webSocketLock) {
            if (webSocket != null) {
                try {
                    webSocket.close(1000, "Cleanup");
                } catch (Exception ignored) {}
                webSocket = null;
            }
        }

        stopPulseAnimation();

        if (toast != null) {
            toast.cancel();
            toast = null;
        }

        isReconnecting.set(false);
        isManualDisconnect.set(false);
        isConnecting.set(false);
        shouldReconnect.set(true);
        isConnected.set(false);
        isHandlingConnectionLoss.set(false);
        pingReconnectCount = 0;
        hasReceivedStatusResponse = 0;

        messageQueue.clear();
        isCleaningUp.set(false);

        OnCleanupCompleted();
    }

    private void cleanupStatusTimeout() {
        if (statusTimeoutHandler != null) {
            if (statusTimeoutRunnable != null) {
                statusTimeoutHandler.removeCallbacks(statusTimeoutRunnable);
                statusTimeoutRunnable = null;
            }
            statusTimeoutHandler.removeCallbacksAndMessages(null);
        }
        hasReceivedStatusResponse = 0;
    }

    private void cleanupPointHandler() {
        if (pointHandler != null) {
            pointHandler.removeCallbacksAndMessages(null);
        }
    }

    private void cleanupKursiHandler() {
        if (kursiHandler != null) {
            kursiHandler.removeCallbacksAndMessages(null);
        }
    }

    // ==================== CHAT METHODS LAINNYA ====================

    @SimpleFunction
    public void SetMutetoroom(boolean isMuted, String roomname) {
        if (roomname == null) roomname = "";
        JSONArray arr = new JSONArray();
        arr.put("setMuteType");
        arr.put(isMuted);
        arr.put(roomname);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void GetMuteType(String roomname) {
        if (roomname == null) roomname = "";
        JSONArray arr = new JSONArray();
        arr.put("getMuteType");
        arr.put(roomname);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void rollangak(String roomname, String username, int angka) {
        if (roomname == null) roomname = "";
        if (username == null) username = "";

        JSONArray arr = new JSONArray();
        arr.put("rollangak");
        arr.put(roomname);
        arr.put(username);
        arr.put(angka);
        sendJson(arr.toString());
    }

    // ==================== MULTI AKUN ====================

    @SimpleFunction
    public void MultiJoinRoom(String username, String roomname) {
        if (username == null || username.isEmpty()) {
            OnMultiError("Username tidak boleh kosong");
            return;
        }
        if (roomname == null || roomname.isEmpty()) {
            OnMultiError("Roomname tidak boleh kosong");
            return;
        }

        JSONArray arr = new JSONArray();
        arr.put("multiJoin");
        arr.put(username);
        arr.put(roomname);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void MultiSetActive(String username) {
        if (username == null || username.isEmpty()) {
            OnMultiError("Username tidak boleh kosong");
            return;
        }

        JSONArray arr = new JSONArray();
        arr.put("setActiveMulti");
        arr.put(username);
        sendJson(arr.toString());
    }

    @SimpleFunction
    public void MultiExit(String username) {
        if (username == null || username.isEmpty()) {
            OnMultiError("Username tidak boleh kosong");
            return;
        }

        JSONArray arr = new JSONArray();
        arr.put("exitMulti");
        arr.put(username);
        sendJson(arr.toString());
    }

    // ==================== DELAY ====================

    private final Handler delayHandler = new Handler(Looper.getMainLooper());
    private Runnable delayRunnable;

    @SimpleFunction
    public void setDelay(int ms) {
        if (delayRunnable != null) {
            delayHandler.removeCallbacks(delayRunnable);
        }

        delayRunnable = new Runnable() {
            @Override
            public void run() {
                OnDelayDone();
            }
        };

        delayHandler.postDelayed(delayRunnable, ms);
    }

    @SimpleFunction
    public void cancelDelay() {
        if (delayRunnable != null) {
            delayHandler.removeCallbacks(delayRunnable);
            delayRunnable = null;
        }
    }

    // ==================== PROCESS BATCH ====================

    private void processKursiBatch(String room, List<Integer> seats, List<JSONObject> infos, int index) {
        if (index >= seats.size()) {
            fastkursi = false;
            return;
        }

        int endIndex = Math.min(index + 5, seats.size());
        for (int i = index; i < endIndex; i++) {
            int seat = seats.get(i);
            JSONObject info = infos.get(i);

            if (fastkursi) {
                OnUpdateKursiHistory2(
                        room, seat,
                        info.optString("noimageUrl", ""),
                        info.optString("namauser", ""),
                        info.optString("color", ""),
                        info.optInt("itembawah", 0),
                        info.optInt("itematas", 0),
                        info.optInt("vip", 0),
                        info.optInt("viptanda", 0)
                );
            } else {
                OnUpdateKursiHistory(
                        room, seat,
                        info.optString("noimageUrl", ""),
                        info.optString("namauser", ""),
                        info.optString("color", ""),
                        info.optInt("itembawah", 0),
                        info.optInt("itematas", 0),
                        info.optInt("vip", 0),
                        info.optInt("viptanda", 0)
                );
            }
        }

        if (endIndex < seats.size()) {
            mainHandler.postDelayed(() -> processKursiBatch(room, seats, infos, endIndex), 50);
        } else {
            fastkursi = false;
        }
    }

    private void processPointBatch(String roomName, List<Integer> seats, List<Float> xs, List<Float> ys, List<Integer> fasts, int index) {
        if (index >= seats.size()) return;

        int endIndex = Math.min(index + 10, seats.size());
        for (int i = index; i < endIndex; i++) {
            OnPointHistory(roomName, seats.get(i), xs.get(i), ys.get(i), fasts.get(i));
        }

        if (endIndex < seats.size()) {
            mainHandler.postDelayed(() -> processPointBatch(roomName, seats, xs, ys, fasts, endIndex), 30);
        }
    }

    private void handleConnectionLoss(String reason) {
        if (!isHandlingConnectionLoss.compareAndSet(false, true) || isCleaningUp.get()) {
            return;
        }

        Disconnecteror();
        isConnecting.set(false);
        isConnected.set(false);

        cleanupPointHandler();

        if (!isManualDisconnect.get() && shouldReconnect.get() && !isReconnecting.get()) {
            mainHandler.postDelayed(() -> {
                isHandlingConnectionLoss.set(false);
                startReconnectProcess();
                OnNeedReconnect(reason);
            }, 2000);
        } else {
            isHandlingConnectionLoss.set(false);
            OnConnectionLost(reason);
        }
    }

    // ==================== CHAT WEB SOCKET LISTENER ====================

    private class WebSocketListenerImpl extends WebSocketListener {

        @Override
        public void onOpen(WebSocket ws, Response response) {
            synchronized (webSocketLock) {
                if (webSocket != null && webSocket != ws) {
                    try {
                        webSocket.close(1000, "Replaced by new connection");
                    } catch (Exception ignored) {}
                }
                webSocket = ws;
            }

            stopReconnectProcess();
            onConnected();

            if (roomnama != null && !roomnama.isEmpty() && myIdTarget != null && !myIdTarget.isEmpty()) {
                fastkursi = true;
                mainHandler.postDelayed(() -> {
                    SetIdTarget2(myIdTarget, false);
                }, 500);
            } else {
                mainHandler.post(() -> {
                    OnOpen();
                });
            }
        }

        @Override
        public void onMessage(WebSocket ws, String text) {
            messageQueue.offer(() -> processMessage(text));
            if (messageQueue.size() == 1) {
                messageHandler.post(messageProcessor);
            }
        }

        private void processMessage(String text) {
            if (text == null || text.isEmpty()) return;

            try {
                JSONArray data = new JSONArray(text);
                String evt = data.getString(0);

                switch (evt) {
                    case "rooMasuk":
                        int seatNr = data.getInt(1);
                        String roomz = data.getString(2);
                        joinroomsucces(roomz, seatNr);
                        ClearMessageQueue();
                        break;

                    case "needJoinRoom":
                        mainHandler.postDelayed(() -> {
                            OnNeedjoinroom();
                        }, 1000);
                        break;

                    case "joinroomawal":
                        OnJoinroomawal();
                        break;

                    case "inRoomStatus":
                        boolean isInRoom = data.getBoolean(1);
                        hasReceivedStatusResponse = isInRoom ? 1 : 2;
                        OnInRoomStatusReceived(isInRoom);
                        break;

                    case "numberKursiSaya":
                        mySeatNumber = data.getInt(1);
                        if (mySeatNumber >= 1 && mySeatNumber <= 45) {
                            OnNumberKursiSaya(mySeatNumber);
                        }
                        break;

                    case "roomFull":
                        OnRoomFull(data.getString(1));
                        break;

                    case "resetRoom": {
                        String roomName = data.getString(1);
                        OnResetRoom(roomName);
                        break;
                    }

                    case "removeKursi": {
                        String roomName = data.getString(1);
                        int seatNumber = data.getInt(2);
                        if (seatNumber >= 1 && seatNumber <= 45) {
                            OnRemoveKursi(roomName, seatNumber);
                        }
                        break;
                    }

                    case "allUpdateKursiList": {
                        final String room = data.getString(1);
                        final JSONObject meta = data.getJSONObject(2);

                        mainHandler.postDelayed(() -> {
                            if (kursiHandler != null) {
                                kursiHandler.removeCallbacksAndMessages(null);
                            }

                            Iterator<String> keys = meta.keys();
                            List<Integer> seats = new ArrayList<>();
                            List<JSONObject> infos = new ArrayList<>();

                            while (keys.hasNext()) {
                                String key = keys.next();
                                try {
                                    int seat = Integer.parseInt(key);
                                    if (seat >= 1 && seat <= 45) {
                                        seats.add(seat);
                                        infos.add(meta.getJSONObject(key));
                                    }
                                } catch (Exception e) {
                                    // Silent fail
                                }
                            }

                            processKursiBatch(room, seats, infos, 0);
                        }, 1000);

                        break;
                    }

                    case "kursiBatchUpdate": {
                        String roomName = data.getString(1);
                        JSONArray kursiList = data.getJSONArray(2);

                        for (int i = 0; i < kursiList.length(); i++) {
                            JSONArray kursi = kursiList.getJSONArray(i);
                            int seat = kursi.getInt(0);

                            if (seat >= 1 && seat <= 45) {
                                JSONObject info = kursi.getJSONObject(1);

                                String noimageUrl = info.optString("noimageUrl", "");
                                String namauser = info.optString("namauser", "");
                                String color = info.optString("color", "");
                                int itembawah = info.optInt("itembawah", 0);
                                int itematas = info.optInt("itematas", 0);
                                int vip = info.optInt("vip", 0);
                                int viptanda = info.optInt("viptanda", 0);

                                OnKursiUpdated(
                                        roomName,
                                        seat,
                                        noimageUrl,
                                        namauser,
                                        color,
                                        itembawah,
                                        itematas,
                                        vip,
                                        viptanda
                                );
                            }
                        }
                        break;
                    }

                    case "allPointsList": {
                        final String roomName = data.optString(1, "");
                        final JSONArray points = data.optJSONArray(2);
                        if (points == null || points.length() == 0) break;

                        mainHandler.postDelayed(() -> {
                            if (pointHandler != null) {
                                pointHandler.removeCallbacksAndMessages(null);
                            }

                            List<Integer> pointSeats = new ArrayList<>();
                            List<Float> pointXs = new ArrayList<>();
                            List<Float> pointYs = new ArrayList<>();
                            List<Integer> pointFasts = new ArrayList<>();

                            for (int i = 0; i < points.length(); i++) {
                                try {
                                    JSONObject p = points.getJSONObject(i);
                                    int seat = p.optInt("seat", -1);
                                    if (seat >= 1 && seat <= 45) {
                                        pointSeats.add(seat);
                                        pointXs.add((float) p.optDouble("x", 0));
                                        pointYs.add((float) p.optDouble("y", 0));
                                        pointFasts.add(p.optInt("fast", 0));
                                    }
                                } catch (JSONException e) {
                                    // Silent fail
                                }
                            }

                            processPointBatch(roomName, pointSeats, pointXs, pointYs, pointFasts, 0);
                        }, 2000);

                        break;
                    }

                    case "pointUpdated": {
                        String roomName = data.getString(1);
                        int seat = data.getInt(2);
                        double x = data.getDouble(3);
                        double y = data.getDouble(4);
                        int fast = data.getInt(5);

                        if (seat >= 1 && seat <= 45) {
                            OnPointUpdated(roomName, seat, x, y, fast);
                        }
                        break;
                    }

                    case "chat": {
                        OnChaRoomReceived(
                                data.optString(1, ""),
                                data.optString(2, ""),
                                data.optString(3, ""),
                                data.optString(4, ""),
                                data.optString(5, ""),
                                data.optString(6, "")
                        );
                        break;
                    }

                    case "private": {
                        OnPrivateMessageReceived(
                                data.optString(1, ""),
                                data.optString(2, ""),
                                data.optString(3, ""),
                                data.optLong(4, 0),
                                data.optString(5, "")
                        );
                        break;
                    }

                    case "notif": {
                        OnReceiveNotif(
                                data.optString(1, ""),
                                data.optString(2, ""),
                                data.optString(3, ""),
                                data.optLong(4, 0)
                        );
                        break;
                    }

                    case "userOnlineStatus": {
                        String tanda = data.length() > 3 ? data.optString(3, "") : "";
                        OnUserOnlineStatus(data.optString(1, ""), data.optBoolean(2, false), tanda);
                        break;
                    }

                    case "roomUserCount": {
                        OnRoomUserCount(data.optString(1, ""), data.optInt(2, 0));
                        break;
                    }

                    case "privateFailed": {
                        OnPrivateFailed(data.optString(1, ""), data.optString(2, ""));
                        break;
                    }

                    case "currentNumber": {
                        OnBgNumberReceived(data.optInt(1, 0));
                        break;
                    }

                    case "allOnlineUsers": {
                        JSONArray onlineList = data.getJSONArray(1);
                        List<String> users = new ArrayList<>();
                        for (int i = 0; i < onlineList.length(); i++) {
                            users.add(onlineList.optString(i, ""));
                        }
                        OnAllUserOnlineList(YailList.makeList(users));
                        break;
                    }

                    case "allRoomsUserCount": {
                        String jsonStr = data.getJSONArray(1).toString();
                        setAllRoomsFromJson(jsonStr);
                        JSONArray rooms = data.getJSONArray(1);
                        for (int i = 0; i < rooms.length(); i++) {
                            JSONObject room = rooms.getJSONObject(i);
                            OnAllJumlahRoom(room.optString("roomName", ""), room.optInt("userCount", 0));
                        }
                        break;
                    }

                    case "gift": {
                        OnGiftReceived(
                                data.optString(1, ""),
                                data.optString(2, ""),
                                data.optString(3, ""),
                                data.optString(4, ""),
                                data.optLong(5, 0)
                        );
                        break;
                    }

                    case "modwarning": {
                        String roomName = data.optString(1, "");
                        OnModwarningReceived(roomName);
                        break;
                    }

                    case "muteTypeResponse":
                        boolean isMuted = data.optBoolean(1, false);
                        String roomName = data.optString(2, "");
                        OnMuteTypeReceived(isMuted, roomName);
                        break;

                    case "muteStatusChanged":
                        boolean isMutedChanged = data.optBoolean(1, false);
                        String roomNameChanged = data.optString(2, "");
                        OnMuteStatusChanged(isMutedChanged, roomNameChanged);
                        break;

                    case "rollangakBroadcast":
                        String roomBroadcast = data.optString(1, "");
                        String usernameBroadcast = data.optString(2, "");
                        int angkaBroadcast = data.optInt(3, 0);
                        OnRollangakBroadcast(roomBroadcast, usernameBroadcast, angkaBroadcast);
                        break;

                    case "rooMasukMulti": {
                        int seatNumber = data.getInt(1);
                        String roomNameMulti = data.getString(2);
                        OnMultiJoinSuccess(seatNumber, roomNameMulti);
                        break;
                    }

                    case "activeChangedMulti": {
                        String username = data.getString(1);
                        int seatNumber = data.optInt(2, -1);
                        OnMultiSetActiveSuccess(username, seatNumber);
                        break;
                    }

                    case "forceExit": {
                        String reason = data.optString(1, "Multi akun dikeluarkan");
                        OnMultiForceExit(reason);
                        break;
                    }

                    case "userActiveChanged": {
                        String username = data.getString(1);
                        int seatNumber = data.optInt(2, -1);
                        OnUserActiveChanged(username, seatNumber);
                        break;
                    }

                    default:
                        break;
                }

            } catch (JSONException e) {
                // Silent fail
            } catch (Exception e) {
                // Silent fail
            }
        }

        @Override
        public void onClosed(WebSocket ws, int code, String reason) {
            synchronized (webSocketLock) {
                if (webSocket == ws) {
                    webSocket = null;
                }
            }

            isConnected.set(false);
            isConnecting.set(false);

            if (code == 1000 || code == 1001) {
                OnWebSocketClosed(code, reason != null ? reason : "Normal close");
                OnConnectionLost("Disconnected normally");
                return;
            }

            if (code == 1006) {
                OnWebSocketClosed(code, "Connection lost (abnormal)");
                OnConnectionLost("Connection lost");
                return;
            }

            OnWebSocketClosed(code, reason != null ? reason : "");
            handleConnectionLoss("Connection closed");
        }

        @Override
        public void onFailure(WebSocket ws, Throwable t, Response r) {
            synchronized (webSocketLock) {
                if (webSocket == ws) {
                    try {
                        ws.close(1000, "Failure cleanup");
                    } catch (Exception ignored) {}
                    webSocket = null;
                }
            }

            isConnected.set(false);
            isConnecting.set(false);

            boolean shouldProcess = true;
            if (minimize && Build.VERSION.SDK_INT >= 45) {
                shouldProcess = false;
            }

            if (shouldProcess && roomnama != null && !roomnama.isEmpty() && !isCleaningUp.get()) {
                mainHandler.postDelayed(() -> {
                    if (!isConnected.get() && !isManualDisconnect.get() && !isCleaningUp.get()) {
                        handleConnectionLoss("Connection failed");
                    }
                }, 2000);
            }

            OnWebSocketFailure(t != null ? t.getMessage() : "Unknown error");
        }
    }

    // ==================== GAME WEB SOCKET LISTENER ====================

    private class GameWebSocketListener extends WebSocketListener {
        @Override
        public void onMessage(WebSocket ws, String text) {
            try {
                JSONArray data = new JSONArray(text);
                String evt = data.getString(0);

                switch (evt) {
                    // ==================== GAME LOWCARD ====================
                    case "gameStatus": {
                        String running = "";
                        try {
                            if (data.length() > 1 && data.get(1) instanceof JSONObject) {
                                JSONObject status = data.getJSONObject(1);
                                running = status.getString("running");
                            }
                        } catch (Exception e) {
                            running = "";
                        }
                        String finalRunning = running;
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameStatusReceived(finalRunning);
                            }
                        });
                        break;
                    }

                    case "gameLowCardStart": {
                        int bet = data.optInt(1, 0);
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardStart(bet);
                            }
                        });
                        break;
                    }

                    case "gameLowCardStartSuccess": {
                        String hostName = data.optString(1, "");
                        int bet = data.optInt(2, 0);
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardStartSuccess(hostName, bet);
                            }
                        });
                        break;
                    }

                    case "gameLowCardJoin": {
                        String player = data.optString(1, "");
                        int bet = data.optInt(2, 0);
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardJoin(player, bet);
                            }
                        });
                        break;
                    }

                    case "gameLowCardNoJoin": {
                        String hostName = data.optString(1, "");
                        int bet = data.optInt(2, 0);
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardNoJoin(hostName, bet);
                            }
                        });
                        break;
                    }

                    case "gameLowCardClosed": {
                        String message = "Players in the game";
                        try {
                            JSONArray arr = data.getJSONArray(1);
                            List<String> playerList = new ArrayList<>();
                            for (int i = 0; i < arr.length(); i++) {
                                playerList.add(arr.optString(i, ""));
                            }
                            message = "Players: " + String.join(", ", playerList);
                        } catch (Exception e) {}
                        final String finalMessage = message;
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardClosedMessage(finalMessage);
                            }
                        });
                        break;
                    }

                    case "gameLowCardPlayerDraw": {
                        String playerId = data.optString(1, "");
                        int number = data.optInt(2, 0);
                        String tanda = data.optString(3, "");
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardPlayerDraw(playerId, number, tanda);
                            }
                        });
                        break;
                    }

                    case "gameLowCardRoundResult": {
                        int round = data.getInt(1);
                        JSONArray losersArr = data.getJSONArray(3);
                        List<String> losersList = new ArrayList<>();
                        for (int i = 0; i < losersArr.length(); i++) {
                            losersList.add(losersArr.optString(i, ""));
                        }
                        String message = "";
                        if (!losersList.isEmpty()) {
                            message = String.join(", ", losersList) + " OUT with the lowest card!";
                        } else {
                            message = "No one eliminated this round! 🎉";
                        }
                        final String finalMessage = message;
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardRoundResult(finalMessage);
                            }
                        });
                        break;
                    }

                    case "gameLowCardWinner": {
                        String winnerId = data.optString(1, "");
                        int totalCoin = data.optInt(2, 0);
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardWinner(winnerId, totalCoin);
                            }
                        });
                        break;
                    }

                    case "gameLowCardNextRound": {
                        int round = data.getInt(1);
                        String message = "ROUND #" + round + "\nGet ready now! click draw";
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardNextRound(message);
                            }
                        });
                        break;
                    }

                    case "gameLowCardTimeLeft": {
                        String timeLeft = data.optString(1, "");
                        String message = "Time left: " + timeLeft;
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardTimeLeft(message);
                            }
                        });
                        break;
                    }

                    case "gameLowCardError": {
                        String error = data.optString(1, "");
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameLowCardError(error);
                            }
                        });
                        break;
                    }

                    case "gameLowCardWait": {
                        String message = data.optString(1, "Please wait for results...");
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameWait(message);
                            }
                        });
                        break;
                    }

                    case "gameLowCardEnd": {
                        String message = "Game has ended";
                        try {
                            JSONArray arr = data.getJSONArray(1);
                            List<String> players = new ArrayList<>();
                            for (int i = 0; i < arr.length(); i++) {
                                players.add(arr.optString(i, ""));
                            }
                            if (!players.isEmpty()) {
                                message = "Game ended. Players: " + String.join(", ", players);
                            }
                        } catch (Exception e) {}
                        final String finalMessage = message;
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnGameEnded(finalMessage);
                            }
                        });
                        break;
                    }

                    // ==================== QUIZ EVENTS ====================

                    case "quizQuestion": {
                        JSONObject qObj = data.optJSONObject(1);
                        String question = qObj != null ? qObj.optString("question", "") : "";
                        JSONObject optionsObj = qObj != null ? qObj.optJSONObject("options") : null;

                        StringBuilder formatted = new StringBuilder();
                        formatted.append(question).append("\n");

                        if (optionsObj != null) {
                            if (optionsObj.has("A")) formatted.append("A. ").append(optionsObj.getString("A")).append("\n");
                            if (optionsObj.has("B")) formatted.append("B. ").append(optionsObj.getString("B")).append("\n");
                            if (optionsObj.has("C")) formatted.append("C. ").append(optionsObj.getString("C")).append("\n");
                            if (optionsObj.has("D")) formatted.append("D. ").append(optionsObj.getString("D")).append("\n");
                        }

                        final String fQuestion = formatted.toString();
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnQuizQuestion(fQuestion);
                            }
                        });
                        break;
                    }

                    case "quizAnswerResult": {
                        JSONObject resultObj = data.optJSONObject(1);
                        String username = resultObj != null ? resultObj.optString("username", "") : "";
                        String answer = resultObj != null ? resultObj.optString("answer", "") : "";
                        boolean isCorrect = resultObj != null && resultObj.optBoolean("isCorrect", false);
                        String correctAnswer = resultObj != null ? resultObj.optString("correctAnswer", "") : "";
                        
                        final String fUsername = username;
                        final String fAnswer = answer;
                        final boolean fIsCorrect = isCorrect;
                        final String fCorrectAnswer = correctAnswer;
                        
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnQuizAnswerResult(fUsername, fAnswer, fIsCorrect, fCorrectAnswer);
                            }
                        });
                        break;
                    }

                    case "quizWinner": {
                        JSONObject winnerObj = data.optJSONObject(1);
                        String username = winnerObj != null ? winnerObj.optString("username", "") : "";
                        final String fUsername = username;
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnQuizWinner(fUsername);
                            }
                        });
                        break;
                    }

                    case "quizNoWinner": {
                        JSONObject noWinnerObj = data.optJSONObject(1);
                        String message = noWinnerObj != null ? noWinnerObj.optString("message", "") : "";
                        final String fMessage = message;
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnQuizNoWinner(fMessage);
                            }
                        });
                        break;
                    }

                    case "quizError": {
                        String error = data.optString(1, "");
                        final String fError = error;
                        mainHandler.post(new Runnable() {
                            @Override
                            public void run() {
                                OnQuizError(fError);
                            }
                        });
                        break;
                    }

                    default:
                        break;
                }
            } catch (JSONException e) {
                // Silent fail
            } catch (Exception e) {
                // Silent fail
            }
        }

        @Override
        public void onOpen(WebSocket ws, Response response) {
            gameWebSocket = ws;
            isGameConnected.set(true);
            reconnectAttempts = 0;

            mainHandler.post(new Runnable() {
                @Override
                public void run() {
                    OnGameConnected();
                }
            });
        }

        @Override
        public void onClosed(WebSocket ws, int code, String reason) {
            isGameConnected.set(false);
            gameWebSocket = null;

            mainHandler.post(new Runnable() {
                @Override
                public void run() {
                    OnGameDisconnected();
                    OnGameError("Disconnected: " + (reason != null ? reason : "Normal"));
                }
            });
        }

        @Override
        public void onFailure(WebSocket ws, Throwable t, Response r) {
            isGameConnected.set(false);
            gameWebSocket = null;

            String error = t != null ? t.getMessage() : "Unknown error";
            mainHandler.post(new Runnable() {
                @Override
                public void run() {
                    OnGameError("Connection error: " + error);
                }
            });

            if (ws != null) {
                try {
                    ws.close(1000, "Failure handled");
                } catch (Exception e) {}
            }

            if (!isManualDisconnect.get() && !isCleaningUp.get()) {
                mainHandler.postDelayed(new Runnable() {
                    @Override
                    public void run() {
                        if (!isGameConnected.get() && !isManualDisconnect.get() && !isCleaningUp.get()) {
                            OnGameError("Attempting to reconnect...");
                            handleReconnection();
                        }
                    }
                }, 3000);
            }
        }
    }

    private void handleReconnection() {
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
            reconnectAttempts++;
            long delay = reconnectAttempts * 2 * 1000;

            new android.os.Handler().postDelayed(() -> {
                if (!isGameConnected.get()) {
                    ConnectGame(gameServerUrl);
                }
            }, delay);

            OnGameError("Connection lost, reconnecting");
        } else {
            reconnectAttempts = 0;
            OnGameError("Check your network");
        }
    }

    // ==================== EVENTS ====================

    @SimpleEvent
    public void OnGiftReceived(String roomname, String sender, String receiver, String giftName, long timestamp) {
        EventDispatcher.dispatchEvent(this, "OnGiftReceived",
                roomname != null ? roomname : "",
                sender != null ? sender : "",
                receiver != null ? receiver : "",
                giftName != null ? giftName : "",
                timestamp);
    }

    @SimpleEvent
    public void OnNeedReconnect(String reason) {
        EventDispatcher.dispatchEvent(this, "OnNeedReconnect", reason != null ? reason : "");
    }

    @SimpleEvent
    public void OnReconnectSuccess(String roomname) {
        EventDispatcher.dispatchEvent(this, "OnReconnectSuccess", roomname != null ? roomname : "");
    }

    @SimpleEvent
    public void Disconnecteror() {
        EventDispatcher.dispatchEvent(this, "Disconnecteror");
    }

    @SimpleEvent
    public void joinroomsucces(String room, int kursi) {
        EventDispatcher.dispatchEvent(this, "joinroomsucces",
                room != null ? room : "",
                kursi);
    }

    @SimpleEvent
    public void OnPointUpdated(String roomname, int seat, double x, double y, int fast) {
        if (roomname != null && seat >= 0) {
            EventDispatcher.dispatchEvent(this, "OnPointUpdated", roomname, seat, x, y, fast);
        }
    }

    @SimpleEvent
    public void OnPrivateMessageReceived(String fromId, String imageUrl, String message, long timestamp, String sender) {
        EventDispatcher.dispatchEvent(this, "OnPrivateMessageReceived",
                fromId != null ? fromId : "",
                imageUrl != null ? imageUrl : "",
                message != null ? message : "",
                timestamp,
                sender != null ? sender : "");
    }

    @SimpleEvent
    public void OnKursiUpdated(String roomname, int seat, String noimageUrl, String namauser,
                               String color, int itembawah, int itematas, int vip, int viptanda) {
        if (roomname != null && seat >= 0) {
            EventDispatcher.dispatchEvent(this, "OnKursiUpdated",
                    roomname, seat,
                    noimageUrl != null ? noimageUrl : "",
                    namauser != null ? namauser : "",
                    color != null ? color : "",
                    itembawah, itematas, vip, viptanda);
        }
    }

    @SimpleEvent
    public void OnUpdateKursiHistory(String roomname, int seat, String noimageUrl, String namauser,
                                     String color, int itembawah, int itematas, int vip, int viptanda) {
        if (roomname != null && seat >= 0) {
            EventDispatcher.dispatchEvent(this, "OnUpdateKursiHistory",
                    roomname, seat,
                    noimageUrl != null ? noimageUrl : "",
                    namauser != null ? namauser : "",
                    color != null ? color : "",
                    itembawah, itematas, vip, viptanda);
        }
    }

    @SimpleEvent
    public void OnUpdateKursiHistory2(String roomname, int seat, String noimageUrl, String namauser,
                                      String color, int itembawah, int itematas, int vip, int viptanda) {
        if (roomname != null && seat >= 0) {
            EventDispatcher.dispatchEvent(this, "OnUpdateKursiHistory2",
                    roomname, seat,
                    noimageUrl != null ? noimageUrl : "",
                    namauser != null ? namauser : "",
                    color != null ? color : "",
                    itembawah, itematas, vip, viptanda);
        }
    }

    @SimpleEvent
    public void OnNumberKursiSaya(int kursi) {
        EventDispatcher.dispatchEvent(this, "OnNumberKursiSaya", kursi);
    }

    @SimpleEvent
    public void OnRoomFull(String roomname) {
        EventDispatcher.dispatchEvent(this, "OnRoomFull", roomname != null ? roomname : "");
    }

    @SimpleEvent
    public void OnPointHistory(String roomname, int seat, double x, double y, int fast) {
        if (roomname != null && !roomname.isEmpty() && seat >= 0) {
            EventDispatcher.dispatchEvent(this, "OnPointHistory", roomname, seat, x, y, fast);
        }
    }

    @SimpleEvent
    public void OnRemoveKursi(String roomname, int seatNumber) {
        EventDispatcher.dispatchEvent(this, "OnRemoveKursi",
                roomname != null ? roomname : "",
                seatNumber);
    }

    @SimpleEvent
    public void OnChaRoomReceived(String roomname, String noImageURL, String username, String message, String usernameColor, String chatTextColor) {
        EventDispatcher.dispatchEvent(this, "OnChaRoomReceived",
                roomname != null ? roomname : "",
                noImageURL != null ? noImageURL : "",
                username != null ? username : "",
                message != null ? message : "",
                usernameColor != null ? usernameColor : "",
                chatTextColor != null ? chatTextColor : "");
    }

    @SimpleEvent
    public void OnRoomUserCount(String roomname, int count) {
        EventDispatcher.dispatchEvent(this, "OnRoomUserCount",
                roomname != null ? roomname : "",
                count);
    }

    @SimpleEvent
    public void setAllRoomsFromJson(String json) {
        EventDispatcher.dispatchEvent(this, "setAllRoomsFromJson", json != null ? json : "");
    }

    @SimpleEvent
    public void OnAllJumlahRoom(String roomName, int jumlah) {
        EventDispatcher.dispatchEvent(this, "OnAllJumlahRoom",
                roomName != null ? roomName : "",
                jumlah);
    }

    @SimpleEvent
    public void OnAllUserOnlineList(YailList users) {
        EventDispatcher.dispatchEvent(this, "OnAllUserOnlineList", users != null ? users : YailList.makeEmptyList());
    }

    @SimpleEvent
    public void OnUserOnlineStatus(String userName, boolean online, String tanda) {
        EventDispatcher.dispatchEvent(this, "OnUserOnlineStatus",
                userName != null ? userName : "",
                online,
                tanda != null ? tanda : "");
    }

    @SimpleEvent
    public void OnBgNumberReceived(int number) {
        EventDispatcher.dispatchEvent(this, "OnBgNumberReceived", number);
    }

    @SimpleEvent
    public void OnReceiveNotif(String imageUrl, String username, String deskripsi, long timestamp) {
        EventDispatcher.dispatchEvent(this, "OnReceiveNotif",
                imageUrl != null ? imageUrl : "",
                username != null ? username : "",
                deskripsi != null ? deskripsi : "",
                timestamp);
    }

    @SimpleEvent
    public void OnPrivateFailed(String username, String reason) {
        EventDispatcher.dispatchEvent(this, "OnPrivateFailed",
                username != null ? username : "",
                reason != null ? reason : "");
    }

    @SimpleEvent
    public void OnResetRoom(String roomName) {
        EventDispatcher.dispatchEvent(this, "OnResetRoom", roomName != null ? roomName : "");
    }

    @SimpleEvent
    public void OnOpen() {
        EventDispatcher.dispatchEvent(this, "OnOpen");
    }

    @SimpleEvent
    public void OnJoinroomawal() {
        EventDispatcher.dispatchEvent(this, "OnJoinroomawal");
    }

    @SimpleEvent
    public void OnNeedjoinroom() {
        EventDispatcher.dispatchEvent(this, "OnNeedjoinroom");
    }

    @SimpleEvent
    public void OnAndroidVersionDetected(int sdkVersion) {
        EventDispatcher.dispatchEvent(this, "OnAndroidVersionDetected", sdkVersion);
    }

    @SimpleEvent
    public void OnPingReceived(long timestamp) {
        EventDispatcher.dispatchEvent(this, "OnPingReceived", timestamp);
    }

    @SimpleEvent
    public void OnMaxReconnectAttemptsReached(int attempts) {
        EventDispatcher.dispatchEvent(this, "OnMaxReconnectAttemptsReached", attempts);
    }

    @SimpleEvent
    public void OnReconnectStarted() {
        EventDispatcher.dispatchEvent(this, "OnReconnectStarted");
    }

    @SimpleEvent
    public void OnReconnectStopped() {
        EventDispatcher.dispatchEvent(this, "OnReconnectStopped");
    }

    @SimpleEvent
    public void OnMinimizeStateChanged(boolean minimized) {
        EventDispatcher.dispatchEvent(this, "OnMinimizeStateChanged", minimized);
    }

    @SimpleEvent
    public void OnPulseAnimationStarted() {
        EventDispatcher.dispatchEvent(this, "OnPulseAnimationStarted");
    }

    @SimpleEvent
    public void OnPulseAnimationStopped() {
        EventDispatcher.dispatchEvent(this, "OnPulseAnimationStopped");
    }

    @SimpleEvent
    public void OnNotificationBatchCompleted(int sent, int total) {
        EventDispatcher.dispatchEvent(this, "OnNotificationBatchCompleted", sent, total);
    }

    @SimpleEvent
    public void OnWebSocketFailure(String error) {
        EventDispatcher.dispatchEvent(this, "OnWebSocketFailure", error != null ? error : "");
    }

    @SimpleEvent
    public void OnWebSocketClosed(int code, String reason) {
        EventDispatcher.dispatchEvent(this, "OnWebSocketClosed", code, reason != null ? reason : "");
    }

    @SimpleEvent
    public void OnConnectionLost(String reason) {
        EventDispatcher.dispatchEvent(this, "OnConnectionLost", reason != null ? reason : "");
    }

    @SimpleEvent
    public void OnInRoomStatusReceived(boolean isInRoom) {
        EventDispatcher.dispatchEvent(this, "OnInRoomStatusReceived", isInRoom);
    }

    @SimpleEvent
    public void OnMuteTypeReceived(boolean isMuted, String roomname) {
        EventDispatcher.dispatchEvent(this, "OnMuteTypeReceived", isMuted, roomname != null ? roomname : "");
    }

    @SimpleEvent
    public void OnMuteStatusChanged(boolean isMuted, String roomname) {
        EventDispatcher.dispatchEvent(this, "OnMuteStatusChanged", isMuted, roomname != null ? roomname : "");
    }

    @SimpleEvent
    public void OnRollangakBroadcast(String roomname, String username, int angka) {
        EventDispatcher.dispatchEvent(this, "OnRollangakBroadcast",
                roomname != null ? roomname : "",
                username != null ? username : "",
                angka);
    }

    @SimpleEvent
    public void OnModwarningReceived(String roomName) {
        EventDispatcher.dispatchEvent(this, "OnModwarningReceived",
                roomName != null ? roomName : "");
    }

    @SimpleEvent
    public void OnCleanupCompleted() {
        EventDispatcher.dispatchEvent(this, "OnCleanupCompleted");
    }

    // ==================== GAME EVENTS ====================

    @SimpleEvent
    public void OnGameConnected() {
        EventDispatcher.dispatchEvent(this, "OnGameConnected");
    }

    @SimpleEvent
    public void OnGameDisconnected() {
        EventDispatcher.dispatchEvent(this, "OnGameDisconnected");
    }

    @SimpleEvent
    public void OnGameError(String error) {
        EventDispatcher.dispatchEvent(this, "OnGameError", error != null ? error : "");
    }

    @SimpleEvent
    public void OnGameWait(String message) {
        EventDispatcher.dispatchEvent(this, "OnGameWait", message != null ? message : "");
    }

    @SimpleEvent
    public void OnGameEnded(String message) {
        EventDispatcher.dispatchEvent(this, "OnGameEnded", message != null ? message : "");
    }

    // ==================== GAME LOWCARD EVENTS ====================

    @SimpleEvent
    public void OnGameLowCardStart(int bet) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardStart", bet);
    }

    @SimpleEvent
    public void OnGameLowCardStartSuccess(String hostName, int bet) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardStartSuccess",
                hostName != null ? hostName : "",
                bet);
    }

    @SimpleEvent
    public void OnGameLowCardJoin(String player, int bet) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardJoin",
                player != null ? player : "",
                bet);
    }

    @SimpleEvent
    public void OnGameLowCardNoJoin(String hostName, int bet) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardNoJoin",
                hostName != null ? hostName : "",
                bet);
    }

    @SimpleEvent
    public void OnGameLowCardClosedMessage(String message) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardClosedMessage", message != null ? message : "");
    }

    @SimpleEvent
    public void OnGameLowCardPlayerDraw(String playerId, int number, String tanda) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardPlayerDraw",
                playerId != null ? playerId : "",
                number,
                tanda != null ? tanda : "");
    }

    @SimpleEvent
    public void OnGameLowCardRoundResult(String message) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardRoundResult", message != null ? message : "");
    }

    @SimpleEvent
    public void OnGameLowCardWinner(String winnerId, int totalCoin) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardWinner",
                winnerId != null ? winnerId : "",
                totalCoin);
    }

    @SimpleEvent
    public void OnGameLowCardNextRound(String message) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardNextRound", message != null ? message : "");
    }

    @SimpleEvent
    public void OnGameLowCardTimeLeft(String message) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardTimeLeft", message != null ? message : "");
    }

    @SimpleEvent
    public void OnGameLowCardError(String message) {
        EventDispatcher.dispatchEvent(this, "OnGameLowCardError", message != null ? message : "");
    }

    @SimpleEvent
    public void OnGameStatusReceived(String isRunning) {
        EventDispatcher.dispatchEvent(this, "OnGameStatusReceived", isRunning);
    }

    // ==================== MULTI AKUN EVENTS ====================

    @SimpleEvent
    public void OnMultiJoinSuccess(int seatNumber, String roomName) {
        EventDispatcher.dispatchEvent(this, "OnMultiJoinSuccess", seatNumber, roomName != null ? roomName : "");
    }

    @SimpleEvent
    public void OnMultiSetActiveSuccess(String username, int seatNumber) {
        EventDispatcher.dispatchEvent(this, "OnMultiSetActiveSuccess", username != null ? username : "", seatNumber);
    }

    @SimpleEvent
    public void OnMultiForceExit(String reason) {
        EventDispatcher.dispatchEvent(this, "OnMultiForceExit", reason != null ? reason : "");
    }

    @SimpleEvent
    public void OnUserActiveChanged(String username, int seatNumber) {
        EventDispatcher.dispatchEvent(this, "OnUserActiveChanged", username != null ? username : "", seatNumber);
    }

    @SimpleEvent
    public void OnMultiError(String error) {
        EventDispatcher.dispatchEvent(this, "OnMultiError", error != null ? error : "");
    }

    @SimpleEvent
    public void OnDelayDone() {
        EventDispatcher.dispatchEvent(this, "OnDelayDone");
    }

    // ==================== QUIZ EVENTS ====================

    @SimpleEvent
    public void OnQuizQuestion(String question) {
        EventDispatcher.dispatchEvent(this, "OnQuizQuestion",
                question != null ? question : ""
        );
    }

    @SimpleEvent
    public void OnQuizAnswerResult(String username, String answer, boolean isCorrect, String correctAnswer) {
        EventDispatcher.dispatchEvent(this, "OnQuizAnswerResult",
                username != null ? username : "",
                answer != null ? answer : "",
                isCorrect,
                correctAnswer != null ? correctAnswer : ""
        );
    }

    @SimpleEvent
    public void OnQuizWinner(String username) {
        EventDispatcher.dispatchEvent(this, "OnQuizWinner",
                username != null ? username : ""
        );
    }

    @SimpleEvent
    public void OnQuizNoWinner(String message) {
        EventDispatcher.dispatchEvent(this, "OnQuizNoWinner",
                message != null ? message : ""
        );
    }

    @SimpleEvent
    public void OnQuizError(String error) {
        EventDispatcher.dispatchEvent(this, "OnQuizError",
                error != null ? error : ""
        );
    }
}
