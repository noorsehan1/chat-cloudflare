// ==================== CLIENT QUIZ & GAME ====================

class GameClient {
  constructor() {
    this.ws = null;
    this.username = null;
    this.currentRoom = null;
    this.isConnected = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectDelay = 3000;
    
    // Event handlers
    this.onMessage = null;
    this.onQuizQuestion = null;
    this.onQuizWinner = null;
    this.onGameStart = null;
    this.onGameUpdate = null;
    this.onError = null;
  }

  // ==================== CONNECTION ====================
  
  connect(username) {
    if (!username || username.trim() === '') {
      this._handleError('Username is required');
      return false;
    }
    
    this.username = username.trim();
    
    // Gunakan WebSocket URL yang sesuai
    const wsUrl = `wss://your-server.com/game/ws`;
    // Untuk local: const wsUrl = `ws://localhost:8787/game/ws`;
    
    try {
      this.ws = new WebSocket(wsUrl);
      
      this.ws.onopen = () => {
        this.isConnected = true;
        this.reconnectAttempts = 0;
        console.log('Connected to game server');
        
        // Setelah connect, pindah ke room default
        this.switchRoom('Lobby', this.username);
      };
      
      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          this._handleMessage(data);
        } catch (e) {
          console.error('Error parsing message:', e);
        }
      };
      
      this.ws.onclose = () => {
        this.isConnected = false;
        console.log('Disconnected from game server');
        this._handleReconnect();
      };
      
      this.ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        this._handleError('Connection error');
      };
      
      return true;
    } catch (e) {
      this._handleError('Failed to connect: ' + e.message);
      return false;
    }
  }
  
  _handleReconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      console.log(`Reconnecting... Attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts}`);
      setTimeout(() => {
        if (this.username) {
          this.connect(this.username);
        }
      }, this.reconnectDelay);
    } else {
      this._handleError('Max reconnection attempts reached');
    }
  }

  // ==================== ROOM MANAGEMENT ====================
  
  switchRoom(roomName, username = null) {
    if (!this.isConnected || !this.ws) {
      this._handleError('Not connected to server');
      return;
    }
    
    const user = username || this.username;
    if (!user) {
      this._handleError('Username is required');
      return;
    }
    
    this._send(['switchRoom', roomName, user]);
    this.currentRoom = roomName;
    console.log(`Switching to room: ${roomName}`);
  }
  
  switchToQuizRoom() {
    this.switchRoom('Quiz', this.username);
  }
  
  switchToGameRoom(roomName) {
    this.switchRoom(roomName, this.username);
  }

  // ==================== QUIZ METHODS ====================
  
  submitQuizAnswer(answer) {
    if (!this.isConnected || !this.ws) {
      this._handleError('Not connected to server');
      return;
    }
    
    if (!this.username) {
      this._handleError('Username is required');
      return;
    }
    
    // Validasi answer: A, B, C, D
    const validAnswers = ['A', 'B', 'C', 'D'];
    const answerKey = answer.toUpperCase().trim();
    if (!validAnswers.includes(answerKey)) {
      this._handleError('Invalid answer. Choose A, B, C, or D');
      return;
    }
    
    this._send(['submitQuizAnswer', this.username, answerKey]);
  }
  
  getQuizLeaderboard(limit = 10) {
    if (!this.isConnected || !this.ws) return;
    this._send(['getQuizLeaderboard', limit]);
  }
  
  getQuizUserPoints(username = null) {
    if (!this.isConnected || !this.ws) return;
    const user = username || this.username;
    if (!user) {
      this._handleError('Username is required');
      return;
    }
    this._send(['getQuizUserPoints', user]);
  }
  
  getQuizLastWeekWinner() {
    if (!this.isConnected || !this.ws) return;
    this._send(['getQuizLastWeekWinner']);
  }

  // ==================== GAME METHODS ====================
  
  startGame(bet) {
    if (!this.isConnected || !this.ws) {
      this._handleError('Not connected to server');
      return;
    }
    
    if (!this.username) {
      this._handleError('Username is required');
      return;
    }
    
    if (this.currentRoom === 'Quiz') {
      this._handleError('Cannot start game in Quiz room');
      return;
    }
    
    const betAmount = parseInt(bet) || 0;
    if (betAmount < 0 || (betAmount !== 0 && betAmount < 100) || betAmount > 100000) {
      this._handleError('Invalid bet (0 or 100-100000)');
      return;
    }
    
    this._send(['gameLowCardStart', betAmount.toString(), this.username]);
  }
  
  joinGame() {
    if (!this.isConnected || !this.ws) {
      this._handleError('Not connected to server');
      return;
    }
    
    if (!this.username) {
      this._handleError('Username is required');
      return;
    }
    
    if (this.currentRoom === 'Quiz') {
      this._handleError('Cannot join game in Quiz room');
      return;
    }
    
    this._send(['gameLowCardJoin', this.username]);
  }
  
  submitNumber(number, tanda = '') {
    if (!this.isConnected || !this.ws) {
      this._handleError('Not connected to server');
      return;
    }
    
    if (!this.username) {
      this._handleError('Username is required');
      return;
    }
    
    if (this.currentRoom === 'Quiz') {
      this._handleError('Cannot submit number in Quiz room');
      return;
    }
    
    const num = parseInt(number);
    if (isNaN(num) || num < 1 || num > 12) {
      this._handleError('Invalid number (1-12)');
      return;
    }
    
    const validTandas = ['C1', 'C2', 'C3', 'C4', ''];
    if (!validTandas.includes(tanda)) {
      tanda = '';
    }
    
    this._send(['gameLowCardNumber', num.toString(), tanda, this.username]);
  }
  
  leaveGame() {
    if (!this.isConnected || !this.ws) {
      this._handleError('Not connected to server');
      return;
    }
    
    if (!this.username) {
      this._handleError('Username is required');
      return;
    }
    
    if (this.currentRoom === 'Quiz') {
      this._handleError('Cannot leave game in Quiz room');
      return;
    }
    
    this._send(['gameLowCardLeave', this.username]);
  }
  
  checkGameRunning(roomName = null) {
    if (!this.isConnected || !this.ws) return;
    const room = roomName || this.currentRoom;
    if (!room) {
      this._handleError('Room is required');
      return;
    }
    this._send(['checkGameRunning', room]);
  }

  // ==================== SEND MESSAGE ====================
  
  _send(data) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      this._handleError('WebSocket is not open');
      return;
    }
    
    try {
      this.ws.send(JSON.stringify(data));
    } catch (e) {
      this._handleError('Failed to send message: ' + e.message);
    }
  }

  // ==================== HANDLE MESSAGES ====================
  
  _handleMessage(data) {
    if (!Array.isArray(data) || data.length === 0) return;
    
    const eventType = data[0];
    
    console.log('Received:', eventType, data.slice(1));
    
    switch (eventType) {
      // ===== QUIZ EVENTS =====
      case 'quizQuestion':
        if (this.onQuizQuestion) {
          this.onQuizQuestion(data[1]);
        }
        break;
        
      case 'quizWinner':
        if (this.onQuizWinner) {
          this.onQuizWinner(data[1]);
        }
        break;
        
      case 'quizNoWinner':
        if (this.onQuizNoWinner) {
          this.onQuizNoWinner(data[1]);
        }
        break;
        
      case 'quizAnswerResult':
        if (this.onQuizAnswerResult) {
          this.onQuizAnswerResult(data[1]);
        }
        break;
        
      case 'quizTimeLeft':
        // [quizTimeLeft, message, canType]
        if (this.onQuizTimeLeft) {
          this.onQuizTimeLeft(data[1], data[2]);
        }
        break;
        
      case 'quizError':
        if (this.onQuizError) {
          this.onQuizError(data[1]);
        }
        break;
        
      case 'quizLeaderboard':
        if (this.onQuizLeaderboard) {
          this.onQuizLeaderboard(data[1]);
        }
        break;
        
      case 'quizUserPoints':
        if (this.onQuizUserPoints) {
          this.onQuizUserPoints(data[1], data[2]);
        }
        break;
        
      case 'quizLastWeekWinner':
        if (this.onQuizLastWeekWinner) {
          this.onQuizLastWeekWinner(data[1], data[2], data[3]);
        }
        break;
        
      case 'quizWeekReset':
        if (this.onQuizWeekReset) {
          this.onQuizWeekReset(data[1]);
        }
        break;
      
      // ===== GAME EVENTS =====
      case 'gameLowCardStart':
        if (this.onGameStart) {
          this.onGameStart(data[1], data[2]); // bet, host
        }
        break;
        
      case 'gameLowCardJoin':
        if (this.onGameJoin) {
          this.onGameJoin(data[1], data[2]); // player, bet
        }
        break;
        
      case 'gameLowCardPlayerDraw':
        if (this.onPlayerDraw) {
          this.onPlayerDraw(data[1], data[2], data[3]); // player, number, tanda
        }
        break;
        
      case 'gameLowCardRoundResult':
        if (this.onRoundResult) {
          this.onRoundResult(data[1], data[2], data[3], data[4], data[5]);
        }
        break;
        
      case 'gameLowCardWinner':
        if (this.onGameWinner) {
          this.onGameWinner(data[1], data[2]); // winner, totalCoin
        }
        break;
        
      case 'gameLowCardEnd':
        if (this.onGameEnd) {
          this.onGameEnd(data[1]);
        }
        break;
        
      case 'gameLowCardTimeLeft':
        if (this.onTimeLeft) {
          this.onTimeLeft(data[1]);
        }
        break;
        
      case 'gameLowCardWait':
        if (this.onGameWait) {
          this.onGameWait(data[1]);
        }
        break;
        
      case 'gameLowCardClosed':
        if (this.onGameClosed) {
          this.onGameClosed(data[1]);
        }
        break;
        
      case 'gameLowCardNextRound':
        if (this.onNextRound) {
          this.onNextRound(data[1]);
        }
        break;
        
      case 'gameLowCardPlayerEliminated':
        if (this.onPlayerEliminated) {
          this.onPlayerEliminated(data[1], data[2]);
        }
        break;
        
      case 'gameLowCardInfo':
        if (this.onGameInfo) {
          this.onGameInfo(data[1]);
        }
        break;
        
      case 'gameStatus':
        if (this.onGameStatus) {
          this.onGameStatus(data[1]);
        }
        break;
        
      case 'gameLowCardError':
        if (this.onGameError) {
          this.onGameError(data[1]);
        }
        break;
        
      default:
        console.log('Unknown event:', eventType, data);
    }
  }

  // ==================== ERROR HANDLING ====================
  
  _handleError(message) {
    console.error('Client Error:', message);
    if (this.onError) {
      this.onError(message);
    }
  }

  // ==================== DISCONNECT ====================
  
  disconnect() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.isConnected = false;
    this.username = null;
    this.currentRoom = null;
  }
}

// ==================== USAGE EXAMPLE ====================

// Create client instance
const client = new GameClient();

// Set up event handlers
client.onQuizQuestion = (questionData) => {
  console.log('Quiz Question:', questionData.question);
  console.log('Options:', questionData.options);
  // Tampilkan di UI
};

client.onQuizWinner = (data) => {
  console.log(`🏆 ${data.username} won with ${data.totalPoints} points!`);
  // Tampilkan winner di UI
};

client.onQuizError = (message) => {
  console.log('Quiz Error:', message);
  // Tampilkan error di UI
};

client.onQuizTimeLeft = (message, canType) => {
  console.log('Time left:', message);
  // Update countdown di UI
};

client.onGameStart = (bet, host) => {
  console.log(`Game started! Bet: ${bet}, Host: ${host}`);
  // Tampilkan game UI
};

client.onGameWinner = (winner, totalCoin) => {
  console.log(`🎉 ${winner} wins ${totalCoin} coins!`);
  // Tampilkan winner di UI
};

client.onGameError = (message) => {
  console.log('Game Error:', message);
  // Tampilkan error di UI
};

client.onError = (message) => {
  console.log('Error:', message);
  // Tampilkan error di UI
};

// ==================== CONNECT ====================

// Connect to server
const username = 'Player1';
if (client.connect(username)) {
  console.log('Connected successfully');
}

// ==================== EXAMPLE USAGE ====================

// Switch to Quiz room
setTimeout(() => {
  client.switchToQuizRoom();
}, 1000);

// After 2 seconds, get leaderboard
setTimeout(() => {
  client.getQuizLeaderboard(10);
}, 3000);

// Submit answer (when quiz is running)
setTimeout(() => {
  client.submitQuizAnswer('A');
}, 5000);

// Switch to game room
setTimeout(() => {
  client.switchToGameRoom('Room1');
}, 8000);

// Start game
setTimeout(() => {
  client.startGame(1000);
}, 9000);

// Join game
setTimeout(() => {
  client.joinGame();
}, 10000);

// Submit number
setTimeout(() => {
  client.submitNumber(5, 'C1');
}, 12000);

// ==================== HTML UI EXAMPLE ====================

/*
  <div id="app">
    <div id="status">Disconnected</div>
    <input id="username" placeholder="Username" value="Player1">
    <button onclick="connect()">Connect</button>
    
    <div id="quiz-section">
      <h3>Quiz Room</h3>
      <div id="question">Waiting for question...</div>
      <div id="options"></div>
      <div id="time-left"></div>
      <button onclick="submitAnswer('A')">A</button>
      <button onclick="submitAnswer('B')">B</button>
      <button onclick="submitAnswer('C')">C</button>
      <button onclick="submitAnswer('D')">D</button>
    </div>
    
    <div id="game-section">
      <h3>Game Room</h3>
      <input id="bet" placeholder="Bet" value="1000">
      <button onclick="startGame()">Start Game</button>
      <button onclick="joinGame()">Join Game</button>
      <input id="number" placeholder="Number (1-12)" type="number">
      <button onclick="submitNumber()">Submit</button>
      <div id="game-status"></div>
    </div>
  </div>

  <script>
    function connect() {
      const username = document.getElementById('username').value;
      if (client.connect(username)) {
        document.getElementById('status').textContent = 'Connected';
      }
    }
    
    function submitAnswer(answer) {
      client.submitQuizAnswer(answer);
    }
    
    function startGame() {
      const bet = document.getElementById('bet').value;
      client.startGame(bet);
    }
    
    function joinGame() {
      client.joinGame();
    }
    
    function submitNumber() {
      const number = document.getElementById('number').value;
      client.submitNumber(number);
    }
  </script>
*/
