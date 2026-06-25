// Client-side JavaScript
class GameClient {
  constructor() {
    this.ws = null;
    this.room = null;
    this.username = null;
    this.playerListInterval = null;
  }

  connect(room, username) {
    this.room = room;
    this.username = username;
    
    // Connect to WebSocket
    this.ws = new WebSocket('wss://your-server.com/game/ws');
    
    this.ws.onopen = () => {
      console.log('Connected to server');
      // Switch to room
      this.send(['switchRoom', room]);
      // Request initial player list
      setTimeout(() => {
        this.requestPlayerList();
      }, 500);
    };
    
    this.ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      this.handleMessage(data);
    };
    
    this.ws.onclose = () => {
      console.log('Disconnected');
      this.stopAutoRefresh();
    };
    
    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
  }

  send(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    }
  }

  handleMessage(data) {
    const eventType = data[0];
    
    switch(eventType) {
      case 'gameLowCardPlayerList':
        this.updatePlayerList(data[1]);
        break;
        
      case 'gameLowCardStart':
        console.log('Game started with bet:', data[1]);
        break;
        
      case 'gameLowCardJoin':
        console.log('Player joined:', data[1]);
        break;
        
      case 'gameLowCardPlayerDraw':
        console.log('Player drew:', data[1], data[2], data[3]);
        break;
        
      case 'gameLowCardRoundResult':
        console.log('Round result:', data);
        break;
        
      case 'gameLowCardWinner':
        console.log('Winner:', data[1], 'won', data[2], 'coins');
        break;
        
      case 'gameLowCardTimeLeft':
        console.log('Time left:', data[1]);
        break;
        
      default:
        console.log('Unknown event:', eventType, data);
    }
  }

  updatePlayerList(playerData) {
    // Update UI with player list
    const playersContainer = document.getElementById('players-list');
    if (!playersContainer) return;
    
    let html = `
      <div class="game-status">
        <p>Phase: ${playerData.phase}</p>
        <p>Round: ${playerData.round}</p>
        <p>Bet: ${playerData.betAmount}</p>
        <p>Active Players: ${playerData.activeCount}/${playerData.totalPlayers}</p>
        ${playerData.registrationOpen ? '<p>Registration Open!</p>' : ''}
      </div>
      <div class="active-players">
        <h3>Active Players (${playerData.activeCount})</h3>
    `;
    
    // Active players
    playerData.activePlayers.forEach(player => {
      html += `
        <div class="player-item active" data-player-id="${player.id}">
          <span class="player-name">${player.isBot ? '🤖 ' : '👤 '} ${player.name}</span>
          ${player.isHost ? '<span class="host-badge">Host</span>' : ''}
          ${player.hasSubmitted ? '<span class="submitted-badge">✓ Submitted</span>' : ''}
          ${player.number ? `<span class="card-number">Card: ${player.number}${player.tanda ? ' ('+player.tanda+')' : ''}</span>` : ''}
        </div>
      `;
    });
    
    html += '</div>';
    
    // Eliminated players
    if (playerData.eliminatedPlayers.length > 0) {
      html += `
        <div class="eliminated-players">
          <h3>Eliminated Players (${playerData.eliminatedCount})</h3>
      `;
      
      playerData.eliminatedPlayers.forEach(player => {
        html += `
          <div class="player-item eliminated" data-player-id="${player.id}">
            <span class="player-name">${player.isBot ? '🤖 ' : '👤 '} ${player.name}</span>
            <span class="eliminated-badge">✕ Eliminated</span>
          </div>
        `;
      });
      
      html += '</div>';
    }
    
    playersContainer.innerHTML = html;
  }

  requestPlayerList(room) {
    this.send(['gameLowCardGetPlayers', room || this.room]);
  }

  startAutoRefresh(interval = 3000) {
    this.stopAutoRefresh();
    this.playerListInterval = setInterval(() => {
      this.requestPlayerList();
    }, interval);
  }

  stopAutoRefresh() {
    if (this.playerListInterval) {
      clearInterval(this.playerListInterval);
      this.playerListInterval = null;
    }
  }

  startGame(bet) {
    this.send(['gameLowCardStart', bet, this.username]);
  }

  joinGame() {
    this.send(['gameLowCardJoin', this.username]);
  }

  submitNumber(number, tanda) {
    this.send(['gameLowCardNumber', number, tanda || '', this.username]);
  }

  leaveGame() {
    this.send(['gameLowCardLeave', this.username]);
  }
}

// Usage
const gameClient = new GameClient();
gameClient.connect('room1', 'player1');
gameClient.startAutoRefresh(3000); // Auto refresh every 3 seconds
