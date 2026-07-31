_clearDiceData() {
  try {
    if (this._diceTimeUpCooldownTimer) {
      clearInterval(this._diceTimeUpCooldownTimer);
      this._diceTimeUpCooldownTimer = null;
    }
    
    this.currentDiceRoll = null;
    this._diceStartTime = null;
    this.diceAnswered = new Set();
    this.diceHasWinner = false;
    this.diceWinner = null;
    this._isShowingDice = false;
    this._winnerProcessed = false;
    this._canSubmitDiceAnswer = false;
    this._diceQuestionStartTime = null;
    this._diceOutOfTimeShown = false;
    this._diceRemainingShown = false;
    this._diceTimeUpShown = false;
    this._lastSentRemaining = -1;
    this._diceTimeUpCooldown = false;
    this._diceNotifiedFlags = {
      20: false,
      10: false,
      timeup: false
    };
    
    this._stopDiceTimerNotifications();
    
    if (this._diceTimeout) {
      clearTimeout(this._diceTimeout);
      this._diceTimeout = null;
    }
    if (this._diceBreakTimeout) {
      clearTimeout(this._diceBreakTimeout);
      this._diceBreakTimeout = null;
    }
    if (this._diceStartTimeout) {
      clearTimeout(this._diceStartTimeout);
      this._diceStartTimeout = null;
    }
    
    this._broadcastDiceNotification("diceError", {
      message: "Dice game has ended.",
      remaining: -1,
      clearUI: true
    });
  } catch(e) {}
}
