// ==================== QUIZ HANDLER DI CLIENT JAVA ====================

private void handleQuizMessage(JSONArray data) {
    try {
        String event = data.optString(0, "");
        
        switch (event) {
            case "quizQuestion": {
                JSONObject questionObj = data.optJSONObject(1);
                if (questionObj == null) return;
                
                String question = cleanHtmlEntities(questionObj.optString("question", ""));
                JSONObject options = questionObj.optJSONObject("options");
                int timeLimit = questionObj.optInt("timeLimit", 20);
                
                final String fQuestion = question;
                final JSONObject fOptions = options;
                final int fTimeLimit = timeLimit;
                
                mainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        OnQuizQuestion(fQuestion, fOptions, fTimeLimit);
                    }
                });
                break;
            }
            
            case "quizWinner": {
                // ✅ MENANGANI STRING LANGSUNG (DARI SERVER)
                String username = "";
                try {
                    // Coba ambil sebagai string langsung
                    username = data.optString(1, "");
                    if (username.isEmpty()) {
                        // Fallback: coba sebagai JSON object
                        JSONObject winnerObj = data.optJSONObject(1);
                        if (winnerObj != null) {
                            username = winnerObj.optString("username", "");
                        }
                    }
                } catch (Exception e) {
                    username = "Unknown Winner";
                }
                
                if (username.isEmpty()) {
                    username = "Unknown Winner";
                }
                
                final String fUsername = cleanHtmlEntities(username);
                mainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        OnQuizWinner(fUsername);
                    }
                });
                break;
            }
            
            case "quizNoWinner": {
                // ✅ MENANGANI STRING LANGSUNG (DARI SERVER)
                String message = "";
                try {
                    // Coba ambil sebagai string langsung
                    message = data.optString(1, "");
                    if (message.isEmpty()) {
                        // Fallback: coba sebagai JSON object
                        JSONObject noWinnerObj = data.optJSONObject(1);
                        if (noWinnerObj != null) {
                            message = noWinnerObj.optString("message", "");
                        }
                    }
                } catch (Exception e) {
                    message = "No one answered correctly this round!";
                }
                
                if (message.isEmpty()) {
                    message = "No one answered correctly this round!";
                }
                
                final String fMessage = cleanHtmlEntities(message);
                mainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        OnQuizNoWinner(fMessage);
                    }
                });
                break;
            }
            
            case "quizAnswerResult": {
                JSONObject resultObj = data.optJSONObject(1);
                if (resultObj == null) return;
                
                String username = cleanHtmlEntities(resultObj.optString("username", ""));
                String answer = resultObj.optString("answer", "?");
                boolean isCorrect = resultObj.optBoolean("isCorrect", false);
                String correctAnswer = resultObj.optString("correctAnswer", "");
                
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
            
            case "quizInfo": {
                String info = cleanHtmlEntities(data.optString(1, ""));
                final String fInfo = info;
                mainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        OnQuizInfo(fInfo);
                    }
                });
                break;
            }
            
            case "quizError": {
                String error = cleanHtmlEntities(data.optString(1, "Unknown error"));
                final String fError = error;
                mainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        OnQuizError(fError);
                    }
                });
                break;
            }
            
            case "forceStartQuizResult": {
                JSONObject result = data.optJSONObject(1);
                if (result == null) return;
                boolean success = result.optBoolean("success", false);
                String message = result.optString("message", "");
                int totalQuestions = result.optInt("questions", 0);
                
                final boolean fSuccess = success;
                final String fMessage = cleanHtmlEntities(message);
                final int fTotalQuestions = totalQuestions;
                
                mainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        OnForceStartQuizResult(fSuccess, fMessage, fTotalQuestions);
                    }
                });
                break;
            }
            
            case "reloadQuestionsResult": {
                JSONObject result = data.optJSONObject(1);
                if (result == null) return;
                boolean success = result.optBoolean("success", false);
                int total = result.optInt("total", 0);
                
                final boolean fSuccess = success;
                final int fTotal = total;
                
                mainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        OnReloadQuestionsResult(fSuccess, fTotal);
                    }
                });
                break;
            }
        }
    } catch (Exception e) {
        Log.e("QuizHandler", "Error handling quiz message: " + e.getMessage());
    }
}

// ==================== QUIZ CALLBACK METHODS ====================

private void OnQuizQuestion(String question, JSONObject options, int timeLimit) {
    // Tampilkan pertanyaan dan pilihan
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            quizQuestionTextView.setText(question);
            
            String optionA = options.optString("A", "");
            String optionB = options.optString("B", "");
            String optionC = options.optString("C", "");
            String optionD = options.optString("D", "");
            
            quizOptionA.setText("A. " + optionA);
            quizOptionB.setText("B. " + optionB);
            quizOptionC.setText("C. " + optionC);
            quizOptionD.setText("D. " + optionD);
            
            // Reset status
            quizAnswered = false;
            quizTimerCountDown = timeLimit;
            quizTimer.setText("Time: " + timeLimit + "s");
            
            // Start timer
            startQuizTimer();
        }
    });
}

private void OnQuizWinner(String username) {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            quizResultTextView.setText("🏆 Winner: " + username + "!");
            quizResultTextView.setVisibility(View.VISIBLE);
            quizAnswered = true;
            stopQuizTimer();
        }
    });
}

private void OnQuizNoWinner(String message) {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            quizResultTextView.setText("😔 " + message);
            quizResultTextView.setVisibility(View.VISIBLE);
            quizAnswered = true;
            stopQuizTimer();
        }
    });
}

private void OnQuizAnswerResult(String username, String answer, boolean isCorrect, String correctAnswer) {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            String status = isCorrect ? "✅ Correct!" : "❌ Wrong!";
            String text = username + " chose " + answer + " - " + status;
            if (!isCorrect) {
                text += " (Correct: " + correctAnswer + ")";
            }
            // Tampilkan di log atau UI
            Log.d("Quiz", text);
        }
    });
}

private void OnQuizInfo(String info) {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            Toast.makeText(getApplicationContext(), info, Toast.LENGTH_LONG).show();
        }
    });
}

private void OnQuizError(String error) {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            Toast.makeText(getApplicationContext(), "Quiz Error: " + error, Toast.LENGTH_LONG).show();
        }
    });
}

private void OnForceStartQuizResult(boolean success, String message, int totalQuestions) {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            if (success) {
                Toast.makeText(getApplicationContext(), 
                    "Quiz started! " + totalQuestions + " questions available.", 
                    Toast.LENGTH_LONG).show();
            } else {
                Toast.makeText(getApplicationContext(), 
                    "Failed to start quiz: " + message, 
                    Toast.LENGTH_LONG).show();
            }
        }
    });
}

private void OnReloadQuestionsResult(boolean success, int total) {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            if (success) {
                Toast.makeText(getApplicationContext(), 
                    "Questions reloaded! Total: " + total, 
                    Toast.LENGTH_LONG).show();
            } else {
                Toast.makeText(getApplicationContext(), 
                    "Failed to reload questions!", 
                    Toast.LENGTH_LONG).show();
            }
        }
    });
}

// ==================== QUIZ TIMER ====================

private Handler quizTimerHandler = new Handler();
private Runnable quizTimerRunnable;
private int quizTimerCountDown = 20;
private boolean quizAnswered = false;

private void startQuizTimer() {
    stopQuizTimer();
    quizTimerCountDown = 20;
    quizAnswered = false;
    
    quizTimerRunnable = new Runnable() {
        @Override
        public void run() {
            if (quizAnswered) return;
            
            quizTimerCountDown--;
            quizTimer.setText("Time: " + quizTimerCountDown + "s");
            
            if (quizTimerCountDown <= 0) {
                quizTimer.setText("Time: 0s");
                // Time's up, disable buttons
                disableQuizButtons();
                quizAnswered = true;
            } else {
                quizTimerHandler.postDelayed(this, 1000);
            }
        }
    };
    
    quizTimerHandler.postDelayed(quizTimerRunnable, 1000);
}

private void stopQuizTimer() {
    if (quizTimerRunnable != null) {
        quizTimerHandler.removeCallbacks(quizTimerRunnable);
        quizTimerRunnable = null;
    }
}

private void disableQuizButtons() {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            quizOptionA.setEnabled(false);
            quizOptionB.setEnabled(false);
            quizOptionC.setEnabled(false);
            quizOptionD.setEnabled(false);
        }
    });
}

private void enableQuizButtons() {
    runOnUiThread(new Runnable() {
        @Override
        public void run() {
            quizOptionA.setEnabled(true);
            quizOptionB.setEnabled(true);
            quizOptionC.setEnabled(true);
            quizOptionD.setEnabled(true);
        }
    });
}
