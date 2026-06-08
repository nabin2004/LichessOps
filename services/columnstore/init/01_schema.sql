-- Lichess analytics star schema + inference tables (MariaDB ColumnStore).
CREATE DATABASE IF NOT EXISTS lichess_analytics;
USE lichess_analytics;

CREATE USER IF NOT EXISTS 'lichess'@'%' IDENTIFIED BY 'Lichess_Analytics1!';
GRANT ALL PRIVILEGES ON lichess_analytics.* TO 'lichess'@'%';
FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS fact_games (
    game_id VARCHAR(32) NOT NULL,
    white_player_id VARCHAR(32),
    black_player_id VARCHAR(32),
    opening_id VARCHAR(32),
    date_id INT,
    result VARCHAR(16),
    white_elo INT,
    black_elo INT,
    white_rating_diff INT,
    black_rating_diff INT,
    time_control VARCHAR(64),
    termination VARCHAR(64),
    event VARCHAR(255),
    utc_datetime VARCHAR(32),
    move_count INT,
    year INT NOT NULL,
    month INT NOT NULL
) ENGINE=Columnstore;

CREATE TABLE IF NOT EXISTS dim_player (
    player_id VARCHAR(32) NOT NULL,
    username VARCHAR(255),
    title VARCHAR(16),
    last_known_elo INT
) ENGINE=Columnstore;

CREATE TABLE IF NOT EXISTS dim_opening (
    opening_id VARCHAR(32) NOT NULL,
    eco VARCHAR(16),
    opening_name VARCHAR(512)
) ENGINE=Columnstore;

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT NOT NULL,
    calendar_date DATE,
    year INT,
    month INT,
    day_of_week INT
) ENGINE=Columnstore;

CREATE TABLE IF NOT EXISTS prediction_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    player_elo INT NOT NULL,
    opponent_elo INT NOT NULL,
    player_color VARCHAR(8),
    eco VARCHAR(16),
    game_type VARCHAR(32),
    predicted_outcome VARCHAR(8) NOT NULL,
    prediction INT,
    prob_lose DOUBLE,
    prob_win DOUBLE,
    prob_draw DOUBLE,
    probabilities LONGTEXT,
    model_uri VARCHAR(512),
    source VARCHAR(32) NOT NULL DEFAULT 'serving',
    inferred_at DATETIME(6) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS batch_predictions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(128) NOT NULL,
    month VARCHAR(7) NOT NULL,
    y_true INT NOT NULL,
    y_pred INT NOT NULL,
    pred_display VARCHAR(8),
    prob_lose DOUBLE,
    prob_win DOUBLE,
    prob_draw DOUBLE,
    player_elo INT,
    opponent_elo INT,
    eco VARCHAR(16),
    game_type VARCHAR(32),
    model_uri VARCHAR(512),
    source VARCHAR(32) NOT NULL DEFAULT 'evaluate',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inference_runs (
    run_id VARCHAR(128) NOT NULL PRIMARY KEY,
    month VARCHAR(7) NOT NULL,
    model_uri VARCHAR(512),
    source VARCHAR(32) NOT NULL,
    row_count INT NOT NULL DEFAULT 0,
    metrics_json LONGTEXT,
    started_at DATETIME(6) NOT NULL,
    completed_at DATETIME(6)
) ENGINE=InnoDB;
