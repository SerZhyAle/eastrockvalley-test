CREATE SCHEMA IF NOT EXISTS gear;

USE gear;

-- tables
-- _________________________________________________________________ --

DROP TABLE IF EXISTS gear.users;

DROP TABLE IF EXISTS gear.temp_games CASCADE;
CREATE TABLE IF NOT EXISTS gear.temp_games
(
    game_id     int         NOT NULL COMMENT 'GameID -int',
    user_id     int         NOT NULL COMMENT 'UserID -int',
    game_type   varchar(50) COMMENT 'GameType -varchar(50)',
    play_date   date        NOT NULL COMMENT 'PlayDate -date',
    duration    int COMMENT 'Duration -int',
    imported_at datetime(6) NOT NULL DEFAULT NOW(6) COMMENT 'Imported -datetime(6)'
) COMMENT 'Games original from S3';
CREATE INDEX index_temp_games_user_id ON gear.temp_games (user_id);
CREATE INDEX index_temp_games_game_id ON gear.temp_games (game_id);

-- _________________________________________________________________ --
DROP TABLE IF EXISTS gear.games;
DROP TABLE IF EXISTS gear.recent_changes;
CREATE TABLE IF NOT EXISTS gear.recent_changes
(
    data_type   char(50)     NOT NULL COMMENT 'Type of stored/recently changed entity element -char(50)',
    -- Variants: user, game, payment, 'user-game-date'
    element_id  int UNSIGNED NOT NULL COMMENT 'Id of stored/recently changed entity element -int',
    data_status enum ('new',
        'changed',
        'deleted')           NOT NULL DEFAULT 'new' COMMENT 'Status of stored/recently changed entity element -enum',
    stored_at   datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Stored/changed -datetime(6)'
) COMMENT 'Inserted recently changed element triggering us to recalculate in aggregations';

CREATE UNIQUE INDEX uniq_index_recent_changes_type_id
    ON gear.recent_changes (data_type, element_id);

CREATE INDEX index_recent_changes_id
    ON gear.recent_changes (element_id);

CREATE INDEX index_recent_changes_status_id
    ON gear.recent_changes (data_status, element_id);

-- _________________________________________________________________ --
DROP TABLE IF EXISTS gear.process_staging_logs;
CREATE TABLE gear.process_staging_logs
(
    table_name      varchar(255) NOT NULL COMMENT 'Table name varchar(255)',
    processed_count int          NULL COMMENT 'Count of processed rows -int',
    imported_at     datetime(6)  NOT NULL COMMENT 'Processed -datetime(6)',
    processed_at    datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Processed -datetime(6)'
) COMMENT 'Log of result of processing Staging -> DataVault';
