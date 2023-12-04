CREATE SCHEMA IF NOT EXISTS bulk;

USE bulk;

DROP TABLE IF EXISTS processed_games CASCADE;
CREATE TABLE IF NOT EXISTS processed_games
(
    game_id      int UNSIGNED NOT NULL COMMENT 'GameID -int',
    user_id      int UNSIGNED NOT NULL COMMENT 'UserID -int',
    game_type    varchar(50) COMMENT 'GameType -varchar(50)',
    play_date    date         NOT NULL COMMENT 'PlayDate -date',
    duration     int COMMENT 'Duration -int',
    imported_at  datetime(6)  NOT NULL COMMENT 'Imported -datetime(6)',
    processed_at datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Processed -datetime(6)'
) COMMENT 'processed Games original from S3';

DROP TABLE IF EXISTS processed_payments CASCADE;
CREATE TABLE IF NOT EXISTS processed_payments
(
    transaction_id   bigint UNSIGNED NOT NULL COMMENT 'TransactionID -bigint',
    user_id          int UNSIGNED    NOT NULL COMMENT 'UserID -int',
    amount           dec(13, 2)      NULL COMMENT 'Amount -dec(13,2)',
    transaction_date date            NOT NULL COMMENT 'TransactionDate --date',
    type             enum ('Deposit',
        'Withdrawal',
        'Correction')                NOT NULL COMMENT 'Type --enum',
    imported_at      datetime(6)     NOT NULL DEFAULT NOW(6) COMMENT 'Imported -datetime(6)',
    processed_at     datetime(6)     NOT NULL DEFAULT NOW(6) COMMENT 'Processed -datetime(6)'
) COMMENT 'processed Payments original from S3';

DROP TABLE IF EXISTS processed_users CASCADE;
CREATE TABLE IF NOT EXISTS processed_users
(
    user_id      int UNSIGNED NOT NULL COMMENT 'UserID -int',
    user_name    varchar(255) NOT NULL COMMENT 'UserName varchar(255)',
    signup_date  date         NOT NULL COMMENT 'SignUpDate -date',
    country      varchar(100) NULL COMMENT 'Country -varchar(100)',
    imported_at  datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Imported -datetime(6)',
    processed_at datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Processed -datetime(6)'
) COMMENT 'processed Users original from S3';
