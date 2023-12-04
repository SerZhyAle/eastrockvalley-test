CREATE SCHEMA IF NOT EXISTS vault;
-- DROP SCHEMA IF EXISTS vault;
USE vault;

DROP TABLE IF EXISTS vault.h_game_types CASCADE;
CREATE TABLE vault.h_game_types
(
    id         tinyint UNSIGNED AUTO_INCREMENT
                           NOT NULL PRIMARY KEY COMMENT 'Game Type ID -tinyint',
    name       varchar(50) NOT NULL COMMENT 'GameType Name -varchar(50)',
    created_at datetime(6) NOT NULL DEFAULT NOW(6) COMMENT 'Created -datetime(6)',
    updated_at datetime(6) NULL     DEFAULT NULL ON UPDATE NOW(6) COMMENT 'Updated -datetime(6)',
    UNIQUE INDEX (name)

) COMMENT 'Game Types catalog';



DROP TABLE IF EXISTS vault.h_games CASCADE;
CREATE TABLE vault.h_games
(
    id         int UNSIGNED     NOT NULL PRIMARY KEY COMMENT 'Game ID -int',
    type_id    tinyint UNSIGNED NOT NULL COMMENT 'GameType ID  -tinyint',
    created_at datetime(6)      NOT NULL DEFAULT NOW(6) COMMENT 'Created -datetime(6)',
    updated_at datetime(6)      NULL     DEFAULT NULL ON UPDATE NOW(6) COMMENT 'Updated -datetime(6)',
    FOREIGN KEY (type_id) REFERENCES vault.h_game_types (id) ON DELETE CASCADE ON UPDATE CASCADE

) COMMENT 'Games catalog';

DROP TABLE IF EXISTS vault.h_countries CASCADE;
CREATE TABLE vault.h_countries
(
    id         smallint UNSIGNED AUTO_INCREMENT
                            NOT NULL PRIMARY KEY COMMENT 'Country ID -smallint',
    name       varchar(150) NOT NULL COMMENT 'Country Name -varchar(150)',
    created_at datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Created -datetime(6)',
    updated_at datetime(6)  NULL     DEFAULT NULL ON UPDATE NOW(6) COMMENT 'Updated -datetime(6)',
    UNIQUE INDEX (name)

) COMMENT 'Countries catalog';

DROP TABLE IF EXISTS vault.h_users CASCADE;
CREATE TABLE vault.h_users
(
    id          int UNSIGNED      NOT NULL PRIMARY KEY COMMENT 'User ID -int',
    name        varchar(150) COMMENT 'User Name -varchar(150)',
    signup_date date              NOT NULL COMMENT 'Sign up date -date',
    country_id  smallint UNSIGNED NOT NULL COMMENT 'Country ID -smallint',
    created_at  datetime(6)       NOT NULL DEFAULT NOW(6) COMMENT 'Created -datetime(6)',
    updated_at  datetime(6)       NULL     DEFAULT NULL ON UPDATE NOW(6) COMMENT 'Updated -datetime(6)',
    FOREIGN KEY (country_id) REFERENCES vault.h_countries (id) ON DELETE CASCADE ON UPDATE CASCADE

) COMMENT 'Users catalog';


DROP TABLE IF EXISTS vault.l_users_games_date CASCADE;
CREATE TABLE vault.l_users_games_date
(
    id         int UNSIGNED AUTO_INCREMENT
                            NOT NULL PRIMARY KEY COMMENT 'ID -int',
    user_id    int UNSIGNED NOT NULL COMMENT 'User ID -int',
    game_id    int UNSIGNED NOT NULL COMMENT 'Game ID -int',
    play_date  date COMMENT 'Play Date -date',
    created_at datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Created -datetime(6)',
    FOREIGN KEY (user_id) REFERENCES vault.h_users (id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (game_id) REFERENCES vault.h_games (id) ON DELETE CASCADE ON UPDATE CASCADE

) COMMENT 'Links: Users - Games - Dates';

CREATE UNIQUE INDEX uniq_index_l_users_games_date
    ON vault.l_users_games_date (user_id, game_id, play_date);


DROP TABLE IF EXISTS vault.s_users_games_date CASCADE;
CREATE TABLE vault.s_users_games_date
(
    l_users_games_date_id int UNSIGNED NOT NULL PRIMARY KEY COMMENT 'ID -int',
    duration              int          NOT NULL COMMENT 'Duration -int',
    created_at            datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Created -datetime(6)',
    updated_at            datetime(6)  NULL     DEFAULT NULL ON UPDATE NOW(6) COMMENT 'Updated -datetime(6)',
    FOREIGN KEY (l_users_games_date_id) REFERENCES vault.l_users_games_date (id) ON DELETE CASCADE ON UPDATE CASCADE

) COMMENT 'Satellite: Users - Games - Dates';

DROP TABLE IF EXISTS vault.payments CASCADE;
CREATE TABLE IF NOT EXISTS vault.payments
(
    transaction_id   bigint UNSIGNED NOT NULL PRIMARY KEY COMMENT 'TransactionID -bigint',
    user_id          int UNSIGNED    NOT NULL COMMENT 'UserID -int',
    amount           dec(13, 2)      NULL COMMENT 'Amount -dec(13,2)',
    transaction_date date            NOT NULL COMMENT 'TransactionDate --date',
    type             enum ('Deposit',
        'Withdrawal',
        'Correction')                NOT NULL COMMENT 'Type --enum',
    created_at       datetime(6)     NOT NULL DEFAULT NOW(6) COMMENT 'Created -datetime(6)',
    updated_at       datetime(6)     NULL     DEFAULT NULL ON UPDATE NOW(6) COMMENT 'Updated -datetime(6)',
    FOREIGN KEY (user_id) REFERENCES vault.h_users (id) ON DELETE CASCADE ON UPDATE CASCADE

) COMMENT 'Payments original from S3';
