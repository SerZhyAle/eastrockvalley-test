USE bus;

-- USERS

DROP TABLE IF EXISTS bus.daily_users;
CREATE TABLE bus.daily_users
(
    date              date NOT NULL COMMENT 'Date',
    user_id           int  NOT NULL COMMENT 'User ID',
    user              varchar(150) COMMENT 'User Name',
    active_games      int COMMENT 'Number of active games',
    active_game_types int COMMENT 'Number of active games Types',
    transactions      int COMMENT 'Number of transactions',
    deposit           dec(13, 2) COMMENT 'Deposit Amount',
    deposits          int COMMENT 'Number of Deposits',
    withdrawal        dec(13, 2) COMMENT 'Withdrawal Amount',
    withdrawals       int COMMENT 'Number of Withdrawals',
    duration          bigint COMMENT 'Duration'
);

CREATE UNIQUE INDEX uniq_i_daily_users
    ON bus.daily_users (date, user_id);

SELECT *
FROM daily_users;


DROP TABLE IF EXISTS bus.total_users;
CREATE TABLE bus.total_users
(
    user_id                   int NOT NULL COMMENT 'User ID',
    user                      varchar(150) COMMENT 'User Name',
    dates                     int COMMENT 'Number of dates',
    first_date                date COMMENT 'First date',
    last_date                 date COMMENT 'Last date',
    active_games              int COMMENT 'Number of active games',
    middle_games_daily        dec(10, 3) COMMENT 'Middle games daily',
    active_game_types         int COMMENT 'Number of active game types',
    middle_game_types_daily   dec(10, 3) COMMENT 'Middle game types daily',
    transactions              int COMMENT 'Number of transactions',
    middle_transactions_daily dec(10, 3) COMMENT 'Middle transactions daily',
    deposit                   dec(15, 2) COMMENT 'Deposit Amount',
    middle_deposit_daily      dec(13, 3) COMMENT 'Middle deposit daily',
    deposits                  int COMMENT 'Number of Deposits',
    middle_deposits_daily     dec(13, 3) COMMENT 'Middle deposits daily',
    withdrawal                dec(15, 2) COMMENT 'Withdrawal Amount',
    middle_withdrawal_daily   dec(13, 3) COMMENT 'Middle withdrawal daily',
    withdrawals               int COMMENT 'Number of Withdrawals',
    middle_withdrawals_daily  dec(13, 3) COMMENT 'Middle withdrawals daily',
    duration                  bigint COMMENT 'Duration',
    middle_duration_daily     dec(13, 3) COMMENT 'Middle duration daily'
);

CREATE UNIQUE INDEX uniq_i_total_users
    ON bus.total_users (user_id);

SELECT *
FROM total_users;

-- _________________________________________________________________ --


-- COUNTRIES

DROP TABLE IF EXISTS bus.daily_countries;
CREATE TABLE bus.daily_countries
(
    date         date     NOT NULL COMMENT 'Date',
    country_id   smallint NOT NULL COMMENT 'Country ID',
    country      varchar(150) COMMENT 'Country Name',
    active_users int COMMENT 'Number of active users',
    active_games int COMMENT 'Number of active games',
    sign_ups     int COMMENT 'Sign-Up users',
    transactions int COMMENT 'Number of transactions',
    deposit      dec(13, 2) COMMENT 'Deposit Amount',
    deposits     int COMMENT 'Number of Deposits',
    withdrawal   dec(13, 2) COMMENT 'Withdrawal Amount',
    withdrawals  int COMMENT 'Number of Withdrawals',
    duration     bigint COMMENT 'Duration'
);

CREATE UNIQUE INDEX uniq_i_daily_countries
    ON bus.daily_countries (date, country_id);

SELECT *
FROM daily_countries dc;

-- _________________________________________________________________ --

DROP TABLE IF EXISTS bus.total_countries;
CREATE TABLE bus.total_countries
(
    country_id                smallint NOT NULL COMMENT 'Country ID',
    country                   varchar(150) COMMENT 'Country Name',
    dates                     int COMMENT 'Number of dates',
    first_date                date COMMENT 'First date',
    last_date                 date COMMENT 'Last date',
    active_users              int COMMENT 'Number of active users',
    middle_users_daily        dec(10, 3) COMMENT 'Middle users daily',
    active_games              int COMMENT 'Number of active games',
    middle_games_daily        dec(10, 3) COMMENT 'Middle games daily',
    sign_ups                  int COMMENT 'Sign-Up users',
    middle_sign_ups_daily     dec(10, 3) COMMENT 'Middle sign ups daily',
    transactions              int COMMENT 'Number of transactions',
    middle_transactions_daily dec(10, 3) COMMENT 'Middle transactions daily',
    deposit                   dec(15, 2) COMMENT 'Deposit Amount',
    middle_deposit_daily      dec(13, 3) COMMENT 'Middle deposit daily',
    deposits                  int COMMENT 'Number of Deposits',
    middle_deposits_daily     dec(13, 3) COMMENT 'Middle deposits daily',
    withdrawal                dec(15, 2) COMMENT 'Withdrawal Amount',
    middle_withdrawal_daily   dec(13, 3) COMMENT 'Middle withdrawal daily',
    withdrawals               int COMMENT 'Number of Withdrawals',
    middle_withdrawals_daily  dec(13, 3) COMMENT 'Middle withdrawals daily',
    duration                  bigint COMMENT 'Duration',
    middle_duration_daily     dec(13, 3) COMMENT 'Middle duration daily'
);

CREATE UNIQUE INDEX uniq_i_total_countries
    ON bus.total_countries (country_id);

SELECT *
FROM total_countries dc;

-- _________________________________________________________________ --

-- GAMES

DROP TABLE IF EXISTS bus.daily_games;
CREATE TABLE bus.daily_games
(
    date             date         NOT NULL COMMENT 'Date',
    game_id          smallint     NOT NULL COMMENT 'Game ID',
    game_type        varchar(100) NOT NULL COMMENT 'Game Type',
    active_users     int COMMENT 'Number of active users',
    active_countries smallint COMMENT 'Number of countries',
    duration         bigint COMMENT 'Duration'
);

CREATE UNIQUE INDEX uniq_i_daily_games
    ON bus.daily_games (date, game_id);

SELECT *
FROM daily_games;

DROP TABLE IF EXISTS bus.total_games;
CREATE TABLE bus.total_games
(
    game_id                 smallint NOT NULL COMMENT 'Game ID',
    game_type               varchar(100) COMMENT 'Game Type',
    dates                   int COMMENT 'Number of dates',
    first_date              date COMMENT 'First date',
    last_date               date COMMENT 'Last date',
    active_users            int COMMENT 'Number of active users',
    middle_users_daily      dec(10, 3) COMMENT 'Middle users daily',
    active_countries        smallint COMMENT 'Number of countries',
    middle_active_countries dec(13, 3) COMMENT 'Middle Number of countries daily',
    duration                bigint COMMENT 'Duration',
    middle_duration_daily   dec(13, 3) COMMENT 'Middle duration daily'
);

CREATE UNIQUE INDEX uniq_i_total_games
    ON bus.total_games (game_id);

SELECT *
FROM total_games;


-- _________________________________________________________________ --

-- GAME TYPES

DROP TABLE IF EXISTS bus.daily_game_types;
CREATE TABLE bus.daily_game_types
(
    date             date         NOT NULL COMMENT 'Date',
    game_type_id     smallint     NOT NULL COMMENT 'Game Type ID',
    game_type        varchar(100) NOT NULL COMMENT 'Game Type',
    active_games     int COMMENT 'Number of Games',
    active_users     int COMMENT 'Number of active users',
    active_countries smallint COMMENT 'Number of countries',
    duration         bigint COMMENT 'Duration'
);

CREATE UNIQUE INDEX uniq_i_daily_game_types
    ON bus.daily_game_types (date, game_type_id);

SELECT *
FROM daily_game_types;

DROP TABLE IF EXISTS bus.total_game_types;
CREATE TABLE bus.total_game_types
(
    game_type_id            smallint NOT NULL COMMENT 'Game Type ID',
    game_type               varchar(100) COMMENT 'Game Type',
    games                   int COMMENT 'Number of games',
    dates                   int COMMENT 'Number of dates',
    first_date              date COMMENT 'First date',
    last_date               date COMMENT 'Last date',
    active_users            int COMMENT 'Number of active users',
    middle_users_daily      dec(10, 3) COMMENT 'Middle users daily',
    active_countries        smallint COMMENT 'Number of countries',
    middle_active_countries dec(13, 3) COMMENT 'Middle Number of countries daily',
    duration                bigint COMMENT 'Duration',
    middle_duration_daily   dec(13, 3) COMMENT 'Middle duration daily'
);

CREATE UNIQUE INDEX uniq_i_total_game_types
    ON bus.total_game_types (game_type_id);

SELECT *
FROM total_game_types;


-- _________________________________________________________________ --
-- PAYMENTS

DROP TABLE IF EXISTS bus.daily_payments;
CREATE TABLE bus.daily_payments
(
    date             date NOT NULL COMMENT 'Date',
    type             enum ('Deposit',
        'Withdrawal',
        'Correction')     NOT NULL COMMENT 'Type --enum',
    active_users     int COMMENT 'Number of active users',
    active_countries smallint COMMENT 'Number of countries',
    transactions     int COMMENT 'Number of transactions',
    deposit          dec(13, 2) COMMENT 'Deposit Amount',
    deposits         int COMMENT 'Number of Deposits',
    withdrawal       dec(13, 2) COMMENT 'Withdrawal Amount',
    withdrawals      int COMMENT 'Number of Withdrawals'
);

CREATE UNIQUE INDEX uniq_i_daily_payments_type
    ON bus.daily_payments (date, type);

SELECT *
FROM daily_payments;

DROP TABLE IF EXISTS bus.total_payments;
CREATE TABLE bus.total_payments
(
    type                      enum ('Deposit',
        'Withdrawal',
        'Correction') NOT NULL COMMENT 'Type --enum',
    dates                     int COMMENT 'Number of dates',
    first_date                date COMMENT 'First date',
    last_date                 date COMMENT 'Last date',
    active_users              int COMMENT 'Number of active users',
    middle_users_daily        dec(10, 3) COMMENT 'Middle users daily',
    active_countries          smallint COMMENT 'Number of countries',
    middle_active_countries   dec(13, 3) COMMENT 'Middle Number of countries daily',
    transactions              int COMMENT 'Number of transactions',
    middle_transactions_daily dec(10, 3) COMMENT 'Middle transactions daily',
    deposit                   dec(15, 2) COMMENT 'Deposit Amount',
    middle_deposit_daily      dec(13, 3) COMMENT 'Middle deposit daily',
    deposits                  int COMMENT 'Number of Deposits',
    middle_deposits_daily     dec(13, 3) COMMENT 'Middle deposits daily',
    withdrawal                dec(15, 2) COMMENT 'Withdrawal Amount',
    middle_withdrawal_daily   dec(13, 3) COMMENT 'Middle withdrawal daily',
    withdrawals               int COMMENT 'Number of Withdrawals',
    middle_withdrawals_daily  dec(13, 3) COMMENT 'Middle withdrawals daily'
);

CREATE UNIQUE INDEX uniq_i_total_payments
    ON bus.total_payments (type);

SELECT *
FROM total_payments;
