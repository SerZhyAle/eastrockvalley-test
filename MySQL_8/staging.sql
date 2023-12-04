CREATE SCHEMA IF NOT EXISTS staging;

USE staging;

DROP TABLE IF EXISTS games CASCADE;
CREATE TABLE IF NOT EXISTS games
(
    gameid      varchar(250) NOT NULL COMMENT 'GameID -int',
    userid      varchar(250) NOT NULL COMMENT 'UserID -int',
    gametype    varchar(250) COMMENT 'GameType -varchar(50)',
    playdate    varchar(250) NOT NULL COMMENT 'PlayDate -date',
    duration    varchar(250) COMMENT 'Duration -int',
    imported_at datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Imported -datetime(6)'
) COMMENT 'Games original from S3';

DROP TABLE IF EXISTS payments CASCADE;
CREATE TABLE IF NOT EXISTS payments
(
    transactionid   varchar(250) NOT NULL COMMENT 'TransactionID -bigint',
    userid          varchar(250)    NOT NULL COMMENT 'UserID -int',
    amount          varchar(250)    NULL COMMENT 'Amount -dec(13,2)',
    transactiondate varchar(250)    NOT NULL COMMENT 'TransactionDate --date',
    type            varchar(250)               NOT NULL COMMENT 'Type --enum',
    imported_at     datetime(6)     NOT NULL DEFAULT NOW(6) COMMENT 'Imported -datetime(6)'
) COMMENT 'Payments original from S3';

DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE IF NOT EXISTS users
(
    userid      varchar(250) NOT NULL COMMENT 'UserID -int',
    username    varchar(255) NOT NULL COMMENT 'UserName varchar(255)',
    signupdate  varchar(250)         NOT NULL COMMENT 'SignUpDate -date',
    country     varchar(250) NULL COMMENT 'Country -varchar(100)',
    imported_at datetime(6)  NOT NULL DEFAULT NOW(6) COMMENT 'Imported -datetime(6)'
) COMMENT 'Users original from S3';

select * from staging.games g;
select * from staging.payments p;
select * from staging.users u;
