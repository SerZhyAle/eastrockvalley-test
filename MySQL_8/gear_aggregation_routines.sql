USE gear;

-- user
DROP PROCEDURE IF EXISTS gear.aggregate_daily_users;
CREATE PROCEDURE gear.aggregate_daily_users(IN in_date date)
BEGIN
    -- call gear.aggregate_daily_users(NULL);

    SET @in_date = in_date;

    IF @in_date IS NULL THEN
        SELECT MIN(u.signup_date) AS min_date
        FROM vault.h_users u
        INTO @in_date;

        SET @this_is_full_reload = 1;
    ELSE
        SET @this_is_full_reload = 0;
    END IF;

    DROP TABLE IF EXISTS gear.temp_daily_users_ugd;
    CREATE TABLE gear.temp_daily_users_ugd
    SELECT lugd.play_date               AS date,
           u.id                         AS user_id,
           COUNT(DISTINCT lugd.game_id) AS active_games,
           COUNT(DISTINCT hgt.id)       AS active_game_types,
           SUM(sugd.duration)           AS duration
    FROM vault.h_users u
        JOIN vault.l_users_games_date lugd
            ON u.id = lugd.user_id
        JOIN vault.s_users_games_date sugd
            ON lugd.id = sugd.l_users_games_date_id
        JOIN vault.h_games hg
            ON hg.id = lugd.game_id
        JOIN vault.h_game_types hgt
            ON hgt.id = hg.type_id
    WHERE lugd.play_date >= @in_date
    GROUP BY lugd.play_date, u.id;

    DROP TABLE IF EXISTS gear.temp_daily_users_payments;
    CREATE TABLE gear.temp_daily_users_payments
    SELECT p.transaction_date                          AS date,
           u.id                                        AS user_id,
           COUNT(DISTINCT p.transaction_id)            AS transactions,
           SUM(IF(p.type = 'Deposit', 1, 0))           AS deposits,
           SUM(IF(p.type = 'Deposit', p.amount, 0))    AS deposit,
           SUM(IF(p.type = 'Withdrawal', 1, 0))        AS withdrawals,
           SUM(IF(p.type = 'Withdrawal', p.amount, 0)) AS withdrawal
    FROM vault.h_users u
        JOIN vault.payments p
            ON p.user_id = u.id
    WHERE p.transaction_date >= @in_date
    GROUP BY p.transaction_date, u.id;

-- _________________________________________________________________ --
    DROP TABLE IF EXISTS gear.swap_daily_users;
    CREATE TABLE gear.swap_daily_users LIKE bus.daily_users;

    INSERT INTO gear.swap_daily_users (date, user_id, user, active_games, active_game_types, transactions, deposit, deposits, withdrawal, withdrawals, duration)
    SELECT dates.date,
           u.id                        AS user_id,
           u.name                      AS user,
           SUM(cugd.active_games)      AS active_games,
           SUM(cugd.active_game_types) AS active_game_types,
           SUM(cp.transactions)        AS transactions,
           SUM(cp.deposit)             AS deposit,
           SUM(cp.deposits)            AS deposits,
           SUM(cp.withdrawal)          AS withdrawal,
           SUM(cp.withdrawals)         AS withdrawals,
           SUM(cugd.duration)          AS duration
    FROM vault.calendar_dates dates
        INNER JOIN vault.h_users u
        LEFT JOIN gear.temp_daily_users_ugd AS cugd
            ON cugd.date = dates.date AND cugd.user_id = u.id
        LEFT JOIN gear.temp_daily_users_payments AS cp
            ON cp.date = dates.date AND cp.user_id = u.id
    WHERE dates.date >= @in_date
      AND dates.date <= CURRENT_DATE
    GROUP BY dates.date, u.name;

    IF @this_is_full_reload = 1 THEN
        START TRANSACTION;

        DROP TABLE IF EXISTS bus.daily_users;
        RENAME TABLE gear.swap_daily_users TO bus.daily_users;
        COMMIT;

    ELSE -- only new
        REPLACE INTO bus.daily_users (date, user_id, user, active_games, active_game_types, transactions, deposit, deposits, withdrawal, withdrawals, duration)
        SELECT date,
               user_id,
               user,
               active_games,
               active_game_types,
               transactions,
               deposit,
               deposits,
               withdrawal,
               withdrawals,
               duration
        FROM gear.swap_daily_users;

        DROP TABLE gear.swap_daily_users;
    END IF;

    CALL gear.aggregate_total_users();

END;

-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.aggregate_total_users;
CREATE PROCEDURE gear.aggregate_total_users()
BEGIN

    DROP TABLE IF EXISTS gear.temp_total_users_games;
    CREATE TABLE gear.temp_total_users_games
    SELECT u.id                         AS user_id,
           COUNT(DISTINCT lugd.game_id) AS active_games,
           COUNT(DISTINCT g.type_id)    AS active_game_types
    FROM vault.h_users u
        JOIN vault.l_users_games_date lugd
            ON u.id = lugd.user_id
        JOIN vault.h_games g
            ON g.id = lugd.game_id
    GROUP BY u.id;

    DROP TABLE IF EXISTS gear.temp_total_users;
    CREATE TABLE gear.temp_total_users
    SELECT user_id,
           user,
           COUNT(DISTINCT date)                     AS dates,
           MIN(date)                                AS first_date,
           MAX(date)                                AS last_date,
           SUM(transactions)                        AS transactions,
           SUM(transactions) / COUNT(DISTINCT date) AS middle_transactions_daily,
           SUM(deposit)                             AS deposit,
           SUM(deposit) / COUNT(DISTINCT date)      AS middle_deposit_daily,
           SUM(deposits)                            AS deposits,
           SUM(deposits) / COUNT(DISTINCT date)     AS middle_deposits_daily,
           SUM(withdrawal)                          AS withdrawal,
           SUM(withdrawal) / COUNT(DISTINCT date)   AS middle_withdrawal_daily,
           SUM(withdrawals)                         AS withdrawals,
           SUM(withdrawals) / COUNT(DISTINCT date)  AS middle_withdrawals_daily,
           SUM(duration)                            AS duration,
           SUM(duration) / COUNT(DISTINCT date)     AS middle_duration_daily
    FROM bus.daily_users AS du
    WHERE user IS NOT NULL
    GROUP BY user_id, user;

    REPLACE INTO bus.total_users(user_id, user, dates, first_date, last_date, active_games, middle_games_daily,
                                 active_game_types, middle_game_types_daily, transactions,
                                 middle_transactions_daily, deposit, middle_deposit_daily, deposits, middle_deposits_daily,
                                 withdrawal, middle_withdrawal_daily, withdrawals, middle_withdrawals_daily, duration, middle_duration_daily)
    SELECT du.user_id,
           du.user,
           du.dates,
           du.first_date,
           du.last_date,
           g.active_games,
           g.active_games / du.dates      AS middle_games_daily,
           g.active_game_types,
           g.active_game_types / du.dates AS middle_game_types_daily,
           du.transactions,
           du.middle_transactions_daily,
           du.deposit,
           du.middle_deposit_daily,
           du.deposits,
           du.middle_deposits_daily,
           du.withdrawal,
           du.middle_withdrawal_daily,
           du.withdrawals,
           du.middle_withdrawals_daily,
           du.duration,
           du.middle_duration_daily
    FROM gear.temp_total_users AS du
        JOIN gear.temp_total_users_games AS g
            ON g.user_id = du.user_id;

END;



-- _________________________________________________________________ --
-- country
DROP PROCEDURE IF EXISTS gear.aggregate_daily_countries;
CREATE PROCEDURE gear.aggregate_daily_countries(IN in_date date)
BEGIN
    -- call gear.aggregate_daily_countries(NULL);

    SET @in_date = in_date;

    IF @in_date IS NULL THEN
        SELECT MIN(u.signup_date) AS min_date
        FROM vault.h_users u
        INTO @in_date;

        SET @this_is_full_reload = 1;
    ELSE
        SET @this_is_full_reload = 0;
    END IF;

    DROP TABLE IF EXISTS gear.temp_daily_countries_users;
    CREATE TABLE gear.temp_daily_countries_users
    SELECT u.signup_date        AS date,
           c.id                 AS country_id,
           COUNT(DISTINCT u.id) AS sign_ups
    FROM vault.h_countries c
        JOIN vault.h_users u
            ON u.country_id = c.id
    WHERE u.signup_date >= @in_date
    GROUP BY u.signup_date, c.id;

    DROP TABLE IF EXISTS gear.temp_daily_countries_ugd;
    CREATE TABLE gear.temp_daily_countries_ugd
    SELECT lugd.play_date               AS date,
           c.id                         AS country_id,
           COUNT(DISTINCT u.id)         AS active_users,
           COUNT(DISTINCT lugd.game_id) AS active_games,
           SUM(sugd.duration)           AS duration
    FROM vault.h_countries c
        JOIN vault.h_users u
            ON u.country_id = c.id
        JOIN vault.l_users_games_date lugd
            ON u.id = lugd.user_id
        JOIN vault.s_users_games_date sugd
            ON lugd.id = sugd.l_users_games_date_id
    WHERE lugd.play_date >= @in_date
    GROUP BY lugd.play_date, c.id;

    DROP TABLE IF EXISTS gear.temp_daily_countries_payments;
    CREATE TABLE gear.temp_daily_countries_payments
    SELECT p.transaction_date                          AS date,
           c.id                                        AS country_id,
           COUNT(DISTINCT p.transaction_id)            AS transactions,
           SUM(IF(p.type = 'Deposit', 1, 0))           AS deposits,
           SUM(IF(p.type = 'Deposit', p.amount, 0))    AS deposit,
           SUM(IF(p.type = 'Withdrawal', 1, 0))        AS withdrawals,
           SUM(IF(p.type = 'Withdrawal', p.amount, 0)) AS withdrawal
    FROM vault.h_countries c
        JOIN vault.h_users u
            ON u.country_id = c.id
        JOIN vault.payments p
            ON p.user_id = u.id
    WHERE p.transaction_date >= @in_date
    GROUP BY p.transaction_date, c.id;

-- _________________________________________________________________ --
    DROP TABLE IF EXISTS gear.swap_daily_countries;
    CREATE TABLE gear.swap_daily_countries LIKE bus.daily_countries;

    INSERT INTO gear.swap_daily_countries (date, country_id, country, active_users, active_games, sign_ups, transactions, deposit, deposits, withdrawal, withdrawals, duration)
    SELECT dates.date,
           hc.id                  AS country_id,
           hc.name                AS country,
           SUM(cugd.active_users) AS active_users,
           SUM(cugd.active_games) AS active_games,
           SUM(cu.sign_ups)       AS sign_ups,
           SUM(cp.transactions)   AS transactions,
           SUM(cp.deposit)        AS deposit,
           SUM(cp.deposits)       AS deposits,
           SUM(cp.withdrawal)     AS withdrawal,
           SUM(cp.withdrawals)    AS withdrawals,
           SUM(cugd.duration)     AS duration
    FROM vault.calendar_dates dates
        INNER JOIN vault.h_countries hc
        LEFT JOIN gear.temp_daily_countries_users AS cu
            ON cu.date = dates.date AND cu.country_id = hc.id
        LEFT JOIN gear.temp_daily_countries_ugd AS cugd
            ON cugd.date = dates.date AND cugd.country_id = hc.id
        LEFT JOIN gear.temp_daily_countries_payments AS cp
            ON cp.date = dates.date AND cp.country_id = hc.id
    WHERE dates.date >= @in_date
      AND dates.date <= CURRENT_DATE
    GROUP BY dates.date, hc.name;

    IF @this_is_full_reload = 1 THEN
        START TRANSACTION;

        DROP TABLE IF EXISTS bus.daily_countries;
        RENAME TABLE gear.swap_daily_countries TO bus.daily_countries;
        COMMIT;

    ELSE -- only new
        REPLACE INTO bus.daily_countries (date, country_id, country, active_users, active_games, sign_ups, transactions, deposit, deposits, withdrawal, withdrawals, duration)
        SELECT date,
               country_id,
               country,
               active_users,
               active_games,
               sign_ups,
               transactions,
               deposit,
               deposits,
               withdrawal,
               withdrawals,
               duration
        FROM gear.swap_daily_countries;

        DROP TABLE gear.swap_daily_countries;
    END IF;

    CALL gear.aggregate_total_countries();

END;

-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.aggregate_total_countries;
CREATE PROCEDURE gear.aggregate_total_countries()
BEGIN

    DROP TABLE IF EXISTS gear.temp_total_countries_games;
    CREATE TABLE gear.temp_total_countries_games
    SELECT c.id                         AS country_id,
           COUNT(DISTINCT lugd.game_id) AS active_games,
           COUNT(DISTINCT u.id)         AS active_users
    FROM vault.h_countries c
        JOIN vault.h_users u
            ON u.country_id = c.id
        JOIN vault.l_users_games_date lugd
            ON u.id = lugd.user_id
        JOIN vault.h_games g
            ON g.id = lugd.game_id
    GROUP BY c.id;

    DROP TABLE IF EXISTS gear.temp_total_countries;
    CREATE TABLE gear.temp_total_countries
    SELECT country_id,
           country,
           COUNT(DISTINCT date)                     AS dates,
           MIN(date)                                AS first_date,
           MAX(date)                                AS last_date,
           SUM(sign_ups)                            AS sign_ups,
           SUM(sign_ups) / COUNT(DISTINCT date)     AS middle_sign_ups_daily,
           SUM(transactions)                        AS transactions,
           SUM(transactions) / COUNT(DISTINCT date) AS middle_transactions_daily,
           SUM(deposit)                             AS deposit,
           SUM(deposit) / COUNT(DISTINCT date)      AS middle_deposit_daily,
           SUM(deposits)                            AS deposits,
           SUM(deposits) / COUNT(DISTINCT date)     AS middle_deposits_daily,
           SUM(withdrawal)                          AS withdrawal,
           SUM(withdrawal) / COUNT(DISTINCT date)   AS middle_withdrawal_daily,
           SUM(withdrawals)                         AS withdrawals,
           SUM(withdrawals) / COUNT(DISTINCT date)  AS middle_withdrawals_daily,
           SUM(duration)                            AS duration,
           SUM(duration) / COUNT(DISTINCT date)     AS middle_duration_daily
    FROM bus.daily_countries AS dc
    WHERE country IS NOT NULL
    GROUP BY country, country_id;

    REPLACE INTO bus.total_countries (country_id, country, dates, first_date, last_date, active_users, middle_users_daily, active_games, middle_games_daily, sign_ups,
                                      middle_sign_ups_daily,
                                      transactions, middle_transactions_daily, deposit, middle_deposit_daily, deposits, middle_deposits_daily, withdrawal, middle_withdrawal_daily,
                                      withdrawals, middle_withdrawals_daily, duration, middle_duration_daily)
    SELECT dc.country_id,
           dc.country,
           dc.dates,
           dc.first_date,
           dc.last_date,
           g.active_users,
           g.active_users / dc.dates AS middle_users_daily,
           g.active_games,
           g.active_games / dc.dates AS middle_games_daily,
           dc.sign_ups,
           dc.middle_sign_ups_daily,
           dc.transactions,
           dc.middle_transactions_daily,
           dc.deposit,
           dc.middle_deposit_daily,
           dc.deposits,
           dc.middle_deposits_daily,
           dc.withdrawal,
           dc.middle_withdrawal_daily,
           dc.withdrawals,
           dc.middle_withdrawals_daily,
           dc.duration,
           dc.middle_duration_daily
    FROM gear.temp_total_countries AS dc
        JOIN gear.temp_total_countries_games AS g
            ON g.country_id = dc.country_id;

END;


-- _________________________________________________________________ --
-- game
-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.aggregate_daily_games;
CREATE PROCEDURE gear.aggregate_daily_games(IN in_date date)
BEGIN
    -- call gear.aggregate_daily_games(NULL);

    SET @in_date = in_date;

    IF @in_date IS NULL THEN
        SELECT MIN(l.play_date) AS min_date
        FROM vault.l_users_games_date l
        INTO @in_date;

        SET @this_is_full_reload = 1;
    ELSE
        SET @this_is_full_reload = 0;
    END IF;

    DROP TABLE IF EXISTS gear.temp_daily_games_ugd;
    CREATE TABLE gear.temp_daily_games_ugd
    SELECT lugd.play_date               AS date,
           g.id                         AS game_id,
           COUNT(DISTINCT u.id)         AS active_users,
           COUNT(DISTINCT u.country_id) AS active_countries,
           SUM(sugd.duration)           AS duration
    FROM vault.h_games g
        JOIN vault.l_users_games_date lugd
            ON g.id = lugd.game_id
        JOIN vault.h_users u
            ON u.id = lugd.user_id
        JOIN vault.s_users_games_date sugd
            ON lugd.id = sugd.l_users_games_date_id
    WHERE lugd.play_date >= @in_date
    GROUP BY lugd.play_date, g.id;

-- _________________________________________________________________ --
    DROP TABLE IF EXISTS gear.swap_daily_games;
    CREATE TABLE gear.swap_daily_games LIKE bus.daily_games;

    INSERT INTO gear.swap_daily_games (date, game_id, game_type, active_users, active_countries, duration)
    SELECT dates.date,
           g.id                       AS game_id,
           gt.name                    AS game_type,
           SUM(cugd.active_users)     AS active_users,
           SUM(cugd.active_countries) AS active_countries,
           SUM(cugd.duration)         AS duration
    FROM vault.calendar_dates dates
        INNER JOIN vault.h_games g
        JOIN vault.h_game_types gt
            ON gt.id = g.type_id
        LEFT JOIN gear.temp_daily_games_ugd AS cugd
            ON cugd.date = dates.date
            AND cugd.game_id = g.id
    WHERE dates.date >= @in_date
      AND dates.date <= CURRENT_DATE
    GROUP BY dates.date, g.id, gt.name;

    IF @this_is_full_reload = 1 THEN
        START TRANSACTION;

        DROP TABLE IF EXISTS bus.daily_games;
        RENAME TABLE gear.swap_daily_games TO bus.daily_games;
        COMMIT;

    ELSE -- only new
        REPLACE INTO bus.daily_games (date, game_id, game_type, active_users, active_countries, duration)
        SELECT date, game_id, game_type, active_users, active_countries, duration
        FROM gear.swap_daily_games;

        DROP TABLE gear.swap_daily_games;
    END IF;

    CALL gear.aggregate_total_games();

END;

-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.aggregate_total_games;
CREATE PROCEDURE gear.aggregate_total_games()
BEGIN

    REPLACE INTO bus.total_games (game_id, game_type, dates, first_date, last_date, active_users, middle_users_daily, active_countries, middle_active_countries, duration,
                                  middle_duration_daily)
    SELECT game_id,
           hgt.name                                                       AS game_type,
           COUNT(DISTINCT lugd.play_date)                                 AS dates,
           MIN(lugd.play_date)                                            AS first_date,
           MAX(lugd.play_date)                                            AS last_date,
           COUNT(DISTINCT lugd.user_id)                                   AS active_users,
           COUNT(DISTINCT lugd.user_id) / COUNT(DISTINCT lugd.play_date)  AS sumiddle_users_daily,
           COUNT(DISTINCT hu.country_id)                                  AS active_countries,
           COUNT(DISTINCT hu.country_id) / COUNT(DISTINCT lugd.play_date) AS middle_countriies_daily,
           SUM(duration)                                                  AS duration,
           SUM(duration) / COUNT(DISTINCT lugd.play_date)                 AS middle_duration_daily
    FROM vault.l_users_games_date lugd
        JOIN vault.h_games hg
            ON hg.id = lugd.game_id
        JOIN vault.h_game_types hgt
            ON hgt.id = hg.type_id
        JOIN vault.h_users hu
            ON hu.id = lugd.user_id
        JOIN vault.s_users_games_date sugd
            ON sugd.l_users_games_date_id = lugd.id
    GROUP BY lugd.game_id, hgt.name;

END;

-- _________________________________________________________________ --
-- game_type
-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.aggregate_daily_game_types;
CREATE PROCEDURE gear.aggregate_daily_game_types(IN in_date date)
BEGIN
    -- call gear.aggregate_daily_game_types(NULL);

    SET @in_date = in_date;

    IF @in_date IS NULL THEN
        SELECT MIN(l.play_date) AS min_date
        FROM vault.l_users_games_date l
        INTO @in_date;

        SET @this_is_full_reload = 1;
    ELSE
        SET @this_is_full_reload = 0;
    END IF;

    DROP TABLE IF EXISTS gear.temp_daily_game_types_ugd;
    CREATE TABLE gear.temp_daily_game_types_ugd
    SELECT lugd.play_date               AS date,
           gt.id                        AS game_type_id,
           gt.name                      AS game_type,
           COUNT(DISTINCT g.id)         AS active_games,
           COUNT(DISTINCT u.id)         AS active_users,
           COUNT(DISTINCT u.country_id) AS active_countries,
           SUM(sugd.duration)           AS duration
    FROM vault.h_games g
        JOIN vault.h_game_types gt
            ON gt.id = g.type_id
        JOIN vault.l_users_games_date lugd
            ON g.id = lugd.game_id
        JOIN vault.h_users u
            ON u.id = lugd.user_id
        JOIN vault.s_users_games_date sugd
            ON lugd.id = sugd.l_users_games_date_id
    WHERE lugd.play_date >= @in_date
    GROUP BY lugd.play_date, gt.id;

-- _________________________________________________________________ --
    DROP TABLE IF EXISTS gear.swap_daily_game_types;
    CREATE TABLE gear.swap_daily_game_types LIKE bus.daily_game_types;

    INSERT INTO gear.swap_daily_game_types (date, game_type_id, game_type, active_games, active_users, active_countries, duration)
    SELECT dates.date,
           gt.id                      AS game_type_id,
           gt.name                    AS game_type,
           COUNT(DISTINCT g.id)       AS active_games,
           SUM(cugd.active_users)     AS active_users,
           SUM(cugd.active_countries) AS active_countries,
           SUM(cugd.duration)         AS duration
    FROM vault.calendar_dates dates
        INNER JOIN vault.h_games g
        JOIN vault.h_game_types gt
            ON gt.id = g.type_id
        LEFT JOIN gear.temp_daily_game_types_ugd AS cugd
            ON cugd.date = dates.date
            AND cugd.game_type_id = g.id
    WHERE dates.date >= @in_date
      AND dates.date <= CURRENT_DATE
    GROUP BY dates.date, gt.id, gt.name;

    IF @this_is_full_reload = 1 THEN
        START TRANSACTION;

        DROP TABLE IF EXISTS bus.daily_game_types;
        RENAME TABLE gear.swap_daily_game_types TO bus.daily_game_types;
        COMMIT;

    ELSE -- only new
        REPLACE INTO bus.daily_game_types (date, game_type_id, game_type, active_games, active_users, active_countries, duration)
        SELECT date, game_type_id, game_type, active_games, active_users, active_countries, duration
        FROM gear.swap_daily_game_types;

        DROP TABLE gear.swap_daily_game_types;
    END IF;

    CALL gear.aggregate_total_game_types();

END;

-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.aggregate_total_game_types;
CREATE PROCEDURE gear.aggregate_total_game_types()
BEGIN

    REPLACE INTO bus.total_game_types (game_type_id, game_type, games, dates, first_date, last_date, active_users, middle_users_daily, active_countries, middle_active_countries,
                                       duration,
                                       middle_duration_daily)
    SELECT hgt.id                                                         AS game_type_id,
           hgt.name                                                       AS game_type,
           COUNT(DISTINCT lugd.game_id)                                   AS games,
           COUNT(DISTINCT lugd.play_date)                                 AS dates,
           MIN(lugd.play_date)                                            AS first_date,
           MAX(lugd.play_date)                                            AS last_date,
           COUNT(DISTINCT lugd.user_id)                                   AS active_users,
           COUNT(DISTINCT lugd.user_id) / COUNT(DISTINCT lugd.play_date)  AS sumiddle_users_daily,
           COUNT(DISTINCT hu.country_id)                                  AS active_countries,
           COUNT(DISTINCT hu.country_id) / COUNT(DISTINCT lugd.play_date) AS middle_countriies_daily,
           SUM(duration)                                                  AS duration,
           SUM(duration) / COUNT(DISTINCT lugd.play_date)                 AS middle_duration_daily
    FROM vault.l_users_games_date lugd
        JOIN vault.h_games hg
            ON hg.id = lugd.game_id
        JOIN vault.h_game_types hgt
            ON hgt.id = hg.type_id
        JOIN vault.h_users hu
            ON hu.id = lugd.user_id
        JOIN vault.s_users_games_date sugd
            ON sugd.l_users_games_date_id = lugd.id
    GROUP BY lugd.game_id, hgt.name;

END;

-- _________________________________________________________________ --
-- payments
-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.aggregate_daily_payments;
CREATE PROCEDURE gear.aggregate_daily_payments(IN in_date date)
BEGIN
    -- call gear.aggregate_daily_payments(NULL);

    SET @in_date = in_date;

    IF @in_date IS NULL THEN
        SELECT MIN(u.signup_date) AS min_date
        FROM vault.h_users u
        INTO @in_date;

        SET @this_is_full_reload = 1;
    ELSE
        SET @this_is_full_reload = 0;
    END IF;

    DROP TABLE IF EXISTS gear.temp_daily_payments;
    CREATE TABLE gear.temp_daily_payments
    SELECT p.transaction_date                          AS date,
           p.type                                      AS type,
           COUNT(DISTINCT transaction_id)              AS transactions,
           COUNT(DISTINCT u.id)                        AS active_users,
           COUNT(DISTINCT u.country_id)                AS active_countries,
           SUM(IF(p.type = 'Deposit', 1, 0))           AS deposits,
           SUM(IF(p.type = 'Deposit', p.amount, 0))    AS deposit,
           SUM(IF(p.type = 'Withdrawal', 1, 0))        AS withdrawals,
           SUM(IF(p.type = 'Withdrawal', p.amount, 0)) AS withdrawal
    FROM vault.h_users u
        JOIN vault.payments p
            ON p.user_id = u.id
    WHERE p.transaction_date >= @in_date
    GROUP BY p.transaction_date, p.type;

-- _________________________________________________________________ --
    DROP TABLE IF EXISTS gear.swap_daily_payments;
    CREATE TABLE gear.swap_daily_payments LIKE bus.daily_payments;

    INSERT INTO gear.swap_daily_payments (date, type, active_users, active_countries, transactions, deposit, deposits, withdrawal, withdrawals)
    SELECT cp.date,
           cp.type,
           SUM(cp.active_users)     AS active_users,
           SUM(cp.active_countries) AS active_countries,
           SUM(cp.transactions)     AS transactions,
           SUM(cp.deposit)          AS deposit,
           SUM(cp.deposits)         AS deposits,
           SUM(cp.withdrawal)       AS withdrawal,
           SUM(cp.withdrawals)      AS withdrawals
    FROM gear.temp_daily_payments AS cp
    WHERE cp.date >= @in_date
      AND cp.date <= CURRENT_DATE
    GROUP BY cp.date, cp.type;

    IF @this_is_full_reload = 1 THEN
        START TRANSACTION;

        DROP TABLE IF EXISTS bus.daily_payments;
        RENAME TABLE gear.swap_daily_payments TO bus.daily_payments;
        COMMIT;

    ELSE -- only new
        REPLACE INTO bus.daily_payments (date, type, active_users, active_countries, transactions, deposit, deposits, withdrawal, withdrawals)
        SELECT date, type, active_users, active_countries, transactions, deposit, deposits, withdrawal, withdrawals
        FROM gear.swap_daily_payments;

        DROP TABLE gear.swap_daily_payments;
    END IF;

-- don't need to run often
  --  CALL gear.aggregate_total_payments();

END;

-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.aggregate_total_payments;
CREATE PROCEDURE gear.aggregate_total_payments()
BEGIN

-- is set for midnight_routine_aggregations

    DROP TABLE IF EXISTS gear.temp_total_users_payments;
    CREATE TABLE gear.temp_total_users_payments
    SELECT p.type,
           COUNT(DISTINCT p.user_id) AS active_users,
           COUNT(DISTINCT u.country_id)    AS active_countries
    FROM vault.payments p
    join     vault.h_users u
    on u.id = p.user_id
    GROUP BY p.type;

    DROP TABLE IF EXISTS gear.temp_total_payment;
    CREATE TABLE gear.temp_total_payment
    SELECT type,
           COUNT(DISTINCT date)                     AS dates,
           MIN(date)                                AS first_date,
           MAX(date)                                AS last_date,
           SUM(transactions)                        AS transactions,
           SUM(transactions) / COUNT(DISTINCT date) AS middle_transactions_daily,
           SUM(deposit)                             AS deposit,
           SUM(deposit) / COUNT(DISTINCT date)      AS middle_deposit_daily,
           SUM(deposits)                            AS deposits,
           SUM(deposits) / COUNT(DISTINCT date)     AS middle_deposits_daily,
           SUM(withdrawal)                          AS withdrawal,
           SUM(withdrawal) / COUNT(DISTINCT date)   AS middle_withdrawal_daily,
           SUM(withdrawals)                         AS withdrawals,
           SUM(withdrawals) / COUNT(DISTINCT date)  AS middle_withdrawals_daily
    FROM bus.daily_payments AS du
    GROUP BY type;

    REPLACE INTO bus.total_payments(type, dates, first_date, last_date, active_users, middle_users_daily, active_countries, middle_active_countries, transactions, middle_transactions_daily, deposit, middle_deposit_daily, deposits, middle_deposits_daily, withdrawal, middle_withdrawal_daily, withdrawals, middle_withdrawals_daily)
    SELECT du.type,
           du.dates,
           du.first_date,
           du.last_date,
           g.active_users,
           g.active_users / du.dates      AS middle_users_daily,
           g.active_countries,
           g.active_countries / du.dates AS middle_countries_daily,
           du.transactions,
           du.middle_transactions_daily,
           du.deposit,
           du.middle_deposit_daily,
           du.deposits,
           du.middle_deposits_daily,
           du.withdrawal,
           du.middle_withdrawal_daily,
           du.withdrawals,
           du.middle_withdrawals_daily
    FROM gear.temp_total_payment AS du
        JOIN gear.temp_total_users_payments AS g
            ON g.type = du.type;

END;

-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.calc_aggregations;
CREATE PROCEDURE gear.calc_aggregations()
BEGIN

    DROP TEMPORARY TABLE IF EXISTS temp_changed_to_aggregate;

    START TRANSACTION ;

-- calculate min_date to process only new date(s)
    SELECT MIN(min_date) AS min_date
    FROM (SELECT MIN(u.signup_date) AS min_date
          FROM gear.recent_changes ch
              JOIN vault.h_users u
                  ON u.id = ch.element_id
          WHERE ch.data_type = 'user'
          UNION ALL
          SELECT MIN(p.transaction_date) AS min_date
          FROM gear.recent_changes ch
              JOIN vault.payments p
                  ON p.transaction_id = ch.element_id
          WHERE ch.data_type = 'payment'
          UNION ALL
          SELECT MIN(lugd.play_date) AS min_date
          FROM gear.recent_changes ch
              JOIN vault.l_users_games_date lugd
                  ON lugd.id = ch.element_id
          WHERE ch.data_type = 'user-game-date') res
    INTO @min_date;

    TRUNCATE gear.recent_changes;

    CREATE TEMPORARY TABLE temp_changed_to_aggregate
    SELECT data_type, element_id, data_status, stored_at
    FROM gear.recent_changes;

    CREATE INDEX index_temp_changed_to_aggregate_data_type_id
        ON temp_changed_to_aggregate (data_type, element_id);

    COMMIT;

    SELECT COUNT(*) AS cnt
    FROM temp_changed_to_aggregate
    INTO @were_changed;
-- _________________________________________________________________ --
    IF COALESCE(@were_changed, 0) > 0 THEN

        SELECT COUNT(*) AS cnt
        FROM temp_changed_to_aggregate
        WHERE data_type = 'user'
        INTO @users_were_changed;

        SELECT COUNT(*) AS cnt
        FROM temp_changed_to_aggregate
        WHERE data_type = 'game'
        INTO @games_were_changed;

        SELECT COUNT(*) AS cnt
        FROM temp_changed_to_aggregate
        WHERE data_type = 'user-game-date'
        INTO @ugd_were_changed;

        SELECT COUNT(*) AS cnt
        FROM temp_changed_to_aggregate
        WHERE data_type = 'payment'
        INTO @payments_were_changed;

        /*
        todo:
        Now realized the condition about the @min_date to run aggregations only for updated/added dates.

        But also in gear.recent_changes (temp_changed_to_aggregate)
        we have ID of changes/inserted elements.
        So we need to concern hot to speed-up the aggregations processing only new/updated elements.
         */

        IF COALESCE(@payments_were_changed, 0) > 0 OR
           COALESCE(@ugd_were_changed, 0) > 0 OR
           COALESCE(@users_were_changed, 0) > 0 THEN

            CALL gear.aggregate_daily_users(@min_date);
            CALL gear.aggregate_daily_countries(@min_date);
        END IF;

        IF COALESCE(@ugd_were_changed, 0) > 0 OR
           COALESCE(@games_were_changed, 0) > 0 OR
           COALESCE(@users_were_changed, 0) > 0 THEN

            CALL gear.aggregate_daily_games(@min_date);
            CALL gear.aggregate_daily_game_types(@min_date);
        END IF;

        IF COALESCE(@ugd_were_changed, 0) > 0 THEN
            CALL gear.aggregate_daily_payments(@min_date);
        END IF;
    END IF;

    DROP TEMPORARY TABLE IF EXISTS temp_changed_to_aggregate;
END;



DROP EVENT IF EXISTS gear.ten_minutes_routine_aggregations;
CREATE EVENT gear.ten_minutes_routine_aggregations
    ON SCHEDULE EVERY 10 MINUTE
        STARTS '2023-12-01 00:05:01' ON COMPLETION PRESERVE ENABLE DO
    CALL gear.calc_aggregations();


DROP EVENT IF EXISTS gear.midnight_routine_aggregations;
CREATE EVENT gear.midnight_routine_aggregations
    ON SCHEDULE EVERY 24 HOUR
        STARTS '2023-12-01 00:00:01' ON COMPLETION PRESERVE ENABLE DO
    CALL gear.aggregate_total_payments();
