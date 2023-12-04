USE gear;

-- procedures
-- _________________________________________________________________ --

DROP PROCEDURE IF EXISTS gear.exec_process_users;
CREATE PROCEDURE gear.exec_process_users()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS temp_in_users;

    SET @processed_users = 0;

    CREATE TEMPORARY TABLE temp_in_users
    SELECT DISTINCT userid                              AS user_id,
                    username                            AS user_name,
                    STR_TO_DATE(signupdate, '%d/%m/%Y') AS signup_date,
                    country,
                    MAX(imported_at)                    AS imported_at
    FROM staging.users
    WHERE userid <> 'UserID'
    GROUP BY userid,
             username,
             STR_TO_DATE(signupdate, '%d/%m/%Y'),
             country;

    CREATE INDEX index_temp_in_users_user_id
        ON temp_in_users (user_id);

    INSERT INTO vault.h_countries(name)
    SELECT DISTINCT country
    FROM temp_in_users new
        LEFT JOIN vault.h_countries old
            ON old.name = new.country
    WHERE old.name IS NULL;

    DROP TEMPORARY TABLE IF EXISTS temp_recently_changed;
    CREATE TEMPORARY TABLE temp_recently_changed LIKE gear.recent_changes;
    #______________________________________________________________________________________

    START TRANSACTION;

-- users
    INSERT INTO temp_recently_changed(data_type, data_status, element_id)
    SELECT 'user', 'changed', new.user_id
    FROM temp_in_users AS new
        JOIN vault.h_users old
            ON old.id = new.user_id
        JOIN vault.h_countries AS countries
            ON countries.name = new.country
    WHERE (old.name <> new.user_name OR
           old.signup_date <> new.signup_date OR
           old.country_id <> countries.id);

    UPDATE vault.h_users old
        JOIN temp_recently_changed AS r_ch
        ON old.id = r_ch.element_id
        JOIN temp_in_users AS new
        ON old.id = new.user_id
        JOIN vault.h_countries AS countries
        ON countries.name = new.country
    SET old.name        = new.user_name,
        old.signup_date = new.signup_date,
        old.country_id  = countries.id
    WHERE (old.name <> new.user_name OR
           old.signup_date <> new.signup_date OR
           old.country_id <> countries.id);
    SET @processed_users = @processed_users + ROW_COUNT();

-- ins
    INSERT INTO temp_recently_changed(data_type, data_status, element_id)
    SELECT 'user', 'new', new.user_id
    FROM temp_in_users AS new
        LEFT JOIN vault.h_users AS old
            ON old.id = new.user_id
    WHERE old.id IS NULL;

    INSERT INTO vault.h_users(id,
                              name,
                              signup_date,
                              country_id)
    SELECT new.user_id,
           new.user_name,
           new.signup_date,
           c.id AS country_id
    FROM temp_recently_changed AS r_ch
        JOIN temp_in_users AS new
            ON r_ch.data_status = 'new'
            AND new.user_id = r_ch.element_id
        JOIN vault.h_countries AS c
            ON c.name = new.country;
    SET @processed_users = @processed_users + ROW_COUNT();

--
    INSERT INTO bulk.processed_users(user_id, user_name, signup_date, country, imported_at)
    SELECT user_id, user_name, signup_date, country, imported_at
    FROM temp_in_users;

    TRUNCATE TABLE staging.users;
    COMMIT;

    SELECT MAX(imported_at) AS imported_at
    FROM temp_in_users tig
    INTO @imported_at;

    DROP TEMPORARY TABLE temp_in_users;

    INSERT gear.process_staging_logs(table_name, processed_count, imported_at)
        VALUE ('users', @processed_users, @imported_at);

    INSERT gear.recent_changes(data_type, element_id, data_status)
    SELECT data_type, element_id, data_status
    FROM temp_recently_changed;

    DROP TEMPORARY TABLE temp_recently_changed;
END;

-- _________________________________________________________________ --

DROP PROCEDURE IF EXISTS gear.exec_process_games;
CREATE PROCEDURE gear.exec_process_games()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS temp_in_games;

    SET @processed_games = 0;

    CREATE TEMPORARY TABLE temp_in_games
    SELECT DISTINCT gameid                            AS game_id,
                    userid                            AS user_id,
                    gametype                          AS game_type,
                    STR_TO_DATE(playdate, '%d/%m/%Y') AS play_date,
                    duration                          AS duration,
                    MAX(imported_at)                  AS imported_at
    FROM staging.games
    WHERE userid <> 'UserID'
    GROUP BY gameid,
             userid,
             gametype,
             STR_TO_DATE(playdate, '%d/%m/%Y'),
             duration;

    CREATE INDEX index_temp_in_games_game_id
        ON temp_in_games (game_id);

    INSERT INTO vault.h_game_types(name)
    SELECT DISTINCT game_type
    FROM temp_in_games new
        LEFT JOIN vault.h_game_types old
            ON old.name = new.game_type
    WHERE old.name IS NULL;

    DROP TEMPORARY TABLE IF EXISTS temp_recently_changed;
    CREATE TEMPORARY TABLE temp_recently_changed LIKE gear.recent_changes;

-- _________________________________________________________________ --
    START TRANSACTION;

-- games
    INSERT INTO temp_recently_changed(data_type, data_status, element_id)
    SELECT 'game', 'changed', new.game_id
    FROM temp_in_games AS new
        JOIN vault.h_games old
            ON old.id = new.game_id
        JOIN vault.h_game_types AS game_types
            ON game_types.name = new.game_type
    WHERE (old.type_id <> game_types.id);

    UPDATE vault.h_games old
        JOIN temp_recently_changed AS r_ch
        ON old.id = r_ch.element_id
        JOIN temp_in_games AS new
        ON old.id = new.game_id
        JOIN vault.h_game_types AS game_types
        ON game_types.name = new.game_type
    SET old.type_id = game_types.id
    WHERE (old.type_id <> game_types.id);
    SET @processed_games = @processed_games + ROW_COUNT();

-- ins
    INSERT INTO temp_recently_changed(data_type, data_status, element_id)
    SELECT 'game', 'new', new.game_id
    FROM temp_in_games AS new
        LEFT JOIN vault.h_games AS old
            ON old.id = new.game_id
    WHERE old.id IS NULL;

    INSERT INTO vault.h_games(id, type_id)
    SELECT new.game_id,
           game_types.id AS type_id
    FROM temp_recently_changed AS r_ch
        JOIN temp_in_games AS new
            ON r_ch.data_status = 'new'
            AND new.game_id = r_ch.element_id
        JOIN vault.h_game_types AS game_types
            ON game_types.name = new.game_type;
    SET @processed_games = @processed_games + ROW_COUNT();

    INSERT INTO gear.temp_games(game_id, user_id, game_type, play_date, duration, imported_at)
    SELECT game_id, user_id, game_type, play_date, duration, imported_at
    FROM temp_in_games;

--
    INSERT INTO bulk.processed_games(game_id, user_id, game_type, play_date, duration, imported_at)
    SELECT game_id, user_id, game_type, play_date, duration, imported_at
    FROM temp_in_games;

    TRUNCATE TABLE staging.games;
    COMMIT;

    SELECT MAX(imported_at) AS imported_at
    FROM temp_in_games tig
    INTO @imported_at;

    DROP TEMPORARY TABLE temp_in_games;

    INSERT gear.process_staging_logs(table_name, processed_count, imported_at)
        VALUE ('games', @processed_games, @imported_at);

    INSERT gear.recent_changes(data_type, element_id, data_status)
    SELECT data_type, element_id, data_status
    FROM temp_recently_changed;

    DROP TEMPORARY TABLE temp_recently_changed;

END;

DROP PROCEDURE IF EXISTS gear.exec_link_users_and_games;
CREATE PROCEDURE gear.exec_link_users_and_games()
BEGIN

    INSERT INTO vault.l_users_games_date(user_id, game_id, play_date)
    SELECT new.user_id,
           new.game_id,
           new.play_date
    FROM gear.temp_games new
        LEFT JOIN vault.l_users_games_date old
            ON old.user_id = new.user_id
            AND old.game_id = new.game_id
            AND old.play_date = new.play_date
    WHERE old.id IS NULL;

    DROP TEMPORARY TABLE IF EXISTS temp_recently_changed;
    CREATE TEMPORARY TABLE temp_recently_changed LIKE gear.recent_changes;

    START TRANSACTION ;

    INSERT INTO temp_recently_changed(data_type, data_status, element_id)
    SELECT 'user-game-date', 'changed', old.id
    FROM vault.l_users_games_date old
        JOIN temp_games new
            ON old.user_id = new.user_id
            AND old.game_id = new.game_id
            AND old.play_date = new.play_date
        JOIN vault.s_users_games_date s_upd
            ON s_upd.l_users_games_date_id = old.id
    WHERE s_upd.duration <> new.duration;

    UPDATE vault.s_users_games_date old
        JOIN temp_recently_changed r_ch
        ON old.l_users_games_date_id = r_ch.element_id
        JOIN vault.l_users_games_date AS l
        ON l.id = r_ch.element_id
        JOIN temp_games new
        ON l.user_id = new.user_id
            AND l.game_id = new.game_id
            AND l.play_date = new.play_date
    SET old.duration = new.duration
    WHERE old.duration <> new.duration;

-- ins
    INSERT INTO temp_recently_changed(data_type, data_status, element_id)
    SELECT 'user-game-date', 'new', old.id
    FROM vault.l_users_games_date old
        JOIN temp_games new
            ON old.user_id = new.user_id
            AND old.game_id = new.game_id
            AND old.play_date = new.play_date
        LEFT JOIN vault.s_users_games_date AS s_upd
            ON s_upd.l_users_games_date_id = old.id
    WHERE s_upd.l_users_games_date_id IS NULL;

    INSERT INTO vault.s_users_games_date(l_users_games_date_id, duration)
    SELECT l.id,
           new.duration
    FROM temp_recently_changed AS r_ch
        JOIN vault.l_users_games_date AS l
            ON r_ch.data_status = 'new'
            AND l.id = r_ch.element_id
        JOIN temp_games new
            ON l.user_id = new.user_id
            AND l.game_id = new.game_id
            AND l.play_date = new.play_date;
    COMMIT;

    TRUNCATE gear.temp_games;

    INSERT gear.recent_changes(data_type, element_id, data_status)
    SELECT data_type, element_id, data_status
    FROM temp_recently_changed;

    DROP TEMPORARY TABLE temp_recently_changed;

END;

-- _________________________________________________________________ --

DROP PROCEDURE IF EXISTS gear.exec_process_payments;
CREATE PROCEDURE gear.exec_process_payments()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS temp_in_payments;

    SET @processed_payments = 0;

    CREATE TEMPORARY TABLE temp_in_payments
    SELECT DISTINCT transactionid                            AS transaction_id,
                    userid                                   AS user_id,
                    CAST(amount AS dec(13, 2))               AS amount,
                    STR_TO_DATE(transactiondate, '%d/%m/%Y') AS transaction_date,
                    type,
                    max(imported_at) as imported_at
    FROM staging.payments
    WHERE userid <> 'UserID'
    GROUP BY transactionid,
             userid,
             CAST(amount AS dec(13, 2)),
             STR_TO_DATE(transactiondate, '%d/%m/%Y'),
             type;

    CREATE INDEX index_temp_in_payments_transaction_id
        ON temp_in_payments (transaction_id);

    DROP TEMPORARY TABLE IF EXISTS temp_recently_changed;
    CREATE TEMPORARY TABLE temp_recently_changed LIKE gear.recent_changes;

    START TRANSACTION;

-- payments
    INSERT INTO temp_recently_changed(data_type, data_status, element_id)
    SELECT 'payment', 'changed', new.transaction_id
    FROM temp_in_payments AS new
        JOIN vault.payments old
            ON old.transaction_id = new.transaction_id
    WHERE old.amount <> new.amount OR
          old.type <> new.type;

    UPDATE vault.payments old
        JOIN temp_recently_changed AS r_ch
        ON r_ch.element_id = old.transaction_id
        JOIN temp_in_payments new
        ON old.transaction_id = new.transaction_id
    SET old.amount = new.amount,
        old.type   = new.type
    WHERE old.amount <> new.amount OR
          old.type <> new.type;
    SET @processed_payments = @processed_payments + ROW_COUNT();

-- ins
    INSERT INTO temp_recently_changed(data_type, data_status, element_id)
    SELECT 'payment', 'new', new.transaction_id
    FROM temp_in_payments AS new
        LEFT JOIN vault.payments old
            ON old.transaction_id = new.transaction_id
    WHERE old.transaction_id IS NULL;

    INSERT INTO vault.payments(transaction_id, user_id, amount, transaction_date, type)
    SELECT transaction_id, user_id, amount, transaction_date, type
    FROM temp_recently_changed AS r_ch
        JOIN temp_in_payments new
            ON r_ch.data_status = 'new'
            AND r_ch.element_id = new.transaction_id;
    SET @processed_payments = @processed_payments + ROW_COUNT();
--
    INSERT INTO bulk.processed_payments(transaction_id, user_id, amount, transaction_date, type, imported_at)
    SELECT transaction_id, user_id, amount, transaction_date, type, imported_at
    FROM temp_in_payments;

    TRUNCATE TABLE staging.payments;
    COMMIT;

    SELECT MAX(imported_at) AS imported_at
    FROM temp_in_payments tig
    INTO @imported_at;

    DROP TEMPORARY TABLE temp_in_payments;

    INSERT gear.process_staging_logs(table_name, processed_count, imported_at)
        VALUE ('payments', @processed_payments, @imported_at);


    INSERT gear.recent_changes(data_type, element_id, data_status)
    SELECT data_type, element_id, data_status
    FROM temp_recently_changed;

    DROP TEMPORARY TABLE temp_recently_changed;

END;


-- _________________________________________________________________ --
DROP PROCEDURE IF EXISTS gear.exec_process_staging;
CREATE PROCEDURE gear.exec_process_staging()
BEGIN
    -- import
    CALL gear.exec_process_users();
    CALL gear.exec_process_games();
    CALL gear.exec_link_users_and_games();
    CALL gear.exec_process_payments();

END;

DROP EVENT IF EXISTS gear.ten_minutes_routine_staging;
CREATE EVENT gear.ten_minutes_routine_staging
    ON SCHEDULE EVERY 10 MINUTE
        STARTS '2023-12-01 00:00:01' ON COMPLETION PRESERVE ENABLE DO
    CALL gear.exec_process_staging();
