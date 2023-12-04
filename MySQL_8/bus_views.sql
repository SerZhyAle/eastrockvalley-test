USE bus;

CREATE OR REPLACE VIEW bus.users AS
SELECT hu.id as user_id,
       hu.name as user,
       hu.signup_date,
       hc.name AS country,
       tu.dates,
       tu.first_date,
       tu.last_date,
       tu.active_games,
       tu.middle_games_daily,
       tu.active_game_types,
       tu.middle_game_types_daily,
       tu.transactions,
       tu.middle_transactions_daily,
       tu.deposit,
       tu.middle_deposit_daily,
       tu.deposits,
       tu.middle_deposits_daily,
       tu.withdrawal,
       tu.middle_withdrawal_daily,
       tu.withdrawals,
       tu.middle_withdrawals_daily,
       tu.duration,
       tu.middle_duration_daily,
       hu.created_at,
       hu.updated_at
FROM vault.h_users hu
    JOIN vault.h_countries hc
        ON hu.country_id = hc.id
    LEFT JOIN bus.total_users tu
        ON hu.id = tu.user_id
;

SELECT user_id,
       user,
       signup_date,
       country,
       dates,
       first_date,
       last_date,
       active_games,
       middle_games_daily,
       active_game_types,
       middle_game_types_daily,
       transactions,
       middle_transactions_daily,
       deposit,
       middle_deposit_daily,
       deposits,
       middle_deposits_daily,
       withdrawal,
       middle_withdrawal_daily,
       withdrawals,
       middle_withdrawals_daily,
       duration,
       middle_duration_daily,
       created_at,
       updated_at
FROM bus.users;
-- _________________________________________________________________ --

CREATE OR REPLACE VIEW bus.games_users_dates AS
SELECT hg.id         AS game_id,
       hgt.name      AS game_type,
       hg.created_at AS game_created_at,
       hu.id         AS user_id,
       hu.name       AS user_name,
       hu.created_at AS user_created_at,
       hu.signup_date,
       lugd.play_date,
       sugd.duration
FROM vault.l_users_games_date lugd
    JOIN vault.s_users_games_date sugd
        ON lugd.id = sugd.l_users_games_date_id
    JOIN vault.h_users hu
        ON hu.id = lugd.user_id
    JOIN vault.h_games hg
        ON hg.id = lugd.game_id
    JOIN vault.h_game_types hgt
        ON hgt.id = hg.type_id;

SELECT game_id,
       game_type,
       game_created_at,
       user_id,
       user_name,
       user_created_at,
       signup_date,
       play_date,
       duration
FROM bus.games_users_dates;

-- _________________________________________________________________ --

CREATE OR REPLACE VIEW bus.payments AS
SELECT p.transaction_id,
       p.user_id,
       p.amount,
       p.transaction_date,
       p.type,
       p.created_at,
       p.updated_at,
       hu.name as user,
       hu.signup_date,
       hc.name AS country
FROM vault.payments p
    JOIN vault.h_users hu
        ON p.user_id = hu.id
    JOIN vault.h_countries hc
        ON hu.country_id = hc.id;

SELECT transaction_id, user_id, amount, transaction_date, type, created_at, updated_at, user, signup_date, country
FROM bus.payments;
