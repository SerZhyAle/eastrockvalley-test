CREATE schema sandbox;

use staging;

drop table if exists test;
create table test(
	id int,
    name varchar(200),
    created_at             datetime(6)       NOT NULL DEFAULT NOW(6) COMMENT 'Created -datetime(6)'
);

select * from staging.test t;


select * from staging.games g;
select * from staging.payments p;
select * from staging.users u;


select * from vault.payments p;

select * from vault.h_game_types hgt;


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
    GROUP BY lugd.play_date, g.id;

select game_id,
count(*) as c
 from vault.l_users_games_date lugd
group by game_id;

SELECT game_id,
count(*) as c
from bulk.processed_games pg
group by game_id;


select type, dates, first_date, last_date, active_users, middle_users_daily, active_countries, middle_active_countries, transactions, middle_transactions_daily, deposit, middle_deposit_daily, deposits, middle_deposits_daily, withdrawal, middle_withdrawal_daily, withdrawals, middle_withdrawals_daily
from bus.total_payments tp
