select *
from total_users
where 1=1
[[ and user_id >= {{user_id}} ]]
[[ and user LIKE concat('%', {{user_name}}, '%') ]]
[[ and first_date >= {{first_date_from}} ]]
[[ and last_date <= {{last_date_to}} ]]
order by deposit desc


select *
from daily_payments p
join calendar_dates d
on d.date = p.date
[[ and p.date >= {{date_from}} ]]
[[ and p.date <= {{date_to}} ]]
order by d.day_of_week
