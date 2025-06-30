-- Consume messages template
-- Select messages ready for processing, respecting delay and status
with ready_messages as (
    select id
    from {name}
    where 
        status = 'NEW' and 
        current_timestamp >= dateadd(minute, delay, inserted_at)
    order by priority desc, inserted_at
    limit :limit
)
update {name} m
set 
    status = 'PROCESSING',
    attempts = attempts + 1,
    last_started_at = current_timestamp
from ready_messages rm
where m.id = rm.id
returning m.*;