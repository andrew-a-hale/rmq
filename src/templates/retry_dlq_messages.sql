update {dlq}
set status = 'MOVING', attempts = 0
from (
    select id
    from {dlq}
    where status = %(failed)s
    order by priority desc, inserted_at desc
    limit %(n)s
) retry
where retry.id = {dlq}.id;

insert into {name}
select
    id,
    message_type,
    payload,
    %(processing)s,
    priority,
    delay,
    attempts,
    max_attempts,
    inserted_at,
    current_timestamp,
    null,
    null
from {dlq}
where status = 'MOVING';

select
    id,
    message_type,
    payload,
    %(processing)s,
    priority,
    delay,
    attempts + 1 as attempts,
    max_attempts,
    inserted_at,
    current_timestamp,
    null,
    null
from {name}
where status = %(processing)s;

delete from {dlq}
where status = 'MOVING';

