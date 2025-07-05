update {name}
set 
    status = %(processing)s,
    attempts = {name}.attempts + 1,
    last_started_at = current_timestamp
from (
    select id
    from {name}
    where status = %(failed)s
    order by priority desc, inserted_at desc
    limit %(n)s
) rm
where {name}.id = rm.id;


select *
from {name}
where status = %(processing)s
limit %(n)s;
