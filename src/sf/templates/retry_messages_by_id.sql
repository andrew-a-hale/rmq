update {name}
set 
    status = %(processing)s,
    attempts = {name}.attempts + 1,
    last_started_at = current_timestamp
from (
    select id
    from {name}
    where
        status = %(failed)s
        and id in (%(ids)s)
) rm
where {name}.id = rm.id;

select *
from {name}
where
    status = %(processing)s
    and id in (%(ids)s);
