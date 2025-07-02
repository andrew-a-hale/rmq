update {name}
set status = %(failed)s
where status = %(processing)s;

insert into {dlq}
select *
from {name}
where attempts >= max_attempts and status != %(completed)s;

delete from {name}
where attempts >= max_attempts and status != %(completed)s;
