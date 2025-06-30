insert into {dlq}
select *
from {mq}
where attempts >= max_attempts;

delete from {mq}
where attempts >= max_attempts;
