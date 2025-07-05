update {name}
set 
    status = %(failed)s,
    failed_at = current_timestamp
where id = %(id)s;
