update {name}
set 
    status = %(completed)s,
    completed_at = current_timestamp
where id = %(id)s;
