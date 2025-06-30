-- Publish messages template
-- Inserts new messages into the queue with initial status and timestamp
insert into {name} (
    id, 
    message_type, 
    data, 
    status, 
    priority, 
    delay, 
    attempts, 
    max_attempts, 
    inserted_at
)
values (
    :id, 
    :message_type, 
    :data, 
    'NEW', 
    :priority, 
    :delay, 
    0, 
    :max_attempts, 
    current_timestamp
);