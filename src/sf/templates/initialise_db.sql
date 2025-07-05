create table {exists} {name} (
  id varchar primary key,
  message_type varchar,
  payload {db_json_type},
  status varchar,
  priority varchar,
  delay int,
  attempts int,
  max_attempts int,
  inserted_at timestamp,
  last_started_at timestamp,
  completed_at timestamp,
  failed_at timestamp
);

create table {exists} {dlq} (
  id varchar primary key,
  message_type varchar,
  payload {db_json_type},
  status varchar,
  priority varchar,
  delay int,
  attempts int,
  max_attempts int,
  inserted_at timestamp,
  last_started_at timestamp,
  completed_at timestamp,
  failed_at timestamp
);
