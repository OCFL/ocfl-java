CREATE TABLE IF NOT EXISTS ocfl_object_lock (
  id SERIAL PRIMARY KEY,
  object_id varchar(255) UNIQUE
);