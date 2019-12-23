CREATE TABLE IF NOT EXISTS ocfl_object_details (
  object_id varchar(255) PRIMARY KEY,
  version_id varchar(255) NOT NULL,
  object_root_path varchar(1024) NOT NULL,
  revision_id varchar(255),
  inventory_digest varchar(128) NOT NULL,
  digest_algorithm varchar(32) NOT NULL,
  inventory bytea,
  update_timestamp timestamptz NOT NULL
);