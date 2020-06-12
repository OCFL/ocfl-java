CREATE TABLE IF NOT EXISTS ocfl_object_details (
  object_id varchar(1024) PRIMARY KEY NOT NULL,
  version_id varchar(255) NOT NULL,
  object_root_path varchar(2048) NOT NULL,
  revision_id varchar(255),
  inventory_digest varchar(255) NOT NULL,
  digest_algorithm varchar(255) NOT NULL,
  inventory bytea,
  update_timestamp timestamp with time zone NOT NULL
);