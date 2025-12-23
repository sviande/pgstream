-- Extend event triggers to capture CREATE TYPE, ALTER TYPE, and DROP TYPE events
-- This allows pgstream to track enum type changes for replication

DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_create_alter_table;
CREATE EVENT TRIGGER pgstream_log_schema_create_alter_table
  ON ddl_command_end
  WHEN TAG IN ('CREATE SCHEMA', 'CREATE TABLE', 'ALTER TABLE', 'CREATE TYPE', 'ALTER TYPE')
  EXECUTE FUNCTION pgstream.log_schema();

DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_drop_schema_table;
CREATE EVENT TRIGGER pgstream_log_schema_drop_schema_table
  ON sql_drop
  WHEN TAG IN ('DROP TABLE', 'DROP SCHEMA', 'DROP TYPE')
  EXECUTE FUNCTION pgstream.log_schema();
