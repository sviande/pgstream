-- Revert to original event triggers (without type support)

DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_create_alter_table;
CREATE EVENT TRIGGER pgstream_log_schema_create_alter_table
  ON ddl_command_end
  EXECUTE FUNCTION pgstream.log_schema();

DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_drop_schema_table;
CREATE EVENT TRIGGER pgstream_log_schema_drop_schema_table
  ON sql_drop
  WHEN TAG IN ('DROP TABLE', 'DROP SCHEMA')
  EXECUTE FUNCTION pgstream.log_schema();
