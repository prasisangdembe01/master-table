
CREATE OR REPLACE PROCEDURE SP0010_create_log_table(
  schema_name TEXT, 
  tbl_name TEXT,
  logtbl_name TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  schema_table TEXT := schema_name || '.' || tbl_name;
  log_table TEXT := schema_name || '.' || logtbl_name;
BEGIN
  -- Check if table exists
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = schema_name AND table_name = tbl_name) THEN
    -- Log success message
    EXECUTE 'INSERT INTO ' || log_table || ' (timestamp, remarks, logtype) VALUES (NOW(), ''SP0010_create_log_table already Exists.'', ''Success'')';
    ELSE
      EXECUTE 'CREATE TABLE ' || schema_table || ' (
        SN SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        remarks VARCHAR(5000),
        logtype VARCHAR(50)
      )';
    -- Log success message
    EXECUTE 'INSERT INTO ' || log_table || ' (timestamp, remarks, logtype) VALUES (NOW(), ''SP0010_create_log_table  created successfully.'', ''Success'')';
    END IF;

   EXCEPTION
    WHEN OTHERS THEN
      -- Log error message
      EXECUTE 'INSERT INTO ' || log_table || ' (timestamp, remarks, logtype) VALUES (NOW(), ''Error  SP0010_create_log_table  Creation: ' || SQLERRM || ''', ''Error'')';
      RETURN;
END;
$$;
call SP0010_create_log_table('SCHEMANAME','re_execution_log','re_execution_log');
drop procedure SP0010_create_log_table;