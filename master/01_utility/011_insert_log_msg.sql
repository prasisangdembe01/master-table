--------------------------log_truncate--------------------------
----------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP0016_log_truncated(schema_name text, logtbl_name text, procedure_name text)
LANGUAGE plpgsql
AS $$
DECLARE
  log_table TEXT := schema_name || '.' || logtbl_name;
  log_remarks TEXT := procedure_name || ' truncated Successfully';
BEGIN
  EXECUTE 'INSERT INTO ' || log_table || ' (timestamp, remarks, logtype) VALUES (NOW(), ''' || log_remarks || ''', ''Success'')';
END;
$$;

-------------------------log_failed-------------------------------
------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP0017_log_failed(schema_name text, logtbl_name text, procedure_name text,sqlerr_msg TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
  log_table TEXT := schema_name || '.' || logtbl_name;
  log_remarks TEXT := procedure_name || ' Failed ERROR: '||sqlerr_msg;
BEGIN
  EXECUTE 'INSERT INTO ' || log_table || ' (timestamp, remarks, logtype) VALUES (NOW(), ''' || log_remarks || ''', ''Error'')';
END;
$$;

------------------------table_creation------------------------------
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP0018_log_creation(schema_name text, logtbl_name text, procedure_name text)
LANGUAGE plpgsql
AS $$
DECLARE
  log_table TEXT := schema_name || '.' || logtbl_name;
  log_remarks TEXT := procedure_name || ' completed';
BEGIN
  EXECUTE 'INSERT INTO ' || log_table || ' (timestamp, remarks, logtype) VALUES (NOW(), ''' || log_remarks || ''', ''Success'')';
END;
$$;

------------------------table initialization------------------------
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP0019_log_initiation(schema_name text, logtbl_name text, procedure_name text)
LANGUAGE plpgsql
AS $$
DECLARE
  log_table TEXT := schema_name || '.' || logtbl_name;
  log_remarks TEXT := procedure_name || ' initiated';
BEGIN
  EXECUTE 'INSERT INTO ' || log_table || ' (timestamp, remarks, logtype) VALUES (NOW(), ''' || log_remarks || ''', ''initiated'')';
END;
$$;


