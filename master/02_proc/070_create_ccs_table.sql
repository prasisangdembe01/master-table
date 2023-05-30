
CREATE OR REPLACE PROCEDURE  SP0070_create_ccs_table(schema_name TEXT, tbl_name TEXT,logtbl_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
  schema_table TEXT := schema_name || '.' || tbl_name;
  log_table TEXT := schema_name || '.' || logtbl_name;
BEGIN
  	-- Log initiation message
		BEGIN
			CALL SP0019_log_initiation(schema_name,logtbl_name,'SP0070_create_ccs_table');   
		END;
		BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = schema_name AND table_name = tbl_name) THEN
    EXECUTE 'TRUNCATE TABLE ' || schema_table;
    -- Log success message
    CALL SP0016_log_truncated(schema_name,logtbl_name, 'SP0070_create_ccs_table'); 
  ELSE
  EXECUTE 'CREATE TABLE ' || schema_table || ' (
    version VARCHAR(15),
    proc_code VARCHAR(15),
	ccs_id varchar(10),
    proc_desc VARCHAR(500)
  )';
   -- Log success message
   call SP0018_log_creation(schema_name,logtbl_name, 'SP0070_create_ccs_table');
  END IF;
  EXCEPTION
    WHEN OTHERS THEN
    -- Log error message
   call SP0017_log_failed(schema_name,logtbl_name, 'SP0070_create_ccs_table',SQLERRM);
    RETURN;
    END;
 END;
$$;
call SP0070_create_ccs_table('SCHEMANAME','ccs_codes','re_execution_log');
drop procedure  SP0070_create_ccs_table;