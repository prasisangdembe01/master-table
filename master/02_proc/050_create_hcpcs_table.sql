CREATE OR REPLACE PROCEDURE SP0050_create_hcpcs_table(
  schema_name TEXT, 
  tbl_name TEXT,
  logtbl_name TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  log_table TEXT := schema_name || '.' || logtbl_name;
BEGIN
    		-- Log initiation message
		BEGIN
			CALL SP0019_log_initiation(schema_name,logtbl_name,'SP0050_create_hcpcs_table');   
		END;
		BEGIN
  -- Check if table exists
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = schema_name AND table_name = tbl_name) THEN
    EXECUTE format('TRUNCATE TABLE %I.%I', schema_name, tbl_name);
    -- Log success message
   CALL SP0016_log_truncated(schema_name,logtbl_name, 'SP0050_create_hcpcs_table'); 
   ELSE
    EXECUTE format('CREATE TABLE %I.%I (version varchar(20) ,hcpcs varchar(20),seqnum int, recid int,long_desc varchar(2000),short_desc varchar(1000))', schema_name, tbl_name);
    -- Log success message
    call SP0018_log_creation(schema_name,logtbl_name, 'SP0050_create_hcpcs_table');
  END IF;
  EXCEPTION
    WHEN OTHERS THEN
      -- Log error message
       call SP0017_log_failed(schema_name,logtbl_name, 'SP0050_create_hcpcs_table',SQLERRM);
      RETURN;
      END;
END;
$$; 
call SP0050_create_hcpcs_table('SCHEMANAME','hcpcs_codes','re_execution_log');
drop procedure SP0050_create_hcpcs_table;

   
   