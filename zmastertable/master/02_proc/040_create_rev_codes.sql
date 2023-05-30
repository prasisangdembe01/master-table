


CREATE OR REPLACE PROCEDURE SP0040_create_rev_codes(
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
			CALL SP0019_log_initiation(schema_name,logtbl_name,'SP0040_create_rev_codes');   
		END;
		BEGIN
  -- Check if table exists
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = schema_name AND table_name = tbl_name) THEN
    EXECUTE format('TRUNCATE TABLE %I.%I', schema_name, tbl_name);
    -- Log success message for table truncation
    CALL SP0016_log_truncated(schema_name,logtbl_name, 'SP0040_create_rev_codes'); 
   ELSE
    EXECUTE format('CREATE TABLE %I.%I (version varchar(20), proc_code varchar(50), proc_desc varchar(500), short_desc_final varchar (500),long_desc_final varchar(1000))', schema_name, tbl_name);
    -- Log success message
    call SP0018_log_creation(schema_name,logtbl_name, 'SP0040_create_rev_codes');
  END IF;
  EXCEPTION
    WHEN OTHERS THEN
      -- Log error message
      call SP0017_log_failed(schema_name,logtbl_name, 'SP0040_create_rev_codes',SQLERRM);
      RETURN;
      END;
END;
$$; 
call SP0040_create_rev_codes('SCHEMANAME', 'rev_codes','re_execution_log');
drop PROCEDURE SP0040_create_rev_codes;

