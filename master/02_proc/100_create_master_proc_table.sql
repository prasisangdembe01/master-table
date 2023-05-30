
CREATE OR REPLACE PROCEDURE  SP0100_create_master_proc_table(schema_name TEXT, tbl_name TEXT,  logtbl_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
  schema_table TEXT := schema_name || '.' || tbl_name;
   log_table TEXT := schema_name || '.' || logtbl_name;
BEGIN
-- Log initiation message
		BEGIN
			CALL SP0019_log_initiation(schema_name,logtbl_name,'SP0130_create_master_proc_table');   
		END;
		BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = schema_name AND table_name = tbl_name) THEN
    EXECUTE 'TRUNCATE TABLE ' || schema_table;
    -- Log success message
    CALL SP0016_log_truncated(schema_name,logtbl_name, 'SP0130_create_master_proc_table');
  ELSE
  EXECUTE 'CREATE TABLE ' || schema_table || ' (
      Version VARCHAR(20),
    proc_code VARCHAR(20),
    proc_type VARCHAR(100),
    proc_desc VARCHAR(1000),
    proc_short_desc VARCHAR(1000),
    proc_long_desc VARCHAR(2000),
    rbcs_cat_code VARCHAR(10),
    rbcs_cat_desc VARCHAR(500),
    rbcs_subcat_code VARCHAR(10),
    rbcs_subcat_desc VARCHAR(500),
    rbcs_family_code VARCHAR(10),
    rbcs_family_desc VARCHAR(500),
    rbcs_major_ind VARCHAR(10),
    ccs_group_code VARCHAR(10),
    ccs_group_desc VARCHAR(500),
    dh_service_type VARCHAR(500),
    dh_category  VARCHAR(500),
    dh_subcategory VARCHAr(500),
    dh_service VARCHAR(500)
  )';

   -- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0130_create_master_proc_table');
  END IF;
  EXCEPTION
 	 WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0130_create_master_proc_table',SQLERRM);
    RETURN;
    END;
 END;
$$;
call SP0100_create_master_proc_table('SCHEMANAME','master_proc_codes','re_execution_log');
drop procedure SP0100_create_master_proc_table;



