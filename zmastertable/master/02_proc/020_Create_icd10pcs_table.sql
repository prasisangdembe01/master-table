--call SP0020_insert_icd10pcs_codes('Schema_name','table_Name');

CREATE OR REPLACE PROCEDURE  SP0020_create_master_icd10pcs_table(schema_name TEXT, tbl_name TEXT,logtbl_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
  schema_table TEXT := schema_name || '.' || tbl_name;
  log_table TEXT := schema_name || '.' || logtbl_name;
BEGIN
  -- Log initiation message
  BEGIN
    CALL SP0019_log_initiation(schema_name,logtbl_name, 'SP0020_create_master_icd10pcs_table');    
  END;
  BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = schema_name AND table_name = tbl_name) THEN
    EXECUTE 'TRUNCATE TABLE ' || schema_table;
     -- Log truncate message
     call SP0016_log_truncated(schema_name,logtbl_name, 'SP0020_create_master_icd10pcs_table');
   ELSE
    EXECUTE 'CREATE TABLE ' || schema_table || ' (
    version VARCHAR(15),
    proc_code VARCHAR(15),
    proc_long_desc VARCHAR(500)
  )';
  -- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0020_create_master_icd10pcs_table');
  END IF;
  EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0020_create_master_icd10pcs_table',SQLERRM);
    RETURN;
    END;
 END;
$$;
call SP0020_create_master_icd10pcs_table('SCHEMANAME','icd10pcs_codes','re_execution_log');
drop procedure  SP0020_create_master_icd10pcs_table;

