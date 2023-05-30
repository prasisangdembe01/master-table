CREATE OR REPLACE PROCEDURE SP0200_create_diagnosis_table(schema_name text, table_name text, logtbl_name text)
LANGUAGE plpgsql
AS $$
BEGIN
-- Log initiation message
  BEGIN
    CALL SP0019_log_initiation(schema_name,logtbl_name, 'SP0200_create_diagnosis_table');    
  END;
BEGIN
IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = schema_name AND tablename = table_name) THEN
execute format('TRUNCATE TABLE  %I.%I', schema_name, table_name);
 -- Log truncate message
     call SP0016_log_truncated(schema_name,logtbl_name, 'SP0200_create_diagnosis_table');
ELSE
execute format('CREATE TABLE IF NOT EXISTS %I.%I ( version varchar(20) default ''2023APR'', diag_code varchar(20), diag_desc varchar(2000),chronic_indicator int)', schema_name, table_name);  
-- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0200_create_diagnosis_table');
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0200_create_diagnosis_table',SQLERRM);
    RETURN;
END;
END;
$$;
call SP0200_create_diagnosis_table('SCHEMANAME', 'diag_table','re_execution_log');
drop procedure SP0200_create_diagnosis_table;