CREATE OR REPLACE PROCEDURE SP0209_create_ccsr_table(schema_name text, table_name text,logtbl_name text)
LANGUAGE plpgsql
AS $$
BEGIN
-- Log initiation message
        BEGIN
            CALL SP0019_log_initiation(schema_name,logtbl_name,'SP0209_create_ccsr_table');   
        END;
BEGIN
IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = schema_name AND tablename = table_name) THEN
 execute format('TRUNCATE TABLE IF EXISTS %I.%I', schema_name, table_name);
-- Log truncate message
     call SP0016_log_truncated(schema_name,logtbl_name, 'SP0209_create_ccsr_table');
ELSE
execute format('CREATE TABLE IF NOT EXISTS %I.%I ( ICD_10_CM_CODE varchar(80),ICD_10_CM_CODE_DESCRIPTION varchar(2000), Default_CCSR_CATEGORY_IP varchar(100), Default_CCSR_CATEGORY_DESCRIPTION_IP varchar(2000),Default_CCSR_CATEGORY_OP varchar(100),
               Default_CCSR_CATEGORY_DESCRIPTION_OP varchar(2000),CCSR_CATEGORY_1 varchar(100),CCSR_CATEGORY_1_DESCRIPTION varchar(2000),CCSR_CATEGORY_2 varchar(100),CCSR_CATEGORY_2_DESCRIPTION varchar(2000))', schema_name, table_name); 
-- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0209_create_ccsr_table');
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0209_create_ccsr_table',SQLERRM);
    RETURN;
END;
END;
$$;
call SP0209_create_ccsr_table('SCHEMANAME', 'ccsr_diag','re_execution_log');
drop procedure SP0209_create_ccsr_table;