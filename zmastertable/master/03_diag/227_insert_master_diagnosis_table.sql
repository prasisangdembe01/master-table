CREATE OR REPLACE PROCEDURE SP0227_insert_master_diagnosis_table(schema_name text, table_name text , diag_table text, ccsr_table text,logtbl_name text)
LANGUAGE plpgsql
AS $$
declare
schematable text := schema_name || '.' || table_name;
schemadiag text := schema_name || '.' || diag_table ;
schemaccsr text := schema_name || '.' || ccsr_table ;
BEGIN
-- Log initiation message
        BEGIN
            CALL SP0019_log_initiation(schema_name,logtbl_name,'SP0227_insert_master_diagnosis_table');   
        END;
BEGIN
 EXECUTE 'INSERT INTO ' || schematable || ' (diag_code, diag_desc, chronic_indicator) ' ||
  'SELECT diag_code, diag_desc, chronic_indicator ' ||
  'FROM ' || schemadiag || ';';

EXECUTE 'UPDATE ' || schematable || ' AS mdt
    SET CCSR_CATEGORY_1 = ccsr.CCSR_CATEGORY_1, 
        CCSR_CATEGORY_1_DESCRIPTION = ccsr.CCSR_CATEGORY_1_DESCRIPTION, 
        CCSR_CATEGORY_2 = ccsr.CCSR_CATEGORY_2, 
        CCSR_CATEGORY_2_DESCRIPTION = ccsr.CCSR_CATEGORY_2_DESCRIPTION
    FROM ' || schemadiag || ' AS diag
    JOIN ' || schemaccsr || ' AS ccsr
    ON diag.diag_code = ccsr.ICD_10_CM_CODE
    WHERE mdt.diag_code = diag.diag_code';
 -- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0227_insert_master_diagnosis_table');
  EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0227_insert_master_diagnosis_table',SQLERRM);
    RETURN;
END;
END;
$$;
call SP0227_insert_master_diagnosis_table('SCHEMANAME','master_diagnosis_table','diag_table','ccsr_diag','re_execution_log');
DROP PROCEDURE  SP0227_insert_master_diagnosis_table;