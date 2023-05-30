CREATE OR REPLACE PROCEDURE SP0300_create_ndc_table(schema_name text, table_name text, logtbl_name text)
LANGUAGE plpgsql
AS $$
BEGIN
-- Log initiation message
  BEGIN
    CALL SP0019_log_initiation(schema_name,logtbl_name, 'SP0300_create_ndc_table');    
  END;
BEGIN
IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = schema_name AND tablename = table_name) THEN
  EXECUTE format('TRUNCATE TABLE  %I.%I', schema_name, table_name);
    -- Log truncate message
  call SP0016_log_truncated(schema_name,logtbl_name, 'SP0300_create_ndc_table');
  ELSE
  EXECUTE format('CREATE TABLE %I.%I
    (
      version text not null default ''2023MAR'',
      PRODUCTID TEXT,
      PRODUCTNDC TEXT,
      PRODUCTTYPENAME TEXT,
      PROPRIETARYNAME TEXT,
      PROPRIETARYNAMESUFFIX TEXT,
      NONPROPRIETARYNAME TEXT,
      DOSAGEFORMNAME TEXT,
      ROUTENAME TEXT,
      STARTMARKETINGDATE TEXT,
      ENDMARKETINGDATE TEXT,
      MARKETINGCATEGORYNAME TEXT,
      APPLICATIONNUMBER TEXT,
      LABELERNAME text,
      SUBSTANCENAME TEXT,
      ACTIVE_NUMERATOR_STRENGTH TEXT,
      ACTIVE_INGRED_UNIT TEXT,
      PHARM_CLASSES TEXT,
      DEASCHEDULE TEXT,
      NDC_EXCLUDE_FLAG TEXT,
      LISTING_RECORD_CERTIFIED_THROUGH TEXT,
      updatedby timestamp default current_timestamp
    )', schema_name, table_name);
    -- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0300_create_ndc_table');
END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0300_create_ndc_table',SQLERRM);
    RETURN;
END;
END;
$$;
call SP0300_create_ndc_table ('SCHEMANAME', 'master_ndc_code','re_execution_log');
drop procedure SP0300_create_ndc_table;

    








