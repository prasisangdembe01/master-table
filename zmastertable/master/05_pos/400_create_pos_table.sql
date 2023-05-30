CREATE OR REPLACE PROCEDURE SP0400_create_pos_tbl(schema_name text, table_name text,logtbl_name text)
LANGUAGE plpgsql
AS $$
BEGIN
-- Log initiation message
  BEGIN
    CALL SP0019_log_initiation(schema_name,logtbl_name, 'SP0400_create_pos_tbl');    
  END;
BEGIN
IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = schema_name AND tablename = table_name) THEN
  EXECUTE format('Truncate TABLE  %I.%I', schema_name, table_name);
  -- Log truncate message
     call SP0016_log_truncated(schema_name,logtbl_name, 'SP0400_create_pos_table');
  ELSE
  EXECUTE format('CREATE TABLE %I.%I
    (
      version text not null default ''2023MAR'',
      POS_CODE int,
      POS_NAME text,
      POS_DESC_LONG text,
      updatedby timestamp default current_timestamp
    )', schema_name, table_name);
    -- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0400_create_pos_tbl');
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0400_create_pos_tbl',SQLERRM);
    RETURN;
    end;
    END;
    $$;

call SP0400_create_pos_tbl('SCHEMANAME','master_pos_code','re_execution_log');
drop procedure SP0400_create_pos_tbl;
