CREATE OR REPLACE PROCEDURE SP0600_create_provider_taxonomy_table(schema_name text, table_name text,logtbl_name text)
LANGUAGE plpgsql
AS $$
BEGIN
-- Log initiation message
  BEGIN
    CALL SP0019_log_initiation(schema_name,logtbl_name, 'SP0600_create_provider_taxonomy_table');    
  END;
BEGIN 
IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = schema_name AND tablename = table_name) THEN
execute format('truncate table  %I.%I',schema_name,table_name);
   -- Log truncate message
     call SP0016_log_truncated(schema_name,logtbl_name, 'SP0600_create_provider_taxonomy_table');
 ELSE
 EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (version varchar(20) default ''BEF2023'',Taxonomy_Code varchar(200),Taxonomy_Grouping varchar(1000),Taxonomy_Specialization varchar(1000),Provider_Category varchar(1000))', schema_name, table_name);
 -- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0600_create_provider_taxonomy_table');
END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0600_create_provider_taxonomy_table',SQLERRM);
    RETURN;
END;
END;
$$;

call SP0600_create_provider_taxonomy_table ('SCHEMANAME','provider_taxonomy_table','re_execution_log');
drop procedure SP0600_create_provider_taxonomy_table(text,text,text);


