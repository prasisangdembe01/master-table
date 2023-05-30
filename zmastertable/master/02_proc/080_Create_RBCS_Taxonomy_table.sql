CREATE OR REPLACE PROCEDURE  SP0080_create_RBCS_Taxonomy_table(schema_name TEXT, tbl_name TEXT,logtbl_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
  schema_table TEXT := schema_name || '.' || tbl_name;
  log_table TEXT := schema_name || '.' || logtbl_name;
BEGIN
	-- Log initiation message
		BEGIN
			CALL SP0019_log_initiation(schema_name,logtbl_name,'SP0080_create_RBCS_Taxonomy_table');   
		END;
		BEGIN
			IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = schema_name AND table_name = tbl_name) THEN
				EXECUTE 'TRUNCATE TABLE ' || schema_table;
				-- Log success message
				 CALL SP0016_log_truncated(schema_name,logtbl_name, 'SP0080_create_RBCS_Taxonomy_table'); 
			ELSE
			EXECUTE 'CREATE TABLE ' || schema_table || ' (
				Version VARCHAR(15),
				HCPCS_Cd VARCHAR(15),
				RBCS_ID VARCHAR(15),
				RBCS_Cat VARCHAR(5),
				RBCS_Cat_Desc VARCHAR(50),
				RBCS_Cat_Subcat VARCHAR(50),
				RBCS_Subcat_Desc VARCHAR(500),
				RBCS_FamNumb VARCHAR(10),
				RBCS_Family_Desc VARCHAR(500),
				RBCS_Major_Ind VARCHAR(5),
				HCPCS_Cd_Add_Dt VARCHAR(15),
				HCPCS_Cd_End_Dt VARCHAR(15),
				RBCS_Assignment_Eff_Dt VARCHAR(15),
				RBCS_Assignment_End_Dt VARCHAR(15)
			)';
			-- Log success message
			 call SP0018_log_creation(schema_name,logtbl_name, 'SP0080_create_RBCS_Taxonomy_table');
			END IF;
			EXCEPTION
				WHEN OTHERS THEN
				-- Log error message
				 call SP0017_log_failed(schema_name,logtbl_name, 'SP0080_create_RBCS_Taxonomy_table',SQLERRM);
				RETURN;
			END;
 END;
$$;
call SP0080_create_RBCS_Taxonomy_table('SCHEMANAME','rbcs_codes','re_execution_log');
drop procedure SP0080_create_RBCS_Taxonomy_table;
