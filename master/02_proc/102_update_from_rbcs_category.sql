
CREATE OR REPLACE PROCEDURE  SP0102_update_form_rbcs_category(schema_name TEXT, tbl_name TEXT,
rbcs_tbl_name TEXT,logtbl_name TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  schema_table  TEXT := schema_name || '.' || tbl_name; --destination schema table
  schema_table1 TEXT := schema_name || '.' || rbcs_tbl_name;  --source schema table 
  log_table TEXT := schema_name || '.' || logtbl_name;

BEGIN
 --log initiation message
		BEGIN
			CALL SP0019_log_initiation(schema_name,logtbl_name, 'SP0102_update_form_rbcs_category');   
		END;
		BEGIN
    --  EXECUTE 'UPDATE ' || schema_table || '
    --   SET dh_category = mpt.rbcs_cat_desc,
    --       dh_subcategory = mpt.rbcs_subcat_desc,
    --       dh_service = mpt.rbcs_family_desc
    --   FROM ' || schema_table || ' AS mpt
    --   WHERE mpt.proc_type IN (''CPT4'', ''HCPCS'')';


    EXECUTE 'UPDATE ' || schema_table || ' AS mpt
    SET  
        dh_category = rt.rbcs_cat_desc, 
        dh_subcategory = rt.rbcs_subcat_desc,  
        dh_service = CASE 
                              WHEN rt.rbcs_family_desc = ''No RBCS Family'' THEN ''--To be categorized--''
                              ELSE rt.rbcs_family_desc 
                          END
    FROM ' || schema_table1 || ' AS rt
    WHERE mpt.proc_code = rt.hcpcs_cd AND mpt.proc_type IN (''CPT4'', ''HCPCS'')';

		  -- Log success message
   call SP0018_log_creation(schema_name,logtbl_name, 'SP0102_update_form_rbcs_category');
  EXCEPTION
 	 WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0102_update_form_rbcs_category',SQLERRM);
    RETURN;
	END;
END;
$$;
call SP0102_update_form_rbcs_category('SCHEMANAME','master_proc_codes','rbcs_codes','re_execution_log');
drop procedure SP0102_update_form_rbcs_category;