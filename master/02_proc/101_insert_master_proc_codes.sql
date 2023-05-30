
CREATE OR REPLACE PROCEDURE  SP0101_insert_master_proc_codes(schema_name TEXT, tbl_name TEXT,
icd10pcs_tbl_name TEXT,
drgcode_tbl_name TEXT,
revcode_tbl_name TEXT,
hcpcscode_tbl_name TEXT,
cptcode_tbl_name TEXT,
rbcs_tbl_name TEXT,
ccs_tbl_name TEXT,
logtbl_name TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  schema_table  TEXT := schema_name || '.' || tbl_name; --destination schema table
  schema_table1 TEXT := schema_name || '.' || icd10pcs_tbl_name; --source schema table 
  schema_table2 TEXT := schema_name || '.' || drgcode_tbl_name; --source schema table 
  schema_table3 TEXT := schema_name || '.' || revcode_tbl_name; --source schema table 
  schema_table4 TEXT := schema_name || '.' || hcpcscode_tbl_name;--source schema table
  schema_table5 TEXT := schema_name || '.' || cptcode_tbl_name;  --source schema table 
  schema_table6 TEXT := schema_name || '.' || rbcs_tbl_name;  --source schema table 
  schema_table7 TEXT := schema_name || '.' || ccs_tbl_name;  --source schema table 
  log_table TEXT := schema_name || '.' || logtbl_name;

BEGIN
 --log initiation message
		BEGIN
			CALL SP0019_log_initiation(schema_name,logtbl_name, 'SP0131_insert_master_proc_codes');   
		END;
		BEGIN
--inserting codes form icd10pcs,drg,rev,hcpcs,cpt4 table to master table
  EXECUTE 'INSERT INTO ' || schema_table || ' (version, proc_code, proc_type, proc_desc, proc_short_desc, proc_long_desc) ' ||
  'SELECT version, proc_code, ''ICD10'', substring(proc_long_desc,1,150),substring(proc_long_desc,1,150), proc_long_desc ' ||
  'FROM ' || schema_table1 || ';';
  EXECUTE 'INSERT INTO ' || schema_table || ' (version, proc_code, proc_type, proc_desc, proc_short_desc, proc_long_desc) ' ||
  'SELECT version, drg_code, ''DRG'', substring(drg_long_desc,1,150),substring(drg_long_desc,1,150), drg_long_desc ' ||
  'FROM ' || schema_table2 || ';';
  EXECUTE 'INSERT INTO ' || schema_table || ' (version, proc_code, proc_type, proc_desc, proc_short_desc, proc_long_desc) ' ||
  'SELECT version, proc_code, ''REV'',proc_desc,short_desc_final,long_desc_final ' ||
  'FROM ' || schema_table3 || ';';
  EXECUTE 'INSERT INTO ' || schema_table || ' (version, proc_code, proc_type, proc_desc, proc_short_desc, proc_long_desc) ' ||
  'SELECT version, hcpcs, ''HCPCS'',short_desc, short_desc,long_desc ' ||
  'FROM ' || schema_table4 || ';';
 EXECUTE 'INSERT INTO ' || schema_table || ' (version, proc_code, proc_type, proc_desc, proc_short_desc, proc_long_desc) ' ||
  'SELECT version, proc_code, ''CPT4'',proc_short_desc, proc_short_desc,proc_long_desc ' ||
  'FROM ' || schema_table5 || ';';
-- for rbcs_caregorization
--  EXECUTE 'UPDATE ' || schema_table || ' AS mpt
--     SET rbcs_cat_code = rt.rbcs_cat, 
--         rbcs_cat_desc = rt.rbcs_cat_desc, 
--         rbcs_subcat_code = rt.rbcs_cat_subcat, 
--         rbcs_subcat_desc = rt.rbcs_subcat_desc, 
--         rbcs_family_code = rt.rbcs_famnumb, 
--         rbcs_family_desc = rt.rbcs_family_desc, 
--         rbcs_major_ind = rt.rbcs_major_ind
--     FROM ' || schema_table6 || ' AS rt
--     WHERE mpt.proc_code = rt.hcpcs_cd AND mpt.proc_type IN (''CPT4'', ''HCPCS'')';

--  EXECUTE 'UPDATE ' || schema_table || ' AS mpt
--     SET rbcs_family_desc = ''--To be categorized--''
--     WHERE mpt.rbcs_family_desc = ''No RBCS Family'' AND mpt.proc_type IN (''CPT4'', ''HCPCS'')';

    EXECUTE 'UPDATE ' || schema_table || ' AS mpt
    SET rbcs_cat_code = rt.rbcs_cat, 
        rbcs_cat_desc = rt.rbcs_cat_desc, 
        rbcs_subcat_code = rt.rbcs_cat_subcat, 
        rbcs_subcat_desc = rt.rbcs_subcat_desc, 
        rbcs_family_code = rt.rbcs_famnumb, 
        rbcs_family_desc = CASE 
                              WHEN rt.rbcs_family_desc = ''No RBCS Family'' THEN ''--To be categorized--''
                              ELSE rt.rbcs_family_desc 
                          END,
        rbcs_major_ind = rt.rbcs_major_ind
    FROM ' || schema_table6 || ' AS rt
    WHERE mpt.proc_code = rt.hcpcs_cd AND mpt.proc_type IN (''CPT4'', ''HCPCS'')';


 --for css_categorization       
 EXECUTE 'UPDATE ' || schema_table || ' AS mpt
    SET ccs_group_code=cs.ccs_id, 
        ccs_group_desc=cs.proc_desc
    FROM ' || schema_table7 || ' AS cs
    WHERE mpt.proc_code = cs.proc_code AND mpt.proc_type IN (''CPT4'', ''HCPCS'')';

     -- Log success message
 call SP0018_log_creation(schema_name,logtbl_name, 'SP0131_insert_master_proc_codes');
  
  EXCEPTION
 	 WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0131_insert_master_proc_codes',SQLERRM);
    RETURN;
    END;
 
 END;
$$;
call SP0101_insert_master_proc_codes('SCHEMANAME','master_proc_codes','icd10pcs_codes','drg_codes','rev_codes','hcpcs_codes','cpt_codes','rbcs_codes','ccs_codes','re_execution_log');
drop procedure SP0101_insert_master_proc_codes;
