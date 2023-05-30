CREATE OR REPLACE PROCEDURE SP0104_update_from_cpt_hcpcs_codes(
  schema_name TEXT,
  table_name TEXT,
  logtbl_name TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  schema_table TEXT := schema_name || '.' || table_name;
  log_table TEXT := schema_name || '.' || logtbl_name;
  category_array TEXT[][] := ARRAY[ 
     --dh_category                      dh_subcategory                                      dh_service									 proc_code

     ARRAY['Anesthesia',              'Anesthesia',                                    '--To be categorized--',                         '{01939,01938,01937,01942,01941,01940}'],
     ARRAY['E&M',                     'Care Management/Coordination',                  '--To be categorized--',                         '{S0220,T1005,S0221}'],
     ARRAY['E&M',                     'Care Management/Coordination',                  'Personal Care Services',                        '{T1020,T1019}'],
     ARRAY['E&M',                     'Critical Care Services',                        '--To be categorized--',                         '{99486,99485}'],
     ARRAY['E&M',                     'E&M - Miscellaneous',                           '--To be categorized--',                         '{Q9003,Q9002,Q9001,Q9004}'],
     ARRAY['E&M',                     'Home Services',                                 '--To be categorized--',                         '{94774,G0077,G0078,G0079,G0086,G0087,G9978,G9979,G9981,G9984,G9985,G9986,Q5001,
																																		  Q5002,Q5009,S9501,S9502,S9503,S9504,S9537,S9538,S9542,S9558,S9559,S9560,S9562,
																																	      S9590,S9810,T1021,T1022,T1028,T1029,T1030,T1031,T2047,99502,99501,99503,99512,
																																		  99510,99506,99504,99500,S0270,S0272,S0273,S0274,S3601,S5035,S5036,S5108,S5109,
																																		  S5110,S5111,S5115,S5116,S5120,S5121,S5125,S5126,S5130,S5131,S5135,S5136,S5150,
																																		  S5151,S5170,S5175,S5180,S5181,S5185,S5199,S5497,S5498,S5501,S5502,S5517,S5518,
																																	      S5520,S5521,S5522,S5523,S9061,S9097,S9098,S9122,S9123,S9124,S9125,S9128,S9129,
																																		  S9131,S9208,S9209,S9211,S9212,S9213,S9214,S9325,S9326,S9327,S9328,S9329,S9330,
																																		  S9331,S9335,S9336,S9338,S9339,S9340,S9341,S9342,S9343,S9345,S9346,S9347,S9348,
																																		  S9349,S9351,S9353,S9355,S9357,S9359,S9361,S9363,S9364,S9365,S9366,S9367,S9368,
																																	      S9370,S9372,S9373,S9374,S9375,S9376,S9377,S9379,S9490,S9494,S9497,S9500}'],
     ARRAY['E&M',                     'Hospice',                                       '--To be categorized--',                         '{S0255,T2043,S0271,T2044,T2045,T2046,Q5010,S9126,Q5007,G9479,G9478,G9477,T2042}'],
     ARRAY['E&M',                     'Hospital Inpatient Services',                   'Hospital E&M - Other',                          '{99026}'],
     ARRAY['E&M',                     'Hospital Inpatient Services',                   'Inpatient Consultation',                        '{99255}'],
     ARRAY['E&M',          		      'Office/Outpatient Services',                    'Other Office/OP Services',                      '{S0260,T1015,99027,S0302,S0610,S0612,S0613,S0622,S3005}'],
     ARRAY['E&M',       	          'Office/Outpatient Services',                    'Office E&M - Prolonged Service',                '{G2212,99417}'],
     ARRAY['E&M',                     'Office/Outpatient Services',                    '--To be categorized--',                         '{S9117,V5364}']
    
  ];
  query TEXT;
 BEGIN
  -- Log initiation message
  BEGIN
    CALL SP0019_log_initiation(schema_name, logtbl_name, 'SP0104_update_from_cpt_hcpcs_codes');   
  END;
  
  BEGIN
    FOR i IN 1..array_length(category_array, 1) LOOP
      query := 'UPDATE ' || schema_table || ' SET dh_category = $1, dh_subcategory = $2, dh_service = $3 WHERE proc_code = ANY($4::text[]) AND dh_service IS NULL';
      EXECUTE query USING category_array[i][1], category_array[i][2], category_array[i][3], category_array[i][4]::text[];
    END LOOP;
       CALL SP0018_log_creation(schema_name, logtbl_name, 'SP0104_update_from_cpt_hcpcs_codes');
  EXCEPTION
    WHEN OTHERS THEN
      -- Log error message
      CALL SP0017_log_failed(schema_name, logtbl_name, 'SP0104_update_from_cpt_hcpcs_codes', SQLERRM);
      RAISE;
  END;
END;


$$;
call SP0104_update_from_cpt_hcpcs_codes('SCHEMANAME','master_proc_codes','re_execution_log');
drop procedure SP0104_update_from_cpt_hcpcs_codes;
