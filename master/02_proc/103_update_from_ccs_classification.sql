CREATE OR REPLACE PROCEDURE SP0103_update_from_ccs_classification(
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
    -- dh_category                      dh_subcategory                                      dh_service									 ccs_group_code
     ARRAY['Other',                    'Ancillary Services',                            '--To be categorized--',                         '{237}'],
     ARRAY['Anesthesia',               'Anesthesia',                                    '--To be categorized--',                         '{232}'],
     ARRAY['Test',                     'Cardiography',                                  '--To be categorized--',                         '{201}'],
     ARRAY['Test',                     'Cardiography',                                  'Electrocardiogram',                             '{202}'],
     ARRAY['Test',                     'Cardiography',                                  'External Electrocardiographic Monitoring',      '{203}'],
     ARRAY['Treatment',                'Chemotherapy',                                  '--To be categorized--',                         '{224}'],
     ARRAY['Procedure',                'Cardiovascular',                                '--To be categorized--',                         '{43,44,47,48,49,50,52,62,63}'],
     ARRAY['Imaging',                  'CT Scan',                                       'CT/CTA - Chest',                                '{178}'],
     ARRAY['Imaging',          		   'CT Scan',                                       'CT/CTA - Abdomen and Pelvis',                   '{179}'],
     ARRAY['Imaging',       	       'CT Scan',                                       '--To be categorized--',                         '{180}'],
     ARRAY['Procedure',                'Digestive/Gastrointestinal',                    '--To be categorized--',                         '{70,71,75,77,93,94,96,97,98,99,244}'],
     ARRAY['Procedure',                'Eye',                                           '--To be categorized--',                         '{14,15,18,19,20,21}'],
     ARRAY['Test',                     'General Laboratory',                            '--To be categorized--',                         '{233,235}'],
     ARRAY['Other',                    'Infertility Services',                          '--To be categorized--',                         '{238}'],
     ARRAY['Treatment',                'Injections and Infusions (nononcologic)',       '--To be categorized--',                         '{228}'],
     ARRAY['Imaging',                  'Magnetic Resonance',                            '--To be categorized--',                         '{198}'],
     ARRAY['Procedure',                'Musculoskeletal',                               '--To be categorized--',                         '{148,149,153,154,156,158,159,161,162,164}'],
     ARRAY['Other',                    'Not Classified',                                '--To be categorized--',                         '{998,999}'],
     ARRAY['Imaging',                  'Nuclear',                                       '--To be categorized--',                         '{207,209,210}'],
     ARRAY['Other',                    'Oral and Dental',                               '--To be categorized--',                         '{997}'],
     ARRAY['DME',                      'Other DME',                                     '--To be categorized--',                         '{241,242,243}'],
     ARRAY['Procedure',                'Other Organ Systems',                           '--To be categorized--',                         '{1,3,7,8,9,12,23,26,31,32,33,36,38,41,42,64,101,105,106,107,113,117,118,121,124,126,130,131,132,141,176}'],
     ARRAY['Treatment',                'Physical, Occupational, and Speech Therapy',    '--To be categorized--',                         '{212,213,215}'],
     ARRAY['Procedure',                'Skin',                                 			'--To be categorized--',                         '{173,174,175}'],
     ARRAY['Imaging',                  'Standard X-ray',                           	    'Mammography',                                   '{182}'],
     ARRAY['Imaging',                  'Standard X-ray',                                'X-ray - Chest',                                 '{183}'],
     ARRAY['Imaging',                  'Standard X-ray',                                '--To be categorized--',                         '{186,230}'],
     ARRAY['Test',                     'Test - Miscellaneous',                          '--To be categorized--',                         '{200,206,234}'],
     ARRAY['Treatment',                'Treatment - Miscellaneous',                     '--To be categorized--',                         '{211,217,218,219,220,222,223,226,231,240}'],
     ARRAY['Imaging',                  'Ultrasound',                                    '--To be categorized--',                         '{192,197}'],
     ARRAY['Imaging',                  'Ultrasound',                                    'Echocardiography (TTE/TEE)',                    '{193}'],
     ARRAY['Procedure',                'Vascular',                                      '--To be categorized--',                         '{54,56,59,61}']
  ];
  query TEXT;
  BEGIN
  -- Log initiation message
  BEGIN
    CALL SP0019_log_initiation(schema_name, logtbl_name, 'SP0103_update_from_ccs_classification');   
  END;
  
  BEGIN
    FOR i IN 1..array_length(category_array, 1) LOOP
      query := 'UPDATE ' || schema_table || ' SET dh_category = $1, dh_subcategory = $2, dh_service = $3 WHERE ccs_group_code = ANY($4::text[]) AND dh_service IS NULL';
      EXECUTE query USING category_array[i][1], category_array[i][2], category_array[i][3], category_array[i][4]::text[];
    END LOOP;
    CALL SP0018_log_creation(schema_name, logtbl_name, 'SP0103_update_from_ccs_classification');
  EXCEPTION
    WHEN OTHERS THEN
      -- Log error message
      CALL SP0017_log_failed(schema_name, logtbl_name, 'SP0103_update_from_ccs_classification', SQLERRM);
      RAISE;
  END;
END;
$$;
call SP0103_update_from_ccs_classification('SCHEMANAME','master_proc_codes','re_execution_log');
drop procedure SP0103_update_from_ccs_classification;
