CREATE OR REPLACE PROCEDURE SP0517_alter_column_data_types(schema_name text, table_name text, new_data_types text[]) AS $$
DECLARE

  column_names TEXT[] := ARRAY['population', 'malepopulation','femalepopulation','medianage','medianagemale','medianagefemale','latitude','longitude','elevation','cbsapopulation','cbsadivisionpopulation','landarea','waterarea','sfdu','mfdu'];
  i INTEGER := 1;  
BEGIN
  FOR i IN 1..array_length(column_names, 1) LOOP
    EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I TYPE %s USING %I::%s', schema_name, table_name, column_names[i], new_data_types[i], column_names[i], new_data_types[i]);

  END LOOP;
END;
$$ LANGUAGE plpgsql;
CALL SP0517_alter_column_data_types('SCHEMANAME', 'zipcode', ARRAY[ 'bigint','bigint','decimal','decimal','decimal','decimal','decimal','decimal','bigint',
                                                              'bigint','bigint','decimal','decimal','bigint','bigint']);
drop procedure SP0517_alter_column_data_types;
                                                             
                                                             