create or replace  procedure SP0500_create_zip_code(schema_name text, table_name text,logtbl_name text)
LANGUAGE plpgsql
AS $$
BEGIN
-- Log initiation message
  BEGIN
    CALL SP0019_log_initiation(schema_name,logtbl_name, 'SP0500_create_zip_code');    
  END;
BEGIN
EXECUTE FORMAT ('DROP TABLE IF EXISTS %I.%I',schema_name,table_name);
execute format('CREATE TABLE IF NOT EXISTS %I.%I ( version text default ''2023JAN'',ZipCode text, PrimaryRecord text,Population text,MalePopulation text,FemalePopulation text, MedianAge text,MedianAgeMale text,MedianAgeFemale text,
               Latitude text,Longitude text,Elevation text,State text, StateFullName text,CityType text,CityAliasAbbreviation varchar(100), AreaCode text, City text, CityAliasName varchar(250),County text,
               CountyFIPS text,StateFIPS text,TimeZone text, DayLightSaving text, MSA text,PMSA text,CSA text,CBSA text,
               CBSA_Div text ,CBSA_Type text,CBSA_Name text, MSA_Name text,PMSA_Name text, 
               Region text, Division text, MailingName text,CBSAPopulation text, CBSADivisionPopulation text, CSAName text, CBSA_Div_Name text,
               CityStateKey text,LandArea text,WaterArea text, SFDU text, MFDU text,StateANSI text,CountyANSI text,ZIPIntroDate text)', schema_name, table_name);
-- Log success message
call SP0018_log_creation(schema_name,logtbl_name, 'SP0500_create_zip_code');
EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0500_create_zip_code',SQLERRM);
    RETURN;
END;
END;
$$;
call SP0500_create_zip_code('SCHEMANAME', 'zipcode','re_execution_log');
drop procedure SP0500_create_zip_code(text,text,text);