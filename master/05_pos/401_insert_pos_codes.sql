CREATE OR REPLACE PROCEDURE SP0401_insert_pos_codes(schema_name text, table_name text,logtbl_name text)
LANGUAGE plpgsql
AS $$
BEGIN
-- Log initiation message
        BEGIN
            CALL SP0019_log_initiation(schema_name,logtbl_name,'SP0401_insert_pos_codes');   
        END;
BEGIN
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (1,%L,%L)',schema_name, table_name,'Pharmacy','A facility or location where drugs and other medically related items and services are sold, dispensed, or otherwise provided directly to patients.(Effective October 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (2,%L,%L)',schema_name, table_name,'Telehealth Provided Other than in Patient’s Home','The location where health services and health related services are provided or received, through telecommunication technology. Patient is not located in their home when receiving health services or health related services through telecommunication technology.  (Effective January 1, 2017) (Description change effective January 1, 2022, and applicable for Medicare April 1, 2022.)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (3,%L,%L)',schema_name, table_name,'School','A facility whose primary purpose is education.(Effective January 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (4,%L,%L)',schema_name, table_name,'Homeless Shelter','A facility or location whose primary purpose is to provide temporary housing to homeless individuals (e.g., emergency shelters, individual or family shelters). (Effective January 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (5,%L,%L)',schema_name, table_name,'Indian Health Service Free-standing Facility','A facility or location, owned and operated by the Indian Health Service, which provides diagnostic, therapeutic (surgical and non-surgical), and rehabilitation services to American Indians and Alaska Natives who do not require hospitalization. (Effective January 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (6,%L,%L)',schema_name, table_name,'Indian Health Service Provider-based Facility','A facility or location, owned and operated by the Indian Health Service, which provides diagnostic, therapeutic (surgical and non-surgical), and rehabilitation services rendered by, or under the supervision of, physicians to American Indians and Alaska Natives admitted as inpatients or outpatients. (Effective January 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (7,%L,%L)',schema_name, table_name,'Tribal 638 Free-standing Facility','A facility or location owned and operated by a federally recognized American Indian or Alaska Native tribe or tribal organization under a 638 agreement, which provides diagnostic, therapeutic (surgical and non-surgical), and rehabilitation services to tribal members who do not require hospitalization. (Effective January 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (9,%L,%L)',schema_name, table_name,'Prison/Correctional Facility','A prison, jail, reformatory, work farm, detention center, or any other similar facility maintained by either Federal, State or local authorities for the purpose of confinement or rehabilitation of adult or juvenile criminal offenders. (Effective July 1, 2006)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (10,%L,%L)',schema_name, table_name,'Telehealth Provided in Patient’s Home','The location where health services and health related services are provided or received, through telecommunication technology. Patient is located in their home (which is a location other than a hospital or other facility where the patient receives care in a private residence) when receiving health services or health related services through telecommunication technology.(This code is effective January 1, 2022, and available to Medicare April 1, 2022.)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (11,%L,%L)',schema_name, table_name,'Office','Location, other than a hospital, skilled nursing facility (SNF), military treatment facility, community health center, State or local public health clinic, or intermediate care facility (ICF), where the health professional routinely provides health examinations, diagnosis, and treatment of illness or injury on an ambulatory basis.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (12,%L,%L)',schema_name, table_name,'Home','Location, other than a hospital or other facility, where the patient receives care in a private residence.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (13,%L,%L)',schema_name, table_name,'Assisted Living Facility','Congregate residential facility with self-contained living units providing assessment of each resident''s needs and on-site support 24 hours a day, 7 days a week, with the capacity to deliver or arrange for services including some health care and other services. (Effective October 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (14,%L,%L)',schema_name, table_name,'Group Home ','A residence, with shared living areas, where clients receive supervision and other services such as social and/or behavioral services, custodial service, and minimal services (e.g., medication administration). (Effective October 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (15,%L,%L)',schema_name, table_name,'Mobile Unit','A facility/unit that moves from place-to-place equipped to provide preventive, screening, diagnostic, and/or treatment services.(Effective January 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (17,%L,%L)',schema_name, table_name,'Walk-in Retail Health Clinic','A walk-in health clinic, other than an office, urgent care facility, pharmacy or independent clinic and not described by any other Place of Service code, that is located within a retail operation and provides, on an ambulatory basis, preventive and primary care services. (This code is available for use immediately with a final effective date of May 1, 2010)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (18,%L,%L)',schema_name, table_name,'Place of Employment-Worksite','A location, not described by any other POS code, owned or operated by a public or private entity where the patient is employed, and where a health professional provides on-going or episodic occupational medical, therapeutic or rehabilitative services to the individual. (This code is available for use effective January 1, 2013 but no later than May 1, 2013)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (19,%L,%L)',schema_name, table_name,'Off Campus-Outpatient Hospital','A portion of an off-campus hospital provider based department which provides diagnostic, therapeutic (both surgical and nonsurgical), and rehabilitation services to sick or injured persons who do not require hospitalization or institutionalization. (Effective January 1, 2016)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (20,%L,%L)',schema_name, table_name,'Urgent Care Facility','Location, distinct from a hospital emergency room, an office, or a clinic, whose purpose is to diagnose and treat illness or injury for unscheduled, ambulatory patients seeking immediate medical attention.(Effective January 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (21,%L,%L)',schema_name, table_name,'Inpatient Hospital','A facility, other than psychiatric, which primarily provides diagnostic, therapeutic (both surgical and nonsurgical), and rehabilitation services by, or under, the supervision of physicians to patients admitted for a variety of medical conditions.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (22,%L,%L)',schema_name, table_name,'On Campus-Outpatient Hospital','A portion of a hospital’s main campus which provides diagnostic, therapeutic (both surgical and nonsurgical), and rehabilitation services to sick or injured persons who do not require hospitalization or institutionalization. (Description change effective January 1, 2016)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (23,%L,%L)',schema_name, table_name,'Emergency Room – Hospital','A portion of a hospital where emergency diagnosis and treatment of illness or injury is provided.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (25,%L,%L)',schema_name, table_name,'Birthing Center','A facility, other than a hospital''s maternity facilities or a physician''s office, which provides a setting for labor, delivery, and immediate post-partum care as well as immediate care of new born infants.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (26,%L,%L)',schema_name, table_name,'Military Treatment Facility','A medical facility operated by one or more of the Uniformed Services. Military Treatment Facility (MTF) also refers to certain former U.S. Public Health Service (USPHS) facilities now designated as Uniformed Service Treatment Facilities (USTF).');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (27,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (28,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (29,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (30,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (31,%L,%L)',schema_name, table_name,'Skilled Nursing Facility','A facility which primarily provides inpatient skilled nursing care and related services to patients who require medical, nursing, or rehabilitative services but does not provide the level of care or treatment available in a hospital.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (33,%L,%L)',schema_name, table_name,'Custodial Care Facility','A facility which provides room, board and other personal assistance services, generally on a long-term basis, and which does not include a medical component.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (34,%L,%L)',schema_name, table_name,'Hospice','A facility, other than a patient''s home, in which palliative and supportive care for terminally ill patients and their families are provided.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (35,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (36,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (37,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (38,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (39,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (41,%L,%L)',schema_name, table_name,'Ambulance - Land','A land vehicle specifically designed, equipped and staffed for lifesaving and transporting the sick or injured.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (42,%L,%L)',schema_name, table_name,'Ambulance – Air or Water','An air or water vehicle specifically designed, equipped and staffed for lifesaving and transporting the sick or injured.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (43,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (44,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (45,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (46,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (47,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (49,%L,%L)',schema_name, table_name,'Independent Clinic','A location, not part of a hospital and not described by any other Place of Service code, that is organized and operated to provide preventive, diagnostic, therapeutic, rehabilitative, or palliative services to outpatients only.(Effective October 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (50,%L,%L)',schema_name, table_name,'Federally Qualified Health Center','A facility located in a medically underserved area that provides Medicare beneficiaries preventive primary medical care under the general direction of a physician.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (51,%L,%L)',schema_name, table_name,'Inpatient Psychiatric Facility','A facility that provides inpatient psychiatric services for the diagnosis and treatment of mental illness on a 24-hour basis, by or under the supervision of a physician.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (52,%L,%L)',schema_name, table_name,'Psychiatric Facility-Partial Hospitalization','A facility for the diagnosis and treatment of mental illness that provides a planned therapeutic program for patients who do not require full time hospitalization, but who need broader programs than are possible from outpatient visits to a hospital-based or hospital-affiliated facility.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (53,%L,%L)',schema_name, table_name,'Community Mental Health Center','A facility that provides the following services: outpatient services, including specialized outpatient services for children, the elderly, individuals who are chronically ill, and residents of the CMHC''s mental health services area who have been discharged from inpatient treatment at a mental health facility; 24 hour a day emergency care services; day treatment, other partial hospitalization services, or psychosocial rehabilitation services; screening for patients being considered for admission to State mental health facilities to determine the appropriateness of such admission; and consultation and education services.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (54,%L,%L)',schema_name, table_name,'Intermediate Care Facility/ Individuals with Intellectual Disabilities','A facility which primarily provides health-related care and services above the level of custodial care to individuals but does not provide the level of care or treatment available in a hospital or SNF.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (55,%L,%L)',schema_name, table_name,'Residential Substance Abuse Treatment Facility','A facility which provides treatment for substance (alcohol and drug) abuse to live-in residents who do not require acute medical care. Services include individual and group therapy and counseling, family counseling, laboratory tests, drugs and supplies, psychological testing, and room and board.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (57,%L,%L)',schema_name, table_name,'Non-residential Substance Abuse Treatment Facility','A location which provides treatment for substance (alcohol and drug) abuse on an ambulatory basis. Services include individual and group therapy and counseling, family counseling, laboratory tests, drugs and supplies, and psychological testing. (Effective October 1, 2003)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (58,%L,%L)',schema_name, table_name,'Non-residential Opioid Treatment Facility','A location that provides treatment for opioid use disorder on an ambulatory basis. Services include methadone and other forms of Medication Assisted Treatment (MAT). (Effective January 1, 2020)');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (59,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (60,%L,%L)',schema_name, table_name,'Mass Immunization Center','A location where providers administer pneumococcal pneumonia and influenza virus vaccinations and submit these services as electronic media claims, paper claims, or using the roster billing method. This generally takes place in a mass immunization setting, such as, a public health center, pharmacy, or mall but may include a physician office setting.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (61,%L,%L)',schema_name, table_name,'Comprehensive Inpatient Rehabilitation Facility','A facility that provides comprehensive rehabilitation services under the supervision of a physician to inpatients with physical disabilities. Services include physical therapy, occupational therapy, speech pathology, social or psychological services, and orthotics and prosthetics services.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (62,%L,%L)',schema_name, table_name,'Comprehensive Outpatient Rehabilitation Facility','A facility that provides comprehensive rehabilitation services under the supervision of a physician to outpatients with physical disabilities. Services include physical therapy, occupational therapy, and speech pathology services.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (63,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (65,%L,%L)',schema_name, table_name,'End-Stage Renal Disease Treatment Facility','A facility other than a hospital, which provides dialysis treatment, maintenance, and/or training to patients or caregivers on an ambulatory or home-care basis.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (66,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (67,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (68,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (69,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (70,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (71,%L,%L)',schema_name, table_name,'Public Health Clinic','A facility maintained by either State or local health departments that provides ambulatory primary medical care under the general direction of a physician.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (73,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (74,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (75,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (76,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (77,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (78,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (79,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (81,%L,%L)',schema_name, table_name,'Independent Laboratory','A laboratory certified to perform diagnostic and/or clinical tests independent of an institution or a physician''s office.');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (82,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (83,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (84,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (85,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (86,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (87,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (89,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (90,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (91,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (92,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (93,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (94,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (95,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (97,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (98,%L,%L)',schema_name, table_name,'Unassigned','N/A');
 EXECUTE format('INSERT INTO %I.%I
          (POS_CODE,POS_NAME, POS_DESC_LONG)
                   VALUES (99,%L,%L)',schema_name, table_name,'Other Place of Service','Other place of service not identified above.');
-- Log success message
  call SP0018_log_creation(schema_name,logtbl_name, 'SP0401_insert_pos_codes');
  EXCEPTION
  WHEN OTHERS THEN
    -- Log error message
    call SP0017_log_failed(schema_name,logtbl_name, 'SP0401_insert_pos_codes',SQLERRM);
    RETURN;
END;
END;
$$;
call SP0401_insert_pos_codes('SCHEMANAME','master_pos_code','re_execution_log');
drop procedure SP0401_insert_pos_codes;