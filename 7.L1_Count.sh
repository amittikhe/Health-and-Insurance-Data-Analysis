#SRC Data Validation
hive -e "use prod;
select 'ADDRESS_hve',count(*) AS COUNT from ADDRESS_hve UNION ALL
select 'BILLING_hve',count(*) AS COUNT from BILLING_hve UNION ALL 
select 'CITY_hve',count(*) AS COUNT from CITY_hve UNION ALL 
select 'CLAIM_hve',count(*) AS COUNT from CLAIM_hve UNION ALL 
select 'COUNTRY_hve',count(*) AS COUNT from COUNTRY_hve UNION ALL 
select 'DEPARTMENT_hve',count(*) AS COUNT from DEPARTMENT_hve UNION ALL 
select 'DISEASE_hve',count(*) AS COUNT from DISEASE_hve UNION ALL 
select 'DOCTOR_hve',count(*) AS COUNT from DOCTOR_hve UNION ALL 
select 'E_ADDRESS_hve',count(*) AS COUNT from E_ADDRESS_hve UNION ALL 
select 'FAMILY_DETAIL_hve',count(*) AS COUNT from FAMILY_DETAIL_hve UNION ALL 
select 'HOSPITAL_hve',count(*) AS COUNT from HOSPITAL_hve UNION ALL 
select 'PATIENT_hve',count(*) AS COUNT from PATIENT_hve UNION ALL 
select 'POLICY_hve',count(*) AS COUNT from POLICY_hve UNION ALL 
select 'STAFF_hve',count(*) AS COUNT from STAFF_hve UNION ALL 
select 'STATE_hve',count(*) AS COUNT from STATE_hve UNION ALL 
select 'TEST_hve',count(*) AS COUNT from TEST_hve;"  >> l1_count.txt