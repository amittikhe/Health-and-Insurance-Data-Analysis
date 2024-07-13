#SRC Data Validation

select 'ADDRESS',count(*) AS COUNT from ADDRESS UNION ALL
select 'BILLING',count(*) AS COUNT from BILLING UNION ALL 
select 'CITY',count(*) AS COUNT from CITY UNION ALL 
select 'CLAIM',count(*) AS COUNT from CLAIM UNION ALL 
select 'COUNTRY',count(*) AS COUNT from COUNTRY UNION ALL 
select 'DEPARTMENT',count(*) AS COUNT from DEPARTMENT UNION ALL 
select 'DISEASE',count(*) AS COUNT from DISEASE UNION ALL 
select 'DOCTOR',count(*) AS COUNT from DOCTOR UNION ALL 
select 'E_ADDRESS',count(*) AS COUNT from E_ADDRESS UNION ALL 
select 'FAMILY_DETAIL',count(*) AS COUNT from FAMILY_DETAIL UNION ALL 
select 'HOSPITAL',count(*) AS COUNT from HOSPITAL UNION ALL 
select 'PAIIENT',count(*) AS COUNT from PAIIENT UNION ALL 
select 'POLICY',count(*) AS COUNT from POLICY UNION ALL 
select 'STAFF',count(*) AS COUNT from STAFF UNION ALL 
select 'STATE',count(*) AS COUNT from STATE UNION ALL 
select 'TEST',count(*) AS COUNT from TEST;