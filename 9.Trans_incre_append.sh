#handle Incremental Data

#create table in hive if not exist 
hive -e "CREATE  TABLE IF NOT EXISTS prod.PATIENT_delta
  (
    PID          INT,
    F_NAME       STRING,
    L_NAME       STRING,
    B_DATE       TIMESTAMP ,
    PHONE        INT,
    AADHAR       INT,
    SYS_CRE_DATE TIMESTAMP,
    SYS_UPD_DATE TIMESTAMP ,
    SEX          STRING ,
    P_ADD        STRING,
    E_ADD        STRING,
    BL_GR        STRING,
    POL_NO       INT ,
    CLAIM_ID     INTEGER ,
    POL_TYPE_ID   INTEGER,
    MED_HIST  STRING 
  ) ROW format delimited 
  FIELDS TERMINATED BY ',' STORED AS TEXTFILE;"

hive -e "CREATE  TABLE  IF NOT EXISTS prod.STAFF_delta
  (
    EMP_ID       INT,
    F_NAME       STRING,
    L_NAME       STRING,
    B_DATE       TIMESTAMP ,
    PHONE        INTEGER,
    AADHAR       INTEGER,
    JOIN_DATE    TIMESTAMP ,
    SYS_CRE_DATE TIMESTAMP ,
    SYS_UPD_DATE TIMESTAMP ,
    SEX          STRING,
    P_ADD        STRING,
    E_ADD        STRING,
    BL_GR        STRING,
    POL_NO       INTEGER ,
    D_ID         STRING,
    CLAIM_ID     INTEGER,
    POL_TYPE_ID  INTEGER,
    MED_HIST    STRING
  )  ROW format delimited 
  FIELDS TERMINATED BY ',' STORED AS TEXTFILE;"

hostname="jdbc:postgresql://database-1.c2ze8lqsljg2.ap-south-1.rds.amazonaws.com:5432/PROD"
user="puser"
pwd="ppassword"

#PAIIENT

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table paiient --incremental append --check-column pid --last-value 100 --target-dir /user/hive/warehouse/prod.db/patient_delta --append

echo "patient_delta Table Imported"

#STAFF

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table staff --incremental append --check-column emp_id --last-value 125  --target-dir /user/hive/warehouse/prod.db/staff_delta --append


echo "staff_delta Table Imported"

echo "##############Transactional Table Incremental Load Completed################"   