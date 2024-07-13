# Health-and-Insurance-Data-Analysis

STEP 1: We need to create database, create tables and insert data into each table in our RDS PostgreSQL database. (In real time, data engineers don't need to do this step as data will be inserted in the database through API).

STEP 2: Copy PostgreSQL jar in "home/hadoop" location using WinSCP software and move the jar from "home/hadoop" to "usr/lib/sqoop/lib" location. (Ex: sudo mv postgresql.42.2..14.jar /usr/lib/sqoop/lib)

STEP 3: Import reference tables in hive L1 layer. Reference tables are ADDRESS, CITY, COUNTRY, DEPARTMENT, DISEASE, DOCTOR, E_ADDRESS, HOSPITAL, POLICY, STATE, TEST. Create shell script file and write sqoop job which will import reference tables in hive. (Ex: vi ref_load.sh) Execute the same shell script file. (Ex: bash ref_load.sh)

STEP 4: Import transactional tables in hive L1 layer. Transactional tables are BILLING, CLAIM, FAMILY_DETAIL, PAIIENT, STAFF. Create shell script file and write sqoop job which will import transactional tables in hive. (Ex: vi trans_load.sh) Execute the same shell script file. (Ex: bash trans_load.sh)L1 - Layer STEP 5: Now, new records gets inserted into source tables PAIIENT, STAFF in PostgreSQL RDS.

STEP 6: Import this incremental inserted data by creating separate tables (i.e. delta tables) in hive L1 layer. Create shell script file and write sqoop job which will import only incremental inserted data in hive. (Ex: vi incre_trans.sh) Execute the same shell script file. (Ex: bash incre_trans.sh)

STEP 7: Now, few records gets updated into source tables PAIIENT, STAFF in PostgreSQL RDS.

STEP 8: Import this incremental updated data into delta tables in hive L1 layer. Create shell script file and write sqoop job which will import only incremental updated data in hive. (Ex: vi up_trans.sh) Execute the same shell script file. (Ex: bash up_trans.sh)

Daily Delta : The incremental/transactional data from source dB on a daily basis. Delta Table : Temporary table to handle daily delta. Target table : Transformed table which can be used for analytical purposes.

Explanation: Once we extracted daily delta from the source table and loaded it in a temporary hive table. We merged it with the existing data which was present in the hive table(all data till previous date ) and removed the duplicate by partitioning on the basis of the key column and arranging in descending order based on timestamp.

Data Validation: Count check Null Check Duplicate Check Referral Integrity Check Detailed Validation

-------------------------------   END OF THE PROJECT   -----------------------------------------

