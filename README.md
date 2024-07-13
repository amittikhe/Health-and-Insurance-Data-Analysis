# Health-and-Insurance-Data-Analysis

1) Create RDS PostgreSQL v12.10-R1
Connect it using workbench
jdbc:postgresql://your_url.rds.amazonaws.com:5432/EDWA1

Create all 8 tables with help of "02.Postgre_DDL"
and insert data into all 8 tables with help of "03.Initial_Dataload"

Now, we have the data in our RDS(RDBMS) and we need to load it into hbase using spark script.

---------------------------------------------------------------------------------------

2) Create EMR cluster  (Hadoop,Spark,Hbase,Zookeeper,Hive,Hue,Phoenix)
Connect it using putty
hostname: hadoop@your_details.compute.amazonaws.com

create ref_load.py, trans_load.py in "/home/hadoop/" and paste the data from spark scripts "05.RDS_to_HBASE_Ref_import", "06.RDS_to_HBASE_Hist_Trans_import" respectively

pwd :- /home/hadoop
vi ref_load.py  ---> paste "05.RDS_to_HBASE_Ref_import" + :wq!
vi trans_load.py ----> paste "06.RDS_to_HBASE_Hist_Trans_import" + :wq!

Reference tables :- Country, city, plan_prepaid, plan_postpaid
Transactional tables :- subscriber, address, staff, complaint

---------------------------------------------------------------------------------------

3) We need the jars(jdbc jars for connecting with database, Phoenix jars to connect with hbase) to import data from RDS to hbase using spark.

Use winSCP and then drag & drop "dep" folder from our LFS to "/home/hadoop"

---------------------------------------------------------------------------------------

4) Now, we need to create tables in hbase through phoenix, otherwise our "spark-submit" job gets fail and give us "Exception: Table Not Found / Table undefined":

Open phoenix terminal:- 
/usr/lib/phoenix/bin/sqlline.py localhost

Paste all queries written in "04.Phoenix_DDL_Hbase_Table_Cre.txt" into phoenix terminal

To check the created tables in hbase:- !tables
Exit :- !q

---------------------------------------------------------------------------------------

5) Now run the below two "spark-submit" jobs for ref and trans tables. This will load all the data from RDBMS to hbase :-

/bin/spark-submit --jars /home/hadoop/dep/postgresql-42.2.14.jar,/home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1 /home/hadoop/ref_load.py

/bin/spark-submit --jars /home/hadoop/dep/postgresql-42.2.14.jar,/home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1 /home/hadoop/trans_load.py

---------------------------------------------------------------------------------------

6) Check in hbase, all the tables data loaded successfully or not.

Open phoenix terminal :-
/usr/lib/phoenix/bin/sqlline.py localhost

select * from subscriber_hb;
select * from plan_postpaid_hb;
 
Now, we have all the data loaded into our hbase

---------------------------------------------------------------------------------------

7) Then, we need to apply transformation(join) on this hbase tables and store the result in hive staging table(sub_details_stg.py, cmp_details_stg.py)

---------------------------------------------------------------------------------------

8) Spark can create tables automatically in hive but we want to create tables manually in hive. So let's create table in hive first.

pwd: /home/hadoop/
vi hive_ddl.hql ---> paste "07.hive_ddl.hql" + :wq!

Run this hql script using:
hive -f hive_ddl.hql

open hive terminal :- hive
use prod;
show tables;
we can see all 4 tables get created. These tables are empty.

Tables :- (complaint_details, complaint_details_staging, subscriber_details, subscriber_details_staging)

Ctrl+c

---------------------------------------------------------------------------------------

9) create files and paste scripts of Join operation in both files

pwd: /home/hadoop/
vi sub_details.py  ---> paste "08.Hb_to_hive_sub_details" + wq!
vi cmp_details.py  ---> paste "09.Hb_to_hive_cmp_details" + wq!

---------------------------------------------------------------------------------------

10) Now, run the job for both the files. Here we have removed jdbc(database-RDS) dependancy.

/bin/spark-submit --jars /home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1 /home/hadoop/sub_details.py

/bin/spark-submit --jars /home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1 /home/hadoop/cmp_details.py

This above will read the data from hbase and write data into hive_staging tables.

---------------------------------------------------------------------------------------------------

11) Check the data in hive_staging tables:-
-------------------------------------------------
NOTE:- Other two final tables are empty in hive
-------------------------------------------------

hive
use prod;
show tables;
select * from subscriber_details_staging;
select * from complaint_details_staging;


---------------------------------------------------------------------------------------------------------

12) Remove duplicate from both hive staging tables by applying the transformation and put the resultant data into the hive final tables. Truncate staging tables and drop the temp tables which is created internally.

pwd:- /home/hadoop/
vi dedup_Compaction.py  --> paste "10.dedup_Compaction" + wq!

( Hive ---> Hive ) Enabled hive support, No need to pass dependancy

/usr/bin/spark-submit  --jars /home/hadoop/dep/postgresql-42.2.14.jar,/home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1  /home/hadoop/dedup_Compaction.py

---------------------------------------------------------------------------------------------------

13) Let's check --> data should be pushed into final table and staging table gets truncated.
hive
use prod;
show tables;
select * from complaint_details_staging;  --> Empty
select * from subscriber_details_staging; --> Empty
select * from subscriber_details;         --> Data pushed here
select * from complaint_details;          --> Data pushed here

---------------------------------------------------------------------------------------------------

14) Now, we will extract data from hive(DW Layer) to AWS s3 and generate the report as per the business requirement. (Analysis Operation :- counting country wise active subscribers and the total revenue)

14.1) Open s3 --> Create s3 bucket > create folder EDWA > create folder REPORT > (here the result file gets save with current date)

---------------------------------------------------------------------------------------
NOTE:- Don't forget to edit "12.Hive_Final_To_S3_Extraction" file with your s3 bucket path
---------------------------------------------------------------------------------------


14.2) Run the job to extract data. (DW layer (i.e, Hive) ----> s3)
vi report.py   --> paste "12.Hive_Final_To_S3_Extraction" + wq!

/usr/bin/spark-submit  --jars /home/hadoop/dep/postgresql-42.2.14.jar,/home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1  /home/hadoop/report.py

14.3) Now, check in s3 bucket. Report file generated in csv format with todays date. Download the report and check the expected result. (106 countries, count sub, total revenue)
In real time, there will be complex operation.

---------- TILL NOW WE WORKED ON HISTORIC DATA ONLY (RDS DATA) ---------------

---------- NOW, WE WILL WORK ON DELTA (NEWLY INSERTED, UPDATED DATA) -----------

*****  SOURCE TEAM WILL DAILY UPLOAD DELTA DATA IN "INPUT" BUCKET OF s3 *****

---------------------------------------------------------------------------------------
NOTE:- 	Suppose for subscriber table, 2 records updated(Address) and 3 records newly inserted. So, In hive_staging table, we will get 5 records & In hive_final, we will get 1003 records. (Previously there was 1000 records).
---------------------------------------------------------------------------------------

15) Create folder in s3 with name "input_data".
Sir provided "data" folder where all 4 transactional tables is present.
Upload "data" folder from our file manager into this "s3_bucket_name > EDWA > input_data > data"

---------------------------------------------------------------------------------------------------

16) Now, we need to move this delta data into hive staging tables.

---------------------------------------------------------------------------------------
NOTE:- Make changes (path of s3) in "11.delta_processing_sb_cmp". Change all 4 paths (sub_path, add_path, cmp_path, staff_path).
---------------------------------------------------------------------------------------

vi delta.py  --> paste "11.delta_processing_sb_cmp" + wq!

Run below job:-

/usr/bin/spark-submit  --jars /home/hadoop/dep/postgresql-42.2.14.jar,/home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1  /home/hadoop/delta.py

This job will move 5 delta records in hive_staging.
We haven't touch final_hive tables yet. So, there will be 1000 records only.

Check in hive:-
hive
use prod;
show tables;
select * from subscriber_details_staging;  ---> (2 updated & 3 new records)

Note down these updated [SID = 135,734] records from staging and same old records from final.

select * from subscriber_details where subscriberid in (135,734);

[SID = 135,734] ==> Compare old(final table) records and updated record, we can see the change in address. Previously it was pune and now it's parabhani. We have changed updated_date as well in new records.

Now, whenever we run ddup job again, it will merge(Union) both tables(i.e, Staging tables[5 records] and final tables[1000 records]). This will give 1005 records. Then it will remove older records and keep new records and at the end we get 1003 records stored in hive final table. Staging tables get clean at last.

---------------------------------------------------------------------------------------
NOTE:- On daily basis ==> delta processing job, ddup job and reporting job will get run. Historic data we don't need to run daily. It will get load only once.
---------------------------------------------------------------------------------------
file name :- "10.dedup_Compaction"

/usr/bin/spark-submit  --jars /home/hadoop/dep/postgresql-42.2.14.jar,/home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1  /home/hadoop/dedup_Compaction.py

Check in Hive:-
hive
use prod;
show tables;
select * from subscriber_details_staging;  ---> Empty
select * from subscriber_details;  ---> 1003 records should present 

Check Parbhani records and new records in the final table(i.e,sub_details).

select * from subscriber_details where subscriberid in (135,734,1111,1112,1113);

---------------------------------------------
Change name of the earlier csv file in s3 to "revenue_report2.csv"

How to rename file :- open file > object actions > Rename object

----------------------------------------------

Now, we have already copied "12.Hive_Final_To_S3_Extraction" script to "report.py" saved in path "home/hadoop/"

/usr/bin/spark-submit  --jars /home/hadoop/dep/postgresql-42.2.14.jar,/home/hadoop/dep/phoenix-4.14.3-HBase-1.4-client.jar,/home/hadoop/dep/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --deploy-mode client --driver-memory 3g --executor-memory 2g --num-executors 1 --executor-cores 1  /home/hadoop/report.py

Check both the reports by downloading it and compare.

In excel we can see difference in india country.

Schema:- Country | Total_Subscriber | total_revenue
Old   :- India   | 104              | 35765
New   :- India   | 107              | 38012

-------------------------------   END OF THE PROJECT   -----------------------------------------

