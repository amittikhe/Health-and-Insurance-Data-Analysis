#handle Incremental Data

hostname="jdbc:postgresql://database-1.c2ze8lqsljg2.ap-south-1.rds.amazonaws.com:5432/PROD"
user="puser"
pwd="ppassword"

#PAIIENT

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table paiient  --incremental lastmodified --check-column sys_upd_date --last-value '1993-05-22 00:00:00.0' --target-dir /user/hive/warehouse/prod.db/patient_delta --append

echo "patient_delta Table Imported"

#STAFF

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table staff --incremental lastmodified --check-column sys_upd_date --last-value '1991-04-08 00:00:00.0' --target-dir /user/hive/warehouse/prod.db/staff_delta --append

echo "staff_delta Table Imported"

hive -e "msck repair table prod.patient_delta;"
hive -e "msck repair table prod.staff_delta;"

echo "##############Transactional Table Incremental Load Completed################"    