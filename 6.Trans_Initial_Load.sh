###Transactional Table Initial Load ###

hostname="jdbc:postgresql://database-1.c2ze8lqsljg2.ap-south-1.rds.amazonaws.com:5432/PROD"
user="puser"
pwd="ppassword"

#BILLING
#CLAIM
#FAMILY_DETAIL
#PAIIENT
#STAFF

###BILLING
hive -e "drop table prod.billing_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table billing --create-hive-table --hive-table prod.billing_hve --hive-import

echo "billing_hve Table Imported"

#CLAIM
hive -e "drop table prod.claim_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table claim  --create-hive-table --hive-table prod.claim_hve --hive-import

echo "claim_hve Table Imported"

#FAMILY_DETAIL
hive -e "drop table prod.family_detail_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table family_detail --create-hive-table --hive-table prod.family_detail_hve --hive-import

echo "family_detail_hve Table Imported"
 
#PAIIENT
hive -e "drop table prod.patient_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table paiient --create-hive-table --hive-table prod.patient_hve --hive-import

echo "patient_hve Table Imported"

#STAFF
hive -e "drop table prod.staff_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table staff --create-hive-table --hive-table prod.staff_hve --hive-import

echo "staff_hve Table Imported"

echo "##############Transactional Table Incremental Load Completed################"   