###Load Ref Tables###

hive -e "drop database prod cascade;"

hive -e "create database prod;"

hostname="jdbc:postgresql://database-1.c2ze8lqsljg2.ap-south-1.rds.amazonaws.com:5432/PROD"

user="puser"

pwd="ppassword"


#ADDRESS
#CITY
#COUNTRY
#DEPARTMENT
#DISEASE
#DOCTOR
#E_ADDRESS
#HOSPITAL
#POLICY
#STATE
#TEST

###ADDRESS

hive -e "drop table prod.address_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table address --create-hive-table --hive-table prod.address_hve --hive-import

echo "Address_hve Table Imported"

###CITY
hive -e "drop table prod.city_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table city --create-hive-table --hive-table prod.city_hve --hive-import

echo "city_hve Table Imported"

###COUNTRY
hive -e "drop table prod.country_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table country --create-hive-table --hive-table prod.country_hve --hive-import

echo "country_hve Table Imported"

###DEPARTMENT
hive -e "drop table prod.department_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table department  --create-hive-table --hive-table prod.department_hve --hive-import

echo "department_hve Table Imported"
 
###DISEASE
hive -e "drop table prod.disease_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table disease --create-hive-table --hive-table prod.disease_hve --hive-import

echo "disease_hve Table Imported"

###DOCTOR
hive -e "drop table prod.doctor_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table doctor --create-hive-table --hive-table prod.doctor_hve --hive-import

echo "doctor_hve Table Imported"

###E_ADDRESS 
hive -e "drop table prod.e_address_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table e_address --create-hive-table --hive-table prod.e_address_hve --hive-import

echo "e_address_hve Table Imported"

###HOSPITAL
hive -e "drop table prod.hospital_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table hospital --create-hive-table --hive-table prod.hospital_hve --hive-import

echo "hospital_hve Table Imported"

###POLICY
hive -e "drop table prod.policy_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table policy --create-hive-table --hive-table prod.policy_hve --hive-import

echo "policy_hve Table Imported"

###STATE
hive -e "drop table prod.state_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table state --create-hive-table --hive-table prod.state_hve --hive-import

echo "state_hve Table Imported"

###TEST
hive -e "drop table prod.test_hve;"

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table test --create-hive-table --hive-table prod.test_hve --hive-import

echo "test_hve Table Imported" 

echo "#######ALL REF TABLES LOADED#########"  