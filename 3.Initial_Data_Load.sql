--------------------------------------------------------------------------------------
-- Name	       : PT (Postgre Tutorial) Sample Database
-- Version     : 1.0
-- Last Updated: Dec-15-2019
--------------------------------------------------------------------------------------

--------------------------------------------------------
--  COUNTRY
--------------------------------------------------------
Insert into COUNTRY (CON_ID,CON_NAME) values (1,'United State');
Insert into COUNTRY (CON_ID,CON_NAME) values (44,'United Kingdom');
Insert into COUNTRY (CON_ID,CON_NAME) values (91,'India');
Insert into COUNTRY (CON_ID,CON_NAME) values (94,'SriLanka');
Insert into COUNTRY (CON_ID,CON_NAME) values (34,'Spain');
Insert into COUNTRY (CON_ID,CON_NAME) values (977,'Nepal');
Insert into COUNTRY (CON_ID,CON_NAME) values (81,'Japan');
Insert into COUNTRY (CON_ID,CON_NAME) values (86,'China');
commit;
--------------------------------------------------------
--  STATE
--------------------------------------------------------

Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('GJ','GUJRAT',91);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('MH','MAHARASHTRA',91);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('AP','ANDRA PRADESH',91);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('MP','MADHYA PRADESH',91);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('PN','Punjab',91);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('CA','California',1);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('AK','Alaska',1);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('TX','Texas',1);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('NM','New Mexico',1);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('NY','New York',1);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('ENG','England',44);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('SCO','Scotland',44);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('NI','Northern Ireland',44);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('BJ','Beijing',86);
Insert into STATE (ST_ID,ST_NAME,CON_ID) values ('GU','Guiyang',86);
commit;


--------------------------------------------------------
--  CITY
--------------------------------------------------------
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('PUN','PUNE','MH');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('MUM','MUMBAI','MH');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('NAG','NAGPUR','MH');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('AMD','AHMADABAD','GJ');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('HYD','HYDRABAD','AP');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('BHO','BHOPAL','MP');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('CND','CHANDIGAD','PN');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('SAN','SAN FRANCISCO','CA');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('WAS','WASILLA','AK');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('DAL','DALLAS','TX');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('SAF','SANTA FE','NM');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('NY','New York','NY');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('BR','BRISTOL','ENG');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('DUN','DUNDI','SCO');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('BAG','BAGHDAD','NI');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('BIG','Beijing','BJ');
Insert into CITY (CT_ID,CT_NAME,ST_ID) values ('LIN','LINDEN','GU');
commit;


--------------------------------------------------------
--  ADDRESS
--------------------------------------------------------

Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_101','Old No.52 New No.8 Dr.BN Road',123456,'PUN');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_102','Old No.52 New No.8 Dr.BN Road',123457,'MUM');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_103','26/7 Jaffer Sarang Street',123458,'NAG');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_104','W-169 WIP',123459,'AMD');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_105','80 Feet Road',123460,'HYD');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_106','Jai Ganesh Building',123461,'BHO');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_107','26/7 Jaffer Sarang Street',123462,'CND');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_108','W-16 WIP',123463,'SAN');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_109','Old No.52 New No.8 Dr.BN Road',123464,'WAS');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_110','Jai Ganesh Building',123465,'DAL');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_111','80 Feet Road',123466,'SAF');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_112','W-169 WIP',123467,'NY');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_113','26/7 Jaffer Sarang Street',123468,'BR');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_114','Classic Tower',123469,'DUN');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_115','Classic Tower',123470,'BAG');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_116','Old No.52 New No.8 Dr.BN Road',123471,'BIG');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_117','80 Feet Road',123472,'LIN');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_118','W-169 WIP',123473,'PUN');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_119','26/7 Jaffer Sarang Street',123474,'MUM');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_120','Classic Tower',123475,'NAG');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_121','W-169 WIP',123476,'PUN');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_122','80 Feet Road',123477,'MUM');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_123','Classic Tower',123478,'NAG');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_124','Old No.52 New No.8 Dr.BN Road',123479,'HYD');
Insert into ADDRESS (P_ADD,AREA,PIN,CT_ID) values ('P_125','80 Feet Road',123480,'BHO');
commit;


--------------------------------------------------------
--  E_ADDRESS
--------------------------------------------------------

Insert into E_ADDRESS (E_ADD,DES) values ('E_101','rajan.ln@3i-infotech.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_102','ramesh.sr@3i-infotech.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_103','rosa@ext.icicibank.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_104','rakhi.patel@ext.icicibank.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_105','bhaskar@aarthiconsultants.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_106','gvlnraju@aarthiconsultants.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_107','murthy@aarthiconsultants.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_108','yesuraju@aarthiconsultants.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_109','prkrishna@aarts.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_110','jeewatrai@abhipra.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_111','absconsultant@vsnl.net');
Insert into E_ADDRESS (E_ADD,DES) values ('E_112','burjor.nariman@acclimited.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_113','sujata.chitre@acclimited.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_114',' arti.yadav@acclimited.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_115','atul.lakhotia@adityabirla.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_116','gopalpamnani@adityabirla.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_117','sushanth.a@adroitcorporate.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_118','valsas@adroitcorporate.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_119','surendrag@adroitcorporate.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_120','pratapp@adroitcorporate.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_121','ganeshs@adroitcorporate.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_122','choksh@vsnl.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_123','choksh@rediff.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_124','jksingla@alankit.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_125','rta@alankit.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_126','ankit_4321@yahoo.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_127','Ankitind@sancharnet.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_128','Pramoad.Sharma@apollotyres.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_129','santoshk.agrawala@bcml.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_130','beetalrta@gmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_131','beetal@beetalfinancial.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_132','rta_admin@bfsl.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_133','manager_rta@bfsl.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_134','cs@bfsl.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_135','secretarial@bharatforge.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_136','Prashann@bharatforge.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_137','yzope@bharatforge.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_138','mohan@bigshareonline.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_139','ashok@bigshareonline.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_140','maksood@bigshareonline.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_141','sunil@bigshareonline.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_142','ashoka@bigshareonline.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_143','helpdesk@btsindia.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_144','cameo@cameoindia.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_145','priya@cameoindia.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_146','ravi@ccsl.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_147','canbankrta@ccsl.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_148','rta@cbmsl.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_149','cilsec@rediffmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_150','advisors@cilsecurities.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_151','kkm31@rediffmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_152','kkm@cilsecurities.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_153','satchid.sargaonkar@citi.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_154','namrata.raut@citi.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_155','subbaraman.viyer@citi.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_156','helpdesk@computechsharecap.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_157','nsdl_relations@camsonline.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_158','sv_karthick@camsonline.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_159','s_hariganesan@camsonline.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_160','smarfatia@compindia.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_161','sbelwate@computronicsindia.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_162','investorqry@dfssl.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_163','shalini_baigh@dfssl.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_164','ganapathy.subramaniam@db.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_165','sundaresan.m@db.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_166','subrojit.saha@db.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_167','transaction.displ@db.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_168','yogesh-kisan.shelar@db.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_169','mayuresh.mathakar@db.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_170','bb.nsdl@db.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_171','b.rajaratnam@dsrc.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_172','invcom@oberoigroup.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_173','SN.Sridhar@oberoigroup.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_174','pradiy@oberiogroup.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_175','dilip.dutta@oberoigroup.com' );
Insert into E_ADDRESS (E_ADD,DES) values ('E_176','corpsect@ndb.vsnl.net.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_177','umg@finolexind.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_178','amtrac_nsk@sancharnet.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_179','vaman.sabharwal@ge.com' );
Insert into E_ADDRESS (E_ADD,DES) values ('E_180','gopalsharma@gnsaindia.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_181','rdevasahayam@gnsaindia.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_182','shares@adityabirla.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_183','dilip.ghocha@adityabirla.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_184','vaapte@gtllimited.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_185','rbpanchal@gnfc.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_186','aashastree@gnfc.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_187','sbshah@gnfc.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_188','girishk@hdfc.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_189','investorcare@hdfc.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_190','a.malik@adityabirla.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_191','iitcsl@bom3.vsnl.net.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_192','iitashu@yahoo.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_193','ikonvision@yahoo.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_194','pkhandelwal@imfa.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_195','Investor_relation@imfa.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_196','Sysadm@imfa.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_197','bharat@hotmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_198','bharat.b@indusinvest.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_199','alfint@vsnl.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_200','sureshbabu@iepindia.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_201','yesbalu@iepindia.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_202','isc@itc.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_203','arun.bose@itc.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_204','jkshr@jkcement.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_205','rc.srivastava@jkcement.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_206','jil@jagatjit.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_207','jayvijay@ambujagroup.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_208','amit.verma@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_209','phani@karvy.com' );
Insert into E_ADDRESS (E_ADD,DES) values ('E_210','vmahesh@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_211','amit.verma@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_212','phani@karvy.com' );
Insert into E_ADDRESS (E_ADD,DES) values ('E_213','vmahesh@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_214','phani@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_215','vmahesh@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_216','phani@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_217','vmahesh@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_218','vmahesh@karvy.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_219','rameshmanuel@kcsl.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_220','rcs2005@rediffmail.com' );
Insert into E_ADDRESS (E_ADD,DES) values ('E_221','knackcorp@gmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_222','sanjeev.nandu@linkintime.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_223','mumbai@linkintime.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_224','kc.agarwal@lml.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_225','sanjeev.kumar@lml.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_226','inces@lupinpharma.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_227','mdpl@cal.vsnl.net.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_228','info@mdpl.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_229','info@masserv.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_230','admin@mcsdel.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_231','cshiren@mcsdel.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_232','mcskol@rediffmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_233','aloke_1mukherjee@yahoo.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_234','mcspanvel@yahoo.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_235','udaymcs@yahoo.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_236','mcsdemat@yahoo.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_237','mcsdemat@rediffmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_238','mcssta@rediffmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_239','subhasbhatt@yahoo.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_240','sub_mcs@rediffmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_241','sandeep@mennenfinance.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_242','mfsl@mennengroup.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_243','kggeorge@mrfmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_244','roshan.varghese@mrfmail.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_245','investors@nagarjunagroup.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_246','KSBabu@nagarjunagroup.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_247','PARaju@nagarjunagroup.com');
Insert into E_ADDRESS (E_ADD,DES) values ('E_248','knravindra@nalcoindia.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_249','ncsahoo@nalcoindia.co.in');
Insert into E_ADDRESS (E_ADD,DES) values ('E_250','rvijaya@nalcoindia.co.in');
commit;


--------------------------------------------------------
--  DEPARTMENT
--------------------------------------------------------

Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('10','Administration','P_101');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('20','Marketing','P_102');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('30','Purchasing','P_103');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('40','Human Resources','P_104');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('50','Shipping','P_105');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('60','IT','P_106');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('70','Public Relations','P_107');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('80','Sales','P_108');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('90','Executive','P_109');
Insert into DEPARTMENT (D_ID,D_NAME,P_ADD) values ('100','Finance','P_110');
commit;


--------------------------------------------------------
--  HOSPITAL
--------------------------------------------------------

Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_01','Ann  Robert H','P_101','YES');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_02','Aspirus Wausau','P_102','YES');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_03','Aurora St. Lukes','P_103','YES');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_04','Banner-University','P_104','YES');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_05','Baylor','P_105','NO');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_06','Baystate Medical','P_106','NO');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_07','Bethesda','P_107','NO');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_08','Boston','P_108','YES');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_09','Brigham','P_109','NO');
Insert into HOSPITAL (HOS_ID,HOS_NAME,P_ADD,IS_NETWORKED) values ('H_10','Carle','P_110','YES');
commit;


--------------------------------------------------------
--  TEST
--------------------------------------------------------
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_01','Bacteria Culture',5000);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_02','Bacterial',10000);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_03','Balance Tests',11000);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_04','Ceruloplasmin',12000);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_05','Chlamydia',2500);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_06','Cortisol',300);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_07','Creatine Kinase',5000);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_08','Creatinine',10000);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_09','Crystals',11000);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_10','Elastography',12000);
Insert into TEST (TEST_ID,TEST_NAME,PRICE) values ('T_11','Electrocardiogram',2500);
commit;


--------------------------------------------------------
--  DOCTOR
--------------------------------------------------------

Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1001,'RAM K','BRAIN',123456780,6,'P_111','E_101');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1002,'KARAN J','OTHO',123456781,7,'P_112','E_102');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1003,'RANI','HEART',123456782,5,'P_113','E_103');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1004,'SHANKAR','TOOTH',123456783,4,'P_114','E_104');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1005,'JAMS','BONES',123456784,2,'P_115','E_105');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1006,'ORCADE','SKINaand HAIR',123456785,6,'P_116','E_106');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1007,'YANG CHONG','KIDNEY',123456786,7,'P_117','E_107');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1008,'ANNA','NURO',123456787,5,'P_118','E_108');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1009,'SWARNIM','PSCYCO',123456788,4,'P_119','E_109');
Insert into DOCTOR (DOCT_ID,D_NAME,SPECIALIST,MOBILE,EXP,P_ADD,E_ADD) values (1010,'SID','EYE',123456789,2,'P_120','E_110');
commit;


--------------------------------------------------------
--  DISEASE
--------------------------------------------------------


Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_101','Cholera','T_01');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_102','Colitis','T_02');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_103','Diphtheria','T_03');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_104','Leprosy','T_04');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_105','Lice','T_05');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_106','Pubic lice','T_06');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_107','Q fever','T_07');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_108','Rabies','T_08');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_109','Warts','T_09');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_110','Vitiligo','T_10');
Insert into DISEASE (DES_ID,DES_NAME,TEST_ID) values ('DIS_111','Scurvy','T_11');
commit;


--------------------------------------------------------
--  BILLING
--------------------------------------------------------

Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_01','2390','12-OCT-2019','12-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_02','3256','13-OCT-2019','13-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_03','3456','14-OCT-2019','14-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_04','7654','15-OCT-2019','15-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_05','3456','16-OCT-2019','16-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_06','6442','17-OCT-2019','17-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_07','2390','18-OCT-2019','18-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_08','3256','19-OCT-2019','19-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_09','3456','20-OCT-2019','20-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_10','7654','21-OCT-2019','21-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_11','3456','22-OCT-2019','22-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_12','6442','23-OCT-2019','23-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_13','2390','24-OCT-2019','24-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_14','3256','25-OCT-2019','25-NOV-2019');
Insert into BILLING (BILL_ID,BILL_AMT,ADD_DATE,DESR_DATE) values ('BILL_15','3456','26-OCT-2019','26-NOV-2019');
commit;


--------------------------------------------------------
--  POLICY
--------------------------------------------------------


Insert into POLICY (POL_TYPE_ID,POL_TYPE,TENURE,BENIFIT,PREMIUM) values (111,'TERM INSURANCE',1,'In thid you will get all family insurance for 5 years',1000);
Insert into POLICY (POL_TYPE_ID,POL_TYPE,TENURE,BENIFIT,PREMIUM) values (112,'PERMANENT INSURANCE',3,'PERMANENT INSURANCE',2000);
Insert into POLICY (POL_TYPE_ID,POL_TYPE,TENURE,BENIFIT,PREMIUM) values (113,'Whole Life',2,'Traditional Whole Life',5000);
Insert into POLICY (POL_TYPE_ID,POL_TYPE,TENURE,BENIFIT,PREMIUM) values (114,'FULL CLAIM',4,'FULL CLAIM',2000);
Insert into POLICY (POL_TYPE_ID,POL_TYPE,TENURE,BENIFIT,PREMIUM) values (115,'FULL FAMILY',5,'FULL FAMILY',5500);
Insert into POLICY (POL_TYPE_ID,POL_TYPE,TENURE,BENIFIT,PREMIUM) values (116,'SINGLE PERSON',5,'SINGLE PERSON',10000);
commit;

--------------------------------------------------------
--  CLAIM
--------------------------------------------------------


Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3000,'inprocess','12-FEB-2019',12000,'H_01',1001,'DIS_101','BILL_01');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3001,'inprocess','13-FEB-2019',11000,'H_02',1002,'DIS_102','BILL_02');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3002,'inprocess','14-FEB-2019',13000,'H_03',1003,'DIS_103','BILL_03');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3003,'inprocess','15-FEB-2019',14000,'H_04',1004,'DIS_104','BILL_04');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3004,'inprocess','16-FEB-2019',12000,'H_05',1005,'DIS_105','BILL_01');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3005,'inprocess','17-FEB-2019',11000,'H_06',1006,'DIS_101','BILL_02');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3006,'inprocess','18-FEB-2019',13000,'H_07',1001,'DIS_102','BILL_03');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3007,'inprocess','19-FEB-2019',14000,'H_08',1002,'DIS_103','BILL_04');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3008,'inprocess','20-FEB-2019',12000,'H_09',1003,'DIS_104','BILL_01');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3009,'inprocess','21-FEB-2019',11000,'H_10',1004,'DIS_105','BILL_02');
Insert into CLAIM (CLAIM_ID,STATUS,ISSUE_DATE,ISSUE_AMT,HOS_ID,DOCT_ID,DES_ID,BILL_ID) values (3010,'inprocess','22-FEB-2019',13000,null,1005,'DIS_101','BILL_03');
commit;


--------------------------------------------------------
--  PAIIENT
--------------------------------------------------------

Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (1,'L.N. Rajan','12-Feb-1993',12345678,12345432,'12-Feb-2019','M','P_101','E_111','A+',100000,3000,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (2,'G. Bhaskara','13-Feb-1993',12345679,12345433,'13-Feb-2019','M','P_102','E_112','B+',100001,3001,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (3,'Jeewat Rai','14-Feb-1993',12345680,12345434,'14-Feb-2019','M','P_103','E_113','AB+',100002,3002,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (4,'Mr.Vijay Chand' ,'15-Feb-1993',12345681,12345435,'15-Feb-2019','M','P_104','E_114','AB-',100003,3003,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (5,'Burjor D.','16-Feb-1993',12345682,12345436,'16-Feb-2019','M','P_105','E_115','B-',100004,3004,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (6,'Atul Lakhotia','17-Feb-1993',12345683,12345437,'17-Feb-2019','M','P_106','E_116','O+',100005,3005,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (7,'Sushanth Alva','18-Feb-1993',12345684,12345438,'18-Feb-2019','M','P_107','E_117','O-',100006,3006,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (8,'Vijay S','19-Feb-1993',12345685,12345439,'19-Feb-2019','M','P_108','E_118','A+',100007,3007,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (9,'Ms. Savita ','20-Feb-1993',12345686,12345440,'20-Feb-2019','M','P_109','E_119','B+',100008,3008,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (10,'P.N. Wahal','21-Feb-1993',12345687,12345441,'21-Feb-2019','M','P_110','E_120','AB+',100009,3009,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (11,'S.K. Agrawala','22-Feb-1993',12345688,12345442,'22-Feb-2019','M','P_111','E_121','AB-',100010,3010,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (12,'Punit Mittal','23-Feb-1993',12345689,12345443,'23-Feb-2019','M','P_112','E_122','B-',100011,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (13,'Ms. Mercy Susan' ,'24-Feb-1993',12345690,12345444,'24-Feb-2019','M','P_113','E_123','O+',100012,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (14,'P. S.','25-Feb-1993',12345691,12345445,'25-Feb-2019','M','P_114','E_124','O-',100013,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (15,'N. V. K. Mohan','26-Feb-1993',12345692,12345446,'26-Feb-2019','M','P_115','E_125','A+',100014,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (16,'S. Ramesh Babu','27-Feb-1993',12345693,12345447,'27-Feb-2019','M','P_116','E_126','B+',100015,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (17,'R. D. Ramasamy','28-Feb-1993',12345694,12345448,'28-Feb-2019','M','P_117','E_127','AB+',100016,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (18,'K. Ravi','01-Mar-1993',12345695,12345449,'01-Mar-2019','M','P_118','E_128','AB-',100017,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (19,'Anjan Mandal','02-Mar-1993',12345696,12345450,'02-Mar-2019','M','P_119','E_129','B-',100018,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (20,'K. K.' ,'03-Mar-1993',12345697,12345451,'03-Mar-2019','M','P_120','E_130','O+',100019,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (21,'Satchid' ,'04-Mar-1993',12345698,12345452,'04-Mar-2019','M','P_121','E_131','O-',100020,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (22,'Ms.Gool' ,'05-Mar-1993',12345699,12345453,'05-Mar-2019','M','P_122','E_132','A+',100021,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (23,'S.V. Karthick' ,'06-Mar-1993',12345700,12345454,'06-Mar-2019','M','P_123','E_133','B+',100022,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (24,'Shishir' ,'07-Mar-1993',12345701,12345455,'07-Mar-2019','F','P_124','E_134','AB+',100023,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (25,'Shalini Baigh','08-Mar-1993',12345702,12345456,'08-Mar-2019','F','P_125','E_135','AB-',100024,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (26,'Ganapathy' ,'09-Mar-1993',12345703,12345457,'09-Mar-2019','F','P_101','E_136','B-',100025,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (27,'Benjamin' ,'10-Mar-1993',12345704,12345458,'10-Mar-2019','F','P_102','E_137','O+',100026,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (28,'P. K.' ,'11-Mar-1993',12345705,12345459,'11-Mar-2019','F','P_103','E_138','O-',100027,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (29,'Vinod Dixit','12-Mar-1993',12345706,12345460,'12-Mar-2019','F','P_104','E_139','A+',100028,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (30,'Umesh M. Gosavi','13-Mar-1993',12345707,12345461,'13-Mar-2019','F','P_105','E_140','B+',100029,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (31,'Bhushan'  ,'14-Mar-1993',12345708,12345462,'14-Mar-2019','F','P_106','E_141','AB+',100030,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (32,'Vaman Sabharwal','15-Mar-1993',12345709,12345463,'15-Mar-2019','F','P_107','E_142','AB-',100031,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (33,'Gopal N' ,'16-Mar-1993',12345710,12345464,'16-Mar-2019','F','P_108','E_143','B-',100032,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (34,'Ashok Malu','17-Mar-1993',12345711,12345465,'17-Mar-2019','F','P_109','E_144','O+',100033,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (35,'Vidyadhar A' ,'18-Mar-1993',12345712,12345466,'18-Mar-2019','F','P_110','E_145','O-',100034,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (36,'R.B. Panchal','19-Mar-1993',12345713,12345467,'19-Mar-2019','F','P_111','E_146','A+',100035,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (37,'Mr.Girish V' ,'20-Mar-1993',12345714,12345468,'20-Mar-2019','F','P_112','E_147','B+',100036,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (38,'Anil Malik','21-Mar-1993',12345715,12345469,'21-Mar-2019','F','P_113','E_148','AB+',100037,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (39,'Pradeep' ,'22-Mar-1993',12345716,12345470,'22-Mar-2019','F','P_114','E_149','AB-',100038,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (40,'Ms. Namrata','23-Mar-1993',12345717,12345471,'23-Mar-2019','F','P_115','E_150','B-',100039,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (41,'Prem' ,'24-Mar-1993',12345718,12345472,'24-Mar-2019','F','P_116','E_151','O+',100040,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (42,'Bharat' ,'25-Mar-1993',12345719,12345473,'25-Mar-2019','F','P_117','E_152','O-',100041,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (43,'S. Vijayagopal','26-Mar-1993',12345720,12345474,'26-Mar-2019','F','P_118','E_153','A+',100042,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (44,'Suresh Babu' ,'27-Mar-1993',12345721,12345475,'27-Mar-2019','F','P_119','E_154','B+',100043,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (45,'Arun Bose','28-Mar-1993',12345722,12345476,'28-Mar-2019','F','P_120','E_155','AB+',100044,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (46,'R.C.Srivastava','29-Mar-1993',12345723,12345477,'29-Mar-2019','F','P_121','E_156','AB-',100045,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (47,'K K Kohli','30-Mar-1993',12345724,12345478,'30-Mar-2019','F','P_122','E_157','B-',100046,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (48,'Dilip' ,'31-Mar-1993',12345725,12345479,'31-Mar-2019','F','P_123','E_158','O+',100047,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (49,'Amit Verma','01-Apr-1993',12345726,12345480,'01-Apr-2019','F','P_124','E_159','O-',100048,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (50,'Amit Verma','02-Apr-1993',12345727,12345481,'02-Apr-2019','F','P_125','E_160','A+',100049,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (51,'Mr.V. Mahesh','03-Apr-1993',12345728,12345482,'03-Apr-2019','F','P_101','E_161','B+',100050,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (52,'S Ganapathy' ,'04-Apr-1993',12345729,12345483,'04-Apr-2019','F','P_102','E_162','AB+',100051,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (53,'V. Mahesh','05-Apr-1993',12345730,12345484,'05-Apr-2019','F','P_103','E_163','AB-',100052,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (54,'H.N. Anantharamu','06-Apr-1993',12345731,12345485,'06-Apr-2019','F','P_104','E_164','B-',100053,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (55,'R. Chandrasekaran','07-Apr-1993',12345732,12345486,'07-Apr-2019','F','P_105','E_165','A+',100054,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (56,'Sanjeev Nandu','08-Apr-1993',12345733,12345487,'08-Apr-2019','F','P_106','E_166','B+',100055,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (57,'K.C Agarwal' ,'09-Apr-1993',12345734,12345488,'09-Apr-2019','F','P_107','E_167','AB+',100056,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (58,'Pradeep S.' ,'10-Apr-1993',12345735,12345489,'10-Apr-2019','F','P_108','E_168','AB-',100057,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (59,'S. Rajagopalan','11-Apr-1993',12345736,12345490,'11-Apr-2019','M','P_109','E_169','B-',100058,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (60,'Sharwan ','12-Apr-1993',12345737,12345491,'12-Apr-2019','M','P_110','E_170','O+',100059,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (61,'Hiren G' ,'13-Apr-1993',12345738,12345492,'13-Apr-2019','M','P_111','E_171','O-',100060,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (62,'Aloke' ,'14-Apr-1993',12345739,12345493,'14-Apr-2019','M','P_112','E_172','A+',100061,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (63,'Uday V' ,'15-Apr-1993',12345740,12345494,'15-Apr-2019','M','P_113','E_173','B+',100062,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (64,'Subhas' ,'16-Apr-1993',12345741,12345495,'16-Apr-2019','M','P_114','E_174','AB+',100063,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (65,'Dinesh Menon','17-Apr-1993',12345742,12345496,'17-Apr-2019','M','P_115','E_175','AB-',100064,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (66,'K. G. George','18-Apr-1993',12345743,12345497,'18-Apr-2019','M','P_116','E_176','B-',100065,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (67,'K Suri Babu','19-Apr-1993',12345744,12345498,'19-Apr-2019','M','P_117','E_177','O+',100066,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (68,'K. N. Ravindra','20-Apr-1993',12345745,12345499,'20-Apr-2019','M','P_118','E_178','O-',100067,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (69,'S. Abbas','21-Apr-1993',12345746,12345500,'21-Apr-2019','M','P_119','E_179','A+',100068,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (70,'Mukesh C.' ,'22-Apr-1993',12345747,12345501,'22-Apr-2019','M','P_120','E_180','B+',100069,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (71,'Manoj Agarwal','23-Apr-1993',12345748,12345502,'23-Apr-2019','M','P_121','E_181','AB+',100070,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (72,'Rajesh Shah','24-Apr-1993',12345749,12345503,'24-Apr-2019','M','P_122','E_182','AB-',100071,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (73,'Ms. Daksha Mehta','25-Apr-1993',12345750,12345504,'25-Apr-2019','M','P_123','E_183','B-',100072,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (74,'Naresh Chander','26-Apr-1993',12345751,12345505,'26-Apr-2019','M','P_124','E_184','O+',100073,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (75,'J. Banerjee','27-Apr-1993',12345752,12345506,'27-Apr-2019','M','P_125','E_185','O-',100074,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (76,'Manoj Kumar' ,'28-Apr-1993',12345753,12345507,'28-Apr-2019','M','P_101','E_186','A+',100075,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (77,'Pinkesh Gupta','29-Apr-1993',12345754,12345508,'29-Apr-2019','M','P_102','E_187','B+',100076,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (78,'Michael ','30-Apr-1993',12345755,12345509,'30-Apr-2019','M','P_103','E_188','AB+',100077,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (79,'K. Ragunathan','01-May-1993',12345756,12345510,'01-May-2019','M','P_104','E_189','AB-',100078,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (80,'Mr.G. R. Rao','02-May-1993',12345757,12345511,'02-May-2019','M','P_105','E_190','B-',100079,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (81,'K.C AjitKumar','03-May-1993',12345758,12345512,'03-May-2019','M','P_106','E_191','A+',100080,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (82,'Manas Roy' ,'04-May-1993',12345759,12345513,'04-May-2019','F','P_107','E_192','B+',100081,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (83,'Shailesh','05-May-1993',12345760,12345514,'05-May-2019','F','P_108','E_193','AB+',100082,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (84,'K. Narendra','06-May-1993',12345761,12345515,'06-May-2019','F','P_109','E_194','AB-',100083,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (85,'Subhash Agarwal','07-May-1993',12345762,12345516,'07-May-2019','F','P_110','E_195','B-',100084,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (86,'Ramani TE','08-May-1993',12345763,12345517,'08-May-2019','F','P_111','E_196','O+',100085,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (87,'K. Raman','09-May-1993',12345764,12345518,'09-May-2019','F','P_112','E_197','O-',100086,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (88,'Mahendra' ,'10-May-1993',12345765,12345519,'10-May-2019','F','P_113','E_198','A+',100087,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (89,'Vinayak A' ,'11-May-1993',12345766,12345520,'11-May-2019','F','P_114','E_199','B+',100088,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (90,'Avinash M.' ,'12-May-1993',12345767,12345521,'12-May-2019','F','P_115','E_200','AB+',100089,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (91,'K. Selvana','13-May-1993',12345768,12345522,'13-May-2019','F','P_116','E_201','AB-',100090,0,111);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (92,'Ravindra Joshi','14-May-1993',12345769,12345523,'14-May-2019','F','P_117','E_202','B-',100091,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (93,'Milind Mondkar','15-May-1993',12345770,12345524,'15-May-2019','F','P_118','E_203','O+',100092,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (94,'Ms. Pushpa ','16-May-1993',12345771,12345525,'16-May-2019','F','P_119','E_204','O-',100093,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (95,'P.V.Srinivasa' ,'17-May-1993',12345772,12345526,'17-May-2019','F','P_120','E_205','A+',100094,0,115);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (96,'R Ram Prasad','18-May-1993',12345773,12345527,'18-May-2019','F','P_121','E_206','B+',100095,0,116);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (97,'Krishan Kant','19-May-1993',12345774,12345528,'19-May-2019','F','P_122','E_207','AB+',100096,0,112);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (98,'K. Raman','20-May-1993',12345775,12345529,'20-May-2019','F','P_123','E_208','AB-',100097,0,113);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (99,'Mahendra' ,'21-May-1993',12345776,12345530,'21-May-2019','F','P_124','E_209','B-',100098,0,114);
Insert into PAIIENT (PID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,CLAIM_ID,POL_TYPE_ID) values (100,'Vinayak A' ,'22-May-1993',12345777,12345531,'22-May-2019','F','P_125','E_210','O+',100099,0,115);
commit;


--------------------------------------------------------
--  STAFF
--------------------------------------------------------

Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (101,'L.N. ','12-Mar-1991',12365678,12555432,'12-Jan-2019','M','P_101','E_211','A+',100100,'10',111);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (102,'G. ','13-Mar-1991',12365678,12555432,'13-Jan-2019','M','P_102','E_212','B+',100101,'20',112);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (103,' Rai','14-Mar-1991',12365678,12555432,'14-Jan-2019','M','P_103','E_213','AB+',100102,'30',113);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (104,'Vijay Chand' ,'15-Mar-1991',12365678,12555432,'15-Jan-2019','M','P_104','E_214','AB-',100103,'40',114);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (105,'Burjor','16-Mar-1991',12365678,12555432,'16-Jan-2019','F','P_105','E_215','B-',100104,'50',115);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (106,'Lakhotia','17-Mar-1991',12365678,12555432,'17-Jan-2019','F','P_106','E_216','O+',100105,'60',116);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (107,'Alva','18-Mar-1991',12365678,12555432,'18-Jan-2019','F','P_107','E_217','O-',100106,'70',111);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (108,'Vijay','19-Mar-1991',12365678,12555432,'19-Jan-2019','F','P_108','E_218','A+',100107,'80',112);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (109,'Savita ','20-Mar-1991',12365678,12555432,'20-Jan-2019','F','P_109','E_219','B+',100108,'90',113);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (110,'Wahal','21-Mar-1991',12365678,12555432,'21-Jan-2019','F','P_110','E_220','AB+',100109,'100',114);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (111,'Agrawala','22-Mar-1991',12365678,12555432,'22-Jan-2019','F','P_111','E_221','AB-',100110,'10',115);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (112,'Mittal','23-Mar-1991',12365678,12555432,'23-Jan-2019','F','P_112','E_222','B-',100111,'20',116);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (113,'Susan' ,'24-Mar-1991',12365678,12555432,'24-Jan-2019','F','P_113','E_223','O+',100112,'30',111);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (114,'P. S.','25-Mar-1991',12365678,12555432,'25-Jan-2019','F','P_114','E_224','O-',100113,'40',112);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (115,'Mohan','26-Mar-1991',12365678,12555432,'26-Jan-2019','M','P_115','E_225','A+',100114,'50',113);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (116,'Babu','27-Mar-1991',12365678,12555432,'27-Jan-2019','F','P_116','E_226','B+',100115,'60',114);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (117,'Ramasamy','28-Mar-1991',12365678,12555432,'28-Jan-2019','F','P_117','E_227','AB+',100116,'70',115);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (118,'Ravi','01-Mar-1991',12365678,12555432,'01-Jan-2019','F','P_118','E_228','AB-',100117,'80',116);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (119,'Mandal','02-Mar-1991',12365678,12555432,'02-Jan-2019','F','P_119','E_229','B-',100118,'90',111);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (120,'K.' ,'03-Mar-1991',12365678,12555432,'03-Jan-2019','F','P_120','E_230','O+',100119,'100',112);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (121,'Satch' ,'04-Mar-1991',12365678,12555432,'04-Jan-2019','M','P_121','E_231','O-',100120,'10',113);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (122,'Gool' ,'05-Mar-1991',12365678,12555432,'05-Jan-2019','M','P_122','E_232','A+',100121,'20',114);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (123,'Karthick' ,'06-Mar-1991',12365678,12555432,'06-Jan-2019','M','P_123','E_233','B+',100122,'30',115);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (124,'Shishir' ,'07-Mar-1991',12365678,12555432,'07-Jan-2019','F','P_124','E_234','AB+',100123,'40',116);
Insert into STAFF (EMP_ID,F_NAME,B_DATE,PHONE,AADHAR,SYS_CRE_DATE,SEX,P_ADD,E_ADD,BL_GR,POL_NO,D_ID,POL_TYPE_ID) values (125,'Shalini','08-Mar-1991',12365678,12555432,'08-Jan-2019','F','P_125','E_235','AB-',100124,'50',111);
commit;

--------------------------------------------------------
--  FAMILY_DETAIL
--------------------------------------------------------

Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100000,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100001,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100002,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100003,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100004,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100005,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100006,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100007,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100008,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100009,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100010,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100011,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100012,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100013,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100014,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100015,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100016,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100017,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100018,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100019,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100020,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100021,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100022,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100023,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100024,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100025,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100026,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100027,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100028,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100029,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100030,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100031,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100032,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100033,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100034,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100035,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100036,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100037,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100038,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100039,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100040,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100041,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100042,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100043,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100044,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100045,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100046,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100047,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100048,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100049,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100050,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100051,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100052,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100053,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100054,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100055,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100056,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100057,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100058,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100059,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100060,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100061,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100062,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100063,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100064,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100065,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100066,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100067,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100068,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100069,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100070,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100071,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100072,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100073,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100074,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100075,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100076,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100077,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100078,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100079,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100080,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100081,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100082,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100083,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100084,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100085,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100086,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100087,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100088,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100089,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100090,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100091,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100092,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100093,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100094,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100095,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100096,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100097,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100098,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100099,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100100,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100101,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100102,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100103,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100104,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100105,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100106,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100107,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100108,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100109,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100110,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100111,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100112,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100113,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100114,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100115,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100116,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100117,4);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100118,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100119,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100120,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100121,3);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100122,1);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100123,2);
Insert into FAMILY_DETAIL (PID,FAM_MEM) values (100124,4);
commit;
--------------------------------------------------------------------