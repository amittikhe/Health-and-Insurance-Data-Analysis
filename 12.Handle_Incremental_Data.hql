--hql

--hive SCD type 1
INSERT OVERWRITE TABLE prod.patient_hve
SELECT pid,
       f_name,
       l_name,
       b_date,
       phone,
       aadhar,
       sys_cre_date,
       sys_upd_date,
       sex,
       p_add,
       e_add,
       bl_gr,
       pol_no,
       claim_id,
       pol_type_id,
       med_hist
FROM   (SELECT *,
               Row_number()
                 OVER (
                   partition BY pid
                   ORDER BY sys_upd_date DESC) rn
        FROM   (SELECT pid,
                       f_name,
                       l_name,
                       To_date(b_date)                                        AS
                       b_date
                       ,
                       phone,
                       aadhar,
                       To_date(sys_cre_date)                                  AS
                       sys_cre_date,
                       COALESCE(To_date(sys_upd_date), '1970-01-01 00:00:00') AS
                       sys_upd_date,
                       sex,
                       p_add,
                       e_add,
                       bl_gr,
                       pol_no,
                       claim_id,
                       pol_type_id,
                       med_hist
                FROM   prod.patient_hve
                UNION ALL
                SELECT pid,
                       f_name,
                       l_name,
                       b_date,
                       phone,
                       aadhar,
                       sys_cre_date,
                       COALESCE(sys_upd_date, '1970-01-01 00:00:00') AS
                       sys_upd_date,
                       sex,
                       p_add,
                       e_add,
                       bl_gr,
                       pol_no,
                       claim_id,
                       pol_type_id,
                       med_hist
                FROM   prod.patient_delta) A)B
WHERE  rn = 1; 


truncate table prod.patient_delta;
