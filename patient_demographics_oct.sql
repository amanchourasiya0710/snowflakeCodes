CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE_TRx" AS
WITH T0 as
(
select year(svc_dt) as year, class, dosage_type, brand_generic,
CASE
  when brand_generic = 'BRANDED' and dosage_type = 'ORAL' then 'Branded Oral'
 when brand_generic = 'GENERIC' and dosage_type = 'ORAL' then 'Generic Oral'
 when dosage_type = 'INJ_LAI' then 'LAIs'
End as treatment_class,
(YEAR(CURRENT_DATE) - B.pat_brth_yr_nbr) as age, schiz_factored_trx_cnt,
 CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 25 then '18-25'
   when age >= 26 and age <= 35 then '26-35'
   when age >= 36 and age <= 45 then '36-45'
   when age >= 46 and age <= 55 then '46-55'
   when age >= 56 and age <= 65 then '56-65'
   when age > 65 then 'Above 65'
End as patient_age_group,

CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 45 then '18-45'
   when age >= 46 and age <= 65 then '46-65'
   when age > 65 then 'Above 65'
End as patient_age_group_4bins,

CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 65 then '18-65'
   when age > 65 then 'Above 65'
End as patient_age_group_3bins

FROM "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" A join "KARUNA_ANALYTICS"."ADM"."PATIENT" B on A.patient_id = B.patient_id
Where B.SCHIZ_DIAG_CD_FLAG = 'Y' and B.pat_brth_yr_nbr is NOT NULL and A.month_id <= '202210'
)
, T1 as
(
select CAST(year as INTEGER) as year, class, dosage_type, brand_generic, treatment_class, 'TRx' as metric_type, NULL as Metric, NULL as Gender, patient_age_group, patient_age_group_3bins, patient_age_group_4bins, sum(schiz_factored_trx_cnt) as value
from T0
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
Select * from T1;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE_NBRx" AS
WITH T0 as
(
select year(svc_dt) as year, class, dosage_type, brand_generic,
CASE
 when brand_generic = 'BRANDED' and dosage_type = 'ORAL' then 'Branded Oral'
 when brand_generic = 'GENERIC' and dosage_type = 'ORAL' then 'Generic Oral'
 when dosage_type = 'INJ_LAI' then 'LAIs'
End as treatment_class,
(YEAR(CURRENT_DATE) - B.pat_brth_yr_nbr) as age, schz_factored_nbrx_cnt,
CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 25 then '18-25'
   when age >= 26 and age <= 35 then '26-35'
   when age >= 36 and age <= 45 then '36-45'
   when age >= 46 and age <= 55 then '46-55'
   when age >= 56 and age <= 65 then '56-65'
   when age > 65 then 'Above 65'
End as patient_age_group,




CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 45 then '18-45'
   when age >= 46 and age <= 65 then '46-65'
   when age > 65 then 'Above 65'
End as patient_age_group_4bins,




CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 65 then '18-65'
   when age > 65 then 'Above 65'
End as patient_age_group_3bins




FROM "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" A join "KARUNA_ANALYTICS"."ADM"."PATIENT" B on A.patient_id = B.patient_id
Where B.SCHIZ_DIAG_CD_FLAG = 'Y' and B.pat_brth_yr_nbr is NOT NULL
)
, T1 as
(
select CAST(year as INTEGER) as year, class, dosage_type, brand_generic, treatment_class, 'NBRx' as metric_type,NULL as Metric, NULL as Gender, patient_age_group, patient_age_group_3bins, patient_age_group_4bins, sum(schz_factored_nbrx_cnt) as value
from T0
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
Select * from T1;








CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE" AS
WITH T0 as
(
Select * from "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE_TRx"
union
select * from "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE_NBRx"
)
select * from T0;




drop table "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE_TRx";
drop table "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE_NBRx";




create or replace table "KARUNA_ANALYTICS"."REPORTING"."PATIENT_GENDER_SCHIZ_DATA" as
with use_data as
(
select distinct A.patient_id, pat_gender_cd, pat_brth_yr_nbr, (YEAR(CURRENT_DATE) - pat_brth_yr_nbr) as age
from "KARUNA_ANALYTICS"."ADM"."PATIENT" A left join "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" B on A.patient_id = B.patient_id
where B.MONTH_ID != 'NULL' and SCHIZ_DIAG_CD_FLAG = 'Y' and B.month_id <= '202210'
)
, mean_data as
(
select pat_gender_cd, avg(year(CURRENT_DATE) - pat_brth_yr_nbr) as mean_age
from use_data
group by 1
)
, number_data as
(
select pat_gender_cd, count(*) as number_patients
from use_data
group by 1
)
, percent_data as
(
select pat_gender_cd,
CASE
   WHEN PAT_GENDER_CD = 'M' THEN number_patients * 100.0 / (
       SELECT SUM(number_patients) FROM number_data
   )
   WHEN PAT_GENDER_CD = 'F' THEN number_patients * 100.0 / (
       SELECT SUM(number_patients) FROM number_data
   )
   WHEN PAT_GENDER_CD = 'U' THEN number_patients * 100.0 / (
       SELECT SUM(number_patients) FROM number_data
   )
END as percent_patients




from number_data
)
, sorted_data AS (
SELECT pat_gender_cd, year(CURRENT_DATE) - pat_brth_yr_nbr AS age,
        ROW_NUMBER() OVER (PARTITION BY pat_gender_cd ORDER BY age) AS row_num,
        COUNT(*) OVER (PARTITION BY pat_gender_cd) AS total_rows
FROM use_data
)
, median_data as
(
SELECT pat_gender_cd, AVG(age) AS median_age
FROM (
 SELECT pat_gender_cd, age
 FROM sorted_data
 WHERE (row_num = FLOOR((total_rows + 1) / 2)) OR
       (row_num = CEIL((total_rows + 1) / 2))
) AS subquery
GROUP BY pat_gender_cd
)
, combined_data AS (
 SELECT pat_gender_cd as gender, 'mean_age' AS metric, mean_age AS value FROM mean_data
 UNION ALL
 SELECT pat_gender_cd as gender, 'median_age' AS metric, median_age AS value FROM median_data
 UNION ALL
 SELECT pat_gender_cd as gender, 'percent_patients' AS metric, percent_patients AS value FROM percent_data
)
, overall_data as(
   SELECT 'Overall' AS gender, 'mean_age' AS metric, AVG(CASE WHEN metric = 'mean_age' THEN value END) AS value
   FROM combined_data
   WHERE gender IN ('M', 'F', 'U')
   UNION ALL
   SELECT 'Overall' AS gender, 'median_age' AS metric, AVG(CASE WHEN metric = 'median_age' THEN value END) AS value
   FROM combined_data
   WHERE gender IN ('M', 'F', 'U')
   UNION ALL
   SELECT 'Overall' AS gender, 'percent_patients' AS metric, SUM(CASE WHEN metric = 'percent_patients' THEN value END) AS value
   FROM combined_data
   WHERE gender IN ('M', 'F', 'U')
   union
   select *
   from combined_data
)
, T1 as(
 select NULL as year, NULL as class, NULL as dosage_type, NULL as brand_generic, NULL as treatment_class, NULL as metric_type, metric, gender, NULL as patient_age_group, NULL as patient_age_group_3bins, NULL as patient_age_group_4bins, value
 from overall_data
)
select * FROM T1;






CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."AGE_GENDER_OUTPUT_TABLE" AS
WITH T0 as
(
select distinct (YEAR(CURRENT_DATE) - B.pat_brth_yr_nbr) as age, PAT_GENDER_CD, A.patient_id,
CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 25 then '18-25'
   when age >= 26 and age <= 35 then '26-35'
   when age >= 36 and age <= 45 then '36-45'
   when age >= 46 and age <= 55 then '46-55'
   when age >= 56 and age <= 65 then '56-65'
   when age > 65 then 'Above 65'
End as patient_age_group,




CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 45 then '18-45'
   when age >= 46 and age <= 65 then '46-65'
   when age > 65 then 'Above 65'
End as patient_age_group_4bins,




CASE
   when age < 18 then 'Below 18'
   when age >= 18 and age <= 65 then '18-65'
   when age > 65 then 'Above 65'
End as patient_age_group_3bins




FROM "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" A join "KARUNA_ANALYTICS"."ADM"."PATIENT" B on A.patient_id = B.patient_id
Where B.SCHIZ_DIAG_CD_FLAG = 'Y' and B.pat_brth_yr_nbr is NOT NULL and A.month_id <= '202210'
)
, T1 as
(
select pat_gender_cd as Gender, patient_age_group, patient_age_group_3bins, patient_age_group_4bins, count(*) as value
from T0
group by 1, 2, 3, 4
)
, T2 as
(
select NULL as year, NULL as class, NULL as dosage_type, NULL as brand_generic, NULL as treatment_class, NULL as metric_type, NULL as metric, gender, patient_age_group, patient_age_group_3bins, patient_age_group_4bins, value
from T1   
)
Select * from T2;




CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PATIENT_DEMOGRAPHICS_TABLEAU" AS
WITH T0 as
(
Select *, 'Treatment' as Data_Source from "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE"
union
select *, 'Patients' as Data_Source from "KARUNA_ANALYTICS"."REPORTING"."PATIENT_GENDER_SCHIZ_DATA"
union
select *, 'Gender' as Data_Source from "KARUNA_ANALYTICS"."REPORTING"."AGE_GENDER_OUTPUT_TABLE"
)
, T1 as
(
select max(SVC_DT) as Data_Date from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" A where A.month_id <= '202210'
)
, T2 as
(
select * from T0 full join T1
)
select * from T2;




drop table "KARUNA_ANALYTICS"."REPORTING"."CLAIMS_RX_OUTPUT_TABLE";
drop table "KARUNA_ANALYTICS"."REPORTING"."PATIENT_GENDER_SCHIZ_DATA";
drop table "KARUNA_ANALYTICS"."REPORTING"."AGE_GENDER_OUTPUT_TABLE";




select * from "KARUNA_ANALYTICS"."REPORTING"."PATIENT_DEMOGRAPHICS_TABLEAU" where data_source='Patients';



