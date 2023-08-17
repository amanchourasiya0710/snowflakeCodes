CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_1" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, month_id
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 as
(
select cast(month_id/100 as INT) as year, * 
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y'
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(ovr_trx_cnt) as value1
from T1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T34 as
(
Select * from T18
union
Select * from T19
union
Select * from T20
union
Select * from T21
union
Select * from T23
union
Select * from T24
union
Select * from T26
union
Select * from T29
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4, 5
)
, T37 as
(
select class, dosage_type, brand_generic, year, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3, 4
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.year, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic and T36.year = T37.year
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, year, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value
from T38
where year <= 2022
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_5" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, month_id
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 as
(
select cast(month_id/100 as INT) as year, * 
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y'
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(ovr_nbrx_cnt) as value1
from T1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T34 as
(
Select * from T18
union
Select * from T19
union
Select * from T20
union
Select * from T21
union
Select * from T23
union
Select * from T24
union
Select * from T26
union
Select * from T29
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4, 5
)
, T37 as
(
select class, dosage_type, brand_generic, year, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3, 4
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.year, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic and T36.year = T37.year
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, year, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value
from T38
where year <= 2022
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_9" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, month_id
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 as
(
select cast(month_id/100 as INT) as year, * 
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y'
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(schiz_factored_trx_cnt) as value1
from T1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T34 as
(
Select * from T18
union
Select * from T19
union
Select * from T20
union
Select * from T21
union
Select * from T23
union
Select * from T24
union
Select * from T26
union
Select * from T29
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4, 5
)
, T37 as
(
select class, dosage_type, brand_generic, year, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3, 4
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.year, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic and T36.year = T37.year
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, year, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value
from T38
where year <= 2022
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_13" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, month_id
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 as
(
select cast(month_id/100 as INT) as year, * 
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y'
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(schiz_nbrx_cnt) as value1
from T1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T34 as
(
Select * from T18
union
Select * from T19
union
Select * from T20
union
Select * from T21
union
Select * from T23
union
Select * from T24
union
Select * from T26
union
Select * from T29
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4, 5
)
, T37 as
(
select class, dosage_type, brand_generic, year, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3, 4
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.year, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic and T36.year = T37.year
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, year, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value
from T38
where year <= 2022
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_" AS
WITH T1 AS
(
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_1"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_5"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_9"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_13"
)
select * from T1;

drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_1";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_5";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_9";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_13";

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_1" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, month_id
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 as
(
select cast(month_id/100 as INT) as year, * 
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN')
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(ovr_trx_cnt) as value1
from T1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T34 as
(
Select * from T18
union
Select * from T19
union
Select * from T20
union
Select * from T21
union
Select * from T23
union
Select * from T24
union
Select * from T26
union
Select * from T29
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4, 5
)
, T37 as
(
select class, dosage_type, brand_generic, year, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3, 4
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.year, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic and T36.year = T37.year
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, year, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value
from T38
where year <= 2022
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_5" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, month_id
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 as
(
select cast(month_id/100 as INT) as year, * 
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN')
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(ovr_nbrx_cnt) as value1
from T1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T34 as
(
Select * from T18
union
Select * from T19
union
Select * from T20
union
Select * from T21
union
Select * from T23
union
Select * from T24
union
Select * from T26
union
Select * from T29
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4, 5
)
, T37 as
(
select class, dosage_type, brand_generic, year, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3, 4
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.year, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic and T36.year = T37.year
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, year, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value
from T38
where year <= 2022
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_9" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, month_id
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 as
(
select cast(month_id/100 as INT) as year, * 
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN')
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(schiz_factored_trx_cnt) as value1
from T1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T34 as
(
Select * from T18
union
Select * from T19
union
Select * from T20
union
Select * from T21
union
Select * from T23
union
Select * from T24
union
Select * from T26
union
Select * from T29
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4, 5
)
, T37 as
(
select class, dosage_type, brand_generic, year, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3, 4
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.year, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic and T36.year = T37.year
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, year, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value
from T38
where year <= 2022
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_13" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, month_id
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 as
(
select cast(month_id/100 as INT) as year, * 
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN')
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(schiz_nbrx_cnt) as value1
from T1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, year, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, year, value1
from T18
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T34 as
(
Select * from T18
union
Select * from T19
union
Select * from T20
union
Select * from T21
union
Select * from T23
union
Select * from T24
union
Select * from T26
union
Select * from T29
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, year, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4, 5
)
, T37 as
(
select class, dosage_type, brand_generic, year, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3, 4
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.year, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic and T36.year = T37.year
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, year, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value
from T38
where year <= 2022
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_" AS
WITH T1 AS
(
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_1"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_5"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_9"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_13"
)
select * from T1;

drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_1";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_5";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_9";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_13";

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PRESCRIBER_RX_SUMMARY_YEAR" AS
WITH T1 AS
(
  select *, '76K' as "HCPs" from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_"
  union
  select *, 'Overall' as "HCPs" from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_"
)
select * from T1;

drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_76KHCP_";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_SD_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_";