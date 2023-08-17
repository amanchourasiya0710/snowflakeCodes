CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_1" AS
WITH T0 as
(
select
CASE
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM = 1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_trx_cnt) as value1
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM = 1
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, '1 Month' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_2" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 3
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_trx_cnt) as value1
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 3
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, '3 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_3" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 6
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_trx_cnt) as value1
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 6
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, '6 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_4" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 12
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_trx_cnt) as value1
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 12
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, '12 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_5" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM = 1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_nbrx_cnt) as value1
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM = 1
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, '1 Month' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_6" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 3
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_nbrx_cnt) as value1
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 3
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, '3 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_7" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 6
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_nbrx_cnt) as value1
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 6
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, '6 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_8" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 12
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_nbrx_cnt) as value1
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 12
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, '12 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_9" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM = 1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_factored_trx_cnt) as value1
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM = 1
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, '1 Month' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_10" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 3
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_factored_trx_cnt) as value1
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 3
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, '3 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_11" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 6
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_factored_trx_cnt) as value1
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 6
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, '6 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_12" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 12
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_factored_trx_cnt) as value1
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 12
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, '12 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_13" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM = 1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_nbrx_cnt) as value1
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM = 1
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, '1 Month' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_14" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 3
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_nbrx_cnt) as value1
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 3
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, '3 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_15" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 6
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_nbrx_cnt) as value1
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 6
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, '6 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_16" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 12
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_nbrx_cnt) as value1
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and MONTH_NUM <= 12
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, '12 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_" AS
WITH T1 AS
(
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_1"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_2"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_3"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_4"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_5"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_6"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_7"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_8"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_9"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_10"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_11"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_12"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_13"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_14"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_15"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_16"
)
select * from T1;

drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_1";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_2";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_3";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_4";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_5";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_6";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_7";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_8";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_9";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_10";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_11";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_12";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_13";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_14";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_15";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_16";

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_1" AS
WITH T0 as
(
select
CASE
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM = 1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_trx_cnt) as value1
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM = 1
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, '1 Month' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_2" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 3
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_trx_cnt) as value1
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 3
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, '3 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_3" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 6
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_trx_cnt) as value1
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 6
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, '6 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_4" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 12
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_trx_cnt) as value1
from T0
where ovr_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 12
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'TRx' as Metric_Type, '12 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_5" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM = 1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_nbrx_cnt) as value1
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM = 1
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, '1 Month' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_6" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 3
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_nbrx_cnt) as value1
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 3
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, '3 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_7" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 6
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_nbrx_cnt) as value1
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 6
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, '6 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_8" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, ovr_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 12
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(ovr_nbrx_cnt) as value1
from T0
where ovr_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 12
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'OVERALL' as Indication, 'NBRx' as Metric_Type, '12 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_9" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM = 1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_factored_trx_cnt) as value1
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM = 1
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, '1 Month' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_10" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 3
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_factored_trx_cnt) as value1
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 3
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, '3 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_11" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 6
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_factored_trx_cnt) as value1
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 6
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, '6 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_12" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_factored_trx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 12
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_factored_trx_cnt) as value1
from T0
where schiz_factored_trx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 12
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'TRx' as Metric_Type, '12 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_13" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM = 1
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_nbrx_cnt) as value1
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM = 1
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, '1 Month' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_14" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 3
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_nbrx_cnt) as value1
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 3
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, '3 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_15" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 6
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_nbrx_cnt) as value1
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 6
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, '6 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_16" AS
WITH T0 as
(
select
CASE 
    WHEN spcl_group = 'PEDS - PSYCHIATRY' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'Pyschiatry? Need to confirm' THEN 'PSYCHIATRY'
    WHEN spcl_group = 'PEDIATRICS' THEN 'OTHERS'
    ELSE spcl_group
END as spcl_group, class, dosage_type, brand_generic, "76K_HCP", provider_id, schiz_nbrx_cnt, MONTH_NUM
FROM "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX"
)
, T1 AS
(
Select class, dosage_type, brand_generic, spcl_group, provider_id
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 12
group by 1, 2, 3, 4, 5
)
-- one 'ALL' data starts
, T2 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T3 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T4 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- level one data ends
-- two 'ALL' data starts
, T6 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, provider_id
from T1
)
, T7 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
, T9 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T12 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, provider_id
from T1
)
-- three 'ALL' data ends
-- four 'ALL' data starts
-- four 'ALL' data ends
, T17 as
(
Select * from T1
union
Select * from T2
union
Select * from T3
union
Select * from T4
union
Select * from T6
union
Select * from T7
union
Select * from T9
union
Select * from T12
)
, T18 AS
(
Select class, dosage_type, brand_generic, spcl_group, sum(schiz_nbrx_cnt) as value1
from T0
where schiz_nbrx_cnt > 0 and (SPCL_GROUP is not null and SPCL_GROUP != '' and SPCL_GROUP != 'UNKNOWN') and "76K_HCP" = 'Y' and MONTH_NUM <= 12
group by 1, 2, 3, 4
)
-- one 'ALL' data starts
, T19 as
(
Select 'ALL' as class, dosage_type, brand_generic, spcl_group, value1
from T18
)
, T20 as
(
Select class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T21 as
(
Select class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- level one data ends
-- two 'ALL' data starts
, T23 as
(
Select 'ALL' as class, 'ALL' as dosage_type, brand_generic, spcl_group, value1
from T18
)
, T24 as
(
Select 'ALL' as class, dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
, T26 as
(
Select class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
from T18
)
-- two 'ALL' data ends
-- three 'ALL' data starts
, T29 as
(
Select 'ALL' as class, 'ALL' as dosage_type, 'ALL' as brand_generic, spcl_group, value1
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
, T35 as
(
Select class, dosage_type, brand_generic, spcl_group, count(distinct provider_id) as total_writers 
from T17
group by 1, 2, 3, 4
)
, T36 as
(
Select class, dosage_type, brand_generic, spcl_group, sum(value1) as metric_value 
from T34
group by 1, 2, 3, 4
)
, T37 as
(
select class, dosage_type, brand_generic, sum(metric_value) as sum_value
from T36
where spcl_group!='ALL'
group by 1, 2, 3
)
, T38 as
(
select T36.class, T36.dosage_type, T36.brand_generic, T36.spcl_group, T36.metric_value, T37.sum_value
from T36 left join T37
on T36.class = T37.class and T36.dosage_type = T37.dosage_type and T36.brand_generic = T37.brand_generic
)
, T39 as
(
select T38.class, T38.dosage_type, T38.brand_generic, T38.spcl_group, T38.metric_value, T38.sum_value, T35.total_writers 
from T38 left join T35 on T38.class = T35.class and T38.dosage_type = T35.dosage_type and T38.brand_generic = T35.brand_generic and T38.spcl_group = T35.spcl_group
)
, T40 as
(
select 'SCHIZOPHRENIA' as Indication, 'NBRx' as Metric_Type, '12 Months' as TimePeriod, class, dosage_type, brand_generic, spcl_group, metric_value, sum_value, total_writers
from T39
)
Select * from T40;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_" AS
WITH T1 AS
(
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_1"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_2"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_3"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_4"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_5"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_6"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_7"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_8"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_9"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_10"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_11"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_12"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_13"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_14"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_15"
  union
  select * from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_16"
)
select * from T1;

drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_1";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_2";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_3";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_4";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_5";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_6";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_7";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_8";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_9";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_10";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_11";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_12";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_13";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_14";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_15";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_16";

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."PRESCRIBER_RX_SUMMARY_SPEC" AS
WITH T1 AS
(
  select *, '76K' as "HCPs" from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_"
  union
  select *, 'Overall' as "HCPs" from "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_"
)
select * from T1;

drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_76KHCP_";
drop table "KARUNA_ANALYTICS"."REPORTING"."PHARMACEUTICAL_DASHBOARD_OUTPUT_TABLE_OVERALLHCP_";