//Set global varibles
SET lives_data_date = '2023-05-01';
SET hierarchy_data_date = '2023-06-14';

select distinct datadate from "KARUNA"."BAP_FORMULARY"."HIERARCHY" order by datadate desc;

select * from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_TYPE";
select distinct month_num, month_id from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_TYPE";
select distinct product_group from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_TYPE";

select distinct month_id from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX";

select * from "KARUNA"."BAP_FORMULARY"."LIVES" limit 100;
select distinct datadate from "KARUNA"."BAP_FORMULARY"."LIVES" order by datadate desc;

select patient_id, pat_brth_yr_nbr, year(current_date) - pat_brth_yr_nbr as age from "KARUNA_ANALYTICS"."ADM"."PATIENT" limit 100;


create or replace table "KARUNA_ANALYTICS"."REPORTING"."STATE_LEVEL_TABLEAU_V2" as
with month_order as (
    select month_id, row_number() over(order by month_id desc) as month_order from (select distinct month_id from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX")
)
, pat_state as (
    select patient_id, year(current_date) - pat_brth_yr_nbr as age, patient_state as state_abr, "State.Full.Name" as state, "Tiers" as tiers from "KARUNA_ANALYTICS"."ADM"."PATIENT" a
    left join "KARUNA"."ANCILLARY_DATA"."States_full_name" b on a.patient_state  = b."State" 
)
, pat_rx as (
    select a.*, b.month_order, c.state_abr, c.state, c.tiers from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" a
    left join month_order b on a.month_id = b.month_id
    left join pat_state c on a.patient_id = c.patient_id
)
, state_rx as (
    select state, tiers, state_abr, month_id, month_order, product_group as product_family, brand_generic, dosage_type, class, treatment_class,
        sum(schiz_factored_trx_cnt) as schz_trx, sum(ovr_factored_trx_cnt) as ovr_trx
    from pat_rx where month_order <= 12 group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
, lives as (
    select a.*, b."State" as state_abr from "KARUNA"."BAP_FORMULARY"."LIVES" a
    left join "KARUNA"."ANCILLARY_DATA"."zip3_to_state" b on left(a.zip, 3) = lpad(b.zip, 3, '0')
    left join "KARUNA"."BAP_FORMULARY"."D_SEGMENT" c on a.segment_id = c.segment_id
    where a.segment_id is not null and c.benefittype_id = 1 and a.datadate = $lives_data_date
)
, state_lives as (
    select state_abr, sum(lives) as lives from lives group by 1
)
, month_order2 as (
    select month_id, row_number() over(order by month_id desc) as month_order from (select distinct month_id from "KARUNA"."IQVIA_APLD"."FACT_DX")
)
, schz_pats as (
    select distinct month_id, patient_id from "KARUNA"."IQVIA_APLD"."FACT_DX" where diag_cd in (select distinct diag_cd from "KARUNA"."ANCILLARY_DATA"."Schizophrenia 87 DG Codes")
)
, schz_pats2 as (
    select a.*, b.month_order, c.state_abr, c.age from schz_pats a
    left join month_order2 b on a.month_id = b.month_id
    left join pat_state c on a.patient_id = c.patient_id
    where month_order <= 12 and age >= 18
)
, state_pats as (
    select state_abr, count(distinct patient_id) as schz_pats from schz_pats2 group by 1
)
, final as (
    select a.*, b.lives, c.schz_pats from state_rx a
    left join state_lives b on a.state_abr = b.state_abr
    left join state_pats c on a.state_abr = c.state_abr
    where a.state_abr is not null
)
select *, case when initcap(product_family) in ('Latuda', 'Vraylar', 'Rexulti', 'Caplyta', 'Lybalvi') then initcap(product_family) else 'All Others' end as product_group
from final where state is not null;

select sum(lives) from "KARUNA_ANALYTICS"."REPORTING"."STATE_LEVEL_TABLEAU_V2";

select * from "KARUNA_ANALYTICS"."REPORTING"."STATE_LEVEL_TABLEAU_V2";// where tiers is null;


// Using State data from MDM. Use this from Jun 20th 2023

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."STATE_LEVEL_TABLEAU_V2" AS
WITH SC AS (
  SELECT "MONTH_ID", "MONTH_NUM" AS MONTH_ORDER, "STATE" AS STATE_ABR, "STATE_FULL_NAME" AS STATE,
  "TIERS", "PRODUCT_GROUP" AS PRODUCT_FAMILY, "BRAND_GENERIC", "DOSAGE_TYPE", "CLASS", "TREATMENT_CLASS",
  "LIVES", "OVERALL_TRX" AS OVR_TRX, "OVERALL_SCHIZ_TRX2" AS SCHZ_TRX, "SCZ_PATIENTS" AS SCHZ_PATS,
  case when initcap(product_family) in ('Latuda', 'Vraylar', 'Rexulti', 'Caplyta', 'Lybalvi') then initcap(product_family) else 'All Others' end as product_group
  FROM "KARUNA_ANALYTICS"."ADM"."STATE_COVERAGE"
)
SELECT * FROM SC;





//Create Formulary Data

//create or replace table "KARUNA_ANALYTICS"."REPORTING"."FORMULARY_COVERAGE_TABLEAU_V2" as
with coverage as (
    select a.formulary_id, a.payer_id, a.segment_id, a.drug_id, b.drug_name, a.tier, a.status, a.nomenclature from "KARUNA"."ANCILLARY_DATA"."COVERAGE_NOMENCLATURE" a
    inner join "KARUNA"."BAP_FORMULARY"."DRUG" b on a.drug_id = b.drug_id
    where UPPER(drug_name) in ('CAPLYTA','LATUDA','VRAYLAR','REXULTI','LYBALVI','INVEGA SUSTENNA')
)
, lives as (
    select a.*, b.drug_id, b.drug_name, b.tier, b.status, b.nomenclature, c.segment_name, c.segment_group,
        case when c.segment_name = 'FFS' and c.segment_group = 'Medicaid' then 'Medicaid/Fee For Service'
            when c.segment_name = 'Managed' and c.segment_group = 'Medicaid' then 'Medicaid/Managed'
            when c.segment_group = 'Medicare' then 'Medicare D'
            else segment_group end as segment_group_update 
    from "KARUNA"."BAP_FORMULARY"."HIERARCHY" a
    inner join coverage b on a.formulary_id = b.formulary_id and a.payer_id = b.payer_id and a.segment_id = b.segment_id
    left join "KARUNA"."BAP_FORMULARY"."SEGMENT" c on a.segment_id = c.segment_id
    where datadate = $hierarchy_data_date
)
, lives_sum as (
    select 'Lives' as metric_type, 'Overall' as indication, segment_group as segment_group_update, nomenclature as status, null as month, null as month_order, drug_name as product_group, sum(lives) as value
    from lives group by 1, 2, 3, 4, 5, 6, 7
)
select * from lives;
, month_order as (
    select month_id, row_number() over(order by month_id desc) as month_order from (select distinct month_id from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX")
)
, claims_rx as (
    select a.*, b.month_order, c.payerplan_id
    from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" a
    left join month_order b on a.month_id = b.month_id
    left join "KARUNA"."IQVIA_APLD"."D_PLAN" c on a.plan_id = c.plan_id
    where month_order <= 12
)
, claims_rx2 as (
    select a.*, b.formulary_id, b.payer_id, b.segment_id from claims_rx a inner join "KARUNA"."BAP_FORMULARY"."BRIDGE" b on a.payerplan_id = b.payerplan_id
)
, claims_rx3 as (
    select payerplan_id, formulary_id, payer_id, segment_id, initcap(mkted_prod_nm) as product, date_trunc('month', svc_dt) as month, month_order,
        sum(schiz_factored_trx_cnt) as schz_trx, sum(ovr_factored_trx_cnt) as ovr_trx
    from claims_rx2 where upper(mkted_prod_nm) in ('CAPLYTA','LATUDA','VRAYLAR','REXULTI','LYBALVI','INVEGA SUSTENNA') group by 1, 2, 3, 4, 5, 6, 7
)
, claims_rx4 as (
    select a.*, b.drug_name, b.tier, b.status, b.nomenclature, c.segment_name, c.segment_group,
        case when c.segment_name = 'FFS' and c.segment_group = 'Medicaid' then 'Medicaid/Fee For Service'
            when c.segment_name = 'Managed' and c.segment_group = 'Medicaid' then 'Medicaid/Managed'
            when c.segment_group = 'Medicare' then 'Medicare D'
            else segment_group end as segment_group_update 
    from claims_rx3 a
    inner join coverage b on a.formulary_id = b.formulary_id and a.payer_id = b.payer_id and a.segment_id = b.segment_id and a.product = b.drug_name
    left join "KARUNA"."BAP_FORMULARY"."SEGMENT" c on a.segment_id = c.segment_id
)
, claims_ovr as (
    select 'TRx' as metric, 'Overall' as indication, segment_group as segment_group_update, nomenclature as status, month, month_order, drug_name as product_group, sum(ovr_trx) as value
    from claims_rx4 group by 1, 2, 3, 4, 5, 6, 7
)
, claims_schz as (
    select 'TRx' as metric, 'Schizophrenia' as indication, segment_group as segment_group_update, nomenclature as status, month, month_order, drug_name as product_group, sum(schz_trx) as value
    from claims_rx4 group by 1, 2, 3, 4, 5, 6, 7
)
, final as (
  select * from claims_ovr
  union all
  select * from claims_schz
  union all
  select * from lives_sum
)
select * from final;

select * from "KARUNA_ANALYTICS"."REPORTING"."Master_Market_Definition_1229" where "Product.Name" = 'FANAPT';
create or replace table "KARUNA_ANALYTICS"."REPORTING"."FORMULARY_COVERAGE_TABLEAU" as
    select * from "KARUNA_ANALYTICS"."REPORTING"."FORMULARY_COVERAGE_TABLEAU_V2";
    
    
    select * from "KARUNA_ANALYTICS"."ADM"."FORMULARY_COVERAGE";

;
select count(*), count(distinct payerplan_id) from "KARUNA"."BAP_FORMULARY"."BRIDGE";
;
select count(*), count(distinct plan_id) from "KARUNA"."IQVIA_XPONENT"."D_PAYERPLAN";


select year(SVC_DT), sum(OVR_TRX_CNT) from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" group by 1;

select * from "KARUNA_ANALYTICS"."REPORTING"."STATE_LEVEL_TABLEAU_V2" limit 50;

select * from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_TYPE_DEDUPED"

//Create FIA Dashboard data

create or replace table "KARUNA_ANALYTICS"."REPORTING"."FIA_CLAIMS_TABLEAU" as
    select * from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_TYPE_DEDUPED";
    
select * from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_TYPE_DEDUPED" limit 10;



//Create OPC data for Tableau

create or replace table "KARUNA_ANALYTICS"."REPORTING"."OPC_TABLEAU" as
with t1 as (
    select * from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_UPDATED_BUCKETS"
)
, t2 as (
    select month_num, month_id, initial_opc_group, product_group, "B.G", 'All Channels' as mop_group, overall_trx_claims, schiz_nbrx_claims, type
    from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_UPDATED_BUCKETS"
)
select * from t1 union select * from t2;

select distinct initial_opc_group from "KARUNA_ANALYTICS"."REPORTING"."OPC_TABLEAU";

//Create Formulary Data

create or replace table "KARUNA_ANALYTICS"."REPORTING"."FORMULARY_COVERAGE_TABLEAU_V4" as
with coverage as (
    select a.formulary_id, a.payer_id, a.segment_id, a.drug_id, b.drug_name, a.tier, a.status, a.nomenclature from "KARUNA"."ANCILLARY_DATA"."COVERAGE_NOMENCLATURE" a
    inner join "KARUNA"."BAP_FORMULARY"."DRUG" b on a.drug_id = b.drug_id
    where UPPER(drug_name) in ('CAPLYTA','LATUDA','VRAYLAR','REXULTI','LYBALVI','INVEGA SUSTENNA')
)
, lives as (
    select a.*, b.drug_id, b.drug_name, b.tier, b.status, b.nomenclature, c.segment_name, c.segment_group,
        case when c.segment_name = 'FFS' and c.segment_group = 'Medicaid' then 'Medicaid/Fee For Service'
            when c.segment_name = 'Managed' and c.segment_group = 'Medicaid' then 'Medicaid/Managed'
            when c.segment_group = 'Medicare' then 'Medicare D'
            else segment_group end as segment_group_update 
    from "KARUNA"."BAP_FORMULARY"."HIERARCHY" a
    inner join coverage b on a.formulary_id = b.formulary_id and a.payer_id = b.payer_id and a.segment_id = b.segment_id
    left join "KARUNA"."BAP_FORMULARY"."SEGMENT" c on a.segment_id = c.segment_id
    where datadate = $hierarchy_data_date
)
, lives_sum as (
    select 'Lives' as metric_type, 'Overall' as indication, segment_group as segment_group_update, nomenclature as status, null as month, null as month_order, drug_name as product_group, sum(lives) as value
    from lives group by 1, 2, 3, 4, 5, 6, 7
)
, month_order as (
    select month_id, row_number() over(order by month_id desc) as month_order from (select distinct month_id from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX")
)
, claims_rx as (
    select a.*, b.month_order, c.payerplan_id
    from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" a
    left join month_order b on a.month_id = b.month_id
    left join "KARUNA"."IQVIA_APLD"."D_PLAN" c on a.plan_id = c.plan_id
    where month_order <= 12
)
, claims_rx2 as (
    select a.*, b.formulary_id, b.payer_id, b.segment_id from claims_rx a inner join "KARUNA"."BAP_FORMULARY"."BRIDGE" b on a.payerplan_id = b.payerplan_id
)
, claims_rx3 as (
    select payerplan_id, formulary_id, payer_id, segment_id, initcap(mkted_prod_nm) as product, date_trunc('month', svc_dt) as month, month_order,
        sum(schiz_factored_trx_cnt) as schz_trx, sum(ovr_factored_trx_cnt) as ovr_trx
    from claims_rx2 where upper(mkted_prod_nm) in ('CAPLYTA','LATUDA','VRAYLAR','REXULTI','LYBALVI','INVEGA SUSTENNA') group by 1, 2, 3, 4, 5, 6, 7
)
, claims_rx4 as (
    select a.*, b.drug_name, b.tier, b.status, b.nomenclature, c.segment_name, c.segment_group,
        case when c.segment_name = 'FFS' and c.segment_group = 'Medicaid' then 'Medicaid/Fee For Service'
            when c.segment_name = 'Managed' and c.segment_group = 'Medicaid' then 'Medicaid/Managed'
            when c.segment_group = 'Medicare' then 'Medicare D'
            else segment_group end as segment_group_update 
    from claims_rx3 a
    inner join coverage b on a.formulary_id = b.formulary_id and a.payer_id = b.payer_id and a.segment_id = b.segment_id and a.product = b.drug_name
    left join "KARUNA"."BAP_FORMULARY"."SEGMENT" c on a.segment_id = c.segment_id
)
, claims_ovr as (
    select 'TRx' as metric, 'Overall' as indication, segment_group as segment_group_update, nomenclature as status, month, month_order, drug_name as product_group, sum(ovr_trx) as value
    from claims_rx4 group by 1, 2, 3, 4, 5, 6, 7
)
, claims_schz as (
    select 'TRx' as metric, 'Schizophrenia' as indication, segment_group as segment_group_update, nomenclature as status, month, month_order, drug_name as product_group, sum(schz_trx) as value
    from claims_rx4 group by 1, 2, 3, 4, 5, 6, 7
)
, final as (
  select * from claims_ovr
  union all
  select * from claims_schz
  union all
  select * from lives_sum
)
select * from final;    




select claim_type, sum(SCHIZ_NBRX_CLAIM_CNT) from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_TYPE_DEDUPED" 
 where PRODUCT_GROUP='BRANDED APS' AND MONTH_NUM IN('1','2','3') AND MOP_GROUP='Medicare'
group by claim_type

select product_group, sum(SCHIZ_NBRX_CLAIM_CNT) from "KARUNA_ANALYTICS"."ADM"."FIA_CLAIMS_TYPE_DEDUPED" 
 where Claim_type='PD' AND MONTH_NUM IN('1','2','3') AND MOP_GROUP='Commercial'
group by product_group
