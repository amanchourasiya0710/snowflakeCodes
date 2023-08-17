SELECT distinct MONTH FROM "KARUNA"."IQVIA_SMART"."NPA_MONTHLY" order by MONTH desc;
SELECT distinct MONTH FROM "KARUNA"."IQVIA_SMART"."PATIENT_INSIGHTS_MONTHLY" order by MONTH desc;
SELECT distinct DATE_TRUNC('month', "SVC_DT") as "MONTH" from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" order by MONTH desc;
SELECT * FROM "KARUNA"."IQVIA_SMART"."NPA_MONTHLY" order by MONTH desc;

select distinct spcl_desc from "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX";
select distinct SPECIALTY_GROUP from "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX";
select distinct SUB_SPCL_DESC from "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX";

SELECT *,"Month" AS MONTH FROM "KARUNA"."ANCILLARY_DATA"."Monthly NBRx_0315";

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_DATA_NBRX" AS
WITH MF AS (
SELECT "Product.Name" ,"Roll.Up.Flag" ,"APLD.Flag" ,"Smart" ,"Xponent" ,"Data.Source" ,"Product_Name_from_v1.4" ,DRUG_GROUP_NAME ,"Product.Group.Name" ,
"B.G" ,DOSAGE_TYPE ,"Class" ,"Acute.Flag"  FROM "KARUNA"."ANCILLARY_DATA"."Master Market Definition_1229"
)
, MNNUM AS (
  select *
  FROM "KARUNA"."IQVIA_SMART"."PATIENT_INSIGHTS_MONTHLY"
    //where month_id not in ('202302', '202301', '202212', '202211')
)
,WKNUM AS (
SELECT MONTH, ROW_NUMBER ()over( ORDER BY MONTH  DESC) AS MONTH_ORDER FROM (SELECT DISTINCT "MONTH" FROM MNNUM)
)
,WKNUM2 AS (
SELECT * FROM MNNUM
)
,JOINMF AS (
SELECT A.*,B."Product.Group.Name" AS PRODUCT_GROUP,B."B.G" AS BRAND_GENERIC,B.DOSAGE_TYPE,B."Class" AS CLASS,C.MONTH_ORDER,
CASE WHEN B."B.G" = 'BRANDED' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Branded'
WHEN B."B.G" = 'GENERIC' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Generic'
WHEN B."Class" = 'Typical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Typical-Oral' 
WHEN B.DOSAGE_TYPE = 'INJ_LAI' THEN 'LAIs' ELSE NULL END AS TREATMENT_CLASS
,CASE WHEN TREATMENT_CLASS='Atypical-Oral-Generic' THEN 1 WHEN TREATMENT_CLASS='Atypical-Oral-Branded' THEN 2 
WHEN TREATMENT_CLASS='LAIs'  THEN 3 WHEN TREATMENT_CLASS='Typical-Oral'  THEN 4 ELSE 5 END AS TCSFLAG
,CASE WHEN B."Product.Group.Name" IN ('ABILIFY MAINTENA','ARIPIPRAZOLE','ARISTADA','CAPLYTA','CLOZAPINE','HALOPERIDOL',
'FANAPT','RISPERDAL CONSTA','INVEGA','INVEGA SUSTENNA','INVEGA TRINZA','LATUDA','LURASIDONE','LYBALVI','OLANZAPINE',
'QUETIAPINE','REXULTI','RISPERIDONE','VRAYLAR') THEN B."Product.Group.Name" ELSE 'ALL Others' END AS FOCUSED_PG
FROM WKNUM2 A LEFT JOIN MF B ON A."PRODUCT_SUM"=B."Product.Name" 
LEFT JOIN WKNUM C ON A.MONTH =C.MONTH
)
SELECT * FROM JOINMF;

CREATE OR REPLACE TABLE "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_DATA_TRX" AS
WITH MF AS (
SELECT "Product.Name" ,"Roll.Up.Flag" ,"APLD.Flag" ,"Smart" ,"Xponent" ,"Data.Source" ,"Product_Name_from_v1.4" ,DRUG_GROUP_NAME ,"Product.Group.Name" ,
"B.G" ,DOSAGE_TYPE ,"Class" ,"Acute.Flag"  FROM "KARUNA"."ANCILLARY_DATA"."Master Market Definition_1229"
)
, MNNUM AS (
  select *
  FROM "KARUNA"."IQVIA_SMART"."NPA_MONTHLY"
    //where month_id not in ('202302', '202301', '202212', '202211')
)
,WKNUM AS (
SELECT MONTH, ROW_NUMBER ()over( ORDER BY MONTH  DESC) AS MONTH_ORDER FROM (SELECT DISTINCT "MONTH" FROM MNNUM)
)
,WKNUM2 AS (
SELECT * FROM MNNUM
)
,JOINMF AS (
SELECT A.*,B."Product.Group.Name" AS PRODUCT_GROUP,B."B.G" AS BRAND_GENERIC,B.DOSAGE_TYPE,B."Class" AS CLASS,C.MONTH_ORDER,
CASE WHEN B."B.G" = 'BRANDED' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Branded'
WHEN B."B.G" = 'GENERIC' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Generic'
WHEN B."Class" = 'Typical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Typical-Oral' 
WHEN B.DOSAGE_TYPE = 'INJ_LAI' THEN 'LAIs' ELSE NULL END AS TREATMENT_CLASS
,CASE WHEN TREATMENT_CLASS='Atypical-Oral-Generic' THEN 1 WHEN TREATMENT_CLASS='Atypical-Oral-Branded' THEN 2 
WHEN TREATMENT_CLASS='LAIs'  THEN 3 WHEN TREATMENT_CLASS='Typical-Oral'  THEN 4 ELSE 5 END AS TCSFLAG
,CASE WHEN B."Product.Group.Name" IN ('ABILIFY MAINTENA','ARIPIPRAZOLE','ARISTADA','CAPLYTA','CLOZAPINE','HALOPERIDOL',
'FANAPT','RISPERDAL CONSTA','INVEGA','INVEGA SUSTENNA','INVEGA TRINZA','LATUDA','LURASIDONE','LYBALVI','OLANZAPINE',
'QUETIAPINE','REXULTI','RISPERIDONE','VRAYLAR') THEN B."Product.Group.Name" ELSE 'ALL Others' END AS FOCUSED_PG
FROM WKNUM2 A LEFT JOIN MF B ON A."PRODUCT_SUM"=B."Product.Name" 
LEFT JOIN WKNUM C ON A.MONTH =C.MONTH
)
SELECT * FROM JOINMF;

create or replace table "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU_OVERALL" as
with union_data as (
        select "PRODUCT_SUM" as "PRODUCT", "PRODUCT_GROUP" AS "PRODUCT_FAMILY", "FOCUSED_PG" AS "PRODUCT_GROUP", "BRAND_GENERIC", "DOSAGE_TYPE", "CLASS", "TREATMENT_CLASS", "TCSFLAG", DATE_TRUNC('month', "MONTH") AS "MONTH", "MONTH_ORDER", 'Overall' as "MARKET", 'NBRx' as "METRIC_TYPE", "NBRX" as "VALUE"
        from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_DATA_NBRX"
    union all
        select "PRODUCT_SUM" as "PRODUCT", "PRODUCT_GROUP" AS "PRODUCT_FAMILY", "FOCUSED_PG" AS "PRODUCT_GROUP", "BRAND_GENERIC", "DOSAGE_TYPE", "CLASS", "TREATMENT_CLASS", "TCSFLAG", DATE_TRUNC('month', "MONTH") AS "MONTH", "MONTH_ORDER", 'Overall' as "MARKET", 'TRx' as "METRIC_TYPE", "TRX" as "VALUE"
        from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_DATA_TRX"
)
select * from union_data where "PRODUCT_FAMILY" is not null;


// For Monthly but Corrrectly!!!!

//select sum(SCHZ_FACTORED_NBRX_CNT), sum(SCHIZ_FACTORED_TRX_CNT), sum(OVR_FACTORED_NBRX_CNT), sum(OVR_FACTORED_TRX_CNT) from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX";
//select sum(SCHZ_FACTORED_NBRX_CNT), sum(SCHIZ_FACTORED_TRX_CNT), sum(OVR_FACTORED_NBRX_CNT), sum(OVR_FACTORED_TRX_CNT) from "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX";

create or replace table "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU_SCHZ" as
    with claims_rx_actual as (
        select MKTED_PROD_NM as "PRODUCT_SUM", DATE_TRUNC('month', "SVC_DT") as "MONTH", sum(SCHZ_FACTORED_NBRX_CNT) as VALUE_NBRX, sum(SCHIZ_FACTORED_TRX_CNT) as VALUE_TRX
            from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" group by 1, 2
    )
    ,claims_rx_projected as (
        select "PRODUCT_GROUP", DATE_FROM_PARTS(LEFT("Month_ID", 4), RIGHT("Month_ID", 2), 1) as "MONTH", "NBRx" as VALUE_NBRX, "TRx" as VALUE_TRX
            from "KARUNA"."ANCILLARY_DATA"."APLD_PROJECTION_20230630"
    )
    ,MF AS (
        SELECT "Product.Name" ,"Roll.Up.Flag" ,"APLD.Flag" ,"Smart" ,"Xponent" ,"Data.Source" ,"Product_Name_from_v1.4" ,DRUG_GROUP_NAME ,"Product.Group.Name" ,
        "B.G" ,DOSAGE_TYPE ,"Class" ,"Acute.Flag"  FROM "KARUNA"."ANCILLARY_DATA"."Master Market Definition_1229"
    )
    ,MF_FAMILY AS (
        SELECT DISTINCT "Product.Group.Name" AS PRODUCT_GROUP, "B.G", DOSAGE_TYPE, "Class"  FROM "KARUNA"."ANCILLARY_DATA"."Master Market Definition_1229"
    )
    ,UNIQUE_MONTHS as (
          SELECT DISTINCT "MONTH" FROM claims_rx_actual
      union
          SELECT DISTINCT "MONTH" FROM claims_rx_projected
      --union
      --    SELECT distinct DATE_TRUNC('month', "MONTH") as "MONTH" FROM "KARUNA"."IQVIA_SMART"."NPA_MONTHLY"
    )
    ,UNIQUE_MONTHS_2 AS (
        SELECT MONTH, ROW_NUMBER() over(ORDER BY MONTH DESC) AS MONTH_ORDER FROM UNIQUE_MONTHS
    )
    ,ACTUAL_TRX AS (
        SELECT A.PRODUCT_SUM, A.MONTH, A.VALUE_TRX AS VALUE, 'Schizophrenia' as "MARKET", 'TRx' as "METRIC_TYPE",
          B."Product.Group.Name" AS PRODUCT_GROUP,B."B.G" AS BRAND_GENERIC,B.DOSAGE_TYPE,B."Class" AS CLASS,C.MONTH_ORDER,
          CASE WHEN B."B.G" = 'BRANDED' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Branded'
          WHEN B."B.G" = 'GENERIC' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Generic'
          WHEN B."Class" = 'Typical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Typical-Oral' 
          WHEN B.DOSAGE_TYPE = 'INJ_LAI' THEN 'LAIs' ELSE NULL END AS TREATMENT_CLASS
          ,CASE WHEN TREATMENT_CLASS='Atypical-Oral-Generic' THEN 1 WHEN TREATMENT_CLASS='Atypical-Oral-Branded' THEN 2 
          WHEN TREATMENT_CLASS='LAIs'  THEN 3 WHEN TREATMENT_CLASS='Typical-Oral'  THEN 4 ELSE 5 END AS TCSFLAG
          ,CASE WHEN B."Product.Group.Name" IN ('ABILIFY MAINTENA','ARIPIPRAZOLE','ARISTADA','CAPLYTA','CLOZAPINE','HALOPERIDOL',
            'FANAPT','RISPERDAL CONSTA','INVEGA','INVEGA SUSTENNA','INVEGA TRINZA','LATUDA','LURASIDONE','LYBALVI','OLANZAPINE',
            'QUETIAPINE','REXULTI','RISPERIDONE','VRAYLAR') THEN B."Product.Group.Name" ELSE 'ALL Others' END AS FOCUSED_PG
        FROM claims_rx_actual A
        LEFT JOIN MF B ON A."PRODUCT_SUM"=B."Product.Name" 
        LEFT JOIN UNIQUE_MONTHS_2 C ON A.MONTH =C.MONTH
    )
    ,ACTUAL_NBRX AS (
        SELECT A.PRODUCT_SUM, A.MONTH, A.VALUE_NBRX AS VALUE, 'Schizophrenia' as "MARKET", 'NBRx' as "METRIC_TYPE",
          B."Product.Group.Name" AS PRODUCT_GROUP,B."B.G" AS BRAND_GENERIC,B.DOSAGE_TYPE,B."Class" AS CLASS,C.MONTH_ORDER,
          CASE WHEN B."B.G" = 'BRANDED' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Branded'
          WHEN B."B.G" = 'GENERIC' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Generic'
          WHEN B."Class" = 'Typical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Typical-Oral' 
          WHEN B.DOSAGE_TYPE = 'INJ_LAI' THEN 'LAIs' ELSE NULL END AS TREATMENT_CLASS
          ,CASE WHEN TREATMENT_CLASS='Atypical-Oral-Generic' THEN 1 WHEN TREATMENT_CLASS='Atypical-Oral-Branded' THEN 2 
          WHEN TREATMENT_CLASS='LAIs'  THEN 3 WHEN TREATMENT_CLASS='Typical-Oral'  THEN 4 ELSE 5 END AS TCSFLAG
          ,CASE WHEN B."Product.Group.Name" IN ('ABILIFY MAINTENA','ARIPIPRAZOLE','ARISTADA','CAPLYTA','CLOZAPINE','HALOPERIDOL',
            'FANAPT','RISPERDAL CONSTA','INVEGA','INVEGA SUSTENNA','INVEGA TRINZA','LATUDA','LURASIDONE','LYBALVI','OLANZAPINE',
            'QUETIAPINE','REXULTI','RISPERIDONE','VRAYLAR') THEN B."Product.Group.Name" ELSE 'ALL Others' END AS FOCUSED_PG
        FROM claims_rx_actual A
        LEFT JOIN MF B ON A."PRODUCT_SUM"=B."Product.Name" 
        LEFT JOIN UNIQUE_MONTHS_2 C ON A.MONTH =C.MONTH
    )
    ,PROJECTED_TRX AS (
        SELECT A.PRODUCT_GROUP, A.MONTH, A.VALUE_TRX AS VALUE, 'Schizophrenia' as "MARKET", 'TRx' as "METRIC_TYPE",
          B."PRODUCT_GROUP" AS PRODUCT_SUM,B."B.G" AS BRAND_GENERIC,B.DOSAGE_TYPE,B."Class" AS CLASS,C.MONTH_ORDER,
          CASE WHEN B."B.G" = 'BRANDED' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Branded'
          WHEN B."B.G" = 'GENERIC' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Generic'
          WHEN B."Class" = 'Typical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Typical-Oral' 
          WHEN B.DOSAGE_TYPE = 'INJ_LAI' THEN 'LAIs' ELSE NULL END AS TREATMENT_CLASS
          ,CASE WHEN TREATMENT_CLASS='Atypical-Oral-Generic' THEN 1 WHEN TREATMENT_CLASS='Atypical-Oral-Branded' THEN 2 
          WHEN TREATMENT_CLASS='LAIs'  THEN 3 WHEN TREATMENT_CLASS='Typical-Oral'  THEN 4 ELSE 5 END AS TCSFLAG
          ,CASE WHEN B."PRODUCT_GROUP" IN ('ABILIFY MAINTENA','ARIPIPRAZOLE','ARISTADA','CAPLYTA','CLOZAPINE','HALOPERIDOL',
            'FANAPT','RISPERDAL CONSTA','INVEGA','INVEGA SUSTENNA','INVEGA TRINZA','LATUDA','LURASIDONE','LYBALVI','OLANZAPINE',
            'QUETIAPINE','REXULTI','RISPERIDONE','VRAYLAR') THEN B."PRODUCT_GROUP" ELSE 'ALL Others' END AS FOCUSED_PG
        FROM claims_rx_projected A
        LEFT JOIN MF_FAMILY B ON A."PRODUCT_GROUP"=B."PRODUCT_GROUP" 
        LEFT JOIN UNIQUE_MONTHS_2 C ON A.MONTH =C.MONTH
    )
    ,PROJECTED_NBRX AS (
        SELECT A.PRODUCT_GROUP, A.MONTH, A.VALUE_NBRX AS VALUE, 'Schizophrenia' as "MARKET", 'NBRx' as "METRIC_TYPE",
          B."PRODUCT_GROUP" AS PRODUCT_SUM,B."B.G" AS BRAND_GENERIC,B.DOSAGE_TYPE,B."Class" AS CLASS,C.MONTH_ORDER,
          CASE WHEN B."B.G" = 'BRANDED' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Branded'
          WHEN B."B.G" = 'GENERIC' AND B."Class" = 'Atypical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Atypical-Oral-Generic'
          WHEN B."Class" = 'Typical AP' AND B.DOSAGE_TYPE = 'ORAL' THEN 'Typical-Oral' 
          WHEN B.DOSAGE_TYPE = 'INJ_LAI' THEN 'LAIs' ELSE NULL END AS TREATMENT_CLASS
          ,CASE WHEN TREATMENT_CLASS='Atypical-Oral-Generic' THEN 1 WHEN TREATMENT_CLASS='Atypical-Oral-Branded' THEN 2 
          WHEN TREATMENT_CLASS='LAIs'  THEN 3 WHEN TREATMENT_CLASS='Typical-Oral'  THEN 4 ELSE 5 END AS TCSFLAG
          ,CASE WHEN B."PRODUCT_GROUP" IN ('ABILIFY MAINTENA','ARIPIPRAZOLE','ARISTADA','CAPLYTA','CLOZAPINE','HALOPERIDOL',
            'FANAPT','RISPERDAL CONSTA','INVEGA','INVEGA SUSTENNA','INVEGA TRINZA','LATUDA','LURASIDONE','LYBALVI','OLANZAPINE',
            'QUETIAPINE','REXULTI','RISPERIDONE','VRAYLAR') THEN B."PRODUCT_GROUP" ELSE 'ALL Others' END AS FOCUSED_PG
        FROM claims_rx_projected A
        LEFT JOIN MF_FAMILY B ON A."PRODUCT_GROUP"=B."PRODUCT_GROUP" 
        LEFT JOIN UNIQUE_MONTHS_2 C ON A.MONTH =C.MONTH
    )
    ,union_data as (
        select "PRODUCT_SUM" as "PRODUCT", "PRODUCT_GROUP" AS "PRODUCT_FAMILY", "FOCUSED_PG" AS "PRODUCT_GROUP", "BRAND_GENERIC", "DOSAGE_TYPE", "CLASS", "TREATMENT_CLASS", "TCSFLAG", "MONTH", "MONTH_ORDER", "MARKET", "METRIC_TYPE", "VALUE"
        from ACTUAL_NBRX
    union all
        select "PRODUCT_SUM" as "PRODUCT", "PRODUCT_GROUP" AS "PRODUCT_FAMILY", "FOCUSED_PG" AS "PRODUCT_GROUP", "BRAND_GENERIC", "DOSAGE_TYPE", "CLASS", "TREATMENT_CLASS", "TCSFLAG", "MONTH", "MONTH_ORDER", "MARKET", "METRIC_TYPE", "VALUE"
        from ACTUAL_TRX
    union all
        select "PRODUCT_SUM" as "PRODUCT", "PRODUCT_GROUP" AS "PRODUCT_FAMILY", "FOCUSED_PG" AS "PRODUCT_GROUP", "BRAND_GENERIC", "DOSAGE_TYPE", "CLASS", "TREATMENT_CLASS", "TCSFLAG", "MONTH", "MONTH_ORDER", "MARKET", "METRIC_TYPE", "VALUE"
        from PROJECTED_NBRX
    union all
        select "PRODUCT_SUM" as "PRODUCT", "PRODUCT_GROUP" AS "PRODUCT_FAMILY", "FOCUSED_PG" AS "PRODUCT_GROUP", "BRAND_GENERIC", "DOSAGE_TYPE", "CLASS", "TREATMENT_CLASS", "TCSFLAG", "MONTH", "MONTH_ORDER", "MARKET", "METRIC_TYPE", "VALUE"
        from PROJECTED_TRX
    )
    SELECT * FROM union_data;
    
    select * from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU_SCHZ";


select distinct month from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU_SCHZ";

// Union all
create or replace table "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU" as
with union_data as (
        select *
        from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU_OVERALL"
    union all
        select *
        from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU_SCHZ"
)
, date_apld as (
    select max(date_trunc('month', "SVC_DT")) as data_date_apld from "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX"
)
, date_projected as (
    select min(date_from_parts(left("Month_ID", 4), right("Month_ID", 2), 1)) as min_date_projected, max(date_from_parts(left("Month_ID", 4), right("Month_ID", 2), 1)) as max_date_projected
    from "KARUNA"."ANCILLARY_DATA"."APLD_PROJECTION_20230630"
)
, date_npa as (
    select max(date_trunc('month', "MONTH")) as data_date_npa from "KARUNA"."IQVIA_SMART"."NPA_MONTHLY"
)
, final as (
    select a.*, b.data_date_apld, c.min_date_projected, c.max_date_projected, d.data_date_npa
    from union_data a
    cross join date_apld b
    cross join date_projected c
    cross join date_npa d
)
select * from final where "PRODUCT_FAMILY" is not null;

select "MARKET", "METRIC_TYPE", sum("VALUE") from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU" group by 1, 2;


select * from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU" where brand_generic is null or dosage_type is null or class is null;

select * from "KARUNA_ANALYTICS"."REPORTING"."MONTHLY_W_PROD_INFO_TABLEAU" where product = 'ABILIFY';
SELECT * FROM "KARUNA"."IQVIA_SMART"."NPA_MONTHLY" where product_sum = 'ABILIFY';

select spcl_group, sum(schz_factored_nbrx_cnt), sum(schiz_nbrx_cnt) from "KARUNA_ANALYTICS"."ADM"."PROVIDER_RX" where substr(month_id, 1, 4) = '2018' and "76K_HCP" = 'Y' group by 1;