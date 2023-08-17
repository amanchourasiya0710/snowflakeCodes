CREATE
OR REPLACE TEMPORARY TABLE "KARUNA_ANALYTICS"."REPORTING"."ANNA_SCZ_PATIENT" AS WITH T1 AS (
    SELECT
        *
    FROM
        KARUNA.IQVIA_APLD.FACT_DX AS A
        JOIN (
            SELECT
                DISTINCT DIAG_CD
            FROM
                KARUNA.ANCILLARY_DATA."Schizophrenia 87 DG Codes"
        ) AS B ON A.DIAG_CD = B.DIAG_CD
),
T2 AS (
    SELECT
        DISTINCT T1.PATIENT_ID,
        FIRST_SCZ_DIAG,
        DATEADD(YEAR, -1, FIRST_SCZ_DIAG) AS SCZ_RX_CUTOFF
    FROM
        T1
        JOIN (
            SELECT
                PATIENT_ID,
                MIN(SVC_DT) AS FIRST_SCZ_DIAG
            FROM
                T1
            GROUP BY
                PATIENT_ID
        ) AS B ON T1.PATIENT_ID = B.PATIENT_ID
)
SELECT
    T2.*,
    B.PAT_BRTH_YR_NBR AS BIRTH_YEAR
FROM
    T2
    JOIN KARUNA.IQVIA_APLD.PATIENT AS B ON T2.PATIENT_ID = B.PATIENT_ID;

--CLEAN UP APLD TABLE, WILL APPLY AGE CRITERIA AT EACH STUDY MONTH LEVEL
CREATE
OR REPLACE TEMPORARY TABLE "KARUNA_ANALYTICS"."REPORTING"."ANNA_APLD_SOB" AS WITH T0 AS -- ONLY include scz patients
(
    SELECT
        A.*,
        BIRTH_YEAR,
        (YEAR(SVC_DT) - BIRTH_YEAR) AS AGE,
        B.SCZ_RX_CUTOFF
    FROM
        KARUNA.IQVIA_APLD.FACT_RX AS A
        JOIN "KARUNA_ANALYTICS"."REPORTING"."ANNA_SCZ_PATIENT" AS B ON A.PATIENT_ID = B.PATIENT_ID
),
T1 AS --MERGE product details FROM product TABLE
(
    SELECT
        A.*,
        B.MKTED_PROD_NM,
        B.STRNT_DESC,
        B.DOSAGE_FORM_NM,
        B.USC_DESC
    FROM
        T0 AS A
        JOIN KARUNA.IQVIA_APLD.PRODUCT AS B ON A.PRODUCT_ID = B.PRODUCT_ID
),
T2 AS --MERGE WITH market difinition & define treatment class
(
    SELECT
        T1.*,
        B."Product.Name",
        B."Product.Group.Name" AS PRODUCT_GROUP,
        B."B.G",
        B."DOSAGE_TYPE",
        B."Class",
        CASE
            WHEN B."DOSAGE_TYPE" = 'INJ_LAI' THEN 'LAIs'
            WHEN B."DOSAGE_TYPE" = 'ORAL'
            AND B."B.G" = 'GENERIC' THEN 'Generic-Orals'
            WHEN B."DOSAGE_TYPE" = 'ORAL'
            AND B."B.G" = 'BRANDED' THEN 'Branded-Orals'
            ELSE 'NA'
        END AS TREATMENT_CLASS
    FROM
        T1
        INNER JOIN KARUNA.ANCILLARY_DATA."Master Market Definition_1229" AS B ON T1.MKTED_PROD_NM = B."Product.Name"
),
T3 AS -- FILTER OUT PROCHLORPERAZINE PRODUCTS
(
    SELECT
        *
    FROM
        T2
    WHERE
        PRODUCT_GROUP != 'PROCHLORPERAZINE'
) -- FILTER DAILY DOSAGE FOR Quetiapine/Seroquel AND Aripiprazole/Abilify 
SELECT
    *,
    REGEXP_SUBSTR(STRNT_DESC, '\\d+') AS STRENGTH,
    STRENGTH * DSPNSD_QTY / NULLIF(DAYS_SUPPLY_CNT, 0) AS DAILY_DOSAGE,
    CASE
        WHEN (
            PRODUCT_GROUP IN ('QUETIAPINE', 'SEROQUEL')
            AND DAILY_DOSAGE < 300
        )
        OR (
            PRODUCT_GROUP IN ('ARIPIPRAZOLE', 'ABILIFY')
            AND DAILY_DOSAGE < 10
        )
        OR (
            PRODUCT_GROUP IN ('QUETIAPINE', 'SEROQUEL')
            AND DAILY_DOSAGE IS NULL
        )
        OR (
            PRODUCT_GROUP IN ('ARIPIPRAZOLE', 'ABILIFY')
            AND DAILY_DOSAGE IS NULL
        ) THEN 'YES'
        ELSE 'NO'
    END AS MG_EXCLUSION
FROM
    T3
WHERE
    MG_EXCLUSION = 'NO';

-- create or replace table "KARUNA_ANALYTICS"."REPORTING"."SOB_MONTHS" (
--     month varchar(6),
--     qtr varchar(6)
-- );
-- insert into karuna_analytics.reporting.sob_months values ('202204', '2022Q2');
-- insert into karuna_analytics.reporting.sob_months values ('202205', '2022Q2');
-- insert into karuna_analytics.reporting.sob_months values ('202206', '2022Q2');
-- insert into karuna_analytics.reporting.sob_months values ('202207', '2022Q3');
-- insert into karuna_analytics.reporting.sob_months values ('202208', '2022Q3');
-- insert into karuna_analytics.reporting.sob_months values ('202209', '2022Q3');
-- insert into karuna_analytics.reporting.sob_months values ('202210', '2022Q4');
-- insert into karuna_analytics.reporting.sob_months values ('202211', '2022Q4');
-- insert into karuna_analytics.reporting.sob_months values ('202212', '2022Q4');
-- insert into karuna_analytics.reporting.sob_months values ('202301', '2023Q1');
-- insert into karuna_analytics.reporting.sob_months values ('202302', '2023Q1');
-- insert into karuna_analytics.reporting.sob_months values ('202303', '2023Q1');
select
    *
from
    "KARUNA_ANALYTICS"."REPORTING"."SOB_MONTHS";

select
    *
from
    "KARUNA_ANALYTICS"."REPORTING"."SOB_OUTPUT"
where
    qtr = '2023Q1'
    or qtr = '2022Q2';

select
    *
from
    "KARUNA_ANALYTICS"."REPORTING"."SOB_OUTPUT"
where
    qtr = '2022Q3'
    or qtr = '2022Q4';

CREATE
OR REPLACE PROCEDURE "KARUNA_ANALYTICS"."REPORTING".automate_sob() RETURNS varchar not null LANGUAGE JAVASCRIPT AS $ $ var s11 = `SELECT * FROM "KARUNA_ANALYTICS"."REPORTING"."SOB_MONTHS" CROSS JOIN (SELECT DISTINCT "Product.Group.Name" AS PRODUCT FROM "KARUNA"."ANCILLARY_DATA"."Master Market Definition_1229");`;

var stmt11 = snowflake.createStatement({ sqlText: s11 });

var res11 = stmt11.execute();

while(res11.next()) { MONTH = res11.getColumnValue(1);

QTR = res11.getColumnValue(2);

PDT = res11.getColumnValue(3);

var s1 = `CREATE OR REPLACE TEMPORARY TABLE "KARUNA_ANALYTICS"."REPORTING"."ANNA_PATIENT_COHORT" AS
        WITH T0 AS (
            SELECT *, '` + QTR + `' AS QTR FROM "KARUNA_ANALYTICS"."REPORTING"."ANNA_APLD_SOB" WHERE MONTH_ID = '` + MONTH + `' AND PRODUCT_GROUP = '` + PDT + `' AND AGE >= 18),
        T1 AS (
        SELECT T0.*, ROW_NUMBER() OVER (PARTITION BY PATIENT_ID ORDER BY SVC_DT ASC, CLAIM_ID) AS ORDER_NUM FROM T0)
        SELECT MONTH_ID, QTR, PATIENT_ID, SVC_DT, PRODUCT_GROUP FROM T1 WHERE ORDER_NUM=1;`;

var stmt1 = snowflake.createStatement({ sqlText: s1 });

var res1 = stmt1.execute();

var s1 = `CREATE OR REPLACE TEMPORARY TABLE "KARUNA_ANALYTICS"."REPORTING"."ANNA_MONTH_PRODUCT" AS  
        WITH T0 AS (
        SELECT * FROM "KARUNA_ANALYTICS"."REPORTING"."ANNA_APLD_SOB" WHERE PATIENT_ID IN (SELECT DISTINCT PATIENT_ID FROM "KARUNA_ANALYTICS"."REPORTING"."ANNA_PATIENT_COHORT")),
        T1 AS (
        SELECT T0.*, A.SVC_DT AS DATE_OF_INTEREST
        FROM T0 JOIN "KARUNA_ANALYTICS"."REPORTING"."ANNA_PATIENT_COHORT" AS A ON T0.PATIENT_ID=A.PATIENT_ID 
        WHERE T0.SVC_DT < DATE_OF_INTEREST AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)), --1 YEAR LOOK BACK FOR SWITCH
        T2 AS (
        SELECT T1.*, ROW_NUMBER() OVER (PARTITION BY PATIENT_ID ORDER BY SVC_DT DESC, CLAIM_ID) AS ORDER_NUM 
        FROM T1 ORDER BY PATIENT_ID, SVC_DT DESC, CLAIM_ID),
        T21 AS ( -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
        SELECT DISTINCT T2.PATIENT_ID FROM T2 JOIN "KARUNA_ANALYTICS"."REPORTING"."ANNA_PATIENT_COHORT" AS B ON T2.PATIENT_ID = B.PATIENT_ID AND T2.PRODUCT_GROUP=B.PRODUCT_GROUP
        WHERE T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)), --1 YEAR LOOK BACK FOR NBRX
        T3 AS (
        SELECT * FROM T2 WHERE ORDER_NUM = 1 AND PATIENT_ID NOT IN (SELECT PATIENT_ID FROM T21)),
        T31 AS (
        SELECT * FROM "KARUNA_ANALYTICS"."REPORTING"."ANNA_PATIENT_COHORT" WHERE PATIENT_ID NOT IN (SELECT PATIENT_ID FROM T21)),
        T4 AS (
        SELECT B.PATIENT_ID, B.STUDY_MONTH, B.QTR, 
        CASE WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET' ELSE T3.PRODUCT_GROUP END AS SWITCH_FROM, 
        CASE WHEN TREATMENT_CLASS IS NULL THEN 'New to Market' ELSE TREATMENT_CLASS END AS SWITCH_FROM_TREATMENT_CLASS, SWITCH_TO FROM T3 
        RIGHT JOIN (SELECT PATIENT_ID, MONTH_ID AS STUDY_MONTH, QTR, PRODUCT_GROUP AS SWITCH_TO FROM T31) AS B
        ON T3.PATIENT_ID=B.PATIENT_ID WHERE SWITCH_FROM != SWITCH_TO),
        T41 AS (
        SELECT PATIENT_ID FROM T4 WHERE SWITCH_FROM = 'NEW TO MARKET'),
        T42 AS (
        SELECT * FROM "KARUNA_ANALYTICS"."REPORTING"."ANNA_APLD_SOB" WHERE PATIENT_ID IN (SELECT DISTINCT PATIENT_ID FROM T41)),
        T43 AS (
        SELECT T42.*, A.SVC_DT AS DATE_OF_INTEREST
        FROM T42 JOIN "KARUNA_ANALYTICS"."REPORTING"."ANNA_PATIENT_COHORT" AS A ON T42.PATIENT_ID=A.PATIENT_ID 
        WHERE T42.SVC_DT < DATE_OF_INTEREST AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)), --4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
        T44 AS (
        SELECT DISTINCT T41.PATIENT_ID, 'RESTART' AS SWITCH_FROM_UPDATE, 'Restart' AS TREATMENT_CLASS_UPDATE
        FROM T41 JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID),
        T45 AS (
        SELECT T4.*, SWITCH_FROM_UPDATE, TREATMENT_CLASS_UPDATE FROM T4 LEFT JOIN T44 ON T4.PATIENT_ID=T44.PATIENT_ID),
        T46 AS (
        SELECT PATIENT_ID, STUDY_MONTH, QTR, 
        CASE WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM ELSE SWITCH_FROM_UPDATE END AS SWITCH_FROM,
        CASE WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS ELSE TREATMENT_CLASS_UPDATE END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO FROM T45),
        T5 AS (
        SELECT STUDY_MONTH, QTR, SWITCH_FROM, SWITCH_FROM_TREATMENT_CLASS, SWITCH_TO, COUNT(*) AS PATIENT_COUNT FROM T46 GROUP BY STUDY_MONTH, QTR, SWITCH_FROM, 
        SWITCH_FROM_TREATMENT_CLASS, SWITCH_TO)
        SELECT T5.*, PATIENT_COUNT/TOTAL_COUNT AS PATIENT_SHARE FROM T5, (SELECT SUM(PATIENT_COUNT) AS TOTAL_COUNT FROM T5) AS B 
        ORDER BY PATIENT_SHARE DESC;`;

var stmt1 = snowflake.createStatement({ sqlText: s1 });

var res1 = stmt1.execute();

var s1 = `INSERT INTO "KARUNA_ANALYTICS"."REPORTING"."SOB_OUTPUT"
        SELECT * FROM "KARUNA_ANALYTICS"."REPORTING"."ANNA_MONTH_PRODUCT"`;

var stmt1 = snowflake.createStatement({ sqlText: s1 });

var res1 = stmt1.execute();

} return '0';

$ $;

call "KARUNA_ANALYTICS"."REPORTING".automate_sob();