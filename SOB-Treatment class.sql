--select * from "KARUNA_ANALYTICS"."REPORTING"."JAN_2022";
create
or replace TEMPORARY table scz_patient as with T1 as (
    select
        *
    from
        "KARUNA_ANALYTICS"."ADM"."PATIENT"
    where
        SCHIZ_DIAG_CD_FLAG = 'Y'
),
T2 AS (
    SELECT
        DISTINCT T1.PATIENT_ID,
        T1.PAT_BRTH_YR_NBR AS BIRTH_YEAR, B.FIRST_SCZ_DIAG,
        DATEADD(YEAR, -1, B.FIRST_SCZ_DIAG) AS SCZ_RX_CUTOFF
    FROM
        T1
        JOIN (
            SELECT
                PATIENT_ID,
                MIN(SVC_DT) AS FIRST_SCZ_DIAG
            FROM
                "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX"
            GROUP BY
                PATIENT_ID
        ) AS B ON T1.PATIENT_ID = B.PATIENT_ID
)
SELECT
    *
FROM
    T2;

-- Valid records of CLAIMS RX Table
-- Applied all business rules here
CREATE
OR REPLACE TEMPORARY TABLE APLD_SOB AS WITH T0 AS -- ONLY include scz patients
(
    SELECT
        A.*,
        CASE
            WHEN A."DOSAGE_TYPE" = 'INJ_LAI' THEN 'LAIs'
            WHEN A."DOSAGE_TYPE" = 'ORAL'
            AND A.brand_generic = 'GENERIC' THEN 'Generic-Orals'
            WHEN A."DOSAGE_TYPE" = 'ORAL'
            AND A.brand_generic = 'BRANDED' THEN 'Branded-Orals'
            ELSE 'NA'
        END AS TREATMENT_CLASS_2,
        BIRTH_YEAR,
        (YEAR(SVC_DT) - BIRTH_YEAR) AS AGE,
        B.SCZ_RX_CUTOFF
    FROM
        "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" AS A
        JOIN SCZ_PATIENT AS B ON A.PATIENT_ID = B.PATIENT_ID
),
T1 AS -- FILTER OUT PROCHLORPERAZINE PRODUCTS
(
    SELECT
        *
    FROM
        T0
    WHERE
        PRODUCT_GROUP != 'PROCHLORPERAZINE'
) -- FILTER DAILY DOSAGE FOR Quetiapine/Seroquel AND Aripiprazole/Abilify 
SELECT
    *,
    CASE
        WHEN (
            PRODUCT_GROUP IN ('QUETIAPINE', 'SEROQUEL')
            AND dosage_per_day < 300
        )
        OR (
            PRODUCT_GROUP IN ('ARIPIPRAZOLE', 'ABILIFY')
            AND dosage_per_day < 10
        )
        OR (
            PRODUCT_GROUP IN ('QUETIAPINE', 'SEROQUEL')
            AND dosage_per_day IS NULL
        )
        OR (
            PRODUCT_GROUP IN ('ARIPIPRAZOLE', 'ABILIFY')
            AND dosage_per_day IS NULL
        ) THEN 'YES'
        ELSE 'NO'
    END AS MG_EXCLUSION
FROM
    T1
WHERE
    MG_EXCLUSION = 'NO';

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202301'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JAN_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202302'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE FEB_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202303'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE MAR_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202204'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE APR_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202205'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE MAY_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202206'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JUN_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202207'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JUL_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202208'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE AUG_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202209'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE SEP_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202210 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202210'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE OCT_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202211 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202211'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE NOV_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202212 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202212'
        AND PRODUCT_GROUP IN (
            'VRAYLAR',
            'REXULTI',
            'CAPLYTA',
            'LYBALVI',
            'FANAPT'
        )
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE DEC_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

CREATE
OR REPLACE TEMPORARY TABLE BRANDED_MONTHLY_SHARE AS
SELECT
    *
FROM
    JAN_2022
UNION
SELECT
    *
FROM
    FEB_2022
UNION
SELECT
    *
FROM
    MAR_2022
UNION
SELECT
    *
FROM
    APR_2022
UNION
SELECT
    *
FROM
    MAY_2022
UNION
SELECT
    *
FROM
    JUN_2022
UNION
SELECT
    *
FROM
    JUL_2022
UNION
SELECT
    *
FROM
    AUG_2022
UNION
SELECT
    *
FROM
    SEP_2022
UNION
SELECT
    *
FROM
    OCT_2022
UNION
SELECT
    *
FROM
    NOV_2022
UNION
SELECT
    *
FROM
    DEC_2022;

Create
or replace table "KARUNA_ANALYTICS"."REPORTING"."SOB_BRANDED" AS WITH T0 AS (
    SELECT
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        SUM(PATIENT_COUNT) AS PATIENT_COUNT
    FROM
        BRANDED_MONTHLY_SHARE
    GROUP BY
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
),
T1 AS (
    SELECT
        QTR,
        SUM(PATIENT_COUNT) AS PATIENT_COUNT_TOTAL
    FROM
        BRANDED_MONTHLY_SHARE
    GROUP BY
        QTR
)
SELECT
    T0.QTR,
    SWITCH_FROM,
    SWITCH_FROM_TREATMENT_CLASS,
    SWITCH_TO,
    PATIENT_COUNT,
    PATIENT_COUNT / PATIENT_COUNT_TOTAL AS PATIENT_SHARE
FROM
    T0
    JOIN T1 ON T0.QTR = T1.QTR;

-----------------------------------------------------------------------------------------------------------------------
create
or replace TEMPORARY table scz_patient as with T1 as (
    select
        *
    from
        "KARUNA_ANALYTICS"."ADM"."PATIENT"
    where
        SCHIZ_DIAG_CD_FLAG = 'Y'
),
T2 AS (
    SELECT
        DISTINCT T1.PATIENT_ID,
        T1.PAT_BRTH_YR_NBR AS BIRTH_YEAR, B.FIRST_SCZ_DIAG,
        DATEADD(YEAR, -1, B.FIRST_SCZ_DIAG) AS SCZ_RX_CUTOFF
    FROM
        T1
        JOIN (
            SELECT
                PATIENT_ID,
                MIN(SVC_DT) AS FIRST_SCZ_DIAG
            FROM
                "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX"
            GROUP BY
                PATIENT_ID
        ) AS B ON T1.PATIENT_ID = B.PATIENT_ID
)
SELECT
    *
FROM
    T2;

-- Valid records of CLAIMS RX Table
-- Applied all business rules here
CREATE
OR REPLACE TEMPORARY TABLE APLD_SOB AS WITH T0 AS -- ONLY include scz patients
(
    SELECT
        A.*,
        CASE
            WHEN A."DOSAGE_TYPE" = 'INJ_LAI' THEN 'LAIs'
            WHEN A."DOSAGE_TYPE" = 'ORAL'
            AND A.brand_generic = 'GENERIC' THEN 'Generic-Orals'
            WHEN A."DOSAGE_TYPE" = 'ORAL'
            AND A.brand_generic = 'BRANDED' THEN 'Branded-Orals'
            ELSE 'NA'
        END AS TREATMENT_CLASS_2,
        BIRTH_YEAR,
        (YEAR(SVC_DT) - BIRTH_YEAR) AS AGE,
        B.SCZ_RX_CUTOFF
    FROM
        "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" AS A
        JOIN SCZ_PATIENT AS B ON A.PATIENT_ID = B.PATIENT_ID
),
T1 AS -- FILTER OUT PROCHLORPERAZINE PRODUCTS
(
    SELECT
        *
    FROM
        T0
    WHERE
        PRODUCT_GROUP != 'PROCHLORPERAZINE'
) -- FILTER DAILY DOSAGE FOR Quetiapine/Seroquel AND Aripiprazole/Abilify 
SELECT
    *,
    CASE
        WHEN (
            PRODUCT_GROUP IN ('QUETIAPINE', 'SEROQUEL')
            AND dosage_per_day < 300
        )
        OR (
            PRODUCT_GROUP IN ('ARIPIPRAZOLE', 'ABILIFY')
            AND dosage_per_day < 10
        )
        OR (
            PRODUCT_GROUP IN ('QUETIAPINE', 'SEROQUEL')
            AND dosage_per_day IS NULL
        )
        OR (
            PRODUCT_GROUP IN ('ARIPIPRAZOLE', 'ABILIFY')
            AND dosage_per_day IS NULL
        ) THEN 'YES'
        ELSE 'NO'
    END AS MG_EXCLUSION
FROM
    T1
WHERE
    MG_EXCLUSION = 'NO';

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202301'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JAN_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202302'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE FEB_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202303'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE MAR_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202204'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE APR_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202205'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE MAY_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202206'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JUN_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202207'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JUL_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202208'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE AUG_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202209'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE SEP_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202210 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202210'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE OCT_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202211 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202211'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE NOV_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202212 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202212'
        AND TREATMENT_CLASS_2 = 'Generic-Orals'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE DEC_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

CREATE
OR REPLACE TEMPORARY TABLE GENERIC_MONTH_SHARE AS
SELECT
    *
FROM
    JAN_2022
UNION
SELECT
    *
FROM
    FEB_2022
UNION
SELECT
    *
FROM
    MAR_2022
UNION
SELECT
    *
FROM
    APR_2022
UNION
SELECT
    *
FROM
    MAY_2022
UNION
SELECT
    *
FROM
    JUN_2022
UNION
SELECT
    *
FROM
    JUL_2022
UNION
SELECT
    *
FROM
    AUG_2022
UNION
SELECT
    *
FROM
    SEP_2022
UNION
SELECT
    *
FROM
    OCT_2022
UNION
SELECT
    *
FROM
    NOV_2022
UNION
SELECT
    *
FROM
    DEC_2022;

Create
or replace table "KARUNA_ANALYTICS"."REPORTING"."SOB_GENERIC" AS WITH T0 AS (
    SELECT
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        SUM(PATIENT_COUNT) AS PATIENT_COUNT
    FROM
        GENERIC_MONTH_SHARE
    GROUP BY
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
),
T1 AS (
    SELECT
        QTR,
        SUM(PATIENT_COUNT) AS PATIENT_COUNT_TOTAL
    FROM
        GENERIC_MONTH_SHARE
    GROUP BY
        QTR
)
SELECT
    T0.QTR,
    SWITCH_FROM,
    SWITCH_FROM_TREATMENT_CLASS,
    SWITCH_TO,
    PATIENT_COUNT,
    PATIENT_COUNT / PATIENT_COUNT_TOTAL AS PATIENT_SHARE
FROM
    T0
    JOIN T1 ON T0.QTR = T1.QTR;

--------------------------------------------------------------------------------------------------------------------------------
create
or replace TEMPORARY table scz_patient as with T1 as (
    select
        *
    from
        "KARUNA_ANALYTICS"."ADM"."PATIENT"
    where
        SCHIZ_DIAG_CD_FLAG = 'Y'
),
T2 AS (
    SELECT
        DISTINCT T1.PATIENT_ID,
        T1.PAT_BRTH_YR_NBR AS BIRTH_YEAR, B.FIRST_SCZ_DIAG,
        DATEADD(YEAR, -1, B.FIRST_SCZ_DIAG) AS SCZ_RX_CUTOFF
    FROM
        T1
        JOIN (
            SELECT
                PATIENT_ID,
                MIN(SVC_DT) AS FIRST_SCZ_DIAG
            FROM
                "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX"
            GROUP BY
                PATIENT_ID
        ) AS B ON T1.PATIENT_ID = B.PATIENT_ID
)
SELECT
    *
FROM
    T2;

-- Valid records of CLAIMS RX Table
-- Applied all business rules here
CREATE
OR REPLACE TEMPORARY TABLE APLD_SOB AS WITH T0 AS -- ONLY include scz patients
(
    SELECT
        A.*,
        CASE
            WHEN A."DOSAGE_TYPE" = 'INJ_LAI' THEN 'LAIs'
            WHEN A."DOSAGE_TYPE" = 'ORAL'
            AND A.brand_generic = 'GENERIC' THEN 'Generic-Orals'
            WHEN A."DOSAGE_TYPE" = 'ORAL'
            AND A.brand_generic = 'BRANDED' THEN 'Branded-Orals'
            ELSE 'NA'
        END AS TREATMENT_CLASS_2,
        BIRTH_YEAR,
        (YEAR(SVC_DT) - BIRTH_YEAR) AS AGE,
        B.SCZ_RX_CUTOFF
    FROM
        "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX" AS A
        JOIN SCZ_PATIENT AS B ON A.PATIENT_ID = B.PATIENT_ID
),
T1 AS -- FILTER OUT PROCHLORPERAZINE PRODUCTS
(
    SELECT
        *
    FROM
        T0
    WHERE
        PRODUCT_GROUP != 'PROCHLORPERAZINE'
) -- FILTER DAILY DOSAGE FOR Quetiapine/Seroquel AND Aripiprazole/Abilify 
SELECT
    *,
    CASE
        WHEN (
            PRODUCT_GROUP IN ('QUETIAPINE', 'SEROQUEL')
            AND dosage_per_day < 300
        )
        OR (
            PRODUCT_GROUP IN ('ARIPIPRAZOLE', 'ABILIFY')
            AND dosage_per_day < 10
        )
        OR (
            PRODUCT_GROUP IN ('QUETIAPINE', 'SEROQUEL')
            AND dosage_per_day IS NULL
        )
        OR (
            PRODUCT_GROUP IN ('ARIPIPRAZOLE', 'ABILIFY')
            AND dosage_per_day IS NULL
        ) THEN 'YES'
        ELSE 'NO'
    END AS MG_EXCLUSION
FROM
    T1
WHERE
    MG_EXCLUSION = 'NO';

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202301'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JAN_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202302'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE FEB_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2023Q1' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202303'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE MAR_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202204'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE APR_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202205'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE MAY_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q2' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202206'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JUN_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202207'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE JUL_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202208'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE AUG_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--Get details of all patients in Jan 2022 which have first order in these product groups
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q3' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202209'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE SEP_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB --CLAIMS RX Data after applying BR
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT -- Patients of Jan 2022
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    -- Last record in 1 year lookback period
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    -- First record of patients in 2022
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202210 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202210'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE OCT_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202211 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202211'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE NOV_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

--202212 APPLY AGE CRITERIA HERE
CREATE
OR REPLACE TEMPORARY TABLE PATIENT_COHORT AS WITH T0 AS (
    SELECT
        *,
        '2022Q4' AS QTR
    FROM
        APLD_SOB
    WHERE
        MONTH_ID = '202212'
        AND TREATMENT_CLASS_2 = 'LAIs'
        AND AGE >= 18
),
T1 AS (
    SELECT
        T0.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT ASC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T0
)
SELECT
    MONTH_ID,
    QTR,
    PATIENT_ID,
    SVC_DT,
    PRODUCT_GROUP
FROM
    T1
WHERE
    ORDER_NUM = 1;

CREATE
OR REPLACE TEMPORARY TABLE DEC_2022 AS WITH T0 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                PATIENT_COHORT
        )
),
T1 AS (
    SELECT
        T0.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T0
        JOIN PATIENT_COHORT AS A ON T0.PATIENT_ID = A.PATIENT_ID
    WHERE
        T0.SVC_DT < DATE_OF_INTEREST
        AND T0.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR SWITCH
T2 AS (
    SELECT
        T1.*,
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_ID
            ORDER BY
                SVC_DT DESC,
                CLAIM_ID
        ) AS ORDER_NUM
    FROM
        T1
    ORDER BY
        PATIENT_ID,
        SVC_DT DESC,
        CLAIM_ID
),
T21 AS (
    -- REMOVE PATIENTS THAT ARE NOT HAVING NBRX IN STUDY MONTH
    SELECT
        DISTINCT T2.PATIENT_ID
    FROM
        T2
        JOIN PATIENT_COHORT AS B ON T2.PATIENT_ID = B.PATIENT_ID
        AND T2.PRODUCT_GROUP = B.PRODUCT_GROUP
    WHERE
        T2.SVC_DT > DATEADD(YEAR, -1, DATE_OF_INTEREST)
),
--1 YEAR LOOK BACK FOR NBRX
T3 AS (
    SELECT
        *
    FROM
        T2
    WHERE
        ORDER_NUM = 1
        AND PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T31 AS (
    SELECT
        *
    FROM
        PATIENT_COHORT
    WHERE
        PATIENT_ID NOT IN (
            SELECT
                PATIENT_ID
            FROM
                T21
        )
),
T4 AS (
    SELECT
        B.PATIENT_ID,
        B.STUDY_MONTH,
        B.QTR,
        CASE
            WHEN T3.PRODUCT_GROUP IS NULL THEN 'NEW TO MARKET'
            ELSE T3.PRODUCT_GROUP
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_2 IS NULL THEN 'New to Market'
            ELSE TREATMENT_CLASS_2
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T3
        RIGHT JOIN (
            SELECT
                PATIENT_ID,
                MONTH_ID AS STUDY_MONTH,
                QTR,
                PRODUCT_GROUP AS SWITCH_TO
            FROM
                T31
        ) AS B ON T3.PATIENT_ID = B.PATIENT_ID
    WHERE
        SWITCH_FROM != SWITCH_TO
),
T41 AS (
    SELECT
        PATIENT_ID
    FROM
        T4
    WHERE
        SWITCH_FROM = 'NEW TO MARKET'
),
T42 AS (
    SELECT
        *
    FROM
        APLD_SOB
    WHERE
        PATIENT_ID IN (
            SELECT
                DISTINCT PATIENT_ID
            FROM
                T41
        )
),
T43 AS (
    SELECT
        T42.*,
        A.SVC_DT AS DATE_OF_INTEREST
    FROM
        T42
        JOIN PATIENT_COHORT AS A ON T42.PATIENT_ID = A.PATIENT_ID
    WHERE
        T42.SVC_DT < DATE_OF_INTEREST
        AND T42.SVC_DT > DATEADD(YEAR, -4, DATE_OF_INTEREST)
),
--4 YEARS LOOK BACK FOR FIRST LINE NEW TO MARKET
T44 AS (
    SELECT
        DISTINCT T41.PATIENT_ID,
        'RESTART' AS SWITCH_FROM_UPDATE,
        'Restart' AS TREATMENT_CLASS_UPDATE
    FROM
        T41
        JOIN T43 ON T41.PATIENT_ID = T43.PATIENT_ID
),
T45 AS (
    SELECT
        T4.*,
        SWITCH_FROM_UPDATE,
        TREATMENT_CLASS_UPDATE
    FROM
        T4
        LEFT JOIN T44 ON T4.PATIENT_ID = T44.PATIENT_ID
),
T46 AS (
    SELECT
        PATIENT_ID,
        STUDY_MONTH,
        QTR,
        CASE
            WHEN SWITCH_FROM_UPDATE IS NULL THEN SWITCH_FROM
            ELSE SWITCH_FROM_UPDATE
        END AS SWITCH_FROM,
        CASE
            WHEN TREATMENT_CLASS_UPDATE IS NULL THEN SWITCH_FROM_TREATMENT_CLASS
            ELSE TREATMENT_CLASS_UPDATE
        END AS SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
    FROM
        T45
),
T5 AS (
    SELECT
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        COUNT(*) AS PATIENT_COUNT
    FROM
        T46
    GROUP BY
        STUDY_MONTH,
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
)
SELECT
    T5.*,
    PATIENT_COUNT / TOTAL_COUNT AS PATIENT_SHARE
FROM
    T5,
    (
        SELECT
            SUM(PATIENT_COUNT) AS TOTAL_COUNT
        FROM
            T5
    ) AS B
ORDER BY
    PATIENT_SHARE DESC;

CREATE
OR REPLACE TEMPORARY TABLE LAIs_MONTH_SHARE AS
SELECT
    *
FROM
    JAN_2022
UNION
SELECT
    *
FROM
    FEB_2022
UNION
SELECT
    *
FROM
    MAR_2022
UNION
SELECT
    *
FROM
    APR_2022
UNION
SELECT
    *
FROM
    MAY_2022
UNION
SELECT
    *
FROM
    JUN_2022
UNION
SELECT
    *
FROM
    JUL_2022
UNION
SELECT
    *
FROM
    AUG_2022
UNION
SELECT
    *
FROM
    SEP_2022
UNION
SELECT
    *
FROM
    OCT_2022
UNION
SELECT
    *
FROM
    NOV_2022
UNION
SELECT
    *
FROM
    DEC_2022;

Create
or replace table "KARUNA_ANALYTICS"."REPORTING"."SOB_LAIS" AS WITH T0 AS (
    SELECT
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO,
        SUM(PATIENT_COUNT) AS PATIENT_COUNT
    FROM
        LAIs_MONTH_SHARE
    GROUP BY
        QTR,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS,
        SWITCH_TO
),
T1 AS (
    SELECT
        QTR,
        SUM(PATIENT_COUNT) AS PATIENT_COUNT_TOTAL
    FROM
        LAIs_MONTH_SHARE
    GROUP BY
        QTR
)
SELECT
    T0.QTR,
    SWITCH_FROM,
    SWITCH_FROM_TREATMENT_CLASS,
    SWITCH_TO,
    PATIENT_COUNT,
    PATIENT_COUNT / PATIENT_COUNT_TOTAL AS PATIENT_SHARE
FROM
    T0
    JOIN T1 ON T0.QTR = T1.QTR;

Create
or replace table "KARUNA_ANALYTICS"."REPORTING"."SOB_TABLEAU" as 
with T0 as(
    select
        QTR as quarter,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS as patient_category,
        SWITCH_TO,
        PATIENT_COUNT,
        PATIENT_SHARE,
        'Branded-Orals' as SOB
    from
        "KARUNA_ANALYTICS"."REPORTING"."SOB_BRANDED"
    UNION
    select
        QTR as quarter,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS as patient_category,
        SWITCH_TO,
        PATIENT_COUNT,
        PATIENT_SHARE,
        'Generic-Orals' as SOB
    from
        "KARUNA_ANALYTICS"."REPORTING"."SOB_GENERIC"
    UNION
    select
        QTR as quarter,
        SWITCH_FROM,
        SWITCH_FROM_TREATMENT_CLASS as patient_category,
        SWITCH_TO,
        PATIENT_COUNT,
        PATIENT_SHARE,
        'LAIs' as SOB
    from
        "KARUNA_ANALYTICS"."REPORTING"."SOB_LAIS"
),
Data_Dt as(
SELECT MAX(SVC_DT) as Data_Date FROM "KARUNA_ANALYTICS"."ADM"."CLAIMS_RX"
),
T1 as (
SELECT * FROM T0 full join Data_Dt
)
select
    *
from
    T1;