CREATE
OR REPLACE TABLE KARUNA_ANALYTICS.STAGING.STATE_SCZ_PATIENTS AS WITH T3 AS (
    SELECT
        *
    FROM
        KARUNA.IQVIA_APLD.F_FACT_DX
    WHERE
        DIAG_CD IN (
            SELECT
                DISTINCT DIAG_CD
            FROM
                KARUNA.IQVIA_APLD.D_DIAGNOSIS
            WHERE
                SCHIZOPHRENIA_FLAG = 'Y'
        )
),
T4 AS (
    SELECT
        A.*,
        B.PATIENT_STATE,
        C.ST_CD AS PROVIDER_STATE
    FROM
        T3 A
        LEFT JOIN KARUNA_ANALYTICS.ADM.APLD_PATIENT B ON A.PATIENT_ID = B.PATIENT_ID
        LEFT JOIN KARUNA.IQVIA_APLD.D_PROVIDER C ON A.RENDERING_PROVIDER_ID = C.PROVIDER_ID
),
T5 AS (
    SELECT
        a.*,
        b.PAT_BRTH_YR_NBR,
        2023 - PAT_BRTH_YR_NBR AS Age
    FROM
        T4 a
        LEFT JOIN KARUNA.IQVIA_APLD.PATIENT b ON a.PATIENT_ID = b.PATIENT_ID
),
T5A AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            ORDER BY
                MONTH_ID DESC
        ) AS MONTH_NUM
    FROM
        T5
),
T6 AS (
    SELECT
        *,
        CASE
            WHEN PATIENT_STATE IS NULL THEN PROVIDER_STATE
            WHEN PATIENT_STATE = 'NOT USED' THEN PROVIDER_STATE
            ELSE PATIENT_STATE
        END AS STATE
    FROM
        T5A
    WHERE
        MONTH_NUM <= 60
        AND AGE >= 18
),
T7 AS (
    SELECT
        STATE,
        COUNT (DISTINCT PATIENT_ID) AS PATIENT_COUNT
    FROM
        T6
    GROUP BY
        1
)
SELECT
    *
FROM
    T7;

-------------------------------------------------------------------------------------------------------
CREATE
OR REPLACE TABLE KARUNA_ANALYTICS.PUBLIC.LIVES_STATE_LEVEL_AGG_0726 AS with t0 AS (
    SELECT
        a.*,
        b."State" AS state,
        c."State.Full.Name" as State_full_name,
        c."Tiers" as Tiers
    FROM
        KARUNA.BAP_FORMULARY.LIVES a
        left JOIN KARUNA.ANCILLARY_DATA."zip3_to_state" b ON LEFT(a.zip, 3) = LPAD(B.ZIP, 3, '0')
        LEFT JOIN KARUNA.ANCILLARY_DATA."States_full_name" C ON b."State" = c."State"
    WHERE
        DATADATE = '2023-06-01'
),
t1 AS (
    SELECT
        a.*,
        b.segment_group,
        b.BENEFITTYPE_ID
    FROM
        t0 a
        LEFT JOIN KARUNA.BAP_FORMULARY.D_SEGMENT b ON a.SEGMENT_ID = b.segment_id
    WHERE
        FORMULARY_ID IS NOT NULL
),
T2 AS (
    SELECT
        DISTINCT STATE,
        STATE_FULL_NAME,
        TIERS,
        payer_id,
        SUM (LIVES) AS LIVES
    FROM
        T1
    GROUP BY
        1,
        2,
        3,
        4,
        5
    ORDER BY
        4 DESC
),
T3 AS (
    SELECT
        *,
        CASE
            WHEN STATE_FULL_NAME IS NULL THEN 'OTHERS'
            ELSE STATE_FULL_NAME
        END AS STATE_FULL_NAME_UPDATED
    FROM
        T2
)
SELECT
    DISTINCT STATE,
    STATE_FULL_NAME_UPDATED,
    TIERS,
    payer_id,
    LIVES
FROM
    T3;

-----------------------------------------------------------------------------------------------------------
create
or replace table karuna_analytics.STAGING.STAGE_STATE_COVERAGE_MOP as with t0 as (
    select
        a.*,
        schz_flag as schiz_flag,
        c.iqvia_mop_classification,
        c.model,
        c.model_type,
        c.operating_state,
        d.st_cd as provider_state
    from
        karuna_analytics.STAGING.stage_apld_claims a
        left join karuna.iqvia_apld.d_plan_for_mdm_apld b using (plan_id)
        left join KARUNA.IQVIA_XPONENT.D_PAYERPLAN c using(payerplan_id)
        left join KARUNA.IQVIA_APLD.D_PROVIDER D ON A.PROVIDER_ID = D.PROVIDER_ID
),
t1 as (
    select
        * exclude iqvia_mop_classification,
        case
            when model = 'HMO' THEN 'COMMERCIAL'
            WHEN model = 'GROUP' THEN 'COMMERCIAL'
            WHEN model = 'DE MMP' THEN 'MEDICARE D'
            WHEN model IN ('UNSPEC', 'UNKNOWN', 'PBM', 'PBM BOB') THEN 'UNSPEC'
            WHEN model IN ('FED ASST', 'FED EMP', 'STATE ASST', 'STATE EMP') THEN 'GOV FUNDED'
            ELSE iqvia_mop_classification
        END AS MOP_CLASSIFICATION
    from
        t0
    where
        month_num <= 60
),
t1a as (
    select
        a.*,
        b.patient_state
    from
        t1 a
        left join KARUNA_ANALYTICS.ADM.APLD_PATIENT b on a.patient_id = b.patient_id
),
t2a as (
    select
        *,
        case
            when OPERATING_STATE <> 'US'
            AND OPERATING_STATE IS NOT NULL THEN OPERATING_STATE
            WHEN (
                OPERATING_STATE = 'US'
                OR OPERATING_STATE IS NULL
            )
            AND PROVIDER_STATE IS NOT NULL THEN PROVIDER_STATE
            WHEN (
                OPERATING_STATE = 'US'
                OR OPERATING_STATE IS NULL
            )
            AND PROVIDER_STATE IS NULL THEN PATIENT_STATE
        END AS STATE
    from
        t1a
),
t2 as (
    SELECT
        A.*,
        B.STATE_FULL_NAME_UPDATED,
        B.TIERS,
        B.LIVES
    FROM
        T2A A
        LEFT JOIN KARUNA_ANALYTICS.PUBLIC.LIVES_STATE_LEVEL_AGG_0726 B ON A.STATE = B.STATE
),
t2c as (
    select
        distinct state,
        STATE_FULL_NAME_UPDATED,
        count (distinct patient_id) AS OVERALL_PATIENTS
    from
        t2
    where
        state != 'NOT USED'
    group by
        1,
        2
),
t3 as (
    SELECT
        DISTINCT MONTH_ID,
        MONTH_NUM,
        STATE,
        STATE_FULL_NAME_UPDATED,
        TIERS,
        PRODUCT_GROUP,
        BRAND_GENERIC,
        DOSAGE_TYPE,
        CLASS,
        TREATMENT_CLASS,
        MOP_CLASSIFICATION,
        LIVES,
        SUM(FACTORED_TRX) AS OVERALL_TRX
    FROM
        T2
    WHERE
        MONTH_NUM <= 12
        AND STATE IS NOT NULL
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12
    ORDER BY
        13 DESC
),
t3a as (
    select
        *
    from
        t3
    where
        MOP_CLASSIFICATION is not null
),
t3b as (
    select
        *
    from
        t3
    where
        MOP_CLASSIFICATION is null
),
T4 AS (
    SELECT
        DISTINCT MONTH_ID,
        MONTH_NUM,
        STATE,
        STATE_FULL_NAME_UPDATED,
        TIERS,
        PRODUCT_GROUP,
        BRAND_GENERIC,
        DOSAGE_TYPE,
        CLASS,
        TREATMENT_CLASS,
        MOP_CLASSIFICATION,
        LIVES,
        SUM(FACTORED_TRX) AS OVERALL_SCHIZ_TRX
    FROM
        T2
    WHERE
        SCHZ_FLAG = '1'
        AND MONTH_NUM <= 12
        AND STATE IS NOT NULL
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12
    ORDER BY
        13 DESC
),
t4a as (
    select
        *
    from
        t4
    where
        MOP_CLASSIFICATION is not null
),
t4b as (
    select
        *
    from
        t4
    where
        MOP_CLASSIFICATION is null
),
t5a as (
    SELECT
        A.*,
        B.OVERALL_SCHIZ_TRX
    FROM
        T3a A FULL
        JOIN T4a B ON A.MONTH_ID = B.MONTH_ID
        AND A.STATE = B.STATE
        AND A.PRODUCT_GROUP = B.PRODUCT_GROUP
        AND A.TREATMENT_CLASS = B.TREATMENT_CLASS
        AND A.MOP_CLASSIFICATION = B.MOP_CLASSIFICATION
),
t5b as (
    SELECT
        A.*,
        B.OVERALL_SCHIZ_TRX
    FROM
        T3b A FULL
        JOIN T4b B ON A.MONTH_ID = B.MONTH_ID
        AND A.STATE = B.STATE
        AND A.PRODUCT_GROUP = B.PRODUCT_GROUP
        AND A.TREATMENT_CLASS = B.TREATMENT_CLASS
),
t5 as (
    select
        *
    from
        t5a
    union
    all
    select
        *
    from
        t5b
),
T6 AS (
    SELECT
        A.*,
        B.PATIENT_COUNT AS SCZ_PATIENTS,
        C.OVERALL_PATIENTS
    FROM
        T5 A FULL
        JOIN KARUNA_ANALYTICS.PUBLIC.STATE_SCZ_PATIENTS_0705 B ON A.STATE = B.STATE FULL
        JOIN T2C C ON A.STATE = C.STATE
        OR B.STATE = C.STATE
    WHERE
        B.STATE IS NOT NULL
        OR C.STATE IS NOT NULL
),
t7 as (
    SELECT
        * EXCLUDE(
            OVERALL_SCHIZ_TRX,
            SCZ_PATIENTS,
            OVERALL_PATIENTS
        ),
        CASE
            WHEN OVERALL_SCHIZ_TRX IS NULL THEN 0
            ELSE OVERALL_SCHIZ_TRX
        END AS OVERALL_SCHIZ_TRX2,
        OVERALL_PATIENTS,
        SCZ_PATIENTS
    FROM
        T6
    WHERE
        PRODUCT_GROUP != 'PROCHLORPERAZINE'
    ORDER BY
        11 DESC,
        1
),
T8 AS (
    SELECT
        *,
        CASE
            WHEN STATE_FULL_NAME_UPDATED IS NULL THEN 'OTHERS'
            ELSE STATE_FULL_NAME_UPDATED
        END AS STATE_FULL_NAME_UPDATED2
    FROM
        T7
)
SELECT
    MONTH_ID,
    MONTH_NUM,
    STATE,
    STATE_FULL_NAME_UPDATED2,
    TIERS,
    PRODUCT_GROUP,
    BRAND_GENERIC,
    DOSAGE_TYPE,
    CLASS,
    TREATMENT_CLASS,
    MOP_CLASSIFICATION,
    LIVES,
    OVERALL_TRX,
    OVERALL_SCHIZ_TRX2,
    OVERALL_PATIENTS,
    SCZ_PATIENTS
FROM
    T8;