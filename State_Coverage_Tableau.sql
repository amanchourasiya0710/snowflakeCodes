-- CREATE
-- OR REPLACE TABLE KARUNA_ANALYTICS.REPORTING.LIVES_STATE_AND_PAYER_LEVEL AS with t0 AS (
--     SELECT
--         a.*,
--         b."State" AS state,
--         c."State.Full.Name" as State_full_name,
--         c."Tiers" as Tiers
--     FROM
--         KARUNA.BAP_FORMULARY.LIVES a
--         left JOIN KARUNA.ANCILLARY_DATA."zip3_to_state" b ON LEFT(a.zip, 3) = LPAD(B.ZIP, 3, '0')
--         LEFT JOIN KARUNA.ANCILLARY_DATA."States_full_name" C ON b."State" = c."State"
--     WHERE
--         DATADATE = '2023-06-01'
-- ),
-- t1 AS (
--     SELECT
--         a.*,
--         b.segment_group,
--         b.BENEFITTYPE_ID
--     FROM
--         t0 a
--         LEFT JOIN KARUNA.BAP_FORMULARY.D_SEGMENT b ON a.SEGMENT_ID = b.segment_id
--     WHERE
--         FORMULARY_ID IS NOT NULL
-- ),
-- T2 AS (
--     SELECT
--         STATE,
--         STATE_FULL_NAME,
--         TIERS,
--         payer_id,
--         SUM (LIVES) AS LIVES
--     FROM
--         T1
--     GROUP BY
--         1,
--         2,
--         3,
--         4
-- ),
-- T3 AS (
--     SELECT
--         *,
--         CASE
--             WHEN STATE_FULL_NAME IS NULL THEN 'OTHERS'
--             ELSE STATE_FULL_NAME
--         END AS STATE_FULL_NAME_UPDATED
--     FROM
--         T2
-- )
-- SELECT
--     DISTINCT STATE,
--     STATE_FULL_NAME_UPDATED,
--     TIERS,
--     payer_id,
--     LIVES
-- FROM
--     T3;

CREATE
OR REPLACE TABLE KARUNA_ANALYTICS.REPORTING.LIVES_COVERAGE_AND_PAYER_LEVEL AS with t0 AS (
    SELECT
        a.payer_id,
        a.formulary_id,
        a.segment_id,
        a.drug_id,
        a.metric_value,
        b.lives
    FROM
        KARUNA.BAP_FORMULARY.COVERAGE_METRICS a
        left join KARUNA.BAP_FORMULARY.HIERARCHY b on a.payer_id = b.payer_id
        and a.formulary_id = b.formulary_id
        and a.segment_id = b.segment_id
    WHERE
        metric_name = 'Karuna_Status'
),
t1 AS (
    SELECT
        payer_id,
        segment_id,
        metric_value,
        lives
    FROM
        (
            SELECT
                payer_id,
                segment_id,
                metric_value,
                lives,
                ROW_NUMBER() OVER (
                    PARTITION BY payer_id,
                    segment_id
                    ORDER BY
                        lives DESC
                ) AS rn
            FROM
                t0
        ) ranked
    WHERE
        rn = 1
)
SELECT
    *
FROM
    t1;

create
or replace table KARUNA_ANALYTICS.REPORTING.REPORT_MEDICAID_TABLEAU as with claims_rx as (
    select
        *,
        date_trunc('month', svc_dt) as month
    from
        KARUNA_ANALYTICS.ADM.APLD_CLAIMS_RX
),
unique_months as (
    select
        month,
        row_number() over(
            order by
                month desc
        ) as month_order
    from
        (
            select
                distinct month
            from
                claims_rx
        )
),
claims_rx_month as (
    select
        a.*,
        b.month_order,
        c.patient_state,
        D.ST_CD AS PROVIDER_STATE
    from
        claims_rx a
        left join unique_months b on a.month = b.month
        LEFT JOIN KARUNA_ANALYTICS.ADM.APLD_PATIENT C ON A.PATIENT_ID = C.PATIENT_ID
        LEFT JOIN KARUNA.IQVIA_APLD.D_PROVIDER D ON A.PROVIDER_ID = D.PROVIDER_ID
    where
        month_order <= 24
),
payer_claims as (
    select
        a.*,
        substring(b.payerplan_id, 1, 5) as payer_id,
        case
            when c.model in ('HMO', 'GROUP') then 'Commercial'
            when c.model = 'DE MMP' then 'Medicare D'
            when c.model in ('UNSPEC', 'UNKNOWN', 'PBM', 'PBM BOB') then 'Unspecified'
            when c.model in ('FED ASST', 'FED EMP', 'STATE ASST', 'STATE EMP') then 'Gov Funded'
            when c.iqvia_mop_classification = 'COMMERCIAL' then 'Commercial'
            when c.iqvia_mop_classification = 'MEDICARE D' then 'Medicare D'
            when c.iqvia_mop_classification = 'MEDICAID / FEE FOR SERVICE' then 'Medicaid FFS'
            when c.iqvia_mop_classification = 'MEDICAID / MANAGED' then 'Managed Medicaid'
            when c.iqvia_mop_classification = 'CASH' then 'Cash'
            when c.iqvia_mop_classification is null then 'Missing'
            else 'QC Data'
        end as channel_type,
        c.payer_name,
        c.plan_name,
        c.operating_state,
        case
            when channel_type = 'Medicaid FFS' then c.plan_name
            else c.payer_name
        end as new_payer_name
    from
        claims_rx_month a
        left join KARUNA.IQVIA_APLD.D_PLAN b on a.plan_id = b.plan_id
        left join KARUNA.IQVIA_XPONENT.D_PAYERPLAN c on b.payerplan_id = c.payerplan_id
        left join KARUNA.IQVIA_XPONENT.D_PBMPLAN d on b.payerplan_id = d.payerplan_id
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
        payer_claims
),
t2a_tiers as (
    SELECT
        A.*,
        B."Tiers" as risk_of_state,
        B."Region",
        B."State.Full.Name" as state_full_name
    FROM
        T2A A
        LEFT JOIN "KARUNA"."ANCILLARY_DATA"."States_full_name" B ON A."STATE" = B."State"
),
aps_sum as (
    select
        month,
        month_order,
        channel_type,
        new_payer_name,
        payer_id,
        'All APS' as product,
        state,
        risk_of_state,
        "Region",
        state_full_name,
        sum(schiz_factored_trx_cnt) as schz_trx,
        sum(ovr_factored_trx_cnt) as ovr_trx
    from
        t2a_tiers
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
aps_sum_final as (
    select
        a.*,
        lives
    from
        aps_sum a
        left join KARUNA_ANALYTICS.REPORTING.LIVES_COVERAGE_AND_PAYER_LEVEL B ON a.payer_id = b.payer_id
),
branded_oral_sum as (
    select
        month,
        month_order,
        channel_type,
        new_payer_name,
        payer_id,
        'Branded Oral' as product,
        state,
        risk_of_state,
        "Region",
        state_full_name,
        sum(schiz_factored_trx_cnt) as schz_trx,
        sum(ovr_factored_trx_cnt) as ovr_trx
    from
        t2a_tiers
    where
        product_group in (
            'CAPLYTA',
            'FANAPT',
            'VRAYLAR',
            'REXULTI',
            'LYBALVI'
        )
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
branded_oral_sum_final as (
    select
        a.*,
        lives
    from
        branded_oral_sum a
        left join KARUNA_ANALYTICS.REPORTING.LIVES_COVERAGE_AND_PAYER_LEVEL B ON a.payer_id = b.payer_id
),
product_sum as (
    select
        month,
        month_order,
        channel_type,
        new_payer_name,
        payer_id,
        product_group as product,
        state,
        risk_of_state,
        "Region",
        state_full_name,
        sum(schiz_factored_trx_cnt) as schz_trx,
        sum(ovr_factored_trx_cnt) as ovr_trx
    from
        t2a_tiers
    where
        product_group in (
            'CAPLYTA',
            'FANAPT',
            'VRAYLAR',
            'REXULTI',
            'LYBALVI'
        )
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
product_sum_final as (
    select
        a.*,
        lives
    from
        product_sum a
        left join KARUNA_ANALYTICS.REPORTING.LIVES_COVERAGE_AND_PAYER_LEVEL B ON a.payer_id = b.payer_id
),
union_all as (
    select
        *
    from
        aps_sum_final
    union
    all
    select
        *
    from
        branded_oral_sum_final
    union
    all
    select
        *
    from
        product_sum_final
)
select
    month,
    month_order,
    channel_type,
    new_payer_name,
    product,
    risk_of_state,
    "Region",
    state_full_name as state,
    schz_trx,
    ovr_trx,
    lives
from
    union_all
where
    channel_type <> 'Missing'
    and channel_type <> 'QC Data';

SELECT
    distinct channel_type
FROM
    KARUNA_ANALYTICS.REPORTING.REPORT_MEDICAID_TABLEAU;

-- SELECT * FROM KARUNA_ANALYTICS.REPORTING.REPORT_MEDICAID_TABLEAU WHERE  channel_type = 'Managed Medicaid'
-- AND State = 'ARKANSAS' AND risk_of_state = 'Tier 2' AND "Region" = 'Southwest' AND new_payer_name = 'CARESOURCE' AND month_order = 12;
SELECT
    sum(schz_trx)
FROM
    KARUNA_ANALYTICS.REPORTING.state_coverage_tableau_nandini
WHERE
    channel_type ilike '%Medicaid/Managed%'
    AND state = 'ARIZONA'
    AND risk_of_state = 'Tier 2'
    AND "Region" = 'Southwest'
    AND payer_name ilike '%CARESOURCE %'
    and month_order <= 12
    and product = 'Branded Oral';

select
    distinct new_payer_name
from
    KARUNA_ANALYTICS.REPORTING.REPORT_MEDICAID_TABLEAU
where
    new_payer_name ilike '%MERCY CARE ADVANTAGE%';

select
    distinct state,
    channel_type,
    risk_of_state,
    "Region",
    new_payer_name,
    month_order
from
    KARUNA_ANALYTICS.REPORTING.REPORT_MEDICAID_TABLEAU
where
    state ilike '%ALASKA%'
    and channel_type ilike '%Medicaid%'
    and month_order <= 12
    and product = 'All APS'
    and risk_of_state = 'Tier 3'
    and "Region" = 'Northwest'
    and "Region" = 'Northwest';

select
    count(distinct payer_id)
from
    KARUNA.IQVIA_XPONENT.D_PAYERPLAN;

with t0 as (
    select
        substring(b.payerplan_id, 1, 5) as payer_id
    from
        KARUNA_ANALYTICS.REPORTING.PAYER_CLAIMS_TABLEAU b
),
t1 as (
    select
        t0.payer_id as payer_id1,
        b.payer_id as payer_id2
    from
        t0
        left join KARUNA_ANALYTICS.REPORTING.LIVES_COVERAGE_AND_PAYER_LEVEL b on b.payer_id = t0.payer_id
),
t2 as (
    select
        distinct payer_id1,
        payer_id2
    from
        t1
    where
        payer_id2 is null
)
select
    *
from
    t2;

select
    count(distinct new_payer_name)
from
    KARUNA_ANALYTICS.REPORTING.PAYER_CLAIMS_TABLEAU
where
    channel_type = 'Commercial';