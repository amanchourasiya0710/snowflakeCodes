-- Create Formulary Data for Tableau
create or replace table "KARUNA_ANALYTICS"."REPORTING"."FORMULARY_COVERAGE_TABLEAU" as
with bap_nomenclature as (
    select
        *,
        metric_value as nomenclature
    from
        "KARUNA"."BAP_FORMULARY"."COVERAGE_METRICS"
    where
        datadate = $ hierarchy_data_date
        and metric_name = 'Karuna_Status'
),
coverage as (
    select
        a.formulary_id,
        a.payer_id,
        a.segment_id,
        a.drug_id,
        b.drug_name,
        a.nomenclature
    from
        bap_nomenclature a
        inner join "KARUNA"."BAP_FORMULARY"."DRUG" b on a.drug_id = b.drug_id
    where
        UPPER(drug_name) in (
            'CAPLYTA',
            'VRAYLAR',
            'REXULTI',
            'LYBALVI',
            'INVEGA SUSTENNA'
        )
),
lives as (
    select
        a.*,
        b.drug_id,
        b.drug_name,
        b.nomenclature,
        c.segment_name,
        c.segment_group,
        case
            when c.segment_name = 'FFS'
            and c.segment_group = 'Medicaid' then 'Medicaid/Fee For Service'
            when c.segment_name = 'Managed'
            and c.segment_group = 'Medicaid' then 'Medicaid/Managed'
            when c.segment_group = 'Medicare' then 'Medicare D'
            else segment_group
        end as segment_group_update
    from
        "KARUNA"."BAP_FORMULARY"."HIERARCHY" a
        inner join coverage b on a.formulary_id = b.formulary_id
        and a.payer_id = b.payer_id
        and a.segment_id = b.segment_id
        left join "KARUNA"."BAP_FORMULARY"."SEGMENT" c on a.segment_id = c.segment_id
    where
        datadate = $ hierarchy_data_date
)
-- select
--     *
-- from
--     lives
-- where
--     payer_id = '00030'
--     and segment_id = '7'
--     and drug_id = '1335'
-- limit
--     100;
, lives_sum as (
    select
        'Lives' as metric_type,
        'Overall' as indication,
        segment_group as segment_group_update,
        nomenclature as status,
        null as month,
        null as month_order,
        drug_name as product_group,
        sum(lives) as value
    from
        lives
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7
),
month_order as (
    select
        month_id,
        row_number() over(
            order by
                month_id desc
        ) as month_order
    from
        (
            select
                distinct month_id
            from
                "KARUNA_ANALYTICS"."ADM"."APLD_CLAIMS_RX"
        )
),
claims_rx as (
    select
        a.*,
        b.month_order,
        c.payerplan_id
    from
        "KARUNA_ANALYTICS"."ADM"."APLD_CLAIMS_RX" a
        left join month_order b on a.month_id = b.month_id
        left join "KARUNA"."IQVIA_APLD"."D_PLAN" c on a.plan_id = c.plan_id
    where
        month_order <= 12
),
claims_rx2 as (
    select
        a.*,
        b.formulary_id,
        b.payer_id,
        b.segment_id,
        b.allocation
    from
        claims_rx a
        inner join (
            select
                *
            from
                "KARUNA"."BAP_FORMULARY"."BRIDGE"
            where
                datadate = $ hierarchy_data_date
        ) b on a.payerplan_id = b.payerplan_id
),
claims_rx2_0 as (
    select
        distinct payerplan_id,
        formulary_id,
        payer_id,
        segment_id,
        initcap(mkted_prod_nm) as mkted_prod_nm,
        date_trunc('month', svc_dt) as month,
        month_order,
        svc_dt,
        schiz_factored_trx_cnt * allocation as schiz_factored_trx_cnt,
        ovr_factored_trx_cnt * allocation as ovr_factored_trx_cnt
    from
        claims_rx2
    where
        upper(mkted_prod_nm) in (
            'CAPLYTA',
            'VRAYLAR',
            'REXULTI',
            'LYBALVI',
            'INVEGA SUSTENNA'
        )
),
claims_rx3 as (
    select
        payerplan_id,
        formulary_id,
        payer_id,
        segment_id,
        initcap(mkted_prod_nm) as product,
        date_trunc('month', svc_dt) as month,
        month_order,
        sum(schiz_factored_trx_cnt) as schz_trx,
        sum(ovr_factored_trx_cnt) as ovr_trx
    from
        claims_rx2_0
    where
        upper(mkted_prod_nm) in (
            'CAPLYTA',
            'VRAYLAR',
            'REXULTI',
            'LYBALVI',
            'INVEGA SUSTENNA'
        )
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7
),
claims_rx4 as (
    select
        a.*,
        b.drug_name,
        b.nomenclature,
        c.segment_name,
        c.segment_group,
        case
            when c.segment_name = 'FFS'
            and c.segment_group = 'Medicaid' then 'Medicaid/Fee For Service'
            when c.segment_name = 'Managed'
            and c.segment_group = 'Medicaid' then 'Medicaid/Managed'
            when c.segment_group = 'Medicare' then 'Medicare D'
            else segment_group
        end as segment_group_update
    from
        claims_rx3 a
        inner join coverage b on a.formulary_id = b.formulary_id
        and a.payer_id = b.payer_id
        and a.segment_id = b.segment_id
        and a.product = b.drug_name
        left join "KARUNA"."BAP_FORMULARY"."SEGMENT" c on a.segment_id = c.segment_id
),
claims_ovr as (
    select
        'TRx' as metric,
        'Overall' as indication,
        segment_group as segment_group_update,
        nomenclature as status,
        month,
        month_order,
        drug_name as product_group,
        sum(ovr_trx) as value
    from
        claims_rx4
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7
),
claims_schz as (
    select
        'TRx' as metric,
        'Schizophrenia' as indication,
        segment_group as segment_group_update,
        nomenclature as status,
        month,
        month_order,
        drug_name as product_group,
        sum(schz_trx) as value
    from
        claims_rx4
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7
),
final as (
    select
        *
    from
        claims_ovr
    union
    all
    select
        *
    from
        claims_schz
    union
    all
    select
        *
    from
        lives_sum
),
all_combinations AS (
    SELECT
        t3.metric as metric,
        t5.indication as indication,
        t4.segment_group_update as segment_group_update,
        t2.status as status,
        t6.month as month,
        t6.month_order as month_order,
        t1.product_group as product_group,
        0 as value
    FROM
        (
            SELECT
                DISTINCT product_group
            FROM
                final
        ) AS t1
        CROSS JOIN (
            SELECT
                DISTINCT status
            FROM
                final
        ) AS t2
        CROSS JOIN (
            SELECT
                DISTINCT metric
            FROM
                final
        ) AS t3
        CROSS JOIN (
            SELECT
                DISTINCT segment_group_update
            FROM
                final
        ) AS t4
        CROSS JOIN (
            SELECT
                DISTINCT indication
            FROM
                final
        ) AS t5
        CROSS JOIN (
            SELECT
                DISTINCT month,
                month_order
            FROM
                final
        ) AS t6
),
missing_combinations AS (
    select
        *
    from
        final
    union
    select
        *
    from
        all_combinations
),
group_common as (
    select
        metric,
        indication,
        segment_group_update,
        status,
        month,
        month_order,
        product_group,
        sum(value) AS Value
    from
        missing_combinations
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7
)
select
    *
from
    group_common;