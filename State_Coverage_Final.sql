set
    lives_data_date = '2023-05-01';

set
    hierarchy_data_date = '2023-07-14';

create
or replace table KARUNA_ANALYTICS.REPORTING.LIVES_COVERAGE_AND_PAYER_LEVEL as with bap_nomenclature as (
    select
        *,
        metric_value as nomenclature
    from
        "KARUNA"."BAP_FORMULARY"."COVERAGE_METRICS"
    where
        datadate = $hierarchy_data_date
        and metric_name = 'Karuna_Status'
),
coverage as (
    select
        a.formulary_id,
        a.payer_id,
        a.segment_id,
        a.drug_id,
        UPPER(b.drug_name) as drug_name,
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
        datadate = $hierarchy_data_date
),
lives_sum as (
    select
        'Lives' as data_source,
        payer_id,
        segment_id,
        nomenclature as status,
        upper(drug_name) as product,
        segment_group_update as channel_type,
        sum(lives) as lives
    from
        lives
    group by
        1,
        2,
        3,
        4,
        5,
        6
)
select
    *
from
    lives_sum;

create
or replace table KARUNA_ANALYTICS.REPORTING.LIVES_COVERAGE_AND_PAYER_LEVEL_WITH_STATES as with bap_nomenclature as (
    select
        *,
        metric_value as nomenclature
    from
        "KARUNA"."BAP_FORMULARY"."COVERAGE_METRICS"
    where
        datadate = $hierarchy_data_date
        and metric_name = 'Karuna_Status'
),
coverage as (
    select
        a.formulary_id,
        a.payer_id,
        a.segment_id,
        a.drug_id,
        UPPER(b.drug_name) as drug_name,
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
        e."Tiers" as risk_of_state,
        e."Region" as region,
        e."State.Full.Name" as state,
        e."Medicaid.State.Wide.Formulary" as medicaid_state_wide_formulary,
        case
            when c.segment_name = 'FFS'
            and c.segment_group = 'Medicaid' then 'Medicaid/Fee For Service'
            when c.segment_name = 'Managed'
            and c.segment_group = 'Medicaid' then 'Medicaid/Managed'
            when c.segment_group = 'Medicare' then 'Medicare D'
            else segment_group
        end as segment_group_update
    from
        KARUNA.BAP_FORMULARY.LIVES a
        inner join coverage b on a.formulary_id = b.formulary_id
        and a.payer_id = b.payer_id
        and a.segment_id = b.segment_id
        left join "KARUNA"."BAP_FORMULARY"."SEGMENT" c on a.segment_id = c.segment_id
        left JOIN KARUNA.ANCILLARY_DATA."zip3_to_state" d ON LEFT(a.zip, 3) = LPAD(d.ZIP, 3, '0')
        LEFT JOIN KARUNA.ANCILLARY_DATA."States_full_name" e ON d."State" = e."State"
    where
        datadate = $lives_data_date
),
lives_sum as (
    select
        'Lives' as data_source,
        risk_of_state,
        region,
        state,
        medicaid_state_wide_formulary,
        payer_id,
        -- segment_id,
        nomenclature as status,
        upper(drug_name) as product,
        segment_group_update as channel_type,
        sum(lives) as lives
    from
        lives
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
),
lives_sum_payer_name as (
    select 
    data_source,
    b.payer_name,
    risk_of_state,
    region,
    state,
    medicaid_state_wide_formulary,
    status,
    product,
    channel_type,
    lives
    from lives_sum a left join KARUNA.BAP_FORMULARY.PAYER b on a.payer_id = b.payer_id
)
select
    *
from
    lives_sum_payer_name;

create
or replace table KARUNA_ANALYTICS.REPORTING.state_coverage_tableau as with month_order as (
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
claims_rx_without_states as (
    select
        a.*,
        b.month_order,
        c.payerplan_id,
        d.patient_state,
        e.ST_CD AS PROVIDER_STATE,
        f.operating_state,
        f.payer_name
    from
        "KARUNA_ANALYTICS"."ADM"."APLD_CLAIMS_RX" a
        left join month_order b on a.month_id = b.month_id
        left join "KARUNA"."IQVIA_APLD"."D_PLAN" c on a.plan_id = c.plan_id
        LEFT JOIN KARUNA_ANALYTICS.ADM.APLD_PATIENT D ON A.PATIENT_ID = d.PATIENT_ID
        LEFT JOIN KARUNA.IQVIA_APLD.D_PROVIDER e ON A.PROVIDER_ID = e.PROVIDER_ID
        left join KARUNA.IQVIA_XPONENT.D_PAYERPLAN f on c.payerplan_id = f.payerplan_id
    where
        month_order <= 12
),
claims_rx as (
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
        END AS state_short_form
    from
        claims_rx_without_states
),
claims_rx2 as (
    select
        a.*,
        b.formulary_id,
        b.payer_id as payer_id,
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
                datadate = $hierarchy_data_date
        ) b on a.payerplan_id = b.payerplan_id
),
claims_rx2_0 as (
    select
        distinct payerplan_id,
        formulary_id,
        payer_id,
        payer_name,
        segment_id,
        UPPER(mkted_prod_nm) as product_group,
        date_trunc('month', svc_dt) as month,
        month_order,
        state_short_form,
        schiz_factored_trx_cnt * allocation as schiz_factored_trx_cnt,
        ovr_factored_trx_cnt * allocation as ovr_factored_trx_cnt
    from
        claims_rx2
),
claims_channel_type as (
    select
        a.*,
        case
            when c.segment_name = 'FFS'
            and c.segment_group = 'Medicaid' then 'Medicaid/Fee For Service'
            when c.segment_name = 'Managed'
            and c.segment_group = 'Medicaid' then 'Medicaid/Managed'
            when c.segment_group = 'Medicare' then 'Medicare D'
            else segment_group
        end as channel_type
    from
        claims_rx2_0 a
        left join "KARUNA"."BAP_FORMULARY"."SEGMENT" c on a.segment_id = c.segment_id
),
aps_sum as (
    select
        month,
        month_order,
        channel_type,
        payer_name,
        payer_id,
        state_short_form,
        segment_id,
        sum(schiz_factored_trx_cnt) as all_aps_schz_trx,
        sum(ovr_factored_trx_cnt) as all_aps_ovr_trx
    from
        claims_channel_type
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7
),
branded_oral_sum as (
    select
        month,
        month_order,
        channel_type,
        payer_name,
        payer_id,
        state_short_form,
        segment_id,
        sum(schiz_factored_trx_cnt) as branded_oral_schz_trx,
        sum(ovr_factored_trx_cnt) as branded_oral_ovr_trx
    from
        claims_channel_type
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
        7
),
product_sum as (
    select
        month,
        month_order,
        channel_type,
        payer_name,
        payer_id,
        product_group as product,
        state_short_form,
        segment_id,
        sum(schiz_factored_trx_cnt) as product_schz_trx,
        sum(ovr_factored_trx_cnt) as product_ovr_trx
    from
        claims_channel_type
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
        8
),
aps_sum_join_branded_oral as (
    select a.*, b.branded_oral_schz_trx, b.branded_oral_ovr_trx
    from aps_sum a left join branded_oral_sum b 
    on a.month = b.month 
    and a.month_order = b.month_order 
    and a.channel_type = b.channel_type
    and a.payer_name = b.payer_name
    and a.payer_id = b.payer_id
    and a.state_short_form = b.state_short_form
    and a.segment_id = b.segment_id
),
aps_sum_join_branded_oral_product as (
    select a.*, b.product, b.product_schz_trx, b.product_ovr_trx
    from aps_sum_join_branded_oral a left join product_sum b
    on a.month = b.month 
    and a.month_order = b.month_order 
    and a.channel_type = b.channel_type
    and a.payer_name = b.payer_name
    and a.payer_id = b.payer_id
    and a.state_short_form = b.state_short_form
    and a.segment_id = b.segment_id
),
aps_sum_join_branded_oral_product_with_state as (
    select a.*,
    B."Tiers" as risk_of_state,
    B."Region" as region,
    B."State.Full.Name" as state,
    B."Medicaid.State.Wide.Formulary" as medicaid_state_wide_formulary
    from aps_sum_join_branded_oral_product a
    LEFT JOIN "KARUNA"."ANCILLARY_DATA"."States_full_name" B ON A.state_short_form = B."State"
),
union_all as (
    select
        payer_name,
        month,
        month_order,
        channel_type,
        product,
        risk_of_state,
        region,
        state,
        medicaid_state_wide_formulary,
        all_aps_schz_trx,
        all_aps_ovr_trx,
        branded_oral_schz_trx,
        branded_oral_ovr_trx,
        product_schz_trx,
        product_ovr_trx,
        null as lives,
        null as status,
        'TRx Data' as data_source
    from
        aps_sum_join_branded_oral_product_with_state
    union all
    select
        payer_name,
        null as month,
        null as month_order,
        channel_type,
        product,
        risk_of_state,
        region,
        state,
        medicaid_state_wide_formulary,
        null as all_aps_schz_trx,
        null as all_aps_ovr_trx,
        null as branded_oral_schz_trx,
        null as branded_oral_ovr_trx,
        null as product_schz_trx,
        null as product_ovr_trx,
        lives,
        status,
        data_source
    from
        KARUNA_ANALYTICS.REPORTING.LIVES_COVERAGE_AND_PAYER_LEVEL_WITH_STATES
)
select
    *
from
    union_all;