select
    count(*)
from
    KARUNA_ANALYTICS.PUBLIC.MARKET_ACCESS_PAYER_STATE
limit
    100;

/ / 648, 880
select
    *
from
    KARUNA_ANALYTICS.PUBLIC.MARKET_ACCESS_PAYER_STATE
limit
    100;

select
    distinct month_id
from
    KARUNA_ANALYTICS.PUBLIC.MARKET_ACCESS_PAYER_STATE
limit
    100;

select
    distinct type,
    product_group
from
    KARUNA_ANALYTICS.PUBLIC.MARKET_ACCESS_PAYER_STATE
limit
    100;

select
    *
from
    KARUNA_ANALYTICS.ADM.APLD_CLAIMS_RX
limit
    100;

select
    *
from
    KARUNA.IQVIA_APLD.D_PLAN
where
limit
    100;

select
    count(*)
from
    (
        select
            distinct plan_id,
            payerplan_id
        from
            KARUNA.IQVIA_APLD.D_PLAN
    );

select
    *
from
    KARUNA.IQVIA_XPONENT.D_PAYERPLAN
limit
    100;

select
    distinct iqvia_mop_classification
from
    (
        select
            *
        from
            KARUNA.IQVIA_XPONENT.D_PAYERPLAN
    );

select
    *
from
    KARUNA.IQVIA_XPONENT.D_PBMPLAN
limit
    100;

create
or replace table KARUNA_ANALYTICS.REPORTING.PAYER_CLAIMS_TABLEAU as with claims_rx as (
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
        b.month_order
    from
        claims_rx a
        left join unique_months b on a.month = b.month
    where
        month_order <= 24
),
payer_claims as (
    select
        a.*,
        b.payerplan_id,
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
        d.pbm_name,
        c.payer_name,
        c.plan_name,
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
aps_sum as (
    select
        month,
        month_order,
        channel_type,
        pbm_name,
        new_payer_name,
        'All APS' as product,
        sum(schiz_factored_trx_cnt) as schz_trx,
        sum(ovr_factored_trx_cnt) as ovr_trx
    from
        payer_claims
    group by
        1,
        2,
        3,
        4,
        5,
        6
),
branded_oral_sum as (
    select
        month,
        month_order,
        channel_type,
        pbm_name,
        new_payer_name,
        'Branded Oral' as product,
        sum(schiz_factored_trx_cnt) as schz_trx,
        sum(ovr_factored_trx_cnt) as ovr_trx
    from
        payer_claims
    where
        product_group in ('CAPLYTA', 'FANAPT', 'VRAYLAR', 'REXULTI', 'LYBALVI')
    group by
        1,
        2,
        3,
        4,
        5,
        6
),
product_sum as (
    select
        month,
        month_order,
        channel_type,
        pbm_name,
        new_payer_name,
        product_group as product,
        sum(schiz_factored_trx_cnt) as schz_trx,
        sum(ovr_factored_trx_cnt) as ovr_trx
    from
        payer_claims
    where
        product_group in ('CAPLYTA', 'FANAPT', 'VRAYLAR', 'REXULTI', 'LYBALVI')
    group by
        1,
        2,
        3,
        4,
        5,
        6
),
aps_sum2 as (
    select
        a.*,
        zeroifnull(b.schz_trx) as bo_schz_trx,
        zeroifnull(b.ovr_trx) as bo_ovr_trx,
        zeroifnull(a.schz_trx) as aps_schz_trx,
        zeroifnull(a.ovr_trx) as aps_ovr_trx
    from
        aps_sum a
        left join branded_oral_sum b on a.month = b.month
        and a.channel_type = b.channel_type
        and a.pbm_name = b.pbm_name
        and a.new_payer_name = b.new_payer_name
),
branded_oral_sum2 as (
    select
        a.*,
        zeroifnull(a.schz_trx) as bo_schz_trx,
        zeroifnull(a.ovr_trx) as bo_ovr_trx,
        zeroifnull(b.schz_trx) as aps_schz_trx,
        zeroifnull(b.ovr_trx) as aps_ovr_trx
    from
        branded_oral_sum a
        left join aps_sum b on a.month = b.month
        and a.channel_type = b.channel_type
        and a.pbm_name = b.pbm_name
        and a.new_payer_name = b.new_payer_name
),
product_sum2 as (
    select
        a.*,
        zeroifnull(b.schz_trx) as bo_schz_trx,
        zeroifnull(b.ovr_trx) as bo_ovr_trx,
        zeroifnull(c.schz_trx) as aps_schz_trx,
        zeroifnull(c.ovr_trx) as aps_ovr_trx
    from
        product_sum a
        left join branded_oral_sum b on a.month = b.month
        and a.channel_type = b.channel_type
        and a.pbm_name = b.pbm_name
        and a.new_payer_name = b.new_payer_name
        left join aps_sum c on a.month = c.month
        and a.channel_type = c.channel_type
        and a.pbm_name = c.pbm_name
        and a.new_payer_name = c.new_payer_name
),
union_all as (
    select
        *
    from
        aps_sum2
    union
    all
    select
        *
    from
        branded_oral_sum2
    union
    all
    select
        *
    from
        product_sum2
),
all_combinations AS (
    SELECT
        t5.month as month,
        t5.month_order as month_order,
        t4.Channel_type as Channel_type,
        t3.Pbm_name as Pbm_name,
        t2.New_payer_name as New_payer_name,
        t1.Product as Product,
        0 as schz_trx,
        0 as ovr_trx,
        0 as bo_schz_trx,
        0 as bo_ovr_trx,
        0 as aps_schz_trx,
        0 as aps_ovr_trx
    FROM
        (
            SELECT
                DISTINCT Product
            FROM
                union_all
        ) AS t1
        CROSS JOIN (
            SELECT
                DISTINCT New_payer_name
            FROM
                union_all
        ) AS t2
        CROSS JOIN (
            SELECT
                DISTINCT Pbm_name
            FROM
                union_all
        ) AS t3
        CROSS JOIN (
            SELECT
                DISTINCT Channel_type
            FROM
                union_all
        ) AS t4
        CROSS JOIN (
            SELECT
                DISTINCT month,
                month_order
            FROM
                union_all
        ) AS t5
),
missing_combinations AS (
    select
        *
    from
        all_combinations
    union
    select
        *
    from
        union_all
),
group_common as (
    select
        Pbm_name,
        Channel_type,
        New_payer_name,
        Product,
        month,
        month_order,
        sum(schz_trx),
        sum(ovr_trx),
        sum(bo_schz_trx),
        sum(bo_ovr_trx),
        sum(aps_ovr_trx),
        sum(aps_schz_trx) AS Value
    from
        missing_combinations
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
    group_common
where
    channel_type <> 'Missing'
    and channel_type <> 'QC Data';

select
    count(*)
from
    KARUNA_ANALYTICS.REPORTING.PAYER_CLAIMS_TABLEAU;