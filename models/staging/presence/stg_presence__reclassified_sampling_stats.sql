WITH CTE AS (
    select    business_id
        , date_trunc(hour, start_time) as report_datetime
        , SUM(iff(classification = 'WALKIN' AND known_to_zenreach AND NOT(known_to_merchant_account) AND NOT(known_to_merchant_location), 1, 0)) as walkin_network
        , SUM(iff(classification = 'WALKIN' AND (known_to_merchant_account OR known_to_merchant_location), 1, 0)) as walkin_merchant
        , SUM(iff(classification = 'WALKIN' AND not(known_to_zenreach), 1, 0)) as walkin_unidentified
        , SUM(iff(classification = 'WALKBY' AND known_to_zenreach AND NOT(known_to_merchant_account) AND NOT(known_to_merchant_location), 1, 0)) as walkby_network
        , SUM(iff(classification = 'WALKBY' AND (known_to_merchant_account OR known_to_merchant_location), 1, 0)) as walkby_merchant
        , SUM(iff(classification = 'WALKBY' AND NOT(known_to_zenreach), 1, 0)) as walkby_unidentified
        , SUM(iff(classification = 'NOTHUMAN', 1, 0)) as not_human -- New Classification since presence_sampling_stats was created
        , MIN(start_time) as created
        , MAX(end_time) as updated
        , current_date as asof_date
    from {{ ref('stg_presence__reclassified_finished_sightings') }}
    where date_trunc(day, start_time) NOT IN ('2019-09-25','2019-09-26')
    group by 1,2
    UNION ALL --- smoothed data for '2019-09-25','2019-09-26'
    select    business_id
            , report_datetime
            , walkin_network
            , walkin_merchant
            , walkin_unidentified
            , walkby_network
            , walkby_merchant
            , walkby_unidentified
            , not_human -- New Classification since presence_sampling_stats was created
            , created
            , updated
            , asof_date
    from    {{ source('PRESENCE', 'RECLASSIFIED_PRESENCE_SAMPLING_STATS_CORRECTED') }}
    where date_trunc(day, report_datetime) IN ('2019-09-25','2019-09-26')
)
select    business_id
        , report_datetime
        , walkin_network
        , walkin_merchant
        , walkin_unidentified
        , walkby_network
        , walkby_merchant
        , walkby_unidentified
        , not_human -- New Classification since presence_sampling_stats was created
        , created
        , updated
        , asof_date
from CTE
order by 1,2
