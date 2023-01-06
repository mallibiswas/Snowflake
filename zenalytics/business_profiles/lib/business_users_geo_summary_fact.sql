--------------------------------------------------------------------------------------
-- ZR user netowrk by geo cuts
--------------------------------------------------------------------------------------

create or replace table zenalytics.business_profiles.business_users_geo_summary_fact
as
select g.country, 
      g.state,
      COALESCE(metro_micro_statistical_area_name,combined_statistical_area_name,economic_region,cma_ca_name) as MSA, 
      g.city, 
      count(distinct udf.email) as users,
      current_date() as asof_date
from zenalytics.business_profiles.business_users_details_fact udf
inner join zenalytics.business_profiles.d_business_geocode g on udf.business_id = g.business_id
left join zenalytics.business_profiles.d_business_acs_census a on g.business_id = a.business_id
left join zenalytics.business_profiles.d_business_statcan_census c on g.business_id = c.business_id
where g.country in ('United States','Canada')
group by 1,2,3,4;
