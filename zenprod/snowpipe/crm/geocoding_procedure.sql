-------------------------------------------------------------------
----------------- geocoding procedure
-------------------------------------------------------------------

-- Create procedure that will geocode using external func new addresses in zenprod.presence.portal_businessprofile monthly and update geocode table
create or replace procedure ZENPROD.CRM.GEOCODE_PROCEDURE()
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
insert into zenalytics.business_profiles.d_business_geocode (business_id,latitude,longitude,accuracy_score,accuracy_type,number,street,city,state,county,zip,country,source,timezone_name,timezone_abbreviation,timezone_utc_offset,timezone_observes_daylight_savings,asof_date)
  select business_id,
       geocoded_info:results[0]:location:lat::number as Latitude,
       geocoded_info:results[0]:location:lng::number as Longitude,
       geocoded_info:results[0]:accuracy::number as accuracy_score,
       geocoded_info:results[0]:accuracy_type::string as accuracy_type,
       geocoded_info:results[0]:address_components:number::string as number,
       geocoded_info:results[0]:address_components:street::string as street,
       geocoded_info:results[0]:address_components:city::string as city,
       geocoded_info:results[0]:address_components:state::string as state,
       geocoded_info:results[0]:address_components:county::string as county,
       geocoded_info:results[0]:address_components:zip::string as zip,
       geocoded_info:results[0]:address_components:country::string as country,
       geocoded_info:results[0]:source::string as Source,
       geocoded_info:results[0]:fields:timezone:name::string as timezone_name,
       geocoded_info:results[0]:fields:timezone:abbreviation::string as timezone_abbreviation,
       geocoded_info:results[0]:fields:timezone:utc_offset::string as timezone_utc_offset,
       geocoded_info:results[0]:fields:timezone:observes_dst::boolean as observes_daylight_savings,
       current_date() as asof_date
       from(
        select zensand.crm.geocode_addresses(address) as geocoded_info,business_id from (
          select  business_id, 
          address:street::string as street, 
          address:city::string as city, 
          address:state::string as state, 
          case address:country::string 
              when  'USA' then 'United States' 
              when  'US' then 'United States' 
              when  'U.S.' then 'United States' 
              when 'United States of America' then 'United States' 
              when 'CA' then 'Canada'
          else address:country::string end as country, 
          address:zipcode::string as zipcode,
          street||','||city||' '||state||' '||zipcode||','||country as address
        from zenalytics.crm.businessprofile_hierarchy
        where business_id not in (select business_id from zenalytics.business_profiles.d_business_geocode)
        and address:street::string is not null
        and (address:country::string in ('US','USA','CA','Canada')
        or lower(address:country::string) like '%united%state%'))) `
     }).execute();
$$;


-- Create task to call the procedure (once a month, at the start of the month)
create task ZENPROD.CRM.GEOCODOING_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 0 1 * * UTC'
as 
    CALL ZENPROD.CRM.GEOCODE_PROCEDURE();

alter task ZENPROD.CRM.GEOCODOING_TASK resume;
