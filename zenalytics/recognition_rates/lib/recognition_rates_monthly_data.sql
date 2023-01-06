-----------------------------------------------------------------------
--------------------- recognition_rates_by_geo ------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

create or replace table &{dbname}.&{schemaname}.RECOGNITION_RATES_BY_GEO
as
WITH business_features as (
          select bp.l1_name as parent, bp.business_name, address,
          trim(initcap(address:city::string)) as city, 
          trim(upper(address:state::string)) as state, 
          upper(address:country::string) as country, 
          business_id
          from crm.businessprofile_hierarchy bp  
          where upper(trim(address:country::string)) in ('US','UNITED STATES','USA','CA','CAN','CANADA')
  ),
  customer as (
                select business_id, 
               count(userprofile_id) as total_collected,
               count(importer_id) as total_imported
               from crm.portal_businessrelationship
              where nvl(is_employee,False) = False
              and contact_allowed = True
              group by business_id
  ),
  sampling as (select to_date(date_trunc('MONTH', date_hour)) as month, 
            business_id,   
            SUM(walkin_merchant) as walkin_merchant, 
            SUM(walkin_network) as walkin_network, 
            SUM(walkin_unidentified) as walkin_unidentified, 
            SUM(walkby_merchant) as walkby_merchant, 
            SUM(walkby_network) as walkby_network, 
            SUM(walkby_unidentified) as walkby_unidentified
          from ads_campaigns.presence_sampling_stats s
          where date_hour >= '2019-01-01' and date_hour < date_trunc('MONTH', current_date())
          group by 1, 2)
select  trim(city) as city, 
case when country in ('US','UNITED STATES','USA') and state = 'AL' then 'ALABAMA'
      when state = 'AK' then 'ALASKA'
      when state = 'AZ' then 'ARIZONA'
      when state = 'AR' then 'ARKANSAS'
      when state = 'CA' then 'CALIFORNIA'
      when state = 'CO' then 'COLORADO'
      when state = 'CT' then 'CONNECTICUT'
      when state = 'DE' then 'DELAWARE'
      when state = 'FL' then 'FLORIDA'
      when state = 'GA' then 'GEORGIA'
      when state = 'HI' then 'HAWAII'
      when state = 'ID' then 'IDAHO'
      when state = 'IL' then 'ILLINOIS'
      when state = 'IN' then 'INDIANA'
      when state = 'IA' then 'IOWA'
      when state = 'KS' then 'KANSAS'
      when state = 'KY' then 'KENTUCKY'
      when state = 'LA' then 'LOUISIANA'
      when state = 'ME' then 'MAINE'
      when state = 'MD' then 'MARYLAND'
      when state = 'MA' then 'MASSACHUSETTS'
      when state = 'MI' then 'MICHIGAN'
      when state = 'MN' then 'MINNESOTA'
      when state = 'MS' then 'MISSISSIPPI'
      when state = 'MO' then 'MISSOURI'
      when state = 'MT' then 'MONTANA'
      when state = 'NE' then 'NEBRASKA'
      when state = 'NV' then 'NEVADA'
      when state = 'NH' then 'NEW HAMPSHIRE'
      when state = 'NJ' then 'NEW JERSEY'
      when state = 'NM' then 'NEW MEXICO'
      when state = 'NY' then 'NEW YORK'
      when state = 'NC' then 'NORTH CAROLINA'
      when state = 'ND' then 'NORTH DAKOTA'
      when state = 'OH' then 'OHIO'
      when state = 'OK' then 'OKLAHOMA'
      when state = 'OR' then 'OREGON'
      when state = 'PA' then 'PENNSYLVANIA'
      when state = 'RI' then 'RHODE ISLAND'
      when state = 'SC' then 'SOUTH CAROLINA'
      when state = 'SD' then 'SOUTH DAKOTA'
      when state = 'TN' then 'TENNESSEE'
      when state = 'TX' then 'TEXAS'
      when state = 'UT' then 'UTAH'
      when state = 'VT' then 'VERMONT'
      when state = 'VA' then 'VIRGINIA'
      when state = 'WA' then 'WASHINGTON'
      when state = 'WV' then 'WEST VIRGINIA'
      when state = 'WI' then 'WISCONSIN'
      when state = 'WY' then 'WYOMING'
      when country in ('CA','CAN','CANADA') and state in ('AL','AB') then 'ALBERTA'
      when state in ('B.C.','BC') then 'BRITISH COLUMBIA'
      when state = 'MB' then 'MANITOBA'
      when state = 'NB' then 'NEW BRUNSWICK'
      when state = 'NL' then 'NEWFOUNDLAND AND LABRADOR'
      when state = 'NS' then 'NOVA SCOTIA'
      when state = 'NT' then 'NORTHWEST TERRITORIES'
      when state = 'NU' then 'NUNAVUT'
      when state = 'ON' then 'ONTARIO'
      when state = 'PE' then 'PRINCE EDWARD ISLAND'
      when state in ('QB','QC') then 'QUEBEC'
      when state in ('SK','SASKETCHEWAN') then 'SASKATCHEWAN'
      when state = 'YT' then 'YUKON'                
else state end as state, 
case when country in ('CA','CAN','CANADA') then 'CANADA'
     when country in ('US','UNITED STATES','USA') then 'USA'
else 'UNK'
end as country,
month,  
SUM(walkin_merchant) as walkin_merchant, 
SUM(walkin_network) as walkin_network, 
SUM(walkin_unidentified) as walkin_unidentified, 
SUM(walkby_merchant) as walkby_merchant, 
SUM(walkby_network) as walkby_network, 
SUM(walkby_unidentified) as walkby_unidentified,
SUM(walkin_merchant+walkin_network)/SUM(walkin_merchant+walkin_network+walkin_unidentified+0.000001) as sample_rate,
SUM(total_collected) as total_collected,
SUM(total_imported) as total_imported,
min(current_date) as asof_date
from business_features bf
left join sampling s on s.business_id = bf.business_id
left join customer c on c.business_id = bf.business_id
group by 1,2,3,4
order by 4;

create or replace table &{dbname}.&{schemaname}.CUMULATIVE_COLLECTIONS_BY_GEO
as
WITH business_features as (
          select bp.l1_name as parent, bp.business_name, address,
          trim(initcap(address:city::string)) as city, 
          trim(upper(address:state::string)) as state, 
          upper(address:country::string) as country, 
          business_id
          from crm.businessprofile_hierarchy bp  
          where upper(trim(address:country::string)) in ('US','UNITED STATES','USA','CA','CAN','CANADA')
  ),
emails_collected as (select bf.country, bf.city, bf.state, 
                     to_date(date_trunc('MONTH', nvl(created,last_updated))) as month, 
                     count(userprofile_id) as total_collected,
                     count(importer_id) as total_imported
                     from crm.portal_businessrelationship br, business_features bf
        where nvl(is_employee,False) = False
        and contact_allowed = True
        and bf.business_id = br.business_id
        group by bf.country, bf.city, bf.state, month
        order by 1, 2, 3, 4)
select case when country in ('CA','CAN','CANADA') then 'CANADA'
     when country in ('US','UNITED STATES','USA') then 'USA'
    else 'UNK'
    end as country, 
        city, 
        case when state = 'AL' then 'ALABAMA'
        when state = 'AK' then 'ALASKA'
        when state = 'AZ' then 'ARIZONA'
        when state = 'AR' then 'ARKANSAS'
        when state = 'CA' then 'CALIFORNIA'
        when state = 'CO' then 'COLORADO'
        when state = 'CT' then 'CONNECTICUT'
        when state = 'DE' then 'DELAWARE'
        when state = 'FL' then 'FLORIDA'
        when state = 'GA' then 'GEORGIA'
        when state = 'HI' then 'HAWAII'
        when state = 'ID' then 'IDAHO'
        when state = 'IL' then 'ILLINOIS'
        when state = 'IN' then 'INDIANA'
        when state = 'IA' then 'IOWA'
        when state = 'KS' then 'KANSAS'
        when state = 'KY' then 'KENTUCKY'
        when state = 'LA' then 'LOUISIANA'
        when state = 'ME' then 'MAINE'
        when state = 'MD' then 'MARYLAND'
        when state = 'MA' then 'MASSACHUSETTS'
        when state = 'MI' then 'MICHIGAN'
        when state = 'MN' then 'MINNESOTA'
        when state = 'MS' then 'MISSISSIPPI'
        when state = 'MO' then 'MISSOURI'
        when state = 'MT' then 'MONTANA'
        when state = 'NE' then 'NEBRASKA'
        when state = 'NV' then 'NEVADA'
        when state = 'NH' then 'NEW HAMPSHIRE'
        when state = 'NJ' then 'NEW JERSEY'
        when state = 'NM' then 'NEW MEXICO'
        when state = 'NY' then 'NEW YORK'
        when state = 'NC' then 'NORTH CAROLINA'
        when state = 'ND' then 'NORTH DAKOTA'
        when state = 'OH' then 'OHIO'
        when state = 'OK' then 'OKLAHOMA'
        when state = 'OR' then 'OREGON'
        when state = 'PA' then 'PENNSYLVANIA'
        when state = 'RI' then 'RHODE ISLAND'
        when state = 'SC' then 'SOUTH CAROLINA'
        when state = 'SD' then 'SOUTH DAKOTA'
        when state = 'TN' then 'TENNESSEE'
        when state = 'TX' then 'TEXAS'
        when state = 'UT' then 'UTAH'
        when state = 'VT' then 'VERMONT'
        when state = 'VA' then 'VIRGINIA'
        when state = 'WA' then 'WASHINGTON'
        when state = 'WV' then 'WEST VIRGINIA'
        when state = 'WI' then 'WISCONSIN'
        when state = 'WY' then 'WYOMING'
        when country in ('CA','CAN','CANADA') and state in ('AL','AB') then 'ALBERTA'
        when state in ('B.C.','BC') then 'BRITISH COLUMBIA'
        when state = 'MB' then 'MANITOBA'
        when state = 'NB' then 'NEW BRUNSWICK'
        when state = 'NL' then 'NEWFOUNDLAND AND LABRADOR'
        when state = 'NS' then 'NOVA SCOTIA'
        when state = 'NT' then 'NORTHWEST TERRITORIES'
        when state = 'NU' then 'NUNAVUT'
        when state = 'ON' then 'ONTARIO'
        when state = 'PE' then 'PRINCE EDWARD ISLAND'
        when state in ('QB','QC') then 'QUEBEC'
        when state in ('SK','SASKETCHEWAN','SASKETCHWAN') then 'SASKATCHEWAN'
        when state = 'YT' then 'YUKON'                
        else state end as state,  
month,
sum(total_collected) over (partition by country, city, state order by month rows between unbounded preceding and current row) as cum_total_collected,
sum(total_imported) over (partition by country, city, state order by month rows between unbounded preceding and current row) as cum_total_imported
from emails_collected 
order by 4;

