----------------------------------------------------------
------ Opprotunity (Id) to Insertion Orders (Id) Lookup/Bridge table 
----------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

create or replace table &{dbname}.&{schemaname}.opportunity_io_lookup 
as
select  c.account_id,
        substr(s.opportunityid,1,15) as salesforce_opportunity_id,
        c.ads_io_id
from    ams_accounts.campaign c,
        ams_accounts.asset a, 
        ams_accounts.order_item oi,
        ams_accounts."order" o,
        ams_accounts.salesforce_quote sq,
        sfdc.quote s
where c.ads_io_id = a.ads_io_id
and a.asset_id = oi.asset_id
and o.order_id  = oi.order_id
and o.salesforce_quote_key = sq.salesforce_quote_key
and sq.salesforce_quote_id = s.id;

alter table opportunity_io_lookup add column asof_date date;
update opportunity_io_lookup set asof_date = current_date;

