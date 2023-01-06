--
use warehouse &{whname};
use database &{dbname};
use schema &{schemaname};
use role &{rolename};
--
ALTER SESSION SET TIMEZONE = 'UTC';
SET MAX_TS = (select to_timestamp_ntz(dateadd(day, -30, current_date())));
--
-- main query
delete 
from  &{dbname}.&{schemaname}.&{tablename}
where &{datefield} < $MAX_TS; 

