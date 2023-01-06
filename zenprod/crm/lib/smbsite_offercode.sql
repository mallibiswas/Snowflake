-----------------------------------------------------------------------
---------------------  smbsite_offercode ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_offercode;

copy into &{dbname}.&{stageschema}.smbsite_offercode
from 
(
select  
      parse_json($1):id::string as offer_id,
      parse_json($1):code::string as offer_code,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_offercode00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_offercode swap with &{dbname}.&{schemaname}.smbsite_offercode;


