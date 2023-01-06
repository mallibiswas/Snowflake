-----------------------------------------------------------------------
------------------------------- CONTRACT_V2 ------------------------------
-----------------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.CONTRACT_V2;

copy into  &{dbname}._STAGING.CONTRACT_V2
from
(
	select 
	$1::integer as contract_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5 as contract_state,
	$6 as salesforce_opportunity_id,
	$7::timestamp as sent_date,
	$8::timestamp as signed_date,
	$9::integer as dashboard_hardware_count,
	$10::integer as dashboard_hardware_fee,
	$11 as network_ads_social_media_accounts,
	$12 as signer_name,
	$13 as signer_email,
	$14 as contract_hard_copy_url,
	$15 as old_world_contract_id,
	$16::integer as installation_fee,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/contract_v2.csv
);

alter table &{dbname}._STAGING.CONTRACT_V2 swap with &{dbname}.AMS.CONTRACT_V2;
