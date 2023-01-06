#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/ams_v2/lib
LOGDIR=$HOME/snowflake/zenalytics/ams_v2/log
#
file_ext=csv
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=AMS_load_daily_data_$logfile_ext.log
stagename=zenalytics.public.s3_ams_stage
dbname=ZENALYTICS
whname=ZENLOADER
rolename=ETL_PROD_ROLE
schemaname=AMS
#
# get latest load date
load_date=$(aws s3 ls s3://zp-uw2-data-archives/rds/ams/ | grep 2019 | sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
#
echo load date = $load_date
#
stagepath=$load_date
stageurl=s3://zp-uw2-data-archives/rds/ams
echo Staging From: $stageurl/$stagepath/
stagepath=$load_date
#
# Set load_lookup_tables to True is need to load lookup tables
#
load_lookup_tables=False
#
# 	List of csv files to process
#
lookup_tables_list="
killswitch
locationreferral
referralpayment
referrer
subscriptionoptionuser
apiauthkey
contractcategory
product
usagebillinginfo_account_through
featureflag
planaddon
subscriptionoption
role
package
plan
usagebillinginfo_package_through
usagebillinginfo
featureflaguser
feature
subscriptionaddon
subscriptionaddonprice
package_feature_through
";
#
oltp_tables_list="
account
amdashboard_collectionstats
partneraccount
staffuser
staffuserrole
contractsubscriptionoptionlog
amdashboard_email_imports
billing_account_v2_migration
amdashboard_walkthroughs
subscription_v2_migration
billing_account_v2
amdashboard_smart_message_usage
amdashboard_smart_messages_config
subscriptions_v2_location_through
subscriptions_v2
contract_v2
amdashboard_locations_with_contracts
amdashboard_routerdata
amdashboard_ticket_price
amdashboard_rep_management
amdashboard_event_logs
amdashboard_billing_data
amdashboard_router_types
billingaccount
amdashboard_last_logged_in_users_salesforce
subscription
amdashboard_yelp
amdashboard_account_managers
amdashboard_dashboard_logins
location
locationstaffuserrole
contract
locationcontract
router
transaction
salesforcetransactionreference
salesforceinvoicereference
event
";
#
if [[ $load_lookup_tables = "True" ]]; then
	all_tables_list="$lookup_tables_list $oltp_tables_list"
	echo loading lookup tables ............ 
	else
	all_tables_list=$oltp_tables_list
	echo loading only oltp tables .......... 
fi

#
#	setup environment
#
cd $SQLDIR;
#
snowsql << SQL;
!set variable_substitution=true;
!define stagename=$stagename;
!define stageurl=$stageurl;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!source $SQLDIR/initialize_ams_load.sql;
SQL
#
# 	loop through files and load
#
for filename in $all_tables_list;

do
	stagefile=$filename.$file_ext
	echo $stageurl/$stagepath/$stagefile
	aws s3 ls $stageurl/$stagepath/$stagefile;
	if [[ $? = 0 ]]; then
		echo ############################### 
		echo file $stagefile exists, loading
		echo stagename=$stagename;
		echo stageurl=$stageurl;
		echo stagepath=$stagepath;
		echo file=$filename.sql;
		echo ############################## 
# Create dynamic sql bulkloader script
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!define stagename=$stagename;
!define stagepath=$stagepath;
!define asof_date=$load_date;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!source $SQLDIR/$filename.sql;
SQL
"
# run sql script to load each file in background
	echo $SQLCMD >> $LOGDIR/$logfile 
	eval "${SQLCMD}" >> $LOGDIR/$logfile 
	else
		echo $stagefile does not exist
	fi	
done
#
# Create dynamics sql script for AMS snapshots
#
SNAPSHOTSQL="
snowsql << SQL;
!set variable_substitution=true;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$schemaname;
!define stagename=$stagename;
!define stagepath=$stagepath;
!define asof_date="$(date -d "$load_date" +%Y-%m-%d)"
!source $SQLDIR/ams_snapshots.sql;
SQL
"
# run sql script to load AMS file in background
eval "${SNAPSHOTSQL}" >> $LOGDIR/$logfile
#
# Post process - clean up staging 
#
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!define stagename=$stagename;
!source $SQLDIR/post_process_ams_load.sql;
SQL
"
#
echo Completed
#
