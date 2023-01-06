#!bin/bash
#
SQLDIR=$HOME/snowflake/zenalytics/bizint/lib
LOGDIR=$HOME/snowflake/zenalytics/bizint/log
#
file_ext=sql
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=EDW_daily_load_$logfile_ext.log
whname=ZENLOADER
dbname=ZENALYTICS
targetschema=BIZINT
sourcedb=ZENALYTICS
rolename=ETL_PROD_ROLE
sourceschema=BIZINT
stageschema=_STAGING
#
load_date=$(date +%x_%H:%M:%S:%N)
#
echo load date = $load_date
#
# 	List of sql files to process sequentially
#
v2_file_list="
order_lifecycle
customer_lifecycle
location_lifecycle
location_details_fact
location_summary_fact
";
#
file_list="
recurly_invoices
salesforce_OLI_summary
salesforce_opportunities
opportunity_subscription_lookup
opportunity_io_lookup
delinquency_lifecycle
adjustments
churn_requests
offsetting_subscriptions
subscription_lifecycle
subscription_details_fact
subscription_summary_fact
";
test_file_list="
opportunity_subscription_lookup
";
#
#
#	setup environment
#
cd $SQLDIR;
#
# 	loop through files and execute sequentially 
#
for filename in $file_list;
do
	echo checking $SQLDIR/$filename.sql
	ls $SQLDIR/$filename.sql;
	if [[ $? = 0 ]]; then
		echo ############################### 
		echo file $filename.sql exists, executing 
		echo file=$filename.sql;
		echo ############################## 


logfile=$filename.log

# Create dynamic sql script
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!set exit_on_error=True;
!set echo=False;
!set output_file=$LOGDIR/$logfile;
!define asof_date="$asof_date";
!define dbname=$dbname;
!define sourcedb=$sourcedb;
!define whname=$whname;
!define schemaname=$sourceschema;
!define stageschemaname=$stageschema;
!define rolename=$rolename;
!source $SQLDIR/$filename.sql;
SQL
"
# run sql script to load each file in foreground
# run script
echo $SQLCMD > $LOGDIR/$logfile 
eval "${SQLCMD}" >> $LOGDIR/$logfile  # run sequentially because of dependencies
	else
		echo $stagefile does not exist
	fi	
done
#
echo Completed
#
