#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/ams_v3/accounts
SETUPDIR=$HOME/snowflake/zenalytics/ams_v3/initialize
LOGDIR=$HOME/snowflake/zenalytics/ams_v3/accounts/log
#
file_ext=csv
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=AMS_accounts_load_daily_data_$logfile_ext.log
stagename=ZENALYTICS._STAGING.s3_ams_accounts_stage
dbname=ZENALYTICS
whname=ZENLOADER
rolename=ETL_PROD_ROLE
schemaname=AMS_ACCOUNTS
stageschemaname=_STAGING
#
# get latest load date
load_date=$(aws s3 ls s3://zp-uw2-data-archives/rds/ams-accounts/ | sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
#
echo load date = $load_date
#
stagepath=$load_date
stageurl=s3://zp-uw2-data-archives/rds/ams-accounts
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
";
oltp_tables_list="
account
asset
charge
order
subscription
payment
recurly
salesforce
ads
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
INITIALIZE="
snowsql << SQL;
!set variable_substitution=true;
!define stagename=$stagename;
!define stageurl=$stageurl;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!source $SETUPDIR/initialize_ams_load.sql;
SQL
"
	echo $INITIALIZE > $LOGDIR/$logfile 
	eval "${INITIALIZE}" >> $LOGDIR/$logfile 
#
# 	loop through files and load
#
for filename in $all_tables_list;

do

logfile=$filename.log

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
!define schemaname=$schemaname;
!define stageschemaname=$stageschemaname;
!source $SQLDIR/$filename.sql;
SQL
"
# run sql script to load each file in background
	echo $SQLCMD >> $LOGDIR/$logfile 
	eval "${SQLCMD}" >> $LOGDIR/$logfile 
done
#
#
echo Completed
#

