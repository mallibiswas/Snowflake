#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/business_profiles/lib
LOGDIR=$HOME/snowflake/zenalytics/business_profiles/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=business_profiles_refresh_data_$logfile_ext.log
whname=ZENLOADER
dbname=ZENALYTICS
rolename=ETL_PROD_ROLE
sourceschemaname=PRESENCE
targetschemaname=BUSINESS_PROFILES
stageschemaname=_STAGING
#
# 	List of sql scripts to process
#
# deprecated mail_hook.sql 6/28
#
file_list="
business_recognition_rate_details_fact.sql
";
#
#	setup environment
#
cd $SQLDIR;
#
# 	loop through files and refresh
#
for filename in $file_list;
do
	ls $SQLDIR | grep -i $filename;
	if [[ $? = 0 ]]; then
		echo executing $filename ...................
# Create dynamics sql script
SQLCMD="
snowsql 2>$LOGDIR/business_profiles_refresh_data.err << SQL;
!set variable_substitution=true;
!define dbname=$dbname;
!define whname=$whname;
!define sourceschemaname=$sourceschemaname;
!define targetschemaname=$targetschemaname;
!define stageschemaname=$stageschemaname;
!define rolename=$rolename;
!source $SQLDIR/$filename;
SQL
"
# run sql script to load each file in background
	eval "${SQLCMD}" >> $LOGDIR/$logfile
	else
		echo $filename does not exist ...................
	fi
done
#
echo Completed
#
