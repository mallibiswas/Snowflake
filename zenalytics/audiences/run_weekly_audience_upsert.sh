#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/audiences/lib
LOGDIR=$HOME/snowflake/zenalytics/audiences/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=audiences_weekly_upsert_$logfile_ext.log
whname=ZENLOADER
srcdbname=ZENALYTICS
srcschemaname=CRM
tgtdbname=ZENALYTICS
tgtschemaname=AUDIENCES
stageschemaname=_STAGING
rolename=ETL_PROD_ROLE
#
# 	List of sql scripts to process
#
# deprecated mail_hook.sql 6/28
#
file_list="
user_profile.sql
user_sightings.sql
audience.sql
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
snowsql 2>$LOGDIR/audiences_weekly_upsert.err << SQL;
!set variable_substitution=true;
!define whname=$whname;
!define srcdbname=$srcdbname;
!define srcschemaname=$srcschemaname;
!define tgtdbname=$tgtdbname;
!define tgtschemaname=$tgtschemaname;
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
