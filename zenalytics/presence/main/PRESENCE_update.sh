#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/presence/main/lib
LOGDIR=$HOME/snowflake/zenalytics/presence/main/log
#
#
# job to run daily DML on presence data for easier consumption in ZENALYTICS
#
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
log_file=PRESENCE_updates_$logfile_ext.log
#
# 	List of files to process
#
file_list="
finished_sightings_update_insert_new
visits_smry
";
#
#	setup environment
#
cd $SQLDIR;
#
# 	loop through files and load
#
for filename in $file_list;
do
echo ###############################
echo file=$filename.sql;
echo ##############################
#
# Create dynamic sql script
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!define whname=ZENLOADER;
!define rolename=ETL_PROD_ROLE;
!define srcdbname=ZENPROD;
!define tgtdbname=ZENALYTICS;
!define srcschemaname=PRESENCE;
!define tgtschemaname=PRESENCE;
!define tgttablename=FINISHED_SIGHTINGS;
!source $filename.sql;
SQL
"
# run sql script to load each file in background
eval "${SQLCMD}" > $LOGDIR/$log_file
#
done
#
#
#
echo Completed
#
