#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/down_router/lib
SETUPDIR=$HOME/snowflake/zenalytics/down_router/initialize
LOGDIR=$HOME/snowflake/zenalytics/down_router/log
#
#
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
log_file=down_router_load_data_$logfile_ext.log
whname=ZENLOADER
dbname=ZENALYTICS
schemaname=PUBLIC
rolename=ETL_PROD_ROLE
#
# list of files
#
filelist="
down_router_last_portal_blip.sql
down_router_churn_request.sql
down_router_last_location_blip.sql
down_router_blip_snapshot.sql
";
#
# get latest load date
#
asof_date=$(date) 
#
# 	List of files to process
#
# After this, startdate and enddate will be valid ISO 8601 dates,
# or the script will have aborted when it encountered unparseable data
# such as input_end=abcd
#
enddate=$(date -d "$asof_date" +%Y%m%d) || exit -1
startdate=$(date -I -d "$enddate - 2 day") # decrement date
#
echo startdate "$startdate" enddate "$enddate"
#
for filename in $filelist;
do
echo Running $filename
echo dbname=$dbname;
echo whname=$whname;
echo rolename=$rolename;
echo schemaname=$schemaname;
echo asof_date="$(date -d "$asof_date" +%Y-%m-%d)" 
echo period_begin_date="$(date -d "$startdate" +%Y-%m-%d)" 
echo period_end_date="$(date -d "$enddate" +%Y-%m-%d)" 
#
# Create dynamic sql script
#
SQLCMD="
snowsql 2>$LOGDIR/$filename.err << SQL;
!set variable_substitution=true;
!set echo=true;
!set quiet=false;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$schemaname;
!define asof_date="$(date -d "$asof_date" +%Y-%m-%d)" 
!define period_begin_date="$(date -d "$startdate" +%Y-%m-%d)" 
!define period_end_date="$(date -d "$enddate" +%Y-%m-%d)" 
!source $SQLDIR/$filename;
SQL
"
# run sql script to load AMS file in background 
eval "${SQLCMD}" >> $LOGDIR/$log_file 
#
done # file loop
#
echo Completed
#
