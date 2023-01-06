#!bin/bash
SQLDIR=$HOME/snowflake/snapshots/lib
SETUPDIR=$HOME/snowflake/snapshots/initialize
LOGDIR=$HOME/snowflake/snapshots/log
#
#
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
log_file=snapshot_load_data_$logfile_ext.log
whname=ZENLOADER
dbname=ZENALYTICS
amsschemaname=AMS
crmschemaname=CRM
rolename=ETL_PROD_ROLE
amsstagename=ZENALYTICS.public.s3_ams_stage
amsstageurl=s3://zp-uw2-data-archives/rds/ams/
crmstagename=ZENALYTICS.public.s3_mongo_stage
crmstageurl=s3://zp-uw2-data-archives/mongo/
#
# begin and end dates
# 
snapshot_begin_date=2019-05-19
snapshot_end_date=2019-05-20
#
#       setup environment
#
cd $SQLDIR;
#
SETUPCMD="
snowsql << SQL;
!set variable_substitution=true;
!define amsstagename=$amsstagename;
!define crmstagename=$crmstagename;
!define amsstageurl=$amsstageurl;
!define crmstageurl=$crmstageurl;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!source $SETUPDIR/initialize_snapshot_load.sql;
SQL
"
#
eval "${SETUPCMD}" > $LOGDIR/$log_file 
#
# get latest load date
#
echo load date = $(date) 
#
# 	List of files to process
#
# After this, startdate and enddate will be valid ISO 8601 dates,
# or the script will have aborted when it encountered unparseable data
# such as input_end=abcd
#
startdate=$(date -d "$snapshot_begin_date" +%Y%m%d) || exit -1
enddate=$(date -d "$snapshot_end_date" +%Y%m%d)     || exit -1
#
echo startdate "$startdate" enddate "$enddate"
#
# date loop
#
d="$startdate"
while [ "$(date -d "$d" +%Y%m%d)" -le "$(date -d "$enddate" +%Y%m%d)" ]; do
	snapshot_date=$d
	echo executing AMS_snapshot.sql for $snapshot_date
	echo building snapshot for $snapshot_date
#
# Create dynamics sql script for AMS snapshots
#
AMSSQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$amsschemaname;
!define stagename=$amsstagename;
!define asof_date="$(date -d "$snapshot_date" +%Y-%m-%d)" 
!define stagepath="$(date -d "$snapshot_date" +%Y-%m-%d)" 
!define tableext="$(date -d "$snapshot_date" +%Y%m%d)" 
!source $SQLDIR/ams_snapshots.sql;
SQL
"
# run sql script to load AMS file in background 
#eval "${AMSSQLCMD}" >> $LOGDIR/$log_file 
#
# Create dynamics sql script for CRM snapshots
#
CRMSQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$crmschemaname;
!define stagename=$crmstagename;
!define asof_date="$(date -d "$snapshot_date" +%Y-%m-%d)" 
!define stagepath="$(date -d "$snapshot_date" +%Y-%m-%d)" 
!define tableext="$(date -d "$snapshot_date" +%Y%m%d)" 
!source $SQLDIR/crm_snapshots.sql;
SQL
"
# run sql script to load CRM file in background
eval "${CRMSQLCMD}" >> $LOGDIR/$log_file 
#
# date loop
  	d=$(date -I -d "$d + 1 day") # increment date
done
#
#
echo Completed
#
