#!bin/bash
#
# [2020/08/07: MB: Changelog: excluded call to initialize_mongo_load
#
SQLDIR=$HOME/snowflake/zenalytics/crm/lib
LOGDIR=$HOME/snowflake/zenalytics/crm/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=CRM_snapshots_$logfile_ext.log # write individual file logs within filename loop
routerstagename=zenalytics.public.s3_mongo_stage
bprstagename=zenalytics.public.s3_businessprofile_stage
whname=ZENLOADER
dbname=ZENALYTICS
rolename=ETL_PROD_ROLE
schemaname=CRM
#
# get latest load dates
bpr_load_date=$(aws s3 ls s3://zp-uw2-data-archives/mongo/portal_businessprofile/ | sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
router_load_date=$(aws s3 ls s3://zp-uw2-data-archives/mongo/ |grep -v "portal_businessprofile"| sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
#
echo load dates = portal_router: $router_load_date portal_businessprofile: $bpr_load_date
#
routerstageurl=$routerstagename/$router_load_date
bprstageurl=$bprstagename/$bpr_load_date
#
#	setup environment
#
cd $SQLDIR;
echo Staging portal_router From: $routerstageurl
#
# Create dynamic sql script for Portal Router snapshots
#
ROUTERSQLCMD="
snowsql -o log_file=$LOGDIR/$logfile 2>$LOGDIR/router_snapshot.err << SQL
!set variable_substitution=true;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$schemaname;
!define stageurl=$routerstageurl;
!define asof_date=$router_load_date;
!source $SQLDIR/router_snapshot.sql;
SQL
"
# run sql script to load CRM file in background
eval "${ROUTERSQLCMD}" > $LOGDIR/$logfile
#
echo Completed router snapshot
#
echo Staging portal_businessprofile From: $bprstageurl
#
BPRSQLCMD="
snowsql -o log_file=$LOGDIR/$logfile 2>$LOGDIR/businessprofile_snapshot.err << SQL
!set variable_substitution=true;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$schemaname;
!define stageurl=$bprstageurl;
!define asof_date=$bpr_load_date;
!source $SQLDIR/businessprofile_snapshot.sql;
SQL
"
# run sql script to load CRM file in background
eval "${BPRSQLCMD}" >> $LOGDIR/$logfile
#
echo Completed businessprofile snapshot
#
