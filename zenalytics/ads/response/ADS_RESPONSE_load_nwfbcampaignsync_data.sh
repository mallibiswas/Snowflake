#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/ads/response/lib
LOGDIR=$HOME/snowflake/zenalytics/ads/response/log
#
file_ext=sql
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=ADS_load_hourly_nwfbcampaignsync_data_$logfile_ext.log
#
#nwbusiness # taken out of bucketlist per John's request to run it every 2 hours
bucketlist="
nwsampling
nwfbcampaignsync
nwomni
liveramp_measurement
nwlrcampaignsync
";
#
dbname=ZENALYTICS
whname=ADSLOADER
rolename=ETL_PROD_ROLE
schemaname=ADS_CAMPAIGNS
#
# loop over bucket names
#
for bucketname in $bucketlist;
do
	stageurl=s3://zp-uw2-data-archives/rds/$bucketname
	stagename=$dbname._staging.s3_ads_${bucketname}_stage
	echo processing bucket: $stageurl stage: $stagename
#
# get latest load date
load_date=$(aws s3 ls ${stageurl}/ | sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
#
echo load date = $load_date
#
stagepath=$load_date
echo Staging From: $stageurl/$stagepath/
#
#	setup environment
#
cd $SQLDIR;
#
snowsql << SQL;
!set variable_substitution=true;
!set output_file=$LOGDIR/initialize_campaigns_$logfile_ext.log;
!define stagename=$stagename;
!define stageurl=$stageurl;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=_STAGING;
!source $SQLDIR/initialize_campaigns_load.sql;
SQL
#
# 	loop through files (same as bucketname) and load
#
filename=$bucketname.$file_ext
#
echo loading from $stageurl/$stagepath
echo processing file $filename
#
# Create dynamic sql bulkloader script
#
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!set output_file=$LOGDIR/$bucketname_$logfile_ext.log;
!define stagename=$stagename;
!define stagepath=$stagepath;
!define asof_date=$load_date;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$schemaname;
!source $SQLDIR/$filename;
SQL
"
# run sql script to load each file in background
eval "${SQLCMD}" >> $LOGDIR/$logfile  **************************
#
done # bucketname loop
