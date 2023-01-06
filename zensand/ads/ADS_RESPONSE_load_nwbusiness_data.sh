#!bin/bash
SQLDIR=$HOME/snowflake/zensand/ads/lib
LOGDIR=$HOME/snowflake/zensand/ads/log
#
file_ext=sql
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=ADS_load_hourly_nwbusiness_data.log
#
bucketlist="
nwbusiness
";
#
dbname=ZENSAND
whname=ADSERVICES
rolename=ZDSERVICE_ROLE
schemaname=ADS
#
# loop over bucket names
#
for bucketname in $bucketlist;
do
	stageurl=s3://zp-uw2-data-archives/rds/$bucketname
	stagename=$dbname.ADS.s3_ads_${bucketname}_stage
	echo processing bucket: $stageurl stage: $stagename
#
# get latest load date
load_date=$(aws s3 ls ${stageurl}/ | sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
#
echo load date = $load_date
#
#
#	setup environment
#
cd $SQLDIR;
stagepath=$load_date
echo Staging From: $stageurl/$stagepath/
#
# 	loop through files (same as bucketname) and load
#
filename=$bucketname
echo processing file $filename
errorfile=$filename.err
#
# Create dynamic sql bulkloader script
#
SQLCMD="
snowsql 2>$LOGDIR/$errorfile << SQL
!set variable_substitution=true;
!set output_file=$LOGDIR/"$bucketname"_$logfile_ext.log;
!define stagename=$stagename;
!define stagepath=$stagepath;
!define asof_date=$load_date;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$schemaname;
!source $SQLDIR/$filename.sql;
SQL
"
# run sql script to load each file in background
eval "${SQLCMD}" >> $LOGDIR/$logfile
#
done # bucketname loop
