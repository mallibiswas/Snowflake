#!bin/bash
SQLDIR=$HOME/snowflake/zenprod/gdpr/lib
LOGDIR=$HOME/snowflake/zenprod/gdpr/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=gdpr_ads_privacy_deletes_$logfile_ext.log
whname=GDPR
dbname=ZENALYTICS
rolename=ETL_PROD_ROLE
#
function execute_sql {
#
cd $SQLDIR;
ls $SQLDIR | grep -i $8;
	if [[ $? = 0 ]]; then
		echo executing $8 ...................
	else
		echo $8 does not exist ...................
		exit
	fi	
#
# Create dynamic sql script
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!define rolename=$1;
!define dbname=$2;
!define whname=$3;
!define refschemaname=$4;
!define reftablename=$5;
!define tgtschemaname=$6;
!define tgttablename=$7;
!source $SQLDIR/$8.sql;
SQL
"
# run sql script to load each file in background
echo "${SQLCMD}" > $LOGDIR/$logfile
eval "${SQLCMD}" > $LOGDIR/$logfile
      }
#
# 	List of sql scripts to process
#
# delete gdpr blacklisted contact record(s) from consented sightings and ads targeting
# declare array of env variables
# array: rolename dbname whname schemaname tablename datefield epoch
#
gdpr_array=($rolename $dbname $whname PRIVACY PRIVACY_REQUEST ADS AUDIENCE_VISIT_AGG gdpr_ads_privacy_deletes)
execute_sql "${gdpr_array[@]}"
#
echo Completed

