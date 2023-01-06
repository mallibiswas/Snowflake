#!bin/bash
SQLDIR=$HOME/snowflake/zenprod/gdpr/lib
LOGDIR=$HOME/snowflake/zenprod/gdpr/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=gdpr_r2f_deletes_$logfile_ext.log
whname=GDPR
dbname=ZENPROD
rolename=ETL_PROD_ROLE
stageschemaname=_STAGE
tgtschemaname=PRESENCE
refschemaname=PRIVACY
reftablename=PRIVACY_DELETES
#
function execute_sql {
#
cd $SQLDIR;
ls $SQLDIR | grep -i $9;
	if [[ $? = 0 ]]; then
		echo executing $9 ...................
	else
		echo $9 does not exist ...................
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
!define stageschemaname=$4;
!define tgtschemaname=$5;
!define tgttablename=$6;
!define refschemaname=$7;
!define reftablename=$8;
!source $SQLDIR/$9.sql;
SQL
"
# run sql script to load each file in background
echo "${SQLCMD}" >> $LOGDIR/$8_$logfile_ext.log 
eval "${SQLCMD}" >> $LOGDIR/$8_$logfile_ext.log 
               }
#
# 	List of sql scripts to process
#
# delete gdpr blacklisted contact record(s) from consented sightings and ads targeting
# declare array of env variables
# array: rolename dbname whname schemaname tablename datefield epoch
# offset if the offset for unix time (EPOCH_SECOND, EPOCH_MILLISECOND ..)
#
gdpr_array=($rolename $dbname $whname $stageschemaname $tgtschemaname CONSENTED_SIGHTINGS $refschemaname $reftablename gdpr_presence_right2forget_updates)
execute_sql "${gdpr_array[@]}"
gdpr_array=($rolename $dbname $whname $stageschemaname $tgtschemaname ENRICHED_SIGHTINGS $refschemaname $reftablename gdpr_presence_right2forget_updates)
execute_sql "${gdpr_array[@]}"
#
echo Completed

