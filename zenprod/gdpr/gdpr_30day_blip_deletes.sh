#!bin/bash
SQLDIR=$HOME/snowflake/zenprod/gdpr/lib
LOGDIR=$HOME/snowflake/zenprod/gdpr/log
#
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=gdpr_30d_blip_deletes_$logfile_ext.log
whname=GDPR
dbname=ZENPROD
rolename=ETL_PROD_ROLE
schemaname=PRESENCE
#
#
function execute_sql {
#
# Create dynamic sql script
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!define rolename=$1;
!define dbname=$2;
!define whname=$3;
!define schemaname=$4;
!define tablename=$5;
!define datefield=$6;
!define epoch=$7;
!source $SQLDIR/$filename;
SQL
"
# run sql script to load each file in background
#echo "${SQLCMD}" > $LOGDIR/$logfile 
eval "${SQLCMD}" >> $LOGDIR/$logfile 
}
#
# 	List of sql scripts to process
#
filename=gdpr_presence_30day_blip_deletes.sql
#
#
cd $SQLDIR;
ls $SQLDIR | grep -i $filename;
	if [[ $? = 0 ]]; then
		echo executing $filename ...................
	else
		echo $filename does not exist ...................
		exit
	fi	
# declare array of env variables
# array: rolename dbname whname schemaname tablename datefield epoch 
# offset if the offset for unix time (EPOCH_SECOND, EPOCH_MILLISECOND ..)
#
# 30 day deletes from BLIP data
#
gdpr_presence_array=($rolename $dbname $whname $schemaname PORTAL_BLIPS SERVER_TIME EPOCH_SECOND)
execute_sql "${gdpr_presence_array[@]}"
gdpr_presence_array=($rolename $dbname $whname $schemaname LOCATION_BLIPS SERVER_TIME EPOCH_SECOND)
execute_sql "${gdpr_presence_array[@]}"
#
echo Completed
#
