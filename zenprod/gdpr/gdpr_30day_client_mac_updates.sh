#!bin/bash
SQLDIR=$HOME/snowflake/zenprod/gdpr/lib
LOGDIR=$HOME/snowflake/zenprod/gdpr/log
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
echo "${SQLCMD}" >> $LOGDIR/$logfile 
eval "${SQLCMD}" >> $LOGDIR/$logfile 
       }  
#
#
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=gdpr_30d_client_mac_updates_$logfile_ext.log
whname=GDPR
dbname=ZENPROD
rolename=ETL_PROD_ROLE
schemaname=PRESENCE
#
# 	List of sql scripts to process
#
filename=gdpr_presence_30day_client_mac_updates.sql
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
gdpr_array=($rolename $dbname $whname $schemaname ENRICHED_SIGHTINGS END_TIME EPOCH_MILLISECOND)
execute_sql "${gdpr_array[@]}"
#
echo Completed
#


