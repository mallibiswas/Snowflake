#!bin/bash
#
SQLDIR=$HOME/snowflake/zenalytics/revrec/lib
LOGDIR=$HOME/snowflake/zenalytics/revrec/log
#
file_ext=sql
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=Revrec_daily_load_$logfile_ext.log
whname=ZENLOADER
dbname=ZENALYTICS
targetschema=REVREC
sourcedb=ZENALYTICS
rolename=ETL_PROD_ROLE
sourceschema=RECURLY
stageschema=_STAGING
#
load_date=$(date +%x_%H:%M:%S:%N)
#
echo load date = $load_date
#
# 	List of sql files to process sequentially
#
file_list="
account_revenue_movements
";
#
#	setup environment
#
cd $SQLDIR;
#
# 	loop through files and execute sequentially
#
for filename in $file_list;
do
	echo checking $SQLDIR/$filename.sql
	ls $SQLDIR/$filename.sql;
	if [[ $? = 0 ]]; then
		echo ###############################
		echo file $filename.sql exists, executing
		echo file=$filename.sql;
		echo ##############################


logfile=$filename.log

# Create dynamic sql script
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!set exit_on_error=True;
!set echo=True;
!set output_file=$LOGDIR/$logfile;
!define asof_date="$asof_date";
!define dbname=$dbname;
!define sourcedb=$sourcedb;
!define whname=$whname;
!define schemaname=$targetschema;
!define stageschemaname=$stageschema;
!define rolename=$rolename;
!source $SQLDIR/$filename.sql;
SQL
"
# run sql script to load each file in foreground
# run script
echo $SQLCMD > $LOGDIR/$logfile
eval "${SQLCMD}" >> $LOGDIR/$logfile  # run sequentially because of dependencies
	else
		echo $stagefile does not exist
	fi
done
#
echo Completed
#
