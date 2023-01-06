#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/presence_metrics/lib
LOGDIR=$HOME/snowflake/zenalytics/presence_metrics/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=presence_metrics_weekly_$logfile_ext.log
whname=ZENLOADER
dbname=ZENALYTICS
rolename=ETL_PROD_ROLE
tgtschemaname=PUBLIC
stageschemaname=_STAGING
#
# 	List of sql scripts to process
#
# 
file_list="
presence_metrics.sql
";
#
#	setup environment
#
cd $SQLDIR;
#
# 	loop through files and refresh 
#
for filename in $file_list;
do
	ls $SQLDIR | grep -i $filename;
	if [[ $? = 0 ]]; then
		echo executing $filename ...................
# Create dynamics sql script
SQLCMD="
snowsql 2>$LOGDIR/presence_weekly_metrics.err << SQL;
!set variable_substitution=true;
!define dbname=$dbname;
!define whname=$whname;
!define tgtschemaname=$tgtschemaname;
!define stageschemaname=$stageschemaname;
!define rolename=$rolename;
!source $SQLDIR/$filename;
SQL
"
# run sql script to load each file in background
	echo "${SQLCMD}" > $LOGDIR/$logfile 
	eval "${SQLCMD}" >> $LOGDIR/$logfile 
	else
		echo $filename does not exist ...................
	fi	
done
#
echo Completed
#
