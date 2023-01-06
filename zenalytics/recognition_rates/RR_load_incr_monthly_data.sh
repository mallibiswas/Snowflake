#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/recognition_rates/lib
LOGDIR=$HOME/snowflake/zenalytics/recognition_rates/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
whname=ZENLOADER
dbname=ZENALYTICS
rolename=ETL_PROD_ROLE
schemaname=PUBLIC
tablename=RECOGNITION_RATES_BY_GEO
#
#	setup environment
#
cd $SQLDIR;
#
filename=recognition_rates_monthly_data.sql
#
logfile=${filename}_$logfile_ext.log
#
# Create dynamics sql script
#
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!define dbname=$dbname;
!define whname=$whname;
!define schemaname=$schemaname;
!define rolename=$rolename;
!define tablename=$tablename;
!source $SQLDIR/$filename;
SQL
"
# run sql script 
eval "${SQLCMD}" > $LOGDIR/$logfile 
#
echo Done inserting new records!
#
#
