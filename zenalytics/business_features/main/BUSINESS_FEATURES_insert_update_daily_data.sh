#!bin/bash
MAINDIR=$HOME/snowflake/zenalytics/business_features/main/
PYDIR=/home/anaconda3/bin
SQLDIR=$HOME/snowflake/zenalytics/business_features/main/lib
LOGDIR=$HOME/snowflake/zenalytics/business_features/main/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
whname=ZENLOADER
dbname=ZENALYTICS
rolename=ETL_PROD_ROLE
schemaname=BUSINESS_FEATURES
tablename=BUSINESS_FEATURES_RAW
#
#	setup environment
#
cd $SQLDIR;
#
filename=incremental_insert_business_features;
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
!source $SQLDIR/$filename.sql;
SQL
"
# run sql script 
eval "${SQLCMD}" > $LOGDIR/$logfile 
#
echo Done inserting new records!
#
echo Begin python updates
#
cd $MAINDIR
#
$PYDIR/python $MAINDIR/main.py $MAINDIR/config.ini "$1" >> $LOGDIR/$logfile 
#
echo Completed python insert/update
#
