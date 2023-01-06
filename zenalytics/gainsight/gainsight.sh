#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/gainsight/lib
LOGDIR=$HOME/snowflake/zenalytics/gainsight/log
HOMEDIR=$HOME/snowflake/zenalytics/gainsight
DATADIR=$HOME/snowflake/zenalytics/gainsight/data
PYDIR=/home/anaconda3/bin
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=gainsight_data_loader_$logfile_ext.log
errorfile=gainsight_data_loader.err
datafile=gainsight_output_file.csv
whname=ZENLOADER
dbname=ZENALYTICS
rolename=ETL_PROD_ROLE
schemaname=PUBLIC
tablename=GAINSIGHT_ACCT_ATTRIBUTES
#
# clean up old outputs
#
rm -rf $DATADIR/*.csv
#
# 	List of sql scripts to process
#
file_list="
gainsight_create_metrics.sql
gainsight_extract.sql
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
#
# Create dynamic sql script
#
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!set output_format=csv;
!set header=true;
!set timing=false;
!set friendly=false;
!set results=true;
!set output_file=$LOGDIR/$errorfile;
!define dbname=$dbname;
!define whname=$whname;
!define schemaname=$schemaname;
!define rolename=$rolename;
!define tablename=$tablename;
!define datadir=$DATADIR;
!source $SQLDIR/$filename;
SQL
"
# run sql script to load each file in background
	eval "${SQLCMD}" > $LOGDIR/$logfile 
	echo Running SQL file $filename  ...
	else
		echo $filename does not exist ...................
	fi	
done
#
for csvfile in $DATADIR/*.csv; do
    [ -e "$filename" ] || continue
    echo Running gainsight_loader.py $filename >> $LOGDIR/$logfile 
    $PYDIR/python $HOMEDIR/gainsight_loader.py $HOMEDIR/config.json ACCOUNT $csvfile >> $LOGDIR/$logfile 
done
#
echo Completed
#
