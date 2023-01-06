#!bin/bash
SQLDIR=$HOME/snowflake/zenshare/lib
LOGDIR=$HOME/snowflake/zenshare/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=zenshare_refresh_daily_data_$logfile_ext.log
errorfile=zenshare_refresh_daily_data.err
#
whname=ZENLOADER
rolename=ETl_PROD_ROLE
#
sourcedb=ZENALYTICS
sourceschema=PRESENCE
#
targetdb=ZENSHARE
targetschema=MAIN
#
analyticsdb=ZENALYTICS
analyticsschema=CRM
#
proddb=ZENPROD
prodschema=CRM
#
# 	List of sql scripts to process
#
file_list="
zenshare_main_objects.sql
customer_profile.sql
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
SQLCMD="
snowsql -o log_level=ERROR -o log_file=$LOGDIR/$logfile 2>$LOGDIR/$errorfile << SQL;
!set variable_substitution=true;
!define whname=$whname;
!define rolename=$rolename;
!define sourcedb=$sourcedb;
!define sourceschema=$sourceschema;
!define proddb=$proddb;
!define prodschema=$prodschema;
!define targetdb=$targetdb;
!define targetschema=$targetschema;
!define analyticsdb=$analyticsdb;
!define analyticsschema=$analyticsschema;
!source $SQLDIR/$filename;
SQL
"
# run sql script to load each file in background
	eval "${SQLCMD}" >> $LOGDIR/$logfile
	else
		echo $filename does not exist ...................
	fi
done
#
echo Completed
#
