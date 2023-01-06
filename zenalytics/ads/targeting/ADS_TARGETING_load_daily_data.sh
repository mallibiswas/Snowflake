#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/ads/targeting/lib
LOGDIR=$HOME/snowflake/zenalytics/ads/targeting/log
#
file_ext=sql
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
#
#
filename=audience_visit_agg
logfile=$filename\_$logfile_ext.log
errorfile=$filename.err
#
# setup snowsql configuration
#
snowsql 2>$LOGDIR/$errorfile << SQL
!set variable_substitution=true;
!set output_file=$LOGDIR/$logfile;
!define whname=ZENLOADER;
!define rolename=ETL_PROD_ROLE;
!define sourcedbname=ZENPROD;
!define targetdbname=ZENALYTICS;
!define sourceschemaname=PRESENCE;
!define targetschemaname=ADS;
!define targettablename=AUDIENCE_VISIT_AGG;
!define sourcetablename=WIFI_CONSENTED_SIGHTINGS;
!source $SQLDIR/$filename.sql;
SQL
#
echo processing file $filename
#
echo Completed
