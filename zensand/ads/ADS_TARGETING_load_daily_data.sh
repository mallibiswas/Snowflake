#!bin/bash
SQLDIR=$HOME/snowflake/zensand/ads/lib
LOGDIR=$HOME/snowflake/zensand/ads/log
#
file_ext=sql
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
#
#
filename=audience_visit_agg
logfile=AUDIENCE_VISIT_AGG_$logfile_ext.log
errorfile=$filename.err
#
# setup snowsql configuration
#
SQLCMD="
snowsql 2>$LOGDIR/$errorfile << SQL
!set variable_substitution=true;
!set output_file=$LOGDIR/$logfile;
!define whname=ADSERVICES;
!define rolename=ZDSERVICE_ROLE;
!define sourcedbname=ZENSAND;
!define targetdbname=ZENSAND;
!define sourceschemaname=PRESENCE;
!define targetschemaname=ADS;
!define targettablename=AUDIENCE_VISIT_AGG;
!define sourcetablename=WIFI_CONSENTED_SIGHTINGS;
!source $SQLDIR/$filename.$file_ext;
SQL
"
#
echo processing job $filename
#
eval "${SQLCMD}" >> $LOGDIR/$logfile
#
echo Completed
