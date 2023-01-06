#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/ads/response/lib
LOGDIR=$HOME/snowflake/zenalytics/ads/response/log
#
file_ext=sql
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
logfile=ADS_load_daily_campaign_metrics_data_$logfile_ext.log
#
dbname=ZENALYTICS
whname=ADSLOADER
rolename=ETL_PROD_ROLE
schemaname=ADS_CAMPAIGNS
#
#       Summary tables
#
cd $SQLDIR;
#
CAMPAIGN_METRICS="
snowsql << SQL;
!set variable_substitution=true;
!set output_file=$LOGDIR/campaign_metrics_$logfile_ext.log;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=ADS_CAMPAIGNS;
!source $SQLDIR/campaign_metrics.sql;
SQL
"
#
AD_METRICS="
snowsql << SQL;
!set variable_substitution=true;
!set output_file=$LOGDIR/ad_metrics_$logfile_ext.log;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=ADS_CAMPAIGNS;
!source $SQLDIR/ad_metrics.sql;
SQL
"
#
# run sql script to load each file in background
        eval "${CAMPAIGN_METRICS}" >> $LOGDIR/$logfile
        eval "${AD_METRICS}" >> $LOGDIR/$logfile
#
echo Completed
#
