#
# Changelog: [5/29, MB] Added reclassified_presence_sampling_stats.sql (daily recreate) to script
#
#!bin/bash
SQLDIR=$HOME/snowflake/zenalytics/presence/main/lib
LOGDIR=$HOME/snowflake/zenalytics/presence/main/log
#
file_ext=sql
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
#
dbname=ZENALYTICS
whname=ZENLOADER
rolename=ETL_PROD_ROLE
schemaname=PRESENCE
srcschemaname=AUDIENCES
#
#       Summary table
#
cd $SQLDIR;
#
logfile=presence_sampling_stats_daily_$logfile_ext.log
#
PRESENCE_SAMPLING_STATS="
snowsql << SQL;
!set variable_substitution=true;
!set output_file=$LOGDIR/$logfile;
!define dbname=$dbname;
!define schemaname=PRESENCE;
!define whname=$whname;
!define rolename=$rolename;
!source $SQLDIR/presence_sampling_stats_incremental.sql;
SQL
"
#
# run sql script to load PRESENCE_SAMPLING_STATS in background
        eval "${PRESENCE_SAMPLING_STATS}" > $LOGDIR/$logfile
#
echo Completed Presence Sampling Stats
#
logfile=reclassified_presence_sampling_stats_daily_$logfile_ext.log
#
RECLASSIFIED_PRESENCE_SAMPLING_STATS="
snowsql << SQL;
!set variable_substitution=true;
!set output_file=$LOGDIR/$logfile;
!define dbname=$dbname;
!define schemaname=PRESENCE;
!define whname=$whname;
!define rolename=$rolename;
!source $SQLDIR/reclassified_presence_sampling_stats.sql;
SQL
"
#
# run sql script to rebuild RECLASSIFIED_PRESENCE_SAMPLING_STATS in background
eval "${RECLASSIFIED_PRESENCE_SAMPLING_STATS}" >> $LOGDIR/$logfile
#
echo Completed Reclassified Presence Sampling Stats
#
# this table does not have the data corrections for 4/6
#
RECLASSIFIED_PRESENCE_SAMPLING_STATS_DEMOGRAPHICS="
snowsql << SQL;
!set variable_substitution=true;
!set output_file=$LOGDIR/$logfile;
!define dbname=$dbname;
!define schemaname=PRESENCE;
!define srcschemaname=AUDIENCES;
!define whname=$whname;
!define rolename=$rolename;
!source $SQLDIR/reclassified_presence_sampling_stats_demographics.sql;
SQL
"
#
# run sql script to rebuild RECLASSIFIED_PRESENCE_SAMPLING_STATS_DEMOGRAPHICS in background
        eval "${RECLASSIFIED_PRESENCE_SAMPLING_STATS_DEMOGRAPHICS}" >> $LOGDIR/$logfile
#
echo Completed Reclassified Presence Sampling Stats - demographics
#
#
logfile=portal_events_smry_$logfile_ext.log
#
PORTAL_EVENTS_SMRY="
snowsql << SQL;
!set variable_substitution=true;
!set output_file=$LOGDIR/$logfile;
!define dbname=$dbname;
!define schemaname=PRESENCE;
!define whname=$whname;
!define rolename=$rolename;
!source $SQLDIR/portal_events_smry.sql;
SQL
"
#
# run sql script to rebuild RECLASSIFIED_PRESENCE_SAMPLING_STATS in background
eval "${PORTAL_EVENTS_SMRY}" >> $LOGDIR/$logfile
#
echo Completed Portal Events Summary
#
