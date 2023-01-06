#!bin/bash
MAINDIR=$HOME/snowflake/zenalytics/business_features/Foursquare/
PYDIR=/home/dataengg/anaconda3/bin
LOGDIR=$HOME/snowflake/zenalytics/business_features/Foursquare/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
#
#	setup environment
#
logfile=Foursquare_$logfile_ext.log
#
cd $MAINDIR
#
$PYDIR/python $MAINDIR/main.py $MAINDIR/config.ini "$1" >> $LOGDIR/$logfile
#
echo Completed Foursquare daily insert/update
#
