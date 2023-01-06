#!bin/bash
MAINDIR=$HOME/snowflake/zenalytics/business_features/Yelp/
PYDIR=$HOME/anaconda3/bin
LOGDIR=$HOME/snowflake/zenalytics/business_features/Yelp/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
#
#	setup environment
#
logfile=Yelp_$logfile_ext.log
#
cd $MAINDIR
#
$PYDIR/python $MAINDIR/main.py $MAINDIR/config.ini "$1" >> $LOGDIR/$logfile
#
echo Completed Yelp daily insert/update
#
