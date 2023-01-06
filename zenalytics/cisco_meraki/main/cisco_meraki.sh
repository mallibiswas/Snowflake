#!bin/bash
SCRIPTDIR=$HOME/snowflake/zenalytics/cisco_meraki/main
PYDIR=/home/anaconda3/bin
LOGDIR=$SCRIPTDIR/log
#
file_ext=py
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
#
cd $SCRIPTDIR
#
$PYDIR/python $SCRIPTDIR/main.py $SCRIPTDIR/config.ini "$1" > /dev/null 
#
echo Completed
#
