#!bin/bash
SCRIPTDIR=$HOME/snowflake/zenalytics/cloudtrax_openmesh/main
PYDIR=/home/anaconda3/bin
LOGDIR=$SCRIPTDIR
#
file_ext=py
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
#
cd $SCRIPTDIR
#
# main.py inputs: configuration file and decryption key
#
$PYDIR/python $SCRIPTDIR/main.py $SCRIPTDIR/config.ini "$1" > /dev/null 
#
echo Completed
#
