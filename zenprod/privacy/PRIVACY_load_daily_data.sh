#!bin/bash
SQLDIR=$HOME/snowflake/zenprod/privacy/lib
LOGDIR=$HOME/snowflake/zenprod/privacy/log
#
filetag=00000
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
errorfile=privacy_errors.log
stagename=ZENPROD._STAGE.s3_privacy_stage
whname=ZENLOADER
dbname=ZENPROD
rolename=ETL_PROD_ROLE
schemaname=PRIVACY
stageschema=_STAGE
#
# get latest load date
load_date=$(aws s3 ls s3://zp-uw2-data-archives/rds/privacy/ | sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
#
echo load date = $load_date
#
stageurl=s3://zp-uw2-data-archives/rds/privacy
stagepath=$load_date
#
echo Staging From: $stageurl
#
# 	List of json files to process
#
file_list="
privacy_request
privacy_request_response
";
#
#	setup environment
#
cd $SQLDIR;
#
SQLCMD="
snowsql 2>$errorfile << SQL;
!set variable_substitution=true;
!set echo=true;
!set quiet=false;
!define stagename=$stagename;
!define stageurl=$stageurl;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define stageschema=$stageschema;
!source $SQLDIR/initialize_privacy_load.sql;
SQL
"
	logfile=initialize_mongo_load.log
#	echo "${SQLCMD}" > $LOGDIR/$logfile 
	eval "${SQLCMD}" >> $LOGDIR/initialize_privacy_load.log 
#
# 	loop through files and load
#
for filename in $file_list;
do
	stagefile=$filename$filetag.$file_ext
	logfile=$filename.log
	echo checking $stageurl/$stagepath/$stagefile
	aws s3 ls $stageurl/$stagepath/$stagefile;
	if [[ $? = 0 ]]; then
		echo ############################### 
		echo file $stagefile exists, loading
		echo stagename=$stagename;
		echo stageurl=$stageurl;
		echo stagepath=$stagepath;
		echo file=$filename;
		echo ############################## 
# Create dynamics sql script
SQLCMD="
snowsql 2>>$errorfile << SQL;
!set variable_substitution=true;
!set echo=true;
!set quiet=false;
!define asof_date=$load_date;
!define stagename=$stagename;
!define stagepath=$stagepath;
!define stagefile=$stagefile;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define schemaname=$schemaname;
!define stageschema=$stageschema;
!source $SQLDIR/$filename.sql;
SQL
"
# run sql script to load each file in background
# echo "${SQLCMD}" > $LOGDIR/$logfile 
	eval "${SQLCMD}" >> $LOGDIR/$filename\_$logfile_ext.log 
	else
		echo $stagefile does not exist
	fi	
done # filename loop
#
echo Completed
#
