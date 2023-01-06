#!bin/bash
SQLDIR=$HOME/snowflake/zenprod/crm/lib
LOGDIR=$HOME/snowflake/zenprod/crm/log
#
filetag=00000
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
stagename=ZENPROD._STAGE.s3_mongo2_stage
whname=ZENLOADER
dbname=ZENPROD
rolename=ETL_PROD_ROLE
schemaname=CRM
stageschema=_STAGE
sqlscript=smbsite_messagelog.sql
#
# get latest load date
load_date=$(aws s3 ls s3://zp-uw2-data-archives/mongo2/inc/ | sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
#
echo load date = $load_date
#
stageurl=s3://zp-uw2-data-archives/mongo2
stagepath=inc/$load_date
#
echo Staging From: $stageurl
#
# 	List of json files to process
#
file_list=" 
$(aws s3 ls s3://zp-uw2-data-archives/mongo2/inc/$load_date/ | awk '{print $4}')
";
#
#	setup environment
#
cd $SQLDIR;
#
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!set echo=true;
!set quiet=false;
!define stagename=$stagename;
!define stageurl=$stageurl;
!define dbname=$dbname;
!define whname=$whname;
!define rolename=$rolename;
!define stageschema=$stageschema;
!source $SQLDIR/initialize_mongo_load.sql;
SQL
"
logfile=initialize_mongo_load.log
	#echo "${SQLCMD}" > $LOGDIR/$logfile 
	eval "${SQLCMD}" >> $LOGDIR/$logfile 
#
# 	loop through files and load
#
for filename in $file_list;
do
	stagefile=$filename
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
# Create dynamic sql script
SQLCMD="
snowsql << SQL;
!set variable_substitution=true;
!set echo=true;
!set quiet=false;
!define stagename=$stagename;
!define asof_date=$load_date;
!define stagepath=$stagepath;
!define dbname=$dbname;
!define whname=$whname;
!define schemaname=$schemaname;
!define stageschema=$stageschema;
!define rolename=$rolename;
!source $SQLDIR/$sqlscript;
SQL
"
# run sql script to load each file in background
	#echo "${SQLCMD}" > $LOGDIR/$logfile 
	eval "${SQLCMD}" >> $LOGDIR/$logfile 
	else
		echo $stagefile does not exist
	fi	
done # filename loop
#
echo Completed
#
