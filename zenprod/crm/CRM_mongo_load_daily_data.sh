#!bin/bash
SQLDIR=$HOME/snowflake/zenprod/crm/lib
LOGDIR=$HOME/snowflake/zenprod/crm/log
#
file_ext=json
logfile_ext=$(date +%Y%m%d_%H:%M:%S)
stagename=ZENPROD._STAGE.s3_mongo_stage
whname=ZENLOADER
dbname=ZENPROD
rolename=ETL_PROD_ROLE
schemaname=CRM
stageschema=_STAGE
#
# get latest load date
# [MB] added grep -v portal_businessprofile to exclude that folder from retrieved
load_date=$(aws s3 ls s3://zp-uw2-data-archives/mongo/ | grep -v portal_businessprofile | sort | tail -n 1 | awk '{print $2}'| cut -d'/' -f 1)
#
echo load date = $load_date
#
stagepath=$load_date
stageurl=s3://zp-uw2-data-archives/mongo
#
echo Staging From: $stageurl
#
# 	List of json files to process
#
file_list="
analytics_aggregatestats
analytics_collectionstats
analytics_messagelogstats
analytics_traffic
auth_user
gdpr_contact_blacklist
mobile_deviceregistrationlog
mobile_engagementlog
mobile_notificationdevice
mobile_notificationdevicelogin
models_accounttutorialcompletion
models_billingandsubscriptionprefs
models_businessbranding
models_messagestats
models_onboardingwizardpagecompletions
models_usertutorialcompletion
portal_accessdevice
portal_accessdeviceownership
portal_businessownership
portal_businessrelationship
portal_portaltermsprivacyconsent
portal_product
portal_productversion
portal_router
portal_routertype
portal_tosconsent
portal_userprofile
repmanagement_businessrating
repmanagement_settings
smbsite_activitylog
smbsite_blacklistedemail
smbsite_clickevent
smbsite_clicklog
smbsite_defaulttrigger
smbsite_emailblast
smbsite_emailimport
smbsite_emailtemplate
smbsite_emailwhitelist
smbsite_gatekeeper
smbsite_gatekeeperentry
";
#analytics_customer
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
	eval "${SQLCMD}" >> $LOGDIR/$logfile
#
# 	loop through files and load
#
for filename in $file_list;
do
	stagefile=$filename.$file_ext
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
!source $SQLDIR/$filename.sql;
SQL
"
# run sql script to load each file in background
# echo "${SQLCMD}" > $LOGDIR/$logfile
	eval "${SQLCMD}" > $LOGDIR/$logfile &
	else
		echo $stagefile does not exist
	fi
done # filename loop
#
