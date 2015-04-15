/*--===========================================================================================================
PIG Script for classification of users as either primarily residential or primarily commercial buyers / browsers

AUTHOR: Wolf Rendall, wrendall@auction.com, rendall.wolf@gmail.com
DATE:	2014-04-03
NOTES:	This script is meant to gather the entire, comprehensive list of user actions and assign them a 1 or 0 for residential or commericial
		THIS SCRIPT IS STEP 1 IN THE RECOMMENDATION ENGINE (PROJECT: BECK) PROCESS
				
								***********************SECTIONS
								1:	USER EMAILS AND USERIDS FROM NOTIFICATIONS AND REGISTRATIONS
								2:	JOINING OMNITURE DATA FOR PAGE BROWSING
								3:	JOINING FAVORITES
								4:	JOINING ALERTS
								5:	JOINING BIDS AND PURCHASES

								---=====*OUTFILE DEFINITION*=====---					
								action_list.csv USING PigStorage(',')
								
								************************DEPENDENCIES
								1: None except Hadoop Tables
--===========================================================================================================
CHANGE DATE 					CHANGED BY 						CHANGE DESCRIPTION		



--===========================================================================================================	
--SET VARIABLES AND IMPORT FUNCTIONS
--===========================================================================================================
*/

--WE NEED SOME ADDITIONAL OPERATIONS SUPPLIED BY PIGGYBANK
-- REGISTER /usr/lib/pig/piggybank.jar ; --NOT NEEDED IN OOZIE
-- REGISTER /usr/lib/pig/dp-udfs.jar ; -- NOT NEEDED IN OOZIE

DEFINE SQRT org.apache.pig.piggybank.evaluation.math.SQRT();
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE DATE_TIME org.apache.pig.piggybank.evaluation.datetime.DATE_TIME();
DEFINE FORMAT_DT org.apache.pig.piggybank.evaluation.datetime.FORMAT_DT();
DEFINE FORMAT org.apache.pig.piggybank.evaluation.string.FORMAT();


--DATE USAGE: export shell_dt=`date +"%F %T.%N"`| pig -params date=$shell_dt RecoEnginePig_AssetSort_NoDate.pig -stop_on_failure
%default now `date +"%s"`;
%default TARGET_FILE '/apps/analysis/user_notes_preference'

/*
--===============================================================================================
-- GATHER USER INFORMATION
-- Email Addresses and users' address or notification state are key
--===============================================================================================
*/

UUID_Data = LOAD 'derived.omniture_visitor_userid_uuid' USING org.apache.hcatalog.pig.HCatLoader();
Reg_Data = LOAD 'clean.registrations' USING org.apache.hcatalog.pig.HCatLoader();
Notif_Data = LOAD 'clean.emailmarketing' USING org.apache.hcatalog.pig.HCatLoader();

-- For all these groups, you need to isolate their email address and their user id
Reg_Data_Gen = FOREACH Reg_Data GENERATE 
LOWER(email) as email, 
user_id as user_id;

Notif_Data_Gen = FOREACH Notif_Data GENERATE
LOWER(email) as email,
'Unregistered' as user_id;

UUID_Data_Gen = FOREACH UUID_Data GENERATE 
uuid
,user_id;

Email_UUID_Join = JOIN Reg_Data_Gen BY user_id, UUID_Data_Gen BY user_id;

userid_map = FOREACH Email_UUID_Join GENERATE
UUID_Data_Gen::user_id as user_id
,UUID_Data_Gen::uuid as uuid
,LOWER(email) as email;

/*
--===============================================================================================
--GATHER DATA SETS FOR BIDS AND BROWSING
--Bids and browsing of commercial vs residential property is the basis for classifing users
--===============================================================================================
*/
pdp_data_load = LOAD 'derived.omniture_pdp' USING org.apache.hcatalog.pig.HCatLoader();
pdp_data_filter = FILTER pdp_data_load BY (int)datestamp > 20140101;
pdp_data_gen = FOREACH pdp_data_filter GENERATE
visitor_uuid as uuid
,user_id
,LOWER(product_type_url) AS product_type
,1.0 AS pdp_views
,CustomFormatToISO(datestamp, 'YYYYmmdd') AS ISO_dt;

pdp_data = FOREACH pdp_data_gen GENERATE
uuid
,user_id
,product_type
,pdp_views
,ISOToUnix(ISO_dt) as UNIX_dt;


bid_data_load = LOAD 'derived.bids_live' USING org.apache.hcatalog.pig.HCatLoader();

bid_data_filter = FILTER bid_data_load BY auto_seller_bidding == 0;

bid_data_gen = FOREACH bid_data_filter GENERATE
user_id
,globalpropid
,CustomFormatToISO(datestamp, 'YYYYmmdd') AS ISO_dt
,bidamount
,derived_product
,LOWER(product_type) AS product_type;

bid_data_join = JOIN bid_data_gen BY user_id, UUID_Data_Gen BY user_id;

bid_data = FOREACH bid_data_join GENERATE
uuid
,UUID_Data_Gen::user_id
,globalpropid
,ISOToUnix(ISO_dt) AS UNIX_dt
,bidamount
,derived_product
,LOWER(product_type) AS product_type;




--PDP Views Assignment and Weighting
notes_views = FILTER pdp_data BY (product_type MATCHES '.*note.*');
non_note_commercial_views = FILTER pdp_data BY (product_type MATCHES '.*comm.*' AND NOT product_type MATCHES '.*note.*');

weighted_notes_views = FOREACH notes_views GENERATE
uuid
,user_id
,(1.0 - ( ($now - UNIX_dt) / $now ) ) * pdp_views AS weighted_action
,pdp_views AS notes_views
,0.0 AS notes_bids;

weighted_comm_views = FOREACH non_note_commercial_views GENERATE
uuid
,user_id
,(1.0 - ( ($now - UNIX_dt) / $now ) )*pdp_views AS weighted_action
,pdp_views as comm_views
,0.0 AS comm_bids;

--Bids Assignment and Weighting, One Bid is worth 10 views
notes_bids = FILTER bid_data BY (product_type MATCHES '.*note.*');
non_note_commercial_bids = FILTER bid_data BY (product_type MATCHES '.*comm.*' AND NOT product_type MATCHES '.*note.*');

weighted_notes_bids = FOREACH notes_bids GENERATE
uuid
,user_id
,(1 - ( ($now - UNIX_dt) / $now ) )*10.0 AS weighted_action
,0.0 AS notes_views
,1.0 AS notes_bids;

weighted_comm_bids = FOREACH non_note_commercial_bids GENERATE
uuid
,user_id
,(1.0 - ( ($now - UNIX_dt) / $now ) )*10.0 AS weighted_action
,0.0 AS comm_views
,1.0 AS comm_bids;

--===============================================================================================
--COLLECT ALL THE ABOVE CRITERIA AND CREATE FINAL USER SCORES
--
--===============================================================================================


notes_union = UNION weighted_notes_bids, weighted_notes_views;
other_comm_union = UNION weighted_comm_bids, weighted_comm_views;

notes_group = GROUP notes_union BY (uuid,user_id);
notes_score = FOREACH notes_group GENERATE 
FLATTEN(group) as (uuid, user_id)
,SUM(notes_union.weighted_action) AS notes_actions
,SUM(notes_union.notes_views) AS notes_views
,SUM(notes_union.notes_bids) AS notes_bids;

other_comm_group = GROUP other_comm_union BY (uuid,user_id);
other_comm_score = FOREACH other_comm_group GENERATE 
FLATTEN(group) as (uuid, user_id)
,SUM(other_comm_union.weighted_action) AS comm_actions
,SUM(other_comm_union.comm_views) AS comm_views
,SUM(other_comm_union.comm_bids) AS comm_bids;

both_join = JOIN notes_score BY uuid LEFT, other_comm_score BY uuid;
both_score = FOREACH both_join GENERATE
notes_score::uuid AS uuid
,notes_score::user_id AS user_id
,notes_score::notes_actions AS notes_actions
,other_comm_score::comm_actions AS comm_actions
,notes_score::notes_bids AS notes_bids
,other_comm_score::comm_bids AS comm_bids
,notes_score::notes_views AS notes_views
,other_comm_score::comm_views AS comm_views;


both_score_clean = FOREACH both_score GENERATE
uuid AS uuid
,user_id AS user_id
,(notes_actions IS NULL?0.0:notes_actions ) AS notes_actions
,(comm_actions IS NULL?0.0:comm_actions ) AS comm_actions
,(notes_bids IS NULL?0.0:notes_bids ) AS notes_bids
,(comm_bids IS NULL?0.0:comm_bids ) AS comm_bids
,(notes_views IS NULL?0.0:notes_views ) AS notes_views
,(comm_views IS NULL?0.0:comm_views ) AS comm_views;

complete_crit = FOREACH both_score_clean GENERATE
uuid
,user_id
,notes_actions / (comm_actions + notes_actions) AS notes_preference
,notes_bids
,comm_bids
,notes_views
,comm_views;

final_crit = FILTER complete_crit BY notes_preference IS NOT NULL ;

ordered_crit = ORDER final_crit BY notes_preference ASC;

join_email = JOIN ordered_crit BY user_id, userid_map by user_id;

out = FOREACH join_email GENERATE
 (chararray)ordered_crit::uuid as uuid
,(int) ordered_crit::user_id AS user_id
,(chararray)userid_map::email AS email
,(double) ordered_crit::notes_preference AS notes_preference
,(int) ordered_crit::notes_bids AS notes_bids
,(int) ordered_crit::comm_bids AS comm_bids
,(int) ordered_crit::notes_views AS notes_views
,(int) ordered_crit::comm_views AS comm_views;


--TEST = FILTER withemail BY notes_preference < 0.5;

rmf $TARGET_FILE
STORE out INTO '$TARGET_FILE' USING PigStorage('\\u001', '-schema');




/*
hcat -e "

use analysis;
drop table if exists analysis.user_notes_preference;
create external table analysis.user_notes_preference
(
uuid string,
user_id bigint,
email string,
notes_preference double,
notes_bids double,
comm_bids double, 
notes_views double, 
comm_views double
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  LINES TERMINATED BY '\n'
LOCATION
  'hdfs://hmas01lax01us:8020/apps/analysis/user_notes_preference';

"
*/
