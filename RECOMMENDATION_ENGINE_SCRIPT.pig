/*--===========================================================================================================
Pig Script for finding best assets for direct mail eligible users.
NOTE: user must define $event_key_1 and $event_key_2 for successful operation OR 


AUTHOR: Wolf Rendall, wrendall@auction.com, rendall.wolf@gmail.com, 951-809-2241
DATE:	2015-03-14
		
				***********************SECTIONS
				1:	IMPORT FUNCTIONS AND PARAMETERS FROM SHELL
				2:	IMPORT PROPERTY DATA, INCL BID END DATE TIME
				3:	GET TIME REMAINING FOR AUCTION, RETURN ONLY COMPLETE DATA ENDING IN WINDOW
				4:	IMPORT PREVIOUSLY CONSOLIDATED USER DATA
				5:	FIND DISTANCE 
    				FROM: USER'S <CENTROID, RESERVE PREFERENCE, TYPE PREFERENCE, NOTES PREFERENCE> VECTOR 
   					TO:   PROPERTY'S <LAT/LON, RESERVE, PROPERTY TYPE, PRODUCT TYPE> VECTOR
				6:	RANK BY DISTANCE
				7:	STORE

				************************DEPENDENCIES
				1: beck_user_data_commercial_union.pig
				2: beck_direct_mail_user_data_consolidation_commercial.pig

--===========================================================================================================
CHANGE DATE 					CHANGED BY 						CHANGE DESCRIPTION		
2015-03-14						Wolf rendall 					Init Release

--===========================================================================================================	

--===========================================================================================================*/

--SET PIG OPTIONS IMPROVING THE PERFOMRANCE OR FILE SIZES

SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;
SET pig.maxCombinedSplitSize 67108864; --64MB
SET mapreduce.reduce.input.limit -1;   

--Need to ensure that the output file is compressed (Gzip)
SET output.compression.enabled true;
SET output.compression.codec org.apache.hadoop.io.compress.GzipCodec

--SET DEFAULT PARALLELISM FOR FASTER EXECUTION
--SET  default_parallel 12;

--DATE USAGE: export shell_dt=`date +"%F %T.%N"`| pig -params date=$shell_dt RecoEnginePig_AssetSort_NoDate.pig -stop_on_failure
%default now `date +"%s"`;
%default min_time_to_close '1036800'; -- 60*60*24*12; Auction ends no shorter than 12 days in the future 
%default max_time_to_close '3888000'; --60*60*24*45; Auction ends no longer than 45 days in the future 
%default min_time_to_start '-864000'; -- 60*60*24*(-10); Auction started no longer than 10 days in the past
%default num_properties '3';
%default store_database 'analysis';
%default event_key_1 'B-152';
%default event_key_2 'N-152';
%default store_table 'per_uuid_direct_mail_commercial';
%default table_directory '/apps/$store_database/$store_table';
%default asset_type_index '3';
%default reserve_index '4';
%default product_type_index '6';
%default asset_type_max_penalty '100.0';
%default notes_max_penalty '500.0';
%default reserve_max_penalty '100.0';
%default reserve_scale_parameter '2.0' --APPROACHES THE MAX X.0 AS QUICKLY
%default temp_store_directory '/apps/analysis/temporary_beck/bidder_data_comm_direct_mail';

--FIRST, WE NEED SOME ADDITIONAL OPERATIONS SUPPLIED BY PIGGYBANK OR ANOTHER GEOPROCESSING SERVICE
--REGISTER /usr/lib/pig/piggybank.jar ; -- NOT NEEDED IN OOZIE
--REGISTER /usr/lib/pig/dp-udfs.jar ; -- NOT NEEDED IN OOZIE
--REGISTER /home/wrendall/dp-udfs-1.0-SNAPSHOT.jar ; -- SANDBOX VERSION, DOESN'T NEED UPDATE FROM JENKINS IF IMPORTED LOCALLY

--DEFINE USEFUL FUNCTIONS
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE TopNFromBag1ByBag2 com.auction.data.hadoop.pig.udf.TopNFromBag1ByBag2('$num_properties,true');
DEFINE NTermEuclideanDistance com.auction.data.hadoop.pig.udf.NTermEuclideanDistance;
DEFINE UserAssetTypeMatch com.auction.data.hadoop.pig.udf.UserAssetTypeMatch;
DEFINE UserAssetReserveMatch com.auction.data.hadoop.pig.udf.UserAssetReserveMatch;
DEFINE HaversineDistanceMiles com.auction.data.hadoop.pig.udf.HaversineDistanceMatch('1,2');
DEFINE UserNotesPreference com.auction.data.hadoop.pig.udf.UserNotesPreference;
DEFINE IfNull com.auction.data.hadoop.pig.udf.NullOrX();
DEFINE LENGTH org.apache.pig.piggybank.evaluation.string.LENGTH();
DEFINE GetPropertyURL com.auction.data.hadoop.pig.udf.GetPropertyURLMap();

--VARIABLES THAT WILL SHOW UP IN THE FILTERING CODE
--THESE MAY NEED TO BE CHANGED TO INCORPORATE DESIRED BUSINESS LOGIC
--time_stamp = LOCAL_DATE_TIME();
--time_stamp = CustomFormatToISO('2014-03-14 08:00:00', 'YYYY-MM-dd HH:mm:ss');
	
--IMPORT THE FULL LIST OF PROPERTIES ON AUCTION.COM
raw_property_data = LOAD 'clean.property' USING org.apache.hcatalog.pig.HCatLoader();

-- GET HIGHEST NUMBER CRE AUCTIONS CLOSING IN 28 Days

raw_auctions = LOAD 'derived.property' USING org.apache.hcatalog.pig.HCatLoader();
gen_auctions = FOREACH raw_auctions GENERATE
auction_id
,auction_number
,bid_end_dt
,auction_date
,LOWER(property_source) AS property_source
,LOWER(is_test) AS is_test;

filter_auctions = FILTER gen_auctions BY (
  is_test == 'n' AND (
    property_source == 'cre' OR property_source == 'notes') AND (
    auction_number MATCHES '[BNbn]-[0-9]*') AND (
    bid_end_dt MATCHES '.*[0-9].*' OR auction_date MATCHES '.*[0-9].*'
    AND (bid_end_dt != '')
    AND (bid_end_dt IS NOT NULL)
    AND (auction_date != '')
    AND (auction_date IS NOT NULL))
  );

fix_auctions = FOREACH filter_auctions GENERATE
auction_id
,auction_number
,CustomFormatToISO(bid_end_dt, 'YYYY-MM-dd HH:mm:ss.s') AS ISOBidEndDt
,property_source
,is_test
,CustomFormatToISO(auction_date, 'MM/dd/YYYY') AS ISOAuctionDate;

date_auctions = FOREACH fix_auctions GENERATE
auction_id
,auction_number
,((ISOBidEndDt MATCHES '.*[0-9].*' AND 
  ISOBidEndDt IS NOT NULL AND LENGTH(ISOBidEndDt) > 5)? ISOBidEndDt:ISOAuctionDate) AS ISOTime;

UNIX_auctions = FOREACH date_auctions GENERATE
auction_id
,auction_number
,ISOTime
,(ISOToUnix(ISOTime)/1000) - $now AS time_to_close;

threshold_auctions = FILTER UNIX_auctions BY (
  time_to_close < $max_time_to_close
  AND time_to_close > $min_time_to_close
  );

date_group = GROUP threshold_auctions BY auction_number;
date_stats = FOREACH date_group GENERATE
group AS auction_number
,MAX(threshold_auctions.ISOTime) AS max_date;

event_number = FOREACH date_stats GENERATE 
auction_number
,(int)REGEX_EXTRACT(auction_number, '[BNbn]-([0-9]*)',1) AS code_number
,max_date;

--DUMP event_number;

event_group = GROUP event_number ALL;

event_selector = FOREACH event_group GENERATE 
MAX(event_number.code_number) AS highest_code;

selected_events = JOIN event_number BY code_number, event_selector BY highest_code;



--PER BUSINESS RULES IDENTIFY ELIGIBLE PROPERTIES THAT ARE PART OF PREDETERMINED AUCTION CYCLE
active_comm_prop = FILTER raw_property_data BY (test_property == 'N' AND statuscsv == 'Auction' AND auctiondaystatus == 2 
	AND (LOWER(property_source) == 'cre' OR LOWER(property_source) == 'notes'))
 	AND (bidenddt != '')
	AND (bidenddt IS NOT NULL)
	AND (bidstartdt != '')
	AND (bidstartdt IS NOT NULL);

selected_comm_prop = JOIN selected_events BY auction_number, active_comm_prop by auction_number;

--NOW GENERATE THE IMPORTANT FIELDS
comm_prop_gen = FOREACH selected_comm_prop GENERATE
active_comm_prop::globalpropid as globalpropertyid
,active_comm_prop::propertyzip as zip
,active_comm_prop::latitude as lat
,active_comm_prop::longitude as lng
,active_comm_prop::property_auctionid as auctionid
,active_comm_prop::auction_number as auction_number
,active_comm_prop::product_type as product_type
,active_comm_prop::propertytype as property_type
,active_comm_prop::propertycity as property_city
,active_comm_prop::propertystate as property_state
,active_comm_prop::auctionsellerreserve as reserve
,active_comm_prop::bidenddt as bidenddt
,active_comm_prop::bidstartdt as bidstartdt
,active_comm_prop::startingbid as startingbid
,active_comm_prop::property_source as property_source
,(( LOWER(IfNull(active_comm_prop::financingavailable, 'no')) MATCHES 'yes')?1:0) as financing_available;

--Comment Intentionally Left Blank
comm_prop_ends = FILTER comm_prop_gen BY bidenddt MATCHES '.*[0-9].*';

--JOIN ON MELISSA'S LAT/LON FOR THE ZIP CODE OF ASSETS WITHOUT A LAT/LON
raw_Melissa = LOAD 'raw.melissadata_zip' using org.apache.hcatalog.pig.HCatLoader();
zip_Melissa = FOREACH raw_Melissa GENERATE 
zip as zip
,state as state
,lat as lat
,lng as lng;

needs_mapped = FILTER comm_prop_ends BY lat == 0.0;

melissa_prop = JOIN needs_mapped BY property_zip, zip_Melissa BY zip;
melissa_prop = FOREACH melissa_prop GENERATE
globalpropertyid
,zip_Melissa::zip as zip
,zip_Melissa::lat as lat
,zip_Melissa::lng as lng
,zip_Melissa::state as property_state
,product_type
,property_type
,property_city
,auctionid as auctionid
,auction_number as auction_number
,reserve
,bidenddt
,bidstartdt
,startingbid
,property_source as property_source
,financing_available as financing_available;

union_prop = UNION ONSCHEMA melissa_prop, comm_prop_ends;
union_prop = DISTINCT union_prop;

--FILTER EARLY AND OFTEN
--THIS FILTER REMOVES CLOSED AND CLOSING-SOON AUCTIONS
property_data = FILTER union_prop BY lat != 0.0;

--CONVERT BID TIME TO UNIX TIME
property_data = FOREACH property_data GENERATE
globalpropertyid
,lat as prop_lat
,lng as prop_lon
,property_state
,property_type
,product_type
,bidenddt
,bidstartdt
,startingbid
,property_city
,auctionid as auctionid
,auction_number as auction_number
,reserve
,CustomFormatToISO(bidstartdt, 'YYYY-MM-dd HH:mm:ss.s') AS ISOStartTime
,CustomFormatToISO(bidenddt, 'YYYY-MM-dd HH:mm:ss.s') AS ISOEndTime
,financing_available;

property_data = FILTER property_data BY (NOT ISOStartTime IS NULL AND NOT ISOEndTime IS NULL);

property_data = FOREACH property_data GENERATE
globalpropertyid
,prop_lat
,prop_lon
,property_state
,product_type
,property_type
,bidenddt
,bidstartdt
,startingbid
,property_city
,auctionid as auctionid
,auction_number as auction_number
,reserve
,ISOStartTime
,ISOEndTime
,ISOToUnix(ISOStartTime) as UNIXTimeBidStartDt
,ISOToUnix(ISOEndTime) as UNIXTimeBidEndDt
,(ISOToUnix(ISOStartTime)/1000) - $now AS time_to_start
,(ISOToUnix(ISOEndTime)/1000) - $now AS time_to_close
,financing_available;

--FILTER EARLY AND OFTEN
--THIS FILTER REMOVES CLOSED AND CLOSING-SOON AUCTIONS
--working_set = FILTER property_data BY (time_to_close > $min_time_to_close) AND (time_to_close < $max_time_to_close) AND (time_to_start > $min_time_to_start);
working_set = property_data;

working_set_query = FOREACH working_set GENERATE 
GetPropertyURL(globalpropertyid, auction_number) AS kv;

working_set_result = FOREACH working_set_query GENERATE
FLATTEN($0);

working_set_flattened = FOREACH working_set_result GENERATE 
FLATTEN($0);

working_set_map = FOREACH working_set_flattened GENERATE 
TOMAP(*);

available_props = FOREACH working_set_map GENERATE 
$0#'global_property_id' AS global_property_id
, $0#'auction_number' AS auction_num
, $0#'thumbnail' AS thumbnail;

working_join = JOIN working_set BY (globalpropertyid, auction_number), available_props BY (global_property_id,auction_num);

/*
AS OF 8/29 WE NO LONGER GENERATE NOTE-SPECIFIC RECOMMENDATIONS,.
PREVIOUSLY, WE GENERATE A NOTES SET AND NON-NOTES SET AND RAN THE ENGINE FOR BOTH CASES
INSTEAD, WE INCLUDE A PARAMETER FOR USER NOTE PREFERENCES AND BOOST NOTES ACCORDINGLY.

non_notes = FILTER working_set BY NOT product_type MATCHES '.*Note.*' ;
yes_notes = FILTER working_set BY product_type MATCHES '.*Note.*' ;
*/

--=========================================================================================================== 
-- END OF CLEANING AND SELECTION OPERATIONS FOR PROPERTIES. PACKAGE THEM FOR FINAL EXPORT.
-- ORDER OF FIELDS IS IMPORTANT. FUNCTIONS IN CALCULATION PHASE LOOK FOR SPECIFIC 0-INDEXED FIELDS.
-- YOU CAN SAFELY ADD NEW FIELDS TO THE END.
--=========================================================================================================== 

-- drop off the fields that were only used for the time filter
eligible_properties = FOREACH working_join GENERATE
globalpropertyid as globalpropertyid
,prop_lat as prop_lat
,prop_lon as prop_lon
,property_type as property_type
,reserve as reserve
,property_state as property_state
,product_type as product_type
,bidenddt as bidenddt
,bidstartdt as bidstartdt
,startingbid as startingbid
,property_city as property_city
,auctionid as auctionid
,auction_number as auction_number
,financing_available as financing_available
,thumbnail as thumbnail;

eligible_properties = FILTER eligible_properties BY NOT thumbnail MATCHES '.*not-available.*';

--GROUP FOR EXPORT TO THE SORTING UDF
grouped_props = GROUP eligible_properties ALL;















--====================================================================================================
--RELATE THE COMPLETE LIST OF UUIDs TO THE COMPLETE LIST OF VALID PROPERTIES
--BY USING CUSTOM FUNCTIONS, YOU CAN AVOID MATERIALIZING AND OPERATING ON A HUGE CARTESIAN JOIN OF ASSETS TO USERS.
--FIND THE DISTANCE FROM EACH PROPERTY TO EACH BECK-GENERATED USER CENTROID
--RETURN THE TOP N AS DEFINED IN THE DEFINITIONS SECTION
--====================================================================================================

loaded_bidder = LOAD '$temp_store_directory' USING PigStorage('\\u001', '-schema');

--NOW THE FUN, BUILD EACH COMPONENT DISTANCE
distance_collection = FOREACH loaded_bidder GENERATE 
uuid, 
user_id AS user_id,
firstname AS firstname,
lastname AS lastname,
email AS email,
address AS address,
address1 AS address1,
city AS city,
statename AS statename,
zipcode AS zipcode,
HaversineDistanceMiles(user_lat, user_lon, grouped_props.eligible_properties) AS distance_bag,
UserAssetTypeMatch(user_property_tuple, $asset_type_index, $asset_type_max_penalty, grouped_props.eligible_properties) AS type_match_bag,
UserNotesPreference(user_note_preference, $product_type_index, $notes_max_penalty, grouped_props.eligible_properties) AS notes_match_bag,
UserAssetReserveMatch(user_reserve_preference, $reserve_index, $reserve_max_penalty, $reserve_scale_parameter, grouped_props.eligible_properties) AS reserve_bag;

/*
PLACEHOLDER STUB FOR DISTANCE NORMALIZATION
EVENTUALLY, WE WANT EACH METRIC TO BE BETWEEN 0 AND 1, THEN WEIGHT THEM ACCORDING TO BUSINESS RULES
AFTER THAT, WE WANT TO CREATE USER-SPECIFIC SENSITIVITIES TO EACH COMPONENT.
THEREBY, DISTANCE, PRICE, OR ANOTHER  COULD BE DEPRIORITIZED IN THE FINAL DISTANCE CALCULATION, EVEN SET TO 0.0 ENTIRELY
*/

--CREATE THE FINAL DISTANCE CALCULATION OF EACH PROP TO EACH USER
distance_final_calc = FOREACH distance_collection GENERATE
uuid, 
user_id AS user_id,
firstname AS firstname,
lastname AS lastname,
email AS email,
address AS address,
address1 AS address1,
city AS city,
statename AS statename,
zipcode AS zipcode,
NTermEuclideanDistance(distance_bag, type_match_bag, notes_match_bag, reserve_bag) AS calculated_distances;

--SELECT THE 'CLOSEST' OR BEST [N] MATCHES FOR EACH USER BASED ON THE DISTANCES
topn = FOREACH distance_final_calc GENERATE
uuid,
user_id AS user_id,
firstname AS firstname,
lastname AS lastname,
email AS email,
address AS address,
address1 AS address1,
city AS city,
statename AS statename,
zipcode AS zipcode,
TopNFromBag1ByBag2(grouped_props.eligible_properties, calculated_distances) as flat_top;

topn_flat = FOREACH topn GENERATE 
uuid AS uuid,
user_id AS user_id,
firstname AS firstname,
lastname AS lastname,
email AS email,
address AS address,
address1 AS address1,
city AS city,
statename AS statename,
zipcode AS zipcode,
FLATTEN(flat_top) AS (top_rank:int, sorting_metric:double, globalpropertyid:int, property_lat:double, property_lon:double,
            propertytype:long, reserve:double, product_type:chararray, property_state:chararray,
            bidenddt:chararray, bidstartdt:chararray, startingbid:int, propertycity:chararray, property_auctionid:int, auction_number:chararray, 
            financing_available:int);

topn_out = FOREACH topn_flat GENERATE 
uuid as uuid,
top_rank as top_rank,
sorting_metric as sorting_metric,
globalpropertyid as globalpropertyid,
property_auctionid as auctionid,
auction_number as auction_number,
property_lat as property_lat,
property_lon as property_lon,
product_type as product_type,
property_state as property_state,
propertytype as property_type,
bidenddt as bidenddt,
bidstartdt as bidstartdt,
startingbid as startingbid,
propertycity as propertycity,
financing_available as financing_available,
user_id AS user_id,
firstname AS firstname,
lastname AS lastname,
email AS email,
address AS address,
address1 AS address1,
city AS city,
statename AS statename,
zipcode AS zipcode;


out1 = FILTER topn_out by address matches '.*[a-z].*';

out = DISTINCT topn_out;
rmf $table_directory;
STORE out INTO '$table_directory' USING PigStorage('\t', '-schema');















/*

RUN FROM SHELL TO CREATE TABLE

hcat -e "

use analysis;
drop table if exists analysis.per_uuid_recommendations_commercial_B;

CREATE EXTERNAL TABLE analysis.per_uuid_recommendations_commercial_B (
  uuid string,
  top_rank int,
  haversine_distance double,
  globalpropertyid bigint,
  auctionid int,
  auction_number string,
  property_lat double,
  property_lon double,
  property_state string,
  product_type string,
  property_type string,
  bidenddt string,
  bidstartdt string,
  startingbid double,
  propertycity string,
  financing_available int)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://hmas01lax01us:8020/apps/analysis/per_uuid_recommendations_commercial_B';

"
*/
