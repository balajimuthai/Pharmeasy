DROP TABLE IF EXISTS  data_models_temp_tables.oms_csrd_registration_appsflyer_final;
CREATE TABLE `data_models_temp_tables.oms_csrd_registration_appsflyer_final`
(
  `customer_id` int,
  `registration_media_source` string,
  `registration_campaign` string,
  `is_retargeting` boolean,
  `adset` string,
  `site_id` string,
  `advertising_id` string,
  `registration_source_attribution` string
 
)PARTITIONED by (`registration_date` date);

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

insert into table data_models_temp_tables.oms_csrd_registration_appsflyer_final partition(`registration_date`)
select customer_id,
registration_media_source,
registration_campaign,
is_retargeting,
adset,
site_id,
advertising_id,
registration_source_attribution,
registration_date
from  data_models_temp_tables.csrd_registration_appsflyer_final_partition;
