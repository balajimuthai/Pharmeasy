set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
SET hive.strict.checks.cartesian.product=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;
set hive.auto.convert.join=false;

DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_install_appsflyer;
CREATE TABLE data_models_temp_tables.oms_csrd_install_appsflyer as
with install_appsflyer as
(
SELECT distinct z.customer_id, z.media_source, z.campaign, 
CASE WHEN (CASE WHEN LOWER(z.media_source) IN ('google','bing') AND LOWER(z.campaign) <> '(not set)' THEN 'Google' ELSE ism.install_source_attribution END) is NULL THEN 'Organic' ELSE (CASE WHEN LOWER(z.media_source) IN ('google','bing') AND LOWER(z.campaign) <> '(not set)' THEN 'Google' ELSE ism.install_source_attribution END) END as install_source_attribution,is_apk

FROM
(SELECT cam.customer_id,
CASE WHEN (ins.media_source='' OR ins.media_source IS NULL) THEN 'organic' ELSE ins.media_source END as media_source,
CASE WHEN (ins.campaign='' OR ins.campaign IS NULL) THEN 'organic' ELSE ins.campaign END as campaign,is_apk,
ROW_NUMBER() OVER(PARTITION BY cam.customer_id ORDER BY cam.updated_at DESC) as rn
FROM
(SELECT appsflyer_id, CASE WHEN media_source='' THEN partner ELSE media_source END as media_source, campaign,
 case when google_play_install_begin_time is NULL then true else false end as is_apk
FROM pe_consumer_af_android.installs_android_snapshot
where dt<=date_sub(current_date,1)
UNION ALL
SELECT appsflyer_id, CASE WHEN media_source='' THEN partner ELSE media_source END as media_source, campaign,
 case when google_play_install_begin_time is NULL then true else false end as is_apk
FROM pe_consumer_af_android.organic_installations_android_snapshot
where dt<=date_sub(current_date,1)
UNION ALL
SELECT appsflyer_id, CASE WHEN media_source='' THEN partner ELSE media_source END as media_source, campaign,
 case when install_time is NULL then true else false end as is_apk
FROM pe_consumer_af_ios.installs_ios_snapshot
where dt<=date_sub(current_date,1)
UNION ALL
SELECT appsflyer_id, CASE WHEN media_source='' THEN partner ELSE media_source END as media_source, campaign,
case when install_time is NULL then true else false end as is_apk
FROM pe_consumer_af_ios.organic_installations_ios_snapshot
where dt<=date_sub(current_date,1)
)as ins
INNER JOIN (select * from pe_pe2_pe2.customer_appsflyer_mapping_snapshot where dt<=date_sub(current_date,1)) cam ON ins.appsflyer_id=cam.appsflyer_id
WHERE cam.customer_id IS NOT NULL) as z
LEFT JOIN pre_analytics.install_source_mapping ism ON LOWER(z.media_source)=LOWER(ism.media_source)
WHERE z.rn=1
),

install_time as
(
select cam.customer_id,
min(ins_a.event_time) as first_android_install_time,
max(ins_a.event_time) as latest_android_install_time,
max(unins_a.event_time) as latest_android_uninstall_time,
min(ins_ios.event_time) as first_ios_install_time,
max(ins_ios.event_time) as latest_ios_install_time
from (select * from pe_pe2_pe2.customer_appsflyer_mapping_snapshot where dt<=date_sub(current_date,1))as cam
left join (select * from pe_consumer_af_android.installs_android_snapshot where dt<=date_sub(current_date,1))ins_a on cam.advertising_id = ins_a.advertising_id
left join (select * from pe_consumer_af_android.uninstalls_android_snapshot where dt<=date_sub(current_date,1))unins_a on cam.advertising_id = unins_a.advertising_id
left join (select * from pe_consumer_af_ios.installs_ios_snapshot where dt<=date_sub(current_date,1))as ins_ios on cam.advertising_id = ins_ios.advertising_id
group by 1
)

select distinct
a.customer_id,
media_source,
campaign,
install_source_attribution,is_apk,
first_android_install_time,
latest_android_install_time,
latest_android_uninstall_time,
first_ios_install_time,
latest_ios_install_time

from install_appsflyer a
left join  install_time b on a.customer_id=b.customer_id;
