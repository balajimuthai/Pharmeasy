DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_registration_temp;
CREATE TABLE data_models_temp_tables.oms_csrd_registration_temp as
with docon_phonepe_customers as
(
select 
	c.id as customer_id, 
	case 
		when lower(customer_source)='docon' then 'Docon'
		when lower(customer_source)='phonepe' then 'Phonepe'
	end as customer_source
from pe_pe2_pe2.customer_snapshot c 
where date(from_utc_timestamp(dateadded, 'IST'))  between date_sub(current_date,10) and date_sub(current_date,1) and dt between date_sub(current_date,12) and date_sub(current_date,1)
and lower(c.customer_source) in ('phonepe','docon')
),

inorganic as
(
SELECT DISTINCT event_time, af_customer_id as customer_id, CASE WHEN (media_source='' or media_source is null) THEN partner ELSE media_source END as media_source, campaign, is_retargeting,adset,site_id,advertising_id
from pe_consumer_af_android.in_app_events_android_snapshot
WHERE event_name='af_new_user_login' and date(event_time) between date_sub(current_date,10) and date_sub(current_date,1)and dt between date_sub(current_date,12) and date_sub(current_date,1)
UNION ALL
SELECT DISTINCT event_time, af_customer_id as customer_id, CASE WHEN (media_source='' or media_source is null) THEN partner ELSE media_source END as media_source, campaign, is_retargeting,adset,site_id,advertising_id
from pe_consumer_af_ios.in_app_events_ios_snapshot
WHERE event_name='af_new_user_login' and date(event_time) between date_sub(current_date,10) and date_sub(current_date,1)and dt between date_sub(current_date,12) and date_sub(current_date,1)
),

retargeting as 
(
SELECT DISTINCT event_time, af_customer_id as customer_id, CASE WHEN (media_source='' or media_source is null) THEN partner ELSE media_source END as media_source, campaign, is_retargeting,adset,site_id,advertising_id
from pe_consumer_af_android.in_app_events_retargeting_android_snapshot
WHERE event_name='af_new_user_login' and date(event_time) between date_sub(current_date,10) and date_sub(current_date,1) and dt between date_sub(current_date,12) and date_sub(current_date,1)
UNION ALL
SELECT DISTINCT event_time, af_customer_id as customer_id, CASE WHEN (media_source='' or media_source is null) THEN partner ELSE media_source END as media_source, campaign, is_retargeting,adset,site_id,advertising_id
from pe_consumer_af_ios.in_app_events_retargeting_ios_snapshot
WHERE event_name='af_new_user_login' and date(event_time) between date_sub(current_date,10) and date_sub(current_date,1)and dt between date_sub(current_date,12) and date_sub(current_date,1)
), 

order_attribution as
(
SELECT cast(c.customer_id as string) as customer_id, c.media_source,c.campaign, c.is_retargeting,c.adset,c.site_id,c.advertising_id
FROM
(SELECT y.id as customer_id, 
		CASE WHEN x.media_source is not null THEN x.media_source ELSE 'organic' END as media_source,
		CASE WHEN x.campaign is not null THEN x.campaign ELSE 'organic' END as campaign,
		CASE WHEN x.is_retargeting is not null THEN x.is_retargeting ELSE FALSE END as is_retargeting,
		adset,
		site_id,advertising_id,
		row_number() OVER(PARTITION BY customer_id order by event_time) as temp_rank
FROM (
SELECT 
	(CASE WHEN a.customer_id=b.customer_id THEN b.event_time
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and a.customer_id is NULL THEN b.event_time
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and b.customer_id is NULL THEN a.event_time END) as event_time,
	(CASE WHEN a.customer_id=b.customer_id THEN b.customer_id
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and a.customer_id is NULL THEN b.customer_id
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and b.customer_id is NULL THEN a.customer_id END) as customer_id,
	(CASE WHEN a.customer_id=b.customer_id THEN b.media_source
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and a.customer_id is NULL THEN b.media_source
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and b.customer_id is NULL THEN a.media_source END) as media_source,
	(CASE WHEN a.customer_id=b.customer_id THEN b.campaign
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and a.customer_id is NULL THEN b.campaign
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and b.customer_id is NULL THEN a.campaign END) as campaign,
	(CASE WHEN a.customer_id=b.customer_id THEN b.is_retargeting
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and a.customer_id is NULL THEN b.is_retargeting
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and b.customer_id is NULL THEN a.is_retargeting END) as is_retargeting,
	(CASE WHEN a.customer_id=b.customer_id THEN b.adset
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and a.customer_id is NULL THEN b.adset
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and b.customer_id is NULL THEN a.adset END) as adset,
	(CASE WHEN a.customer_id=b.customer_id THEN b.site_id
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and a.customer_id is NULL THEN b.site_id
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and b.customer_id is NULL THEN a.site_id END) as site_id,
 (CASE WHEN a.customer_id=b.customer_id THEN b.advertising_id
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and a.customer_id is NULL THEN b.advertising_id
				WHEN CASE WHEN a.customer_id is not null THEN a.customer_id ELSE 0 END <> CASE WHEN b.customer_id is not null THEN b.customer_id ELSE 0 END and b.customer_id is NULL THEN a.advertising_id END) as advertising_id
FROM inorganic a
FULL OUTER JOIN retargeting b on a.customer_id=b.customer_id
UNION ALL
select cast(event_time as timestamp) as event_time,
cast(a.customer_id as string) as customer_id,a.media_source,a.campaign,
a.is_retargeting, NULL as adset, NULL as site_id, Null as advertising_id
from
(
SELECT 
	customer_id, 
	CASE WHEN LOWER(ga.source) IN ('google','bing') AND trim(lower(ga.campaign)) != '' and ga.medium = 'organic' THEN 'SEO' else ga.source end as media_source, 
	medium as medium, 
	campaign as campaign, 
	FALSE as is_retargeting,
    from_unixtime(ga.skull_createdat DIV 1000) as event_time,
	date(from_unixtime(ga.skull_createdat DIV 1000)) as created_at,
	RANK() OVER(PARTITION BY  customer_id order by source, medium, campaign DESC) as temp_rank 
FROM pe_consumer_ga.ga_registrations_snapshot ga
) a
inner join pe_pe2_pe2.customer_snapshot c on a.customer_id=c.id and date(from_utc_timestamp(c.dateadded, 'IST'))=date(a.created_at)
where date(a.created_at) between '2020-07-08' and '2021-08-30' and temp_rank=1 and a.customer_id NOT IN (SELECT customer_id FROM pre_analytics.ga_registration_23092020_13102020)
union
select 
cast(event_time as timestamp) as event_time,
cast(a.customer_id as string)as customer_id,a.media_source,a.campaign,a.is_retargeting, NULL as adset, NULL as site_id,
null as advertising_id
from
(
SELECT 
	customer_id, 
	CASE WHEN LOWER(ga.source) IN ('google','bing') AND trim(lower(ga.campaign)) != '' and ga.medium = 'organic' THEN 'SEO' else ga.source end as media_source, 
	medium as medium, 
	campaign as campaign, 
	FALSE as is_retargeting,
	from_unixtime(ga.skull_createdat DIV 1000) as event_time,
	date(from_unixtime(ga.skull_createdat DIV 1000)) as created_at,
	RANK() OVER(PARTITION BY  customer_id order by source, medium, campaign DESC) as temp_rank 
FROM pe_consumer_ga.ga_registrations_snapshot ga
) a
inner join pe_pe2_pe2.customer_snapshot c on a.customer_id=c.id and date(from_utc_timestamp(c.dateadded, 'IST'))=date(a.event_time)
where date(a.created_at) between '2020-08-31' and '2021-09-20' and temp_rank=1 and a.customer_id NOT IN (SELECT customer_id FROM pre_analytics.ga_registration_23092020_13102020)
union
select 
cast(event_time as timestamp) as event_time,
cast(a.customer_id as string) as customer_id,a.media_source,a.campaign,a.is_retargeting,
NULL as adset,
NULL as site_id,
NULL as advertising_id
from
(
SELECT 
	customer_id, 
	CASE WHEN LOWER(ga.source) IN ('google','bing') AND trim(lower(ga.campaign)) != '' and ga.medium = 'organic' THEN 'SEO' else ga.source end as media_source, 
	medium as medium, 
	campaign as campaign, 
	FALSE as is_retargeting,
	timestamp(from_unixtime(ga.skull_createdat DIV 1000)) as event_time,
	date(from_unixtime(ga.skull_createdat DIV 1000)) as created_at,
	RANK() OVER(PARTITION BY  customer_id order by source, medium, campaign DESC) as temp_rank 
FROM pe_consumer_ga.ga_registrations_snapshot ga
WHERE ga.dt<'2021-12-01'
union
select
    customer_id,
    CASE WHEN LOWER(gb.source) IN ('google','bing') AND trim(lower(gb.campaign)) != '' and gb.medium = 'organic' THEN 'SEO' else gb.source end as media_source, 
    medium as medium, 
	campaign as campaign, 
	FALSE as is_retargeting,
	timestamp(cast(event_time as bigint)) as event_time,
	date(`date`) as created_at,
	RANK() OVER(PARTITION BY  customer_id order by source, medium, campaign DESC) as temp_rank 
from pe_consumer_ga_bigquery.registrations_snapshot_nrt gb
where dt between date_sub(current_date,10) and date_sub(current_date(),1)
) a
inner join pe_pe2_pe2.customer_snapshot c on a.customer_id=c.id and date(from_utc_timestamp(c.dateadded, 'IST'))=date(a.created_at)
where date(a.created_at) >='2021-09-21' and temp_rank=1 and a.customer_id NOT IN (SELECT customer_id FROM pre_analytics.ga_registration_23092020_13102020)
union
select
cast(b.registration_time as timestamp) as event_time,
cast(b.customer_id as string) as customer_id,
CASE WHEN LOWER(b.source) IN ('google','bing') AND trim(lower(b.campaign)) != '' and b.medium = 'organic' THEN 'SEO' else b.source end as media_source,
b.campaign as campaign,
FALSE as is_retargeting,
NULL as adset,
NULL as site_id,
null as advertising_id
FROM pre_analytics.ga_registration_23092020_13102020 b
) x
RIGHT JOIN (select * from pe_pe2_pe2.customer_snapshot where dt between date_sub(current_date,12) and date_sub(current_date,1))y on cast(x.customer_id as string)=cast(y.id as string)
)c
WHERE c.temp_rank=1
GROUP BY 1,2,3,4,5,6,7

),

seo_tagging as
(
select foc.customer_id from data_model.f_order_consumer_snapshot foc 
where foc.install_source_attribution='SEO' and foc.order_placed_count=1 and foc.order_placed_date<'2020-07-08' and foc.dt<'2020-07-06'
),

order_attribution_mapping as
(
select 
oa.customer_id,
case when cast(st.customer_id as string)=cast(oa.customer_id as string) then 'SEO' else oa.media_source end as media_source,
case when cast(st.customer_id as string)=cast(oa.customer_id as string) then '(not set)' else oa.campaign end as campaign,
oa.is_retargeting,
oa.adset,
oa.site_id,
  oa.advertising_id
from order_attribution oa
left join seo_tagging st on oa.customer_id=st.customer_id
),

registration_attribution  as (
select 
case 
 when 
 (
 CASE WHEN LOWER(a.registration_media_source) IN ('seo') then 'SEO'
 	  WHEN LOWER(a.registration_media_source) IN ('docon') then 'Docon'
 	  WHEN LOWER(a.registration_media_source) IN ('phonepe') then 'Phonepe'
 	  WHEN LOWER(a.registration_media_source) IN ('seowebtraffic') and ((trim(site_id) = '' or trim(site_id) not like ('%%?%%')) or trim(site_id) is null) then 'SEO'
      WHEN LOWER(a.registration_media_source) IN ('seowebtraffic') and trim(site_id) like ('%%gclid%%') then 'Google'
	  WHEN LOWER(a.registration_media_source) IN ('seowebtraffic') and trim(site_id) like ('%%utm%%') and trim(site_id) not like ('%%gclid%%') then 'Affiliates'
      WHEN LOWER(a.registration_media_source) IN ('google','bing') AND LOWER(a.registration_campaign) <> '(not set)' THEN 'Google'
      when LOWER(a.registration_media_source) is not null and mis.install_source_attribution is null then 'Unmapped'
      ELSE mis.install_source_attribution end
 ) 
 is null then 'Organic' else 
 (
 CASE WHEN LOWER(a.registration_media_source) IN ('seo') then 'SEO'
 	  WHEN LOWER(a.registration_media_source) IN ('docon') then 'Docon'
 	  WHEN LOWER(a.registration_media_source) IN ('phonepe') then 'Phonepe'
 	  WHEN LOWER(a.registration_media_source) IN ('seowebtraffic') and ((trim(site_id) = '' or trim(site_id) not like ('%%?%%')) or trim(site_id) is null) then 'SEO'
      WHEN LOWER(a.registration_media_source) IN ('seowebtraffic') and trim(site_id) like ('%%gclid%%') then 'Google'
	  WHEN LOWER(a.registration_media_source) IN ('seowebtraffic') and trim(site_id) like ('%%utm%%') and trim(site_id) not like ('%%gclid%%') then 'Affiliates'
 	  WHEN LOWER(a.registration_media_source) IN ('google','bing') AND LOWER(a.registration_campaign) <> '(not set)' THEN 'Google'
	  when LOWER(a.registration_media_source) is not null and mis.install_source_attribution is null then 'Unmapped'
	  ELSE mis.install_source_attribution end) 
 end as registration_source_attribution,
a.customer_id
from
(
	select 
	c.id as customer_id,
	case when dpc.customer_id=c.id then dpc.customer_source else oam.media_source end as registration_media_source,
	case when dpc.customer_id=c.id and dpc.customer_source='Docon' then 'Docon_leads' else oam.campaign end as registration_campaign,
	oam.site_id
	from pe_pe2_pe2.customer_snapshot c
	left join docon_phonepe_customers dpc on c.id=dpc.customer_id
	left join order_attribution_mapping oam on c.id=oam.customer_id
)a
LEFT JOIN pre_analytics.install_source_mapping mis ON LOWER(a.registration_media_source)=LOWER(mis.media_source)
 )
 select cast(c.id as string) as id,media_source as registration_media_source,campaign as registration_campaign,is_retargeting,adset,site_id,advertising_id,
 registration_source_attribution,date(registration_time) as registration_date
 from pe_pe2_pe2.customer_snapshot c
 left join order_attribution_mapping a on a.customer_id=c.id
 left join registration_attribution b on c.id=b.customer_id
 left join  data_models_temp_tables.oms_csrd_customer_data d on c.id=d.customer_id
 where c.dt>=date_sub(current_Date,12) and c.id not in (SELECT DISTINCT customer_id FROM pe_pe2_pe2.customer_flags_snapshot cf  where flag_id =71)
 group by 1,2,3,4,5,6,7,8,9;
 
insert overwrite table data_models_temp_tables.oms_csrd_registration_appsflyer_final partition(`registration_date`)
select cast(id as string) as customer_id,
registration_media_source,
registration_campaign,
is_retargeting,
adset,
site_id,
advertising_id,
registration_source_attribution,
registration_date
from data_models_temp_tables.oms_csrd_registration_temp
where date(registration_date)>=date_sub(current_Date,10)
group by 1,2,3,4,5,6,7,8,9;
