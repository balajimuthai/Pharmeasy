set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
SET hive.strict.checks.cartesian.product=false;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;

drop table if exists oms_csrd_latest_related_columns_union;
CREATE TEMPORARY TABLE oms_csrd_latest_related_columns_union as
with final as
(
select
customer_id,
current_app_version,
current_app_os,
current_phone_details,
latest_order_rating,
latest_placed_order_time,
latest_placed_order_id,
latest_placed_supplier_city_name,
latest_delivered_order_time,
latest_delivered_order_id,
latest_rated_order_id,
latest_delivered_order_issue_flag,
latest_order_discount,
recent_order_source,
Latest_fulfilled_delivery_city_name,
Latest_fulfilled_order_source,
Latest_fulfilled_subscription_id,
recent_delivery_city_tier,
customer_type
from data_models_temp_tables.oms_csrd_latest_related_final
union
select 
customer_id,
current_app_version,
current_app_os,
current_phone_details,
latest_order_rating,
latest_placed_order_time,
latest_placed_order_id,
latest_placed_supplier_city_name,
latest_delivered_order_time,
latest_delivered_order_id,
latest_rated_order_id,
latest_delivered_order_issue_flag,
latest_order_discount,
recent_order_source,
Latest_fulfilled_delivery_city_name,
Latest_fulfilled_order_source,
cast('null' as string) as Latest_fulfilled_subscription_id,
recent_delivery_city_tier,
customer_type
from data_models_temp_tables.oms_csrd_latest_related_metrics
),  

latest_placed_columns as
  (
  select 
  customer_id,
  current_app_version,
current_app_os,
current_phone_details,
latest_order_rating,
latest_placed_order_time,
latest_placed_order_id,
latest_placed_supplier_city_name,
latest_order_discount,
recent_order_source,
customer_type
from (select *,row_number()over(partition by customer_id order by latest_placed_order_time desc) as l_rk
		from final )a
where l_rk=1
	),

delivered_order_columns as
	(
	  select customer_id,
	  latest_delivered_order_time,
latest_delivered_order_id,
latest_delivered_order_issue_flag,
Latest_fulfilled_delivery_city_name,
Latest_fulfilled_order_source,
Latest_fulfilled_subscription_id,
recent_delivery_city_tier
from (select *,row_number()over(partition by customer_id order by latest_delivered_order_time desc) as d_rk from final)a
where d_rk=1
	),
	
rated as 
(
select 
customer_id,
latest_rated_order_id
from (select *,row_number()over(partition by customer_id order by latest_delivered_order_id desc) as r_rk 
      from final
      where latest_rated_order_id is not null)a
 where r_rk=1
 )
  select 
  a.customer_id,
current_app_version,
current_app_os,
current_phone_details,
latest_order_rating,
latest_placed_order_time,
latest_placed_order_id,
latest_placed_supplier_city_name,
latest_delivered_order_time,
latest_delivered_order_id,
latest_rated_order_id,
latest_delivered_order_issue_flag,
latest_order_discount,
recent_order_source,
Latest_fulfilled_delivery_city_name,
Latest_fulfilled_order_source,
Latest_fulfilled_subscription_id,
recent_delivery_city_tier,
customer_type

from latest_placed_columns a
left join delivered_order_columns b on a.customer_id=b.customer_id
left join rated d on a.customer_id=d.customer_id;

insert overwrite table data_models_temp_tables.oms_csrd_latest_related_final
select distinct
customer_id,
current_app_version,
current_app_os,
current_phone_details,
latest_order_rating,
latest_placed_order_time,
latest_placed_order_id,
latest_placed_supplier_city_name,
latest_delivered_order_time,
latest_delivered_order_id,
latest_rated_order_id,
latest_delivered_order_issue_flag,
latest_order_discount,
recent_order_source,
Latest_fulfilled_delivery_city_name,
Latest_fulfilled_order_source,
Latest_fulfilled_subscription_id,
recent_delivery_city_tier,
customer_type
from oms_csrd_latest_related_columns_union;
