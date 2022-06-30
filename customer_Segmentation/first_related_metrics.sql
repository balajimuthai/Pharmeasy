set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
SET hive.strict.checks.cartesian.product=false;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;
set hive.auto.convert.join=false;


DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_first_related_table ;
CREATE TABLE data_models_temp_tables.oms_csrd_first_related_table as
with f_order_consumer_total as 
(
select foc.customer_id,
foc.order_id,
foc.order_status_id,
foc.order_status,
foc.order_placed_at,
foc.fulfilled_discount_percentage,
foc.order_app_version as current_app_version,
UPPER(foc.order_source||' '||foc.order_app_os_version) as current_app_os,
UPPER(order_phone_build) as current_phone_details,
foc.fulfilled_mrp,
foc.fulfilled_discounted_mrp,
foc.order_placed_count,
foc.service_defect,
foc.rating,
foc.customer_type,
foc.order_type,
foc.delivery_city_name,
foc.delivery_pincode,
foc.delivery_state,
foc.supplier_city_name
from data_model.f_order_consumer foc 
where foc.dt <= date_sub(current_date(),1) 
),

customer_segments_order_time_info AS
(
select
ord.customer_id,
min(ord.order_placed_at) as first_placed_order_time,
min(case when ord.order_status in ('ORDER COMPLETE' , 'ORDER COMPLETE - PARTIAL') then ord.order_placed_at end) as first_delivered_order_time
from f_order_consumer_total ord
group by 1
),

first_placed_order_id as
(
select customer_id,
first_placed_order_id from
(select distinct customer_id,
order_id as first_placed_order_id,
row_number() over (partition by customer_id order by order_placed_at asc) as order_rank
from f_order_consumer_total foc)x
where order_rank = 1
order by 1
),

first_delivered_order_id as
(
select customer_id,
first_delivered_order_id from
(select distinct customer_id,
order_id as first_delivered_order_id,
row_number() over (partition by customer_id order by order_placed_at asc) as order_rank
from f_order_consumer_total foc where foc.order_status in ('ORDER COMPLETE' , 'ORDER COMPLETE - PARTIAL'))a
where order_rank = 1
order by 1
),

first_fulfilled_order_gmv_discounted_gmv as
(
select foc.customer_id,
foc.fulfilled_mrp as first_fulfilled_order_gmv,
foc.fulfilled_discounted_mrp as first_fulfilled_order_discounted_gmv
from f_order_consumer_total foc
inner join first_delivered_order_id fdoi on foc.order_id = fdoi.first_delivered_order_id
),

first_placed_order_city_state as
(
select customer_id,
first_placed_order_city,
first_placed_order_state
from
(
select customer_id, 
delivery_city_name  as first_placed_order_city, 
delivery_state as first_placed_order_state,
row_number() over(partition by customer_id order by order_placed_at) as rnum
from f_order_consumer_total foc
)x
where rnum = 1
group by 1,2,3
),
first_fulfilled_disc_city AS
(
SELECT foc.customer_id, 
foc.fulfilled_discount_percentage as first_delivered_order_disc_percentage,
foc.supplier_city_name as first_delivered_order_supplier_city
FROM f_order_consumer_total foc
INNER JOIN first_delivered_order_id fdoi ON foc.order_id = fdoi.first_delivered_order_id
group by 1,2,3
),

customer_type as
(
SELECT foc.customer_id, 
min(case when customer_type='Power Customer' then order_placed_at end ) as first_power_tagged_time
FROM f_order_consumer_total foc  
group by 1
),

first_fulfilled_order_discount as 
(
select customer_id,
CAST(case when round(fulfilled_discount_percentage,2) is NULL then 0 else fulfilled_discount_percentage end as INTEGER) as first_fulfilled_order_discount
from
(
select 
foc.customer_id,
foc.fulfilled_discount_percentage
from f_order_consumer_total foc
inner join first_delivered_order_id fdoi 
on foc.order_id = fdoi.first_delivered_order_id
)x
group by 1,2
),

first_placed_order_discount_old as 
(
select csoti.customer_id,
case when odd.discount is not NULL then trunc(odd.discount,2) else 0.00 end as first_placed_order_discount
from customer_segments_order_time_info csoti
left join first_placed_order_id fpoi on csoti.customer_id = fpoi.customer_id
left join pe_pe2_pe2.order_discount_snapshot odd on odd.order_id=fpoi.first_placed_order_id
),
first_placed_order_discount_new as 
(
select csoti.customer_id,
discount_amount as first_placed_order_discount
from customer_segments_order_time_info csoti
left join first_placed_order_id fpoi on csoti.customer_id = fpoi.customer_id
inner join (
select po.external_id as order_id,
sum(popsd.totaldiscount) as discount_amount
from pe_oms_iron.parent_order_snapshot_nrt po
join pe_oms_iron.parent_order_price_snapshot_snapshot_nrt pops on po.id = pops.parent_order_id
join pe_oms_iron.parent_order_price_snapshot_discounts_snapshot_nrt popsd on popsd.parent_order_price_snapshot_id = pops.id
where pops.snapshot_type = 'PARENT_ORDER_CREATE' and popsd.type != 'ITEM_LEVEL'
group by po.external_id
)b on b.order_id=fpoi.first_placed_order_id
),

first_placed_order_discount as (
select * from first_placed_order_discount_old
union
select * from first_placed_order_discount_new
),

final AS
(
SELECT 
csoti.customer_id,
csoti.first_placed_order_time,
csoti.first_delivered_order_time,
fpoi.first_placed_order_id,
fdoi.first_delivered_order_id,
ffdc.first_delivered_order_disc_percentage,
ffdc.first_delivered_order_supplier_city,
ct.first_power_tagged_time,
fpocs.first_placed_order_city,
fpocs.first_placed_order_state,
ffogmv.first_fulfilled_order_gmv,
ffogmv.first_fulfilled_order_discounted_gmv,
case when first_placed_order_discount is not NULL then first_placed_order_discount else 0.00 end as first_placed_order_discount,
ffod.first_fulfilled_order_discount
from customer_segments_order_time_info csoti
left join first_placed_order_id fpoi on csoti.customer_id = fpoi.customer_id
left join first_delivered_order_id fdoi on csoti.customer_id = fdoi.customer_id
left join first_fulfilled_disc_city ffdc on csoti.customer_id = ffdc.customer_id
left join customer_type ct on csoti.customer_id = ct.customer_id
left join first_placed_order_city_state fpocs on csoti.customer_id = fpocs.customer_id
left join first_fulfilled_order_gmv_discounted_gmv ffogmv on csoti.customer_id = ffogmv.customer_id
left join first_fulfilled_order_discount ffod on csoti.customer_id = ffod.customer_id
left join first_placed_order_discount fpod on fpod.customer_id=csoti.customer_id
)
select 
customer_id,
first_placed_order_time,
first_delivered_order_time,
first_placed_order_id,
first_delivered_order_id,
first_delivered_order_disc_percentage,
first_delivered_order_supplier_city,
cast(null as string) as  fdo_disc_percentage_split,
first_power_tagged_time,
first_placed_order_city,
first_placed_order_state,
first_fulfilled_order_gmv,
first_fulfilled_order_discounted_gmv,
first_placed_order_discount,
first_fulfilled_order_discount
from final;
