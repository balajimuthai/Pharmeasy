drop table if exists data_models_temp_tables.oms_csrd_segmention;
create table data_models_temp_tables.oms_csrd_segmention as 
with f_order_consumer_total as (
select foc.customer_id,
foc.order_id,
foc.experience_score,
foc.order_placed_at,foc.order_status
from (select customer_id,order_id,experience_score,order_placed_at,order_status from data_model.f_order_consumer where dt<current_date()) foc 
),
latest_order as
( 
select customer_id,
max(case when recent_order_number = 1 then experience_score else 0 end) as experience_score
from (select distinct customer_id,experience_score,row_number()over(partition by customer_id 
order by date(order_placed_at) desc)as recent_order_number  from f_order_consumer_total
where order_status in ('ORDER COMPLETE','ORDER COMPLETE - PARTIAL'))a
group by 1
),

fm_and_order_bucket as (
select distinct date(segmentation_date) as segmentation_date,
customer_id,
new_segment_name,
order_bucket
from data_model.customer_historic_fm_segments
where segmentation_date in
(select max(segmentation_date) from data_model.customer_historic_fm_segments)
)

select distinct l.customer_id,
l.experience_score,new_segment_name as recent_fm_segment,order_bucket as recent_order_bucket,discount_affinity
from latest_order l
left join fm_and_order_bucket f on l.customer_id=f.customer_id
left join pre_analytics.retention_customer_segments_rr d on d.customer_id=l.customer_id;
