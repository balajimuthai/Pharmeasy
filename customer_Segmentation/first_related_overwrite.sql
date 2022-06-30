set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;
set hive.auto.convert.join=false;

drop table if exists adhoc_analysis.oms_csrd_first_related_columns_overwrite;
CREATE TABLE adhoc_analysis.oms_csrd_first_related_columns_overwrite as
with final as 
(
select
customer_id,
first_placed_order_time,
first_delivered_order_time,
first_placed_order_id,
first_delivered_order_id,
first_delivered_order_disc_percentage,
first_delivered_order_supplier_city,
fdo_disc_percentage_split,
first_power_tagged_time,
first_placed_order_city,
first_placed_order_state,
first_fulfilled_order_gmv,
first_fulfilled_order_discounted_gmv,
first_placed_order_discount,
first_fulfilled_order_discount
from data_models_temp_tables.oms_csrd_first_related_table
union
select
customer_id,
first_placed_order_time,
first_delivered_order_time,
first_placed_order_id,
first_delivered_order_id,
first_delivered_order_disc_percentage,
first_delivered_order_supplier_city,
fdo_disc_percentage_split,
first_power_tagged_time,
first_placed_order_city,
first_placed_order_state,
first_fulfilled_order_gmv,
first_fulfilled_order_discounted_gmv,
first_placed_order_discount,
first_fulfilled_order_discount
from data_models_temp_tables.oms_csrd_first_related_metrics
),

first_placed_columns as
(
select 
customer_id,
first_placed_order_time,
first_placed_order_id,
first_placed_order_city,
first_placed_order_state,
first_placed_order_discount
from
(select
customer_id,
first_placed_order_time,
first_placed_order_id,
first_placed_order_city,
first_placed_order_state,
first_placed_order_discount,
row_number() over(partition by customer_id order by first_placed_order_time) as rnum
from final
)x
where rnum = 1
),

first_delivered_columns as 
(
select 
customer_id,
first_delivered_order_time,
first_delivered_order_id,
first_delivered_order_disc_percentage,
first_delivered_order_supplier_city,
fdo_disc_percentage_split,
first_fulfilled_order_GMV,
first_fulfilled_order_discounted_GMV,
first_fulfilled_order_discount
from
(SELECT customer_id,
first_delivered_order_time,
first_delivered_order_id,
first_delivered_order_disc_percentage,
first_delivered_order_supplier_city,
fdo_disc_percentage_split,
first_fulfilled_order_GMV,
first_fulfilled_order_discounted_GMV,
first_fulfilled_order_discount,
row_number() over(partition by customer_id order by first_delivered_order_time nulls last) as rnum
from final
)x
where rnum = 1
),
first_power_tagged_time as 
(
select customer_id,first_power_tagged_time 
from (select customer_id,first_power_tagged_time,
row_number() over(partition by customer_id order by first_power_tagged_time) as p_num
from final
where first_power_tagged_time is not null
)a
where p_num=1
)
select
fpc.customer_id,
fpc.first_placed_order_time,
fpc.first_placed_order_id,
fpc.first_placed_order_city,
fpc.first_placed_order_state,
fpt.first_power_tagged_time,
fpc.first_placed_order_discount,
fdc.first_delivered_order_time,
fdc.first_delivered_order_id,
fdc.first_delivered_order_disc_percentage,
fdc.first_delivered_order_supplier_city,
fdc.fdo_disc_percentage_split,
fdc.first_fulfilled_order_GMV,
fdc.first_fulfilled_order_discounted_GMV,
fdc.first_fulfilled_order_discount
from first_placed_columns fpc
left join first_delivered_columns fdc on fpc.customer_id = fdc.customer_id
left join first_power_tagged_time fpt on fpc.customer_id=fpt.customer_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;

insert overwrite table data_models_temp_tables.oms_csrd_first_related_table 
select 
customer_id,
first_placed_order_time,
first_delivered_order_time,
first_placed_order_id,
first_delivered_order_id,
first_delivered_order_disc_percentage,
first_delivered_order_supplier_city,
fdo_disc_percentage_split,
first_power_tagged_time,
first_placed_order_city,
first_placed_order_state,
first_fulfilled_order_gmv,
first_fulfilled_order_discounted_gmv,
first_placed_order_discount,
first_fulfilled_order_discount
from adhoc_analysis.oms_csrd_first_related_columns_overwrite;
