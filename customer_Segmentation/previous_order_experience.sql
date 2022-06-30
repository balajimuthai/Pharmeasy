drop table if EXISTS data_models_temp_tables.oms_previous_order_experience;
create table data_models_temp_tables.oms_previous_order_experience as
with latest_order as
( 
select customer_id,
order_id,
experience_score,order_placed_at,
row_number()over(partition by customer_id 
order by date(order_placed_at) desc)as rk 
from (select distinct customer_id,order_id,experience_score,order_placed_at 
	  from data_model.f_order_consumer
where order_status in ('ORDER COMPLETE','ORDER COMPLETE - PARTIAL') and dt<=date_sub(current_date,1) )a
),
nth_value as 
(
  select customer_id,experience_score,rk,
  lead(experience_score, 1)over(partition by customer_id order by order_placed_at desc) as n
  from latest_order
)
  
select customer_id,abs(experience_score-n) as previous_order_experience_score_change
from nth_value
where rk=1;
