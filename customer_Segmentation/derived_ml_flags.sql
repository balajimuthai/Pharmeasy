SET hive.groupby.orderby.position.alias=true;

drop table if exists data_models_temp_tables.oms_derived_ml_flags;
create table data_models_temp_tables.oms_derived_ml_flags as
with ml_flags as 
(
SELECT customer_id, 
case when no_of_delivered_orders>0 then 'Fulfilled'
when no_of_delivered_orders=0 and no_of_placed_orders>0 then 'Placed_not_Fulfilled'
when no_of_delivered_orders=0 and no_of_placed_orders=0 then 'Registered_not_placed'
else 'check' end as ML_user_type,
datediff(current_date,last_pharma_ml_placed_order_date)as ML_days_since_latest_order,
case when datediff(current_date,last_pharma_ml_placed_order_date)>=60 then 'Inactive' 
when datediff(current_date,last_pharma_ml_placed_order_date) is null then 'Registered_not_placed'
else 'Active' end as ML_dormant_flag,
power_customer as ml_power_customer
FROM pre_analytics.clevertap_user_properties_ml_mod
),
ml_flags_express as 
(
SELECT customer_id, 
case when no_of_delivered_orders>0 then 'Fulfilled'
when no_of_delivered_orders=0 and no_of_placed_orders>0 then 'Placed_not_Fulfilled'
when no_of_delivered_orders=0 and no_of_placed_orders=0 then 'Registered_not_placed'
else 'check' end as ML_user_type,
datediff(current_date,last_pharma_ml_placed_order_date)as ML_days_since_latest_order,
case when datediff(current_date,last_pharma_ml_placed_order_date)>=60 then 'Inactive' 
when datediff(current_date,last_pharma_ml_placed_order_date) is null then 'Registered_not_placed'
else 'Active' end as ML_dormant_flag,
power_customer as ml_power_customer
FROM 
(select * from pre_analytics.clevertap_user_properties_ml_express
union 
select * from pre_analytics.clevertap_user_properties_ml_express2)a
)

select c.customer_id,
case 
	when cs46.ML_user_type is not null then cs46.ML_user_type
	when cs46.ML_user_type is null and cs47.ML_user_type is not null then cs47.ML_user_type
	else cs46.ML_user_type end as ML_user_type,
case 
	when cs46.ML_days_since_latest_order is not null then cs46.ML_days_since_latest_order
	when cs46.ML_days_since_latest_order is null and cs47.ML_days_since_latest_order is not null then cs47.ML_days_since_latest_order
	else cs46.ML_days_since_latest_order end as ML_days_since_latest_order,
case 
	when cs46.ML_dormant_flag is not null then cs46.ML_dormant_flag
	when cs46.ML_dormant_flag is null and cs47.ML_dormant_flag is not null then cs47.ML_dormant_flag
	else cs46.ML_dormant_flag end as ML_dormant_flag,
case 
	when cs46.ml_power_customer is not null then cs46.ml_power_customer
	when cs46.ml_power_customer is null and cs47.ml_power_customer is not null then cs47.ml_power_customer || ' Customer'
	else cs46.ml_power_customer end as ml_power_customer
from (select * from data_models_temp_tables.oms_csrd_customer_data where registration_date < current_Date()) c
left join ml_flags cs46 on c.customer_id=cs46.customer_id
left join ml_flags_express cs47 on c.customer_id = cs47.customer_id
group by 1,2,3,4,5;
