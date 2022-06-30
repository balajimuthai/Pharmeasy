DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_other_aggregated_metrics;
CREATE TABLE data_models_temp_tables.oms_csrd_other_aggregated_metrics as
with f_order_consumer_total as 
(
select foc.customer_id,
foc.order_id,
foc.dt,
foc.delivery_pincode,
foc.delivery_address_id
from data_model.f_order_consumer foc 
where foc.dt <= date_sub(current_date(),1)
),

final as 
(
select 
foc.customer_id, 
count(distinct case when foc.delivery_pincode is not null and foc.delivery_pincode != 0 then foc.delivery_pincode end) as no_of_pincodes,
count(distinct foc.delivery_address_id) as count_of_addresses,
count(distinct case when (dpo.doctor_name is null or length(dpo.doctor_name) <= 3) then null else dpo.doctor_name end) as no_of_unique_doctors
from f_order_consumer_total foc
LEFT JOIN (select order_id, doctor_name from data_model.f_doctor_program_order where dt <= date_sub(current_date(),1))dpo
ON foc.order_id = dpo.order_id
group by 1
)

select 
x.customer_id,
coalesce(x.no_of_pincodes,0) as no_of_pincodes,
coalesce(x.count_of_addresses,0) as count_of_addresses,
coalesce(x.no_of_unique_doctors,0) as no_of_unique_doctors
from final x;
