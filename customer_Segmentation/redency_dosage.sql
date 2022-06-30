drop table if EXISTS data_models_temp_tables.oms_redency_dosage;
create table data_models_temp_tables.oms_redency_dosage as
select a.customer_id,
min(date(a.ideal_next_order_date)) as ideal_next_order_date_min,
max(date(a.ideal_next_order_date)) as ideal_next_order_date_max,
date(max(date_add(to_date(ideal_next_order_date),datediff(to_date(ideal_next_order_date),latest_order_date)))) as ideal_next_order_date_2_max,
min(case when dcp.is_chronic = 1 then date(a.ideal_next_order_date) end) as ideal_next_order_date_min_chronic,
max(case when dcp.is_chronic = 1 then date(a.ideal_next_order_date) end) as ideal_next_order_date_max_chronic,
date(max(case when dcp.is_chronic = 1 then date_add(to_date(ideal_next_order_date),datediff(to_date(ideal_next_order_date),latest_order_date))end)) as ideal_next_order_date_2_max_chronic
from pre_analytics.customer_medicine_predictor_combined  as a
left join data_model.d_catalog_product as dcp
on a.ucode = dcp.ucode
group by 1;
