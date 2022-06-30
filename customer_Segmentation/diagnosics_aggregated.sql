SET hive.groupby.orderby.position.alias=true;

DROP TABLE IF EXISTS data_models_temp_tables.oms_diagnostics_aggregated_metrics;
CREATE TABLE data_models_temp_tables.oms_diagnostics_aggregated_metrics AS 
select 
dof.user_id as customer_id,
count(distinct dof.order_id) as no_of_diagnostic_orders,
count(distinct case when dof.status = 'order_completed' then dof.order_id end) as no_of_diagnostic_fulfilled_orders,
sum(case when dof.status = 'order_completed' then dof.order_base_price end) as diagnostic_gmv
from  (select user_id,order_id,status,order_base_price from  data_model.diagnostics_f_order where dt<=date_sub(current_Date,1)) dof 
group by 1;
