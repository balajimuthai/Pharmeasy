set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
SET hive.strict.checks.cartesian.product=false;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;


DROP TABLE IF EXISTS data_models_temp_tables.oms_derived_customer_outlier_flag;
CREATE TABLE data_models_temp_tables.oms_derived_customer_outlier_flag as
select am.customer_id,
round(aof.average_order_frequency,2) as average_order_frequency,
(case when am.no_of_orders > 20 and aof.average_order_frequency <= 2.00 then 'Outlier' else 'Not_Outlier' end) as customer_outlier_flag
from data_models_temp_tables.oms_csrd_aggregated_metrics am 
left join 
(
select distinct 
am.customer_id,
CAST(ROUND((CAST(datediff(date(lrm.latest_placed_order_time),date(frm.first_placed_order_time)) as float)/CAST(am.count_of_order_placed_dates AS float)),2) AS float) as average_order_frequency
from data_models_temp_tables.oms_csrd_aggregated_metrics am 
left join data_models_temp_tables.oms_csrd_first_related_table frm on am.customer_id = frm.customer_id
left join data_models_temp_tables.oms_csrd_latest_related_final lrm on am.customer_id = lrm.customer_id
)aof
on aof.customer_id = am.customer_id
group by 1,2,3;
