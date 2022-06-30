set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;
set hive.auto.convert.join=false;

DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_derived_metrics_final_merge;
CREATE TABLE data_models_temp_tables.oms_csrd_derived_metrics_final_merge AS
select 
ccdf.customer_id,
date(ccdf.registration_time) as registration_date,
case 
	when am.no_of_delivered_orders is not null then ROUND(CAST(am.mrp_revenue as float)/CAST(am.no_of_delivered_orders as float),2)
	else null 
end as average_mrp_order_value,
datediff(current_date(),date(frm.first_placed_order_time))+1 as days_since_first_order,
datediff(current_date(),date(lrm.latest_placed_order_time)) as days_since_latest_order,
datediff(current_date(),date(lrm.latest_delivered_order_time)) as days_since_latest_delivered_order,
case 
	when no_of_chronic_orders > 0 then true else false 
end as customer_chronic_flag,
datediff(date(frm.first_placed_order_time), date(from_unixtime(unix_timestamp(c.dateadded)+19800))) as registration_to_first_order_days,
case 
	when am.average_order_rating <= 6 then '1_Detractor'
	when am.average_order_rating > 6 and am.average_order_rating <= 8 then '2_Neutral'
	when am.average_order_rating > 8 then '3_Promoter' else null 
end as customer_NPS_bucket,
case when cof.customer_outlier_flag = 'Outlier' then 'Outlier' else 'Not_Outlier' end as customer_outlier_flag,
registration_state,
registration_city,
ML_user_type,
ML_days_since_latest_order,
ML_dormant_flag,
ml_power_customer,
min(cof.average_order_frequency) as average_order_frequency

from (select * from data_models_temp_tables.oms_csrd_customer_data where registration_date < current_Date())ccdf
left join data_models_temp_tables.oms_csrd_aggregated_metrics am on ccdf.customer_id = am.customer_id
left join data_models_temp_tables.oms_csrd_first_related_table  frm on ccdf.customer_id = frm.customer_id
left join data_models_temp_tables.oms_csrd_latest_related_final lrm on ccdf.customer_id = lrm.customer_id
left join (select * from pe_pe2_pe2.customer_snapshot where dt< current_Date()) c on ccdf.customer_id = c.id
left join data_models_temp_tables.oms_derived_customer_outlier_flag cof on ccdf.customer_id = cof.customer_id 
left join data_models_temp_tables.oms_derived_registration_city drcn on drcn.customer_id=ccdf.customer_id
left join data_models_temp_tables.oms_derived_ml_flags dmf on dmf.customer_id=ccdf.customer_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;
