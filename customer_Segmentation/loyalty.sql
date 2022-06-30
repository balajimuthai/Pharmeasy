set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;
set hive.auto.convert.join=false;

DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_loyalty;
CREATE TABLE data_models_temp_tables.oms_csrd_loyalty as
with loyalty as 
(
select a.customer_id,
 program_id as current_loyalty_program_id,
variant_id as current_loyalty_variant_id,
 date(starts_at) as current_loyalty_enrollment_date,
date(expires_at) as current_loyalty_expiry_date,
loyalty_doctor_consultation_eligibility as current_loyalty_doctor_consultation_eligibility,
sum(coalesce(program_price,0)) as current_loyalty_program_purchase_price,
sum(coalesce(diag_placed_orders,0)) as current_loyalty_diagnostic_orders,
sum(coalesce(diag_savings,0)) as current_loyalty_diagnostic_savings,
sum(coalesce(med_cashback,0)) as current_loyalty_medicine_cashback_savings,
sum(coalesce(med_fulfilled_orders,0)) as current_loyalty_medicine_orders,
sum(coalesce(med_savings,0))+sum(coalesce(diag_savings,0)) as current_loyalty_savings
from data_model.loyalty_enrolled_data_snapshot a
where expired_flag=0 and dt<current_Date()
group by 1,2,3,4,5,6
),

loyalty_days_to_expiry as (
select a.customer_id,
 datediff(expires_at,current_date()) as current_loyalty_days_to_expiry
from (select customer_id,expired_flag,max(expires_at) as expires_At from 
data_model.loyalty_enrolled_data_snapshot
where dt<current_Date()      
group by 1,2) a
where expired_flag=0
)
select a.customer_id,
loyalty_enrolled,
loyalty_all_program_savings,
loyalty_all_program_orders,
current_loyalty_program_id,
current_loyalty_variant_id,
current_loyalty_enrollment_date,
current_loyalty_expiry_date,
current_loyalty_days_to_expiry,
current_loyalty_program_purchase_price,
current_loyalty_savings,
current_loyalty_medicine_cashback_savings,
current_loyalty_medicine_orders,
current_loyalty_doctor_consultation_eligibility,
current_loyalty_diagnostic_orders,
current_loyalty_diagnostic_savings
from 
(select a.customer_id,
  			case when min(expired_flag)=0 then 'yes' else 'no' end as loyalty_enrolled,
  			sum(coalesce(med_savings,0))+sum(coalesce(diag_savings,0)) as loyalty_all_program_savings,
 			  sum(coalesce(med_placed_orders,0))+sum(coalesce(diag_placed_orders,0)) as loyalty_all_program_orders
 from  data_model.loyalty_enrolled_data_snapshot a
 where dt<current_Date()
 group by 1)a
 left join loyalty b on a.customer_id=b.customer_id
 left join loyalty_days_to_expiry c on c.customer_id=a.customer_id;
