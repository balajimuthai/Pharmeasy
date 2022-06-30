DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_total_reffered;
CREATE TABLE data_models_temp_tables.oms_csrd_total_reffered as 
select
referred_by_id as customer_id,
count(*) as total_referred_customer
from pe_pe2_pe2.used_referral_snapshot a
where dt<current_Date()
group by 1;
