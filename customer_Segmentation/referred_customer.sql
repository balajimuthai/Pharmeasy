DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_is_referred_customer;
CREATE TABLE data_models_temp_tables.oms_csrd_is_referred_customer as 
select
        used_by_id as customer_id
        from pe_pe2_pe2.used_referral_snapshot 
        where dt<current_Date()
        group by 1;
