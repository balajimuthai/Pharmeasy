DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_therapy_group;
CREATE TABLE data_models_temp_tables.oms_csrd_therapy_group as
select o.customer_id,
  concat_ws(',',sort_array(collect_list(distinct  cp.system))) as therapy_group
  from (select * from data_model.f_order_ucode where dt<current_Date()) mn
  inner join (select * from data_model.f_order_consumer where dt<current_Date()) o on mn.order_id=o.order_id
  inner join data_model.d_catalog_product cp on mn.ucode=cp.ucode
  where cp.system in ('ANTI-DIABETIC' , 'DERMATOLOGY' ,'GASTROINTESTINAL' , 'CARDIOVASCULAR')
  GROUP by 1;
