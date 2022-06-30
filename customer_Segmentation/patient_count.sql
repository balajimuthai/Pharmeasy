DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_patient_count;
CREATE TABLE data_models_temp_tables.oms_csrd_patient_count as 
select customer_id,count(distinct patient_id)  as patient_count from 
(
  select distinct cast(foc.customer_id as string) as customer_id, 
  cast(p.id as string) as patient_id
from (select customer_id,order_id,order_placed_at from data_model.f_order_consumer_snapshot where dt<current_date()) as foc
left join (select * from pe_pe2_pe2.order_image_snapshot where dt<current_Date()) oi on foc.order_id = oi.order_id
left join (Select * from pe_pe2_pe2.image_snapshot where dt<current_Date()) i on i.id = oi.image_id
left join (select * from pe_pe2_pe2.rx_snapshot where dt<current_Date()) r on i.rx_id = r.id
left join (select * from pe_pe2_pe2.patient_snapshot where dt<current_date()) p on r.patient_id = p.id
where date(foc.order_placed_at) <= date_sub(CURRENT_DATE,1)
UNION
select distinct customer_id,
patient_id
from (select customer_id,patient_id
from pe_mongo_rx.patients_snapshot_nrt
where dt <=date_sub(CURRENT_DATE,1))p
)b
group by 1;
