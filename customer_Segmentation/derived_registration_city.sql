set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;
set hive.auto.convert.join=false;


DROP TABLE IF EXISTS data_models_temp_tables.oms_derived_registration_city;
CREATE TABLE data_models_temp_tables.oms_derived_registration_city as
with customer_city_state  as
(
select
frm.customer_id,
frm.first_placed_order_state,
frm.first_placed_order_city
from data_models_temp_tables.oms_csrd_first_related_table frm
),

customer_registration_city_data as
(
select customer_id, registration_city, registration_state, platform 
from
(
select
c.customer_id,
case when ct.name is not null then ct.name else t1.first_placed_order_city end as registration_city,
case when s.name is not null then s.name else t1.first_placed_order_state end as registration_state,
platform
from (Select * from data_models_temp_tables.oms_csrd_customer_data where registration_date <current_date()) c
left join (select * from pe_pe2_pe2.customer_register_info_snapshot where dt<current_Date()) cri on c.customer_id=cri.customer_id
left join customer_city_state t1 on c.customer_id=t1.customer_id
left join pe_pe2_pe2.city_snapshot ct on cri.city_id=ct.id
left join pe_pe2_pe2.state_snapshot s on ct.state_id=s.id
GROUP BY 1,2,3,4
)x
where registration_city is not null or registration_state is not null
),

support_table as
(
	select a.customer_id from customer_registration_city_data a 
	inner join (Select * from data_models_temp_tables.oms_csrd_customer_data where registration_date <current_Date()) b on a.customer_id=b.customer_id
	where a.platform in ('mweb','web','order-on-call') and registration_city='Mumbai'
),

m_web_1  as
(
select 
a.customer_id,
a.first_placed_order_city
from customer_city_state a
inner join support_table b on a.customer_id=b.customer_id
),

m_web_2  as 
(
select
c.customer_id,
cri.city_name
from (select * from data_models_temp_tables.oms_csrd_customer_data where registration_date <current_Date()) c
inner join (select * from pe_pe2_pe2.customer_register_info_snapshot where dt<current_Date()) cri on c.customer_id =cri.customer_id
inner join pe_pe2_pe2.city_snapshot c2 on lower(cri.city_name)=LOWER(c2.`name`)
where c.platform in ('web','mweb') and date(c.registration_time) >= '2020-04-01' and city_name is not null
),

registration_city_new  as
(
select
customer_id,
case when platform in ('mweb','web') and mis_city_mapping is null then city_name else mis_city_mapping end as new_registration_city
from
(
select
a.customer_id,
a.platform,
(case when a.platform in ('mweb','web','order-on-call') and crcd.registration_city='Mumbai'
then mw1.first_placed_order_city else crcd.registration_city
end) as mis_city_mapping,
mw2.city_name
from (select * from data_models_temp_tables.oms_csrd_customer_data where registration_date <current_Date()) a
left join customer_registration_city_data  crcd on a.customer_id =crcd.customer_id
left join m_web_1  mw1 on a.customer_id = mw1.customer_id
left join m_web_2  mw2 on a.customer_id = mw2.customer_id
)x
)

select a.customer_id, 
registration_state,
rcn.new_registration_city as registration_city
from (select * from data_models_temp_tables.oms_csrd_customer_data where registration_date <current_Date()) a
left join registration_city_new rcn on rcn.customer_id=a.customer_id
left join customer_registration_city_data b on b.customer_id=a.customer_id
group by 1,2,3;
