set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
SET hive.strict.checks.cartesian.product=false;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;

DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_customer_data_temp;
CREATE TABLE data_models_temp_tables.oms_csrd_customer_data_temp as
select 
  	cast(c.id as string) as customer_id,
	is_email_verified,
	case when c.platform in ('ANDROID','Android_App','android') then 'android'
	when c.platform in ('iOS_App','IOS') then 'ios'
	when c.platform in ('DWEB','MWEB','Website','Mobile_Website','mweb','web') then 'web'
	when c.platform in ('ASSISTED_SALES_WEB','assisted-sales-web','assisted-sales-mweb') then 'assisted_sales'
	when c.platform in ('ORDER_ON_CALL','Order_On_Call','order-on-call') then 'order-on-call'
	when c.platform in ('Third_Party_API') then 'third_party_api'
	else 'others' end as platform,
	from_utc_timestamp(c.dateadded, 'IST') as registration_time,
	customer_source,
	case 
		when t.flag_id=78 then 'ML'
		when t.flag_id=79 then 'PE+ML'
		else 'PE'
	end as tenant,
	s.name as state_name,
	cast(null as int) as chronic_intent,
	cast(null as int) as acute_intent,
	date(from_utc_timestamp(c.dateadded, 'IST')) as registration_date
	
  from (select * from pe_pe2_pe2.customer_snapshot where dt>=date_sub(current_Date,11)) c
  left join (select * from pe_pe2_pe2.customer_register_info_snapshot where dt<=date_sub(current_Date,1))cri on c.id=cri.customer_id
  left join pe_pe2_pe2.city_snapshot ct on cri.city_id=ct.id
  left join pe_pe2_pe2.state_snapshot  s on ct.state_id=s.id
  left join
  (
	select customer_id,flag_id from pe_pe2_pe2.customer_flags_snapshot
	where flag_id in (78,79) and skull_opcode !='D' and dt<=date_sub(current_Date,1)
	group by 1,2
  )t on c.id=t.customer_id
  where c.id not in (SELECT DISTINCT customer_id FROM pe_pe2_pe2.customer_flags_snapshot cf  where flag_id =71);
  
  

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=1000;
insert overwrite table  data_models_temp_tables.oms_csrd_customer_data partition (`registration_date`)
select 
customer_id,
is_email_verified,
platform,
registration_time,
customer_source,
tenant,
state_name,
chronic_intent,
acute_intent,
registration_date
from data_models_temp_tables.oms_csrd_customer_data_temp
where registration_date>=date_sub(current_date,10);
