set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;
set hive.auto.convert.join=false;

DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_diag_test_package;
CREATE TABLE data_models_temp_tables.oms_csrd_diag_test_package as
select customer_id, 
	concat_ws(' | ', collect_list(diagnosticTests)) as diagnosticTests,
	concat_ws(' | ', collect_list(diagnosticPackage)) as diagnosticPackage
from 
(
	SELECT dfo.user_id as customer_id, 
		case when ddt.test_type IN ('test','profile') then ddt.test_name end as diagnosticTests,
		case when ddt.test_type = 'package' then ddt.test_name end as diagnosticPackage
	from (Select * from data_model.diagnostics_f_order where dt<current_Date()) dfo
	left join (select * from data_model.diagnostics_d_test where dt<current_Date()) ddt on dfo.order_id=ddt.order_id
	where dfo.status = 'order_completed' 
	group by 1,2,3
) a
group by customer_id
order by customer_id;
