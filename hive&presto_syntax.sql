COALESCE(listagg(distinct upper(u.customer_source) ,' |') within group (order by u.customer_source) , 'PHARMEASY') --redshift

coalesce(concat_ws('|',sort_array(collect_list(distinct upper(name)))),'PHARMEASY') as customer_source --hive

listagg(case when payment_group = 'pe_wallet' then '' else payment_group end,' | ') within group (order by payment_group) as payment_method

concat_ws('|',sort_array(collect_list(case when payment_group = 'pe_wallet' then '' else payment_group end)))

FAILED: NullPointerException null(redshift error )
addd coalesce on join key 

create_engine   conn.dispose()
psycopg2.connect conn.close()

-- hue

timestamp(from_unixtime(unix_timestamp(time_column)+19800))
 from_utc_timestamp(column_name, 'IST')
 
 MONTHS_BETWEEN(date(TRUNC(registration_time,'MM')),date(TRUNC(foc.order_placed_date,'MM')))
 
 foc.dt between date_add(last_day(add_months(current_date, -2)),1) and date_Sub(current_Date,1)

date(next_day(date_sub(date_add(last_day(add_months(current_date, -2)),1),7),'MON'))

date(next_day(date_sub(order_placed_date, 7), 'MON'))

percentile_approx(column_name,0.5)

ALTER TABLE logs DROP IF EXISTS PARTITION(dt>'2022-01-01');

set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
SET hive.strict.checks.cartesian.product=false;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;

set hive.auto.convert.join=false;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

set tez.am.resource.memory.mb =2048;
set hive.map.aggr=false;


-- for table locking (run manually on adhoc cluster with this parameter)
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;


import requests
def test():
  msg='success'
  test='{"text":"%s"}' % msg
  response=requests.post('https://hooks.slack.com/services/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',data=test)
  print(response)
  
  
----------------------------------- presto
 date_trunc('Month',(current_date - interval '1' day)) (first day of current_month)
