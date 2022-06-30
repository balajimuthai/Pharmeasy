DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_payment_at_delivery;
CREATE TABLE data_models_temp_tables.oms_csrd_payment_at_delivery as
select customer_id,
count(distinct case when payment_method in ('cash','mswipe','mosambee') then order_id end) as payment_at_delivery_orders
from (
select customer_id,order_id,
  concat_ws('',sort_array(collect_list(case when payment_group = 'pe_wallet' then '' else payment_group end ))) as payment_method
  --listagg(case when payment_group = 'pe_wallet' then '' else payment_group end,' | ') within group (order by payment_group) as payment_method
from (select customer_id,order_id,payment_group,order_status_id from(
SELECT pt.aggregator_transaction_id,
pt.payment_method_option,
pt.updated_at as transaction_date,
pt.amount as amount_paid,
e1.value as payment_group,
foc.order_id,
foc.customer_id,
foc.order_status_id
from pe_payments_payments.payments_transactions_snapshot pt
inner join pe_payments_payments.enums_snapshot e1
on e1.key = pt.payment_method and e1.column_name = 'payment_method' and e1.table_name = 'payments_transactions'
inner join pe_payments_payments.payments_orders_snapshot po
on cast(po.id as string) = cast(pt.payments_order_id as string)
inner join pe_order_payments_pe_db_order_payments.order_payments_payments_snapshot opp
ON opp.pg_order_id = po.caller_order_id
inner join pe_order_payments_pe_db_order_payments.order_payments_external_orders_snapshot opeo
ON cast(opeo.id as string) = cast(opp.external_order_id as string)
inner join (select * from data_model.f_order_consumer where dt<current_Date()) foc
on opeo.vendor_order_id = foc.order_id
where pt.status = 4
and date(foc.order_placed_at) <= CURRENT_DATE
and pt.is_refund = FALSE) as a group by 1,2,3,4) as b
where order_status_id in (9,10)
group by 1,2) as x
group by 1;
