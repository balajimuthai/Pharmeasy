DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_preferred_payement;
CREATE TABLE data_models_temp_tables.oms_csrd_preferred_payement as 
with payment as
(
  SELECT foc.customer_id,
cast(foc.order_id as string) as order_id,
date(foc.order_placed_at) as order_placed_date,
pt.aggregator_transaction_id,
pt.payment_method_option,
pt.updated_at as transaction_date,
pt.amount as amount_paid,
pt.payment_method_option_details_card_type,
pt.payment_method_option_details_using_saved_card,
e1.value as payment_group,
pt.status
from (select * from pe_payments_payments.payments_transactions_snapshot where dt<current_Date()) pt
inner join pe_payments_payments.enums_snapshot e1 on e1.key = pt.payment_method and e1.column_name = 'payment_method' and e1.table_name = 'payments_transactions'
inner join (select * from pe_payments_payments.payments_orders_snapshot where dt<current_date())po on po.id = pt.payments_order_id
inner join (select * from pe_order_payments_pe_db_order_payments.order_payments_payments_snapshot where dt<current_Date()) opp ON opp.pg_order_id = po.caller_order_id
inner join (select * from pe_order_payments_pe_db_order_payments.order_payments_external_orders_snapshot where dt<current_Date()) opeo ON opeo.id = opp.external_order_id
inner join (select * from data_model.f_order_consumer where dt<current_Date()) foc on opeo.vendor_order_id = foc.order_id
where pt.is_refund = FALSE
and date(foc.order_placed_at) <= date_sub(current_date,1) 
),
base_table as
(
select 
pt.pg_reference_id as payment_transaction_id,
e.value as payment_gateway,
'Online' as payment_method,
po.vendor_order_id as pe_order_id, 
date(from_utc_timestamp(pt.updated_at, 'IST')) as transaction_date,
pt.amount as amount_paid,
pt.status,
foc.customer_id,
e.value as payment_group,
foc.order_placed_date
from pe_payments_online_payment.payment_transaction_snapshot pt
inner join pe_payments_online_payment.payment_order_snapshot po on po.id =pt.payment_order_id 
inner join data_model.f_order_consumer foc on po.vendor_order_id =foc.order_id 
left join pe_payments_online_payment.enums_snapshot e on e.table_name = 'payment_transaction' and e.column_name = 'sub_payment_gateway' and e.key = pt.sub_payment_gateway
where pt.status =4
and foc.order_placed_date >='2022-02-25'
),
payment_details as
(
SELECT customer_id,
order_id,
order_placed_date,
aggregator_transaction_id,
payment_method_option,
transaction_date,
amount_paid,
payment_method_option_details_card_type,
payment_method_option_details_using_saved_card,
payment_group,
status
from payment
union 
select 
customer_id,
pe_order_id,
order_placed_date,
payment_transaction_id,
payment_method,
transaction_date,
amount_paid,
null as payment_method_option_details_card_type,
null as payment_method_option_details_using_saved_card,
payment_group,
status
from base_table
),

customer_payment_pref as
(
  select customer_id,
payment_group,
max(transaction_date) as max_trans_date,
count(distinct aggregator_transaction_id) as payment_count,
row_number() over (partition by customer_id order by count(distinct aggregator_transaction_id) desc,max(transaction_date) desc) as trans_rank
from payment_details
where status = 4
and payment_group != 'pe_wallet'
group by 1,2
),
customer_card_pref as
(
  select customer_id,
NVL(upper(payment_method_option),'') || ' - ' || NVL(payment_method_option_details_card_type,'') as card_option_type,
max(transaction_date) as max_trans_date,
count(distinct aggregator_transaction_id) as payment_count,
row_number() over (partition by customer_id order by count(distinct aggregator_transaction_id) desc,max(transaction_date) desc) as card_rank
from payment_details
where payment_group = 'card'
and status = 4
group by 1,2
),

customer_wallet_pref as
(
  select customer_id,
upper(payment_method_option) as wallet_option,
max(transaction_date) as max_trans_date,
count(distinct aggregator_transaction_id) as payment_count,
row_number() over (partition by customer_id order by count(distinct aggregator_transaction_id) desc,max(transaction_date) desc) as wallet_rank
from payment_details
where payment_group = 'wallet'
and status = 4
group by 1,2
),

customer_netbanking_pref as
(
  select customer_id,
upper(payment_method_option) as netbanking_option,
max(transaction_date) as max_trans_date,
count(distinct aggregator_transaction_id) as payment_count,
row_number() over (partition by customer_id order by count(distinct aggregator_transaction_id) desc,max(transaction_date) desc) as netbanking_rank
from payment_details
where payment_group = 'netbanking'
and status = 4
group by 1,2
)
select a.customer_id,
a.payment_group as preferred_payment_method,
b.card_option_type as preferred_card_option,
c.wallet_option as preferred_wallet_option,
d.netbanking_option as preferred_netbanking_option
from (select * from customer_payment_pref where trans_rank = 1) as a
left join (select * from customer_card_pref where card_rank = 1) as b
on a.customer_id = b.customer_id
left join (select * from customer_wallet_pref where wallet_rank = 1) as c
on a.customer_id = c.customer_id
left join (select * from customer_netbanking_pref where netbanking_rank = 1) as d
on a.customer_id = d.customer_id;
