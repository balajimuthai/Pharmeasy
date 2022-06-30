DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_wallet_promotional;
CREATE TABLE data_models_temp_tables.oms_csrd_wallet_promotional as 
with balance as 
(
SELECT uwm.customer_id,(COALESCE(expirable_balance,0)+COALESCE(non_expirable_balance,0)) as balance
FROM  pe_wallets_wallet_service.wallet_snapshot o
inner join pe_pe2_pe2.wallet_customer_lookup_snapshot uwm on uwm.wallet_id=o.id and uwm.dt<=date_sub(current_Date,1)
where o.dt<=date_sub(current_Date,1)	
group by 1,2
),

promo_trans_cash as 
(
SELECT uwm.user_id as customer_id, o.expirable_balance as promotional_cash,
	o.non_expirable_balance as transactional_cash
FROM pe_wallets_wallet_service.wallet_snapshot o
INNER join pe_order_payments_pe_db_order_payments.order_payments_user_wallet_mappings_snapshot uwm on uwm.wallet_id=o.id and uwm.dt<=date_sub(current_Date,1)
where o.dt<=date_sub(current_Date,1)	
group by 1,2,3
)

select c.customer_id,balance,promotional_cash,
transactional_cash 
from balance b 
full outer join promo_trans_cash c on c.customer_id=b.customer_id;
