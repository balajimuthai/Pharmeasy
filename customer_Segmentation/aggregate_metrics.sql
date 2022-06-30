set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set mapreduce.reduce.memory.mb = 10096 ;
SET hive.groupby.orderby.position.alias=true;
SET hive.strict.checks.cartesian.product=false;
set hive.tez.container.size=10192;
set hive.tez.java.opts = -Xmx13312m;
set hive.auto.convert.join=false;


DROP TABLE IF EXISTS data_models_temp_tables.oms_csrd_aggregated_metrics;
CREATE TABLE data_models_temp_tables.oms_csrd_aggregated_metrics as
with f_order_consumer_total as 
(
select foc.customer_id,
foc.order_id,
foc.dt,
foc.order_status_id,
foc.order_status,
foc.order_placed_date,
foc.order_placed_at,
Foc.chronic_flag, 
foc.fulfilled_discount_percentage,
foc.order_type,
foc.order_app_version as current_app_version,
UPPER(foc.order_source||' '||foc.order_app_os_version) as current_app_os,
UPPER(order_phone_build) as current_phone_details,
foc.fulfilled_mrp,
foc.fulfilled_discounted_mrp,
foc.rating,
foc.cancel_reason,
foc.delivery_address_id,
foc.delivery_city_name,
foc.delivery_pincode,
foc.delivery_state,
foc.supplier_city_name,
sop.cash_on_delivery, 
sop.paid_via_wallet, 
sop.paid_online, 
sop.card_at_delivery, 
sop.cash_less_amount
from data_model.f_order_consumer foc 
left join 
(select order_id, cash_on_delivery, paid_via_wallet, paid_online, card_at_delivery, cash_less_amount 
from data_model.sale_order_payment where dt <= date_sub(current_date(),1))sop 
on sop.order_id = foc.order_id
where foc.dt <= date_sub(current_date(),1)
),

customer_segments_order_info AS
(
select
ord.customer_id,
count(distinct order_placed_date) as no_of_unique_order_placed_dates,
count(distinct case when chronic_flag = 1 then ord.order_id end) as no_of_chronic_orders, 
count(distinct case when ord.order_status in ('ORDER COMPLETE' , 'ORDER COMPLETE - PARTIAL') and chronic_flag = 1 then ord.order_id end) as no_of_chronic_orders_delivered,
count(distinct ord.order_id) as no_of_orders,
count(distinct case when ord.order_status in ('ORDER COMPLETE' , 'ORDER COMPLETE - PARTIAL') then ord.order_id end) as no_of_delivered_orders,
count(distinct case when ord.order_status in ('ORDER COMPLETE') then ord.order_id end) as no_of_completely_delivered_orders,
count(distinct case when ord.order_status in ('ORDER COMPLETE - PARTIAL') then ord.order_id end) as no_of_partially_delivered_orders,
count(distinct case when (dpo.doctor_name is null or length(dpo.doctor_name) <= 3) then null else dpo.doctor_name end) as no_of_unique_doctors,
count(distinct case when cash_on_delivery > 0 then ord.order_id end) as Payment_Mode_COD,
count(distinct case when paid_via_wallet > 0 then ord.order_id end) as Payment_Mode_Paid_via_wallet,
count(distinct case when paid_online > 0 then ord.order_id end) as Payment_Mode_Paid_online,
count(distinct case when card_at_delivery > 0 then ord.order_id end) as Payment_Mode_card_at_delivery,
count(distinct case when cash_less_amount > 0 then ord.order_id end) as Payment_Mode_cash_less_amount
from f_order_consumer_total ord
LEFT JOIN (select order_id, doctor_name from data_model.f_doctor_program_order where dt <= date_sub(current_date(),1))dpo
ON ord.order_id = dpo.order_id
group by 1
),



customer_segment_disease_info2 AS
(
select ord.customer_id,
count(distinct case when dcp.therapy = 'CARDIAC' then ord.order_id end) as no_of_orders_cardiac,
count(distinct case when dcp.therapy = 'ANTI DIABETIC' then ord.order_id end) as no_of_orders_antidiabetic,
count(distinct case when dcp.therapy = 'VITAMINS & NUTRITIONAL SUPPLEMENTS' then ord.order_id end) as no_of_orders_Vitamins_and_Supplements,
count(distinct case when dcp.therapy = 'GASTROINTESTINAL' then ord.order_id end) as no_of_orders_gastrointestinal,
count(distinct case when dcp.therapy = 'NERVOUS SYSTEM' then ord.order_id end) as no_of_orders_nervous_system,
count(distinct case when dcp.therapy = 'PAIN MANAGEMENT' then ord.order_id end) as no_of_orders_pain_management,
count(distinct case when dcp.therapy = 'GYNAECOLOGY' then ord.order_id end) as no_of_orders_gynaecology,
count(distinct case when dcp.therapy = 'RESPIRATORY' then ord.order_id end) as no_of_orders_respiratory,
count(distinct case when dcp.therapy = 'BLOOD RELATED' then ord.order_id end) as no_of_orders_blood_related,
count(distinct case when dcp.therapy = 'DERMATOLOGICAL' then ord.order_id end) as no_of_orders_dermatological
from f_order_consumer_total ord
left join (select distinct 
order_id,ucode from data_model.integrated_f_order_ucode)fou on ord.order_id = fou.order_id
left join data_model.d_catalog_product dcp on fou.ucode = dcp.ucode
group by 1
),


customer_segments_revenue_info AS
(
select foc.customer_id, 
ROUND(cast(sum(fulfilled_mrp) as float),2) as MRP_Revenue, 
ROUND(cast(sum(fulfilled_discounted_mrp) as float),2) as Discounted_Revenue
from f_order_consumer_total foc
group by 1
),


customer_segment_cnr_info AS
(
select foc.customer_id,
count(distinct case when foc.cancel_reason in ('ORDERED BY MISTAKE','CUSTOMER WANTS ENTIRE ORDER OR NOTHING','CUSTOMER FOUND MEDICINES TOO COSTLY','OTHER',
'CUSTOMER ENTERED WRONG INFORMATION','ORDER PLACED ON WRONG ADDRESS','ITEM NOT IN STOCK','INCORRECT ADDRESS SELECTED','I WANT TO MODIFY ITEMS IN MY ORDER',
'CUSTOMER CANCELLED ON CALL','FORGOT TO APPLY COUPON','PLACED A NEW ORDER','NO LONGER NEED THE MEDICINE','WANTS MORE DISCOUNT','JUST TRYING THE APP',
'FACING PAYMENT RELATED ISSUES','BOUGHT ITEM FROM OUTSIDE','PHARMEASY WALLET BALANCE ISSUE','DELAY IN ORDER','DID NOT APPLY PROMO CODE') then foc.order_id end) 
  as cancel_reason_Cancelled_by_Customer,
count(distinct case when foc.cancel_reason in ('NOT READY TO PAY CONVENIENCE CHARGES','WRONG PINCODE','DELAY IN DISPATCH','CANCELLED AT DELIVERY DUE TO IMPROPER PRESCRIPTION',
'AREA CURRENTLY UNSERVICEABLE','DAMAGED MEDICINE DELIVERED','INCOMPLETE ORDER DELIVERED','CUSTOMER DID NOT HAVE CHANGE','OUT OF SERVICE AREA',
'WRONG MEDICINE DELIVERED','MEDICINE NOT DELIVERABLE IN THIS CITY','CANCELLED AT DELIVERY','ESTIMATED DELIVERY IS LATE') then foc.order_id end) as cancel_reason_Delivery_Issue,
count(distinct case when foc.cancel_reason in ('CUSTOMER ALREADY PLACED A NEW ORDER WITH A VALID RX','CUSTOMER NOT RECEIVING CALL',
'CUSTOMER GOT A NEW RX FROM OWN DOCTOR','ORDER CANCELLED BY USER',"DOCTOR CAN'T WRITE NEW RX WITHOUT PHYSICAL EXAMINATION",'DOCTOR CANNOT PRESCRIBE MEDICINE OVER CALL',
'CUSTOMER REFUSED CONSULTATION','CUSTOMER ALREADY BOUGHT MEDICINE') then foc.order_id end) as cancel_reason_Doctor_Teleconsultation_Issue,
count(distinct case when foc.cancel_reason in ('FAKE OR TEST ORDER') then foc.order_id end) as cancel_reason_Fake_Order,
count(distinct case when foc.cancel_reason in ('CUSTOM MEDICINE UNAVAILABLE','CUSTOMER WANTS SUBSTITUTE','DOSAGE COMPLETED/EXPIRED','MEDICINE UNAVAILABLE',
'BANNED MEDICINE','CUSTOMER WANTS LOCAL MEDICINES','CANNOT PROVIDE REQUESTED QUANTITY','PACKAGING ISSUE','CUSTOMER WANTS AYURVEDIC MEDICINES','SCHEDULE X/H1',
'NOT A PRESCRIPTION MEDICINE','DOCTOR CHANGED MEDICINES') then foc.order_id end) as cancel_reason_Medicine_Issue,
count(distinct case when foc.cancel_reason in ('PRESCRIPTION DID NOT MATCH','UPLOADED WRONG PRESCRIPTION','PRESCRIPTION TOO OLD','INVALID PRESCRIPTION',
'MEDICINE NOT MENTIONED IN THE PRESCRIPTION','CUSTOMER REFUSED PRESCRIPTION PICKUP','FRAUD PRESCRIPTION','DELAY IN PRESCRIPTION PICKUP',
'CUSTOMER DID NOT HAVE PRESCRIPTION','DOCTOR NOT ALLOWED TO PRESCRIBE ALLOPATHIC','PHOTOCOPY OF PRESCRIPTION PROVIDED') then foc.order_id end)
  as cancel_reason_Prescription_Issue,
count(distinct case when foc.cancel_reason in ('IMAGE IS INVALID','PRESCRIPTION PHOTO UNCLEAR','IMAGE OF MEDICINES') then foc.order_id end) as cancel_reason_Image_Issue,
count(distinct case when foc.cancel_reason in ('INVALID PHONE NUMBER','PROFILE BLOCKED','CHANGE DELIVERY DATE','INCORRECT DIGITIZATION','ORDER IS DUPLICATE',
'INVALID ADDRESS','CUSTOMER NOT READY TO PAY ONLINE (COURIER ORDER)','REJECTED BY DOCTOR','TEMPORARY REJECTION - INVENTORY MANAGEMENT','OTC MP UNSERVICEABLE AREA',
'CANT PROVIDE REFRIGERATED PRODUCT (COURIER ORDER)','DUPLICATE ORDER - CORPORATE ORDER','SYSTEM ERROR IN ORDER','CORPORATE WALLET ISSUE','SHIPMENT LOST AND DAMAGED',
'DOCTOR CONSULTATION SUCCESSFUL, ORDER WILL BE PROCESSED AGAIN','CUSTOMER NOT READY TO PROVIDE QUANTITY','FULFILMENT SYSTEM REJECTED','CUSTOMER NOT REACHABLE'
) then foc.order_id end) as cancel_reason_Others
from f_order_consumer_total foc
group by 1
),


customer_segment_rating_info as
(
select
rating.customer_id,
rating.average_order_rating,
case when rating.average_order_rating <= 6 then '1_Detractor'
when rating.average_order_rating > 6 and rating.average_order_rating <= 8 then '2_Neutral'
when rating.average_order_rating > 8 then '3_Promoter' else null end as customer_NPS_bucket
from
(select foc.customer_id,
case when count(distinct case when foc.order_status in ('ORDER COMPLETE' , 'ORDER COMPLETE - PARTIAL') and foc.rating is not null then foc.order_id end) = 0 then null
else (sum(case when foc.order_status in ('ORDER COMPLETE' , 'ORDER COMPLETE - PARTIAL') and foc.rating is not null then foc.rating else 0 end)/CAST(count(distinct case when foc.order_status in ('ORDER COMPLETE' , 'ORDER COMPLETE - PARTIAL') and foc.rating is not null then foc.order_id end) AS float))end as average_order_rating
from f_order_consumer_total foc
group by 1) as rating
group by 1,2,3
),

doctor_consultation as
(
select
foc.customer_id,
count(distinct case when foc.order_id=cast(dpo.order_id as string)  then foc.order_id end) as Doctor_Consultation_Pitched,
count(distinct dpo.order_id) as Opted_For_Doctor_Consulation ,
count(distinct case when latest_case_status in (4) then dpo.order_id end) as Successful_Doctor_Consultation,
count(distinct case when latest_case_status in (4) and foc.order_status in ('ORDER COMPLETE' , 'ORDER COMPLETE - PARTIAL') then dpo.order_id end) as Order_Delivered_After_Doctor_Consulation
from f_order_consumer_total foc 
left join 
(select order_id,latest_case_status from data_model.f_doctor_program_order 
where dt <= date_sub(current_date(),1)
)dpo 
on foc.order_id =dpo.order_id
group by 1
),

count_of_order_placed_dates as
(
select foc.customer_id,
count(distinct foc.order_placed_date) as count_of_order_placed_dates
from f_order_consumer_total foc
group by 1
),

final as
(
select 
csoi.customer_id,
csoi.no_of_orders,
csoi.no_of_delivered_orders,
csoi.no_of_partially_delivered_orders,
csoi.no_of_completely_delivered_orders,
csoi.no_of_chronic_orders, 
csoi.no_of_chronic_orders_delivered,
csoi.Payment_Mode_COD,
csoi.Payment_Mode_Paid_via_wallet,
csoi.Payment_Mode_Paid_online,
csoi.Payment_Mode_card_at_delivery,
csoi.Payment_Mode_cash_less_amount,
csoi.no_of_unique_order_placed_dates,
csdi.no_of_orders_cardiac,
csdi.no_of_orders_antidiabetic,
csdi.no_of_orders_vitamins_and_supplements,
csdi.no_of_orders_gastrointestinal,
csdi.no_of_orders_nervous_system,
csdi.no_of_orders_pain_management,
csdi.no_of_orders_gynaecology,
csdi.no_of_orders_respiratory,
csdi.no_of_orders_blood_related,
csdi.no_of_orders_dermatological,
csri.mrp_revenue,
csri.discounted_revenue,
cscnri.cancel_reason_cancelled_by_customer,
cscnri.cancel_reason_delivery_issue,
cscnri.cancel_reason_doctor_teleconsultation_issue,
cscnri.cancel_reason_fake_order,
cscnri.cancel_reason_medicine_issue,
cscnri.cancel_reason_prescription_issue,
cscnri.cancel_reason_image_issue,
cscnri.cancel_reason_others,
csrai.average_order_rating,
dc.Doctor_Consultation_Pitched,
dc.Opted_For_Doctor_Consulation,
dc.Successful_Doctor_Consultation,
dc.Order_Delivered_After_Doctor_Consulation,
coopd.count_of_order_placed_dates
from customer_segments_order_info csoi
left join customer_segments_revenue_info csri on csoi.customer_id = csri.customer_id
left join customer_segment_disease_info2 csdi on csoi.customer_id = csdi.customer_id
left join customer_segment_CnR_info cscnri on csoi.customer_id = cscnri.customer_id
left join customer_segment_rating_info csrai on csoi.customer_id = csrai.customer_id
left join doctor_consultation dc on csoi.customer_id = dc.customer_id
left join count_of_order_placed_dates coopd on csoi.customer_id = coopd.customer_id
order by 1
)

select
customer_id,
case when no_of_orders is NULL then 0 else no_of_orders end as no_of_orders,
case when no_of_delivered_orders is NULL then 0 else no_of_delivered_orders end as no_of_delivered_orders,
case when no_of_partially_delivered_orders is NULL then 0 else no_of_partially_delivered_orders end as no_of_partially_delivered_orders,
case when no_of_completely_delivered_orders is NULL then 0 else no_of_completely_delivered_orders end as no_of_completely_delivered_orders,
case when no_of_chronic_orders is NULL then 0 else no_of_chronic_orders end as no_of_chronic_orders, 
case when no_of_chronic_orders_delivered is NULL then 0 else no_of_chronic_orders_delivered end as no_of_chronic_orders_delivered,
--case when no_of_healthcare_orders is NULL then 0 else no_of_healthcare_orders end as no_of_healthcare_orders,
--case when med_fulfilled_orders_with_promo_code is NULL then 0 else med_fulfilled_orders_with_promo_code end as med_fulfilled_orders_with_promo_code,
case when payment_mode_cod is null then 0 else payment_mode_cod end as payment_mode_cod,
case when payment_mode_paid_via_wallet is null then 0 else payment_mode_paid_via_wallet end as payment_mode_paid_via_wallet,
case when payment_mode_paid_online is null then 0 else payment_mode_paid_online end as payment_mode_paid_online,
case when payment_mode_card_at_delivery is null then 0 else payment_mode_card_at_delivery end as payment_mode_card_at_delivery,
case when payment_mode_cash_less_amount is null then 0 else payment_mode_cash_less_amount end as payment_mode_cash_less_amount,
case when no_of_unique_order_placed_dates is NULL then 0 else no_of_unique_order_placed_dates end as no_of_unique_order_placed_dates,
case when no_of_orders_cardiac is NULL then 0 else no_of_orders_cardiac end as no_of_orders_cardiac,
case when no_of_orders_antidiabetic is NULL then 0 else no_of_orders_antidiabetic end as no_of_orders_antidiabetic,
case when no_of_orders_vitamins_and_supplements is NULL then 0 else no_of_orders_vitamins_and_supplements end as no_of_orders_vitamins_and_supplements,
case when no_of_orders_gastrointestinal is NULL then 0 else no_of_orders_gastrointestinal end as no_of_orders_gastrointestinal,
case when no_of_orders_nervous_system is NULL then 0 else no_of_orders_nervous_system end as no_of_orders_nervous_system,
case when no_of_orders_pain_management is NULL then 0 else no_of_orders_pain_management end as no_of_orders_pain_management,
case when no_of_orders_gynaecology is NULL then 0 else no_of_orders_gynaecology end as no_of_orders_gynaecology,
case when no_of_orders_respiratory is NULL then 0 else no_of_orders_respiratory end as no_of_orders_respiratory,
case when no_of_orders_blood_related is NULL then 0 else no_of_orders_blood_related end as no_of_orders_blood_related,
case when no_of_orders_dermatological is NULL then 0 else no_of_orders_dermatological end as no_of_orders_dermatological,
mrp_revenue,
discounted_revenue,
case when cancel_reason_cancelled_by_customer is null then 0 else cancel_reason_cancelled_by_customer end as cancel_reason_cancelled_by_customer,
case when cancel_reason_delivery_issue is null then 0 else cancel_reason_delivery_issue end as cancel_reason_delivery_issue,
case when cancel_reason_doctor_teleconsultation_issue is null then 0 else cancel_reason_doctor_teleconsultation_issue end as cancel_reason_doctor_teleconsultation_issue,
case when cancel_reason_fake_order is null then 0 else cancel_reason_fake_order end as cancel_reason_fake_order,
case when cancel_reason_medicine_issue is null then 0 else cancel_reason_medicine_issue end as cancel_reason_medicine_issue,
case when cancel_reason_prescription_issue is null then 0 else cancel_reason_prescription_issue end as cancel_reason_prescription_issue,
case when cancel_reason_image_issue is null then 0 else cancel_reason_image_issue end as cancel_reason_image_issue,
case when cancel_reason_others is null then 0 else cancel_reason_others end as cancel_reason_others,
average_order_rating,
case when doctor_consultation_pitched is null then 0 else doctor_consultation_pitched end as doctor_consultation_pitched,
case when opted_for_doctor_consulation is null then 0 else opted_for_doctor_consulation end as opted_for_doctor_consulation,
case when successful_doctor_consultation is null then 0 else successful_doctor_consultation end as successful_doctor_consultation,
case when order_delivered_after_doctor_consulation is null then 0 else order_delivered_after_doctor_consulation end as order_delivered_after_doctor_consulation,
case when count_of_order_placed_dates is null then 0 else count_of_order_placed_dates end as count_of_order_placed_dates
from final
