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

DROP TABLE IF EXISTS data_models_temp_tables.oms_customer_segmentation_raw_data_final_merge;
CREATE TABLE data_models_temp_tables.oms_customer_segmentation_raw_data_final_merge as
select distinct
c.customer_id,
c.is_email_verified,
c.platform,
c.registration_time,
c.tenant,
case when cci.customer_id is not null then 1 else 0 end as chronic_intent,
case when cai.customer_id is not null then 1 else 0 end as acute_intent,
cci.chronic_intent_date,
csrd1.first_placed_order_time,
csrd1.first_delivered_order_time,
csrd1.first_placed_order_id,
csrd1.first_delivered_order_id,
csrd1.first_delivered_order_disc_percentage,
csrd1.first_delivered_order_supplier_city,
csrd1.fdo_disc_percentage_split,
csrd1.first_power_tagged_time,
csrd2.current_app_version,
csrd2.current_app_os,
csrd2.current_phone_details,
csrd2.latest_order_rating,
csrd2.latest_placed_order_time,
csrd2.latest_placed_order_id,
csrd2.latest_delivered_order_time,
csrd2.latest_delivered_order_id,
csrd2.latest_rated_order_id,
csrd2.latest_delivered_order_issue_flag,
case when csrd2.latest_order_discount is null then 0 else csrd2.latest_order_discount end as latest_order_discount,
case when csrd3.no_of_orders is null then 0 else csrd3.no_of_orders end as no_of_orders,
case when csrd3.no_of_delivered_orders is null then 0 else csrd3.no_of_delivered_orders end as no_of_delivered_orders,
case when csrd3.no_of_completely_delivered_orders is null then 0 else csrd3.no_of_completely_delivered_orders end as no_of_completely_delivered_orders,
case when csrd3.no_of_chronic_orders is null then 0 else csrd3.no_of_chronic_orders end as no_of_chronic_orders,
case when csrd3.no_of_chronic_orders_delivered is null then 0 else csrd3.no_of_chronic_orders_delivered end as no_of_chronic_orders_delivered,
case when csrd3.payment_mode_cod is null then 0 else csrd3.payment_mode_cod end as payment_mode_cod,
case when csrd3.payment_mode_paid_via_wallet is null then 0 else csrd3.payment_mode_paid_via_wallet end as payment_mode_paid_via_wallet,
case when csrd3.payment_mode_paid_online is null then 0 else csrd3.payment_mode_paid_online end as payment_mode_paid_online,
case when csrd3.payment_mode_card_at_delivery is null then 0 else csrd3.payment_mode_card_at_delivery end as payment_mode_card_at_delivery,
case when csrd3.payment_mode_cash_less_amount is null then 0 else csrd3.payment_mode_cash_less_amount end as payment_mode_cash_less_amount,
case when csrd3.no_of_orders_cardiac is null then 0 else csrd3.no_of_orders_cardiac end as no_of_orders_cardiac,
case when csrd3.no_of_orders_antidiabetic is null then 0 else csrd3.no_of_orders_antidiabetic end as no_of_orders_antidiabetic,
case when csrd3.no_of_orders_vitamins_and_supplements is null then 0 else csrd3.no_of_orders_vitamins_and_supplements end as no_of_orders_vitamins_and_supplements,
case when csrd3.no_of_orders_gastrointestinal is null then 0 else csrd3.no_of_orders_gastrointestinal end as no_of_orders_gastrointestinal,
case when csrd3.no_of_orders_nervous_system is null then 0 else csrd3.no_of_orders_nervous_system end as no_of_orders_nervous_system,
case when csrd3.no_of_orders_pain_management is null then 0 else csrd3.no_of_orders_pain_management end as no_of_orders_pain_management,
case when csrd3.no_of_orders_gynaecology is null then 0 else csrd3.no_of_orders_gynaecology end as no_of_orders_gynaecology,
case when csrd3.no_of_orders_respiratory is null then 0 else csrd3.no_of_orders_respiratory end as no_of_orders_respiratory, 
case when csrd3.no_of_orders_blood_related is null then 0 else csrd3.no_of_orders_blood_related end as no_of_orders_blood_related,
csrd3.mrp_revenue,
csrd3.discounted_revenue,
cast(null as int) as cancel_reason_cancelled_by_customer,
cast(null as int) as cancel_reason_delivery_issue,
cast(null as int) as cancel_reason_doctor_teleconsultation_issue,
cast(null as int) as cancel_reason_fake_order,
cast(null as int) as cancel_reason_medicine_issue,
cast(null as int) as cancel_reason_prescription_issue,
cast(null as int) as cancel_reason_image_issue,
cast(null as int) as cancel_reason_others,
-- case when csrd3.cancel_reason_cancelled_by_customer is null then 0 else csrd3.cancel_reason_cancelled_by_customer end as cancel_reason_cancelled_by_customer,
-- case when csrd3.cancel_reason_delivery_issue is null then 0 else csrd3.cancel_reason_delivery_issue end as cancel_reason_delivery_issue,
-- case when csrd3.cancel_reason_doctor_teleconsultation_issue is null then 0 else csrd3.cancel_reason_doctor_teleconsultation_issue end as cancel_reason_doctor_teleconsultation_issue,
-- case when csrd3.cancel_reason_fake_order is null then 0 else csrd3.cancel_reason_fake_order end as cancel_reason_fake_order,
-- case when csrd3.cancel_reason_medicine_issue is null then 0 else csrd3.cancel_reason_medicine_issue end as cancel_reason_medicine_issue,
-- case when csrd3.cancel_reason_prescription_issue is null then 0 else csrd3.cancel_reason_prescription_issue end as cancel_reason_prescription_issue,
-- case when csrd3.cancel_reason_image_issue is null then 0 else csrd3.cancel_reason_image_issue end as cancel_reason_image_issue,
-- case when csrd3.cancel_reason_others is null then 0 else csrd3.cancel_reason_others end as cancel_reason_others,
csrd3.average_order_rating,
csrd3.doctor_consultation_pitched,
csrd3.opted_for_doctor_consulation,
csrd3.successful_doctor_consultation,
csrd3.order_delivered_after_doctor_consulation,
case when csrd3.no_of_partially_delivered_orders is null then 0 else csrd3.no_of_partially_delivered_orders end as no_of_partially_delivered_orders,
case when csrd3.no_of_orders_dermatological is null then 0 else csrd3.no_of_orders_dermatological end as no_of_orders_dermatological,
csrd5.average_mrp_order_value,
csrd5.days_since_first_order,
csrd5.days_since_latest_order,
csrd5.days_since_latest_delivered_order,
csrd5.registration_to_first_order_days,
csrd2.customer_type,
csrd5.customer_nps_bucket,
csrd5.customer_outlier_flag,
csrd5.average_order_frequency,
csrd5.ml_user_type,
csrd5.ml_days_since_latest_order,
csrd5.ml_dormant_flag,
csrd5.ml_power_customer,
csrd14.is_retargeting,
csrd14.adset,
csrd14.site_id,
csrd14.advertising_id,
case when csrd13.is_apk=1 then true else false end as is_apk,
csrd14.registration_source_attribution,
case when csrd6.no_of_diagnostic_orders is null then 0 else csrd6.no_of_diagnostic_orders end as no_of_diagnostic_orders,
case when csrd6.no_of_diagnostic_fulfilled_orders is null then 0 else csrd6.no_of_diagnostic_fulfilled_orders end as no_of_diagnostic_fulfilled_orders,
csrd6.diagnostic_gmv,
case when lty.loyalty_enrolled='yes' then 'Yes'
when lty.loyalty_enrolled='no' then 'No' end as loyalty_enrolled,
lty.loyalty_all_program_savings,
lty.loyalty_all_program_orders,
lty.current_loyalty_program_id,
lty.current_loyalty_variant_id,
lty.current_loyalty_enrollment_date,
lty.current_loyalty_expiry_date,
lty.current_loyalty_days_to_expiry,
lty.current_loyalty_program_purchase_price,
lty.current_loyalty_savings,
lty.current_loyalty_medicine_cashback_savings,
lty.current_loyalty_medicine_orders,
lty.current_loyalty_doctor_consultation_eligibility,
lty.current_loyalty_diagnostic_orders,
lty.current_loyalty_diagnostic_savings,
csrd11.balance as wallet_balance,
csrd11.promotional_cash,
csrd11.transactional_cash,
csrd9.total_referred_customer,
case when csrd10.customer_id is not null then True else False end as is_referred_customer, 
case when csrd12.no_of_pincodes is null then 0 else csrd12.no_of_pincodes end as no_of_pincodes,
csrd12.no_of_unique_doctors,
csrd20.therapy_group,
csrd23.diagnosticTests as diagnostic_tests,
csrd23.diagnosticPackage as diagnostic_package,
csrd12.count_of_addresses,
csrd2.recent_order_source,
customer_chronic_flag,
csrd2.latest_fulfilled_delivery_city_name as latest_fulfilled_delivery_city,
case when csrd2.recent_delivery_city_tier is null then 'Tier-3' else csrd2.recent_delivery_city_tier end as recent_delivery_city_tier,
csrd2.latest_placed_supplier_city_name,
csrd8.patient_count,
cast(null as string) as preferred_payment_method,
cast(null as string) as preferred_card_option,
cast(null as string) as preferred_wallet_option,
cast(null as string) as preferred_netbanking_option,
cast(null as string) as payment_at_delivery_orders,
-- csrd17.preferred_payment_method,
-- csrd17.preferred_card_option,
-- csrd17.preferred_wallet_option,
-- csrd17.preferred_netbanking_option,
-- csrd18.payment_at_delivery_orders,
cast(null as string) as first_android_install_time,
cast(null as string) as latest_android_install_time,
cast(null as string) as latest_android_uninstall_time,
cast(null as string) as first_ios_install_time,
cast(null as string) as latest_ios_install_time,
case when csrd13.media_source is null then 'organic' else csrd13.media_source end as install_media_source,
case when csrd13.campaign is null then 'organic' else csrd13.campaign end as install_campaign,
case when csrd13.install_source_attribution is null then 'Organic' else csrd13.install_source_attribution end as install_source_attribution,
csrd14.registration_media_source,
csrd14.registration_campaign,
csrd5.registration_city,
csrd5.registration_state,
csrd28.ideal_next_order_date_min,
csrd28.ideal_next_order_date_max,
csrd28.ideal_next_order_date_2_max,
csrd28.ideal_next_order_date_min_chronic,
csrd28.ideal_next_order_date_max_chronic,
csrd28.ideal_next_order_date_2_max_chronic,
case when csrd28.ideal_next_order_date_min >= CURRENT_DATE then 'Green'
     when csrd28.ideal_next_order_date_min < CURRENT_DATE and csrd28.ideal_next_order_date_max >= CURRENT_DATE then 'Yellow'
     when csrd28.ideal_next_order_date_max < CURRENT_DATE and csrd28.ideal_next_order_date_2_max >= CURRENT_DATE then 'Red'
     when csrd28.ideal_next_order_date_2_max < CURRENT_DATE then 'Black'
end as recency_segment,
csrd26.recent_fm_segment,
csrd26.recent_order_bucket,
csrd26.discount_affinity as recent_discount_affinity_segment,
csrd27.previous_order_experience_score_change,
csrd26.experience_score
   
from  (select * from data_models_temp_tables.oms_csrd_customer_data where registration_date< current_Date())  c
left join reporting_batch.customer_acute_intent cai on cai.customer_id=c.customer_id
left join reporting_batch.customer_chronic_intent cci on cci.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_first_related_table csrd1 on csrd1.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_latest_related_final csrd2 on csrd2.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_aggregated_metrics csrd3 on csrd3.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_loyalty lty on lty.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_derived_metrics_final_merge csrd5 on csrd5.customer_id=c.customer_id
left join data_models_temp_tables.oms_diagnostics_aggregated_metrics csrd6 on csrd6.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_patient_count csrd8 on csrd8.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_total_reffered csrd9 on csrd9.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_is_referred_customer csrd10 on c.customer_id=csrd10.customer_id
left join data_models_temp_tables.oms_csrd_wallet_promotional csrd11 on csrd11.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_other_aggregated_metrics csrd12 on csrd12.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_install_appsflyer csrd13 on csrd13.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_registration_appsflyer_final csrd14 on csrd14.customer_id=c.customer_id
-- left join data_models_temp_tables.oms_csrd_preferred_payement csrd17 on csrd17.customer_id=c.customer_id
-- left join data_models_temp_tables.oms_csrd_payment_at_delivery csrd18 on csrd18.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_therapy_group csrd20 on csrd20.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_diag_test_package csrd23 on csrd23.customer_id=c.customer_id
left join data_models_temp_tables.oms_csrd_segmention csrd26 on csrd26.customer_id=c.customer_id
left join data_models_temp_tables.oms_previous_order_experience csrd27 on csrd27.customer_id=c.customer_id
left join data_models_temp_tables.oms_redency_dosage csrd28 on csrd28.customer_id=c.customer_id;
