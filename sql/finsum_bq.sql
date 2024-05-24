-- finsum_bq

drop table if exists `dp_bi.finsum_bq`;

create or replace table `dp_bi.finsum_bq`
partition by DATE_TRUNC(transaction_date_financial, DAY)
as (

select
      NULL as booked_by_csr_user_id
     ,NULLIF(Brand, '<NA>') as brand
     ,NULL as carrier
     ,carrier_id as carrier_id
     ,NULLIF(`Carrier Display Name`, '<NA>') as carrier_display_name
     ,carrier_service_level_id as carrier_service_level_id
     ,NULL as carrier_service_level_report_name
     ,NULLIF(`Carrier Tracking ID`, '<NA>') as carrier_tracking_id
     ,cost_cents as cost_cents
     ,NULLIF(coupon_id, '<NA>') as coupon_id
     ,NULL as destination_ship_point_address_type
     ,NULL as destination_ship_point_attention_name
     ,NULL as destination_ship_point_city
     ,NULL as destination_ship_point_company_name
     ,NULL as destination_ship_point_country_code
     ,NULL as destination_ship_point_cruise
     ,NULL as destination_ship_point_delivery_address_line
     ,NULL as destination_ship_point_delivery_address_line_1
     ,NULL as destination_ship_point_facility_id
     ,NULL as destination_ship_point_google_place_id
     ,NULL as destination_ship_point_lat
     ,NULL as destination_ship_point_lng
     ,NULL as destination_ship_point_state
     ,NULL as destination_ship_point_zip5
     ,`Estimated Ship Date` as estimated_ship_date
     ,NULLIF(`Item Type Temp`, '<NA>') as item_type
     ,NULL as leg_id
     ,NULLIF(micro_site_id,'<NA>') as micro_site_id
     ,NULL as internal_order_id
     ,NULLIF(`Order ID`, '<NA>') as order_id
     ,NULL as order_line_item_category
     ,NULLIF(`Order Line Item ID`, '<NA>') as order_line_item_id
     ,NULL as origination_ship_point_address_type
     ,NULL as origination_ship_point_attention_name
     ,NULL as origination_ship_point_city
     ,NULL as origination_ship_point_company_name
     ,NULL as origination_ship_point_country_code
     ,NULL as origination_ship_point_cruise
     ,NULL as origination_ship_point_delivery_address_line
     ,NULL as origination_ship_point_delivery_address_line_1
     ,NULL as origination_ship_point_facility_id
     ,NULL as origination_ship_point_google_place_id
     ,NULL as origination_ship_point_lat
     ,NULL as origination_ship_point_lng
     ,NULL as origination_ship_point_state
     ,NULL as origination_ship_point_zip5
     ,NULLIF(payment_method_id, '<NA>') as payment_method_id
     ,NULLIF(`Payment Method Type`, '<NA>') as payment_method
     ,price_cents as price_cents
     ,NULLIF(`Shipment State`, '<NA>') as shipment_state
     ,NULL as shipment_actual_delivery
     ,`Shipment Created At` as shipment_created_at
     ,NULL as shipment_estimated_delivery
     ,NULLIF(`State`, '<NA>') as state
     ,`Transaction Date - Action` as transaction_date_action
     ,`Transaction Date - Action Timestamp` as transaction_date_action_timestamp
     ,`Transaction Date - Financial` as transaction_date_financial
     ,NULLIF(`Tracking ID`, '<NA>') as tracking_id
     ,NULLIF(traveler_id, '<NA>') as traveler_id
     ,NULLIF(`Traveler Email`, '<NA>') as traveler_email
     ,NULLIF(`Traveler Name`, '<NA>') as traveler_name
     ,NULLIF(`User Email`, '<NA>') as user_email
     ,NULLIF(user_id, '<NA>') as user_id
     ,`User is Admin` as user_is_admin
     ,`User is Pro` as user_is_pro
     ,`v5 Insurance Value` as insurance_value
from `bi.financial_summary_detail_v5`

)