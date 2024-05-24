drop table if exists `dp_bi.finsum_bq`;
create or replace table `dp_bi.finsum_bq`
partition by date_trunc(transaction_date_financial, month)
as
select
  null as booked_by_csr_user_id
  , nullif(Brand, '<NA>') as brand
  , null as carrier
  , carrier_id as carrier_id
  , nullif(`Carrier Display Name`, '<NA>') as carrier_display_name
  , carrier_service_level_id as carrier_service_level_id
  , null as carrier_service_level_report_name
  , nullif(`Carrier Tracking ID`, '<NA>') as carrier_tracking_id
  , cost_cents as cost_cents
  , nullif(coupon_id, '<NA>') as coupon_id
  , null as destination_ship_point_address_type
  , null as destination_ship_point_attention_name
  , null as destination_ship_point_city
  , null as destination_ship_point_company_name
  , null as destination_ship_point_country_code
  , null as destination_ship_point_cruise
  , null as destination_ship_point_delivery_address_line
  , null as destination_ship_point_delivery_address_line_1
  , null as destination_ship_point_facility_id
  , null as destination_ship_point_google_place_id
  , null as destination_ship_point_lat
  , null as destination_ship_point_lng
  , null as destination_ship_point_state
  , null as destination_ship_point_zip5
  , `Estimated Ship Date` as estimated_ship_date
  , nullif(`Item Type Temp`, '<NA>') as item_type
  , null as leg_id
  , nullif(micro_site_id,'<NA>') as micro_site_id
  , null as internal_order_id
  , nullif(`Order ID`, '<NA>') as order_id
  , null as order_line_item_category
  , nullif(`Order Line Item ID`, '<NA>') as order_line_item_id
  , null as origination_ship_point_address_type
  , null as origination_ship_point_attention_name
  , null as origination_ship_point_city
  , null as origination_ship_point_company_name
  , null as origination_ship_point_country_code
  , null as origination_ship_point_cruise
  , null as origination_ship_point_delivery_address_line
  , null as origination_ship_point_delivery_address_line_1
  , null as origination_ship_point_facility_id
  , null as origination_ship_point_google_place_id
  , null as origination_ship_point_lat
  , null as origination_ship_point_lng
  , null as origination_ship_point_state
  , null as origination_ship_point_zip5
  , nullif(payment_method_id, '<NA>') as payment_method_id
  , nullif(`Payment Method Type`, '<NA>') as payment_method
  , price_cents as price_cents
  , nullif(`Shipment State`, '<NA>') as shipment_state
  , null as shipment_actual_delivery
  , `Shipment Created At` as shipment_created_at
  , null as shipment_estimated_delivery
  , nullif(`State`, '<NA>') as state
  , date(`Transaction Date - Action`) as transaction_date_action
  , `Transaction Date - Action Timestamp` as transaction_date_action_timestamp
  , date(`Transaction Date - Financial`) as transaction_date_financial
  , nullif(`Tracking ID`, '<NA>') as tracking_id
  , nullif(traveler_id, '<NA>') as traveler_id
  , nullif(`Traveler Email`, '<NA>') as traveler_email
  , nullif(`Traveler Name`, '<NA>') as traveler_name
  , nullif(`User Email`, '<NA>') as user_email
  , nullif(user_id, '<NA>') as user_id
  , `User is Admin` as user_is_admin
  , `User is Pro` as user_is_pro
  , `v5 Insurance Value` as insurance_value
from `bi.financial_summary_detail_v5`
;