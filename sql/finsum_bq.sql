drop table if exists `dp_bi.finsum_bq`;
create or replace table `dp_bi.finsum_bq`
partition by date_trunc(transaction_date_financial, month)
as
select
  nullif(Brand, '<NA>') as brand
  , nullif(brand_id, '<NA>') as brand_id
  -- dates
  , date(`Transaction Date - Action`) as transaction_action_date
  , datetime(`Transaction Date - Action Timestamp`) as transaction_action_at
  , date(`Transaction Date - Financial`) as transaction_financial_date
  , date(`Shipment Created At`) as shipment_created_date
  , date(`Estimated Ship Date`) as estimated_ship_date
  , null as shipment_actual_delivery_date 
  , null as shipment_estimated_delivery
  -- metrics
  , price_cents
  , cost_cents
  , `v5 Insurance Value` as insurance_value
  -- dimensions
  , `Shipment ID` as shipment_id
  , null as carrier
  , carrier_id as carrier_id
  , nullif(`Carrier Display Name`, '<NA>') as carrier_display_name
  , carrier_service_level_id as carrier_service_level_id
  , null as carrier_service_level_report_name
  , nullif(`Carrier Tracking ID`, '<NA>') as carrier_tracking_id
  , nullif(coupon_id, '<NA>') as coupon_id
  , nullif(`Item Type Temp`, '<NA>') as item_type
  , null as leg_id
  , nullif(micro_site_id,'<NA>') as micro_site_id
  , order_id
  , nullif(`Order ID`, '<NA>') as internal_order_id
  , null as order_line_item_category
  , nullif(`Order Line Item ID`, '<NA>') as order_line_item_id
  , nullif(payment_method_id, '<NA>') as payment_method_id
  , nullif(`Payment Method Type`, '<NA>') as payment_method
  , nullif(`Shipment State`, '<NA>') as shipment_state
  , nullif(`State`, '<NA>') as state
  , nullif(`Tracking ID`, '<NA>') as tracking_id
  , nullif(traveler_id, '<NA>') as traveler_id
  , nullif(`Traveler Email`, '<NA>') as traveler_email
  , nullif(`Traveler Name`, '<NA>') as traveler_name
  , nullif(`User Email`, '<NA>') as user_email
  , nullif(user_id, '<NA>') as user_id
  , null as booked_by_csr_user_id
  , `User is Admin` as user_is_admin
  , `User is Pro` as user_is_pro
from `bi.financial_summary_detail_v5`
;