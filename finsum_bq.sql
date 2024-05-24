
-- dp_bi.finsum_bq

declare begin_time date;
set begin_time = '2023-01-01';

create or replace table `dp_bi.finsum_bq`
(
    booked_by_csr_user_id STRING,
    brand STRING,
    carrier STRING,
    carrier_id STRING,
    carrier_display_name STRING,
    carrier_service_level_id STRING,
    carrier_service_level_report_name STRING,
    carrier_tracking_id STRING,
    cost_cents INT64,
    coupon_id STRING,
    destination_ship_point_address_type STRING,
    destination_ship_point_attention_name STRING,
    destination_ship_point_city STRING,
    destination_ship_point_company_name STRING,
    destination_ship_point_country_code STRING,
    destination_ship_point_cruise STRING,
    destination_ship_point_delivery_address_line STRING,
    destination_ship_point_delivery_address_line_1 STRING,
    destination_ship_point_facility_id STRING,
    destination_ship_point_google_place_id STRING,
    destination_ship_point_lat FLOAT64,
    destination_ship_point_lng FLOAT64,
    destination_ship_point_state STRING,
    destination_ship_point_zip5 STRING,
    estimated_ship_date DATE,
    item_type STRING,
    leg_id STRING,
    micro_site_id STRING,
    internal_order_id STRING,
    order_id STRING,
    order_line_item_category STRING,
    order_line_item_id STRING,
    origination_ship_point_address_type STRING,
    origination_ship_point_attention_name STRING,
    origination_ship_point_city STRING,
    origination_ship_point_company_name STRING,
    origination_ship_point_country_code STRING,
    origination_ship_point_cruise STRING,
    origination_ship_point_delivery_address_line STRING,
    origination_ship_point_delivery_address_line_1 STRING,
    origination_ship_point_facility_id STRING,
    origination_ship_point_google_place_id STRING,
    origination_ship_point_lat FLOAT64,
    origination_ship_point_lng FLOAT64,
    origination_ship_point_state STRING,
    origination_ship_point_zip5 STRING,
    payment_method_id STRING,
    payment_method STRING,
    price_cents INT64,
    shipment_state STRING,
    shipment_actual_delivery DATE,
    shipment_created_at DATE,
    shipment_estimated_delivery DATE,
    state STRING,
    transaction_date_action DATE,
    transaction_date_action_timestamp DATETIME,
    transaction_date_financial DATE,
    tracking_id STRING,
    traveler_id STRING,
    traveler_email STRING,
    traveler_name STRING,
    user_email STRING,
    user_id STRING,
    user_is_admin BOOLEAN,
    user_is_pro BOOLEAN,
    insurance_value INT64
)
as (

select
     NULL as booked_by_csr_user_id,
     NULLIF(Brand, '<NA>') as brand,
     NULL as carrier,
     carrier_id as carrier_id,
     NULLIF(`Carrier Display Name`, '<NA>') as carrier_display_name,
     carrier_service_level_id as carrier_service_level_id,
     NULL as carrier_service_level_report_name,
     NULLIF(`Carrier Tracking ID`, '<NA>') as carrier_tracking_id,
     cost_cents as cost_cents,
     NULLIF(coupon_id, '<NA>') as coupon_id,
     NULL as destination_ship_point_address_type,
     NULL as destination_ship_point_attention_name,
     NULL as destination_ship_point_city,
     NULL as destination_ship_point_company_name,
     NULL as destination_ship_point_country_code,
     NULL as destination_ship_point_cruise,
     NULL as destination_ship_point_delivery_address_line,
     NULL as destination_ship_point_delivery_address_line_1,
     NULL as destination_ship_point_facility_id,
     NULL as destination_ship_point_google_place_id,
     NULL as destination_ship_point_lat,
     NULL as destination_ship_point_lng,
     NULL as destination_ship_point_state,
     NULL as destination_ship_point_zip5,
     `Estimated Ship Date` as estimated_ship_date,
     NULLIF(`Item Type Temp`, '<NA>') as item_type,
     NULL as leg_id,
     CASE WHEN micro_site_id = '<NA>' THEN NULL ELSE micro_site_id END as micro_site_id,
     NULL as internal_order_id,
     NULLIF(`Order ID`, '<NA>') as order_id,
     NULL as order_line_item_category,
     NULLIF(`Order Line Item ID`, '<NA>') as order_line_item_id,
     NULL as origination_ship_point_address_type,
     NULL as origination_ship_point_attention_name,
     NULL as origination_ship_point_city,
     NULL as origination_ship_point_company_name,
     NULL as origination_ship_point_country_code,
     NULL as origination_ship_point_cruise,
     NULL as origination_ship_point_delivery_address_line,
     NULL as origination_ship_point_delivery_address_line_1,
     NULL as origination_ship_point_facility_id,
     NULL as origination_ship_point_google_place_id,
     NULL as origination_ship_point_lat,
     NULL as origination_ship_point_lng,
     NULL as origination_ship_point_state,
     NULL as origination_ship_point_zip5,
     NULLIF(payment_method_id, '<NA>') as payment_method_id,
     NULLIF(`Payment Method Type`, '<NA>') as payment_method,
     NULL as price_cents,
     NULLIF(`Shipment State`, '<NA>') as shipment_state,
     NULL as shipment_actual_delivery,
    `Shipment Created At` as shipment_created_at,
     NULL as shipment_estimated_delivery,
     NULLIF(`State`, '<NA>') as state,
    `Transaction Date - Action` as transaction_date_action,
    `Transaction Date - Action Timestamp` as transaction_date_action_timestamp,
    `Transaction Date - Financial` as transaction_date_financial,
     NULLIF(`Tracking ID`, '<NA>') as tracking_id,
     NULLIF(traveler_id, '<NA>') as traveler_id,
     NULLIF(`Traveler Email`, '<NA>') as traveler_email,
     NULLIF(`Traveler Name`, '<NA>') as traveler_name,
     NULLIF(`User Email`, '<NA>') as user_email,
     NULLIF(user_id, '<NA>') as user_id,
     `User is Admin` as user_is_admin,
     `User is Pro` as user_is_pro,
     `v5 Insurance Value` as insurance_value

from `bi.financial_summary_detail_v5`
where `Shipment Created At` >= begin_time
)
