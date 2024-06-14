drop table if exists dp_bi.finsum;
create or replace table dp_bi.finsum as
with finsum_0 as (
select
  -- dates
  date(`Transaction Date - Financial`) as transaction_financial_date
  , date(`Transaction Timestamp - Financial`) as transaction_financial_at
  , date(`Transaction Date - Action`) as transaction_action_date
  , datetime(`Transaction Date - Action Timestamp`) as transaction_action_at
  , date(`Shipment Created At`) as shipment_created_date
  , datetime(`Shipment Created At Timestamp`) as shipment_created_at
  , date(`Estimated Ship Date`) as shipment_estimated_ship_date
  , date(`Shipment Actual Delivery`) as shipment_actual_delivery_date 
  , date(`Shipment Estimated Delivery`) as shipment_estimated_delivery_date
  -- metrics
  , `Order Line Item Price Cents` as price_cents
  , cost_cents
  , `v5 Insurance Value` as insurance_value
  -- dimensions
  , nullif(Brand, '<NA>') as brand
  , nullif(`Brand ID`, '<NA>') as brand_id
  , `Shipment ID` as shipment_id
  , `Transaction ID` as internal_transaction_id
  , transaction_id as transaction_id
  , nullif(`State`, '<NA>') as transaction_reporting_state
  , nullif(`Transaction Type`, '<NA>') as transaction_type
  , nullif(`Transaction State`, '<NA>') as transaction_state
  , `v5 Product Type` as product_type
  , `v5 Product Full Name` as product_name
  , 'v5 Product ID'  as product_id
  , sku as product_sku
  , Carrier as carrier
  , carrier_id
  , nullif(`Carrier Display Name`, '<NA>') as carrier_display_name
  , carrier_service_level_id as carrier_service_level_id
  , nullif(`Carrier Service Level Report Name`,'<NA>') as carrier_service_level_report_name
  , nullif(`Carrier Tracking ID`, '<NA>') as carrier_tracking_id
  , nullif(coupon_id, '<NA>') as coupon_id
  , nullif(`Item Type Temp`, '<NA>') as item_type
  , `Shipment Leg ID` as leg_id
  , nullif(micro_site_id,'<NA>') as micro_site_id
  , order_id
  , nullif(`Order ID`, '<NA>') as internal_order_id
  , nullif(`Order Line Item Category`,'<NA>')  as order_line_item_category
  , nullif(`Order Line Item ID`, '<NA>') as order_line_item_id
  , nullif(payment_method_id, '<NA>') as payment_method_id
  , `Payment Method` as payment_method
  , `Payment Method Type` as payment_method_type
  , nullif(`Shipment State`, '<NA>') as shipment_state
  , nullif(`Tracking ID`, '<NA>') as tracking_id
  , nullif(traveler_id, '<NA>') as traveler_id
  , nullif(`Traveler Email`, '<NA>') as traveler_email
  , nullif(`Traveler Name`, '<NA>') as traveler_name
  , nullif(`User Email`, '<NA>') as user_email
  , nullif(user_id, '<NA>') as user_id
  , nullif(`Booked by CSR User ID`,'<NA>') as booked_by_csr_user_id
  , `User is Admin` as user_is_admin
  , `User is Pro` as user_is_pro
from `bi.financial_summary_detail_v5`
), finsum_1 as (
select
  f.*
  , origination_ship_point_address_type
  , origination_ship_point_attention_name
  , origination_ship_point_city
  , origination_ship_point_company_name
  , origination_ship_point_country_code
  , origination_ship_point_delivery_address_line
  , origination_ship_point_delivery_address_line_1
  , origination_ship_point_facility_id
  , origination_ship_point_google_place_id
  , origination_ship_point_lat
  , origination_ship_point_lng
  , origination_ship_point_phone_number
  , origination_ship_point_service_area_code
  , origination_ship_point_state
  , origination_ship_point_zip5
  , destination_ship_point_address_type
  , destination_ship_point_attention_name
  , destination_ship_point_city
  , destination_ship_point_company_name
  , destination_ship_point_country_code
  , destination_ship_point_delivery_address_line
  , destination_ship_point_delivery_address_line_1
  , destination_ship_point_facility_id
  , destination_ship_point_google_place_id
  , destination_ship_point_lat
  , destination_ship_point_lng
  , destination_ship_point_phone_number
  , destination_ship_point_service_area_code
  , destination_ship_point_state
  , destination_ship_point_zip5
from finsum_0 as f
left outer join `mongo_land.v5_shipments` as s
  on f.shipment_id = s.shipment_id
), finsum_2 as (
select
  f.*
  , m.name as micro_site
  , m.sub_domain as micro_site_subdomain
from finsum_1 as f
left outer join mongo_land.micro_sites as m 
  on f.micro_site_id = m._id
), finsum_3 as (
select 
  f.*
  , if(t.travel_network = '' or travel_network = 'None', null, t.travel_network) as travel_network
  , if(t.travel_company = '', null, t.travel_company) as travel_company
  , if(t.agent_name = '', null, t.agent_name) as agent_name
  , if(t.travel_company <> '' or t.travel_network <> '' or t.agent_name <> '' or travel_network <> 'None', true, null) as travel_referral
from finsum_2 as f
left outer join mongo_land.travel_referrals as t
  on f.internal_order_id = t.order_id
), club_pros as (
select
  club_id
  , c.name as club_name
  , reporting_brand_id
  , b.name as reporting_brand
  , reporting_account_type
  , ship_to_address_state
  , no_pro
  , pro_name
  , if(pro_name is not null, true, false) as club_pro
  , user_id
  , micro_site_id
  , facility_type
from `mongo_land.clubs` as c
join `mongo_land.brands` as b
  on c.reporting_brand_id = b._id
where 
  c.no_pro is false
  and c.user_id <> '<NA>'
), finsum_4 as (
select
  f.*
  , p.club_id
  , p.club_name
  -- , p.reporting_brand_id
  -- , p.reporting_brand
  , p.reporting_account_type
  , p.ship_to_address_state
  , p.pro_name
  , p.facility_type
from finsum_3 as f
left outer join club_pros as p 
  on p.user_id = f.user_id
), finsum_5 as (
select
  f.*
  , if(u.segment_user_profile_is_travel_agent is true, true, null) as is_travel_agent
from finsum_4 as f
left outer join mongo_land.users as u
  on u._id = f.user_id
)
select 
  *
  , case when pro_name is not null then 'club_pro'
      when pro_name is null and is_travel_agent is true then 'travel_agent'
      when (pro_name is null) and (is_travel_agent is not true) and (micro_site is not null) then 'micro_site'
      when (pro_name is null) and (is_travel_agent is not true) and (micro_site is null) and travel_referral is not null then 'travel_referral'
      end as b2b_revenue_attrabution
from finsum_5
;

-- credit waterfall (at shipment level)
-- 1) club pro
-- 2) travel-agent user role
-- 3) utm attrabution
-- 4) micro-site 
-- 5) travel referrals
