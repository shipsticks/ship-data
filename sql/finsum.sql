drop table if exists dp_bi.finsum;
create or replace table dp_bi.finsum 
partition by date_trunc(transaction_financial_date, month) as
with base_finsum as (
select
  -- dates
  date(`Transaction Date - Financial`) as transaction_financial_date
  , datetime(`Transaction Timestamp - Financial`) as transaction_financial_at
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
from bi.financial_summary_detail_v5
), microsites as (
select
  f.*
  , m.name as micro_site
  , m.sub_domain as micro_site_subdomain
from base_finsum as f
left outer join mongo_land.micro_sites as m 
  on f.micro_site_id = m._id
-- where m._id not in('62975451141c04016357d2bf', '64088dd9b3a8d901c073cf31','6400d8f4b4046e01cff6b6c5','610bfa7a2c4fc90176f1652f','55e9751dce94aa501750fd49e','55e35349af5ff72330000019','54ecdb77f4825e54fb000012')  
), travel_referrals as (
select 
  f.*
  , if(t.travel_network = '' or travel_network = 'None', null, t.travel_network) as travel_network
  , if(t.travel_company = '', null, t.travel_company) as travel_company
  , if(t.agent_name = '', null, t.agent_name) as agent_name
  , if(t.travel_company <> '' or t.travel_network <> '' or t.agent_name <> '' or travel_network <> 'None', true, null) as travel_referral
from microsites as f
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
from mongo_land.clubs as c
join mongo_land.brands as b
  on c.reporting_brand_id = b._id
where 
  c.no_pro is false
  and c.user_id <> '<NA>'
  -- and c.club_id not in('5a95de7535db5c0188001688','54da0c49f4825eb481000033','6218f0600f46db01612acdfa','5879035aaf5ff75f63000092','650327797de1ee01508fc7f6','JAP-01-0162')
), clubs as (
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
from travel_referrals as f
left outer join club_pros as p 
  on p.user_id = f.user_id
), travel_agent as (
select
  f.*
  , if(u.segment_user_profile_is_travel_agent is true, true, null) as is_travel_agent
from clubs as f
left outer join mongo_land.users as u
  on u._id = f.user_id
), first_transaction as (
select 
  f.*
  , min(f.transaction_financial_date) over (partition by f.user_id, f.brand) as first_transaction_date
from travel_agent f    
)
select 
  *
  , case when transaction_financial_date = first_transaction_date then true else false end as is_first_transaction
  , case when pro_name is not null then 'club_pro'
    when pro_name is null and is_travel_agent is true then 'travel_agent'
    when (pro_name is null) and (is_travel_agent is not true) and (micro_site is not null) then 'micro_site'
    when (pro_name is null) and (is_travel_agent is not true) and (micro_site is null) and travel_referral is not null then 'travel_referral'
    end as b2b_revenue_attrabution
from first_transaction
;
