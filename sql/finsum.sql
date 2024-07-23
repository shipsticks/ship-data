drop table if exists `dp_bi.finsum`; 
create or replace table `dp_bi.finsum`
partition by date_trunc(transaction_financial_date, month) as
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
    , nullif(`Shipment ID`, '<NA>') as shipment_id
    , `Transaction ID` as internal_transaction_id
    , transaction_id as transaction_id
    , nullif(`State`, '<NA>') as transaction_reporting_state
    , nullif(`Transaction Type`, '<NA>') as transaction_type
    , nullif(`Transaction State`, '<NA>') as transaction_state
    , `v5 Product Type` as product_type
    , nullif(`v5 Product Full Name`, '<NA>') as product_name
    , 'v5 Product ID' as product_id
    , sku as product_sku
    , Carrier as carrier
    , carrier_id
    , nullif(`Carrier Display Name`, '<NA>') as carrier_display_name
    , carrier_service_level_id as carrier_service_level_id
    , nullif(`Carrier Service Level Report Name`, '<NA>') as carrier_service_level_report_name
    , nullif(`Carrier Tracking ID`, '<NA>') as carrier_tracking_id
    , nullif(`Origination Ship Point Facility Id`, '<NA>') as origination_facility_id
    , nullif(`Origination Ship Point Company Name`, '<NA>') as origination_company_name
    , concat(nullif(`Origination Ship Point Delivery Address Line`, '<NA>'), ' ', nullif(`Origination Ship Point Delivery Address Line 1`, '<NA>')) as origination_delivery_address
    , `Origination Ship Point City` as origination_city
    , `Origination Ship Point State` as origination_state
    , `Origination Ship Point Zip5` as origination_zip5
    , `Origination Ship Point Country Code` as origination_country_code
    , nullif(`Destination Ship Point Facility Id`, '<NA>') as destination_facility_id
    , nullif(`Destination Ship Point Company Name`, '<NA>') as destination_company_name
    , concat(nullif(`Destination Ship Point Delivery Address Line`, '<NA>'), ' ', nullif(`Destination Ship Point Delivery Address Line 1`, '<NA>')) as destination_delivery_address
    , `Destination Ship Point City` as destination_city
    , `Destination Ship Point State` as destination_state
    , `Destination Ship Point Zip5` as destination_zip5
    , `Destination Ship Point Country Code` as destination_country_code
    , nullif(coupon_id, '<NA>') as coupon_id
    , nullif(`Item Type Temp`, '<NA>') as item_type
    , `Shipment Leg ID` as leg_id
    , nullif(micro_site_id, '<NA>') as micro_site_id
    , order_id
    , nullif(`Order ID`, '<NA>') as internal_order_id
    , nullif(`Order Line Item Category`, '<NA>') as order_line_item_category
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
    , nullif(`Booked by CSR User ID`, '<NA>') as booked_by_csr_user_id
    , `User is Admin` as user_is_admin
    , `User is Pro` as user_is_pro
  from bi.financial_summary_detail_v5
;

drop table if exists `dp_bi.finsum_b2b`; 
create or replace table `dp_bi.finsum_b2b`
partition by date_trunc(transaction_financial_date, month) as
--Begin Clubs
--gathering club pro information
with club_pros as (  
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
), 

--joining club pros to clubs on user_id 
clubs as (  
  select
      f.*
    , p.club_id
    , p.club_name
    , p.reporting_account_type
    , p.ship_to_address_state
    , p.pro_name
    , p.facility_type
  from `dp_bi.finsum` as f
  left outer join club_pros as p 
    on p.user_id = f.user_id 
  group by all
),

--creating temp CTE to find user_ids tied to > 1 club
mult_club_users as (  
  select 
      user_id
    , cast(max(club_id) as string) as max_club_id
  from clubs
  group by user_id
  having count(distinct club_id) > 1
),

--filtering out the transactions where either the user is only tied to 1 club, 
--or the user has > 1 club and any of their clubs are either in the origination or destination facility id
filtered_clubs as (   
  select 
      f.* 
    , cast(null as string) as final_club_id
  from clubs f
  where user_id not in (select user_id from mult_club_users)
    or (user_id in (select user_id from mult_club_users) 
      and (club_id = origination_facility_id or club_id = destination_facility_id))
),

--if the user id is tied to more than 1 club, and none of the clubs are in origination or destination facillity id, then we get a max(club_id) 
clubs_max as (
  select 
      f.*
    , (select max_club_id from mult_club_users where mult_club_users.user_id = f.user_id) as final_club_id
  from clubs f
  where (f.user_id in (select user_id from mult_club_users)
    and (club_id <> origination_facility_id or club_id <> destination_facility_id))
),

--final step, union the filtered and max club results.
clubs_final as (
  select * from filtered_clubs
  union all
  (select * from clubs_max cm
  where cm.final_club_id = cm.club_id)
),
-- End Clubs

travel_agent as (
  select
      f.*
    , if(u.segment_user_profile_is_travel_agent is true, true, null) as is_travel_agent
  from clubs_final as f
  left outer join mongo_land.users as u
    on u._id = f.user_id
), 

microsites as (
  select
      f.*
    , m.name as micro_site
    , m.sub_domain as micro_site_subdomain
  from travel_agent as f
  left outer join mongo_land.micro_sites as m 
    on f.micro_site_id = m._id
),

travel_referrals as (
  select 
      f.*
    , if(t.travel_network = '' or travel_network = 'None', null, t.travel_network) as travel_network
    , if(t.travel_company = '', null, t.travel_company) as travel_company
    , if(t.agent_name = '', null, t.agent_name) as agent_name
    , if(t.travel_company <> '' or t.travel_network <> '' or t.agent_name <> '' or travel_network <> 'None', true, null) as travel_referral
    , case
        when length(trim(travel_network)) > 0 and lower(trim(travel_network)) <> 'none' then concat('Travel Network: ', travel_network)
        when length(trim(travel_company)) > 0 then concat('Travel Company: ', travel_company)
        when length(trim(agent_name)) > 0 then concat('Agent Name: ', agent_name)
        else 'None'
      end as travel_referral_info
    , case
        when length(trim(travel_network)) > 0 and lower(trim(travel_network)) not in ('none', 'n/a', 'repeat customer', 'happy previous customer', 'not available') then 1
        when length(trim(travel_company)) > 0 and lower(trim(travel_company)) not in ('none', 'n/a', 'repeat customer', 'happy previous customer', 'not available') then 1
        when length(trim(agent_name)) > 0 and lower(trim(agent_name)) not in ('none', 'n/a', 'repeat customer', 'happy previous customer', 'not available') then 1
        else 0
      end as travel_referral_cleanup_flag
  from microsites as f
  left outer join mongo_land.travel_referrals as t
    on f.internal_order_id = t.order_id
),

first_transaction as (
  select 
      f.*
    , min(f.transaction_financial_date) over (partition by f.user_id, f.brand) as first_transaction_date
  from travel_referrals as f    
)

select 
    *
  , case when transaction_financial_date = first_transaction_date then true else false end as is_first_transaction
  , case when pro_name is not null then 'Club Pro'
         when pro_name is null and is_travel_agent is true then 'Travel Agent'
         when (pro_name is null) and (is_travel_agent is not true) and (micro_site is not null) then 'Microsite'
         when (pro_name is null) and (is_travel_agent is not true) and (micro_site is null) and travel_referral_cleanup_flag = 1 then 'Travel Referral'
    end as b2b_revenue_attribution
from first_transaction;
