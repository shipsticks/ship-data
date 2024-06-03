-- credit waterfall (at shipment level)
-- 1) club pro
-- 2) travel-agent user role
-- 3) utm attrabution
-- 4) micro-site 
-- 5) travel referrals

-- drop table if exists dp_bi.finsum;
-- create or replace table dp_bi.finsum as
with finsum_0 as (
select *
from dp_bi.finsum
where transaction_financial_date >= '2024-01-01'
), finsum_1 as (
select
  f.*
  , m.name as mico_site
  , m.sub_domain as mico_site_subdomain
from finsum_0 as f
left outer join mongo_land.micro_sites as m 
  on f.micro_site_id = m._id
), finsum_2 as (
select 
  f.*
  , t.travel_company
  , t.travel_network
  , t.agent_name
from finsum_1 as f
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
  , user_id
  , micro_site_id
  , facility_type
from `mongo_land.clubs` as c
join `mongo_land.brands` as b
  on c.reporting_brand_id = b._id
where 
  c.no_pro is false
  and c.user_id <> '<NA>'
), finsum_3 as (
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
from finsum_2 as f
left outer join club_pros as p 
  on p.user_id = f.user_id
), finsum_4 as (
select
  f.*
  , u.segment_user_profile_is_travel_agent as is_travel_agent
from finsum_3 as f
left outer join mongo_land.users as u
  on u._id = f.user_id
)
select *
from finsum_4
;