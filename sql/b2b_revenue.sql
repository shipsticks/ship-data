drop table if exists dp_staging.finsum_b2b;
create or replace table dp_staging.finsum_b2b as
with finsum_0 as (
select *
from dp_bi.finsum
where transaction_financial_date >= '2024-01-01'
), finsum_1 as (
select
  f.*
  , m.name as micro_site
  , m.sub_domain as micro_site_subdomain
from finsum_0 as f
left outer join mongo_land.micro_sites as m 
  on f.micro_site_id = m._id
), finsum_2 as (
select 
  f.*
  , t.travel_company
  , t.travel_network
  , t.agent_name
  , if(t.travel_company is not null or t.travel_network is not null or t.agent_name is not null, true, null) as travel_referral
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
  , if(u.segment_user_profile_is_travel_agent is true, true, null) as is_travel_agent
from finsum_3 as f
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
from finsum_4
;

-- credit waterfall (at shipment level)
-- 1) club pro
-- 2) travel-agent user role
-- 3) utm attrabution
-- 4) micro-site 
-- 5) travel referrals
