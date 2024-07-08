drop table if exists dp_bi.utm_campaigns;
create table if not exists dp_bi.utm_campaigns as
select
  brand
  , date(attrabtion_at) as attrabtion_date
  , channel
  , source
  , utm_medium
  , utm_source
  , utm_campaign
  -- funnel 
  , count(1) as prospects
  , count(first_generate_lead_at) as leads
  , count(distinct user_id) as users
  , count(first_sign_up_at) as sign_up
  , count(first_login_at) as login
  , count(first_quote_at) as quote
  , count(first_start_shipping_at) as start_shipping
  , count(first_begin_checkout_at) as begin_checkout
  , count(first_shipment_at) as purchase
  -- ltv
  , sum(ltv_day7) as revenue_day7
  , sum(ltv_day14) as revenue_day14
  , sum(ltv_day30) as revenue_day30
  , sum(ltv_day60) as revenue_day60
  , sum(ltv_day90) as revenue_day90
  , sum(ltv_full) as revenue_total
from dp_bi.prospects
where 
  attrabtion_at is not null
  and is_admin is not true
  and is_pro is not true
group by all
;