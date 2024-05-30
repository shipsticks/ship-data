drop table if exists dp_bi.utm_campaigns;
create table if not exists dp_bi.utm_campaigns as
with site_visits as (
select
  brand
  , date(timestamp_utc) as event_date
  , channel
  , source
  -- , utm_medium
  -- , utm_source
  , utm_campaign
  , count(1) as page_views
  , count(distinct anonymous_id) as site_visits
from dp_bi.rudderstack_events
where
  date(timestamp_utc) >= '2024-04-09'
group by all
), user_att as (
select
  brand
  , date(attrabtion_at) as attrabtion_date
  , channel
  , source
  -- , utm_medium
  -- , utm_source
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
  , count(first_purchase_at) as purchase
  -- ltv
  , sum(ltv_day7) as revenue_day7
  , sum(ltv_day14) as revenue_day14
  , sum(ltv_day30) as revenue_day30
  , sum(ltv_day60) as revenue_day60
  , sum(ltv_full) as revenue_total
  -- from first event diffs
  , sum(timestamp_diff(user_created_at, first_event_at, day)) as first_event_2_user_days
  , sum(timestamp_diff(first_generate_lead_at, first_event_at, day)) as first_event_2_lead_days
  , sum(timestamp_diff(first_purchase_at, first_event_at, day)) as first_event_2_purchase_days
  -- purchase funnel diffs
  , sum(timestamp_diff(first_quote_at, first_event_at, day)) as first_event_2_quote_days
  , sum(timestamp_diff(first_start_shipping_at, first_quote_at, day)) as quote_2_start_shipping_days
  , sum(timestamp_diff(first_begin_checkout_at, first_start_shipping_at, day)) as start_shipping_2_begin_checkout_days
  , sum(timestamp_diff(first_purchase_at, first_begin_checkout_at, day)) as begin_checkout_2_purchase_days
  , sum(timestamp_diff(first_purchase_at, first_quote_at, day)) as quote_2_purchase_days
from dp_bi.prospects
where 
  attrabtion_at is not null
  and is_admin is not true
  and is_pro is not true
group by all
)
select
  coalesce(u.brand, v.brand) as brand
  , coalesce(u.attrabtion_date, v.event_date) as event_date
  , coalesce(u.channel, v.channel) as channel
  , coalesce(u.source, v.source) as source
  -- , coalesce(u.utm_medium, v.utm_medium) as utm_medium
  -- , coalesce(u.utm_source, v.utm_source) as utm_source
  , coalesce(u.utm_campaign, v.utm_campaign) as utm_campaign
  , v.page_views
  , v.site_visits
  , u.prospects
  , u.leads
  , u.users
  , u.quote
  , u.sign_up
  , u.login
  , u.start_shipping
  , u.begin_checkout
  , u.purchase
  , u.revenue_day7
  , u.revenue_day14 
  , u.revenue_day30
  , u.revenue_day60
  , u.revenue_total
  , u.first_event_2_user_days
  , u.first_event_2_lead_days
  , u.first_event_2_quote_days
  , u.first_event_2_purchase_days
  , u.quote_2_purchase_days
from user_att as u
left outer join site_visits as v
  on v.brand = u.brand
  and v.event_date = u.attrabtion_date
  and v.channel = u.channel
  and v.source = u.source
  and v.utm_campaign = u.utm_campaign
  -- and v.utm_medium = u.utm_medium
  -- and v.utm_source = u.utm_source
;