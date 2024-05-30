drop table if exists dp_staging.campaign_metrics;
create table if not exists dp_staging.campaign_metrics as
with source_spend as (
select
  brand
  , date(spend_date) as spend_date
  , source
  , campaign
  , campaign_name
  , sum(spend) as spend
  , sum(spend_day7) as spend_day7
  , sum(spend_day14) as spend_day14
  , sum(spend_day30) as spend_day30
  , sum(impressions) as impressions
  , sum(clicks) as clicks
from `dp_bi.ad_spend`
where spend_date >= '2024-04-09'
group by all
), site_visits as (
select
  brand
  , date(timestamp_utc) as event_date
  , source
  , utm_campaign as campaign
  , count(1) as pageviews
  , count(distinct anonymous_id) as visitors
from dp_bi.rudderstack_events
where
  date(timestamp_utc) >= '2024-04-09'
group by all
), user_att as (
select
  brand
  , date(attrabtion_at) as attrabtion_date
  , source
  , utm_campaign as campaign
  -- , landing_page
  , count(1) as unq_anons
  , count(distinct user_id) as unq_users
  , count(first_sign_up_at) as total_sign_up
  , count(first_login_at) as total_login
  , count(first_generate_lead_at) as total_lead_gen
  , count(first_quote_at) as total_quote
  , count(first_start_shipping_at) as total_start_shipping
  , count(first_begin_checkout_at) as total_begin_checkout
  , count(first_purchase_at) as total_purchase
  , sum(ltv_day7) as revenue_day7
  , sum(ltv_day14) as revenue_day14
  , sum(ltv_day30) as revenue_day30
  , sum(ltv_full) as revenue_total

  -- from first event diffs
  , avg(timestamp_diff(user_created_at, first_event_at, day)) as first_event_2_user_days
  , avg(timestamp_diff(first_generate_lead_at, first_event_at, day)) as first_event_2_lead_days
  , avg(timestamp_diff(first_purchase_at, first_event_at, day)) as first_event_2_purchase_days
  
  -- purchase funnel diffs
  , avg(timestamp_diff(first_quote_at, first_event_at, day)) as first_event_2_quote_days
  , avg(timestamp_diff(first_start_shipping_at, first_quote_at, day)) as quote_2_start_shipping_days
  , avg(timestamp_diff(first_begin_checkout_at, first_start_shipping_at, day)) as start_shipping_2_begin_checkout_days
  , avg(timestamp_diff(first_purchase_at, first_begin_checkout_at, day)) as begin_checkout_2_purchase_days

  , avg(timestamp_diff(first_purchase_at, first_quote_at, day)) as quote_2_purchase_days
from dp_bi.prospects
where 
  attrabtion_at is not null
  and is_admin is not true
  and is_pro is not true
group by all
), campaigns as (
select
  coalesce(s.brand, u.brand) as brand
  , coalesce(s.spend_date, u.attrabtion_date) as event_date
  , coalesce(s.source, u.source) as source
  , coalesce(s.campaign_name, u.campaign) as campaign
  -- , landing_page
  , s.spend
  , s.spend_day7
  , s.spend_day14
  , s.spend_day30

  , s.impressions
  , s.clicks
  , v.pageviews
  , v.visitors
  , u.unq_anons as new_prospects
  , u.total_lead_gen as new_leads
  , u.unq_users as new_users
  , u.total_quote
  , u.total_start_shipping
  , u.total_begin_checkout
  , u.total_purchase
  , u.revenue_day7
  , u.revenue_day14 
  , u.revenue_day30

  , u.revenue_total
  , u.first_event_2_user_days
  , u.first_event_2_lead_days
  , u.first_event_2_quote_days
  , u.first_event_2_purchase_days
  , u.quote_2_purchase_days
from user_att as u
left outer join source_spend as s
  on s.brand = u.brand
  and s.spend_date = u.attrabtion_date
  and s.source = u.source
  and s.campaign = u.campaign
left outer join site_visits as v
  on v.brand = u.brand
  and v.event_date = u.attrabtion_date
  and v.source = u.source
  and v.campaign = u.campaign
)
select 
  *
  , case 
      when source in ('Google') and contains_substr(campaign, 'pmax') then 'Google - Pmax'
      when source in ('Bing','Google') and contains_substr(campaign, '_brand_') then source || ' - ' || 'Brand'
      when source in ('Bing','Google') and not contains_substr(campaign, '_brand_') then source || ' - ' || 'Non-Brand'
      when source not in ('Organic','Facebook') then 'Other Sources'
      when source = 'Bing' and campaign = '10 - Brand - Old' then 'Bing - Brand'
      else source
    end as display_source
from campaigns
order by 1, 2, 3, 4
;

-- drop table if exists dp_staging.utm_campaigns_backup;
-- create table if not exists dp_staging.utm_campaigns_backup as
-- select *
-- from dp_bi.utm_campaigns
-- ;


-- drop table if exists dp_bi.utm_campaigns;
-- create table if not exists dp_bi.utm_campaigns as
-- select *
-- from dp_staging.campaign_metrics
-- ;
