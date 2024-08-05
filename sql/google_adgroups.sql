drop table if exists dp_staging.google_adgroup_spend;
create table if not exists dp_staging.google_adgroup_spend as
with brand_names as (
select distinct
  customer_id
  , customer_descriptive_name as brand
from `google_ads__land.account_performance_report`
), campaign_names as (
select distinct
  customer_id
  , campaign_id
from `google_ads__land.campaign_budget`
), names as (
select 
  b.customer_id
  , b.brand
  , c.campaign_id
from brand_names as b
join campaign_names as c
  on b.customer_id = c.customer_id
), ad_groups as (
select
  segments_date as spend_date
  , campaign_id
  , ad_group_campaign
  , ad_group_id
  , ad_group_name
  , ad_group_type
  , sum(metrics_cost_micros) / 1000000 as spend
from `google_ads__land.ad_group`
where metrics_cost_micros > 0
group by all
), campaigns as (
select 
  segments_date as spend_date
  , campaign_id
  , campaign_name
  , campaign_advertising_channel_type as campaign_type
  , sum(metrics_cost_micros) / 1000000 as spend
  , sum(metrics_impressions) as impressions
  , sum(metrics_clicks) as clicks
from `google_ads__land.campaign` as c
where metrics_cost_micros > 0
group by all
), adgroup_spend as (
select
  n.brand
  , coalesce(a.spend_date, c.spend_date) as spend_date
  , case when campaign_type = 'SEARCH' then 'Paid Search'
      when campaign_type = 'PERFORMANCE_MAX' then 'Performance Max'
      when campaign_type = 'VIDEO' then 'YouTube'
    end as channel
  , 'Google' as source
  , coalesce(a.campaign_id, c.campaign_id) as campaign_id
  , c.campaign_name
  , a.ad_group_id
  , a.ad_group_name
  , coalesce(a.spend, c.spend) as spend
--   , c.impressions
--   , c.clicks
--   , a.spend as a_spend
--   , c.spend as c_spend
from campaigns as c
left outer join ad_groups as a
  on c.spend_date = a.spend_date
  and c.campaign_id = a.campaign_id
left outer join names as n
  on c.campaign_id = n.campaign_id  
)
select
  *
  , if(spend_date >= date_sub(current_date(), interval 7 day), null, spend) as spend_day7
  , if(spend_date >= date_sub(current_date(), interval 14 day), null, spend) as spend_day14
  , if(spend_date >= date_sub(current_date(), interval 30 day), null, spend) as spend_day30
  , if(spend_date >= date_sub(current_date(), interval 60 day), null, spend) as spend_day60
  , if(spend_date >= date_sub(current_date(), interval 90 day), null, spend) as spend_day90
from adgroup_spend
order by 1 desc, 2
;


drop table if exists dp_bi.utm_campaigns_term;
create table if not exists dp_bi.utm_campaigns_term as
select
  brand
  , date(attrabtion_at) as attrabtion_date
  , channel
  , source
  , utm_medium
  , utm_source
  , utm_campaign
  , utm_term
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


drop table if exists dp_bi.google_adgroups;
create table if not exists dp_bi.google_adgroups as
with ad_spend as (
select 
  brand
  , spend_date
  , source
  , campaign_id
  , campaign_name
  , ad_group_id
  , ad_group_name
  , spend
  , spend_day7
  , spend_day14
  , spend_day30
  , spend_day60
  , spend_day90
from dp_staging.google_adgroup_spend
where spend_date >= '2024-04-09'
), metrics as (
select
  coalesce(s.brand, u.brand) as brand
  , coalesce(s.spend_date, u.attrabtion_date) as event_date
  , coalesce(s.source, u.source) as source
  , coalesce(s.campaign_name, u.utm_campaign) as campaign
  , coalesce(s.ad_group_name, u.utm_term) as adgroup
  , s.spend
  , s.spend_day7
  , s.spend_day14
  , s.spend_day30
  , s.spend_day60
  , s.spend_day90
  , u.prospects
  , u.leads
  , u.users
  , u.quote
  , u.start_shipping
  , u.begin_checkout
  , u.sign_up
  , u.login
  , u.purchase
  , u.revenue_day7
  , u.revenue_day14 
  , u.revenue_day30
  , u.revenue_day60
  , u.revenue_day90
  , u.revenue_total
from dp_bi.utm_campaigns_term as u
join ad_spend as s
  on s.brand = u.brand
  and s.spend_date = u.attrabtion_date
  and s.source = u.source
  and cast(s.campaign_id as string) = u.utm_campaign
  and cast(s.ad_group_id as string) = u.utm_term
where u.source = 'Google'
)
select 
  *
  , case
      when source in ('Google') and contains_substr(campaign, 'pmax') then 'Google - Pmax'
      when source in ('Bing','Google') and contains_substr(campaign, '_brand_') then source || ' - ' || 'Brand'
      when source in ('Bing','Google') and not contains_substr(campaign, '_brand_') then source || ' - ' || 'Non-Brand'
    end as display_source
from metrics
order by 1, 2, 3, 4
;
