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


drop table if exists dp_bi.paid_campaigns;
create table if not exists dp_bi.paid_campaigns as
with ad_spend as (
select 
  brand
  , spend_date
  , source
  , campaign_name
  , campaign
  , spend
  , spend_day7
  , spend_day14
  , spend_day30
  , spend_day60
  , spend_day90
  , impressions
  , clicks
from dp_bi.ad_spend
where spend_date >= '2024-04-09'
), metrics as (
select
  coalesce(s.brand, u.brand) as brand
  , coalesce(s.spend_date, u.attrabtion_date) as event_date
  , coalesce(s.source, u.source) as source
  , coalesce(s.campaign_name, u.utm_campaign) as campaign
  , u.utm_medium
  , u.channel
  , s.spend
  , s.spend_day7
  , s.spend_day14
  , s.spend_day30
  , s.spend_day60
  , s.spend_day90
  , s.impressions
  , s.clicks
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
from dp_bi.utm_campaigns as u
full outer join ad_spend as s
  on s.brand = u.brand
  and s.spend_date = u.attrabtion_date
  and s.source = u.source
  and s.campaign = u.utm_campaign
)
select 
  *
  , case
      when source in ('Google') and contains_substr(campaign, 'pmax') then 'Google - Pmax'
      when source in ('Bing','Google') and contains_substr(campaign, '_brand_') then source || ' - ' || 'Brand'
      when source in ('Bing','Google') and not contains_substr(campaign, '_brand_') then source || ' - ' || 'Non-Brand'
      when source = 'Offline' then 'Offline'
      when source = 'Bing' and campaign = '10 - Brand - Old' then 'Bing - Brand'
      when source in ('Organic','Facebook') then source
      else 'Other Sources'
    end as display_source
from metrics
order by 1, 2, 3, 4
;
