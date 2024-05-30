drop table if exists dp_bi.paid_campaigns;
create table if not exists dp_bi.paid_campaigns as
with ad_spend as (
select *
from dp_bi.ad_spend
where spend_date >= '2024-04-09'    
), metrics as (
select
  coalesce(s.brand, u.brand) as brand
  , coalesce(s.spend_date, u.event_date) as event_date
  , coalesce(s.source, u.source) as source
  , coalesce(s.campaign_name, u.utm_campaign) as campaign
  , s.spend
  , s.spend_day7
  , s.spend_day14
  , s.spend_day30
  , s.impressions
  , s.clicks
  , u.page_views
  , u.site_visits
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
  , u.revenue_total
  , u.first_event_2_lead_days
  , u.first_event_2_quote_days
  , u.first_event_2_user_days
  , u.first_event_2_purchase_days
  , u.quote_2_purchase_days
from dp_bi.utm_campaigns as u
full outer join ad_spend as s
  on s.brand = u.brand
  and s.spend_date = u.event_date
  and s.source = u.source
  and s.campaign = u.utm_campaign
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
from metrics
order by 1, 2, 3, 4
;
