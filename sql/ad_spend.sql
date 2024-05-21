-- marketing spend data
drop table if exists dp_staging.google_ads_spend;
create table if not exists dp_staging.google_ads_spend as 
with brand_names as (
select distinct
  customer_id
  , customer_descriptive_name as brand
from `google_ads__land.account_performance_report`
), campaign_names as (
select distinct
  customer_id
  , campaign_id
  , campaign_budget_name as campaign_name
from `google_ads__land.campaign_budget`
), names as (
select 
  b.customer_id
  , b.brand
  , c.campaign_id
  , c.campaign_name
from brand_names as b
join campaign_names as c
  on b.customer_id = c.customer_id
)
select 
  date_trunc(segments_date, day) as spend_date
  , case when campaign_advertising_channel_type = 'SEARCH' then 'Paid Search'
      when campaign_advertising_channel_type = 'PERFORMANCE_MAX' then 'Performance Max'
      when campaign_advertising_channel_type = 'VIDEO' then 'YouTube'
    end as channel
  , 'Google' as source
  , n.brand
  , cast(n.campaign_id as string) as campaign
  , n.campaign_name
  , sum(c.metrics_cost_micros) / 1000000 as spend
  , sum(metrics_impressions) as impressions
  , sum(metrics_clicks) as clicks
from `google_ads__land.campaign` as c
join names as n
  on n.campaign_id = c.campaign_id
group by all
order by 1 desc, 2
;


drop table if exists dp_staging.source_spend;
create table if not exists dp_staging.source_spend as 
with source_spend as (
select
  date(date_trunc(TimePeriod, day)) as spend_date
  , 'Paid Search' as channel
  , 'Bing' as source
  , AccountName as brand
  , CampaignName as campaign
  , null as campaign_name
  -- , AdGroupName as adgroup
  , sum(Spend) as spend
  , sum(Impressions) as impressions
  , sum(Clicks) as clicks
from `microsoft_ads__land.campaign_performance_report_daily` 
where Spend > 0
group by all

union all 

select
  date(date_trunc(date_start, day)) as spend_date
  , 'Paid Social' as channel
  , 'Facebook' as source
  , account_name as brand
  , campaign_name as campaign
  , null as campaign_name
  -- , adset_name as adgroup
  -- , ad_name
  , sum(spend) as spend
  , sum(impressions) as impressions
  , sum(clicks) as clicks
from `facebook__land.ads_insights`
where spend > 0
group by all

union all

select 
  spend_date
  , channel
  , source
  , brand
  , campaign
  , campaign_name
  , spend
  , impressions
  , clicks
from dp_staging.google_ads_spend
where spend > 0
)
select 
  * 
  , if(spend_date >= date_sub(current_date(), interval 7 day), null, spend) as spend_day7
  , if(spend_date >= date_sub(current_date(), interval 14 day), null, spend) as spend_day14
  , if(spend_date >= date_sub(current_date(), interval 30 day), null, spend) as spend_day30
  , if(spend_date >= date_sub(current_date(), interval 60 day), null, spend) as spend_day60
from source_spend
order by 1 desc, 2, 3, 4, 5, 6
;
