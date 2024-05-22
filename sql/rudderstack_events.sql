 -- MERGE dp_staging.meta_events as t
-- USING dp_staging.meta_events_staging as s
-- ON t.it = s.id
-- -- WHEN MATCHED THEN
-- --   UPDATE SET annual_ctc = S.annual_ctc
-- WHEN NOT MATCHED THEN
--   INSERT ROW
-- ;


-- union rudderstack track+page events. classify utm parameters
drop table if exists dp_staging.meta_events;
create table if not exists dp_staging.meta_events as
with events as (
select
  id 
  , timestamp
  , context_session_id as session_id
  , net.reg_domain(context_page_url) as domain
  , anonymous_id
  , user_id
  , 'pageview' as event
  , cast(null as numeric) as revenue
  , cast(null as string) as user_email
  , context_page_path as url_path
  , referring_domain
  , context_campaign_medium as utm_medium
  , context_campaign_source as utm_source
  , context_campaign_name as utm_campaign
from `gse-dw-prod.rudderstack_prod_land.pages`
-- where
--   date(_PARTITIONTIME) between date_sub(current_date(), INTERVAL 10 day) and current_date()
qualify 
  row_number() over (partition by id order by loaded_at desc) = 1

union all

select
  id 
  , timestamp
  , context_session_id as session_id
  , net.reg_domain(context_page_url) as domain
  , anonymous_id
  , user_id
  , event
  , cast(null as numeric) as revenue
  , cast(null as string) as user_email
  , context_page_path as url_path
  , cast(null as string) as referring_domain
  , context_campaign_medium as utm_medium
  , context_campaign_source as utm_source
  , context_campaign_name as utm_campaign
from `gse-dw-prod.rudderstack_prod_land.tracks`
where
  event not in ('select_service_level','purchase','generate_lead')
  -- and date(_PARTITIONTIME) between date_sub(current_date(), INTERVAL 10 day) and current_date()
qualify 
  row_number() over (partition by id order by loaded_at desc) = 1

union all

select
  id 
  , timestamp
  , context_session_id as session_id
  , net.reg_domain(context_page_url) as domain
  , anonymous_id
  , coalesce(user_id, ecommerce_purchase_action_field_user_id) as user_id
  , 'purchase' as event
  , cast(ecommerce_purchase_action_field_revenue as numeric) as revenue
  , cast(null as string) as user_email
  , context_page_path as url_path
  , cast(null as string) as referring_domain
  , cast(null as string) as utm_medium
  , cast(null as string) as utm_source
  , cast(null as string) as utm_campaign
from `gse-dw-prod.rudderstack_prod_land.purchase`
-- where
  -- and date(_PARTITIONTIME) between date_sub(current_date(), INTERVAL 10 day) and current_date()
qualify 
  row_number() over (partition by id order by loaded_at desc) = 1

union all

select
  id 
  , timestamp
  , context_session_id as session_id
  , net.reg_domain(context_page_url) as domain
  , anonymous_id
  , user_id
  , 'generate_lead' as event
  , cast(null as numeric) as revenue
  , user_email
  , context_page_path as url_path
  , cast(null as string) as referring_domain
  , cast(null as string) as utm_medium
  , cast(null as string) as utm_source
  , cast(null as string) as utm_campaign
from `gse-dw-prod.rudderstack_prod_land.generate_lead`
-- where
  -- and date(_PARTITIONTIME) between date_sub(current_date(), INTERVAL 10 day) and current_date()
qualify 
  row_number() over (partition by id order by loaded_at desc) = 1

)
select 
  *
  , case when domain = 'shipgo.com' then 'ShipGo' 
      when domain = 'shipsticks.com' then 'Ship Sticks' 
      when domain = 'shipskis.com' then 'Ship Skis'
      when domain = 'shipcamps.com' then 'Ship Camps'
      when domain = 'luggagefree.com' then 'Luggage Free'
      when domain = 'shipschools.com' then 'Ship Schools'
      when domain = 'shipplay.com' then 'Ship & Play'
    end as brand
  , case
      when (utm_medium is null) or (utm_source is null)
          or (contains_substr(utm_medium || utm_source, 'organic')) then 'Organic'
      when contains_substr(utm_medium || utm_source, 'email') then 'Email'
      when (contains_substr(utm_medium || utm_source, 'referral') 
          or contains_substr(utm_medium || utm_source, 'referrral')) then 'Referral'
      when (contains_substr(utm_medium || utm_source, 'cpc') 
          or contains_substr(utm_source, 'google')) then 'Paid Search'
      when (contains_substr(utm_medium || utm_source, 'social')
          or contains_substr(utm_medium || utm_source, 'facebook')
          or contains_substr(utm_medium || utm_source, 'paidfb')) then 'Paid Social'
      when (contains_substr(utm_medium || utm_source, 'display') 
          or contains_substr(utm_medium || utm_source, 'adroll')) then 'Display'
      when contains_substr(utm_medium || utm_source, 'partner') then 'Partner'
      when contains_substr(utm_medium || utm_source, 'performancemax') then 'Performance Max'
      when contains_substr(utm_medium || utm_source, 'youtube') then 'YouTube'
      when contains_substr(utm_medium || utm_source, 'affiliate') then 'Affiliate'
      else 'Other'
    end as channel
  , case 
      when contains_substr(utm_source, 'bing') then 'Bing'
      when (contains_substr(utm_source, 'facebook') or contains_substr(utm_source, 'fb')) then 'Facebook'
      when (contains_substr(utm_source, 'google') or contains_substr(utm_source, 'adwords')) then 'Google'
      when contains_substr(utm_source, 'email') then 'Sailthru'
      when contains_substr(utm_source, 'organic') or utm_source = '<NA>' then 'Organic'
      else utm_source
    end as source
  , case 
      when contains_substr(utm_source, 'bing') then 'Bing'
      when (contains_substr(utm_source, 'facebook') or contains_substr(utm_source, 'fb')) then 'Facebook'
      when (contains_substr(utm_source, 'google') or contains_substr(utm_source, 'adwords')) then 'Google'
      else null
    end as paid_digital
  , case when contains_substr(utm_source, 'bing') 
      or contains_substr(utm_source, 'facebook') 
      or contains_substr(utm_source, 'fb')
      or contains_substr(utm_source, 'google')
      or contains_substr(utm_source, 'adwords') 
      then True else False
    end is_paid_digital
  , case when utm_source is not null
      and lower(utm_medium) <> 'email' 
      and lower(utm_source) <> 'sailthru'
      then True else False
    end is_non_email_source
  , case when utm_medium = 'email' and starts_with(utm_campaign, 'skis_')
      or utm_medium = 'email' and starts_with(utm_campaign, 'sticks_')
      or utm_medium = 'email' and starts_with(utm_campaign, 'shipgo_')
      or utm_medium = 'email' and starts_with(utm_campaign, 'schools_')
      or utm_medium = 'email' and starts_with(utm_campaign, 'free_')
      or utm_medium = 'email' and starts_with(utm_campaign, 'camps_')
      or utm_medium = 'email' and utm_source = 'SendGrid'
      or utm_medium = 'email' and contains_substr(utm_source, 'Transactional')
      or utm_medium = 'email' and contains_substr(utm_source, 'confirmationlink') 
      then True else False 
    end as is_transaction_email
from events
where 
  domain not in ('ledgesvacationrentals.com', 'rtcc.net')
  and timestamp >= '2024-04-09'
;


drop table if exists dp_bi.rudderstack_events;
create table if not exists dp_bi.rudderstack_events as
select *
from dp_staging.meta_events
;