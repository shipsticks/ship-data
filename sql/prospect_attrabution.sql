-- get anon first event at and userid created at
drop table if exists dp_staging.att_0;
create table if not exists dp_staging.att_0 as
with tmp1 as (
select
  anonymous_id
  , brand
  , count(distinct user_id) as unq_users
  , min(user_id) as user_id
  , count(distinct if(is_non_email_source is true, utm_source, null)) as unq_att_noemail_source
  , count(distinct if(paid_digital is not null, utm_source, null)) as unq_att_paid_source
  , min(timestamp) as first_event_at
  , min(if(event = 'login', timestamp, null)) as first_login_at
  , min(if(user_id is not null, timestamp, null)) as first_user_id_at
  , min(if(event = 'sign_up', timestamp, null)) as first_sign_up_at
  , min(if(event = 'generate_lead', timestamp, null)) as first_generate_lead_at
  , min(if(event in ('quick_quote', 'pricing_page_quote'), timestamp, null)) as first_quote_at
  , min(if(event = 'start_shipping', timestamp, null)) as first_start_shipping_at
  , min(if(event = 'begin_checkout', timestamp, null)) as first_begin_checkout_at
  , min(if(event = 'purchase', timestamp, null)) as first_purchase_at
  , min(user_email) as user_email
  , sum(revenue) as total_rs_revenue
from dp_staging.meta_events
group by all
)
select
  a.*
  , u.created_at as user_created_at
  , registration_domain
  , if(u._id is not null, 'cookie_conversion', 'no_conversion') as user_conversion_type
  , segment_user_profile_is_admin as is_admin
  , segment_user_profile_is_pro as is_pro
from tmp1 as a
left outer join mongo_land.users as u
  on a.user_id = u._id
;

-- also join on email to get user-id for lead-gen conversion
drop table if exists dp_staging.att_1;
create table if not exists dp_staging.att_1 as
with email as (
select
  a.anonymous_id
  , a.brand
  , a.user_email
  , u._id as user_id
  , u.created_at as user_created_at
  , u.registration_domain
  , 'email_conversion' as user_conversion_type
  , u.segment_user_profile_is_admin as is_admin
  , u.segment_user_profile_is_pro as is_pro
from dp_staging.att_0 as a
join mongo_land.users as u
  on a.user_email = u.email
), tmp2 as (
select
  a.anonymous_id
  , a.brand
  , a.unq_users
  , coalesce(a.user_id, e.user_id) as user_id
  , coalesce(a.user_created_at, e.user_created_at) as user_created_at
  , a.user_email
  , coalesce(a.registration_domain, e.registration_domain) as registration_domain
  , coalesce(a.is_admin, e.is_admin) as is_admin
  , coalesce(a.is_pro, e.is_pro) as is_pro
  , a.unq_att_noemail_source
  , a.unq_att_paid_source
  , a.first_event_at
  , a.first_login_at
  , coalesce(a.first_user_id_at, e.user_created_at) as first_user_id_at
  , a.first_sign_up_at
  , a.first_generate_lead_at
  , a.first_quote_at
  , a.first_start_shipping_at
  , a.first_begin_checkout_at
  , a.first_purchase_at
  , a.total_rs_revenue
  , coalesce(a.user_conversion_type, e.user_conversion_type) as user_conversion_type
from dp_staging.att_0 as a
left outer join email as e
  on a.anonymous_id = e.anonymous_id
  and a.brand = e.brand
), tmp3 as (
select 
  *
  , case when first_event_at > user_created_at then 'user-created-pre-rudder'
      when first_event_at <= user_created_at then 'user-created-post-rudder'
      when user_created_at is null then 'prospect'
    end as user_type
from tmp2
)
select *
from tmp3
where user_type <> 'user-created-pre-rudder'
;


-- add first touch attrabution
drop table if exists dp_staging.att_2;
create table if not exists dp_staging.att_2 as
select
  e.anonymous_id
  , s.brand
  , e.channel
  , e.source
  , e.utm_medium
  , e.utm_source
  , e.utm_campaign
  , e.domain || e.url_path as landing_page
  , min(e.timestamp) as attrabtion_at
from dp_staging.att_1 as s
join dp_staging.meta_events as e
  on s.anonymous_id = e.anonymous_id
  and date(e.timestamp) between date(s.first_event_at) and date(s.user_created_at)
where
  s.user_type = 'user-created-post-rudder'
  and e.is_non_email_source = True 
group by all
qualify row_number() over (partition by e.anonymous_id, s.brand order by min(e.timestamp)) = 1

union all

select
  e.anonymous_id
  , s.brand
  , e.channel
  , e.source
  , e.utm_medium
  , e.utm_source
  , e.utm_campaign
  , e.domain || e.url_path as landing_page
  , min(e.timestamp) as attrabtion_at
from dp_staging.att_1 as s
join dp_staging.meta_events as e
  on s.anonymous_id = e.anonymous_id
where
  s.user_type = 'prospect'
  and e.is_non_email_source = True
group by all
qualify row_number() over (partition by e.anonymous_id, s.brand order by min(e.timestamp)) = 1
;


-- add organic prospects/users
drop table if exists dp_staging.att_3;
create table if not exists dp_staging.att_3 as
select 
  s.anonymous_id
  , s.brand
  , coalesce(a.attrabtion_at, s.first_event_at) as attrabtion_at
  , coalesce(a.channel, 'Organic') as channel
  , coalesce(a.source, 'Organic') as source
  , a.utm_medium
  , a.utm_source
  , a.utm_campaign
  , a.landing_page
from dp_staging.att_1 as s
left outer join dp_staging.att_2 as a
  on s.anonymous_id = a.anonymous_id
  and s.brand = a.brand
;


-- merge event coversion and attrabution
drop table if exists dp_staging.att_4;
create table if not exists dp_staging.att_4 as
select 
  a.*
  , b.attrabtion_at
  , b.channel
  , b.source
  , b.utm_medium
  , b.utm_source
  , b.utm_campaign
  , b.landing_page
from dp_staging.att_1 as a
join dp_staging.att_3 as b
  on a.anonymous_id = b.anonymous_id
  and a.brand = b.brand
;


-- dedup users with multiple anonymous_ids
-- drop table if exists dp_staging.att_5a;
-- create table if not exists dp_staging.att_5a as
-- select *
-- from dp_staging.att_4
-- where user_id is null

-- union all

-- select *
-- from dp_staging.att_4
-- where user_id is not null
-- qualify row_number() over(partition by brand, user_id order by first_event_at) = 1
-- ;


-- dedup users with multiple anonymous_ids, use first non-organic anon if present.
drop table if exists dp_staging.att_5;
create table if not exists dp_staging.att_5 as
with paid_anons as (
select *
from dp_staging.att_4
where 
  user_id is not null
  and source <> 'Organic'
qualify row_number() over(partition by brand, user_id order by first_event_at) = 1
), all_anons as (
select *
from dp_staging.att_4
where 
  user_id is not null
qualify row_number() over(partition by brand, user_id order by first_event_at) = 1
), users as (
select
  coalesce(p.anonymous_id, a.anonymous_id) as anonymous_id
  , a.brand
  , 1 as unq_users
  , a.user_id
  , a.user_created_at
  , coalesce(p.user_email, a.user_email) as user_email
  , a.registration_domain
  , a.is_admin
  , a.is_pro
  , coalesce(p.unq_att_noemail_source, a.unq_att_noemail_source) as unq_att_noemail_source
  , coalesce(p.unq_att_paid_source, a.unq_att_paid_source) as unq_att_paid_source
  , coalesce(p.first_event_at, a.first_event_at) as first_event_at
  , coalesce(p.first_login_at, a.first_login_at) as first_login_at
  , coalesce(p.first_user_id_at, a.first_user_id_at) as first_user_id_at
  , coalesce(p.first_sign_up_at, a.first_sign_up_at) as first_sign_up_at
  , coalesce(p.first_generate_lead_at, a.first_generate_lead_at) as first_generate_lead_at
  , coalesce(p.first_quote_at, a.first_quote_at) as first_quote_at

  , coalesce(p.first_start_shipping_at, a.first_start_shipping_at) as first_start_shipping_at
  , coalesce(p.first_begin_checkout_at, a.first_begin_checkout_at) as first_begin_checkout_at
  , coalesce(p.first_purchase_at, a.first_purchase_at) as first_purchase_at
  , coalesce(p.total_rs_revenue, a.total_rs_revenue) as total_rs_revenue
  
  , coalesce(p.user_conversion_type, a.user_conversion_type) as user_conversion_type
  , coalesce(p.user_type, a.user_type) as user_type
  , coalesce(p.attrabtion_at, a.attrabtion_at) as attrabtion_at
  , coalesce(p.channel, a.channel) as channel
  , coalesce(p.source, a.source) as source
  , coalesce(p.utm_medium, a.utm_medium) as utm_medium
  , coalesce(p.utm_source, a.utm_source) as utm_source
  , coalesce(p.utm_campaign, a.utm_campaign) as utm_campaign
  , coalesce(p.landing_page, a.landing_page) as landing_page
from all_anons as a
left outer join paid_anons as p
  on a.brand = p.brand
  and a.user_id = p.user_id
)
select *
from users

union all

select *
from `dp_staging.att_4`
where user_id is null
;


-- add user LTV
drop table if exists dp_bi.prospects;
create table if not exists dp_bi.prospects as
with tmp as (
select
  u.*
  , sum(case when f.`Shipment Created At` >= date(u.attrabtion_at)
          and f.`Shipment Created At` <= date_add(date(u.attrabtion_at), interval 7 day) 
          then f.price_cents else 0 end) / 100 as ltv_day7
  , sum(case when f.`Shipment Created At` >= date(u.attrabtion_at)
          and f.`Shipment Created At` <= date_add(date(u.attrabtion_at), interval 14 day) 
          then f.price_cents else 0 end) / 100 as ltv_day14
  , sum(case when f.`Shipment Created At` >= date(u.attrabtion_at)
          and f.`Shipment Created At` <= date_add(date(u.attrabtion_at), interval 30 day) 
          then f.price_cents else 0 end) / 100 as ltv_day30
  , sum(case when f.`Shipment Created At` >= date(u.attrabtion_at)
          and f.`Shipment Created At` <= date_add(date(u.attrabtion_at), interval 60 day) 
          then f.price_cents else 0 end) / 100 as ltv_day60
  , sum(case when f.`Shipment Created At` >= date(u.attrabtion_at)
          then f.price_cents else 0 end) / 100 as ltv_full
from dp_staging.att_5 as u
left outer join `bi.financial_summary_detail_v5` as f
  on u.user_id = f.user_id
  and f.`Brand` = u.brand
group by all
)
select 
  * except (ltv_day7, ltv_day14, ltv_day30, ltv_day60)
  , if(user_created_at >= timestamp_sub(current_timestamp(), interval 7 day), null, ltv_day7) as ltv_day7
  , if(user_created_at >= timestamp_sub(current_timestamp(), interval 14 day), null, ltv_day14) as ltv_day14
  , if(user_created_at >= timestamp_sub(current_timestamp(), interval 30 day), null, ltv_day30) as ltv_day30
  , if(user_created_at >= timestamp_sub(current_timestamp(), interval 60 day), null, ltv_day60) as ltv_day60
from tmp
;
