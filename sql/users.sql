drop table if exists adhoc.user_geo;
create table if not exists adhoc.user_geo as
with user_origination_geos as (
select
  user_id
  , origination_ship_point_country_code || '-' || origination_ship_point_state || '-' || origination_ship_point_city as user_geo
  , count(1) as total
from mongo_land.v5_shipments
where
  origination_ship_point_facility_id = '<NA>'
  and state <> 'cancelled'
group by all
), user_origination_geo as (
select 
  user_id
  , user_geo
from user_origination_geos
qualify row_number() over(partition by user_id order by total desc) = 1
order by user_id, total desc
), user_destination_geos as (
select
  user_id
  , destination_ship_point_country_code || '-' || destination_ship_point_state || '-' || destination_ship_point_city as user_geo
  , count(1) as total
from mongo_land.v5_shipments
where 
  destination_ship_point_facility_id = '<NA>'
  and state <> 'cancelled'
group by all
), user_destination_geo as (
select 
  user_id
  , user_geo
from user_destination_geos
qualify row_number() over(partition by user_id order by total desc) = 1
)
select 
  COALESCE(o.user_id, d.user_id) as user_id
  , COALESCE(o.user_geo, d.user_geo) as user_geo
from user_origination_geo as o
full outer join user_destination_geo as d
  on o.user_id = d.user_id
order by user_id
;

drop table if exists adhoc.user_geo_state;
create table if not exists adhoc.user_geo_state as
with user_origination_geos as (
select
  user_id
  , origination_ship_point_country_code || '-' || origination_ship_point_state as user_state
  , count(1) as total
from mongo_land.v5_shipments
where 
  origination_ship_point_facility_id = '<NA>'
  and state <> 'cancelled'
group by all
), user_origination_geo as (
select 
  user_id
  , user_state
from user_origination_geos
qualify row_number() over(partition by user_id order by total desc) = 1
order by user_id, total desc
), user_destination_geos as (
select
  user_id
  , destination_ship_point_country_code || '-' || destination_ship_point_state as user_state
  , count(1) as total
from mongo_land.v5_shipments
where 
  destination_ship_point_facility_id = '<NA>'
  and state <> 'cancelled'
group by all
), user_destination_geo as (
select 
  user_id
  , user_state
from user_destination_geos
qualify row_number() over(partition by user_id order by total desc) = 1
)
select 
  COALESCE(o.user_id, d.user_id) as user_id
  , COALESCE(o.user_state, d.user_state) as user_state
from user_origination_geo as o
full outer join user_destination_geo as d
  on o.user_id = d.user_id
order by user_id
;


drop table if exists adhoc.user_geo_zip;
create table if not exists adhoc.user_geo_zip as
with user_origination_geos as (
select
  user_id
  , origination_ship_point_country_code || '-' || origination_ship_point_zip5 as user_zip
  , count(1) as total
from mongo_land.v5_shipments
where 
  origination_ship_point_facility_id = '<NA>'
  and state <> 'cancelled'
group by all
), user_origination_geo as (
select 
  user_id
  , user_zip
from user_origination_geos
qualify row_number() over(partition by user_id order by total desc) = 1
order by user_id, total desc
), user_destination_geos as (
select
  user_id
  , destination_ship_point_country_code || '-' || destination_ship_point_zip5 as user_zip
  , count(1) as total
from mongo_land.v5_shipments
where 
  destination_ship_point_facility_id = '<NA>'
  and state <> 'cancelled'
group by all
), user_destination_geo as (
select 
  user_id
  , user_zip
from user_destination_geos
qualify row_number() over(partition by user_id order by total desc) = 1
)
select 
  COALESCE(o.user_id, d.user_id) as user_id
  , COALESCE(o.user_zip, d.user_zip) as user_zip
from user_origination_geo as o
full outer join user_destination_geo as d
  on o.user_id = d.user_id
order by user_id
;


drop table if exists dp_bi.users;
create table if not exists dp_bi.users as
with users as (
select
  _id as user_id
  , registration_domain
  , case when registration_domain = 'shipsticks' then 'Ship Sticks'
      when registration_domain = 'shipskis' then 'Ship Skis'
      when registration_domain = 'shipgo' then 'ShipGo'
      when registration_domain = 'luggagefree' then 'Luggage Free'
      when registration_domain = 'shipcamps' then 'Ship Camps'
      when registration_domain = 'shipplay' then 'Ship & Play'
      when registration_domain = 'shipschools' then 'Ship School'
    end as brand
  , created_at as user_created_at
  , split(email, '@')[offset(1)] as email_domain
  , segment_user_profile_gender as gender
  , segment_user_profile_is_pro as is_pro
  , segment_user_profile_is_admin as is_admin
  , segment_user_profile_is_vip as is_vip
  , segment_user_profile_is_user as is_user
  , provider
  , registered_device
  , registration_location
  , segment_user_profile_country_of_residence as country
  , country_code
  , birth_date
  , club_name
  , default_address_id
  , last_sign_in_ip
  , affiliate_id
  , hear_about_us
  , sign_in_count
from `mongo_land.users`
where created_at >= '2019-01-01'  
)
select 
  u.*
  , g.user_geo
  , gs.user_state
  , gz.user_zip
  , sum(f.price_cents) / 100 as full_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 7 day) 
          then f.price_cents else 0 end) / 100 as day7_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 30 day) 
          then f.price_cents else 0 end) / 100 as day30_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 60 day) 
          then f.price_cents else 0 end) / 100 as day60_ltv 
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 90 day) 
          then f.price_cents else 0 end) / 100 as day90_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 120 day)
          then f.price_cents else 0 end) / 100 as day120_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 1 year)
          then f.price_cents else 0 end) / 100 as year1_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 2 year)
          then f.price_cents else 0 end) / 100 as year2_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 3 year)
          then f.price_cents else 0 end) / 100 as year3_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 4 year)
          then f.price_cents else 0 end) / 100 as year4_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          and f.`Shipment Created At` < date_add(date(u.user_created_at), interval 5 year)
          then f.price_cents else 0 end) / 100 as year5_ltv
  , sum(case when f.`Shipment Created At` >= date(u.user_created_at)
          then f.price_cents else 0 end) / 100 as full_ltv
from users as u
left outer join bi.financial_summary_detail_v5 as f
  on f.user_id = u.user_id
    and f.`Brand` = u.brand
left outer join adhoc.user_geo as g
  on u.user_id = g.user_id
left outer join adhoc.user_geo_zip as gz
  on u.user_id = gz.user_id
left outer join adhoc.user_geo_state as gs
  on u.user_id = gs.user_id
group by all
;


drop table if exists adhoc.user_ltv;
create table if not exists adhoc.user_ltv as
select * from dp_bi.users
;


drop table if exists adhoc.geo_ltv;
create table if not exists adhoc.geo_ltv as
select
  brand
  , user_geo
  , count(1) as total_users
  , avg(day30_ltv) as day30_ltv
  , avg(day90_ltv) as day90_ltv
  , avg(day120_ltv) as day120_ltv
  , avg(year1_ltv) as year1_ltv 
  , avg(year2_ltv) as year2_ltv 
  , avg(year3_ltv) as year3_ltv 
from `gse-dw-prod.adhoc.user_ltv` 
where 
  is_pro = false
  and user_geo is not null
group by all
having total_users > 50
order by 1, 3 desc
;

drop table if exists adhoc.state_ltv;
create table if not exists adhoc.state_ltv as
select
  brand
  , user_state
  , count(1) as total_users
  , avg(day30_ltv) as day30_ltv
  , avg(day90_ltv) as day90_ltv
  , avg(day120_ltv) as day120_ltv
  , avg(year1_ltv) as year1_ltv
  , avg(year2_ltv) as year2_ltv
  , avg(year3_ltv) as year3_ltv
from `gse-dw-prod.adhoc.user_ltv` 
where
  is_pro = false
  and user_state is not null
group by all
having total_users > 50
order by 1, 3 desc
;

drop table if exists adhoc.zip_ltv;
create table if not exists adhoc.zip_ltv as
select
  brand
  , user_zip
  , count(1) as total_users
  , avg(day30_ltv) as day30_ltv
  , avg(day90_ltv) as day90_ltv
  , avg(day120_ltv) as day120_ltv
  , avg(year1_ltv) as year1_ltv
  , avg(year2_ltv) as year2_ltv
  , avg(year3_ltv) as year3_ltv
from `gse-dw-prod.adhoc.user_ltv` 
where
  is_pro = false
  and user_zip is not null
group by all
having total_users > 50
order by 1, 3 desc
;

drop table if exists adhoc.clubs_ltv;
create table if not exists adhoc.clubs_ltv as
select
  brand
  , if(club_name = '' or club_name = '<NA>' or upper(club_name) = 'N/A' or lower(club_name) = 'na' or club_name is null or club_name = 'none' or club_name = 'None', 'No-Club', club_name) as club_name
  , count(1) as total_users
  , avg(day30_ltv) as day30_ltv
  , avg(day90_ltv) as day90_ltv
  , avg(day120_ltv) as day120_ltv
  , avg(year1_ltv) as year1_ltv 
  , avg(year2_ltv) as year2_ltv 
  , avg(year3_ltv) as year3_ltv 
from `gse-dw-prod.adhoc.user_ltv`
where 
  is_pro = false
  and club_name is not null
group by all
having total_users > 50
;

drop table if exists adhoc.hear_about_us_ltv;
create table if not exists adhoc.hear_about_us_ltv as
select
  brand
  , if(hear_about_us = '' 
    or hear_about_us = '<NA>' 
    or upper(hear_about_us) = 'N/A' 
    or lower(hear_about_us) = 'na' 
    or hear_about_us is null 
    or hear_about_us = 'none' 
    or hear_about_us = 'None', 
    'NA', hear_about_us) as hear_about_us
  , count(1) as total_users
  , avg(day30_ltv) as day30_ltv
  , avg(day90_ltv) as day90_ltv
  , avg(day120_ltv) as day120_ltv
  , avg(year1_ltv) as year1_ltv
  , avg(year2_ltv) as year2_ltv
  , avg(year3_ltv) as year3_ltv
from `gse-dw-prod.adhoc.user_ltv` 
where is_pro = false
group by all
having total_users > 50
;


drop table if exists adhoc.brand_cohort_ltv;
create table if not exists adhoc.brand_cohort_ltv as
select
  brand
  , date_trunc(date(user_created_at), month) as user_created
  , count(1) as total_users
  , avg(day7_ltv) as day7_ltv
  -- , avg(if(user_created_at >= timestamp_sub(current_timestamp(), interval 8 day), 7, day7_ltv)) as day7_ltv
  -- , avg(day30_ltv) as day30_ltv
  , avg(if(user_created_at >= timestamp_sub(current_timestamp(), interval 30 day), null, day30_ltv)) as day30_ltv
  -- , avg(day90_ltv) as day90_ltv
  , avg(if(user_created_at >= timestamp_sub(current_timestamp(), interval 90 day), null, day90_ltv)) as day90_ltv
  -- , avg(day120_ltv) as day120_ltv
  , avg(if(user_created_at >= timestamp_sub(current_timestamp(), interval 120 day), null, day120_ltv)) as day120_ltv
  -- , avg(year1_ltv) as year1_ltv
  , avg(if(user_created_at >= timestamp_sub(current_timestamp(), interval 365 day), null, year1_ltv)) as year1_ltv
  -- , avg(year2_ltv) as year2_ltv
  , avg(if(user_created_at >= timestamp_sub(current_timestamp(), interval 730 day), null, year2_ltv)) as year2_ltv
  -- , avg(year3_ltv) as year3_ltv
  , avg(if(user_created_at >= timestamp_sub(current_timestamp(), interval 1068 day), null, year3_ltv)) as year3_ltv
from `gse-dw-prod.adhoc.user_ltv` 
where 
  is_pro = false
  and is_admin = false
group by all
-- having total_users > 50
;


drop table if exists adhoc.registered_device_ltv;
create table if not exists adhoc.registered_device_ltv as
select
  brand
  , registered_device
  , count(1) as total_users
  , avg(day30_ltv) as day30_ltv
  , avg(day90_ltv) as day90_ltv
  , avg(day120_ltv) as day120_ltv
  , avg(year1_ltv) as year1_ltv
  , avg(year2_ltv) as year2_ltv
  , avg(year3_ltv) as year3_ltv
from `gse-dw-prod.adhoc.user_ltv` 
where 
  is_pro = false
  and registered_device is not null
group by all
having total_users > 50
order by 1, 2 desc
;


drop table if exists adhoc.country_ltv;
create table if not exists adhoc.country_ltv as
select
  brand
  , if(country = 'US', 'United States', country) as country
  , count(1) as total_users
  , avg(day30_ltv) as day30_ltv
  , avg(day90_ltv) as day90_ltv
  , avg(day120_ltv) as day120_ltv
  , avg(year1_ltv) as year1_ltv
  , avg(year2_ltv) as year2_ltv
  , avg(year3_ltv) as year3_ltv
from `gse-dw-prod.adhoc.user_ltv` 
where 
  is_pro = false
  and country is not null
group by all
having total_users > 50
order by 1, 2 desc
;
