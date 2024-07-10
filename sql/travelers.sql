drop table if exists adhoc.user_ltv_2;
create table if not exists adhoc.user_ltv_2 as 
with tmp as (
select
  brand
  , user_id
  , min(transaction_action_date) as first_purchase_date
  , count(distinct shipment_id) as total_shipments
  , sum(price_cents) / 100 as total_revenue
from `dp_bi.finsum`
where user_is_pro = false
group by all
)
select
  u.brand
  , u.user_id
  , u.first_purchase_date
  , u.total_shipments
  , u.total_revenue
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 30 day)
          then f.price_cents else 0 end) / 100 as ltv_day30
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 1 year)
          then f.price_cents else 0 end) / 100 as ltv_year1
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 2 year)
          then f.price_cents else 0 end) / 100 as ltv_year2
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 3 year)
          then f.price_cents else 0 end) / 100 as ltv_year3
from `dp_bi.finsum` as f
join tmp as u
  on f.brand = u.brand
  and f.user_id = u.user_id
group by all
;


drop table if exists adhoc.traveler_ltv;
create table if not exists adhoc.traveler_ltv as 
with tmp as (
select
  brand
  , traveler_id
  , traveler_name
  , user_id as traveler_email
  , min(transaction_action_date) as first_purchase_date
  , count(distinct shipment_id) as total_shipments
  , sum(price_cents) / 100 as total_revenue
from `dp_bi.finsum`
where 
  user_is_pro = true
  and traveler_email is not null
group by all
)
select
  u.brand
  , u.traveler_id
  , u.traveler_name
  , u.traveler_email
  , u.first_purchase_date
  , u.total_shipments
  , u.total_revenue
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 30 day)
          then f.price_cents else 0 end) / 100 as ltv_day30
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 1 year)
          then f.price_cents else 0 end) / 100 as ltv_year1
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 2 year)
          then f.price_cents else 0 end) / 100 as ltv_year2
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 3 year)
          then f.price_cents else 0 end) / 100 as ltv_year3
from `dp_bi.finsum` as f
join tmp as u
  on f.brand = u.brand
  and f.traveler_email = u.traveler_email
where f.traveler_email is not null  
group by all
;


drop table if exists adhoc.traveler_ltv_2;
create table if not exists adhoc.traveler_ltv_2 as 
with tmp as (
select
  brand
  , user_id
  , min(transaction_action_date) as first_purchase_date
  , count(distinct shipment_id) as total_shipments
  , sum(price_cents) / 100 as total_revenue
from `dp_bi.finsum`
where 
  user_is_pro = true
--   and traveler_email is not null
group by all
)
select
  u.brand
  , u.user_id
  , u.first_purchase_date
  , u.total_shipments
  , u.total_revenue
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 30 day)
          then f.price_cents else 0 end) / 100 as ltv_day30
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 1 year)
          then f.price_cents else 0 end) / 100 as ltv_year1
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 2 year)
          then f.price_cents else 0 end) / 100 as ltv_year2
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 3 year)
          then f.price_cents else 0 end) / 100 as ltv_year3
from `dp_bi.finsum` as f
join tmp as u
  on f.brand = u.brand
  and f.user_id = u.user_id
group by all
;


drop table if exists adhoc.traveler_ltv_3;
create table if not exists adhoc.traveler_ltv_3 as 
with tmp as (
select
  brand
  , traveler_id
  , min(transaction_action_date) as first_purchase_date
  , count(distinct shipment_id) as total_shipments
  , sum(price_cents) / 100 as total_revenue
from `dp_bi.finsum`
where 
  user_is_pro = true
  and traveler_id is not null
group by all
)
select
  u.brand
  , u.traveler_id
  , u.first_purchase_date
  , u.total_shipments
  , u.total_revenue
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 30 day)
          then f.price_cents else 0 end) / 100 as ltv_day30
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 1 year)
          then f.price_cents else 0 end) / 100 as ltv_year1
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 2 year)
          then f.price_cents else 0 end) / 100 as ltv_year2
  , sum(case when f.shipment_created_date >= date(u.first_purchase_date)
          and f.shipment_created_date < date_add(date(u.first_purchase_date), interval 3 year)
          then f.price_cents else 0 end) / 100 as ltv_year3
from `dp_bi.finsum` as f
join tmp as u
  on f.brand = u.brand
  and f.traveler_id = u.traveler_id
where f.user_is_pro = true  
group by all
;
