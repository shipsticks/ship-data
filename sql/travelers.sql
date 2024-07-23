drop table if exists adhoc.user_ltv_2;
create table if not exists adhoc.user_ltv_2 as 
with tmp as (
select
  brand
  , user_id
  , min(transaction_financial_date) as first_purchase_date
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
where f.user_is_pro = false  
group by all
;


drop table if exists adhoc.traveler_ltv;
create table if not exists adhoc.traveler_ltv as 
with tmp as (
select
  brand
  , d.dlink_pid
  , max(user_is_pro) as is_pro
  , max(If(user_is_pro is false, true, false)) as is_dtc
  , If(Count(DISTINCT user_is_pro) > 1, true, false) as is_pro_dtc
  , min(shipment_created_date) as first_purchase_date
  , count(distinct shipment_id) as total_shipments
  , sum(price_cents) / 100 as total_revenue
from `dp_bi.finsum` as f
join `ltv.ltv_dlink` as d
  on f.traveler_email = d.TRAVELER_EMAIL
  and f.traveler_name = d.TRAVELER_NAME
-- where
  -- f.transaction_financial_date >= '2024-01-01'
group by all
), tmp2 as (
select
  *
  , case when is_pro_dtc is true then 'pro_dtc'
      when is_pro is true then 'pro'
      when is_dtc is true then 'dtc'
    end as user_type
from tmp
)
select
  u.brand
  -- , u.traveler_email
  , d.dlink_pid
  , u.user_type
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
join `ltv.ltv_dlink` as d
  on f.traveler_email = d.TRAVELER_EMAIL
  and f.traveler_name = d.TRAVELER_NAME
join tmp2 as u
  on d.dlink_pid = u.dlink_pid
-- where 
  -- f.traveler_email is not null
  -- and f.brand = 'Ship Sticks'
group by all
;
