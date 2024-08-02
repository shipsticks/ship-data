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


drop table if exists adhoc.dlink_ltv_old;
create table if not exists adhoc.dlink_ltv_old as 
with finsum as (
select
  f.brand
  , f.user_id
  , f.user_is_pro
  , f.traveler_email
  , f.traveler_name
  , d.DLINK_PID as dlink_pid
  , f.order_id
  , f.shipment_id
  , f.price_cents
  , f.micro_site_id
  , f.shipment_created_at
  , f.transaction_financial_at
from `dp_bi.finsum` as f
join `ltv.ltv_dlink` as d
  on f.traveler_email = d.TRAVELER_EMAIL
  and f.traveler_name = d.TRAVELER_NAME
where
  f.transaction_financial_date >= '2018-01-01'
  -- and brand = 'Ship Sticks'
  and user_is_admin is false
), tmp1 as (
select
  brand
  , dlink_pid
  , order_id
  , min(micro_site_id) as micro_site_id
  , min(user_is_pro) as user_is_pro
  , min(shipment_created_at) as first_order_at
  , sum(price_cents) / 100 as total_revenue
from finsum
group by all  
having sum(price_cents) > 0
qualify row_number() over (partition by brand, dlink_pid order by first_order_at) = 1
)
select
  u.brand
  , u.dlink_pid
  -- , u.user_is_pro
  , if((u.micro_site_id is not null) or (u.user_is_pro is true), 'PRO', 'DTC') as user_type
  , min(u.first_order_at) as first_order_at
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 1 year)
          then f.price_cents else 0 end) / 100 as ltv_year1
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 2 year)
          then f.price_cents else 0 end) / 100 as ltv_year2
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 3 year)
          then f.price_cents else 0 end) / 100 as ltv_year3
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 4 year)
          then f.price_cents else 0 end) / 100 as ltv_year4
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 5 year)
          then f.price_cents else 0 end) / 100 as ltv_year5   
  , sum(if(f.shipment_created_at >= date(u.first_order_at), f.price_cents, 0)) / 100 as ltv_total
from finsum as f
join tmp1 as u
  on f.dlink_pid = u.dlink_pid
  and f.brand = u.brand
group by all  
;


drop table if exists adhoc.dlink_ltv;
create table if not exists adhoc.dlink_ltv as 
with finsum as (
select
  f.brand
  , f.user_id
  , f.user_is_pro
  , f.traveler_email
  , f.traveler_name
  , d.DLINK_PID as dlink_pid
  , f.order_id
  , f.shipment_id
  , f.price_cents
  , f.b2b_revenue_attribution
  , f.shipment_created_at
  , f.transaction_financial_at
from `dp_bi.finsum_b2b` as f
join `ltv.ltv_dlink` as d
  on f.traveler_email = d.TRAVELER_EMAIL
  and f.traveler_name = d.TRAVELER_NAME
where
  f.transaction_financial_date >= '2018-01-01'
  -- and brand = 'Ship Sticks'
  and user_is_admin is false
), tmp1 as (
select
  brand
  , dlink_pid
  , order_id
  , min(b2b_revenue_attribution) as b2b_revenue_attribution
  , min(user_is_pro) as user_is_pro
  , min(shipment_created_at) as first_order_at
  , sum(price_cents) / 100 as total_revenue
from finsum
group by all  
having sum(price_cents) > 0
qualify row_number() over (partition by brand, dlink_pid order by first_order_at) = 1
)
select
  u.brand
  , u.dlink_pid
  -- , u.user_is_pro
  , if(u.b2b_revenue_attribution is not null, 'PRO', 'DTC') as user_type
  , min(u.first_order_at) as first_order_at
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 1 year)
          then f.price_cents else 0 end) / 100 as ltv_year1
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 2 year)
          then f.price_cents else 0 end) / 100 as ltv_year2
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 3 year)
          then f.price_cents else 0 end) / 100 as ltv_year3
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 4 year)
          then f.price_cents else 0 end) / 100 as ltv_year4
  , sum(case when f.shipment_created_at >= date(u.first_order_at)
          and f.shipment_created_at < date_add(date(u.first_order_at), interval 5 year)
          then f.price_cents else 0 end) / 100 as ltv_year5   
  , sum(if(f.shipment_created_at >= date(u.first_order_at), f.price_cents, 0)) / 100 as ltv_total
from finsum as f
join tmp1 as u
  on f.dlink_pid = u.dlink_pid
  and f.brand = u.brand
group by all  
;