drop table if exists dp_bi.finsum_metrics;
create or replace table dp_bi.finsum_metrics as
select distinct
  brand
  , carrier
  , carrier_display_name
  , payment_method
  , transaction_financial_date

  , (count(distinct case when transaction_reporting_state = 'enqueued' then tracking_id end) -
      count(distinct case when transaction_reporting_state = 'cancelled' then tracking_id end)) as shipments

  , sum(price_cents) / 100 as total_revenue

  , ((sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'enqueued' then price_cents else 0 end) +
      sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'cancelled' then price_cents else 0  end)) / 100) as item_revenue

  , sum(case when product_type = 'V5::Insurance' then price_cents else 0 end) / 100 as insurance_revenue

  , (sum(case when product_type = 'V5::Discount' and order_line_item_category in ('coupon','wholesale','upgrade','comp','bundle') and transaction_reporting_state = 'enqueued' then price_cents else 0 end) +
      sum(case when product_type = 'V5::Discount' and order_line_item_category in ('coupon','wholesale','upgrade','comp','bundle') and transaction_reporting_state =  'cancelled' then price_cents else 0 end)) / 100 as total_discount

  , (sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'enqueued' then cost_cents else 0 end) +
      sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'cancelled' then cost_cents else 0 end)) / 100 as COGS --note that this excludes insurance cogs - need to determine if that should be included
  , (sum(price_cents) -
      (sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'enqueued' then cost_cents else 0 end) +
      sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'cancelled' then cost_cents else 0 end))) / 100 as gross_profit

  , (case when
      (count(distinct case when transaction_reporting_state = 'enqueued' then tracking_id end) -
      count(distinct case when transaction_reporting_state = 'cancelled' then tracking_id end)) = 0 then 0
    else (sum(price_cents)/
      (count(distinct case when transaction_reporting_state = 'enqueued' then tracking_id end) -
      count(distinct case when transaction_reporting_state = 'cancelled' then tracking_id end))) / 100
    end) as ASV

  , sum(case when product_type = 'V5::Fee' then price_cents else 0 end) / 100 as assessorials

  , case when sum(case when transaction_reporting_state = 'enqueued' then price_cents else 0 end) +
      sum(case when transaction_reporting_state = 'cancelled' then price_cents else 0 end) > 0
    then count(distinct case when transaction_reporting_state in ('enqueued', 'cancelled') then order_id end)
    else 0 end as orders

  , case when (
      sum(case when transaction_reporting_state = 'enqueued' then price_cents end) +
      sum(case when transaction_reporting_state = 'cancelled' then price_cents end)) > 0
    then (
      sum(case when transaction_reporting_state = 'enqueued' then price_cents end) +
      sum(case when transaction_reporting_state = 'cancelled' then price_cents end)) / 100
    else 0 end as order_amount

  , (case when (
      sum(case when transaction_reporting_state = 'enqueued' then price_cents end) +
      sum(case when transaction_reporting_state = 'cancelled' then price_cents end)) > 0
    then (
      sum(case when transaction_reporting_state = 'enqueued' then price_cents end) +
      sum(case when transaction_reporting_state = 'cancelled' then price_cents end)) / 100
    else 0 end ) /
      nullif((case when sum(case when transaction_reporting_state = 'enqueued' then price_cents else 0 end) +
      sum(case when transaction_reporting_state = 'cancelled' then price_cents else 0 end) > 0
    then count(distinct case when transaction_reporting_state in ('enqueued', 'cancelled') then order_id end)
    else 0 end), 0) as AOV

from dp_bi.finsum
group by all
order by
  transaction_financial_date desc
  , shipments desc
;