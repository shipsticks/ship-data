
declare begintime date;
declare endtime date;
set endtime = current_date();
set begintime = date_sub(date_sub(current_date(), interval extract(year from current_date()) -
                extract(year from date_sub(current_date(), interval 2 year)) year), interval extract(dayofyear from date_sub(current_date(), interval 2 year)) - 1 day);

create or replace table `dp_bi.finsum_metrics`
as (
select distinct 
    Brand
  --,tracking_id  
   ,'' as Carrier
   , carrier_display_name as `Carrier Display Name`
   , '' as `Payment Method`
   , concat('Wk. ', extract(week from transaction_financial_date)) as `Transaction Financial Week`
   , format_timestamp('%b', transaction_financial_date) as `Transaction Financial Month`
   , cast(extract(year from transaction_financial_date) as string) as `Transaction Financial Year`
   ,(count(distinct case when transaction_reporting_state = 'enqueued' then carrier_tracking_id end) - 
     count(distinct case when transaction_reporting_state = 'cancelled' then carrier_tracking_id end)) as Shipments
   , sum(price_cents) / 100 as `Total Revenue`
   ,((sum(case 
            when product_type = 'V5::Label' and transaction_reporting_state = 'enqueued' 
            then price_cents
            else 0 
        end) + 
    sum(case 
            when product_type = 'V5::Label' and transaction_reporting_state = 'cancelled' 
            then price_cents
            else 0 
        end)) / 100) + (sum(case when product_type = 'V5::Fee' then price_cents else 0 end) / 100) as `Item Revenue`

   , sum(case when product_type = 'V5::Insurance' then price_cents else 0 end) / 100 as `Insurance Revenue`
   , sum(case when product_type = 'V5::Discount' and order_line_item_category in ('coupon','wholesale','upgrade','comp') and transaction_reporting_state = 'enqueued' then price_cents else 0 end) / 100 as `Total Discount`
   ,(sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'enqueued' then cost_cents else 0 end) +
      sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'cancelled' then cost_cents else 0 end)) / 100 as `COGS` --note that this excludes insurance cogs - need to determine if that should be included
   ,(sum(price_cents) - 
     (sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'enqueued' then cost_cents else 0 end) + 
      sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'cancelled' then cost_cents else 0 end))) / 100 as `Gross Profit`
      
   ,(case 
       when sum(price_cents) = 0 then 0 
       else ((sum(price_cents) / 100) - 
         (sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'enqueued' then cost_cents else 0 end) +
          sum(case when product_type = 'V5::Label' and transaction_reporting_state = 'cancelled' then cost_cents else 0 end)) / 100) / 
         (sum(price_cents) / 100)
     end) as `Gross Profit %`
     
   ,(case 
        when (count(distinct case when transaction_reporting_state = 'enqueued' then carrier_tracking_id end) - 
              count(distinct case when transaction_reporting_state = 'cancelled' then carrier_tracking_id end)) = 0 then 0
        else (sum(price_cents)/
              (count(distinct case when transaction_reporting_state = 'enqueued' then carrier_tracking_id end) - 
               count(distinct case when transaction_reporting_state = 'cancelled' then carrier_tracking_id end))) / 100 
        end) as `ASV`
        
   , sum(case when product_type = 'V5::Fee' then price_cents else 0 end) / 100 as `Assessorials`

   --, count(distinct(case when shipment_estimated_delivery_date >= begintime and shipment_estimated_delivery_date endtime and transaction_reporting_state = 'enqueued' then tracking_id end)) as scheduled_delivered 

   --, count(distinct(case when shipment_actual_delivery_date >= begintime and shipment_actual_delivery_date endtime and transaction_reporting_state = 'enqueued' then tracking_id end)) as delivered   

from dp_bi.finsum_bq
where transaction_financial_date >= begintime and transaction_financial_date <= endtime
--where order_id = 'O02232B713163' and transaction_financial_date = '2023-02-15' --spot check order (with partitioned date field)
  
group by all
order by 
    `Transaction Financial Year` desc
  , `Transaction Financial Month` desc
  , `Transaction Financial Week` desc
  , Shipments desc 
)
;



