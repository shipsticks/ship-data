drop table if exists dp_staging.manual_ad_spend_snapshot;
create table if not exists dp_staging.manual_ad_spend_snapshot as 
select
  spend_date
  , 'Offline' as channel
  , source
  , brand
  , campaign
  , null as campaign_name
  , spend
  , null as impressions
  , null as clicks
from dp_staging.manual_ad_spend
where spend_date is not null
;