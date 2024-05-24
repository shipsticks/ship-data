select
  'att0'
  , count(1) as anon
  , count(distinct user_id) as users
from dp_staging.att_0
union all
select
  'att1'
  , count(1) as anon
  , count(distinct user_id) as users
from dp_staging.att_1
union all
select
  'att2'
  , count(1) as anon
  , null as users
from dp_staging.att_2
union all
select
  'att3'
  , count(1) as anon
  , null as users
from dp_staging.att_3
union all
select
  'att4'
  , count(1) as anon
  , count(distinct user_id) as users
from dp_staging.att_4
union all
select
  'att5'
  , count(1) as anon
  , count(distinct user_id) as users
from dp_staging.att_5
union all
select
  'prospects'
  , count(1) as anon
  , count(distinct user_id) as users
from `dp_bi.prospects`
order by 1
;
