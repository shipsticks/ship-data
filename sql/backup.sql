-- backup
drop table if exists dp_bi.ad_spend_backup;
create table if not exists dp_bi.ad_spend_backup as
select *
from dp_bi.ad_spend
;
