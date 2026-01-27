/* NOTE:
Sensitive data (like real table names, table fields) has been replaced with fictional values while preserving code structure and logic
*/

/* PURPOSE:
Determine the number of KS network users whose devices support 5G.
*/

select count(distinct imsi) as total_active_users,
sum(case when d.ter_modename like '%NR%' then 1 else 0 end) as Support_5G,
round(100*sum(case when d.ter_modename like '%NR%' then 1 else 0 end)/count(distinct imsi),2) as Support_5G_pct
from(
select distinct imsi as imsi, last(imei) as imei 
from(
select imsi, last(imei) as imei 
from core_voice_sessions
where SRVSTAT = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1
union all
select imsi, last(imei) as imei
from packet_data_sessions
where PROC_SUCCED_FLAG = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1
union ALL
select imsi, last(imei) as imei
from lte_signaling_sessions
where PROC_SUCCED_FLAG = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1) as X
group by 1) as Y
join dim_device d on substr(Y.imei,1,8)=d.tac;
