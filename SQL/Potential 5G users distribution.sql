/* NOTE:
Sensitive data (like real table names, table fields) has been replaced with fictional values while preserving code structure and logic
*/

/* PURPOSE:
Determine the number of KS network users whose devices don't support 5G and analyze their traffic distribution by location.
*/

SELECT CGISAI as cell_id,
cell_name,
YPOS/1000000 as latitude, 
XPOS/1000000 as longitude,
LAYER2NAME as region,
round (sum(l4_dw_throughput)/1024/1024, 2) as traffic_mb,
count(distinct LTE_REG_IMSI) as Support_4G,
round(100*count(distinct LTE_REG_IMSI)/count(distinct ALL_REG_IMSI),2) as Support_4G_pct
FROM (
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_browsing
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_streaming
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_ftp
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_im
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_fileaccess
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_voip
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_other
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_email
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from traffic_dns
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, null as l4_dw_throughput, 
case when ter.ter_modename like '%LTE%' and ter.ter_modename not like '%NR%' then imsi else null end as LTE_REG_IMSI,imsi as ALL_REG_IMSI
from(
select imsi, last(imei) as imei, concat(MCC,MNC,LAC,CI) as LAST_SAI_CGI_ECGI
from core_voice_sessions
where SRVSTAT = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1,3
union all
select imsi, last(imei) as imei, SAI_CGI_ECGI as LAST_SAI_CGI_ECGI
from packet_data_sessions
where PROC_SUCCED_FLAG = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1,3
union ALL
select imsi, last(imei) as imei, SAI_CGI_ECGI as LAST_SAI_CGI_ECGI
from lte_signaling_sessions
where PROC_SUCCED_FLAG = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1,3) as X
join dim_device ter on ter.tac=substr(X.imei,1,8)
group by 1,2,3,4
) Y
JOIN dim_cgisai loc on Y.LAST_SAI_CGI_ECGI=loc.CGISAI
GROUP BY 
CGISAI,
cell_name,
YPOS,
XPOS,
LAYER2NAME;
