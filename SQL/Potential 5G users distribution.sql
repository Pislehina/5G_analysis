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
from ps.detail_ufdr_http_browsing_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from ps.detail_ufdr_streaming_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from ps.detail_ufdr_ftp_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from ps.detail_ufdr_im_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from ps.detail_ufdr_fileaccess_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from ps.detail_ufdr_voip_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from ps.detail_ufdr_other_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from ps.detail_ufdr_email_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, sum(l4_dw_throughput) as l4_dw_throughput, null as LTE_REG_IMSI,null as ALL_REG_IMSI
from ps.detail_ufdr_dns_20465
where rat = 6 and substr(imsi,1,5) in ('25503','25502') and homemcc=mcc and homemnc=mnc
group by 1
union all
select LAST_SAI_CGI_ECGI, null as l4_dw_throughput, 
case when ter.ter_modename like '%LTE%' and ter.ter_modename not like '%NR%' then imsi else null end as LTE_REG_IMSI,imsi as ALL_REG_IMSI
from(
select imsi, last(imei) as imei, concat(MCC,MNC,LAC,CI) as LAST_SAI_CGI_ECGI
from cs.TDR_AIU_MM_20465 
where SRVSTAT = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1,3
union all
select imsi, last(imei) as imei, SAI_CGI_ECGI as LAST_SAI_CGI_ECGI
from ps.DETAIL_CDR_GbIuPS_20465
where PROC_SUCCED_FLAG = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1,3
union ALL
select imsi, last(imei) as imei, SAI_CGI_ECGI as LAST_SAI_CGI_ECGI
from ps.DETAIL_CDR_S1MME_20465
where PROC_SUCCED_FLAG = 0 and substr(imsi,1,5) in ('25503','25502') and imei is not null and imei != ''
group by 1,3) as X
join nethouse.dim_terminal ter on ter.tac=substr(X.imei,1,8)
group by 1,2,3,4
) Y
JOIN nethouse.dim_loc_cgisai loc on Y.LAST_SAI_CGI_ECGI=loc.CGISAI
GROUP BY 
CGISAI,
cell_name,
YPOS,
XPOS,
LAYER2NAME;
