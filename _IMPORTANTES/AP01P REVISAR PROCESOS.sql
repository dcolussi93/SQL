/*
select route_path, step, id, processid, rollingalternative, seqgroup, productivecentername
from 
omp_owner.nifi_md_tam_legacy_hdr_raw 
where id = 1908294
--where productid = 'H0355'
order by routealternative::numeric, step, processid::numeric


select * from 
omp_owner.nifi_md_tam_legacy_hdr_raw_header
where id = '1908294'
--where demandid = 'TM303053001'
--and tipo_hdr like '%RECOVERY%'
--and tipo_hdr like '%STANDARD%
order by id desc
*/
SELECT 
t1.hdr_raw_id, t1.processid,
max(rollingalternative) as rolling,
string_agg(seqnrinprocess||'-'||prefmachineid, ', ' order by seqnrinprocess) as cadena,
max(t1.creation_date) as fec

FROM omp_owner.nifi_md_omp_operation t1 --limit 20
inner join (
	select hdr_raw_id as id, count(distinct processid) as cant from omp_owner.nifi_md_omp_operation 
	where 1=1--processid like '%3974%4004__REC%'
	--and processid  like '%__REC%'
	and hdr_raw_id = '1905047'
	--1908295
	--and to_char(creation_date ,'yymm') = '2210'
	group by hdr_raw_id
	having  count(distinct processid) >0
	--select * from  omp_owner.nifi_md_omp_process  where hdr_raw_id = '1812548'
	--select * from omp_owner.nifi_md_tam_legacy_hdr_raw_header where demandid = 'TM300434001' order by id desc
	-- recovery std "id" 1812548
	-- "id" estandar 1730949
	
	--Otro caso "hdr_raw_id" 1725717 ; "id"1574980

) t2 on t2.id = t1.hdr_raw_id
inner join (
	select * from  omp_owner.nifi_md_omp_process --limit 20
) t3 on t3.hdr_raw_id = t2.id and t1.processid = t3.processid
group by t1.hdr_raw_id, t1.processid
order by t1.hdr_raw_id desc