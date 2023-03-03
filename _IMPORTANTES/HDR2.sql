with 
	v1 (prod) as (
		values ('70452')
	)
	, v2_idhdr as (
		select max(id) as hrid 
		from omp_owner.nifi_md_sid_legacy_hdr_raw_header 
		where tipo_hdr = 'STANDARD'
		and productid =  (select prod from v1)
	)

select
	step, 
		string_agg(distinct operationname, ',') as operation,
		string_agg(distinct inputstatus , ',' order by inputstatus) as inputstatus,
		string_agg( distinct centros, ', ' order by centros) as centros 
from (
	select distinct
		routealternative,
		step,
		operationname,
		productivecentername as centros,
		inputstatus,
		outputstatus,
		processid
	from 
		(
			select * from omp_owner.nifi_md_sid_legacy_hdr_raw
			where id =  (select * from v2_idhdr)
		) t1
		inner join (
			select 
				productivecentername as centro,
				min(step) as minstep, 
				max(id) as hdrid 
			from
				omp_owner.nifi_md_sid_legacy_hdr_raw 
				where id =  (select * from v2_idhdr)
			group by productivecentername
			order by minstep
		) t3 on t3.hdrid = t1.id and t1.productivecentername = t3.centro and t1.step = t3.minstep
) x group by step