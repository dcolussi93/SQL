with 
	v1 (demand) as (
		values ('SD5055801')
	)
	, v2_idhdr as (
		--select * from omp_owner.nifi_md_sid_legacy_hdr_raw_header where productid = '70142'
		select max(id) as hrid 
		from omp_owner.nifi_md_sid_legacy_hdr_raw_header 
		where tipo_hdr = 'STANDARD'
		and demandid =  (select demand from v1)
		--where id = 1459633
	)

select
	max(productid) productid,
	max(id) as hdrid,
	step, 
	seqgroup,	
		string_agg(distinct operationname, ',') as operation,
		string_agg(distinct inputstatus , ',' order by inputstatus) as inputstatus,
		string_agg( distinct centros, ', ' order by centros) as centros 
from (
	select distinct
		routealternative,
		seqgroup,
		id,
		step,
		operationname,
		productivecentername as centros,
		inputstatus,
		outputstatus,
		processid,
		productid
	from 
		(
			select * from omp_owner.nifi_md_sid_legacy_hdr_raw --limit 10
			where id =  (select * from v2_idhdr)
		) t1
		/*inner join (
			select 
				seqgroup as centro,
				max(step) as minstep, 
				max(id) as hdrid 
			from
				omp_owner.nifi_md_sid_legacy_hdr_raw
				where id =  (select * from v2_idhdr)
			group by seqgroup
			order by minstep
		) t3 on t3.hdrid = t1.id and t1.seqgroup = t3.centro and t1.step = t3.minstep*/
) x group by step, seqgroup, productid