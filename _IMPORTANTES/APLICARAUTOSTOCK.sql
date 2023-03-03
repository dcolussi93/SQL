select distinct 
	t1.product_id, 
	t1.order_id, 
	t1.status, t1.bundle_origin, t3.centro_id ,
	--t4.outofroute_id,
	--t4.template_id,
	t6.temp_pos,
	t5.fecha,
	t5.pzs,
	t5.tons,
	t5.loc
from
	
	(
		select * from sip_owner.sip_stock_wip
		where stock_source = 'SSF'
		and status <> 70
	) t1
	inner join
		sip_owner.sip_status
	t2 on t2.status::text = t1.status::text and t2.status_domain = 'WIP' and t2.flag_fdr = 'Y'
	inner join 
		sip_owner.sip_centros
	t3 on t3.nombre = t1.bundle_origin and t3.mill_id = 1
	inner join (
		select 
			order_id, bundle_origin, status, 
			max(state_date) as fecha ,
			sum(pieces) as pzs,
			round((sum(kilos)/1000)::numeric,2) as tons,
			string_agg(distinct location, ', ' order by location) as loc
		from sip_owner.sip_stock_wip 
		where status <> 70 and stock_source = 'SSF'
		--and state_date >= '2022-08-01'
		group by order_id, bundle_origin, status
	) t5 on t5.order_id = t1.order_id and t5.bundle_origin = t1.bundle_origin and t5.status = t1.status
	left join
		rmt_owner.rmt_outofroute
	t4 on t4.order_id = t1.order_id and t4.status::text = t1.status::text and t3.centro_id = t4.centro_id
	
	left join (
		select centro_id, status_inicial, string_agg(distinct t1.template_id::text, ';') as temp_pos 
		from rmt_owner.rmt_template t1
		inner join (select * from rmt_owner.rmt_template_center_origin) t2
		on t2.template_id = t1.template_id
		where t1.estado_template = 'A'
		group by centro_id, status_inicial
	) t6 on t6.centro_id = t3.centro_id and t1.status::integer = t6.status_inicial::integer
where 1=1 --t1.bundle_origin in ('BO39')
and outofroute_id is null
and temp_pos is not null
order by fecha desc 