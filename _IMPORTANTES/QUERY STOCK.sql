select distinct 
	t1.product_id, 
	t1.order_id, 
	t1.status, t1.bundle_origin, t3.centro_id ,
	t4.outofroute_id,
	--t4.template_id,
	t6.temp_pos
	--t1.fechaestado
from
	-- Stock y max statedate. No tenemos en cuenta chatarra . Y solo vemos stock fuera de centro.
	(
		select distinct product_id, order_id, status, bundle_origin 
		--, max(state_date) as fechaestado 
		from sip_owner.sip_stock_wip
		where stock_source = 'SSF'
		and status <> 70
		--group by product_id, order_id, status, bundle_origin 
	) t1
	-- Estados Fuera HDR Siderca
	inner join
		sip_owner.sip_status
	t2 on t2.status::text = t1.status::text and t2.status_domain = 'WIP' and t2.flag_fdr = 'Y' and mill_id = 1
	
	inner join 
		sip_owner.sip_centros
	t3 on t3.nombre = t1.bundle_origin and t3.mill_id = 1
	-- Demandas (Ordenes) en el programa
	inner join (
		select distinct t1.tsorderid from sip_owner.sip_pub_omp t1 -- where tsorderid = 'SD5192802'
		union
		select distinct t2.tsorderid from sip_owner.sip_pubplan_omp t2-- where tsorderid = 'SD5192802'
	) t7 on REPLACE(t7.tsorderid::text,'SD', '') = t1.order_id
	
	-- Calcular si tiene outofroute asignado.
	left join
		rmt_owner.rmt_outofroute
	t4 on t4.order_id = t1.order_id and t4.status::text = t1.status::text and t3.centro_id = t4.centro_id
	
	-- calcular si tiene un template posible que se pueda aplicar.
	left join (
		select centro_id, status_inicial, string_agg(distinct t1.template_id::text, ';') as temp_pos 
		from rmt_owner.rmt_template t1
		inner join (select * from rmt_owner.rmt_template_center_origin) t2
		on t2.template_id = t1.template_id
		where t1.estado_template = 'A'
		and tipo_template = 'E'
		group by centro_id, status_inicial
	) t6 on t6.centro_id = t3.centro_id and t1.status::integer = t6.status_inicial::integer
where 1=1--t1.bundle_origin in ('TLC1')
and outofroute_id is null
and temp_pos is not null
--order by fechaestado desc 