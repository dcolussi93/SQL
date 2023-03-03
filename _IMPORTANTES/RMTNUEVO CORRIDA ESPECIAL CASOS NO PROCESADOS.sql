select
	t7.fabrica_centro as planta,
	t7.nombre as centro,
	t1.estado_origen as estado,
	t1.id_ordenitem_origen as order_id,
	--t2.status,
	t1.id_orden_item as codigo_rmt1,
	t2.outofroute_id as codigo_rmt2,
	--t2.order_id,
	nifi1 as proceso_rmt1,
	nifi2 as proceso_rmt2,
	t1.fecha_creacion,
	--to_char(t2.creation_date,'yyyy-mm-dd') as fecha_rmt2,
	--t1.id_proceso_template as tempaplic_rmt1,
	--t2.template_id as tempaplic_rmt2
	--t2.lastupdate_date,
	t2.hdrr_id,
	t2.processomp

	
from 
	(
		select * from sip_owner.sip_hrr_ordenes_item --limit 20
		where 1=1--fecha_creacion::text like '%202208%'
		and estado_retorno not in (0)
	) t1
	inner join (
		select * from rmt_owner.rmt_outofroute --limit 1
	) t2 on t2.order_id = t1.id_ordenitem_origen and t2.centro_id::numeric = t1.id_centro_origen::numeric and t2.status::numeric = t1.estado_origen::numeric
	inner join (
		select * from sip_owner.sip_hrr_procesos --limit 1
	) t3 on t3.id_orden_item = t1.id_orden_item
	inner join (
		select id_proceso, string_agg(distinct mdlw_processed_status, ', ') as nifi1 from sip_owner.sip_hrr_procesoper_centr_posib
		group by id_proceso
		having string_agg(distinct mdlw_processed_status, ', ') not like '%OMPOK%'
	) t4 on t4.id_proceso = t3.id_proceso
	inner join (
		select outofroute_id, string_agg(distinct mdlw_processed_status, ',') as nifi2 from rmt_owner.rmt_outofroute_hdr
		group by outofroute_id
	) t5 on t5.outofroute_id = t2.outofroute_id
	inner join (
		select distinct order_id , status, bundle_origin, t2.centro_id as id_centro_origen
		from sip_owner.sip_stock_wip t1
		inner join sip_owner.sip_centros t2 on t2.nombre = t1.bundle_origin and t2.mill_id = 1
		where t1.status <> 70 and t1.stock_source = 'SSF'	
		--group by order_id , status, bundle_origin, t2.centro_id
	) t6 on t6.order_id = t1.id_ordenitem_origen and t6.status::numeric = t1.estado_origen and t1.id_centro_origen::numeric = t6.id_centro_origen
	inner join (
		select * from sip_owner.sip_hrr_centros 
	) t7 on t7.id_centro = t2.centro_id
order by fecha_creacion desc


