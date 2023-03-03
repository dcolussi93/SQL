select
	t5.id as idoutofroute,
	t5.fecha,
	t5.templ,
	id_proceso_template,
	t1.id_proceso,
	t3.id_ordenitem_origen,
	t3.id_orden_item,
	t4.nombre as bundle_origin,
	t3.estado_origen,
	t3.fecha_creacion,
	t1.status,
	estado_retorno
	
	
from 
	(
		/*
		 select * from sip_hrr_procesoper_centr_posib shpcp where id_proceso = 8167
		 */
		select id_proceso, string_agg(distinct mdlw_processed_status, ', ') as status from sip_owner.sip_hrr_procesoper_centr_posib
		group by id_proceso
	) t1
	inner join (
		select * from sip_owner.sip_hrr_procesos where id_orden_item is not null
	) t2 on t2.id_proceso = t1.id_proceso
	inner join (
		select * from sip_owner.sip_hrr_ordenes_item --where id_ordenitem_origen = '5002403'
		
	) t3 on t3.id_orden_item = t2.id_orden_item
	inner join (
		select * from sip_owner.sip_hrr_centros shc 
	) t4 on t4.id_centro::text = t3.id_centro_origen::text
	left join (
		select 
			order_id, centro_id, status,
			string_agg(distinct template_id::text , '; ' ) as templ ,
			string_agg(distinct outofroute_id::text , '; ') as id,
			max(creation_date) as fecha
		from rmt_owner.rmt_outofroute 
		group by order_id, centro_id, status
	) t5 on t5.centro_id::text = t3.id_centro_origen::text and t5.order_id = t3.id_ordenitem_origen and t5.status::text = t3.estado_origen::text
where estado_retorno <> '0' and t1.status is null and t5.fecha is not null
order by fecha desc

