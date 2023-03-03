select distinct
	t1.centro_id,
	t2.nombre as centrorecup,
	t1.operation_id,
	t4.operation_name as operacion,
	string_agg(distinct t3.template_id::text,', ' order by t3.template_id::text desc ) as template_id,
	string_agg(distinct t1.outofroute_id::text,', ' order by t1.outofroute_id::text desc ) as outofrouteids,
	count(distinct t1.outofroute_id) as cantcasosfail,
	max(xmlproductividades) as xml
from 
	(
		select * from rmt_owner.rmt_outofroute_hdr
		where productividad = 0
	) t1
	inner join (
		select * from sip_owner.sip_centros
	) t2 on t2.centro_id = t1.centro_id
	inner join (
		select * from rmt_owner.rmt_outofroute
	) t3 on t3.outofroute_id = t1.outofroute_id
	inner join (
		select * from sip_owner.sip_operations
	) t4 on t4.operation_id = t1.operation_id and t4.mill_id = 1
group by
	t1.centro_id,
	t2.nombre,
	t1.operation_id,
	t4.operation_name
order by t1.centro_id
