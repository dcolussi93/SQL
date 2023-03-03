select 
	t2.outofroute_id,
	t2.order_id,
	t3.legacy_product_code,
	t2.centro_id,
	t4.nombre as centro, 
	t4.fabrica_centro,
	t2.template_id,
	t2.hdrr_id,
	t2.processomp
from 
	(
		select distinct outofroute_id from rmt_owner.rmt_outofroute_hdr 
		where mdlw_processed_status = 'READY'
		order by outofroute_id desc
		limit 40
	) t1
	inner join (
		select * from rmt_owner.rmt_outofroute
	) t2 on t2.outofroute_id = t1.outofroute_id
	inner join (
		select * from sip_owner.sip_orders where mill_id = 1
	) t3 on t3.order_id = t2.order_id
	inner join (
		select * from sip_owner.sip_hrr_centros where fabrica_centro <> 'Premium'
	) t4 on t4.id_centro = t2.centro_id
order by fabrica_centro, outofroute_id desc
