select
	t1.outofroute_id, 
	
	t2.hdrr_id,
	
	'SD'||t2.order_id as demandid,
	t3.legacy_product_code,
	t4.nombre as bundle_origin,
	t2.status,
	t2.template_id,
	t1.status_reingreso,
	t1.mdlw_processed_status,
	t2.processomp
from (
	select distinct outofroute_id , mdlw_processed_status, status_reingreso from rmt_owner.rmt_outofroute_hdr 
	--where mdlw_processed_status = 'READY'
	--and status_reingreso::numeric = 99
	where outofroute_id in (2682,
	2670,
	2664,
	2656,
	2649,
	2638,
	2634,
	2609)
	order by outofroute_id desc 
	--limit 8
	) t1
	inner join (
		select * from rmt_owner.rmt_outofroute
	) t2 on t2.outofroute_id = t1.outofroute_id
	inner join (
		select * from sip_owner.sip_orders where mill_id=1
	) t3 on t3.order_id = t2.order_id
	inner join (
		select * from sip_owner.sip_centros where mill_id = 1 
	) t4 on t4.centro_id = t2.centro_id
	
	
/*	
select * from rmt_owner.rr_nifiomp_resultadohdr_fn(2682,'READYOMP');
select * from rmt_owner.rr_nifiomp_resultadohdr_fn(2670,'READYOMP');
select * from rmt_owner.rr_nifiomp_resultadohdr_fn(2664,'READYOMP');
select * from rmt_owner.rr_nifiomp_resultadohdr_fn(2656,'READYOMP');
select * from rmt_owner.rr_nifiomp_resultadohdr_fn(2649,'READYOMP');
select * from rmt_owner.rr_nifiomp_resultadohdr_fn(2638,'READYOMP');
select * from rmt_owner.rr_nifiomp_resultadohdr_fn(2634,'READYOMP');
select * from rmt_owner.rr_nifiomp_resultadohdr_fn(2609,'READYOMP');
*/