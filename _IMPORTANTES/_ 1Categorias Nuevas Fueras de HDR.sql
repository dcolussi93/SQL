select distinct caso, caso2 from (
select 
	
	case
		when t3.template_id is null then '(1) Sin HDR'
		when t3.template_id is not null then '(2) Con HDR'
		else 'OTROS'
	end caso,
	
	Case
		when t4.nifi_status like '%FAILURE%' then 'Error IT'
		when t11.pending_testing_flag = 'Y' then 'Retenido'
		when t5.operationid is not null and t3.processomp is not null then 'Programado'
		when t3.processomp is not null then '(2) No Programado'
		when t3.template_id is null then '(1) Falta Template'
		when t4.nifi_status like '%WAITING%' and (t4.status_reingreso = '-1' or t4.productividad like '0,' or t4.productividad = '0') then '(1) Faltan Datos'
		when t3.template_id is not null and t4.outofroute_id is null then '(3) Template sin Operaciones'
		when t4.nifi_status like '%WAITING%' then 'Procesando'
		when t4.nifi_status like '%READY%' then 'Procesando'
		
	END caso2,
	t1.* 
from 
	(
		select distinct 
			order_id,
			status,
			bundle_origin
		from sip_owner.sip_stock_wip where stock_source = 'SSF' and status <> '70'
	) t1
	
	inner join (
		-- Estados Fuera de Ruta
		select * from sip_owner.sip_status where mill_id =1
		and flag_fdr = 'Y'
	) t11 on t11.status::integer = t1.status::integer
	
	left join (
		select * from sip_owner.sip_centros where mill_id = 1
	) t2 on t2.nombre = t1.bundle_origin
	
	left join (
		select * from rmt_owner.rmt_outofroute
	) t3 on t3.order_id = t1.order_id and t3.centro_id = t2.centro_id and t3.status::integer = t1.status::integer
	
	left join (
		select 
			outofroute_id , 
			string_agg(distinct mdlw_processed_status, ', ') as nifi_status,
			string_agg(distinct status_reingreso, ', ') as status_reingreso,
			string_agg(distinct productividad::text, ', ') as productividad
		from rmt_owner.rmt_outofroute_hdr 
		group by outofroute_id
	) t4 on t4.outofroute_id = t3.outofroute_id
	
	left join (
		select operationid from sip_owner.sip_pub_omp
		where operationid like '%_REC%' --limit 10
	) t5 on t5.operationid like '%'||t3.outofroute_id||'_REC%'
) x