select 
	t1.outofroute_id as id_eliminar,
	t2.ids as lista_relacionados, 
	t1.outofroute_id = t2.max_outofroute as flag_mantener
from 
	(
		select * from rmt_owner.rmt_outofroute
	) t1 

	-- ENCONTRAR DUPLICADOS Relacionados a una orden,centro,estado.
	inner join (
		select 
			order_id,
			centro_id,
			status,
			to_char(creation_date,'yymmdd') as fecha,
			count(distinct outofroute_id) cant_outofroute,
			max(outofroute_id) as max_outofroute,
			string_agg(distinct outofroute_id::text, '; ' order by outofroute_id::text desc) as ids
		from 
			rmt_owner.rmt_outofroute
		group by 
			order_id, centro_id, status, to_char(creation_date,'yymmdd')
		having 
			count(distinct outofroute_id) > 1
		order by 
			cant_outofroute desc
	) t2 on 
			t2.order_id = t1.order_id 
		and t2.centro_id = t1.centro_id 
		and t2.status = t1.status
		and t2.fecha = to_char(t1.creation_date,'yymmdd')
	-- Condicion de buscar todos los que no sean el maximo, asociado a esa orden,estado,centro
where t1.outofroute_id <> t2.max_outofroute
order by outofroute_id desc