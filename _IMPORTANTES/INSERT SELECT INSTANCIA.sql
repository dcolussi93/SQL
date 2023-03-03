with 
	p1 (orden,centro,estado) as (
		values('5042802','DANO','96')
	)
select 
	t3.order_id, t3.order_id_date, t2.centro_id, t2.nombre , t4.outofroute_id, t5.tempinstanciar
from 
	
	-- Tabla de parametros
	p1
	
	-- Datos de codigo de centro
	inner join (
		select * from sip_owner.sip_centros 
		where mill_id = 1
		and nombre = (select centro from p1)
	) t2 on 1=1
	
	-- Datos de ordenes: Fecha y id orden. Ultima fecha.
	inner join (
		select * from sip_owner.sip_orders 
		where order_id = (select orden from p1)
		-- Me quedo con la ultima fecha de la orden.
		order by order_id_date desc limit 1
	) t3 on 1=1
	
	-- Datos de si tiene o no tiene outofroute. Necesitamos los que no tengan.
	left join (
		select * from rmt_owner.rmt_outofroute
	) t4 on t4.order_id = t3.order_id and t4.order_id_date = t3.order_id_date and t4.centro_id = t2.centro_id and t4.status = p1.estado
	
	-- Maximo template activo a instanciar para ese centro y ese estado.
	left join (
		select max(t1.template_id) as tempinstanciar, t1.status_inicial, t2.centro_id 
		from rmt_owner.rmt_template t1
		inner join 	rmt_owner.rmt_template_center_origin t2 on t2.template_id = t1.template_id
		where t1.estado_template = 'A'
		group by 
			t1.status_inicial, t2.centro_id
	) t5 on t5.centro_id = t2.centro_id and t5.status_inicial = p1.estado
where outofroute_id is null
and tempinstanciar is not null
limit 1