--select * from rmt_owner.rmt_outofroute_hdr where outofroute_id = '9398'

select 
    distinct 
    t1.centro_id, 
	t2.operation_id,
	t6.nombre,
	t7.operation_name,
	t5.idproductividad

 from
    (
        select * from rmt_template_operation_centers
    ) t1
    inner join (
        select * from rmt_template_operations
    ) t2 on t2.template_id = t1.template_id and t1.secuencia = t2.secuencia
    inner join (
        select * from rmt_template where estado_template = 'A'
    ) t4 on t4.template_id = t1.template_id
	left join (
		select centro_id, operation_id, 
		string_agg(distinct productivity_id ::text , ', ') as idproductividad
		from sip_owner.rmt_productivities
		group by centro_id, operation_id
	) t5 on t5.centro_id = t1.centro_id and  t5.operation_id = t2.operation_id
	inner join (
		select * from sip_owner.sip_centros where mill_id = 2
	) t6 on t6.centro_id = t1.centro_id
	inner join (
		select * from sip_owner.sip_operations where mill_id = 2
	) t7 on t7.operation_id = t2.operation_id
order by t2.operation_id , t1.centro_id