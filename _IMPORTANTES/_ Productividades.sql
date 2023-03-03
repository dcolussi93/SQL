
select 
    distinct 
    null as productivity_id,
	'1' as mill_id,
    t1.centro_id, 
	t2.operation_id,
    t3.diameter_min,
    t3.diameter_max,
    t3.productividad,
    t3.descarte,
    t3.utilization,
    t3.coef_recup_en_linea,
    t3.coef_recup_fuera_de_linea,
    t3.coef_descarte_no_recup,
    t3.pzas_por_turno,
	'60066739' as user_insert,
    now() as insert_date,
	'60066739' as user_update,
    now() as last_upd_date

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
    inner join (
        select
            t1.centro_id,
            t1.operation_id,
            count(distinct t1.outofroute_id),
            min(diameter) as diameter_min,
            max(diameter) as diameter_max,
            avg(productividad) as productividad,
            avg(descarte) as descarte,
            avg(utilization) as utilization,
            avg(coef_recup_en_linea) as coef_recup_en_linea,
            avg(coef_recup_fuera_de_linea) as coef_recup_fuera_de_linea,
            avg(coef_descarte_no_recup) as coef_descarte_no_recup,
            avg(pzas_por_turno) as pzas_por_turno
        from 
            (
                select 
                    outofroute_id, centro_id, operation_id ,
                    productividad,
                    descarte,
                    utilization,
                    coef_recup_en_linea,
                    coef_recup_fuera_de_linea,
                    coef_descarte_no_recup,
                    pzas_por_turno
                from  rmt_owner.rmt_outofroute_hdr
                where mdlw_processed_status in ('READY','SUCCESS','READYOMP')
                order by operation_id, centro_id
            ) t1
            inner join (
                select * from rmt_owner.rmt_outofroute
            ) t2 on t2.outofroute_id = t1.outofroute_id
            inner join (
                select * from sip_owner.sip_orders where mill_id = 1
            ) t3 on t3.order_id = t2.order_id
            inner join (
                select * from sip_owner.sip_products
            ) t4 on t4.product_id = t3.legacy_product_code
            group by t1.centro_id,  t1.operation_id
        order by operation_id, t1.centro_id
    ) t3 on t3.centro_id = t1.centro_id and t3.operation_id = t2.operation_id
order by t2.operation_id , t1.centro_id