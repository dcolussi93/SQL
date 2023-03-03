/*
call rmt_owner.rr_instanciabatch_materialoutofroute_sp(
	'300396007', 
	'LIN2',
	'29'
);
*/
select distinct 
   t1.product_id,
   t1.order_id,
   t1.status, t1.bundle_origin, t3.centro_id ,
   t4.outofroute_id,
   --t4.template_id,
   t6.temp_pos
   --t1.fechaestado
from
   -- Stock y max statedate. No tenemos en cuenta chatarra . Y solo vemos stock fuera de centro.
   (  
      	select distinct x1.product_code as product_id, x1.order_id as op, x2.order_id, status_id as status, bundle_origin
      	--, max(state_date) as fechaestado
	    from sip_owner.sip_stock_wip x1
		inner join sip_owner.sip_orders x2 on x2.op_legacy = x1.order_id 
	  	--inner join sip_owner.rr_rmt_stockwip_tam_fn() x3 on x3.lingada = x1.bundle
	  	where x1.stock_source = 'TAM-PIP'
      	and x1.program_name in ('STU PIP','SSE PIP','PTU PIP','ZTO PIP')
      	and x1.product_type_desc <> 'COP'
	  	--and product_id <> 0
            --and order_id = '372510'
		and x1.status_id <> '99'
	   --and bundle_origin = 'LIN2'
	   --and status_id = 'CD'
	 
      --where stock_source = 'SSF'
      --and status <> 70
      --group by product_id, order_id, status, bundle_origin
   ) t1
   -- Estados Fuera HDR Siderca
   inner join
      sip_owner.sip_status
   t2 on t2.status::text = t1.status::text and t2.status_domain = 'WIP' and t2.flag_fdr = 'Y' and mill_id = 2
   
   inner join 
      sip_owner.sip_centros
   t3 on t3.nombre = t1.bundle_origin and t3.mill_id = 2
   -- Demandas (Ordenes) en el programa
   left join (
      select distinct t1.tsorderid from sip_owner.sip_pub_omp t1 -- where tsorderid = 'SD5192802'
      union
      select distinct t2.tsorderid from sip_owner.sip_pubplan_omp t2-- where tsorderid = 'SD5192802'
   ) t7 on REPLACE(t7.tsorderid::text,'TM', '') = t1.order_id
   
   -- Calcular si tiene outofroute asignado.
   left join
      rmt_owner.rmt_outofroute
   t4 on t4.order_id = t1.order_id and t4.status::text = t1.status::text and t3.centro_id = t4.centro_id
   
   -- calcular si tiene un template posible que se pueda aplicar.
   left join (
      select centro_id, status_inicial, string_agg(distinct t1.template_id::text, ';') as temp_pos
      from rmt_owner.rmt_template t1
      inner join (select * from rmt_owner.rmt_template_center_origin) t2
      on t2.template_id = t1.template_id
      where 1=1-- t1.estado_template = 'A'
      and 1=1--tipo_template = 'E'
      group by centro_id, status_inicial
   ) t6 on t6.centro_id = t3.centro_id and t1.status = t6.status_inicial
where 1=1--t1.bundle_origin in ('TLC1')
and outofroute_id is null
and temp_pos is not null
--order by fechaestado desc