select distinct
	t1.workflow_id as workflow,
	t1.status,
	--t4.unique_id,
	t4.stock_heat as colada,
	coalesce( t3.clone_product,	t4.demand_product ) as productousar,
	t4.demand_product as productodestino,
	t3.clone_product as productoclon,
	-- Producto a usar. Si tiene informado clon usa el clon, sinó usará el del destino.
	t4.stock_heat_quality as calidadcolada

from 
	(
		select * from sip_wf_workflows where status in ('STARTED','APPROVED')
		--where workflow_id = 'xxxxx'
	) t1
	
	inner join (
		select * from sip_wf_workflow_steps 
	) t2 on t2.workflow_id = t1.workflow_id
	
	inner join (
		select * from sip_wf_mat_allocations
		where 
			stock_heat_quality not in ('1','0')
			and stock_heat_quality is not null
	) t4 on t4.unique_id = t1.unique_id
	
	left join (
		select * from sip_wf_workflow_step_responses where clone_product is not null
	) t3 on t3.workflow_step_id = t2.workflow_step_id
	
order by t1.workflow_id desc
