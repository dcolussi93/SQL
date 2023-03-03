

select 
	t3.steel_knowledge_base_id as rule,
	--t3.processed_status,
	--t3.specific_product_clone_id,
	--t1.stock_steel_code_id,
	--t1.demand_product,
	--t1.demand_steel_id,
	t2.workflow_id,
	t2.status,
	t1.mat_stock_id,
	t3.comments
from (
	select * from sip_wf_mat_allocations where mat_stock_id in (
		'BA_S_356/310_03_45185_10700_TM930137031_386309',
		'BA_S_776/270_03_45052_13460_TM930066086_256713',
		'BA_S_310/215_03_44687_09840_TM299211001_384606',
		'BA_S_696/215_03_41673_09852_TM290286001_365533'		
	)
) t1
inner join (
	select * from sip_wf_workflows
) t2 on t2.unique_id = t1.unique_id
left join (
	select * from sip_wf_steelknowledgebase --where steel_knowledge_base_id in ('60','61')
) t3 on t3.comments like '%'||	t2.unique_id ||'%'
order by t2.workflow_id desc