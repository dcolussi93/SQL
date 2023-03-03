select
	string_agg(distinct t3.perfil_difa, ' ,') pdifa,
	t1.workflow_id,
	t1.status,
	count(distinct t3.unique_id),
	max(t1.last_updated_date),
	count(distinct t3.perfil_difa )
from 
	(
		select * from sip_wf_workflows where status in ('APPROVED','REJECTED')
		and to_char(last_updated_date,'yymm') in ('2204', '2205')
	) t1
	inner join (
		select * from sip_wf_packs
	) t2 on t2.workflow_id = t1.workflow_id
	inner join (
		select * from sip_wf_pack_responses where delete_date is null
	) t3 on t3.pack_id = t2.pack_id
	group by t1.workflow_id, t1.status
	
	having count(distinct t3.perfil_difa )>1
	order by max(t1.last_updated_date) desc
	
	