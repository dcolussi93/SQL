
select 
	t1.workflow_id as wf,
	t6.workflow_step_id as step,
	t7.description as grupo,
	t1.status as status,
	t3.allocrequeridos as allocrequeridos,
	coalesce(t4.allocrespuestas,0) as allocrespuestas,
	t5.grupos_difa as gruposdifa,
	-- Calcula si puede avanzar o no el paso de DIFA.
	t3.allocrequeridos = coalesce(t4.allocrespuestas,0) as puedeavanzardifa
from 
	(
		select * from sip_wf_workflows
		where status = 'STARTED'
	) t1
	
	-- Solo va a traer workflows de WIP porque son los que tienen packses.
	inner join (
		select * from sip_wf_packs
	) t2 on t2.workflow_id = t1.workflow_id
	
	-- Traer solo los workflows que matcheen con algun paso de DIFA.
	inner join (
		select * from sip_wf_workflow_steps
	) t6 on t6.workflow_id = t1.workflow_id
	inner join (
		select * from sip_wf_auth_groups where description = 'DIFA'
	) t7 on t7.authorizer_group_id = t6.authorizer_group_id
	
	-- CALC1: Cantidad total de lotes no borrados que tiene que responder un WF wip.
	inner join (
		select 
			pack_id , 
			count(distinct unique_id) as allocrequeridos 
		from sip_wf_pack_responses where delete_date is null
		group by pack_id
	) t3 on t3.pack_id = t2.pack_id 
	
	-- CALC 1.1: Cantidad de perfiles difas presentes en una respuesta.
	inner join (
		select 
			pack_id, 
			string_agg(distinct perfil_difa, ', ' order by perfil_difa) as grupos_difa
		from 
			sip_wf_pack_responses where delete_date is null
		group by pack_id
	) t5 on t5.pack_id = t2.pack_id
	
	-- CALC2: Cantidad de packs no borrados ya respondidos por algun perfil de difa dentro de un wf.
	left join (
		select 
			pack_id , 
			count(distinct unique_id) as allocrespuestas 
		from sip_wf_pack_responses 
		where 
			delete_date is null
			and rta_difa_ok is not null
		--select * from sip_wf_pack_responses
		group by pack_id	
	) t4 on t4.pack_id = t2.pack_id


order by t1.workflow_id desc

