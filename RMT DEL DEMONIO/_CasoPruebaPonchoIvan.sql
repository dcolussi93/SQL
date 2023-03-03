select * from sip_products where product_id = 'F7457'
select * from sip_alternativas_laminacion where productid = 'F7457'
select * from wf_extornos_ciclocom_nifialterlamiretry_fn() 

select * from sip_procesos_log where nombre_proceso = 'wf_extornos_ciclocom_solicitar_tam_fn'
and to_char(fecha_log , 'yymmdd') = '220405'

update sip_wf_mat_allocations set insert_date = now() where unique_id = 'c6e5/195fd_BA_S_78/620_40055_07830_TM930000179_G1012_'
select * from wf_extornos_ciclocom_solicitar_tam_fn('{"uniqueid":"c6e5/195fd_BA_S_78/620_40055_07830_TM930000179_G1012_"}')

select * from sip_owner.sip_proceso_new_alterlami 

--delete from sip_owner.sip_proceso_new_alterlami where alterlami_id >1;

select * from sip_wf_steelknowledgebase where comments like '%c6e5/195fd_BA_S_78/620_40055_07830_TM930000179_G1012_%'


select * from sip_wf_workflows where unique_id = 'c6e5/195fd_BA_S_78/620_40055_07830_TM930000179_G1012_'
select * from sip_mat_allocation where unique_id = 'c6e5/195fd_BA_S_78/620_40055_07830_TM930000179_G1012_'

update sip_wf_workflows set status = 'STARTED' where workflow_id in (165,164,163,159,98);

update sip_wf_workflow_steps t1 set status = 'PENDING'
from (
select * from sip_wf_workflow_steps
where workflow_id in (165,164,163,159,98)
and sequence=1
order by workflow_id, sequence
) t2
where t1.workflow_step_id = t2.workflow_step_id;

update sip_wf_workflow_steps t1 set status = 'WAITING'
from (
select * from sip_wf_workflow_steps
where workflow_id in (165,164,163,159,98)
and sequence>1
order by workflow_id, sequence
) t2
where t1.workflow_step_id = t2.workflow_step_id;

	select t1.* from 
	(
		select * from sip_stock_wip where bundle_origin = 'LIN1'
	) t1
	inner join (
		select * from sip_owner.rr_rmt_stockwip_tam_fn()
	) t2 on t2.lingada = t1.bundle


 
--delete from sip_owner.sip_proceso_new_alterlami where alterlami_id >1
select * from wf_extornos_ciclocom_nifialterlamiretry_fn()

  select  
            'Test'::text as x_uniqueid,
            'Test'::text as x_alterlamicalc
select 
	uniqueid,
	-- Llamada a funcion para solicitar alternativas lami
	wf_extornos_ciclocom_solicitar_tam_fn(  
		('{"uniqueid":"'||uniqueid||'"}')::json 
	) 
from wf_extornos_ciclocom_allocacionesposibles_fn()

select * from sip_wf_mat_allocations limit 50

select * from wf_extornos_ciclocom_allocacionesposibles_tam_fn()



select * from sip_wf_mat_allocations where unique_id = 'c6e5/195fd_BA_S_78/620_40055_07830_TM930000179_G1012_'
--update sip_wf_mat_allocations set alterlamifaildate = now() where unique_id = 'c6e5/195fd_BA_S_78/620_40055_07830_TM930000179_G1012_';
select * from sip_owner.sip_proceso_new_alterlami 
select * from sip_wf_mat_allocations where unique_id = '3f79c/943af_TO_S_310/215_04_37914_03271_TM930137043_386729_'
--delete from sip_owner.sip_proceso_new_alterlami  where alterlami_id >1

select x.processed_status, x.* from sip_wf_steelknowledgebase x where steel_knowledge_base_id = '66'



        select  
            unique_id ::text,
			alterlamifaildate ::text ,
            sip_owner.wf_extornos_ciclocom_solicitar_tam_fn( unique_id )::text as x_calculadoraok,
			1=1
        from (
			select * from sip_owner.sip_wf_mat_allocations 
			where 
				alterlamifaildate is not null 
			and to_char(alterlamifaildate,'yymm') = to_char(now(),'yymm')
        ) x
		
		--select * from wf_extornos_ciclocom_nifialterlamiretry_fn()
		
	
select t2.workflow_id , t1.unique_id from 
(select * from sip_wf_mat_allocations where mat_stock_id in (
		'TO_S_654/215_04_43050_03290_TM298782012_385095',
		'BA_S_302/215_BM/03_42062_03290_TM297214001_379879',
		'TO_S_310/215_04_37914_03271_TM930137043_386729',
		'BA_S_602/310_03_44878_12120_TM296813003_380898',
		'BA_S_648/310_03_43604_09010_TM400448025_374252',
		'BA_S_776/270_03_45052_13480_TM930066086_256713',
		'BA_S_647/270_03_44746_12480_TM400451097_385119',
		'BA_S_334/270_03_43989_11140_TM298129001_382191'	
) ) t1
inner join (
	select * from sip_wf_workflows
) t2 on t2.unique_id = t1.unique_id



		