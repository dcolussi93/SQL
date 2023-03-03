select
    t3.demand_od as diametro_demanda,
    t5.nombre as estado_desc,
    t1.stock_status as estado,
    t5.guia_perfil_difa as perfil_compatible_estado,
    t6.planta_matalloc,
    t1.perfil_difa as perfil_asignado_final,
    t3.unique_id as allocacion,
    t6.prod_origen as producto_origen,
    t3.demand_product as producto_destino,
    t4.workflow_id as wf,
    t4.status as estadowf
    
    
    
from
    -- Pack Responses de Workflows (solo WIP)
    (
        select * from sip_wf_pack_responses where delete_date is null
    ) t1
    
    inner join (
        select * from sip_wf_packs
    ) t2 on t2.pack_id = t1.pack_id
    
    -- Info allocaciones WFS
    inner join (
        select * from sip_wf_mat_allocations
    ) t3 on t3.unique_id = t1.unique_id
    
    -- Info de los Workflows.
    inner join (
        select * from sip_wf_workflows where status in ('APPROVED','STARTED','REJECTED')
        and to_char(created_date,'yymm') in ('2204')
    ) t4 on t4.workflow_id = t2.workflow_id
    
    -- Informacion de los estados.    
    left join (
        select * from sip_hrr_estados
    ) t5 on t5.id_estado::text = t1.stock_status::text
    
    -- Clasificacion de Planta. y extraccion producto.
    inner join (
        select
            unique_id,
            case 
                when unique_id like '%LACO1%' then 'LACO1'
                when unique_id like '%LACO2%' then 'LACO2'
                when unique_id like '%AFT%' then 'AFT'
                when unique_id like '%BARRA%' then 'BARRA'
                when unique_id like '%PREMI%' then 'PREMI'
                else null
            end as planta_matalloc,
            split_part (
                split_part(unique_id ,'SD' ,2),
                '_',
                2
            ) as prod_origen
            
        from sip_wf_mat_allocations 
    ) t6 on t6.unique_id = t3.unique_id
    
--where planta is null
order by t4.workflow_id desc