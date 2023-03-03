/*CONSULTAR PARAMETROS DE INGRESO 
Utilizar siguiente funcion select * from rmt_owner.rr_instanciatemp_estadoretornocaracteristicas_fn(24)
QUERY HDR: Consulta de estado de reingreso indicando
código de producto (Sip_orders), codigo de centro (Sip_centers), Demanda (SDxxxxxxx) casos de Recovery.*/

with 
   -- Consulta estado anterior
   parameters1 (_demandid, _productid, _center) as ( values ('${orden}', '${producto}'::character varying(20),'${centro_desc}') )
   --parameters1 (_productid, _center) as ( values (substr('022586',2),'151') )
   -- Hoja estandar
   ,last_hdr1 as ( 
		SELECT 
			id
		FROM 
			omp_owner.nifi_md_tam_legacy_hdr_raw_header hdr_h,
			parameters1 p
		where 
			hdr_h.productid = p._productid 
			and hdr_h.tipo_hdr = 'STANDARD' 
		order by id desc limit 1 
   )
   -- Hoja recuperacion
   ,last_hdr2 as ( 
		SELECT 
	   		id
		FROM 
	   		omp_owner.nifi_md_tam_legacy_hdr_raw_header hdr_h, 
	   		parameters1 p
		where 
	   		hdr_h.productid = p._productid 
	   		and hdr_h.demandid = _demandid   
	   		and hdr_h.tipo_hdr = 'RECOVERYSTD' 
		order by id desc --limit 1 
   )

-- QUERY PRINCIPAL
select 
	HDR, tipo, modoreingreso, estadoretorno 
from (
   	
   	-- Siguiente paso estandar
   	(
		select 
			'1 STD' as hdr,
			'SIGUIENTE' as tipo,
			'10' as modoreingreso,
			hdr_r.outputstatus as estadoretorno
		from
			omp_owner.nifi_md_tam_legacy_hdr_raw hdr_r,
			last_hdr1,
			parameters1 p
		where 
			hdr_r.id = last_hdr1.id 
			and hdr_r.productivecenter = p._center 
		order by processid, step desc 
		limit 1
   	)
   
   -- Anterior paso estandar
  	union
   	(
		select 
			'1 STD' as hdr,
			'ANTERIOR' as tipo,
			'20' as modoreingreso,
			hdr_r.inputstatus as estadoretorno
		from 
			omp_owner.nifi_md_tam_legacy_hdr_raw hdr_r,
			last_hdr1, 
			parameters1 p
		where 
			hdr_r.id = last_hdr1.id 
			and hdr_r.productivecenter = p._center 
		order by  processid, step 
		limit 1
   	)
   	
   	-- SECCION RECUPERACION CON RECOVERY ESTANDAR
   	-- Siguiente paso recovery
   	union
   	(
		select 
			'2 RECOVERY' as hdr,
			'SIGUIENTE' as tipo,
			'10' as modoreingreso,
		 	hdr_r.outputstatus as estadoretorno
		from 
		 	omp_owner.nifi_md_tam_legacy_hdr_raw hdr_r,
		 	last_hdr2,
		 	parameters1 p
		where 
		 	hdr_r.id = last_hdr2.id 
		 	and hdr_r.productivecenter = p._center 
		order by hdr_r.id desc, hdr_r.processid, hdr_r.step desc 
		limit 1
   )
   -- Anterior paso recovery
   union
   (
		select 
			'2 RECOVERY' as hdr,
			'ANTERIOR' as tipo,
			'20' as modoreingreso,
		 	hdr_r.inputstatus as estadoretorno
		from 
		 	omp_owner.nifi_md_tam_legacy_hdr_raw hdr_r,
		 	last_hdr2,
		 	parameters1 p
		where 
		 	hdr_r.id = last_hdr2.id 
		 	and hdr_r.productivecenter = p._center 
		order by hdr_r.id desc, hdr_r.processid, hdr_r.step
		limit 1
   )

) x  
where modoreingreso =  '${tipo_reingreso}'
order by hdr, modoreingreso
-- Toma el 1ero. si es estandar toma el valor '1 STD' por el order by.
limit 1
