-- FUNCTION: omp_owner.nifi_md_tam_legacy_hdr_raw_upsert_fn(integer)

-- DROP FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_upsert_fn(integer);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_upsert_fn(
	p_raw_header_id integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
declare
  
  v_ret boolean default false;
  
  v_productid varchar;
  v_productid_tia varchar;
  v_demandid varchar;
  v_is_tia boolean default false;
  v_ccg_id int4;
  
  v_cic_c_tipo varchar;
  v_inputdiameter numeric;
  v_inputlength numeric;
  v_outputlength numeric;
  v_outputsections integer;
  
  v_rolling_mill_mark varchar default null;
  v_mh_type varchar default null;
  
  
 begin
	
	-- obtengo datos cabecera del pedido
	select productid, demandid, is_tia, productid_tia from nifi_md_tam_legacy_hdr_raw_header where id = p_raw_header_id into v_productid, v_demandid, v_is_tia, v_productid_tia;
	-- fin obtengo		
	
	-- selecciono el ccg_id correcto para producto, cv, cv_partida
	with parameter (_c_prodto, _c_cv, _c_partda_cv) as ( values (v_productid, SUBSTRING(v_demandid,3,6) , SUBSTRING(v_demandid,10,2) ) ),
	  ccg_id as ( select max(ccg.ccg_id) id, ccg.c_cv
				  from omp_owner.ccg_ciclo_completo_general ccg, parameter p
				  where 
					(ccg.c_prodto = _c_prodto and ccg.c_cv = '000000') 
					 or 
					(ccg.c_prodto = _c_prodto and ccg.c_cv = p._c_cv and ccg.c_partda_cv = p._c_partda_cv )
				  group by ccg.c_cv, ccg.c_partda_cv
				  order by c_cv desc, id desc limit 1
				 )
	select id from ccg_id into v_ccg_id;
	-- fin selecciono
	
	-- Inserta los datos de la ruta en la hdr_raw ( leyendo ccg, ccl, ccp )
	delete from omp_owner.nifi_md_tam_legacy_hdr_raw hr where hr.id = p_raw_header_id;
	
	with parameter (_ccg_id) as ( values (v_ccg_id) ),
	 ccg_ccl as (
			select 			
				p_raw_header_id id, 
				ccp.c_prodto productid, 
				ccp.n_ord_ruta routealternative,
				ccp.n_ord_proc,         -- Este campo no estaba antes
				row_number() OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) AS step0,
				ccp.c_estado_elabor operation,
				ccp.operationname operationname,
				ccp.c_estado_elabor inputstatus,
				case
					when (LEAD(ccp.c_estado_elabor,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int)) IS NULL
					THEN ccl.c_estado_elabor_sal
				else (LEAD(ccp.c_estado_elabor,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int))
				END outputstatus,
				coalesce(ccg.c_prioridad_laminac,'none') c_prioridad_laminac,		 		
				ccl.c_ruta operativesequence,				
				cast(ccg.f_aplica as date) processingdate,
				left(ccl.couplingasemblycenter,4) couplingassemblycentercode,
				case when ccl.couplingasemblycenter is null then false else true end couplingassemblycenter,
				left(ccl.c_seg_linea,4) c_seg_linea,
				--left(ccl.c_seg_linea,4) productivecentername,
				'DEFAULTGROUP' seqgroup,
				--ccp.n_tns_hr piecespershiftprogrammingrate,
				case when
						LAG(left(ccl.c_seg_linea,4),1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = left(ccl.c_seg_linea,4) 
						and
						LAG(ccl.c_estado_elabor_sal ,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = ccl.c_estado_elabor_sal 					
					then null
				else coalesce(ccl.n_prom_l_el * ccl.q_tiempo_disp * ccl.p_util / 100,ccl.n_std1 * 8) * case when ccp.operationname = 'P/CORTE DE MANGUITOS' then ccl.q_tramos else 1 end
				end piecespershiftprogrammingrate,
				--ccp.n_std1 * 8 piecespershiftprogrammingrate,
				case when
						LAG(left(ccl.c_seg_linea,4),1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = left(ccl.c_seg_linea,4) 
						and
						LAG(ccl.c_estado_elabor_sal ,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = ccl.c_estado_elabor_sal 					
					then null
				else ccl.n_std1
				end standardpiecespershiftrate,
				case when
						LAG(left(ccl.c_seg_linea,4),1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = left(ccl.c_seg_linea,4) 
						and
						LAG(ccl.c_estado_elabor_sal ,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = ccl.c_estado_elabor_sal 					
					then null
				else ccl.n_diam_mm
				end inputdiameter0,
				case when
						LAG(left(ccl.c_seg_linea,4),1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = left(ccl.c_seg_linea,4) 
						and
						LAG(ccl.c_estado_elabor_sal ,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = ccl.c_estado_elabor_sal 					
					then null
				else ccl.n_long
				end inputlength0,
				case when
						LAG(left(ccl.c_seg_linea,4),1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = left(ccl.c_seg_linea,4) 
						and
						LAG(ccl.c_estado_elabor_sal ,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = ccl.c_estado_elabor_sal 					
					then null
				else ccl.n_long_sal
				end outputlength,
				case when
						LAG(left(ccl.c_seg_linea,4),1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = left(ccl.c_seg_linea,4) 
						and
						LAG(ccl.c_estado_elabor_sal ,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = ccl.c_estado_elabor_sal 					
					then null
				else ccl.q_tramos
				end outputsections0,
				case when
						LAG(left(ccl.c_seg_linea,4),1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = left(ccl.c_seg_linea,4) 
						and
						LAG(ccl.c_estado_elabor_sal ,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = ccl.c_estado_elabor_sal 					
					then null
				else ccl.p_dnr
				end scrapyield,
				case when
						LAG(left(ccl.c_seg_linea,4),1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = left(ccl.c_seg_linea,4) 
						and
						LAG(ccl.c_estado_elabor_sal ,1) OVER (PARTITION BY ccp.n_ord_ruta ORDER BY ccp.n_ord_proc::int) = ccl.c_estado_elabor_sal 					
					then null
				else ccl.p_util
				end utilization,
				ccl.tempaust1,
				ccl.tempaust2,
				ccl.tempaust3,
				ccl.tempreve1,
				ccl.tempreve2,
				ccl.tempreve3,
				ccl.tempreve4,
				null upsetpunch1, 
				null upsetpunch2,
				null upsetmatrix1,
				null upsetmatrix2,
				null::numeric upsetspec,
				cast(ccg.f_aplica as date) fromdate,
				cast('9999-12-31' as date) uptodate
			from omp_owner.ccp_ciclo_completo_proceso as ccp
			inner join omp_owner.ccl_ciclo_completo_linea ccl
				on (ccp.ccg_id = ccl.ccg_id 
					and ccp.n_ord_ruta = ccl.n_ord_ruta
					and ccp.n_ord_linea::int = ccl.n_ord_linea::int
					)
			inner join omp_owner.ccg_ciclo_completo_general ccg
				on (ccp.ccg_id = ccg.ccg_id), parameter p
			where 
				ccg.ccg_id = p._ccg_id -- parameter
				and lpad(ccp.c_estado_elabor,2,'0') in (
					select distinct lpad(ccl.c_estado_elabor,2,'0') 
					from omp_owner.ccl_ciclo_completo_linea as ccl
					where 
					ccl.ccg_id = p._ccg_id -- parameter
					group by ccl.c_estado_elabor)
			order by ccp.n_ord_ruta,ccp.n_ord_linea::integer,ccp.n_ord_proc::integer
				),
		-- Join con alt para generar toda la ruta para las demas alternativas 22/09/2022
		ccg_ccl_x_alts as ( 
			select  case when alts.c_proces in ('PQF3','LAC3') then 'Q'
						 when alts.c_proces in ('LAPE') then 'P'
						 when alts.c_proces in ('LACO') then 'L'
						 else 'VERRRR' end rollingmill,
					case when ccg_ccl.operationname = 'P/LAMINAR' then case 
																		 when alts.c_proces = 'PQF3' then 'LAC3' 
																		 else alts.c_proces 
																	   end 
					         else left(ccg_ccl.c_seg_linea,4) end productivecenter,
				  	case when ccg_ccl.operationname = 'P/LAMINAR' then case 
																		 when alts.c_proces = 'PQF3' then 'LAC3' 
																		 else alts.c_proces 
																	   end 
					         else left(ccg_ccl.c_seg_linea,4) end productivecentername,
				  	case when ccg_ccl.operationname = 'P/LAMINAR' then case 
																			when alts.c_proces in ('LACO','LAPE') then alts.r_diam_tocho
																			when alts.c_proces in ('PQF3','LAC3') then alts.n_diam_tocho																			
																		else alts.n_diam_tocho end
																  else inputdiameter0 end inputdiameter,
			      	case when ccg_ccl.operationname = 'P/LAMINAR' then case 
																			when alts.c_proces in ('LACO','LAPE') then alts.r_long_tocho
																			when alts.c_proces in ('PQF3','LAC3') then alts.n_long_tocho																			
																		else alts.n_long_tocho end 
																  else inputlength0 end inputlength,
					case when ccg_ccl.operationname = 'P/LAMINAR' then case 
																			when alts.c_proces in ('LACO','LAPE') then alts.r_esbozo_tocho
																			when alts.c_proces in ('PQF3','LAC3') then alts.n_esbozo_tocho																			
																		else alts.n_esbozo_tocho end 
																  else outputsections0 end outputsections,
			      	coalesce ( case when alts.c_proces ='PQF3' then 'Q' 
									when alts.c_proces = 'LACO' then 'L' 
									when alts.c_proces = 'LAPE' then 'P' 
										else alts.c_proces end ||  '_' || to_char(case 
																			when alts.c_proces in ('LACO','LAPE') then alts.r_diam_tocho
																			when alts.c_proces in ('PQF3','LAC3') then alts.n_diam_tocho																			
																				else alts.n_diam_tocho end,'FM9999.0') || '_' || 
																		  to_char(case 
																			when alts.c_proces in ('LACO','LAPE') then alts.r_long_tocho
																			when alts.c_proces in ('PQF3','LAC3') then alts.n_long_tocho																			
																				else alts.n_long_tocho end,'FM9999.000')
							  , ccg_ccl.c_prioridad_laminac ) rollingalternative,
				    alts.m_origen_prodto_lamina, alts.m_alternativa_lamina,
					ccg_ccl.*
			from ccg_ccl left outer join omp_owner.nifi_md_tam_alternativa_lami alts on ( ccg_ccl.productid = alts.c_prodto and alts.c_proces not in ('LAC3') )
		),
	ccg_ccl_x_alts_final as ( select *, row_number() OVER (PARTITION BY routealternative, m_origen_prodto_lamina, m_alternativa_lamina ORDER BY step0 ) AS step
							  from ccg_ccl_x_alts
							  where productivecentername not in ('COB3','HRI3', 'COBA')
							 )
	-- INSERT INTO HDR RAW
	INSERT INTO omp_owner.nifi_md_tam_legacy_hdr_raw
	 ( id, productid, rollingalternative, routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
	   operation, operationname, productivecenter, productivecentername, seqgroup, inputstatus, outputstatus,
	   piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
	   standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
	   upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate, m_origen_prodto_lamina, m_alternativa_lamina )		
    SELECT id, productid, rollingalternative, routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
	   operation, operationname, productivecenter, productivecentername, seqgroup, inputstatus, outputstatus,
	   piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
	   standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
	   upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate, m_origen_prodto_lamina, m_alternativa_lamina
	from ccg_ccl_x_alts_final	
	on CONFLICT ON CONSTRAINT nifi_md_tam_legacy_hdr_raw_pk do nothing;
	-- Fin inserta
	
	
	-- Obtencion datos requeridos para los casos de rutas cortas especiales
	if nullif(v_demandid,'') is not null
		then
		 select rolling_mill_mark, mh_type
		 from nifi_md_tam_legacy_hdr_demand
		 where hdr_raw_id = -1 and demandid = v_demandid
		 into v_rolling_mill_mark, v_mh_type;
	end if;
	
	with parameters (_c_prodto ) as ( values (v_productid) )
	select cic_c_tipo, cic_n_diam_mm inputdiameter, (n_rgo_hasta + n_rgo_desde)/2 inputlength, (n_rgo_hasta + n_rgo_desde)/2 outputlength , 1 outputsections  
	from omp_owner.nifi_md_tam_producto, parameters
	where c_prodto = _c_prodto --and cic_c_tipo = 'BAR'
	into v_cic_c_tipo, v_inputdiameter, v_inputlength, v_outputlength, v_outputsections;
	-- fin Obtencion datos requeridos para los casos de rutas cortas especiales
	
	-- Casos rutas cortas especiales
	CASE 
	 WHEN v_cic_c_tipo = 'BAR' then
	
	    delete from omp_owner.nifi_md_tam_legacy_hdr_raw hr where hr.id = p_raw_header_id;
	  
		with parameter (_id, _productid, _inputdiameter, _inputlength, _outputlength, _outputsections) as ( values (p_raw_header_id, v_productid, v_inputdiameter, v_inputlength, v_outputlength, v_outputsections ) )
			INSERT INTO omp_owner.nifi_md_tam_legacy_hdr_raw
		( id, productid, rollingalternative, routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
		operation, operationname, productivecenter, productivecentername, seqgroup, inputstatus, outputstatus,
		piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
		standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
		upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate )		
		SELECT _id, _productid, 'none', '1', '9999999999', 'VERRRR', cast(now() as date), null, 1, 
		'99', 'P/COLAR', 'CC1', 'CC1', 'COLADAGROUP', '00', '00',
		99.99, _inputdiameter, _inputlength, _outputlength, _outputsections, 0, 99.99, false,
		99.99, 0, 0, 0, 0, 0, 0, 0, '', 
		'', '', '', 0, cast('2021-01-01' as date), cast('9999-12-31' as date)
		from parameter;
	
	WHEN v_rolling_mill_mark = 'T' and v_mh_type = 'N' and v_cic_c_tipo not in ('COP','COC','MAN') then

		-- Ejecuta los pasos desde la raw hasta la summary solo para obtener el correcto outputlength
		v_ret = nifi_md_tam_legacy_hdr_raw_delete_steps_for_clon_fn(p_raw_header_id);
		v_ret =  nifi_md_tam_legacy_hdr_raw_upsert_additionals_fn(p_raw_header_id);
		v_ret =  nifi_md_tam_legacy_hdr_sintetizado_fn(p_raw_header_id);
		v_ret =  nifi_md_tam_legacy_hdr_dummies_fn(p_raw_header_id);
		v_ret =  nifi_md_tam_legacy_hdr_set_inputstatus_omp_fn(p_raw_header_id);
		
		select outputlength from omp_owner.nifi_md_tam_legacy_hdr_sintetizado
		where id = p_raw_header_id and  step_omp is not null order by step_omp desc limit 1
		into v_outputlength;
		-- Fin obtencion correcto outputlength
		
		-- Limpia la raw e inserta una sola operacion P/STOCK
		delete from omp_owner.nifi_md_tam_legacy_hdr_raw hr where hr.id = p_raw_header_id;
	  
		with parameter (_id, _productid, _inputdiameter, _inputlength, _outputlength, _outputsections) as ( values (p_raw_header_id, v_productid, v_inputdiameter, v_inputlength, v_outputlength, v_outputsections ) )
			INSERT INTO omp_owner.nifi_md_tam_legacy_hdr_raw
		( id, productid, rollingalternative, routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
		operation, operationname, productivecenter, productivecentername, seqgroup, inputstatus, outputstatus,
		piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
		standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
		upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate )		
		SELECT _id, _productid, 'none', '1', '9999999999', 'VERRRR', cast(now() as date), null, 1, 
		'99', 'P/STOCK', 'STOCK', 'STOCK', 'STOCKGROUP', '10', '00',
		99.99, _inputdiameter, coalesce(nullif(_inputlength,0),1), coalesce(nullif(_outputlength,0),1), _outputsections, 0, 99.99, false,
		99.99, 0, 0, 0, 0, 0, 0, 0, '', 
		'', '', '', 0, cast('2021-01-01' as date), cast('9999-12-31' as date)
		from parameter;
	  
	
  	else
	
	   	
	
	END CASE;
	-- fin -- Casos rutas cortas especiales
	
    return true;
  
  END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_upsert_fn(integer)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_upsert_fn(integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_upsert_fn(integer) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_upsert_fn(integer) TO omp_rw_role;

