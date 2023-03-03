-- FUNCTION: omp_owner.nifi_md_tam_legacy_hdr_to_omp_fn(integer)

-- DROP FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_fn(integer);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_fn(
	p_raw_header_id integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
declare
  
	v_now timestamp default now();
	
	v_ret boolean default false;
	_additional_data_json json default null;
	_additional_alternativas_laminacion_json json default null;
	_insertos_data_json json default null;
	
	v_laminador varchar default null;
	v_route_path jsonb default null;
	v_route_path_without_rollingalternative jsonb default null;
       
	
    -- Cursors definition for main_stock_batch and feedback_summary by centro, expediente
	-- Se excluyen las operaciones dummy que se sintetizan
    -- cur_main_legacy_hdr cursor for select * 
	--								 from nifi_md_tam_legacy_hdr_sintetizado 
	--								 where id = p_raw_header_id and step_omp is not null --not ( sintetizar = true and operation_dummy = true )
	--								 order by processid asc, step asc, coalesce(productivity,0) desc;
	cur_main_legacy_hdr cursor for								 
	 with omp_steps as ( select *
						from omp_owner.nifi_md_tam_legacy_hdr_sintetizado 
						where id = p_raw_header_id and step_omp is not null
						order by processid asc, step asc, coalesce(productivity,0) desc ),
     omp_steps_for_mounting_couplings as ( select omp_steps.*  , 
												case 
													when ( couplingassemblycenter is true and operationname like 'P/ROSCAR%') then 1																								     
													when ( couplingassemblycenter is true and operationname not like 'P/ROSCAR%') then 2
													when ( operationname like 'P/PONE-APRIETACOPLE%') then 3
												   else 99 
												end  coupling_mounting_omp_step_priority, 
												case 
													when ( couplingassemblycenter is true and operationname like 'P/ROSCAR%') then 1																								     
													when ( couplingassemblycenter is true and operationname not like 'P/ROSCAR%') then 2
													when ( operationname like 'P/PONE-APRIETACOPLE%') then 3
												   else 99 
												end  protectors_mounting_omp_step_priority
										   from omp_steps ),
	 min_priority as ( select processid, min(coupling_mounting_omp_step_priority) min_priority , min(protectors_mounting_omp_step_priority) min_protector_priority
					   from omp_steps_for_mounting_couplings
					   group by processid )	
	select id, omp_steps_for_mounting_couplings.processid, step, operation, productivecentername, sintetizar, dummy, 
		   productivity, inputstatus, operation_dummy, step_omp, routealternative, inputstatus_omp, 
		   productivity_forzada, productid, rollingalternative, operationname, 
		   case when coupling_mounting_omp_step_priority =  min_priority and min_priority <> 99 then true else false end couplingassemblycenter, couplingassemblycentercode,
		   case when protectors_mounting_omp_step_priority =  min_protector_priority and min_protector_priority <> 99 then true else false end protectorassemblycenter,
		  creation_date, inputlength, outputlength, m_origen_prodto_lamina, m_alternativa_lamina				  
	from omp_steps_for_mounting_couplings 
			left outer join min_priority on (omp_steps_for_mounting_couplings.processid = min_priority.processid )
    order by omp_steps_for_mounting_couplings.processid asc, step asc, coalesce(productivity,0) desc, ( select jsonb_array_elements_text(routealternative)::numeric c1 order by c1 limit 1) asc;
	
	 
	 cur_main_legacy_hdr_record nifi_md_tam_legacy_hdr_sintetizado;
	 cur_main_legacy_hdr_record_prev nifi_md_tam_legacy_hdr_sintetizado;
	 
	 cur_main_legacy_hdr_record_prev_operation nifi_md_tam_legacy_hdr_sintetizado;
	-- End cursors definition
	
	-- omp_process record and array 
	_omp_process nifi_md_omp_process;
	_array_of_omp_process nifi_md_omp_process[];
	
	-- omp_operation record and array 
	_omp_operation nifi_md_omp_operation;
	_array_of_omp_operation nifi_md_omp_operation[];
	_omp_operation_prev nifi_md_omp_operation;
	
	-- omp_bom record and array 
	_omp_bom nifi_md_omp_bom;
	_array_of_omp_bom nifi_md_omp_bom[];
	_array_of_omp_all_bom nifi_md_omp_bom[];
	
	-- omp_rate record and array 
	_omp_rate nifi_md_omp_rate;
	_array_of_omp_rate nifi_md_omp_rate[];
	_array_of_all_omp_rate nifi_md_omp_rate[];
		
    
 begin
	 
	-- Load the adittional product/demand required data into a json
	with parameter (_raw_header_id ) as ( values (p_raw_header_id ) ),
				hdr as ( 
									select productid, demandid, tipo_hdr, is_tia, productid_tia
									from nifi_md_tam_legacy_hdr_raw_header, parameter p
									where id = p._raw_header_id
				),
				ccg as ( select ccg.ccg_id id
						 from omp_owner.ccg_ciclo_completo_general ccg, hdr pd
						 where (ccg.c_prodto = pd.productid and 
								ccg.c_cv = SUBSTRING(pd.demandid,3,6) and 
								ccg.c_partda_cv = SUBSTRING(pd.demandid,10,2) ) or 	( ccg.c_prodto = pd.productid ) 
					     order by c_cv desc limit 1 ),
				ccl as ( select * from ccl_ciclo_completo_linea ccl, ccg
				where ccl.ccg_id = ccg.id and couplingasemblycenter is not null ),
			more_add_data as (	select finallenghwithcoupling from ccl limit 1 )
    select row_to_json(row) from (
			select  prod.c_prodto productid, raw_header.tipo_hdr, raw_header.demandid, null::varchar recovery_order, prod.c_cve_acero productsteelcode, prod.cic_c_tipo prodtypecode, nullif(prod.c_prodto_cople,'00000') couplingid, more_add_data.finallenghwithcoupling lengthref, nullif(prod.c_prodto_semiel,'00000') hollowid, nullif(prod.c_prodto_madre,'00000') accesoryhollowid, omptype, unitofmeasure, 
			        nullif(protpin,'0') protpin, nullif(protbox,'0') protbox, nullif(protpinalt,'0') protpinalt, nullif(protboxalt,'0') protboxalt, nullif(protpininternal,'0') protpininternal, nullif(protboxinternal,'0') protboxinternal,
					clon.product_final_status last_output_status, prod.n_rgo_desde, prod.n_rgo_hasta, case when omptype = 'Couplings' and raw_header.demandid is null then prod.c_tia else null end c_tia, prod_cupla.c_tia c_tia_cupla,
					raw_header.is_tia , raw_header.productid_tia
			from hdr raw_header
	            inner join parameter p on (true)
				left outer join more_add_data on (true)
				left join nifi_md_tam_producto prod on (prod.c_prodto = raw_header.productid)
				left outer join nifi_md_tam_producto prod_cupla on (prod_cupla.c_prodto = nullif(prod.c_prodto_cople,'00000') )
				left outer join nifi_md_tam_producto_clon clon on ( clon.product_id = raw_header.productid )
				left join nifi_md_omp_planningproducttypeuom uom on (prod.cic_c_tipo = uom.subtypeid)
				left join nifi_md_tam_legacy_hdr_demand demand on (demand.hdr_raw_id = -1 and demand.demandid = raw_header.demandid )
			 ) row into _additional_data_json;
	--select row_to_json(row) from (
	--		select  prod.c_prodto productid, raw_header.tipo_hdr, raw_header.demandid, null::varchar recovery_order, prod.c_cve_acero productsteelcode, prod.cic_c_tipo prodtypecode, nullif(prod.c_prodto_cople,'00000') couplingid, 0 lengthref, nullif(prod.c_prodto_semiel,'00000') hollowid, nullif(prod.c_prodto_madre,'00000') accesoryhollowid, omptype, unitofmeasure, nullif(protpin,'0') protpin, nullif(protbox,'0') protbox, nullif(protpinalt,'0') protpinalt, nullif(protboxalt,'0') protboxalt, nullif(protpininternal,'0') protpininternal, nullif(protboxinternal,'0') protboxinternal
	--		from nifi_md_tam_legacy_hdr_raw_header raw_header
	--			left join nifi_md_tam_producto prod on (prod.c_prodto = raw_header.productid)
	--			left join nifi_md_omp_planningproducttypeuom uom on (prod.cic_c_tipo = uom.subtypeid)
	--			left join nifi_md_tam_legacy_hdr_demand demand on (demand.hdr_raw_id = -1 and demand.demandid = raw_header.demandid )
	--		where raw_header.id = p_raw_header_id ) row into _additional_data_json;
			
	open cur_main_legacy_hdr;
	fetch cur_main_legacy_hdr into cur_main_legacy_hdr_record;
	
	-- Load the insertos required data into a json
	if cur_main_legacy_hdr_record.id is not null then 
	with parameters (_productid) as ( values (cur_main_legacy_hdr_record.productid) )
		select array_to_json(array_agg(row_to_json(t)))
			from (
				  SELECT productid, endfamilyname, gradefamilyname, machinegroupid, machinegroupname, sidetype, materialid, performance, preference
				  FROM nifi_md_tam_legacy_hdr_insertos, parameters p
				  where hdr_raw_id = -1 and productid = p._productid
				 ) t into _insertos_data_json;	
	-- End Load insertos
	
	loop
	  exit when not found;
	  -- Process for every processid, Main corte de control
	  if (  ( cur_main_legacy_hdr_record.processid <> cur_main_legacy_hdr_record_prev.processid ) or cur_main_legacy_hdr_record_prev.processid is null )
			then
				-- Sets the corresponding centro de laminado para el proceso a ser generado
				SELECT productivecentername
				FROM nifi_md_tam_legacy_hdr_sintetizado
				where id = cur_main_legacy_hdr_record.id and  processid = cur_main_legacy_hdr_record.processid and ( operation = '4' or operation = '04' )
				limit 1 into v_laminador;
				
			-- Load the alternativa data for the v_laminador found in previous step
			with parameter (_raw_header_id, _laminador ) as ( values (p_raw_header_id, v_laminador ) ),
				hdr as ( 
									select id, productid, demandid, tipo_hdr
									from nifi_md_tam_legacy_hdr_raw_header, parameter p
									where id = p._raw_header_id
				),				
				hdr_raw as ( select * from omp_owner.nifi_md_tam_legacy_hdr_raw r, hdr
					     where r.id = hdr.id and r.operationname = 'P/LAMINAR' and r.processid = cur_main_legacy_hdr_record.processid limit 1),
				lamina as (	select hdr_raw.productivecentername centername, 
				                   hdr_raw.m_alternativa_lamina rollingpriority, 
								   000 steelcode,
								   hdr_raw.outputsections,
								   hdr_raw.outputlength length,
								   case when p._laminador in ('PQF3','LAC3') then 	n_long_min_obj
										when p._laminador in ('LACO') then  p_rango_calculo_1_o_2
										when p._laminador in ('LAPE') then	p_rgo_calculo_lp
										else n_long_min_obj
									end minimumlength,
								   case when p._laminador in ('PQF3','LAC3') then 	n_long_max_obj
										when p._laminador in ('LACO') then  p_rango_calculo_1_o_2
										when p._laminador in ('LAPE') then	p_rgo_calculo_lp
										else n_long_max_obj
									end maximumlength,
								   --n_long_min_obj minimumlength,
								   --n_long_max_obj maximumlength,	   	
								   case 
										when p._laminador in ('LACO') then r_long_tocho
										when p._laminador in ('LAPE') then r_long_tocho 
										when p._laminador in ('LAC3','PQF3') then n_long_tocho
										else n_long_tocho
								   end billlength,							   
								   case when p._laminador in ('LAC3','PQF3') then n_diam_tocho
										when p._laminador in ('LACO','LAPE') then r_diam_tocho
										else n_diam_tocho
								   end billdiameter,
								   0 mediumtrim,
								   0 endtrim,
								   0 peaktrim,
								   case 
										when p._laminador in ('LACO') then p_n_long_perf_1_o_2
										when p._laminador in ('LAPE') then p_Long_perf_lp 
										when p._laminador in ('PQF3','LAC3') then n_long_ctp
										else 0
								   end lengthatdriller,
								   n_long_sal_cex lengthatextractor,
								   n_diam_ctp diameteratextractor,
								   0 lengthatgauge,
								   --n_long_bme finaltubelength,
								   case when p._laminador in ('LAC3','PQF3') then n_long_bme
										when p._laminador in ('LACO') then p_rango_calculo_1_o_2
										when p._laminador in ('LAPE') then p_rgo_calculo_lp
										else n_long_bme
								   end finaltubelength,
								   0 manufacturinglength,
								   0 minimummanufacturinglength,
								   0 maximummanufacturinglength,
								   0 remaininglength,
								   case when p._laminador in ('LACO','LAPE') then to_char(r_diam_mandr,'FM000.0') else to_char(n_diam_mandr,'FM000.0') end mandreldiameter,	  
								   case when p._laminador in ('LACO','LAPE') then to_char(r_calib,'FM000') else to_char(n_calib,'FM000') end caliber,
								   d_serie_lamina calibration,	   
								   0 endtrimatcontinuous,
								   0 peaktrimatcontinuous,
								   0 endtrimatlre, 
								   0 peaktrimatlre,
								   0 wtatextractor,
								   case when p._laminador in ('LAC3','PQF3') then n_esp_pqf
										when p._laminador in ('LACO') then p_n_esp_mpm
										when p._laminador in ('LAPE') then 0
										else 0
								   end rollingwt,
								   case when p._laminador in ('LAC3','PQF3') then n_esp_pqf
										when p._laminador in ('LACO') then p_n_esp_mpm
										when p._laminador in ('LAPE') then 0
										else 0
								   end manufacturingwt,
								   case 
										when p._laminador in ('LACO') then r_long_Lamina * 1.012
										when p._laminador in ('LAC3','PQF3') then n_long_srm
										when p._laminador in ('LAPE') then p_long_sal_calib_lp
										else 0
								   end lengthaftersizing,
								   p_n_long_mpm_1_o_2,
								   n_long_sal_pqf,
								   p_n_desp_laco_p1_o_2,
								   n_long_desp_punta,
								   n_long_lamina, 
								   n_tubos_esbozo,
								   n_long_bme,
								   p_long_esb_util_lp,
								   n_despunte_cex,
								   p_n_desp_laco_c1_o_2,
								   n_long_desp_cola,
								   p_long_sal_calib_lp,
								   p_n_long_term_1_o_2,
								   p_rgo_calculo_lp,
								   p_rango_calculo_1_o_2,
								   r_esbozo_tocho,
								   n_long_em
							from omp_owner.nifi_md_tam_alternativa_lami, hdr_raw, hdr, parameter p
							where c_prodto = hdr.productid and nifi_md_tam_alternativa_lami.m_origen_prodto_lamina = hdr_raw.m_origen_prodto_lamina and nifi_md_tam_alternativa_lami.m_alternativa_lamina = hdr_raw.m_alternativa_lamina )
			    select row_to_json(row) data_as_json from ( select * from lamina ) row into _additional_alternativas_laminacion_json;				
				-- end Load alternativa data
				
				-- Gets the route_path for each process ( used for getting the process_number PRO_NN_...)
				select route_path, route_path->jsonb_object_keys(route_path) from nifi_md_tam_legacy_hdr_raw
				where id = cur_main_legacy_hdr_record.id and processid = cur_main_legacy_hdr_record.processid and route_path is not null limit 1
				into v_route_path, v_route_path_without_rollingalternative;			
				-- End gets the route_path
				
				-- creates new omp_process
				_omp_process = nifi_md_tam_legacy_hdr_to_process_fn(cur_main_legacy_hdr_record, _additional_data_json::json, _additional_alternativas_laminacion_json::json, v_route_path_without_rollingalternative);
				_array_of_omp_process =  array_append(_array_of_omp_process,_omp_process);
				
				-- creates the LAST bom_out for an intermediate process
					if cur_main_legacy_hdr_record_prev_operation.id is not null then 
						_omp_bom = nifi_md_tam_legacy_hdr_to_bom_out_last_fn(cur_main_legacy_hdr_record_prev_operation,_omp_operation_prev, _additional_data_json);
						_array_of_omp_all_bom =  array_append(_array_of_omp_all_bom,_omp_bom);
					end if;
					
				-- resets prev operation info
				cur_main_legacy_hdr_record_prev_operation = null;
				_omp_operation_prev = null;
			end if;
	
	   if ( ( cur_main_legacy_hdr_record.processid <> cur_main_legacy_hdr_record_prev.processid ) or
			( cur_main_legacy_hdr_record.step <> cur_main_legacy_hdr_record_prev.step ) or cur_main_legacy_hdr_record_prev.step is null )
			then
				-- creates new omp_operation
				--if not coalesce(cur_main_legacy_hdr_record_prev.sintetizar,false) and 
				--   not coalesce(cur_main_legacy_hdr_record.dummy,false)
				-- then 
				    -- creates an operation for a process
					_omp_operation = nifi_md_tam_legacy_hdr_to_operation_fn(cur_main_legacy_hdr_record,_omp_process, _omp_operation_prev, _array_of_omp_operation, _additional_data_json);
					_array_of_omp_operation =  array_append(_array_of_omp_operation,_omp_operation);
					
					-- creates the rates for an operation
					_array_of_omp_rate = nifi_md_tam_legacy_hdr_to_rate_fn(cur_main_legacy_hdr_record,_omp_operation, _additional_data_json);
					 if cardinality(_array_of_omp_rate) > 0 then 
						foreach _omp_rate IN ARRAY _array_of_omp_rate
						 loop
						   _array_of_all_omp_rate =  array_append(_array_of_all_omp_rate,_omp_rate);							
						end loop;
					 end if;
					 
					-- creates the bom_out for the prev operation
					if cur_main_legacy_hdr_record_prev_operation.id is not null then 
						_omp_bom = nifi_md_tam_legacy_hdr_to_bom_out_fn(cur_main_legacy_hdr_record, cur_main_legacy_hdr_record_prev_operation,_omp_operation,_omp_operation_prev, _additional_data_json);
						_array_of_omp_all_bom =  array_append(_array_of_omp_all_bom,_omp_bom);
					end if;
										
					
					-- creates the bom_in(s) for the current operation
					_array_of_omp_bom = nifi_md_tam_legacy_hdr_to_bom_in_fn(cur_main_legacy_hdr_record,_omp_operation, _array_of_omp_all_bom, _additional_data_json, _insertos_data_json);
					if cardinality(_array_of_omp_bom) > 0 then 
						foreach _omp_bom IN ARRAY _array_of_omp_bom
						 loop
						   _array_of_omp_all_bom =  array_append(_array_of_omp_all_bom,_omp_bom);							
						end loop;
					 end if;					
					
					-- prev operation info - use for the bom_out
					cur_main_legacy_hdr_record_prev_operation = cur_main_legacy_hdr_record;
					_omp_operation_prev = _omp_operation;
				 --end if;
			end if;
	  
	  cur_main_legacy_hdr_record_prev = cur_main_legacy_hdr_record;
	  fetch cur_main_legacy_hdr into cur_main_legacy_hdr_record;
    end loop;
	
	 -- creates the LAST bom_out for the LAST process
	if cur_main_legacy_hdr_record_prev_operation.id is not null then 
		_omp_bom = nifi_md_tam_legacy_hdr_to_bom_out_last_fn(cur_main_legacy_hdr_record_prev_operation,_omp_operation_prev, _additional_data_json);
		_array_of_omp_all_bom =  array_append(_array_of_omp_all_bom,_omp_bom);
	end if;

	 -- Finally populates the omp tables
	 if cardinality(_array_of_omp_process) > 0 then 
	   v_ret = nifi_md_tam_legacy_hdr_to_omp_tables_fn(_array_of_omp_process, _array_of_omp_operation, _array_of_omp_all_bom, _array_of_all_omp_rate );
	   return v_ret;
	  else 
	   return false;
	 end if;
	 
    else
	  return false;	  
    end if; 
  
  END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_fn(integer)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_fn(integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_fn(integer) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_fn(integer) TO omp_rw_role;

