-- FUNCTION: omp_owner.nifi_md_sid_legacy_hdr_to_omp_fn(integer)

-- DROP FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_omp_fn(integer);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_omp_fn(
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
	--								 from nifi_md_sid_legacy_hdr_sintetizado 
	--								 where id = p_raw_header_id and step_omp is not null --not ( sintetizar = true and operation_dummy = true )
	--								 order by processid asc, step asc, coalesce(productivity,0) desc;
	cur_main_legacy_hdr cursor for								 
	 with omp_steps as ( select *
						from omp_owner.nifi_md_sid_legacy_hdr_sintetizado 
						where id = p_raw_header_id and step_omp is not null
						order by processid asc, step asc, coalesce(productivity,0) desc ),
     omp_steps_for_mounting_couplings as ( select omp_steps.*  , 
												case 
												    when ( operationname like ('ROSCADO%') ) then 1  -- Operaciones de Roscado
													when ( couplingassemblycenter is true and operation = '40' ) then 2 --Apretado de Cuplas
													when ( couplingassemblycenter is true and operation = '461' and couplingassemblycentercode in ('848','849') ) then 3 -- Tratamiento DPLS BOX AND DOP4 
													when ( operation = '40') then 4 -- Apretado de Cuplas
													when (couplingassemblycenter is true) then 5  -- 
													when ( operation = '9' ) then 6 -- Tratamiento Termico, debe funcionar como válvula de escape…..
													when ( operation = '3' ) then 7 -- Corte, debe funcionar como 2da. válvula de escape…..
												   else 99 
												end  coupling_mounting_omp_step_priority, 
												case 
													when ( couplingassemblycenter is true ) then 1  -- Lleva protectores
													when (  operationname = 'ROSCADO' ) then 2  -- Operaciones de Roscado
													when ( productivecentername in ('LLIP','LIDE','TEHE','LPIP','LIN1','FPAC','FPEC') ) then 3 --debe funcionar como válvula de escape…..
												   else 99 
												end  protectors_mounting_omp_step_priority
										   from omp_steps ),
	 min_priority as ( select processid, min(coupling_mounting_omp_step_priority) min_priority , min(protectors_mounting_omp_step_priority) min_protector_priority
					   from omp_steps_for_mounting_couplings
					   group by processid )	
	select id, omp_steps_for_mounting_couplings.processid, step, operation, productivecentername, sintetizar, dummy, 
		   creation_date, productivity, inputstatus, operation_dummy, step_omp, routealternative, inputstatus_omp, 
		   productivity_forzada, productid, rollingalternative, operationname,
		   case when coupling_mounting_omp_step_priority =  min_priority and min_priority <> 99 then true else false end couplingassemblycenter, couplingassemblycentercode,
		    case when protectors_mounting_omp_step_priority =  min_protector_priority and min_protector_priority <> 99 then true else false end protectorassemblycenter,
			lastlegacyupdate, inputlength, outputlength
	from omp_steps_for_mounting_couplings 
			left outer join min_priority on (omp_steps_for_mounting_couplings.processid = min_priority.processid )
    order by omp_steps_for_mounting_couplings.processid asc, step asc, coalesce(productivity,0) desc;
	
	 
	 cur_main_legacy_hdr_record nifi_md_sid_legacy_hdr_sintetizado;
	 cur_main_legacy_hdr_record_prev nifi_md_sid_legacy_hdr_sintetizado;
	 
	 cur_main_legacy_hdr_record_prev_operation nifi_md_sid_legacy_hdr_sintetizado;
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
	_omp_bom_out nifi_md_omp_bom;
	_array_of_omp_bom nifi_md_omp_bom[];
	_array_of_omp_all_bom nifi_md_omp_bom[];
	
	-- omp_rate record and array 
	_omp_rate nifi_md_omp_rate;
	_array_of_omp_rate nifi_md_omp_rate[];
	_array_of_all_omp_rate nifi_md_omp_rate[];
		
    
 begin
	 
	-- Load the adittional product/demand required data into a json
	select row_to_json(row) from (
			select  prod.productid, raw_header.tipo_hdr, raw_header.demandid, raw_header.recovery_order, productsteelcode, prodtypecode, nullif(couplingid,'0') couplingid, coalesce(coalesce(lengthref1,lengthref2),0) lengthref, nullif(hollowid,'0') hollowid, nullif(accessorieshollowid,'0') accessorieshollowid, omptype, unitofmeasure, nullif(protpin,'0') protpin, nullif(protbox,'0') protbox, nullif(protpinalt,'0') protpinalt, nullif(protboxalt,'0') protboxalt, nullif(protpininternal,'0') protpininternal, nullif(protboxinternal,'0') protboxinternal,
					oOfr.id out_of_route_id, oOfr.inputstatus out_of_route_inputstatus, oOfr.operation out_of_operation_id, prod.last_output_status last_output_status
			from nifi_md_sid_legacy_hdr_raw_header raw_header
				left join nifi_md_sid_legacy_hdr_producto prod on (prod.hdr_raw_id = -1 and prod.productid = raw_header.productid)
				left join nifi_md_omp_planningproducttypeuom uom on (prod.prodtypecode = uom.subtypeid)
				left join nifi_md_sid_legacy_hdr_demand demand on (demand.hdr_raw_id = -1 and demand.demandid = raw_header.demandid)
				left join nifi_md_sid_legacy_hdr_raw_out_of_route_operation oOfr on ( oOfr.id = raw_header.id )
			where raw_header.id = p_raw_header_id ) row into _additional_data_json;
				
			
	open cur_main_legacy_hdr;
	fetch cur_main_legacy_hdr into cur_main_legacy_hdr_record;
	if cur_main_legacy_hdr_record.id is not null then 
    -- Load the insertos required data into a json
	with parameters (_productid) as ( values (lpad(cur_main_legacy_hdr_record.productid,6,'0')) )
		select array_to_json(array_agg(row_to_json(t)))
			from (
				  SELECT productid, endfamilyname, gradefamilyname, machinegroupid, machinegroupname, sidetype, materialid, performance
				  FROM nifi_md_sid_legacy_hdr_insertos, parameters p
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
				FROM nifi_md_sid_legacy_hdr_sintetizado
				where id = cur_main_legacy_hdr_record.id and  processid = cur_main_legacy_hdr_record.processid and operation = '1'
				limit 1 into v_laminador;
				
				-- Load the alternativa data for the v_laminador found in previous step
				if v_laminador is not null then
				  with parameters (p_productid, p_centername, p_rollingalternative) as ( values (cur_main_legacy_hdr_record.productid, v_laminador, cur_main_legacy_hdr_record.rollingalternative) )
					select row_to_json(row) data_as_json from (
						(select centername, rollingpriority, steelcode, outputsections, length, minimumlength, maximumlength, billlength, billdiameter, mediumtrim, endtrim, peaktrim, lengthatdriller, lengthatextractor, diameteratextractor, lengthatgauge, finaltubelength, manufacturinglength, minimummanufacturinglength, maximummanufacturinglength, remaininglength, mandreldiameter, caliber, calibration, endtrimatcontinuous, peaktrimatcontinuous, endtrimatlre, peaktrimatlre, wtatextractor, rollingwt, manufacturingwt
						 from nifi_md_sid_legacy_hdr_alternativa_lami, parameters
						 where hdr_raw_id = -1 and productid = p_productid and centername = p_centername and rollingpriority = p_rollingalternative
						 order by coalesce (lastupdated_date,creation_date) desc limit 1 )
 						 union all
						( select centername, rollingpriority, steelcode, outputsections, length, minimumlength, maximumlength, billlength, billdiameter, mediumtrim, endtrim, peaktrim, lengthatdriller, lengthatextractor, diameteratextractor, lengthatgauge, finaltubelength, manufacturinglength, minimummanufacturinglength, maximummanufacturinglength, remaininglength, mandreldiameter, caliber, calibration, endtrimatcontinuous, peaktrimatcontinuous, endtrimatlre, peaktrimatlre, wtatextractor, rollingwt, manufacturingwt
						  from nifi_md_sid_legacy_hdr_alternativa_lami, parameters
						  where hdr_raw_id = -1 and productid = p_productid and centername = p_centername and rollingalternative = p_rollingalternative
						  order by coalesce (lastupdated_date,creation_date) desc limit 1 )
					) row into _additional_alternativas_laminacion_json;
					
				end if;				
				-- end Load alternativa data
				
				-- Gets the route_path for each process ( used for getting the process_number PRO_NN_...)
				select route_path, route_path->jsonb_object_keys(route_path) from nifi_md_sid_legacy_hdr_raw
				where id = cur_main_legacy_hdr_record.id and processid = cur_main_legacy_hdr_record.processid and route_path is not null limit 1
				into v_route_path, v_route_path_without_rollingalternative;				
				-- End gets the route_path
				
				-- creates new omp_process
				_omp_process = nifi_md_sid_legacy_hdr_to_process_fn(cur_main_legacy_hdr_record, _additional_data_json, _additional_alternativas_laminacion_json, v_route_path_without_rollingalternative);
				_array_of_omp_process =  array_append(_array_of_omp_process,_omp_process);
				
				-- creates the LAST bom_out for an intermediate process
					if cur_main_legacy_hdr_record_prev_operation.id is not null then 
						_omp_bom_out = nifi_md_sid_legacy_hdr_to_bom_out_last_fn(cur_main_legacy_hdr_record_prev_operation,_omp_operation_prev, _omp_bom_out, _additional_data_json);
						_array_of_omp_all_bom =  array_append(_array_of_omp_all_bom,_omp_bom_out);						
						_omp_bom_out = null; -- resets _omp_bom_out when changing the process
					end if;
					
				-- resets prev operation info
				cur_main_legacy_hdr_record_prev_operation = null;
				_omp_operation_prev = null;
			end if;
	
	   if (  ( cur_main_legacy_hdr_record.processid <> cur_main_legacy_hdr_record_prev.processid ) OR
	         ( cur_main_legacy_hdr_record.step <> cur_main_legacy_hdr_record_prev.step ) or cur_main_legacy_hdr_record_prev.step is null 
		  )
			then
				-- creates new omp_operation
				--if not coalesce(cur_main_legacy_hdr_record_prev.sintetizar,false) and 
				--   not coalesce(cur_main_legacy_hdr_record.dummy,false)
				-- then 
				    -- creates an operation for a process
					_omp_operation = nifi_md_sid_legacy_hdr_to_operation_fn(cur_main_legacy_hdr_record,_omp_process, _omp_operation_prev, _array_of_omp_operation, _additional_data_json);
					_array_of_omp_operation =  array_append(_array_of_omp_operation,_omp_operation);
					
					-- creates the rates for an operation
					_array_of_omp_rate = nifi_md_sid_legacy_hdr_to_rate_fn(cur_main_legacy_hdr_record,_omp_operation, _additional_data_json);
					 if cardinality(_array_of_omp_rate) > 0 then 
						foreach _omp_rate IN ARRAY _array_of_omp_rate
						 loop
						   _array_of_all_omp_rate =  array_append(_array_of_all_omp_rate,_omp_rate);							
						end loop;
					 end if;
					 
					-- creates the bom_out for the prev operation
					if cur_main_legacy_hdr_record_prev_operation.id is not null then 
						_omp_bom_out = nifi_md_sid_legacy_hdr_to_bom_out_fn(cur_main_legacy_hdr_record, cur_main_legacy_hdr_record_prev_operation,_omp_operation,_omp_operation_prev, _omp_bom_out, _additional_data_json);
						_array_of_omp_all_bom =  array_append(_array_of_omp_all_bom,_omp_bom_out);
					end if;
										
					
					-- creates the bom_in(s) for the current operation
					_array_of_omp_bom = nifi_md_sid_legacy_hdr_to_bom_in_fn(cur_main_legacy_hdr_record,_omp_operation, _array_of_omp_all_bom,  _omp_bom_out, _additional_data_json, _insertos_data_json);
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
		_omp_bom_out = nifi_md_sid_legacy_hdr_to_bom_out_last_fn(cur_main_legacy_hdr_record_prev_operation,_omp_operation_prev, _omp_bom_out, _additional_data_json);
		_array_of_omp_all_bom =  array_append(_array_of_omp_all_bom,_omp_bom_out);
	end if;

	 -- Finally populates the omp tables
	  if cardinality(_array_of_omp_process) > 0 then 
	   v_ret = nifi_md_sid_legacy_hdr_to_omp_tables_fn(_array_of_omp_process, _array_of_omp_operation, _array_of_omp_all_bom, _array_of_all_omp_rate );
	   return v_ret;
	  else 
	   return false;
	  end if;
	  
	 else
	  return false;	  
    end if; 
  END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_omp_fn(integer)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_omp_fn(integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_omp_fn(integer) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_omp_fn(integer) TO omp_rw_role;

