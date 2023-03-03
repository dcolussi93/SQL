-- FUNCTION: omp_owner.nifi_md_tam_legacy_hdr_to_process_fn(nifi_md_tam_legacy_hdr_sintetizado, json, json, jsonb)

-- DROP FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_process_fn(nifi_md_tam_legacy_hdr_sintetizado, json, json, jsonb);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_process_fn(
	_legacy_hdr_record nifi_md_tam_legacy_hdr_sintetizado,
	_additional_data_json json,
	_additional_alternativas_laminacion_json json,
	_route_path jsonb)
    RETURNS nifi_md_omp_process
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
declare  
	 declare
	 				
   _omp_process nifi_md_omp_process%rowtype;
   
   _additional_data_record record;
   _additional_alternativas_laminacion_record record;
   
   v_process_number numeric;
   
   begin 
            -- reads jsons parameter into a record
			select * from json_to_record(_additional_data_json) as x(productid text,  tipo_hdr text, demandid text,recovery_order text, productsteelcode text, prodtypecode text, couplingid text, lengthref numeric, hollowid text ,accessorieshollowid text,omptype text ,unitofmeasure text,protpin text,protbox text,protpinalt text,protboxalt text,protpininternal text,protboxinternal text, last_output_status text, n_rgo_desde numeric, n_rgo_hasta numeric, c_tia text, c_tia_cupla text, is_tia boolean, productid_tia text)
		    into _additional_data_record where productid is not null;
			
			select * from json_to_record(_additional_alternativas_laminacion_json) as x(centername text, rollingpriority text,steelcode text, outputsections numeric, length numeric, minimumlength numeric, maximumlength numeric, billlength numeric, billdiameter numeric, mediumtrim numeric, endtrim numeric, peaktrim numeric, lengthatdriller numeric, lengthatextractor numeric, diameteratextractor numeric, lengthatgauge numeric, finaltubelength numeric, manufacturinglength numeric, minimummanufacturinglength numeric, maximummanufacturinglength numeric, remaininglength numeric, mandreldiameter numeric, caliber numeric, calibration text, endtrimatcontinuous numeric, peaktrimatcontinuous numeric, endtrimatlre numeric, peaktrimatlre numeric, wtatextractor numeric, rollingwt numeric, manufacturingwt numeric, lengthaftersizing numeric, p_n_long_mpm_1_o_2 numeric, n_long_sal_pqf numeric, p_n_desp_laco_p1_o_2 numeric, n_long_desp_punta numeric, n_long_lamina numeric, n_tubos_esbozo numeric, n_long_bme numeric, p_long_esb_util_lp numeric, n_despunte_cex  numeric, p_n_desp_laco_c1_o_2  numeric, n_long_desp_cola  numeric, p_long_sal_calib_lp numeric, p_n_long_term_1_o_2 numeric,p_rgo_calculo_lp numeric, p_rango_calculo_1_o_2 numeric, r_esbozo_tocho numeric, n_long_em numeric)
			into _additional_alternativas_laminacion_record;
			-- end reads json parameters												
			
			_omp_process.hdr_raw_id = _legacy_hdr_record.id;
			_omp_process.omptype = _additional_data_record.omptype;
			_omp_process.process_type = _additional_data_record.tipo_hdr;
			
			if nullif(_additional_data_record.last_output_status,'') is null then
			   if _additional_data_record.is_tia is true then
			     _omp_process.productid = _additional_data_record.productid_tia;
			     else
				_omp_process.productid = _legacy_hdr_record.productid;
			   end if;	
			 else
				_omp_process.productid = concat(_legacy_hdr_record.productid,'_',lpad(_additional_data_record.last_output_status,2,'0'));
			end if;
			
			v_process_number = nifi_md_tam_legacy_hdr_get_omp_process_number_fn(_omp_process.productid, nullif(concat(_additional_data_record.demandid,'_' || _additional_data_record.recovery_order),''), _route_path);
			_omp_process.rollingalternative = _legacy_hdr_record.rollingalternative; 
			
			if ( _additional_data_record.tipo_hdr = 'RECOVERYSTD')
			 then
			    _omp_process.processid = concat('PRO','_',lpad(v_process_number::text,2,'0'),case when _additional_data_record.omptype not in ('Couplings') then '_' || nullif(_omp_process.rollingalternative,'none') else null end,'_',_omp_process.productid,'_', _additional_data_record.demandid, '_', _additional_data_record.recovery_order, '_REC');
				_omp_process.recoveryprocess = 1;
			 else
				_omp_process.processid = concat('PRO','_',lpad(v_process_number::text,2,'0'),case when _additional_data_record.omptype not in ('Couplings') then '_' || nullif(_omp_process.rollingalternative,'none') else null end,'_',_omp_process.productid,case when ( _additional_data_record.demandid is null or _omp_process.omptype in ('Couplings') )then null else concat('_',_additional_data_record.demandid) end );				
			end if;	
			    		
			_omp_process.label= _omp_process.processid; 
			--_omp_process.productid = _legacy_hdr_record.productid; 
			_omp_process.locationid = 'TAMSA'; 
			
			if _omp_process.omptype in ('Couplings') then
			 _omp_process.demandid = null;
			 else
			 _omp_process.demandid = nullif(_additional_data_record.demandid,'');
			end if;
			
			_omp_process.preference = _legacy_hdr_record.processid; 
			_omp_process.alternativeroute = v_process_number;
			
			--
			_omp_process.steelcodeid = _additional_data_record.productsteelcode; --product.PRODUCTSTEELCODE -- o COALESCE(NULLIF('<xsl:value-of select="lacos/SteelCodeId"/>',''),NULLIF('<xsl:value-of select="$productSteelCode"/>','')) STEELCODEID
			-- Comienzan los campos que utilizan la info de las alternativas - solo tubos en gral.

			case 
				when _additional_alternativas_laminacion_record.centername = 'LACO' then
			
					--_omp_process.rollingalternative = concat('1',_omp_process.rollingalternative);
					
					--if ( _additional_alternativas_laminacion_record.maximummanufacturinglength / 1000 ) < _additional_alternativas_laminacion_record.finaltubelength then
					--		_omp_process.tubelength = _additional_alternativas_laminacion_record.maximummanufacturinglength / 1000;
					--	else
					_omp_process.lengthref = _additional_data_record.lengthref;  --select="omp_operations/omp_operation[last()]/gdm_boms/bom[last()]/LENGTHOUT"
					--_omp_process.tubelength = _additional_data_record.lengthref;
					--_omp_process.tubelength =  _additional_alternativas_laminacion_record.p_n_long_term_1_o_2;
					_omp_process.tubelength =  omp_owner.nifi_md_tam_laco_long_salida_balanza_fn(_additional_data_record.productid::character varying);
					--_omp_process.length = _additional_alternativas_laminacion_record.p_n_long_term_1_o_2;
					_omp_process.length = omp_owner.nifi_md_tam_laco_long_salida_balanza_fn(_additional_data_record.productid::character varying);
					--end if;
					
					_omp_process.billetdiameter = _additional_alternativas_laminacion_record.billdiameter; 	--lacos			
					_omp_process.billetlength = _additional_alternativas_laminacion_record.billlength; 		--lacos
					_omp_process.caliber = _additional_alternativas_laminacion_record.caliber;
					_omp_process.lengthafterpiercing = _additional_alternativas_laminacion_record.lengthatdriller; --lacos
					_omp_process.lengthafterrolling = _additional_alternativas_laminacion_record.p_n_long_mpm_1_o_2;  --lacos
					_omp_process.lengthaftersizing = _additional_alternativas_laminacion_record.lengthaftersizing;   --lacos
					_omp_process.tubesperbillet = _additional_alternativas_laminacion_record.outputsections;
					_omp_process.cropafterrolling = coalesce(_additional_alternativas_laminacion_record.endtrimatcontinuous,0) + coalesce(_additional_alternativas_laminacion_record.peaktrimatcontinuous,0);
					_omp_process.cropaftersizing = coalesce(_additional_alternativas_laminacion_record.p_n_desp_laco_p1_o_2,0) + coalesce(_additional_alternativas_laminacion_record.p_n_desp_laco_c1_o_2,0);
					_omp_process.discardlength = _additional_alternativas_laminacion_record.remaininglength;
					-- _omp_process.minpipelength = coalesce(_additional_alternativas_laminacion_record.minimumlength,0);
					-- _omp_process.maxpipelength = coalesce(_additional_alternativas_laminacion_record.maximumlength,0);
					--_omp_process.minpipelength = _additional_data_record.n_rgo_desde + coalesce(( _additional_alternativas_laminacion_record.p_n_long_term_1_o_2 - _additional_alternativas_laminacion_record.p_rango_calculo_1_o_2 ),0);
					--_omp_process.maxpipelength = _additional_data_record.n_rgo_hasta + coalesce(( _additional_alternativas_laminacion_record.p_n_long_term_1_o_2 - _additional_alternativas_laminacion_record.p_rango_calculo_1_o_2 ),0);
					_omp_process.minpipelength = _additional_data_record.n_rgo_desde + coalesce(( _additional_alternativas_laminacion_record.p_n_long_term_1_o_2 - omp_owner.nifi_md_tam_laco_long_salida_balanza_fn(_additional_data_record.productid::character varying) ),0);
					_omp_process.maxpipelength = _additional_data_record.n_rgo_hasta + coalesce(( _additional_alternativas_laminacion_record.p_n_long_term_1_o_2 - omp_owner.nifi_md_tam_laco_long_salida_balanza_fn(_additional_data_record.productid::character varying) ),0);
					_omp_process.mandrel = _additional_alternativas_laminacion_record.mandreldiameter || '_' || _additional_alternativas_laminacion_record.caliber;
					_omp_process.seriesid = _additional_alternativas_laminacion_record.calibration; 
					_omp_process.rollingwt = _additional_alternativas_laminacion_record.manufacturingwt;					
				
				when _additional_alternativas_laminacion_record.centername in ( 'LAC3','PQF3') then

					--_omp_process.rollingalternative = concat('2',_omp_process.rollingalternative);
					
					--if ( _additional_alternativas_laminacion_record.outputsections is null or _additional_alternativas_laminacion_record.lengthatgauge is null ) then  
					_omp_process.tubelength = _additional_data_record.lengthref;
					_omp_process.length = _additional_alternativas_laminacion_record.n_long_bme;
					_omp_process.lengthref = _additional_data_record.lengthref;  --select="omp_operations/omp_operation[last()]/gdm_boms/bom[last()]/LENGTHOUT"					
					--	else
					--		_omp_process.tubelength = _additional_alternativas_laminacion_record.lengthatgauge * 0.99 / _additional_alternativas_laminacion_record.outputsections;
					--end if;
					
					_omp_process.billetdiameter = _additional_alternativas_laminacion_record.billdiameter; 	--lacos			
					_omp_process.billetlength = _additional_alternativas_laminacion_record.billlength; 		--lacos
					_omp_process.caliber = _additional_alternativas_laminacion_record.caliber;
					_omp_process.lengthafterpiercing = _additional_alternativas_laminacion_record.lengthatdriller; --lacos
					_omp_process.lengthafterrolling = _additional_alternativas_laminacion_record.n_long_em;  --lacos
					_omp_process.lengthaftersizing = _additional_alternativas_laminacion_record.lengthaftersizing;   --lacos
					_omp_process.tubesperbillet = _additional_alternativas_laminacion_record.n_tubos_esbozo;
					_omp_process.cropafterrolling = _additional_alternativas_laminacion_record.n_despunte_cex;
					_omp_process.cropaftersizing = coalesce(_additional_alternativas_laminacion_record.n_long_desp_punta,0) + coalesce(_additional_alternativas_laminacion_record.n_long_desp_cola,0);
					_omp_process.discardlength = 0.00;
					
					--_omp_process.minpipelength = coalesce(_additional_alternativas_laminacion_record.minimumlength,0) + ( ( coalesce(_additional_alternativas_laminacion_record.peaktrim,0) + coalesce(_additional_alternativas_laminacion_record.endtrim,0) + ( coalesce(_additional_alternativas_laminacion_record.mediumtrim,0) * coalesce(_additional_alternativas_laminacion_record.outputsections,0) ) ) / coalesce(_additional_alternativas_laminacion_record.outputsections,0));
					_omp_process.minpipelength = coalesce(_additional_alternativas_laminacion_record.minimumlength,0);
					--if _additional_data_record.prodtypecode in ('0608','0641','0642','0643','0644','0645') then
					--  _omp_process.maxpipelength = 14.5;
					--else
					--	_omp_process.maxpipelength = coalesce(_additional_alternativas_laminacion_record.maximumlength,0) + ( ( coalesce(_additional_alternativas_laminacion_record.peaktrim,0) + coalesce(_additional_alternativas_laminacion_record.endtrim,0) + ( coalesce(_additional_alternativas_laminacion_record.mediumtrim,0) * coalesce(_additional_alternativas_laminacion_record.outputsections,0) ) ) / coalesce(_additional_alternativas_laminacion_record.outputsections,0));
					--end if;
					_omp_process.maxpipelength = coalesce(_additional_alternativas_laminacion_record.maximumlength,0);			
					_omp_process.mandrel = _additional_alternativas_laminacion_record.mandreldiameter || '_' || _additional_alternativas_laminacion_record.caliber;
					_omp_process.seriesid = _additional_alternativas_laminacion_record.calibration; 
					_omp_process.rollingwt = _additional_alternativas_laminacion_record.rollingwt;					

				when _additional_alternativas_laminacion_record.centername = 'LAPE' then

					--_omp_process.rollingalternative = concat('2',_omp_process.rollingalternative);
					
					--if ( _additional_alternativas_laminacion_record.outputsections is null or _additional_alternativas_laminacion_record.lengthatgauge is null ) then  
					_omp_process.tubelength = _additional_alternativas_laminacion_record.p_long_sal_calib_lp;
					_omp_process.length = _additional_alternativas_laminacion_record.p_long_sal_calib_lp;
					_omp_process.lengthref = _additional_alternativas_laminacion_record.p_rgo_calculo_lp;  --select="omp_operations/omp_operation[last()]/gdm_boms/bom[last()]/LENGTHOUT"
					--	else
					--		_omp_process.tubelength = _additional_alternativas_laminacion_record.lengthatgauge * 0.99 / _additional_alternativas_laminacion_record.outputsections;
					--end if;
					
					_omp_process.billetdiameter = _additional_alternativas_laminacion_record.billdiameter; 	--lacos			
					_omp_process.billetlength = _additional_alternativas_laminacion_record.billlength; 		--lacos
					_omp_process.caliber = _additional_alternativas_laminacion_record.caliber;
					_omp_process.lengthafterpiercing = _additional_alternativas_laminacion_record.lengthatdriller; --lacos
					_omp_process.lengthafterrolling = _additional_alternativas_laminacion_record.p_long_esb_util_lp;  --lacos
					_omp_process.lengthaftersizing = _additional_alternativas_laminacion_record.lengthaftersizing;   --lacos
					-- _omp_process.tubesperbillet = _additional_alternativas_laminacion_record.n_tubos_esbozo;
					_omp_process.tubesperbillet = _additional_alternativas_laminacion_record.r_esbozo_tocho;
					
					if (_additional_alternativas_laminacion_record.wtatextractor < 13 ) then					
						_omp_process.cropafterrolling = _additional_alternativas_laminacion_record.diameteratextractor / 1000;
					  else
					     _omp_process.cropafterrolling = 0.00;
					end if;
					 
					_omp_process.cropaftersizing = _additional_alternativas_laminacion_record.n_long_desp_punta;
					_omp_process.discardlength = 0.00;
					
					--_omp_process.minpipelength = coalesce(_additional_alternativas_laminacion_record.minimumlength,0) + ( ( coalesce(_additional_alternativas_laminacion_record.peaktrim,0) + coalesce(_additional_alternativas_laminacion_record.endtrim,0) + ( coalesce(_additional_alternativas_laminacion_record.mediumtrim,0) * coalesce(_additional_alternativas_laminacion_record.outputsections,0) ) ) / coalesce(_additional_alternativas_laminacion_record.outputsections,0));
					_omp_process.minpipelength = coalesce(_additional_alternativas_laminacion_record.minimumlength,0);
					--if _additional_data_record.prodtypecode in ('0608','0641','0642','0643','0644','0645') then
					--  _omp_process.maxpipelength = 14.5;
					--else
					--	_omp_process.maxpipelength = coalesce(_additional_alternativas_laminacion_record.maximumlength,0) + ( ( coalesce(_additional_alternativas_laminacion_record.peaktrim,0) + coalesce(_additional_alternativas_laminacion_record.endtrim,0) + ( coalesce(_additional_alternativas_laminacion_record.mediumtrim,0) * coalesce(_additional_alternativas_laminacion_record.outputsections,0) ) ) / coalesce(_additional_alternativas_laminacion_record.outputsections,0));
					--end if;
					_omp_process.maxpipelength = coalesce(_additional_alternativas_laminacion_record.maximumlength,0);			
					_omp_process.mandrel = _additional_alternativas_laminacion_record.mandreldiameter || '_' || _additional_alternativas_laminacion_record.caliber;
					_omp_process.seriesid = _additional_alternativas_laminacion_record.calibration; 
					_omp_process.rollingwt = _additional_alternativas_laminacion_record.rollingwt;

				else
				 null;
				 
			end case;									
			
			_omp_process.lastlegacyupdate = now();
			
    return _omp_process;
  
  END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_process_fn(nifi_md_tam_legacy_hdr_sintetizado, json, json, jsonb)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_process_fn(nifi_md_tam_legacy_hdr_sintetizado, json, json, jsonb) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_process_fn(nifi_md_tam_legacy_hdr_sintetizado, json, json, jsonb) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_process_fn(nifi_md_tam_legacy_hdr_sintetizado, json, json, jsonb) TO omp_rw_role;

