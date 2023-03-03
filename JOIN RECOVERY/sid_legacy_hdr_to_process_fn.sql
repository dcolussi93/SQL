-- FUNCTION: omp_owner.nifi_md_sid_legacy_hdr_to_process_fn(nifi_md_sid_legacy_hdr_sintetizado, json, json, jsonb)

-- DROP FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_process_fn(nifi_md_sid_legacy_hdr_sintetizado, json, json, jsonb);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_process_fn(
	_legacy_hdr_record nifi_md_sid_legacy_hdr_sintetizado,
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
   _processid_postfix varchar default null;
   
   v_process_number numeric;
   
   begin 
            -- reads jsons parameter into a record
			select * from json_to_record(_additional_data_json) as x(productid text,  tipo_hdr text, demandid text,recovery_order text, productsteelcode text, prodtypecode text, couplingid text, lengthref numeric, hollowid text ,accessorieshollowid text,omptype text ,unitofmeasure text,protpin text,protbox text,protpinalt text,protboxalt text,protpininternal text,protboxinternal text,
			                                                         out_of_route_id numeric, out_of_route_inputstatus text, out_of_operation_id text, last_output_status text )
		    into _additional_data_record where productid is not null;
			
			
			select * from json_to_record(_additional_alternativas_laminacion_json) as x(centername text, rollingpriority text,steelcode text, outputsections numeric, length numeric, minimumlength numeric, maximumlength numeric, billlength numeric, billdiameter numeric, mediumtrim numeric, endtrim numeric, peaktrim numeric, lengthatdriller numeric, lengthatextractor numeric, diameteratextractor numeric, lengthatgauge numeric, finaltubelength numeric, manufacturinglength numeric, minimummanufacturinglength numeric, maximummanufacturinglength numeric, remaininglength numeric, mandreldiameter numeric, caliber numeric, calibration numeric, endtrimatcontinuous numeric, peaktrimatcontinuous numeric, endtrimatlre numeric, peaktrimatlre numeric, wtatextractor numeric, rollingwt numeric, manufacturingwt numeric)
			into _additional_alternativas_laminacion_record;
			-- end reads json parameters												
			
			_omp_process.hdr_raw_id = _legacy_hdr_record.id;
			_omp_process.omptype = _additional_data_record.omptype;
			
			if nullif(_additional_data_record.last_output_status,'') is null then
				_omp_process.productid = _legacy_hdr_record.productid;
			 else
				_omp_process.productid = concat(_legacy_hdr_record.productid,'_',lpad(_additional_data_record.last_output_status,2,'0'));
			end if;
			
			v_process_number = nifi_md_sid_legacy_hdr_get_omp_process_number_fn(_omp_process.productid, nullif(concat(_additional_data_record.demandid,'_' || _additional_data_record.recovery_order),''), _route_path);
			
			-- _omp_process.rollingalternative = _legacy_hdr_record.rollingalternative; 
		    _omp_process.rollingalternative = concat(case when (_legacy_hdr_record.rollingalternative ~ '^([0-9]+[.]?[0-9]*|[.][0-9]+)$') is true then '2' else '1' end,_legacy_hdr_record.rollingalternative);
			
			if ( _additional_data_record.out_of_route_id is not null) then
			 _processid_postfix = concat('_',lpad(_additional_data_record.out_of_route_inputstatus,2,'0'),'_',lpad(_additional_data_record.out_of_operation_id,2,'0'),'_','REC');
			end if; 
			
			if ( _additional_data_record.tipo_hdr = 'RECOVERYSTD')
			 then
			    _omp_process.processid = concat('PRO',case when _additional_data_record.omptype not in ('Couplings') then concat('_',_omp_process.rollingalternative) else null end,'_',lpad(v_process_number::text,2,'0'),'_',_omp_process.productid,'_', _additional_data_record.demandid, '_', _additional_data_record.recovery_order, '_REC');
				_omp_process.recoveryprocess = 1;
				_omp_process.preference = 100;
			 else
				_omp_process.processid = concat('PRO',case when _additional_data_record.omptype not in ('Couplings') then concat('_',_omp_process.rollingalternative) else null end,'_',lpad(v_process_number::text,2,'0'),'_',_omp_process.productid,case when ( _additional_data_record.demandid is null or _additional_data_record.omptype in ('Couplings') ) then null else concat('_',_additional_data_record.demandid) end, _processid_postfix );
				_omp_process.preference = _legacy_hdr_record.processid;
			end if;	
			    		
			_omp_process.label= _omp_process.processid; 			
			_omp_process.locationid = 'SIDERCA';

			if _additional_data_record.omptype in ('Couplings') then 
			  _omp_process.demandid = null;
			 else 
			  _omp_process.demandid = nullif(_additional_data_record.demandid,'');			 
			end if;
			 
			--_omp_process.alternativeroute = _legacy_hdr_record.processid;
			_omp_process.alternativeroute = v_process_number;
			
			_omp_process.lengthref = _additional_data_record.lengthref;  --select="omp_operations/omp_operation[last()]/gdm_boms/bom[last()]/LENGTHOUT"
			--
			_omp_process.steelcodeid = _additional_data_record.productsteelcode; --product.PRODUCTSTEELCODE -- o COALESCE(NULLIF('<xsl:value-of select="lacos/SteelCodeId"/>',''),NULLIF('<xsl:value-of select="$productSteelCode"/>','')) STEELCODEID
			
			-- Comienzan los campos que utilizan la info de las alternativas - solo tubos en gral.
			case 
				when _additional_alternativas_laminacion_record.centername = 'LAMI' then								
					
					if ( _additional_alternativas_laminacion_record.maximummanufacturinglength / 1000 ) < _additional_alternativas_laminacion_record.finaltubelength then
							_omp_process.tubelength = _additional_alternativas_laminacion_record.maximummanufacturinglength / 1000;
						else
							_omp_process.tubelength = _additional_alternativas_laminacion_record.finaltubelength;
					end if;
					
					_omp_process.billetdiameter = _additional_alternativas_laminacion_record.billdiameter; 	--lacos			
					_omp_process.billetlength = _additional_alternativas_laminacion_record.billlength; 		--lacos
					_omp_process.caliber = _additional_alternativas_laminacion_record.caliber;
					_omp_process.lengthafterpiercing = _additional_alternativas_laminacion_record.lengthatdriller; --lacos
					_omp_process.lengthafterrolling = _additional_alternativas_laminacion_record.lengthatextractor;  --lacos
					_omp_process.lengthaftersizing = _additional_alternativas_laminacion_record.manufacturinglength;   --lacos
					_omp_process.tubesperbillet = _additional_alternativas_laminacion_record.outputsections;
					_omp_process.cropafterrolling = coalesce(_additional_alternativas_laminacion_record.endtrimatcontinuous,0) + coalesce(_additional_alternativas_laminacion_record.peaktrimatcontinuous,0);
					_omp_process.cropaftersizing = _additional_alternativas_laminacion_record.endtrimatlre + _additional_alternativas_laminacion_record.peaktrimatlre;
					_omp_process.discardlength = _additional_alternativas_laminacion_record.remaininglength;
					_omp_process.minpipelength = _additional_alternativas_laminacion_record.minimummanufacturinglength / 1000;        --lacos
					_omp_process.maxpipelength = _additional_alternativas_laminacion_record.maximummanufacturinglength / 1000;
					_omp_process.mandrel = to_char(_additional_alternativas_laminacion_record.mandreldiameter,'FM999.0');
					_omp_process.seriesid = _additional_alternativas_laminacion_record.calibration; 
					_omp_process.rollingwt = _additional_alternativas_laminacion_record.manufacturingwt;					
				
				when _additional_alternativas_laminacion_record.centername = 'LAC2' then				
					
					if ( _additional_alternativas_laminacion_record.outputsections is null or _additional_alternativas_laminacion_record.lengthatgauge is null ) then  
							_omp_process.tubelength = _additional_alternativas_laminacion_record.finaltubelength;
						else
							_omp_process.tubelength = _additional_alternativas_laminacion_record.lengthatgauge * 0.99 / _additional_alternativas_laminacion_record.outputsections;
					end if;
					
					_omp_process.billetdiameter = _additional_alternativas_laminacion_record.billdiameter; 	--lacos			
					_omp_process.billetlength = _additional_alternativas_laminacion_record.billlength; 		--lacos
					_omp_process.caliber = _additional_alternativas_laminacion_record.caliber;
					_omp_process.lengthafterpiercing = _additional_alternativas_laminacion_record.lengthatdriller; --lacos
					_omp_process.lengthafterrolling = _additional_alternativas_laminacion_record.lengthatextractor;  --lacos
					_omp_process.lengthaftersizing = _additional_alternativas_laminacion_record.lengthatgauge;   --lacos
					_omp_process.tubesperbillet = _additional_alternativas_laminacion_record.outputsections;
					
					if (_additional_alternativas_laminacion_record.wtatextractor < 13 ) then					
						_omp_process.cropafterrolling = _additional_alternativas_laminacion_record.diameteratextractor / 1000;
					  else
					     _omp_process.cropafterrolling = 0.00;
					end if;
					 
					_omp_process.cropaftersizing = 0.00;
					_omp_process.discardlength = 0.00;
					
					_omp_process.minpipelength = coalesce(_additional_alternativas_laminacion_record.minimumlength,0) + ( ( coalesce(_additional_alternativas_laminacion_record.peaktrim,0) + coalesce(_additional_alternativas_laminacion_record.endtrim,0) + ( coalesce(_additional_alternativas_laminacion_record.mediumtrim,0) * coalesce(_additional_alternativas_laminacion_record.outputsections,0) ) ) / coalesce(_additional_alternativas_laminacion_record.outputsections,0));
					
					 if _additional_data_record.prodtypecode in ('0608','0641','0642','0643','0644','0645') then
					    _omp_process.maxpipelength = 14.5;
					  else
					     _omp_process.maxpipelength = coalesce(_additional_alternativas_laminacion_record.maximumlength,0) + ( ( coalesce(_additional_alternativas_laminacion_record.peaktrim,0) + coalesce(_additional_alternativas_laminacion_record.endtrim,0) + ( coalesce(_additional_alternativas_laminacion_record.mediumtrim,0) * coalesce(_additional_alternativas_laminacion_record.outputsections,0) ) ) / coalesce(_additional_alternativas_laminacion_record.outputsections,0));
					 end if;
					 
					_omp_process.mandrel = to_char(_additional_alternativas_laminacion_record.mandreldiameter,'FM999.0');
					_omp_process.seriesid = _additional_alternativas_laminacion_record.calibration; 
					_omp_process.rollingwt = _additional_alternativas_laminacion_record.rollingwt;					
				
				else
				 if _legacy_hdr_record.productivecentername in ('CC2','CC3') -- - Special route for Steel Bars ( SOLO COLADO )
					then
					 _omp_process.billetlength = _additional_data_record.lengthref;
					 _omp_process.tubelength = _additional_data_record.lengthref;
					 _omp_process.tubesperbillet = 1;
					 _omp_process.rollingalternative = null;					 
				 end if;
				 
			end case;									
			
			--_omp_process.lastlegacyupdate = now();
			_omp_process.lastlegacyupdate = _legacy_hdr_record.lastlegacyupdate;
			
    return _omp_process;
  
  END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_process_fn(nifi_md_sid_legacy_hdr_sintetizado, json, json, jsonb)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_process_fn(nifi_md_sid_legacy_hdr_sintetizado, json, json, jsonb) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_process_fn(nifi_md_sid_legacy_hdr_sintetizado, json, json, jsonb) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_to_process_fn(nifi_md_sid_legacy_hdr_sintetizado, json, json, jsonb) TO omp_rw_role;

