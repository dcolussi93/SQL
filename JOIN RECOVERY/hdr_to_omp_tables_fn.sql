-- FUNCTION: omp_owner.nifi_md_tam_legacy_hdr_to_omp_tables_fn(nifi_md_omp_process[], nifi_md_omp_operation[], nifi_md_omp_bom[], nifi_md_omp_rate[])

-- DROP FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_tables_fn(nifi_md_omp_process[], nifi_md_omp_operation[], nifi_md_omp_bom[], nifi_md_omp_rate[]);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_tables_fn(
	_array_of_omp_process nifi_md_omp_process[],
	_array_of_omp_operation nifi_md_omp_operation[],
	_array_of_omp_bom nifi_md_omp_bom[],
	_array_of_omp_rate nifi_md_omp_rate[])
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
declare
    _omp_process nifi_md_omp_process;
	_v_array_of_omp_process nifi_md_omp_process[];
	
	v_omp_process_id int4 default null;
	
  begin
  	
	-- Processes insert
	DELETE FROM nifi_md_omp_process WHERE hdr_raw_id = _array_of_omp_process[1].hdr_raw_id;
	DELETE FROM nifi_md_omp_operation WHERE hdr_raw_id = _array_of_omp_process[1].hdr_raw_id;
	DELETE FROM nifi_md_omp_rate WHERE hdr_raw_id = _array_of_omp_process[1].hdr_raw_id;
	DELETE FROM nifi_md_omp_bom  WHERE hdr_raw_id = _array_of_omp_process[1].hdr_raw_id;
			
	FOREACH _omp_process IN ARRAY _array_of_omp_process
	LOOP
	
	    _v_array_of_omp_process[1] =  _omp_process;
		INSERT INTO nifi_md_omp_process ( hdr_raw_id, processid, label, productid, locationid, demandid, process_type, preference, alternativeroute, lengthref, length, steelcodeid, billetdiameter, rollingalternative, billetlength, caliber, lengthafterpiercing, lengthafterrolling, lengthaftersizing, tubesperbillet, tubelength, cropafterrolling, cropaftersizing, discardlength, minpipelength, maxpipelength, mandrel, seriesid, rollingwt, omptype, recoveryprocess, lastlegacyupdate )
		SELECT hdr_raw_id, processid, label, productid, locationid, demandid, process_type, preference, alternativeroute, lengthref, length, steelcodeid, billetdiameter, rollingalternative, billetlength, caliber, lengthafterpiercing, lengthafterrolling, lengthaftersizing, tubesperbillet, tubelength, cropafterrolling, cropaftersizing, discardlength, minpipelength, maxpipelength, mandrel, seriesid, rollingwt, omptype, recoveryprocess, lastlegacyupdate
		FROM unnest(_v_array_of_omp_process)
		RETURNING id into v_omp_process_id;
		
		-- Operations insert		
		
		INSERT INTO nifi_md_omp_operation ( id, hdr_raw_id, processid, operationid, "label", mainproductid, duration, locationid, prefmachineid, prefmachineid_dummy, quantity, seqnrinprocess, recoveryorderid, yield, utilization, tempaus1, tempaus2, tempaus3, temprev1, temprev2, temprev3, temprev4, punzon1, punzon2, matrix1, matrix2, upsetspec, productivity, inputlength, outputlength, outputsections, lastlegacyupdate )
		SELECT v_omp_process_id, hdr_raw_id, processid, operationid, "label", case when total_operations = operation_index then mainproductid else null end, duration, locationid, case when prefmachineid_dummy is true and prefmachineid not like '%DUMMY'then concat(prefmachineid,'_','DUMMY') else prefmachineid end, prefmachineid_dummy, quantity, seqnrinprocess, recoveryorderid, yield, utilization, tempaus1, tempaus2, tempaus3, temprev1, temprev2, temprev3, temprev4, punzon1, punzon2, matrix1, matrix2, upsetspec, productivity, inputlength, outputlength, outputsections, lastlegacyupdate
		FROM (
				SELECT count(*) over() total_operations, row_number() over() operation_index, *
				FROM unnest(_array_of_omp_operation)
				WHERE processid = _omp_process.processid ORDER BY seqnrinprocess ) ops;
				
		INSERT INTO nifi_md_omp_rate ( id, hdr_raw_id, processid, operationid, rateid, machineid, duration, quantity, utilization, tempaus1, tempaus2, tempaus3, temprev1, temprev2, temprev3, temprev4, punzon1, punzon2, matrix1, matrix2, upsetspec, lastlegacyupdate )
		SELECT v_omp_process_id, hdr_raw_id, processid, operationid, rateid, machineid, duration, quantity, utilization, tempaus1, tempaus2, tempaus3, temprev1, temprev2, temprev3, temprev4, punzon1, punzon2, matrix1, matrix2, upsetspec, lastlegacyupdate
		FROM  unnest(_array_of_omp_rate) where operationid is not null and processid = _omp_process.processid;
		
		INSERT INTO  nifi_md_omp_bom ( id, hdr_raw_id, processid, operationid, bomid, quantity, productid, billetlength, locationid, toolingcaliber, segments, lengthin, lengthout, inputstatus, mainproductbom, mainproductid, omptype, possiblemachines, machinepreference, lastlegacyupdate )
		SELECT v_omp_process_id, hdr_raw_id, processid, operationid, bomid, quantity, productid, billetlength, locationid, toolingcaliber, segments, lengthin, lengthout, inputstatus, mainproductbom, mainproductid, omptype, possiblemachines, machinepreference, lastlegacyupdate
		FROM  unnest(_array_of_omp_bom) where bomid is not null and processid = _omp_process.processid;
		
		
	END LOOP;
			
   return true;
  
  END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_tables_fn(nifi_md_omp_process[], nifi_md_omp_operation[], nifi_md_omp_bom[], nifi_md_omp_rate[])
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_tables_fn(nifi_md_omp_process[], nifi_md_omp_operation[], nifi_md_omp_bom[], nifi_md_omp_rate[]) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_tables_fn(nifi_md_omp_process[], nifi_md_omp_operation[], nifi_md_omp_bom[], nifi_md_omp_rate[]) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_to_omp_tables_fn(nifi_md_omp_process[], nifi_md_omp_operation[], nifi_md_omp_bom[], nifi_md_omp_rate[]) TO omp_rw_role;

