-- FUNCTION: omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_recovery_fn(integer)

-- DROP FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_recovery_fn(integer);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_recovery_fn(
	p_raw_header_id integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
DECLARE

    v_id 			int4;
	v_xml           xml;	
	v_ROrder		character varying;
	v_DemandId		character varying;
	v_OpSequence	character varying;
	v_ProductId		character varying;
	--v_TipoHRR		character varying;
    v_tabla_reg  	nifi_md_sid_legacy_hdr_raw%ROWTYPE;
    
	l_context        text;
    r RECORD;

BEGIN
	
	select id, payload from nifi_md_sid_legacy_hdr_raw_header where id = p_raw_header_id into v_id, v_xml;
	
    --v_xml := p_xml;
	
	--v_TipoHRR := 'RECOVERY';
	
			/*EN ESTA PARTE PONES LOS TAGS DEL XML DEL NODO CABECERA*/
			WITH x AS (SELECT v_xml AS t )

			SELECT  nullif(btrim(xpath('ORDERIT/text()', node)::text,'{}'),''),-- AS Orderit,
					nullif(btrim(xpath('DEMANDID/text()', node)::text,'{}'),''),-- AS Demandid,
					nullif(btrim(xpath('OPERATIVESEQUENCE/text()', node)::text,'{}'),''),-- AS Operativesequence,
					nullif(btrim(xpath('PRODUCTID/text()', node)::text,'{}'),'')-- AS Productid
			INTO STRICT v_ROrder, v_DemandId, v_OpSequence, v_ProductId
			FROM (SELECT unnest(xpath('/Envelope/Body/SIN033WS/SIA033WS-IN', t)) AS node FROM x) sub;

    FOR r IN (WITH x AS (SELECT
                            v_xml AS t
                          )
         SELECT nullif(btrim(xpath('ROUTEALTERNATIVE/text()',node)::text,'{}'),'') AS Routealternative,         		
				nullif(btrim(xpath('SEQUENCE/text()', node)::text,'{}'),'') AS Secuencia,
			  	nullif(btrim(xpath('PRODUCTIVECENTER/text()', node)::text,'{}'),'') AS Productivecenter,
			  	nullif(btrim(xpath('OPERATIVECENTERDESCRIPTION/text()', node)::text,'{}'),'') AS Productivecenterdesc,
			  	nullif(btrim(xpath('OPERATION/text()', node)::text,'{}'),'') AS Operation,
			    nullif(btrim(xpath('OPERATIONNAME/text()', node)::text,'{}'),'') AS Operationname,
			    nullif(btrim(xpath('OUTPUTSTATUS/text()', node)::text,'{}'),'') AS Outputstatus,
			    nullif(btrim(xpath('INPUTSTATUS/text()', node)::text,'{}'),'') AS Inputstatus,
			  	nullif(btrim(xpath('AVERAGEPIECESPERSHIFT/text()', node)::text,'{}'),'') AS Piecepershift,
			    nullif(btrim(xpath('UTILIZATION/text()', node)::text,'{}'),'') AS Utilization,
			    nullif(btrim(xpath('NONREWORKABLEREJECTIONRATE/text()', node)::text,'{}'),'') AS Scrapyield,
			    ----
			  	nullif(btrim(xpath('INPUTLENGTH/text()',node)::text,'{}'),'') AS Inputlength,
			  	nullif(btrim(xpath('OUTPUTLENGTH/text()',node)::text,'{}'),'') AS Outputlength,				
			  	nullif(btrim(xpath('OUTPUTSECTIONS/text()',node)::text,'{}'),'') AS Outputsections,					 
			  	----
				nullif(btrim(xpath('INLINEREWORKABLEMATERIALRATE/text()',node)::text,'{}'),'') AS Inlinereworkablematerialrate,
				nullif(btrim(xpath('OFFLINEREWORKABLEMATERIALRATE/text()',node)::text,'{}'),'') AS Offlinereworkablematerialrate,
				nullif(btrim(xpath('PRODUCTIVITY/text()',node)::text,'{}'),'') AS Productivity,
				nullif(btrim(xpath('PRODUCTIVITYUPDATECOEFFICIENT/text()',node)::text,'{}'),'') AS Productivityupdatecoefficient,				 					 					 
				nullif(btrim(xpath('PRODUCTIVITYIDENTIFIER/text()',node)::text,'{}'),'') AS Productivityidentifier,
				nullif(btrim(xpath('UTILIZATIONIDENTIFIER/text()',node)::text,'{}'),'') AS Utilizationidentifier,
				nullif(btrim(xpath('REWORKABLEIDENTIFIER/text()',node)::text,'{}'),'') AS Reworkableidentifier
			  
		 FROM (SELECT unnest(xpath('/Envelope/Body/SIN033WS/SIA033WS-IN/HRRDETAILSs/HRRDETAILS', t)) AS node FROM x) sub)
      
	  LOOP

        v_tabla_reg.processid := 1;
		v_tabla_reg.productid := v_ProductId;
		v_tabla_reg.rollingalternative := '0';
		v_tabla_reg.routealternative := r.Routealternative;
        v_tabla_reg.operativesequence := v_OpSequence;
		v_tabla_reg.rollingmill := '0';
		v_tabla_reg.step := r.Secuencia::numeric;
		v_tabla_reg.operation := r.Operation;
		v_tabla_reg.operationname := r.Operationname;
		v_tabla_reg.productivecenter := r.Productivecenter;
		v_tabla_reg.productivecentername := r.Productivecenterdesc;
		v_tabla_reg.inputstatus := r.Inputstatus;
		v_tabla_reg.outputstatus := r.Outputstatus;
		v_tabla_reg.outputsections := 1;
		v_tabla_reg.piecespershiftprogrammingrate := r.Piecepershift;
		v_tabla_reg.scrapyield := r.Scrapyield;
		v_tabla_reg.utilization := r.Utilization;
		--v_tabla_reg.inputlength := r.Inputlength;
		--v_tabla_reg.outputlength := r.Outputlength;
		--v_tabla_reg.outputsections := r.Outputsections;
		
		INSERT INTO nifi_md_sid_legacy_hdr_raw
		(id,
		seqgroup,
		processid,
		productid,
		rollingalternative,
		routealternative,
        operativesequence,
		rollingmill,
		step,
		operation,
		operationname,
		productivecenter,
		productivecentername,
		inputstatus,
		outputstatus,
		--outsections,
		piecespershiftprogrammingrate,
		scrapyield,
		utilization,
		--inputlength,
		--outputlength,
		--outputsections,
		processingdate,
		lastlegacyupdate)
		VALUES
		(
		v_id,
		'RECUPERO',
		v_tabla_reg.processid,
		v_tabla_reg.productid,
		v_tabla_reg.rollingalternative,
		v_tabla_reg.routealternative,
        v_tabla_reg.operativesequence,
		v_tabla_reg.rollingmill,
		v_tabla_reg.step,
		v_tabla_reg.operation,
		v_tabla_reg.operationname,
		v_tabla_reg.productivecenter,
		v_tabla_reg.productivecentername,
		v_tabla_reg.inputstatus,
		v_tabla_reg.outputstatus,
		--v_tabla_reg.outsections,
		v_tabla_reg.piecespershiftprogrammingrate,
		v_tabla_reg.scrapyield,
		v_tabla_reg.utilization,
		--v_tabla_reg.inputlength,
		--v_tabla_reg.outputlength,
		--v_tabla_reg.outputsections,
		now(),
		now());
  END LOOP;
  
  return true;
  EXCEPTION
        WHEN OTHERS THEN
 			GET STACKED DIAGNOSTICS l_context = PG_EXCEPTION_CONTEXT;
    		RAISE EXCEPTION 'ERROR wf_workflow_mngmnt_sp:% SQLSTATE:% SQLERRM: %', l_context, sqlstate, sqlerrm;
  END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_recovery_fn(integer)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_recovery_fn(integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_recovery_fn(integer) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_recovery_fn(integer) TO omp_rw_role;

