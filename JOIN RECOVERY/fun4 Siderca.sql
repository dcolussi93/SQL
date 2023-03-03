-- FUNCTION: omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_fn(integer)

-- DROP FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_fn(integer);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_fn(
	p_raw_header_id integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
declare
  
  v_id int4;
  v_productid varchar default null;
  v_ret boolean default false;
  v_routes_tag varchar default 'PRODUCTs';
  v_hdr_xml text default null;
  
  v_typeid varchar default null;
  v_current_hdr_count smallint default 0;
  v_out_of_route_id int4 default null;
  
  
 begin
	 
	select id, productid, payload from nifi_md_sid_legacy_hdr_raw_header where id = p_raw_header_id into v_id, v_productid, v_hdr_xml;
	
	select id from nifi_md_sid_legacy_hdr_raw_out_of_route_operation where id = p_raw_header_id into v_out_of_route_id;
	
	select left(prodtypecode,2) typeid from nifi_md_sid_legacy_hdr_producto where hdr_raw_id = -1 and productid = v_productid into v_typeid;
	
	INSERT INTO nifi_md_sid_legacy_hdr_raw
	 ( id, productid, rollingalternative, routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
	   operation, operationname, productivecenter, productivecentername, seqgroup, inputstatus, outputstatus,
	   piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
	   standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
	   upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate, lastlegacyupdate )		
    SELECT v_id id,
		   nullif(btrim(xpath('PRODUCTID/text()',hdr.operation)::text,'{}'),'') productid,
		   nullif(btrim(xpath('ROLLINGALTERNATIVE/text()',hdr.operation)::text,'{}'),'') rollingalternative,
		   nullif(btrim(xpath('ROUTEALTERNATIVE/text()',hdr.operation)::text,'{}'),'') routealternative,
		   -- start Header data: ORDER(-1)
           cast(nullif(btrim(xpath('OPERATIVESEQUENCE/text()',hdr_header.operation)::text,'{}'),'') as numeric) operativesequence,
		   nullif(btrim(xpath('ROLLINGMILL/text()',hdr_header.operation)::text,'{}'),'') rollingmill,
		   cast( nullif(btrim(xpath('PROCESSINGDATE/text()',hdr_header.operation)::text,'{}'),'') as date) processingdate,
		   nullif(btrim(xpath('COUPLINGASSEMBLYCENTER/text()',hdr_header.operation)::text,'{}'),'') couplingassemblycentercode,
		   -- end Header data
		   cast (nullif(btrim(xpath('ORDER/text()',hdr.operation)::text,'{}'),'') as integer) step,
		   nullif(btrim(xpath('OPERATION/text()',hdr.operation)::text,'{}'),'') operation,
   	       nullif(btrim(xpath('OPERATIONNAME/text()',hdr.operation)::text,'{}'),'') operationname,
		   nullif(btrim(xpath('PRODUCTIVECENTER/text()',hdr.operation)::text,'{}'),'') productivecenter,
		   nullif(btrim(xpath('PRODUCTIVECENTERNAME/text()',hdr.operation)::text,'{}'),'') productivecentername,
		   nullif(btrim(xpath('SEQGROUP/text()',hdr.operation)::text,'{}'),'') seqgroup,
		   nullif(btrim(xpath('INPUTSTATUS/text()',hdr.operation)::text,'{}'),'') inputstatus,
		   nullif(btrim(xpath('OUTPUTSTATUS/text()',hdr.operation)::text,'{}'),'') outputstatus,
		   cast(nullif(btrim(xpath('PIECESPERSHIFTPROGRAMMINGRATE/text()',hdr.operation)::text,'{}'),'') as numeric) piecespershiftprogrammingrate,
		   cast(nullif(btrim(xpath('INPUTDIAMETER/text()',hdr.operation)::text,'{}'),'') as numeric) inputdiameter,
		   cast(nullif(btrim(xpath('INPUTLENGTH/text()',hdr.operation)::text,'{}'),'') as numeric) inputlength,
		   cast(nullif(btrim(xpath('OUTPUTLENGTH/text()',hdr.operation)::text,'{}'),'') as numeric) outputlength,
		   cast(nullif(btrim(xpath('OUTPUTSECTIONS/text()',hdr.operation)::text,'{}'),'') as numeric) outputsections,
		   cast(nullif(btrim(xpath('SCRAPYIELD/text()',hdr.operation)::text,'{}'),'') as numeric) scrapyield,
		   cast(nullif(btrim(xpath('UTILIZATION/text()',hdr.operation)::text,'{}'),'') as numeric) utilization,
		   case 
			 when nullif(btrim(xpath('PRODUCTIVECENTER/text()',hdr.operation)::text,'{}'),'') = nullif(btrim(xpath('COUPLINGASSEMBLYCENTER/text()',hdr_header.operation)::text,'{}'),'')
			   then
			    true
			   else
			    false
		    end couplingassemblycenter,
		   cast(nullif(btrim(xpath('STANDARDPIECESPERSHIFTRATE/text()',hdr.operation)::text,'{}'),'') as numeric) standardpiecespershiftrate, 
		   cast(nullif(btrim(xpath('TEMPAUST1/text()',hdr.operation)::text,'{}'),'') as numeric) tempaust1, 
		   cast(nullif(btrim(xpath('TEMPAUST2/text()',hdr.operation)::text,'{}'),'') as numeric) tempaust2, 
		   cast(nullif(btrim(xpath('TEMPAUST3/text()',hdr.operation)::text,'{}'),'') as numeric) tempaust3, 
		   cast(nullif(btrim(xpath('TEMPREVE1/text()',hdr.operation)::text,'{}'),'') as numeric) tempreve1, 
		   cast(nullif(btrim(xpath('TEMPREVE2/text()',hdr.operation)::text,'{}'),'') as numeric) tempreve2, 
		   cast(nullif(btrim(xpath('TEMPREVE3/text()',hdr.operation)::text,'{}'),'') as numeric) tempreve3, 
		   cast(nullif(btrim(xpath('TEMPREVE4/text()',hdr.operation)::text,'{}'),'') as numeric) tempreve4, 
		   nullif(btrim(xpath('PUNZON1/text()',hdr.operation)::text,'{}'),'') upsetpunch1, 
		   nullif(btrim(xpath('PUNZON2/text()',hdr.operation)::text,'{}'),'') upsetpunch2, 
		   nullif(btrim(xpath('MATRIX1/text()',hdr.operation)::text,'{}'),'') upsetmatrix1, 
		   nullif(btrim(xpath('MATRIX2/text()',hdr.operation)::text,'{}'),'') upsetmatrix2,
		   cast(nullif(btrim(xpath('UPSETSPEC/text()',hdr.operation)::text,'{}'),'') as numeric) upsetspec,		   
		   cast( nullif(btrim(xpath('FROMDATE/text()',hdr.operation)::text,'{}'),'') as date) fromdate,
		   cast( nullif(btrim(xpath('UPTODATE/text()',hdr.operation)::text,'{}'),'') as date) uptodate,
		   --coalesce(TO_TIMESTAMP( nullif(btrim(xpath('INSERTDATE/text()',hdr.operation)::text,'{}'),'') || nullif(lpad(nullif(btrim(xpath('INSERTHOUR/text()',hdr.operation)::text,'{}'),''),4,'0'),''),'YYYYMMDDHH24MI'),now()) lastlegacyupdate
		   --coalesce(TO_TIMESTAMP( nullif(nullif(btrim(xpath('INSERTDATE/text()',hdr.operation)::text,'{}'),'0'),'') || nullif(lpad(nullif(btrim(xpath('INSERTHOUR/text()',hdr.operation)::text,'{}'),'0'),4,'0'),''),'YYYYMMDDHH24MI'),now()) lastlegacyupdate
		   coalesce(TO_TIMESTAMP( nullif(nullif(btrim(xpath('PROCESSINGDATETIME/text()',hdr.operation)::text,'{}'),'0'),''),'YYYYMMDDHH24MISS'),now()) lastlegacyupdate
	FROM ( SELECT unnest(xpath('//' || v_routes_tag || '/PRODUCT[ORDER>0]',v_hdr_xml::xml)) AS operation) hdr left outer join
	     ( SELECT unnest(xpath('//' || v_routes_tag || '/PRODUCT[ORDER=-1]',v_hdr_xml::xml)) AS operation ) hdr_header on
	     
	     	( nullif(btrim(xpath('PRODUCTID/text()',hdr.operation)::text,'{}'),'') = nullif(btrim(xpath('PRODUCTID/text()',hdr_header.operation)::text,'{}'),'')
	     	 and
	     	  nullif(btrim(xpath('ROLLINGALTERNATIVE/text()',hdr.operation)::text,'{}'),'')= nullif(btrim(xpath('ROLLINGALTERNATIVE/text()',hdr_header.operation)::text,'{}'),'') 
	     	 and 
	     	  nullif(btrim(xpath('ROUTEALTERNATIVE/text()',hdr.operation)::text,'{}'),'') = nullif(btrim(xpath('ROUTEALTERNATIVE/text()',hdr_header.operation)::text,'{}'),'')
	     	)
	WHERE current_date between cast( nullif(btrim(xpath('FROMDATE/text()',hdr.operation)::text,'{}'),'') as date) and cast( nullif(btrim(xpath('UPTODATE/text()',hdr.operation)::text,'{}'),'') as date) and 
	      current_date between cast( nullif(btrim(xpath('FROMDATE/text()',hdr_header.operation)::text,'{}'),'') as date) and cast( nullif(btrim(xpath('UPTODATE/text()',hdr_header.operation)::text,'{}'),'') as date) and
	      nullif(btrim(xpath('PRODUCTIVECENTERNAME/text()',hdr.operation)::text,'{}'),'') not in ('TOCH','PROL','LING','PROF','ALMA','APUC','ASAC','PRCU','PPTT')
	on CONFLICT ON CONSTRAINT nifi_md_sid_legacy_hdr_raw_pk do nothing;      
	
    -- Special route for Steel Bars
	if v_typeid in ('10','80') THEN
	  
	  update nifi_md_sid_legacy_hdr_raw
	  set operationname = 'COLADO',
	      productivecentername = case when inputdiameter in (148,170) then 'CC2' else 'CC3' end
	  where id = v_id and step = 1;	
	  
	  -- If necessary ( see "where" clausule bellow ) insert a new record for rate on CC2
	  insert into nifi_md_sid_legacy_hdr_raw
	  ( id, productid, rollingalternative, routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
	   operation, operationname, productivecenter, productivecentername, seqgroup, inputstatus, outputstatus,
	   piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
	   standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
	   upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate, lastlegacyupdate )
	   select 
		id, productid, rollingalternative, (routealternative::integer + 1)::varchar routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
		operation, operationname, productivecenter, 'CC2' productivecentername, seqgroup, inputstatus, outputstatus,
		piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
		standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
		upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate, lastlegacyupdate
	   from nifi_md_sid_legacy_hdr_raw
	   where id = v_id and step = 1 and inputdiameter = 215 and productivecentername = 'CC3';
	  -- End If necessary
	  
	  
	end if;
	-- end special route fir Steel Bars
	
	-- Load last hdr version for the corresponding product is case there are no current information in the IPP SYSTEM
	select count(id) from nifi_md_sid_legacy_hdr_raw where id = v_id into v_current_hdr_count;
	
	if v_current_hdr_count = 0 or v_current_hdr_count is null THEN
	
		INSERT INTO nifi_md_sid_legacy_hdr_raw
	    ( id, productid, rollingalternative, routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
	      operation, operationname, productivecenter, productivecentername, seqgroup, inputstatus, outputstatus,
	      piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
	      standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
	      upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate, lastlegacyupdate )
		SELECT v_id , productid, rollingalternative, routealternative, operativesequence, rollingmill, processingdate, couplingassemblycentercode, step, 
	      operation, operationname, productivecenter, productivecentername, seqgroup, inputstatus, outputstatus,
	      piecespershiftprogrammingrate, inputdiameter, inputlength, outputlength, outputsections, scrapyield, utilization, couplingassemblycenter,
	      standardpiecespershiftrate, tempaust1, tempaust2, tempaust3, tempreve1, tempreve2, tempreve3, tempreve4, upsetpunch1, 
	      upsetpunch2, upsetmatrix1, upsetmatrix2, upsetspec, fromdate, uptodate, lastlegacyupdate
		FROM nifi_md_sid_legacy_hdr_raw
		WHERE id = ( SELECT max(id) FROM nifi_md_sid_legacy_hdr_raw where productid = v_productid );
	
	end if;		
	-- End Load
	
	if (v_out_of_route_id is not null) then
	  v_ret = nifi_md_sid_legacy_hdr_raw_modify_with_out_of_route_fn(v_out_of_route_id);
	end if;
	
    return true;
  
  END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_fn(integer)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_fn(integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_fn(integer) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_sid_legacy_hdr_raw_upsert_fn(integer) TO omp_rw_role;

