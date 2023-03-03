-- FUNCTION: omp_owner.nifi_md_tam_legacy_hdr_raw_header_upsert_fn(character varying, character varying, character varying, character varying, integer, character varying, numeric, text)

-- DROP FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_header_upsert_fn(character varying, character varying, character varying, character varying, integer, character varying, numeric, text);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_header_upsert_fn(
	p_productid character varying,
	p_demandid character varying,
	p_recovery_order character varying,
	p_tipo_hdr character varying,
	p_using_header_id integer,
	p_base_rollingalternative character varying,
	p_billetlength numeric,
	p_hdr_xml text)
    RETURNS TABLE(hdr_id integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
declare
  
  v_id int4 default null;
  v_ret boolean default false;
  v_xml_comment_dummy text;
  
  v_is_tia boolean default false;
  v_productid character varying default null;
  v_productid_tia character varying default null;
   
 begin
  if nullif(p_productid,'') is not null then
  
   if xml_is_well_formed(p_hdr_xml) or nullif(p_hdr_xml,'') is null then
   
    -- Block to detect if it is a TIA process request ( maybe should be passed as parameter )
    if p_productid like '0000%' then
	  v_is_tia = true;
	  -- search for the corresponding real product and sets v_productid
	  select c_prodto from nifi_md_tam_producto where c_tia = p_productid limit 1 into v_productid;	  
	  -- End search for the corresponding real product
	  v_productid_tia = p_productid;
	 else	  
	   v_is_tia = false;
	   v_productid = p_productid;
	   v_productid_tia = null;	   
	end if;
	-- End block to detect if it is TIA
	
	INSERT INTO nifi_md_tam_legacy_hdr_raw_header
	 ( tipo_hdr, productid, demandid, recovery_order, payload, is_tia, productid_tia, using_header_id, base_rollingalternative, billetlength )
	SELECT coalesce(nullif(p_tipo_hdr,''),'STANDARD'), v_productid, nullif(p_demandid,''), p_recovery_order, p_hdr_xml::xml, v_is_tia, v_productid_tia, p_using_header_id, coalesce(nullif(p_base_rollingalternative,''),'none'), p_billetlength
	returning id into v_id;
   
    else
	  v_xml_comment_dummy = xmlconcat(xmlcomment('bad formated xml'),p_hdr_xml::xml);
  end if;
  
  case nullif(p_tipo_hdr,'')
  when 'RECOVERY' then
  
    -- call for populating nifi_md_tam_legacy_hdr_raw from recovery hdrs.	
	v_ret = nifi_md_tam_legacy_hdr_raw_upsert_recovery_fn(v_id);
	-- call for joining recovery and standard product routes
	v_id = nifi_md_tam_legacy_hdr_raw_join_recovery_fn(v_id);
  
  when 'STANDARD' then   
    -- call for populating nifi_md_tam_legacy_hdr_raw
	v_ret = nifi_md_tam_legacy_hdr_raw_upsert_fn(v_id);
  else
	-- do nothing here
  end case;
  
  case nullif(p_tipo_hdr,'')
   when ( 'BCP' ) then
   -- llamar a nueva function para hacer la copia de las rutas
    v_ret = nifi_md_tam_bcp_omp_processes_fn(v_id);
   else 
  -- call for deletion of steps from raw table when product clone
  v_ret = nifi_md_tam_legacy_hdr_raw_delete_steps_for_clon_fn(v_id);
  -- call for populating nifi_md_tam_legacy_hdr_raw additionals fields ( procestam, route_path, etc )
  v_ret = nifi_md_tam_legacy_hdr_raw_upsert_additionals_fn(v_id);
  -- call for detecting Sintetizar
  v_ret = nifi_md_tam_legacy_hdr_sintetizado_fn(v_id);
  -- call for detecting dummies
  v_ret = nifi_md_tam_legacy_hdr_dummies_fn(v_id);
  -- call for populating inputstatus_omp
  v_ret = nifi_md_tam_legacy_hdr_set_inputstatus_omp_fn(v_id);
  
   end case;
  
  
  end if;
  
  return Query ( SELECT v_id as hdr_id where v_id is not null); 
 END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_header_upsert_fn(character varying, character varying, character varying, character varying, integer, character varying, numeric, text)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_header_upsert_fn(character varying, character varying, character varying, character varying, integer, character varying, numeric, text) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_header_upsert_fn(character varying, character varying, character varying, character varying, integer, character varying, numeric, text) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_header_upsert_fn(character varying, character varying, character varying, character varying, integer, character varying, numeric, text) TO omp_rw_role;

