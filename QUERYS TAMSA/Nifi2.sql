--select * from rmt_owner.rmt_outofroute_hdr order by outofroute_id desc  where outofroute_id = 2059
--select * from rmt_owner.rmt_template_operation_centers where template_id = 573
SELECT
  hdr.outofroute_id AS id_proceso,        -- prcntpos.id_proceso, 
    hdr.outofroute_id AS operativesequence,   -- prcntpos.id_proceso AS operativesequence,
    hdr.secuencia,                  -- prcntpos.secuencia,
    nifi.routealternative,              -- prcntpos.routealternative,
    hdr.secuencia AS sequence,            -- procops.id_secuencia AS sequence,
    hdr.centro_id AS id_centro,         -- prcntpos.id_centro,
  /*CASE
    WHEN procs.template_onhold_flag = 'E'
    THEN
      '0' || RIGHT(procs.id_orden_item, 6)::character varying
    ELSE procs.id_orden_item
  END AS orderit,*/
  right('0000000'|| hdr.outofroute_id,7) AS orderit,
    'SD' || ords.order_id AS demandid,              -- 'SD' || oitem.id_ordenitem_origen AS demandid,
    ords.legacy_product_code::character varying(20) AS productid,            -- oitem.id_producto AS productid,
    hdr.status_reingreso AS estado_retorno,           -- oitem.estado_retorno,
    -- oitem.id_centro_origen,
  outof.centro_id AS id_centro_origen,
    cents.nombre AS operativecenterdescription,
    hdr.operation_id AS operation,                -- procops.id_operacion AS operation,
    oper.operation_descrip AS operationname,           -- oper.descripcion AS operationname,
    hdr.status_in AS inputstatus,               -- procops.id_estado_entrada AS inputstatus,
  COALESCE
  (
    (
      /*SELECT proper.id_estado_entrada
      FROM sip_owner.sip_hrr_proceso_operaciones proper
      WHERE proper.id_proceso = prcntpos.id_proceso
      AND proper.id_secuencia = prcntpos.secuencia + 1*/

      SELECT proper.status_in::bigint
      FROM rmt_owner.rmt_template_operations proper
      WHERE proper.template_id = outof.template_id
      AND proper.secuencia = hdr.secuencia + 1
    ),
    hdr.status_reingreso::bigint -- (oitem.estado_retorno)::bigint
  ) AS outputstatus,
    12 AS inputlength,
    12 AS outputlength,
    1 AS outputsections,
    hdr.coef_descarte_no_recup AS nonreworkablerejectionrate, -- prcntpos.coef_descarte_no_recup AS nonreworkablerejectionrate,
  -- CASE 
  --  WHEN prcntpos.coef_recup_en_linea > 9
  --  THEN
  --    9
  --  ELSE
  --    prcntpos.coef_recup_en_linea
  -- END AS inlinereworkablematerialrate, 
  CASE 
    WHEN hdr.coef_recup_en_linea > 9
    THEN
      9
    ELSE
      hdr.coef_recup_en_linea
  END AS inlinereworkablematerialrate,  
    hdr.coef_recup_fuera_de_linea AS offlinereworkablematerialrate,
    hdr.productividad AS productivity,
    -- hdr.template_productividad AS productivityupdatecoefficient,
  1 AS productivityupdatecoefficient,
    hdr.pzas_por_turno AS averagepiecespershift,
    hdr.utilization,
    -- hdr.identi_productividad AS productivityidentifier,
    -- hdr.identi_utilizacion AS utilizationidentifier,
    -- hdr.identi_descarte AS reworkableidentifier,
    NULL AS productivityidentifier,
    NULL AS utilizationidentifier,
    NULL AS reworkableidentifier,
    hdr.mdlw_action AS action
FROM rmt_owner.rmt_outofroute outof
INNER JOIN sip_owner.sip_orders ords
ON ords.order_id = outof.order_id
AND ords.order_id_date = outof.order_id_date
AND ords.mill_id = 2 -- LUEGO CALCULAR MILL_ID EN LA FUNCION O PROCEDURE SEGUN LA BD
INNER JOIN rmt_owner.rmt_outofroute_hdr hdr
ON hdr.outofroute_id = outof.outofroute_id
INNER JOIN sip_owner.sip_centros cents
ON cents.centro_id = hdr.centro_id
INNER JOIN sip_owner.sip_operations oper
ON oper.operation_id = hdr.operation_id
--***--
INNER JOIN rmt_owner.rr_nifiomp_hrrformatolegacy_fn(2081) nifi
ON nifi.outofroute_id = hdr.outofroute_id
AND nifi.secuencia  = hdr.secuencia
AND nifi.centro_id  = hdr.centro_id
--***--
ORDER BY id_proceso, routealternative, sequence;