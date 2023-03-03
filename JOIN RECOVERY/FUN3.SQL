-- FUNCTION: omp_owner.nifi_md_tam_legacy_hdr_raw_join_recovery_fn(integer)

-- DROP FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_join_recovery_fn(integer);

CREATE OR REPLACE FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_join_recovery_fn(
    p_recovery_raw_id integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
DECLARE

                v_ROrder         character varying;
                v_DemandId       character varying;
                v_Retornostatus  character varying;
                v_ProductId      character varying;
                v_routealternativeAnt   numeric;
                v_stdClaveAnt    character varying;
                v_flgAnt         character varying;
                v_stepAnt        numeric;
                v_opsequenceAnt  numeric;
                v_opsequenceNew  numeric;
                v_id             int4;
                v_ind            integer;
                v_error          text;
                
                v_MaxStdAlternative  numeric;
                v_stdStepsqty        numeric;
                v_RecoverStepqty     numeric;
                v_stdRollingqty      numeric;
                v_maxStdOperativeSeq numeric;
                v_MaxRecoverAlternativ  numeric;
                v_RecuLength         numeric;
                v_TipoHRR            character varying;
                v_stepaux            numeric;
                v_debugAux           numeric;
                v_rollingStdaux      character varying;
                v_rollingStdant      character varying;
                v_auxAlternative     character varying;
                v_maxStdAlternatives numeric;
    
                v_tabconsolidado_raw nifi_md_tam_legacy_hdr_raw[];
                v_reg_waw            nifi_md_tam_legacy_hdr_raw;
                
                l_context   text;
                r           RECORD;

BEGIN

        SELECT routealternative, step, productid, outputstatus
        INTO STRICT v_MaxRecoverAlternativ, v_RecoverStepqty, v_ProductId, v_Retornostatus
        FROM nifi_md_tam_legacy_hdr_raw
        WHERE id = p_recovery_raw_id
        AND step = (select max(step) as stepmax from nifi_md_tam_legacy_hdr_raw
                                                           where id = p_recovery_raw_id)
        ORDER BY routealternative DESC, step DESC
        limit 1;
        
        RAISE NOTICE 'nifi_md_tam_legacy_hdr_raw_join_recovery_fn:';
        RAISE NOTICE 'v_MaxRecoverAlternativ, v_RecoverStepqty, v_ProductId, v_Retornostatus: % % % %', v_MaxRecoverAlternativ, v_RecoverStepqty, v_ProductId, v_Retornostatus;
         --inserto cabecera para recovery unificada con std
        INSERT INTO nifi_md_tam_legacy_hdr_raw_header
        ( tipo_hdr, productid, demandid, recovery_order )
        SELECT 'RECOVERYSTD', productid, demandid, recovery_order
        FROM nifi_md_tam_legacy_hdr_raw_header
        WHERE id = p_recovery_raw_id
        returning id into v_id;
        
        RAISE NOTICE 'insert en raw_header con v_id: %', v_id;
        
        IF v_Retornostatus <> '99' THEN

                --Inserto en una tabla temporal los registros de la ruta original 
                -- cuyo estado de inicio sea mayor o igua al que quiero reinsertarme.               
                -- levanta la porcion final de la ruta a partir del estado de retorno
                DROP TABLE IF EXISTS ruta_std_recortada;
                
                CREATE TEMPORARY TABLE ruta_std_recortada ON COMMIT DROP AS
                (WITH step_retorno AS( 
                --Levanta los steps y routalternatives cuyo inputstatus sea el estado de retorno de la recuperación
                -- de la última ruta estándar registrada
                        select *
                        from (
                        select id, productid, routealternative, rollingalternative, step, productivecenter,
                        row_number() over (partition by rollingalternative, routealternative) reorder
                        from  nifi_md_tam_legacy_hdr_raw
                        where (inputstatus = v_Retornostatus or
                        (v_Retornostatus in ('55','56') and inputstatus in ('55','56')))
                        --and piecespershiftprogrammingrate is not null
                        and productid = v_ProductId
                        and id = (select max(raw.id) 
                                  from nifi_md_tam_legacy_hdr_raw raw
                                  inner join nifi_md_tam_legacy_hdr_raw_header ra on raw.id=ra.id 
                                  and ra.tipo_hdr = 'STANDARD'
                                  where raw.productid = v_ProductId)
                                        ) t1
                        where reorder = 1
                        )
                SELECT ra.*
                FROM nifi_md_tam_legacy_hdr_raw ra
                INNER JOIN step_retorno sret ON ra.id = sret.id and ra.productid = sret.productid 
                and ra.routealternative = sret.routealternative and ra.rollingalternative = sret.rollingalternative
                WHERE ra.productid = v_ProductId and ra.step >= sret.step
                );
                
                select count(*) from ruta_std_recortada into v_debugAux;
                RAISE NOTICE 'ruta_std_recortada %', v_debugAux;
                
                --***Inicio FIX de Rutas no uniformes ***
                --Eliminar aquellas rolling alternatives que no tienen la máxima cantidad de routeAlternatives
                WITH rutas_by_rolling AS (
                    SELECT  rollingalternative, count(DISTINCT routealternative) as cantRouteAlternatives
                    FROM ruta_std_recortada
                    GROUP BY rollingalternative)
                
                DELETE FROM ruta_std_recortada
                WHERE rollingalternative NOT IN (select rollingalternative from rutas_by_rolling
                                                 where cantRouteAlternatives = (select max(cantRouteAlternatives)
                                                                                from rutas_by_rolling));
                
                select count(*) from ruta_std_recortada into v_debugAux;
                RAISE NOTICE 'ruta_std_recortada %', v_debugAux;
                
                SELECT  count(DISTINCT routealternative) as maxStdAlternatives, max(operativesequence), min(inputlength)
                INTO  v_maxStdAlternatives, v_maxStdOperativeSeq, v_RecuLength
                FROM ruta_std_recortada
                GROUP BY rollingalternative
                ORDER BY 1
                LIMIT 1;
RAISE NOTICE 'v_maxStdAlternatives: %', v_maxStdAlternatives;
                --Reacomodar routeAlternatives de ruta_std_recortada para que arranquen de 1 en adelante
                FOR i IN 1..(v_maxStdAlternatives) LOOP
                        UPDATE ruta_std_recortada
                        SET routealternative = i
                        WHERE routealternative = (SELECT MIN(routealternative) FROM ruta_std_recortada 
                                                  WHERE routealternative::integer >= i);
                END LOOP;
                --***Fin de FIX de Rutas no uniformes***
                
                SELECT routealternative, count(DISTINCT step) as stdStepsqty, count(DISTINCT rollingalternative) as stdRollingqty
                INTO v_auxAlternative, v_stdStepsqty, v_stdRollingqty
                FROM ruta_std_recortada
                GROUP BY routealternative
                ORDER BY 1 DESC
                LIMIT 1;
                
                SELECT DISTINCT rollingalternative
                INTO STRICT v_rollingStdant
                FROM ruta_std_recortada
                GROUP BY rollingalternative
                ORDER BY 1
                LIMIT 1;
                
                --levanta la ruta recovery inicial
                DROP TABLE IF EXISTS ruta_recovery;
                CREATE TEMPORARY TABLE ruta_recovery ON COMMIT DROP AS
                SELECT *
                FROM nifi_md_tam_legacy_hdr_raw
                WHERE id = p_recovery_raw_id;
                
                --setear input y outputlength y la 1er rolling alternative en recovery route
                UPDATE ruta_recovery
                SET inputlength = v_RecuLength,
                outputlength = v_RecuLength,
                outputsections = 1,
                rollingalternative = v_rollingStdant;

                --generar tabla de recuperación complemento con ctrl 0
                DROP TABLE IF EXISTS ruta_recovery_complemento;
                CREATE TEMPORARY TABLE ruta_recovery_complemento ON COMMIT DROP AS
                (SELECT 0 as ctrl, * FROM ruta_recovery
                WHERE id = p_recovery_raw_id
                ORDER BY routealternative desc
                LIMIT 1
                );

                --generar tabla de std complemento con ctrl 0
                DROP TABLE IF EXISTS ruta_std_complemento;
                CREATE TEMPORARY TABLE ruta_std_complemento ON COMMIT DROP AS
                (SELECT 0 as ctrl, * FROM ruta_std_recortada
                ORDER BY routealternative, rollingalternative desc
                LIMIT 1
                );
                                

                IF (v_maxStdAlternatives < v_MaxRecoverAlternativ) THEN
                -- Completar la tabla temporal std_recortada con el delta faltante
                                
                                FOR i IN 1..(v_MaxRecoverAlternativ-v_maxStdAlternatives) LOOP
                                
                                INSERT INTO ruta_std_complemento
                                (SELECT i as ctrl, * FROM ruta_std_recortada
                                ORDER BY routealternative desc, rollingalternative asc
                                LIMIT (v_stdStepsqty*v_stdRollingqty)
                                );
                                
                                 --seteo las nuevas operativeSequence y route alternative
                                UPDATE ruta_std_complemento
                                SET routealternative = v_maxStdAlternatives + i,
                                                operativesequence = v_maxStdOperativeSeq + i
                                WHERE ctrl = i;
                                
                                END LOOP;
                                
                                --Multiplico la recovery route complemento logrado por la cantidad de alternativas de laminación
                                IF  v_stdRollingqty > 1 THEN

                                                -- multiplicar todo ruta recovery por la cantidad de rolling alternatives.
                                                FOR i IN 1..(v_stdRollingqty-1) LOOP
                
                                                INSERT INTO ruta_recovery_complemento
                                                (SELECT (999+i), * FROM ruta_recovery);

                                                -- asignarle la rolling alternative siguiente y su min length
                                                SELECT DISTINCT rollingalternative, min(inputlength)
                                                INTO STRICT v_rollingStdaux, v_RecuLength 
                                                FROM ruta_std_recortada
                                                WHERE  rollingalternative > v_rollingStdant
                                                GROUP BY rollingalternative 
                                                ORDER BY 1
                                                LIMIT 1;
                                                
                                                UPDATE ruta_recovery_complemento
                                                SET rollingalternative = v_rollingStdaux,
                                                inputlength = v_RecuLength,
                                                outputlength = v_RecuLength,
                                                outputsections = 1
                                                WHERE ctrl = (999+i);
                                                
                                                v_rollingStdant := v_rollingStdaux;

                                                END LOOP;
                                                
                                END IF;
                                
                ELSEIF (v_maxStdAlternatives >= v_MaxRecoverAlternativ /*OR v_stdRollingqty > 1*/ ) THEN
                -- Completar la tabla temporal recovery_complemento con el delta faltante
                                
                                --generar los caminos complemento faltantes
                                FOR i IN 1..((v_maxStdAlternatives-v_MaxRecoverAlternativ)) LOOP
                                
                                                INSERT INTO ruta_recovery_complemento
                                                (SELECT i as ctrl, * FROM nifi_md_tam_legacy_hdr_raw
                                                WHERE id = p_recovery_raw_id
                                                ORDER BY routealternative desc
                                                LIMIT (v_RecoverStepqty)
                                                );
                                                --nombro la nueva alternativa con el número siguiente de alternativa
                                                UPDATE ruta_recovery_complemento
                                                SET routealternative = v_MaxRecoverAlternativ + i
                                                WHERE ctrl = i;
                                
                                END LOOP;
                                
                                --setear a 1er rolling alternative en toda la recovery route complemento
                                UPDATE ruta_recovery_complemento
                                SET rollingalternative = v_rollingStdant,
                                                inputlength = v_RecuLength,
                                                outputlength = v_RecuLength,
                                                outputsections = 1;

                                --Multiplico la recovery route complemento logrado por la cantidad de alternativas de laminación
                                IF  v_stdRollingqty > 1 THEN
                                
                                                -- multiplicar todo ruta recovery y complemento por la cantidad de rolling alternatives.
                                                FOR i IN 1..(v_stdRollingqty-1) LOOP

                                                DROP TABLE IF EXISTS ruta_recovery_complementoAux;
                                                CREATE TEMPORARY TABLE ruta_recovery_complementoAux ON COMMIT DROP AS
                                                (SELECT * FROM ruta_recovery_complemento WHERE Ctrl > 0);
                                                
                                                UPDATE ruta_recovery_complementoAux
                                                SET Ctrl = (999+i);
                                                
                                                INSERT INTO ruta_recovery_complemento
                                                (SELECT * FROM ruta_recovery_complementoAux
                                                UNION SELECT (999+i) as ctrl, * FROM ruta_recovery
                                                );

                                                -- asignarle la rolling alternative siguiente y su min length
                                                SELECT DISTINCT rollingalternative, min(inputlength)
                                                INTO STRICT v_rollingStdaux, v_RecuLength 
                                                FROM ruta_std_recortada
                                                WHERE  rollingalternative > v_rollingStdant
                                                GROUP BY rollingalternative
                                                ORDER BY 1
                                                LIMIT 1;
                                                
                                                UPDATE ruta_recovery_complemento
                                                SET rollingalternative = v_rollingStdaux,
                                                inputlength = v_RecuLength,
                                                outputlength = v_RecuLength,
                                                outputsections = 1
                                                WHERE ctrl = (999+i);
                                                
                                                v_rollingStdant := v_rollingStdaux;

                                                END LOOP;
                                                
                                END IF;
                                
                                
                END IF;
                
                --v_i := 0;
                v_ind := 1;
                v_flgAnt := '0';
                v_opsequenceNew := v_maxStdOperativeSeq; -- operative sequence ficticia
                v_opsequenceAnt := 0;
                v_stepAnt := 0;
                
                --INI FIX Aug21 renumeración con clave ampliada
                --renumerar steps en la ruta_std y complemento para que sea coherente con las otras
                FOR r IN (
                    SELECT DISTINCT newStep, step, operation, rollingalternative, routealternative
                    FROM (
                    SELECT row_number() over (partition by rollingalternative, routealternative order by rollingalternative, routealternative, step) as newStep, ra.*
                    FROM ruta_std_recortada ra
                         ) t2
                    order by 1
                                    )
                LOOP
                        UPDATE ruta_std_recortada
                        SET step = v_RecoverStepqty + r.newStep
                        WHERE step = r.step and operation = r.operation
                        AND rollingalternative =  r.rollingalternative
                        AND routealternative   =  r.routealternative; 
        
                        /*                                
                        UPDATE ruta_std_complemento
                        SET step = v_RecoverStepqty + r.newStep
                        WHERE step = r.step and operation = r.operation;*/
                                
                END LOOP;
                
        FOR r IN (
                    SELECT DISTINCT newStep, step, operation, rollingalternative, routealternative
                    FROM (
                    SELECT row_number() over (partition by rollingalternative, routealternative order by rollingalternative, routealternative, step) as newStep, ra.*
                    FROM ruta_std_complemento ra
                         ) t2
                    order by 1
                                    )
                LOOP
                        UPDATE ruta_std_complemento
                        SET step = v_RecoverStepqty + r.newStep
                        WHERE step = r.step and operation = r.operation
                        AND rollingalternative =  r.rollingalternative
                        AND routealternative   =  r.routealternative; 
                                
                END LOOP;
        --FIN FIX Aug21 renumeración con clave ampliada
                                
                --Consolidar e insertar todo en la tabla hdr
                v_ind := 1;
                
                FOR r IN (
                                SELECT 99 as ctrl, * FROM ruta_std_recortada
                                UNION SELECT * FROM ruta_std_complemento where ctrl > 0
                                UNION SELECT 99 as ctrl, * FROM ruta_recovery
                                UNION SELECT * FROM ruta_recovery_complemento where ctrl > 0
                                --ORDER BY 1, operativesequence, rollingalternative, step, routealternative
                                ORDER BY 1)
                LOOP
                
                                v_reg_waw.id := v_id;
                                --v_reg_waw.processid := null;
                                v_reg_waw.productid := r.productid;
                                v_reg_waw.rollingalternative := r.rollingalternative;
                                v_reg_waw.routealternative := r.routealternative;                            
                                v_reg_waw.operativesequence := r.operativesequence; 
                                v_reg_waw.routealternative := r.routealternative;
                                v_reg_waw.rollingmill := r.rollingmill;
                                v_reg_waw.step := r.step;
                                v_reg_waw.operation := r.operation;
                                v_reg_waw.operationname := r.operationname;
                                v_reg_waw.productivecenter := r.productivecenter;
                                v_reg_waw.productivecentername := r.productivecentername;
                                v_reg_waw.seqgroup := r.seqgroup;
                                v_reg_waw.inputstatus := r.inputstatus;
                                v_reg_waw.inputlength := r.inputlength;
                                v_reg_waw.outputstatus := r.outputstatus;
                                v_reg_waw.outputlength := r.outputlength;
                                v_reg_waw.outputsections := r.outputsections;
                                v_reg_waw.piecespershiftprogrammingrate := r.piecespershiftprogrammingrate;
                                v_reg_waw.scrapyield := r.scrapyield;
                                v_reg_waw.utilization := r.utilization;
                                v_reg_waw.creation_date := clock_timestamp();
                                v_reg_waw.processingdate := clock_timestamp()::date;
                                -- v_reg_waw.lastlegacyupdate := clock_timestamp();
                                
                                v_tabconsolidado_raw[v_ind] := v_reg_waw;
                                
                                RAISE NOTICE 'v_reg_waw: %', concat('id: ',v_reg_waw.id,' prod: ',v_reg_waw.productid ,' rolling: ', v_reg_waw.rollingalternative,' route: ', v_reg_waw.routealternative, ' step: ',v_reg_waw.step);
                                
                                v_ind := v_ind + 1;
        
                END LOOP;
            
            -- ****NUEVO**** EstadoRetorno=99 -> significa que no hay porción de ruta std a concatenar
            ELSE 
                
                DROP TABLE IF EXISTS ruta_std;
                
                CREATE TEMPORARY TABLE ruta_std ON COMMIT DROP AS (
                    select *
                    from  nifi_md_tam_legacy_hdr_raw
                    where id = (select max(raw.id) 
                                from nifi_md_tam_legacy_hdr_raw raw
                                inner join nifi_md_tam_legacy_hdr_raw_header ra on raw.id=ra.id 
                                and ra.tipo_hdr = 'STANDARD'
                                where raw.productid = '83645')
                );
                
                SELECT max(outputlength)
                INTO STRICT v_RecuLength
                FROM ruta_std;

                SELECT DISTINCT rollingalternative
                INTO STRICT v_rollingStdant
                FROM ruta_std
                WHERE outputlength = v_RecuLength
                ORDER BY 1
                LIMIT 1;
                
                --levanta la ruta recovery inicial
                DROP TABLE IF EXISTS ruta_recovery;
                CREATE TEMPORARY TABLE ruta_recovery ON COMMIT DROP AS
                SELECT *
                FROM nifi_md_tam_legacy_hdr_raw
                WHERE id = p_recovery_raw_id;
                
                --setear input y outputlength y la 1er rolling alternative en recovery route
                UPDATE ruta_recovery
                SET inputlength = v_RecuLength,
                outputlength = v_RecuLength,
                outputsections = 1,
                rollingalternative = v_rollingStdant;

                --Consolidar e insertar todo en la tabla hdr
                v_ind := 1;
                
                FOR r IN (
                    SELECT 99 as ctrl, * FROM ruta_recovery
                         )
                LOOP
                
                    v_reg_waw.id := v_id;
                    --v_reg_waw.processid := null;
                    v_reg_waw.productid := r.productid;
                    v_reg_waw.rollingalternative := r.rollingalternative;
                    v_reg_waw.routealternative := r.routealternative;                            
                    v_reg_waw.operativesequence := r.operativesequence; 
                    v_reg_waw.routealternative := r.routealternative;
                    v_reg_waw.rollingmill := r.rollingmill;
                    v_reg_waw.step := r.step;
                    v_reg_waw.operation := r.operation;
                    v_reg_waw.operationname := r.operationname;
                    v_reg_waw.productivecenter := r.productivecenter;
                    v_reg_waw.productivecentername := r.productivecentername;
                    v_reg_waw.seqgroup := r.seqgroup;
                    v_reg_waw.inputstatus := r.inputstatus;
                    v_reg_waw.inputlength := r.inputlength;
                    v_reg_waw.outputstatus := r.outputstatus;
                    v_reg_waw.outputlength := r.outputlength;
                    v_reg_waw.outputsections := r.outputsections;
                    v_reg_waw.piecespershiftprogrammingrate := r.piecespershiftprogrammingrate;
                    v_reg_waw.scrapyield := r.scrapyield;
                    v_reg_waw.utilization := r.utilization;
                    v_reg_waw.creation_date := clock_timestamp();
                    v_reg_waw.processingdate := clock_timestamp()::date;
                    -- v_reg_waw.lastlegacyupdate := clock_timestamp();
                                
                    v_tabconsolidado_raw[v_ind] := v_reg_waw;

                    RAISE NOTICE 'v_reg_waw: %', concat('id: ',v_reg_waw.id,' prod: ',v_reg_waw.productid ,' rolling: ', v_reg_waw.rollingalternative,' route: ', v_reg_waw.routealternative, ' step: ',v_reg_waw.step);

                    v_ind := v_ind + 1;
        
                END LOOP;
            
            END IF;

            INSERT INTO nifi_md_tam_legacy_hdr_raw
            SELECT * FROM unnest(v_tabconsolidado_raw);
  
  return v_id;
  EXCEPTION
        WHEN OTHERS THEN
                                
        v_error := sqlerrm;
        GET STACKED DIAGNOSTICS l_context = PG_EXCEPTION_CONTEXT;
        RAISE EXCEPTION 'ERROR merge_recovery_std:% SQLSTATE:% SQLERRM: %', l_context, sqlstate, sqlerrm;
END;
$BODY$;

ALTER FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_join_recovery_fn(integer)
    OWNER TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_join_recovery_fn(integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_join_recovery_fn(integer) TO omp_owner;

GRANT EXECUTE ON FUNCTION omp_owner.nifi_md_tam_legacy_hdr_raw_join_recovery_fn(integer) TO omp_rw_role;

