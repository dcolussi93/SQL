with 
			-- Datasource del stock actual fuera de centro. RMT. Solo stock wip.
			stock1 as (
				select
					tv.bundle,
					tv.toneladas,
					tv.piezas,
					case 
						when status in (42,48,49,99) then 'FHR - En Espera de Liberacion'
						when tv.estado_orden_item = 'FHR - Ya Programable' then 'FHR - Con Ruta de Recuperacion'
						when tv.estado_orden_item = 'FHR - Dejado atrás' then 'FHR - Expediente Cumplido'
						else tv.estado_orden_item 
					end as categoria,
					--tv.estado_orden_item as categoria,
					ts.state_date,
					DATE_PART('day', now()::timestamp - ts.state_date::timestamp) as aging,
					coalesce(t3.warehouse_name,'OTROS') as ubicacion,
					ts.bundle_origin,
					ts.status
				from 
					(
						select * from sip_owner.sip_stock_wip 
					) ts
					inner join (
						select * from sip_owner.rr_categoriasindex_lingadas_vw 
					) tv on tv.bundle = ts.bundle
					left join (
						select * from sip_owner.sip_warehouses --where warehouse_name = 'Laco I'
					) t3 on t3.warehouse_id_for_stock_wip = ts.factory_id
			)
			
			-- Calcular piezas tons y lingadas maximas por centro.
			-- Se usa para calcular el promedio ponderado. Necesitamos joinear esta tabla con el datasource.
			, stock2 as (
				select 
					categoria,
					ubicacion,
					sum(piezas) as piezas_totales,
					sum(toneladas) as toneladas_totales,
					count(bundle) as cant_lingadas
				from 
					stock1 
				group by ubicacion, categoria
			)
			
			-- Tabla Dataset para calcular Ponderacion
			-- Join para saber cuales son las toneladas maximas por el centro y categoria de cada lingada.
			-- Con esto se arma la ponderacion
			, dataset_final as (
				select 
					s1.*,
					(s1.toneladas/s2.toneladas_totales) as ponderador,
					s2.piezas_totales,
					s2.toneladas_totales,
					s2.cant_lingadas
				from 
				
				-- Tabla Datasource del stock wip.
				(
					select * from stock1
				) s1
				
				-- Joineamos contra los valores totales agrupados por categorias
				inner join(
					select * from stock2
				) s2 on (s1.ubicacion = s2.ubicacion) and (s1.categoria = s2.categoria)
			)
		
		-- SELECT PRINCIPAL. Acá se calcula el indicador.
		-- Se convierte ese indicador en un Json.
		-- select * from stock3
		
			select
				s1.ubicacion,
				s1.categoria,
					count(s1.bundle) as cant_lingadas,
					sum(s1.piezas)::integer as sum_piezas,
					sum(s1.toneladas)::integer as sum_tons,
						-- Promedio ponderado
					sum(s1.aging * s1.ponderador )::numeric as aging_prom_pond,
						-- Promedio
					avg(s1.aging)::numeric as aging_prom,
						-- Mediana
					PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY s1.aging) as aging_mediana	
			from 
				
				-- Tabla datasource de Stock
				(
					select * from dataset_final
				) s1
				--where lower(s1.categoria) like '%pend%'	
			group by 
				s1.ubicacion,s1.categoria
		