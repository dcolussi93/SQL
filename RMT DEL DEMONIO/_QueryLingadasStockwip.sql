		select
			-- Inferencia de categoria
			coalesce( 
				t3.tubocorto,
				t3.fhdr,
				'EHR'
			)::text as clasificacion,
			-- Datos adicionales
			t1.order_id ::text as orderid,
			t1.status_id ::text as statusid,
			t1.bundle_origin ::text as bundleorigin,
			t1.product_code ::text as productode,
			t1.product_type_desc::text as productype,
			t1.steel_grade_id::text as steelgrade,
			t1.customer_desc::text as customerdesc,
			t1.diameter::text as diametro,
			t1.espesor::text as espesor,
			(t1.kilos/1000)::text as toneladas,
			t1.pieces::text as piezas,
			t1.factory_id::text as factory,
			t1.state_date::text as statedate,
			t1.id_extremo::text as idextremo,
			t1.bundle::text as bundle
		from 
			-- (1) Info de todas las lingadas
			(
				select * from sip_stock_wip where bundle_origin like 'LIN1'
			) t1
			
			-- (2) Lista de lingadas relevantes para RMT.
			inner join (
				select * from sip_owner.rr_rmt_stockwip_tam_fn()
			) t2 on t2.lingada = t1.bundle
			
			-- (3) Analisis de Estados FHDR
			left join (
				select
					x1.*,
					replace(tipo,'I','FHDR') as fhdr,
					case 
						when estado like 'Q%' then 'FHDR - TuboCorto' 
						else null 
					end as tubocorto
				from (
					select * from rr_rmt_estadosfhdr_tam_fn()
				) x1
			) t3 on t3.estado = t1.status_id


