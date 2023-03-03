select
	clasificacion, bundle_origin, order_id,
	string_agg(status_id ||'('||piezas||')' ,', ') as piezas
from (
	select 
		clasificacion, order_id, status_id, bundle_origin,
		sum(pieces) as piezas
	from (
		select
			-- Inferencia de Qué es.
			coalesce(
				t3.tcorto,
				t3.fhdr, 
				'EHR'
			) as clasificacion,
			t1.*
		from 
			-- (1) Info de todas las lingadas
			(
				select * from sip_stock_wip where bundle_origin like 'LIN%'
			) t1
			
			-- (2) Lista de lingadas relevantes para RMT.
			inner join (
				select * from sip_owner.rr_rmt_stockwip_tam_fn()
			) t2 on t2.lingada = t1.bundle
			
			-- (3) Analisis de Estados FHDR
			left join (
				select
					t1.*,
					replace(tipo,'I','FHDR') as fhdr,
					case when estado like 'Q%' then 'FHDR - TUB CORTO' else null end as tcorto
				from (
					select * from rr_rmt_estadosfhdr_tam_fn()
				) t1
			) t3 on t3.estado = t1.status_id
	) x
	group by 
		clasificacion, order_id, status_id, bundle_origin
) xr where clasificacion <> 'EHR'
group by clasificacion, bundle_origin, order_id
order by order_id
