select 
	order_id, count(order_id) ,
	string_agg(distinct diameter::text,'; ') listadiametros,
	string_agg(distinct espesor::text, ' ;') listaespesores,
	string_agg(distinct steel_grade_id, ' ;') listaaceros,
	string_agg(distinct product_type_desc, '; ') lista_tiposprod,
	string_agg(distinct customer_desc,'; ') listaclientes
from (
	select distinct 
		order_id,
		steel_grade_id,
		diameter,
		espesor,
		--length,
		customer_desc,
		product_type_desc,
		product_code
	from (

		select *  from 
			sip_stock_wip 
		where 
			stock_source = 'TAM-PIP'
			and program_name in (
				'STU PIP',
				'SSE PIP',
				'PTU PIP',
				'ZTO PIP'
			)
			and product_type_desc <> 'COP'
			--and order_id = '372510'
			and status_id <> '99'
			
			
	) x1
	--order by order_id
) x group by order_id having count(order_id) >1 order by count(order_id) desc, lista_tiposprod desc