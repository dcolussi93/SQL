select t2.tipo, sum(pieces), round(sum (kilos/1000),0)from (
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
				--and bundle_origin like '%ACE%'
				--and order_id = '372510'
				and status_id <> '99'
) t1
left join (
	select * from rr_rmt_estadosfhdr_tam_fn() 
) t2 on t2.estado = t1.status_id
group by t2.tipo