select program_name, min(diameter) , max(diameter) ,
	 			string_agg(steel_grade_id, ', ')
	 			from 
						sip_stock_wip 
					where 
						stock_source = 'TAM-PIP'
						and product_type_desc <> 'COP'
						--and order_id = '381888'
						and status_id <> '99'
						group by program_name
						order by max(diameter) desc