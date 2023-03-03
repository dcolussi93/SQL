select to_char(timestart,'yyyy') as hora,
				count(distinct ITEMID) as cant
				from DWOPTE_OWNER.TSW_FT_XLR_PBIRS_EXEC_LOG
				where ITEMACTION in (
					--'DataRefresh' 
					'QueryData'
					)
				and to_char(TIMESTART,'yyyy') = '2023'
				--and ITEMPATH like '/Business Analytics Reports/Regions/Southern Cone/IT/Metricas/%'
				group by  to_char(timestart,'yyyy')
				order by to_char(timestart,'yyyy')