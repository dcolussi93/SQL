select anio, cant_rep, cant,  cant/cant_rep as uso from 
	(
		select count(distinct name)  as cant_rep
			--name, path, ITEMID 
		from 
			DWOPTE_OWNER.TSW_LK_XLR_PBIRS_CATALG 
			where path like '/Business Analytics Reports/Regions/Southern Cone/IT/Metricas/%'
			and to_char(SNAPSHOTTIME,'yy') = '23'
			and type = 13
	) x1
	inner join (
		select to_char(timestart,'yyyy') as anio,
		count(distinct ITEMID) as cant
		from DWOPTE_OWNER.TSW_FT_XLR_PBIRS_EXEC_LOG
		where ITEMACTION in (
			--'DataRefresh' 
			'QueryData'
			)
		and to_char(TIMESTART,'yyyy') = '2023'
		and ITEMPATH like '/Business Analytics Reports/Regions/Southern Cone/IT/Metricas/%'
		group by  to_char(timestart,'yyyy')
		--order by to_char(timestart,'yyyy')
	) x2 on 1=1