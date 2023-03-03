	
	select 
		to_char(SNAPSHOTTIME,'yyyy') as anio,
		count (distinct name) as cant_rep
	from DWOPTE_OWNER.TSW_LK_XLR_PBIRS_CATALG 
	where type = 13
	group by to_char(SNAPSHOTTIME,'yyyy')