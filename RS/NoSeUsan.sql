select 
	name,
	max(fecha),
	sum(uso)
from (
	select
		t3.fecha, 
		name,
		uso,
		path 
	from 
		-- Catalogo de reportes Activos. Ultima foto
		(
			select distinct 
				name, path, ITEMID 
			from 
				DWOPTE_OWNER.TSW_LK_XLR_PBIRS_CATALG 
				where path like '/Business Analytics Reports/Regions/Southern Cone/IT/Metricas/%'
				and to_char(SNAPSHOTTIME,'yymmdd') = '230301'
				and type = 13
		) t1
		
		-- Cantidad de uso por mes de esos reportes. 
		left join (
			select 
				ITEMID , count(1) as uso, to_char(timestart,'yymm') as fecha 
			from 
				DWOPTE_OWNER.TSW_FT_XLR_PBIRS_EXEC_LOG
				where to_char(timestart,'yyyy') in ('2023','2022')
				and ITEMPATH like '/Business Analytics Reports/Regions/Southern Cone/IT/Metricas/%'
				and ITEMACTION = 'QueryData'
				group by ITEMID, to_char(timestart,'yymm')
		) t3 on t3.ITEMID = t1.itemid
) x
group by name
