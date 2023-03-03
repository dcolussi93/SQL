select * from (
	select
		to_char(timestart,'yymm') as fecha, 
		--USERNAME, 
		count(distinct name) as rep 
		--type,
		--ITEMACTION, 
		--path 
		
	from 
			
		(
			select * from DWOPTE_OWNER.TSW_LK_XLR_PBIRS_CATALG 
	--		where rownum < 10
			where path like '%IT/Metricas%'
			--and upper(name) like '%ECAL%'
			and type = 13
		) t1
		
		inner join (
			select max(SNAPSHOTTIME) as snap 
			from DWOPTE_OWNER.TSW_LK_XLR_PBIRS_CATALG
		) t2 on t1.snapshottime = t2.snap
	
		inner join (
					
			select * from DWOPTE_OWNER.TSW_FT_XLR_PBIRS_EXEC_LOG
			where to_char(timestart,'yyyy') = '2022'
			--and ITEMPATH like '%IT/Metricas%'
			and ITEMACTION = 'QueryData'
			--USERNAME = 'TENARIS\60066739'
	
		) t3 on t3.ITEMID = t1.itemid
		
	group by to_char(timestart,'yymm') 
) t1
inner join (
	select 
		to_char(SNAPSHOTTIME,'yymm') as anio,
		count (distinct name) as cant_rep
	from DWOPTE_OWNER.TSW_LK_XLR_PBIRS_CATALG 
	where type = 13
	and to_char(SNAPSHOTTIME,'yyyy') = '2022'
	group by to_char(SNAPSHOTTIME,'yymm')
) t2 on t2.anio = t1.fecha
	
	