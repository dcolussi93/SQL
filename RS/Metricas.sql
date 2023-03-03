select
--	timestart, USERNAME, name, ITEMACTION, path 
	count(1)
from 
		
	(
		select * from DWOPTE_OWNER.TSW_LK_XLR_PBIRS_CATALG 
		--where rownum < 10
		where path like '%IT/Metricas%'
		and name like '%Cert%'
	) t1
	
	inner join (
		select max(SNAPSHOTTIME) as snap 
		from DWOPTE_OWNER.TSW_LK_XLR_PBIRS_CATALG
	) t2 on t1.snapshottime = t2.snap

	inner join (
				
		select count(1) from DWOPTE_OWNER.TSW_FT_XLR_PBIRS_EXEC_LOG
		where to_char(timestart,'yymm') = '2302'
		--and ITEMPATH like '%IT/Metricas%'
		and ITEMACTION = 'QueryData'
		--USERNAME = 'TENARIS\60066739'

	) t3 on t3.ITEMID = t1.itemid
	

	