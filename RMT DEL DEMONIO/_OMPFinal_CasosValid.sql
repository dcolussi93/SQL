select STATUS, count(1) from (
select distinct 
	t1.CREATIONDATE,
	t1.MATSTOCKID,
	t1.STATUS,
	t1.TOBECHECKEDBYMETALLURGY,
	t1.TOBECHECKEDBYROLLING,
	t1.DEMANDID,
	t1.CLONEPRODUCTID,
	t1.KNOWLEDGEBASECOMMENTS
	
from 
	(
		select 
			distinct DATEPART(week, x1.CREATIONDATE) as semana,
			x1.* 
		from MATACCEPTEDALLOCATION x1
		where ADSTAT = 'A'
		and STATUS in ('Valid','Pending')
		and MATTYPE like '%Steel%'
		and CONVERT(varchar, CREATIONDATE,102) like '%22.05%'
		--order by CREATIONDATE DESC
		--and DATEPART(week, x1.CREATIONDATE) = '20'
		--SELECT DATEPART(week, '2007-04-21 ')
	) t1
	inner join (
		select * from PUBMATACCEPTEDALLOCATION p where ADSTAT = 'A'
	) t2 on t2.MATSTOCKID = t1.MATSTOCKID
--order by CREATIONDATE desc
) x group by STATUS
	