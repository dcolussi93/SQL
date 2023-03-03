select 
	max(order_id) as expediente,
	bundle_origin||' ('||status_id||')' as centro_estado,
	string_agg(distinct accion, chr(10) order by accion desc) accion
	
from (
	select 
		to_char(creation_date,'yyyy-mm-dd: ')||chr(10)||'('||fhr_comment||'): '||suggested_action as accion,
		x1.* 
	from sip_ctw_fhr_comments x1 where order_id = '5774701'
) t1
group by bundle_origin,
	status_id
order by accion desc
