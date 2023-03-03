select x.bundle_origin , x.* from sip_stock_wip x where status_id = '89' and status_id <>'70'
and stock_source = 'SSF'
and product_id = '83777'
order by bundle_origin

select STATUS , m.* from MATACCEPTEDALLOCATION m where 
	ADSTAT = 'A' 
	and MATSTOCKID = 'BA_S_334/310_03_42598_10000_TM295280002_374889'


select * from process where PRODUCTID like '%H3509%'
select * from CLONEPRODUCT where CLONEPRODUCTID like '%H3509%'
select * from CLONEPRODUCT where CLONEPRODUCTID like '%H3509%' 
select * from PROCESS where PRODUCTID like '%F8811%'



select * from CHGGDMPROCESS where PRODUCTID like '%H3509%'