select * from STEELKNOWLEDGEBASE where STEELKNOWLEDGEBASEID = '66'
select * from CHGLDMSTEELKNOWLEDGEBASE where STEELKNOWLEDGEBASEID = '66'
select * from LDMSTEELKNOWLEDGEBASE where STEELKNOWLEDGEBASEID = '66'

select * from MATACCEPTEDALLOCATION 
	where MATSTOCKID in (
		'TO_S_654/215_04_43050_03290_TM298782012_385095',
		'BA_S_302/215_BM/03_42062_03290_TM297214001_379879',
		'TO_S_310/215_04_37914_03271_TM930137043_386729',
		'BA_S_602/310_03_44878_12120_TM296813003_380898',
		'BA_S_648/310_03_43604_09010_TM400448025_374252',
		'BA_S_776/270_03_45052_13480_TM930066086_256713',
		'BA_S_647/270_03_44746_12480_TM400451097_385119',
		'BA_S_334/270_03_43989_11140_TM298129001_382191'	
	)
and ADSTAT = 'A'


select e.PROCESSEDSTATUS, e.* from EXPPUBMATACCEPTEDALLOCATION e 
	where MATSTOCKID in (
		'TO_S_654/215_04_43050_03290_TM298782012_385095',
		'BA_S_302/215_BM/03_42062_03290_TM297214001_379879',
		'TO_S_310/215_04_37914_03271_TM930137043_386729',
		'BA_S_602/310_03_44878_12120_TM296813003_380898',
		'BA_S_648/310_03_43604_09010_TM400448025_374252',
		'BA_S_776/270_03_45052_13480_TM930066086_256713',
		'BA_S_647/270_03_44746_12480_TM400451097_385119',
		'BA_S_334/270_03_43989_11140_TM298129001_382191'	
	)
and ADSTAT = 'A'