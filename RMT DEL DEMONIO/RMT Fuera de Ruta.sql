select 
	sum(kilos) as tons,
	tipo,
	bundle_origin
from (
	select 
		fr.tipo as tipo,
		t1.*
	from 
	(
		select * from sip_stock_wip where stock_source = 'TAM-PIP'
		and ( 
			bundle_origin like 'LIN%'
			or bundle_origin like 'UT%'
			--or 1=1
		)
	) t1
	left join (
		select '05' estado, 'P/CORTE/REPARACION' as desc, 'I' as tipo union
		select '22' estado, 'TUBO PATRON' as desc, 'I' as tipo union
		select '23' estado, 'P/EVALUAR POR UT' as desc, 'I' as tipo union
		select '29' estado, 'P/RECUPERACION' as desc, 'I' as tipo union
		select '31' estado, 'P/2A INSPECCION' as desc, 'I' as tipo union
		select '41' estado, 'NO PASA MANDRIL' as desc, 'I' as tipo union
		select '59' estado, 'TERMINACION DE PRUEB' as desc, 'I' as tipo union
		select '77' estado, 'P/REPASADO' as desc, 'I' as tipo union
		select '84' estado, 'PARA VO. BO. CALIDAD' as desc, 'I' as tipo union
		select '86' estado, 'PARA VO.BO. LAB.' as desc, 'I' as tipo union
		select '89' estado, 'F/ESPECIFICACION PRE' as desc, 'I' as tipo union
		select '90' estado, 'SOBRANTE TERMINADO' as desc, 'I' as tipo union
		select '97' estado, 'PARA ETIQUETADO APRO' as desc, 'I' as tipo union
		select '9B' estado, 'DET X MEZCLA ACERO' as desc, 'I' as tipo union
		select 'A2' estado, 'PARA DESACOPLE' as desc, 'I' as tipo union
		select 'AP' estado, 'ALTO PESO EN BALANZA' as desc, 'I' as tipo union
		select 'B3' estado, 'BARRA PEND.CORTE ACE' as desc, 'I' as tipo union
		select 'B6' estado, 'BARRA,TOCHO,RETENIDO' as desc, 'I' as tipo union
		select 'BE' estado, 'ACERO EXT.PEND CORTE' as desc, 'I' as tipo union
		select 'BM' estado, 'BARRA RETENIDA META' as desc, 'I' as tipo union
		select 'BP' estado, 'BAJO PESO EN BALANZA' as desc, 'I' as tipo union
		select 'BV' estado, 'BARRAS VIRTUALES' as desc, 'I' as tipo union
		select 'C5' estado, 'P/CORTADORA BI3' as desc, 'I' as tipo union
		select 'CF' estado, 'DEFLEJADO Y MARC TUB' as desc, 'I' as tipo union
		select 'DA' estado, 'DIAMTRO ALTO DE TT' as desc, 'I' as tipo union
		select 'DC' estado, 'TPC DEVOLUC FACO' as desc, 'I' as tipo union
		select 'DI' estado, 'DEFECTO INTERNO    O' as desc, 'I' as tipo union
		select 'EP' estado, 'R.ANALISIS ESPECTRO.' as desc, 'I' as tipo union
		select 'FB' estado, 'P/FOSFATIZAR 2DO EXT' as desc, 'I' as tipo union
		select 'GS' estado, 'GESTION DE ACERIA' as desc, 'I' as tipo union
		select 'GT' estado, 'GESTION TRATAMIENTO' as desc, 'I' as tipo union
		select 'I1' estado, 'CONTR CALIDAD PLANTA' as desc, 'I' as tipo union
		select 'I2' estado, 'CALD. DE SALIDA COFI' as desc, 'I' as tipo union
		select 'I3' estado, 'INSP MAT ACCIDENTADO' as desc, 'I' as tipo union
		select 'I4' estado, 'CALD. FUER ESTD COFI' as desc, 'I' as tipo union
		select 'I5' estado, 'INSP GAMA EN COFI' as desc, 'I' as tipo union
		select 'L1' estado, 'CON PROBETA' as desc, 'I' as tipo union
		select 'L2' estado, 'SIN PROBETA' as desc, 'I' as tipo union
		select 'L3' estado, 'PTE. PRUEBA NACE' as desc, 'I' as tipo union
		select 'L5' estado, 'CON PROBETA P/MAQUIL' as desc, 'I' as tipo union
		select 'LA' estado, 'DETENIDO POR LACO' as desc, 'I' as tipo union
		select 'LC' estado, 'CON PROBETA PIP Z A' as desc, 'I' as tipo union
		select 'LD' estado, 'FALTA ANILLOS/RESULT' as desc, 'I' as tipo union
		select 'LO' estado, 'CON PROBETA PIP Z A' as desc, 'I' as tipo union
		select 'LQ' estado, 'FALTA LIBERAR X LABO' as desc, 'I' as tipo union
		select 'LT' estado, 'DETENIDO POR TELERAN' as desc, 'I' as tipo union
		select 'MB' estado, 'TUBERIA P/ACCESORIOS' as desc, 'I' as tipo union
		select 'PB' estado, 'MAT PROBETEO' as desc, 'I' as tipo union
		select 'Q2' estado, 'TUBERIA ESPESOR ALTO' as desc, 'I' as tipo union
		select 'Q3' estado, 'TUBERIA DIAMET. BAJO' as desc, 'I' as tipo union
		select 'Q4' estado, 'TUBERIA ESPESOR BAJO' as desc, 'I' as tipo union
		select 'Q5' estado, 'T, SOLO CUMPLE C/API' as desc, 'I' as tipo union
		select 'Q7' estado, 'T.CORTO(MAYOR 10.36)' as desc, 'I' as tipo union
		select 'Q8' estado, 'T.CORTO(7.62 A10.35)' as desc, 'I' as tipo union
		select 'Q9' estado, 'NO CUMPLIO PROPIEDAD' as desc, 'I' as tipo union
		select 'R1' estado, 'REP LADO PIÃ±ON' as desc, 'I' as tipo union
		select 'R2' estado, 'REP LADO COPLE' as desc, 'I' as tipo union
		select 'RA' estado, 'INSP. VISUAL' as desc, 'I' as tipo union
		select 'RH' estado, 'TUB P/REACONDICIONAR' as desc, 'I' as tipo union
		select 'RP' estado, 'P/CORTE/REP.PREMIUM' as desc, 'I' as tipo union
		select 'RR' estado, 'REVISION DE ROSCAS' as desc, 'I' as tipo union
		select 'X1' estado, 'REUNE TUBO APLIC/LTR' as desc, 'I' as tipo union
		select 'X9' estado, 'CHATARRA DE PRUEBAS' as desc, 'I' as tipo union
		select 'XP' estado, 'P/ING.AL APT VIRTUAL' as desc, 'I' as tipo union
		select 'Y1' estado, 'REUNE TUBO APLIC/AMA' as desc, 'I' as tipo union
		select '57' estado, 'PROBLEA DE RASTREABI' as desc, 'I' as tipo union
		select 'DR' estado, 'DETENIDO REDE MX' as desc, 'I' as tipo union
		select 'BW' estado, 'BARRA PARA DOWNGRADE' as desc, 'I' as tipo union
		select 'B0' estado, 'CONFORMADO EN CALIEN' as desc, 'I' as tipo union
		select 'LH' estado, 'LIB.HIGH REJECTION' as desc, 'I' as tipo union
		select 'TT' estado, 'AVANZADA TT' as desc, 'I' as tipo union
		select 'BD' estado, 'BARRA DECLASADA' as desc, 'I' as tipo union
		select 'B1' estado, 'ACERO EXTERNO FA' as desc, 'I' as tipo union
		select 'LE' estado, 'LIBERACION ETRACK' as desc, 'I' as tipo union
		select 'GR' estado, 'GESTION DE RECALCADO' as desc, 'I' as tipo 
	) fr on t1.substatus_id = fr.estado
) x group by tipo, bundle_origin order by bundle_origin