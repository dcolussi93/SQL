WITH x AS (  -- not the missing namespace below
   SELECT '<Envelope >
   <Body>
      <SIN033WS>
         <EAA-STD-IN>
            <SECURITYID/>
            <LANGUAGEID/>
            
         </EAA-STD-IN>
         <SIA033WS-IN >
            <ORDERIT>0002374</ORDERIT>
            <DEMANDID>TM299427003</DEMANDID>
            <PRODUCTID>H1670</PRODUCTID>
            <OPERATIVESEQUENCE>2374</OPERATIVESEQUENCE>
            <HRRDETAILSs>
               <HRRDETAILS>
                  <ROUTEALTERNATIVE>1</ROUTEALTERNATIVE>
                  <SEQUENCE>1</SEQUENCE>
                  <PRODUCTIVECENTER>101</PRODUCTIVECENTER>
                  <OPERATIVECENTERDESCRIPTION>CEL2</OPERATIVECENTERDESCRIPTION>
                  <OPERATION>65</OPERATION>
                  <OPERATIONNAME>P/RECUPERACION</OPERATIONNAME>
                  <OUTPUTSTATUS>CD</OUTPUTSTATUS>
                  <INPUTSTATUS>29</INPUTSTATUS>
                  <INPUTLENGTH>12</INPUTLENGTH>
                  <OUTPUTLENGTH>12</OUTPUTLENGTH>
                  <OUTPUTSECTIONS>1</OUTPUTSECTIONS>
                  <NONREWORKABLEREJECTIONRATE>1.0</NONREWORKABLEREJECTIONRATE>
                  <INLINEREWORKABLEMATERIALRATE>1.0</INLINEREWORKABLEMATERIALRATE>
                  <OFFLINEREWORKABLEMATERIALRATE>1.0</OFFLINEREWORKABLEMATERIALRATE>
                  <PRODUCTIVITY>1.0</PRODUCTIVITY>
                  <PRODUCTIVITYUPDATECOEFFICIENT>1</PRODUCTIVITYUPDATECOEFFICIENT>
                  <AVERAGEPIECESPERSHIFT>1.0</AVERAGEPIECESPERSHIFT>
                  <UTILIZATION>1.0</UTILIZATION>
                  <PRODUCTIVITYIDENTIFIER/>
                  <UTILIZATIONIDENTIFIER/>
                  <REWORKABLEIDENTIFIER/>
               </HRRDETAILS>
            </HRRDETAILSs>
            <ACTION>A</ACTION>
            <CREATEDUSER>NIFI</CREATEDUSER>
            <UPDATEDUSER>NIFI</UPDATEDUSER>
         </SIA033WS-IN>
      </SIN033WS>
   </Body>
</Envelope>'::xml as p_xml
   )
 SELECT 
 	( select unnest( xpath('/Envelope/Body/SIN033WS/SIA033WS-IN/ORDERIT/text()', p_xml) ) as orderit from x ) ,
	( select unnest( xpath('/Envelope/Body/SIN033WS/SIA033WS-IN/DEMANDID/text()', p_xml) ) as demand from x ) ,
	( select unnest( xpath('/Envelope/Body/SIN033WS/SIA033WS-IN/OPERATIVESEQUENCE/text()', p_xml) ) as operativesequence from x ) ,
	( select unnest( xpath('/Envelope/Body/SIN033WS/SIA033WS-IN/PRODUCTID/text()', p_xml) ) as productid from x ) 
	
