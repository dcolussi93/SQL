SELECT omp_owner.nifi_md_tam_legacy_hdr_raw_header_upsert_fn(
p_productid=>'H1670'::character varying,
p_demandid=>'TM299427003'::character varying,
p_tipo_hdr =>'RECOVERY'::character varying,
p_recovery_order=>'0002374'::character varying,
p_hdr_xml=>'<?xml version="1.0" encoding="UTF-8"?>
<Envelope >
   <Body>
      <SIN033WS >
         <EAA-STD-IN >
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
</Envelope>
'::text,
--p_out_of_route_json=>null,
p_using_header_id=>null, 
p_base_rollingalternative=>null,
p_billetlength=>null
); 