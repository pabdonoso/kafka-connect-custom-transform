package org.pabdonoso.kafka.connect.smtp;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class RedisSinkCustomTransformTest {

    private final RedisSinkCustomTransform<SourceRecord> xform = new RedisSinkCustomTransform<SourceRecord>();

    @Test
    public void generateKeyAndValueWhenKeyIsOneField(){

        String key="{LegalEntityIdentifier=21380022L4VUUBIPZ289}";
        String value = "Struct{before=Struct{personID=0,lastName=Donoso ,firstName=Pablo ,address=Gran via,city=Madrid},after=Struct{personID=0,lastName=Donoso,firstName=Pablo,address=Gran via 123,city=Madrid},source=Struct{version=1.6.0.Final,connector=sqlserver,name=RegistrTestDB,ts_ms=1627642159997,db=RegistrTestDB,schema=dbo,table=persons,change_lsn=0000004c:00008f70:0003,commit_lsn=0000004c:00008f70:0004,event_serial_no=2},op=u,ts_ms=1627642164123}";

        String transform_key="topic-test:21380022L4VUUBIPZ289";
        String transform_value="{personID=0,lastName=Donoso,firstName=Pablo,address=Gran via 123,city=Madrid}";

        Map<String, String> configMap = new HashMap<String, String>();
        configMap.put("regexKey", "(?<=\\=)(\\w+)");

        configMap.put("regexValue", "^.*after=Struct(\\{[0-9A-Za-zñÑáéíóúÁÉÍÓÚ=,_\\s\\-\\#\\.]*\\})");
        xform.configure(configMap);

        final SourceRecord record = new SourceRecord(null, null, "topic-test",null, null, key, null,value);

        final SourceRecord transformRecord = xform.apply(record);

        assertEquals(transform_key, transformRecord.key());
        assertEquals(transform_value, transformRecord.value());

    }


    @Test
    public void generateKeyAndValueWhenKeyIsMultipleFields(){

        String key="Struct{RptSubmitgNttyLei=959800JD9KFX1ULV2478,NttyRspnsblForRptLei=ZVIMWIFXTC2J79D70175}";
        String value = "Struct{before=Struct{personID=0,lastName=Donoso ,firstName=Pablo ,address=Gran via,city=Madrid},after=Struct{personID=0,lastName=Donoso,firstName=Pablo,address=Gran via 123,city=Madrid},source=Struct{version=1.6.0.Final,connector=sqlserver,name=RegistrTestDB,ts_ms=1627642159997,db=RegistrTestDB,schema=dbo,table=persons,change_lsn=0000004c:00008f70:0003,commit_lsn=0000004c:00008f70:0004,event_serial_no=2},op=u,ts_ms=1627642164123}";

        String transform_key="topic-test:959800JD9KFX1ULV2478:ZVIMWIFXTC2J79D70175";
        String transform_value="{personID=0,lastName=Donoso,firstName=Pablo,address=Gran via 123,city=Madrid}";

        Map<String, String> configMap = new HashMap<String, String>();
        configMap.put("regexKey", "(?<=\\=)(\\w+)");
        configMap.put("regexValue", "^.*after=Struct(\\{[0-9A-Za-zñÑáéíóúÁÉÍÓÚ=,_\\s\\-\\#\\.]*\\})");
        xform.configure(configMap);

        final SourceRecord record = new SourceRecord(null, null, "topic-test",null, null, key, null,value);

        final SourceRecord transformRecord = xform.apply(record);

        assertEquals(transform_key, transformRecord.key());
        assertEquals(transform_value, transformRecord.value());

    }

}