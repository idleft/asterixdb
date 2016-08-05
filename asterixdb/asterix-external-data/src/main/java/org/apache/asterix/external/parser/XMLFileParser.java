/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.external.parser;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.input.stream.AsterixInputStreamReader;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.json.JSONException;
import org.xml.sax.*;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.CharArrayReader;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Xikui on 6/28/16.
 */
public class XMLFileParser extends AbstractDataParser implements IStreamDataParser, IRecordDataParser<char[]> {

    private ARecordType recordType;
    private XMLReader xmlReader;
    private IARecordBuilder rb;
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool =
            new ListObjectPool<>(new AbvsBuilderFactory());


    private boolean writeField(ATypeTag typeTag, DataOutput fieldOut, Object fieldValue) throws HyracksDataException {
        switch (typeTag){
            case INT64:
                aInt64.setValue((long)fieldValue);
                int64Serde.serialize(aInt64, fieldOut);
                break;
            case STRING:
                aString.setValue((String) fieldValue);
                stringSerde.serialize(aString, fieldOut);
                break;
            case DOUBLE:
                aDouble.setValue((double) fieldValue);
                doubleSerde.serialize(aDouble,fieldOut);
        }
        return true;
    }

    private boolean parseRecord(ARecordType recordType, XMLReader xmlReader, DataOutput out) throws Exception {
        ArrayBackedValueStorage fieldBuffer = getTempBuffer();
        String[] attrNames = recordType.getFieldNames();

        rb.reset(recordType);
        rb.init();
        for(int iter1 = 0; iter1<attrNames.length; iter1++){
            fieldBuffer.reset();
            writeField(recordType.getFieldType(attrNames[iter1]).getTypeTag(), fieldBuffer.getDataOutput(),
                    xmlReader.getProperty(attrNames[iter1]));
//            reco
            rb.addField(iter1, fieldBuffer);
        }
        rb.write(out,true);
        return true;
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws IOException {
        char[] rawRecord = record.get();
        resetPools();
        try {
            xmlReader.parse(new InputSource(new CharArrayReader(rawRecord)));
            parseRecord(recordType, xmlReader, out);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }


    }

    public XMLFileParser(ARecordType recordType) throws SAXException {
        this.recordType = recordType;
        xmlReader = XMLReaderFactory.createXMLReader();
        rb = new RecordBuilder();
    }

    @Override
    public void setInputStream(InputStream in) throws IOException {

    }

    @Override
    public boolean parse(DataOutput out) throws IOException {
        return false;
    }

    @Override
    public boolean reset(InputStream in) throws IOException {
        return false;
    }


    private ArrayBackedValueStorage getTempBuffer(){
        return (ArrayBackedValueStorage)abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    private void resetPools(){
        abvsBuilderPool.reset();
    }
}
