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
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

/**
 * Created by Xikui on 6/28/16.
 */
public class XMLFileParser extends AbstractDataParser implements IRecordDataParser<char[]> {

    private ARecordType recordType;
    private SAXParser xmlParser;
    private IARecordBuilder rb;
    private String[] attrNameList;
    private ArrayBackedValueStorage fieldValueBuffer, fieldNameBuffer;
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<>(
            new AbvsBuilderFactory());
    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<>(
            new RecordBuilderFactory());


    public XMLFileParser(ARecordType recordType) throws ParserConfigurationException, SAXException {
        this.recordType = recordType;
        xmlParser = SAXParserFactory.newInstance().newSAXParser();
        attrNameList = recordType.getFieldNames();
        fieldValueBuffer = getTempBuffer();
        fieldNameBuffer = getTempBuffer();
        rb = getRecordBuilder();
    }

    private int getAttrNameIdx(String attrName) {
        int idx = 0;
        for (String name : attrNameList) {
            if (name.equals(attrName))
                return idx;
            idx++;
        }
        return -1;
    }

    private boolean writeField(int idx, String fieldName, String fieldValue) throws HyracksDataException {
        fieldNameBuffer.reset();
        fieldValueBuffer.reset();
        aString.setValue(fieldValue);
        stringSerde.serialize(aString, fieldValueBuffer.getDataOutput());
        if (idx > 0) {
            rb.addField(idx, fieldValueBuffer);
        } else {
            aString.setValue(fieldName);
            stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
            rb.addField(fieldNameBuffer, fieldValueBuffer);
        }
        return true;
    }

    private DefaultHandler handler = new DefaultHandler() {

        String curEleName;
        int curLvl = 0;

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
            curEleName = qName;
            curLvl ++;
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            // no op
            String curEleVal = new String(ch, start, length).trim();
            if(curLvl!=2||curEleVal.length()==0){
                return;
            }
            try {
                writeField(getAttrNameIdx(curEleName), curEleName, curEleVal);
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void endElement (String uri, String localName, String qName)
                throws SAXException
        {
            //do nothing
            String qn = qName;
            curLvl--;
        }

    };

    @Override public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws IOException {
        String strRecord = record.toString();
        resetPools();

        try {
            rb.reset(recordType);
            rb.init();
            xmlParser.parse(new InputSource(new StringReader(strRecord)), handler);
            rb.write(out, true);
        } catch (SAXException e) {
            e.printStackTrace();
            throw new IOException(e);
        }

    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    private void resetPools() {
        abvsBuilderPool.reset();
    }
}
