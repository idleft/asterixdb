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

import com.sun.org.apache.xerces.internal.impl.xs.XMLSchemaLoader;
import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.ws.commons.schema.*;
import org.apache.ws.commons.schema.constants.Enum;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.bind.Element;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;

/**
 * Created by Xikui on 6/28/16.
 */
public class XMLFileParser extends AbstractDataParser implements IRecordDataParser<char[]> {

    private IARecordBuilder rb;
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<>(
            new AbvsBuilderFactory());
    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<>(
            new RecordBuilderFactory());
    private ARecordType recordType;
    private SAXParser xmlParser;
    private ArrayList<String> listAttrNames;
    private ArrayBackedValueStorage fieldNameBuffer;
    private ArrayList<ArrayBackedValueStorage> bufferList;
    private ArrayList<IARecordBuilder> rbList;
    HashMap<String, ElementType> dataTypeMap;
    private UnorderedListBuilder ulb;
    String xmlSchemaPath;



    private enum ElementType{
        COMPLEX,SIMPLE,COMPLEX_N,SIMPLE_N
    }

    public XMLFileParser(ARecordType recordType, String schemaPath) throws ParserConfigurationException, SAXException {
        xmlSchemaPath = "/Volumes/Storage/Users/Xikui/Work/XMLParser/CAPS_Schema.xsd";
        this.recordType = recordType;
        xmlParser = SAXParserFactory.newInstance().newSAXParser();
        fieldNameBuffer = getBuffer();
        bufferList = new ArrayList<>();
        rbList = new ArrayList<>();
        initSchemaMap();
    }

    private void complexTypeMapBuilder(XmlSchemaElement element, HashMap<String, ElementType> dataTypeMap) {
        if (element.getSchemaType() instanceof XmlSchemaComplexType) {
            XmlSchemaComplexType schemaComplexType = (XmlSchemaComplexType) element.getSchemaType();
            XmlSchemaSequence elementSequence = (XmlSchemaSequence) schemaComplexType.getParticle();
            Iterator iterator = elementSequence.getItems().getIterator();
            while (iterator.hasNext()) {
                Object nextObj = iterator.next();
                if(nextObj instanceof XmlSchemaAny){
                    continue;
                    //temporarily skip XmlSchemaAnyType
                }
                XmlSchemaElement curElement = (XmlSchemaElement) nextObj;
                complexTypeMapBuilder(curElement, dataTypeMap);
            }
            dataTypeMap.put(element.getName(), element.getMaxOccurs()>1?ElementType.COMPLEX_N: ElementType.COMPLEX);
        } else {
            dataTypeMap.put(element.getName(), element.getMaxOccurs()>1? ElementType.SIMPLE_N: ElementType.SIMPLE);
        }
    }

    private void initSchemaMap() throws SAXException {
        XmlSchema xmlSchema = null;
        XmlSchemaCollection coll = new XmlSchemaCollection();
        try {
            xmlSchema = coll.read(new StreamSource(new FileInputStream(xmlSchemaPath)), null);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        dataTypeMap = new HashMap<>();
        complexTypeMapBuilder((XmlSchemaElement) xmlSchema.getItems().getItem(0), dataTypeMap);
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws IOException {
        try {
            JSONObject xmlObj = XML.toJSONObject(record.toString());
            String jsonStr = xmlObj.toString(4);
            jsonStr.toCharArray();
        } catch (JSONException e) {
            new IOException(e);
        }
//        String strRecord = record.toString();
//        resetPools();
//        DefaultHandler handler = new AsterixSAXHandler(bufferList, recordType, rbList, 1);
//        try {
//            bufferList.add(getBuffer());
//            rbList.add(getRecordBuilder());
//            rbList.get(0).reset(recordType);
//            rbList.get(0).init();
//            xmlParser.parse(new InputSource(new StringReader(strRecord)), handler);
//            rbList.get(0).write(out, true);
//        } catch (SAXException e) {
//            e.printStackTrace();
//            throw new IOException(e);
//        }
    }

    private ArrayBackedValueStorage getBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    private void resetPools() {
        abvsBuilderPool.reset();
        recordBuilderPool.reset();
        bufferList.clear();
    }

    private class AsterixSAXHandler extends DefaultHandler {

        ArrayList<ArrayBackedValueStorage> bufferList;
        ARecordType recordType;
        ArrayList<IARecordBuilder> rbList;
        String curEleName;
        Stack<Boolean> recordTypeTracker;
        boolean preList = false;
        int curLvl;
        int skipLvlN, maxLvlN;

        public AsterixSAXHandler(ArrayList<ArrayBackedValueStorage> bufferList, ARecordType recordType,
                ArrayList<IARecordBuilder> rbList, int skipLvlN) {
            super();
            this.bufferList = bufferList;
            this.recordType = recordType;
            this.skipLvlN = skipLvlN;
            this.rbList = rbList;
            recordTypeTracker = new Stack<>();
            maxLvlN = 0;
            curLvl = 0;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
            if (qName.equals("alert"))
                return;
            try {
                if (listAttrNames.contains(qName)) {
                    if (preList == false) {
                        // do nothing
                        ulb.reset(null);
                    } else {
                        if (qName != curEleName) {
                            ulb.write(bufferList.get(curLvl).getDataOutput(), true);
                            rbList.get(curLvl - 1).addField(fieldNameBuffer, bufferList.get(curLvl));
                            ulb.reset(null);
                        }
                    }
                }
            } catch (HyracksDataException e) {
                throw new SAXException(e);
            }
            curLvl++;
            curEleName = qName;
            if (bufferList.size() < curLvl + 1) {
                bufferList.add(getBuffer());
                rbList.add(getRecordBuilder());
            }

            bufferList.get(curLvl).reset();
            rbList.get(curLvl).reset(null);
            rbList.get(curLvl).init();
            recordTypeTracker.push(true);
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            String curEleVal = new String(ch, start, length).trim();
            if (curEleVal.length() == 0) {
                return;
            }
            try {
                aString.setValue(curEleVal);
                stringSerde.serialize(aString, bufferList.get(curLvl).getDataOutput());
                recordTypeTracker.pop();
                recordTypeTracker.push(false);
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            try {
                if (qName.equals("alert"))
                    return;
                aString.setValue(qName);
                fieldNameBuffer.reset();
                stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
                Boolean curRecordType = recordTypeTracker.pop();
                if (qName.equals("identifier"))
                    rbList.get(curLvl - 1).addField(0, bufferList.get(curLvl));
                else {
                    if (curRecordType) {
                        rbList.get(curLvl).write(bufferList.get(curLvl).getDataOutput(), true);
                    } else {
                        rbList.get(curLvl - 1).addField(fieldNameBuffer, bufferList.get(curLvl));
                    }
                }
                curLvl--;
            } catch (HyracksDataException e) {
                throw new SAXException(e);
            }
        }
    }
}
