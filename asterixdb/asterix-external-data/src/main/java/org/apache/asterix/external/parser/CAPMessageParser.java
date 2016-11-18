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

import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.apache.velocity.runtime.directive.Parse;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

public class CAPMessageParser extends AbstractDataParser implements IRecordDataParser<char[]> {

    private static final String CAP_DATETIME_REGEX = "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d[-,+]\\d\\d:\\d\\d";
    private static final String CAP_BOOLEAN_TRUE = "true";
    private static final String CAP_BOOLEAN_FALSE = "false";
    public static final String  CAP_ROOT_ELEMENT_NAME = "alert";
    public static final String CAP_PKEY_NAME = "identifier";
    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<>(
            new RecordBuilderFactory());
    private final IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<>(
            new ListBuilderFactory());
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<>(
            new AbvsBuilderFactory());
    private ARecordType recordType;
    private SAXParserFactory capMessageParserFactory;
    private SAXParser capMessageParser;
    private CAPMessageHandler capMessageHandler;
    private ArrayBackedValueStorage fieldNameBuffer;
    private ArrayList<IARecordBuilder> rbList;
    private ArrayList<ArrayBackedValueStorage> bufferList;
    private HashMap<String, Integer> listElementNames;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");

    public CAPMessageParser(ARecordType recordType) throws HyracksDataException {
        this.recordType = recordType;
        bufferList = new ArrayList<>();
        rbList = new ArrayList<>();
        fieldNameBuffer = getTempBuffer();
        try {
            capMessageParserFactory = SAXParserFactory.newInstance();
            capMessageParser = capMessageParserFactory.newSAXParser();
        } catch (ParserConfigurationException | SAXException e) {
            new HyracksDataException(e);
        }
    }

    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
    }

    public HashMap<String, Integer> getElementNameList(IRawRecord<? extends char[]> record)
            throws IOException, SAXException {
        HashMap<String, Integer> elementNames = new HashMap<>();
        ListElementHandler elementNameHandler = new ListElementHandler(elementNames);
        capMessageParser.parse(new ByteArrayInputStream(record.toString().getBytes()), elementNameHandler);
        return elementNames;
    }

    private void clearBuffer() {
        resetPools();
        bufferList.clear();
        rbList.clear();
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws HyracksDataException {
        try {
            clearBuffer();
            capMessageParser = capMessageParserFactory.newSAXParser();
            capMessageHandler = new CAPMessageHandler(recordType, bufferList, rbList);
            listElementNames = getElementNameList(record);
            bufferList.add(getTempBuffer());
            rbList.add(getRecordBuilder());
            rbList.get(0).reset(recordType);
            rbList.get(0).init();
            capMessageParser.parse(new ByteArrayInputStream(record.toString().getBytes()), capMessageHandler);
            rbList.get(0).write(out, true);
        } catch (SAXException | IOException | ParserConfigurationException e) {
            throw new HyracksDataException(e);
        }
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    private IAsterixListBuilder getOrderedListBuilder() {
        return listBuilderPool.allocate(ATypeTag.ORDEREDLIST);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    private class ListElementHandler extends DefaultHandler {

        private void init() {
            preElementName = "";
            listElementNames.clear();
        }

        public HashMap<String, Integer> listElementNames;
        String preElementName;

        public ListElementHandler(HashMap<String, Integer> listElementNames) {
            preElementName = "";
            this.listElementNames = listElementNames;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attrs) {
            if (qName.equals(preElementName)) {
                listElementNames.put(qName, listElementNames.get(qName) == null ? 2 : listElementNames.get(qName) + 1);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
            preElementName = qName;
        }
    }

    private class CAPMessageHandler extends DefaultHandler {

        ArrayList<ArrayBackedValueStorage> bufferList;
        ARecordType recordType;
        ArrayList<IARecordBuilder> rbList;
        String curEleName;
        Stack<Boolean> recordTypeTracker;
        Stack<String> nestedListTracker;
        ArrayList<IAsterixListBuilder> listBuilder;
        int curLvl;

        public CAPMessageHandler(ARecordType recordType, ArrayList<ArrayBackedValueStorage> bufferList,
                ArrayList<IARecordBuilder> rbList) throws HyracksDataException {
            super();
            this.bufferList = bufferList;
            this.recordType = recordType;
            this.rbList = rbList;
            recordTypeTracker = new Stack<>();
            nestedListTracker = new Stack<>();
            listBuilder = new ArrayList<>();
            curLvl = 0;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
            if (qName.equals(CAP_ROOT_ELEMENT_NAME)) {
                return;
            }
            curLvl++;
            curEleName = qName;
            if (bufferList.size() < curLvl + 1) {
                bufferList.add(getTempBuffer());
                rbList.add(getRecordBuilder());
            }

            bufferList.get(curLvl).reset();
            rbList.get(curLvl).reset(null);
            rbList.get(curLvl).init();
            recordTypeTracker.push(true);
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            String content = new String(ch, start, length).trim();
            if (content.length() == 0) {
                // skip empty chars before/after element name
                return;
            }
            try {
                if (Ints.tryParse(content) != null) {
                    // ints
                    aInt32.setValue(Integer.valueOf(content));
                    int32Serde.serialize(aInt32, bufferList.get(curLvl).getDataOutput());
                } else if (Doubles.tryParse(content) != null) {
                    // doubles
                    aDouble.setValue(Double.valueOf(content));
                    doubleSerde.serialize(aDouble, bufferList.get(curLvl).getDataOutput());
                } else if (content.equals(CAP_BOOLEAN_TRUE) || content.equals(CAP_BOOLEAN_FALSE)) {
                    // boolean
                    booleanSerde.serialize(ABoolean.valueOf(content.equals(CAP_BOOLEAN_TRUE)),
                            bufferList.get(curLvl).getDataOutput());
                } else if (Pattern.matches(CAP_DATETIME_REGEX, content)) {
                    // datetime
                    aDateTime.setValue(dateFormat.parse(content).getTime());
                    datetimeSerde.serialize(aDateTime, bufferList.get(curLvl).getDataOutput());
                } else {
                    // string
                    aString.setValue(content);
                    stringSerde.serialize(aString, bufferList.get(curLvl).getDataOutput());
                }
                recordTypeTracker.pop();
                recordTypeTracker.push(false);
            } catch (IOException | ParseException | NullPointerException e) {
                throw new SAXException(e);
            }
        }

        private void handleNestedOrderedList(String qName) throws HyracksDataException{
            if (nestedListTracker.size() == 0 || !nestedListTracker.peek().equals(qName)) {
                nestedListTracker.push(qName);
                if (listBuilder.size() < nestedListTracker.size()) {
                    listBuilder.add(getOrderedListBuilder());
                    listBuilder.get(listBuilder.size() - 1).reset(null);
                }
            }
            listBuilder.get(nestedListTracker.size() - 1).addItem(bufferList.get(curLvl));
            listElementNames.put(qName, listElementNames.get(qName) - 1);
            if (listElementNames.get(qName) == 0) {
                bufferList.get(curLvl).reset();
                listBuilder.get(nestedListTracker.size() - 1).write(bufferList.get(curLvl).getDataOutput(),
                        true);
                listBuilder.get(nestedListTracker.size() - 1).reset(null);
                rbList.get(curLvl - 1).addField(fieldNameBuffer, bufferList.get(curLvl));
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            try {
                if (qName.equals(CAP_ROOT_ELEMENT_NAME))
                    return;
                aString.setValue(qName);
                fieldNameBuffer.reset();
                stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
                Boolean curRecordType = recordTypeTracker.pop();
                if (qName.equals(CAP_PKEY_NAME))
                    rbList.get(curLvl - 1).addField(0, bufferList.get(curLvl));
                else {
                    if (curRecordType) {
                        rbList.get(curLvl).write(bufferList.get(curLvl).getDataOutput(), true);
                    }
                    if (listElementNames.keySet().contains(qName)) {
                        handleNestedOrderedList(qName);
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

