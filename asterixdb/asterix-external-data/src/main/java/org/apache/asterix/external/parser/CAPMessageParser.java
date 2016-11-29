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
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

public class CAPMessageParser extends AbstractDataParser implements IRecordDataParser<char[]> {

    private static final String CAP_DATETIME_REGEX = "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d[-,+]\\d\\d:\\d\\d";
    private static final String CAP_BOOLEAN_TRUE = "true";
    private static final String CAP_BOOLEAN_FALSE = "false";
    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<>(
            new RecordBuilderFactory());
    private final IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<>(
            new ListBuilderFactory());
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<>(
            new AbvsBuilderFactory());
    private ARecordType recordType;
    private SAXParserFactory capMessageParserFactory;
    private SAXParser capMessageParser;
    private ArrayBackedValueStorage fieldNameBuffer;
    private ArrayList<IARecordBuilder> rbList;
    private ArrayList<ArrayBackedValueStorage> bufferList;
    private ListElementHandler listElementHandler;
    private CAPMessageHandler capMessageHandler;
    private HashMap<String, Integer> listElementNames;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");

    public CAPMessageParser(ARecordType recordType) throws HyracksDataException {
        this.recordType = recordType;
        bufferList = new ArrayList<>();
        rbList = new ArrayList<>();
        listElementNames = new HashMap<>();
        fieldNameBuffer = getTempBuffer();
        listElementHandler = new ListElementHandler(listElementNames);
        capMessageHandler = new CAPMessageHandler(recordType, bufferList, rbList);
        try {
            capMessageParserFactory = SAXParserFactory.newInstance();
            capMessageParser = capMessageParserFactory.newSAXParser();
        } catch (ParserConfigurationException | SAXException e) {
            throw new HyracksDataException(e);
        }
    }

    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
    }

    public void getElementNameList(IRawRecord<? extends char[]> record)
            throws IOException, SAXException {
        listElementNames.clear();
        listElementHandler.init();
        capMessageParser.parse(new ByteArrayInputStream(record.toString().getBytes()), listElementHandler);
    }

    private void clearBufferInit() {
        resetPools();
        bufferList.clear();
        rbList.clear();
        capMessageHandler.clear();
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws HyracksDataException {
        try {
            clearBufferInit();
            getElementNameList(record);
            bufferList.add(getTempBuffer());
            rbList.add(getRecordBuilder());
            rbList.get(0).reset(recordType);
            rbList.get(0).init();
            capMessageParser.parse(new ByteArrayInputStream(record.toString().getBytes()), capMessageHandler);
            rbList.get(0).write(out, true);
        } catch (SAXException | IOException e) {
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

    private void setValueBasedOnTypeTag(String content, ATypeTag fieldTypeTag, DataOutput out)
            throws HyracksDataException {
        try {
            switch (fieldTypeTag) {
                case STRING:
                    aString.setValue(content);
                    stringSerde.serialize(aString, out);
                    break;
                case INT64:
                    aInt64.setValue(Long.valueOf(content));
                    int64Serde.serialize(aInt64, out);
                    break;
                case INT32:
                    aInt32.setValue(Integer.valueOf(content));
                    int32Serde.serialize(aInt32, out);
                    out.write(BuiltinType.AINT32.getTypeTag().serialize());
                    break;
                case DOUBLE:
                    aDouble.setValue(Double.valueOf(content));
                    doubleSerde.serialize(aDouble, out);
                    break;
                case BOOLEAN:
                    booleanSerde.serialize(ABoolean.valueOf(content.equals(CAP_BOOLEAN_TRUE)), out);
                    break;
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private class ListElementHandler extends DefaultHandler {

        public HashMap<String, Integer> listElementNames;
        Stack<String> curPathStack;
        String fullPathName;
        String preElementName;

        public ListElementHandler(HashMap<String, Integer> listElementNames) {
            preElementName = "";
            this.listElementNames = listElementNames;
            curPathStack = new Stack<>();
        }

        private void init() {
            preElementName = "";
            curPathStack.clear();
        }

        private String getFullPathName() {
            return String.join(".", curPathStack);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attrs) {
            curPathStack.push(qName);
            if (qName.equals(preElementName)) {
                fullPathName = getFullPathName();
                listElementNames.put(fullPathName,
                        listElementNames.get(fullPathName) == null ? 2 : listElementNames.get(fullPathName) + 1);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
            curPathStack.pop();
            preElementName = qName;
        }
    }

    private class CAPMessageHandler extends DefaultHandler {

        ArrayList<ArrayBackedValueStorage> bufferList;
        ARecordType recordType;
        ArrayList<IARecordBuilder> rbList;
        String curEleName;
        String curFullPathName;
        IAType curFieldType;
        Stack<Boolean> recordTypeMarker;
        Stack<IAType> fieldTypeTracker;
        Stack<String> nestedListTracker;
        Stack<String> curPathStack;
        ArrayList<IAsterixListBuilder> listBuilder;
        int skipLvl;
        int curLvl;

        public CAPMessageHandler(ARecordType recordType, ArrayList<ArrayBackedValueStorage> bufferList,
                ArrayList<IARecordBuilder> rbList) throws HyracksDataException {
            this.bufferList = bufferList;
            this.recordType = recordType;
            this.rbList = rbList;
            recordTypeMarker = new Stack<>();
            nestedListTracker = new Stack<>();
            fieldTypeTracker = new Stack<>();
            curPathStack = new Stack<>();
            listBuilder = new ArrayList<>();
            fieldTypeTracker.push(recordType);
            curLvl = 0;
            skipLvl = 1;
        }

        public void clear() {
            recordTypeMarker.clear();
            nestedListTracker.clear();
            curPathStack.clear();
            listBuilder.clear();
            fieldTypeTracker.clear();
            fieldTypeTracker.push(recordType);
            curLvl = 0;
            curFieldType = recordType;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
            curPathStack.push(qName);
            if (curPathStack.size() <= skipLvl) {
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
            recordTypeMarker.push(true);
            curFieldType = curFieldType == null ? null : ((ARecordType) curFieldType).getFieldType(qName);
            fieldTypeTracker.push(curFieldType);
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            String content = new String(ch, start, length).trim();
            if (content.length() == 0 || curPathStack.size() <= skipLvl) {
                // skip empty chars before/after element name
                return;
            }
            try {
                if (curFieldType != null) {
                    setValueBasedOnTypeTag(content, curFieldType.getTypeTag(), bufferList.get(curLvl).getDataOutput());
                } else {
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
                }
                recordTypeMarker.pop();
                recordTypeMarker.push(false);
            } catch (IOException | ParseException | NullPointerException e) {
                throw new SAXException(e);
            }
        }

        private void handleNestedOrderedList(String fullPathName) throws HyracksDataException {
            if (nestedListTracker.size() == 0 || !nestedListTracker.peek().equals(fullPathName)) {
                nestedListTracker.push(fullPathName);
                if (listBuilder.size() < nestedListTracker.size()) {
                    listBuilder.add(getOrderedListBuilder());
                    listBuilder.get(listBuilder.size() - 1).reset(null);
                }
            }
            listBuilder.get(nestedListTracker.size() - 1).addItem(bufferList.get(curLvl));
            listElementNames.put(fullPathName, listElementNames.get(fullPathName) - 1);
            if (listElementNames.get(fullPathName) == 0) {
                bufferList.get(curLvl).reset();
                listBuilder.get(nestedListTracker.size() - 1).write(bufferList.get(curLvl).getDataOutput(), true);
                listBuilder.get(nestedListTracker.size() - 1).reset(null);
                rbList.get(curLvl - 1).addField(fieldNameBuffer, bufferList.get(curLvl));
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            try {
                if (curPathStack.size() <= skipLvl) {
                    curPathStack.pop();
                    return;
                }
                fieldTypeTracker.pop();
                curFieldType = fieldTypeTracker.peek();
                aString.setValue(qName);
                fieldNameBuffer.reset();
                stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
                Boolean curRecordType = recordTypeMarker.pop();
                Integer fieldNameIdx = ((ARecordType)curFieldType).getFieldIndex(qName);
                if (qName.equals(recordType.getFieldNames()[0]))
                    rbList.get(curLvl - 1).addField(0, bufferList.get(curLvl));
                else {
                    if (curRecordType) {
                        rbList.get(curLvl).write(bufferList.get(curLvl).getDataOutput(), true);
                    }
                    curFullPathName = String.join(".", curPathStack);
                    if (listElementNames.keySet().contains(curFullPathName)) {
                        handleNestedOrderedList(curFullPathName);
                    } else {
                        rbList.get(curLvl - 1).addField(fieldNameBuffer, bufferList.get(curLvl));
                    }
                }
                curPathStack.pop();
                curLvl--;
            } catch (HyracksDataException e) {
                throw new SAXException(e);
            }

        }
    }

}
