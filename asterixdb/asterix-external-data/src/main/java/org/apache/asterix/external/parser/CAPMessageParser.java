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
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.avro.generic.GenericData;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class CAPMessageParser extends AbstractDataParser implements IRecordDataParser<char[]> {

    private static final String CAP_DATETIME_REGEX = "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:"
            + "\\d\\d:\\d\\d[-,+]\\d\\d:\\d\\d";
    private static final String CAP_DOUBLE_REGEX = "\\d+\\.\\d+";
    private static final String CAP_INTEGER_REGEX = "^[1-9]\\d*";
    private static final String CAP_BOOLEAN_TRUE = "true";
    private static final String CAP_BOOLEAN_FALSE = "false";

    public static final int SMART_BUILDER_STATE_RECORD = 1;
    public static final int SMART_BUILDER_STATE_LIST = 2;
    public static final int SMART_BUILDER_STATE_VALUE = 3;

    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<>(
            new RecordBuilderFactory());
    private final IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<>(
            new ListBuilderFactory());
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<>(
            new AbvsBuilderFactory());
    private ARecordType recordType;
    private SAXParser capMessageSAXParser;
    private ComplexBuilder complexBuilder;
    private ArrayList<ComplexBuilder> builderList;
    private ListElementHandler listElementHandler;
    private CAPMessageHandler capMessageHandler;
    private HashMap<String, Integer> listElementNames;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");

    public CAPMessageParser(ARecordType recordType) throws HyracksDataException {
        this.recordType = recordType;
        builderList = new ArrayList<>();
        listElementNames = new HashMap<>();
        listElementHandler = new ListElementHandler(listElementNames);
        capMessageHandler = new CAPMessageHandler(recordType, builderList);
        try {
            capMessageSAXParser = SAXParserFactory.newInstance().newSAXParser();
        } catch (ParserConfigurationException | SAXException e) {
            throw new HyracksDataException(e);
        }
    }

    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
    }

    public void getElementNameList(IRawRecord<? extends char[]> record) throws IOException, SAXException {
        listElementNames.clear();
        listElementHandler.init();
        capMessageSAXParser.parse(new ByteArrayInputStream(record.toString().getBytes()), listElementHandler);
    }

    private void clearBufferInit() {
        resetPools();
        builderList.clear();
        capMessageHandler.clear();
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws HyracksDataException {
        try {
            clearBufferInit();
            getElementNameList(record);
            builderList.add(new ComplexBuilder());
            complexBuilder = builderList.get(0);
            complexBuilder.setState(SMART_BUILDER_STATE_RECORD, recordType);
            capMessageSAXParser.parse(
                    new ByteArrayInputStream(record.toString().replaceAll("[\\r|\\n|\\r\\n]+", " ").getBytes()),
                    capMessageHandler);
            complexBuilder.write(out);
        } catch (SAXException | IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private class ListElementHandler extends DefaultHandler {

        Deque<String> curPathStack;
        String fullPathName;
        String preElementName;
        String parentPath;
        private Map<String, Integer> listElementNames;

        public ListElementHandler(Map<String, Integer> listElementNames) {
            preElementName = "";
            this.listElementNames = listElementNames;
            curPathStack = new LinkedList<>();
        }

        private void init() {
            preElementName = "";
            curPathStack.clear();
        }

        private String getFullPath() {
            return String.join(".", curPathStack);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attrs) {
            parentPath = String.join(".", curPathStack);
            curPathStack
                    .push(qName + (listElementNames.containsKey(parentPath) ? listElementNames.get(parentPath) : 1));
            if (qName.equals(preElementName)) {
                fullPathName = getFullPath();
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

        private ArrayBackedValueStorage elementContentBuffer;
        private ARecordType recordType;
        private List<ComplexBuilder> builderList;
        private String curFullPathName;
        private IAType curFieldType;
        private String parentPath;
        private Deque<String> curPathStack;
        private ArrayBackedValueStorage fieldNameBuffer;
        // In order to handle nested list. This gives each list element an unique path
        private Map<String, Integer> listElementCounter;

        int skipLvl;
        int curLvl;
        String qName;
        String content;

        public CAPMessageHandler(ARecordType recordType, List<ComplexBuilder> builderList) throws HyracksDataException {
            this.recordType = recordType;
            this.builderList = builderList;
            curPathStack = new LinkedList<>();
            listElementCounter = new HashMap<>();
            elementContentBuffer = new ArrayBackedValueStorage();
            fieldNameBuffer = new ArrayBackedValueStorage();
            curLvl = 0;
            skipLvl = 1;
            content = "";
        }

        public void clear() {
            curPathStack.clear();
            listElementCounter.clear();
            curLvl = 0;
            content = "";
            curFieldType = recordType;
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
                        break;
                    case DOUBLE:
                        aDouble.setValue(Double.valueOf(content));
                        doubleSerde.serialize(aDouble, out);
                        break;
                    case BOOLEAN:
                        booleanSerde.serialize(ABoolean.valueOf(content.equals(CAP_BOOLEAN_TRUE)), out);
                        break;
                    case DATETIME:
                        aDateTime.setValue(dateFormat.parse(content).getTime());
                        datetimeSerde.serialize(aDateTime, out);
                        break;
                    default:
                        throw new HyracksDataException("Data type not supported");
                }
            } catch (ParseException | IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
            this.qName = qName;
            this.content = "";
            parentPath = String.join(".", curPathStack);
            curPathStack.push(
                    qName + (listElementCounter.containsKey(parentPath) ? listElementCounter.get(parentPath) : 1));
            if (curPathStack.size() <= skipLvl) {
                return;
            }
            // initialize the buffer and builder
            curLvl++;
            curFullPathName = String.join(".", curPathStack);
            listElementCounter.put(curFullPathName,
                    listElementCounter.containsKey(curFullPathName) ? listElementCounter.get(curFullPathName) + 1 : 1);
            if (builderList.size() <= curLvl) {
                builderList.add(new ComplexBuilder());
            }
            elementContentBuffer.reset();
            try {
                // assign builder type if any
                curFieldType = curFieldType == null ? null : ((ARecordType) curFieldType).getFieldType(qName);
                if (listElementNames.containsKey(curFullPathName)) {
                    builderList.get(curLvl).setState(SMART_BUILDER_STATE_LIST, curFieldType);
                } else {
                    builderList.get(curLvl).setState(SMART_BUILDER_STATE_RECORD, curFieldType);
                }
            } catch (HyracksDataException e) {
                throw new SAXException(e);
            }
        }

        @Override
        public void characters(char ch[], int start, int length) throws SAXException {
            content = new String(ch, start, length).trim() + content;
            if (content.length() == 0 || curPathStack.size() <= skipLvl) {
                // skip empty chars before/after element name
                return;
            }
            try {
                builderList.get(curLvl).setState(SMART_BUILDER_STATE_VALUE, curFieldType);
            } catch (HyracksDataException e) {
                throw new SAXException(e);
            }
        }

        private void processContent() throws HyracksDataException, ParseException {
            if (curFieldType != null) {
                setValueBasedOnTypeTag(content, curFieldType.getTypeTag(), elementContentBuffer.getDataOutput());
            } else {
                if (Pattern.matches(CAP_INTEGER_REGEX, content)) {
                    // ints
                    aInt32.setValue(Integer.valueOf(content));
                    int32Serde.serialize(aInt32, elementContentBuffer.getDataOutput());
                } else if (Pattern.matches(CAP_DOUBLE_REGEX, content)) {
                    // doubles
                    aDouble.setValue(Double.valueOf(content));
                    doubleSerde.serialize(aDouble, elementContentBuffer.getDataOutput());
                } else if (content.equals(CAP_BOOLEAN_TRUE) || content.equals(CAP_BOOLEAN_FALSE)) {
                    // boolean
                    booleanSerde.serialize(ABoolean.valueOf(content.equals(CAP_BOOLEAN_TRUE)),
                            elementContentBuffer.getDataOutput());
                } else if (Pattern.matches(CAP_DATETIME_REGEX, content)) {
                    // datetime
                    aDateTime.setValue(dateFormat.parse(content).getTime());
                    datetimeSerde.serialize(aDateTime, elementContentBuffer.getDataOutput());
                } else {
                    // string
                    aString.setValue(content);
                    stringSerde.serialize(aString, elementContentBuffer.getDataOutput());
                }
            }
            content = "";
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            try {
                if (curPathStack.size() <= skipLvl) {
                    curPathStack.pop();
                    return;
                }
                ComplexBuilder curBuilder = builderList.get(curLvl);
                ComplexBuilder parentBuilder = builderList.get(curLvl - 1);
                if (content.length() != 0) {
                    processContent();
                }
                aString.setValue(qName);
                fieldNameBuffer.reset();
                stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
                IAType parentRecordType = parentBuilder.getDataType();
                int fieldNameIdx = parentRecordType == null ? -1
                        : ((ARecordType) parentRecordType).getFieldIndex(qName);
                curFullPathName = String.join(".", curPathStack);
                if (listElementNames.containsKey(curFullPathName)) {
                    handleNestedOrderedList(curFullPathName, fieldNameIdx, fieldNameBuffer, elementContentBuffer);
                } else if (curBuilder.getState() == SMART_BUILDER_STATE_VALUE) {
                    parentBuilder.addAttribute(fieldNameIdx, fieldNameBuffer, elementContentBuffer);
                } else {
                    if (curBuilder.resetRecordFlag) {
                        elementContentBuffer.reset();
                        nullSerde.serialize(ANull.NULL, elementContentBuffer.getDataOutput());
                        parentBuilder.addAttribute(fieldNameIdx, fieldNameBuffer, elementContentBuffer);
                    } else {
                        curBuilder.write();
                        parentBuilder.addAttribute(fieldNameIdx, fieldNameBuffer, curBuilder.getBuffer());
                        curBuilder.setResetFlag();
                    }
                }
                curFieldType = parentRecordType;
                elementContentBuffer.reset();
                curPathStack.pop();
                curLvl--;
            } catch (HyracksDataException | ParseException e) {
                throw new SAXException(e);
            }
        }

        /*
        * OrderedList Handling in CAP Message
        *
        * In order to process list easier, we scan the document twice. First pass finds all list elements. Second
        * pass construct list elements using list builder. Once all elements in one list are consumed, add this list
        * to its parent's record builder. Each list element is assigned a unique path to handle nested list case.
        * */

        private void handleNestedOrderedList(String fullPathName, int fieldNameIdx, IValueReference fieldNameBuffer,
                IValueReference contentBuffer) throws HyracksDataException {
            builderList.get(curLvl).addListElement(contentBuffer);
            listElementNames.put(fullPathName, listElementNames.get(fullPathName) - 1); //update list element counter
            if (listElementNames.get(fullPathName) == 0) {
                // if it's the last element of list, write to upper lvl
                builderList.get(curLvl).flushListToBuffer();
                builderList.get(curLvl - 1).addAttribute(fieldNameIdx, fieldNameBuffer,
                        builderList.get(curLvl).getBuffer());
            }
        }
    }

    private class ComplexBuilder {

        private IARecordBuilder recordBuilder;
        private IAsterixListBuilder listBuilder;
        private ArrayBackedValueStorage buffer;
        private IAType dataType;
        private boolean resetRecordFlag, resetListFlag;

        private int state;

        public ComplexBuilder() {
            this.buffer = getTempBuffer();
            state = SMART_BUILDER_STATE_VALUE;
            dataType = null;
            resetRecordFlag = true;
            resetListFlag = true;
        }

        public void setState(int state, IAType dataType) throws HyracksDataException {
            this.state = state;
            this.dataType = dataType;
        }

        public int getState() {
            return this.state;
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

        public IAType getDataType() {
            return this.dataType;
        }

        public void addAttribute(int fieldIdx, IValueReference fieldNameBuffer, IValueReference fieldValueBuffer)
                throws HyracksDataException {
            if (recordBuilder == null) {
                recordBuilder = getRecordBuilder();
            }
            if (resetRecordFlag) {
                recordBuilder.reset((ARecordType) dataType);
                recordBuilder.init();
                resetRecordFlag = false;
            }
            if (fieldIdx == -1) {
                recordBuilder.addField(fieldNameBuffer, fieldValueBuffer);
            } else {
                recordBuilder.addField(fieldIdx, fieldValueBuffer);
            }
        }

        public void flushListToBuffer() throws HyracksDataException {
            buffer.reset();
            listBuilder.write(buffer.getDataOutput(), true);
            setResetFlag();
        }

        public void addListElement(IValueReference fieldValueBuffer) throws HyracksDataException {
            if (listBuilder == null) {
                listBuilder = getOrderedListBuilder();
            }
            if (resetListFlag) {
                listBuilder.reset(null);
                resetListFlag = false;
            }
            if (fieldValueBuffer.getLength() == 0) {
                write();
                listBuilder.addItem(buffer);
                resetRecordFlag = true;
            } else {
                listBuilder.addItem(fieldValueBuffer);
            }
        }

        public void write() throws HyracksDataException {
            buffer.reset();
            recordBuilder.write(buffer.getDataOutput(), true);
        }

        public void write(DataOutput output) throws HyracksDataException {
            recordBuilder.write(output, true);
        }

        public IValueReference getBuffer() {
            return this.buffer;
        }

        public void setResetFlag() {
            resetRecordFlag = true;
            resetListFlag = true;
        }
    }

}
