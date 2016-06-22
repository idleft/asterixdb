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

import org.apache.asterix.builders.*;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.*;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.ophelpers.LongArrayList;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.eclipse.jetty.util.ajax.JSON;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import static org.apache.asterix.om.types.ATypeTag.UNION;

public class TweetParser extends AbstractDataParser implements IRecordDataParser<String> {
    //TODO Union type on record attribute
    // KNOWN Problem: null attribute cannot be eliminated by not(is-null($rec.geo))
    //NOTE: indicate non-optional, string can go through, but read will have problem
    // indicate non-optional, record with null field(not string) will not go through
    // define notnull, assign null value, sometimes can read
    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<IARecordBuilder, ATypeTag>(
            new RecordBuilderFactory());
    private final IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<IAsterixListBuilder, ATypeTag>(
            new ListBuilderFactory());
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<IMutableValueStorage, ATypeTag>(
            new AbvsBuilderFactory());
    private ARecordType recordType;
    private UTF8StringWriter utf8Writer = new UTF8StringWriter();

    public TweetParser(ARecordType recordType) {
        this.recordType = recordType;
        aPoint = new AMutablePoint(0, 0);
    }

    private void parseUnorderedList(JSONArray jArray, DataOutput output) throws IOException, JSONException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) getUnorderedListBuilder();

        unorderedListBuilder.reset(null);
        for (int iter1 = 0; iter1 < jArray.length(); iter1++) {
            itemBuffer.reset();
            if(writeField(jArray.get(iter1),null,itemBuffer.getDataOutput()))
                unorderedListBuilder.addItem(itemBuffer);
        }
        unorderedListBuilder.write(output, true);
    }

    private boolean writeField(Object fieldObj, IAType fieldType, DataOutput out)
            throws IOException {
        // save fieldType for closed type check
        String nstt;
        boolean writeResult = true;
        if(fieldType!=null){
            if(fieldType.getTypeTag() == BuiltinType.ASTRING.getTypeTag()){
                out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                utf8Writer.writeUTF8(fieldObj.toString(), out);
            }
            else if (fieldType.getTypeTag() == BuiltinType.AINT64.getTypeTag()){
                aInt64.setValue((long) fieldObj);
                int64Serde.serialize(aInt64, out);
            }
            else if(fieldType.getTypeTag() == BuiltinType.AINT32.getTypeTag()){
                out.write(BuiltinType.AINT32.getTypeTag().serialize());
                out.writeInt((Integer) fieldObj);
            }
            else if(fieldType.getTypeTag() == BuiltinType.ADOUBLE.getTypeTag()){
                out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                out.writeDouble((Double) fieldObj);
            }
            else if(fieldType.getTypeTag() == BuiltinType.ABOOLEAN.getTypeTag()){
                out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                out.writeBoolean((Boolean) fieldObj);
            }

            else
                writeResult = false;
        }
        else {
            try {
                if (fieldObj == JSONObject.NULL) {
                    nullSerde.serialize(ANull.NULL, out);
                } else if (fieldObj instanceof Integer) {
                    out.write(BuiltinType.AINT32.getTypeTag().serialize());
                    out.writeInt((Integer) fieldObj);
                } else if (fieldObj instanceof Boolean) {
                    out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                    out.writeBoolean((Boolean) fieldObj);
                } else if (fieldObj instanceof Double) {
                    out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                    out.writeDouble((Double) fieldObj);
                } else if (fieldObj instanceof Long) {
                    out.write(BuiltinType.AINT64.getTypeTag().serialize());
                    out.writeLong((Long) fieldObj);
                } else if (fieldObj instanceof String) {
                    out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                    utf8Writer.writeUTF8((String) fieldObj, out);
                } else if (fieldObj instanceof JSONArray) {
                    if (((JSONArray) fieldObj).length() != 0)
                        parseUnorderedList((JSONArray) fieldObj, out);
                    else
                        writeResult = false;
                } else if (fieldObj instanceof JSONObject) {
                    if (((JSONObject) fieldObj).length() != 0)
                        writeRecord((JSONObject) fieldObj, out, null);
                    else
                        writeResult = false;
                }
            } catch (JSONException e) {
                writeResult = false;
            }
        }
        return writeResult;
    }

    private int checkAttrNameIdx(String[] nameList, String name){
        int idx = 0;
        if(nameList!=null)
            for(String nln :nameList){
                if(name.equals(nln))
                    return idx;
                idx++;
            }
        return -1;
    }

    public void writeRecord(JSONObject obj, DataOutput out, ARecordType curRecType) throws IOException, JSONException {
        IAType[] curTypes = null;
        String[] curFNames = null;

        ArrayBackedValueStorage fieldValueBuffer = getTempBuffer();
        ArrayBackedValueStorage fieldNameBuffer = getTempBuffer();
        IARecordBuilder recBuilder = getRecordBuilder();

        int fieldN;
        int attrIdx;

        if(curRecType!=null){
            curTypes = curRecType.getFieldTypes();
            curFNames = curRecType.getFieldNames();
        }

        recBuilder.reset(curRecType);
        recBuilder.init();

        if(curRecType!=null && !curRecType.isOpen()){
            fieldN = curFNames.length;
            for (int iter1 = 0; iter1 < fieldN; iter1++) {
                fieldValueBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                if(obj.isNull(curFNames[iter1])){
                    if(curRecType.isClosedField(curFNames[iter1]))
                        throw new HyracksDataException("Closed field "+curFNames[iter1]+" has null value.");
                    else
                        continue;
                }
                else {
                 if (writeField(obj.get(curFNames[iter1]), curTypes[iter1], fieldOutput))
                    recBuilder.addField(iter1, fieldValueBuffer);
                }
            }
        } else{
                int closedFieldCount = 0;
                for (String attrName : JSONObject.getNames(obj)){
                    if(obj.isNull(attrName)||obj.length()==0) continue;
                    attrIdx = checkAttrNameIdx(curFNames, attrName);
                    fieldValueBuffer.reset();
                    fieldNameBuffer.reset();
                    DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                    if (writeField(obj.get(attrName), null, fieldOutput)) {
                        if(attrIdx == -1){
                            aString.setValue(attrName);
                            stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
                            recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                        }
                        else {
                            recBuilder.addField(attrIdx, fieldValueBuffer);
                            closedFieldCount++;
                        }
                    }
                }
            if(curRecType!=null && closedFieldCount<curFNames.length)
                throw new HyracksDataException("Non-null field is null");
        }

        // can use this to skip store null attr value
        recBuilder.write(out, true);
    }


    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    private IAsterixListBuilder getUnorderedListBuilder() {
        return listBuilderPool.allocate(ATypeTag.UNORDEREDLIST);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }


    @Override
    public void parse(IRawRecord<? extends String> record, DataOutput out) throws HyracksDataException {
        try {
            //TODO get rid of this temporary json
            resetPools();
            JSONObject jsObj = new JSONObject(record.get());
            writeRecord(jsObj, out, recordType);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
    }
}
