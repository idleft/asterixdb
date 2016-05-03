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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import static org.apache.asterix.om.types.ATypeTag.UNION;

public class TweetParser extends AbstractDataParser implements IRecordDataParser<String> {
    //TODO Union type on record attribute
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

    private void parseUnorderedList(JSONArray jArray, DataOutput output, Integer curLvl) throws IOException, JSONException, ParseException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) getUnorderedListBuilder();

        unorderedListBuilder.reset(null);
        for (int iter1 = 0; iter1 < jArray.length(); iter1++) {
            itemBuffer.reset();
            if(writeField(jArray.get(iter1),null,itemBuffer.getDataOutput(),curLvl+1))
                unorderedListBuilder.addItem(itemBuffer);
        }
            unorderedListBuilder.write(output, true);
    }

    private boolean writeField(Object fieldObj, IAType fieldType, DataOutput out, Integer curLvl)
            throws IOException, ParseException {
        // save fieldType for closed type check
        try {
            if(fieldObj instanceof Integer){
                out.write(BuiltinType.AINT32.getTypeTag().serialize());
                out.writeInt((Integer) fieldObj);
            }
            else if (fieldObj instanceof Boolean){
                out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                out.writeBoolean((Boolean) fieldObj);
            }
            else if (fieldObj instanceof Double){
                out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                out.writeDouble((Double) fieldObj);
            }
            else if (fieldObj instanceof Long){
                out.write(BuiltinType.AINT64.getTypeTag().serialize());
                out.writeLong((Long) fieldObj);
            }
            else if (fieldObj instanceof String){
                out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                utf8Writer.writeUTF8((String) fieldObj, out);
            }
            else if (fieldObj instanceof JSONArray){
                if(((JSONArray) fieldObj).length() == 0)
                    return false;
                else
                    parseUnorderedList((JSONArray) fieldObj, out, curLvl);
            }
            else if (fieldObj instanceof JSONObject){
                if(((JSONObject) fieldObj).length() ==0)
                    return false;
                else
                    writeRecord((JSONObject)fieldObj, out, curLvl+1, null);
            }
        } catch (JSONException e) {
            return false;
        }
        return true;
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

    public void writeRecord(JSONObject obj, DataOutput out, Integer curLvl, ARecordType curRecType) throws IOException, JSONException, ParseException {
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

        if (curRecType==null || curRecType.isOpen()) {
            try{
            for (String attrName : JSONObject.getNames(obj)){
                if(obj.isNull(attrName)) continue;
                attrIdx = checkAttrNameIdx(curFNames, attrName);
                fieldValueBuffer.reset();
                fieldNameBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                if (writeField(obj.get(attrName), null, fieldOutput, curLvl+1)) {
                    if(attrIdx == -1){
                        aString.setValue(attrName);
                        stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
                        recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                    }
                    else
                        recBuilder.addField(attrIdx, fieldValueBuffer);
                }
            }}
            catch (NullPointerException e){
                e.printStackTrace();
            }
        } else {
            fieldN = curFNames.length;
            for (int iter1 = 0; iter1 < fieldN; iter1++) {
                fieldValueBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                if (writeField(obj.get(curFNames[iter1]), curTypes[iter1], fieldOutput, curLvl+1)) {
                    recBuilder.addField(iter1, fieldValueBuffer);
                }
            }

        }
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
            writeRecord(jsObj, out, 0, recordType);
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
