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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
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
    private ArrayBackedValueStorage[] fieldValueBuffer;
    private ArrayBackedValueStorage[] attrNameBuffer;
    private RecordBuilder[] recBuilder;
    private ARecordType recordType;
    private UTF8StringWriter utf8Writer = new UTF8StringWriter();
    private UnorderedListBuilder unorderedListBuilder = new UnorderedListBuilder();

    public TweetParser(ARecordType recordType) {
        this.recordType = recordType;
        int lvl = 10;
        recBuilder = new RecordBuilder[lvl];
        fieldValueBuffer = new ArrayBackedValueStorage[lvl];
        attrNameBuffer = new ArrayBackedValueStorage[lvl];
        bufferInit(lvl);

        aPoint = new AMutablePoint(0, 0);
//        tweetSdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
    }

    private void bufferInit(Integer bufferLvl) {
        for (int iter1 = 0; iter1 < bufferLvl; iter1++) {
            fieldValueBuffer[iter1] = new ArrayBackedValueStorage();
            attrNameBuffer[iter1] = new ArrayBackedValueStorage();
            recBuilder[iter1] = new RecordBuilder();
        }
    }

    private void parseUnorderedList(JSONArray jArray, DataOutput output, Integer curLvl) throws IOException, JSONException {

        unorderedListBuilder.reset(new AUnorderedListType(null,""));
        byte tagByte = BuiltinType.ASTRING.getTypeTag().serialize();
        for (int iter1 = 0; iter1 < jArray.length(); iter1++) {
            writeField(jArray.get())
//            fieldValueBuffer[curLvl].reset();
//            final DataOutput listOutput = fieldValueBuffer[curLvl].getDataOutput();
//            listOutput.writeByte(tagByte);
//            utf8Writer.writeUTF8(jArray.getString(iter1), listOutput);
//            unorderedListBuilder.addItem(fieldValueBuffer[curLvl]);
        }
        unorderedListBuilder.write(output, true);
    }

    private boolean writeField(JSONObject obj, String fieldName, IAType fieldType, DataOutput out, Integer curLvl, boolean writeHead)
            throws IOException, ParseException {
        // save fieldType for closed type check
        try {
            Object fieldObj = obj.get(fieldName);
            if(fieldObj instanceof Integer){
                // process integer
                out.write(BuiltinType.AINT32.getTypeTag().serialize());
                out.writeInt((Integer) fieldObj);
            }
            else if (fieldObj instanceof Boolean){
                // process boolean value
                out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                out.writeBoolean((Boolean) fieldObj);
            }
            else if (fieldObj instanceof Double){
                // process double
                out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                out.writeDouble((Double) fieldObj);
            }
            else if (fieldObj instanceof Long){
                // process long
                out.write(BuiltinType.AINT64.getTypeTag().serialize());
                out.writeLong((Long) fieldObj);
            }
            else if (fieldObj instanceof String){
                out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                utf8Writer.writeUTF8((String) fieldObj, out);
            }
            else if (fieldObj instanceof JSONArray){
                // process array list
                return false;
//                if(((JSONArray) fieldObj).length() == 0)
//                    return false;
//                else
//                    parseUnorderedList((JSONArray) fieldObj, out, curLvl+1);
            }
            else if (fieldObj instanceof JSONObject){
                // process sub record
                if(((JSONObject) fieldObj).length() ==0)
                    return false;
                else
                    writeRecord((JSONObject)fieldObj, out, curLvl+1, null);
            }
        } catch (JSONException e) {
//            out.write(typeTag.serialize());
//            nullSerde.serialize(ANull.NULL, out);
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
        int fieldN;
        int attrIdx;

        if(curRecType!=null){
            curTypes = curRecType.getFieldTypes();
            curFNames = curRecType.getFieldNames();
        }

        recBuilder[curLvl].reset(curRecType);
        recBuilder[curLvl].init();

        if (curRecType==null || curRecType.isOpen()) {
            // do according to json type
            try{
            for (String attrName : JSONObject.getNames(obj)){
                if(obj.isNull(attrName)) continue;
                attrIdx = checkAttrNameIdx(curFNames, attrName);
                fieldValueBuffer[curLvl].reset();
                attrNameBuffer[curLvl].reset();
                DataOutput fieldOutput = fieldValueBuffer[curLvl].getDataOutput();
                if (writeField(obj, attrName, null, fieldOutput, curLvl+1,false)) {
                    if(attrIdx == -1){
                        aString.setValue(attrName);
                        stringSerde.serialize(aString, attrNameBuffer[curLvl].getDataOutput());
                        recBuilder[curLvl].addField(attrNameBuffer[curLvl], fieldValueBuffer[curLvl]);
                    }
                    else
                        recBuilder[curLvl].addField(attrIdx, fieldValueBuffer[curLvl]);
                }
            }}
            catch (NullPointerException e){
                e.printStackTrace();
            }
        } else {
            // do according to record Type
            fieldN = curFNames.length;
            for (int iter1 = 0; iter1 < fieldN; iter1++) {
                fieldValueBuffer[curLvl].reset();
                DataOutput fieldOutput = fieldValueBuffer[curLvl].getDataOutput();
                if (writeField(obj, curFNames[iter1], curTypes[iter1], fieldOutput, curLvl+1,true)) {
                    recBuilder[curLvl].addField(iter1, fieldValueBuffer[curLvl]);
                }
            }

        }
        recBuilder[curLvl].write(out, true);
    }

    @Override
    public void parse(IRawRecord<? extends String> record, DataOutput out) throws HyracksDataException {
        try {
            //TODO get rid of this temporary json
            JSONObject jsObj = new JSONObject(record.get());
            writeRecord(jsObj, out, 0, recordType);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
