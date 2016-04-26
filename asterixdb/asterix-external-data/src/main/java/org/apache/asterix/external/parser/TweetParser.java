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
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TweetParser extends AbstractDataParser implements IRecordDataParser<String> {

    private ArrayBackedValueStorage[] fieldValueBuffer;
    private RecordBuilder[] recBuilder;
    private ARecordType recordType;
    private UTF8StringWriter utf8Writer = new UTF8StringWriter();
    //    private UnorderedListBuilder unorderedListBuilder = new UnorderedListBuilder();
    private SimpleDateFormat tweetSdf;

    private AMutablePoint aPoint;


    public TweetParser(ARecordType recordType) {
        this.recordType = recordType;
        int lvl = 2;
        recBuilder = new RecordBuilder[lvl];
        fieldValueBuffer = new ArrayBackedValueStorage[lvl];
//        recordBuffer = new ArrayBackedValueStorage[lvl];
        bufferInit(lvl);

        aPoint = new AMutablePoint(0, 0);
        tweetSdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
    }

    private void bufferInit(Integer bufferLvl) {
        for (int iter1 = 0; iter1 < bufferLvl; iter1++) {
            fieldValueBuffer[iter1] = new ArrayBackedValueStorage();
            recBuilder[iter1] = new RecordBuilder();
        }
    }

//    private void parseUnorderedList(long[] uolist, DataOutput output) throws IOException {
//        if(uolist.length>0)
//            System.out.println("hello!");
//        unorderedListBuilder.reset(new AUnorderedListType(BuiltinType.AINT64,""));
//        byte tagByte = BuiltinType.AINT64.getTypeTag().serialize();
//        for (int iter1 = 0; iter1<uolist.length; iter1++){
//            inFieldValueBuffer.reset();
//            final DataOutput listOutput = inFieldValueBuffer.getDataOutput();
//            listOutput.writeByte(tagByte);
//            parseInt64(uolist[iter1],listOutput);
//            unorderedListBuilder.addItem(inFieldValueBuffer);
//        }
//        unorderedListBuilder.write(output, false);
//    }

    private void writeField(JSONObject obj, String fieldName, IAType fieldType, DataOutput out, Integer curLvl)
            throws IOException, JSONException, ParseException {
        ATypeTag typeTag = fieldType.getTypeTag();
        switch (typeTag) {
            case INT64:
                out.write(typeTag.serialize());
                out.writeLong(obj.getLong(fieldName));
                break;
            case INT32:
                out.write(typeTag.serialize());
                out.writeInt(obj.getInt(fieldName));
                break;
            case STRING:
                out.write(typeTag.serialize());
                utf8Writer.writeUTF8(obj.getString(fieldName), out);
                break;
            case BOOLEAN:
                out.write(typeTag.serialize());
                out.writeBoolean(obj.getBoolean(fieldName));
                break;
            case DATETIME:
                out.write(typeTag.serialize());
                out.writeLong(tweetSdf.parse(obj.getString(fieldName)).getTime());
                break;
            case RECORD:
//                String subJStr = obj.getString(fieldName);
                writeRecord(obj.getString(fieldName), out, curLvl + 1, (ARecordType) fieldType);
                break;
            case POINT:
                String pointField = obj.getString(fieldName);
                if ("null" != pointField) {
                    aPoint.setValue(obj.getJSONObject(fieldName).getJSONArray("coordinates").getDouble(0),
                            obj.getJSONObject(fieldName).getJSONArray("coordinates").getDouble(1));
                    pointSerde.serialize(aPoint, out);
                }
                break;
            case UNORDEREDLIST:
                break;
        }
    }

    public void writeRecord(String objStr, DataOutput out, Integer curLvl, ARecordType curRecType) throws IOException, JSONException, ParseException {
        JSONObject obj = new JSONObject(objStr);
        IAType[] curTypes = curRecType.getFieldTypes();
        String[] curFNames = curRecType.getFieldNames();
        int fieldN = curFNames.length;
        recBuilder[curLvl].reset(curRecType);
        recBuilder[curLvl].init();
        for (int iter1 = 0; iter1 < fieldN; iter1++) {
            fieldValueBuffer[curLvl].reset();
            DataOutput fieldOutput = fieldValueBuffer[curLvl].getDataOutput();
            writeField(obj, curFNames[iter1], curTypes[iter1], fieldOutput, curLvl);
            recBuilder[curLvl].addField(iter1, fieldValueBuffer[curLvl]);
        }
        recBuilder[curLvl].write(out, true);
    }

    @Override
    public void parse(IRawRecord<? extends String> record, DataOutput out) throws HyracksDataException {
        try {
            writeRecord(record.get(), out, 0, recordType);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
