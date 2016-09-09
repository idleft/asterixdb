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

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.om.types.ARecordType;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import java.io.DataOutput;
import java.io.IOException;

public class XMLFileParser extends AbstractDataParser implements IRecordDataParser<char[]> {

    ARecordType recordType;
    ADMDataParser admDataParser;
    CharArrayRecord charArrayRecord;

    public XMLFileParser(ARecordType recordType, ADMDataParser admDataParser){
        this.recordType = recordType;
        this.admDataParser = admDataParser;
        charArrayRecord = new CharArrayRecord();
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws IOException {
        try {
            JSONObject xmlObj = XML.toJSONObject(record.toString());
            String jsonStr = xmlObj.getJSONObject("alert").toString(4);
            charArrayRecord.set(jsonStr.toCharArray());
            charArrayRecord.endRecord();
            admDataParser.parse(charArrayRecord,out);
        } catch (JSONException e) {
            new IOException(e);
        }
    }
}
