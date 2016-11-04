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
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.mortbay.util.ajax.JSON;

import java.io.DataOutput;
import java.io.IOException;

public class CAPMessageParser extends AbstractDataParser implements IRecordDataParser<char[]> {

    private ADMDataParser admDataParser;
    private CharArrayRecord charArrayRecord;
    private String DOCUMENT_HEADER = "<?xml version = \"1.0\" encoding = \"UTF-8\"?>\n";

    public CAPMessageParser(ADMDataParser admDataParser) {
        this.admDataParser = admDataParser;
        charArrayRecord = new CharArrayRecord();
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws IOException {
        try {
            JSONObject xmlObj = XML.toJSONObject(DOCUMENT_HEADER + record.toString());
            String jsonStr = xmlObj.getJSONObject(xmlObj.names().getString(0)).toString();
            charArrayRecord.set(jsonStr.toCharArray());
            charArrayRecord.endRecord();
            admDataParser.parse(charArrayRecord,out);
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }
}
