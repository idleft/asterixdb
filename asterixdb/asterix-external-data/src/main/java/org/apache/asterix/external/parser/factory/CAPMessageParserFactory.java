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

package org.apache.asterix.external.parser.factory;

import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.external.parser.CAPMessageParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.util.Map;

public class CAPMessageParserFactory implements IRecordDataParserFactory<char[]> {

    private ARecordType recordType;
    private Map<String, String> configuration;

    @Override
    public IRecordDataParser<char[]> createRecordParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return new CAPMessageParser(new ADMDataParser(recordType, ExternalDataUtils.getDataSourceType(configuration)
                .equals(IExternalDataSourceFactory.DataSourceType.STREAM)));
    }

    @Override
    public void configure(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public Class<?> getRecordClass() {
        return char[].class;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        // do nothing
    }
}
