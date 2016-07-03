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

import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.XMLFileParser;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.xml.sax.SAXException;

/**
 * Created by Xikui on 6/28/16.
 */
public class XMLFileParserFactory extends AbstractRecordStreamParserFactory<char[]> {
    @Override
    public IRecordDataParser<char[]> createRecordParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return createParser();
    }

    private XMLFileParser createParser() throws HyracksDataException{
        try {
            return new XMLFileParser(recordType);
        } catch (SAXException e) {
            throw new HyracksDataException(e.getMessage());
        }
    }

    @Override
    public Class<?> getRecordClass() {
        return char[].class;
    }

    @Override
    public IStreamDataParser createInputStreamParser(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        return createParser();
    }

    @Override
    public void setMetaType(ARecordType metaType) {

    }
}