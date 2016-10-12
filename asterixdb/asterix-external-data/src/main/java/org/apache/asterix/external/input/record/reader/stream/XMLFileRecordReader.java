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

package org.apache.asterix.external.input.record.reader.stream;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;

import java.io.IOException;

public class XMLFileRecordReader extends StreamRecordReader {

    protected boolean newRecordFormed;
    protected boolean prevCharLF = false;

    public XMLFileRecordReader(AsterixInputStream inputStream) {
        super(inputStream);
    }

    @Override public boolean hasNext() throws IOException {
        newRecordFormed = false;
        record.reset();
        prevCharLF = false;
        while (!newRecordFormed) {
            if (done)
                return false;

            bufferLength = reader.read(inputBuffer);

            if((bufferLength == 1 && inputBuffer[0]==ExternalDataConstants.BYTE_LF) || bufferLength == -1){
                newRecordFormed = true;
//                record.endRecord();
            } else{
                record.append(inputBuffer, 0, bufferLength);
            }
        }
        return newRecordFormed;
    }
}
