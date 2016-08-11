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

/**
 * Created by Xikui on 6/28/16.
 */
public class XMLFileRecordReader extends StreamRecordReader {

    protected boolean newRecordFormed;
    protected int startPosn = 0;
    protected boolean prevCharLF = false;

    public XMLFileRecordReader(AsterixInputStream inputStream) {
        super(inputStream);
    }

    @Override public boolean hasNext() throws IOException {
        newRecordFormed = false;
        record.reset();
        //        startPosn = 0;
        prevCharLF = false;
        while (!newRecordFormed) {
            if (done)
                return false;

            if (bufferPosn >= bufferLength) {
                // load new buffer
                startPosn = bufferPosn = 0;
                bufferLength = reader.read(inputBuffer);
                if (bufferLength <= 0) {
                    if (record.size() > 0) {
                        record.endRecord();
                        return true;
                    } else {
                        close();
                        return false;
                    }
                }
            }

            // using empty line as new record
            newRecordFormed = true;
//            while (bufferPosn < bufferLength) {
//                if (inputBuffer[bufferPosn] == ExternalDataConstants.LF) {
//                    if (prevCharLF) {
//                        bufferPosn++;
//                        newRecordFormed = true;
//                        break;
//                    } else
//                        prevCharLF = true;
//                } else
//                    prevCharLF = false;
//                bufferPosn++;
//            }
//            if (bufferPosn > startPosn) {
                record.append(inputBuffer, startPosn, bufferLength);
//                startPosn = bufferPosn;
//            }
        }
        return newRecordFormed;
    }
}
