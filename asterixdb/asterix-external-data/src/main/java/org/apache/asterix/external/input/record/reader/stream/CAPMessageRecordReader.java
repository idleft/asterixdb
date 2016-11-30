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

public class CAPMessageRecordReader extends StreamRecordReader {

    protected boolean newRecordFormed;
    private int curLvl;
    private int recordLvl;

    public CAPMessageRecordReader(AsterixInputStream inputStream, String collection) {
        super(inputStream);
        bufferPosn = 0;
        curLvl = 0;
        if (collection != null) {
            this.recordLvl = "true".equals(collection) ? 1 : 0;
        } else {
            this.recordLvl = 0;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        newRecordFormed = false;
        record.reset();
        int startPos = bufferPosn;
        Integer curStatus = 0;
        do {
            // chk whether there is enough data in buffer
            if (bufferPosn >= bufferLength) {
                if (curLvl > 0) {
                    record.append(inputBuffer, startPos, bufferPosn - startPos);
                }
                startPos = bufferPosn = 0;
                bufferLength = reader.read(inputBuffer);
                if (bufferLength < 0) {
                    close();
                    return false;
                }
            }
            // TODO: simplify the state numbers (xikui)
            switch (inputBuffer[bufferPosn]) {
                case '<':
                    curStatus = 1;
                    break;
                case '/':
                    if (curStatus == 1) {
                        curStatus = 2;
                    }
                    break;
                case '>':
                    if (curStatus == 4) {
                        // add lvl
                        curLvl++;
                    } else if (curStatus == 5) {
                        // decrease lvl
                        curLvl--;
                    } else if (curStatus == 3) {
                        // document head
                    } else if (curStatus == 7) {
                        // schema definition
                    }
                    if (curLvl == recordLvl && curStatus == 5) {
                        int appendLength = bufferPosn + 1 - startPos;
                        record.append(inputBuffer, startPos, appendLength);
                        record.endRecord();
                        newRecordFormed = true;
                    }
                    curStatus = 0;
                    break;
                case '?':
                    if (curStatus == 1) {
                        curStatus = 3;
                    } else if (curStatus == 6) {
                        // in document head, do nothing.
                    }
                    break;
                case '!':
                    if (curStatus == 1) {
                        curStatus = 7; // in schema definition
                    }
                    break;
                default:
                    if (curStatus == 1) {
                        if (curLvl == recordLvl) {
                            startPos = bufferPosn - 1;
                        }
                        curStatus = 4; // in start element name
                    } else if (curStatus == 2) {
                        curStatus = 5; // in end element name
                    } else if (curStatus == 3) {
                        // inside document head
                        curStatus = 6;
                    }
            }
            bufferPosn++;
        } while (!newRecordFormed);

        return newRecordFormed;
    }
}
