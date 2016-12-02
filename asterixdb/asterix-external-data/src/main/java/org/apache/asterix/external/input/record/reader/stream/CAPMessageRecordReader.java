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
        final int INIT_STATE = 0;
        final int START_OF_ELEMENT_NAME = 1;
        final int END_OF_ELEMENT_NAME = 2;
        final int START_OF_PROLOG = 3;
        final int IN_START_OF_ELEMENT_NAME = 4;
        final int IN_END_OF_ELEMENT_NAME = 5;
        final int IN_PROLOG = 6;
        final int IN_SCHEMA_DEFINITION = 7;
        boolean newRecordFormed = false;

        record.reset();
        int startPos = bufferPosn;
        int state = INIT_STATE;
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
                    state = START_OF_ELEMENT_NAME;
                    break;
                case '/':
                    if (state == START_OF_ELEMENT_NAME) {
                        state = END_OF_ELEMENT_NAME;
                    }
                    break;
                case '>':
                    if (state == IN_START_OF_ELEMENT_NAME) {
                        // add lvl
                        curLvl++;
                    } else if (state == IN_END_OF_ELEMENT_NAME) {
                        // decrease lvl
                        curLvl--;
                    } else if (state == START_OF_PROLOG) {
                        // document head
                    } else if (state == IN_SCHEMA_DEFINITION) {
                        // schema definition
                    }
                    if (curLvl == recordLvl && state == IN_END_OF_ELEMENT_NAME) {
                        int appendLength = bufferPosn + 1 - startPos;
                        record.append(inputBuffer, startPos, appendLength);
                        record.endRecord();
                        newRecordFormed = true;
                    }
                    state = INIT_STATE;
                    break;
                case '?':
                    if (state == START_OF_ELEMENT_NAME) {
                        state = START_OF_PROLOG;
                    } else if (state == IN_PROLOG) {
                        // in document head, do nothing.
                    }
                    break;
                case '!':
                    if (state == START_OF_ELEMENT_NAME) {
                        state = IN_SCHEMA_DEFINITION; // in schema definition
                    }
                    break;
                default:
                    if (state == START_OF_ELEMENT_NAME) {
                        if (curLvl == recordLvl) {
                            startPos = bufferPosn - 1;
                        }
                        state = IN_START_OF_ELEMENT_NAME; // in start element name
                    } else if (state == END_OF_ELEMENT_NAME) {
                        state = IN_END_OF_ELEMENT_NAME; // in end element name
                    } else if (state == START_OF_PROLOG) {
                        // inside document head
                        state = IN_PROLOG;
                    }
            }
            bufferPosn++;
        } while (!newRecordFormed);

        return newRecordFormed;
    }
}
