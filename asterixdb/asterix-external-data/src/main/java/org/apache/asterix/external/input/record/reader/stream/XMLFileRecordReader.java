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
    private int curLvl;

    public XMLFileRecordReader(AsterixInputStream inputStream) {
        super(inputStream);
        bufferPosn = 0;
        curLvl = 0;
    }

    private void moveCursor() {
        while (inputBuffer[bufferPosn++] != ExternalDataConstants.GT) {
            //do nothing, move to the end of document
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        newRecordFormed = false;
        record.reset();
        do {
            int startPos = bufferPosn;
            // chk whether there is enough data in buffer
            if (bufferPosn >= bufferLength) {
                startPos = bufferPosn = 0;
                bufferLength = reader.read(inputBuffer);
                if (bufferLength < 0) {
                    close();
                    return false;
                }
            }
            while (bufferPosn < bufferLength) {
                if (inputBuffer[bufferPosn] == ExternalDataConstants.LT
                        && inputBuffer[bufferPosn + 1] == ExternalDataConstants.SLASH) {
                    // end of an element
                    curLvl--;
                    if (curLvl == 0) {
                        moveCursor();
                        int appendLength = bufferPosn - startPos;
                        record.append(inputBuffer, startPos, appendLength);
                        record.endRecord();
                        newRecordFormed = true;
                        break;
                    }
                } else if (inputBuffer[bufferPosn] == ExternalDataConstants.LT
                        && inputBuffer[bufferPosn + 1] == ExternalDataConstants.QUESTION_MARK) {
                    // start of an record
                    startPos = bufferPosn;
                } else if (inputBuffer[bufferPosn] == ExternalDataConstants.LT) {
                    // start of an element
                    curLvl++;
                }
                bufferPosn++;
            }
        } while (!newRecordFormed);

        return newRecordFormed;
    }

    @Override
    public boolean stop() {
        try {
            reader.stop();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
