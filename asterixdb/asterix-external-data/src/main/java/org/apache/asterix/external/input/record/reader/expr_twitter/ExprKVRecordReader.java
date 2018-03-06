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
package org.apache.asterix.external.input.record.reader.expr_twitter;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.generator.DataGenerator;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.util.FeedLogManager;

import java.io.IOException;

public class ExprKVRecordReader implements IRecordReader<char[]> {

    private CharArrayRecord record;
    private Long targetAmount;
    private long recordCounter;
    private DataGenerator dataGenerator;

    public ExprKVRecordReader(long targetAmount) {
        this.record = new CharArrayRecord();
        this.targetAmount = targetAmount;
        this.recordCounter = 0;
        dataGenerator = new DataGenerator();
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public boolean hasNext() {
        if (recordCounter < targetAmount) {
            return true;
        } else {
            return false;
        }
    }

    private String makeRecord() {
        return "{ \"id\" : int64(\"" + recordCounter + "\"), \"val1\" : \"val" + recordCounter + "\" }";
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        String admRecord = makeRecord();
        record.set(admRecord.toCharArray());
        record.endRecord();
        recordCounter++;
        return record;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
        // do nothing
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        // do nothing
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }
}
