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

import java.io.IOException;
import java.util.List;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.generator.DataGenerator;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class ExprTwitterRecordReader implements IRecordReader<char[]> {

    private CharArrayRecord record;
    private Long targetAmount;
    private long recordCounter;
    private DataGenerator dataGenerator;
    private DataGenerator.TweetMessageIterator tweetIterator;
    private char[] inputBuffer;

    public ExprTwitterRecordReader(long targetAmount) {
        this.record = new CharArrayRecord();
        this.targetAmount = targetAmount;
        this.recordCounter = 0;
        inputBuffer = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
        dataGenerator = new DataGenerator();
        tweetIterator = dataGenerator.new TweetMessageIterator(0);
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

    @Override
    public IRawRecord<char[]> next() throws IOException {
        String admRecord = tweetIterator.next().getAdmEquivalent(null);
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
