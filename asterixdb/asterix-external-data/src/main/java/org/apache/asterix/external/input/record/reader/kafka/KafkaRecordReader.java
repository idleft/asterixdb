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
package org.apache.asterix.external.input.record.reader.kafka;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaRecordReader implements IRecordReader<String> {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Deque<ConsumerRecord<String, String>> recordReaderBuffer;
    private final GenericRecord<String> record;

    private final int CONSUMER_TIMEOUT = 100;
    private String topic;

    public KafkaRecordReader(String bootstrapServer, String groupId, String autoCommitInterval, String topic) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", autoCommitInterval);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.topic = topic;

        recordReaderBuffer = new LinkedList<>();
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        record = new GenericRecord<>();
    }

    @Override
    public boolean hasNext() throws Exception {
        return true;
    }

    @Override
    public IRawRecord<String> next() throws IOException, InterruptedException {
        if(recordReaderBuffer.isEmpty()) {
            ConsumerRecords<String, String> polledMessages = kafkaConsumer.poll(CONSUMER_TIMEOUT);
            for (ConsumerRecord<String, String> message : polledMessages) {
                recordReaderBuffer.add(message);
            }
        }
        ConsumerRecord<String, String> headMessage = recordReaderBuffer.pollFirst();
        record.set(headMessage.value());
        return record;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        // do nothing
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) throws HyracksDataException {
        // do nothing
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}
