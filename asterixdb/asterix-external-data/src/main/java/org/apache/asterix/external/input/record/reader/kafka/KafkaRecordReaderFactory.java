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

import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.util.Map;

public class KafkaRecordReaderFactory implements IRecordReaderFactory<String> {

    private static final long serialVersionUID = 1L;

    private transient AlgebricksAbsolutePartitionConstraint algebricksPartitionConstraint;
    private final int numberOfConsumer = 1;
    private Map<String, String> configuration;

    private final String KAFKA_CONFIG_BOOTSTRAP_SERVER = "bootstrap.servers";
    private final String KAFKA_CONFIG_GROUP_ID = "group.id";
    private final String KAFKA_CONFIG_INTERVAL = "auto.commit.interval.ms";
    private final String KAFKA_CONFIG_TOPIC = "topic";

    @Override
    public IRecordReader<? extends String> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        return new KafkaRecordReader(configuration.get(KAFKA_CONFIG_BOOTSTRAP_SERVER),
                configuration.get(KAFKA_CONFIG_GROUP_ID), configuration.get(KAFKA_CONFIG_INTERVAL),
                configuration.get(KAFKA_CONFIG_TOPIC));
    }

    @Override
    public Class<? extends String> getRecordClass() {
        return String.class;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint()
            throws AlgebricksException, HyracksDataException {
        algebricksPartitionConstraint = IExternalDataSourceFactory
                .getPartitionConstraints(algebricksPartitionConstraint, numberOfConsumer);
        return algebricksPartitionConstraint;
    }

    @Override
    public void configure(Map<String, String> configuration) throws AlgebricksException, HyracksDataException {
        this.configuration = configuration;
    }
}
