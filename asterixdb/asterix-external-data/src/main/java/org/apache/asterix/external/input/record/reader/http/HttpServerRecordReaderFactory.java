/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.asterix.external.input.record.reader.http;

import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.util.Map;

public class HttpServerRecordReaderFactory implements IRecordReaderFactory<String> {

    private String serverPortNumber;
    private static final int HTTP_SERVER_NUM = 1;
    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;


    @Override
    public IRecordReader<? extends String> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        return new HttpServerRecordReader(serverPortNumber);
    }

    @Override
    public Class<?> getRecordClass() {
        return String.class;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint()
            throws AlgebricksException, HyracksDataException {
        clusterLocations = IExternalDataSourceFactory.getPartitionConstraints(clusterLocations, HTTP_SERVER_NUM);
        return clusterLocations;
    }

    @Override
    public void configure(Map<String, String> configuration) throws AlgebricksException, HyracksDataException {
        this.serverPortNumber = configuration.get(ExternalDataConstants.READER_HTTP_READER_PORT);
    }
}
