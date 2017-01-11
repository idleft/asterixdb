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

import java.util.Map;

import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.stream.factory.LocalFSInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketClientInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketServerInputStreamFactory;
import org.apache.asterix.external.provider.StreamRecordReaderProvider;
import org.apache.asterix.external.provider.StreamRecordReaderProvider.Format;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class StreamRecordReaderFactory implements IRecordReaderFactory<char[]> {

    private static final long serialVersionUID = 1L;
    protected IInputStreamFactory streamFactory;
    protected Map<String, String> configuration;
    protected Format format;
    protected String recordReaderName;
    private String[] recordReaderNames = { ExternalDataConstants.ALIAS_LOCALFS_ADAPTER,
            ExternalDataConstants.ALIAS_SOCKET_ADAPTER, ExternalDataConstants.SOCKET,
            ExternalDataConstants.STREAM_SOCKET_CLIENT };

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public Class<?> getRecordClass() {
        return char[].class;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint()
            throws HyracksDataException, AlgebricksException {
        return streamFactory.getPartitionConstraint();
    }

    @Override
    public void configure(Map<String, String> configuration) throws HyracksDataException, AlgebricksException {
        this.configuration = configuration;
        recordReaderName = configuration.get(ExternalDataConstants.KEY_READER);
        switch (recordReaderName) {
            case ExternalDataConstants.ALIAS_LOCALFS_ADAPTER:
                this.streamFactory = new LocalFSInputStreamFactory();
                break;
            case ExternalDataConstants.ALIAS_SOCKET_ADAPTER:
            case ExternalDataConstants.SOCKET:
                this.streamFactory = new SocketServerInputStreamFactory();
                break;
            case ExternalDataConstants.STREAM_SOCKET_CLIENT:
                this.streamFactory = new SocketClientInputStreamFactory();
                break;
        }
        streamFactory.configure(configuration);
        format = StreamRecordReaderProvider.getReaderFormat(configuration);
    }

    @Override
    public IRecordReader<? extends char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        return StreamRecordReaderProvider.createRecordReader(format, streamFactory.createInputStream(ctx, partition),
                configuration);
    }

    @Override
    public String[] getRecordReaderNames() {
        return recordReaderNames;
    }
}
