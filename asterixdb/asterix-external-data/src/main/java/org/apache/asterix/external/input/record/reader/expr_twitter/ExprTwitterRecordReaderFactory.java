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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExprTwitterRecordReaderFactory implements IRecordReaderFactory<char[]> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();

    private static final List<String> recordReaderNames = Collections.singletonList("expr_twitter");

    private long requriedAmount;
    private String[] ingestionLocation;

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return new AlgebricksAbsolutePartitionConstraint(ingestionLocation);
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration) throws HyracksDataException {
        this.requriedAmount = Long.valueOf(configuration.getOrDefault("expr_amount", "0"));
        String assignedIntakeLocation = configuration.get("ingestion-location");
        if (ingestionLocation == null) {
            ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
            ingestionLocation = appCtx.getClusterStateManager().getClusterLocations().getLocations();
        } else {
            List<String> ncs =
                    RuntimeUtils.getAllNodeControllers((ICcApplicationContext) serviceCtx.getApplicationContext());
            if (!ncs.contains(ingestionLocation)) {
                throw new HyracksDataException("host" + ingestionLocation + StringUtils.join(ncs, ", "));
            }
            ingestionLocation = new String[] { assignedIntakeLocation };
        }
        LOGGER.log(Level.INFO, "Expr twitter generator requested " + requriedAmount);
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public IRecordReader<? extends char[]> createRecordReader(IHyracksTaskContext ctx, int partition) {
        IRecordReader<char[]> exprTwttierRecordReader = new ExprTwitterRecordReader(requriedAmount);
        return exprTwttierRecordReader;
    }

    @Override
    public Class<? extends char[]> getRecordClass() {
        return char[].class;
    }
}
