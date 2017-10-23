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

package org.apache.asterix.lang.common.statement;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.external.feed.watch.StatsSubscriber;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class StartFeedStatement implements Statement {

    public static final String WAIT_FOR_COMPLETION = "wait-for-completion-feed";
    private Identifier dataverseName;
    private Identifier feedName;

    public StartFeedStatement(Pair<Identifier, Identifier> feedNameComp) {
        dataverseName = feedNameComp.first;
        feedName = feedNameComp.second;
    }

    @Override
    public byte getKind() {
        return Kind.START_FEED;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getFeedName() {
        return feedName;
    }

    public void addLogDataset(ICcApplicationContext appCtx, String failedDatasetName, IMetadataLockManager lockManager,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc,
            IActiveEntityEventsListener listener, String dvName) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // drop the previous failed record dataset. there will be a case the restart feed, partition changed.
        // so, the datset must be removed.
        MetadataLockUtil.dropDatasetBegin(lockManager, metadataProvider.getLocks(), dvName,
                dvName + "." + failedDatasetName);
        Dataset ds = metadataProvider.findDataset(dvName, failedDatasetName);
        if (ds != null) {
            ds.drop(metadataProvider, new MutableObject<>(mdTxnCtx), new ArrayList<>(), new MutableBoolean(true),
                    new MutableObject<>(JobUtils.ProgressState.NO_PROGRESS), hcc, true);
            mdTxnCtx = metadataProvider.getMetadataTxnContext();
        }

        // get failed record log file
        StatsSubscriber feedStatsSubscriber = new StatsSubscriber(listener);
        listener.refreshStats(2000);
        feedStatsSubscriber.sync();
        String feedStats = listener.getStats();

        ObjectMapper statsMapper = new ObjectMapper();
        JsonNode statsNode = statsMapper.readTree(feedStats);

        StringBuilder failedRecordsFilePath = new StringBuilder();

        // Add all log files for the external dataset
        for (int iter1 = 0; iter1 < statsNode.size(); iter1++) {
            if (iter1 > 0) {
                failedRecordsFilePath.append(',');
            }
            JsonNode partitionNode = statsNode.get(iter1);
            failedRecordsFilePath.append(partitionNode.get(ExternalDataConstants.FEED_NODE_ID_NAME).asText()
                    + "://" + partitionNode.get(ExternalDataConstants.FAILED_RECORD_LOG_LOCATION_NAME).asText());
        }

        // create a new failed record dataset
        MetadataLockUtil.createDatasetBegin(lockManager, metadataProvider.getLocks(), dvName, "Metadata",
                "Metadata" + "." + MetadataBuiltinEntities.ANY_OBJECT_RECORD_TYPE.getTypeName(), dvName,
                dvName + "." + null, null, null, dvName + "." + failedDatasetName, true);
        Map<String, String> failedDatasetProp = new LinkedHashMap<>();
        failedDatasetProp.put(ExternalDataConstants.KEY_FORMAT, ExternalDataConstants.FORMAT_JSON);
        failedDatasetProp.put(ExternalDataConstants.KEY_PATH, failedRecordsFilePath.toString());
        Map<String, String> hints = new HashMap<>();
        String ngName = DatasetUtil.configureNodegroupForDataset(appCtx, hints, dvName, failedDatasetName,
                metadataProvider);
        metadataProvider.findDataset(dvName, failedDatasetName);
        Dataset failedRecordDataset = new Dataset(dvName, failedDatasetName, "Metadata",
                MetadataBuiltinEntities.ANY_OBJECT_RECORD_TYPE.getTypeName(), ngName,
                GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME, GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES,
                new ExternalDatasetDetails(ExternalDataConstants.ALIAS_LOCALFS_ADAPTER, failedDatasetProp, new Date(),
                        DatasetConfig.TransactionState.COMMIT),
                hints, DatasetConfig.DatasetType.EXTERNAL, DatasetIdFactory.generateDatasetId(),
                MetadataUtil.PENDING_ADD_OP);
        MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), failedRecordDataset);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }
}
