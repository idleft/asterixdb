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

package org.apache.asterix.metadata.entities;

import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;

/**
 * Metadata describing a dataset.
 */
public class Dataset implements IMetadataEntity<Dataset>, IDataset {

    /**
     * Dataset related operations
     */
    public static final byte OP_READ = 0x00;
    public static final byte OP_INSERT = 0x01;
    public static final byte OP_DELETE = 0x02;
    public static final byte OP_UPSERT = 0x03;

    private static final long serialVersionUID = 1L;
    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String datasetName;
    // Dataverse of ItemType for this dataset
    private final String itemTypeDataverseName;
    // Type of items stored in this dataset.
    private final String itemTypeName;
    private final String nodeGroupName;
    private final String compactionPolicy;
    private final Map<String, String> compactionPolicyProperties;
    private final DatasetType datasetType;
    private final IDatasetDetails datasetDetails;
    // Hints related to cardinatlity of dataset, avg size of tuples etc.
    private final Map<String, String> hints;
    private final int datasetId;
    // Type of pending operations with respect to atomic DDL operation
    private int pendingOp;

    // Dataverse of Meta ItemType for this dataset.
    private final String metaItemTypeDataverseName;
    // Type of Meta items stored in this dataset.
    private final String metaItemTypeName;

    public Dataset(String dataverseName, String datasetName, String itemTypeDataverseName, String itemTypeName,
            String nodeGroupName, String compactionPolicy, Map<String, String> compactionPolicyProperties,
            IDatasetDetails datasetDetails, Map<String, String> hints, DatasetType datasetType, int datasetId,
            int pendingOp) {
        this(dataverseName, datasetName, itemTypeDataverseName, itemTypeName, null, null, nodeGroupName,
                compactionPolicy, compactionPolicyProperties, datasetDetails, hints, datasetType, datasetId, pendingOp);
    }

    public Dataset(String dataverseName, String datasetName, String itemTypeDataverseName, String itemTypeName,
            String metaItemTypeDataverseName, String metaItemTypeName, String nodeGroupName, String compactionPolicy,
            Map<String, String> compactionPolicyProperties, IDatasetDetails datasetDetails, Map<String, String> hints,
            DatasetType datasetType, int datasetId, int pendingOp) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.itemTypeName = itemTypeName;
        this.itemTypeDataverseName = itemTypeDataverseName;
        this.metaItemTypeDataverseName = metaItemTypeDataverseName;
        this.metaItemTypeName = metaItemTypeName;
        this.nodeGroupName = nodeGroupName;
        this.compactionPolicy = compactionPolicy;
        this.compactionPolicyProperties = compactionPolicyProperties;
        this.datasetType = datasetType;
        this.datasetDetails = datasetDetails;
        this.datasetId = datasetId;
        this.pendingOp = pendingOp;
        this.hints = hints;
    }

    public Dataset(Dataset dataset) {
        this(dataset.dataverseName, dataset.datasetName, dataset.itemTypeDataverseName, dataset.itemTypeName,
                dataset.metaItemTypeDataverseName, dataset.metaItemTypeName, dataset.nodeGroupName,
                dataset.compactionPolicy, dataset.compactionPolicyProperties, dataset.datasetDetails, dataset.hints,
                dataset.datasetType, dataset.datasetId, dataset.pendingOp);
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getItemTypeName() {
        return itemTypeName;
    }

    public String getItemTypeDataverseName() {
        return itemTypeDataverseName;
    }

    public String getNodeGroupName() {
        return nodeGroupName;
    }

    public String getCompactionPolicy() {
        return compactionPolicy;
    }

    public Map<String, String> getCompactionPolicyProperties() {
        return compactionPolicyProperties;
    }

    public DatasetType getDatasetType() {
        return datasetType;
    }

    public IDatasetDetails getDatasetDetails() {
        return datasetDetails;
    }

    public Map<String, String> getHints() {
        return hints;
    }

    public int getDatasetId() {
        return datasetId;
    }

    public int getPendingOp() {
        return pendingOp;
    }

    public String getMetaItemTypeDataverseName() {
        return metaItemTypeDataverseName;
    }

    public String getMetaItemTypeName() {
        return metaItemTypeName;
    }

    public boolean hasMetaPart() {
        return metaItemTypeDataverseName != null && metaItemTypeName != null;
    }

    public void setPendingOp(int pendingOp) {
        this.pendingOp = pendingOp;
    }

    @Override
    public Dataset addToCache(MetadataCache cache) {
        return cache.addDatasetIfNotExists(this);
    }

    @Override
    public Dataset dropFromCache(MetadataCache cache) {
        return cache.dropDataset(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Dataset)) {
            return false;
        }
        Dataset otherDataset = (Dataset) other;
        if (!otherDataset.dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!otherDataset.datasetName.equals(datasetName)) {
            return false;
        }
        return true;
    }

    public boolean allow(ILogicalOperator topOp, byte operation) {
        return !hasMetaPart();
    }

    @Override
    public String toString() {
        return dataverseName + "." + datasetName;
    }
}
