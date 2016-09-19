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

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

import java.util.ArrayList;

/**
 * Feed connection records the feed --> dataset mapping.
 */
public class FeedConnection implements IMetadataEntity<FeedConnection> {


    private static final long serialVersionUID = 1L;

    private String connectionId;
    private String dataverseName;
    private String feedName;
    private String datasetName;
    private ArrayList<String> appliedFunctions;

    public FeedConnection(String dataverseName, String feedName, String datasetName, ArrayList<String> appliedFunctions){
        this.dataverseName = dataverseName;
        this.feedName = feedName;
        this.datasetName = datasetName;
        this.appliedFunctions = appliedFunctions;
        this.connectionId = feedName+":"+datasetName;
    }

    public ArrayList<String> getAppliedFunctions(){
        return appliedFunctions;
    }

    public void setAppliedFunctions(ArrayList<String> appliedFunctions){
        this.appliedFunctions = appliedFunctions;
    }

    @Override
    public boolean equals(Object other){
        if (this == other) {
            return true;
        }
        if (!(other instanceof FeedConnection)) {
            return false;
        }
        return ((FeedConnection)other).getConnectionId().equals(connectionId);
    }

    @Override
    public FeedConnection addToCache(MetadataCache cache) {
        return null;
    }

    @Override
    public FeedConnection dropFromCache(MetadataCache cache) {
        return null;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public String getFeedName() {
        return feedName;
    }
}
