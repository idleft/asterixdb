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
package org.apache.asterix.external.feed.watch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.active.ActiveJob;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.feed.api.IFeedJoint;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.util.FeedUtils.JobType;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedConnectJobInfo extends ActiveJob {

    private static final long serialVersionUID = 1L;
    private final FeedConnectionId connectionId;
    private final Map<String, String> feedPolicy;

    private List<String> collectLocations;
    private List<String> computeLocations;
    private List<String> storageLocations;
    private int partitionStarts = 0;
    private List<String> targetDatasets;

    public FeedConnectInfo(EntityId entityId, JobId jobId, ActivityState state,
            JobSpecification spec,
            Map<String, String> feedPolicy) {
        super(entityId, jobId, state, JobType.FEED_CONNECT, spec);
        this.feedPolicy = feedPolicy;
        targetDatasets = new ArrayList();
    }

    public List<String> getCollectLocations() {
        return collectLocations;
    }

    public List<String> getComputeLocations() {
        return computeLocations;
    }

    public List<String> getStorageLocations() {
        return storageLocations;
    }

    public void setCollectLocations(List<String> collectLocations) {
        this.collectLocations = collectLocations;
    }

    public void setComputeLocations(List<String> computeLocations) {
        this.computeLocations = computeLocations;
    }

    public void setStorageLocations(List<String> storageLocations) {
        this.storageLocations = storageLocations;
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public void partitionStart() {
        partitionStarts++;
    }

    public boolean collectionStarted() {
        return partitionStarts == collectLocations.size();
    }

    public void addTargetDataset(String targetDataset){
        targetDatasets.add(targetDataset);
    }

    public void removeTargetDataset(String targetDataset){
        targetDatasets.remove(targetDataset);
    }

}
