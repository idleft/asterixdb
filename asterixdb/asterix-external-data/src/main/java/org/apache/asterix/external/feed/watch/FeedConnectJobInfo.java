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
import org.apache.avro.generic.GenericData;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedConnectJobInfo extends ActiveJob {

    private static final long serialVersionUID = 1L;

    private List<String> intakeLocations;
    private List<String> computeLocations;
    private List<String> storageLocations;
    private List<String> targetDatasets;

    public FeedConnectJobInfo(EntityId entityId, JobId jobId, ActivityState state, JobSpecification spec) {
        super(entityId, jobId, state, JobType.FEED_CONNECT, spec);
        targetDatasets = new ArrayList();
    }


    public List<String> getComputeLocations() {
        return computeLocations;
    }

    public void setComputeLocations(List<String> computeLocations) {
        this.computeLocations = computeLocations;
    }

    public List<String> getStorageLocations() {
        return storageLocations;
    }

    public void setStorageLocations(List<String> storageLocations) {
        this.storageLocations = storageLocations;
    }

    public List<String> getIntakeLocations() {
        return intakeLocations;
    }

    public void setIntakeLocations(List<String> intakeLocations) {
        this.intakeLocations = intakeLocations;
    }
}
