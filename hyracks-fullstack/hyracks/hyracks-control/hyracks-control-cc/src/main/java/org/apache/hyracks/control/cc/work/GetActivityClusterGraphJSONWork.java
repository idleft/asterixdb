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
package org.apache.hyracks.control.cc.work;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class GetActivityClusterGraphJSONWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final JobId jobId;
    private ObjectNode json;

    public GetActivityClusterGraphJSONWork(ClusterControllerService ccs, JobId jobId) {
        this.ccs = ccs;
        this.jobId = jobId;
    }

    @Override
    protected void doRun() throws Exception {

        ObjectMapper om = new ObjectMapper();
        JobRun run = ccs.getActiveRunMap().get(jobId);
        if (run == null) {
            run = ccs.getRunMapArchive().get(jobId);
            if (run == null) {
                json = om.createObjectNode();
                return;
            }
        }
        json = run.getActivityClusterGraph().toJSON();
    }

    public ObjectNode getJSON() {
        return json;
    }
}
