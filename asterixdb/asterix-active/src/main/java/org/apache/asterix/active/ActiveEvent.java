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
package org.apache.asterix.active;

import org.apache.hyracks.api.job.JobId;

public class ActiveEvent {

    public static final byte JOB_CREATED = 0x00;
    public static final byte JOB_START = 0x01;
    public static final byte JOB_FINISH = 0x02;
    public static final byte PARTITION_EVENT = 0x03;

    private final JobId jobId;
    private final EntityId entityId;
    private final byte eventKind;
    private final Object eventObject;


    public ActiveEvent(JobId jobId, byte eventKind, EntityId entityId, Object eventObject) {
        this.jobId = jobId;
        this.entityId = entityId;
        this.eventKind = eventKind;
        this.eventObject = eventObject;
    }

    public ActiveEvent(JobId jobId, byte eventKind, EntityId entityId) {
        this(jobId, eventKind, entityId, null);
    }

    public JobId getJobId() {
        return jobId;
    }

    public EntityId getEntityId() {
        return entityId;
    }

    public byte getEventKind() {
        return eventKind;
    }

    public Object getEventObject() {
        return eventObject;
    }

    @Override
    public String toString() {
        switch (eventKind) {
            case JOB_CREATED:
                return jobId + " Created";
            case JOB_START:
                return jobId + " Started";
            case JOB_FINISH:
                return jobId + " Finished";
            case PARTITION_EVENT:
                return jobId + " Partition Event";
            default:
                return "Unknown event kind";
        }
    }
}