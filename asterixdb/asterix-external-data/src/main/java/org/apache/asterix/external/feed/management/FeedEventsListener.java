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
package org.apache.asterix.external.feed.management;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveLifecycleListener;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEventSubscriber;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.external.feed.watch.FeedEventSubscriber;
import org.apache.asterix.external.feed.watch.NoOpSubscriber;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;

public class FeedEventsListener extends ActiveEntityEventsListener {
    // constants
    private static final Logger LOGGER = Logger.getLogger(FeedEventsListener.class.getName());
    // members
    private final ICcApplicationContext appCtx;
    private final String[] sources;
    private final List<IActiveEventSubscriber> subscribers;
    private int numRegistered;
    private List<JobId> registeredJobIds;

    public FeedEventsListener(ICcApplicationContext appCtx, EntityId entityId, List<IDataset> datasets,
            String[] sources) {
        this.appCtx = appCtx;
        this.entityId = entityId;
        this.datasets = datasets;
        this.sources = sources;
        registeredJobIds = new ArrayList<>();
        subscribers = new ArrayList<>();
        state = ActivityState.STOPPED;
    }

    public boolean registerJobId(JobId jobId) {
        return this.registeredJobIds.add(jobId);
    }

    @Override
    public synchronized void notify(ActiveEvent event) {
        try {
            switch (event.getEventKind()) {
                case JOB_CREATED:
                    this.state = ActivityState.CREATED;
                    break;
                case JOB_STARTED:
                    start(event);
                    break;
                case JOB_FINISHED:
                    finish();
                    break;
                case PARTITION_EVENT:
                    partition((ActivePartitionMessage) event.getEventObject());
                    break;
                default:
                    LOGGER.log(Level.WARNING, "Unhandled feed event notification: " + event);
                    break;
            }
            notifySubscribers(event);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unhandled Exception", e);
        }
    }

    private synchronized void notifySubscribers(ActiveEvent event) {
        notifyAll();
        Iterator<IActiveEventSubscriber> it = subscribers.iterator();
        while (it.hasNext()) {
            IActiveEventSubscriber subscriber = it.next();
            if (subscriber.done()) {
                it.remove();
            } else {
                subscriber.notify(event);
                if (subscriber.done()) {
                    it.remove();
                }
            }
        }
    }

    private void partition(ActivePartitionMessage message) throws Exception{
        if (message.getEvent() == ActivePartitionMessage.FEED_ADAPTER_STARTED) {
            numRegistered++;
            if (numRegistered == getSources().length) {
                state = ActivityState.STARTED;
            }
        } else if (message.getEvent() == ActivePartitionMessage.EXECUTE_PRECOMPILED_JOB) {
            for (JobId jobId : registeredJobIds) {
                JobUtils.startPrecompiledJob(this.appCtx.getHcc(), jobId);
            }
        }
    }

    private void finish() throws Exception {
        IHyracksClientConnection hcc = appCtx.getHcc();
        JobStatus status = hcc.getJobStatus(jobId);
        state = status.equals(JobStatus.FAILURE) ? ActivityState.FAILED : ActivityState.STOPPED;
        ActiveLifecycleListener activeLcListener = (ActiveLifecycleListener) appCtx.getActiveLifecycleListener();
        activeLcListener.getNotificationHandler().removeListener(this);
    }

    private void start(ActiveEvent event) {
        this.jobId = event.getJobId();
        state = ActivityState.STARTING;
    }

    @Override
    public IActiveEventSubscriber subscribe(ActivityState state) throws HyracksDataException {
        if (state != ActivityState.STARTED && state != ActivityState.STOPPED) {
            throw new HyracksDataException("Can only wait for STARTED or STOPPED state");
        }
        synchronized (this) {
            if (this.state == ActivityState.FAILED) {
                throw new HyracksDataException("Feed has failed");
            } else if (this.state == state) {
                return NoOpSubscriber.INSTANCE;
            }
            return doSubscribe(state);
        }
    }

    // Called within synchronized block
    private FeedEventSubscriber doSubscribe(ActivityState state) {
        FeedEventSubscriber subscriber = new FeedEventSubscriber(this, state);
        subscribers.add(subscriber);
        return subscriber;
    }

    public String[] getSources() {
        return sources;
    }
}
