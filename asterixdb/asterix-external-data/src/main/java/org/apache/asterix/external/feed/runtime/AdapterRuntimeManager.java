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
package org.apache.asterix.external.feed.runtime;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.RotateRunFileReader;
import org.apache.hyracks.dataflow.common.io.RotateRunFileWriter;

/**
 * This class manages the execution of an adapter within a feed
 */
public class AdapterRuntimeManager {

    public enum State{
        CREATED,
        ACTIVE,
        FINISHED,
        FAILED
    }

    private static final Logger LOGGER = Logger.getLogger(AdapterRuntimeManager.class.getName());

    private final EntityId feedId; // (dataverse-feed)

    private final FeedAdapter feedAdapter; // The adapter

    private final AdapterExecutor adapterExecutor; // The executor for the adapter

    private final int partition; // The partition number

    private final IHyracksTaskContext ctx;

    private Future<?> execution;

    private volatile State state;

    private RotateRunFileWriter rotateRunFileWriter;

    private final int initConnectionN;
    private Integer currentConnectionN;

    public AdapterRuntimeManager(IHyracksTaskContext ctx, EntityId entityId, FeedAdapter feedAdapter,
            RotateRunFileWriter writer, int partition, int initConnectionN) {
        this.ctx = ctx;
        this.feedId = entityId;
        this.feedAdapter = feedAdapter;
        this.partition = partition;
        this.adapterExecutor = new AdapterExecutor(writer, feedAdapter, this);
        this.rotateRunFileWriter = writer;
        this.initConnectionN = initConnectionN;
        this.state = State.CREATED;
        this.currentConnectionN = 0;
    }

    public synchronized void start() throws InterruptedException {
        while (currentConnectionN < initConnectionN) {
            wait();
        }
        synchronized (adapterExecutor) {
            if (state != State.FINISHED) {
                execution = ctx.getExecutorService().submit(adapterExecutor);
                this.state = State.ACTIVE;
            } else {
                LOGGER.log(Level.WARNING, "Someone stopped me before I even start. I will simply not start");
            }
        }
    }

    public synchronized void stop() throws HyracksDataException, InterruptedException {
        synchronized (adapterExecutor) {
            try {
                if (this.state == State.ACTIVE) {
                    try {
                        ctx.getExecutorService().submit(() -> {
                            if (feedAdapter.stop()) {
                                execution.get();
                            }
                            return null;
                        }).get(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        LOGGER.log(Level.WARNING, "Interrupted while trying to stop an adapter runtime", e);
                        this.state = State.FAILED;
                        throw e;
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Exception while trying to stop an adapter runtime", e);
                        this.state = State.FAILED;
                        throw HyracksDataException.create(e);
                    } finally {
                        execution.cancel(true);
                        state = State.FINISHED;
                    }
                } else {
                    LOGGER.log(Level.WARNING, "Adapter executor was stopped before it starts");
                }
            } finally {
                LOGGER.log(Level.FINER, "AdapterRuntimeManager finished");
                notifyAll();
            }
        }
    }

    public EntityId getFeedId() {
        return feedId;
    }

    @Override
    public String toString() {
        return feedId + "[" + partition + "]";
    }

    public FeedAdapter getFeedAdapter() {
        return feedAdapter;
    }

    public AdapterExecutor getAdapterExecutor() {
        return adapterExecutor;
    }

    public int getPartition() {
        return partition;
    }

    public boolean isFailed() {
        return this.state == State.FAILED;
    }

    public void setFailed() {
        this.state = State.FAILED;
    }

    public void setFinished() {
        this.state = State.FINISHED;
    }

    public synchronized void waitForFinish() throws HyracksDataException {
        while (this.state != State.FINISHED && this.state != State.FAILED) {
            try {
                LOGGER.log(Level.FINER, "AdapterRuntimeManager waits for finish.");
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    public synchronized RotateRunFileReader subscribe() {
        currentConnectionN += 1;
        notify();
        return rotateRunFileWriter.getReader();
    }

}
