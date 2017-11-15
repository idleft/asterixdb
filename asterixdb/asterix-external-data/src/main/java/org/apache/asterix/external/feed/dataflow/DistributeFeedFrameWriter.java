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
package org.apache.asterix.external.feed.dataflow;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Provides mechanism for distributing the frames, as received from an operator to a
 * set of registered readers. Each reader typically operates at a different pace. Readers
 * are isolated from each other to ensure that a slow reader does not impact the progress of
 * others.
 **/
public class DistributeFeedFrameWriter implements IFrameWriter {

    private static Logger LOGGER = Logger.getLogger(DistributeFeedFrameWriter.class.getName());

    /** A unique identifier for the feed to which the incoming tuples belong. **/
    private final EntityId feedId;

    /**
     * An instance of FrameDistributor that provides the mechanism for distributing a frame to multiple readers, each
     * operating in isolation.
     **/
    private final FrameDistributor frameDistributor;

    /** The original frame writer instantiated as part of job creation **/
    private final IFrameWriter writer;

    /** The value of the partition 'i' if this is the i'th instance of the associated operator **/
    private final int partition;

    private final FeedAdapter feedAdapter;

    private final int initConnNum;

    private int currentConnNum;

    private final Object mutex = new Object();

    public DistributeFeedFrameWriter(EntityId feedId, IFrameWriter writer, int partition, FeedAdapter adapter,
            int initConnNum) {
        this.feedId = feedId;
        this.frameDistributor = new FrameDistributor();
        this.partition = partition;
        this.writer = writer;
        this.feedAdapter = adapter;
        this.initConnNum = initConnNum;
        currentConnNum = 0;
    }

    public void subscribe(FeedFrameCollector collector) throws HyracksDataException {
        currentConnNum++;
        frameDistributor.registerFrameCollector(collector);
        if (currentConnNum >= initConnNum) {
            synchronized (mutex) {
                mutex.notifyAll();
            }
        }
    }

    public void unsubscribeFeed(EntityId collectorId) throws HyracksDataException {
        currentConnNum--;
        frameDistributor.deregisterFrameCollector(collectorId);
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            frameDistributor.close();
        } finally {
            writer.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        frameDistributor.nextFrame(frame);
    }

    private void startAdapter() throws HyracksDataException {
        // Start by getting the partition number from the manager
        LOGGER.info("Starting ingestion for partition:" + partition);
        try {
            doRun();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Unhandled Exception", e);
            throw HyracksDataException.create(e);
        }
    }

    private void doRun() throws HyracksDataException, InterruptedException {
        while (true) {
            try {
                // Start the adapter
                feedAdapter.start(partition, writer);
                // Adapter has completed execution
                return;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Exception during feed ingestion ", e);
                throw HyracksDataException.create(e);
            }
        }
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        synchronized (mutex) {
            try {
                mutex.wait();
                startAdapter();
            } catch (InterruptedException e) {
                new HyracksDataException("Interrupted adapter execution");
            }
        }
    }

    @Override
    public String toString() {
        return feedId.toString() + this.getClass().getSimpleName() +"[" + partition + "]";
    }

    @Override
    public void flush() throws HyracksDataException {
        frameDistributor.flush();
    }
}
