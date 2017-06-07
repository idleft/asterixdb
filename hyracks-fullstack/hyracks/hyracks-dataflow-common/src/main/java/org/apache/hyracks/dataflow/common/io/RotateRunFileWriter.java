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

package org.apache.hyracks.dataflow.common.io;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RotateRunFileWriter implements IFrameWriter {

    private static final Logger LOGGER = Logger.getLogger(RotateRunFileWriter.class.getSimpleName());

    public int bufferFileNumber = 16;
    public int framePerBufferFile = 20;
    public int defaultFrameSize = 32768;
    public int bufferFileSize = framePerBufferFile * defaultFrameSize;

    private final String bufferFilesPrefix;
    private final IHyracksTaskContext ctx;
    private final IIOManager ioManager;
    private RunFileWriter[] bwList = new RunFileWriter[bufferFileNumber];
    private FileReference[] bufferFileList = new FileReference[bufferFileNumber];
    private List<Integer> readerOnFileCount = new ArrayList<>();
    private boolean failed;
    private boolean finished;
    public AtomicInteger currentWriterIdx;

    public Object writeToReadMutex = new Object();
    public Object readToWriteMutex = new Object();

    public RotateRunFileWriter(String prefix, IHyracksTaskContext ctx, int bufferFileN, int framePerFile,
            int frameSize) {
        this.bufferFilesPrefix = prefix;
        this.ctx = ctx;
        this.ioManager = ctx.getIoManager();
        this.failed = false;
        this.finished = false;
        this.bufferFileNumber = bufferFileN;
        this.framePerBufferFile = framePerFile;
        this.defaultFrameSize = frameSize;
        this.bufferFileSize = framePerBufferFile * defaultFrameSize;
    }

    @Override
    public void open() throws HyracksDataException {
        for (int iter1 = 0; iter1 < bufferFileNumber; iter1++) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(bufferFilesPrefix + iter1);
            bufferFileList[iter1] = file;
            bwList[iter1] = new RunFileWriter(file, ioManager);
            bwList[iter1].open();
            readerOnFileCount.add(0);
        }
        currentWriterIdx = new AtomicInteger(0);
    }

    @Override
    public synchronized void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // add rotate logic here
        long currentBufferUsage = bwList[currentWriterIdx.get()].getFileSize() / defaultFrameSize;
        if (currentBufferUsage >= framePerBufferFile) {
            // proceed to next writer
            int nextWriterIdx = (currentWriterIdx.get() + 1) % bufferFileNumber;
            synchronized (writeToReadMutex) {
                while (readerOnFileCount.get(nextWriterIdx) > 0) {
                    try {
                        LOGGER.finest("Waits for reader to finish at " + currentWriterIdx);
                        writeToReadMutex.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        fail();
                    }
                }
            }
            if (bwList[nextWriterIdx].getFileSize() > 0) {
                bwList[nextWriterIdx].refresh();
            }
            LOGGER.fine("Writer: shift from " + currentWriterIdx + " to " + nextWriterIdx);
            currentWriterIdx.set(nextWriterIdx);
            synchronized (readToWriteMutex) {
                readToWriteMutex.notifyAll();
            }
        }
        LOGGER.finest("Writer proceed");
        bwList[currentWriterIdx.get()].nextFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        for (RunFileWriter bw : bwList) {
            bw.fail();
        }
        failed = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!failed) {
            this.finished = true;
            synchronized (readToWriteMutex) {
                readToWriteMutex.notifyAll();
            }
            for (RunFileWriter bw : bwList) {
                bw.close();
            }
        }
    }

    public boolean isFinished() {
        return finished;
    }

    public long getWriterSize(int writerIdx) {
        return bwList[writerIdx].getFileSize();
    }

    public RotateRunFileReader getReader() {
        int writerIdxSnapshot = currentWriterIdx.get();
        return new RotateRunFileReader(writerIdxSnapshot, ioManager, bwList[writerIdxSnapshot].getFileSize(),
                bufferFileList, this);
    }

    public int referenceFile(int fileIdx) {
        synchronized (writeToReadMutex) {
            readerOnFileCount.set(fileIdx, readerOnFileCount.get(fileIdx) + 1);
            return readerOnFileCount.get(fileIdx);
        }
    }

    public int dereferenceFile(int fileIdx) {
        synchronized (writeToReadMutex) {
            readerOnFileCount.set(fileIdx, readerOnFileCount.get(fileIdx) - 1);
            return readerOnFileCount.get(fileIdx);
        }
    }
}
