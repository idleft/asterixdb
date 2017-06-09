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

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class RotateRunFileReader implements IFrameReader {

    private static final Logger LOGGER = Logger.getLogger(RotateRunFileReader.class.getSimpleName());

    private long currentSize;
    private final IIOManager ioManager;
    private FileReference file;
    private final FileReference[] bufferFileList;
    private IFileHandle handle;
    private long readPtr;
    private boolean finished;
    private final RotateRunFileWriter rotateRunFileWriter;
    private int currentFileIdx;
    private final int token;

    public RotateRunFileReader(int currentFileIdx, IIOManager ioManager, long initLength,
            FileReference[] bufferFileList, RotateRunFileWriter rotateRunFileWriter, int token) {
        this.ioManager = ioManager;
        this.currentSize = initLength;
        this.file = bufferFileList[currentFileIdx];
        this.bufferFileList = bufferFileList;
        this.rotateRunFileWriter = rotateRunFileWriter;
        this.currentFileIdx = currentFileIdx;
        this.finished = false;
        this.token = token;
    }

    @Override
    public void open() throws HyracksDataException {
        LOGGER.setLevel(Level.ALL);
        handle = ioManager.open(file, IIOManager.FileReadWriteMode.READ_ONLY,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        readPtr = 0;
    }

    @Override
    public synchronized boolean nextFrame(IFrame frame) throws HyracksDataException {
        try {
            //            System.out.println("Reader: readPtr " + readPtr + " currentSize " + currentSize + " writerSize "
            //                    + rotateRunFileWriter.getWriterSize(currentFileIdx));
            while (readPtr >= currentSize) {
                if ((rotateRunFileWriter.isFinished() && currentFileIdx == rotateRunFileWriter.currentWriterIdx.get())
                        || finished) {
                    return false;
                } else if (readPtr < rotateRunFileWriter.getWriterSize(currentFileIdx)) {
                    // update the current file size
                    currentSize = rotateRunFileWriter.getWriterSize(currentFileIdx);
                    LOGGER.fine("Catch up with writer in " + currentFileIdx);
                } else if (readPtr < rotateRunFileWriter.bufferFileSize) {
                    // current file still growing
                    synchronized (rotateRunFileWriter.readToWriteMutex) {
                        LOGGER.fine("Waiting for writer at " + currentFileIdx);
                        System.out.println("Waiting for writer at " + currentFileIdx);
                        rotateRunFileWriter.readToWriteMutex.wait();
                        currentSize = rotateRunFileWriter.getWriterSize(currentFileIdx);
                    }
                } else {
                    // move to next file
                    if (currentFileIdx == rotateRunFileWriter.currentWriterIdx.get()) {
                        synchronized (rotateRunFileWriter.readToWriteMutex) {
                            rotateRunFileWriter.readToWriteMutex.wait();
                        }
                    }
                    int nextFileIdx = (currentFileIdx + 1) % bufferFileList.length;
                    LOGGER.fine("Reader: Moving to next file from " + currentFileIdx + " to " + nextFileIdx);
                    System.out.println("Reader: Moving to next file from " + currentFileIdx + " to " + nextFileIdx);
                    currentSize = rotateRunFileWriter.getWriterSize(nextFileIdx);
                    file = bufferFileList[nextFileIdx];
                    ioManager.close(handle);
                    handle = ioManager.open(file, IIOManager.FileReadWriteMode.READ_ONLY,
                            IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                    readPtr = 0;
                    rotateRunFileWriter.detachFile(currentFileIdx, token);
                    currentFileIdx = nextFileIdx;
                }
            }

            LOGGER.finest("Reader Proceed");
            frame.reset();
            int readLength = ioManager.syncRead(handle, readPtr, frame.getBuffer());
            if (readLength <= 0) {
                // as there should be a frame there if the file is not ended.
                throw new HyracksDataException("Premature end of file");
            }
            readPtr += readLength;
            frame.ensureFrameSize(frame.getMinSize() * FrameHelper.deserializeNumOfMinFrame(frame.getBuffer()));
            if (frame.getBuffer().hasRemaining()) {
                if (readPtr < rotateRunFileWriter.getWriterSize(currentFileIdx)) {
                    readLength = ioManager.syncRead(handle, readPtr, frame.getBuffer());
                    if (readLength < 0) {
                        throw new HyracksDataException("Premature end of file");
                    }
                    readPtr += readLength;
                }
                if (frame.getBuffer().hasRemaining()) { // file is vanished.
                    FrameHelper.clearRemainingFrame(frame.getBuffer(), frame.getBuffer().position());
                }
            }
            frame.getBuffer().flip();
            return true;
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (handle != null) {
            rotateRunFileWriter.detachFile(currentFileIdx, token);
            ioManager.close(handle);
            handle = null;
            this.finished = true;
        }
    }

    public int getCurrentFileIdx() {
        return this.currentFileIdx;
    }
}
