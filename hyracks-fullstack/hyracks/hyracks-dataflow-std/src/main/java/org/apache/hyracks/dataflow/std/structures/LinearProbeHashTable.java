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
package org.apache.hyracks.dataflow.std.structures;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

import java.nio.ByteBuffer;

public class LinearProbeHashTable implements ISerializableTable {
    private static final int INT_SIZE = 4;
    private static final int ENTRY_SIZE = 8;
    private static final int INVERSE_LOAD_FACTOR = 2;
    private IHyracksFrameMgrContext ctx;
    private int tableSize;
    private int frameCnt;
    private int frameSize;
    private int frameCapacity;
    private int currentByteSize;
    private int tupleCount;

    private IntSerDeBuffer[] frames;

    public LinearProbeHashTable(int totalTupleCount, final IHyracksFrameMgrContext ctx) {
        this.ctx = ctx;
        this.tableSize = totalTupleCount * INVERSE_LOAD_FACTOR;
        this.frameSize = ctx.getInitialFrameSize();
        this.frameCapacity = frameSize / (ENTRY_SIZE); // Frame capacity in bucket
        this.frameCnt = (int) Math.ceil(tableSize * 1.0 / frameCapacity);
        this.frames = new IntSerDeBuffer[frameCnt];
        this.currentByteSize = 0;
    }

    private ByteBuffer getFrame(int size) throws HyracksDataException {
        currentByteSize += size;
        return ctx.allocateFrame(size);
    }

    private boolean safePeakEmptyBucket(int frameIdx, int tupleOffset) throws HyracksDataException {
        if (frames[frameIdx] == null) {
            ByteBuffer newBufferFrame = getFrame(frameSize);
            frames[frameIdx] = new IntSerDeBuffer(newBufferFrame);
            return true;
        }
        return frames[frameIdx].bytes[tupleOffset * ENTRY_SIZE] < 0;
    }

    private int entryToTupleOffset(int entry) {
        return entry % frameCapacity;
    }

    @Override
    public boolean insert(int entry, TuplePointer tuplePointer) throws HyracksDataException {
        int entryPtr = entry;
        int visitedRecords = 0;
        // insert is guaranteed to be good
        while (!safePeakEmptyBucket(entryPtr / frameCapacity, entryToTupleOffset(entryPtr))
                && visitedRecords < tableSize) {
            visitedRecords++;
            entryPtr = (entryPtr + 1) % tableSize;
        }
        if (visitedRecords >= tableSize) {
            return false;
        }
        writeEntry(entryPtr / frameCapacity, entryToTupleOffset(entryPtr), tuplePointer);
        tupleCount += 1;
        return true;
    }

    private void writeEntry(int frameIndex, int tupleOffset, TuplePointer tuplePointer) throws HyracksDataException {
        int entryOffset = tupleOffset * ENTRY_SIZE / INT_SIZE;
        frames[frameIndex].writeInt(entryOffset, tuplePointer.getFrameIndex());
        frames[frameIndex].writeInt(entryOffset + 1, tuplePointer.getTupleIndex());
    }

    @Override
    public void delete(int entry) {
        // no op
    }

    @Override
    public boolean getTuplePointer(int entry, int tupleOffset, TuplePointer tuplePointer) {
        int actualEntry = (entry + tupleOffset) % tableSize;
        if (frames[actualEntry / frameCapacity] == null
                || frames[actualEntry / frameCapacity].getInt(entryToTupleOffset(actualEntry) * ENTRY_SIZE / INT_SIZE)
                < 0) {
            return false;
        }
        int frameIndex =
                frames[actualEntry / frameCapacity].getInt(entryToTupleOffset(actualEntry) * ENTRY_SIZE / INT_SIZE);
        int tupleIndex =
                frames[actualEntry / frameCapacity].getInt(entryToTupleOffset(actualEntry) * ENTRY_SIZE / INT_SIZE + 1);
        tuplePointer.reset(frameIndex, tupleIndex);
        return true;
    }

    @Override
    public int getCurrentByteSize() {
        return currentByteSize;
    }

    @Override
    public int getTupleCount() {
        return tupleCount;
    }

    @Override
    public int getTupleCount(int entry) {
        int result = 0;
        int ptr = entry;
        while (frames[ptr / frameCapacity] != null
                && frames[ptr / frameCapacity].getInt(entryToTupleOffset(ptr) * ENTRY_SIZE / INT_SIZE) >= 0
                && result < tableSize) {
            result++;
            ptr = (ptr + 1) % tableSize;
        }
        return result;
    }

    @Override
    public void reset() {
        for (IntSerDeBuffer frame : frames) {
            if (frame != null) {
                frame.resetFrame();
            }
        }
    }

    @Override
    public void close() {
        int framesToDeallocate = 0;
        for (int iter1 = 0; iter1 < frames.length; iter1++) {
            if (frames[iter1] != null) {
                framesToDeallocate++;
                frames[iter1] = null;
            }
        }
        tupleCount = 0;
        currentByteSize = 0;
        ctx.deallocateFrames(framesToDeallocate);
    }

    @Override
    public boolean isGarbageCollectionNeeded() {
        return true;
    }

    @Override
    public int collectGarbage(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc)
            throws HyracksDataException {
        throw new HyracksDataException("Not supported");
    }

    @Override
    public String printInfo() {
        return "{" + "\"tuple_count\":" + tupleCount + ", \"table_size\":" + tableSize + ",\"current_byte_size\":"
                + currentByteSize + "}";
    }

    @Override
    public int getTableSize() {
        return tableSize;
    }

    public static long getExpectedTableFrameCount(long tupleCount, int frameSize) {
        return (long) (Math.ceil((double) tupleCount * INVERSE_LOAD_FACTOR * ENTRY_SIZE / (double) frameSize));
    }

    public static long getExpectedTableByteSize(long tupleCount, int frameSize) {
        return getExpectedTableFrameCount(tupleCount, frameSize) * frameSize;
    }

    public static long calculateFrameCountDeltaForTableSizeChange(long origTupleCount, long delta, int frameSize) {
        long originalFrameCount = getExpectedTableFrameCount(origTupleCount, frameSize);
        long newFrameCount = getExpectedTableFrameCount(origTupleCount + delta, frameSize);
        return newFrameCount - originalFrameCount;
    }

    public static long calculateByteSizeDeltaForTableSizeChange(long origTupleCount, long delta, int frameSize) {
        return calculateFrameCountDeltaForTableSizeChange(origTupleCount, delta, frameSize) * frameSize;
    }
}
