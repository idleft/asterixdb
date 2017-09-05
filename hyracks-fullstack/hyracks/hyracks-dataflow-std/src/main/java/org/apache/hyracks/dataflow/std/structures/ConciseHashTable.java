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

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

import java.nio.ByteBuffer;

public class ConciseHashTable implements ISerializableTable {
    private static final int INT_SIZE = 4;
    private static final int ENTRY_SIZE = 8;
    public static final int PROBE_THRESHOLD = 8;
    public static final int BITMAP_WORD_TOTAL_LENGTH = 64;
    public static final int BITMAP_WORD_MAP_LENGTH = 32;
    public static final int BITMAP_WORD_COUNT_LENGTH = BITMAP_WORD_TOTAL_LENGTH - BITMAP_WORD_MAP_LENGTH;
    public static final long BITMAP_WORD_COUNT_MASK = 0xffffffff;
    public static final int FILL_FACTOR = 8;
    private IHyracksFrameMgrContext ctx;
    private int tableSize;
    private int frameCnt;
    private int frameSize;
    private int frameCapacity;
    private int currentByteSize;
    private int actualTableSize;
    private int tupleCount;
    public int overflowTablePtr;
    public boolean buildDone;
    public int probeLimit;

    private IntSerDeBuffer[] frames;
    public long[] bitmapWords;
    public int bitWordsNum;
    public int bitmapSize;

    // debug

    public int normalCtr = 0;
    public int ofCtr = 0;
    private int preNormalCtr = 0;
    private int preOfCtr = 0;
    public int writeNum = 0;

    public ConciseHashTable(int totalTupleCount, final IHyracksFrameMgrContext ctx) {
        this.ctx = ctx;
        this.actualTableSize = totalTupleCount;
        this.tableSize = totalTupleCount * FILL_FACTOR;
        this.frameSize = ctx.getInitialFrameSize();
        this.frameCapacity = frameSize / (ENTRY_SIZE); // Frame capacity in bucket
        this.frameCnt = (int) Math.ceil(tableSize * 1.0 / frameCapacity);
        this.frames = new IntSerDeBuffer[frameCnt];
        this.currentByteSize = 0;
        this.overflowTablePtr = actualTableSize; // Ptr - 1 is the start of OF records
        this.bitWordsNum = (int) Math.ceil((double) totalTupleCount * FILL_FACTOR / 32);
        this.bitmapSize = totalTupleCount * FILL_FACTOR;
        this.bitmapWords = new long[bitWordsNum];
        this.probeLimit = PROBE_THRESHOLD > tableSize ? tableSize : PROBE_THRESHOLD;
        buildDone = false;
    }

    private ByteBuffer getFrame(int size) throws HyracksDataException {
        currentByteSize += size;
        return ctx.allocateFrame(size);
    }

    private int safeFrameRead(int frameIdx, int tupleOffset) throws HyracksDataException {
        if (frames[frameIdx] == null) {
            ByteBuffer newBufferFrame = getFrame(frameSize);
            frames[frameIdx] = new IntSerDeBuffer(newBufferFrame);
        }
        int result = frames[frameIdx].getInt(tupleOffset * (ENTRY_SIZE / INT_SIZE));
        return result;
    }

    public int getActualTableSize() {
        return actualTableSize;
    }

    private int entryToTupleOffset(int entry) {
        return (entry % frameCapacity);
    }

    private boolean peakFrameEmptyByTrueEntry(int trueEntry) {
        if (frames[trueEntry / frameCapacity] == null
                || frames[trueEntry / frameCapacity].getInt(entryToTupleOffset(trueEntry) * ENTRY_SIZE / INT_SIZE) < 0)
            return true;
        else
            return false;
    }

    @Override
    public boolean insert(int entry, TuplePointer tuplePointer) throws HyracksDataException {
        // insert is guaranteed to be good
        tupleCount++;
        for (int iter1 = 0; iter1 < probeLimit; iter1++) {
            int shiftEntry = entry + iter1;
            int trueEntry = getTrueEntry(shiftEntry);
            if (peakFrameEmptyByTrueEntry(trueEntry)) {
                normalCtr++;
                writeEntry(trueEntry / frameCapacity, entryToTupleOffset(trueEntry), tuplePointer);
                return true;
            }
        }
        // failed to insert within limit, insert into OF table
        overflowTablePtr--;
        writeEntry(overflowTablePtr / frameCapacity, entryToTupleOffset(overflowTablePtr), tuplePointer);
        ofCtr++;
        return true;
    }

    private void writeEntry(int frameIdx, int tupleOffset, TuplePointer tuplePointer) throws HyracksDataException {
        if (frames[frameIdx] == null) {
            ByteBuffer newBufferFrame = getFrame(frameSize);
            frames[frameIdx] = new IntSerDeBuffer(newBufferFrame);
            frames[frameIdx].resetFrame();
        }
        int entryOffset = tupleOffset * ENTRY_SIZE / INT_SIZE;
        if (frames[frameIdx].getInt(entryOffset) >= 0) {
            //            System.out.println("We screwed");
            throw new HyracksDataException("YES WE SCREWED.");
        }
        frames[frameIdx].writeInt(entryOffset, tuplePointer.getFrameIndex());
        frames[frameIdx].writeInt(entryOffset + 1, tuplePointer.getTupleIndex());
        writeNum++;
    }

    @Override
    public void delete(int entry) throws HyracksDataException {
        throw new HyracksDataException("Not supported.");
    }

    @Override
    public boolean getTuplePointer(int entry, int tupleOffset, TuplePointer tuplePointer) {
        int actualEntry = (entry + tupleOffset) % actualTableSize;
        if (frames[actualEntry / frameCapacity] == null
                || frames[actualEntry / frameCapacity].getInt(entryToTupleOffset(actualEntry) * ENTRY_SIZE / INT_SIZE)
                < 0) {
            return false;
        }
        int frameIndex =
                frames[actualEntry / frameCapacity].getInt(entryToTupleOffset(actualEntry) * ENTRY_SIZE / INT_SIZE);
        int tupleIndex =
                frames[actualEntry / frameCapacity].getInt(entryToTupleOffset(actualEntry) * ENTRY_SIZE / INT_SIZE + 1);
        //        System.out.println("Read tuple " + frameIndex + "-" + tupleIndex + " from " + actualEntry);
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
    public int getTupleCount(int entry) throws HyracksDataException {
        throw new HyracksDataException("NOT SUPPORT");
    }

    @Override
    public void reset() {
        for (IntSerDeBuffer frame : frames) {
            if (frame != null) {
                frame.resetFrame();
            }
        }
        for (int iter1 = 0; iter1 < bitmapWords.length; iter1++) {
            bitmapWords[iter1] = 0;
        }
        overflowTablePtr = tableSize - 1;
        buildDone = false;
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
        bitmapWords = null;
        buildDone = false;
        overflowTablePtr = tableSize;
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
        return "NA";
    }

    @Override
    public int getTableSize() {
        return tableSize;
    }

    public static long getExpectedTableFrameCount(long tupleCount, int frameSize) {
        return (long) (Math.ceil((double) tupleCount * ENTRY_SIZE / (double) frameSize));
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

    public void updatebitmapWords(int entry) {
        for (int iter1 = 0; iter1 < probeLimit; iter1++) {
            int shiftEntry = (entry + iter1) % tableSize;
            int idx = shiftEntry / BITMAP_WORD_MAP_LENGTH;
            int bitOffset = shiftEntry % BITMAP_WORD_MAP_LENGTH + BITMAP_WORD_COUNT_LENGTH;
            if ((bitmapWords[idx] & (1l << (bitOffset))) == 0) {
                bitmapWords[idx] = bitmapWords[idx] | (1l << (bitOffset));
                preNormalCtr++;
                return;
            }
        }
        preOfCtr++;
    }

    private int popCount(long number) {
        int counter = 0;
        for (int iter1 = 0; iter1 < BITMAP_WORD_MAP_LENGTH; iter1++) {
            if ((number & (1 << iter1)) != 0) {
                counter++;
            }
        }
        return counter;
    }

    public int extractCountFromWord(long word) {
        return (int) (word & BITMAP_WORD_COUNT_MASK);
    }

    public boolean peakEmptyEntry(int entry) {
        entry = entry % tableSize;
        int wordListIdx = entry / BITMAP_WORD_MAP_LENGTH;
        int bitPos = entry % BITMAP_WORD_MAP_LENGTH + BITMAP_WORD_COUNT_LENGTH;
        return (bitmapWords[wordListIdx] & 1l << bitPos) == 0;
    }

    public int getTrueEntry(int entry) {
        // This method doesn't guarantee entry is empty
        entry = entry % tableSize;
        int wordListIdx = entry / BITMAP_WORD_MAP_LENGTH;
        int preWordCount = wordListIdx > 0 ? extractCountFromWord(bitmapWords[wordListIdx - 1]) : 0;

        int currentUpToBit = popCount(
                bitmapWords[wordListIdx] >> BITMAP_WORD_COUNT_LENGTH & ((1 << entry % BITMAP_WORD_MAP_LENGTH) - 1));
        return preWordCount + currentUpToBit;
    }

    public void populateCountsInBitwords() {
        int aggNum = 0;
        for (int iter1 = 0; iter1 < bitmapWords.length; iter1++) {
            long curMap = (bitmapWords[iter1] >> BITMAP_WORD_COUNT_LENGTH);
            aggNum += popCount(curMap);
            bitmapWords[iter1] = bitmapWords[iter1] | aggNum;
        }
    }

}
