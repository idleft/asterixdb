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
package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.TupleInFrameListAccessor;
import org.apache.hyracks.dataflow.std.structures.ConciseHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConciseHashJoin {

    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final ITuplePartitionComputer tpcBuild;
    private IFrameTupleAccessor accessorProbe;
    private final ITuplePartitionComputer tpcProbe;
    private final FrameTupleAppender appender;
    private final FrameTuplePairComparator tpComparator;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder missingTupleBuild;
    private final ConciseHashTable table;
    private final TuplePointer storedTuplePointer;
    private final boolean reverseOutputOrder; //Should we reverse the order of tuples, we are writing in output
    private final IPredicateEvaluator predEvaluator;
    private TupleInFrameListAccessor tupleAccessor;
    // To release frames
    ISimpleFrameBufferManager bufferManager;
    private final boolean isTableCapacityNotZero;
    private Map<String, Integer> hashValueCache;
    // debug
    private int totalRecordNum = 0;
//    public HashMap<Integer, Integer> freq = new HashMap<>();

    private static final Logger LOGGER = Logger.getLogger(ConciseHashJoin.class.getName());

    public ConciseHashJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessorProbe, ITuplePartitionComputer tpcProbe,
            FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild, ITuplePartitionComputer tpcBuild,
            FrameTuplePairComparator comparator, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ConciseHashTable table, IPredicateEvaluator predEval, ISimpleFrameBufferManager bufferManager)
            throws HyracksDataException {
        this(ctx, accessorProbe, tpcProbe, accessorBuild, rDBuild, tpcBuild, comparator, isLeftOuter,
                missingWritersBuild, table, predEval, false, bufferManager);
    }

    public ConciseHashJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessorProbe, ITuplePartitionComputer tpcProbe,
            FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild, ITuplePartitionComputer tpcBuild,
            FrameTuplePairComparator comparator, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ConciseHashTable table, IPredicateEvaluator predEval, boolean reverse,
            ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this.table = table;
        storedTuplePointer = new TuplePointer();
        buffers = new ArrayList<>();
        this.accessorBuild = accessorBuild;
        this.tpcBuild = tpcBuild;
        this.accessorProbe = accessorProbe;
        this.tpcProbe = tpcProbe;
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        tpComparator = comparator;
        predEvaluator = predEval;
        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter) {
            int fieldCountOuter = accessorBuild.getFieldCount();
            missingTupleBuild = new ArrayTupleBuilder(fieldCountOuter);
            DataOutput out = missingTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCountOuter; i++) {
                missingWritersBuild[i].writeMissing(out);
                missingTupleBuild.addFieldEndOffset();
            }
        } else {
            missingTupleBuild = null;
        }
        reverseOutputOrder = reverse;
        this.tupleAccessor = new TupleInFrameListAccessor(rDBuild, buffers);
        this.bufferManager = bufferManager;
        if (table.getTableSize() != 0) {
            isTableCapacityNotZero = true;
        } else {
            isTableCapacityNotZero = false;
        }
        LOGGER.fine("InMemoryHashJoin has been created for a table size of " + table.getTableSize() + " for Thread ID "
                + Thread.currentThread().getId() + ".");
        hashValueCache = new HashMap<>();
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        buffers.add(buffer);
        accessorBuild.reset(buffer);
        int tCount = accessorBuild.getTupleCount();
        totalRecordNum += tCount;
        for (int i = 0; i < tCount; ++i) {
            int entry = tpcBuild.partition(accessorBuild, i, table.getTableSize());
            hashValueCache.put(String.valueOf(buffers.size() - 1) + "-" + i, entry);
            table.updatebitmapWords(entry);
        }
    }

    /**
     * Tries to compact the table to make some space.
     *
     * @return the number of frames that have been reclaimed. If no compaction has happened, the value -1 is returned.
     */
    public int compactHashTable() throws HyracksDataException {
        if (table.isGarbageCollectionNeeded()) {
            return table.collectGarbage(tupleAccessor, tpcBuild);
        }
        return -1;
    }

    /**
     * Reads the given tuple from the probe side and joins it with tuples from the build side.
     * This method assumes that the accessorProbe is already set to the current probe frame.
     */

    boolean compareRecord(int tid, int trueEntry, int offset, IFrameWriter writer) throws HyracksDataException {
        boolean res = false;
        table.getTuplePointer(trueEntry, offset, storedTuplePointer);
        int bIndex = storedTuplePointer.getFrameIndex();
        int tIndex = storedTuplePointer.getTupleIndex();
        accessorBuild.reset(buffers.get(bIndex));
        int c = tpComparator.compare(accessorProbe, tid, accessorBuild, tIndex);
        if (c == 0) {
            boolean predEval = evaluatePredicate(tid, tIndex);
            if (predEval) {
                res = true;
                appendToResult(tid, tIndex, writer);
            }
        }
        storedTuplePointer.reset(-1, -1);
        return res;
    }

    void join(int tid, IFrameWriter writer) throws HyracksDataException {
        if (!table.buildDone) {
            table.populateCountsInBitwords();
            int frameCount = buffers.size();
            int totalNum = 0;
            for (int iter0 = 0; iter0 < frameCount; iter0++) {
                ByteBuffer buffer = buffers.get(iter0);
                accessorBuild.reset(buffer);
                int tCount = accessorBuild.getTupleCount();
                totalNum += tCount;
                for (int iter1 = 0; iter1 < tCount; iter1++) {
                    //                    int entry = tpcBuild.partition(accessorBuild, iter1, table.getTableSize());
                    int entry = hashValueCache.get(String.valueOf(iter0) + "-" + iter1);
//                    if (freq.containsKey(entry))
//                        freq.put(entry, freq.get(entry) + 1);
//                    else
//                        freq.put(entry, 0);
                    storedTuplePointer.reset(iter0, iter1);
                    table.insert(entry, storedTuplePointer);
                }
            }
            table.buildDone = true;
        }
//        for (Integer key : freq.keySet()) {
//            System.out.println(key + "  --  " + freq.get(key));
//        }
        boolean matchFound = false;
        if (isTableCapacityNotZero) {
            int entry = tpcProbe.partition(accessorProbe, tid, table.getTableSize());
            if (!table.peakEmptyEntry(entry)) {
                // TODO: change trueEntry to entry
                for (int iter1 = 0; iter1 < table.probeLimit; iter1++) {
                    // TODO: can be optimized
                    if (!table.peakEmptyEntry(entry + iter1) && compareRecord(tid, table.getTrueEntry(entry + iter1), 0,
                            writer)) {
                        matchFound = true;
                    }
                }
                for (int iter1 = table.overflowTablePtr; iter1 < table.getActualTableSize(); iter1++) {
                    if (compareRecord(tid, iter1, 0, writer)) {
                        matchFound = true;
                    }
                }
            }
        }
        if (!matchFound && isLeftOuter) {
            FrameUtils
                    .appendConcatToWriter(writer, appender, accessorProbe, tid, missingTupleBuild.getFieldEndOffsets(),
                            missingTupleBuild.getByteArray(), 0, missingTupleBuild.getSize());
        }
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount0 = accessorProbe.getTupleCount();
        for (int i = 0; i < tupleCount0; ++i) {
            join(i, writer);
        }
    }

    public void resetAccessorProbe(IFrameTupleAccessor newAccessorProbe) {
        accessorProbe.reset(newAccessorProbe.getBuffer());
    }

    public void completeJoin(IFrameWriter writer) throws HyracksDataException {
        appender.write(writer, true);
    }

    public void releaseMemory() throws HyracksDataException {
        int nFrames = buffers.size();
        // Frames assigned to the data table will be released here.
        if (bufferManager != null) {
            for (int i = 0; i < nFrames; i++) {
                bufferManager.releaseFrame(buffers.get(i));
            }
        }
        buffers.clear();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "InMemoryHashJoin has finished using " + nFrames + " frames for Thread ID " + Thread.currentThread()
                            .getId() + ".");
        }
    }

    public void closeTable() throws HyracksDataException {
        hashValueCache.clear();
        table.close();
    }

    private boolean evaluatePredicate(int tIx1, int tIx2) {
        if (reverseOutputOrder) { //Role Reversal Optimization is triggered
            return (predEvaluator == null) || predEvaluator.evaluate(accessorBuild, tIx2, accessorProbe, tIx1);
        } else {
            return (predEvaluator == null) || predEvaluator.evaluate(accessorProbe, tIx1, accessorBuild, tIx2);
        }
    }

    private void appendToResult(int probeSidetIx, int buildSidetIx, IFrameWriter writer) throws HyracksDataException {
        if (reverseOutputOrder) {
            FrameUtils.appendConcatToWriter(writer, appender, accessorBuild, buildSidetIx, accessorProbe, probeSidetIx);
        } else {
            FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, probeSidetIx, accessorBuild, buildSidetIx);
        }
    }

}
