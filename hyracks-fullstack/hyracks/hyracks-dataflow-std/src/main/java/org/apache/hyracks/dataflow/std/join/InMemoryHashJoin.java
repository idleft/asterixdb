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

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.TupleInFrameListAccessor;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

public class InMemoryHashJoin {

    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final ITuplePartitionComputer tpcBuild;
    private IFrameTupleAccessor accessorProbe;
    private final ITuplePartitionComputer tpcProbe;
    private final FrameTupleAppender appender;
    private final FrameTuplePairComparator tpComparator;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder missingTupleBuild;
    private final ISerializableTable table;
    private final TuplePointer storedTuplePointer;
    private final boolean reverseOutputOrder; //Should we reverse the order of tuples, we are writing in output
    private final IPredicateEvaluator predEvaluator;
    private TupleInFrameListAccessor tupleAccessor;
    // To release frames
    ISimpleFrameBufferManager bufferManager;
    private final boolean isTableCapacityNotZero;
    private int bloomFilterSize = 1024;
    private int bloomHashNum = 5; // Per field
    private IBinaryHashFunction bloomFilterHashFunctions[];
    private BitSet bloomFilter;

    private final int[] buildKeys;
    private final int[] probeKeys;

    private static final Logger LOGGER = Logger.getLogger(InMemoryHashJoin.class.getName());

    public InMemoryHashJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessorProbe, ITuplePartitionComputer tpcProbe,
            FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild, ITuplePartitionComputer tpcBuild,
            FrameTuplePairComparator comparator, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ISerializableTable table, IPredicateEvaluator predEval, ISimpleFrameBufferManager bufferManager,
            int[] probeKeys, int[] buildKeys, IBinaryHashFunctionFactory[] hashFunctionFactories)
            throws HyracksDataException {
        this(ctx, accessorProbe, tpcProbe, accessorBuild, rDBuild, tpcBuild, comparator, isLeftOuter,
                missingWritersBuild, table, predEval, false, bufferManager, probeKeys, buildKeys);
        // initialize with factory
        for (int iter1 = 0; iter1 < bloomHashNum; iter1++) {
            for (int iter2 = 0; iter2 < buildKeys.length; iter2++) {
                bloomFilterHashFunctions[iter1 * buildKeys.length + iter2] =
                        hashFunctionFactories[iter2].createBinaryHashFunction();
            }
        }
    }

    public InMemoryHashJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessorProbe, ITuplePartitionComputer tpcProbe,
            FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild, ITuplePartitionComputer tpcBuild,
            FrameTuplePairComparator comparator, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ISerializableTable table, IPredicateEvaluator predEval, boolean reverse,
            ISimpleFrameBufferManager bufferManager, int[] probeKeys, int[] buildKeys,
            IBinaryHashFunctionFamily[] hashFunctionFamily) throws HyracksDataException {
        this(ctx, accessorProbe, tpcProbe, accessorBuild, rDBuild, tpcBuild, comparator, isLeftOuter,
                missingWritersBuild, table, predEval, reverse, bufferManager, probeKeys, buildKeys);
        // initialize with factory
        for (int iter1 = 0; iter1 < bloomHashNum; iter1++) {
            for (int iter2 = 0; iter2 < buildKeys.length; iter2++) {
                int idx = iter1 * buildKeys.length + iter2;
                bloomFilterHashFunctions[idx] =
                        hashFunctionFamily[iter2].createBinaryHashFunction(idx);
            }
        }
    }

    public InMemoryHashJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessorProbe, ITuplePartitionComputer tpcProbe,
            FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild, ITuplePartitionComputer tpcBuild,
            FrameTuplePairComparator comparator, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ISerializableTable table, IPredicateEvaluator predEval, boolean reverse,
            ISimpleFrameBufferManager bufferManager, int[] probeKeys, int[] buildKeys) throws HyracksDataException {
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
        this.bloomFilterHashFunctions = new IBinaryHashFunction[bloomHashNum * buildKeys.length];
        bloomFilter = new BitSet(bloomFilterSize);
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
    }

    private int hashByBytes(int tupleIdx, int blhIdx, IFrameTupleAccessor accessor, int[] keyIdx)
            throws HyracksDataException {
        int h = 0;
        int startOffset = accessor.getTupleStartOffset(tupleIdx);
        int slotLength = accessor.getFieldSlotsLength();
        for (int j = 0; j < keyIdx.length; ++j) {
            int fIdx = keyIdx[j];
            IBinaryHashFunction hashFn = bloomFilterHashFunctions[blhIdx * keyIdx.length + j];
            int fStart = accessor.getFieldStartOffset(tupleIdx, fIdx);
            int fEnd = accessor.getFieldEndOffset(tupleIdx, fIdx);
            int fh = hashFn.hash(accessor.getBuffer().array(), startOffset + slotLength + fStart, fEnd - fStart);
            h += fh;
        }
        if (h < 0) {
            h = -(h + 1);
        }
        return h % bloomFilterSize;
    }

    private void updateBloomFilter(FrameTupleAccessor accessor, int idx, int[] keyIdx) throws HyracksDataException {
        for (int iter1 = 0; iter1 < bloomHashNum; iter1++) {
            bloomFilter.set(hashByBytes(idx, iter1, accessor, keyIdx));
        }
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        buffers.add(buffer);
        int bIndex = buffers.size() - 1;
        accessorBuild.reset(buffer);
        int tCount = accessorBuild.getTupleCount();
        for (int i = 0; i < tCount; ++i) {
            int entry = tpcBuild.partition(accessorBuild, i, table.getTableSize());
            updateBloomFilter(accessorBuild, i, buildKeys);
            storedTuplePointer.reset(bIndex, i);
            // If an insertion fails, then tries to insert the same tuple pointer again after compacting the table.
            if (!table.insert(entry, storedTuplePointer)) {
                compactTableAndInsertAgain(entry, storedTuplePointer);
            }
        }
    }

    public boolean compactTableAndInsertAgain(int entry, TuplePointer tPointer) throws HyracksDataException {
        boolean oneMoreTry = false;
        if (compactHashTable() >= 0) {
            oneMoreTry = table.insert(entry, tPointer);
        }
        return oneMoreTry;
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

    private boolean checkBloomFilter(IFrameTupleAccessor accessor, int idx, int[] keyIdx) throws HyracksDataException {
        for (int iter1 = 0; iter1 < bloomHashNum; iter1++) {
            if (!bloomFilter.get(hashByBytes(idx, iter1, accessor, keyIdx))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads the given tuple from the probe side and joins it with tuples from the build side.
     * This method assumes that the accessorProbe is already set to the current probe frame.
     */
    void join(int tid, IFrameWriter writer) throws HyracksDataException {
        boolean matchFound = false;
//                if (isTableCapacityNotZero) {
        if (isTableCapacityNotZero && checkBloomFilter(accessorProbe, tid, probeKeys)) {
            int entry = tpcProbe.partition(accessorProbe, tid, table.getTableSize());
            int tupleCount = table.getTupleCount(entry);
            for (int i = 0; i < tupleCount; i++) {
                table.getTuplePointer(entry, i, storedTuplePointer);
                int bIndex = storedTuplePointer.getFrameIndex();
                int tIndex = storedTuplePointer.getTupleIndex();
                accessorBuild.reset(buffers.get(bIndex));
                int c = tpComparator.compare(accessorProbe, tid, accessorBuild, tIndex);
                if (c == 0) {
                    boolean predEval = evaluatePredicate(tid, tIndex);
                    if (predEval) {
                        matchFound = true;
                        //                        int val1 = hashByBytes(tid, 0, accessorProbe, probeKeys);
                        //                        int val2 = hashByBytes(tIndex, 0, accessorBuild, buildKeys);
                        //                        System.out.println("Hashvalue: " + val1 + "    B: " + val2);
                        appendToResult(tid, tIndex, writer);
                    }
                }
                storedTuplePointer.reset(-1, -1);
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
        bloomFilter.clear();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "InMemoryHashJoin has finished using " + nFrames + " frames for Thread ID " + Thread.currentThread()
                            .getId() + ".");
        }
    }

    public void closeTable() throws HyracksDataException {
        table.close();
        bloomFilter.clear();
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
