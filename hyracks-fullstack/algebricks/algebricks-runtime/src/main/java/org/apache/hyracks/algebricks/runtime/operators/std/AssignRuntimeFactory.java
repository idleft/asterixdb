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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AssignRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private static final int THREAD_N = 1;

    private int[] outColumns;
    private IScalarEvaluatorFactory[] evalFactories;
    private final boolean flushFramesRapidly;

    /**
     * @param outColumns
     *            a sorted array of columns into which the result is written to
     * @param evalFactories
     * @param projectionList
     *            an array of columns to be projected
     */

    public AssignRuntimeFactory(int[] outColumns, IScalarEvaluatorFactory[] evalFactories, int[] projectionList) {
        this(outColumns, evalFactories, projectionList, false);
    }

    public AssignRuntimeFactory(int[] outColumns, IScalarEvaluatorFactory[] evalFactories, int[] projectionList,
            boolean flushFramesRapidly) {
        super(projectionList);
        this.outColumns = outColumns;
        this.evalFactories = evalFactories;
        this.flushFramesRapidly = flushFramesRapidly;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("assign [");
        for (int i = 0; i < outColumns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(outColumns[i]);
        }
        sb.append("] := [");
        for (int i = 0; i < evalFactories.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(evalFactories[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        final int[] projectionToOutColumns = new int[projectionList.length];
        for (int j = 0; j < projectionList.length; j++) {
            projectionToOutColumns[j] = Arrays.binarySearch(outColumns, projectionList[j]);
        }

        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private IScalarEvaluator[] eval = new IScalarEvaluator[evalFactories.length];
            private boolean first = true;
            private boolean isOpen = false;
            private ExecutorService executorService = Executors.newFixedThreadPool(THREAD_N);
            private Object frameAccessLock = new Object();
            private Queue<Pair<Integer, Exception>> exceptionQueue = new LinkedBlockingQueue<>();

            @Override
            public void open() throws HyracksDataException {
                if (first) {
                    initAccessAppendRef(ctx);
                    first = false;
                    int n = evalFactories.length;
                    for (int i = 0; i < n; i++) {
                        eval[i] = evalFactories[i].createScalarEvaluator(ctx);
                    }
                }
                isOpen = true;
                writer.open();
            }

            @Override
            public void close() throws HyracksDataException {
                if (isOpen) {
                    super.close();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // what if nTuple is 0?
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                if (nTuple < 0) {
                    throw new HyracksDataException("Negative number of tuples in the frame: " + nTuple);
                } else if (nTuple == 0) {
                    appender.flush(writer);
                } else {
                    List<Future> futureList = new ArrayList<>();
                    for (int iter1 = 0; iter1 < nTuple; iter1++) {
                        futureList.add(executorService.submit(new AssignOpWorker(iter1)));
                    }
                    for (Future future : futureList) {
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            new HyracksDataException(e);
                        }
                    }
                    if (!exceptionQueue.isEmpty()) {
                        Pair<Integer, Exception> exceptionPair = exceptionQueue.poll();
                        throw HyracksDataException.create(ErrorCode.ERROR_PROCESSING_TUPLE, exceptionPair.getRight(),
                                exceptionPair.getLeft());
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                if (isOpen) {
                    super.fail();
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                appender.flush(writer);
            }

            class AssignOpWorker implements Runnable {

                private ArrayTupleBuilder tupleBuilder;
                private FrameTupleReference tupleRef;
                private IPointable result = VoidPointable.FACTORY.createPointable();
                private int tupleIdx;

                public AssignOpWorker(int tupleIdx) {
                    tupleBuilder = new ArrayTupleBuilder(projectionList.length);
                    tupleRef = new FrameTupleReference();
                    tupleRef.reset(tAccess, tupleIdx);
                    this.tupleIdx = tupleIdx;
                }

                @Override
                public void run() {
                    try {
                        for (int iter1 = 0; iter1 < projectionList.length; iter1++) {
                            int k = projectionToOutColumns[iter1];
                            if (k >= 0) {
                                eval[k].evaluate(tupleRef, result);
                                tupleBuilder.addField(result.getByteArray(), result.getStartOffset(),
                                        result.getLength());
                            } else {
                                tupleBuilder.addField(tAccess, tupleIdx, projectionList[iter1]);
                            }
                        }
                        synchronized (frameAccessLock) {
                            if (flushFramesRapidly) {
                                appendToFrameFromTupleBuilder(tupleBuilder, true);
                            } else {
                                appendToFrameFromTupleBuilder(tupleBuilder);
                            }
                        }
                    } catch (Exception e) {
                        exceptionQueue.add(Pair.of(tupleIdx, e));
                    }
                }
            }
        };
    }
}
