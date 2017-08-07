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
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LinearProbeHashTableTest {

    private IHyracksFrameMgrContext ctx;

    @Before
    public void setUp() {
        ctx = new FrameManager(256);
    }

    @Test
    public void testLinearProbeWithoutCollision() throws Exception {
        int elementRange = 100;
        LinearProbeHashTable hashTable = new LinearProbeHashTable(elementRange, ctx);
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            hashTable.insert(iter1, new TuplePointer(iter1, iter1));
        }
        TuplePointer readPtr = new TuplePointer(-1, -1);
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            hashTable.getTuplePointer(iter1, 0, readPtr);
            Assert.assertEquals(iter1, readPtr.getFrameIndex());
            Assert.assertEquals(iter1, readPtr.getTupleIndex());
            readPtr.reset(-1, -1);
        }
    }

    @Test
    public void testLinearProbeWithCollision() throws Exception {
        int elementRange = 100;
        int repeat = 4;
        LinearProbeHashTable hashTable = new LinearProbeHashTable(elementRange * repeat, ctx);
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < repeat; iter2++) {
                hashTable.insert(iter1, new TuplePointer(iter1, iter1));
            }
        }
        TuplePointer readPtr = new TuplePointer(-1, -1);
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            int bucketSize = hashTable.getTupleCount(iter1);
            int foundCnt = 0;
            for (int iter2 = 0; iter2 < bucketSize && foundCnt < repeat; iter2++) {
                hashTable.getTuplePointer(iter1, iter2, readPtr);
                if (readPtr.getTupleIndex() == iter1) {
                    foundCnt++;
                }
            }
            Assert.assertEquals(repeat, foundCnt);
        }
    }
}
