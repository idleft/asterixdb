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

public class ConciseHashTableTest {

    private IHyracksFrameMgrContext ctx;

    @Before
    public void setUp() {
        ctx = new FrameManager(256);
    }

    @Test
    public void testLinearProbeWithoutCollision() throws Exception {
        int elementRange = 100;
        ConciseHashTable hashTable = new ConciseHashTable(300, ctx);
        TuplePointer tuplePointer = new TuplePointer();
        // update bitmap word list
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < hashTable.PROBE_THRESHOLD + 1; iter2++) {
                hashTable.updatebitmapWords(iter1 * (hashTable.PROBE_THRESHOLD));
            }
        }
        // update counters
        hashTable.populateCountsInBitwords();
        // insert real words
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < hashTable.PROBE_THRESHOLD + 1; iter2++) {
                tuplePointer.reset(iter1, iter1);
                hashTable.insert(iter1 * (hashTable.PROBE_THRESHOLD), tuplePointer);
            }
        }
        // read all element out
        for (int iter1 = 0; iter1 < 300; iter1++) {
            hashTable.getTuplePointer(iter1, 0, tuplePointer);
            System.out.println("Get " + tuplePointer.getFrameIndex() + "  --  " + tuplePointer.getTupleIndex());
            if (iter1 < 200) {
                Assert.assertEquals(iter1 / 2, tuplePointer.getFrameIndex());
                Assert.assertEquals(iter1 / 2, tuplePointer.getTupleIndex());
            } else {
                Assert.assertEquals(300 - iter1 - 1, tuplePointer.getFrameIndex());
                Assert.assertEquals(300 - iter1 - 1, tuplePointer.getTupleIndex());
            }
        }
    }

    @Test
    public void testConciseTable() throws Exception {
        int elementRange = 100;
        ConciseHashTable hashTable = new ConciseHashTable(300, ctx);
        TuplePointer tuplePointer = new TuplePointer();
        // update bitmap word list
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < hashTable.PROBE_THRESHOLD + 1; iter2++) {
                hashTable.updatebitmapWords(iter1 * (hashTable.PROBE_THRESHOLD));
            }
        }
        // update counters
        hashTable.populateCountsInBitwords();
        // insert real words
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < hashTable.PROBE_THRESHOLD + 1; iter2++) {
                tuplePointer.reset(iter1, iter2);
                hashTable.insert(iter1 * (hashTable.PROBE_THRESHOLD), tuplePointer);
            }
        }
        // read all element out
        for (int iter1 = 0; iter1 < 300; iter1++) {
            hashTable.getTuplePointer(iter1, 0, tuplePointer);
            System.out.println(
                    "Get " + tuplePointer.getFrameIndex() + "  --  " + tuplePointer.getTupleIndex() + " at index "
                            + iter1);
            if (iter1 < 200) {
                Assert.assertEquals(iter1 / 2, tuplePointer.getFrameIndex());
                Assert.assertEquals(iter1 % 2, tuplePointer.getTupleIndex());
            } else {
                Assert.assertEquals(300 - iter1 - 1, tuplePointer.getFrameIndex());
                Assert.assertEquals(2, tuplePointer.getTupleIndex());
            }
        }
        // probe
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < hashTable.PROBE_THRESHOLD + 1; iter2++) {
                boolean found = false;
                for (int iter3 = 0; iter3 < hashTable.PROBE_THRESHOLD && found == false; iter3++) {
                    hashTable.getTuplePointer(iter1 * hashTable.PROBE_THRESHOLD, iter3, tuplePointer);
                    if (tuplePointer.getFrameIndex() == iter1 && tuplePointer.getTupleIndex() == iter2) {
                        found = true;
                        System.out.println("Found " + iter1 + " -- " + iter2 + " at table");
                    }
                }
                for (int iter3 = hashTable.overflowTablePtr;
                     iter3 < hashTable.getActualTableSize() && found == false; iter3++) {
                    hashTable.getTuplePointer(iter3, 0, tuplePointer);
                    if (tuplePointer.getFrameIndex() == iter1 && tuplePointer.getTupleIndex() == iter2) {
                        found = true;
                        System.out.println("Found " + iter1 + " -- " + iter2 + " at OT");
                    }
                }
                Assert.assertTrue(found);
            }
        }
    }

    @Test
    public void testNormalInsertTable() throws Exception {
        int elementRange = 100;
        ConciseHashTable hashTable = new ConciseHashTable(300, ctx);
        TuplePointer tuplePointer = new TuplePointer();
        // update bitmap word list
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < hashTable.PROBE_THRESHOLD + 1; iter2++) {
                hashTable.updatebitmapWords(iter1 * (hashTable.PROBE_THRESHOLD));
            }
        }
        // update counters
        hashTable.populateCountsInBitwords();
        // insert real words
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < hashTable.PROBE_THRESHOLD + 1; iter2++) {
                tuplePointer.reset(iter1, iter2);
                hashTable.insert(iter1 * (hashTable.PROBE_THRESHOLD), tuplePointer);
            }
        }
        // read all element out
        for (int iter1 = 0; iter1 < 300; iter1++) {
            hashTable.getTuplePointer(iter1, 0, tuplePointer);
            System.out.println(
                    "Get " + tuplePointer.getFrameIndex() + "  --  " + tuplePointer.getTupleIndex() + " at index "
                            + iter1);
            if (iter1 < 200) {
                Assert.assertEquals(iter1 / 2, tuplePointer.getFrameIndex());
                Assert.assertEquals(iter1 % 2, tuplePointer.getTupleIndex());
            } else {
                Assert.assertEquals(300 - iter1 - 1, tuplePointer.getFrameIndex());
                Assert.assertEquals(2, tuplePointer.getTupleIndex());
            }
        }
        // probe
        for (int iter1 = 0; iter1 < elementRange; iter1++) {
            for (int iter2 = 0; iter2 < hashTable.PROBE_THRESHOLD + 1; iter2++) {
                boolean found = false;
                for (int iter3 = 0; iter3 < hashTable.PROBE_THRESHOLD && found == false; iter3++) {
                    hashTable.getTuplePointer(iter1 * hashTable.PROBE_THRESHOLD, iter3, tuplePointer);
                    if (tuplePointer.getFrameIndex() == iter1 && tuplePointer.getTupleIndex() == iter2) {
                        found = true;
                        System.out.println("Found " + iter1 + " -- " + iter2 + " at table");
                    }
                }
                for (int iter3 = hashTable.overflowTablePtr;
                     iter3 < hashTable.getActualTableSize() && found == false; iter3++) {
                    hashTable.getTuplePointer(iter3, 0, tuplePointer);
                    if (tuplePointer.getFrameIndex() == iter1 && tuplePointer.getTupleIndex() == iter2) {
                        found = true;
                        System.out.println("Found " + iter1 + " -- " + iter2 + " at OT");
                    }
                }
                Assert.assertTrue(found);
            }
        }
    }
}
