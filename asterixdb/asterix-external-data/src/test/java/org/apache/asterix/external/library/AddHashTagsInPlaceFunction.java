/*  1
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
package org.apache.asterix.external.library;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.builtin.JLong;

public class AddHashTagsInPlaceFunction implements IExternalScalarFunction {
    int processedRecords = 0;

    private JLong varCounter = null;
    // local config
    //    private int sliceQuota = 1600000; //5 piece 8M
    private int sliceQuota = 3200000; //
    private int tms = 100;

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        varCounter = new JLong(0l);
        processedRecords = 0;
    }

    @Override
    public void deinitialize() {
        // no op
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        //        JString text = (JString) inputRecord.getValueByName("message_text");
        // measure in counter
        //        long valStart = 0;
        //        while (valStart < 8000000) {
        //            valStart++;
        //        }
        // measure in counter piece
        long etime = System.currentTimeMillis() + tms;
        while (System.currentTimeMillis() < etime) {
            long valStart = 0;
            while (valStart < sliceQuota) {
                valStart++;
            }
        }
        varCounter.setValue(etime);
        inputRecord.addField("ctr", varCounter);
        functionHelper.setResult(inputRecord);
    }
}
