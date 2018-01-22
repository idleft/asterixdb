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

import org.apache.asterix.external.library.java.JObjects;
import org.apache.asterix.external.library.java.base.builtin.JBuiltinType;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.builtin.JString;
import org.apache.asterix.external.library.java.base.JUnorderedList;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.util.Datatypes;

import java.io.FileWriter;
import java.time.Instant;

public class AddHashTagsInPlaceFunction implements IExternalScalarFunction {
    int processedRecords = 0;

    private JUnorderedList list = null;
    private JObjects.JLong varCounter = null;

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
        varCounter = new JObjects.JLong(0l);
        processedRecords = 0;
//        fw = new FileWriter("/lv_scratch/scratch/xikuiw/logs/worker_"
//                        fw = new FileWriter("/Volumes/Storage/Users/Xikui/worker_"
//                + this.hashCode() + ".txt");
        //        fw.write("Worker " + Thread.currentThread().getId() + "initialized \n");
    }

    @Override
    public void deinitialize() {
        // no op
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        //        list.clear();
//        JString text = (JString) inputRecord.getValueByName(Datatypes.Tweet.MESSAGE);
//
//        String[] tokens = text.getValue().split(" ");
//        for (String tk : tokens) {
//            if (tk.startsWith("#")) {
//                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
//                newField.setValue(tk);
//                list.add(newField);
//            }
//        }
//        inputRecord.addField(Datatypes.ProcessedTweet.TOPICS, list);
        long varStart = 0;
//
//        if (Instant.now().compareTo(evalutaionEtime) < 0) {
//            //            while (varStart < 520000000) { // this offers 20 tps
        while (varStart < 8000000) {
            //            while (varStart < 80000000) {
            varStart++;
        }
//            processedRecords++;
//        }
//        varCounter.setValue(varStart);
        inputRecord.addField(Datatypes.ProcessedTweet.VAR_COUNTER, varCounter);
        functionHelper.setResult(inputRecord);
    }

}
