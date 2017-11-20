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

import org.apache.asterix.external.library.java.JObjects.JRecord;
import org.apache.asterix.external.library.java.JObjects.JString;
import org.apache.asterix.external.library.java.JObjects.JUnorderedList;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.util.Datatypes;

import java.io.FileWriter;
import java.time.Instant;

public class AddHashTagsInPlaceFunction implements IExternalScalarFunction {
    int processedRecords = 0;
    Instant evalutaionEtime;
    FileWriter fw;

    private JUnorderedList list = null;

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
        processedRecords = 0;
        evalutaionEtime = null;
        fw = new FileWriter(System.getProperty("user.home") + "/worker_"
                + this.hashCode() + ".txt");
        //        fw.write("Worker " + Thread.currentThread().getId() + "initialized \n");
        fw.flush();
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        if (evalutaionEtime == null) {
            System.out.println("Function time refreshed for " + this.hashCode());
            evalutaionEtime = Instant.now().plusSeconds(60);
        }
        list.clear();
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JString text = (JString) inputRecord.getValueByName(Datatypes.Tweet.MESSAGE);

        String[] tokens = text.getValue().split(" ");
        for (String tk : tokens) {
            if (tk.startsWith("#")) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(tk);
                list.add(newField);
            }
        }
        inputRecord.addField(Datatypes.ProcessedTweet.TOPICS, list);
        functionHelper.setResult(inputRecord);
        long varStart = 0;

        if (Instant.now().compareTo(evalutaionEtime) < 0) {
            //            while (varStart < 520000000) { // this offers 20 tps
            while (varStart < 8000000) {
                //            while (varStart < 80000000) {
                varStart++;
            }
            processedRecords++;
        }
        fw.write(String.valueOf(processedRecords) + "\n");
        fw.flush();
    }

}
