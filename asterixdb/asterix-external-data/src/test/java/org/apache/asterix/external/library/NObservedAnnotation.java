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

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JBuiltinType;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;

public class NObservedAnnotation implements IExternalScalarFunction {

    private HashMap<String, Set<String>> countryList1;
    private HashMap<String, Set<String>> countryList2;
    private HashMap<String, Set<String>> countryList3;
    private HashMap<String, Set<String>> countryList4;
    private HashMap<String, Set<String>> countryList5;

    private String dictPath1;
    private String dictPath2;
    private String dictPath3;
    private String dictPath4;
    private String dictPath5;
    private List<String> functionParameters;
    private JOrderedList list1 = null;
    private JOrderedList list2 = null;
    private JOrderedList list3 = null;
    private JOrderedList list4 = null;
    private JOrderedList list5 = null;
    private int refreshRate;
    private int refreshCount;
    private String pathPrefix;
    private int nObserved;

    @Override
    public void initialize(IFunctionHelper functionHelper, String nodeInfo) throws Exception {
        if (nodeInfo.startsWith("asterix")) {
            pathPrefix = "/Users/xikuiw/IdeaProjects/TestProject/";
        } else {
            pathPrefix = "/home/xikuiw/decoupled/";
        }
        list1 = new JOrderedList(JBuiltinType.JSTRING);
        list2 = new JOrderedList(JBuiltinType.JSTRING);
        list3 = new JOrderedList(JBuiltinType.JSTRING);
        list4 = new JOrderedList(JBuiltinType.JSTRING);
        list5 = new JOrderedList(JBuiltinType.JSTRING);

        countryList1 = new HashMap<>();
        countryList2 = new HashMap<>();
        countryList3 = new HashMap<>();
        countryList4 = new HashMap<>();
        countryList5 = new HashMap<>();

        functionParameters = functionHelper.getParameters();
        refreshRate = Integer.valueOf(functionParameters.get(0));
        nObserved = Integer.valueOf(functionParameters.get(1));

        dictPath1 = pathPrefix + "/" + functionParameters.get(2);
        dictPath2 = pathPrefix + "/" + functionParameters.get(3);
        dictPath3 = pathPrefix + "/" + functionParameters.get(4);
        dictPath4 = pathPrefix + "/" + functionParameters.get(5);
        dictPath5 = pathPrefix + "/" + functionParameters.get(6);

        refreshCount = 0;
        loadList();
    }

    @Override
    public void deinitialize() {
    }

    private void loadList() throws Exception {

        BufferedReader fr1 = Files.newBufferedReader(Paths.get(dictPath1));
        fr1.lines().forEach(line -> {
            String[] items = line.split("\\|");
            if (!countryList1.containsKey(items[2])) {
                countryList1.put(items[2], new HashSet<>());
            }
            countryList1.get(items[2]).add(items[1]);
        });
        fr1.close();

        if (nObserved > 1) {
            BufferedReader fr2 = Files.newBufferedReader(Paths.get(dictPath2));
            fr2.lines().forEach(line -> {
                String[] items = line.split("\\|");
                if (!countryList2.containsKey(items[2])) {
                    countryList2.put(items[2], new HashSet<>());
                }
                countryList2.get(items[2]).add(items[1]);
            });
            fr2.close();
        }

        if (nObserved > 2) {
            BufferedReader fr3 = Files.newBufferedReader(Paths.get(dictPath3));
            fr3.lines().forEach(line -> {
                String[] items = line.split("\\|");
                if (!countryList3.containsKey(items[2])) {
                    countryList3.put(items[2], new HashSet<>());
                }
                countryList3.get(items[2]).add(items[1]);
            });
            fr3.close();
        }

        if (nObserved > 3) {
            BufferedReader fr4 = Files.newBufferedReader(Paths.get(dictPath4));
            fr4.lines().forEach(line -> {
                String[] items = line.split("\\|");
                if (!countryList4.containsKey(items[2])) {
                    countryList4.put(items[2], new HashSet<>());
                }
                countryList4.get(items[2]).add(items[1]);
            });
            fr4.close();
        }

        if (nObserved > 4) {
            BufferedReader fr5 = Files.newBufferedReader(Paths.get(dictPath5));
            fr5.lines().forEach(line -> {
                String[] items = line.split("\\|");
                if (!countryList5.containsKey(items[2])) {
                    countryList5.put(items[2], new HashSet<>());
                }
                countryList5.get(items[2]).add(items[1]);
            });
            fr5.close();
        }
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {

        if (refreshRate > 0) {
            refreshCount = (refreshCount + 1) % refreshRate;
            if (refreshCount == 0) {
                loadList();
            }
        }

        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JString countryCode = (JString) inputRecord.getValueByName("country");

        list1.reset();
        for (String client : countryList1.get(countryCode.getValue())) {
            JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
            newField.setValue(client);
            list1.add(newField);
        }
        inputRecord.addField("observedByClients1", list1);

        if (nObserved > 1) {
            list2.reset();
            for (String client : countryList2.get(countryCode.getValue())) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(client);
                list2.add(newField);
            }
            inputRecord.addField("observedByClients2", list2);
        }

        if (nObserved > 2) {
            list3.reset();
            for (String client : countryList3.get(countryCode.getValue())) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(client);
                list3.add(newField);
            }
            inputRecord.addField("observedByClients3", list3);
        }

        if (nObserved > 3) {
            list4.reset();
            for (String client : countryList4.get(countryCode.getValue())) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(client);
                list4.add(newField);
            }
            inputRecord.addField("observedByClients4", list4);
        }

        if (nObserved > 4) {
            list5.reset();
            for (String client : countryList5.get(countryCode.getValue())) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(client);
                list5.add(newField);
            }
            inputRecord.addField("observedByClients5", list5);
        }

        functionHelper.setResult(inputRecord);
    }
}
