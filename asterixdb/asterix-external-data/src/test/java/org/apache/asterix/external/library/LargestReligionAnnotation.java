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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JBuiltinType;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.commons.lang3.tuple.Pair;

public class LargestReligionAnnotation implements IExternalScalarFunction {

    private HashMap<String, List<Pair<String, Integer>>> countryList;
    private String dictPath;
    private List<String> functionParameters;
    private JOrderedList list = null;
    private int refreshRate;
    private int refreshCount;
    private String pathPrefix;
    int nlarge;

    @Override
    public void initialize(IFunctionHelper functionHelper, String nodeInfo) throws Exception {
        if (nodeInfo.startsWith("asterix")) {
            pathPrefix = "/Users/xikuiw/IdeaProjects/TestProject/";
        } else {
            pathPrefix = "/home/xikuiw/decoupled/";
        }
        list = new JOrderedList(JBuiltinType.JSTRING);
        countryList = new HashMap<>();
        functionParameters = functionHelper.getParameters();
        dictPath = pathPrefix + "/" + functionParameters.get(0);
        refreshRate = Integer.valueOf(functionParameters.get(1));
        nlarge = Integer.valueOf(functionParameters.get(2));
        refreshCount = 0;
        loadList();
    }

    @Override
    public void deinitialize() {
    }

    private void loadList() throws Exception {
        BufferedReader fr = Files.newBufferedReader(Paths.get(dictPath));
        fr.lines().forEach(line -> {
            String[] items = line.split("\\|");
            if (!countryList.containsKey(items[1])) {
                countryList.put(items[1], new ArrayList<>());
            }
            countryList.get(items[1]).add(Pair.of(items[2], Integer.valueOf(items[3])));
        });
        for (List<Pair<String, Integer>> e : countryList.values()) {
            Collections.sort(e, ((o1, o2) -> o2.getRight() - o1.getRight()));
        }
        fr.close();
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

        list.reset();
        int ctr = 0;
        for (Pair<String, Integer> client : countryList.get(countryCode.getValue())) {
            JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
            if (ctr++ < nlarge) {
                newField.setValue(client.getLeft());
                list.add(newField);
            } else {
                break;
            }
        }
        inputRecord.addField("rargestReligions", list);
        functionHelper.setResult(inputRecord);
    }
}
