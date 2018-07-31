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

public class ObservedByAnnotation implements IExternalScalarFunction {

    private HashMap<String, Set<String>> countryList;
    private String dictPath;
    private List<String> functionParameters;
    private JOrderedList list = null;
    private JInt obN = null;
    private int refreshRate;
    private int refreshCount;
    private String pathPrefix;

    @Override
    public void initialize(IFunctionHelper functionHelper, String nodeInfo) throws Exception {
        if (nodeInfo.startsWith("asterix")) {
            pathPrefix = "/Users/xikuiw/IdeaProjects/TestProject/";
        } else {
            pathPrefix = "/home/xikuiw/decoupled/";
        }
        list = new JOrderedList(JBuiltinType.JSTRING);
        obN = new JInt(0);
        countryList = new HashMap<>();
        functionParameters = functionHelper.getParameters();
        dictPath = pathPrefix + "/" + functionParameters.get(0);
        refreshRate = Integer.valueOf(functionParameters.get(1));
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
            if (!countryList.containsKey(items[2])) {
                countryList.put(items[2], new HashSet<>());
            }
            countryList.get(items[2]).add(items[1]);
        });
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
        for (String client : countryList.get(countryCode.getValue())) {
            JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
            newField.setValue(client);
            list.add(newField);
        }
        inputRecord.addField("observedByClients", list);
        obN.setValue(list.size());
        inputRecord.addField("observedByN", obN);
        functionHelper.setResult(inputRecord);
    }
}
