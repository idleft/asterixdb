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
package org.apache.asterix.external.library;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;

public class ReligiousPopulationAnnotation implements IExternalScalarFunction {

    private HashMap<String, Integer> countryList;
    private String dictPath;
    private List<String> functionParameters;
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
            if (!countryList.containsKey(items[1])) {
                countryList.put(items[1], 0);
            }
            countryList.put(items[1], countryList.get(items[1]) + Integer.valueOf(items[3]));
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

        obN.setValue(countryList.getOrDefault(countryCode.getValue(), 0));
        inputRecord.addField("religiousPopulation", obN);
        functionHelper.setResult(inputRecord);
    }
}