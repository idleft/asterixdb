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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JBuiltinType;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.library.java.base.JDouble;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.commons.lang3.tuple.Pair;

public class NearbyMonumentsAnnotation implements IExternalScalarFunction {

    private HashMap<String, Pair<Double, Double>> monumentList;
    private String dictPath;
    private List<String> functionParameters;
    private int refreshRate;
    private int refreshCount;
    private JOrderedList list = null;
    private String pathPrefix;
    double max_dist;

    @Override
    public void initialize(IFunctionHelper functionHelper, String nodeInfo) throws Exception {
        if (nodeInfo.startsWith("asterix")) {
            pathPrefix = "/Users/xikuiw/IdeaProjects/TestProject/";
        } else {
            pathPrefix = "/home/xikuiw/decoupled/";
        }
        monumentList = new HashMap<>();
        list = new JOrderedList(JBuiltinType.JSTRING);
        functionParameters = functionHelper.getParameters();
        dictPath = pathPrefix + "/" + functionParameters.get(0);
        refreshRate = Integer.valueOf(functionParameters.get(1));
        max_dist = Double.valueOf(functionParameters.get(2));
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
            monumentList.put(items[0], Pair.of(Double.valueOf(items[1]), Double.valueOf(items[2])));
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

        list.reset();
        JDouble lat = (JDouble) inputRecord.getValueByName("latitude");
        JDouble lon = (JDouble) inputRecord.getValueByName("longitude");
        for (Map.Entry entry : monumentList.entrySet()) {
            Double clat = ((Pair<Double, Double>) entry.getValue()).getKey();
            Double clon = ((Pair<Double, Double>) entry.getValue()).getValue();
            Double distance = Math.sqrt((clat - lat.getValue()) * (clat - lat.getValue())
                    + (clon - lon.getValue()) * (clon - lon.getValue()));
            if (distance.compareTo(max_dist) < 0) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue((String) entry.getKey());
                list.add(newField);
            }
        }
        inputRecord.addField("nearbyMonuments", list);
        functionHelper.setResult(inputRecord);
    }
}
