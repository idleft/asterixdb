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
import org.apache.asterix.external.library.java.JBuiltinType;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.asterix.external.library.java.base.JUnorderedList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class CountryAnnotation implements IExternalScalarFunction {

    private HashMap<String, String> countryList;
    private String dictPath;
    private String coff;
    private List<String> functionParameters;
    private JOrderedList list = null;

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        list = new JOrderedList(JBuiltinType.JSTRING);
        countryList = new HashMap<>();
        functionParameters = functionHelper.getParameters();
        dictPath = functionParameters.get(0);
        coff = functionParameters.get(1);
        BufferedReader fr = Files.newBufferedReader(Paths.get(dictPath + "_" + coff + ".txt"));
        fr.lines().forEach(line -> {
            String[] items = line.split("\\|");
            countryList.put(items[0], items[1]);
        });
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JString countryCode = (JString) inputRecord.getValueByName("country");

        JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
        newField.setValue(countryList.getOrDefault(countryCode.getValue(), "Not Found"));
        list.reset();
        list.add(newField);
        inputRecord.addField("full-country", list);
        functionHelper.setResult(inputRecord);
    }
}
