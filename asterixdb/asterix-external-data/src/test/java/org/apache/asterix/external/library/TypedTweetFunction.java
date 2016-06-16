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

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JObjects;
import org.apache.asterix.external.library.java.JObjects.JRecord;

public class TypedTweetFunction implements IExternalScalarFunction {

    @Override
    public void initialize(IFunctionHelper functionHelper) {
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);

        JObjects.JLong id = (JObjects.JLong) inputRecord.getValueByName("id");
        JObjects.JString text = (JObjects.JString) inputRecord.getValueByName("text");
        JObjects.JString created_at_str = (JObjects.JString) inputRecord.getValueByName("timestamp_ms");
        JObjects.JDateTime created_at = new JObjects.JDateTime(Long.valueOf(created_at_str.getValue()));


        JRecord result = (JRecord) functionHelper.getResultObject();
        result.setField("id", id);
        result.setField("text", text);
        result.setField("created_at",created_at);
        functionHelper.setResult(result);
    }
}
