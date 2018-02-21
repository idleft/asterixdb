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

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.builtin.JLong;
import org.apache.asterix.external.library.java.base.builtin.JString;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;

public class ONLPSentimentExtractorFunction implements IExternalScalarFunction {

    private static String[] sentimentType = { "Very Negative", "Negative", "Neutral", "Positive", "Very Positive" };
    private static DoccatModel m;
    private JString sentimentText;
    private JLong ctr;
    //    private long limit;

    @Override
    public void deinitialize() {
        System.out.println("De-Initialized");
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        //        JString text = ((JString) functionHelper.getArgument(0));
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JString text = (JString) inputRecord.getValueByName("message_text");
        String sentiment = sentimentType[getSentiment(text.getValue(), m)];
        long varStart = 0;
        while (varStart < 8000000) {
            varStart++;
        }
        sentimentText.setValue(sentiment);
        //        ctr.setValue(varStart);
        inputRecord.addField("sentiment", sentimentText);
        //        inputRecord.addField("ctr", ctr);
        functionHelper.setResult(inputRecord);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        InputStream in = new FileInputStream("/home/xikuiw/en-doccat.bin");
        m = new DoccatModel(in);
        sentimentText = new JString("");
        ctr = new JLong(0l);
    }

    public static int getSentiment(String tweet, DoccatModel model) {
        DocumentCategorizerME myCategorizer = new DocumentCategorizerME(model);
        double[] outcomes = myCategorizer.categorize(tweet);
        String category = myCategorizer.getBestCategory(outcomes);

        if (category.equalsIgnoreCase("0")) {
            return 0;
        } else if (category.equalsIgnoreCase("1")) {
            return 1;
        } else if (category.equalsIgnoreCase("2")) {
            return 2;
        } else if (category.equalsIgnoreCase("3")) {
            return 3;
        } else {
            return 4;
        }
    }
}
