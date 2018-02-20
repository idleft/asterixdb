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

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalFunction;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileWriter;
import java.time.Instant;

public class ExternalFunctionProvider {

    public static IExternalFunction getExternalFunctionEvaluator(IExternalFunctionInfo finfo,
            IScalarEvaluatorFactory args[], IHyracksTaskContext context, IApplicationContext appCtx)
            throws HyracksDataException {
        switch (finfo.getKind()) {
            case SCALAR:
                return new ExternalScalarFunction(finfo, args, context, appCtx);
            case AGGREGATE:
            case UNNEST:
                throw new RuntimeDataException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND, finfo.getKind());
            default:
                throw new RuntimeDataException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND, finfo.getKind());
        }
    }
}

class ExternalScalarFunction extends ExternalFunction implements IExternalScalarFunction, IScalarEvaluator {

    Logger LOGGER = LogManager.getLogger();

    private String resultFilePath;
    private FileWriter fw;
    private long elapsedTime;
    private long recordCounter;

    public ExternalScalarFunction(IExternalFunctionInfo finfo, IScalarEvaluatorFactory args[],
            IHyracksTaskContext context, IApplicationContext appCtx) throws HyracksDataException {
        super(finfo, args, context, appCtx);
        try {
            initialize(functionHelper);
            // start of test
            String nodeId = context.getJobletContext().getServiceContext().getNodeId();
            if (nodeId.startsWith("asterix")) {
                resultFilePath = "/Volumes/Storage/Users/Xikui/worker_";
            } else if (nodeId.startsWith("local")) {
                resultFilePath = "/lv_scratch/scratch/xikuiw/logs/worker_";
            } else if (nodeId.startsWith("aws")) {
                resultFilePath = System.getProperty("user.home") + "/expr_logs/worker_";
            }
            // end of test
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        try {
            try {
                // Start of evaluation code
                if (fw == null) {
                    fw = new FileWriter(resultFilePath + this.hashCode() + ".txt");
                    LOGGER.log(Level.INFO, this.hashCode() + ".txt opened for writing result");
                    elapsedTime = 0;
                    recordCounter = 0;
                }
                // end of evaluation code
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            long val0;
            long startTime = System.currentTimeMillis();
            setArguments(tuple);
            evaluate(functionHelper);
            result.set(resultBuffer.getByteArray(), resultBuffer.getStartOffset(), resultBuffer.getLength());
            functionHelper.reset();
            val0 = System.currentTimeMillis() - startTime;
            elapsedTime = elapsedTime + val0;
            recordCounter++;
            fw.write(Instant.now().toString() + " Record " + recordCounter + " etime0 " + val0 + "\n");
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void evaluate(IFunctionHelper argumentProvider) throws HyracksDataException {
        try {
            resultBuffer.reset();
            ((IExternalScalarFunction) externalFunction).evaluate(argumentProvider);
            if (!argumentProvider.isValidResult()) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_UDF_RESULT_TYPE_ERROR);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() {
        System.out.println("Function is closed " + externalFunction.getClass().getSimpleName());
        // Start of evaluation
        try {
            if (fw != null) {
                fw.write("Summary " + recordCounter + " " + elapsedTime + "\n" + String.valueOf(elapsedTime) + "\n");
                fw.flush();
            }
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, e);
        }
        // End of evaluation
        externalFunction.deinitialize();
    }

}
