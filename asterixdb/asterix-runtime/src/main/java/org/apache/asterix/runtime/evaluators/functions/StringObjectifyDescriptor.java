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

package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
//import org.apache.asterix.external.input.record.GenericRecord;
//import org.apache.asterix.external.parser.ADMDataParser;
//import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class StringObjectifyDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
//    private static final ADMDataParser parser = new ADMDataParser(MetadataBuiltinEntities.ANY_OBJECT_RECORD_TYPE, false);


    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringObjectifyDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    // Argument evaluators.
                    private final IScalarEvaluator stringEval = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator patternEval = args[1].createScalarEvaluator(ctx);

                    // Argument pointers.
                    private final IPointable argString = new VoidPointable();
                    private final IPointable argPattern = new VoidPointable();
                    private final UTF8StringPointable argStrPtr = new UTF8StringPointable();
                    private final UTF8StringPointable argPatternPtr = new UTF8StringPointable();

                    // For an output string item.
                    private final ArrayBackedValueStorage itemStorge = new ArrayBackedValueStorage();
                    private final DataOutput itemOut = itemStorge.getDataOutput();
                    private final byte[] tempLengthArray = new byte[5];
//                    private GenericRecord<char []> stringRecord;

                    // For the output list of strings.
                    private final AOrderedListType intListType = new AOrderedListType(BuiltinType.ASTRING, null);
                    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            resultStorage.reset();
                            // Calls argument evaluators.
                            stringEval.evaluate(tuple, argString);
                            patternEval.evaluate(tuple, argPattern);

                            // Gets the bytes of the source string.
                            byte[] srcString = argString.getByteArray();
//                            stringRecord.set(srcString.toString().toCharArray());
//                            parser.parse(stringRecord, out);
                            result.set(resultStorage);
                        } catch (IOException e1) {
                            throw new HyracksDataException(e1);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_SPLIT;
    }

}
