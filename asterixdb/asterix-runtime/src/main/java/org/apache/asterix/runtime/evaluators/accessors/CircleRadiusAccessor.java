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
package org.apache.asterix.runtime.evaluators.accessors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CircleRadiusAccessor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private static final FunctionIdentifier FID = AsterixBuiltinFunctions.GET_CIRCLE_RADIUS_ACCESSOR;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CircleRadiusAccessor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable argPtr = new VoidPointable();
                    private final IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
                    private final AMutableDouble aDouble = new AMutableDouble(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ADouble> doubleSerde = SerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADOUBLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval.evaluate(tuple, argPtr);
                        byte[] bytes = argPtr.getByteArray();
                        int startOffset = argPtr.getStartOffset();
                        resultStorage.reset();

                        try {
                            double radius;
                            if (bytes[startOffset] == ATypeTag.SERIALIZED_CIRCLE_TYPE_TAG) {
                                radius = ADoubleSerializerDeserializer.getDouble(bytes,
                                        startOffset + ACircleSerializerDeserializer.getRadiusOffset());
                                aDouble.setValue(radius);
                                doubleSerde.serialize(aDouble, out);
                            } else {
                                throw new TypeMismatchException(getIdentifier(), 0, bytes[startOffset],
                                        ATypeTag.SERIALIZED_CIRCLE_TYPE_TAG);
                            }
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
