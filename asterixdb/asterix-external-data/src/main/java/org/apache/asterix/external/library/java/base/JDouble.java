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
package org.apache.asterix.external.library.java.base;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.io.DataOutput;
import java.io.IOException;

public final class JDouble extends JObject {

    public JDouble(double v) {
        super(new AMutableDouble(v));
    }

    public void setValue(double v) {
        ((AMutableDouble) value).setValue(v);
    }

    public double getValue() {
        return ((AMutableDouble) value).getDoubleValue();
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        if (writeTypeTag) {
            try {
                dataOutput.writeByte(value.getType().getTypeTag().serialize());
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        ADoubleSerializerDeserializer.INSTANCE.serialize((ADouble) value, dataOutput);
    }

    @Override
    public void reset() {
        ((AMutableDouble) value).setValue(0);
    }

}