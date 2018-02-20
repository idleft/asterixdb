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
package org.apache.asterix.external.library.java.base.builtin;

import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public abstract class JBuiltinType implements IJType {

    public static JBuiltinType JBooleanType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ABOOLEAN;
        }
    };

    public static JBuiltinType JByteType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINT8;
        }
    };
    public static JBuiltinType JCircleType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ACIRCLE;
        }
    };
    public static JBuiltinType JDateType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ADATE;
        }
    };
    public static JBuiltinType JDateTimeType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ADATETIME;
        }
    };

    public static JBuiltinType JDoubleType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ADOUBLE;
        }
    };

    public static JBuiltinType JDurationType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ADURATION;
        }
    };

    public static JBuiltinType JFloatType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AFLOAT;
        }
    };

    public static JBuiltinType JIntType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINT32;
        }
    };

    public static JBuiltinType JIntervalType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINTERVAL;
        }
    };

    public static JBuiltinType JLineType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ALINE;
        }
    };
    public static JBuiltinType JLongType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINT64;
        }
    };
    public static JBuiltinType JMissingType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AMISSING;
        }
    };
    public static JBuiltinType JNullType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ANULL;
        }
    };
    public static JBuiltinType JPointType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.APOINT;
        }
    };
    public static JBuiltinType JPoint3DType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.APOINT3D;
        }
    };
    public static JBuiltinType JPolygonType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.APOLYGON;
        }
    };
    public static JBuiltinType JRectangleType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ARECTANGLE;
        }
    };
    public static JBuiltinType JShortType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINT8;
        }
    };
    public static JBuiltinType JStringType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ASTRING;
        }
    };
    public static JBuiltinType JTimeType = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ATIME;
        }
    };

}
