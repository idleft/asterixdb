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
package org.apache.hyracks.algebricks.common.exceptions;

import java.io.Serializable;

public class AlgebricksException extends Exception {
    private static final long serialVersionUID = 1L;

    public static final String NONE = "";
    public static final int UNKNOWN = 0;

    private final String component;
    private final int errorCode;
    private final Serializable[] params;
    private final String nodeId;

    public AlgebricksException(String component, int errorCode, String message, Throwable cause, String nodeId,
            Serializable... params) {
        super(message, cause);
        this.errorCode = errorCode;
        this.component = component;
        this.nodeId = nodeId;
        this.params = params;
    }

    public AlgebricksException(String component, int errorCode, String message, Serializable... params) {
        this(component, errorCode, message, null, null, params);
    }

    public AlgebricksException(String component, int errorCode, Throwable cause, Serializable... params) {
        this(component, errorCode, cause.getMessage(), cause, null, params);
    }

    public AlgebricksException(String message) {
        this(NONE, UNKNOWN, message, (Throwable) null, (String) null);
    }

    public AlgebricksException(Throwable cause) {
        this(NONE, UNKNOWN, cause.getMessage(), cause, (String) null);
    }

    public AlgebricksException(String message, Throwable cause) {
        this(NONE,UNKNOWN, message, cause, (String) null);
    }
}
