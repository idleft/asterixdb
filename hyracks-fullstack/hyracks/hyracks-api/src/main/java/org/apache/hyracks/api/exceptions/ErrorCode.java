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
package org.apache.hyracks.api.exceptions;

import java.util.HashMap;
import java.util.Map;

/**
 * A registry of runtime error codes
 */

// Error code:
// 0 --- 999:  runtime errors
// 1000 ---- 1999: compilation errors
// 2000 ---- 2999: storage errors
// 3000 ---- 3999: feed errors
// 4000 ---- 4999: lifecycle management errors
public class ErrorCode {
    public static final String HYRACKS = "HYR";
    public static final int ERROR_PROCESSING_TUPLE = 0;
    public static final int INVALID_OPERATOR_OPERATION = 1;
    public static final int FAILURE_ON_NODE = 2;
    public static final int ILLEGAL_ARGUMENT = 3;


    private static Map<Integer, String> errorMessageMap = new HashMap<>();


    // storage errors
    public static final int ERROR_FAIL_CREATE_OPENED_INDEX = 2001;
    public static final int ERROR_FAIL_ACTIVATE_ACTIVATED_INDEX = 2002;
    public static final int ERROR_FAIL_DEACTIVATE_DEACTIVEATED_INDEX = 2003;
    public static final int ERROR_FAIL_DESTROY_OPEN_INDEX = 2004;
    public static final int ERROR_FAIL_CLEAR_OPEN_INDEX = 2005;
    public static final int ERROR_UNABLE_FIND_FREE_PAGE_IN_BUFFER_CACHE_AFTER = 2006;
//    public static final int ERROR_IPC_NOT_IN_CONNECTED_STATE = 2007; // in IPCHandle

    private static final String ERROR_MESSAGE_FAIL_CREATE_OPENED_INDEX = "Failed to create since index is already open.";
    private static final String ERROR_MESSAGE_FAIL_ACTIVATE_ACTIVATED_INDEX = "Failed to activate the index since it is already activated.";
    private static final String ERROR_MESSAGE_FAIL_DEACTIVATE_DEACTIVEATED_INDEX = "Failed to deactivate the index since it is already deactivated.";
    private static final String ERROR_MESSAGE_FAIL_DESTROY_OPEN_INDEX = "Failed to destroy since index is already open.";
    private static final String ERROR_MESSAGE_FAIL_CLEAR_OPEN_INDEX =    "Failed to clear since index is not open.";
    private static final String ERROR_MESSAGE_UNABLE_FIND_FREE_PAGE_IN_BUFFER_CACHE_AFTER = "Unable to find free page in buffer cache after %1$s cycles (buffer cache undersized?); %2$s successful pins since start of cycle";
//    private static final String ERROR_MESSAGE_IPC_NOT_IN_CONNECTED_STATE = "Handle is not in Connected state";

    // feed errors

    public static final int ERROR_PARSER_DATAFLOW_STD_FILE_FIELD_CURSOR_FOR_DELIMITED_PARSER_QUOTE_BEGINNING = 3001;
    public static final int ERROR_PARSER_DATAFLOW_STD_FILE_FIELD_CURSOR_FOR_DELIMITED_PARSER_QUOTE_END = 3002;



    private static final String ERROR_MESSAGE_PARSER_DATAFLOW_STD_FILE_FIELD_CURSOR_FOR_DELIMITED_PARSER_QUOTE =
            "At record: %1$s, field#: %2$s - a quote enclosing a field needs to be placed in the beginning of that field.";
    private static final String ERROR_MESSAGE_PARSER_DATAFLOW_STD_FILE_FIELD_CURSOR_FOR_DELIMITED_PARSER_QUOTE_END=
            "At record: %1$s, field#: %2$s -  A quote enclosing a field needs to be followed by the delimiter.";

    static {
        errorMessageMap.put(ERROR_PARSER_DATAFLOW_STD_FILE_FIELD_CURSOR_FOR_DELIMITED_PARSER_QUOTE_BEGINNING,
                ERROR_MESSAGE_PARSER_DATAFLOW_STD_FILE_FIELD_CURSOR_FOR_DELIMITED_PARSER_QUOTE);
        errorMessageMap.put(ERROR_PARSER_DATAFLOW_STD_FILE_FIELD_CURSOR_FOR_DELIMITED_PARSER_QUOTE_END,
                ERROR_MESSAGE_PARSER_DATAFLOW_STD_FILE_FIELD_CURSOR_FOR_DELIMITED_PARSER_QUOTE_END);
    }


    private ErrorCode() {
    }

    public static String getErrorMessage(int errorCode) {
        String msg = errorMessageMap.get(errorCode);
        if (msg == null) {
            throw new IllegalStateException("Undefined error code: " + errorCode);
        }
        return msg;
    }
}
