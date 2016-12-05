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
package org.apache.asterix.common.exceptions;

import java.util.HashMap;
import java.util.Map;

// Error code:
// 0 --- 999:  runtime errors
// 1000 ---- 1999: compilation errors
// 2000 ---- 2999: storage errors
// 3000 ---- 3999: feed errors
// 4000 ---- 4999: lifecycle management errors
public class ErrorCode {
    public static final String ASTERIX = "ASX";

    // Extension errors
    public static final int ERROR_EXTENSION_ID_CONFLICT = 4001;
    public static final int ERROR_EXTENSION_COMPONENT_CONFLICT = 4002;

    // Runtime errors
    public static final int ERROR_CASTING_FIELD = 1;
    public static final int ERROR_TYPE_MISMATCH = 2;
    public static final int ERROR_TYPE_INCOMPATIBLE = 3;
    public static final int ERROR_TYPE_UNSUPPORTED = 4;
    public static final int ERROR_TYPE_ITEM = 5;
    public static final int ERROR_INVALID_FORMAT = 6;
    public static final int ERROR_OVERFLOW = 7;
    public static final int ERROR_UNDERFLOW = 8;
    public static final int ERROR_INJECTED_FAILURE = 9;
    public static final int ERROR_NEGATIVE_VALUE = 10;
    public static final int ERROR_OUT_OF_BOUND = 11;
    public static final int ERROR_COERCION = 12;
    public static final int ERROR_DUPLICATE_FIELD = 13;

    // Compilation errors
    public static final int ERROR_PARSE_ERROR = 1001;

    private static final String ERROR_MESSAGE_ID_CONFLICT = "Two Extensions share the same Id: %1$s";
    private static final String ERROR_MESSAGE_COMPONENT_CONFLICT = "Extension Conflict between %1$s and %2$s both "
            + "extensions extend %3$s";
    private static final String ERROR_MESSAGE_TYPE_MISMATCH = "Type mismatch: function %1$s expects"
            + " its %2$s input parameter to be type %3$s, but the actual input type is %4$s";
    private static final String ERROR_MESSAGE_TYPE_INCOMPATIBLE = "Type incompatibility: function %1$s gets"
            + " incompatible input values: %2$s and %3$s";
    private static final String ERROR_MESSAGE_TYPE_UNSUPPORTED = "Unsupported type: %1$s"
            + " cannot process input type %2$s";
    private static final String ERROR_MESSAGE_TYPE_ITEM = "Invalid item type: function %1$s"
            + " cannot process item type %2$s in an input array (or multiset)";
    public static final String ERROR_MESSAGE_INVALID_FORMAT = "Invalid format for %1$s in %2$s";
    public static final String ERROR_MESSAGE_OVERFLOW = "Overflow happend in %1$s";
    public static final String ERROR_MESSAGE_UNDERFLOW = "Underflow happend in %1$s";
    public static final String ERROR_MESSAGE_INJECTED_FAILURE = "Injected failure in %1$s";
    public static final String ERROR_MESSAGE_NEGATIVE_VALUE = "Invalid value: function %1$s expects"
            + " its %2$s input parameter to be a non-negative value, but gets %3$s";
    public static final String ERROR_MESSAGE_OUT_OF_BOUND = "Index out of bound in %1$s: %2$s";
    public static final String ERROR_MESSAGE_COERCION = "Invalid implicit scalar to collection coercion in %1$s";
    public static final String ERROR_MESSAGE_DUPLICATE_FIELD = "Get duplicate fields in %1$s";

    // Feed errors
    public static final int ERROR_ILLEGAL_STATE = 3001;
    public static final int ERROR_TUPLE_TOO_LARGE = 3002;
    public static final int ERROR_UNKNOWN_FOWARD_POLICY = 3003;
    // 3004 in hyracks error code
    public static final int ERROR_PARSER_DELIMITED_NONOPTIONAL_NULL = 3005;
    public static final int ERROR_PARSER_DELIMITED_ILLEGAL_FIELD = 3006;
    public static final int ERROR_ACTIVE_JOB_FAILURE = 3007;
    public static final int ERROR_FAIL_AT_INGESTION = 3008;
    public static final int ERROR_INVALID_SUBSCRIBABLE_RUNTIME = 3009;
    public static final int ERROR_PARSER_HIVE_NON_PRIMITIVE_LIST_NOT_SUPPORT = 3010;
    public static final int ERROR_PARSER_HIVE_FIELD_TYPE = 3011;
    public static final int ERROR_PARSER_HIVE_GET_COLUMNS = 3012;
    public static final int ERROR_PARSER_HIVE_NO_CLOSED_COLUMNS = 3013;
    public static final int ERROR_PARSER_HIVE_NOT_SUPPORT_NON_OP_UNION = 3014;
    public static final int ERROR_PARSER_HIVE_MISSING_FIELD_TYPE_INFO = 3015;
    public static final int ERROR_PARSER_HIVE_NULL_FIELD = 3016;
    public static final int ERROR_PARSER_HIVE_NULL_VALUE_IN_LIST = 3017;
    public static final int ERROR_READER_RECORD_W_META_PK_NULL_IN_NON_OPTIONAL = 3018;
    public static final int ERROR_READER_RECORD_W_META_PK_GET_PKEY = 3019;
    public static final int ERROR_FEED_EVENT_LISTENER_FEED_JOINT_REGISTERED = 3020;
    public static final int ERROR_FEED_EVENT_REGISTER_INTAKE_JOB_FAIL = 3021;
    public static final int ERROR_PROVIDER_DATAFLOW_CONTROLLER_UNKNOWN_DATA_SOURCE = 3022;
    public static final int ERROR_PROVIDER_DATASOURCE_FACTORY_UNKNOWN_INPUT_STREAM_FACTORY = 3023;
    public static final int ERROR_FAIL_CREATE_STREAM_FACTORY = 3024;
    public static final int ERROR_UNKNOWN_RECORD_READER_FACTORY = 3025;
    public static final int ERROR_PROVIDER_STREAM_RECORD_READER_UNKNOWN_FORMAT = 3026;
    public static final int ERROR_UNKNOWN_RECORD_FORMAT_FOR_META_PARSER = 3027;
    public static final int ERROR_EXTERNAL_FUNC_JOBJECTS_FIELD_ALREADY_DEFINED = 3028;
    public static final int ERROR_EXTERNAL_FUNC_JOBJECTS_UNKNOWN_FIELD= 3029;
    public static final int ERROR_NODE_RESOLVER_COULDNT_RESOLVE_ADDRESS = 3030;
    public static final int ERROR_NODE_RESOLVER_NO_NODE_CONTROLLERS = 3031;
    public static final int ERROR_NODE_RESOLVER_UNABLE_RESOLVE_HOST = 3032;
    public static final int ERROR_RECORD_CONVERTER_UNKNOWN_DCP_REQUEST = 3033;
    public static final int ERROR_FRAME_DISTRIBUTOR_REGISTER_FAILED_DATA_PROVIDER = 3034;
    public static final int ERROR_FEED_EVENTS_LISTENER_ALREADY_HAVE_INTAKE_JOB = 3035;
    public static final int ERROR_FEED_EVENTS_LISTENER_INTAKE_JOB_REGISTERED = 3036;
    public static final int ERROR_FEED_EVENTS_LISTENER_FEED_JOB_REGISTERED = 3037;
    public static final int ERROR_RECORD_READER_CHAR_ARRAY_RECORD_TOO_LARGE = 3038;
    public static final int ERROR_LIBRARY_JOBJECT_ACCESSOR_CANNOT_PARSE_TYPE = 3039;
    public static final int ERROR_LIBRARY_JOBJECT_UTIL_ILLEGAL_ARGU_TYPE = 3040;
    public static final int ERROR_LIBRARY_EXTERNAL_FUNCTION_UNABLE_TO_LOAD_CLASS = 3041;
    public static final int ERROR_LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND = 3042;
    public static final int ERROR_LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND = 3043;
    public static final int ERROR_LIBRARY_EXTERNAL_LIBRARY_CLASS_REGISTERED = 3044;
    public static final int ERROR_LIBRARY_JAVA_FUNCTION_HELPER_CANNOT_HANDLE_ARGU_TYPE = 3045;
    public static final int ERROR_LIBRARY_JAVA_FUNCTION_HELPER_OBJ_TYPE_NOT_SUPPORTED = 3046;
    public static final int ERROR_LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_NAME = 3047;
    public static final int ERROR_OPERATOR_FEED_META_OPERATOR_DESCRIPTOR_INVALID_RUNTIME = 3048;
    public static final int ERROR_PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_NOT_VALID_DELIMITER = 3049;
    public static final int ERROR_PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_NOT_VALID_QUOTE = 3050;
    public static final int ERROR_PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_QUOTE_DELIMITER_MISMATCH = 3051;
    public static final int ERROR_PARSER_FACTORY_HIVE_DATA_PARSER_FACTORY_NO_SERDE = 3052;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_FIELD_NOT_NULL = 3053;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_TYPE_MISMATCH = 3054;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_KIND = 3055;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_ILLEGAL_ESCAPE = 3056;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_RECORD_END_UNEXPECTED = 3057;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_EXTRA_FIELD_IN_CLOSED_RECORD = 3058;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_WHEN_EXPECT_COMMA = 3059;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_FOUND_COMMA_WHEN = 3060;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_UNSUPPORTED_INTERVAL_TYPE = 3061;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_INTERVAL_NOT_CLOSED = 3062;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_INTERVAL_BEGIN_END_POINT_MISMATCH = 3063;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_INTERVAL_MISSING_COMMA = 3064;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_INTERVAL_INVALID_DATETIME = 3065;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_INTERVAL_UNSUPPORTED_TYPE = 3066;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_INTERVAL_INTERVAL_ARGUMENT_ERROR = 3067;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_LIST_FOUND_END_COLLECTION = 3068;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_BEFORE_LIST = 3069;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_EXPECTING_ITEM = 3070;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_LIST_FOUND_END_RECOD = 3071;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_CAST_ERROR = 3072;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_CONSTRUCTOR_MISSING_DESERIALIZER = 3073;
    public static final int ERROR_PARSER_ADM_DATA_PARSER_WRONG_INSTANCE= 3074;
    public static final int ERROR_PARSER_TWEET_PARSER_CLOSED_FIELD_NULL = 3075;
    public static final int ERROR_UTIL_FILE_SYSTEM_WATCHER_NO_FILES_FOUND = 3076;
    public static final int ERROR_UTIL_LOCAL_FILE_SYSTEM_UTILS_PATH_NOT_FOUND = 3077;
    public static final int ERROR_UTIL_HDFS_UTILS_CANNOT_OBTAIN_HDFS_SCHEDULER = 3078;


    public static final String ERROR_MESSAGE_ILLEGAL_STATE = "Illegal state: %1$s.";
    public static final String ERROR_MESSAGE_TUPLE_TOO_LARGE = "Tuple is too large for a frame";
    public static final String ERROR_MESSAGE_UNKNOWN_FORWARD_POLICY = "Unknown tuple forward policy";
    public static final String ERROR_MESSAGE_PARSER_DELIMITED_NONOPTIONAL_NULL = "At record: %1$s - Field %2$s is not "
            + "an optional type so it cannot accept null value.";
    public static final String ERROR_MESSAGE_PARSER_DELIMITED_ILLEGAL_FIELD = "Illegal field %1$s in closed type %2$s";
    public static final String ERROR_MESSAGE_ACTIVE_JOB_FAILURE = "Failure in active job.";
    private static final String ERROR_MESSAGE_FAIL_AT_INGESTION = "Unable to ingest data";
    private static final String ERROR_MESSAGE_INVALID_SUBSCRIBABLE_RUNTIME = "Invalid subscribable runtime type %1$s";
    public static final String ERROR_MESSAGE_PARSER_HIVE_NON_PRIMITIVE_LIST_NOT_SUPPORT = "doesn't support hive data "
            + "with list of non-primitive types";
    public static final String ERROR_MESSAGE_PARSER_HIVE_FIELD_TYPE = "Can't get hive type for field of type %1$s";
    public static final String ERROR_MESSAGE_PARSER_HIVE_GET_COLUMNS = "Failed to get columns of record";
    public static final String ERROR_MESSAGE_PARSER_HIVE_NO_CLOSED_COLUMNS = "Can't deserialize hive records with no closed columns";
    public static final String ERROR_MESSAGE_PARSER_HIVE_NOT_SUPPORT_NON_OP_UNION = "Non-optional UNION type is not supported.";
    public static final String ERROR_MESSAGE_HIVE_MISSING_FIELD_TYPE_INFO = "Failed to get the type information for field %1$s.";
    public static final String ERROR_MESSAGE_PARSER_HIVE_NULL_FIELD = "can't parse null field";
    public static final String ERROR_MESSAGE_PARSER_HIVE_NULL_VALUE_IN_LIST = "can't parse hive list with null values";
    public static final String ERROR_MESSAGE_READER_RECORD_W_META_PK_NULL_IN_NON_OPTIONAL = "Field %1$s of meta record is not an optional type so it cannot accept null value.";
    public static final String ERROR_MESSAGE_READER_RECORD_W_META_PK_GET_PKEY = "Can't get PK from record part";
    public static final String ERROR_MESSAGE_FEED_EVENT_LISTENER_FEED_JOINT_REGISTERED = "Feed joint %1$s already registered";
    public static final String ERROR_MESSAGE_FEED_EVENT_REGISTER_INTAKE_JOB_FAIL = "Could not register feed intake job [%1$s] for feed  %2$s";
    public static final String ERROR_MESSAGE_PROVIDER_DATAFLOW_CONTROLLER_UNKNOWN_DATA_SOURCE = "Unknown data source type: %1$s";
    public static final String ERROR_MESSAGE_PROVIDER_DATASOURCE_FACTORY_UNKNOWN_INPUT_STREAM_FACTORY = "unknown input stream factory: %1$s";
    public static final String ERROR_MESSAGE_CREATE_STREAM_FACTORY = "Failed to create stream factory";
    public static final String ERROR_MESSAGE_UNKNOWN_RECORD_READER_FACTORY = "Unknown record reader factory: %1$s";
    public static final String ERROR_MESSAGE_PROVIDER_STREAM_RECORD_READER_UNKNOWN_FORMAT = "Unknown format: %1$s";
    public static final String ERROR_MESSAGE_UNKNOWN_RECORD_FORMAT_FOR_META_PARSER = "Unknown record format for a record with meta parser. Did you specify the parameter %1$s"; // record-format is a constant from external data
    public static final String ERROR_MESSAGE_EXTERNAL_FUNC_JOBJECTS_FIELD_ALREADY_DEFINED = "field already defined in %1$s part";
    public static final String ERROR_MESSAGE_EXTERNAL_FUNC_JOBJECTS_UNKNOWN_FIELD= "unknown field: %1$s";
    public static final String ERROR_MESSAGE_NODE_RESOLVER_COULDNT_RESOLVE_ADDRESS = "address passed: '%1$s' couldn't be resolved to an ip address and is not an NC id. Existing NCs are %2$s";
    public static final String ERROR_MESSAGE_NODE_RESOLVER_NO_NODE_CONTROLLERS = " No node controllers found at the address: %1$s";
    public static final String ERROR_MESSAGE_NODE_RESOLVER_UNABLE_RESOLVE_HOST = "Unable to resolve hostname '%1$s' to an IP address";
    public static final String ERROR_MESSAGE_RECORD_CONVERTER_UNKNOWN_DCP_REQUEST = "Unknown DCP request: %1$s";
    public static final String ERROR_MESSAGE_FRAME_DISTRIBUTOR_REGISTER_FAILED_DATA_PROVIDER = "attempt to register to a failed feed data provider";
    public static final String ERROR_MESSAGE_FEED_EVENTS_LISTENER_ALREADY_HAVE_INTAKE_JOB = "Feed already has an intake job";
    public static final String ERROR_MESSAGE_FEED_EVENTS_LISTENER_INTAKE_JOB_REGISTERED = "Feed job already registered in intake jobs";
    public static final String ERROR_MESSAGE_FEED_EVENTS_LISTENER_FEED_JOB_REGISTERED = "Feed job already registered in all jobs";
    public static final String ERROR_MESSAGE_RECORD_READER_CHAR_ARRAY_RECORD_TOO_LARGE = "Record is too large!. Maximum record size is %1$s";
    public static final String ERROR_MESSAGE_LIBRARY_JOBJECT_ACCESSOR_CANNOT_PARSE_TYPE = "Cannot parse list item of type %1$s";
    public static final String ERROR_MESSAGE_LIBRARY_JOBJECT_UTIL_ILLEGAL_ARGU_TYPE = "Argument type: %1$s";
    public static final String ERROR_MESSAGE_LIBRARY_EXTERNAL_FUNCTION_UNABLE_TO_LOAD_CLASS = " Unable to load/instantiate class %1$s";
    public static final String ERROR_MESSAGE_LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND = " UDF of kind %1$s not supported.";
    public static final String ERROR_MESSAGE_LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND = "Unknown function kind %1$s";
    public static final String ERROR_MESSAGE_LIBRARY_EXTERNAL_LIBRARY_CLASS_REGISTERED = "Library class loader already registered!";
    public static final String ERROR_MESSAGE_LIBRARY_JAVA_FUNCTION_HELPER_CANNOT_HANDLE_ARGU_TYPE = "Cannot handle a function argument of type %1$s";
    public static final String ERROR_MESSAGE_LIBRARY_JAVA_FUNCTION_HELPER_OBJ_TYPE_NOT_SUPPORTED = "Object of type %1$s not supported.";
    public static final String ERROR_MESSAGE_LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_NAME = "External %1$s not supported";
    public static final String ERROR_MESSAGE_OPERATOR_FEED_META_OPERATOR_DESCRIPTOR_INVALID_RUNTIME = "Invalid feed runtime: %1$s";
    public static final String ERROR_MESSAGE_PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_NOT_VALID_DELIMITER = "'%1$s' is not a valid delimiter. The length of a delimiter should be 1.";
    public static final String ERROR_MESSAGE_PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_NOT_VALID_QUOTE = "'%1$s' is not a valid quote. The length of a quote should be 1.";
    public static final String ERROR_MESSAGE_PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_QUOTE_DELIMITER_MISMATCH = "Quote '%1$s' cannot be used with the delimiter '%2$s'. ";
    public static final String ERROR_MESSAGE_PARSER_FACTORY_HIVE_DATA_PARSER_FACTORY_NO_SERDE = "no hive serde provided for hive deserialized records";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_FIELD_NOT_NULL = "Field %1$s can not be null";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_TYPE_MISMATCH = "Mismatch Type, expecting a value of type %1$s";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_KIND = "Unexpected ADM token kind: %1$s.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_ILLEGAL_ESCAPE = "Illegal escape '\\%1$s'";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_RECORD_END_UNEXPECTED = "Found END_RECORD while expecting a record field.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_EXTRA_FIELD_IN_CLOSED_RECORD = "This record is closed, you can not add extra fields! new field name: %1$s";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_WHEN_EXPECT_COMMA = "Unexpected ADM token kind: %1$s while expecting \":\".";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_FOUND_COMMA_WHEN = "Found COMMA %1$s %2$s record field.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_UNSUPPORTED_INTERVAL_TYPE = "Unsupported interval type: %1$s.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_INTERVAL_NOT_CLOSED = "Interval was not closed.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_INTERVAL_BEGIN_END_POINT_MISMATCH = "The interval start and end point types do not match: %1$s != %2$s";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_INTERVAL_MISSING_COMMA = "Missing COMMA before interval end point.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_INTERVAL_INVALID_DATETIME = "This can not be an instance of interval: missing T for a datetime value.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_INTERVAL_UNSUPPORTED_TYPE = "Unsupported interval type: %1$s.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_INTERVAL_INTERVAL_ARGUMENT_ERROR = "Interval argument not properly constructed.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_LIST_FOUND_END_COLLECTION = "Found END_COLLECTION while expecting a list item.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_BEFORE_LIST = "Found COMMA before any list item.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_EXPECTING_ITEM = "Found COMMA while expecting a list item.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_LIST_FOUND_END_RECOD = "Found END_RECORD while expecting a list item.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_CAST_ERROR = "Can't cast the %1$s type to the %2$s type.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_CONSTRUCTOR_MISSING_DESERIALIZER = "Missing deserializer method for constructor: %1$s.";
    public static final String ERROR_MESSAGE_PARSER_ADM_DATA_PARSER_WRONG_INSTANCE = "This can not be an instance of %1$s";
    public static final String ERROR_MESSAGE_PARSER_TWEET_PARSER_CLOSED_FIELD_NULL = "Closed field %1$s has null value.";
    public static final String ERROR_MESSAGE_UTIL_FILE_SYSTEM_WATCHER_NO_FILES_FOUND = "%1$s: no files found";
    public static final String ERROR_MESSAGE_UTIL_LOCAL_FILE_SYSTEM_UTILS_PATH_NOT_FOUND = "%1$s: path not found";
    public static final String ERROR_MESSAGE_UTIL_HDFS_UTILS_CANNOT_OBTAIN_HDFS_SCHEDULER = "Cannot obtain hdfs scheduler";

    private static Map<Integer, String> errorMessageMap = new HashMap<>();

    static {
        // compilation errors
        errorMessageMap.put(ERROR_TYPE_MISMATCH, ERROR_MESSAGE_TYPE_MISMATCH);
        errorMessageMap.put(ERROR_TYPE_INCOMPATIBLE, ERROR_MESSAGE_TYPE_INCOMPATIBLE);
        errorMessageMap.put(ERROR_TYPE_ITEM, ERROR_MESSAGE_TYPE_ITEM);
        errorMessageMap.put(ERROR_TYPE_UNSUPPORTED, ERROR_MESSAGE_TYPE_UNSUPPORTED);
        errorMessageMap.put(ERROR_INVALID_FORMAT, ERROR_MESSAGE_INVALID_FORMAT);
        errorMessageMap.put(ERROR_OVERFLOW, ERROR_MESSAGE_OVERFLOW);
        errorMessageMap.put(ERROR_UNDERFLOW, ERROR_MESSAGE_UNDERFLOW);
        errorMessageMap.put(ERROR_INJECTED_FAILURE, ERROR_MESSAGE_INJECTED_FAILURE);
        errorMessageMap.put(ERROR_NEGATIVE_VALUE, ERROR_MESSAGE_NEGATIVE_VALUE);
        errorMessageMap.put(ERROR_OUT_OF_BOUND, ERROR_MESSAGE_OUT_OF_BOUND);
        errorMessageMap.put(ERROR_COERCION, ERROR_MESSAGE_COERCION);
        errorMessageMap.put(ERROR_DUPLICATE_FIELD, ERROR_MESSAGE_DUPLICATE_FIELD);

        // lifecycle management errors
        errorMessageMap.put(ERROR_EXTENSION_ID_CONFLICT, ERROR_MESSAGE_ID_CONFLICT);
        errorMessageMap.put(ERROR_EXTENSION_COMPONENT_CONFLICT, ERROR_MESSAGE_COMPONENT_CONFLICT);
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
