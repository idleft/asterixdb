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

import java.io.InputStream;
import java.util.Map;

import org.apache.hyracks.api.util.ErrorMessageUtil;

// Error code:
// 0 --- 999:  runtime errors
// 1000 ---- 1999: compilation errors
// 2000 ---- 2999: storage errors
// 3000 ---- 3999: feed errors
// 4000 ---- 4999: lifecycle management errors
public class ErrorCode {
    private static final String RESOURCE_PATH = "asx_errormsg/en.properties";
    public static final String ASTERIX = "ASX";

    // Extension errors
    public static final int EXTENSION_ID_CONFLICT = 4001;
    public static final int EXTENSION_COMPONENT_CONFLICT = 4002;
    public static final int UNSUPPORTED_MESSAGE_TYPE = 4003;
    public static final int INVALID_CONFIGURATION = 4004;
    public static final int UNSUPPORTED_REPLICATION_STRATEGY = 4005;

    // Runtime errors
    public static final int CASTING_FIELD = 1;
    public static final int TYPE_MISMATCH = 2;
    public static final int TYPE_INCOMPATIBLE = 3;
    public static final int TYPE_UNSUPPORTED = 4;
    public static final int TYPE_ITEM = 5;
    public static final int INVALID_FORMAT = 6;
    public static final int OVERFLOW = 7;
    public static final int UNDERFLOW = 8;
    public static final int INJECTED_FAILURE = 9;
    public static final int NEGATIVE_VALUE = 10;
    public static final int OUT_OF_BOUND = 11;
    public static final int COERCION = 12;
    public static final int DUPLICATE_FIELD_NAME = 13;
    public static final int PROPERTY_NOT_SET = 14;
    public static final int INSTANTIATION_ERROR = 100;

    // Compilation errors
    public static final int PARSE_ERROR = 1001;
    public static final int COMPILATION_TYPE_MISMATCH = 1002;
    public static final int COMPILATION_TYPE_INCOMPATIBLE = 1003;
    public static final int COMPILATION_TYPE_UNSUPPORTED = 1004;
    public static final int COMPILATION_TYPE_ITEM = 1005;
    public static final int COMPILATION_DUPLICATE_FIELD_NAME = 1006;
    public static final int COMPILATION_INVALID_EXPRESSION = 1007;
    public static final int COMPILATION_INVALID_PARAMETER_NUMBER = 1008;
    public static final int COMPILATION_INVALID_RETURNING_EXPRESSION = 1009;
    public static final int COMPILATION_FULLTEXT_PHRASE_FOUND = 1010;
    public static final int COMPILATION_UNKNOWN_DATASET_TYPE = 1011;
    public static final int COMPILATION_UNKNOWN_INDEX_TYPE = 1012;
    public static final int COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD = 1013;
    public static final int COMPILATION_FIELD_NOT_FOUND = 1014;
    public static final int COMPILATION_ILLEGAL_INDEX_FOR_DATASET_WITH_COMPOSITE_PRIMARY_INDEX = 1015;
    public static final int COMPILATION_INDEX_TYPE_NOT_SUPPORTED_FOR_DATASET_TYPE = 1016;
    public static final int COMPILATION_FILTER_CANNOT_BE_NULLABLE = 1017;
    public static final int COMPILATION_ILLEGAL_FILTER_TYPE = 1018;
    public static final int COMPILATION_CANNOT_AUTOGENERATE_COMPOSITE_PRIMARY_KEY = 1019;
    public static final int COMPILATION_ILLEGAL_AUTOGENERATED_TYPE = 1020;
    public static final int COMPILATION_PRIMARY_KEY_CANNOT_BE_NULLABLE = 1021;
    public static final int COMPILATION_ILLEGAL_PRIMARY_KEY_TYPE = 1022;
    public static final int COMPILATION_CANT_DROP_ACTIVE_DATASET = 1023;
    public static final int COMPILATION_AQLPLUS_IDENTIFIER_NOT_FOUND = 1024;
    public static final int COMPILATION_AQLPLUS_NO_SUCH_JOIN_TYPE = 1025;
    public static final int COMPILATION_FUNC_EXPRESSION_CANNOT_UTILIZE_INDEX = 1026;
    public static final int COMPILATION_DATASET_TYPE_DOES_NOT_HAVE_PRIMARY_INDEX = 1027;
    public static final int COMPILATION_UNSUPPORTED_QUERY_PARAMETER = 1028;
    public static final int NO_METADATA_FOR_DATASET = 1029;
    public static final int SUBTREE_HAS_NO_DATA_SOURCE = 1030;
    public static final int SUBTREE_HAS_NO_ADDTIONAL_DATA_SOURCE = 1031;
    public static final int NO_INDEX_FIELD_NAME_FOR_GIVEN_FUNC_EXPR = 1032;
    public static final int NO_SUPPORTED_TYPE = 1033;
    public static final int NO_TOKENIZER_FOR_TYPE = 1034;
    public static final int INCOMPATIBLE_SEARCH_MODIFIER = 1035;
    public static final int UNKNOWN_SEARCH_MODIFIER = 1036;

    // Feed errors
    public static final int DATAFLOW_ILLEGAL_STATE = 3001;
    public static final int UTIL_DATAFLOW_UTILS_TUPLE_TOO_LARGE = 3002;
    public static final int UTIL_DATAFLOW_UTILS_UNKNOWN_FORWARD_POLICY = 3003;
    public static final int OPERATORS_FEED_INTAKE_OPERATOR_DESCRIPTOR_CLASSLOADER_NOT_CONFIGURED = 3004;
    public static final int PARSER_DELIMITED_NONOPTIONAL_NULL = 3005;
    public static final int PARSER_DELIMITED_ILLEGAL_FIELD = 3006;
    public static final int ADAPTER_TWITTER_TWITTER4J_LIB_NOT_FOUND = 3007;
    public static final int OPERATORS_FEED_INTAKE_OPERATOR_NODE_PUSHABLE_FAIL_AT_INGESTION = 3008;
    public static final int FEED_CREATE_FEED_DATATYPE_ERROR = 3009;
    public static final int PARSER_HIVE_NON_PRIMITIVE_LIST_NOT_SUPPORT = 3010;
    public static final int PARSER_HIVE_FIELD_TYPE = 3011;
    public static final int PARSER_HIVE_GET_COLUMNS = 3012;
    public static final int PARSER_HIVE_NO_CLOSED_COLUMNS = 3013;
    public static final int PARSER_HIVE_NOT_SUPPORT_NON_OP_UNION = 3014;
    public static final int PARSER_HIVE_MISSING_FIELD_TYPE_INFO = 3015;
    public static final int PARSER_HIVE_NULL_FIELD = 3016;
    public static final int PARSER_HIVE_NULL_VALUE_IN_LIST = 3017;
    public static final int INPUT_RECORD_RECORD_WITH_METADATA_AND_PK_NULL_IN_NON_OPTIONAL = 3018;
    public static final int INPUT_RECORD_RECORD_WITH_METADATA_AND_PK_CANNT_GET_PKEY = 3019;
    public static final int FEED_MANAGEMENT_FEED_EVENT_LISTENER_FEED_JOINT_REGISTERED = 3020;
    public static final int FEED_MANAGEMENT_FEED_EVENT_REGISTER_INTAKE_JOB_FAIL = 3021;
    public static final int PROVIDER_DATAFLOW_CONTROLLER_UNKNOWN_DATA_SOURCE = 3022;
    public static final int PROVIDER_DATASOURCE_FACTORY_UNKNOWN_INPUT_STREAM_FACTORY = 3023;
    public static final int UTIL_EXTERNAL_DATA_UTILS_FAIL_CREATE_STREAM_FACTORY = 3024;
    public static final int UNKNOWN_RECORD_READER_FACTORY = 3025;
    public static final int PROVIDER_STREAM_RECORD_READER_UNKNOWN_FORMAT = 3026;
    public static final int UNKNOWN_RECORD_FORMAT_FOR_META_PARSER = 3027;
    public static final int LIBRARY_JAVA_JOBJECTS_FIELD_ALREADY_DEFINED = 3028;
    public static final int LIBRARY_JAVA_JOBJECTS_UNKNOWN_FIELD = 3029;
    public static final int NODE_RESOLVER_NO_NODE_CONTROLLERS = 3031;
    public static final int NODE_RESOLVER_UNABLE_RESOLVE_HOST = 3032;
    public static final int INPUT_RECORD_CONVERTER_DCP_MSG_TO_RECORD_CONVERTER_UNKNOWN_DCP_REQUEST = 3033;
    public static final int FEED_DATAFLOW_FRAME_DISTR_REGISTER_FAILED_DATA_PROVIDER = 3034;
    public static final int FEED_MANAGEMENT_FEED_EVENTS_LISTENER_ALREADY_HAVE_INTAKE_JOB = 3035;
    public static final int FEED_MANAGEMENT_FEED_EVENTS_LISTENER_INTAKE_JOB_REGISTERED = 3036;
    public static final int FEED_MANAGEMENT_FEED_EVENTS_LISTENER_FEED_JOB_REGISTERED = 3037;
    public static final int INPUT_RECORD_READER_CHAR_ARRAY_RECORD_TOO_LARGE = 3038;
    public static final int LIBRARY_JOBJECT_ACCESSOR_CANNOT_PARSE_TYPE = 3039;
    public static final int LIBRARY_JOBJECT_UTIL_ILLEGAL_ARGU_TYPE = 3040;
    public static final int LIBRARY_EXTERNAL_FUNCTION_UNABLE_TO_LOAD_CLASS = 3041;
    public static final int LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND = 3042;
    public static final int LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND = 3043;
    public static final int LIBRARY_EXTERNAL_LIBRARY_CLASS_REGISTERED = 3044;
    public static final int LIBRARY_JAVA_FUNCTION_HELPER_CANNOT_HANDLE_ARGU_TYPE = 3045;
    public static final int LIBRARY_JAVA_FUNCTION_HELPER_OBJ_TYPE_NOT_SUPPORTED = 3046;
    public static final int LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_NAME = 3047;
    public static final int OPERATORS_FEED_META_OPERATOR_DESCRIPTOR_INVALID_RUNTIME = 3048;
    public static final int PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_NOT_VALID_DELIMITER = 3049;
    public static final int PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_NOT_VALID_QUOTE = 3050;
    public static final int PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_QUOTE_DELIMITER_MISMATCH = 3051;
    public static final int INDEXING_EXTERNAL_FILE_INDEX_ACCESSOR_UNABLE_TO_FIND_FILE_INDEX = 3052;
    public static final int PARSER_ADM_DATA_PARSER_FIELD_NOT_NULL = 3053;
    public static final int PARSER_ADM_DATA_PARSER_TYPE_MISMATCH = 3054;
    public static final int PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_KIND = 3055;
    public static final int PARSER_ADM_DATA_PARSER_ILLEGAL_ESCAPE = 3056;
    public static final int PARSER_ADM_DATA_PARSER_RECORD_END_UNEXPECTED = 3057;
    public static final int PARSER_ADM_DATA_PARSER_EXTRA_FIELD_IN_CLOSED_RECORD = 3058;
    public static final int PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_WHEN_EXPECT_COMMA = 3059;
    public static final int PARSER_ADM_DATA_PARSER_FOUND_COMMA_WHEN = 3060;
    public static final int PARSER_ADM_DATA_PARSER_UNSUPPORTED_INTERVAL_TYPE = 3061;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_NOT_CLOSED = 3062;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_BEGIN_END_POINT_MISMATCH = 3063;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_MISSING_COMMA = 3064;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_INVALID_DATETIME = 3065;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_UNSUPPORTED_TYPE = 3066;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_INTERVAL_ARGUMENT_ERROR = 3067;
    public static final int PARSER_ADM_DATA_PARSER_LIST_FOUND_END_COLLECTION = 3068;
    public static final int PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_BEFORE_LIST = 3069;
    public static final int PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_EXPECTING_ITEM = 3070;
    public static final int PARSER_ADM_DATA_PARSER_LIST_FOUND_END_RECOD = 3071;
    public static final int PARSER_ADM_DATA_PARSER_CAST_ERROR = 3072;
    public static final int PARSER_ADM_DATA_PARSER_CONSTRUCTOR_MISSING_DESERIALIZER = 3073;
    public static final int PARSER_ADM_DATA_PARSER_WRONG_INSTANCE = 3074;
    public static final int PARSER_TWEET_PARSER_CLOSED_FIELD_NULL = 3075;
    public static final int UTIL_FILE_SYSTEM_WATCHER_NO_FILES_FOUND = 3076;
    public static final int UTIL_LOCAL_FILE_SYSTEM_UTILS_PATH_NOT_FOUND = 3077;
    public static final int UTIL_HDFS_UTILS_CANNOT_OBTAIN_HDFS_SCHEDULER = 3078;
    public static final int ACTIVE_MANAGER_SHUTDOWN = 3079;
    public static final int FEED_METADATA_UTIL_UNEXPECTED_FEED_DATATYPE = 3080;

    private ErrorCode() {
    }

    private static class Holder {
        private static final Map<Integer, String> errorMessageMap;

        static {
            // Loads the map that maps error codes to error message templates.
            try (InputStream resourceStream = ErrorCode.class.getClassLoader().getResourceAsStream(RESOURCE_PATH)) {
                errorMessageMap = ErrorMessageUtil.loadErrorMap(resourceStream);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private Holder() {
        }
    }

    public static String getErrorMessage(int errorCode) {
        String msg = Holder.errorMessageMap.get(errorCode);
        if (msg == null) {
            throw new IllegalStateException("Undefined error code: " + errorCode);
        }
        return msg;
    }
}
