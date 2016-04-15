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
package org.apache.asterix.external.util;

public class Datatypes {

    /*
        The following assumes this DDL (but ignoring the field name orders):
        create type TwitterUser if not exists as open{
            screen_name: string,
            language: string,
            friends_count: int32,
            status_count: int32,
            name: string,
            followers_count: int32
        };
        create type Tweet if not exists as open{
            id: string,
            user: TwitterUser,
            latitude:double,
            longitude:double,
            created_at:string,
            message_text:string
        };
    */
    public static class Tweet {
        public static final String ID = "id";
        public static final String USER = "user";
        public static final String GEOLOCATION = "geolocation";
        public static final String CREATED_AT = "created_at";
        public static final String TEXT = "text";

        // consistency consider
        public static final String MESSAGE = "text_message";
        public static final String LATITUDE = "latitude";
        public static final String LONGITUDE = "longititude";

        public static final String COUNTRY = "country";
        // Extended Tweet fields
        public static final String PLACE = "place";
        public static final String SOURCE = "source";
        public static final String TRUNCATED = "truncated";
        public static final String REPLY_TO_STATUS_ID = "reply_to_status_id";
        public static final String REPLY_TO_USER_ID = "reply_to_user_id";
        public static final String REPLY_TO_SCREENNAME = "reply_to_screen_name";
        public static final String FAVORITED = "favorited";
        public static final String RETWEETED = "retweeted";
        public static final String FAVORITE_COUNT = "favorite_count";
        public static final String RETWEET = "retweet";
        // skip get getRetweetedStatus
        public static final String CONTRIBUTORS = "contributors";
        public static final String RETWEET_COUNT = "retweet_count";
        public static final String RETWEETED_BY_ME = "retweeted_by_me";
        public static final String CURRENT_USER_RETWEET_ID = "current_user_retweet_id";
        public static final String LANGUAGE = "language";
        // skip below


        // User fields (for the sub record "user")
        public static final String SCREEN_NAME = "screen_name";
        public static final String USER_PREFERRED_LANGUAGE = "user_preferred_language";
        public static final String FRIENDS_COUNT = "friends_count";
        public static final String STATUS_COUNT = "status_count";
        public static final String NAME = "name";
        public static final String FOLLOWERS_COUNT = "followers_count";

    }

    /*
        The following assumes this DDL (but ignoring the field name orders):
        create type ProcessedTweet if not exists as open {
            id: string,
            user_name:string,
            location:point,
            created_at:string,
            message_text:string,
            country: string,
            topics: [string]
        };
    */
    public static final class ProcessedTweet {
        public static final String USER_NAME = "user_name";
        public static final String LOCATION = "location";
        public static final String TOPICS = "topics";
    }

}
