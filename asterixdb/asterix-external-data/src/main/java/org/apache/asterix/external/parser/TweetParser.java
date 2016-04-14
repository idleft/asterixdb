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
package org.apache.asterix.external.parser;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.library.java.JObjectUtil;
import org.apache.asterix.external.util.Datatypes.Tweet;
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.types.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import twitter4j.Status;
import twitter4j.User;

public class TweetParser implements IRecordDataParser<Status> {

    private IAObject[] mutableTweetFields;
    private IAObject[] mutableUserFields;
    private AMutableRecord mutableRecord;
    private AMutableRecord mutableUser;
    private final Map<String, Integer> userFieldNameMap = new HashMap<>();
    private final Map<String, Integer> tweetFieldNameMap = new HashMap<>();
    private RecordBuilder recordBuilder = new RecordBuilder();

    public TweetParser(ARecordType recordType) {
        initFieldNames(recordType);
        mutableUserFields = new IAObject[] { new AMutableString(null), new AMutableString(null), new AMutableInt32(0),
                new AMutableInt32(0), new AMutableString(null), new AMutableInt32(0) };
        mutableUser = new AMutableRecord((ARecordType) recordType.getFieldTypes()[tweetFieldNameMap.get(Tweet.USER)],
                mutableUserFields);
//        ArrayList<IAObject> mutableArray = new ArrayList();

        mutableTweetFields = new IAObject[] { new AMutableString(null), mutableUser, new AMutableDouble(0),
                new AMutableDouble(0), new AMutableString(null), new AMutableString(null),// below extended attr
                new AMutableString(null), new AMutableString(null), new AMutableInt64(0), new AMutableInt64(0),//last ReToUId
                new AMutableString(null), new AMutableString(null), new AMutableString(null), new AMutableInt32(0),// last GetFavCNT
                new AMutableString(null), new AMutableUnorderedList(new AUnorderedListType(BuiltinType.AINT64,null)), new AMutableInt32(0), new AMutableString(null),//last ReByMe
                new AMutableInt64(0)};
        mutableRecord = new AMutableRecord(recordType,mutableTweetFields);//mutableArray.toArray(mutableArray.toArray(new IAObject[mutableArray.size()]
    }

    // Initialize the hashmap values for the field names and positions
    private void initFieldNames(ARecordType recordType) {
        String tweetFields[] = recordType.getFieldNames();
        for (int i = 0; i < tweetFields.length; i++) {
            tweetFieldNameMap.put(tweetFields[i], i);
            if (tweetFields[i].equals(Tweet.USER)) {
                IAType fieldType = recordType.getFieldTypes()[i];
                if (fieldType.getTypeTag() == ATypeTag.RECORD) {
                    String userFields[] = ((ARecordType) fieldType).getFieldNames();
                    for (int j = 0; j < userFields.length; j++) {
                        userFieldNameMap.put(userFields[j], j);
                    }
                }

            }
        }
    }

    @Override
    public void parse(IRawRecord<? extends Status> record, DataOutput out) throws HyracksDataException {
        Status tweet = record.get();
        User user = tweet.getUser();
        // Tweet user data
        ((AMutableString) mutableUserFields[userFieldNameMap.get(Tweet.SCREEN_NAME)])
                .setValue(JObjectUtil.getNormalizedString(user.getScreenName()));
        ((AMutableString) mutableUserFields[userFieldNameMap.get(Tweet.LANGUAGE)])
                .setValue(JObjectUtil.getNormalizedString(user.getLang()));
        ((AMutableInt32) mutableUserFields[userFieldNameMap.get(Tweet.FRIENDS_COUNT)]).setValue(user.getFriendsCount());
        ((AMutableInt32) mutableUserFields[userFieldNameMap.get(Tweet.STATUS_COUNT)]).setValue(user.getStatusesCount());
        ((AMutableString) mutableUserFields[userFieldNameMap.get(Tweet.NAME)])
                .setValue(JObjectUtil.getNormalizedString(user.getName()));
        ((AMutableInt32) mutableUserFields[userFieldNameMap.get(Tweet.FOLLOWERS_COUNT)])
                .setValue(user.getFollowersCount());

        // Tweet data
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.ID)]).setValue(String.valueOf(tweet.getId()));

        int userPos = tweetFieldNameMap.get(Tweet.USER);
        for (int i = 0; i < mutableUserFields.length; i++) {
            ((AMutableRecord) mutableTweetFields[userPos]).setValueAtPos(i, mutableUserFields[i]);
        }
        if (tweet.getGeoLocation() != null) {
            ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(Tweet.LATITUDE)])
                    .setValue(tweet.getGeoLocation().getLatitude());
            ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(Tweet.LONGITUDE)])
                    .setValue(tweet.getGeoLocation().getLongitude());
        } else {
            ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(Tweet.LATITUDE)]).setValue(0);
            ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(Tweet.LONGITUDE)]).setValue(0);
        }
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.CREATED_AT)])
                .setValue(JObjectUtil.getNormalizedString(tweet.getCreatedAt().toString()));
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.MESSAGE)])
                .setValue(JObjectUtil.getNormalizedString(tweet.getText()));

        // extend fields assignments
//        for (String fieldName : mutableRecord.getType().getFieldNames()){
//            if(fieldName == Tweet.USER){
//
//            }
//
//            if(mutableRecord.getType().getFieldType(fieldName) instanceof AMutableDouble)
//                ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(fieldName)]).setValue();
//
//        }
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.SOURCE)]).setValue(tweet.getSource());
        ((AMutableString)mutableTweetFields[tweetFieldNameMap.get(Tweet.TRUNCATED)]).setValue(String.valueOf(tweet.isTruncated()));
        ((AMutableInt64) mutableTweetFields[tweetFieldNameMap.get(Tweet.REPLY_TO_STATUS_ID)]).setValue(
                tweet.getInReplyToStatusId());
        ((AMutableInt64) mutableTweetFields[tweetFieldNameMap.get(Tweet.REPLY_TO_USER_ID)]).setValue(tweet.getInReplyToUserId());
        if(tweet.getInReplyToScreenName()!=null)
            ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.REPLY_TO_SCREENNAME)]).setValue(tweet.getInReplyToScreenName());
        else
            ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.REPLY_TO_SCREENNAME)]).setValue("");

        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.FAVORITED)]).setValue(String.valueOf(tweet.isFavorited()));
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.RETWEETED)]).setValue(String.valueOf(tweet.isRetweeted()));
        ((AMutableInt32) mutableTweetFields[tweetFieldNameMap.get(Tweet.FAVORITED_COUNT)]).setValue(tweet.getFavoriteCount());
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.RETWEET)]).setValue(String.valueOf(tweet.isRetweet()));
//        ((AMutableUnorderedList) mutableTweetFields[tweetFieldNameMap.get(Tweet.CONTRIBUTORS)]).add(
//                        new AMutableInt64(0));
        if(tweet.getContributors()!=null) {
            for (long cid : tweet.getContributors()) {
                ((AMutableUnorderedList) mutableTweetFields[tweetFieldNameMap.get(Tweet.CONTRIBUTORS)]).add(
                        new AMutableInt64(cid));
            }
        }
        else
            ((AMutableUnorderedList) mutableTweetFields[tweetFieldNameMap.get(Tweet.CONTRIBUTORS)]).add(
                    new AMutableInt64(0));
        ((AMutableInt32) mutableTweetFields[tweetFieldNameMap.get(Tweet.RETWEET_COUNT)]).setValue(
                tweet.getRetweetCount());
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.RETWEETED_BY_ME)]).setValue(
                String.valueOf(tweet.isRetweetedByMe()));
        ((AMutableInt64) mutableTweetFields[tweetFieldNameMap.get(Tweet.CURRENT_USER_RETWEET_ID)]).setValue(
                tweet.getCurrentUserRetweetId());
////        Tweet.LANGUAGE


        //Assign value to record
        for (int i = 0; i < mutableTweetFields.length; i++) {
            mutableRecord.setValueAtPos(i, mutableTweetFields[i]);
        }
        recordBuilder.reset(mutableRecord.getType());
        recordBuilder.init();
        IDataParser.writeRecord(mutableRecord, out, recordBuilder);
    }
}
