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
import java.util.List;
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

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class TweetParser implements IRecordDataParser<Status> {

    private IAObject[] mutableTweetFields;
    private IAObject[] mutableUserFields;
    private AMutableRecord mutableRecord;
    private AMutableRecord mutableUser;
//    private final Map<String, Integer> userFieldNameMap = new HashMap<>();
//    private final Map<String, Integer> tweetFieldNameMap = new HashMap<>();
    private List<String> tweetFieldNames = new ArrayList<>();
    private Map<String, List<String>> recordFieldNameMap = new HashMap<>();
//    private ArrayList<String>
    private RecordBuilder recBuilder;
    private ARecordType recordType;
    private IAType[] fieldTypes;
    private int fieldN;

    public TweetParser(ARecordType recordType) {
//        initFields(recordType);
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        this.recordType = recordType;

        fieldN = recordType.getFieldNames().length;
        recBuilder = new RecordBuilder();
        recBuilder.reset(recordType);
        recBuilder.init();
//        mutableUserFields = new IAObject[] { new AMutableString(null), new AMutableString(null), new AMutableInt32(0),
//                new AMutableInt32(0), new AMutableString(null), new AMutableInt32(0) };
//        mutableUser = new AMutableRecord((ARecordType) recordType.getFieldTypes()[tweetFieldNameMap.get(Tweet.USER)],
//                mutableUserFields);
//        ArrayList<IAObject> mutableArray = new ArrayList();
//        for (IAType fieldType : recordType.getFieldTypes()){
//            System.out.println(fieldType.getDisplayName());
//        }
//
//        mutableTweetFields = new IAObject[] { new AMutableString(null), mutableUser, new AMutableDouble(0),
//                new AMutableDouble(0), new AMutableString(null), new AMutableString(null),// below extended attr
//                new AMutableString(null), new AMutableString(null), new AMutableInt64(0), new AMutableInt64(0),//last ReToUId
//                new AMutableString(null), new AMutableString(null), new AMutableString(null), new AMutableInt32(0),// last GetFavCNT
//                new AMutableString(null), new AMutableUnorderedList(new AUnorderedListType(BuiltinType.AINT64,null)), new AMutableInt32(0), new AMutableString(null),//last ReByMe
//                new AMutableInt64(0)};

        mutableRecord = new AMutableRecord(recordType,mutableTweetFields);//mutableArray.toArray(mutableArray.toArray(new IAObject[mutableArray.size()]
    }

    // Initialize the hashmap values for the field names and positions
    private void initFields(ARecordType recordType) {
        String tweetFields[] = recordType.getFieldNames();
        for (int i = 0; i < tweetFields.length; i++) {
            tweetFieldNames.add(tweetFields[i]);
            IAType fieldType = recordType.getFieldTypes()[i];
            // need also create object in mutable tweet field
            if (fieldType.getTypeTag() == ATypeTag.RECORD) {
                String recordFields[] = ((ARecordType) fieldType).getFieldNames();
                ArrayList<String> recordFieldNames = new ArrayList<>();
                for (int j = 0; j < recordFields.length; j++) {
                    recordFieldNames.add(recordFields[j]);
                    // need also add objects in mutable record field
                }
                recordFieldNameMap.put(tweetFields[i],recordFieldNames);
            }
        }
    }

    private void typeValueAssign(Status tweet){

        for (int iter1 = 0; iter1 < tweetFieldNames.size(); iter1++){
            String fieldName = tweetFieldNames.get(iter1);
            if(fieldName == Tweet.USER){
                // do person
            }
            else if (fieldName == Tweet.PLACE){
                // do places
            }
            else{
                switch (fieldName){
                    case Tweet.CREATED_AT:
                        ((AMutableDateTime) mutableTweetFields[iter1]).setValue(tweet.getCreatedAt().getTime());
                        break;
                    case Tweet.ID:
                        ((AMutableInt64) mutableTweetFields[iter1]).setValue(tweet.getId());
                        break;
                    case Tweet.TEXT:
                        ((AMutableString) mutableTweetFields[iter1]).setValue(tweet.getText());
                        break;
                    case Tweet.SOURCE:
                        ((AMutableString) mutableTweetFields[iter1]).setValue(tweet.getSource());
                        break;
                    case Tweet.REPLY_TO_STATUS_ID:
                        ((AMutableInt64) mutableTweetFields[iter1]).setValue(tweet.getInReplyToStatusId());
                        break;
                    case Tweet.REPLY_TO_USER_ID:
                        ((AMutableInt64) mutableTweetFields[iter1]).setValue(tweet.getInReplyToUserId());
                        break;
                    case Tweet.REPLY_TO_SCREENNAME:
                        ((AMutableString) mutableTweetFields[iter1]).setValue(tweet.getInReplyToScreenName());
                        break;
                    case Tweet.GEOLOCATION:
                        ((AMutablePoint) mutableTweetFields[iter1]).setValue(tweet.getGeoLocation().getLongitude(),
                                tweet.getGeoLocation().getLatitude());
                        break;
                    case Tweet.FAVORITE_COUNT:
                        ((AMutableInt32) mutableTweetFields[iter1]).setValue(tweet.getFavoriteCount());
                        break;
                    case Tweet.RETWEET_COUNT:
                        ((AMutableInt32) mutableTweetFields[iter1]).setValue(tweet.getRetweetCount());
                        break;
                    case Tweet.CURRENT_USER_RETWEET_ID:
                        ((AMutableInt64) mutableTweetFields[iter1]).setValue(tweet.getCurrentUserRetweetId());
                        break;
                    case Tweet.LANGUAGE:
                        ((AMutableString) mutableTweetFields[iter1]).setValue(tweet.getLang());
                        break;
                    // all boolean attr are skipped for now
                }
            }
        }
    }

    @Override
    public void parse(IRawRecord<? extends Status> record, DataOutput out) throws HyracksDataException {
        Status tweet = record.get();
        User user = tweet.getUser();
        String jsonStrTweet = TwitterObjectFactory.getRawJSON(tweet);
        JSONObject jsonTweet = null;
        try {
            jsonTweet = new JSONObject(jsonStrTweet);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        try {
            System.out.println(jsonTweet.get("user"));
        } catch (JSONException e) {
            e.printStackTrace();
        }

        // for field in record,
        // init field buffer assign value
        // write to recbuilder

        //Assign value to record
//        for (int i = 0; i < mutableTweetFields.length; i++) {
//            mutableRecord.setValueAtPos(i, mutableTweetFields[i]);
//        }
//        recordBuilder.reset(mutableRecord.getType());
//        recordBuilder.init();
//        IDataParser.writeRecord(mutableRecord, out, recordBuilder);
    }
}
