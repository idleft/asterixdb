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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.util.Datatypes.Tweet;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.types.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class TweetParser implements IRecordDataParser<Status> {

    private ArrayBackedValueStorage fieldValueBuffer;
    private ArrayBackedValueStorage listItemBuffer;
    private RecordBuilder recBuilder;
    private ARecordType recordType;
    private IAType[] fieldTypes;
    private String[] fieldNames;
    private byte[] fieldTypeTags;
    private int fieldN;
    private UTF8StringWriter utf8Writer = new UTF8StringWriter();
    private UnorderedListBuilder unorderedListBuilder = new UnorderedListBuilder();
    private AMutablePoint aPoint;
    private ArrayList<Long> emptyArray = new ArrayList<>();


    public TweetParser(ARecordType recordType) {
        this.recordType = recordType;
        fieldNames = recordType.getFieldNames();
        fieldN = recordType.getFieldNames().length;
        recBuilder = new RecordBuilder();
        recBuilder.reset(recordType);
        recBuilder.init();

        fieldValueBuffer = new ArrayBackedValueStorage();
        listItemBuffer = new ArrayBackedValueStorage();
        aPoint = new AMutablePoint(-1,-1);

        fieldTypeTags = new byte[fieldN];
        for (int iter1 = 0; iter1 < fieldN; iter1++) {
            ATypeTag tag = recordType.getFieldTypes()[iter1].getTypeTag();
            fieldTypeTags[iter1] = tag.serialize();
        }
    }

    private Object getTweetFieldValue(Status tweet, String fieldName) {
        Object res = null;
        switch (fieldName) {
            case Tweet.ID:
                res = tweet.getId();
                break;
            case Tweet.TEXT:
                res = tweet.getText();
                break;
            case Tweet.CREATED_AT:
                res = tweet.getCreatedAt().getTime();
                break;
            case Tweet.SOURCE:
                res = tweet.getSource();
                break;
            case Tweet.REPLY_TO_STATUS_ID:
                res = tweet.getInReplyToStatusId();
                break;
            case Tweet.REPLY_TO_USER_ID:
                res = tweet.getInReplyToUserId();
                break;
            case Tweet.REPLY_TO_SCREENNAME:
                res = tweet.getInReplyToScreenName();
                break;
//            case Tweet.GEOLOCATION:
//                ((AMutablePoint) mutableTweetFields[iter1]).setValue(tweet.getGeoLocation().getLongitude(),
//                        tweet.getGeoLocation().getLatitude());
//                break;
            case Tweet.FAVORITE_COUNT:
                res = tweet.getFavoriteCount();
                break;
            case Tweet.RETWEET_COUNT:
                res = tweet.getRetweetCount();
                break;
            case Tweet.CURRENT_USER_RETWEET_ID:
                res = tweet.getCurrentUserRetweetId();
                break;
            case Tweet.LANGUAGE:
                res = tweet.getLang();
                break;
            case Tweet.TRUNCATED:
                res = tweet.isTruncated();
                break;
            case Tweet.FAVORITED:
                res = tweet.isFavorited();
                break;
            case Tweet.RETWEETED:
                res = tweet.isRetweeted();
                break;
            case Tweet.RETWEET:
                res = tweet.isRetweet();
                break;
            case Tweet.RETWEETED_BY_ME:
                res = tweet.isRetweetedByMe();
                break;
            case Tweet.SENSITIVE:
                res = tweet.isPossiblySensitive();
                break;
            case Tweet.GEOLOCATION:
                aPoint.setValue(-1,-1);
                GeoLocation location = tweet.getGeoLocation();
                if(location!=null)
                    aPoint.setValue(tweet.getGeoLocation().getLongitude(),tweet.getGeoLocation().getLatitude());
                res = aPoint;
                break;
        }
        return res;

    }

    private void parseUnorderedList(long[] uolist, DataOutput output) throws IOException {
        if(uolist.length>0)
            System.out.println("hello!");
        unorderedListBuilder.reset(new AUnorderedListType(BuiltinType.AINT64,""));
        byte tagByte = BuiltinType.AINT64.getTypeTag().serialize();
        for (int iter1 = 0; iter1<uolist.length; iter1++){
            listItemBuffer.reset();
            final DataOutput listOutput = listItemBuffer.getDataOutput();
            listOutput.writeByte(tagByte);
            parseInt64(uolist[iter1],listOutput);
            unorderedListBuilder.addItem(listItemBuffer);
        }
        unorderedListBuilder.write(output, false);
    }

    private void parseInt64(long value, DataOutput output) throws IOException{
        output.writeLong(value);
    }

    @SuppressWarnings("unchecked")
    private void parsePoint(AMutablePoint point, DataOutput output) throws IOException{
        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(
                aPoint.getType()).serialize(aPoint,output);
    }

    private void writeFieldValue(Status tweet, String fieldName, DataOutput fieldOutput) throws IOException {
        switch (fieldName) {
            case Tweet.USER:
                break;
            case Tweet.PLACE:
                break;
            case Tweet.GEOLOCATION:
                parsePoint((AMutablePoint) getTweetFieldValue(tweet,fieldName),fieldOutput);
                break;
            // int64 attrs
            case Tweet.ID:
            case Tweet.REPLY_TO_STATUS_ID:
            case Tweet.REPLY_TO_USER_ID:
            case Tweet.CURRENT_USER_RETWEET_ID:
            case Tweet.CREATED_AT: // datetime is treated as long
//                fieldOutput.writeLong((long) getTweetFieldValue(tweet, fieldName));
                parseInt64((long) getTweetFieldValue(tweet, fieldName), fieldOutput);
                break;
            // String attrs
            case Tweet.TEXT:
            case Tweet.SOURCE:
            case Tweet.REPLY_TO_SCREENNAME:
            case Tweet.LANGUAGE:
                utf8Writer.writeUTF8((String) getTweetFieldValue(tweet, fieldName),fieldOutput);
//                fieldOutput.write(((String)getTweetFieldValue(tweet, fieldName)).getBytes());
                break;
            // int32 attrs
            case Tweet.FAVORITE_COUNT:
            case Tweet.RETWEET_COUNT:
                fieldOutput.writeInt((Integer) getTweetFieldValue(tweet, fieldName));
                break;
            // boolean attrs
            case Tweet.TRUNCATED:
            case Tweet.FAVORITED:
            case Tweet.RETWEETED:
            case Tweet.RETWEET:
            case Tweet.RETWEETED_BY_ME:
            case Tweet.SENSITIVE:
                fieldOutput.writeBoolean((boolean) getTweetFieldValue(tweet, fieldName));
                break;
            // unordered List
            case Tweet.CONTRIBUTORS:
                parseUnorderedList(tweet.getContributors(), fieldOutput);
                break;
        }
    }

    @Override
    public void parse(IRawRecord<? extends Status> record, DataOutput out) throws HyracksDataException {
        Status tweet = record.get();
        User user = tweet.getUser();
        try {
            // for field in record,
            recBuilder.reset(recordType);
            recBuilder.init();
            for (int iter1 = 0; iter1 < fieldN; iter1++) {
                fieldValueBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                fieldOutput.write(fieldTypeTags[iter1]);
                writeFieldValue(tweet, fieldNames[iter1], fieldOutput);
                recBuilder.addField(iter1, fieldValueBuffer);
            }
            recBuilder.write(out, true);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
