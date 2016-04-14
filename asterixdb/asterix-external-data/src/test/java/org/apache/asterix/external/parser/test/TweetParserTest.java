package org.apache.asterix.external.parser.test;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.parser.TweetParser;
import org.apache.asterix.external.parser.factory.TweetParserFactory;
import org.apache.asterix.external.util.Datatypes.Tweet;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.junit.Test;
import twitter4j.*;

import java.util.Date;

/**
 * Created by Xikui on 4/5/16.
 */
public class TweetParserTest {


    @Test
    public void sampleTest() throws TwitterException {
        // Construct tweet object
        String tweetRawJson = "{\"created_at\":\"Tue Nov 24 00:14:03 +0000 2015\",\"id\":668945640186101761,\"id_str\":\"668945640186101761\",\"text\":\"Just posted a photo @ Campus Martius Park https:\\/\\/t.co\\/5Ax4E2CdWZ\",\"source\":\"\\u003ca href=\\\"http:\\/\\/instagram.com\\\" rel=\\\"nofollow\\\"\\u003eInstagram\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":48121888,\"id_str\":\"48121888\",\"name\":\"Kevin McKague\",\"screen_name\":\"KevinOfMI\",\"location\":\"Davison, Michigan\",\"url\":\"http:\\/\\/KevinMcKague.wordpress.com\",\"description\":\"I need to ride my bike until my brain shuts up and my muscles are screaming.\\u00a0\\nRight after these donuts. Dad of 3.\\n Visit my blog 18 Wheels and a 12-Speed Bike.\",\"protected\":false,\"verified\":false,\"followers_count\":1178,\"friends_count\":1780,\"listed_count\":50,\"favourites_count\":2319,\"statuses_count\":22263,\"created_at\":\"Wed Jun 17 21:24:03 +0000 2009\",\"utc_offset\":-18000,\"time_zone\":\"Eastern Time (US & Canada)\",\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"EBF5ED\",\"profile_background_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_background_images\\/378800000153875492\\/VrUNrXXF.jpeg\",\"profile_background_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_background_images\\/378800000153875492\\/VrUNrXXF.jpeg\",\"profile_background_tile\":true,\"profile_link_color\":\"0084B4\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"DDFFCC\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/646097130977890304\\/izCl0tCD_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/646097130977890304\\/izCl0tCD_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/48121888\\/1441581344\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":{\"type\":\"Point\",\"coordinates\":[42.33170228,-83.04647491]},\"coordinates\":{\"type\":\"Point\",\"coordinates\":[-83.04647491,42.33170228]},\"place\":{\"id\":\"b463d3bd6064861b\",\"url\":\"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/b463d3bd6064861b.json\",\"place_type\":\"city\",\"name\":\"Detroit\",\"full_name\":\"Detroit, MI\",\"country_code\":\"US\",\"country\":\"United States\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[-83.288056,42.255085],[-83.288056,42.450488],[-82.910520,42.450488],[-82.910520,42.255085]]]},\"attributes\":{}},\"contributors\":null,\"is_quote_status\":false,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/5Ax4E2CdWZ\",\"expanded_url\":\"https:\\/\\/instagram.com\\/p\\/-cnH0kFL_g\\/\",\"display_url\":\"instagram.com\\/p\\/-cnH0kFL_g\\/\",\"indices\":[42,65]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1448324043686\"}\n";
        Status tInstance = TwitterObjectFactory.createStatus(tweetRawJson);

        // Constuct Asterix datatype
        String userTypeName = "FullUserType";
        String[] userFieldNames = new String[]{Tweet.SCREEN_NAME,Tweet.LANGUAGE, Tweet.FRIENDS_COUNT, Tweet.STATUS_COUNT,
        Tweet.NAME, Tweet.FOLLOWERS_COUNT};
        IAType[] userFieldTypes = new IAType[]{BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32,
                BuiltinType.AINT32, BuiltinType.ASTRING, BuiltinType.AINT32};
        ARecordType userType = new ARecordType(userTypeName, userFieldNames, userFieldTypes, true);
        String tweetTypeName = "FullTweets";
        String[] tweetFieldNames = new String[]{Tweet.ID,Tweet.USER,Tweet.LATITUDE,
                Tweet.LONGITUDE, Tweet.CREATED_AT, Tweet.MESSAGE};
        IAType[] tweetFieldTypes = new IAType[]{BuiltinType.ASTRING,userType,BuiltinType.ADOUBLE,
        BuiltinType.ADOUBLE,BuiltinType.ASTRING,BuiltinType.ASTRING};

        ARecordType tweetRecordType = new ARecordType(tweetTypeName, tweetFieldNames, tweetFieldTypes, true);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(tweetFieldNames.length);
        //Construct TweetParser
        TweetParser tp = new TweetParser(tweetRecordType);

        // Make record
        GenericRecord<Status> record = new GenericRecord<>();
        record.set(tInstance);
        System.out.println(record.get().getId());

        // Process
        try {
            tp.parse(record,tb.getDataOutput());

        } catch (HyracksDataException e) {
            e.printStackTrace();
        }

    }
}
