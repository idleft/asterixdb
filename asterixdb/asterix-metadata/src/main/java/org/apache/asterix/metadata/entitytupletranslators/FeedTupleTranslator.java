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

package org.apache.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Feed metadata entity to an ITupleReference and vice versa.
 */
public class FeedTupleTranslator extends AbstractTupleTranslator<Feed> {
    // Field indexes of serialized Feed in a tuple.
    // Key field.
    public static final int FEED_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int FEED_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized feed.
    public static final int FEED_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.FEED_RECORDTYPE);

    public FeedTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FEED_DATASET.getFieldCount());
    }

    @Override
    public Feed getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord feedRecord = recordSerDes.deserialize(in);
        return createFeedFromARecord(feedRecord);
    }

    private Feed createFeedFromARecord(ARecord feedRecord){
        Feed feed = null;
        String dn, fn;
        String dataverseName = ((AString) feedRecord
                .getValueByPos(MetadataRecordTypes.FEED_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        String feedName = ((AString) feedRecord.getValueByPos(MetadataRecordTypes.FEED_ARECORD_FEED_NAME_FIELD_INDEX))
                .getStringValue();

        ARecord feedConfig = (ARecord) feedRecord.getValueByPos(MetadataRecordTypes.FEED_ADAPTER_CONFIGURATION_FIELD_INDEX);
        String adapterName = ((AString) feedConfig
                .getValueByPos(MetadataRecordTypes.FEED_ADAPTER_NAME_FIELD_INDEX))
                .getStringValue();

        IACursor cursor = ((AUnorderedList) feedConfig.getValueByPos(
                MetadataRecordTypes.FEED_ADAPTER_CONFIGURATION_FIELD_INDEX))
                .getCursor();

        // restore configurations
        String key;
        String value;
        Map<String, String> adaptorConfiguration = new HashMap<String, String>();
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX))
                    .getStringValue();
            value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX))
                    .getStringValue();
            adaptorConfiguration.put(key, value);
        }
        feed = new Feed(dataverseName, feedName, feedName, adapterName,
                adaptorConfiguration);

        // restore conns with function
        Object o = feedRecord.getValueByPos(MetadataRecordTypes.FEED_ARECORD_CONNS_FIELD_INDEX);
        if (!(o instanceof ANull) && !(o instanceof AMissing)) {
            Map<String, ArrayList<FunctionSignature>> feedConns = new HashMap<>();
            cursor = ((AOrderedList) feedRecord.getValueByPos(
                    MetadataRecordTypes.FEED_ARECORD_CONNS_FIELD_INDEX)).getCursor();
            while(cursor.next()){
                dn = ((AString)((ARecord)cursor.get()).getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX))
                        .getStringValue();
                fn = ((AString)((ARecord)cursor.get()).getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX))
                        .getStringValue();
                if(!feedConns.containsKey(dn))
                    feedConns.put(dn, new ArrayList<>());
                feedConns.get(dn).add(new FunctionSignature(dataverseName, fn,1));
            }
            feed.setFeedConns(feedConns);
        }

        return feed;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Feed feed) throws IOException, MetadataException {
        // write the key in the first two fields of the tuple
        tupleBuilder.reset();
        aString.setValue(feed.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(feed.getFeedName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.FEED_RECORDTYPE);

        // write dataverse name 0
        fieldValue.reset();
        aString.setValue(feed.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write feed name 1
        fieldValue.reset();
        aString.setValue(feed.getFeedName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_FEED_NAME_FIELD_INDEX, fieldValue);

        // write feedConns, 2
        fieldValue.reset();
        writeFeedConnsField(recordBuilder, feed, fieldValue);

        // write adaptor configuration, 3
        fieldValue.reset();
        writeFeedAdaptorField(recordBuilder, feed, fieldValue);

        // write timestampe, 4
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeFeedConnsField(IARecordBuilder rb, Feed feed, ArrayBackedValueStorage vb) throws HyracksDataException {
        ArrayBackedValueStorage listEleBuffer = new ArrayBackedValueStorage();
        OrderedListBuilder orderedListBuilder = new OrderedListBuilder();

        if(feed.getFeedConns()!=null){
            Map<String, ArrayList<FunctionSignature>> feedConns = feed.getFeedConns();
            for(String dn: feedConns.keySet()){
                for(FunctionSignature fs: feedConns.get(dn)){
                    listEleBuffer.reset();
                    writePropertyTypeRecord(dn, fs.getName(), listEleBuffer.getDataOutput());
                    orderedListBuilder.addItem(listEleBuffer);
                }
            }
            orderedListBuilder.write(vb.getDataOutput(),true);
            rb.addField(MetadataRecordTypes.FEED_ARECORD_CONNS_FIELD_INDEX, vb);
        }
    }

    private void writeFeedAdaptorField(IARecordBuilder recordBuilder, Feed feed, ArrayBackedValueStorage fieldValueBuffer)
            throws HyracksDataException {
        IARecordBuilder fieldRecordBuilder = new RecordBuilder();
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        ArrayBackedValueStorage nestedFieldBuffer = new ArrayBackedValueStorage();
        ArrayBackedValueStorage listEleBuffer = new ArrayBackedValueStorage();

        fieldRecordBuilder.reset(MetadataRecordTypes.FEED_ADAPTER_CONFIGURATION_RECORDTYPE);

        aString.setValue(feed.getAdapterName());
        stringSerde.serialize(aString, nestedFieldBuffer.getDataOutput());
        fieldRecordBuilder.addField(MetadataRecordTypes.FEED_ADAPTER_NAME_FIELD_INDEX, nestedFieldBuffer);

        listBuilder.reset((AUnorderedListType)MetadataRecordTypes.FEED_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FEED_ARECORD_ADAPTOR_CONFIG_FIELD_INDEX]);
        for (Map.Entry<String, String> property: feed.getAdapterConfiguration().entrySet()){
            String name = property.getKey();
            String value = property.getValue();
            listEleBuffer.reset();
            writePropertyTypeRecord(name, value, listEleBuffer.getDataOutput());
            listBuilder.addItem(listEleBuffer);
        }
        nestedFieldBuffer.reset();
        listBuilder.write(nestedFieldBuffer.getDataOutput(), true);
        fieldRecordBuilder.addField(MetadataRecordTypes.FEED_ADAPTER_CONFIGURATION_FIELD_INDEX, nestedFieldBuffer);

        fieldRecordBuilder.write(fieldValueBuffer.getDataOutput(),true);
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_ADAPTOR_CONFIG_FIELD_INDEX, fieldValueBuffer);
    }

    @SuppressWarnings("unchecked")
    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(0, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(1, fieldValue);

        propertyRecordBuilder.write(out, true);
    }
}
