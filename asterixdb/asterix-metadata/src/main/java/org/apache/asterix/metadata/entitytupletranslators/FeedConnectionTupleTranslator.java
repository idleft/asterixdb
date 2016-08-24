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

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.om.base.*;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Xikui on 7/12/16.
 */
public class FeedConnectionTupleTranslator extends AbstractTupleTranslator<FeedConnection> {

    public static final int FEED_CONN_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int FEED_CONN_FEED_NAME_FIELD_INDEX = 1;
    public static final int FEED_CONN_DATASET_NAME_FIELD_INDEX = 2;

    public static final int FEED_CONN_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.FEED_CONNECTION_RECORDTYPE);

    public FeedConnectionTupleTranslator(boolean getTuple){
        super(getTuple, MetadataPrimaryIndexes.FEED_CONN_DATASET.getFieldCount());
    }
    @Override
    public FeedConnection getMetadataEntityFromTuple(ITupleReference frameTuple) throws MetadataException, IOException {
        byte[] serRecord = frameTuple.getFieldData(FEED_CONN_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FEED_CONN_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FEED_CONN_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord feedConnRecord = recordSerDes.deserialize(in);
        return createFeedConnFromRecord(feedConnRecord);
    }

    private FeedConnection createFeedConnFromRecord(ARecord feedConnRecord){
        String dataverseName = ((AString)feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        String feedId = ((AString)feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_FEED_NAME_FIELD_INDEX)).getStringValue();
        String datasetName = ((AString)feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_DATASET_NAME_FIELD_INDEX)).getStringValue();
        ArrayList<String> appliedFunctions = null;
        Object o = feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX);
        IACursor cursor = null;

        if (!(o instanceof ANull) && !(o instanceof AMissing)) {
            appliedFunctions = new ArrayList<>();
            cursor = ((AOrderedList) feedConnRecord.getValueByPos(
                    MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX)).getCursor();
            while(cursor.next()){
                appliedFunctions.add(((AString) cursor.get()).getStringValue());
            }
        }

        return new FeedConnection(dataverseName, feedId, datasetName, appliedFunctions);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FeedConnection me) throws MetadataException, IOException {
        tupleBuilder.reset();

        // key: dataverse
        aString.setValue(me.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // key: feedName
        aString.setValue(me.getFeedName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // key: dataset
        aString.setValue(me.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.FEED_CONNECTION_RECORDTYPE);
        // field dataverse
        fieldValue.reset();
        aString.setValue(me.getDataverseName());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_DATAVERSE_NAME_FIELD_INDEX,fieldValue);

        // field: feedId
        fieldValue.reset();
        aString.setValue(me.getFeedName());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_DATASET_NAME_FIELD_INDEX, fieldValue);

        // field: dataset
        fieldValue.reset();
        aString.setValue(me.getDatasetName());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_DATASET_NAME_FIELD_INDEX, fieldValue);

        // field: appliedFunctions
        fieldValue.reset();
        writeAppliedFunctionsField(recordBuilder, me, fieldValue);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeAppliedFunctionsField(IARecordBuilder rb, FeedConnection fc, ArrayBackedValueStorage buffer)
            throws HyracksDataException {
        OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage listEleBuffer = new ArrayBackedValueStorage();

        if(fc.getAppliedFunctions()!=null){
            ArrayList<String> appliedFunctions = fc.getAppliedFunctions();
            for (String af:appliedFunctions){
                aString.setValue(af);
                stringSerde.serialize(aString,listEleBuffer.getDataOutput());
                orderedListBuilder.addItem(listEleBuffer);
            }
            orderedListBuilder.write(buffer.getDataOutput(),true);
            rb.addField(MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX, buffer);
        }
    }
}
