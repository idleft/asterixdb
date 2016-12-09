package org.apache.asterix.external.input.record.reader.kafka;

import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.util.Map;

public class KafkaRecordReaderFactory implements IRecordReaderFactory<String> {

    private static final long serialVersionUID = 1L;

    private transient AlgebricksAbsolutePartitionConstraint algebricksPartitionConstraint;
    private final int numberOfConsumer = 1;
    private Map<String, String> configuration;

    private final String KAFKA_CONFIG_BOOTSTRAP_SERVER = "bootstrap.servers";
    private final String KAFKA_CONFIG_GROUP_ID = "group.id";
    private final String KAFKA_CONFIG_INTERVAL = "auto.commit.interval.ms";
    private final String KAFKA_CONFIG_TOPIC = "topic";

    @Override
    public IRecordReader<? extends String> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        return new KafkaRecordReader(configuration.get(KAFKA_CONFIG_BOOTSTRAP_SERVER),
                configuration.get(KAFKA_CONFIG_GROUP_ID), configuration.get(KAFKA_CONFIG_INTERVAL),
                configuration.get(KAFKA_CONFIG_TOPIC));
    }

    @Override
    public Class<? extends String> getRecordClass() {
        return String.class;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint()
            throws AlgebricksException, HyracksDataException {
        algebricksPartitionConstraint = IExternalDataSourceFactory
                .getPartitionConstraints(algebricksPartitionConstraint, numberOfConsumer);
        return algebricksPartitionConstraint;
    }

    @Override
    public void configure(Map<String, String> configuration) throws AlgebricksException, HyracksDataException {
        this.configuration = configuration;
    }
}
