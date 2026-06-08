package com.nurse.stress.prediction.source;

import com.nurse.stress.prediction.SensorRecord;
import com.nurse.stress.prediction.processing.Constants;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.AvroDeserializationSchema;

/*
* enables reading data from kafka topic
* */
public class KafkaSourceFactory {


    public static KafkaSource<SensorRecord> create(String brokers, String topics){
        return KafkaSource.<SensorRecord>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setGroupId(Constants.GROUP_ID)
                .setValueOnlyDeserializer(AvroDeserializationSchema.forSpecific(SensorRecord.class))
                .build();
    }
}
