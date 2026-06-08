package com.nurse.stress.prediction.processing;

import com.nurse.stress.prediction.SensorRecord;
import com.nurse.stress.prediction.model.IOTPing;
import com.nurse.stress.prediction.sink.InfluxSinkPing;
import com.nurse.stress.prediction.source.KafkaSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The Flink Job flow
 */
@Slf4j
public class StressPredictorJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set parallelism 2
        env.setParallelism(2);
        // 5s checkpoint, save progress, prevent reprocessing if within checkpoint
        env.enableCheckpointing(5000);

        // read from kafka source
        KafkaSource<SensorRecord> source = KafkaSourceFactory.create(Constants.BROKER_URL, Constants.TOPIC_NAME);
        // watermark strategy in case we get out of order events
        DataStream<SensorRecord> records = env.fromSource(source,WatermarkStrategyFactory.create(),"Kafka Source");

        //group by id and process with tumbing window size 5s
        //data within window is averaged and sent to flask api to get predicted stress value

        DataStream<IOTPing> predictedStress = AsyncDataStream.orderedWait(
                records
                        .keyBy(r -> r.getId())
                        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                        .aggregate(new AverageAggregator(), new WindowResultFunction())
                        .name("Stress Prediction 5s batch"),
                new StressPredictionAsyncFunction(),
                500,
                TimeUnit.MILLISECONDS,
                20
        );

        //persist aggregated data and predicted stress to Influx DB
        predictedStress.addSink(new InfluxSinkPing());
        env.execute("Flink Streaming Metrics Example");
    }
}

