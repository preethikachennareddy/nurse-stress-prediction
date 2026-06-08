package com.nurse.stress.prediction.processing;
/*
* maintains constant values referenced in code
* */
public interface Constants {
    String BROKER_URL="kafka:9092";
    String TOPIC_NAME="stress-topic";
    String GROUP_ID = "flink-consumer";
    String INFLUX_URL= "http://influxdb:8086";
    String JOB_SINK_INFLUX_DB= "flink_sink";
    String STRESS_PREDICTION_ML_ENDPOINT="http://stress-prediction-service:5000/model/api/predict?x=%f&y=%f&z=%f&eda=%f&hr=%f&temp=%f";
    String STRESS_LEVEL_RESPONSE_FIELD="stress_level_prediction";
}
