package com.nurse.stress.prediction.sink;

import com.nurse.stress.prediction.model.IOTPing;
import com.nurse.stress.prediction.processing.Constants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * enables sinking IOTPing objects containing processed data to influx db
 */
public class InfluxSinkPing extends RichSinkFunction<IOTPing> {
    private String influxUrl;

    @Override
    public void open(Configuration parameters) {
        influxUrl = Constants.INFLUX_URL+"/write?db="+Constants.JOB_SINK_INFLUX_DB;
    }

    @Override
    public void invoke(IOTPing ping, Context ctx) throws Exception {
        long tsNano = ping.datetime * 1_000_000L;
        // formatting measurement to be persisted
        String line = String.format(
                "flink_events,id=%s EDA=%f,HR=%f,TEMP=%f,stressLevel=%d %d",
                ping.id,
                ping.EDA,
                ping.HR,
                ping.TEMP,
                ping.stressLevel,
                tsNano
        );
        URL url = new URL(influxUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);

        try (OutputStream os = con.getOutputStream()) {
            os.write(line.getBytes());
        }

        int code = con.getResponseCode();
        if (code >= 400) {
            System.err.println("InfluxDB write failed: HTTP " + code);
        }

        con.disconnect();
    }
}
