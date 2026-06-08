package com.nurse.stress.prediction.processing;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nurse.stress.prediction.SensorRecord;
import com.nurse.stress.prediction.model.IOTPing;
import com.nurse.stress.prediction.model.NurseMetrics;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
/*
* Flow to get predicted value for aggregated data from nurse stress prediction service
* */
public class StressPredictionAsyncFunction implements AsyncFunction<NurseMetrics, IOTPing> {

    private transient HttpClient client;
    private transient ObjectMapper objectMapper;

    @Override
    public void asyncInvoke(NurseMetrics metrics, ResultFuture<IOTPing> resultFuture) throws Exception {
        if (client == null) {
            client = HttpClient.newHttpClient();
        }

        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        // passing feature values as query params
        String uri = String.format(
                Constants.STRESS_PREDICTION_ML_ENDPOINT,
                metrics.getX(), metrics.getY(), metrics.getZ(),
                metrics.getEDA(), metrics.getHR(), metrics.getTEMP()
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .GET()
                .build();

        CompletableFuture<HttpResponse<String>> responseFuture = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());

        responseFuture.thenAccept(response -> {
            try {
                String body = response.body();
                // extract stress_level_prediction from JSON response
                JsonNode node = objectMapper.readTree(body);
                int predictedStress = node.get(Constants.STRESS_LEVEL_RESPONSE_FIELD).asInt();
                // pojo with data to persist in influx db
                IOTPing ping = new IOTPing(
                        metrics.getId(),
                        metrics.getEDA(),
                        metrics.getHR(),
                        metrics.getTEMP(),
                        metrics.getDatetime(),
                        predictedStress
                );

                resultFuture.complete(Collections.singleton(ping));
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
            }
        }).exceptionally(ex -> {
            resultFuture.completeExceptionally(ex);
            return null;
        });
    }
}