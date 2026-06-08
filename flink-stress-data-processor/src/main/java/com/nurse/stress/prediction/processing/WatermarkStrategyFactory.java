package com.nurse.stress.prediction.processing;

import com.nurse.stress.prediction.SensorRecord;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;

/**
 * the watermark strategy to filter data to be processed
 */
public class WatermarkStrategyFactory {
    private static final DateTimeFormatter FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .optionalEnd()
                    .toFormatter();

    // all events within 5s of watermark will still be considered if they arrive out of order
    public static WatermarkStrategy<SensorRecord> create() {
        return WatermarkStrategy
                .<SensorRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorRecord>() {
                    @Override
                    public long extractTimestamp(SensorRecord element, long recordTimestamp) {
                        return parseTimestamp(element.getDatetime());
                    }
                })
                .withIdleness(Duration.ofSeconds(5));
    }

    private static long parseTimestamp(String ts) {
        if (ts == null) return System.currentTimeMillis();

        try {
            LocalDateTime ldt = LocalDateTime.parse(ts, FORMATTER);
            return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException e) {
            // fallback in case of unexpected format
            return System.currentTimeMillis();
        }
    }

}
