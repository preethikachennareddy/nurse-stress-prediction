package com.nurse.stress.prediction.processing;

import com.nurse.stress.prediction.model.NurseMetrics;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * computes average over window data based on aggregate values and count of records over window
 */
public class WindowResultFunction extends ProcessWindowFunction<Tuple7<Double, Double, Double, Double, Double, Double, Long>,
        NurseMetrics,
        String,
        TimeWindow> {
    @Override
    public void process(String nurseId,
                        ProcessWindowFunction<Tuple7<Double, Double, Double, Double, Double, Double, Long>, NurseMetrics, String, TimeWindow>.Context context,
                        Iterable<Tuple7<Double, Double, Double, Double, Double, Double, Long>> input,
                        Collector<NurseMetrics> out) throws Exception {
        Tuple7<Double, Double, Double, Double, Double, Double, Long> acc = input.iterator().next();

        long count = acc.f6;
        long lastEventTimeStamp = context.window().maxTimestamp();
        // divide by count to get average
        NurseMetrics nurseMetrics = new NurseMetrics(
                nurseId,
                (float)(acc.f0/count),
                (float)(acc.f1/count),
                (float)(acc.f2/count),
                (float)(acc.f3/count),
                (float)(acc.f4/count),
                (float)(acc.f5/count),
                lastEventTimeStamp,
                context.window().getStart(),
                context.window().getEnd()
                );
        out.collect(nurseMetrics);
    }
}
