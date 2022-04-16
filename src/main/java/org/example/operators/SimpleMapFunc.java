package org.example.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SimpleMapFunc implements MapFunction<String, Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> map(String value) {
        Long sourceTime = System.currentTimeMillis();
        return new Tuple2<>(value, sourceTime);
    }
}
