package org.example.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SimpleFilterFunc implements FilterFunction<Tuple2<String, Long>> {

    @Override
    public boolean filter(Tuple2<String, Long> value) {
        // 受信レコードの3カラム目がフィルタ用番号
        String number = value.f0.split(",")[2];
        // フィルタ用番号の下１桁
        String last = number.substring(number.length() - 1);
        // 下１桁が0のレコード（全体の約10分の1）は破棄
        boolean result = !last.equals("0");

        return result;
    }
}
