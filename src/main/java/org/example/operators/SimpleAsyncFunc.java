package org.example.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class SimpleAsyncFunc extends RichAsyncFunction<Tuple2<String, Long>, String> {

    @Override
    public final void asyncInvoke(Tuple2<String, Long> input, final ResultFuture<String> resultFuture) {
        CompletableFuture<String> result = new CompletableFuture<>();
        Executors.newCachedThreadPool().submit(() -> {
            Thread.sleep(1000); // 1秒かかる重い処理を擬似
            result.complete("");
            return null;
        });

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // Normally handled explicitly.
                    return null;
                }
            }
        }).thenAccept((String resultStr) -> {
            String payload = input.f0;
            long sendTime = Long.parseLong(payload.split(",")[1]);
            long inputTime = Long.parseLong(payload.split(",")[0]);
            long sendToInput = inputTime - sendTime;

            long sourceTime = input.f1;
            long inputToSource = sourceTime - inputTime;

            long sinkTime = System.currentTimeMillis();
            long sourceToSink = sinkTime - sourceTime;

            String output = sinkTime
                    + "," + sourceToSink
                    + "," + sourceTime
                    + "," + inputToSource
                    + "," + sendToInput
                    + "," + payload;

            resultFuture.complete(Collections.singleton(output));
        });
    }
}
