package org.example;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.example.operators.SimpleAsyncFunc;
import org.example.operators.SimpleFilterFunc;
import org.example.operators.SimpleMapFunc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

public class StreamingJob {

    private final SourceFunction<String> source;
    private final SinkFunction<String> sink;
    private final Properties operatorProperties;

    public StreamingJob(SourceFunction<String> source,
                        SinkFunction<String> sink,
                        Properties operatorProperties) {
        this.source = source;
        this.sink = sink;
        this.operatorProperties = operatorProperties;
    }

    public void build(StreamExecutionEnvironment see) {
        boolean enableOperatorParallelism =
                !this.operatorProperties.isEmpty()
                        && parseBoolean(this.operatorProperties.getProperty("enableOperatorParallelism"));

        SingleOutputStreamOperator<String> sourceStream =
                see.addSource(this.source, TypeInformation.of(String.class));
        if (enableOperatorParallelism) {
            sourceStream.setParallelism(parseInt(this.operatorProperties.getProperty("sourceParallelism")));
        }

        SingleOutputStreamOperator<Tuple2<String, Long>> mappedStream =
                sourceStream.map(new SimpleMapFunc());
        if (enableOperatorParallelism) {
            mappedStream.setParallelism(parseInt(this.operatorProperties.getProperty("mapParallelism")));
        }

        SingleOutputStreamOperator<Tuple2<String, Long>> filteredStream =
                mappedStream.filter(new SimpleFilterFunc());
        if (enableOperatorParallelism) {
            filteredStream.setParallelism(parseInt(this.operatorProperties.getProperty("filterParallelism")));
        }

        SingleOutputStreamOperator<String> asyncStream =
                AsyncDataStream.unorderedWait(
                        filteredStream,
                        new SimpleAsyncFunc(),
                        3000,
                        TimeUnit.MILLISECONDS,
                        100);
        if (enableOperatorParallelism) {
            asyncStream.setParallelism(parseInt(this.operatorProperties.getProperty("asyncParallelism")));
        }

        DataStreamSink<String> sinkStream = asyncStream
                .addSink(this.sink);
        if (enableOperatorParallelism) {
            sinkStream.setParallelism(parseInt(this.operatorProperties.getProperty("sinkParallelism")));
        }
    }

    private static final String region = "ap-northeast-1";
    private static final String inputStreamName = "ExampleInputStream";
    private static final String outputStreamName = "ExampleOutputStream";

    private static SourceFunction<String> createSourceFromStaticConfig() {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return new FlinkKinesisConsumer<>(inputStreamName, new KinesisDeserializationSchema<String>() {
            @Override
            public String deserialize(byte[] recordValue,
                                      String partitionKey,
                                      String seqNum,
                                      long approxArrivalTimestamp,
                                      String stream,
                                      String shardId) throws IOException {
                String recordValueStr = new String(recordValue, StandardCharsets.UTF_8);
                return approxArrivalTimestamp + "," + recordValueStr;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return null;
            }
        }, inputProperties);
    }

    private static FlinkKinesisProducer<String> createSinkFromStaticConfig() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(AWSConfigConstants.AWS_REGION, region);
        outputProperties.setProperty("AggregationEnabled", "false");

        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    private static SourceFunction<String> createSourceFromApplicationProperties() throws IOException {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

        Properties inputStreamProperties = applicationProperties.get("InputStreamProperties");
        System.out.println(inputStreamProperties.toString());
        String inputStreamName = inputStreamProperties.getProperty("streamName");
        return new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(),
                applicationProperties.get("ConsumerConfigProperties"));
    }

    private static SinkFunction<String> createSinkFromApplicationProperties() throws IOException {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(),
                applicationProperties.get("ProducerConfigProperties"));

        Properties outputStreamProperties = applicationProperties.get("OutputStreamProperties");
        System.out.println(outputStreamProperties.toString());
        String outputStreamName = outputStreamProperties.getProperty("streamName");
        String defaultPartition = outputStreamProperties.getProperty("defaultPartition");

        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition(defaultPartition);
        return sink;
    }

    private static Properties createOperatorProperties() {
        try {
            Map<String, Properties> runtimeProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            return runtimeProperties.get("ApplicationProperties");
        } catch (IOException e) {
            e.printStackTrace();
            return new Properties();
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        new StreamingJob(
//                createSourceFromApplicationProperties(),
//                createSinkFromApplicationProperties()
                createSourceFromStaticConfig(),
                createSinkFromStaticConfig(),
                createOperatorProperties()
        ).build(see);

        see.execute("Flink Streaming Java TestApp");
    }
}
