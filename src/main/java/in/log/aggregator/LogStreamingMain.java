package in.log.aggregator;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Author     : nidhi
 * Created on : 10/7/17.
 */
public class LogStreamingMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogStreamingMain.class);
    private static String KAFKA_TOPIC = "topic";

    public static void main(String[] args) throws Exception {

        String propertiesFile = args[0];
        if (StringUtils.isEmpty(propertiesFile)) {
            LOGGER.error("Please pass properties file path as a argument.");
            return;
        }
        Properties kafkaProperties = new Properties();
        kafkaProperties.load(new FileReader(propertiesFile));

        kafkaProperties.put("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.put("value.deserializer", StringDeserializer.class.getName());

        String kafkaTopic = kafkaProperties.getProperty(KAFKA_TOPIC);

        if (StringUtils.isEmpty(kafkaTopic)) {
            LOGGER.error("Not a valid kafka topic from properties file.");
            return;
        }

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<String>(kafkaTopic,
                new SimpleStringSchema(), kafkaProperties);


        DataStreamSource<String> dataSource = environment.addSource(kafkaSource);

        DataStream<Tuple3<String, String, Integer>> aggregatedResult =
                dataSource.filter(new FilterFunction<String>() {
                    public boolean filter(String s) throws Exception {
                        if (StringUtils.isNotEmpty(s)) {
                            return true;
                        }
                        return false;
                    }
                }).flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
                    public void flatMap(String input, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
                        String[] logSplit = input.split("-");
                        if (logSplit != null && logSplit.length == 9) {
                            String[] timeSplit = logSplit[3].split(":");
                            String timeInMin = new StringBuilder(timeSplit[0]).append(":")
                                    .append(timeSplit[1]).toString();
                            String timeStamp = new StringBuilder(logSplit[1]).append("-")
                                    .append(logSplit[2]).append("-")
                                    .append(timeInMin).toString();
                            String requestType = logSplit[5];
                            LOGGER.info("request-type : " + requestType + ", time: " + timeStamp);
                            collector.collect(new Tuple3<String, String, Integer>(timeStamp, requestType, 1));
                        }
                    }
                }).keyBy(0, 1).timeWindow(Time.of(1, TimeUnit.MINUTES)).sum(2);

        aggregatedResult.print();

        environment.execute();

    }
}
