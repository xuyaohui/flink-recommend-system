package flink.task;

import flink.function.LogMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author XINZE
 */
public class LogTask {


    public static void main(String[] args) throws Exception {
        String topicName = "log";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty("group.id","log");

        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), properties));
        dataStream.map(new LogMapFunction());

        env.execute("Log message receive");
    }
}
