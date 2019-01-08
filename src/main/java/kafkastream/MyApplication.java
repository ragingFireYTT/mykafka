package kafkastream;


import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by ytt on 2019/1/8.
 * 流处理。
 */
public class MyApplication {
    public static void main(String[] args) {

        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "myTestStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        StreamsConfig config = new StreamsConfig(props);

        // 设置拓扑图
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", "first")
                .addProcessor("PROCESS", new ProcessorSupplier<byte[], byte[]>() {
                    public Processor<byte[], byte[]> get() {
                        // 具体分析处理
                        return new LogProcessor();
                    }
                }, "SOURCE")
                .addSink("SINK", "san", "PROCESS");
        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

    }
}
