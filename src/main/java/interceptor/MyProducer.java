package interceptor;

import org.apache.kafka.clients.producer.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by ytt on 2019/1/8.
 */
public class MyProducer {
    @SuppressWarnings("all")
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        // 应答级别
        props.put("acks", "all");
        props.put("retries", 0);
        // 一次提交数据大小
        props.put("batch.size", 16384);
        // 多长时间提交一次
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 添加 拦截器 interceptor
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList(new TimeInterceptor().getClass(), new SumInterceptor().getClass()));

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(
                    new ProducerRecord<String, String>("san", Integer.toString(i))
            );
        }

        producer.close();
    }
}
