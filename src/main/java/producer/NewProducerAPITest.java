package producer;

import org.apache.kafka.clients.producer.*;
import org.junit.Test;

import java.util.Properties;

/*
 * Created by ytt on 2019/1/7.
 */
public class NewProducerAPITest {
    @Test
    public void testNewProducerAPI() {
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
        // 设置分区
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"producer.MyPartitioner");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(
                    new ProducerRecord<String, String>("san", Integer.toString(i)),
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            // 回调方法
                            if (metadata != null) {
                                System.out.println("metadata"
                                        + "---" + metadata.topic()
                                        + "---" + metadata.partition()
                                        + "---" + metadata.offset());
                            }
                        }
                    });
        }

        producer.close();
    }
}
