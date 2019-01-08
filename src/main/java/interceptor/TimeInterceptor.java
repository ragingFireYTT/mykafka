package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by ytt on 2019/1/8.
 *
 * 添加时间
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        ProducerRecord<String, String> returnP = new ProducerRecord<String, String>
                (record.topic(), record.partition(), record.key(), System.currentTimeMillis()+":::"+record.value());
        return returnP;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
