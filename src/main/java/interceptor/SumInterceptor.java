package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by ytt on 2019/1/8.
 */
public class SumInterceptor implements ProducerInterceptor<String, String> {
    int success = 0;
    int exc = 0;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            exc = exc + 1;
        } else {
            success = success + 1;
        }
    }

    public void close() {
        System.out.println("success = " + success);
        System.out.println("exc = " + exc);
    }

    public void configure(Map<String, ?> configs) {

    }
}
