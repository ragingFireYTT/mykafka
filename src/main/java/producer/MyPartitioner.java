package producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by ytt on 2019/1/7.
 */
public class MyPartitioner implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
