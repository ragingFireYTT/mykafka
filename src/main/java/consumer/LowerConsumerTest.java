package consumer;

import kafka.api.*;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ytt on 2019/1/7.
 */
public class LowerConsumerTest {
    public static void main(String[] args) {
        // 获取指定 topic 指定 partition 指定 offset 的数据

        //集群
        List<String> brokers = Arrays.asList("hadoop102","hadoop103","hadoop104");
        //端口
        int port = 9092;

        String topic = "san";

        int partition=0;

        long offset = 0;

        new LowerConsumerTest().getData(brokers,port,topic,partition,offset);

    }

    //获取数据
    private void getData(List<String> hosts, int port, String topic, int partition, long offset){
        BrokerEndPoint leader = getLeader(hosts, port, topic, partition);
        String host = leader.host();

        //创建连接集群的客户端( 连接单个broker 的客户端 )
        SimpleConsumer consumer = new SimpleConsumer(host, port, 1000, 1024 * 64, "leader");
        kafka.api.FetchRequest request = new FetchRequestBuilder()
                .addFetch(topic,partition,offset,50000)
                .build();
        FetchResponse fetch = consumer.fetch(request); // 通过客户端抓取数据
        ByteBufferMessageSet messageAndOffsets = fetch.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            Message message = messageAndOffset.message();
            ByteBuffer payload = message.payload();
            byte[] b= new byte[payload.limit()];
            payload.get(b);
            System.out.println("topic:"+topic
                    +"---partition:"+partition
                    +"---offset:"+messageAndOffset.offset()
                    +"---value:"+new String(b));
        }

    }

    // 获取分区的leader
    private BrokerEndPoint getLeader(List<String> hosts, int port, String topic, int partition) {
        for (String host : hosts) {
            //创建连接集群的客户端( 连接单个broker 的客户端 )
            SimpleConsumer consumer = new SimpleConsumer(host, port, 1000, 1024 * 64, "leader");
            //获得，集群的，topic ，partitions 的 metaData 数据
            TopicMetadataRequest request = new TopicMetadataRequest(Arrays.asList(topic));
            TopicMetadataResponse metadataResponse = consumer.send(request); // 获得元数据信息Topic
            List<TopicMetadata> topicMetadatas = metadataResponse.topicsMetadata();
            for (TopicMetadata topicMetadata : topicMetadatas) {
                List<PartitionMetadata> partitionMetadatas = topicMetadata.partitionsMetadata();
                for (PartitionMetadata partitionMetadata : partitionMetadatas) {
                    if (partitionMetadata.partitionId() == partition) {
                        BrokerEndPoint leader = partitionMetadata.leader();
                        consumer.close();
                        return leader;
                    }
                }
            }
            consumer.close(); // 关闭外部资源
        }
        return null;
    }


}
