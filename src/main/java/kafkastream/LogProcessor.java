package kafkastream;


import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Created by ytt on 2019/1/8.
 */
public class LogProcessor implements Processor<byte[], byte[]> {
    ProcessorContext context = null;

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public void process(byte[] key, byte[] value) {
        String line = new String(value);
        if (line != null) {
            line = line.replace(">>", "");
        }
        context.forward(key,line.getBytes());
    }

    public void punctuate(long timestamp) {

    }

    public void close() {

    }
}
