package io.jafka.producer;

import java.util.Map;
import java.util.Properties;


import io.jafka.producer.serializer.StringEncoder;

/**
 * Simple String producer
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.8.0
 */
public class StringProducers {

    private final Producer<String, String> producer;

    private StringProducers(Properties props, boolean autoClosed) {
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
        if (autoClosed) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    producer.close();
                }
            });
        }
    }

    public Producer<String, String> getProducer() {
        return this.producer;
    }

    /**
     * send the message to message-queue
     *
     * @param message message
     */
    public void send(StringProducerData message) {
        producer.send(message);
    }

    private static volatile StringProducers instance = null;

    /**
     * create a producer each time
     *
     * @param props producer configuration
     * @return a producer
     */
    public static StringProducers build(Properties props, boolean autoClosed) {
        return new StringProducers(props, autoClosed);
    }

    /**
     * build the global producer (thread-safety)
     *
     * @param zookeeperConfig connect config of zookeeper; ex: 127.0.0.1:2181/jafka
     * @return the global producer
     */
    public static StringProducers buildGlobal(String zookeeperConfig) {
        if (instance == null) {
            synchronized (StringProducers.class) {
                if (instance == null) {
                    final Properties props = new Properties();
                    props.put("zk.connect", zookeeperConfig);
                    props.put("serializer.class", StringEncoder.class.getName());
                    //
                    final String JAFKA_PREFIX = "jafka.";
                    for (Map.Entry<Object, Object> e : System.getProperties().entrySet()) {
                        String name = (String) e.getKey();
                        if (name.startsWith(JAFKA_PREFIX)) {
                            props.put(name.substring(JAFKA_PREFIX.length()), (String) e.getValue());
                        }
                    }
                    instance = new StringProducers(props, true);
                }
            }
        }
        return instance;
    }
}