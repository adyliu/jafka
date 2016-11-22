package io.jafka.consumer;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import io.jafka.producer.serializer.StringDecoder;
import io.jafka.utils.Closer;
import io.jafka.utils.ImmutableMap;

/**
 * Simple string consumer factory
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.8.0
 */
public class StringConsumers implements Closeable {

    private ExecutorService executor;
    private final ConsumerConnector connector;

    private StringConsumers(Properties props, final String topic, final int threads, final IMessageListener<String> listener) {
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        connector = Consumer.create(consumerConfig);

        Map<String, List<MessageStream<String>>> topicMessageStreams = connector.createMessageStreams(//
                ImmutableMap.of(topic, threads), new StringDecoder());
        List<MessageStream<String>> streams = topicMessageStreams.get(topic);
        //
        executor = Executors.newFixedThreadPool(threads);
        for (final MessageStream<String> stream : streams) {
            executor.submit(new Runnable() {

                public void run() {
                    for (String message : stream) {
                        listener.onMessage(message);
                    }
                }
            });
        }
        //
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                StringConsumers.this.close();
            }
        }));
    }


    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
            Closer.closeQuietly(connector);
            executor = null;
        }
    }

    /**
     * build a callback consumer
     *
     * @param props    consumer configuration
     * @param topic    the topic to be watched
     * @param groupId  grouping the consumer clients
     * @param listener message listener
     * @param threads  threads of consumer per topic
     * @return the real consumer
     */
    public static StringConsumers buildConsumer(Properties props, final String topic,//
                                                final String groupId,//
                                                final IMessageListener<String> listener,//
                                                final int threads
    ) {
        if (props == null || props.isEmpty()) {
            props = new Properties();
        }
        props.put("groupid", groupId);
        return new StringConsumers(props, topic, threads, listener);
    }

    /**
     * build a callback consumer
     *
     * @param zookeeperConfig connect config of zookeeper; ex: 127.0.0.1:2181/jafka
     * @param topic           the topic to be watched
     * @param groupId         grouping the consumer clients
     * @param listener        message listener
     * @param threads         threads of consumer per topic
     * @return the real consumer
     */
    public static StringConsumers buildConsumer(
            final String zookeeperConfig,//
            final String topic,//
            final String groupId,//
            final IMessageListener<String> listener, //
            final int threads) {
        Properties props = new Properties();
        props.put("zk.connect", zookeeperConfig);
        props.put("zk.sessiontimeout.ms", "30000");
        props.put("zk.connectiontimeout.ms", "30000");

        final String JAFKA_PREFIX = "jafka.";
        for (Map.Entry<Object, Object> e : System.getProperties().entrySet()) {
            String name = (String) e.getKey();
            if (name.startsWith(JAFKA_PREFIX)) {
                props.put(name.substring(JAFKA_PREFIX.length()), (String) e.getValue());
            }
        }
        return buildConsumer(props, topic, groupId, listener, threads);
    }

    /**
     * create a consumer
     *
     * @param zookeeperConfig connect config of zookeeper; ex: 127.0.0.1:2181/jafka
     * @param topic           the topic to be watched
     * @param groupId         grouping the consumer clients
     * @param listener        message listener
     * @return the real consumer
     */
    public static StringConsumers buildConsumer(
            final String zookeeperConfig,//
            final String topic,//
            final String groupId, //
            final IMessageListener<String> listener) {
        return buildConsumer(zookeeperConfig, topic, groupId, listener, 2);
    }
}