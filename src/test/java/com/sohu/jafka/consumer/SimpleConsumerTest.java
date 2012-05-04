package com.sohu.jafka.consumer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.api.MultiFetchResponse;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringEncoder;
import com.sohu.jafka.utils.Utils;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SimpleConsumerTest extends BaseJafkaServer {

    Jafka jafka;

    SimpleConsumer consumer;
    final static AtomicInteger count = new AtomicInteger();
    public SimpleConsumerTest() {
        System.out.println("build instance "+count.incrementAndGet());
    }

    @Before
    public void init() {
        if (jafka == null) {
            Properties props = new Properties();
            //force flush message to disk
            //we will fetch nothing while messages have note been flushed to disk
            props.put("log.flush.interval", "1");
            jafka = createJafka(props);
            Properties producerConfig = new Properties();
            producerConfig.setProperty("broker.list", "0:localhost:9092");
            producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
            Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
            StringProducerData data = new StringProducerData("demo");
            StringProducerData data2 = new StringProducerData("demo2");
            for (int i = 0; i < 10; i++) {
                data.add("message#" + i);
                data2.add("message#demo2#" + i);
            }
            producer.send(data);
            producer.send(data2);
            producer.close();
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1L));//waiting for log created
        }
        if (consumer == null) {
            consumer = new SimpleConsumer("localhost", 9092, 10 * 1000, 1024 * 1024);
        }
    }

    @After
    public void destroy() throws Throwable {
        close(jafka);
        jafka = null;
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    /**
     * Test method for {@link com.sohu.jafka.consumer.SimpleConsumer#close()}.
     */
    @Test
    public void testClose() throws IOException {
        consumer.close();
        //
        //reconnect by itself
        testFetch();
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#fetch(com.sohu.jafka.api.FetchRequest)}.
     * 
     * @throws IOException
     */
    @Test
    public void testFetch() throws IOException {
        FetchRequest request = new FetchRequest("demo", 0, 0, 100 * 1000);
        ByteBufferMessageSet messages = consumer.fetch(request);
        long offset = 0;
        for (MessageAndOffset msg : messages) {
            System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
            offset = msg.offset;
        }
        assertTrue(offset > 0);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#getOffsetsBefore(java.lang.String, int, long, int)}
     * .
     */
    @Test
    public void testGetOffsetsBefore() throws IOException {
        long[] offsets = consumer.getOffsetsBefore("demo", 0, OffsetRequest.LatestTime, 1);
        // System.out.println(Arrays.toString(offsets));
        assertTrue(offsets.length > 0);
        assertTrue(offsets[0] >= 0);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#multifetch(java.util.List)}.
     */
    @Test
    public void testMultifetch() throws IOException {
        FetchRequest request1 = new FetchRequest("demo", 0, 0, 100 * 1000);
        FetchRequest request2 = new FetchRequest("demo2", 0, 0, 100 * 1000);
        MultiFetchResponse responses = consumer.multifetch(Arrays.asList(request1, request2));
        for (ByteBufferMessageSet messages : responses) {
            long offset = 0;
            for (MessageAndOffset msg : messages) {
                System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
                offset = msg.offset;
            }
            assertTrue(offset > 0);
        }

    }

}
