package com.sohu.jafka.producer;

import java.util.Properties;

import org.junit.Test;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.producer.serializer.StringEncoder;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ProducerTest extends BaseJafkaServer {

    /**
     * Test method for
     * {@link com.sohu.jafka.producer.Producer#send(com.sohu.jafka.producer.ProducerData)}.
     */
    @Test
    public void testSend() {
        Jafka jafka = createJafka();
        Properties producerConfig = new Properties();
        producerConfig.setProperty("broker.list", "0:localhost:9092");
        producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
        producer.send(new StringProducerData("demo").add("Hello jafka").add("https://github.com/adyliu/jafka"));
        producer.close();
        close(jafka);
    }

}
