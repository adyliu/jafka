/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sohu.jafka.consumer;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.PortUtils;
import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.api.MultiFetchResponse;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringEncoder;
import com.sohu.jafka.utils.Closer;
import com.sohu.jafka.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.net.www.http.HttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SimpleConsumerTest extends BaseJafkaServer {

    Jafka jafka;

    SimpleConsumer consumer;

    public SimpleConsumerTest() {
    }

    final int partitions = 3;
    final int INIT_MESSAGE_COUNT = 755;
    final int MESSAGE_BATCH_SIZE = 50;
    final int httpPort = PortUtils.checkAvailablePort(9093);

    @Before
    public void init() throws IOException{
        if (jafka == null) {

            Properties props = new Properties();
            //force flush message to disk
            //we will fetch nothing while messages have note been flushed to disk
            props.put("log.flush.interval", "1");
            props.put("log.default.flush.scheduler.interval.ms", "100");//flush to disk every 100ms
            props.put("log.file.size", "5120");//5k for rolling
            props.put("num.partitions", "" + partitions);//default divided three partitions
            props.put("http.port",""+httpPort);
            jafka = createJafka(props);
            sendSomeMessages(INIT_MESSAGE_COUNT, "demo", "test");

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));//waiting to receive all message
            flush(jafka);//force flush all logs to the disk
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        if (consumer == null) {
            consumer = new SimpleConsumer("localhost", jafka.getPort(), 10 * 1000, 1024 * 1024);
        }
    }

    private void sendSomeMessages(final int count, final String... topics) {
        Properties producerConfig = new Properties();
        producerConfig.setProperty("broker.list", "0:localhost:" + jafka.getPort());
        producerConfig.setProperty("serializer.class", StringEncoder.class.getName());
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerConfig));
        int index = 0;
        boolean over = false;
        while (!over) {
            StringProducerData[] data = new StringProducerData[topics.length];
            for (int x = 0; x < data.length; x++) {
                data[x] = new StringProducerData(topics[x]);
            }
            int batch = 50;
            while (batch-- > 0 && !over) {
                for (StringProducerData sd : data) {
                    if(!(over = index >= count)) {
                        sd.add(sd.getTopic() + "#message#" + (index++));
                    }
                }
            }
            for (StringProducerData sd : data) {
                producer.send(sd);
            }
        }
        Closer.closeQuietly(producer);
        logger.info("SEND_MESSAGE_COUNT {}/{}", index,count);
        assertEquals(count,index);
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


//    @Test
//    public void testCreatePartitions() throws IOException{
//        int size = consumer.createPartitions("demo", partitions-1, false);
//        assertEquals(partitions, size);
//        size = consumer.createPartitions("demo", partitions+1, false);
//        assertEquals(partitions, size);
//        size = consumer.createPartitions("demo", partitions+3, true);
//        assertEquals(partitions, size);
//        //
//        final String largePartitionTopic = "largepartition";
//        size = consumer.createPartitions(largePartitionTopic, partitions+5, true);
//        assertEquals(partitions+5, size);
//        sendSomeMessages(1000, largePartitionTopic);
//    }

    private void sendMessageByHttp(int port,String topic,int partition,byte[] data) throws IOException{
        URL url = new URL(String.format("http://127.0.0.1:%s/",port));
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("request_key","PRODUCE");
        conn.setRequestProperty("topic",topic);
        conn.setRequestProperty("partition",""+partition);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        //
        conn.getOutputStream().write(data);
        conn.getOutputStream().flush();
        conn.getOutputStream().close();
        //
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(),"UTF-8"));
        assertEquals("OK",reader.readLine());
        reader.close();

    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#close()}.
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
     * {@link com.sohu.jafka.consumer.SimpleConsumer#fetch(com.sohu.jafka.api.FetchRequest)}
     * .
     *
     * @throws IOException
     */
    @Test
    public void testFetch() throws IOException {
        long offset = -1;
        int cnt = 0;
        for (int i = 0; i < partitions; i++) {
            FetchRequest request = new FetchRequest("demo", i, 0, 1000 * 1000);
            ByteBufferMessageSet messages = consumer.fetch(request);
            for (MessageAndOffset msg : messages) {
                System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
                offset = Math.max(offset, msg.offset);
                cnt++;
            }
        }
        System.out.println("receive message count: " + cnt);
        assertTrue(offset > 0);
        assertTrue(cnt > 0);
    }

    @Test
    public void testFetchMessageCreatedByHttp() throws Exception{

        sendMessageByHttp(httpPort,"httpdemo",0,"HTTPMESSAGE".getBytes());
        sendMessageByHttp(httpPort,"httpdemo",0,"OVER".getBytes());
        FetchRequest request = new FetchRequest("httpdemo",0,0,100*1000);
        ByteBufferMessageSet messages = consumer.fetch(request);
        String ret = "";
        for(MessageAndOffset msg:messages){
            ret+=Utils.toString(msg.message.payload(),"UTF-8");
        }
        assertEquals("HTTPMESSAGEOVER",ret);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#getOffsetsBefore(java.lang.String, int, long, int)}
     * .
     */
    @Test
    public void testGetOffsetsBefore() throws IOException {
        int size = 0;
        long maxoffset = -1;
        for (int i = 0; i < 3; i++) {
            long[] offsets = consumer.getOffsetsBefore("demo", i, OffsetRequest.LATES_TTIME, 100);
            size += offsets.length;
            if (offsets.length > 0 && offsets[0] > maxoffset) {
                maxoffset = offsets[0];
            }
        }
        System.out.println("demo largest offset: " + maxoffset);
        assertTrue(size > 0);
        assertTrue(maxoffset > 0);
    }

    /**
     * Test method for
     * {@link com.sohu.jafka.consumer.SimpleConsumer#multifetch(java.util.List)}
     * .
     */
    @Test
    public void testMultifetch() throws IOException {

        long bigestOffset = -1;
        int total = 0, democnt = 0, testcnt = 0;
        //
        int loop = 10;
        while (loop-- > 0 && total < INIT_MESSAGE_COUNT) {
            for (int i = 0; i < partitions && total < INIT_MESSAGE_COUNT; i++) {

                long demoOffset = 0;
                long testOffset = 0;

                FetchRequest request1 = new FetchRequest("demo", i, demoOffset, 1000 * 1000);
                FetchRequest request2 = new FetchRequest("test", i, testOffset, 1000 * 1000);
                MultiFetchResponse responses = consumer.multifetch(Arrays.asList(request1, request2));
                assertEquals(2, responses.size());
                Iterator<ByteBufferMessageSet> iter = responses.iterator();
                assertTrue(iter.hasNext());
                ByteBufferMessageSet demoMessages = iter.next();
                assertTrue(iter.hasNext());
                ByteBufferMessageSet testMessages = iter.next();
                assertFalse(iter.hasNext());
                //
                for (MessageAndOffset msg : demoMessages) {
                    //System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
                    bigestOffset = Math.max(bigestOffset, msg.offset);
                    total++;
                    democnt++;
                    demoOffset = msg.offset;
                }
                for (MessageAndOffset msg : testMessages) {
                    //System.out.println("Receive message: " + Utils.toString(msg.message.payload(), "UTF-8"));
                    bigestOffset = Math.max(bigestOffset, msg.offset);
                    total++;
                    testcnt++;
                    testOffset = msg.offset;
                }

            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
        consumer.close();
        logger.info("multifetch receive message count: {}, demo={}, test={}", total, democnt, testcnt);
        assertTrue(bigestOffset > 0);
        assertTrue("some messages left",total >= INIT_MESSAGE_COUNT);
    }

}
