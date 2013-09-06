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

package com.sohu.jafka.producer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


import com.sohu.jafka.api.ProducerRequest;
import com.sohu.jafka.cluster.Broker;
import com.sohu.jafka.cluster.Partition;
import com.sohu.jafka.common.InvalidConfigException;
import com.sohu.jafka.common.UnavailableProducerException;
import com.sohu.jafka.common.annotations.ClientSide;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.producer.async.AsyncProducer;
import com.sohu.jafka.producer.async.AsyncProducerConfig;
import com.sohu.jafka.producer.async.CallbackHandler;
import com.sohu.jafka.producer.async.DefaultEventHandler;
import com.sohu.jafka.producer.async.EventHandler;
import com.sohu.jafka.producer.serializer.Encoder;
import com.sohu.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
public class ProducerPool<V> implements Closeable {

    private final ProducerConfig config;

    private final Encoder<V> serializer;

    private final ConcurrentMap<Integer, SyncProducer> syncProducers;

    private final ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers;

    private final EventHandler<V> eventHandler;

    private final CallbackHandler<V> callbackHandler;

    private boolean sync = true;

    private final Logger logger = LoggerFactory.getLogger(ProducerPool.class);

    public ProducerPool(ProducerConfig config,//
            Encoder<V> serializer, //
            ConcurrentMap<Integer, SyncProducer> syncProducers,//
            ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers, //
            EventHandler<V> inputEventHandler, //
            CallbackHandler<V> callbackHandler) {
        super();
        this.config = config;
        this.serializer = serializer;
        this.syncProducers = syncProducers;
        this.asyncProducers = asyncProducers;
        this.eventHandler = inputEventHandler != null ? inputEventHandler : new DefaultEventHandler<V>(config, callbackHandler);
        this.callbackHandler = callbackHandler;
        if (serializer == null) {
            throw new InvalidConfigException("serializer passed in is null!");
        }
        this.sync = !"async".equalsIgnoreCase(config.getProducerType());
    }

    public ProducerPool(ProducerConfig config, Encoder<V> serializer,//
            EventHandler<V> eventHandler,//
            CallbackHandler<V> callbackHandler) {
        this(config,//
                serializer,//
                new ConcurrentHashMap<Integer, SyncProducer>(),//
                new ConcurrentHashMap<Integer, AsyncProducer<V>>(),//
                eventHandler,//
                callbackHandler);
    }

    @SuppressWarnings("unchecked")
    public ProducerPool(ProducerConfig config, Encoder<V> serializer) {
        this(config,//
                serializer,//
                new ConcurrentHashMap<Integer, SyncProducer>(),//
                new ConcurrentHashMap<Integer, AsyncProducer<V>>(),//
                (EventHandler<V>) Utils.getObject(config.getEventHandler()),//
                (CallbackHandler<V>) Utils.getObject(config.getCbkHandler()));
    }

    /**
     * add a new producer, either synchronous or asynchronous, connecting
     * to the specified broker
     * 
     * @param broker broker to producer
     */
    public void addProducer(Broker broker) {
        Properties props = new Properties();
        props.put("host", broker.host);
        props.put("port", "" + broker.port);
        props.putAll(config.getProperties());
        if (sync) {
            SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
            logger.info("Creating sync producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port);
            syncProducers.put(broker.id, producer);
        } else {
            AsyncProducer<V> producer = new AsyncProducer<V>(new AsyncProducerConfig(props),//
                    new SyncProducer(new SyncProducerConfig(props)),//
                    serializer,//
                    eventHandler,//
                    config.getEventHandlerProperties(),//
                    this.callbackHandler, //
                    config.getCbkHandlerProperties());
            producer.start();
            logger.info("Creating async producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port);
            asyncProducers.put(broker.id, producer);
        }
    }

    /**
     * selects either a synchronous or an asynchronous producer, for the
     * specified broker id and calls the send API on the selected producer
     * to publish the data to the specified broker partition
     * 
     * @param ppd the producer pool request object
     */
    public void send(ProducerPoolData<V> ppd) {
        if (logger.isDebugEnabled()) {
            logger.debug("send message: " + ppd);
        }
        if (sync) {
            Message[] messages = new Message[ppd.data.size()];
            int index = 0;
            for (V v : ppd.data) {
                messages[index] = serializer.toMessage(v);
                index++;
            }
            ByteBufferMessageSet bbms = new ByteBufferMessageSet(config.getCompressionCodec(), messages);
            ProducerRequest request = new ProducerRequest(ppd.topic, ppd.partition.partId, bbms);
            SyncProducer producer = syncProducers.get(ppd.partition.brokerId);
            if (producer == null) {
                throw new UnavailableProducerException("Producer pool has not been initialized correctly. " + "Sync Producer for broker "
                        + ppd.partition.brokerId + " does not exist in the pool");
            }
            producer.send(request.topic, request.partition, request.messages);
        } else {
            AsyncProducer<V> asyncProducer = asyncProducers.get(ppd.partition.brokerId);
            for (V v : ppd.data) {
                asyncProducer.send(ppd.topic, v, ppd.partition.partId);
            }
        }
    }

    public void send(List<ProducerPoolData<V>> poolData) {
        if (sync) {
            syncSend(poolData);
        } else {
            asyncSend(poolData);
        }
    }

    private void asyncSend(List<ProducerPoolData<V>> poolData) {
        for (ProducerPoolData<V> ppd : poolData) {
            AsyncProducer<V> asyncProducer = asyncProducers.get(ppd.partition.brokerId);
            for (V v : ppd.data) {
                asyncProducer.send(ppd.topic, v, ppd.partition.partId);
            }
        }
    }

    private void syncSend(List<ProducerPoolData<V>> poolData) {
        final Map<Integer, List<ProducerRequest>> topicBrokerIdData = new HashMap<Integer, List<ProducerRequest>>();
        for (ProducerPoolData<V> ppd : poolData) {
            List<ProducerRequest> messageSets = topicBrokerIdData.get(ppd.partition.brokerId);
            if (messageSets == null) {
                messageSets = new ArrayList<ProducerRequest>();
                topicBrokerIdData.put(ppd.partition.brokerId, messageSets);
            }
            Message[] messages = new Message[ppd.data.size()];
            int index = 0;
            for (V v : ppd.data) {
                messages[index] = serializer.toMessage(v);
                index++;
            }
            ByteBufferMessageSet bbms = new ByteBufferMessageSet(config.getCompressionCodec(), messages);
            messageSets.add(new ProducerRequest(ppd.topic, ppd.partition.partId, bbms));
        }
        for (Map.Entry<Integer, List<ProducerRequest>> e : topicBrokerIdData.entrySet()) {
            SyncProducer producer = syncProducers.get(e.getKey());
            if (producer == null) {
                throw new UnavailableProducerException("Producer pool has not been initialized correctly. " + "Sync Producer for broker " + e.getKey()
                        + " does not exist in the pool");
            }
            if (e.getValue().size() == 1) {
                ProducerRequest request = e.getValue().get(0);
                producer.send(request.topic, request.partition, request.messages);
            } else {
                producer.multiSend(e.getValue());
            }
        }
    }

    /**
     * Closes all the producers in the pool
     */
    public void close() {
        logger.info("Closing all sync producers");
        if (sync) {
            for (SyncProducer p : syncProducers.values()) {
                p.close();
            }
        } else {
            for (AsyncProducer<V> p : asyncProducers.values()) {
                p.close();
            }
        }

    }

    /**
     * This constructs and returns the request object for the producer pool
     * 
     * @param topic the topic to which the data should be published
     * @param bidPid the broker id and partition id
     * @param data the data to be published
     */
    public ProducerPoolData<V> getProducerPoolData(String topic, Partition bidPid, List<V> data) {
        return new ProducerPoolData<V>(topic, bidPid, data);
    }
}
