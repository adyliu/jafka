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

package com.sohu.jafka.server;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.api.MultiFetchRequest;
import com.sohu.jafka.api.MultiProducerRequest;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.api.ProducerRequest;
import com.sohu.jafka.api.RequestKeys;
import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.log.Log;
import com.sohu.jafka.log.LogManager;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.message.MessageSet;
import com.sohu.jafka.mx.BrokerTopicStat;
import com.sohu.jafka.network.HandlerMapping;
import com.sohu.jafka.network.HandlerMappingFactory;
import com.sohu.jafka.network.OffsetArraySend;
import com.sohu.jafka.network.Receive;
import com.sohu.jafka.network.Send;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-6
 */
public class RequestHandlers {

    class FetchHandler implements HandlerMapping {

        public Send handler(RequestKeys requestType, Receive request) {
            FetchRequest fetchRequest = FetchRequest.readFrom(request.buffer());
            if (logger.isDebugEnabled()) {
                logger.debug("Fetch request " + fetchRequest.toString());
            }
            return readMessageSet(fetchRequest);
        }
    }

    class MultiFetchHandler implements HandlerMapping {

        public Send handler(RequestKeys requestType, Receive request) {
            MultiFetchRequest multiFetchRequest = MultiFetchRequest.readFrom(request.buffer());
            List<FetchRequest> fetches = multiFetchRequest.getFetches();
            if (logger.isDebugEnabled()) {
                logger.debug("Multifetch request objects size: " + fetches.size());
                for (FetchRequest fetch : fetches) {
                    logger.debug(fetch);
                }
            }
            List<MessageSetSend> responses = new ArrayList<MessageSetSend>(fetches.size());
            for (FetchRequest fetch : fetches) {
                responses.add(readMessageSet(fetch));
            }
            return new MultiMessageSetSend(responses);
        }
    }

    class MultiProduceHandler implements HandlerMapping {

        public Send handler(RequestKeys requestType, Receive receive) {
            MultiProducerRequest request = MultiProducerRequest.readFrom(receive.buffer());
            if (logger.isDebugEnabled()) {
                logger.debug("Multiproducer request " + request);
            }
            for (ProducerRequest produce : request.produces) {
                handleProducerRequest(produce, "MultiProducerRequest");
            }
            return null;
        }
    }

    class OffsetsHandler implements HandlerMapping {

        public Send handler(RequestKeys requestType, Receive request) {
            OffsetRequest offsetRequest = OffsetRequest.readFrom(request.buffer());
            return new OffsetArraySend(logManager.getOffsets(offsetRequest));
        }
    }

    class ProducerHandler implements HandlerMapping {

        public Send handler(RequestKeys requestType, Receive receive) {
            final long st = System.currentTimeMillis();
            ProducerRequest request = ProducerRequest.readFrom(receive.buffer());
            if (logger.isDebugEnabled()) {
                logger.debug("Producer request " + request.toString());
            }
            handleProducerRequest(request, "ProduceRequest");
            long et = System.currentTimeMillis();
            if (logger.isDebugEnabled()) {
                logger.debug("produce a message(set) cost " + (et - st) + " ms");
            }
            return null;
        }
    }

    private final Logger logger = Logger.getLogger(RequestHandlers.class);

    final LogManager logManager;

    public RequestHandlers(LogManager logManager) {
        this.logManager = logManager;
    }

    private void handleProducerRequest(ProducerRequest request, String requestHandlerName) {
        int partition = request.getTranslatedPartition(logManager);
        try {
            final Log log = logManager.getOrCreateLog(request.getTopic(), partition);
            log.append(request.getMessages());
            long messageSize = request.getMessages().getSizeInBytes();
            if (logger.isDebugEnabled()) {
                logger.debug(messageSize + " bytes written to logs " + log);
                for (MessageAndOffset m : request.getMessages()) {
                    logger.debug("wrote message " + m.offset + " to disk");
                }
            }
            BrokerTopicStat.getInstance(request.getTopic()).recordBytesIn(messageSize);
            BrokerTopicStat.getBrokerAllTopicStat().recordBytesIn(messageSize);
        } catch (RuntimeException e) {
            logger.error("Error processing " + requestHandlerName + " on " + request.getTopic() + ":" + partition, e);
            BrokerTopicStat.getInstance(request.getTopic()).recordFailedProduceRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
            throw e;
        } catch (Exception e) {
            logger.error("Error processing " + requestHandlerName + " on " + request.getTopic() + ":" + partition, e);
            BrokerTopicStat.getInstance(request.getTopic()).recordFailedProduceRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public HandlerMappingFactory handlerFor() {
        return new HandlerMappingFactory() {

            final FetchHandler fetchHandler = new FetchHandler();

            final MultiFetchHandler multiFetchHandler = new MultiFetchHandler();

            final MultiProduceHandler multiProduceHandler = new MultiProduceHandler();

            final OffsetsHandler offsetsHandler = new OffsetsHandler();

            final ProducerHandler producerHandler = new ProducerHandler();

            public HandlerMapping mapping(RequestKeys id, Receive request) {
                switch (id) {
                    case Fetch:
                        return fetchHandler;
                    case Produce:
                        return producerHandler;
                    case MultiFetch:
                        return multiFetchHandler;
                    case MultiProduce:
                        return multiProduceHandler;
                    case Offsets:
                        return offsetsHandler;
                }
                throw new IllegalStateException("No mapping found for handler id " + id);
            }
        };
    }

    private MessageSetSend readMessageSet(FetchRequest fetchRequest) {
        final String topic = fetchRequest.topic;
        MessageSetSend response = null;
        try {
            Log log = logManager.getLog(topic, fetchRequest.partition);
            if (logger.isDebugEnabled()) {
                logger.debug("Fetching log segment for request=" + fetchRequest + ", log=" + log);
            }
            if (log != null) {
                response = new MessageSetSend(log.read(fetchRequest.offset, fetchRequest.maxSize));
                BrokerTopicStat.getInstance(topic).recordBytesOut(response.messages.getSizeInBytes());
                BrokerTopicStat.getBrokerAllTopicStat().recordBytesOut(response.messages.getSizeInBytes());
            } else {
                response = new MessageSetSend();
            }
        } catch (Exception e) {
            logger.error("error when processing request " + fetchRequest, e);
            BrokerTopicStat.getInstance(topic).recordFailedFetchRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedFetchRequest();
            response = new MessageSetSend(MessageSet.Empty, ErrorMapping.valueOf(e));
        }
        return response;
    }

}
