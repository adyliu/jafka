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

package io.jafka.network.handlers;

import io.jafka.api.FetchRequest;
import io.jafka.api.MultiFetchRequest;
import io.jafka.api.RequestKeys;
import io.jafka.log.LogManager;
import io.jafka.network.MessageSetSend;
import io.jafka.network.MultiMessageSetSend;
import io.jafka.network.Receive;
import io.jafka.network.Send;

import java.util.ArrayList;
import java.util.List;

/**
 * handler for multi fetch request
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class MultiFetchHandler extends FetchHandler {

    public MultiFetchHandler(LogManager logManager) {
        super(logManager);
    }

    public Send handler(RequestKeys requestType, Receive request) {
        MultiFetchRequest multiFetchRequest = MultiFetchRequest.readFrom(request.buffer());
        List<FetchRequest> fetches = multiFetchRequest.getFetches();
        if (logger.isDebugEnabled()) {
            logger.debug("Multifetch request objects size: " + fetches.size());
            for (FetchRequest fetch : fetches) {
                logger.debug(fetch.toString());
            }
        }
        List<MessageSetSend> responses = new ArrayList<MessageSetSend>(fetches.size());
        for (FetchRequest fetch : fetches) {
            responses.add(readMessageSet(fetch));
        }
        return new MultiMessageSetSend(responses);
    }
}