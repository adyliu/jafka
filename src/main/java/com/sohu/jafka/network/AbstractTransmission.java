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

package com.sohu.jafka.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class AbstractTransmission implements Transmission {

    private boolean done = false;

    final protected Logger logger = LoggerFactory.getLogger(getClass());

    public void expectIncomplete() {
        if (complete()) {
            throw new IllegalStateException("This operation cannot be completed on a complete request.");
        }
    }

    public void expectComplete() {
        if (!complete()) {
            throw new IllegalStateException("This operation cannot be completed on an incomplete request.");
        }
    }

    public boolean complete() {
        return done;
    }

    public void setCompleted() {
        this.done = true;
    }
}
