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

package com.sohu.jafka.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * a template for object iterator
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public abstract class IteratorTemplate<T> implements Iterator<T> {

    enum State {
        DONE, READY, NOT_READY, FAILED;
    }

    private State state = State.NOT_READY;

    private T nextItem = null;

    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.NOT_READY;
        return nextItem;
    }

    public boolean hasNext() {
        switch (state) {
            case FAILED:
                throw new IllegalStateException("Iterator is in failed state");
            case DONE:
                return false;
            case READY:
                return true;
            case NOT_READY:
                break;
        }
        return maybeComputeNext();
    }

    protected abstract T makeNext();

    private boolean maybeComputeNext() {
        state = State.FAILED;
        nextItem = makeNext();
        if (state == State.DONE) return false;
        state = State.READY;
        return true;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected void resetState() {
        state = State.NOT_READY;
    }

    protected T allDone() {
        state = State.DONE;
        return null;
    }
}
