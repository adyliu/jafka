package com.sohu.jafka.producer.async;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Empty handler
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.4.0
 */
public abstract class AbstractCallbackHandler<T> implements CallbackHandler<T> {
    @Override
    public void init(Properties properties) {
    }

    @Override
    public QueueItem<T> beforeEnqueue(QueueItem<T> data) {
        return data;
    }

    @Override
    public QueueItem<T> afterEnqueue(QueueItem<T> data, boolean added) {
        return data;
    }

    @Override
    public List<QueueItem<T>> afterDequeuingExistingData(QueueItem<T> data) {
        return Collections.emptyList();
    }

    @Override
    public List<QueueItem<T>> beforeSendingData(List<QueueItem<T>> data) {
        return data;
    }

    @Override
    public List<QueueItem<T>> lastBatchBeforeClose() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }
}
