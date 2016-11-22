package io.jafka.consumer;

/**
 * Simple message callback
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.8.0
 */
public interface IMessageListener<T> {

    /**
     * consume the message
     * <p>
     * This method should never throw any exception; otherwise this will interupt the stream
     * </p>
     *
     * @param message message body
     */
    void onMessage(T message);
}