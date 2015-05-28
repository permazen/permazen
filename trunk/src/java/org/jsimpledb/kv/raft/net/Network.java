
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft.net;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstraction layer representing "the network", over which we communicate with other nodes.
 *
 * <p>
 * In general, messages may be delayed, dropped or delivered out of order. Specific implementations
 * may provide more strict guarantees.
 * </p>
 *
 * <p>
 * Remote peers are identified by {@link String}s; the interpretation of these {@link String}s is up to the implementation.
 * </p>
 *
 * <p>
 * Notifications are delivered to the specified {@link Handler}.
 * </p>
 */
public interface Network {

    /**
     * Start this instance.
     *
     * @param handler handler for notifications
     * @throws IllegalStateException if already started
     * @throws IOException if an error occurs
     */
    void start(Handler handler) throws IOException;

    /**
     * Stop this instance.
     *
     * <p>
     * Does nothing if already stopped.
     * </p>
     */
    void stop();

    /**
     * Send (or enqueue for sending) a message to a remote peer.
     *
     * <p>
     * If this method returns true, then {@link Handler#outputQueueEmpty Handler.outputQueueEmpty()}
     * is guaranteed to be invoked with parameter {@code peer} at some later point.
     *
     * @param peer message destination
     * @param msg message to send
     * @return true if message was succesfully enqueued for output; false if message failed to be delivered due to local reasons,
     *  such as failure to initiate a new connection or output queue overflow
     * @throws IllegalArgumentException if {@code peer} cannot be interpreted
     * @throws IllegalArgumentException if {@code peer} or {@code msg} is null
     */
    boolean send(String peer, ByteBuffer msg);

// Handler

    /**
     * Callback interface used by {@link Network} implementations.
     */
    public interface Handler {

        /**
         * Handle an incoming message from a remote peer.
         *
         * <p>
         * The {@code msg} buffer is read-only; its contents are not guaranteed to be valid after this method returns.
         *
         * @param peer message source
         * @param msg message received
         */
        void handle(String peer, ByteBuffer msg);

        /**
         * Receive notification that a remote peer's output queue has transitioned from a non empty state to an empty state
         *
         * @param peer remote peer
         */
        void outputQueueEmpty(String peer);
    }
}

