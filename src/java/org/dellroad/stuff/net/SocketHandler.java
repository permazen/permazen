
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.net;

import java.io.IOException;
import java.net.Socket;

/**
 * Implemented by objects that handle individual connections accepted by a {@link SocketAcceptor}.
 */
public interface SocketHandler {

    /**
     * Handle the connection.
     *
     * @param socket connection socket
     * @throws IOException if needed
     */
    void handleConnection(Socket socket) throws IOException;

    /**
     * Receive notification that the server is shutting down.
     * This method should ensure that the thread currently executing {@link #handleConnection handleConnection()}
     * returns as soon as possible.
     *
     * <p>
     * After this method returns, the {@code socket} will be closed (if not already). So one way to implement this method
     * is to simply do nothing, which will trigger an {@link IOException} on the next access from within
     * {@link #handleConnection handleConnection()}.
     *
     * <p>
     * Alternately, set some flag that {@link #handleConnection handleConnection()} detects each time around its processing loop,
     * or use {@link Thread#interrupt}, etc.
     *
     * <p>
     * In any case, it is important that {@link #handleConnection handleConnection()} thread does eventually return,
     * otherwise the thread invoking {@link SocketAcceptor#stop SocketAcceptor.stop()} will hang.
     *
     * <p>
     * Note: it may be that {@link #handleConnection handleConnection()} has already returned when this method is invoked,
     * in which case this method should do nothing.
     *
     * <p>
     *
     * @param thread the thread invoking {@link #handleConnection handleConnection()}
     * @param socket connection socket
     */
    void stop(Thread thread, Socket socket);
}

