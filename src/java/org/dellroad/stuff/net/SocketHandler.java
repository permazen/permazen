
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
     * This instance should exit from {@link #handleConnection} as soon as possible.
     * One way to implement this method is to simply close the socket.
     *
     * <p>
     * It may be that {@link handleConnection} has already returned, in which case this
     * method should do nothing.
     *
     * <p>
     * In any case, after this method returns, the socket will in fact be closed (if not already).
     * So doing nothing is often sufficient.
     *
     * @param socket connection socket
     */
    void stop(Socket socket);
}

