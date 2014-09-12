
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.net;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * Maintains a persistent {@link Socket} connection to a remote server, automatically reconnecting when the connection fails.
 *
 * <p>
 * Subclasses need only override {@link #handleConnection handleConnection()}.
 * </p>
 */
public abstract class PersistentSocketConnection extends PersistentConnection<Socket> {

    private SocketAddress serverAddress;

// Properties

    /**
     * Get the address to which this instance should connect.
     */
    public SocketAddress getServerAddress() {
        return this.serverAddress;
    }

    /**
     * Configure the address to which this instance should connect.
     *
     * <p>
     * Required property.
     * </p>
     */
    public void setServerAddress(SocketAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

// Lifecycle

    @Override
    public void start() {
        if (this.serverAddress == null)
            throw new IllegalArgumentException("no serverAddress configured");
        super.start();
    }

    /**
     * Create a new server connection.
     *
     * <p>
     * The implementation in {@link PersistentSocketConnection} creates a socket via {@link #createSocket}
     * and connects the socket to the configured server.
     * </p>
     */
    @Override
    protected Socket createConnection() throws InterruptedException, IOException {
        final Socket socket = this.createSocket();
        socket.connect(this.serverAddress);
        return socket;
    }

    /**
     * Perform cleanup after a server connection ends.
     *
     * <p>
     * The implementation in {@link PersistentSocketConnection} ensures the socket is closed.
     * </p>
     *
     * @param socket socket returned from {@link #createConnection} when this connection was created
     */
    @Override
    protected void cleanupConnection(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * Create the {@link Socket} to use for a new connection. The socket should <b>not</b> be connected yet.
     *
     * <p>
     * The implementation in {@link PersistentSocketConnection} creates a normal {@link Socket} and sets the keep-alive flag
     * to ensure connection failures are detected promptly. Subclasses may override to configure the socket differently.
     * </p>
     */
    protected Socket createSocket() throws IOException {
        final Socket socket = new Socket();
        socket.setKeepAlive(true);
        return socket;
    }
}

