
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.net;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Network} implementation based on TCP sockets.
 *
 * <p>
 * Remote peers have the {@link String} form <code>IP-Address[:port]</code>. If the port is omitted,
 * the default port provided to the constructor is assumed.
 * </p>
 */
public class TCPNetwork extends SelectorSupport implements Network {

    /**
     * Default maximum number of simultaneous connections ({@value #DEFAULT_MAX_CONNECTIONS}).
     *
     * @see #getMaxConnections
     */
    public static final int DEFAULT_MAX_CONNECTIONS = 1000;

    /**
     * Default idle connection timeout ({@value #DEFAULT_MAX_IDLE_TIME} milliseconds).
     *
     * @see #getMaxIdleTime
     */
    public static final long DEFAULT_MAX_IDLE_TIME = 30 * 1000L;                // 30 sec

    /**
     * Default connect timeout for outgoing connections ({@value #DEFAULT_CONNECT_TIMEOUT} milliseconds).
     *
     * @see #getConnectTimeout
     */
    public static final long DEFAULT_CONNECT_TIMEOUT = 20 * 1000L;              // 20 sec

    /**
     * Default maximum allowed size of an incoming message ({@value #DEFAULT_MAX_MESSAGE_SIZE} bytes).
     *
     * @see #getMaxMessageSize
     */
    public static final int DEFAULT_MAX_MESSAGE_SIZE = 32 * 1024 * 1024;        // 32 MB

    /**
     * Default maximum allowed size of a connection's outgoing queue before we start dropping messages
     * ({@value #DEFAULT_MAX_OUTPUT_QUEUE_SIZE} bytes).
     *
     * @see #getMaxOutputQueueSize
     */
    public static final long DEFAULT_MAX_OUTPUT_QUEUE_SIZE = 64 * 1024 * 1024;   // 64 MB

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final HashMap<String, Connection> connectionMap = new HashMap<>();
    private final int defaultPort;

    private InetSocketAddress listenAddress;
    private int maxConnections = DEFAULT_MAX_CONNECTIONS;
    private long maxIdleTime = DEFAULT_MAX_IDLE_TIME;
    private long connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
    private long maxOutputQueueSize = DEFAULT_MAX_OUTPUT_QUEUE_SIZE;

    private Network.Handler handler;
    private ServerSocketChannel serverSocketChannel;
    private SelectionKey selectionKey;
    private ExecutorService executor;

// Constructors

    /**
     * Constructor.
     *
     * @param defaultPort default TCP port when no port is explicitly proviced
     * @throws IllegalArgumentException if {@code port} is invalid
     */
    public TCPNetwork(int defaultPort) {
        Preconditions.checkArgument(defaultPort > 0 && defaultPort < 65536, "invalid default port " + defaultPort);
        this.defaultPort = defaultPort;
        this.listenAddress = new InetSocketAddress(this.defaultPort);
    }

// Public API

    /**
     * Get the {@link InetSocketAddress} to which this instance is bound or will bind.
     *
     * @return listen address, possibly null for default behavior
     */
    public synchronized InetSocketAddress getListenAddress() {
        return this.listenAddress;
    }

    /**
     * Configure the {@link InetSocketAddress} to which this instance should bind.
     *
     * <p>
     * If this instance is already started, invoking this method will have no effect until it is
     * {@linkplain #stop stopped} and restarted.
     * </p>
     *
     * <p>
     * By default, instances listen on all interfaces on the defaul port configured in the constructor.
     * </p>
     *
     * @param address listen address, or null to listen on all interfaces on the default port provided to the constructor
     * @throws IllegalArgumentException if {@code address} is null
     */
    public synchronized void setListenAddress(InetSocketAddress address) {
        Preconditions.checkArgument(address != null, "null address");
        this.listenAddress = address;
    }

    /**
     * Get the maximum number of allowed connections. Default is {@value #DEFAULT_MAX_CONNECTIONS}.
     *
     * @return max allowed connections
     */
    public synchronized int getMaxConnections() {
        return this.maxConnections;
    }
    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * Get the maximum idle time for connections before automatically closing them down.
     * Default is {@value #DEFAULT_MAX_IDLE_TIME}ms.
     *
     * @return max connection idle time in milliseconds
     */
    public synchronized long getMaxIdleTime() {
        return this.maxIdleTime;
    }
    public synchronized void setMaxIdleTime(long maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    /**
     * Get the outgoing connection timeout. Default is {@value #DEFAULT_CONNECT_TIMEOUT}ms.
     *
     * @return outgoing connection timeout in milliseconds
     */
    public synchronized long getConnectTimeout() {
        return this.connectTimeout;
    }
    public synchronized void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Get the maximum allowed length for incoming messages. Default is {@value #DEFAULT_MAX_MESSAGE_SIZE} bytes.
     *
     * @return max allowed incoming message length in bytes
     */
    public synchronized int getMaxMessageSize() {
        return this.maxMessageSize;
    }
    public synchronized void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    /**
     * Get the maximum allowed size of the queue for outgoing messages.
     * Default is {@value #DEFAULT_MAX_OUTPUT_QUEUE_SIZE} bytes.
     *
     * @return max allowed outgoing message queue length in bytes
     */
    public synchronized long getMaxOutputQueueSize() {
        return this.maxOutputQueueSize;
    }
    public synchronized void setMaxOutputQueueSize(long maxOutputQueueSize) {
        this.maxOutputQueueSize = maxOutputQueueSize;
    }

// Lifecycle

    @Override
    public synchronized void start(Handler handler) throws IOException {
        super.start();
        boolean successful = false;
        try {
            if (this.handler != null)
                return;
            if (this.log.isDebugEnabled())
                this.log.debug("starting " + this + " listening on " + this.listenAddress);
            this.serverSocketChannel = ServerSocketChannel.open();
            this.configureServerSocketChannel(this.serverSocketChannel);
            this.serverSocketChannel.bind(this.listenAddress);
            this.selectionKey = this.createSelectionKey(this.serverSocketChannel, new IOHandler() {
                @Override
                public void serviceIO(SelectionKey key) throws IOException {
                    if (key.isAcceptable())
                        TCPNetwork.this.handleAccept();
                }
                @Override
                public void close(Throwable cause) {
                    TCPNetwork.this.log.error("stopping " + this + " due to exception", cause);
                    TCPNetwork.this.stop();
                }
            });
            this.selectForAccept(true);
            this.executor = Executors.newSingleThreadExecutor();
            this.handler = handler;
            successful = true;
        } finally {
            if (!successful)
                this.stop();
        }
    }

    @Override
    public void stop() {
        super.stop();
        synchronized (this) {
            if (this.handler == null)
                return;
            if (this.log.isDebugEnabled())
                this.log.debug("stopping " + this);
            if (this.serverSocketChannel != null) {
                try {
                    this.serverSocketChannel.close();
                } catch (Exception e) {
                    // ignore
                }
                this.serverSocketChannel = null;
            }
            if (this.executor != null) {
                this.executor.shutdownNow();
                try {
                    this.executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                this.executor = null;
            }
            this.selectionKey = null;
            this.handler = null;
        }
    }

// Network

    @Override
    public synchronized boolean send(String peer, ByteBuffer msg) {

        // Sanity check
        Preconditions.checkArgument(peer != null, "null peer");

        // Get/create connection
        Connection connection = this.connectionMap.get(peer);
        if (connection == null) {

            // Create connection
            try {
                connection = this.createConnection(peer);
            } catch (IOException e) {
                this.log.info(this + " unable to send message to `" + peer + "': " + e.getMessage());
                return false;
            }

            // Record connection
            this.connectionMap.put(peer, connection);
        }

        // Send message
        return connection.output(msg);
    }

// Utility methods

    /**
     * Parse out the address part of an address that has an optional colon plus TCP port number suffix.
     *
     * @param address address of the form {@code ipaddr} or {@code ipaddr:port}
     * @return the IP address part
     */
    public static String parseAddressPart(String address) {
        return (String)TCPNetwork.parseAddress(address, 0)[0];
    }

    /**
     * Parse out the port part of an address that has an optional colon plus TCP port number suffix.
     *
     * @param address address of the form {@code ipaddr} or {@code ipaddr:port}
     * @param defaultPort default port if none specified in {@code address}
     * @return the port part, or {@code defaultPort} if there is no explicit port
     */
    public static int parsePortPart(String address, int defaultPort) {
        return (Integer)TCPNetwork.parseAddress(address, defaultPort)[1];
    }

    private static Object[] parseAddress(String string, int defaultPort) {
        final int colon = string.lastIndexOf(':');
        if (colon == -1)
            return new Object[] { string, defaultPort };
        try {
            final int port = Integer.parseInt(string.substring(colon + 1), 10);
            if (port < 1 || port > 65535)
                return new Object[] { string, defaultPort };
            return new Object[] { string.substring(0, colon), port };
        } catch (Exception e) {
            return new Object[] { string, defaultPort };
        }
    }

// Subclass methods

    /**
     * Configure the {@link ServerSocketChannel} to be used by this instance. This method is invoked by {@link #start}.
     *
     * <p>
     * The implementation in {@link TCPNetwork} does nothing. Subclasses may override to configure socket options, etc.
     * </p>
     *
     * @param serverSocketChannel channel to configure
     */
    protected void configureServerSocketChannel(ServerSocketChannel serverSocketChannel) {
    }

    /**
     * Configure a new {@link SocketChannel} to be used by this instance. This method is invoked when new connections
     * are created.
     *
     * <p>
     * The implementation in {@link TCPNetwork} does nothing. Subclasses may override to configure socket options, etc.
     * </p>
     *
     * @param socketChannel channel to configure
     */
    protected void configureSocketChannel(SocketChannel socketChannel) {
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[port=" + this.listenAddress.getPort() + "]";
    }

// I/O Ready Conditions

    // Invoked when we have a new incoming connection
    private void handleAccept() throws IOException {

        // Sanity check
        assert this.isServiceThread();

        // Check connection size limit
        if (this.connectionMap.size() >= this.maxConnections) {
            this.log.warn("too many network connections (" + this.connectionMap.size() + " >= "
              + this.maxConnections + "), not accepting any more (for now)");
            this.selectForAccept(false);
            return;
        }

        // Accept connection
        SocketChannel socketChannel = this.serverSocketChannel.accept();
        if (socketChannel == null)
            return;
        this.log.info("accepted incoming connection from " + socketChannel.getRemoteAddress());
        socketChannel
          .setOption(StandardSocketOptions.SO_KEEPALIVE, true)
          .setOption(StandardSocketOptions.TCP_NODELAY, true);
        this.configureSocketChannel(socketChannel);

        // Create peer
        final InetSocketAddress remote = (InetSocketAddress)socketChannel.socket().getRemoteSocketAddress();
        final String peer = remote.getHostString() + (remote.getPort() != this.defaultPort ? ":" + remote.getPort() : "");

        // Are we already connected to this peer? If so (deterministically) choose which connection wins
        Connection connection = this.connectionMap.get(peer);
        if (connection != null) {

            // Compare the socket addresses of the initiator side of each connection
            final SocketAddress oldAddr = (InetSocketAddress)connection.getSocketChannel().socket().getLocalSocketAddress();
            final SocketAddress newAddr = (InetSocketAddress)socketChannel.getRemoteAddress();
            final String oldDesc = oldAddr.toString().replaceAll("^[^/]*/", "");            // strip off hostname part, if any
            final String newDesc = newAddr.toString().replaceAll("^[^/]*/", "");            // strip off hostname part, if any
            final int diff = newDesc.compareTo(oldDesc);
            this.log.info("connection mid-air collision: old: " + oldDesc + ", new: " + newDesc
              + ", winner: " + (diff < 0 ? "new" : diff > 0 ? "old" : "neither (?)"));

            // Close the loser(s)
            if (diff >= 0) {
                this.log.info("rejecting incoming connection from " + socketChannel.getRemoteAddress() + " as duplicate");
                socketChannel.close();
                socketChannel = null;
            }
            if (diff <= 0) {
                this.log.info("closing existing duplicate connection to " + socketChannel.getRemoteAddress());
                this.connectionMap.remove(peer);
                connection.close(new IOException("duplicate connection"));
                connection = null;
            }
        }

        // Create connection from new socket if needed
        if (connection == null && socketChannel != null) {
            connection = new Connection(this, peer, socketChannel);
            this.connectionMap.put(peer, connection);
            this.handleOutputQueueEmpty(connection);
        }
    }

    // Enable/disable incoming connections
    private void selectForAccept(boolean enabled) throws IOException {
        if (this.selectionKey == null)
            return;
        if (enabled && (this.selectionKey.interestOps() & SelectionKey.OP_ACCEPT) == 0) {
            this.selectFor(this.selectionKey, SelectionKey.OP_ACCEPT, true);
            if (this.log.isDebugEnabled())
                this.log.debug(this + " started listening for incoming connections");
        } else if (!enabled && (this.selectionKey.interestOps() & SelectionKey.OP_ACCEPT) != 0) {
            this.selectFor(this.selectionKey, SelectionKey.OP_ACCEPT, false);
            if (this.log.isDebugEnabled())
                this.log.debug(this + " stopped listening for incoming connections");
        }
    }

// Connection API

    // Invoked when a message arrives on a connection
    void handleMessage(final Connection connection, final ByteBuffer msg) {
        assert Thread.holdsLock(this);
        assert this.isServiceThread();
        this.executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    TCPNetwork.this.handler.handle(connection.getPeer(), msg);
                } catch (Throwable t) {
                    TCPNetwork.this.log.error("exception in callback", t);
                }
            }
        });
    }

    // Invoked a connection's output queue goes empty
    void handleOutputQueueEmpty(final Connection connection) {
        assert Thread.holdsLock(this);
        assert this.isServiceThread();
        this.executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    TCPNetwork.this.handler.outputQueueEmpty(connection.getPeer());
                } catch (Throwable t) {
                    TCPNetwork.this.log.error("exception in callback", t);
                }
            }
        });
    }

    // Invoked when a connection closes
    void handleConnectionClosed(Connection connection) {
        assert Thread.holdsLock(this);
        assert this.isServiceThread();
        if (this.log.isDebugEnabled())
            this.log.debug(this + " handling closed connection " + connection);
        this.connectionMap.remove(connection.getPeer());
        this.handleOutputQueueEmpty(connection);
        this.wakeup();
    }

// Internal API

    // Create a new connection to the specified peer
    private synchronized Connection createConnection(String peer) throws IOException {

        // Create new one
        final SocketChannel socketChannel = SocketChannel.open()
          .setOption(StandardSocketOptions.SO_KEEPALIVE, true)
          .setOption(StandardSocketOptions.TCP_NODELAY, true);
        this.configureSocketChannel(socketChannel);
        socketChannel.configureBlocking(false);
        if (this.log.isDebugEnabled())
            this.log.debug(this + " looking up peer address `" + peer + "'");

        // Resolve peer name into a socket address
        InetSocketAddress socketAddress = null;
        try {
            socketAddress = new InetSocketAddress(
              TCPNetwork.parseAddressPart(peer), TCPNetwork.parsePortPart(peer, this.defaultPort));
        } catch (IllegalArgumentException e) {
            if (this.log.isTraceEnabled())
                this.log.trace(this + " peer address `" + peer + "' is invalid", e);
        }
        if (socketAddress == null || socketAddress.isUnresolved())
            throw new IOException("invalid or unresolvable peer address `" + peer + "'");

        // Initiate connection to peer
        if (this.log.isDebugEnabled()) {
            this.log.debug(this + ": resolved peer address `" + peer + "' to " + socketAddress.getAddress()
              + "; now initiating connection");
        }
        socketChannel.connect(socketAddress);

        // Create new connection
        return new Connection(this, peer, socketChannel);
    }

// SelectorSupport

    @Override
    protected void serviceHousekeeping() {

        // Perform connection housekeeping
        for (Connection connection : new ArrayList<Connection>(this.connectionMap.values())) {
            try {
                connection.performHousekeeping();
            } catch (IOException e) {
                if (this.log.isDebugEnabled())
                    this.log.debug("I/O error from " + connection, e);
                connection.close(e);
            } catch (Throwable t) {
                this.log.error("error performing housekeeping for " + connection, t);
                connection.close(t);
            }
        }

        // Perform my own housekeeping
        try {
            this.selectForAccept(this.connectionMap.size() < this.maxConnections);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    @Override
    protected void serviceCleanup() {
        for (Connection connection : new ArrayList<Connection>(this.connectionMap.values()))
            connection.close(null);
    }
}

