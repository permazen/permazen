
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft.net;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One TCP connection.
 *
 * <b>Locking</b>
 *
 * <p>
 * All access to this class must be with the associated {@link TCPNetwork} instance locked.
 */
class Connection extends SelectorSupport implements SelectorSupport.IOHandler {

    /**
     * Minimum buffer size to use a direct buffer.
     */
    private static final int MIN_DIRECT_BUFFER_SIZE = 128;

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final TCPNetwork network;
    private final String peer;
    private final SocketChannel socketChannel;
    private final SelectionKey selectionKey;
    private final ArrayDeque<ByteBuffer> output = new ArrayDeque<>();

    private ByteBuffer inbuf;
    private long queueSize;                                 // invariant: always equals the total number of bytes in 'output'
    private long lastActiveTime;
    private boolean readingLength;                          // indicates 'inbuf' is reading the message length (4 bytes)
    private boolean closed;

// Constructors

    public Connection(TCPNetwork network, Selector selector, String peer, SocketChannel socketChannel) throws IOException {

        // Sanity check
        if (network == null)
            throw new IllegalArgumentException("null network");
        if (selector == null)
            throw new IllegalArgumentException("null selector");
        if (socketChannel == null)
            throw new IllegalArgumentException("null socketChannel");
        if (peer == null)
            throw new IllegalArgumentException("null peer");

        // Initialize
        this.network = network;
        this.peer = peer;
        this.socketChannel = socketChannel;
        this.lastActiveTime = System.nanoTime();

        // Set up initial selection
        this.selectionKey = this.createSelectionKey(selector, this.socketChannel, this);
        if (this.socketChannel.isConnectionPending())
            this.selectFor(this.selectionKey, SelectionKey.OP_CONNECT, true);
        else
            this.selectFor(this.selectionKey, SelectionKey.OP_READ, true);
        this.network.wakeup();

        // Initialize input state
        this.inbuf = ByteBuffer.allocate(4);
        this.readingLength = true;
    }

    /**
     * Get remote peer's identity.
     */
    public String getPeer() {
        return this.peer;
    }

    /**
     * Get the associated {@link SocketChannel}.
     */
    public SocketChannel getSocketChannel() {
        return this.socketChannel;
    }

    /**
     * Get time in milliseconds since last activity.
     */
    public long getIdleTime() {
        return (System.nanoTime() - this.lastActiveTime) / 1000000L;
    }

    /**
     * Enqueue an outgoing message on this connection.
     *
     * @param buf outgoing data
     * @return true if message was enqueued, false if output buffer was full
     */
    public boolean output(ByteBuffer buf) {

        // Sanity check
        assert Thread.holdsLock(this.network);
        if (buf == null)
            throw new IllegalArgumentException("null buf");

        // Avoid anyone else mucking with my buffer position, etc.
        buf = buf.asReadOnlyBuffer();

        // Check output queue capacity
        final int length = buf.remaining();
        final int increment = length + 4;
        if (this.queueSize + increment > this.network.getMaxOutputQueueSize())
            return false;

        // Add to queue
        this.output.add((ByteBuffer)ByteBuffer.allocate(4).putInt(length).flip());
        this.output.add(buf);
        this.queueSize += increment;

        // Notify us when socket is writable (unless still waiting on connection)
        if (this.socketChannel.isConnected())
            this.selectFor(this.selectionKey, SelectionKey.OP_WRITE, true);
        this.network.wakeup();

        // Done
        this.lastActiveTime = System.nanoTime();
        return true;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[peer=" + this.peer + ",closed=" + this.closed + "]";
    }

// IOHandler

    @Override
    public void serviceIO(SelectionKey key) throws IOException {
        assert TCPNetwork.isServiceThread();
        assert Thread.holdsLock(this.network);
        if (key.isConnectable())
            this.handleConnectable();
        if (key.isReadable())
            this.handleReadable();
        if (key.isWritable())
            this.handleWritable();
    }

    @Override
    public void close(Throwable cause) {
        assert Thread.holdsLock(this.network);
        if (this.closed)
            return;
        this.closed = true;
        if (this.log.isDebugEnabled())
            this.log.debug("closing " + this + (cause != null ? " due to " + cause : ""));
        try {
            this.socketChannel.close();
        } catch (IOException e) {
            // ignore
        }
        this.network.handleConnectionClosed(this);
    }

// I/O Ready Conditions

    // Handle connection succeeded
    private void handleConnectable() throws IOException {

        // Leave connecting state
        this.selectFor(this.selectionKey, SelectionKey.OP_CONNECT, false);
        if (!this.socketChannel.finishConnect())                    // this should never occur
            throw new IOException("connection failed");
        if (this.log.isDebugEnabled())
            this.log.debug(this + ": connection succeeded");

        // Notify us when readable/writeable
        this.selectFor(this.selectionKey, SelectionKey.OP_READ, true);
        this.selectFor(this.selectionKey, SelectionKey.OP_WRITE, !this.output.isEmpty());
        this.network.wakeup();

        // Update timestamp
        this.lastActiveTime = System.nanoTime();

        // Notify client we are open for business
        this.network.handleOutputQueueEmpty(this);
    }

    private void handleReadable() throws IOException {
        while (true) {

            // Update timestamp
            this.lastActiveTime = System.nanoTime();

            // Read bytes
            final long len = this.socketChannel.read(this.inbuf);
            if (len == -1)
                throw new EOFException("connection closed");

            // Is the message (or length header) still incomplete?
            if (this.inbuf.hasRemaining())
                break;

            // Set up for reading
            this.inbuf.flip();

            // Completed length header?
            if (this.readingLength) {

                // Get and validate length
                assert this.inbuf.remaining() == 4;
                final int length = this.inbuf.getInt();
                if (length < 0 || length > this.network.getMaxMessageSize())
                    throw new IOException("rec'd message with bogus length " + length);

                // Set up for reading the actual message
                this.inbuf = length >= MIN_DIRECT_BUFFER_SIZE ? ByteBuffer.allocateDirect(length) : ByteBuffer.allocate(length);
                this.readingLength = false;
                continue;
            }

            // Deliver the completed message
            this.network.handleMessage(this, this.inbuf);

            // Set up for reading next length header
            this.inbuf = ByteBuffer.allocate(4);
            this.readingLength = true;
        }

        // Done
        this.lastActiveTime = System.nanoTime();
    }

    private void handleWritable() throws IOException {

        // Write more data, if present
        boolean queueBecameEmpty = false;
        if (!this.output.isEmpty()) {

            // Write data
            final long written = this.socketChannel.write(this.output.toArray(new ByteBuffer[this.output.size()]));
            this.queueSize -= written;

            // Clear away empty buffers
            while (!this.output.isEmpty() && !this.output.peekFirst().hasRemaining())
                this.output.removeFirst();

            // Set flag if queue became empty
            queueBecameEmpty = this.output.isEmpty();
        }

        // Notify when writeable - only if queue still not empty
        this.selectFor(this.selectionKey, SelectionKey.OP_WRITE, !this.output.isEmpty());
        // this.network.wakeup();                                       // not needed, we were already selecting for writes

        // Update timestamp
        this.lastActiveTime = System.nanoTime();

        // Notify client if queue became empty
        if (queueBecameEmpty)
            this.network.handleOutputQueueEmpty(this);
    }

// Housekeeping

    // Check timeouts
    void performHousekeeping() throws IOException {
        assert Thread.holdsLock(this.network);
        assert TCPNetwork.isServiceThread();
        if (this.socketChannel.isConnectionPending()) {
            if (this.getIdleTime() >= this.network.getConnectTimeout())
                throw new IOException("connection unsuccessful after " + this.getIdleTime() + "ms");
        } else {
            if (this.getIdleTime() >= this.network.getMaxIdleTime())
                throw new IOException("connection idle timeout after " + this.getIdleTime() + "ms");
        }
    }
}

