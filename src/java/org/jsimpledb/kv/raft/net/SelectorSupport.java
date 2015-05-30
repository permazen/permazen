
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.net;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for classes that perform NIO selection operations.
 */
public abstract class SelectorSupport {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Create a new {@link SelectionKey} by registering the given channel on the given {@link Selector}
     * and associating the specified {@link IOHandler} to handle I/O ready conditions.
     *
     * <p>
     * Initially, no I/O operations will be selected. Use {@link #selectFor selectFor()} to add/remove them.
     *
     * @param selector NIO selector
     * @param channel I/O channel
     * @param handler I/O handler
     * @return new selection key
     * @throws IllegalArgumentException if any parameter is null
     * @throws ClosedChannelException if {@code channel} is closed
     */
    protected SelectionKey createSelectionKey(Selector selector, SelectableChannel channel, IOHandler handler)
      throws ClosedChannelException {

        // Sanity check
        Preconditions.checkArgument(selector != null, "null selector");
        Preconditions.checkArgument(channel != null, "null channel");
        Preconditions.checkArgument(handler != null, "null handler");

        // Register channel with our selector
        return channel.register(selector, 0, handler);
    }

    /**
     * Enable or disable listening for the specified I/O operation(s).
     *
     * @param selectionKey selection key; may be null, in which case this method does nothing
     * @param ops I/O operations
     * @param enabled true to enable, false to disable
     */
    protected void selectFor(SelectionKey selectionKey, int ops, boolean enabled) {
        if (selectionKey == null)
            return;
        final int currentOps = selectionKey.interestOps();
        selectionKey.interestOps(enabled ? currentOps | ops : currentOps & ~ops);
    }

    /**
     * Handle any ready I/O operations available from the given {@link Selector}.
     *
     * <p>
     * This assumes an {@link IOHandler} is assocated with each {@link SelectionKey} (e.g., they were created
     * via {@link #createSelectionKey createSelectionKey()}).
     *
     * @param selector NIO selector
     */
    protected void handleIO(Selector selector) {
        for (Iterator<SelectionKey> i = selector.selectedKeys().iterator(); i.hasNext(); ) {
            final SelectionKey key = i.next();
            i.remove();
            final IOHandler handler = (IOHandler)key.attachment();
            if (this.log.isTraceEnabled())
                this.log.trace("I/O ready: key=" + dbg(key) + " handler=" + handler);
            try {
                handler.serviceIO(key);
            } catch (IOException e) {
                if (this.log.isDebugEnabled())
                    this.log.debug("I/O error from " + handler, e);
                handler.close(e);
            } catch (Throwable t) {
                this.log.error("service error from " + handler, t);
                handler.close(t);
            }
        }
    }

    /**
     * Get a debug description of the given keys.
     *
     * @param keys selection keys
     * @return debug description
     */
    protected static String dbg(Iterable<? extends SelectionKey> keys) {
        final ArrayList<String> strings = new ArrayList<>();
        for (SelectionKey key : keys)
            strings.add(dbg(key));
        return strings.toString();
    }

    /**
     * Get a debug description of the given key.
     *
     * @param key selection key
     * @return debug description
     */
    protected static String dbg(SelectionKey key) {
        try {
            return "Key[interest=" + dbgOps(key.interestOps()) + ",ready="
              + dbgOps(key.readyOps()) + ",obj=" + key.attachment() + "]";
        } catch (java.nio.channels.CancelledKeyException e) {
            return "Key[canceled]";
        }
    }

    /**
     * Get a debug description of the given I/O ops.
     *
     * @param ops I/O operation bits
     * @return debug description
     */
    protected static String dbgOps(int ops) {
        final StringBuilder buf = new StringBuilder(4);
        if ((ops & SelectionKey.OP_ACCEPT) != 0)
            buf.append("A");
        if ((ops & SelectionKey.OP_CONNECT) != 0)
            buf.append("C");
        if ((ops & SelectionKey.OP_READ) != 0)
            buf.append("R");
        if ((ops & SelectionKey.OP_WRITE) != 0)
            buf.append("W");
        return buf.toString();
    }

// IOHandler

    /**
     * Callback interface used by {@link SelectorSupport}.
     */
    public interface IOHandler {

        /**
         * Handle ready I/O.
         *
         * @param key selection key
         * @throws IOException if an I/O error occurs; this will result in this instance being {@link #close close()}'ed
         */
        void serviceIO(SelectionKey key) throws IOException;

        /**
         * Notification that an exception was thrown by {@link #serviceIO serviceIO()}.
         * Typically this method will close the associated channel, which implicitly unregisters
         * the associated {@link SelectionKey}s.
         *
         * <p>
         * This method must be idempotent.
         *
         * @param cause the error that occurred
         */
        void close(Throwable cause);
    }
}

