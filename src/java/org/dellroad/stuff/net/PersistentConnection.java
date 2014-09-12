
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.net;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for clients that want to maintain a persistent connection to some server.
 *
 * <p>
 * This class is suitable for use with any abstract notion of "client", "server", and "connection". Typically
 * it would be used to maintain a persistent connection to a remote server over the network. This class
 * mainly serves to implement the connection state machine, including an exponential back-off retry timer,
 * subclass notifications for state transitions, and guaranteed thread safety.
 * </p>
 *
 * <p>
 * Each instance has a dedicated thread that manages the connection and performs any required work while connected.
 * The lifecycle of a connection is delimited by calls (by this thread) to the subclass methods
 * {@link #createConnection createConnection()} and {@link #cleanupConnection cleanupConnection()}.
 * The actual per-connection work is performed by {@link #handleConnection handleConnection()}.
 * Each connection has an associated connection context (defined by the subclass) and passed to these methods.
 * </p>
 *
 * <p>
 * The subclass is also notified of state machine transitions via the state transition methods
 * {@link #started started()}, {@link #stopped stopped()}, {@link #connectionSuccessful connectionSuccessful()},
 * {@link #connectionFailed connectionFailed()}, and {@link #connectionEnded connectionEnded()};
 * all of these methods are invoked by the background thread.
 * </p>
 *
 * @param <C> connection context type
 */
public abstract class PersistentConnection<C> {

    /**
     * Default minimum restart delay.
     *
     * @see #setMinRestartDelay
     */
    public static final long DEFAULT_MIN_RESTART_DELAY = 10 * 1000L;

    /**
     * Default maximum restart delay.
     *
     * @see #setMaxRestartDelay
     */
    public static final long DEFAULT_MAX_RESTART_DELAY = 10 * 60 * 1000L;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private long minRestartDelay = DEFAULT_MIN_RESTART_DELAY;
    private long maxRestartDelay = DEFAULT_MAX_RESTART_DELAY;

    private volatile ClientThread thread;

// Properties

    /**
     * Set the minimum restart delay after being disconnected from the server.
     * Default is {@link #DEFAULT_MIN_RESTART_DELAY} ({@value #DEFAULT_MIN_RESTART_DELAY}}ms).
     *
     * @param minRestartDelay minimum restart delay in milliseconds
     */
    public void setMinRestartDelay(long minRestartDelay) {
        this.minRestartDelay = minRestartDelay;
    }
    public long getMinRestartDelay() {
        return this.minRestartDelay;
    }

    /**
     * Set the maximum restart delay after being disconnected from the server.
     * Default is {@link #DEFAULT_MAX_RESTART_DELAY} ({@value #DEFAULT_MAX_RESTART_DELAY}}ms).
     *
     * @param maxRestartDelay maximum restart delay in milliseconds
     */
    public void setMaxRestartDelay(long maxRestartDelay) {
        this.maxRestartDelay = maxRestartDelay;
    }
    public long getMaxRestartDelay() {
        return this.maxRestartDelay;
    }

    /**
     * Get the name to use for the network client thread.
     *
     * <p>
     * The implementation in {@link PersistentConnection} returns {@code this.toString() + " thread"}.
     * </p>
     */
    protected String getThreadName() {
        return this + " thread";
    }

// Lifecycle

    /**
     * Start this instance. This starts the background thread, which initiates the first connection attempt.
     *
     * <p>
     * If this instance is already started, nothing happens.
     * </p>
     */
    @PostConstruct
    public synchronized void start() {
        if (this.thread != null)
            return;
        this.thread = new ClientThread();
        this.thread.start();
    }

    /**
     * Stop this client.
     *
     * <p>
     * This method simply {@linkplain Thread#interrupt interrupts} the background thread, if any.
     * </p>
     */
    @PreDestroy
    public synchronized void stop() {
        if (this.thread != null) {
            this.log.info("stopping " + this + " thread " + this.thread);
            this.thread.interrupt();
            this.thread = null;
        }
    }

    /**
     * Determine if this instance is started.
     */
    public final synchronized boolean isRunning() {
        return this.thread != null;
    }

// Main loop

    private void poll() {

        // Loop reconnecting
        for (long restartDelay = this.minRestartDelay; this.thread == Thread.currentThread(); ) {

            // Do one connection cycle
            if (this.doConnection())
                restartDelay = this.minRestartDelay;

            // Check whether we should exit
            if (this.thread != Thread.currentThread()) {
                this.log.info(this + " thread exiting");
                break;
            }

            // Pause a while before trying to connect again
            try {
                this.log.info(this + " pausing " + restartDelay + "ms before next connection attempt");
                Thread.sleep(restartDelay);
            } catch (InterruptedException e) {
                this.log.info(this + " thread exiting due to interrupt");
                break;
            }

            // Increase restart delay for next time
            restartDelay = Math.max(this.minRestartDelay, Math.min(restartDelay * 2, this.maxRestartDelay));
        }
    }

    // Perform one connection cycle
    private boolean doConnection() {

        // Get this ClientThread object
        final PersistentConnection<?>.ClientThread thisClientThread = (PersistentConnection<?>.ClientThread)Thread.currentThread();

        // Create new connection to server
        try {
            thisClientThread.createConnection();
        } catch (Exception e) {
            this.connectionFailed(e);
            return false;
        }
        this.connectionSuccessful();
        try {

            // Check whether we should exit
            if (this.thread != Thread.currentThread())
                return true;

            // Handle connection
            Exception exception = null;
            try {
                thisClientThread.handleConnection();
            } catch (Exception e) {
                exception = e;
            }
            this.connectionEnded(exception);

            // Done
            return true;
        } finally {

            // Clean up connection
            thisClientThread.cleanupConnection();
        }
    }

// Connection Callbacks

    /**
     * Create a new server connection.
     *
     * <p>
     * If this method throws an unchecked exception, {@link #stopped stopped()} will be invoked with the exception
     * and this instance will be automatically stopped.
     * </p>
     *
     * @throws InterruptedException if interrupted
     * @throws IOException if there is a problem establishing the connection
     * @return context for the connection, which will be supplied to {@link #handleConnection handleConnection()}
     * and {@link #cleanupConnection cleanupConnection()}
     */
    protected abstract C createConnection() throws InterruptedException, IOException;

    /**
     * Handle one server connection.
     *
     * <p>
     * This method may either throw an exception or return normally; the only difference is whether
     * {@link #connectionEnded connectionEnded()} is invoked with a non-null parameter or not.
     * </p>
     *
     * <p>
     * Ideally this method should never return normally. However, in practice there are legitimate reasons to do so,
     * for example, if there is an application-level error that indicates the particular connection is no longer usable.
     * </p>
     *
     * <p>
     * If this method throws an unchecked exception, {@link #stopped stopped()} will be invoked with the exception
     * and this instance will be automatically stopped.
     * </p>
     *
     * @param connectionContext connection context returned from {@link #createConnection} when this connection was created
     * @throws InterruptedException if interrupted
     * @throws IOException if there is a problem during the connection or the connection fails
     */
    protected abstract void handleConnection(C connectionContext) throws InterruptedException, IOException;

    /**
     * Perform cleanup after a server connection ends. This method should close any open sockets, etc.
     *
     * <p>
     * For each successful invocation of {@link #createConnection} there is guaranteed be exactly
     * one invocation of this method.
     * </p>
     *
     * <p>
     * The {@code exception} parameter indicates either a normal return (if null) or thrown exception (if not null) from
     * {@link #handleConnection handleConnection()}. In any case, this instance will automatically begin attempting to reconnect
     * when this method returns.
     * </p>
     *
     * <p>
     * If this method throws an unchecked exception, {@link #stopped stopped()} will be invoked with the exception
     * and this instance will be automatically stopped.
     * </p>
     *
     * <p>
     * The implementation in {@link PersistentConnection} does nothing. Subclasses should override if necessary.
     * </p>
     *
     * @param connectionContext connection context returned from {@link #createConnection} when this connection was created
     */
    protected void cleanupConnection(C connectionContext) {
    }

// State Machine Transitions

    /**
     * State machine transition: this instance was started via {@link #start start()}.
     *
     * <p>
     * The implementation in {@link PersistentConnection} does nothing; subclasses may override.
     * </p>
     */
    protected void started() {
    }

    /**
     * State machine transition: this instance was stopped via {@link #stop stop()} of because or
     * an unchecked exception was thrown by one of the subclass methods.
     *
     * <p>
     * The implementation in {@link PersistentConnection} does nothing; subclasses may override.
     * </p>
     *
     * @param t unexpected exception, or null if this instance was stopped via {@link #stop stop()}.
     */
    protected void stopped(Throwable t) {
    }

    /**
     * State machine transition: initialization of a new connection via {@link #createConnection} was successful
     * and we are connected to the server.
     *
     * <p>
     * The implementation in {@link PersistentConnection} does nothing; subclasses may override.
     * </p>
     */
    protected void connectionSuccessful() {
    }

    /**
     * State machine transition: initialization of a new connection via {@link #createConnection} failed due to an exception.
     * This instance will automatically attempt to retry.
     *
     * <p>
     * The implementation in {@link PersistentConnection} does nothing; subclasses may override.
     * </p>
     *
     * @param e Exception thrown by {@link #createConnection}.
     */
    protected void connectionFailed(Exception e) {
    }

    /**
     * State machine transition: an established connection to the server has ended. This indicates either normal
     * return or a thrown an exception from {@link #handleConnection handleConnection()}.
     * This instance will automatically attempt to reconnect.
     *
     * <p>
     * The implementation in {@link PersistentConnection} does nothing; subclasses may override.
     * </p>
     *
     * @param e Exception thrown by {@link #handleConnection handleConnection()},
     *  or null if {@link #handleConnection handleConnection()} returned normally
     */
    protected void connectionEnded(Exception e) {
    }

// ClientThread

    private class ClientThread extends Thread {

        private volatile C context;

        ClientThread() {
            super(PersistentConnection.this.getThreadName());
        }

        @Override
        public void run() {
            Throwable exception = null;
            try {
                PersistentConnection.this.log.info(PersistentConnection.this + " thread starting");
                PersistentConnection.this.started();
                PersistentConnection.this.poll();
            } catch (Throwable t) {
                exception = t;
                if (t instanceof Error)
                    throw (Error)t;
                if (t instanceof RuntimeException)
                    throw (RuntimeException)t;
                throw new RuntimeException(t);
            } finally {
                synchronized (PersistentConnection.this) {
                    if (PersistentConnection.this.thread == this)
                        PersistentConnection.this.thread = null;
                }
                PersistentConnection.this.stopped(exception);
            }
        }

        void createConnection() throws Exception {
            this.context = PersistentConnection.this.createConnection();
        }

        void handleConnection() throws Exception {
            PersistentConnection.this.handleConnection(this.context);
        }

        void cleanupConnection() {
            PersistentConnection.this.cleanupConnection(this.context);
        }
    }
}

