
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.dellroad.stuff.io.NullModemInputStream.WriteCallback;

/**
 * Handles external process I/O and async execution.
 *
 * <p>
 * This class makes it convenient to execute an external process and gather it's output without having to deal
 * directly with the inherent issues relating to threads, race conditions, and deadlocks.
 * </p>
 */
public class ProcessRunner {

    private final Process process;
    private final WriteCallback inputWriter;
    private final ByteArrayOutputStream stdoutBuffer = new ByteArrayOutputStream();
    private final ByteArrayOutputStream stderrBuffer = new ByteArrayOutputStream();

    private int state;
    private boolean discardStandardOutput;
    private boolean discardStandardError;

    /**
     * No-input constructor.
     *
     * <p>
     * Use this constructor when the process requires no input on {@code stdin}.
     * </p>
     *
     * @param process a newly-created process
     * @throws IllegalArgumentException if {@code process} is null
     */
    public ProcessRunner(Process process) {
        this(process, (WriteCallback)null);
    }

    /**
     * Fixed input constructor.
     *
     * <p>
     * Use this constructor when the process input can be expressed as a {@code byte[]} array.
     * </p>
     *
     * @param process newly-created process
     * @param input process input
     * @throws IllegalArgumentException if {@code process} is null
     * @throws IllegalArgumentException if {@code input} is null
     */
    public ProcessRunner(Process process, final byte[] input) {
        this(process, new WriteCallback() {
            @Override
            public void writeTo(OutputStream output) throws IOException {
                output.write(input);
            }
        });
    }

    /**
     * Dynamic input constructor.
     *
     * <p>
     * Use this constructor when the process input is computed in an online manner.
     * </p>
     *
     * @param process newly-created process
     * @param inputWriter object capable of writing input to the process, which will be invoked
     *  from a separate thread, or null if the process does not get any input
     * @throws IllegalArgumentException if {@code process} is null
     */
    public ProcessRunner(Process process, WriteCallback inputWriter) {
        if (process == null)
            throw new IllegalArgumentException("null process");
        this.process = process;
        this.inputWriter = inputWriter;
    }

    /**
     * Get the {@link Process} associated with this instance.
     */
    public Process getProcess() {
        return this.process;
    }

    /**
     * Set whether to standard output should be discarded. Default is false.
     * If configured to discard, then {@link #getStandardOutput} will throw {@link IllegalStateException}.
     *
     * @param discardStandardOutput true to discard standard output, otherwise false
     * @throws IllegalStateException if {@link #run} has already been invoked
     */
    public synchronized void setDiscardStandardOutput(boolean discardStandardOutput) {
        if (this.state != 0)
            throw new IllegalStateException("run() has already been invoked");
        this.discardStandardOutput = discardStandardOutput;
    }

    /**
     * Set whether to standard error should be discarded. Default is false.
     * If configured to discard, then {@link #getStandardError} will throw {@link IllegalStateException}.
     *
     * @param discardStandardError true to discard standard error, otherwise false
     * @throws IllegalStateException if {@link #run} has already been invoked
     */
    public synchronized void setDiscardStandardError(boolean discardStandardError) {
        if (this.state != 0)
            throw new IllegalStateException("run() has already been invoked");
        this.discardStandardError = discardStandardError;
    }

    /**
     * Send the process its standard input, read its standard output and standard error,
     * and wait for it to exit.
     *
     * <p>
     * If the current thread is interrupted, the standard input, output, and error connections to
     * the process are closed and an {@link InterruptedException} is thrown.
     * </p>
     *
     * @return exit value
     * @throws IllegalStateException if this method has already been invoked
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public int run() throws InterruptedException {

        // Update state
        synchronized (this) {
            if (this.state != 0)
                throw new IllegalStateException("process has already been run");
            this.state = 1;
        }

        // Create stdin thread
        final IOThread<OutputStream> stdin = new IOThread<OutputStream>("stdin",
          new BufferedOutputStream(this.process.getOutputStream())) {
            @Override
            protected void runIO() throws IOException {
                if (ProcessRunner.this.inputWriter != null)
                    ProcessRunner.this.inputWriter.writeTo(this.stream);
            }
        };

        // Create stdout thread
        final IOThread<InputStream> stdout = new IOThread<InputStream>("stdout", this.process.getInputStream()) {
            @Override
            protected void runIO() throws IOException {
                final byte[] buf = new byte[1000];
                int r;
                while ((r = this.stream.read(buf)) != -1)
                    ProcessRunner.this.handleStandardOutput(buf, 0, r);
            }
        };

        // Create stderr thread
        final IOThread<InputStream> stderr = new IOThread<InputStream>("stderr", this.process.getErrorStream()) {
            @Override
            protected void runIO() throws IOException {
                final byte[] buf = new byte[1000];
                int r;
                while ((r = this.stream.read(buf)) != -1)
                    ProcessRunner.this.handleStandardError(buf, 0, r);
            }
        };

        // Start threads
        stdin.start();
        stdout.start();
        stderr.start();

        // Wait for process to exit
        Integer exitValue = null;
        try {
            exitValue = this.process.waitFor();
        } finally {

            // Update state
            synchronized (this) {
                this.state = 2;
            }

            // In case of exception prior to process exit, close the sockets to wake up the threads
            if (exitValue == null) {
                stdin.close();
                stdout.close();
                stderr.close();
            }

            // Wait for threads to finish
            stdin.join();
            stdout.join();
            stderr.join();
        }

        // Done
        return exitValue;
    }

    /**
     * Handle data received from the standard output of the process.
     *
     * <p>
     * The implementation in {@link ProcessRunner} simply discards the data if this instance is configured to do so,
     * otherwise it adds the data to an in-memory buffer, which is made available when the process completes via
     * {@link #getStandardOutput}. Subclasses may override if necessary, e.g., to send/mirror the data elsewhere.
     * </p>
     *
     * <p>
     * This method will be invoked by a separate thread from the one that invoked {@link #run}.
     * </p>
     *
     * @param buf data buffer
     * @param off offset of the first data byte
     * @param len length of the data
     */
    protected void handleStandardOutput(byte[] buf, int off, int len) {
        if (this.discardStandardOutput)
            return;
        this.stdoutBuffer.write(buf, off, len);
    }

    /**
     * Handle data received from the error output of the process.
     *
     * <p>
     * The implementation in {@link ProcessRunner} simply discards the data if this instance is configured to do so,
     * otherwise it adds the data to an in-memory buffer, which is made available when the process completes via
     * {@link #getStandardError}. Subclasses may override if necessary, e.g., to send/mirror the data elsewhere.
     * </p>
     *
     * <p>
     * This method will be invoked by a separate thread from the one that invoked {@link #run}.
     * </p>
     *
     * @param buf data buffer
     * @param off offset of the first data byte
     * @param len length of the data in {@code buf}
     */
    protected void handleStandardError(byte[] buf, int off, int len) {
        if (this.discardStandardError)
            return;
        this.stderrBuffer.write(buf, off, len);
    }

    /**
     * Get the standard output of the process.
     *
     * @throws IllegalStateException if {@link #run} has not been invoked yet or is still executing
     * @throws IllegalStateException if this instance was configured to
     *  {@linkplain #setDiscardStandardOutput discard standard output}
     */
    public synchronized byte[] getStandardOutput() {

        // Check state
        if (this.state != 2)
            throw new IllegalStateException("run() has not been invoked yet");
        if (this.discardStandardOutput)
            throw new IllegalStateException("this instance was configured to discard stdout");

        // Return buffer
        return this.stdoutBuffer.toByteArray();
    }

    /**
     * Get the standard error of the process.
     *
     * @throws IllegalStateException if {@link #run} has not been invoked yet or is still executing
     * @throws IllegalStateException if this instance was configured to
     *  {@linkplain #setDiscardStandardError discard standard error}
     */
    public synchronized byte[] getStandardError() {

        // Check state
        if (this.state != 2)
            throw new IllegalStateException("run() has not been invoked yet");
        if (this.discardStandardError)
            throw new IllegalStateException("this instance was configured to discard stderr");

        // Return buffer
        return this.stderrBuffer.toByteArray();
    }

// Thread classes

    private abstract class IOThread<T extends Closeable> extends Thread {

        protected final T stream;

        public IOThread(String name, T stream) {
            super(name + " for " + ProcessRunner.this.process);
            this.stream = stream;
        }

        @Override
        public final void run() {
            try {
                this.runIO();
            } catch (IOException e) {
                // ignore
            } finally {
                this.close();
            }
        }

        protected void close() {
            try {
                this.stream.close();
            } catch (IOException e) {
                // ignore
            }
        }

        protected abstract void runIO() throws IOException;
    };
}

