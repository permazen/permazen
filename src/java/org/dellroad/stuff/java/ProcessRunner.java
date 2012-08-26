
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.dellroad.stuff.io.NullModemInputStream.WriteCallback;

/**
 * Makes it convenient to execute an external process and gather it's output without having to deal
 * directly with any threads or deadlock issues.
 */
public class ProcessRunner {

    private final Process process;
    private final WriteCallback inputWriter;
    private final ByteArrayOutputStream stdoutBuffer = new ByteArrayOutputStream();
    private final ByteArrayOutputStream stderrBuffer = new ByteArrayOutputStream();

    private int state;

    /**
     * No-input constructor.
     *
     * <p>
     * Use this constructor when the process requires no input on {@code stdin}.
     * </p>
     *
     * @param process newly-created process
     * @throws IllegalArgumentException if {@code process} is null
     */
    public ProcessRunner(Process process) {
        this(process, (WriteCallback)null);
    }

    /**
     * Fixed input constructor.
     *
     * <p>
     * Use this constructor when the process input can be expressed as a {@link byte[]}.
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
     * Send the process its standard input, read its standard output and standard error,
     * and wait for it to exit.
     *
     * @return exit value
     * @throws IllegalStateException if this method has already been invoked
     */
    public synchronized int run() throws InterruptedException {

        // Check state
        if (this.state != 0)
            throw new IllegalStateException("process has already been run");
        this.state = 1;

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
        final InputThread stdout = new InputThread("stdout",
          new BufferedInputStream(this.process.getInputStream()), this.stdoutBuffer);

        // Create stderr thread
        final InputThread stderr = new InputThread("stderr",
          new BufferedInputStream(this.process.getErrorStream()), this.stderrBuffer);

        // Run it
        try {

            // Start threads
            stdin.start();
            stdout.start();
            stderr.start();

            // Wait for process to exit
            return this.process.waitFor();
        } finally {

            // Update state
            this.state = 2;

            // Ensure sockets are cleaned up (in case of InterruptedException)
            stdin.close();
            stdout.close();
            stderr.close();

            // Wait for threads to finish
            stdin.join();
            stdout.join();
            stderr.join();
        }
    }

    /**
     * Get the standard output of the process.
     *
     * @throws IllegalStateException if {@link #run} has not been invoked yet or is still executing
     */
    public synchronized byte[] getStandardOutput() {

        // Check state
        if (this.state != 2)
            throw new IllegalStateException("run() has not been invoked yet");

        // Return buffer
        return this.stdoutBuffer.toByteArray();
    }

    /**
     * Get the standard error of the process.
     *
     * @throws IllegalStateException if {@link #run} has not been invoked yet or is still executing
     */
    public synchronized byte[] getStandardError() {

        // Check state
        if (this.state != 2)
            throw new IllegalStateException("run() has not been invoked yet");

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

    private class InputThread extends IOThread<InputStream> {

        private final ByteArrayOutputStream buffer;

        public InputThread(String name, InputStream stream, ByteArrayOutputStream buffer) {
            super(name, stream);
            this.buffer = buffer;
        }

        @Override
        protected void runIO() throws IOException {
            final byte[] buf = new byte[1000];
            int r;
            while ((r = this.stream.read(buf)) != -1)
                this.buffer.write(buf, 0, r);
        }
    }
}

