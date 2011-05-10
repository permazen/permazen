
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.ByteArrayInputStream;

import org.dellroad.stuff.TestSupport;
import org.testng.annotations.Test;

public class AsyncInputStreamTest extends TestSupport implements AsyncInputStream.Listener {

    private byte[] buf;
    private int pos;
    private int wantFail;
    private boolean gotFail;
    private boolean gotEOF;

    @Test
    public synchronized void testAsyncInputStream() {

        // Set up test
        this.buf = new byte[5000 + (int)this.random.nextDouble() * 20000];
        this.wantFail = 10000 + (int)this.random.nextDouble() * 20000;
        this.random.nextBytes(this.buf);

        // Create input and wait for all data to be read (or exception thrown)
        AsyncInputStream ais = new AsyncInputStream(new ByteArrayInputStream(this.buf), this.getClass().getName(), this);
        while (!this.gotEOF && !this.gotFail) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException("interrupted", e);
            }
        }

        // Verify what we got
        if (this.gotEOF) {
            assert !this.gotFail;
            assert this.pos == this.buf.length;
        } else
            assert this.pos == this.wantFail;
    }

    @Override
    public synchronized void handleInput(byte[] data, int off, int len) {
        for (int i = 0; i < len; i++) {
            if (this.pos == this.wantFail)
                throw new RuntimeException();
            assert this.pos < this.buf.length : "read too much data";
            assert data[off + i] == this.buf[this.pos] : "data mismatch at offset " + this.pos;
            this.pos++;
        }
    }

    @Override
    public synchronized void handleException(Throwable e) {
        this.gotFail = true;
        this.notify();
    }

    @Override
    public synchronized void handleEOF() {
        this.gotEOF = true;
        this.notify();
    }
}

