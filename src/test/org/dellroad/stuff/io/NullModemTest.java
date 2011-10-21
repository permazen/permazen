
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.dellroad.stuff.TestSupport;
import org.testng.annotations.Test;

public class NullModemTest extends TestSupport implements NullModemOutputStream.ReadCallback, NullModemInputStream.WriteCallback {

    private byte[] inputData;
    private byte[] outputData;

// NullModemOutputStream

    @Test
    public synchronized void testOutput() throws Exception {

        // Create data
        byte[] data = new byte[this.random.nextInt(1000)];
        this.random.nextBytes(data);

        // Write data
        NullModemOutputStream output = new NullModemOutputStream(this, "Null Output");
        DataOutputStream dataOutput = new DataOutputStream(output);
        dataOutput.write(data);
        dataOutput.close();

        // Wait for reader to finish reading
        synchronized (this) {
            while (this.outputData == null)
                this.wait();
        }

        // Check
        assert Arrays.equals(this.outputData, data);
    }

    @Override
    public void readFrom(InputStream input) throws IOException {
        this.outputData = StreamsTest.readAll(input);
        synchronized (this) {
            this.notifyAll();
        }
    }

// NullModemInputStream

    @Test
    public synchronized void testInput() throws IOException {

        // Create data
        this.inputData = new byte[this.random.nextInt(1000)];
        this.random.nextBytes(this.inputData);

        // Read data
        NullModemInputStream input = new NullModemInputStream(this, "Null Input");
        DataInputStream dataInput = new DataInputStream(input);
        byte[] data = StreamsTest.readAll(dataInput);
        dataInput.close();

        // Check
        assert Arrays.equals(data, this.inputData);
    }

    @Override
    public void writeTo(OutputStream output) throws IOException {
        DataOutputStream dataOutput = new DataOutputStream(output);
        dataOutput.write(this.inputData);
        dataOutput.flush();
    }
}

