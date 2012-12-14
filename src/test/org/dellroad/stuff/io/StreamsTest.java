
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.dellroad.stuff.TestSupport;
import org.testng.annotations.Test;

public class StreamsTest extends TestSupport implements NullModemOutputStream.ReadCallback {

    private byte[] data;

    @Test
    public synchronized void testStreams() throws Exception {

        // Create input datasets
        byte[][] dataIn = new byte[this.random.nextInt(7)][];
        for (int i = 0; i < dataIn.length; i++) {
            dataIn[i] = new byte[this.random.nextBoolean() ? this.random.nextInt(10000) : 0];
            this.random.nextBytes(dataIn[i]);
        }

        // Write datas
        NullModemOutputStream output = new NullModemOutputStream(this, "Null Output");
        OutputStreamWriter streamWriter = new OutputStreamWriter(output);
        for (int i = 0; i < dataIn.length; i++) {
            streamWriter.start();
            DataOutputStream dataOutput = new DataOutputStream(streamWriter);
            dataOutput.write(dataIn[i]);
            dataOutput.flush();
            if (i < dataIn.length - 1 || this.random.nextBoolean())
                streamWriter.stop();
        }
        streamWriter.close();

        // Wait for reader to finish reading
        synchronized (this) {
            while (this.data == null)
                this.wait();
        }

        // Read back datas
        byte[][] dataOut = new byte[dataIn.length][];
        InputStreamReader streamReader = new InputStreamReader(new ByteArrayInputStream(this.data));
        for (int i = 0; i < dataOut.length; i++) {
            InputStream is = streamReader.read();
            assert is != null;
            dataOut[i] = StreamsTest.readAll(is);
            if (this.random.nextBoolean())
                is.close();
        }
        streamReader.close();

        // Check
        for (int i = 0; i < dataIn.length; i++)
            assert Arrays.equals(dataIn[i], dataOut[i]);
    }

    @Override
    public void readFrom(InputStream input) throws IOException {
        this.data = StreamsTest.readAll(input);
        synchronized (this) {
            this.notifyAll();
        }
    }

    public static byte[] readAll(InputStream input) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] temp = new byte[7];
        int r;
        while ((r = input.read(temp)) != -1)
            buf.write(temp, 0, r);
        return buf.toByteArray();
    }
}

