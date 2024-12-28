
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import io.permazen.test.TestSupport;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.testng.annotations.Test;

public class ByteDataTest extends TestSupport {

    @Test
    public void testBasic() throws Exception {

        ByteData data1 = ByteData.empty();
        assert data1.isEmpty();
        assert data1.size() == 0;
        assert data1.equals(data1);
        assert data1.equals(data1.substring(0));
        assert data1.equals(data1.substring(0, 0));
        assert data1.toHex().equals("");
        assert data1.equals(ByteData.fromHex(""));
        assert Arrays.equals(data1.toByteArray(), new byte[0]);
        assert data1.compareTo(data1) == 0;

        ByteData data2 = ByteData.of((byte)0x01, (byte)0x99, (byte)0x01);
        assert data2.equals(ByteData.of((byte)0x01, (byte)0x99, (byte)0x01));
        assert data2.equals(ByteData.of(0x01, 0x99, 0x01));
        assert !data2.isEmpty();
        assert data2.size() == 3;
        assert data2.startsWith(data1);
        assert !data1.startsWith(data2);
        assert !data1.equals(data2);
        assert !data2.equals(data1);
        assert data2.equals(data2);
        assert data2.byteAt(0) == (byte)0x01;
        assert data2.byteAt(1) == (byte)0x99;
        assert data2.byteAt(2) == (byte)0x01;
        assert data2.ubyteAt(0) == 0x01;
        assert data2.ubyteAt(1) == 0x99;
        assert data2.ubyteAt(2) == 0x01;
        assert data2.substring(0, 0).equals(data1);
        assert !data2.substring(0, 1).equals(data2.substring(1, 2));
        assert data2.substring(0, 1).equals(data2.substring(2, 3));
        assert data2.startsWith(data2.substring(0, 1));
        assert data2.startsWith(data2.substring(2, 3));
        assert data2.endsWith(data2.substring(0, 1));
        assert data2.endsWith(data2.substring(2, 3));
        assert Arrays.equals(data2.toByteArray(), new byte[] { (byte)0x01, (byte)0x99, (byte)0x01 });
        assert data2.compareTo(data1) > 0;
        assert data1.compareTo(data2) < 0;
        assert data2.compareTo(data2) == 0;

        byte[] buf = new byte[5];
        data2.writeTo(buf, 1);
        assert Arrays.equals(buf, new byte[] { (byte)0x00, (byte)0x01, (byte)0x99, (byte)0x01, (byte)0x00 });

        assert data2.toHex().equals("019901");
        assert data2.equals(ByteData.fromHex("019901"));

        assert data2.toHex(99).equals("019901");
        assert data2.toHex(3).equals("019901");
        assert data2.toHex(2).equals("0199...");
        assert data2.toHex(1).equals("01...");
        assert data2.toHex(0).equals("...");

        assert data1.concat(data1).equals(data1);
        assert data2.concat(data1).equals(data2);
        assert data1.concat(data2).equals(data2);
        assert data2.concat(data2).equals(ByteData.of(0x01, 0x99, 0x01, 0x01, 0x99, 0x01));

        assert ByteData.numEqual(data1, 0, data2, 0) == 0;
        assert ByteData.numEqual(data2, 0, data1, 0) == 0;

        assert ByteData.numEqual(data2, 0, data2, 0) == 3;
        assert ByteData.numEqual(data2, 1, data2, 0) == 0;
        assert ByteData.numEqual(data2, 2, data2, 0) == 1;
        assert ByteData.numEqual(data2, 3, data2, 0) == 0;

        assert ByteData.numEqual(data2, 0, data2, 0) == 3;
        assert ByteData.numEqual(data2, 0, data2, 1) == 0;
        assert ByteData.numEqual(data2, 0, data2, 2) == 1;
        assert ByteData.numEqual(data2, 0, data2, 3) == 0;

        assert ByteData.numEqual(data2, 1, data2, 0) == 0;
        assert ByteData.numEqual(data2, 1, data2, 1) == 2;
        assert ByteData.numEqual(data2, 1, data2, 2) == 0;
        assert ByteData.numEqual(data2, 1, data2, 3) == 0;

        assert ByteData.numEqual(data2, 2, data2, 0) == 1;
        assert ByteData.numEqual(data2, 2, data2, 1) == 0;
        assert ByteData.numEqual(data2, 2, data2, 2) == 1;
        assert ByteData.numEqual(data2, 2, data2, 3) == 0;

        assert ByteData.numEqual(data2, 3, data2, 0) == 0;
        assert ByteData.numEqual(data2, 3, data2, 1) == 0;
        assert ByteData.numEqual(data2, 3, data2, 2) == 0;
        assert ByteData.numEqual(data2, 3, data2, 3) == 0;

        assert data2.newReader().peek() == 0x01;
        assert data2.substring(1).newReader().peek() == 0x99;
        assert data2.substring(2).newReader().peek() == 0x01;
        assert data2.substring(3).newReader().remain() == 0;
        assert data2.newReader().readRemaining().equals(data2);

        assert data2.stream().mapToObj(i -> i).collect(Collectors.toList())
          .equals(Arrays.asList(0x01, 0x99, 0x01));

        ByteData.Reader r = data2.newReader();
        assert r.getByteData().equals(data2);
        r.read();
        r.read();
        r.read();
        try {
            r.readByte();
            assert false : "expected exception";
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
        assert r.getByteData().equals(data2);

        ByteData.Writer w = ByteData.newWriter();
        w.write(0x01);
        w.write(new byte[] { (byte)0x99, (byte)0x01 });
        ByteData data3 = w.toByteData();
        assert data3.equals(data2);
        w.truncate(2);
        w.write(0x05);
        w.write(0x06);
        assert data3.equals(data2);
        ByteData data4 = w.toByteData();
        assert data4.equals(ByteData.of(0x01, 0x99, 0x05, 0x06));
        assert data3.equals(data2);

        final byte[] bbufArray = new byte[5];
        final ByteBuffer bbuf = ByteBuffer.wrap(bbufArray);
        data4.writeTo(bbuf);
        assert Arrays.equals(bbufArray, new byte[] { (byte)0x01, (byte)0x99, (byte)0x05, (byte)0x06, (byte)0x00 });

        // Writing to a malicious OutputStream should not affect the data
        data4.writeTo(new OutputStream() {
            @Override
            public void write(int b) {
            }
            @Override
            public void write(byte[] buf, int off, int len) {
                Arrays.fill(buf, (byte)0xff);
            }
        });
        assert data4.equals(ByteData.of(0x01, 0x99, 0x05, 0x06));

        assert ByteData.of(0xff, 0x01, 0x03).compareTo(ByteData.of(0xff, 0x01, 0x03)) == 0;
        assert ByteData.of(0xff, 0x01, 0x03).compareTo(ByteData.of(0xff, 0x01, 0x02)) > 0;
        assert ByteData.of(0xff, 0x01, 0x03).compareTo(ByteData.of(0xff, 0x01, 0x04)) < 0;
        assert ByteData.of(0xff, 0x01, 0x03).compareTo(ByteData.of(0xff, 0x01)) > 0;
        assert ByteData.of(0xff, 0x01, 0x03).compareTo(ByteData.of(0xff, 0x01, 0x03, 0x04)) < 0;

        ByteData data5 = ByteData.of(1, 2, 3, 4, 5, 6, 7);
        r = data5.newReader();
        assert r.getOffset() == 0 && r.remain() == 7;
        assert r.read() == 1;
        assert r.getOffset() == 1 && r.remain() == 6;
        r.read(new byte[3], 1, 2);
        assert r.getOffset() == 3 && r.remain() == 4;
        assert r.read() == 4;
        assert r.getOffset() == 4 && r.remain() == 3;
        assert r.readBytes(2).equals(ByteData.of(5, 6));
        assert r.getOffset() == 6 && r.remain() == 1;
        assert r.readRemaining().equals(ByteData.of(7));
        r.unread(5);
        assert r.readRemaining().equals(data5.substring(2));
        assert r.getByteData().equals(data5);
    }
}
