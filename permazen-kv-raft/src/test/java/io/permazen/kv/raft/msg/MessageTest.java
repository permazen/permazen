
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.msg;

import io.permazen.kv.KeyRanges;
import io.permazen.kv.mvcc.Reads;
import io.permazen.kv.mvcc.Writes;
import io.permazen.kv.raft.Timestamp;
import io.permazen.kv.test.KVTestSupport;
import io.permazen.util.ByteData;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MessageTest extends KVTestSupport {

    @Test(dataProvider = "msgs")
    public void testMessage(Message msg1) {
        for (int version = 1; version <= Message.getCurrentProtocolVersion(); version++) {
            final ByteBuffer buf1 = msg1.encode(version);
            final ByteBuffer buf1b = buf1.duplicate();
            final int decodedVersion = Message.decodeProtocolVersion(buf1b);
            final Message msg2 = Message.decode(buf1b, decodedVersion);
            final ByteBuffer buf2 = msg2.encode(version);
            Assert.assertEquals(buf1, buf2, "bad version " + version + " encode/decode for:"
              + "\n  msg1=" + msg1
              + "\n  msg2=" + msg2
              + "\n  buf1=" + ByteData.of(Arrays.copyOfRange(buf1.array(), buf1.arrayOffset(), buf1.remaining())).toHex()
              + "\n  buf2=" + ByteData.of(Arrays.copyOfRange(buf2.array(), buf2.arrayOffset(), buf2.remaining())).toHex());
        }
    }

    @DataProvider(name = "msgs")
    private Object[][] messages() throws Exception {

        final Writes writes = new Writes();
        writes.getPuts().put(b("0102"), b("0304"));
        writes.getRemoves().add(new KeyRanges(b("5555"), b("6666")));
        writes.getAdjusts().put(b("8888"), 99L);
        final ByteArrayOutputStream writesBuf = new ByteArrayOutputStream();
        writes.serialize(writesBuf);
        final byte[] writesData = writesBuf.toByteArray();

        final Reads reads = new Reads(new KeyRanges(b("33"), b("44")));
        final ByteArrayOutputStream readsBuf = new ByteArrayOutputStream();
        reads.serialize(readsBuf);
        final byte[] readsData = readsBuf.toByteArray();

        final HashMap<String, String> config = new HashMap<>();
        config.put("foo", "bar");
        config.put("jam", "bluz");
        return new Object[][] {
            { new AppendRequest(123, "foobar", "dest\u0800", 123, new Timestamp(), new Timestamp(), 456, 1 << 50, 1 << 13) },
            { new AppendRequest(123, "foobar", "Dest",
              123, new Timestamp(), new Timestamp(), Long.MAX_VALUE, 1 << 50, 1 << 13, 1 << 15, null) },
            { new AppendRequest(123, "foobar", "Dest",
              123, new Timestamp(), new Timestamp(), Long.MAX_VALUE, 1 << 50, 1 << 13, 1 << 15, ByteBuffer.wrap(writesData)) },
            { new AppendResponse(123, "foobar", "Dest", 99, new Timestamp(123), false, 123123, 4544253) },
            { new AppendResponse(123, "foobar", "Dest", 99, new Timestamp(0x7fffffff), true, 34234, 4544253) },
            { new CommitRequest(123, "@#$Q@$%@\u0000\uffff!", "Dest", 123123, 123123, 3343, 34343,
              ByteBuffer.wrap(readsData), ByteBuffer.wrap(writesData)) },
            { new CommitResponse(123, "sender", "Dest", 4444, 555555, "commit failed") },
            { new CommitResponse(123, "sender", "", 4444, 123, 45678, 459487463) },
            { new CommitResponse(123, "sender", "\uffff", 4444, 123, 45678, 459487463, new Timestamp(12313423)) },
            { new GrantVote(123, "blah", "namama", 4444) },
            { new InstallSnapshot(123, "adlasdf", "\u1234haha", 234453, 234234, 34545, 0,
              config, false, ByteBuffer.wrap(writesData)) },
            { new InstallSnapshot(123, "adlasdf", "\u1234haha", 234453, 234234, 34545, 787,
              null, false, ByteBuffer.wrap(writesData)) },
            { new RequestVote(123, "adlasdf", "blooby", 234453, 234234, 34545) },
        };
    }
}
