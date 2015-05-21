
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft.msg;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.mvcc.Reads;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.raft.Timestamp;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MessageTest extends TestSupport {

    @Test(dataProvider = "msgs")
    public void testMessage(Message msg1) {
        final ByteBuffer buf1 = msg1.encode();
        final Message msg2 = Message.decode(buf1.duplicate());
        final ByteBuffer buf2 = msg2.encode();
        Assert.assertEquals(buf1, buf2);
    }

    @DataProvider(name = "msgs")
    private Object[][] messages() throws Exception {

        final Writes writes = new Writes();
        writes.getPuts().put(b("0102"), b("0304"));
        writes.setRemoves(new KeyRanges(b("5555"), b("6666")));
        writes.getAdjusts().put(b("8888"), 99L);
        final ByteArrayOutputStream writesBuf = new ByteArrayOutputStream();
        writes.serialize(writesBuf);
        final byte[] writesData = writesBuf.toByteArray();

        final Reads reads = new Reads();
        reads.setReads(new KeyRanges(b("33"), b("44")));
        final ByteArrayOutputStream readsBuf = new ByteArrayOutputStream();
        reads.serialize(readsBuf);
        final byte[] readsData = readsBuf.toByteArray();

        return new Object[][] {
            { new AppendRequest("foobar", "dest\u0800", 123, new Timestamp(), new Timestamp(), 456, 1 << 50, 1 << 13) },
            { new AppendRequest("foobar", "Dest",
              123, new Timestamp(), new Timestamp(), Long.MAX_VALUE, 1 << 50, 1 << 13, 1 << 15, null) },
            { new AppendRequest("foobar", "Dest",
              123, new Timestamp(), new Timestamp(), Long.MAX_VALUE, 1 << 50, 1 << 13, 1 << 15, ByteBuffer.wrap(writesData)) },
            { new AppendResponse("foobar", "Dest", 99, new Timestamp(123), false, 123123, 4544253) },
            { new AppendResponse("foobar", "Dest", 99, new Timestamp(0x7fffffff), true, 34234, 4544253) },
            { new CommitRequest("@#$Q@$%@\u0000\uffff!", "Dest", 123123, 123123, 3343, 34343,
              ByteBuffer.wrap(readsData), ByteBuffer.wrap(writesData)) },
            { new CommitResponse("sender", "Dest", 4444, 555555, "commit failed") },
            { new CommitResponse("sender", "", 4444, 123, 45678, 459487463) },
            { new CommitResponse("sender", "\uffff", 4444, 123, 45678, 459487463, new Timestamp(12313423)) },
            { new GrantVote("blah", "namama", 4444) },
            { new InstallSnapshot("adlasdf", "\u1234haha", 234453, 234234, 34545, 4544, false, ByteBuffer.wrap(writesData)) },
            { new RequestVote("adlasdf", "blooby", 234453, 234234, 34545) },
        };
    }
}

