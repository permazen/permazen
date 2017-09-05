
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

import javax.xml.stream.XMLStreamException;

import io.permazen.core.util.XMLObjectSerializer;
import io.permazen.kv.test.KVTestSupport;

/**
 * Base class for core API unit tests.
 */
public abstract class CoreAPITestSupport extends KVTestSupport {

    /**
     * Dump KV contents to the log.
     */
    protected void showKV(Transaction tx, String label) {
        this.showKV(tx, label, null, null);
    }

    /**
     * Dump KV portion to the log.
     */
    protected void showKV(Transaction tx, String label, byte[] minKey, byte[] maxKey) {
        this.showKV(tx.getKVTransaction(), label, minKey, maxKey);
    }

    /**
     * Dump object contents to the log.
     */
    protected void showObjects(Transaction tx, String label) {
        try {
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            new XMLObjectSerializer(tx).write(buf, true, true);
            this.log.info("{}\n{}", label, new String(buf.toByteArray(), Charset.forName("UTF-8")));
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }
}
