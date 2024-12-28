
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.Transaction;
import io.permazen.core.util.XMLObjectSerializer;
import io.permazen.kv.test.KVTestSupport;
import io.permazen.util.ByteData;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import javax.xml.stream.XMLStreamException;

/**
 * Base class for unit tests.
 */
public abstract class MainTestSupport extends KVTestSupport {

    /**
     * Dump KV contents to the log.
     */
    protected void showKV(PermazenTransaction ptx, String label) {
        this.showKV(ptx.getTransaction(), label);
    }

    /**
     * Dump KV contents to the log.
     */
    protected void showKV(Transaction tx, String label) {
        this.showKV(tx, label, null, null);
    }

    /**
     * Dump KV portion to the log.
     */
    protected void showKV(Transaction tx, String label, ByteData minKey, ByteData maxKey) {
        this.showKV(tx.getKVTransaction(), label, minKey, maxKey);
    }

    /**
     * Dump object contents to the log.
     */
    protected void showObjects(Transaction tx, String label) {
        final XMLObjectSerializer.OutputOptions options = XMLObjectSerializer.OutputOptions.builder()
          .includeStorageIds(true)
          .elementsAsNames(true)
          .build();
        try {
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            new XMLObjectSerializer(tx).write(buf, options);
            this.log.info("{}\n{}", label, new String(buf.toByteArray(), StandardCharsets.UTF_8));
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }
}
