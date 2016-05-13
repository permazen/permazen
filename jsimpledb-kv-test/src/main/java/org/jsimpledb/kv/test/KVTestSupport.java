
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.util.XMLSerializer;
import org.jsimpledb.test.TestSupport;

/**
 * Base class for key/value unit tests.
 */
public abstract class KVTestSupport extends TestSupport {

    /**
     * Dump KV contents to the log.
     *
     * @param tx transaction
     * @param label descriptive label
     * @return exception thrown during query, or null if successful
     */
    protected Exception showKV(KVTransaction tx, String label) {
        return this.showKV(tx, label, null, null);
    }

    /**
     * Dump KV portion to the log.
     *
     * @param tx transaction
     * @param label descriptive label
     * @param minKey minimum key
     * @param maxKey maximum key
     * @return exception thrown during query, or null if successful
     */
    protected Exception showKV(KVTransaction tx, String label, byte[] minKey, byte[] maxKey) {
        try {
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            final XMLStreamWriter writer = new IndentXMLStreamWriter(
              XMLOutputFactory.newInstance().createXMLStreamWriter(buf, "UTF-8"));
            new XMLSerializer(tx).write(writer, minKey, maxKey);
            this.log.info("{}\n{}", label, new String(buf.toByteArray(), Charset.forName("UTF-8")));
            return null;
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            this.log.info("{} - oops, got " + e, label);
            if (this.log.isTraceEnabled())
                this.log.trace(label + " exception trace:", e);
            return e;
        }
    }

    protected String s(KVPair pair) {
        return pair != null ? ("[" + s(pair.getKey()) + ", " + s(pair.getValue()) + "]") : "null";
    }

    protected static KVPair kv(String... key) {
        if (key.length != 1 && key.length != 2)
            throw new IllegalArgumentException();
        return new KVPair(b(key[0]), key.length > 1 ? b(key[1]) : new byte[0]);
    }
}
