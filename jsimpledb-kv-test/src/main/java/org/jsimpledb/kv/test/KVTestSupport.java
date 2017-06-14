
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.NavigableMap;
import java.util.NavigableSet;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.util.XMLSerializer;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.ConvertedNavigableSet;

/**
 * Base class for key/value unit tests.
 */
public abstract class KVTestSupport extends TestSupport {

    /**
     * Dump KV contents to the log.
     *
     * @param kv k/v store
     * @param label descriptive label
     * @return exception thrown during query, or null if successful
     */
    protected Exception showKV(KVStore kv, String label) {
        return this.showKV(kv, label, null, null);
    }

    /**
     * Dump KV portion to the log.
     *
     * @param kv k/v store
     * @param label descriptive label
     * @param minKey minimum key
     * @param maxKey maximum key
     * @return exception thrown during query, or null if successful
     */
    protected Exception showKV(KVStore kv, String label, byte[] minKey, byte[] maxKey) {
        final String xml;
        try {
            xml = this.toXmlString(kv, minKey, maxKey);
        } catch (Exception e) {
            this.log.info("{} - oops, got " + e, label);
            if (this.log.isTraceEnabled())
                this.log.trace(label + " exception trace:", e);
            return e;
        }
        this.log.info("{}\n{}", label, xml);
        return null;
    }

    protected String toXmlString(KVStore kv) {
        return this.toXmlString(kv, null, null);
    }

    protected String toXmlString(KVStore kv, byte[] minKey, byte[] maxKey) {
        try (final ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            final XMLStreamWriter writer = new IndentXMLStreamWriter(
              XMLOutputFactory.newInstance().createXMLStreamWriter(buf, "UTF-8"));
            new XMLSerializer(kv).write(writer, minKey, maxKey);
            return new String(buf.toByteArray(), Charset.forName("UTF-8")).replaceAll("^<\\?xml [^>]+>\\s+", "").trim();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    public static NavigableMap<String, String> stringView(NavigableMap<byte[], byte[]> byteMap) {
        if (byteMap == null)
            return null;
        return new ConvertedNavigableMap<String, String, byte[], byte[]>(byteMap,
          ByteUtil.STRING_CONVERTER.reverse(), ByteUtil.STRING_CONVERTER.reverse());
    }

    public static NavigableSet<String> stringView(NavigableSet<byte[]> byteSet) {
        if (byteSet == null)
            return null;
        return new ConvertedNavigableSet<String, byte[]>(byteSet, ByteUtil.STRING_CONVERTER.reverse());
    }
}
