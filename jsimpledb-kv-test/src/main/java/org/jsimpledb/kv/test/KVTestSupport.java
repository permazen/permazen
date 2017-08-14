
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.util.XMLSerializer;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.ConvertedNavigableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Base class for key/value unit tests.
 */
public abstract class KVTestSupport extends TestSupport {

    protected final AtomicInteger numTransactionAttempts = new AtomicInteger();
    protected final AtomicInteger numTransactionRetries = new AtomicInteger();

    private final TreeMap<String, AtomicInteger> retryReasons = new TreeMap<>();

    @BeforeClass
    public void setupTransactionAttemptCounters() throws Exception {
        this.numTransactionAttempts.set(0);
        this.numTransactionRetries.set(0);
    }

    @AfterClass
    public void teardownTransactionAttemptCounters() throws Exception {
        final double retryRate = (double)this.numTransactionRetries.get() / (double)this.numTransactionAttempts.get();
        this.log.info("\n\n****************\n");
        this.log.info(String.format("Retry rate: %.2f%% (%d / %d)",
          retryRate * 100.0, this.numTransactionRetries.get(), this.numTransactionAttempts.get()));
        this.log.info("Retry reasons:");
        for (Map.Entry<String, AtomicInteger> entry : this.retryReasons.entrySet())
            this.log.info(String.format("%10d %s", entry.getValue().get(), entry.getKey()));
        this.log.info("\n\n****************\n");
    }

    /**
     * Dump KV contents to the log.
     *
     * @param kv k/v store
     * @param label descriptive label
     * @return exception thrown during query, or null if successful
     */
    protected RuntimeException showKV(KVStore kv, String label) {
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
    protected RuntimeException showKV(KVStore kv, String label, byte[] minKey, byte[] maxKey) {
        final String xml;
        try {
            xml = this.toXmlString(kv, minKey, maxKey);
        } catch (RuntimeException e) {
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

    protected void tryNtimes(KVDatabase kvdb, Consumer<KVTransaction> consumer) {
        this.<Void>tryNtimesWithResult(kvdb, kvt -> {
            consumer.accept(kvt);
            return null;
        });
    }

    protected <R> R tryNtimesWithResult(KVDatabase kvdb, Function<KVTransaction, R> function) {
        RetryTransactionException retry = null;
        for (int count = 0; count < this.getNumTries(); count++) {
            try {
                this.numTransactionAttempts.incrementAndGet();
                final KVTransaction tx = kvdb.createTransaction();
                final R result = function.apply(tx);
                tx.commit();
                return result;
            } catch (RetryTransactionException e) {
                this.updateRetryStats(e);
                this.log.debug("attempt #" + (count + 1) + " yeilded " + e);
                retry = e;
            }
            try {
                Thread.sleep(100 + count * 200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw retry;
    }

    protected int getNumTries() {
        return 3;
    }

    // Some k/v databases can throw a RetryTransaction from createTransaction()
    protected KVTransaction createKVTransaction(KVDatabase kvdb) {
        RetryTransactionException retry = null;
        for (int count = 0; count < this.getNumTries(); count++) {
            try {
                return kvdb.createTransaction();
            } catch (RetryTransactionException e) {
                retry = e;
            }
            try {
                Thread.sleep(100 + count * 200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw retry;
    }

    protected void updateRetryStats(RetryTransactionException e) {
        this.numTransactionRetries.incrementAndGet();
        String message = e.getMessage();
        if (message != null)
            message = this.mapRetryExceptionMessage(message);
        synchronized (this) {
            AtomicInteger counter = this.retryReasons.get(message);
            if (counter == null) {
                counter = new AtomicInteger();
                this.retryReasons.put(message, counter);
            }
            counter.incrementAndGet();
        }
    }

    protected String mapRetryExceptionMessage(String message) {
        return message.replaceAll("[0-9]+", "NNN");
    }
}
