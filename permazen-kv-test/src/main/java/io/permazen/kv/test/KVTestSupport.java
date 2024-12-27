
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.test;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.util.XMLSerializer;
import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.ConvertedNavigableMap;
import io.permazen.util.ConvertedNavigableSet;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
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
    protected RuntimeException showKV(KVStore kv, String label, ByteData minKey, ByteData maxKey) {
        final String xml;
        try {
            xml = this.toXmlString(kv, minKey, maxKey);
        } catch (RuntimeException e) {
            this.log.info("{} - oops, got {}", label, e.toString());
            if (this.log.isTraceEnabled())
                this.log.trace("{} exception trace", label, e);
            return e;
        }
        this.log.info("{}\n{}", label, xml);
        return null;
    }

    protected String toXmlString(KVStore kv) {
        return this.toXmlString(kv, null, null);
    }

    protected String toXmlString(KVStore kv, ByteData minKey, ByteData maxKey) {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            final XMLStreamWriter writer = new IndentXMLStreamWriter(
              XMLOutputFactory.newInstance().createXMLStreamWriter(buf, "UTF-8"));
            new XMLSerializer(kv).write(writer, minKey, maxKey);
            return new String(buf.toByteArray(), StandardCharsets.UTF_8).replaceAll("^<\\?xml [^>]+>\\s+", "").trim();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static String s(ByteData data) {
        return ByteUtil.toString(data);
    }

    protected static ByteData b(String string) {
        return ByteData.fromHex(string);
    }

    protected static String s(KVPair pair) {
        return pair != null ? ("[" + s(pair.getKey()) + ", " + s(pair.getValue()) + "]") : "null";
    }

    protected static KVPair kv(String... key) {
        if (key.length != 1 && key.length != 2)
            throw new IllegalArgumentException();
        return new KVPair(b(key[0]), key.length > 1 ? b(key[1]) : ByteData.empty());
    }

    public static NavigableMap<String, String> stringView(NavigableMap<ByteData, ByteData> byteMap) {
        if (byteMap == null)
            return null;
        return new ConvertedNavigableMap<String, String, ByteData, ByteData>(byteMap,
          ByteUtil.STRING_CONVERTER.reverse(), ByteUtil.STRING_CONVERTER.reverse());
    }

    public static NavigableSet<String> stringView(NavigableSet<ByteData> byteSet) {
        if (byteSet == null)
            return null;
        return new ConvertedNavigableSet<String, ByteData>(byteSet, ByteUtil.STRING_CONVERTER.reverse());
    }

    protected void tryNtimes(KVDatabase kvdb, Consumer<KVTransaction> consumer) {
        this.<Void>tryNtimesWithResult(kvdb, kvt -> {
            consumer.accept(kvt);
            return null;
        });
    }

    protected <R> R tryNtimesWithResult(KVDatabase kvdb, Function<KVTransaction, R> function) {
        RetryKVTransactionException retry = null;
        for (int count = 0; count < this.getNumTries(); count++) {
            try {
                this.numTransactionAttempts.incrementAndGet();
                final KVTransaction tx = this.doCreateTransaction(kvdb);
                final R result = function.apply(tx);
                tx.commit();
                return result;
            } catch (RetryKVTransactionException e) {
                this.updateRetryStats(e);
                this.log.debug("attempt #{} yeilded {}", count + 1, e.toString());
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
        RetryKVTransactionException retry = null;
        for (int count = 0; count < this.getNumTries(); count++) {
            try {
                return this.doCreateTransaction(kvdb);
            } catch (RetryKVTransactionException e) {
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

    protected KVTransaction doCreateTransaction(KVDatabase kvdb) {
        return kvdb.createTransaction();
    }

    protected void updateRetryStats(RetryKVTransactionException e) {
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
        return message
          .replaceAll("[0-9]+", "NNN")
          .replaceAll("(read/(write|remove|adjust) conflict at )[\\p{Graph}]+", "$1...");
    }
}
