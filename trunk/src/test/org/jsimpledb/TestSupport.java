
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.collect.Sets;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.util.XMLSerializer;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.XMLObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

/**
 * Base class for unit tests providing logging and random seed setup.
 */
public abstract class TestSupport {

    private static boolean reportedSeed;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected Validator validator;
    protected Random random;

    @BeforeClass
    public void setUpValidator() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        this.validator = factory.getValidator();
    }

    @BeforeClass
    @Parameters({ "randomSeed" })
    public void seedRandom(String randomSeed) {
        this.random = getRandom(randomSeed);
    }

    public static Random getRandom(String randomSeed) {
        long seed;
        try {
            seed = Long.parseLong(randomSeed);
        } catch (NumberFormatException e) {
            seed = System.currentTimeMillis();
        }
        if (!reportedSeed) {
            reportedSeed = true;
            LoggerFactory.getLogger(TestSupport.class).info("test seed = " + seed);
        }
        return new Random(seed);
    }

    protected <T> Set<ConstraintViolation<T>> checkValid(T object, boolean valid) {
        Set<ConstraintViolation<T>> violations = this.validator.validate(object);
        if (valid) {
            for (ConstraintViolation<T> violation : violations)
                log.error("unexpected validation error: [" + violation.getPropertyPath() + "]: " + violation.getMessage());
            Assert.assertTrue(violations.isEmpty(), "found constraint violations: " + violations);
        } else
            Assert.assertFalse(violations.isEmpty(), "expected constraint violations but none were found");
        return violations;
    }

    /**
     * Read some file in as a UTF-8 encoded string.
     */
    protected String readResource(File file) {
        try {
            return this.readResource(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw new RuntimeException("can't URL'ify file: " + file);
        }
    }

    /**
     * Read some classpath resource in as a UTF-8 encoded string.
     */
    protected String readResource(String path) {
        final URL url = getClass().getResource(path);
        if (url == null)
            throw new RuntimeException("can't find resource `" + path + "'");
        return this.readResource(url);
    }

    /**
     * Read some URL resource in as a UTF-8 encoded string.
     */
    protected String readResource(URL url) {
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(url.openStream(), "UTF-8");
            final StringWriter writer = new StringWriter();
            char[] buf = new char[1024];
            for (int r; (r = reader.read(buf)) != -1; )
                writer.write(buf, 0, r);
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException("error reading from " + url, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void checkSet(Set actual, Set expected) {
        TestSupport.checkSet(actual, expected, true);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void checkSet(Set actual, Set expected, boolean recurse) {

        // Check set equals
        Assert.assertEquals(actual, expected);

        // Check iterators
        Assert.assertEquals(Sets.newHashSet(actual.iterator()), expected);
        Assert.assertEquals(actual, Sets.newHashSet(expected.iterator()));

        // Check descending set
        if (recurse && actual instanceof NavigableSet)
            TestSupport.checkSet(((NavigableSet)actual).descendingSet(), expected, false);
        if (recurse && expected instanceof NavigableSet)
            TestSupport.checkSet(actual, ((NavigableSet)expected).descendingSet(), false);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void checkMap(Map actual, Map expected) {
        TestSupport.checkMap(actual, expected, true);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void checkMap(Map actual, Map expected, boolean recurse) {

        // Check key sets
        TestSupport.checkSet(actual.keySet(), expected.keySet(), true);
        if (actual instanceof NavigableMap) {
            TestSupport.checkSet(((NavigableMap)actual).descendingMap().navigableKeySet(), expected.keySet(), true);
            TestSupport.checkSet(((NavigableMap)actual).navigableKeySet().descendingSet(), expected.keySet(), true);
        }
        if (expected instanceof NavigableMap) {
            TestSupport.checkSet(actual.keySet(), ((NavigableMap)expected).descendingMap().navigableKeySet(), true);
            TestSupport.checkSet(actual.keySet(), ((NavigableMap)expected).navigableKeySet().descendingSet(), true);
        }

        // Check entry sets
        TestSupport.checkSet(actual.entrySet(), expected.entrySet(), true);

        // Check descending map
        if (recurse && actual instanceof NavigableMap)
            TestSupport.checkMap(((NavigableMap)actual).descendingMap(), expected, false);
        if (recurse && expected instanceof NavigableMap)
            TestSupport.checkMap(actual, ((NavigableMap)expected).descendingMap(), false);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Set buildSet(Object... items) {
        final HashSet set = new HashSet();
        for (int i = 0; i < items.length; i++)
            set.add(items[i]);
        return set;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static SortedSet buildSortedSet(Object... items) {
        final TreeSet set = new TreeSet();
        for (int i = 0; i < items.length; i++)
            set.add(items[i]);
        return set;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static List buildList(Object... items) {
        final ArrayList list = new ArrayList();
        for (int i = 0; i < items.length; i++)
            list.add(items[i]);
        return list;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Map buildMap(Object... kv) {
        final HashMap map = new HashMap();
        for (int i = 0; i < kv.length; i += 2)
            map.put(kv[i], kv[i + 1]);
        return map;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static SortedMap buildSortedMap(Object... kv) {
        final TreeMap map = new TreeMap();
        for (int i = 0; i < kv.length; i += 2)
            map.put(kv[i], kv[i + 1]);
        return map;
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
    protected void showKV(Transaction tx, String label, byte[] minKey, byte[] maxKey) {
        try {
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            final XMLStreamWriter writer = new IndentXMLStreamWriter(
              XMLOutputFactory.newInstance().createXMLStreamWriter(buf, "UTF-8"));
            new XMLSerializer(tx.getKVTransaction()).write(writer, minKey, maxKey);
            this.log.info("{}\n{}", label, new String(buf.toByteArray(), Charset.forName("UTF-8")));
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
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

    protected String show(Throwable t) {
        final StringWriter buf = new StringWriter();
        final PrintWriter pw = new PrintWriter(buf);
        t.printStackTrace(pw);
        pw.flush();
        return buf.toString();
    }

    protected KeyRange randomKeyRange() {
        final byte[] b1 = this.randomBytes(true);
        final byte[] b2 = this.randomBytes(true);
        return b1 == null || b2 == null || ByteUtil.compare(b1, b2) <= 0 ? new KeyRange(b1, b2) : new KeyRange(b2, b1);
    }

    protected byte[] randomBytes(boolean allowNull) {
        if (allowNull && this.random.nextFloat() < 0.1f)
            return null;
        final byte[] bytes = new byte[this.random.nextInt(6)];
        this.random.nextBytes(bytes);
        return bytes;
    }

    protected static KeyRange kr(String min, String max) {
        return new KeyRange(b(min), b(max));
    }

    protected static KeyRanges krs(KeyRange... ranges) {
        return new KeyRanges(Arrays.asList(ranges));
    }

    protected static byte[][] ba(String... sa) {
        final byte[][] ba = new byte[sa.length][];
        for (int i = 0; i < sa.length; i++)
            ba[i] = b(sa[i]);
        return ba;
    }

    protected static KVPair kv(String... key) {
        if (key.length != 1 && key.length != 2)
            throw new IllegalArgumentException();
        return new KVPair(b(key[0]), key.length > 1 ? b(key[1]) : ByteUtil.EMPTY);
    }

    protected static byte[] b(String s) {
        return s == null ? null : ByteUtil.parse(s);
    }

    protected String s(KVPair pair) {
        return pair != null ? ("[" + s(pair.getKey()) + ", " + s(pair.getValue()) + "]") : "null";
    }

    protected static String s(byte[] b) {
        return b == null ? "null" : ByteUtil.toString(b);
    }
}

