
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.test;

import com.google.common.collect.Sets;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.dellroad.stuff.java.ProcessRunner;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
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
    private static long defaultRandomSeed = System.currentTimeMillis();

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected Validator validator;
    protected Random random;

    @BeforeClass
    public void setUpValidator() {
        final ValidatorFactory factory = Validation.byDefaultProvider()
          .configure()
          .messageInterpolator(new ParameterMessageInterpolator())
          .buildValidatorFactory();
        this.validator = factory.getValidator();
    }

    @BeforeClass
    @Parameters("randomSeed")
    public void seedRandom(String randomSeed) {
        this.random = TestSupport.getRandom(randomSeed);
    }

    protected File createTempDirectory() throws IOException {
        File file = File.createTempFile(this.getClass().getName(), "");
        if (!file.delete())
            throw new IOException(String.format("error deleting \"%s\"", file));
        if (!file.mkdir())
            throw new IOException(String.format("error creating directory \"%s\"", file));
        return file;
    }

    protected void deleteDirectoryHierarchy(File root) throws IOException {
        Files.walkFileTree(root.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static Random getRandom(String randomSeed) {
        long seed;
        try {
            seed = Long.parseLong(randomSeed);
        } catch (NumberFormatException e) {
            seed = TestSupport.defaultRandomSeed;
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
     *
     * @param file file to read
     * @return file content
     */
    protected String readResource(File file) {
        try {
            return this.readResource(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw new RuntimeException(String.format("can't URL'ify file: %s", file));
        }
    }

    /**
     * Read some classpath resource in as a UTF-8 encoded string.
     *
     * @param path classpath resource
     * @return file content
     */
    protected String readResource(String path) {
        final URL url = getClass().getResource(path);
        if (url == null)
            throw new RuntimeException(String.format("can't find resource \"%s\"", path));
        return this.readResource(url);
    }

    /**
     * Read some URL resource in as a UTF-8 encoded string.
     *
     * @param url resource URL
     * @return resource content
     */
    protected String readResource(URL url) {
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(url.openStream(), StandardCharsets.UTF_8);
            final StringWriter writer = new StringWriter();
            char[] buf = new char[1024];
            for (int r; (r = reader.read(buf)) != -1; )
                writer.write(buf, 0, r);
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(String.format("error reading from %s", url), e);
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
        TestSupport.checkSet(expected, actual, true);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void checkSet(Set actual, Set expected, boolean recurse) {

        // Check set equals
        Assert.assertEquals(actual, expected);

        // Check size() and isEmpty()
        Assert.assertEquals(actual.size(), expected.size());
        Assert.assertEquals(actual.isEmpty(), expected.isEmpty());

        // Check hashCode()
        Assert.assertEquals(actual.hashCode(), expected.hashCode());

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
        TestSupport.checkMap(expected, actual, true);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void checkMap(Map actual, Map expected, boolean recurse) {

        // Check map equals
        Assert.assertEquals(actual, expected);

        // Check size() and isEmpty()
        Assert.assertEquals(actual.size(), expected.size());
        Assert.assertEquals(actual.isEmpty(), expected.isEmpty());

        // Check hashCode()
        Assert.assertEquals(actual.hashCode(), expected.hashCode());

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

    // Workaround https://github.com/cbeust/testng/issues/1734
    public static void assertEquals(float[] a1, float[] a2, String message) {
        try {
            Assert.assertEquals(a1, a2, message);
        } catch (AssertionError e) {
            if (a1 != null && a2 != null && Arrays.equals(a1, a2))
                return;
            throw e;
        }
    }

    // Workaround https://github.com/cbeust/testng/issues/1734
    public static void assertEquals(double[] a1, double[] a2, String message) {
        try {
            Assert.assertEquals(a1, a2, message);
        } catch (AssertionError e) {
            if (a1 != null && a2 != null && Arrays.equals(a1, a2))
                return;
            throw e;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Set buildSet(Object... items) {
        return Stream.of(items).collect(Collectors.toSet());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static SortedSet buildSortedSet(Object... items) {
        return Stream.of(items).collect(Collectors.toCollection(TreeSet::new));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static List buildList(Object... items) {
        return Stream.of(items).collect(Collectors.toList());
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

    protected String show(Throwable t) {
        final StringWriter buf = new StringWriter();
        final PrintWriter pw = new PrintWriter(buf);
        t.printStackTrace(pw);
        pw.flush();
        return buf.toString();
    }

    protected void assertSameOrDiff(String expected, String actual) {
        final String diff = this.diff(expected, actual);
        if (diff != null)
            throw new AssertionError("differences in strings found:\n" + diff);
    }

    /**
     * Run {@code diff(1)} on two strings.
     *
     * @param s1 first string
     * @param s2 second string
     * @return the diff, or null if strings are the same
     */
    protected String diff(String s1, String s2) {
        if (s1.equals(s2))
            return null;
        File[] files = new File[2];
        try {

            // Write strings to temp files
            for (int i = 0; i < files.length; i++) {
                files[i] = File.createTempFile("diff.", ".txt");
                try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(files[i]))) {
                    output.write((i == 0 ? s1 : s2).getBytes(StandardCharsets.UTF_8));
                }
            }

            // Run diff(1) command
            final ProcessRunner runner = new ProcessRunner(Runtime.getRuntime().exec(new String[] {
              "diff", "-U", "5", "--strip-trailing-cr", "--text", "--expand-tabs", files[0].toString(), files[1].toString()
            }));
            switch (runner.run()) {
            case 0:
            case 1:
                break;
            default:
                throw new RuntimeException(String.format(
                  "diff(1) error: %s", new String(runner.getStandardError(), StandardCharsets.UTF_8)));
            }

            // Skip first two lines (filenames)
            String diff = new String(runner.getStandardOutput(), StandardCharsets.UTF_8);
            for (int i = 0; i < 2; i++)
                diff = diff.substring(diff.indexOf('\n') + 1);

            // Done
            return diff;
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException("diff(1) error", e);
        } finally {
            for (File file : files) {               // delete temp files
                if (file != null)
                    file.delete();
            }
        }
    }
}
