
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.util.ArrayList;
import java.util.Arrays;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.util.ParseContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ExprParseTest extends TestSupport {

    private static final Object PARSE_FAIL = new Object() {
        @Override
        public String toString() {
            return "PARSE_FAIL";
        }
    };

    private static final Object EVAL_FAIL = new Object() {
        @Override
        public String toString() {
            return "EVAL_FAIL";
        }
    };

    private ParseSession session;

    @BeforeClass
    public void setup() throws Exception {
        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);
        this.session = new ParseSession(db);
        this.session.getImports().add(this.getClass().getName());
    }

    @Test(dataProvider = "cases")
    public void testExprParse(String expr, Object expected) {

        // Get top parse level
        ExprParser p = new ExprParser();

        // Parse
        final Node node;
        try {
            node = p.parse(this.session, new ParseContext(expr), false);
            assert expected != PARSE_FAIL : "expected parse failure for `" + expr + "' but parse succeeded with " + node;
        } catch (ParseException e) {
            if (expected != PARSE_FAIL)
                throw new AssertionError("expected " + expected + " for `" + expr + "' but parse failed", e);
            return;
        }

        // Evaluate
        final Object actual;
        try {
            actual = node.evaluate(this.session).get(this.session);
            assert expected != EVAL_FAIL : "expected failure evaluating `" + expr + "' but evaluation succeeded with " + actual;
        } catch (Exception e) {
            if (expected != EVAL_FAIL)
                throw new AssertionError("expected " + expected + " for `" + expr + "' but evaluation failed", e);
            return;
        }

        // Verify
        if (expected instanceof boolean[]) {
            Assert.assertTrue(actual instanceof boolean[]);
            Assert.assertEquals((boolean[])actual, (boolean[])expected);
        } else if (expected instanceof byte[]) {
            Assert.assertTrue(actual instanceof byte[]);
            Assert.assertEquals((byte[])actual, (byte[])expected);
        } else if (expected instanceof char[]) {
            Assert.assertTrue(actual instanceof char[]);
            Assert.assertEquals((char[])actual, (char[])expected);
        } else if (expected instanceof short[]) {
            Assert.assertTrue(actual instanceof short[]);
            Assert.assertEquals((short[])actual, (short[])expected);
        } else if (expected instanceof int[]) {
            Assert.assertTrue(actual instanceof int[]);
            Assert.assertEquals((int[])actual, (int[])expected);
        } else if (expected instanceof float[]) {
            Assert.assertTrue(actual instanceof float[]);
            Assert.assertEquals((float[])actual, (float[])expected);
        } else if (expected instanceof long[]) {
            Assert.assertTrue(actual instanceof long[]);
            Assert.assertEquals((long[])actual, (long[])expected);
        } else if (expected instanceof double[]) {
            Assert.assertTrue(actual instanceof double[]);
            Assert.assertEquals((double[])actual, (double[])expected);
        } else if (expected instanceof Object[]) {
            Assert.assertTrue(actual instanceof Object[]);
            Assert.assertTrue(Arrays.deepEquals((Object[])actual, (Object[])expected));
        } else
            Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "cases")
    @SuppressWarnings("rawtypes")
    public Object[][] genExprParseCases() {
        final ArrayList<Object[]> list = new ArrayList<>();
        list.addAll(Arrays.asList(new Object[][] {

            // Literals
            { "null", null },
            { "true", true },
            { "(true)", true },
            { "false", false },
            { "0", 0 },
            { "0L", 0L },
            { "123.45f", 123.45f },
            { "123.45", 123.45 },
            { "'a'", 'a' },
            { "'\\''", '\'' },
            { "'\u1234'", '\u1234' },
            { "\"line1\\nline2\\n\"", "line1\nline2\n" },
            { "\"\\\"quoted\\\"\"", "\"quoted\"" },
            { "@1111111111111111", new ObjId("1111111111111111") },
            { "Object.class", Object.class },

            // Literal fails
            { "'\''", PARSE_FAIL },
            { "\"foo", PARSE_FAIL },
            { "6fa", PARSE_FAIL },
            { "123.45fabc", PARSE_FAIL },

            // Idents and variables
            { "foobar", PARSE_FAIL },
            { "$foobar", EVAL_FAIL },
            { "$foobar = 12", 12 },
            { "$foobar", 12 },

            // Check assignment bug
            { "$a = $b = 100", 100 },
            { "$a", 100 },
            { "$b", 100 },
            { "$a = 32", 32 },
            { "$b = 21", 21 },
            { "$b -= $a = 3", 18 },
            { "$a", 3 },
            { "$b", 18 },

            // Multiplicative
            { "12 * 5", 60 },
            { "12 * 5 / 2", 30 },
            { "12 / 5 * 2", 4 },

            // Additive
            { "5 * 3 + 4", 19 },
            { "5 + 3 * 4", 17 },

            // Arrays
            { "new int[0]", new int[0] },
            { "new int[7]", new int[7] },
            { "new int[7][]", new int[7][] },
            { "new int[7][][][]", new int[7][][][] },
            { "new int[][] { { 1 }, { 2, 3 }, { } }", new int[][] { { 1 }, { 2, 3 }, { } } },
            { "new int[]", PARSE_FAIL },
            { "new int[] { }", new int[] { } },
            { "new int[] { \"abc\" }", EVAL_FAIL },
            { "new int[7].length", 7 },
            { "new void[]", PARSE_FAIL },
            { "new String[]", PARSE_FAIL },
            { "new String[3]", new String[3] },
            { "new String[] { \"foo\", \"bar\" }", new String[] { "foo", "bar" } },
            { "new java.util.Map[] { }", new java.util.Map[] { } },
            { "new java.util.Map.Entry[] { }", new java.util.Map.Entry[] { } },

            // Classes
            { "void.class", void.class },
            { "void[].class", PARSE_FAIL },
            { "Void.class", Void.class },
            { "Void[].class", Void[].class },
            { "int.class", int.class },
            { "int[].class", int[].class },
            { "int[][][].class", int[][][].class },
            { "Object[][][].class", Object[][][].class },
            { "int"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][].class",
              int
                 [][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]
                 [][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]
                 [][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]
                 [][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]
                 [][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]
                 [][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]
                 [][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]
                 [][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][].class },
            { "int"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
              + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][].class",
              PARSE_FAIL },

            // Methods
            { "\"abc\".length()", 3 },
            { "\"abc\".bytes", new byte[] { (byte)'a', (byte)'b', (byte)'c' } },

            // Fields
            { "new ExprParseTest.ClassWithFields().private_instance",
               new ExprParseTest.ClassWithFields().private_instance },
            { "new ExprParseTest.ClassWithFields().public_instance",
               new ExprParseTest.ClassWithFields().public_instance },
            { "ExprParseTest.ClassWithFields.private_static",
               ExprParseTest.ClassWithFields.private_static },
            { "ExprParseTest.ClassWithFields.public_static",
               ExprParseTest.ClassWithFields.public_static },
            { "ExprParseTest.ClassWithFields.public_instance", PARSE_FAIL },
            { "new ExprParseTest.ClassWithFields().public_static", PARSE_FAIL },

        //CHECKSTYLE OFF: SimplifyBooleanExpression

            // Operators
            { "$x = 12", 12 },
            { "$x += 12", 24 },
            { "$x -= 10", 14 },
            { "$x *= 2", 28 },
            { "$x /= 3", 9 },
            { "$x %= 4", 1 },
            { "$x &= 5", 1 },
            { "$x |= 6", 7 },
            { "$x ^= 15", 8 },
            { "$x <<= 2", 32 },
            { "$x >>= 1", 16 },
            { "$x >>>= 1", 8 },
            { "5 < 3 ? 17 : 19",
               5 < 3 ? 17 : 19 },
            { "true || false",
               true || false },
            { "true && false",
               true && false },
            { "true | false",
               true | false },
            { "true & false",
               true & false },
            { "true ^ false",
               true ^ false },
            { "!true",
               !true },
            { "!false",
               !false },
            { "5 instanceof Integer", true },
            { "5 instanceof java.lang.Integer", true },
            { "new java.util.HashMap() instanceof java.util.HashMap", true },
            { "1 + 2 / 3 instanceof java.lang.Integer", true },
            { "5 == 3",
               5 == 3 },
            { "5 != 3",
               5 != 3 },
            { "5 > 3",
               5 > 3 },
            { "5 >= 3",
               5 >= 3 },
            { "5 < 3",
               5 < 3 },
            { "5 <= 3",
               5 <= 3 },
            { "100 << 2",
               100 << 2 },
            { "100 >> 2",
               100 >> 2 },
            { "100 >>> 2",
               100 >>> 2 },
            { "-2147483648",
               -2147483648 },
            { "2147483647",
               2147483647 },
            { "-9223372036854775808L",
               -9223372036854775808L },
            { "9223372036854775807L",
               9223372036854775807L },
            { "0x7FFF0001 << 31",
               0x7FFF0001 << 31 },
            { "0x7FFF0001 << 32",
               0x7FFF0001 << 32 },
            { "0x7FFF0001 << 63",
               0x7FFF0001 << 63 },
            { "0x7FFF0001 << -12345",
               0x7FFF0001 << -12345 },
            { "0x7FFF0000L << 31",
               0x7FFF0000L << 31 },
            { "0x7FFF0000L << 32",
               0x7FFF0000L << 32 },
            { "0x7FFF0000L << 63",
               0x7FFF0000L << 63 },
            { "0x7FFF0000L << -12345",
               0x7FFF0000L << -12345 },
            { "0x70000000FFFF0000L >> 31",
               0x70000000FFFF0000L >> 31 },
            { "0x70000000FFFF0000L >> 32",
               0x70000000FFFF0000L >> 32 },
            { "0x70000000FFFF0000L >> 63",
               0x70000000FFFF0000L >> 63 },
            { "0x70000000FFFF0000L >> -12345",
               0x70000000FFFF0000L >> -12345 },
            { "0x70000000FFFF0000L >>> 31",
               0x70000000FFFF0000L >>> 31 },
            { "0x70000000FFFF0000L >>> 32",
               0x70000000FFFF0000L >>> 32 },
            { "0x70000000FFFF0000L >>> 63",
               0x70000000FFFF0000L >>> 63 },
            { "0x70000000FFFF0000L >>> -12345",
               0x70000000FFFF0000L >>> -12345 },
            { "0xFFFF0001 << 31",
               0xFFFF0001 << 31 },
            { "0xFFFF0001 << 32",
               0xFFFF0001 << 32 },
            { "0xFFFF0001 << 63",
               0xFFFF0001 << 63 },
            { "0xFFFF0001 << -12345",
               0xFFFF0001 << -12345 },
            { "0xFFFF0000L << 31",
               0xFFFF0000L << 31 },
            { "0xFFFF0000L << 32",
               0xFFFF0000L << 32 },
            { "0xFFFF0000L << 63",
               0xFFFF0000L << 63 },
            { "0xFFFF0000L << -12345",
               0xFFFF0000L << -12345 },
            { "0xF0000000FFFF0000L >> 31",
               0xF0000000FFFF0000L >> 31 },
            { "0xF0000000FFFF0000L >> 32",
               0xF0000000FFFF0000L >> 32 },
            { "0xF0000000FFFF0000L >> 63",
               0xF0000000FFFF0000L >> 63 },
            { "0xF0000000FFFF0000L >> -12345",
               0xF0000000FFFF0000L >> -12345 },
            { "0xF0000000FFFF0000L >>> 31",
               0xF0000000FFFF0000L >>> 31 },
            { "0xF0000000FFFF0000L >>> 32",
               0xF0000000FFFF0000L >>> 32 },
            { "0xF0000000FFFF0000L >>> 63",
               0xF0000000FFFF0000L >>> 63 },
            { "0xF0000000FFFF0000L >>> -12345",
               0xF0000000FFFF0000L >>> -12345 },
            { "037777777777",
               037777777777 },
            { "01777777777777777777777L",
               01777777777777777777777L },
            { "0b10000000000000000000000000000000",
               0b10000000000000000000000000000000 },
            { "0b1000000000000000000000000000000000000000000000000000000000000000L",
               0b1000000000000000000000000000000000000000000000000000000000000000L },
            { "100 + 2",
               100 + 2 },
            { "100 - 2",
               100 - 2 },
            { "100 * 2",
               100 * 2 },
            { "100 / 2",
               100 / 2 },
            { "100 % 2",
               100 % 2 },
            { "-(100)",
               -(100) },
            { "(Object)123 instanceof Integer",
               (Object)123 instanceof Integer },
            { "new String[3] instanceof String[]",
               new String[3] instanceof String[] },
            { "new String[3].getClass() == String[].class",
               new String[3].getClass() == String[].class },
            { "((Object[])new String[] { \"abc\" }).length",
               ((Object[])new String[] {  "abc"  }).length },

            // Misc
            { "java.lang.annotation.ElementType.FIELD",
               java.lang.annotation.ElementType.FIELD },
            { "java.util.Map.class",
               java.util.Map.class },
            { "java.util.Map.Entry.class",
               java.util.Map.Entry.class },
            { "new String(\"abcd\").hashCode()",
               new String("abcd").hashCode() },
            { "new String(\"abcd\").class",
               String.class },
            { "(byte)(short)(int)(float)123.45",
               (byte)(short)(int)(float)123.45 },
            { "4 & 7 | 6 << 5 >>> 2 << 1 >>> 2 + 6 * 3 - 7 / 2 ^ 99",
               4 & 7 | 6 << 5 >>> 2 << 1 >>> 2 + 6 * 3 - 7 / 2 ^ 99 },
            { "(null)",
              null },
            { "System.out.println(\"foobar\")", null },

            // Varargs
            { "String.format(\"abc\")", String.format("abc") },
            { "String.format(\"%s\", 123)", String.format("%s", 123) },
            { "String.format(\"%s %s\", 123, 456)", String.format("%s %s", 123, 456) },
            { "String.format(\"%s %s %s\", 123, 456, 789)", String.format("%s %s %s", 123, 456, 789) },
            { "new ExprParseTest.VarargsConstructor()", new ExprParseTest.VarargsConstructor() },
            { "new ExprParseTest.VarargsConstructor(\"a\")", new ExprParseTest.VarargsConstructor("a") },
            { "new ExprParseTest.VarargsConstructor(\"a\", \"b\")", new ExprParseTest.VarargsConstructor("a", "b") },
            { "new ExprParseTest.VarargsConstructor(1, 2)", new ExprParseTest.VarargsConstructor(1, 2) },
            { "new ExprParseTest.VarargsConstructor(1, 2, \"x\")", new ExprParseTest.VarargsConstructor(1, 2, "x") },

        //CHECKSTYLE ON: SimplifyBooleanExpression

        }));

        // Java 8 stuff
        if (System.getProperty("java.version").compareTo("1.8") >= 0) {
            list.addAll(Arrays.asList(new Object[][] {

                { "java.util.Arrays.asList(new String[] { \"abc\", \"d\", \"efghij\" }).stream().mapToInt(String::length).sum()",
                   10 },
                { "java.util.Arrays.asList(new String[] { \"abc\", \"def\" }).stream().map(Object::hashCode)"
                  + ".collect(java.util.stream.Collectors.toList()).toString()",
                   "[96354, 99333]" },

            }));
        }

        // Done
        return list.toArray(new Object[list.size()][]);
    }

    @Test(dataProvider = "multiCases")
    public void testBeanProperty(String[] exprs, Object expected) throws Exception {
        final ExprParser p = new ExprParser();
        Object actual = null;
        for (String expr : exprs) {
            final Node node = p.parse(this.session, new ParseContext(expr), false);
            actual = node.evaluate(this.session).get(this.session);
        }
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "multiCases")
    public Object[][] genMultiExprParseCases() {
        return new Object[][] {
            {
                new String[] {
                    "$x = new java.util.HashMap()",
                    "$x.put(\"abc\", \"def\")",
                    "$x.entrySet().iterator().next().value"
                },
                "def"
            }
        };
    }

    public static class VarargsConstructor {

        private final int x;
        private final int y;
        private final String[] array;

        public VarargsConstructor(String... array) {
            this(0, 0, array);
        }

        public VarargsConstructor(int x, int y, String... array) {
            this.x = x;
            this.y = y;
            this.array = array;
        }

        @Override
        public boolean equals(Object obj) {
            return this.toString().equals(String.valueOf(obj));
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return this.x + "," + this.y + "," + Arrays.<String>asList(this.array).toString();
        }
    }

    public static class ClassWithFields {

        public static int public_static = 123;
        public int public_instance = 456;
        private static int private_static = 789;
        private int private_instance = 333;

    }
}

