
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import java.util.Arrays;

import org.jsimpledb.TestSupport;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
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
        return new Object[][] {

            // Literals
            { "true", true },
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

            // Literal fails
            { "'\''", PARSE_FAIL },
            { "\"foo", PARSE_FAIL },
            { "6fa", PARSE_FAIL },
            { "123.45fabc", PARSE_FAIL },

            // Idents and variables
            { "foobar", EVAL_FAIL },
            { "$foobar", EVAL_FAIL },
            { "$foobar = 12", 12 },
            { "$foobar", 12 },

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

            // Methods
            { "\"abc\".length()", 3 },
            { "\"abc\".bytes", new byte[] { (byte)'a', (byte)'b', (byte)'c' } },

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

        //CHECKSTYLE ON: SimplifyBooleanExpression

        };
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
}

