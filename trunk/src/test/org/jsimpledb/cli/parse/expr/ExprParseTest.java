
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import org.jsimpledb.TestSupport;
import org.jsimpledb.cli.Console;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
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

    private Session session;

    @BeforeClass
    public void setup() throws Exception {
        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);
        final Console console = new Console(db, System.in, System.out);
        this.session = console.getSession();
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
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "cases")
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

        };
    }
}

