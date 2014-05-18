
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.TestSupport;
import org.jsimpledb.util.ParseContext;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ParamParserTest extends TestSupport {

    @Test(dataProvider = "cases")
    public void testParamParser(String specs, String command, Map<String, Object> expected) throws Exception {
        this.log.info("*** ParamParserTest: specs=\"" + specs + "\" command=\"" + command + "\"");
        final ParamParser paramParser = new ParamParser(new QuitCommand(), specs);
        try {
            final Map<String, Object> actual = paramParser.parseParameters(null, new ParseContext(command), false);
            Assert.assertEquals(actual, expected, "\n  ACTUAL: " + actual + "\nEXPECTED: " + expected + "\n");
        } catch (ParseException e) {
            if (expected != null)
                throw new Exception("parse failed for \"" + command + "\"", e);
        }
    }

    @DataProvider(name = "cases")
    public Object[][] genParamParserCases() {
        return new Object[][] {

        {
            "",
            "",
            buildMap()
        },

        {
            "",
            " foo",
            null,
        },

        {
            "-v",
            "",
            buildMap()
        },

        {
            "-v",
            "-v",
            buildMap("-v", null)
        },

        {
            "-v:",
            " -v",
            null,
        },

        {
            "-v:",
            " -v foo",
            buildMap("-v", "foo")
        },

        {
            "-v:",
            " -v foo; bar",
            buildMap("-v", "foo")
        },

        {
            "-v:int",
            " -v 123",
            buildMap("-v", 123)
        },

        {
            "foo:int*",
            "123",
            buildMap("foo", buildList(123)),
        },

        {
            "foo:int*",
            "",
            buildMap("foo", buildList()),
        },

        {
            "-v:int -d --foo p1:java.lang.String p2:double?",
            " --foo -v 123 -d -- \"hello there\"",
            buildMap("--foo", null, "-v", 123, "-d", null, "p1", "hello there")
        },

        {
            "-v:int -d --foo p1:java.lang.String p2:double?",
            " --foo -v 123 -d -- \"why not?\" 15.3",
            buildMap("--foo", null, "-v", 123, "-d", null, "p1", "why not?", "p2", 15.3)
        },

        {
            "-v:int -d --foo p1:java.lang.String p2:double+",
            " --foo -v 123 -d -- \"frobbely wobbely\"",
            null
        },

        {
            "-v:int -d --foo p1:java.lang.String p2:double+",
            " --foo -v 123 -d -- \"hello there\" -342.574e17",
            buildMap("--foo", null, "-v", 123, "-d", null, "p1", "hello there", "p2", buildList(-342.574e17))
        },

        {
            "-v:int -d --foo p1:java.lang.String p2:double*",
            " \"\" -Infinity -0.0 +0.0 123.45",
            buildMap("p1", "", "p2", buildList(Double.NEGATIVE_INFINITY, -0.0, +0.0, 123.45))
        },

        };
    }
}

