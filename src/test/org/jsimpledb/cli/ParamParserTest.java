
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.TestSupport;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.FieldTypeRegistry;
import org.jsimpledb.parse.FieldTypeParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.Parser;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ParamParserTest extends TestSupport {

    @Test(dataProvider = "cases")
    public void testParamParser(String specs, String command, Map<String, Object> expected) throws Exception {
        //this.log.info("*** ParamParserTest: specs=\"" + specs + "\" command=\"" + command + "\"");
        final ParamParser parser = new ParamParser(specs) {
            @Override
            protected Parser<?> getParser(String typeName) {
                final FieldType<?> fieldType = new FieldTypeRegistry().getFieldType(typeName);
                return fieldType != null ? this.createFieldTypeParser(fieldType) : super.getParser(typeName);
            }
            private <T> FieldTypeParser createFieldTypeParser(FieldType<T> fieldType) {
                return new FieldTypeParser<T>(fieldType);
            }
        };
        //this.log.info("*** ParamParserTest: optionFlags=" + parser.getOptionFlags());
        //this.log.info("*** ParamParserTest: parameters=" + parser.getParameters());
        try {
            final Map<String, Object> actual = parser.parse(null, new ParseContext(command), false);
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
            "-v:verbose",
            "",
            buildMap()
        },

        {
            "-v:verbose",
            " foo",
            null,
        },

        {
            "-v:verbose",
            " -v",
            buildMap("verbose", true)
        },

        {
            "-v:verbose",
            "",
            buildMap()
        },

        {
            "-c:count:int",
            " -v",
            null,
        },

        {
            "-c:count:int",
            " -c",
            null,
        },

        {
            "-c:count:int",
            " -c 234",
            buildMap("count", 234)
        },

        {
            "-c:count:int",
            " -c foo",
            null,
        },

        {
            "-c:count:word",
            " -c foo; bar",
            buildMap("count", "foo")
        },

        {
            "-v:foo:int",
            " -v 123",
            buildMap("foo", 123)
        },

        {
            "foo:int*",
            " 123",
            buildMap("foo", buildList(123)),
        },

        {
            "foo:int*",
            "",
            buildMap("foo", buildList()),
        },

        {
            "-v:version:int -d:debug --foo:foo p1:java.lang.String p2:double?",
            " --foo -v 123 -d -- \"hello there\"",
            buildMap("foo", true, "version", 123, "debug", true, "p1", "hello there")
        },

        {
            "-v:version:int -d:debug --foo:foo p1:java.lang.String p2:double?",
            " --foo -v 123 -d -- \"why not?\" 15.3",
            buildMap("foo", true, "version", 123, "debug", true, "p1", "why not?", "p2", 15.3)
        },

        {
            "-v:version:int -d:debug --foo:foo p1:java.lang.String p2:double+",
            " --foo -v 123 -d -- \"frobbely wobbely\"",
            null
        },

        {
            "-v:version:int -d:debug --foo:foo p1:java.lang.String p2:double+",
            " --foo -v 123 -d -- \"hello there\" -342.574e17",
            buildMap("foo", true, "version", 123, "debug", true, "p1", "hello there", "p2", buildList(-342.574e17))
        },

        {
            "-v:version:int -d:debug --foo:foo p1:java.lang.String p2:double*",
            " \"\" -Infinity -0.0 +0.0 123.45",
            buildMap("p1", "", "p2", buildList(Double.NEGATIVE_INFINITY, -0.0, +0.0, 123.45))
        },

        };
    }
}

